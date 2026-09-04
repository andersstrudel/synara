/**
 * Tails a Prime Agent session file and reports token-usage updates as Prime
 * appends entries.
 *
 * Two triggers share one reader: a file watcher and a poll (fast while a turn
 * is in flight, slow otherwise). On macOS the watcher (FSEvents) is what fires
 * first and the poll only covers coalesced events. Prime rewrites the file
 * once through a temp file and `rename` (its first persisted assistant
 * message), which an inotify watch on Linux does not survive, so there the
 * poll is the trigger that matters after that rewrite. Reads are incremental
 * from a byte offset, so only appended bytes are parsed after the initial
 * catch-up, and reports are throttled so a fast provider (Cerebras answers
 * several times a second) cannot flood the runtime event stream.
 *
 * @module PrimeSessionUsageTail
 */
import { Effect, FileSystem, Queue, Semaphore, Stream } from "effect";

import {
  applyPrimeSessionEntry,
  createPrimeSessionUsageState,
  parsePrimeSessionLine,
  PrimeSessionLineSplitter,
  type PrimeSessionUsageChange,
  type PrimeSessionUsageState,
  readPrimeCompactionEntry,
} from "./PrimeSessionUsage.ts";

const PRIME_SESSION_READ_CHUNK_BYTES = 1024 * 1024;
const DEFAULT_ACTIVE_POLL_MS = 250;
const DEFAULT_IDLE_POLL_MS = 2_000;
// ~10 reports per second at most.
const DEFAULT_MIN_REPORT_INTERVAL_MS = 100;

export type PrimeSessionUsageUpdateReason = "catch-up" | PrimeSessionUsageChange;

export interface PrimeSessionUsageUpdate {
  readonly state: PrimeSessionUsageState;
  readonly reason: PrimeSessionUsageUpdateReason;
  /** The last entry that changed the reportable state. */
  readonly entry: unknown;
}

export interface PrimeSessionUsageTailOptions {
  readonly fileSystem: FileSystem.FileSystem;
  readonly file: string;
  /** True while a turn or compaction is in flight; selects the fast poll. */
  readonly isActive: () => boolean;
  readonly onUpdate: (update: PrimeSessionUsageUpdate) => Effect.Effect<void>;
  readonly activePollMs?: number;
  readonly idlePollMs?: number;
  readonly minReportIntervalMs?: number;
}

export interface PrimeSessionUsageTail {
  /**
   * Catches up on the whole file (one report), then tails it until
   * interrupted. Never fails: read errors are treated as "nothing new".
   */
  readonly run: Effect.Effect<void>;
  /**
   * Reads and reports whatever was appended since the last read, now. Before
   * `run` has caught up, this read is the catch-up (one report, no boundary
   * reporting), never a live tail of the whole file.
   */
  readonly flush: Effect.Effect<void>;
}

interface PrimeSessionReadResult {
  readonly change: PrimeSessionUsageChange;
  readonly entry: unknown;
  /** True when the read folded the file from its start: the first read, or a replaced file. */
  readonly catchUp: boolean;
}

function strongerChange(
  current: PrimeSessionUsageChange,
  next: PrimeSessionUsageChange,
): PrimeSessionUsageChange {
  // Exact usage supersedes an estimate within one read; either beats none.
  if (current === "usage" || next === "usage") {
    return "usage";
  }
  return next === "none" ? current : next;
}

export const makePrimeSessionUsageTail = (
  options: PrimeSessionUsageTailOptions,
): Effect.Effect<PrimeSessionUsageTail> =>
  Effect.gen(function* () {
    const activePollMs = options.activePollMs ?? DEFAULT_ACTIVE_POLL_MS;
    const idlePollMs = options.idlePollMs ?? DEFAULT_IDLE_POLL_MS;
    const minReportIntervalMs = options.minReportIntervalMs ?? DEFAULT_MIN_REPORT_INTERVAL_MS;
    // Serializes the reader between the tail loop and explicit flushes so the
    // byte offset and line buffer are only ever advanced by one fiber.
    const readLock = yield* Semaphore.make(1);
    // Capacity-one sliding queue: any number of watch/poll wake-ups that land
    // during a read collapse into a single follow-up read.
    const wake = yield* Queue.sliding<void>(1);

    let state = createPrimeSessionUsageState();
    const splitter = new PrimeSessionLineSplitter();
    let offset = 0;
    let lastReportAt = 0;
    let stopped = false;
    // False until a read has consumed the file from its start. Whichever
    // caller (run's catch-up or an early flush) gets the read lock first
    // performs that catch-up; the file's history is folded exactly once and
    // never reported entry by entry.
    let caughtUp = false;

    // `onBoundary` (live reads only) makes a compaction entry a report
    // boundary: the pending usage is reported first, then the compaction with
    // the fresh estimate, so the meter sees "compacted" between the two
    // readings even when Prime wrote both within one read. A catch-up never
    // reports boundaries: its compactions are history, not news.
    const readAppended = (
      onBoundary: (change: PrimeSessionUsageChange, entry: unknown) => Effect.Effect<void>,
    ): Effect.Effect<PrimeSessionReadResult | undefined> =>
      Effect.gen(function* () {
        const info = yield* options.fileSystem.stat(options.file);
        const size = Number(info.size);
        if (!Number.isFinite(size)) {
          return undefined;
        }
        let catchUp = !caughtUp;
        if (size < offset) {
          // Prime only appends; a shrunken file means it was replaced. Start over.
          state = createPrimeSessionUsageState();
          splitter.reset();
          offset = 0;
          catchUp = true;
        }
        if (size === offset) {
          return { change: "none", entry: undefined, catchUp } satisfies PrimeSessionReadResult;
        }
        const file = yield* options.fileSystem.open(options.file);
        yield* file.seek(offset, "start");
        let change: PrimeSessionUsageChange = "none";
        let entry: unknown;
        while (offset < size) {
          const chunk = yield* file.readAlloc(
            Math.min(size - offset, PRIME_SESSION_READ_CHUNK_BYTES),
          );
          if (chunk === undefined || chunk.length === 0) {
            break;
          }
          offset += chunk.length;
          for (const line of splitter.push(chunk)) {
            const parsed = parsePrimeSessionLine(line);
            if (parsed === undefined) {
              continue;
            }
            const isBoundary = !catchUp && readPrimeCompactionEntry(parsed) !== undefined;
            if (isBoundary && change !== "none") {
              yield* onBoundary(change, entry);
              change = "none";
              entry = undefined;
            }
            const lineChange = applyPrimeSessionEntry(state, parsed);
            if (lineChange === "none") {
              continue;
            }
            if (isBoundary) {
              yield* onBoundary(lineChange, parsed);
              continue;
            }
            change = strongerChange(change, lineChange);
            entry = parsed;
          }
        }
        return { change, entry, catchUp };
      }).pipe(
        Effect.scoped,
        // A momentary stat/open failure (file being rotated, permissions) is
        // retried by the next wake-up; there is nothing to surface per tick.
        Effect.orElseSucceed(() => undefined),
      );

    const throttle = Effect.suspend(() => {
      const waitMs = minReportIntervalMs - (Date.now() - lastReportAt);
      return waitMs > 0 ? Effect.sleep(waitMs) : Effect.void;
    });

    const readAndReport = readLock.withPermits(1)(
      Effect.gen(function* () {
        if (stopped) {
          return;
        }
        const report = (reason: PrimeSessionUsageUpdateReason, entry: unknown) =>
          options
            .onUpdate({ state, reason, entry })
            .pipe(Effect.tap(() => Effect.sync(() => (lastReportAt = Date.now()))));
        const result = yield* readAppended(report);
        if (result === undefined) {
          return;
        }
        caughtUp = true;
        if (result.change === "none") {
          return;
        }
        yield* throttle;
        yield* report(result.catchUp ? "catch-up" : result.change, result.entry);
      }),
    );

    const run = Effect.gen(function* () {
      yield* readAndReport;
      yield* Stream.runForEach(options.fileSystem.watch(options.file), () =>
        Queue.offer(wake, undefined),
      ).pipe(Effect.ignoreCause({ log: true }), Effect.forkScoped);
      yield* Effect.forever(
        Effect.gen(function* () {
          yield* Effect.sleep(options.isActive() ? activePollMs : idlePollMs);
          yield* Queue.offer(wake, undefined);
        }),
      ).pipe(Effect.forkScoped);
      yield* Effect.forever(
        Effect.gen(function* () {
          yield* Queue.take(wake);
          yield* readAndReport;
        }),
      );
    }).pipe(
      Effect.scoped,
      Effect.ensuring(
        Effect.sync(() => {
          stopped = true;
        }),
      ),
    );

    return { run, flush: readAndReport } satisfies PrimeSessionUsageTail;
  });
