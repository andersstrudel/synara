import { appendFileSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { Effect, Exit, FileSystem, Scope } from "effect";
import { afterEach, describe, expect, it } from "vitest";

import { snapshotPrimeSessionUsage } from "./PrimeSessionUsage.ts";
import {
  makePrimeSessionUsageTail,
  type PrimeSessionUsageTail,
  type PrimeSessionUsageUpdateReason,
} from "./PrimeSessionUsageTail.ts";

interface ObservedUpdate {
  readonly reason: PrimeSessionUsageUpdateReason;
  readonly usedTokens: number | undefined;
  readonly assistantMessages: number;
  readonly entryId: unknown;
}

const tempDirs: string[] = [];
afterEach(() => {
  for (const dir of tempDirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

function makeSessionFile(): string {
  const dir = mkdtempSync(path.join(os.tmpdir(), "synara-prime-usage-tail-"));
  tempDirs.push(dir);
  const file = path.join(dir, "session.jsonl");
  writeFileSync(file, `${line(header())}\n`, "utf8");
  return file;
}

function header() {
  return {
    type: "session",
    version: 3,
    id: "prime-header-tail",
    timestamp: "2026-09-04T16:55:09.000Z",
    cwd: "/tmp/work",
    rlmDepth: 0,
  };
}

function line(entry: unknown): string {
  return JSON.stringify(entry);
}

function userLine(id: string, text: string): string {
  return line({
    type: "message",
    id,
    parentId: null,
    timestamp: "2026-09-04T16:55:09.500Z",
    message: { role: "user", content: [{ type: "text", text }], timestamp: 1 },
  });
}

function assistantLine(id: string, input: number, output: number): string {
  return line({
    type: "message",
    id,
    parentId: null,
    timestamp: "2026-09-04T16:55:10.000Z",
    message: {
      role: "assistant",
      content: [{ type: "text", text: "Hi there" }],
      api: "openai-completions",
      provider: "cerebras",
      model: "qwen-3.8-27b",
      stopReason: "stop",
      responseId: `resp-${id}`,
      timestamp: 1,
      usage: {
        input,
        output,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: input + output,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0.01 },
      },
    },
  });
}

function compactionLine(id: string, firstKeptEntryId: string, summary: string): string {
  return line({
    type: "compaction",
    id,
    parentId: null,
    timestamp: "2026-09-04T16:56:00.000Z",
    firstKeptEntryId,
    tokensBefore: 10_000,
    summary,
    details: {},
    fromHook: false,
  });
}

const waitFor = (predicate: () => boolean, timeoutMs = 5_000) =>
  Effect.gen(function* () {
    const startedAt = Date.now();
    while (!predicate()) {
      if (Date.now() - startedAt > timeoutMs) {
        throw new Error("Timed out waiting for tail update");
      }
      yield* Effect.sleep(10);
    }
  });

function startTail(
  file: string,
  updates: ObservedUpdate[],
  options: { readonly isActive?: () => boolean } = {},
) {
  return Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const tail = yield* makePrimeSessionUsageTail({
      fileSystem,
      file,
      isActive: options.isActive ?? (() => true),
      activePollMs: 50,
      idlePollMs: 100,
      minReportIntervalMs: 20,
      onUpdate: (update) =>
        Effect.sync(() => {
          const entry = update.entry as { readonly id?: unknown } | undefined;
          updates.push({
            reason: update.reason,
            usedTokens: snapshotPrimeSessionUsage(update.state, undefined)?.usedTokens,
            assistantMessages: update.state.assistantMessages,
            entryId: entry?.id,
          });
        }),
    });
    yield* tail.run.pipe(Effect.forkScoped);
    return tail;
  });
}

describe("makePrimeSessionUsageTail", () => {
  it("catches up, then reports appended usage, partial lines and compaction estimates", async () => {
    const file = makeSessionFile();
    appendFileSync(file, `${userLine("u1", "hello prime")}\n${assistantLine("a1", 7_000, 500)}\n`);
    const updates: ObservedUpdate[] = [];

    await Effect.runPromise(
      Effect.gen(function* () {
        yield* startTail(file, updates);
        yield* waitFor(() => updates.length >= 1);
        expect(updates[0]).toEqual({
          reason: "catch-up",
          usedTokens: 7_500,
          assistantMessages: 1,
          entryId: "a1",
        });

        // A partial line is held until its newline lands.
        const nextLine = assistantLine("a2", 7_600, 40);
        appendFileSync(file, nextLine.slice(0, 40));
        yield* Effect.sleep(200);
        expect(updates).toHaveLength(1);
        appendFileSync(file, `${nextLine.slice(40)}\n`);
        yield* waitFor(() => updates.length >= 2);
        expect(updates[1]).toEqual({
          reason: "usage",
          usedTokens: 7_640,
          assistantMessages: 2,
          entryId: "a2",
        });

        // Several lines in one append fold into a single report.
        appendFileSync(
          file,
          [
            assistantLine("a3", 8_000, 10),
            assistantLine("a4", 8_100, 10),
            assistantLine("a5", 8_200, 10),
          ]
            .map((entry) => `${entry}\n`)
            .join(""),
        );
        yield* waitFor(() => updates.length >= 3);
        yield* Effect.sleep(200);
        expect(updates).toHaveLength(3);
        expect(updates[2]).toMatchObject({
          usedTokens: 8_210,
          assistantMessages: 5,
          entryId: "a5",
        });

        // A compaction switches to the estimate: summary (25) + kept "hello prime"
        // (3) + a1..a5 ("Hi there" = 2 each).
        appendFileSync(file, `${compactionLine("c1", "u1", "s".repeat(100))}\n`);
        yield* waitFor(() => updates.length >= 4);
        expect(updates[3]).toEqual({
          reason: "estimate",
          usedTokens: 25 + 3 + 10,
          assistantMessages: 5,
          entryId: "c1",
        });
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    );
  });

  it("flush reports appended lines immediately and stops reporting once the scope closes", async () => {
    const file = makeSessionFile();
    const updates: ObservedUpdate[] = [];
    let tail: PrimeSessionUsageTail | undefined;

    await Effect.runPromise(
      Effect.gen(function* () {
        const scope = yield* Scope.make();
        // Idle tail: no fast poll, so a flush is what surfaces the append.
        tail = yield* startTail(file, updates, { isActive: () => false }).pipe(
          Scope.provide(scope),
        );
        yield* Effect.sleep(100);
        expect(updates).toHaveLength(0);

        appendFileSync(file, `${userLine("u1", "hello")}\n${assistantLine("a1", 100, 10)}\n`);
        yield* tail.flush;
        expect(updates).toHaveLength(1);
        expect(updates[0]).toMatchObject({ reason: "usage", usedTokens: 110 });
        // Nothing new: flush is a no-op.
        yield* tail.flush;
        expect(updates).toHaveLength(1);

        yield* Scope.close(scope, Exit.void);
        appendFileSync(file, `${assistantLine("a2", 200, 10)}\n`);
        yield* Effect.sleep(300);
        yield* tail.flush;
        expect(updates).toHaveLength(1);
      }).pipe(Effect.provide(NodeServices.layer)),
    );
  });

  it("starts over when the file shrinks", async () => {
    const file = makeSessionFile();
    appendFileSync(file, `${assistantLine("a1", 100, 10)}\n${assistantLine("a2", 200, 10)}\n`);
    const updates: ObservedUpdate[] = [];
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* startTail(file, updates);
        yield* waitFor(() => updates.length >= 1);
        // Rewritten shorter than the consumed offset: the tail must not keep
        // the old counters (Prime itself only appends; this is defensive).
        writeFileSync(file, `${line(header())}\n`, "utf8");
        appendFileSync(file, `${assistantLine("b1", 50, 5)}\n`);
        yield* waitFor(() => updates.length >= 2);
        expect(updates[1]).toMatchObject({ usedTokens: 55, assistantMessages: 1, entryId: "b1" });
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    );
  });
});
