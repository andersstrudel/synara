/**
 * Prime Agent session-file service tier — reads the tier a `--resume` would
 * seed and appends the `service_tier_change` entry Prime's own `/fast`
 * command writes.
 *
 * Prime's ACP mode cannot set the tier per request (`/fast` is a TUI-only
 * command, `session/new` takes no tier, and there is no flag or env var), but
 * `createAgentSession` seeds a resumed session from the last
 * `service_tier_change` on its branch. Synara therefore edits the session
 * file while no Prime child owns it and relaunches with `--resume`.
 *
 * Mirrors prime-agent 0.9.1 `SessionManager`: the leaf is the last entry in
 * file order (`_buildIndex`), the branch is the parentId walk from the leaf
 * (`getBranch`), the tier in force is the last `service_tier_change` on that
 * branch or `default` (`buildSessionContext`), and a new entry takes an
 * 8-hex id unique within the file with the leaf as its parent
 * (`appendServiceTierChange`). Blank and malformed lines are skipped the way
 * Prime's `parseSessionEntries` skips them; a header-only file has no leaf.
 *
 * @module PrimeSessionServiceTier
 */
import { randomUUID } from "node:crypto";

import { Effect, FileSystem, type PlatformError } from "effect";

import { PRIME_SERVICE_TIER_DEFAULT, type PrimeServiceTier } from "./PrimeAcpSupport.ts";
import { parsePrimeSessionLine } from "./PrimeSessionUsage.ts";

const PRIME_SESSION_ENTRY_ID_LENGTH = 8;
const PRIME_SESSION_ENTRY_ID_ATTEMPTS = 100;

export interface PrimeSessionEntryIndex {
  /** Entries with an id, keyed by id; the `session` header has none and is skipped. */
  readonly byId: ReadonlyMap<string, Record<string, unknown>>;
  /** Id of the last entry in the file — where Prime appends next. */
  readonly leafId: string | undefined;
}

/** The entry Prime appends for a tier change (session v3). */
export interface PrimeServiceTierChangeEntry {
  readonly type: "service_tier_change";
  readonly id: string;
  readonly parentId: string | null;
  readonly timestamp: string;
  readonly serviceTier: PrimeServiceTier;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Indexes a session file's entries the way Prime's `_buildIndex` does. */
export function indexPrimeSessionEntries(content: string): PrimeSessionEntryIndex {
  const byId = new Map<string, Record<string, unknown>>();
  let leafId: string | undefined;
  for (const line of content.split("\n")) {
    const entry = parsePrimeSessionLine(line);
    if (
      !isRecord(entry) ||
      entry.type === "session" ||
      typeof entry.id !== "string" ||
      entry.id.length === 0
    ) {
      continue;
    }
    byId.set(entry.id, entry);
    leafId = entry.id;
  }
  return { byId, leafId };
}

/**
 * The tier the next `--resume` would seed: the nearest `service_tier_change`
 * walking up from the leaf, `default` when the branch has none. Prime writes
 * `default` or `priority`; an unrecognised value is returned verbatim so a
 * caller comparing against the tier it wants still reconciles it.
 */
export function resolvePrimeSessionServiceTier(index: PrimeSessionEntryIndex): string {
  // Prime's walk assumes a well-formed chain; a corrupted file must not hang here.
  const visited = new Set<string>();
  let currentId = index.leafId;
  while (currentId !== undefined && !visited.has(currentId)) {
    visited.add(currentId);
    const entry = index.byId.get(currentId);
    if (entry === undefined) {
      break;
    }
    if (
      entry.type === "service_tier_change" &&
      typeof entry.serviceTier === "string" &&
      entry.serviceTier.length > 0
    ) {
      return entry.serviceTier;
    }
    currentId = typeof entry.parentId === "string" ? entry.parentId : undefined;
  }
  return PRIME_SERVICE_TIER_DEFAULT;
}

/** Prime's `generateId`: the first 8 hex chars of a UUID, unique within the file. */
export function generatePrimeSessionEntryId(byId: ReadonlyMap<string, unknown>): string {
  for (let attempt = 0; attempt < PRIME_SESSION_ENTRY_ID_ATTEMPTS; attempt += 1) {
    const id = randomUUID().slice(0, PRIME_SESSION_ENTRY_ID_LENGTH);
    if (!byId.has(id)) {
      return id;
    }
  }
  return randomUUID();
}

export function buildPrimeServiceTierChangeEntry(
  index: PrimeSessionEntryIndex,
  serviceTier: PrimeServiceTier,
  now: Date = new Date(),
): PrimeServiceTierChangeEntry {
  return {
    type: "service_tier_change",
    id: generatePrimeSessionEntryId(index.byId),
    parentId: index.leafId ?? null,
    timestamp: now.toISOString(),
    serviceTier,
  };
}

/** The tier Prime would seed on the next `--resume` of `file`. */
export const readPrimeSessionServiceTier = (
  file: string,
): Effect.Effect<string, PlatformError.PlatformError, FileSystem.FileSystem> =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const content = yield* fileSystem.readFileString(file);
    return resolvePrimeSessionServiceTier(indexPrimeSessionEntries(content));
  });

/**
 * Appends the entry Prime's `/fast` would write, as one JSONL line. Only call
 * while no Prime child owns the file: Prime rewrites the whole file from
 * memory on its first persisted assistant message, so an entry appended
 * underneath a live child would be lost or interleaved.
 */
export const appendPrimeServiceTierChange = (
  file: string,
  serviceTier: PrimeServiceTier,
): Effect.Effect<PrimeServiceTierChangeEntry, PlatformError.PlatformError, FileSystem.FileSystem> =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const content = yield* fileSystem.readFileString(file);
    const entry = buildPrimeServiceTierChangeEntry(indexPrimeSessionEntries(content), serviceTier);
    // A file whose last line lost its newline would otherwise swallow the
    // entry into that line.
    const separator = content.length === 0 || content.endsWith("\n") ? "" : "\n";
    yield* fileSystem.writeFileString(file, `${separator}${JSON.stringify(entry)}\n`, {
      flag: "a",
    });
    return entry;
  });
