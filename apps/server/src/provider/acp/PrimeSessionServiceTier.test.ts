import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { Effect } from "effect";
import { afterEach, describe, expect, it } from "vitest";

import { PRIME_SERVICE_TIER_DEFAULT, PRIME_SERVICE_TIER_FAST } from "./PrimeAcpSupport.ts";
import {
  appendPrimeServiceTierChange,
  buildPrimeServiceTierChangeEntry,
  generatePrimeSessionEntryId,
  indexPrimeSessionEntries,
  readPrimeSessionServiceTier,
  resolvePrimeSessionServiceTier,
} from "./PrimeSessionServiceTier.ts";

// Synthetic entries in the shapes prime-agent writes (session v3): a header
// line without an id, then entries chained through parentId.
const HEADER = {
  type: "session",
  version: 3,
  id: "prime-header-tier",
  timestamp: "2026-09-04T10:00:00.000Z",
  cwd: "/work",
  rlmDepth: 0,
};

function messageEntry(id: string, parentId: string | null, text: string) {
  return {
    type: "message",
    id,
    parentId,
    timestamp: "2026-09-04T10:00:01.000Z",
    message: { role: "user", content: [{ type: "text", text }], timestamp: 0 },
  };
}

function tierEntry(id: string, parentId: string | null, serviceTier: string) {
  return {
    type: "service_tier_change",
    id,
    parentId,
    timestamp: "2026-09-04T10:00:02.000Z",
    serviceTier,
  };
}

function jsonl(entries: ReadonlyArray<unknown>): string {
  return entries.map((entry) => `${JSON.stringify(entry)}\n`).join("");
}

function readEntries(file: string): ReadonlyArray<Record<string, unknown>> {
  return readFileSync(file, "utf8")
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

const tempDirs: string[] = [];
afterEach(() => {
  for (const dir of tempDirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

function writeSessionFile(content: string): string {
  const dir = mkdtempSync(path.join(os.tmpdir(), "synara-prime-tier-"));
  tempDirs.push(dir);
  const file = path.join(dir, `${crypto.randomUUID()}.jsonl`);
  writeFileSync(file, content, "utf8");
  return file;
}

const run = <A, E>(effect: Effect.Effect<A, E, never>) => Effect.runPromise(effect);
const withFs = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(NodeServices.layer));

describe("readPrimeSessionServiceTier", () => {
  it("reads default from a header-only file and from a branch without a tier entry", async () => {
    const headerOnly = writeSessionFile(jsonl([HEADER]));
    expect(await run(withFs(readPrimeSessionServiceTier(headerOnly)))).toBe(
      PRIME_SERVICE_TIER_DEFAULT,
    );

    const noTier = writeSessionFile(
      jsonl([HEADER, messageEntry("a1", null, "hi"), messageEntry("a2", "a1", "again")]),
    );
    expect(await run(withFs(readPrimeSessionServiceTier(noTier)))).toBe("default");
  });

  it("reads the last tier entry on the leaf's branch", async () => {
    const file = writeSessionFile(
      jsonl([
        HEADER,
        tierEntry("t1", null, "default"),
        messageEntry("m1", "t1", "hi"),
        tierEntry("t2", "m1", "priority"),
        messageEntry("m2", "t2", "faster please"),
      ]),
    );
    expect(await run(withFs(readPrimeSessionServiceTier(file)))).toBe(PRIME_SERVICE_TIER_FAST);
  });

  it("follows the leaf's branch after a rollback, not file order", async () => {
    // The tier switched to priority on a branch the user then rolled back
    // (b3 hangs off m1 again); Prime resumes from the leaf b3's ancestry.
    const rolledBack = writeSessionFile(
      jsonl([
        HEADER,
        messageEntry("m1", null, "hi"),
        tierEntry("t1", "m1", "priority"),
        messageEntry("m2", "t1", "fast reply"),
        tierEntry("b3", "m1", "default"),
        messageEntry("b4", "b3", "back on the slow lane"),
      ]),
    );
    expect(await run(withFs(readPrimeSessionServiceTier(rolledBack)))).toBe("default");

    const rolledForward = writeSessionFile(
      jsonl([
        HEADER,
        messageEntry("m1", null, "hi"),
        tierEntry("t1", "m1", "default"),
        messageEntry("m2", "t1", "reply"),
        tierEntry("b3", "m1", "priority"),
      ]),
    );
    expect(await run(withFs(readPrimeSessionServiceTier(rolledForward)))).toBe("priority");
  });

  it("skips blank and malformed lines like Prime's parseSessionEntries", async () => {
    const file = writeSessionFile(
      `${JSON.stringify(HEADER)}\n\n{not json\n${JSON.stringify(tierEntry("t1", null, "priority"))}\n"a string line"\n`,
    );
    expect(await run(withFs(readPrimeSessionServiceTier(file)))).toBe("priority");
  });

  it("fails on a missing file instead of guessing", async () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), "synara-prime-tier-"));
    tempDirs.push(dir);
    const exit = await Effect.runPromiseExit(
      withFs(readPrimeSessionServiceTier(path.join(dir, "missing.jsonl"))),
    );
    expect(exit._tag).toBe("Failure");
  });
});

describe("appendPrimeServiceTierChange", () => {
  it("appends an entry parented on the leaf with a fresh 8-hex id", async () => {
    const file = writeSessionFile(
      jsonl([
        HEADER,
        tierEntry("t1", null, "default"),
        messageEntry("m1", "t1", "hi"),
        messageEntry("m2", "m1", "again"),
      ]),
    );
    const entry = await run(withFs(appendPrimeServiceTierChange(file, PRIME_SERVICE_TIER_FAST)));

    expect(entry).toMatchObject({
      type: "service_tier_change",
      parentId: "m2",
      serviceTier: "priority",
    });
    expect(entry.id).toMatch(/^[0-9a-f]{8}$/u);
    expect(["t1", "m1", "m2"]).not.toContain(entry.id);
    expect(Number.isNaN(Date.parse(entry.timestamp))).toBe(false);

    const entries = readEntries(file);
    expect(entries).toHaveLength(5);
    expect(entries.at(-1)).toEqual(entry);
    expect(readFileSync(file, "utf8").endsWith("\n")).toBe(true);
    expect(await run(withFs(readPrimeSessionServiceTier(file)))).toBe("priority");
  });

  it("starts the chain on a header-only file", async () => {
    const file = writeSessionFile(jsonl([HEADER]));
    const entry = await run(withFs(appendPrimeServiceTierChange(file, PRIME_SERVICE_TIER_FAST)));

    expect(entry.parentId).toBeNull();
    expect(readEntries(file)).toEqual([HEADER, entry]);
    expect(await run(withFs(readPrimeSessionServiceTier(file)))).toBe("priority");

    // A second change chains onto the first.
    const reverted = await run(
      withFs(appendPrimeServiceTierChange(file, PRIME_SERVICE_TIER_DEFAULT)),
    );
    expect(reverted.parentId).toBe(entry.id);
    expect(reverted.id).not.toBe(entry.id);
    expect(await run(withFs(readPrimeSessionServiceTier(file)))).toBe("default");
  });

  it("keeps the entry on its own line when the file lost its trailing newline", async () => {
    const file = writeSessionFile(
      `${JSON.stringify(HEADER)}\n${JSON.stringify(messageEntry("m1", null, "hi"))}`,
    );
    const entry = await run(withFs(appendPrimeServiceTierChange(file, PRIME_SERVICE_TIER_FAST)));
    expect(entry.parentId).toBe("m1");
    expect(readEntries(file).map((line) => line.id)).toEqual(["prime-header-tier", "m1", entry.id]);
  });
});

describe("Prime session entry index", () => {
  it("indexes entries by id with the last entry as the leaf", () => {
    const index = indexPrimeSessionEntries(
      jsonl([HEADER, messageEntry("m1", null, "hi"), tierEntry("t1", "m1", "priority")]),
    );
    expect([...index.byId.keys()]).toEqual(["m1", "t1"]);
    expect(index.leafId).toBe("t1");
    expect(indexPrimeSessionEntries("").leafId).toBeUndefined();
  });

  it("returns default for a cyclic or dangling parent chain instead of looping", () => {
    expect(
      resolvePrimeSessionServiceTier(
        indexPrimeSessionEntries(jsonl([messageEntry("a", "b", "x"), messageEntry("b", "a", "y")])),
      ),
    ).toBe("default");
    expect(
      resolvePrimeSessionServiceTier(
        indexPrimeSessionEntries(jsonl([messageEntry("a", "missing", "x")])),
      ),
    ).toBe("default");
  });

  it("returns an unrecognised tier verbatim so callers still reconcile it", () => {
    expect(
      resolvePrimeSessionServiceTier(
        indexPrimeSessionEntries(jsonl([tierEntry("t", null, "flex")])),
      ),
    ).toBe("flex");
  });

  it("generates ids that avoid the ones already in the file", () => {
    const taken = new Map<string, Record<string, unknown>>();
    const id = generatePrimeSessionEntryId(taken);
    expect(id).toMatch(/^[0-9a-f]{8}$/u);
    const entry = buildPrimeServiceTierChangeEntry(
      { byId: taken, leafId: undefined },
      PRIME_SERVICE_TIER_DEFAULT,
      new Date("2026-09-04T12:00:00.000Z"),
    );
    expect(entry).toMatchObject({
      type: "service_tier_change",
      parentId: null,
      timestamp: "2026-09-04T12:00:00.000Z",
      serviceTier: "default",
    });
  });
});
