import { describe, expect, it } from "vitest";

import {
  applyPrimeSessionEntry,
  calculatePrimeContextTokens,
  createPrimeSessionUsageState,
  describePrimeSessionEntry,
  estimatePrimeMessageTokens,
  parsePrimeSessionLine,
  PrimeSessionLineSplitter,
  type PrimeSessionUsageState,
  resolvePrimeSessionUsageModel,
  snapshotPrimeSessionUsage,
} from "./PrimeSessionUsage.ts";

// Synthetic entries in the shapes prime-agent writes (session v3). Token
// numbers are made up; the structure (ids, parent chain, usage block with
// cost, provider/model on the assistant message) mirrors the real file.
const CEREBRAS_QWEN = { provider: "cerebras", model: "qwen-3.8-27b" } as const;

function usageBlock(input: {
  readonly input: number;
  readonly output: number;
  readonly cacheRead?: number;
  readonly cacheWrite?: number;
  readonly totalTokens?: number | null;
  readonly costTotal?: number;
}) {
  const cacheRead = input.cacheRead ?? 0;
  const cacheWrite = input.cacheWrite ?? 0;
  const total =
    input.totalTokens === undefined
      ? input.input + input.output + cacheRead + cacheWrite
      : input.totalTokens;
  return {
    input: input.input,
    output: input.output,
    cacheRead,
    cacheWrite,
    ...(total === null ? {} : { totalTokens: total }),
    cost: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      total: input.costTotal ?? 0,
    },
  };
}

function assistantEntry(input: {
  readonly id: string;
  readonly parentId: string;
  readonly content?: ReadonlyArray<Record<string, unknown>>;
  readonly usage?: ReturnType<typeof usageBlock>;
  readonly stopReason?: string;
  readonly model?: { readonly provider: string; readonly model: string };
}) {
  const model = input.model ?? CEREBRAS_QWEN;
  return {
    type: "message",
    id: input.id,
    parentId: input.parentId,
    timestamp: "2026-09-04T16:55:10.000Z",
    message: {
      role: "assistant",
      content: input.content ?? [{ type: "text", text: "Hi there" }],
      api: "openai-completions",
      provider: model.provider,
      model: model.model,
      stopReason: input.stopReason ?? "stop",
      responseId: `resp-${input.id}`,
      timestamp: 1_788_000_000_000,
      ...(input.usage ? { usage: input.usage } : {}),
    },
  };
}

function userEntry(id: string, parentId: string | null, text: string) {
  return {
    type: "message",
    id,
    parentId,
    timestamp: "2026-09-04T16:55:09.500Z",
    message: {
      role: "user",
      content: [{ type: "text", text }],
      timestamp: 1_788_000_000_000,
    },
  };
}

function toolResultEntry(id: string, parentId: string, text: string) {
  return {
    type: "message",
    id,
    parentId,
    timestamp: "2026-09-04T16:55:11.000Z",
    message: {
      role: "toolResult",
      toolCallId: "call-1",
      toolName: "read",
      content: [{ type: "text", text }],
      isError: false,
      timestamp: 1_788_000_000_000,
    },
  };
}

function compactionEntry(input: {
  readonly id: string;
  readonly parentId: string;
  readonly firstKeptEntryId: string;
  readonly summary: string;
  readonly tokensBefore?: number;
}) {
  return {
    type: "compaction",
    id: input.id,
    parentId: input.parentId,
    timestamp: "2026-09-04T16:56:00.000Z",
    firstKeptEntryId: input.firstKeptEntryId,
    tokensBefore: input.tokensBefore ?? 118_959,
    summary: input.summary,
    details: { readFiles: [], modifiedFiles: [] },
    fromHook: false,
  };
}

const SESSION_HEADER = {
  type: "session",
  version: 3,
  id: "01a0-session",
  timestamp: "2026-09-04T16:55:09.000Z",
  cwd: "/tmp/work",
  rlmDepth: 0,
};

function fold(state: PrimeSessionUsageState, entries: ReadonlyArray<unknown>) {
  return entries.map((entry) => applyPrimeSessionEntry(state, entry));
}

describe("calculatePrimeContextTokens", () => {
  it("prefers the native total and falls back to the component sum", () => {
    expect(
      calculatePrimeContextTokens({
        input: 10,
        output: 5,
        cacheRead: 2,
        cacheWrite: 1,
        totalTokens: 99,
        costUsd: undefined,
      }),
    ).toBe(99);
    expect(
      calculatePrimeContextTokens({
        input: 10,
        output: 5,
        cacheRead: 2,
        cacheWrite: 1,
        totalTokens: undefined,
        costUsd: undefined,
      }),
    ).toBe(18);
  });
});

describe("estimatePrimeMessageTokens", () => {
  it("mirrors Prime's chars/4 estimate per role", () => {
    expect(estimatePrimeMessageTokens({ role: "user", content: "x".repeat(10) })).toBe(3);
    expect(
      estimatePrimeMessageTokens({
        role: "user",
        content: [
          { type: "text", text: "x".repeat(8) },
          { type: "image", data: "ignored-for-user" },
        ],
      }),
    ).toBe(2);
    expect(
      estimatePrimeMessageTokens({
        role: "assistant",
        content: [
          { type: "thinking", thinking: "t".repeat(8) },
          { type: "text", text: "a".repeat(4) },
          { type: "toolCall", id: "call-1", name: "read", arguments: { path: "x" } },
        ],
      }),
      // 8 + 4 + ("read".length + JSON.stringify({path:"x"}).length = 4 + 12)
    ).toBe(Math.ceil((8 + 4 + 16) / 4));
    expect(
      estimatePrimeMessageTokens({
        role: "toolResult",
        content: [
          { type: "text", text: "r".repeat(40) },
          { type: "image", mimeType: "image/png", data: "" },
        ],
      }),
    ).toBe(Math.ceil((40 + 4_800) / 4));
    expect(estimatePrimeMessageTokens({ role: "custom", content: "c".repeat(9) })).toBe(3);
    expect(
      estimatePrimeMessageTokens({ role: "bashExecution", command: "ls", output: "a\nb" }),
    ).toBe(2);
    expect(
      estimatePrimeMessageTokens({ role: "compactionSummary", summary: "s".repeat(400) }),
    ).toBe(100);
    expect(estimatePrimeMessageTokens({ role: "somethingElse", content: "xxxx" })).toBe(0);
    expect(estimatePrimeMessageTokens("not a message")).toBe(0);
  });
});

describe("applyPrimeSessionEntry", () => {
  it("folds assistant usage into an exact context snapshot with cumulative totals", () => {
    const state = createPrimeSessionUsageState();
    const changes = fold(state, [
      SESSION_HEADER,
      {
        type: "model_change",
        id: "m1",
        parentId: null,
        timestamp: "t",
        ...CEREBRAS_QWEN,
        modelId: CEREBRAS_QWEN.model,
      },
      userEntry("u1", "m1", "hello prime"),
      assistantEntry({
        id: "a1",
        parentId: "u1",
        content: [
          { type: "text", text: "Reading" },
          { type: "toolCall", id: "call-1", name: "read", arguments: { path: "README.md" } },
        ],
        stopReason: "toolUse",
        usage: usageBlock({ input: 7_153, output: 378, costTotal: 0.0471 }),
      }),
      toolResultEntry("r1", "a1", "# README"),
      assistantEntry({
        id: "a2",
        parentId: "r1",
        usage: usageBlock({ input: 7_600, output: 50, cacheRead: 100, costTotal: 0.03 }),
      }),
    ]);

    expect(changes).toEqual(["none", "none", "none", "usage", "none", "usage"]);
    expect(snapshotPrimeSessionUsage(state, 131_072)).toEqual({
      usedTokens: 7_750,
      usedPercent: (7_750 / 131_072) * 100,
      totalProcessedTokens: 7_153 + 378 + 7_600 + 50 + 100,
      maxTokens: 131_072,
      inputTokens: 7_153 + 7_600,
      cachedInputTokens: 100,
      outputTokens: 378 + 50,
      lastUsedTokens: 7_750,
      lastInputTokens: 7_600,
      lastCachedInputTokens: 100,
      lastOutputTokens: 50,
      toolUses: 1,
      compactsAutomatically: true,
    });
    expect(state.costUsd).toBeCloseTo(0.0771, 10);
    expect(state.assistantMessages).toBe(2);
    expect(resolvePrimeSessionUsageModel(state)).toEqual({
      provider: "cerebras",
      modelId: "qwen-3.8-27b",
    });
  });

  it("omits maxTokens and usedPercent when the context window is unknown", () => {
    const state = createPrimeSessionUsageState();
    fold(state, [
      userEntry("u1", null, "hi"),
      assistantEntry({ id: "a1", parentId: "u1", usage: usageBlock({ input: 100, output: 20 }) }),
    ]);
    const snapshot = snapshotPrimeSessionUsage(state, undefined);
    expect(snapshot).toMatchObject({ usedTokens: 120, compactsAutomatically: true });
    expect(snapshot).not.toHaveProperty("maxTokens");
    expect(snapshot).not.toHaveProperty("usedPercent");
    expect(snapshotPrimeSessionUsage(state, 0)).not.toHaveProperty("maxTokens");
  });

  it("has nothing to report before the first assistant usage", () => {
    const state = createPrimeSessionUsageState();
    fold(state, [SESSION_HEADER, userEntry("u1", null, "hello")]);
    expect(snapshotPrimeSessionUsage(state, 131_072)).toBeUndefined();
    expect(resolvePrimeSessionUsageModel(state)).toBeUndefined();
  });

  it("skips aborted and error responses for context but keeps their totals", () => {
    const state = createPrimeSessionUsageState();
    fold(state, [
      userEntry("u1", null, "hello"),
      assistantEntry({
        id: "a1",
        parentId: "u1",
        usage: usageBlock({ input: 1_000, output: 10, costTotal: 0.01 }),
      }),
      assistantEntry({
        id: "a2",
        parentId: "a1",
        stopReason: "aborted",
        usage: usageBlock({ input: 1_200, output: 5, costTotal: 0.02 }),
      }),
      assistantEntry({
        id: "a3",
        parentId: "a2",
        stopReason: "error",
        usage: usageBlock({ input: 1_300, output: 0 }),
      }),
    ]);
    expect(snapshotPrimeSessionUsage(state, undefined)).toMatchObject({
      usedTokens: 1_010,
      totalProcessedTokens: 1_000 + 10 + 1_200 + 5 + 1_300,
      lastUsedTokens: 1_300,
      lastInputTokens: 1_300,
    });
    expect(state.costUsd).toBeCloseTo(0.03, 10);
  });

  it("bills attributed sub-agent usage to the totals and cost, never the context", () => {
    const state = createPrimeSessionUsageState();
    const changes = fold(state, [
      SESSION_HEADER,
      userEntry("u1", null, "hello"),
      assistantEntry({
        id: "a1",
        parentId: "u1",
        usage: usageBlock({ input: 500, output: 50, costTotal: 0.01 }),
      }),
      // Prime's attributeChildUsage adds the child's usage to a1's block on
      // load but keeps a1's totalTokens, so /usage totals move and context
      // does not.
      {
        type: "child_usage_attributed",
        id: "x1",
        parentId: "a1",
        timestamp: "t",
        targetId: "a1",
        childUsage: usageBlock({ input: 9_000, output: 900, cacheRead: 40, costTotal: 0.2 }),
        aggregateUsage: usageBlock({
          input: 9_500,
          output: 950,
          cacheRead: 40,
          totalTokens: 550,
          costTotal: 0.21,
        }),
        origin: { kind: "rlm", depth: 1 },
      },
    ]);
    expect(changes).toEqual(["none", "none", "usage", "usage"]);
    expect(snapshotPrimeSessionUsage(state, undefined)).toMatchObject({
      usedTokens: 550,
      totalProcessedTokens: 550 + 9_940,
      inputTokens: 9_500,
      cachedInputTokens: 40,
      outputTokens: 950,
      lastUsedTokens: 550,
      lastInputTokens: 500,
    });
    expect(state.assistantMessages).toBe(1);
    expect(state.costUsd).toBeCloseTo(0.21, 10);
    // A malformed attribution is skipped like a malformed usage block.
    expect(
      fold(state, [
        {
          type: "child_usage_attributed",
          id: "x2",
          parentId: "x1",
          timestamp: "t",
          targetId: "a1",
        },
        {
          type: "child_usage_attributed",
          id: "x3",
          parentId: "x2",
          timestamp: "t",
          targetId: "a1",
          childUsage: "9000",
        },
      ]),
    ).toEqual(["none", "none"]);
    expect(
      applyPrimeSessionEntry(state, {
        type: "child_usage_attributed",
        id: "x4",
        parentId: "x3",
        timestamp: "t",
        targetId: "a1",
        childUsage: { input: Number.NaN, output: -5, cacheRead: "7", cost: { total: "free" } },
      }),
    ).toBe("usage");
    expect(snapshotPrimeSessionUsage(state, undefined)).toMatchObject({
      totalProcessedTokens: 550 + 9_940,
    });
    expect(state.costUsd).toBeCloseTo(0.21, 10);
  });

  it("ignores headers, daemon bookkeeping and malformed lines", () => {
    const state = createPrimeSessionUsageState();
    fold(state, [
      SESSION_HEADER,
      userEntry("u1", null, "hello"),
      assistantEntry({ id: "a1", parentId: "u1", usage: usageBlock({ input: 500, output: 50 }) }),
      { type: "agent_status", id: "s1", parentId: "a1", timestamp: "t", status: "idle" },
      { type: "session_state", id: "s2", parentId: "s1", timestamp: "t", state: {} },
      {
        type: "thinking_level_change",
        id: "t1",
        parentId: "s2",
        timestamp: "t",
        thinkingLevel: "high",
      },
      "not an object",
      null,
      { noType: true },
    ]);
    for (const line of ["", "   ", "{ this is not json", "[1,2,3]", '"a string"']) {
      expect(parsePrimeSessionLine(line)).toBeUndefined();
    }
    expect(snapshotPrimeSessionUsage(state, undefined)).toMatchObject({
      usedTokens: 550,
      totalProcessedTokens: 550,
      inputTokens: 500,
    });
    expect(state.assistantMessages).toBe(1);
    expect(state.recentEntries.map((entry) => entry.id)).toEqual([
      "01a0-session",
      "u1",
      "a1",
      "s1",
      "s2",
      "t1",
    ]);
  });

  it("estimates the reduced context after a compaction until the next exact usage", () => {
    const state = createPrimeSessionUsageState();
    const summary = "S".repeat(400);
    const before = fold(state, [
      SESSION_HEADER,
      userEntry("u0", null, "an older prompt that gets summarized away"),
      assistantEntry({
        id: "a0",
        parentId: "u0",
        usage: usageBlock({ input: 100_000, output: 100 }),
      }),
      userEntry("u1", "a0", "hello prime"),
      assistantEntry({
        id: "a1",
        parentId: "u1",
        usage: usageBlock({ input: 118_000, output: 959, costTotal: 0.5 }),
      }),
    ]);
    expect(before.at(-1)).toBe("usage");

    // Compaction keeps u1 onward: summary (100) + "hello prime" (3) + "Hi there" (2).
    expect(
      applyPrimeSessionEntry(
        state,
        compactionEntry({ id: "c1", parentId: "a1", firstKeptEntryId: "u1", summary }),
      ),
    ).toBe("estimate");
    expect(state.estimate).toEqual({
      summaryTokens: 100,
      retainedTokens: 5,
      trailingTokens: 0,
    });
    expect(snapshotPrimeSessionUsage(state, 131_072)).toMatchObject({
      usedTokens: 105,
      maxTokens: 131_072,
      totalProcessedTokens: 100_100 + 118_959,
      lastUsedTokens: 118_959,
    });

    // Entries appended after the compaction extend the estimate, an aborted
    // assistant message included; a model switch re-targets the window.
    expect(applyPrimeSessionEntry(state, toolResultEntry("r1", "c1", "x".repeat(40)))).toBe(
      "estimate",
    );
    expect(
      applyPrimeSessionEntry(
        state,
        assistantEntry({
          id: "a2",
          parentId: "r1",
          stopReason: "aborted",
          content: [{ type: "text", text: "wait" }],
          usage: usageBlock({ input: 0, output: 0 }),
        }),
      ),
    ).toBe("usage");
    expect(snapshotPrimeSessionUsage(state, 131_072)?.usedTokens).toBe(105 + 10 + 1);
    expect(
      applyPrimeSessionEntry(state, {
        type: "model_change",
        id: "m2",
        parentId: "a2",
        timestamp: "t",
        provider: "anthropic",
        modelId: "claude-fable-5-1",
      }),
    ).toBe("estimate");
    expect(resolvePrimeSessionUsageModel(state)).toEqual({
      provider: "anthropic",
      modelId: "claude-fable-5-1",
    });

    // The next valid assistant usage replaces the estimate with exact context.
    expect(
      applyPrimeSessionEntry(
        state,
        assistantEntry({
          id: "a3",
          parentId: "m2",
          model: { provider: "anthropic", model: "claude-fable-5-1" },
          usage: usageBlock({ input: 9_000, output: 100, costTotal: 0.1 }),
        }),
      ),
    ).toBe("usage");
    expect(state.estimate).toBeUndefined();
    expect(snapshotPrimeSessionUsage(state, 1_000_000)).toMatchObject({
      usedTokens: 9_100,
      maxTokens: 1_000_000,
      lastUsedTokens: 9_100,
    });
    expect(state.costUsd).toBeCloseTo(0.6, 10);
    // Subsequent tool results no longer move the exact figure (Prime reports
    // the last usage until the model answers again).
    expect(applyPrimeSessionEntry(state, toolResultEntry("r2", "a3", "y".repeat(400)))).toBe(
      "none",
    );
    expect(snapshotPrimeSessionUsage(state, 1_000_000)?.usedTokens).toBe(9_100);
  });

  it("falls back to summary plus trailing entries when the kept entry is outside the window", () => {
    const state = createPrimeSessionUsageState();
    fold(state, [
      userEntry("u1", null, "hello"),
      assistantEntry({
        id: "a1",
        parentId: "u1",
        usage: usageBlock({ input: 50_000, output: 10 }),
      }),
    ]);
    applyPrimeSessionEntry(
      state,
      compactionEntry({
        id: "c1",
        parentId: "a1",
        firstKeptEntryId: "not-remembered",
        summary: "s".repeat(80),
      }),
    );
    expect(state.estimate).toEqual({
      summaryTokens: 20,
      retainedTokens: 0,
      trailingTokens: 0,
    });
    applyPrimeSessionEntry(state, userEntry("u2", "c1", "next".repeat(2)));
    expect(snapshotPrimeSessionUsage(state, undefined)?.usedTokens).toBe(22);
  });

  it("uses the last assistant model, then a model_change, for the context window", () => {
    const state = createPrimeSessionUsageState();
    fold(state, [
      {
        type: "model_change",
        id: "m1",
        parentId: null,
        timestamp: "t",
        provider: "anthropic",
        modelId: "claude-fable-5-1",
      },
      userEntry("u1", "m1", "hello"),
    ]);
    expect(resolvePrimeSessionUsageModel(state)).toEqual({
      provider: "anthropic",
      modelId: "claude-fable-5-1",
    });
    fold(state, [
      assistantEntry({ id: "a1", parentId: "u1", usage: usageBlock({ input: 10, output: 1 }) }),
      {
        type: "model_change",
        id: "m2",
        parentId: "a1",
        timestamp: "t",
        provider: "openai",
        modelId: "gpt-5",
      },
    ]);
    // Exact usage was measured by the model that produced it.
    expect(resolvePrimeSessionUsageModel(state)).toEqual({
      provider: "cerebras",
      modelId: "qwen-3.8-27b",
    });
  });
});

describe("describePrimeSessionEntry", () => {
  it("drops message content and compaction summaries from raw payloads", () => {
    expect(
      describePrimeSessionEntry(
        assistantEntry({ id: "a1", parentId: "u1", usage: usageBlock({ input: 1, output: 1 }) }),
      ),
    ).toMatchObject({
      type: "message",
      id: "a1",
      message: { role: "assistant", contentBlocks: 1, usage: { input: 1 } },
    });
    expect(
      describePrimeSessionEntry(assistantEntry({ id: "a1", parentId: "u1" })),
    ).not.toHaveProperty(["message", "content"]);
    const compaction = describePrimeSessionEntry(
      compactionEntry({ id: "c1", parentId: "a1", firstKeptEntryId: "u1", summary: "abcd" }),
    );
    expect(compaction).toMatchObject({
      type: "compaction",
      firstKeptEntryId: "u1",
      summaryChars: 4,
    });
    expect(compaction).not.toHaveProperty("summary");
    expect(compaction).not.toHaveProperty("details");
    expect(describePrimeSessionEntry("raw")).toBe("raw");
  });
});

describe("PrimeSessionLineSplitter", () => {
  const encoder = new TextEncoder();

  it("buffers partial lines and multi-byte characters split across reads", () => {
    const splitter = new PrimeSessionLineSplitter();
    const text = '{"a":"héllo 🚀"}\n{"b":2}\n{"c":';
    const bytes = encoder.encode(text);
    // Split inside the rocket's 4-byte sequence and inside the second line.
    const rocketOffset = encoder.encode('{"a":"héllo ').length + 2;
    const first = splitter.push(bytes.subarray(0, rocketOffset));
    const second = splitter.push(bytes.subarray(rocketOffset, rocketOffset + 12));
    const third = splitter.push(bytes.subarray(rocketOffset + 12));
    expect(first).toEqual([]);
    expect([...second, ...third]).toEqual(['{"a":"héllo 🚀"}', '{"b":2}']);
    expect(splitter.pendingBytes).toBe(encoder.encode('{"c":').length);
    expect(splitter.push(encoder.encode("3}\n"))).toEqual(['{"c":3}']);
    expect(splitter.pendingBytes).toBe(0);
  });

  it("discards an oversize partial line through its newline and resumes", () => {
    const splitter = new PrimeSessionLineSplitter(16);
    expect(splitter.push(encoder.encode("x".repeat(20)))).toEqual([]);
    expect(splitter.pendingBytes).toBe(0);
    expect(splitter.push(encoder.encode("yyyy\nshort\n"))).toEqual(["short"]);
    expect(splitter.push(encoder.encode("more\n"))).toEqual(["more"]);
  });

  it("resets its buffer", () => {
    const splitter = new PrimeSessionLineSplitter();
    splitter.push(encoder.encode("partial"));
    splitter.reset();
    expect(splitter.pendingBytes).toBe(0);
    expect(splitter.push(encoder.encode("line\n"))).toEqual(["line"]);
  });
});
