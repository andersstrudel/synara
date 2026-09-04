/**
 * Prime Agent session-file usage — folds the JSONL entries Prime appends to
 * its session file into a running token-usage state and projects that state
 * onto Synara's `ThreadTokenUsageSnapshot`.
 *
 * Prime's ACP mode never sends `usage_update`, but its session file records
 * exact usage for every LLM call. The context math here mirrors prime-agent's
 * `core/compaction/compaction.js` (`calculateContextTokens`, `estimateTokens`,
 * `estimateContextTokens`) and `core/session-manager.js` (`buildSessionContext`)
 * so the composer dial agrees with Prime's own `/usage`.
 *
 * Everything in this module is pure and incremental: entries are folded one
 * at a time, so a tail never re-parses the whole file after its catch-up read.
 *
 * @module PrimeSessionUsage
 */
import type { ThreadTokenUsageSnapshot } from "@synara/contracts";

import { computeUsagePercent, positiveInteger } from "../tokenUsage.ts";

// Prime's estimateTokens charges an image as 4800 chars (~1200 tokens).
const PRIME_IMAGE_ESTIMATE_CHARS = 4_800;
// Entries remembered for post-compaction retention lookups. Prime keeps about
// `keepRecentTokens` (20k tokens) of recent entries, which is a few dozen
// entries; this bound is generous while keeping the tracker's memory flat on
// very long sessions.
const PRIME_RECENT_ENTRY_LIMIT = 4_096;
// A single session line larger than this (a multi-megabyte tool result) is
// skipped instead of buffered: it can never be an assistant usage entry.
export const PRIME_SESSION_LINE_MAX_BYTES = 16 * 1024 * 1024;

const NEWLINE_BYTE = 0x0a;

export interface PrimeSessionModel {
  readonly provider: string;
  readonly modelId: string;
}

/** One assistant message's usage block, as Prime writes it. */
export interface PrimeSessionUsageTokens {
  readonly input: number;
  readonly output: number;
  readonly cacheRead: number;
  readonly cacheWrite: number;
  /** Prime's native context total when the provider reports one. */
  readonly totalTokens: number | undefined;
  readonly costUsd: number | undefined;
}

export interface PrimeSessionAssistantUsage {
  readonly usage: PrimeSessionUsageTokens;
  readonly model: PrimeSessionModel | undefined;
  readonly stopReason: string | undefined;
}

/**
 * Context estimate in force after a `compaction` entry until the next valid
 * assistant usage. Mirrors the no-usage branch of Prime's
 * `estimateContextTokens`: summary + retained entries + everything appended
 * since, all at chars/4.
 */
export interface PrimeSessionUsageEstimate {
  readonly summaryTokens: number;
  readonly retainedTokens: number;
  /** False when `firstKeptEntryId` fell outside the remembered entry window. */
  readonly retainedKnown: boolean;
  trailingTokens: number;
}

export interface PrimeSessionRecentEntry {
  readonly id: string;
  readonly tokens: number;
}

/**
 * Mutable running state. Cumulative counters cover every assistant message
 * with a usage block (the way Prime's `getSessionStats` sums them); context
 * fields follow Prime's compaction rules (aborted/error responses skipped).
 */
export interface PrimeSessionUsageState {
  entries: number;
  assistantMessages: number;
  inputTokens: number;
  cachedInputTokens: number;
  cacheWriteTokens: number;
  outputTokens: number;
  totalProcessedTokens: number;
  /** Undefined until an assistant usage carried a cost block. */
  costUsd: number | undefined;
  toolUses: number;
  /** Last assistant usage that counts as context (stopReason not aborted/error). */
  lastContextUsage: PrimeSessionAssistantUsage | undefined;
  /** Last assistant usage of any stopReason (the meter's "last call" line). */
  lastAssistantUsage: PrimeSessionAssistantUsage | undefined;
  /** Model in force on the branch: last `model_change` or assistant message. */
  currentModel: PrimeSessionModel | undefined;
  estimate: PrimeSessionUsageEstimate | undefined;
  /** Bounded window of entry ids and their chars/4 estimate, oldest first. */
  readonly recentEntries: Array<PrimeSessionRecentEntry>;
}

/**
 * What folding an entry changed: `usage` for an assistant usage block (exact
 * context and/or totals), `estimate` for a post-compaction estimate change.
 */
export type PrimeSessionUsageChange = "none" | "usage" | "estimate";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function textLength(value: unknown): number {
  return typeof value === "string" ? value.length : 0;
}

function nonNegativeFinite(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : 0;
}

function toNonNegativeInt(value: number): number {
  return Math.max(0, Math.round(value));
}

function charsToTokens(chars: number): number {
  return Math.ceil(chars / 4);
}

function jsonLength(value: unknown): number {
  try {
    const serialized = JSON.stringify(value);
    return typeof serialized === "string" ? serialized.length : 0;
  } catch {
    return 0;
  }
}

export function createPrimeSessionUsageState(): PrimeSessionUsageState {
  return {
    entries: 0,
    assistantMessages: 0,
    inputTokens: 0,
    cachedInputTokens: 0,
    cacheWriteTokens: 0,
    outputTokens: 0,
    totalProcessedTokens: 0,
    costUsd: undefined,
    toolUses: 0,
    lastContextUsage: undefined,
    lastAssistantUsage: undefined,
    currentModel: undefined,
    estimate: undefined,
    recentEntries: [],
  };
}

/** Prime's `calculateContextTokens`: native total, else the component sum. */
export function calculatePrimeContextTokens(usage: PrimeSessionUsageTokens): number {
  return usage.totalTokens || usage.input + usage.output + usage.cacheRead + usage.cacheWrite;
}

function estimateContentChars(content: unknown, imageChars: number): number {
  if (typeof content === "string") {
    return content.length;
  }
  if (!Array.isArray(content)) {
    return 0;
  }
  let chars = 0;
  for (const block of content) {
    if (!isRecord(block)) continue;
    if (block.type === "text") {
      chars += textLength(block.text);
    } else if (block.type === "image") {
      chars += imageChars;
    }
  }
  return chars;
}

/**
 * Prime's `estimateTokens` (chars/4 by role). Shapes are read defensively:
 * a malformed block counts as zero instead of throwing the way Prime would.
 */
export function estimatePrimeMessageTokens(message: unknown): number {
  if (!isRecord(message)) {
    return 0;
  }
  switch (message.role) {
    case "user":
      return charsToTokens(estimateContentChars(message.content, 0));
    case "assistant": {
      let chars = 0;
      if (Array.isArray(message.content)) {
        for (const block of message.content) {
          if (!isRecord(block)) continue;
          if (block.type === "text") {
            chars += textLength(block.text);
          } else if (block.type === "thinking") {
            chars += textLength(block.thinking);
          } else if (block.type === "toolCall") {
            chars += textLength(block.name) + jsonLength(block.arguments);
          }
        }
      }
      return charsToTokens(chars);
    }
    case "custom":
    case "toolResult":
      return charsToTokens(estimateContentChars(message.content, PRIME_IMAGE_ESTIMATE_CHARS));
    case "bashExecution":
      return charsToTokens(textLength(message.command) + textLength(message.output));
    case "branchSummary":
    case "compactionSummary":
      return charsToTokens(textLength(message.summary));
    default:
      return 0;
  }
}

/**
 * Tokens an entry contributes to the model context (Prime's
 * `getMessageFromEntry`: message, custom_message, branch_summary). Compaction
 * summaries are handled by the estimate itself; other entry types are
 * bookkeeping that never reaches the model.
 */
export function estimatePrimeEntryTokens(entry: Record<string, unknown>): number {
  switch (entry.type) {
    case "message":
      return estimatePrimeMessageTokens(entry.message);
    case "custom_message":
      return estimatePrimeMessageTokens({ role: "custom", content: entry.content });
    case "branch_summary":
      return typeof entry.summary === "string" && entry.summary.length > 0
        ? charsToTokens(entry.summary.length)
        : 0;
    default:
      return 0;
  }
}

function contributesToContext(entry: Record<string, unknown>): boolean {
  return (
    entry.type === "message" ||
    entry.type === "custom_message" ||
    (entry.type === "branch_summary" &&
      typeof entry.summary === "string" &&
      entry.summary.length > 0)
  );
}

export function parsePrimeSessionUsageTokens(value: unknown): PrimeSessionUsageTokens | undefined {
  if (!isRecord(value)) {
    return undefined;
  }
  const totalTokens =
    typeof value.totalTokens === "number" &&
    Number.isFinite(value.totalTokens) &&
    value.totalTokens > 0
      ? value.totalTokens
      : undefined;
  const cost = isRecord(value.cost) ? value.cost : undefined;
  const costUsd =
    cost !== undefined &&
    typeof cost.total === "number" &&
    Number.isFinite(cost.total) &&
    cost.total >= 0
      ? cost.total
      : undefined;
  return {
    input: nonNegativeFinite(value.input),
    output: nonNegativeFinite(value.output),
    cacheRead: nonNegativeFinite(value.cacheRead),
    cacheWrite: nonNegativeFinite(value.cacheWrite),
    totalTokens,
    costUsd,
  };
}

function parsePrimeSessionModel(
  provider: unknown,
  modelId: unknown,
): PrimeSessionModel | undefined {
  const providerId = typeof provider === "string" ? provider.trim() : "";
  const id = typeof modelId === "string" ? modelId.trim() : "";
  return providerId && id ? { provider: providerId, modelId: id } : undefined;
}

function foldAssistantMessage(
  state: PrimeSessionUsageState,
  message: Record<string, unknown>,
): PrimeSessionUsageChange {
  const model = parsePrimeSessionModel(message.provider, message.model);
  if (model !== undefined) {
    state.currentModel = model;
  }
  if (Array.isArray(message.content)) {
    for (const block of message.content) {
      if (isRecord(block) && block.type === "toolCall") {
        state.toolUses += 1;
      }
    }
  }
  const usage = parsePrimeSessionUsageTokens(message.usage);
  if (usage === undefined) {
    return "none";
  }
  state.assistantMessages += 1;
  state.inputTokens += usage.input;
  state.cachedInputTokens += usage.cacheRead;
  state.cacheWriteTokens += usage.cacheWrite;
  state.outputTokens += usage.output;
  state.totalProcessedTokens += usage.input + usage.output + usage.cacheRead + usage.cacheWrite;
  if (usage.costUsd !== undefined) {
    state.costUsd = (state.costUsd ?? 0) + usage.costUsd;
  }
  const stopReason = typeof message.stopReason === "string" ? message.stopReason : undefined;
  const record: PrimeSessionAssistantUsage = { usage, model, stopReason };
  state.lastAssistantUsage = record;
  // Prime's getAssistantUsage skips aborted/error responses: their usage does
  // not describe the context the next call will send.
  if (stopReason !== "aborted" && stopReason !== "error") {
    state.lastContextUsage = record;
    state.estimate = undefined;
  }
  return "usage";
}

function startEstimate(state: PrimeSessionUsageState, entry: Record<string, unknown>): void {
  const summary = typeof entry.summary === "string" ? entry.summary : "";
  const firstKeptEntryId =
    typeof entry.firstKeptEntryId === "string" ? entry.firstKeptEntryId : undefined;
  let retainedTokens = 0;
  let retainedKnown = firstKeptEntryId === undefined;
  if (firstKeptEntryId !== undefined) {
    // Prime's buildSessionContext keeps every message-producing entry from
    // firstKeptEntryId up to the compaction, in order, after the summary.
    const entries = state.recentEntries;
    let index = entries.length - 1;
    while (index >= 0 && entries[index]?.id !== firstKeptEntryId) {
      index -= 1;
    }
    if (index >= 0) {
      retainedKnown = true;
      for (let cursor = index; cursor < entries.length; cursor += 1) {
        retainedTokens += entries[cursor]?.tokens ?? 0;
      }
    }
  }
  state.estimate = {
    summaryTokens: charsToTokens(summary.length),
    retainedTokens,
    retainedKnown,
    trailingTokens: 0,
  };
}

function rememberEntry(state: PrimeSessionUsageState, id: unknown, tokens: number): void {
  if (typeof id !== "string" || id.length === 0) {
    return;
  }
  state.recentEntries.push({ id, tokens });
  if (state.recentEntries.length > PRIME_RECENT_ENTRY_LIMIT) {
    state.recentEntries.splice(0, state.recentEntries.length - PRIME_RECENT_ENTRY_LIMIT);
  }
}

/**
 * Folds one parsed session entry. `session` headers, `child_usage_attributed`
 * (sub-agent usage is attributed to the parent's cost, never its context) and
 * daemon bookkeeping entries are ignored.
 */
export function applyPrimeSessionEntry(
  state: PrimeSessionUsageState,
  entry: unknown,
): PrimeSessionUsageChange {
  if (!isRecord(entry) || typeof entry.type !== "string") {
    return "none";
  }
  state.entries += 1;
  const tokens = estimatePrimeEntryTokens(entry);
  let change: PrimeSessionUsageChange = "none";
  let contextUsageReplaced = false;
  switch (entry.type) {
    case "message": {
      if (isRecord(entry.message) && entry.message.role === "assistant") {
        const hadEstimate = state.estimate !== undefined;
        change = foldAssistantMessage(state, entry.message);
        contextUsageReplaced = hadEstimate && state.estimate === undefined;
      }
      break;
    }
    case "model_change": {
      const model = parsePrimeSessionModel(entry.provider, entry.modelId);
      if (model !== undefined) {
        state.currentModel = model;
        if (state.estimate !== undefined) {
          change = "estimate";
        }
      }
      break;
    }
    case "compaction": {
      startEstimate(state, entry);
      change = "estimate";
      break;
    }
    default:
      break;
  }
  // Until an assistant answers after the compaction, every entry that reaches
  // the model extends the estimate the way estimateContextTokens' no-usage
  // branch would; an aborted assistant message is context too.
  if (state.estimate !== undefined && !contextUsageReplaced && contributesToContext(entry)) {
    state.estimate.trailingTokens += tokens;
    if (change === "none") {
      change = "estimate";
    }
  }
  rememberEntry(state, entry.id, tokens);
  return change;
}

/** Parses one JSONL line; blank and malformed lines yield undefined. */
export function parsePrimeSessionLine(line: string): unknown {
  const trimmed = line.trim();
  if (trimmed.length === 0) {
    return undefined;
  }
  try {
    const parsed: unknown = JSON.parse(trimmed);
    return isRecord(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
}

export function applyPrimeSessionLine(
  state: PrimeSessionUsageState,
  line: string,
): PrimeSessionUsageChange {
  const entry = parsePrimeSessionLine(line);
  return entry === undefined ? "none" : applyPrimeSessionEntry(state, entry);
}

export function primeSessionEstimateTokens(estimate: PrimeSessionUsageEstimate): number {
  return estimate.summaryTokens + estimate.retainedTokens + estimate.trailingTokens;
}

/**
 * Model whose context window applies to the reported context: the model that
 * produced the exact usage, or (while estimating after a compaction) the model
 * the next call will use.
 */
export function resolvePrimeSessionUsageModel(
  state: PrimeSessionUsageState,
): PrimeSessionModel | undefined {
  if (state.estimate !== undefined) {
    return state.currentModel ?? state.lastContextUsage?.model;
  }
  return state.lastContextUsage?.model ?? state.currentModel;
}

/**
 * Projects the state onto the runtime snapshot. Undefined until the session
 * has something to report (no assistant usage and no compaction yet).
 */
export function snapshotPrimeSessionUsage(
  state: PrimeSessionUsageState,
  contextWindow: number | undefined,
): ThreadTokenUsageSnapshot | undefined {
  const usedTokens =
    state.estimate !== undefined
      ? primeSessionEstimateTokens(state.estimate)
      : state.lastContextUsage !== undefined
        ? calculatePrimeContextTokens(state.lastContextUsage.usage)
        : undefined;
  if (usedTokens === undefined) {
    return undefined;
  }
  const maxTokens = positiveInteger(contextWindow);
  const used = toNonNegativeInt(usedTokens);
  const usedPercent = computeUsagePercent(used, maxTokens);
  const last = state.lastAssistantUsage;
  return {
    usedTokens: used,
    ...(usedPercent !== undefined ? { usedPercent } : {}),
    totalProcessedTokens: toNonNegativeInt(state.totalProcessedTokens),
    ...(maxTokens !== undefined ? { maxTokens } : {}),
    inputTokens: toNonNegativeInt(state.inputTokens),
    cachedInputTokens: toNonNegativeInt(state.cachedInputTokens),
    outputTokens: toNonNegativeInt(state.outputTokens),
    ...(last !== undefined
      ? {
          lastUsedTokens: toNonNegativeInt(calculatePrimeContextTokens(last.usage)),
          lastInputTokens: toNonNegativeInt(last.usage.input),
          lastCachedInputTokens: toNonNegativeInt(last.usage.cacheRead),
          lastOutputTokens: toNonNegativeInt(last.usage.output),
        }
      : {}),
    toolUses: toNonNegativeInt(state.toolUses),
    compactsAutomatically: true,
  };
}

/**
 * Bounded projection of an entry for event raw payloads: message content and
 * compaction summaries can run to megabytes and never inform the snapshot.
 */
export interface PrimeCompactionEntry {
  readonly tokensBefore: number | undefined;
}

/** The `compaction` entry Prime appends when it folds older context into a summary. */
export function readPrimeCompactionEntry(entry: unknown): PrimeCompactionEntry | undefined {
  if (!isRecord(entry) || entry.type !== "compaction") {
    return undefined;
  }
  const tokensBefore = entry.tokensBefore;
  return {
    tokensBefore:
      typeof tokensBefore === "number" && Number.isFinite(tokensBefore) && tokensBefore >= 0
        ? Math.round(tokensBefore)
        : undefined,
  };
}

export function describePrimeSessionEntry(entry: unknown): unknown {
  if (!isRecord(entry)) {
    return entry;
  }
  if (entry.type === "message" && isRecord(entry.message)) {
    const { content, ...message } = entry.message;
    return {
      ...entry,
      message: {
        ...message,
        contentBlocks: Array.isArray(content)
          ? content.length
          : typeof content === "string"
            ? 1
            : 0,
      },
    };
  }
  if (entry.type === "compaction") {
    const { summary, details: _details, ...rest } = entry;
    return { ...rest, summaryChars: textLength(summary) };
  }
  if (entry.type === "custom_message" || entry.type === "branch_summary") {
    const { content: _content, summary: _summary, details: _entryDetails, ...rest } = entry;
    return rest;
  }
  return entry;
}

/**
 * Splits appended bytes into complete lines. `\n` is a single byte in UTF-8,
 * so scanning bytes is safe even when a multi-byte character straddles two
 * reads; the trailing partial line stays buffered until its newline arrives.
 * A partial line over `maxLineBytes` is discarded through its newline.
 */
export class PrimeSessionLineSplitter {
  private pending: Uint8Array = new Uint8Array(0);
  private discarding = false;
  private readonly decoder = new TextDecoder();

  constructor(private readonly maxLineBytes: number = PRIME_SESSION_LINE_MAX_BYTES) {}

  get pendingBytes(): number {
    return this.pending.length;
  }

  reset(): void {
    this.pending = new Uint8Array(0);
    this.discarding = false;
  }

  push(chunk: Uint8Array): Array<string> {
    const lines: Array<string> = [];
    let buffer: Uint8Array;
    if (this.pending.length === 0) {
      buffer = chunk;
    } else {
      buffer = new Uint8Array(this.pending.length + chunk.length);
      buffer.set(this.pending, 0);
      buffer.set(chunk, this.pending.length);
    }
    let start = 0;
    while (start < buffer.length) {
      const newline = buffer.indexOf(NEWLINE_BYTE, start);
      if (newline < 0) {
        break;
      }
      if (this.discarding) {
        this.discarding = false;
      } else {
        lines.push(this.decoder.decode(buffer.subarray(start, newline)));
      }
      start = newline + 1;
    }
    const remainder = buffer.subarray(start);
    if (this.discarding || remainder.length > this.maxLineBytes) {
      this.discarding = true;
      this.pending = new Uint8Array(0);
    } else {
      this.pending = remainder.length > 0 ? remainder.slice() : new Uint8Array(0);
    }
    return lines;
  }
}
