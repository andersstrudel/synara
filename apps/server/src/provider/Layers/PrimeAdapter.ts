/**
 * PrimeAdapterLive — Prime Agent (`prime-agent --mode acp`) via ACP.
 *
 * A thin adapter around `AcpSessionRuntime` that reuses the shared ACP
 * lifecycle and event-stream plumbing. Prime owns its credentials and session
 * persistence: model selection rides the process-start `--model` flag, and a
 * thread resumes by relaunching the CLI with `--resume <session id>` followed
 * by a fresh `session/new` (Prime does not implement `session/load`).
 *
 * Context-window usage is tailed from Prime's session file (see
 * `PrimeSessionUsageTail`): Prime's ACP mode never sends `usage_update`, but
 * the file records exact usage for every LLM call.
 *
 * Fast mode is Prime's `priority` service tier. ACP mode cannot set it per
 * request, so the adapter seeds it through the session file the way Prime's
 * own `/fast` would (see `PrimeSessionServiceTier`) and relaunches with
 * `--resume`; a fresh start that wants the tier restarts once into the resume
 * path as soon as its session file is known.
 *
 * @module PrimeAdapterLive
 */
import {
  type ChatAttachment,
  EventId,
  type PrimeModelOptions,
  type ProviderComposerCapabilities,
  type ProviderInteractionMode,
  ProviderListCommandsInput,
  type ProviderListCommandsResult,
  type ProviderListModelsResult,
  type ProviderListSkillsInput,
  type ProviderListSkillsResult,
  type ProviderRuntimeEvent,
  type ProviderSession,
  RuntimeItemId,
  type RuntimeMode,
  ThreadId,
  TurnId,
} from "@synara/contracts";
import {
  Cause,
  DateTime,
  Deferred,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Layer,
  Option,
  PubSub,
  Random,
  Semaphore,
  Scope,
  Stream,
} from "effect";
import { ChildProcessSpawner } from "effect/unstable/process";
import type * as Acp from "@agentclientprotocol/sdk";

import { buildAcpSynaraMcpServers } from "../../agentGateway/mcpInjection.ts";
import {
  type SynaraHarnessPolicyDeliveryState,
  takeSynaraHarnessPolicyTextPartForProviderSession,
} from "../../agentGateway/harnessPolicy.ts";
import { AgentGatewayCredentials } from "../../agentGateway/Services/AgentGatewayCredentials.ts";
import {
  acquireAgentGatewaySessionLease,
  cancelAgentGatewayTurn,
  startAgentGatewaySessionLeaseExitWatcher,
  type AgentGatewaySessionLease,
  withAgentGatewayTurnCancellation,
} from "../../agentGateway/sessionLease.ts";
import { ServerConfig } from "../../config.ts";
import { appendFileAttachmentsPromptBlock } from "../attachmentProjection.ts";
import { loadProviderPromptImageBlocks } from "../promptAttachments.ts";
import {
  ProviderAdapterError,
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import {
  classifyAcpPromptTurnCompletion,
  mapAcpToAdapterError,
  readAcpFailedToolDetail,
} from "../acp/AcpAdapterSupport.ts";
import {
  acceptAcpPlanUpdate,
  clearAcpActiveTurn,
  finalizeAcpActiveTurnCost,
  forkAcpAdapterTurnIdleWatchdog,
  makeAcpThreadLock,
  recordAcpSessionCost,
  resolveAcpSessionCwd,
  resolveAcpTurnInteractionMode,
  scopeAcpRuntimeItemIdForTurn,
  scopeAcpToolCallStateForTurn,
  waitForAcpQueuedTurnEventsDrained,
  withAcpPlanModePrompt,
} from "../acp/AcpAdapterSessionSupport.ts";
import {
  makeAcpAssistantItemEvent,
  makeAcpContentDeltaEvent,
  makeAcpPlanUpdatedEvent,
  makeAcpTokenUsageEvent,
  makeAcpToolCallEvent,
  stampAcpRuntimeEventLifecycleGeneration,
} from "../acp/AcpCoreRuntimeEvents.ts";
import { type AcpPlanUpdate, type AcpToolCallState } from "../acp/AcpRuntimeModel.ts";
import {
  redactAcpLogSecrets,
  makeAcpDebugLoggers,
  makeAcpNativeLoggers,
} from "../acp/AcpNativeLogging.ts";
import {
  isAcpTurnProgressEventTag,
  resolveAcpTurnIdleTimeoutMs,
} from "../acp/AcpTurnIdleWatchdog.ts";
import {
  buildPrimeModelFlag,
  canonicalPrimeSessionCwd,
  detectNewPrimeSession,
  makePrimeAcpRuntime,
  mapPrimeCommands,
  mapPrimeSkills,
  type PrimeCommandDescriptor,
  parsePrimeCommands,
  parsePrimeModelRegistry,
  parsePrimeRegistryModels,
  PRIME_LOGIN_GUIDANCE,
  PRIME_RPC_COMMANDS_REQUEST_ID,
  PRIME_RPC_MODELS_REQUEST_ID,
  PRIME_RPC_STATE_REQUEST_ID,
  PRIME_SERVICE_TIER_DEFAULT,
  type PrimeAcpRuntimeSettings,
  type PrimeDetectedSession,
  primeModelSlug,
  primeModelSupportsFastMode,
  type PrimeRegistryModel,
  type PrimeRpcDiscoveryResult,
  type PrimeServiceTier,
  primeServiceTierFor,
  type PrimeSessionFileSnapshot,
  readPrimeUserSettings,
  resolvePrimeAgentDir,
  resolvePrimeBinaryPath,
  resolvePrimeSessionFile,
  resolvePrimeSessionsDir,
  runPrimeAcpCompactionCommand,
  runPrimeRpcDiscovery,
  snapshotPrimeSessionFiles,
} from "../acp/PrimeAcpSupport.ts";
import {
  type AcpSessionRuntimeShape,
  type AcpSessionRuntimeStartResult,
} from "../acp/AcpSessionRuntime.ts";
import {
  appendPrimeServiceTierChange,
  readPrimeSessionServiceTier,
} from "../acp/PrimeSessionServiceTier.ts";
import {
  describePrimeSessionEntry,
  type PrimeSessionModel,
  readPrimeCompactionEntry,
  resolvePrimeSessionUsageModel,
  snapshotPrimeSessionUsage,
} from "../acp/PrimeSessionUsage.ts";
import {
  makePrimeSessionUsageTail,
  type PrimeSessionUsageUpdate,
} from "../acp/PrimeSessionUsageTail.ts";
import { makeEventNdjsonLogger, type EventNdjsonLogger } from "./EventNdjsonLogger.ts";
import {
  PROVIDER_ADAPTER_RUNTIME_EVENT_BUFFER_CAPACITY,
  type ProviderThreadSnapshot,
  type ProviderThreadTurnSnapshot,
} from "../Services/ProviderAdapter.ts";
import { PrimeAdapter, type PrimeAdapterShape } from "../Services/PrimeAdapter.ts";

const PROVIDER = "prime" as const;
const PRIME_RESUME_VERSION = 1 as const;

const PRIME_MODEL_DISCOVERY_TIMEOUT_MS = 15_000;
const PRIME_MODEL_DISCOVERY_CACHE_MS = 5 * 60_000;
const PRIME_COMMAND_DISCOVERY_TIMEOUT_MS = 15_000;
const PRIME_COMMAND_DISCOVERY_CACHE_MS = 5 * 60_000;
const PRIME_DISCOVERY_CACHE_MAX_ENTRIES = 16;
const PRIME_ACP_TRANSPORT_DEBUG_MARKER = "prime-acp-v1";
const PRIME_ACP_LOG_PAYLOAD_LIMIT = 4_000;
const PRIME_ACP_DEBUG_ENV = "SYNARA_PRIME_ACP_DEBUG";
// Prime writes its session file at process start, so the header normally
// exists before session/new returns; this bounds the post-start poll.
const PRIME_SESSION_DETECT_TIMEOUT_MS = 4_000;
// Shorter re-check when the first detection missed (e.g. a slow disk); the
// file has had a whole turn to appear by then.
const PRIME_SESSION_DETECT_RETRY_TIMEOUT_MS = 1_000;
// Prime's session worker keeps its lease for a few seconds after the CLI
// exits; a restart-with-resume that lands inside that window fails with
// "Session worker is stopping". Retry a resumed start a bounded number of
// times before surfacing the failure.
const PRIME_RESUME_START_ATTEMPTS = 3;
const PRIME_RESUME_START_RETRY_DELAY_MS = 1_500;
// Bounded wait for queued session updates to be consumed before a turn
// settles, so late tool/content state is folded into its completion.
const PRIME_TURN_SETTLE_DRAIN_MAX_WAIT_MS = 1_000;
const PRIME_TURN_SETTLE_DRAIN_POLL_MS = 25;
// After a timed-out /compact the cancel is only best-effort: the child may
// still stream stale compaction updates for a moment. Hold new turns for this
// long so those events cannot be attributed to the next active turn.
const PRIME_COMPACT_ABANDON_QUIET_MS = 5_000;
// Row title for compactions Prime performs on its own mid-turn (manual /compact
// completes through thread.state.changed instead).
const PRIME_COMPACTION_COMPLETED_TITLE = "Context compacted";
// Bounded wait for the forked post-timeout cancel to be written before the
// next prompt is dispatched (stdio delivers in order).
const PRIME_COMPACT_CANCEL_WAIT_MS = 10_000;
// The compaction outcome (failed tool detail) is recorded by the notification
// consumer, which can lag the /compact response; wait for inbound activity to
// go quiet (bounded) before deciding success.
const PRIME_COMPACT_OUTCOME_QUIET_MS = 200;
const PRIME_COMPACT_OUTCOME_MAX_WAIT_MS = 2_000;
// Prime's ACP mode never sends usage_update; context usage is tailed from the
// session file instead. The tail wakes on a file watch and on this poll (fast
// while a turn or compaction is in flight, slow otherwise). On macOS the
// watch fires first and the poll only covers coalesced events; on Linux the
// inotify watch is lost when Prime rewrites the file through a rename (its
// first persisted assistant message), so the poll is what drives it from then on.
const PRIME_SESSION_USAGE_ACTIVE_POLL_MS = 250;
const PRIME_SESSION_USAGE_IDLE_POLL_MS = 2_000;
// Prime writes its compaction entry before the /compact prompt answers, so
// the tail sees the reduced context while compactingThread is still set. The
// meter treats a completed compaction as invalidating whatever snapshot came
// before it, so usage observed during that window is held until the terminal
// compaction event is out, bounded by this delay.
const PRIME_SESSION_USAGE_COMPACTION_HOLD_MS = 1_000;
// A failed registry probe (a usage line's context window, fast-mode
// eligibility at start) is not retried on every lookup; the dial keeps
// working without maxTokens meanwhile.
const PRIME_REGISTRY_DISCOVERY_RETRY_MS = 60_000;
const PRIME_SESSION_FILE_USAGE_SOURCE = "prime.session-file.entry";
const PRIME_SESSION_FILE_USAGE_METHOD = "session/file";

const PRIME_PLAN_MODE_PROMPT_PREFIX = [
  "Prime Agent plan mode is active.",
  "Do not implement or mutate files in this turn.",
  "Do not ask follow-up questions or wait for confirmation; if scope is ambiguous, choose a reasonable default and state the assumption in the plan.",
  "When ready, create the final implementation plan.",
].join("\n");

export interface PrimeAdapterTimeouts {
  readonly turnIdleMs: number;
  readonly toolIdleMs: number;
  readonly sessionDetectMs: number;
  readonly resumeStartRetryDelayMs: number;
}

export function resolvePrimeAdapterTimeouts(
  env: NodeJS.ProcessEnv = process.env,
): PrimeAdapterTimeouts {
  return {
    turnIdleMs: resolveAcpTurnIdleTimeoutMs({
      envVar: "SYNARA_PRIME_TURN_IDLE_TIMEOUT_MS",
      defaultMs: 30 * 60 * 1000,
      env,
    }),
    toolIdleMs: resolveAcpTurnIdleTimeoutMs({
      envVar: "SYNARA_PRIME_TOOL_IDLE_TIMEOUT_MS",
      defaultMs: 60 * 60 * 1000,
      env,
    }),
    sessionDetectMs: PRIME_SESSION_DETECT_TIMEOUT_MS,
    resumeStartRetryDelayMs: PRIME_RESUME_START_RETRY_DELAY_MS,
  };
}

interface PrimeAdapterLiveOptions {
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
  readonly makeAcpRuntime?: typeof makePrimeAcpRuntime;
  readonly runRpcDiscovery?: typeof runPrimeRpcDiscovery;
  readonly onSessionUpdateProcessed?: () => void;
  readonly timeouts?: PrimeAdapterTimeouts;
}

interface PrimeSessionDetection {
  readonly snapshot: PrimeSessionFileSnapshot;
  readonly spawnedAt: number;
  /** Key of the sessions-dir + cwd lock the detection runs under (see primeSessionLocks). */
  readonly lockKey: string;
}

/** A sessions-dir + cwd lock and how many starts or retries currently reference it. */
interface PrimeSessionLockEntry {
  readonly semaphore: Semaphore.Semaphore;
  users: number;
}

interface PrimeStartedRuntime {
  readonly acp: AcpSessionRuntimeShape;
  readonly started: AcpSessionRuntimeStartResult;
}

interface PrimeSessionContext extends SynaraHarnessPolicyDeliveryState {
  readonly threadId: ThreadId;
  readonly lifecycleGeneration: string | undefined;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  readonly acp: AcpSessionRuntimeShape;
  notificationFiber: Fiber.Fiber<void, never> | undefined;
  turns: Array<ProviderThreadTurnSnapshot>;
  activeInteractionMode: ProviderInteractionMode | undefined;
  activeTurnId: TurnId | undefined;
  activeTurnHadAssistantContent: boolean;
  readonly activeAssistantItemsWithContent: Set<string>;
  activeTurnFailedToolDetail: string | undefined;
  activePromptFiber: Fiber.Fiber<void, never> | undefined;
  // True once ctx.acp.prompt has returned for the current turn (success or
  // failure). An interrupt that lands while the post-prompt drain is still
  // running must not reclassify the turn as cancelled.
  activePromptResolved: boolean;
  lastPlanFingerprint: string | undefined;
  lastTurnActivityAt: number | undefined;
  readonly primeToolCallLifecycleById: Map<string, "active" | "terminal">;
  // Compared against acp.sessionUpdatesEnqueuedCount to detect when queued
  // session updates have been fully handled by the notification consumer.
  sessionUpdatesProcessed: number;
  // Pending until startSession has completed its post-registration setup
  // (session-file detection). Resolved by stopSessionInternal too, so a
  // failed startup never strands waiters.
  sessionConfigReady: Deferred.Deferred<void> | undefined;
  // Set when a fresh start could not locate Prime's session file yet; the
  // first completed turn retries detection so the resume cursor is published.
  sessionDetection: PrimeSessionDetection | undefined;
  // Prime session header id — the durable identity behind the resume cursor.
  primeSessionId: string | undefined;
  // True while sendTurn is between its compaction check and settling the turn;
  // compactThread reads it so a compaction prompt cannot slip into the gap
  // before ctx.activeTurnId is assigned.
  turnStarting: boolean;
  // Set by interruptTurn while a turn is still starting (no prompt fiber to
  // interrupt yet); startPrimeTurn re-checks it before dispatching.
  pendingTurnInterrupted: boolean;
  compactingThread: boolean;
  compactionFailedToolDetail: string | undefined;
  // Epoch-ms until which an abandoned (timed-out) /compact may still stream
  // stale updates; new turns wait it out so they cannot pollute the next turn.
  compactionQuietUntil: number | undefined;
  compactionCancelFiber: Fiber.Fiber<void> | undefined;
  latestSessionCostUsd: number | undefined;
  // Registry probe target (binary + agent dir) for context-window lookups.
  readonly discoveryTarget: PrimeDiscoveryTarget;
  // Usage tail bound to Prime's session file once it has been located (fresh
  // detection or resume). The tail fiber lives in `scope`.
  sessionUsageTail: PrimeSessionUsageTailHandle | undefined;
  // Usage emission deferred while a manual compaction is in flight (see
  // PRIME_SESSION_USAGE_COMPACTION_HOLD_MS); the hold fiber flushes it if the
  // compaction outcome takes longer than the bound.
  pendingSessionUsageEmit: Effect.Effect<void> | undefined;
  pendingSessionUsageHoldFiber: Fiber.Fiber<void> | undefined;
  stopped: boolean;
  gatewaySessionLease: AgentGatewaySessionLease | undefined;
}

interface PrimeSessionUsageTailHandle {
  readonly file: string;
  readonly flush: Effect.Effect<void>;
  readonly fiber: Fiber.Fiber<void>;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readPrimeProviderStartOptions(
  providerOptions: unknown,
): { readonly binaryPath?: string; readonly agentDir?: string } | undefined {
  if (!isRecord(providerOptions) || !isRecord(providerOptions.prime)) {
    return undefined;
  }
  const binaryPath = providerOptions.prime.binaryPath;
  const agentDir = providerOptions.prime.agentDir;
  return {
    ...(typeof binaryPath === "string" ? { binaryPath } : {}),
    ...(typeof agentDir === "string" ? { agentDir } : {}),
  };
}

export function parsePrimeResume(
  resumeCursor: unknown,
): { readonly sessionId: string } | undefined {
  if (!isRecord(resumeCursor)) {
    return undefined;
  }
  const schemaVersion = resumeCursor.schemaVersion;
  const sessionId = resumeCursor.sessionId;
  if (
    schemaVersion !== PRIME_RESUME_VERSION ||
    typeof sessionId !== "string" ||
    !sessionId.trim()
  ) {
    return undefined;
  }
  return { sessionId: sessionId.trim() };
}

export function buildPrimeResumeCursor(sessionId: string): {
  readonly schemaVersion: typeof PRIME_RESUME_VERSION;
  readonly sessionId: string;
} {
  return { schemaVersion: PRIME_RESUME_VERSION, sessionId };
}

/**
 * Prime never requests permissions over ACP and exposes no session modes, so
 * an approval-gated runtime mode cannot be honored. Fail closed instead of
 * silently running with full access.
 */
export function validatePrimeRuntimeMode(
  runtimeMode: RuntimeMode,
): Effect.Effect<void, ProviderAdapterValidationError> {
  if (runtimeMode !== "approval-required") {
    return Effect.void;
  }
  return Effect.fail(
    new ProviderAdapterValidationError({
      provider: PROVIDER,
      operation: "startSession",
      issue:
        'Prime Agent cannot gate tool execution behind approvals (it never requests permissions over ACP). Use the "auto" or "full-access" runtime mode.',
    }),
  );
}

/** `provider/id[:thinkingLevel]` for `--model`; undefined leaves Prime's own default. */
export function resolvePrimeStartModel(
  modelSelection:
    | { readonly model: string; readonly options?: PrimeModelOptions | undefined }
    | undefined,
): string | undefined {
  return buildPrimeModelFlag(modelSelection?.model, modelSelection?.options?.thinkingLevel);
}

export function scopePrimeRuntimeItemIdForTurn(turnId: TurnId, itemId: string): string {
  return scopeAcpRuntimeItemIdForTurn(PROVIDER, turnId, itemId);
}

// Prime streams thoughts as reasoning_text; only visible text opens a message.
export function isRenderablePrimeAssistantDelta(input: {
  readonly streamKind?: string | undefined;
  readonly text: string;
}): boolean {
  return input.streamKind !== "reasoning_text" && input.text.trim().length > 0;
}

export function scopePrimeToolCallStateForTurn(
  turnId: TurnId,
  toolCall: AcpToolCallState,
): AcpToolCallState {
  return scopeAcpToolCallStateForTurn(PROVIDER, turnId, toolCall);
}

/** Key of the lock a fresh start holds: Prime's sessions dir plus the canonical session cwd. */
function primeSessionLockKey(sessionsDir: string, cwdKey: string): string {
  return `${sessionsDir}\0${cwdKey}`;
}

function setPrimeDiscoveryCacheEntry<Entry extends { readonly expiresAt: number }>(
  cache: Map<string, Entry>,
  key: string,
  value: Entry,
): void {
  cache.delete(key);
  cache.set(key, value);
  while (cache.size > PRIME_DISCOVERY_CACHE_MAX_ENTRIES) {
    const oldestKey = cache.keys().next().value;
    if (oldestKey === undefined) {
      break;
    }
    cache.delete(oldestKey);
  }
}

export interface PrimeDiscoveryTarget {
  readonly binaryPath: string;
  readonly agentDir: string | undefined;
}

function primeDiscoveryCacheKey(target: PrimeDiscoveryTarget): string {
  return `${resolvePrimeBinaryPath(target.binaryPath)}\u0000${target.agentDir?.trim() ?? ""}`;
}

export function makeCachedPrimeModelDiscovery<E, R>(input: {
  readonly discoveryLock: Semaphore.Semaphore;
  readonly discover: (
    target: PrimeDiscoveryTarget,
  ) => Effect.Effect<ProviderListModelsResult, E, R>;
}) {
  const cache = new Map<
    string,
    { readonly expiresAt: number; readonly result: ProviderListModelsResult }
  >();
  return (target: PrimeDiscoveryTarget, options?: { readonly forceReload?: boolean }) => {
    const cacheKey = primeDiscoveryCacheKey(target);
    const cached = cache.get(cacheKey);
    if (options?.forceReload !== true && cached && cached.expiresAt > Date.now()) {
      return Effect.succeed({ ...cached.result, cached: true });
    }
    return input.discoveryLock.withPermits(1)(
      Effect.gen(function* () {
        const cached = cache.get(cacheKey);
        if (options?.forceReload !== true && cached && cached.expiresAt > Date.now()) {
          return { ...cached.result, cached: true };
        }
        const result = yield* input.discover({
          binaryPath: resolvePrimeBinaryPath(target.binaryPath),
          agentDir: target.agentDir?.trim() || undefined,
        });
        if (result.error === undefined) {
          setPrimeDiscoveryCacheEntry(cache, cacheKey, {
            expiresAt: Date.now() + PRIME_MODEL_DISCOVERY_CACHE_MS,
            result,
          });
        }
        return result;
      }),
    );
  };
}

function isPrimeAcpDebugEnabled(): boolean {
  return process.env[PRIME_ACP_DEBUG_ENV] === "1";
}

function redactPrimeDiscoveryError(value: unknown): string {
  const message = value instanceof Error ? value.message : String(value);
  return String(redactAcpLogSecrets(message));
}

function describePrimeDiscoveryFailure(
  binaryPath: string,
  result: Pick<PrimeRpcDiscoveryResult, "stderr" | "exitCode">,
  fallback: string,
): string {
  const stderr = result.stderr.trim();
  if (stderr) {
    return redactPrimeDiscoveryError(stderr);
  }
  return result.exitCode !== 0
    ? `'${binaryPath} --mode rpc' exited with code ${result.exitCode}.`
    : fallback;
}

function acpToAdapterError(threadId: ThreadId) {
  return (cause: { readonly message: string }) =>
    new ProviderAdapterProcessError({
      provider: PROVIDER,
      threadId,
      detail: cause.message,
      cause,
    });
}

function buildPrimePromptParts(input: {
  readonly text: string | undefined;
  readonly attachments: ReadonlyArray<ChatAttachment> | undefined;
  readonly attachmentsDir: string;
  readonly interactionMode: ProviderInteractionMode;
  readonly fileSystem: FileSystem.FileSystem;
}): Effect.Effect<Array<Acp.ContentBlock>, ProviderAdapterRequestError> {
  return Effect.gen(function* () {
    const promptText = appendFileAttachmentsPromptBlock({
      text: input.text
        ? withAcpPlanModePrompt({
            text: input.text.trim(),
            interactionMode: input.interactionMode,
            promptPrefix: PRIME_PLAN_MODE_PROMPT_PREFIX,
          })
        : undefined,
      attachments: input.attachments,
      attachmentsDir: input.attachmentsDir,
      include: "all-files",
    });

    const promptParts: Array<Acp.ContentBlock> = [];
    if (promptText?.trim()) {
      promptParts.push({ type: "text", text: promptText });
    }

    promptParts.push(
      ...(yield* loadProviderPromptImageBlocks({
        attachments: input.attachments,
        attachmentsDir: input.attachmentsDir,
        provider: PROVIDER,
        method: "session/prompt",
        readFile: input.fileSystem.readFile,
      })),
    );
    return promptParts;
  });
}

// Settles the active turn. Returns whether the turn was actually cleared
// (false when it already settled, keeping the call sites idempotent).
function settlePrimeActiveTurn(ctx: PrimeSessionContext, turnId: TurnId): boolean {
  if (!clearAcpActiveTurn(ctx, turnId)) {
    return false;
  }
  clearPrimeActiveToolCallIdleState(ctx);
  return true;
}

function resolvePrimeCurrentIdleTimeoutMs(
  ctx: Pick<PrimeSessionContext, "primeToolCallLifecycleById">,
  timeouts: PrimeAdapterTimeouts,
): number {
  for (const lifecycle of ctx.primeToolCallLifecycleById.values()) {
    if (lifecycle === "active") {
      return timeouts.toolIdleMs;
    }
  }
  return timeouts.turnIdleMs;
}

function updatePrimeToolCallIdleState(
  ctx: Pick<PrimeSessionContext, "primeToolCallLifecycleById">,
  toolCall: AcpToolCallState,
): void {
  const { toolCallId, status } = toolCall;
  if (status === "completed" || status === "failed") {
    ctx.primeToolCallLifecycleById.set(toolCallId, "terminal");
    return;
  }
  if (
    (status === "pending" || status === "inProgress") &&
    ctx.primeToolCallLifecycleById.get(toolCallId) !== "terminal"
  ) {
    ctx.primeToolCallLifecycleById.set(toolCallId, "active");
  }
}

function clearPrimeActiveToolCallIdleState(ctx: PrimeSessionContext): void {
  ctx.primeToolCallLifecycleById.clear();
}

function describeCause(cause: Cause.Cause<unknown>): string {
  const squashed = Cause.squash(cause);
  return squashed instanceof Error ? squashed.message : String(squashed);
}

function isRetryablePrimeResumeStartFailure(cause: Cause.Cause<ProviderAdapterError>): boolean {
  if (Cause.hasInterruptsOnly(cause)) {
    return false;
  }
  // Only transport/process failures are retried; validation and request
  // errors describe a configuration problem that a retry cannot fix.
  return Cause.squash(cause) instanceof ProviderAdapterProcessError;
}

export function makePrimeAdapter(
  primeSettings: PrimeAcpRuntimeSettings = {},
  options?: PrimeAdapterLiveOptions,
) {
  const timeouts = options?.timeouts ?? resolvePrimeAdapterTimeouts();
  const watchdogIntervalMs = Math.min(5_000, timeouts.turnIdleMs, timeouts.toolIdleMs);

  return Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const serverConfig = yield* Effect.service(ServerConfig);
    const createAcpRuntime = options?.makeAcpRuntime ?? makePrimeAcpRuntime;
    const runRpcDiscovery = options?.runRpcDiscovery ?? runPrimeRpcDiscovery;
    const agentGatewayCredentials = Option.getOrUndefined(
      yield* Effect.serviceOption(AgentGatewayCredentials),
    );

    let nativeEventLogger = options?.nativeEventLogger;
    let managedNativeEventLogger: EventNdjsonLogger | undefined;
    if (nativeEventLogger === undefined && options?.nativeEventLogPath !== undefined) {
      managedNativeEventLogger = yield* makeEventNdjsonLogger(options.nativeEventLogPath, {
        stream: "native",
      });
      nativeEventLogger = managedNativeEventLogger;
    }

    const sessions = new Map<ThreadId, PrimeSessionContext>();
    const commandDiscoveryCache = new Map<
      string,
      { readonly expiresAt: number; readonly descriptors: ReadonlyArray<PrimeCommandDescriptor> }
    >();
    const discoveryLock = yield* Semaphore.make(1);
    // `provider/model` -> registry entry per discovery target, filled from the
    // same get_available_models answer that backs listModels; serves context
    // windows for the usage meter and fast-mode eligibility at session start.
    // Bounded like the discovery caches (one entry per target).
    const primeRegistryModels = new Map<
      string,
      { readonly expiresAt: number; readonly models: ReadonlyMap<string, PrimeRegistryModel> }
    >();
    const primeRegistryRetryAt = new Map<string, number>();
    const withThreadLock = yield* makeAcpThreadLock();
    // One lock per Prime sessions dir and session cwd. A fresh start holds it
    // from its pre-spawn snapshot until session-file detection settles, so two
    // fresh starts in the same cwd can never both claim the earliest new file.
    // Prime keeps every project's sessions in one dir (getDefaultSessionDir
    // ignores cwd) but detection only binds a file whose header cwd matches,
    // so starts in different cwds have nothing to serialize. Entries are
    // reference-counted and dropped once no start or retry holds them, the
    // way providerLifecycleCoordinator keeps its per-thread locks.
    const primeSessionLocks = new Map<string, PrimeSessionLockEntry>();
    const referencePrimeSessionLock = (key: string): PrimeSessionLockEntry => {
      let entry = primeSessionLocks.get(key);
      if (entry === undefined) {
        entry = { semaphore: Semaphore.makeUnsafe(1), users: 0 };
        primeSessionLocks.set(key, entry);
      }
      entry.users += 1;
      return entry;
    };
    const releasePrimeSessionLock = (key: string, entry: PrimeSessionLockEntry) =>
      Effect.sync(() => {
        entry.users -= 1;
        if (entry.users === 0 && primeSessionLocks.get(key) === entry) {
          primeSessionLocks.delete(key);
        }
      });
    // Holds the lock until the returned release effect runs or the calling
    // scope closes, whichever comes first (the release is idempotent).
    const acquirePrimeSessionLock = (
      key: string,
    ): Effect.Effect<Effect.Effect<void>, never, Scope.Scope> =>
      Effect.acquireRelease(
        Effect.suspend(() => {
          const entry = referencePrimeSessionLock(key);
          let released = false;
          const release = Effect.suspend(() => {
            if (released) {
              return Effect.void;
            }
            released = true;
            return entry.semaphore
              .release(1)
              .pipe(Effect.andThen(releasePrimeSessionLock(key, entry)));
          });
          return entry.semaphore.take(1).pipe(Effect.as(release));
        }),
        (release) => release,
      );
    const withPrimeSessionLock = <A, E, R>(
      key: string,
      effect: Effect.Effect<A, E, R>,
    ): Effect.Effect<A, E, R> =>
      Effect.suspend(() => {
        const entry = referencePrimeSessionLock(key);
        return entry.semaphore
          .withPermits(1)(effect)
          .pipe(Effect.ensuring(releasePrimeSessionLock(key, entry)));
      });
    // Session ids a fresh start has detected but not registered yet. A
    // fast-mode restart relaunches with --resume after releasing its session
    // lock, so without this another thread's deferred detection (whose
    // snapshot predates the file) could bind the file meanwhile.
    const startingPrimeSessionIds = new Map<ThreadId, string>();
    // Prime session ids already bound to (or reserved by) other threads;
    // detection must never re-bind one of them to a second thread.
    const claimedPrimeSessionIds = (exceptThreadId: ThreadId): ReadonlySet<string> => {
      const claimed = new Set<string>();
      for (const other of sessions.values()) {
        if (other.threadId !== exceptThreadId && other.primeSessionId !== undefined) {
          claimed.add(other.primeSessionId);
        }
      }
      for (const [threadId, sessionId] of startingPrimeSessionIds) {
        if (threadId !== exceptThreadId) {
          claimed.add(sessionId);
        }
      }
      return claimed;
    };
    const runtimeEventPubSub = yield* PubSub.bounded<ProviderRuntimeEvent>(
      PROVIDER_ADAPTER_RUNTIME_EVENT_BUFFER_CAPACITY,
    );

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const nextEventId = Effect.map(Random.nextUUIDv4, (id) => EventId.makeUnsafe(id));
    const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

    const resolveDiscoveryTarget = (input: {
      readonly binaryPath?: string | undefined;
      readonly agentDir?: string | undefined;
    }): PrimeDiscoveryTarget => ({
      binaryPath: resolvePrimeBinaryPath(input.binaryPath?.trim() || primeSettings.binaryPath),
      agentDir: input.agentDir?.trim() || primeSettings.agentDir?.trim() || undefined,
    });

    const discoverPrimeModelsUncached = (target: PrimeDiscoveryTarget) => {
      const unavailableResult = {
        models: [],
        source: "prime.unavailable",
        cached: false,
      } satisfies ProviderListModelsResult;

      return Effect.gen(function* () {
        const discovery = yield* runRpcDiscovery({
          childProcessSpawner,
          binaryPath: target.binaryPath,
          agentDir: target.agentDir,
          requests: [
            { id: PRIME_RPC_MODELS_REQUEST_ID, type: "get_available_models" },
            { id: PRIME_RPC_STATE_REQUEST_ID, type: "get_state" },
          ],
        });
        const modelsResponse = discovery.responses.get(PRIME_RPC_MODELS_REQUEST_ID);
        const stateResponse = discovery.responses.get(PRIME_RPC_STATE_REQUEST_ID);
        if (modelsResponse === undefined || modelsResponse.success === false) {
          return {
            ...unavailableResult,
            error: describePrimeDiscoveryFailure(
              target.binaryPath,
              discovery,
              modelsResponse?.error ??
                `'${target.binaryPath} --mode rpc' did not answer get_available_models.`,
            ),
          } satisfies ProviderListModelsResult;
        }
        const settings = yield* Effect.promise(() =>
          readPrimeUserSettings(resolvePrimeAgentDir(target.agentDir)),
        );
        const registry = new Map<string, PrimeRegistryModel>();
        for (const registryModel of parsePrimeRegistryModels(modelsResponse.data)) {
          const slug = primeModelSlug(registryModel);
          if (!registry.has(slug)) {
            registry.set(slug, registryModel);
          }
        }
        setPrimeDiscoveryCacheEntry(primeRegistryModels, primeDiscoveryCacheKey(target), {
          expiresAt: Date.now() + PRIME_MODEL_DISCOVERY_CACHE_MS,
          models: registry,
        });
        const models = parsePrimeModelRegistry({
          models: modelsResponse.data,
          state: stateResponse?.success === false ? undefined : stateResponse?.data,
          settings,
        });
        if (models.length === 0) {
          // Prime only lists models for providers with stored credentials.
          return {
            ...unavailableResult,
            error: `'${target.binaryPath} --mode rpc' returned no models. ${PRIME_LOGIN_GUIDANCE}`,
          } satisfies ProviderListModelsResult;
        }
        return {
          models,
          source: "prime-rpc",
          cached: false,
        } satisfies ProviderListModelsResult;
      }).pipe(
        Effect.catch((error) =>
          Effect.succeed({
            ...unavailableResult,
            error: redactPrimeDiscoveryError(error),
          } satisfies ProviderListModelsResult),
        ),
        Effect.scoped,
        Effect.timeoutOption(PRIME_MODEL_DISCOVERY_TIMEOUT_MS),
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.succeed({
                ...unavailableResult,
                error: `Timed out after ${Math.round(PRIME_MODEL_DISCOVERY_TIMEOUT_MS / 1000)}s while discovering Prime Agent models via RPC.`,
              } satisfies ProviderListModelsResult),
            onSome: (result) => Effect.succeed(result),
          }),
        ),
      );
    };
    const discoverPrimeModels = makeCachedPrimeModelDiscovery({
      discoveryLock,
      discover: discoverPrimeModelsUncached,
    });

    const offerRuntimeEvent = (
      lifecycleGeneration: string | undefined,
      event: ProviderRuntimeEvent,
    ) =>
      PubSub.publish(
        runtimeEventPubSub,
        stampAcpRuntimeEventLifecycleGeneration(event, lifecycleGeneration),
      ).pipe(Effect.asVoid);

    const logNative = (threadId: ThreadId, method: string, payload: unknown) =>
      Effect.gen(function* () {
        if (!nativeEventLogger) return;
        const observedAt = new Date().toISOString();
        yield* nativeEventLogger.write(
          {
            observedAt,
            event: {
              id: crypto.randomUUID(),
              kind: "notification",
              provider: PROVIDER,
              createdAt: observedAt,
              method,
              threadId,
              payload,
            },
          },
          threadId,
        );
      });

    const emitPlanUpdate = (
      ctx: PrimeSessionContext,
      payload: AcpPlanUpdate,
      rawPayload: unknown,
    ) =>
      Effect.gen(function* () {
        if (!acceptAcpPlanUpdate(ctx, payload)) return;
        yield* offerRuntimeEvent(
          ctx.lifecycleGeneration,
          makeAcpPlanUpdatedEvent({
            stamp: yield* makeEventStamp(),
            provider: PROVIDER,
            threadId: ctx.threadId,
            turnId: ctx.activeTurnId,
            payload,
            source: "acp.jsonrpc",
            method: "session/update",
            rawPayload,
          }),
        );
      });

    const requireSession = (threadId: ThreadId) => {
      const ctx = sessions.get(threadId);
      if (!ctx || ctx.stopped) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({
            provider: PROVIDER,
            threadId,
          }),
        );
      }
      return Effect.succeed(ctx);
    };

    const stopSessionInternal = (ctx: PrimeSessionContext) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        clearPrimeActiveToolCallIdleState(ctx);
        yield* cancelAgentGatewayTurn(ctx.gatewaySessionLease, ctx.activeTurnId);
        ctx.gatewaySessionLease?.release();
        if (ctx.sessionConfigReady !== undefined) {
          yield* Deferred.succeed(ctx.sessionConfigReady, undefined);
          ctx.sessionConfigReady = undefined;
        }
        if (ctx.notificationFiber) {
          yield* Fiber.interrupt(ctx.notificationFiber);
        }
        ctx.pendingSessionUsageEmit = undefined;
        const sessionUsageTail = ctx.sessionUsageTail;
        ctx.sessionUsageTail = undefined;
        if (sessionUsageTail !== undefined) {
          yield* Fiber.interrupt(sessionUsageTail.fiber);
        }
        yield* Effect.ignore(Scope.close(ctx.scope, Exit.void));
        sessions.delete(ctx.threadId);
        yield* offerRuntimeEvent(ctx.lifecycleGeneration, {
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { exitKind: "graceful" },
        });
      });

    const waitForPrimeQueuedTurnEventsDrained = (ctx: PrimeSessionContext) =>
      waitForAcpQueuedTurnEventsDrained({
        sessionUpdatesEnqueuedCount: ctx.acp.sessionUpdatesEnqueuedCount,
        sessionUpdatesProcessed: () => ctx.sessionUpdatesProcessed,
        maxWaitMs: PRIME_TURN_SETTLE_DRAIN_MAX_WAIT_MS,
        pollMs: PRIME_TURN_SETTLE_DRAIN_POLL_MS,
      });

    const noteSuppressedPrimeRuntimeEvent = (ctx: PrimeSessionContext, eventTag: string) =>
      Effect.gen(function* () {
        if (!isPrimeAcpDebugEnabled()) {
          return;
        }
        yield* Effect.logInfo("prime.acp.runtime_event_suppressed", {
          threadId: ctx.threadId,
          turnId: ctx.activeTurnId,
          eventTag,
          reason: "orphan-turn-event",
        });
      });

    const activeTurnIdForPrimeRuntimeEvent = (ctx: PrimeSessionContext, eventTag: string) =>
      Effect.gen(function* () {
        if (ctx.compactingThread) {
          return undefined;
        }
        if (ctx.activeTurnId === undefined) {
          yield* noteSuppressedPrimeRuntimeEvent(ctx, eventTag);
          return undefined;
        }
        return ctx.activeTurnId;
      });

    // One item per compaction, like Pi: Prime compacts repeatedly within a
    // thread, and a shared id would make the timeline treat every later
    // compaction as an update of the first row.
    const makePrimeCompactionItemId = () =>
      RuntimeItemId.makeUnsafe(`prime-compaction-${crypto.randomUUID()}`);

    const emitPrimeContextCompactionRuntimeEvent = (
      ctx: PrimeSessionContext,
      input: {
        readonly itemId: RuntimeItemId;
        readonly lifecycle: "item.updated" | "item.completed";
        readonly status: "inProgress" | "completed" | "failed";
        readonly title: string;
        readonly detail?: string;
      },
    ) =>
      Effect.gen(function* () {
        yield* offerRuntimeEvent(ctx.lifecycleGeneration, {
          type: input.lifecycle,
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          itemId: input.itemId,
          payload: {
            itemType: "context_compaction",
            status: input.status,
            title: input.title,
            ...(input.detail ? { detail: input.detail } : {}),
          },
        });
      });

    // Waits until the notification consumer has been quiet briefly so state it
    // records from queued events (e.g. compactionFailedToolDetail) is visible
    // before the compaction outcome is decided. Bounded.
    const settlePrimeCompactionOutcome = (ctx: PrimeSessionContext) =>
      Effect.gen(function* () {
        yield* waitForPrimeQueuedTurnEventsDrained(ctx);
        const startedAt = Date.now();
        while (true) {
          const now = Date.now();
          const lastActivityAt = Math.max(ctx.lastTurnActivityAt ?? 0, startedAt);
          if (
            now - lastActivityAt >= PRIME_COMPACT_OUTCOME_QUIET_MS ||
            now - startedAt >= PRIME_COMPACT_OUTCOME_MAX_WAIT_MS
          ) {
            return;
          }
          yield* Effect.sleep(50);
        }
      });

    // After a timed-out /compact, hold new prompts until the forked cancel is
    // on the wire (bounded) and the stale update stream has had its quiet
    // window, so stragglers cannot be attributed to the new turn.
    const waitForAbandonedPrimeCompaction = (ctx: PrimeSessionContext) =>
      Effect.gen(function* () {
        const cancelFiber = ctx.compactionCancelFiber;
        if (cancelFiber !== undefined) {
          yield* Fiber.join(cancelFiber).pipe(
            Effect.ignoreCause(),
            Effect.timeoutOption(PRIME_COMPACT_CANCEL_WAIT_MS),
          );
          ctx.compactionCancelFiber = undefined;
          if (ctx.compactionQuietUntil !== undefined) {
            ctx.compactionQuietUntil = Math.max(
              ctx.compactionQuietUntil,
              Date.now() + PRIME_COMPACT_ABANDON_QUIET_MS,
            );
          }
        }
        const compactionQuietUntil = ctx.compactionQuietUntil;
        if (compactionQuietUntil !== undefined) {
          const waitMs = compactionQuietUntil - Date.now();
          if (waitMs > 0) {
            yield* Effect.sleep(waitMs);
          }
          ctx.compactionQuietUntil = undefined;
        }
      });

    // Publishes the Prime session id behind the resume cursor once its
    // session file has been located. Idempotent per session.
    const publishPrimeSessionId = (ctx: PrimeSessionContext, sessionId: string) =>
      Effect.gen(function* () {
        if (ctx.primeSessionId === sessionId) {
          return;
        }
        ctx.primeSessionId = sessionId;
        ctx.sessionDetection = undefined;
        ctx.session = {
          ...ctx.session,
          resumeCursor: buildPrimeResumeCursor(sessionId),
          updatedAt: yield* nowIso,
        };
      });

    // A model's entry in Prime's registry. Served from the discovery cache
    // while it is fresh (the same five minutes as listModels); a cold or
    // expired cache runs one probe, and a failed probe backs off so a busy
    // turn never blocks on it repeatedly. Undefined for an unknown model.
    const resolvePrimeRegistryModel = (
      input: { readonly threadId: ThreadId; readonly discoveryTarget: PrimeDiscoveryTarget },
      slug: string,
    ) =>
      Effect.gen(function* () {
        const cacheKey = primeDiscoveryCacheKey(input.discoveryTarget);
        const cached = primeRegistryModels.get(cacheKey);
        const known =
          cached !== undefined && cached.expiresAt > Date.now()
            ? cached.models.get(slug)
            : undefined;
        if (known !== undefined) {
          return known;
        }
        const retryAt = primeRegistryRetryAt.get(cacheKey);
        if (retryAt !== undefined && retryAt > Date.now()) {
          return undefined;
        }
        const result = yield* discoverPrimeModels(input.discoveryTarget);
        if (result.error !== undefined) {
          primeRegistryRetryAt.set(cacheKey, Date.now() + PRIME_REGISTRY_DISCOVERY_RETRY_MS);
          yield* Effect.logWarning("prime.acp.registry_model_unavailable", {
            threadId: input.threadId,
            model: slug,
            detail: result.error,
            retryInMs: PRIME_REGISTRY_DISCOVERY_RETRY_MS,
          });
          return undefined;
        }
        primeRegistryRetryAt.delete(cacheKey);
        // Whatever the probe (or the models cache it was served from, which
        // expires at the same moment) left behind is as fresh as listModels.
        return primeRegistryModels.get(cacheKey)?.models.get(slug);
      });

    // Context window for the model that produced a usage line.
    const resolvePrimeContextWindow = (ctx: PrimeSessionContext, model: PrimeSessionModel) =>
      Effect.map(
        resolvePrimeRegistryModel(ctx, `${model.provider}/${model.modelId}`),
        (registryModel) => registryModel?.contextWindow,
      );

    // The tier Synara asks Prime to seed for a thread. Fast mode is honoured
    // only where Prime's own supportsFastMode would honour it (an unknown
    // model is not eligible): Prime clamps every other model to "default" at
    // session creation, so a restart could not change anything there.
    const resolvePrimeDesiredServiceTier = (
      input: { readonly threadId: ThreadId; readonly discoveryTarget: PrimeDiscoveryTarget },
      modelSelection:
        | { readonly model: string; readonly options?: PrimeModelOptions | undefined }
        | undefined,
    ): Effect.Effect<PrimeServiceTier> =>
      Effect.gen(function* () {
        const requested = primeServiceTierFor(modelSelection?.options);
        if (requested === PRIME_SERVICE_TIER_DEFAULT || modelSelection === undefined) {
          return PRIME_SERVICE_TIER_DEFAULT;
        }
        const registryModel = yield* resolvePrimeRegistryModel(input, modelSelection.model);
        if (registryModel === undefined || !primeModelSupportsFastMode(registryModel)) {
          yield* Effect.logInfo("prime.acp.fast_mode_unsupported", {
            threadId: input.threadId,
            model: modelSelection.model,
            knownModel: registryModel !== undefined,
          });
          return PRIME_SERVICE_TIER_DEFAULT;
        }
        return requested;
      });

    // The tier Prime's next --resume of `file` would seed; undefined (after a
    // warning) when the file cannot be read, so the start proceeds on
    // whatever tier the file has.
    const readPrimeSessionServiceTierOrWarn = (threadId: ThreadId, file: string) =>
      readPrimeSessionServiceTier(file).pipe(
        Effect.provideService(FileSystem.FileSystem, fileSystem),
        Effect.catch((error) =>
          Effect.logWarning("prime.acp.service_tier_read_failed", {
            threadId,
            file,
            detail: error.message,
          }).pipe(Effect.as(undefined)),
        ),
      );

    // Appends the entry Prime's /fast would write. Only called between
    // stopping the previous child and spawning the next one, never while a
    // child may own the file. False (after a warning) when the append failed;
    // the thread then keeps the tier the file already has.
    const appendPrimeServiceTierChangeOrWarn = (
      threadId: ThreadId,
      file: string,
      serviceTier: PrimeServiceTier,
    ) =>
      appendPrimeServiceTierChange(file, serviceTier).pipe(
        Effect.provideService(FileSystem.FileSystem, fileSystem),
        Effect.as(true),
        Effect.catch((error) =>
          Effect.logWarning("prime.acp.service_tier_append_failed", {
            threadId,
            file,
            serviceTier,
            detail: error.message,
          }).pipe(Effect.as(false)),
        ),
      );

    // Emits whatever usage the compaction hold deferred. Idempotent: the
    // compaction outcome path and the hold fiber both call it.
    const flushPendingPrimeSessionUsage = (ctx: PrimeSessionContext) =>
      Effect.gen(function* () {
        const holdFiber = ctx.pendingSessionUsageHoldFiber;
        ctx.pendingSessionUsageHoldFiber = undefined;
        const emit = ctx.pendingSessionUsageEmit;
        ctx.pendingSessionUsageEmit = undefined;
        if (holdFiber !== undefined) {
          yield* Fiber.interrupt(holdFiber);
        }
        if (emit !== undefined && !ctx.stopped) {
          yield* emit;
        }
      });

    // Tail callback: projects the folded session state onto a usage snapshot
    // and publishes it the way an ACP usage_update would.
    const emitPrimeSessionUsage = (ctx: PrimeSessionContext, update: PrimeSessionUsageUpdate) =>
      Effect.gen(function* () {
        if (ctx.stopped) {
          return;
        }
        const model = resolvePrimeSessionUsageModel(update.state);
        const contextWindow =
          model === undefined ? undefined : yield* resolvePrimeContextWindow(ctx, model);
        const usage = snapshotPrimeSessionUsage(update.state, contextWindow);
        if (usage === undefined) {
          return;
        }
        // Cumulative cost rides the same path an ACP cost notification would:
        // turn.completed reads it when the turn settles (see finalizeAcpActiveTurnCost).
        if (update.state.costUsd !== undefined) {
          recordAcpSessionCost(ctx, { amount: update.state.costUsd, currency: "USD" });
        }
        const rawPayload = describePrimeSessionEntry(update.entry);
        // Prime compacts on its own mid-turn once the window fills. The shared
        // ACP runtime model drops Prime's compaction metadata, so the session
        // file is the only signal: publish the same row the manual path does,
        // ahead of the estimate that follows it (a completed compaction resets
        // the meter, and the estimate then repopulates it).
        const autoCompaction =
          update.reason === "estimate" && !ctx.compactingThread
            ? readPrimeCompactionEntry(update.entry)
            : undefined;
        if (autoCompaction !== undefined) {
          yield* emitPrimeContextCompactionRuntimeEvent(ctx, {
            itemId: makePrimeCompactionItemId(),
            lifecycle: "item.completed",
            status: "completed",
            title: PRIME_COMPACTION_COMPLETED_TITLE,
            // The timeline renders the title and detail as one line, so the
            // detail continues the sentence: "Context compacted from N tokens".
            ...(autoCompaction.tokensBefore !== undefined
              ? {
                  detail: `from ${autoCompaction.tokensBefore.toLocaleString("en-US")} tokens`,
                }
              : {}),
          });
        }
        const emit = Effect.gen(function* () {
          if (ctx.stopped) {
            return;
          }
          yield* offerRuntimeEvent(
            ctx.lifecycleGeneration,
            makeAcpTokenUsageEvent({
              stamp: yield* makeEventStamp(),
              provider: PROVIDER,
              threadId: ctx.threadId,
              turnId: ctx.compactingThread ? undefined : ctx.activeTurnId,
              usage,
              source: PRIME_SESSION_FILE_USAGE_SOURCE,
              method: PRIME_SESSION_FILE_USAGE_METHOD,
              rawPayload,
            }),
          );
        });
        if (!ctx.compactingThread) {
          yield* emit;
          return;
        }
        // Manual /compact in flight: the terminal compaction event has not
        // been published yet and would invalidate this snapshot on the meter.
        // Keep only the latest snapshot; compactThread flushes it once the
        // outcome is out, or the hold fiber does after the bound.
        ctx.pendingSessionUsageEmit = emit;
        if (ctx.pendingSessionUsageHoldFiber === undefined) {
          ctx.pendingSessionUsageHoldFiber = yield* Effect.sleep(
            PRIME_SESSION_USAGE_COMPACTION_HOLD_MS,
          ).pipe(
            Effect.andThen(
              Effect.suspend(() => {
                // Clear before flushing so the flush never interrupts this fiber.
                ctx.pendingSessionUsageHoldFiber = undefined;
                return flushPendingPrimeSessionUsage(ctx);
              }),
            ),
            Effect.forkIn(ctx.scope),
          );
        }
      });

    const flushPrimeSessionUsage = (ctx: PrimeSessionContext) =>
      ctx.sessionUsageTail?.flush ?? Effect.void;

    // Starts (or replaces) the usage tail for a session file. The fiber lives
    // in the session scope, so stopSessionInternal, session replacement and
    // stopAll all end it; the watcher closes with the fiber.
    const startPrimeSessionUsageTail = (ctx: PrimeSessionContext, file: string) =>
      Effect.gen(function* () {
        if (ctx.stopped || ctx.sessionUsageTail?.file === file) {
          return;
        }
        const previous = ctx.sessionUsageTail;
        ctx.sessionUsageTail = undefined;
        if (previous !== undefined) {
          yield* Fiber.interrupt(previous.fiber);
        }
        const tail = yield* makePrimeSessionUsageTail({
          fileSystem,
          file,
          isActive: () =>
            ctx.activeTurnId !== undefined || ctx.turnStarting || ctx.compactingThread,
          onUpdate: (update) => emitPrimeSessionUsage(ctx, update),
          activePollMs: PRIME_SESSION_USAGE_ACTIVE_POLL_MS,
          idlePollMs: PRIME_SESSION_USAGE_IDLE_POLL_MS,
        });
        const fiber = yield* tail.run.pipe(
          // The tail cannot fail; a defect is worth a log line, an interrupt
          // (session teardown) is not.
          Effect.tapCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.void
              : Effect.logWarning("prime.acp.session_usage_tail_failed", {
                  threadId: ctx.threadId,
                  file,
                  detail: describeCause(cause),
                }),
          ),
          Effect.ignoreCause(),
          Effect.forkIn(ctx.scope),
        );
        ctx.sessionUsageTail = { file, flush: tail.flush, fiber };
        yield* Effect.logInfo("prime.acp.session_usage_tail_started", {
          threadId: ctx.threadId,
          sessionId: ctx.primeSessionId,
          file,
        });
      });

    // A located session file publishes the resume cursor and starts the
    // usage tail (its catch-up read reports the current context at once).
    const bindPrimeSessionFile = (
      ctx: PrimeSessionContext,
      binding: { readonly sessionId: string; readonly file: string },
    ) =>
      Effect.gen(function* () {
        yield* publishPrimeSessionId(ctx, binding.sessionId);
        yield* startPrimeSessionUsageTail(ctx, binding.file);
      });

    // Polls for the session file a fresh start's child wrote. Callers hold
    // the session lock for `detection.lockKey` while this runs.
    const detectPrimeSessionFile = (input: {
      readonly threadId: ThreadId;
      readonly cwd: string;
      readonly detection: PrimeSessionDetection;
      readonly timeoutMs: number;
    }): Effect.Effect<PrimeDetectedSession | undefined> =>
      Effect.gen(function* () {
        const detected = yield* Effect.promise(() =>
          detectNewPrimeSession({
            before: input.detection.snapshot,
            cwd: input.cwd,
            sinceMs: input.detection.spawnedAt,
            timeoutMs: input.timeoutMs,
            claimedSessionIds: claimedPrimeSessionIds(input.threadId),
          }),
        );
        if (detected === undefined) {
          yield* Effect.logInfo("prime.acp.session_file_not_detected", {
            threadId: input.threadId,
            sessionsDir: input.detection.snapshot.sessionsDir,
            timeoutMs: input.timeoutMs,
          });
          return undefined;
        }
        yield* Effect.logInfo("prime.acp.session_file_detected", {
          threadId: input.threadId,
          sessionId: detected.sessionId,
          file: detected.file,
        });
        return detected;
      });

    const startSession: PrimeAdapterShape["startSession"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          if (input.provider !== undefined && input.provider !== PROVIDER) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            });
          }
          yield* validatePrimeRuntimeMode(input.runtimeMode);

          const cwd = resolveAcpSessionCwd({
            inputCwd: input.cwd,
            serverCwd: serverConfig.cwd,
            homeDir: serverConfig.homeDir,
          });
          if (cwd === undefined) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "cwd is required and no server cwd fallback is available.",
            });
          }

          const primeModelSelection =
            input.modelSelection?.provider === PROVIDER ? input.modelSelection : undefined;

          const existing = sessions.get(input.threadId);
          if (existing && !existing.stopped) {
            yield* stopSessionInternal(existing);
          }

          // Replaced on each resumed-start retry; the finalizer below reads
          // the current value so a retried attempt never leaks its child.
          let sessionScope = yield* Scope.make("sequential");
          let sessionScopeTransferred = false;

          const gatewaySessionLease = acquireAgentGatewaySessionLease(
            agentGatewayCredentials,
            input.threadId,
            PROVIDER,
          );

          yield* Effect.addFinalizer(() =>
            sessionScopeTransferred ? Effect.void : Scope.close(sessionScope, Exit.void),
          );
          yield* Effect.addFinalizer(() =>
            sessionScopeTransferred || !gatewaySessionLease
              ? Effect.void
              : Effect.sync(gatewaySessionLease.release),
          );
          // A detected-but-unregistered session id is reserved only for the
          // rest of this start; once registered, `sessions` claims it.
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              startingPrimeSessionIds.delete(input.threadId);
            }),
          );

          let ctx!: PrimeSessionContext;
          const acpNativeLoggers = makeAcpNativeLoggers({
            nativeEventLogger,
            provider: PROVIDER,
            threadId: input.threadId,
          });
          const acpRuntimeLoggers = makeAcpDebugLoggers({
            base: acpNativeLoggers,
            enabled: isPrimeAcpDebugEnabled(),
            provider: PROVIDER,
            marker: PRIME_ACP_TRANSPORT_DEBUG_MARKER,
            payloadLimit: PRIME_ACP_LOG_PAYLOAD_LIMIT,
            shouldMirrorIncomingRaw: (payload) => payload.includes("ai.primeintellect"),
          });
          const providerPrimeOptions = readPrimeProviderStartOptions(input.providerOptions);
          const effectivePrimeSettings: PrimeAcpRuntimeSettings = {
            ...(primeSettings.binaryPath !== undefined
              ? { binaryPath: primeSettings.binaryPath }
              : {}),
            ...(primeSettings.agentDir !== undefined ? { agentDir: primeSettings.agentDir } : {}),
            ...(providerPrimeOptions?.binaryPath !== undefined
              ? { binaryPath: providerPrimeOptions.binaryPath }
              : {}),
            ...(providerPrimeOptions?.agentDir !== undefined
              ? { agentDir: providerPrimeOptions.agentDir }
              : {}),
          };
          const effectiveModel = resolvePrimeStartModel(primeModelSelection);
          const sessionsDir = resolvePrimeSessionsDir(
            resolvePrimeAgentDir(effectivePrimeSettings.agentDir),
          );

          // A persisted cursor names a Prime session header id. Verify the
          // file still exists before relaunching with --resume: Prime exits
          // non-zero for an unknown id, which would fail the whole start.
          const requestedResumeSessionId = parsePrimeResume(input.resumeCursor)?.sessionId;
          let resumeSessionId: string | undefined;
          let resumeFile: string | undefined;
          if (requestedResumeSessionId !== undefined) {
            resumeFile = yield* Effect.promise(() =>
              resolvePrimeSessionFile(sessionsDir, requestedResumeSessionId),
            );
            if (resumeFile === undefined) {
              yield* Effect.logWarning("prime.acp.resume_session_missing", {
                threadId: input.threadId,
                sessionId: requestedResumeSessionId,
                sessionsDir,
              });
            } else {
              resumeSessionId = requestedResumeSessionId;
            }
          }

          const discoveryTarget = resolveDiscoveryTarget(effectivePrimeSettings);
          const serviceTier = yield* resolvePrimeDesiredServiceTier(
            { threadId: input.threadId, discoveryTarget },
            primeModelSelection,
          );

          // A resumed session keeps the tier its file says. Reconcile it with
          // the thread's toggle now: the previous child for this thread is
          // stopped (above) and the next one is not spawned yet, so nothing
          // owns the file.
          if (resumeSessionId !== undefined && resumeFile !== undefined) {
            const currentTier = yield* readPrimeSessionServiceTierOrWarn(
              input.threadId,
              resumeFile,
            );
            if (currentTier !== undefined && currentTier !== serviceTier) {
              const appended = yield* appendPrimeServiceTierChangeOrWarn(
                input.threadId,
                resumeFile,
                serviceTier,
              );
              if (appended) {
                yield* Effect.logInfo("prime.acp.service_tier_reconciled", {
                  threadId: input.threadId,
                  sessionId: resumeSessionId,
                  from: currentTier,
                  to: serviceTier,
                });
              }
            }
          }

          yield* Effect.logInfo("prime.acp.start", {
            marker: PRIME_ACP_TRANSPORT_DEBUG_MARKER,
            debugEnv: PRIME_ACP_DEBUG_ENV,
            threadId: input.threadId,
            cwd,
            resume: resumeSessionId !== undefined,
            model: effectiveModel,
            requestedModel: primeModelSelection?.model,
            thinkingLevel: primeModelSelection?.options?.thinkingLevel,
            serviceTier,
            alwaysApprove: input.runtimeMode === "full-access",
            binaryPath: resolvePrimeBinaryPath(effectivePrimeSettings.binaryPath),
            agentDir: effectivePrimeSettings.agentDir,
          });

          // A fresh start binds to the session file Prime writes at process
          // start. Hold the lock for this sessions dir and cwd until this start
          // has settled (the startSession scope releases it; a fast-mode
          // restart releases it early) so overlapping fresh starts in one cwd
          // snapshot -> spawn -> detect one at a time.
          let sessionDetection: PrimeSessionDetection | undefined;
          let releaseSessionLock: Effect.Effect<void> = Effect.void;
          if (resumeSessionId === undefined) {
            const lockKey = primeSessionLockKey(
              sessionsDir,
              yield* Effect.promise(() => canonicalPrimeSessionCwd(cwd)),
            );
            releaseSessionLock = yield* acquirePrimeSessionLock(lockKey);
            // Snapshot the sessions dir before Prime can write its new file so
            // the post-start detection has a stable "before" set.
            sessionDetection = {
              snapshot: yield* Effect.promise(() => snapshotPrimeSessionFiles(sessionsDir)),
              spawnedAt: Date.now(),
              lockKey,
            };
          }

          const startPrimeRuntime = (attemptScope: Scope.Closeable) =>
            Effect.gen(function* () {
              const acp = yield* createAcpRuntime({
                primeSettings: effectivePrimeSettings,
                childProcessSpawner,
                cwd,
                runtimeMode: input.runtimeMode,
                spawnOptions: {
                  ...(effectiveModel !== undefined ? { model: effectiveModel } : {}),
                  ...(resumeSessionId !== undefined ? { resumeSessionId } : {}),
                },
                clientInfo: { name: "Synara", version: "0.0.0" },
                ...(agentGatewayCredentials && gatewaySessionLease
                  ? {
                      buildMcpServers: (initializeResult: Acp.InitializeResponse) =>
                        buildAcpSynaraMcpServers({
                          connection: gatewaySessionLease.connection,
                          initializeResult,
                          stdioProxy: agentGatewayCredentials.stdioProxy,
                        }),
                    }
                  : {}),
                ...acpRuntimeLoggers,
              }).pipe(
                Effect.provideService(Scope.Scope, attemptScope),
                Effect.mapError(acpToAdapterError(input.threadId)),
              );

              // prime-agent's acp-mode never sends session/request_permission
              // or session/elicitation (validatePrimeRuntimeMode fails closed
              // on that premise), so neither request is surfaced to the user.
              // Fail-closed responders still answer them: an unregistered
              // handler would leave a request from a future release buffered
              // in the runtime's startup registry until the session ends.
              yield* acp.handleRequestPermission((params) =>
                Effect.gen(function* () {
                  yield* logNative(input.threadId, "session/request_permission", params);
                  yield* Effect.logWarning("prime.acp.unsupported_permission_request", {
                    threadId: input.threadId,
                    turnId: ctx?.activeTurnId,
                  });
                  return { outcome: { outcome: "cancelled" } as const };
                }),
              );

              yield* acp.handleElicitation((params) =>
                Effect.gen(function* () {
                  yield* logNative(input.threadId, "session/elicitation", params);
                  yield* Effect.logWarning("prime.acp.unsupported_elicitation", {
                    threadId: input.threadId,
                    turnId: ctx?.activeTurnId,
                  });
                  return { action: "decline" } satisfies Acp.CreateElicitationResponse;
                }),
              );

              const started = yield* acp
                .start()
                .pipe(Effect.mapError(acpToAdapterError(input.threadId)));
              return { acp, started };
            });

          // Spawns into the current sessionScope. A resumed start retries a
          // bounded number of times; a fresh start never does. Reads
          // resumeSessionId when run, so the fast-mode restart below reuses it.
          const startPrimeRuntimeWithRetries: Effect.Effect<
            PrimeStartedRuntime,
            ProviderAdapterError
          > = Effect.gen(function* () {
            let attempt = 0;
            while (true) {
              attempt += 1;
              const outcome = yield* startPrimeRuntime(sessionScope).pipe(Effect.exit);
              if (Exit.isSuccess(outcome)) {
                return outcome.value;
              }
              if (
                resumeSessionId === undefined ||
                attempt >= PRIME_RESUME_START_ATTEMPTS ||
                !isRetryablePrimeResumeStartFailure(outcome.cause)
              ) {
                return yield* Effect.failCause(outcome.cause);
              }
              yield* Effect.logWarning("prime.acp.resume_start_retry", {
                threadId: input.threadId,
                sessionId: resumeSessionId,
                attempt,
                maxAttempts: PRIME_RESUME_START_ATTEMPTS,
                delayMs: timeouts.resumeStartRetryDelayMs,
                detail: describeCause(outcome.cause),
              });
              yield* Effect.ignore(Scope.close(sessionScope, Exit.void));
              yield* Effect.sleep(timeouts.resumeStartRetryDelayMs);
              sessionScope = yield* Scope.make("sequential");
            }
          });
          let runtime = yield* startPrimeRuntimeWithRetries;

          // A fresh start locates its session file before the session is
          // registered, so a fast-mode restart can relaunch into the resume
          // path without tearing down a registered session.
          let detectedSession: PrimeDetectedSession | undefined;
          if (sessionDetection !== undefined) {
            detectedSession = yield* detectPrimeSessionFile({
              threadId: input.threadId,
              cwd,
              detection: sessionDetection,
              timeoutMs: timeouts.sessionDetectMs,
            });
            if (detectedSession === undefined) {
              // Detection is retried after the first turn, when a restart would
              // discard that turn; the tier applies from the next restart.
              if (serviceTier !== PRIME_SERVICE_TIER_DEFAULT) {
                yield* Effect.logDebug("prime.acp.service_tier_deferred", {
                  threadId: input.threadId,
                  serviceTier,
                });
              }
            } else {
              startingPrimeSessionIds.set(input.threadId, detectedSession.sessionId);
              sessionDetection = undefined;
              if (serviceTier !== PRIME_SERVICE_TIER_DEFAULT) {
                const currentTier = yield* readPrimeSessionServiceTierOrWarn(
                  input.threadId,
                  detectedSession.file,
                );
                if (currentTier !== undefined && currentTier !== serviceTier) {
                  // The entry can only be appended while no child owns the
                  // file: kill this child, append, and relaunch with --resume
                  // through the resumed-start machinery above. Other fresh
                  // starts in this cwd may proceed once the file is settled.
                  yield* Effect.ignore(Scope.close(sessionScope, Exit.void));
                  const appended = yield* appendPrimeServiceTierChangeOrWarn(
                    input.threadId,
                    detectedSession.file,
                    serviceTier,
                  );
                  if (appended) {
                    yield* Effect.logInfo("prime.acp.service_tier_restart", {
                      threadId: input.threadId,
                      sessionId: detectedSession.sessionId,
                      from: currentTier,
                      to: serviceTier,
                    });
                  }
                  resumeSessionId = detectedSession.sessionId;
                  resumeFile = detectedSession.file;
                  detectedSession = undefined;
                  yield* releaseSessionLock;
                  sessionScope = yield* Scope.make("sequential");
                  runtime = yield* startPrimeRuntimeWithRetries;
                }
              }
            }
          }
          const { acp, started } = runtime;

          // Only a successfully started child may release the gateway lease
          // on exit; the early exit of a retried attempt or of a fresh child
          // replaced by a fast-mode restart must not tear it down.
          yield* startAgentGatewaySessionLeaseExitWatcher(gatewaySessionLease, acp.awaitExit);

          // The session file this start knows: the resumed file, or the one a
          // fresh start just detected. Its header id is the resume cursor.
          const boundSession: PrimeDetectedSession | undefined =
            resumeSessionId !== undefined && resumeFile !== undefined
              ? { sessionId: resumeSessionId, file: resumeFile }
              : detectedSession;

          const sessionConfigReady = yield* Deferred.make<void>();
          const now = yield* nowIso;
          const session: ProviderSession = {
            provider: PROVIDER,
            status: "ready",
            runtimeMode: input.runtimeMode,
            cwd,
            model: primeModelSelection?.model,
            threadId: input.threadId,
            ...(boundSession !== undefined
              ? { resumeCursor: buildPrimeResumeCursor(boundSession.sessionId) }
              : {}),
            createdAt: now,
            updatedAt: now,
          };

          ctx = {
            threadId: input.threadId,
            lifecycleGeneration: input.lifecycleGeneration,
            session,
            scope: sessionScope,
            acp,
            notificationFiber: undefined,
            turns: [],
            activeInteractionMode: undefined,
            activeTurnId: undefined,
            activeTurnHadAssistantContent: false,
            activeAssistantItemsWithContent: new Set(),
            activeTurnFailedToolDetail: undefined,
            activePromptFiber: undefined,
            activePromptResolved: false,
            lastPlanFingerprint: undefined,
            lastTurnActivityAt: undefined,
            primeToolCallLifecycleById: new Map(),
            sessionUpdatesProcessed: 0,
            sessionConfigReady,
            sessionDetection,
            primeSessionId: boundSession?.sessionId,
            turnStarting: false,
            pendingTurnInterrupted: false,
            compactingThread: false,
            compactionFailedToolDetail: undefined,
            compactionQuietUntil: undefined,
            compactionCancelFiber: undefined,
            latestSessionCostUsd: undefined,
            discoveryTarget,
            sessionUsageTail: undefined,
            pendingSessionUsageEmit: undefined,
            pendingSessionUsageHoldFiber: undefined,
            stopped: false,
            gatewaySessionLease,
          };

          const nf = yield* Stream.runDrain(
            Stream.mapEffect(acp.getEvents(), (event) =>
              Effect.gen(function* () {
                // Only genuine turn-progress events keep the idle watchdog at
                // bay; config/usage heartbeats must not mask a hung turn.
                if (event._tag !== "ToolCallUpdated" && isAcpTurnProgressEventTag(event._tag)) {
                  ctx.lastTurnActivityAt = Date.now();
                }
                switch (event._tag) {
                  case "ModeChanged":
                    return;

                  case "AssistantItemStarted":
                    {
                      const activeTurnId = yield* activeTurnIdForPrimeRuntimeEvent(ctx, event._tag);
                      if (activeTurnId === undefined) {
                        return;
                      }
                      // Content deltas open the visible message; empty starts only add noise.
                    }
                    return;

                  case "AssistantItemCompleted":
                    {
                      const activeTurnId = yield* activeTurnIdForPrimeRuntimeEvent(ctx, event._tag);
                      if (activeTurnId === undefined) {
                        return;
                      }
                      const scopedItemId = scopePrimeRuntimeItemIdForTurn(
                        activeTurnId,
                        event.itemId,
                      );
                      if (!ctx.activeAssistantItemsWithContent.has(scopedItemId)) {
                        if (isPrimeAcpDebugEnabled()) {
                          yield* Effect.logInfo("prime.acp.empty_assistant_item_suppressed", {
                            threadId: ctx.threadId,
                            turnId: activeTurnId,
                            itemId: scopedItemId,
                          });
                        }
                        return;
                      }
                      ctx.activeAssistantItemsWithContent.delete(scopedItemId);
                      yield* offerRuntimeEvent(
                        input.lifecycleGeneration,
                        makeAcpAssistantItemEvent({
                          stamp: yield* makeEventStamp(),
                          provider: PROVIDER,
                          threadId: ctx.threadId,
                          turnId: activeTurnId,
                          itemId: scopedItemId,
                          lifecycle: "item.completed",
                        }),
                      );
                    }
                    return;

                  case "PlanUpdated":
                    {
                      const activeTurnId = yield* activeTurnIdForPrimeRuntimeEvent(ctx, event._tag);
                      if (activeTurnId === undefined) {
                        return;
                      }
                      yield* logNative(ctx.threadId, "session/update", event.rawPayload);
                      yield* emitPlanUpdate(ctx, event.payload, event.rawPayload);
                    }
                    return;

                  case "ToolCallUpdated":
                    {
                      if (ctx.compactingThread) {
                        const failedToolDetail = readAcpFailedToolDetail(event.toolCall);
                        if (failedToolDetail !== undefined) {
                          ctx.compactionFailedToolDetail = failedToolDetail;
                        }
                        return;
                      }
                      const activeTurnId = yield* activeTurnIdForPrimeRuntimeEvent(ctx, event._tag);
                      if (activeTurnId === undefined) {
                        return;
                      }
                      ctx.lastTurnActivityAt = Date.now();
                      updatePrimeToolCallIdleState(ctx, event.toolCall);
                      yield* logNative(ctx.threadId, "session/update", event.rawPayload);
                      const failedToolDetail = readAcpFailedToolDetail(event.toolCall);
                      if (failedToolDetail !== undefined) {
                        ctx.activeTurnFailedToolDetail = failedToolDetail;
                      }
                      yield* offerRuntimeEvent(
                        input.lifecycleGeneration,
                        makeAcpToolCallEvent({
                          stamp: yield* makeEventStamp(),
                          provider: PROVIDER,
                          threadId: ctx.threadId,
                          turnId: activeTurnId,
                          toolCall: scopePrimeToolCallStateForTurn(activeTurnId, event.toolCall),
                          rawPayload: event.rawPayload,
                        }),
                      );
                    }
                    return;

                  case "ContentDelta":
                    {
                      const activeTurnId = yield* activeTurnIdForPrimeRuntimeEvent(ctx, event._tag);
                      if (activeTurnId === undefined) {
                        return;
                      }
                      yield* logNative(ctx.threadId, "session/update", event.rawPayload);
                      const scopedItemId = event.itemId
                        ? scopePrimeRuntimeItemIdForTurn(activeTurnId, event.itemId)
                        : undefined;
                      if (isRenderablePrimeAssistantDelta(event)) {
                        ctx.activeTurnHadAssistantContent = true;
                        if (scopedItemId !== undefined) {
                          ctx.activeAssistantItemsWithContent.add(scopedItemId);
                        }
                      }
                      yield* offerRuntimeEvent(
                        input.lifecycleGeneration,
                        makeAcpContentDeltaEvent({
                          stamp: yield* makeEventStamp(),
                          provider: PROVIDER,
                          threadId: ctx.threadId,
                          turnId: activeTurnId,
                          ...(scopedItemId ? { itemId: scopedItemId } : {}),
                          text: event.text,
                          ...(event.streamKind ? { streamKind: event.streamKind } : {}),
                          rawPayload: event.rawPayload,
                        }),
                      );
                    }
                    return;

                  case "UsageUpdated":
                    {
                      const activeTurnId = yield* activeTurnIdForPrimeRuntimeEvent(ctx, event._tag);
                      if (activeTurnId === undefined) {
                        return;
                      }
                      yield* logNative(ctx.threadId, "session/update", event.rawPayload);
                      recordAcpSessionCost(ctx, event.cost);
                      yield* offerRuntimeEvent(
                        input.lifecycleGeneration,
                        makeAcpTokenUsageEvent({
                          stamp: yield* makeEventStamp(),
                          provider: PROVIDER,
                          threadId: ctx.threadId,
                          turnId: activeTurnId,
                          usage: event.usage,
                          method: "session/update",
                          rawPayload: event.rawPayload,
                        }),
                      );
                    }
                    return;
                }
              }).pipe(
                // Bump the processed count only after the handler fully ran, so
                // waitForPrimeQueuedTurnEventsDrained cannot observe an event as
                // consumed while its state updates are still being applied.
                Effect.ensuring(
                  Effect.sync(() => {
                    ctx.sessionUpdatesProcessed += 1;
                    options?.onSessionUpdateProcessed?.();
                  }),
                ),
              ),
            ),
            // The drain's lifetime is the session's, not the caller's: forking it as
            // a child of the fiber that called startSession kills it as soon as that
            // fiber returns, silently dropping every session/update.
          ).pipe(Effect.forkIn(sessionScope));

          ctx.notificationFiber = nf;
          sessions.set(input.threadId, ctx);
          sessionScopeTransferred = true;

          // Startup finalization runs after the consumer fork. The session is
          // already registered and the start-scope finalizer no longer owns the
          // session scope, so any failure OR interruption of the remaining
          // startup steps must tear the session down explicitly.
          yield* Effect.gen(function* () {
            // Startup configuration has settled; turns gated on this deferred
            // can now prompt. Prime model options are process-start settings.
            yield* Deferred.succeed(sessionConfigReady, undefined);
            ctx.sessionConfigReady = undefined;

            yield* offerRuntimeEvent(input.lifecycleGeneration, {
              type: "session.started",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              payload: { resume: started.initializeResult },
            });
            yield* offerRuntimeEvent(input.lifecycleGeneration, {
              type: "session.state.changed",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              payload: { state: "ready", reason: "Prime Agent ACP session ready" },
            });
            yield* offerRuntimeEvent(input.lifecycleGeneration, {
              type: "thread.started",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              // The ACP session id is per-process; the Prime session id is the
              // durable identity a later --resume relaunch reopens.
              payload: { providerThreadId: ctx.primeSessionId ?? started.sessionId },
            });
            // The bound file's catch-up read reports the thread's current
            // context right after thread.started (nothing yet for a fresh file).
            if (boundSession !== undefined) {
              yield* bindPrimeSessionFile(ctx, boundSession);
            }
          }).pipe(
            Effect.onExit((exit) =>
              Exit.isSuccess(exit) ? Effect.void : Effect.ignore(stopSessionInternal(ctx)),
            ),
          );

          return ctx.session;
        }).pipe(Effect.scoped),
      );

    // A resumed start that fell back to a fresh session (missing session file)
    // publishes a new Prime session id; report it so the orchestration
    // rebuilds prior context instead of assuming native continuity.
    const didResumeSession: NonNullable<PrimeAdapterShape["didResumeSession"]> = (
      input,
      session,
    ) => {
      const requested = parsePrimeResume(input.resumeCursor)?.sessionId;
      const actual = parsePrimeResume(session.resumeCursor)?.sessionId;
      return requested !== undefined && requested === actual;
    };

    // Idle-progress watchdog escape hatch: force-fail a turn whose prime child
    // is alive but has gone completely silent. Mirrors the prompt-fiber
    // onFailure branch and stays idempotent via settlePrimeActiveTurn.
    const failPrimeTurnAsTimedOut = (ctx: PrimeSessionContext, turnId: TurnId, idleMs: number) =>
      Effect.gen(function* () {
        const promptFiber = ctx.activePromptFiber;
        if (ctx.activeTurnId !== turnId) {
          return;
        }
        yield* cancelAgentGatewayTurn(ctx.gatewaySessionLease, turnId);
        if (!settlePrimeActiveTurn(ctx, turnId)) {
          return;
        }
        const completedCost = finalizeAcpActiveTurnCost(ctx);
        const idleSeconds = Math.round(idleMs / 1000);
        const detail = `Prime Agent stopped responding (no activity for ${idleSeconds}s); the turn was timed out.`;
        ctx.turns.push({
          id: turnId,
          items: [{ prompt: turnId, timedOut: true, idleMs }],
        });
        ctx.session = {
          ...ctx.session,
          status: "error",
          updatedAt: yield* nowIso,
          lastError: detail,
        };
        yield* Effect.logWarning("prime.acp.turn_idle_timeout", {
          threadId: ctx.threadId,
          turnId,
          idleMs,
        });
        yield* offerRuntimeEvent(ctx.lifecycleGeneration, {
          type: "turn.completed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          turnId,
          payload: {
            state: "failed",
            stopReason: null,
            errorMessage: detail,
            ...completedCost,
          },
        });
        // Best-effort: tell the child to abandon the turn, then unwind the
        // pending prompt fiber. The cancel is forked, not awaited — a hung
        // session/cancel must not block the interrupt or leak the watchdog.
        yield* Effect.ignore(ctx.acp.cancel).pipe(Effect.forkIn(ctx.scope));
        if (promptFiber) {
          yield* Fiber.interrupt(promptFiber);
        }
      });

    const sendTurn: PrimeAdapterShape["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(input.threadId);
        // compactThread holds the thread lock but sendTurn intentionally does not
        // (turns are long-running); reject instead of racing a second prompt whose
        // events the compaction suppression would silently drop.
        if (ctx.compactingThread) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Cannot start a turn while Prime Agent context compaction is in progress.",
          });
        }
        if (ctx.turnStarting) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Another Prime Agent turn is still starting for this thread.",
          });
        }
        if (ctx.activeTurnId !== undefined) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Another Prime Agent turn is already active for this thread.",
          });
        }
        ctx.turnStarting = true;
        ctx.pendingTurnInterrupted = false;
        return yield* startPrimeTurn(ctx, input).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              ctx.turnStarting = false;
            }),
          ),
        );
      });

    const startPrimeTurn = (
      ctx: PrimeSessionContext,
      input: Parameters<PrimeAdapterShape["sendTurn"]>[0],
    ) =>
      Effect.gen(function* () {
        // Startup registers the session before post-registration setup settles;
        // a turn routed in during that window must wait for setup to finish.
        if (ctx.sessionConfigReady !== undefined) {
          yield* Deferred.await(ctx.sessionConfigReady);
        }
        yield* waitForAbandonedPrimeCompaction(ctx);
        // The gate above is resolved by stopSessionInternal too; a turn that
        // was blocked on it must fail here instead of emitting lifecycle
        // events for a dead session.
        if (ctx.stopped) {
          return yield* new ProviderAdapterSessionNotFoundError({
            provider: PROVIDER,
            threadId: input.threadId,
          });
        }
        const turnId = TurnId.makeUnsafe(crypto.randomUUID());
        const model =
          input.modelSelection?.provider === PROVIDER ? input.modelSelection.model : undefined;
        // Model and thinking level ride the process-start `--model` flag;
        // plan mode is a prompt-prefix contract because Prime has no modes.
        const interactionMode = resolveAcpTurnInteractionMode(input.interactionMode);

        const promptParts = yield* buildPrimePromptParts({
          text: input.input,
          attachments: input.attachments,
          attachmentsDir: serverConfig.attachmentsDir,
          interactionMode,
          fileSystem,
        });

        if (promptParts.length === 0) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Turn requires non-empty text or attachments.",
          });
        }

        const harnessPolicy = takeSynaraHarnessPolicyTextPartForProviderSession(ctx, {
          provider: PROVIDER,
          scopedGatewayConnectionAvailable: ctx.gatewaySessionLease !== undefined,
        });
        if (harnessPolicy) {
          promptParts.unshift(harnessPolicy);
        }

        // A stop can land while the pre-prompt work or attachment reads above
        // were in flight; opening the turn now would publish turn.started for
        // a session that already exited.
        if (ctx.stopped) {
          return yield* new ProviderAdapterSessionNotFoundError({
            provider: PROVIDER,
            threadId: input.threadId,
          });
        }
        ctx.activeTurnId = turnId;
        clearPrimeActiveToolCallIdleState(ctx);
        ctx.activeTurnHadAssistantContent = false;
        ctx.activeAssistantItemsWithContent.clear();
        ctx.activeTurnFailedToolDetail = undefined;
        // A new turn starts with an unresolved prompt; a late interrupt must be
        // free to cancel it until ctx.acp.prompt actually returns.
        ctx.activePromptResolved = false;
        ctx.activeInteractionMode = interactionMode;
        ctx.lastPlanFingerprint = undefined;
        ctx.lastTurnActivityAt = Date.now();

        const { lastError: _lastError, ...sessionWithoutLastError } = ctx.session;
        ctx.session = {
          ...sessionWithoutLastError,
          status: "running",
          activeTurnId: turnId,
          updatedAt: yield* nowIso,
          ...(model ? { model } : {}),
        };

        yield* offerRuntimeEvent(ctx.lifecycleGeneration, {
          type: "turn.started",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: input.threadId,
          turnId,
          payload: model ? { model } : {},
        });

        const runPrompt = Effect.suspend(() =>
          // interruptTurn during the pre-prompt waits or between turn.started
          // publishing and this fiber being registered sets
          // pendingTurnInterrupted; honor it (and a concurrent stop) here so a
          // cancelled turn is never prompted. Self-interrupting routes through
          // the onInterrupt branch below.
          ctx.pendingTurnInterrupted || ctx.stopped
            ? Effect.interrupt
            : ctx.acp.prompt({ prompt: promptParts }),
        ).pipe(
          Effect.mapError((error) =>
            mapAcpToAdapterError(PROVIDER, input.threadId, "session/prompt", error),
          ),
          Effect.matchEffect({
            onFailure: (error) =>
              Effect.gen(function* () {
                if (ctx.activeTurnId !== turnId) return;
                ctx.activePromptResolved = true;
                yield* waitForPrimeQueuedTurnEventsDrained(ctx);
                yield* flushPrimeSessionUsage(ctx);
                yield* cancelAgentGatewayTurn(ctx.gatewaySessionLease, turnId);
                if (!settlePrimeActiveTurn(ctx, turnId)) return;
                const completedCost = finalizeAcpActiveTurnCost(ctx);
                ctx.turns.push({ id: turnId, items: [{ prompt: promptParts, error }] });
                const detail = error.message;
                ctx.session = {
                  ...ctx.session,
                  status: "error",
                  updatedAt: yield* nowIso,
                  ...(model ? { model } : {}),
                  lastError: detail,
                };
                yield* offerRuntimeEvent(ctx.lifecycleGeneration, {
                  type: "turn.completed",
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId,
                  payload: {
                    state: "failed",
                    stopReason: null,
                    errorMessage: detail,
                    ...completedCost,
                  },
                });
                // Transport/prompt failures make the ACP child unusable. Remove
                // it from routing immediately so ProviderService can recover on
                // the next send instead of reusing a dead session forever.
                yield* stopSessionInternal(ctx);
              }),
            onSuccess: (result) =>
              Effect.gen(function* () {
                if (ctx.activeTurnId !== turnId) return;
                ctx.activePromptResolved = true;
                // Drain BEFORE snapshotting turn state: queued events may still
                // set activeTurnFailedToolDetail or assistant-content flags.
                yield* waitForPrimeQueuedTurnEventsDrained(ctx);
                // Prime writes the turn's last assistant usage to the session
                // file before answering the prompt; read it now so the final
                // snapshot carries this turn id and the cost precedes
                // finalizeAcpActiveTurnCost below.
                yield* flushPrimeSessionUsage(ctx);
                const hadAssistantContent = ctx.activeTurnHadAssistantContent;
                const failedToolDetail = ctx.activeTurnFailedToolDetail;
                yield* cancelAgentGatewayTurn(ctx.gatewaySessionLease, turnId);
                if (!settlePrimeActiveTurn(ctx, turnId)) return;
                const completedCost = finalizeAcpActiveTurnCost(ctx);
                ctx.turns.push({ id: turnId, items: [{ prompt: promptParts, result }] });
                const { lastError: _lastError, ...sessionWithoutLastError } = ctx.session;
                ctx.session = {
                  ...sessionWithoutLastError,
                  status: "ready",
                  updatedAt: yield* nowIso,
                  ...(model ? { model } : {}),
                };
                if (!hadAssistantContent && result.stopReason !== "cancelled") {
                  yield* Effect.logWarning("prime.acp.turn_completed_without_content", {
                    threadId: input.threadId,
                    turnId,
                    stopReason: result.stopReason ?? null,
                    hasUsage: result.usage !== undefined,
                  });
                }
                // A fresh start that could not locate Prime's session file
                // retries now: the file has had a whole turn to appear. The
                // session lock keeps the retry from overlapping another fresh
                // start's snapshot -> spawn -> detect window in the same cwd.
                const pendingDetection = ctx.sessionDetection;
                const detectionCwd = ctx.session.cwd;
                if (pendingDetection !== undefined && detectionCwd !== undefined) {
                  yield* withPrimeSessionLock(
                    pendingDetection.lockKey,
                    Effect.gen(function* () {
                      const detected = yield* detectPrimeSessionFile({
                        threadId: ctx.threadId,
                        cwd: detectionCwd,
                        detection: pendingDetection,
                        timeoutMs: PRIME_SESSION_DETECT_RETRY_TIMEOUT_MS,
                      });
                      if (detected !== undefined && !ctx.stopped) {
                        yield* bindPrimeSessionFile(ctx, detected);
                      }
                    }),
                  );
                }
                const completion = classifyAcpPromptTurnCompletion({
                  stopReason: result.stopReason,
                  ...(failedToolDetail !== undefined ? { failedToolDetail } : {}),
                });
                yield* offerRuntimeEvent(ctx.lifecycleGeneration, {
                  type: "turn.completed",
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId,
                  payload: {
                    state: completion.state,
                    stopReason: result.stopReason ?? null,
                    ...(completion.errorMessage !== undefined
                      ? { errorMessage: completion.errorMessage }
                      : {}),
                    ...(result.usage ? { usage: result.usage } : {}),
                    ...completedCost,
                  },
                });
              }),
          }),
          Effect.onInterrupt(() =>
            Effect.gen(function* () {
              // User interruption leaves a resolved prompt fiber alive. If
              // teardown interrupts it while the turn remains active, settle
              // it here before session.exited.
              if (!settlePrimeActiveTurn(ctx, turnId)) return;
              const completedCost = finalizeAcpActiveTurnCost(ctx);
              ctx.turns.push({
                id: turnId,
                items: [{ prompt: promptParts, interrupted: true }],
              });
              const { lastError: _lastError, ...sessionWithoutLastError } = ctx.session;
              ctx.session = {
                ...sessionWithoutLastError,
                status: "ready",
                updatedAt: yield* nowIso,
                ...(model ? { model } : {}),
              };
              yield* offerRuntimeEvent(ctx.lifecycleGeneration, {
                type: "turn.completed",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: input.threadId,
                turnId,
                payload: {
                  state: "cancelled",
                  stopReason: "cancelled",
                  ...completedCost,
                },
              });
            }),
          ),
          Effect.ignoreCause({ log: true }),
          Effect.forkIn(ctx.scope),
        );

        ctx.activePromptFiber = yield* runPrompt;

        // Backstop the forked prompt: if the child goes silent, fail the turn
        // instead of leaving it "Working" forever. Self-terminates when the
        // turn settles (Prime never pauses on a human approval).
        yield* forkAcpAdapterTurnIdleWatchdog({
          context: ctx,
          turnId,
          idleTimeoutMs: timeouts.turnIdleMs,
          currentIdleTimeoutMs: () => resolvePrimeCurrentIdleTimeoutMs(ctx, timeouts),
          checkIntervalMs: watchdogIntervalMs,
          onIdleTimeout: (idleMs) => failPrimeTurnAsTimedOut(ctx, turnId, idleMs),
        });

        return {
          threadId: input.threadId,
          turnId,
          resumeCursor: ctx.session.resumeCursor,
        };
      });

    const interruptTurn: PrimeAdapterShape["interruptTurn"] = (threadId, turnId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        if (turnId !== undefined && turnId !== ctx.activeTurnId) {
          yield* Effect.logWarning("prime.acp.stale_interrupt_ignored", {
            threadId,
            requestedTurnId: turnId,
            activeTurnId: ctx.activeTurnId,
          });
          return;
        }
        const activeTurnId = turnId ?? ctx.activeTurnId;
        // A turn that is still starting has no prompt fiber to interrupt yet;
        // flag it so startPrimeTurn aborts before prompting.
        if (ctx.turnStarting && ctx.activePromptFiber === undefined) {
          ctx.pendingTurnInterrupted = true;
        }
        yield* withAgentGatewayTurnCancellation(
          ctx.gatewaySessionLease,
          activeTurnId,
          Effect.gen(function* () {
            const activePromptFiber = ctx.activePromptFiber;
            yield* Effect.ignore(
              ctx.acp.cancel.pipe(
                Effect.mapError((error) =>
                  mapAcpToAdapterError(PROVIDER, threadId, "session/cancel", error),
                ),
              ),
            );
            // A resolved prompt is already draining or settling its result.
            // Leave that fiber alive so onInterrupt cannot reclassify it.
            if (activePromptFiber !== undefined && !ctx.activePromptResolved) {
              yield* Fiber.interrupt(activePromptFiber);
            }
          }),
        );
      });

    // Prime never opens approval or user-input requests (see the fail-closed
    // responders in startSession), so there is never a pending request to answer.
    const respondToRequest: PrimeAdapterShape["respondToRequest"] = (threadId, requestId) =>
      Effect.gen(function* () {
        yield* requireSession(threadId);
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session/request_permission",
          detail: `Unknown pending approval request: ${requestId}`,
        });
      });

    const respondToUserInput: PrimeAdapterShape["respondToUserInput"] = (threadId, requestId) =>
      Effect.gen(function* () {
        yield* requireSession(threadId);
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session/elicitation",
          detail: `Unknown pending user-input request: ${requestId}`,
        });
      });

    const readThread: PrimeAdapterShape["readThread"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        return {
          threadId,
          turns: ctx.turns,
          cwd: ctx.session.cwd ?? null,
        } satisfies ProviderThreadSnapshot;
      });

    const rollbackThread: PrimeAdapterShape["rollbackThread"] = (threadId, _numTurns) =>
      Effect.gen(function* () {
        yield* requireSession(threadId);
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "rollbackThread",
          issue: "Prime Agent does not support conversation rollback.",
        });
      });

    const stopSession: PrimeAdapterShape["stopSession"] = (threadId) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = sessions.get(threadId);
          if (!ctx) return;
          yield* stopSessionInternal(ctx);
        }),
      );

    const listSessions: PrimeAdapterShape["listSessions"] = () =>
      Effect.sync(() => Array.from(sessions.values(), (c) => ({ ...c.session })));

    const hasSession: PrimeAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => {
        const c = sessions.get(threadId);
        return c !== undefined && !c.stopped;
      });

    const getComposerCapabilities: NonNullable<PrimeAdapterShape["getComposerCapabilities"]> = () =>
      Effect.succeed({
        provider: PROVIDER,
        supportsSkillMentions: true,
        supportsSkillDiscovery: true,
        supportsNativeSlashCommandDiscovery: true,
        supportsPluginMentions: false,
        supportsPluginDiscovery: false,
        supportsRuntimeModelList: true,
        supportsThreadCompaction: true,
        supportsThreadImport: false,
      } satisfies ProviderComposerCapabilities);

    // Commands and skills come from the same `get_commands` RPC answer, so one
    // cache entry (keyed by binary, agent dir, and cwd) backs both listings.
    const discoverCommandDescriptors = (
      input: {
        readonly cwd?: string | undefined;
        readonly binaryPath?: string | undefined;
        readonly agentDir?: string | undefined;
        readonly forceReload?: boolean | undefined;
      },
      method: "command/list" | "skill/list",
    ) => {
      const cwd = resolveAcpSessionCwd({
        inputCwd: input.cwd,
        serverCwd: serverConfig.cwd,
        homeDir: serverConfig.homeDir,
      });
      const target = resolveDiscoveryTarget(input);
      const cacheKey =
        cwd === undefined ? undefined : `${primeDiscoveryCacheKey(target)}\u0000${cwd}`;
      const cached = cacheKey === undefined ? undefined : commandDiscoveryCache.get(cacheKey);
      // Fast path: serve a fresh cached result without serializing behind the
      // discovery lock.
      if (
        cacheKey !== undefined &&
        input.forceReload !== true &&
        cached &&
        cached.expiresAt > Date.now()
      ) {
        return Effect.succeed({ descriptors: cached.descriptors, cached: true });
      }
      const operation = method === "command/list" ? "listCommands" : "listSkills";
      const subject = method === "command/list" ? "commands" : "skills";
      return discoveryLock.withPermits(1)(
        Effect.gen(function* () {
          if (cwd === undefined || cacheKey === undefined) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation,
              issue: "cwd is required and no server cwd fallback is available.",
            });
          }
          // Recheck under the lock: a concurrent discovery may have populated
          // the cache while this fiber waited for the permit.
          const cached = commandDiscoveryCache.get(cacheKey);
          if (input.forceReload !== true && cached && cached.expiresAt > Date.now()) {
            return { descriptors: cached.descriptors, cached: true };
          }

          // Project-local skills are resolved relative to the RPC cwd.
          const discovery = yield* runRpcDiscovery({
            childProcessSpawner,
            binaryPath: target.binaryPath,
            agentDir: target.agentDir,
            cwd,
            requests: [{ id: PRIME_RPC_COMMANDS_REQUEST_ID, type: "get_commands" }],
          });
          const commandsResponse = discovery.responses.get(PRIME_RPC_COMMANDS_REQUEST_ID);
          if (commandsResponse === undefined || commandsResponse.success === false) {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method,
              detail: describePrimeDiscoveryFailure(
                target.binaryPath,
                discovery,
                commandsResponse?.error ??
                  `'${target.binaryPath} --mode rpc' did not answer get_commands.`,
              ),
            });
          }
          const descriptors = parsePrimeCommands(commandsResponse.data);
          setPrimeDiscoveryCacheEntry(commandDiscoveryCache, cacheKey, {
            expiresAt: Date.now() + PRIME_COMMAND_DISCOVERY_CACHE_MS,
            descriptors,
          });
          return { descriptors, cached: false };
        }).pipe(
          Effect.scoped,
          Effect.mapError((cause) =>
            cause instanceof ProviderAdapterValidationError ||
            cause instanceof ProviderAdapterRequestError
              ? cause
              : new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method,
                  detail: redactPrimeDiscoveryError(cause),
                }),
          ),
          Effect.timeoutOption(PRIME_COMMAND_DISCOVERY_TIMEOUT_MS),
          Effect.flatMap(
            Option.match({
              onNone: () =>
                Effect.fail(
                  new ProviderAdapterRequestError({
                    provider: PROVIDER,
                    method,
                    detail: `Timed out while discovering Prime Agent ${subject} via RPC.`,
                  }),
                ),
              onSome: (result) => Effect.succeed(result),
            }),
          ),
        ),
      );
    };

    const listCommands: NonNullable<PrimeAdapterShape["listCommands"]> = (
      input: ProviderListCommandsInput,
    ) =>
      Effect.map(
        discoverCommandDescriptors(input, "command/list"),
        ({ descriptors, cached }) =>
          ({
            commands: mapPrimeCommands(descriptors),
            source: "prime-rpc",
            cached,
          }) satisfies ProviderListCommandsResult,
      );

    // Built-in Prime skills live inside the prime-agent package, so only this
    // native listing surfaces them; the Synara catalog merges in the user and
    // project skill directories on top.
    const listSkills: NonNullable<PrimeAdapterShape["listSkills"]> = (
      input: ProviderListSkillsInput,
    ) =>
      Effect.map(
        discoverCommandDescriptors(input, "skill/list"),
        ({ descriptors, cached }) =>
          ({
            skills: mapPrimeSkills(descriptors),
            source: "prime-rpc",
            cached,
          }) satisfies ProviderListSkillsResult,
      );

    const compactThread: NonNullable<PrimeAdapterShape["compactThread"]> = (threadId) =>
      Effect.gen(function* () {
        // Wait for startup setup before taking the thread lock: stopSession
        // and startSession need that lock, and stopping the session is what
        // resolves the deferred early.
        const preLockCtx = yield* requireSession(threadId);
        if (preLockCtx.sessionConfigReady !== undefined) {
          yield* Deferred.await(preLockCtx.sessionConfigReady);
        }
        // Claim the compaction slot under the thread lock, but run the
        // (potentially long) /compact prompt outside it: a hung compaction
        // must never block stopSessionInternal from killing the child.
        const ctx = yield* withThreadLock(threadId, claimPrimeCompactionSlot(threadId, preLockCtx));
        return yield* runPrimeCompaction(ctx).pipe(
          // compactingThread stays set until this clears it: sendTurn only
          // rejects while the flag is true. The terminal compaction event is
          // out by now, so usage the tail held back during the compaction can
          // follow it (the meter reads the reduced context immediately).
          Effect.ensuring(
            Effect.suspend(() => {
              ctx.compactingThread = false;
              return flushPendingPrimeSessionUsage(ctx);
            }),
          ),
        );
      });

    const claimPrimeCompactionSlot = (threadId: ThreadId, preLockCtx: PrimeSessionContext) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        // The pre-lock wait resolves early when the session is stopped; if a
        // restart won the lock first, this thread id now maps to a fresh
        // session that the original compaction request never targeted.
        if (ctx !== preLockCtx) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "compactThread",
            issue:
              "The Prime Agent session was restarted while waiting to compact; retry once it settles.",
          });
        }
        // The prompt runs outside the thread lock, so a concurrent /compact can
        // reach this point while one is already in flight; reject it here.
        if (ctx.compactingThread) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "compactThread",
            issue: "A Prime Agent context compaction is already in progress.",
          });
        }
        // turnStarting covers a sendTurn that is past its compaction check but
        // has not assigned ctx.activeTurnId yet.
        if (ctx.activeTurnId !== undefined || ctx.turnStarting) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "compactThread",
            issue: "Cannot compact while a Prime Agent turn is still active.",
          });
        }
        ctx.compactingThread = true;
        ctx.compactionFailedToolDetail = undefined;
        return ctx;
      });

    // Every compaction failure path records the same terminal failed event
    // and surfaces the same request error; only the title/detail differ.
    const failPrimeCompaction = (
      ctx: PrimeSessionContext,
      itemId: RuntimeItemId,
      title: string,
      detail: string,
    ) =>
      Effect.gen(function* () {
        yield* emitPrimeContextCompactionRuntimeEvent(ctx, {
          itemId,
          lifecycle: "item.completed",
          status: "failed",
          title,
          detail,
        });
        return yield* Effect.fail(
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "session/prompt",
            detail,
          }),
        );
      });

    const runPrimeCompaction = (ctx: PrimeSessionContext) =>
      Effect.gen(function* () {
        // A previous timed-out /compact may still be cancelling; preserve the
        // same ordering requirement as new turns.
        yield* waitForAbandonedPrimeCompaction(ctx);
        const itemId = makePrimeCompactionItemId();
        yield* emitPrimeContextCompactionRuntimeEvent(ctx, {
          itemId,
          lifecycle: "item.updated",
          status: "inProgress",
          title: "Compacting context",
        });

        const compactResult = yield* runPrimeAcpCompactionCommand(ctx.acp).pipe(
          Effect.mapError((error) =>
            mapAcpToAdapterError(PROVIDER, ctx.threadId, "session/prompt", error),
          ),
          Effect.timeoutOption(timeouts.turnIdleMs),
          Effect.exit,
        );

        if (Exit.isFailure(compactResult)) {
          // Interruption (session stopping) is not a compaction failure; let it unwind.
          if (Cause.hasInterruptsOnly(compactResult.cause)) {
            return yield* Effect.failCause(compactResult.cause);
          }
          return yield* failPrimeCompaction(
            ctx,
            itemId,
            "Context compaction failed",
            describeCause(compactResult.cause),
          );
        }

        const promptResponse = Option.getOrUndefined(compactResult.value);
        if (promptResponse === undefined) {
          // Timed out: tell the child to abandon the prompt (best effort) and
          // surface the failure instead of leaving compactingThread wedged.
          // The cancel is forked, not awaited: the child just proved it can go
          // silent, and a hung session/cancel would wedge the flag forever.
          ctx.compactionQuietUntil = Date.now() + PRIME_COMPACT_ABANDON_QUIET_MS;
          ctx.compactionCancelFiber = yield* Effect.ignore(ctx.acp.cancel).pipe(
            Effect.forkIn(ctx.scope),
          );
          const detail = `Prime Agent did not finish context compaction within ${Math.round(timeouts.turnIdleMs / 1000)}s; the compaction was abandoned.`;
          yield* Effect.logWarning("prime.acp.compact_timeout", {
            threadId: ctx.threadId,
            timeoutMs: timeouts.turnIdleMs,
          });
          return yield* failPrimeCompaction(ctx, itemId, "Context compaction timed out", detail);
        }

        // The failed-tool detail below is recorded by the notification
        // consumer, which can lag the prompt response; wait for inbound
        // activity to go quiet before deciding the outcome.
        yield* settlePrimeCompactionOutcome(ctx);

        // ACP can answer a /compact prompt successfully with stopReason
        // "cancelled" (user interrupt via session/cancel); that is not a
        // completed compaction and must not be persisted as one.
        if (promptResponse.stopReason === "cancelled") {
          const detail = "Prime Agent context compaction was cancelled before it completed.";
          return yield* failPrimeCompaction(ctx, itemId, "Context compaction cancelled", detail);
        }

        const failedToolDetail = ctx.compactionFailedToolDetail;
        if (failedToolDetail !== undefined) {
          return yield* failPrimeCompaction(
            ctx,
            itemId,
            "Context compaction failed",
            failedToolDetail,
          );
        }

        // Success: thread.state.changed is the single terminal signal —
        // ingestion projects it into the "Context compacted manually" row.
        yield* offerRuntimeEvent(ctx.lifecycleGeneration, {
          type: "thread.state.changed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: {
            state: "compacted",
            detail: { reason: "provider.compactThread" },
          },
        });
      });

    const listModels: NonNullable<PrimeAdapterShape["listModels"]> = (input) =>
      discoverPrimeModels(resolveDiscoveryTarget(input));

    const stopAll: PrimeAdapterShape["stopAll"] = () =>
      Effect.forEach(Array.from(sessions.values()), stopSessionInternal, { discard: true });

    yield* Effect.addFinalizer(() =>
      Effect.forEach(Array.from(sessions.values()), stopSessionInternal, {
        discard: true,
      }).pipe(
        Effect.tap(() => PubSub.shutdown(runtimeEventPubSub)),
        Effect.tap(() => managedNativeEventLogger?.close() ?? Effect.void),
      ),
    );

    const streamEvents = Stream.fromPubSub(runtimeEventPubSub);

    return {
      provider: PROVIDER,
      capabilities: {
        sessionModelSwitch: "restart-session",
        conversationRollback: "restart-session",
        supportsRuntimeModelList: true,
      },
      startSession,
      didResumeSession,
      sendTurn,
      interruptTurn,
      readThread,
      rollbackThread,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      getComposerCapabilities,
      listCommands,
      listSkills,
      compactThread,
      listModels,
      hasSession,
      stopAll,
      streamEvents,
    } satisfies PrimeAdapterShape;
  });
}

export const PrimeAdapterLive = Layer.effect(PrimeAdapter, makePrimeAdapter());

export function makePrimeAdapterLive(
  primeSettings: PrimeAcpRuntimeSettings = {},
  options?: PrimeAdapterLiveOptions,
) {
  return Layer.effect(PrimeAdapter, makePrimeAdapter(primeSettings, options));
}
