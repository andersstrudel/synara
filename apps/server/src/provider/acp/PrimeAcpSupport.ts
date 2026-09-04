/**
 * Prime Agent ACP support - builds the `prime-agent --mode acp` stdio command,
 * runs RPC-mode discovery, and locates Prime's on-disk session files.
 *
 * Prime Agent speaks the Agent Client Protocol natively and owns its own
 * credential store (`<agentDir>/auth.json`), so no auth method is negotiated
 * over ACP. Like Pi, its model registry also resolves upstream API keys from
 * the environment (ANTHROPIC_API_KEY, OPENAI_API_KEY, ...), so the child
 * inherits the host's provider credentials.
 *
 * @module PrimeAcpSupport
 */
import { open, readdir, readFile, realpath, stat } from "node:fs/promises";
import * as nodeOs from "node:os";
import * as nodePath from "node:path";

import {
  PRIME_THINKING_LEVEL_OPTIONS,
  type PrimeThinkingLevel,
  type ProviderListCommandsResult,
  type ProviderListSkillsResult,
  type ProviderModelDescriptor,
  type RuntimeMode,
} from "@synara/contracts";
import { Effect, Layer, PlatformError, Scope, ServiceMap, Stream } from "effect";
import * as AcpErrors from "./AcpErrors.ts";
import type * as Acp from "@agentclientprotocol/sdk";
import { ChildProcessSpawner } from "effect/unstable/process";
import { makeEffectProcessCommand } from "../../platform/effectProcessRuntime.ts";

import { buildProviderChildEnvironment } from "../../providerChildEnvironment.ts";
import { collectUint8StreamText } from "../../stream/collectUint8StreamText.ts";
import { PI_THINKING_LEVEL_DESCRIPTORS } from "../piThinkingLevels.ts";
import {
  AcpSessionRuntime,
  type AcpSessionRuntimeOptions,
  type AcpSessionRuntimeShape,
  type AcpSpawnInput,
} from "./AcpSessionRuntime.ts";

export interface PrimeAcpRuntimeSettings {
  readonly binaryPath?: string;
  readonly agentDir?: string;
}

export interface PrimeAcpSpawnOptions {
  /** `provider/id[:thinkingLevel]`, passed as the process-start `--model` flag. */
  readonly model?: string;
  /** Prime session header id (or absolute session file path) for `--resume`. */
  readonly resumeSessionId?: string;
}

export interface PrimeAcpRuntimeInput extends Omit<
  AcpSessionRuntimeOptions,
  "authMethodId" | "resolveAuthMethodId" | "spawn"
> {
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly primeSettings: PrimeAcpRuntimeSettings | null | undefined;
  readonly runtimeMode: RuntimeMode;
  readonly spawnOptions?: PrimeAcpSpawnOptions;
  readonly sessionConfig?: { readonly childEnvironment: NodeJS.ProcessEnv };
}

export const PRIME_DEFAULT_BINARY = "prime-agent";
export const PRIME_AGENT_DIR_ENV = "PRIME_AGENT_CODING_AGENT_DIR";
export const PRIME_SESSION_DIR_ENV = "PRIME_AGENT_SESSION_DIR";
export const PRIME_LOGIN_GUIDANCE = "Run `prime-agent` and use /login to add a provider.";
const PRIME_COMPACT_COMMAND_NAME = "compact";
const PRIME_COMPACT_COMMAND_DESCRIPTION = "Compact the conversation context";
const PRIME_COMPACT_PROMPT = "/compact";
const PRIME_SESSION_FILE_EXTENSION = ".jsonl";
const PRIME_SESSION_HEADER_MAX_BYTES = 8_192;
const PRIME_SESSION_DETECT_POLL_MS = 100;
// Session files are stamped by Prime's clock; tolerate small skew between
// the header timestamp and the host's spawn timestamp.
const PRIME_SESSION_DETECT_CLOCK_SKEW_MS = 2_000;

// Mirrors prime-agent's own `BUILT_IN_PROVIDER_DISPLAY_NAMES`
// (dist/core/provider-display-names.js) so picker group headers and the
// health auth label read the way Prime's picker does. `openai-codex` is not
// in that table (Prime labels it through its OAuth provider entry, "ChatGPT
// Plus/Pro (Codex Subscription)"); the short form below keeps it distinct
// from the API-key `openai` provider.
const PRIME_UPSTREAM_PROVIDER_NAMES: Readonly<Record<string, string>> = {
  anthropic: "Anthropic",
  "amazon-bedrock": "Amazon Bedrock",
  "azure-openai-responses": "Azure OpenAI Responses",
  cerebras: "Cerebras",
  "cloudflare-ai-gateway": "Cloudflare AI Gateway",
  "cloudflare-workers-ai": "Cloudflare Workers AI",
  deepseek: "DeepSeek",
  fireworks: "Fireworks",
  google: "Google Gemini",
  "google-vertex": "Google Vertex AI",
  groq: "Groq",
  huggingface: "Hugging Face",
  "kimi-coding": "Kimi For Coding",
  mistral: "Mistral",
  minimax: "MiniMax",
  "minimax-cn": "MiniMax (China)",
  moonshotai: "Moonshot AI",
  "moonshotai-cn": "Moonshot AI (China)",
  opencode: "OpenCode Zen",
  "opencode-go": "OpenCode Go",
  openai: "OpenAI",
  "openai-codex": "OpenAI Codex",
  openrouter: "OpenRouter",
  "prime-agent-traces": "Prime Agent Traces",
  "prime-inference": "Prime Inference",
  "vercel-ai-gateway": "Vercel AI Gateway",
  xai: "xAI",
  zai: "ZAI",
  xiaomi: "Xiaomi MiMo",
  "xiaomi-token-plan-cn": "Xiaomi MiMo Token Plan (China)",
  "xiaomi-token-plan-ams": "Xiaomi MiMo Token Plan (Amsterdam)",
  "xiaomi-token-plan-sgp": "Xiaomi MiMo Token Plan (Singapore)",
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function trimToUndefined(value: unknown): string | undefined {
  const trimmed = typeof value === "string" ? value.trim() : "";
  return trimmed.length > 0 ? trimmed : undefined;
}

export function resolvePrimeBinaryPath(binaryPath?: string | null | undefined): string {
  return trimToUndefined(binaryPath) ?? PRIME_DEFAULT_BINARY;
}

/**
 * Prime's agent dir: explicit setting, then `PRIME_AGENT_CODING_AGENT_DIR`,
 * then the CLI default `~/.prime/agent`.
 */
export function resolvePrimeAgentDir(
  agentDir?: string | null | undefined,
  env: NodeJS.ProcessEnv = process.env,
  homeDir: string = nodeOs.homedir(),
): string {
  return (
    trimToUndefined(agentDir) ??
    trimToUndefined(env[PRIME_AGENT_DIR_ENV]) ??
    nodePath.join(homeDir, ".prime", "agent")
  );
}

/** Prime's session dir: `PRIME_AGENT_SESSION_DIR`, then `<agentDir>/sessions`. */
export function resolvePrimeSessionsDir(
  agentDir: string,
  env: NodeJS.ProcessEnv = process.env,
): string {
  return trimToUndefined(env[PRIME_SESSION_DIR_ENV]) ?? nodePath.join(agentDir, "sessions");
}

export function isPrimeThinkingLevel(value: unknown): value is PrimeThinkingLevel {
  return (
    typeof value === "string" &&
    (PRIME_THINKING_LEVEL_OPTIONS as ReadonlyArray<string>).includes(value)
  );
}

/** `provider/id[:thinkingLevel]` as accepted by `prime-agent --model`. */
export function buildPrimeModelFlag(
  model: string | undefined,
  thinkingLevel: PrimeThinkingLevel | undefined,
): string | undefined {
  const slug = trimToUndefined(model);
  if (!slug) {
    return undefined;
  }
  return thinkingLevel ? `${slug}:${thinkingLevel}` : slug;
}

/** Prime's display name for an upstream provider id; unknown ids get a capitalized slug. */
export function formatPrimeUpstreamProviderName(providerId: string): string {
  const known = PRIME_UPSTREAM_PROVIDER_NAMES[providerId];
  if (known) {
    return known;
  }
  return providerId.charAt(0).toUpperCase() + providerId.slice(1);
}

export function buildPrimeAcpSpawnInput(
  primeSettings: PrimeAcpRuntimeSettings | null | undefined,
  cwd: string,
  runtimeMode: RuntimeMode,
  childEnvironment?: NodeJS.ProcessEnv,
  spawnOptions: PrimeAcpSpawnOptions = {},
): AcpSpawnInput {
  // Prime never requests permissions over ACP and exposes no session modes;
  // the runtime mode is enforced by the adapter, not by a process flag.
  void runtimeMode;
  const args = ["--mode", "acp", "--cwd", cwd];
  const model = trimToUndefined(spawnOptions.model);
  if (model) {
    args.push("--model", model);
  }
  const resumeSessionId = trimToUndefined(spawnOptions.resumeSessionId);
  if (resumeSessionId) {
    args.push("--resume", resumeSessionId);
  }

  const agentDir = trimToUndefined(primeSettings?.agentDir);
  const overrides: NodeJS.ProcessEnv = agentDir ? { [PRIME_AGENT_DIR_ENV]: agentDir } : {};

  return {
    command: resolvePrimeBinaryPath(primeSettings?.binaryPath),
    args,
    cwd,
    env: buildProviderChildEnvironment({
      provider: "prime",
      baseEnv: childEnvironment ?? process.env,
      overrides,
    }),
  };
}

/**
 * Prime advertises no ACP auth methods and does not implement `authenticate`;
 * credentials live in its own store. If session setup ever reports that auth
 * is required, surface the CLI login path instead of attempting the request.
 */
export const resolvePrimeAcpAuthMethodId = (
  initializeResult: Acp.InitializeResponse,
): Effect.Effect<string, AcpErrors.AcpError> =>
  Effect.fail(
    new AcpErrors.AcpRequestError({
      code: -32602,
      errorMessage: `Prime Agent reported that authentication is required, but it manages credentials itself. ${PRIME_LOGIN_GUIDANCE}`,
      data: {
        authMethods: (initializeResult.authMethods ?? []).map((method) => method.id),
        reason: "credentials_missing",
      },
    }),
  );

export function runPrimeAcpCompactionCommand(
  runtime: Pick<AcpSessionRuntimeShape, "prompt">,
): Effect.Effect<Acp.PromptResponse, AcpErrors.AcpError> {
  // Prime compacts natively when a prompt starts with `/compact`; it never
  // advertises slash commands over ACP, so no available-command check applies.
  return runtime.prompt({ prompt: [{ type: "text", text: PRIME_COMPACT_PROMPT }] });
}

export const makePrimeAcpRuntime = (
  input: PrimeAcpRuntimeInput,
): Effect.Effect<AcpSessionRuntimeShape, AcpErrors.AcpError, Scope.Scope> =>
  Effect.gen(function* () {
    const acpContext = yield* Layer.build(
      AcpSessionRuntime.layer({
        ...input,
        spawn: buildPrimeAcpSpawnInput(
          input.primeSettings,
          input.cwd,
          input.runtimeMode,
          input.sessionConfig?.childEnvironment,
          input.spawnOptions,
        ),
        authPolicy: "on-demand",
        resolveAuthMethodId: resolvePrimeAcpAuthMethodId,
      }).pipe(
        Layer.provide(
          Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, input.childProcessSpawner),
        ),
      ),
    );
    return ServiceMap.getUnsafe(acpContext, AcpSessionRuntime);
  });

// ── RPC discovery ──────────────────────────────────────────────────

export const PRIME_RPC_MODELS_REQUEST_ID = "models";
export const PRIME_RPC_STATE_REQUEST_ID = "state";
export const PRIME_RPC_COMMANDS_REQUEST_ID = "commands";

export interface PrimeRpcRequest {
  readonly id: string;
  readonly type: string;
}

export interface PrimeRpcResponse {
  readonly id: string;
  readonly success: boolean;
  readonly data?: unknown;
  readonly error?: string;
}

export interface PrimeRpcDiscoveryInput {
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly binaryPath?: string | null | undefined;
  readonly agentDir?: string | null | undefined;
  /** Working directory for the probe; defaults to the OS temp dir. */
  readonly cwd?: string;
  readonly requests: ReadonlyArray<PrimeRpcRequest>;
  readonly env?: NodeJS.ProcessEnv;
}

export interface PrimeRpcDiscoveryResult {
  readonly responses: ReadonlyMap<string, PrimeRpcResponse>;
  readonly stderr: string;
  readonly exitCode: number;
}

export function buildPrimeRpcDiscoveryArgs(cwd: string): ReadonlyArray<string> {
  return ["--mode", "rpc", "--no-session", "--offline", "--cwd", cwd];
}

export function serializePrimeRpcRequests(requests: ReadonlyArray<PrimeRpcRequest>): string {
  return requests.map((request) => `${JSON.stringify(request)}\n`).join("");
}

/** Keeps only `type: "response"` lines; RPC-mode stdout also carries event lines. */
export function parsePrimeRpcResponses(stdout: string): Map<string, PrimeRpcResponse> {
  const responses = new Map<string, PrimeRpcResponse>();
  for (const line of stdout.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    let parsed: unknown;
    try {
      parsed = JSON.parse(trimmed);
    } catch {
      continue;
    }
    if (!isRecord(parsed) || parsed.type !== "response" || typeof parsed.id !== "string") {
      continue;
    }
    responses.set(parsed.id, {
      id: parsed.id,
      success: parsed.success !== false,
      ...(parsed.data !== undefined ? { data: parsed.data } : {}),
      ...(typeof parsed.error === "string" ? { error: parsed.error } : {}),
    });
  }
  return responses;
}

function collectStreamAsString<E>(stream: Stream.Stream<Uint8Array, E>): Effect.Effect<string, E> {
  return collectUint8StreamText({ stream }).pipe(Effect.map(({ text }) => text));
}

/**
 * Runs Prime in RPC mode for one batch of requests. The requests are written
 * up front and stdin is closed; Prime answers every request before exiting.
 */
export const runPrimeRpcDiscovery = (
  input: PrimeRpcDiscoveryInput,
): Effect.Effect<PrimeRpcDiscoveryResult, PlatformError.PlatformError, Scope.Scope> =>
  Effect.gen(function* () {
    const cwd = input.cwd ?? nodeOs.tmpdir();
    const agentDir = trimToUndefined(input.agentDir);
    const env = buildProviderChildEnvironment({
      provider: "prime",
      baseEnv: input.env ?? process.env,
      overrides: agentDir ? { [PRIME_AGENT_DIR_ENV]: agentDir } : {},
    });
    const child = yield* input.childProcessSpawner.spawn(
      makeEffectProcessCommand(
        resolvePrimeBinaryPath(input.binaryPath),
        buildPrimeRpcDiscoveryArgs(cwd),
        { cwd, env },
      ),
    );
    const requestBytes = new TextEncoder().encode(serializePrimeRpcRequests(input.requests));
    const [, stdout, stderr, exitCode] = yield* Effect.all(
      [
        Stream.make(requestBytes).pipe(Stream.run(child.stdin)),
        collectStreamAsString(child.stdout),
        collectStreamAsString(child.stderr),
        child.exitCode.pipe(Effect.map(Number)),
      ],
      { concurrency: "unbounded" },
    );
    return { responses: parsePrimeRpcResponses(stdout), stderr, exitCode };
  });

// ── Model registry ─────────────────────────────────────────────────

export interface PrimeRegistryModel {
  readonly id: string;
  readonly name: string;
  readonly provider: string;
  readonly reasoning: boolean;
  readonly thinkingLevelMap?: Readonly<Record<string, string | null>>;
  readonly input: ReadonlyArray<string>;
  readonly contextWindow?: number;
}

export interface PrimeRegistryState {
  readonly model?: PrimeRegistryModel;
  readonly thinkingLevel?: PrimeThinkingLevel;
}

export interface PrimeUserSettings {
  readonly defaultProvider?: string;
  readonly defaultModel?: string;
  readonly defaultThinkingLevel?: PrimeThinkingLevel;
  readonly recentModels: ReadonlyArray<string>;
}

export function parsePrimeRegistryModel(value: unknown): PrimeRegistryModel | undefined {
  if (!isRecord(value)) {
    return undefined;
  }
  const id = trimToUndefined(value.id);
  const provider = trimToUndefined(value.provider);
  if (!id || !provider) {
    return undefined;
  }
  const thinkingLevelMap = isRecord(value.thinkingLevelMap)
    ? Object.fromEntries(
        Object.entries(value.thinkingLevelMap).flatMap(([level, mapped]) =>
          mapped === null || typeof mapped === "string" ? [[level, mapped] as const] : [],
        ),
      )
    : undefined;
  const input = Array.isArray(value.input)
    ? value.input.filter((entry): entry is string => typeof entry === "string")
    : [];
  const contextWindow =
    typeof value.contextWindow === "number" &&
    Number.isFinite(value.contextWindow) &&
    value.contextWindow > 0
      ? value.contextWindow
      : undefined;
  return {
    id,
    name: trimToUndefined(value.name) ?? id,
    provider,
    reasoning: value.reasoning === true,
    ...(thinkingLevelMap ? { thinkingLevelMap } : {}),
    input,
    ...(contextWindow !== undefined ? { contextWindow } : {}),
  };
}

/** `get_available_models` → `data.models[]`. */
export function parsePrimeRegistryModels(data: unknown): PrimeRegistryModel[] {
  if (!isRecord(data) || !Array.isArray(data.models)) {
    return [];
  }
  return data.models
    .map(parsePrimeRegistryModel)
    .filter((model): model is PrimeRegistryModel => model !== undefined);
}

/** `get_state` → `data.model` (current default) and `data.thinkingLevel`. */
export function parsePrimeRegistryState(data: unknown): PrimeRegistryState {
  if (!isRecord(data)) {
    return {};
  }
  const model = parsePrimeRegistryModel(data.model);
  return {
    ...(model ? { model } : {}),
    ...(isPrimeThinkingLevel(data.thinkingLevel) ? { thinkingLevel: data.thinkingLevel } : {}),
  };
}

export function parsePrimeUserSettings(raw: unknown): PrimeUserSettings {
  if (!isRecord(raw)) {
    return { recentModels: [] };
  }
  const defaultProvider = trimToUndefined(raw.defaultProvider);
  const defaultModel = trimToUndefined(raw.defaultModel);
  const recentModels = Array.isArray(raw.recentModels)
    ? raw.recentModels.map(trimToUndefined).filter((value): value is string => value !== undefined)
    : [];
  return {
    ...(defaultProvider ? { defaultProvider } : {}),
    ...(defaultModel ? { defaultModel } : {}),
    ...(isPrimeThinkingLevel(raw.defaultThinkingLevel)
      ? { defaultThinkingLevel: raw.defaultThinkingLevel }
      : {}),
    recentModels,
  };
}

export async function readPrimeUserSettings(agentDir: string): Promise<PrimeUserSettings> {
  const raw = await readFile(nodePath.join(agentDir, "settings.json"), "utf8").catch(
    () => undefined,
  );
  if (raw === undefined) {
    return { recentModels: [] };
  }
  try {
    return parsePrimeUserSettings(JSON.parse(raw));
  } catch {
    return { recentModels: [] };
  }
}

/** Upstream provider ids with stored credentials in `<agentDir>/auth.json`. */
export async function readPrimeAuthProviders(agentDir: string): Promise<ReadonlyArray<string>> {
  const raw = await readFile(nodePath.join(agentDir, "auth.json"), "utf8").catch(() => undefined);
  if (raw === undefined) {
    return [];
  }
  try {
    const parsed: unknown = JSON.parse(raw);
    return isRecord(parsed) ? Object.keys(parsed).filter((key) => key.trim().length > 0) : [];
  } catch {
    return [];
  }
}

export function primeModelSlug(model: Pick<PrimeRegistryModel, "provider" | "id">): string {
  return `${model.provider}/${model.id}`;
}

// A level is valid unless the model maps it to null; models without a map
// accept every level. Non-reasoning models have none.
export function getPrimeSupportedThinkingLevels(
  model: Pick<PrimeRegistryModel, "reasoning" | "thinkingLevelMap">,
): ReadonlyArray<PrimeThinkingLevel> {
  if (!model.reasoning) {
    return [];
  }
  return PRIME_THINKING_LEVEL_OPTIONS.filter((level) => model.thinkingLevelMap?.[level] !== null);
}

export function formatPrimeContextWindow(contextWindow: number): string {
  if (contextWindow >= 1_000_000) {
    const millions = contextWindow / 1_000_000;
    return `${Number.isInteger(millions) ? millions : millions.toFixed(1)}M`;
  }
  return `${Math.round(contextWindow / 1_000)}K`;
}

function describePrimeModel(model: PrimeRegistryModel): string {
  const parts = [model.provider];
  if (model.contextWindow !== undefined) {
    parts.push(`${formatPrimeContextWindow(model.contextWindow)} context`);
  }
  if (model.input.includes("image")) {
    parts.push("vision");
  }
  return parts.join(" · ");
}

export function toPrimeProviderModelDescriptor(
  model: PrimeRegistryModel,
  defaultThinkingLevel: PrimeThinkingLevel | undefined,
): ProviderModelDescriptor {
  const supportedLevels = getPrimeSupportedThinkingLevels(model);
  const supportedDescriptors = PI_THINKING_LEVEL_DESCRIPTORS.filter((descriptor) =>
    supportedLevels.includes(descriptor.value),
  );
  return {
    slug: primeModelSlug(model),
    name: model.name,
    description: describePrimeModel(model),
    upstreamProviderId: model.provider,
    upstreamProviderName: formatPrimeUpstreamProviderName(model.provider),
    ...(model.contextWindow !== undefined ? { contextWindowTokens: model.contextWindow } : {}),
    ...(supportedDescriptors.length > 0
      ? {
          supportedReasoningEfforts: supportedDescriptors.map((descriptor) => ({
            value: descriptor.value,
            label: descriptor.label,
            description: descriptor.description,
          })),
          ...(defaultThinkingLevel !== undefined && supportedLevels.includes(defaultThinkingLevel)
            ? { defaultReasoningEffort: defaultThinkingLevel }
            : {}),
        }
      : {}),
  };
}

/**
 * Orders the registry the way Prime's own picker does: the current default
 * model first, then `recentModels` in their stored order, then the rest
 * alphabetically by slug.
 */
export function orderPrimeModelSlugs(
  slugs: ReadonlyArray<string>,
  input: { readonly defaultSlug?: string; readonly recentModels: ReadonlyArray<string> },
): string[] {
  const available = new Set(slugs);
  const ordered: string[] = [];
  const push = (slug: string | undefined) => {
    if (slug !== undefined && available.has(slug) && !ordered.includes(slug)) {
      ordered.push(slug);
    }
  };
  push(input.defaultSlug);
  for (const recent of input.recentModels) {
    push(recent);
  }
  for (const slug of [...available].toSorted((left, right) => left.localeCompare(right))) {
    push(slug);
  }
  return ordered;
}

export function parsePrimeModelRegistry(input: {
  readonly models: unknown;
  readonly state: unknown;
  readonly settings?: PrimeUserSettings;
}): ProviderModelDescriptor[] {
  const models = parsePrimeRegistryModels(input.models);
  const state = parsePrimeRegistryState(input.state);
  const settings = input.settings ?? { recentModels: [] };
  const defaultThinkingLevel = state.thinkingLevel ?? settings.defaultThinkingLevel;
  const defaultSlug =
    state.model !== undefined
      ? primeModelSlug(state.model)
      : settings.defaultProvider && settings.defaultModel
        ? `${settings.defaultProvider}/${settings.defaultModel}`
        : undefined;

  const bySlug = new Map<string, PrimeRegistryModel>();
  for (const model of models) {
    const slug = primeModelSlug(model);
    if (!bySlug.has(slug)) {
      bySlug.set(slug, model);
    }
  }
  return orderPrimeModelSlugs([...bySlug.keys()], {
    ...(defaultSlug !== undefined ? { defaultSlug } : {}),
    recentModels: settings.recentModels,
  }).map((slug) => toPrimeProviderModelDescriptor(bySlug.get(slug)!, defaultThinkingLevel));
}

// ── Commands ───────────────────────────────────────────────────────

export interface PrimeCommandDescriptor {
  readonly name: string;
  readonly description?: string;
  /** Prime's command kind: "skill", "prompt", or "extension". */
  readonly source?: string;
  /** SKILL.md / template path from `sourceInfo.path`. */
  readonly path?: string;
  /** Where Prime loaded it from (`sourceInfo.source`): "builtin", "auto", ... */
  readonly origin?: string;
  /** Prime's scope label (`sourceInfo.scope`): "user" or "project". */
  readonly scope?: string;
}

/** `get_commands` → `data.commands[]` (`skill:<name>`, prompts, extensions). */
export function parsePrimeCommands(data: unknown): PrimeCommandDescriptor[] {
  if (!isRecord(data) || !Array.isArray(data.commands)) {
    return [];
  }
  return data.commands.flatMap((entry): PrimeCommandDescriptor[] => {
    if (!isRecord(entry)) return [];
    const name = trimToUndefined(entry.name);
    if (!name) return [];
    const description = trimToUndefined(entry.description);
    const source = trimToUndefined(entry.source);
    const sourceInfo = isRecord(entry.sourceInfo) ? entry.sourceInfo : undefined;
    const path = trimToUndefined(sourceInfo?.path);
    const origin = trimToUndefined(sourceInfo?.source);
    const scope = trimToUndefined(sourceInfo?.scope);
    return [
      {
        name,
        ...(description ? { description } : {}),
        ...(source ? { source } : {}),
        ...(path ? { path } : {}),
        ...(origin ? { origin } : {}),
        ...(scope ? { scope } : {}),
      },
    ];
  });
}

export const PRIME_COMPACT_COMMAND: ProviderListCommandsResult["commands"][number] = {
  name: PRIME_COMPACT_COMMAND_NAME,
  description: PRIME_COMPACT_COMMAND_DESCRIPTION,
};

/** Synthetic `compact` first, then Prime's discovered commands (deduped by name). */
export function mapPrimeCommands(
  commands: ReadonlyArray<PrimeCommandDescriptor>,
): ProviderListCommandsResult["commands"] {
  const result: Array<ProviderListCommandsResult["commands"][number]> = [PRIME_COMPACT_COMMAND];
  const seen = new Set<string>([PRIME_COMPACT_COMMAND_NAME]);
  for (const command of commands) {
    const key = command.name.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    result.push({
      name: command.name,
      ...(command.description ? { description: command.description } : {}),
    });
  }
  return result;
}

export const PRIME_SKILL_COMMAND_PREFIX = "skill:";
const PRIME_BUILTIN_SKILL_ORIGIN = "builtin";
const PRIME_BUILTIN_SKILL_SCOPE = "prime";

/**
 * Prime skills are the `skill:<name>` commands. Built-ins ship inside the
 * prime-agent package (so only Prime can enumerate them); user and project
 * skills carry Prime's own scope label. Skills without a SKILL.md path cannot
 * be shown or toggled, so they are dropped.
 */
export function mapPrimeSkills(
  commands: ReadonlyArray<PrimeCommandDescriptor>,
): ProviderListSkillsResult["skills"] {
  const result: Array<ProviderListSkillsResult["skills"][number]> = [];
  const seen = new Set<string>();
  for (const command of commands) {
    if (command.source !== "skill" || !command.path) continue;
    const name = command.name.startsWith(PRIME_SKILL_COMMAND_PREFIX)
      ? command.name.slice(PRIME_SKILL_COMMAND_PREFIX.length)
      : command.name;
    const key = name.toLowerCase();
    if (!name || seen.has(key)) continue;
    seen.add(key);
    const scope =
      command.origin === PRIME_BUILTIN_SKILL_ORIGIN ? PRIME_BUILTIN_SKILL_SCOPE : command.scope;
    result.push({
      name,
      ...(command.description ? { description: command.description } : {}),
      path: command.path,
      enabled: true,
      ...(scope ? { scope } : {}),
    });
  }
  return result;
}

// ── Session files ──────────────────────────────────────────────────

export interface PrimeSessionFileSnapshot {
  readonly sessionsDir: string;
  readonly files: ReadonlySet<string>;
}

export interface PrimeSessionHeader {
  readonly id: string;
  readonly cwd: string;
  readonly timestampMs?: number;
}

export interface PrimeDetectedSession {
  /** Header `id` — the value Prime accepts for `--resume`. */
  readonly sessionId: string;
  readonly file: string;
}

function isPrimeSessionFileName(name: string): boolean {
  return name.endsWith(PRIME_SESSION_FILE_EXTENSION);
}

async function listPrimeSessionFiles(sessionsDir: string): Promise<string[]> {
  const entries = await readdir(sessionsDir).catch(() => [] as string[]);
  return entries.filter(isPrimeSessionFileName);
}

export async function snapshotPrimeSessionFiles(
  sessionsDir: string,
): Promise<PrimeSessionFileSnapshot> {
  return { sessionsDir, files: new Set(await listPrimeSessionFiles(sessionsDir)) };
}

export function parsePrimeSessionHeader(firstLine: string): PrimeSessionHeader | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(firstLine);
  } catch {
    return undefined;
  }
  if (!isRecord(parsed) || parsed.type !== "session") {
    return undefined;
  }
  const id = trimToUndefined(parsed.id);
  const cwd = trimToUndefined(parsed.cwd);
  if (!id || !cwd) {
    return undefined;
  }
  const timestampMs =
    typeof parsed.timestamp === "string" ? Date.parse(parsed.timestamp) : Number.NaN;
  return {
    id,
    cwd,
    ...(Number.isFinite(timestampMs) ? { timestampMs } : {}),
  };
}

/** Reads only the first line; session files grow with every turn. */
export async function readPrimeSessionHeader(
  file: string,
): Promise<PrimeSessionHeader | undefined> {
  const handle = await open(file, "r").catch(() => undefined);
  if (handle === undefined) {
    return undefined;
  }
  try {
    const buffer = Buffer.alloc(PRIME_SESSION_HEADER_MAX_BYTES);
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
    const text = buffer.subarray(0, bytesRead).toString("utf8");
    const newline = text.indexOf("\n");
    if (newline === -1 && bytesRead === buffer.length) {
      return undefined;
    }
    return parsePrimeSessionHeader(newline === -1 ? text : text.slice(0, newline));
  } catch {
    return undefined;
  } finally {
    await handle.close().catch(() => undefined);
  }
}

/**
 * The form of a session cwd that `samePrimeSessionCwd` compares: the real
 * path when it resolves, the absolute path otherwise. Used to key per-cwd
 * work in the sessions dir, since Prime keeps every project's sessions in one
 * directory and only same-cwd starts can bind each other's files.
 */
export async function canonicalPrimeSessionCwd(cwd: string): Promise<string> {
  const resolved = nodePath.resolve(cwd);
  return realpath(resolved).catch(() => resolved);
}

async function samePrimeSessionCwd(left: string, right: string): Promise<boolean> {
  if (nodePath.resolve(left) === nodePath.resolve(right)) {
    return true;
  }
  const [leftReal, rightReal] = await Promise.all([
    realpath(left).catch(() => undefined),
    realpath(right).catch(() => undefined),
  ]);
  return leftReal !== undefined && leftReal === rightReal;
}

export interface PrimeSessionDetectInput {
  readonly before: PrimeSessionFileSnapshot;
  readonly cwd: string;
  /** Spawn time; headers stamped earlier than this (minus clock skew) belong to older sessions. */
  readonly sinceMs: number;
  readonly timeoutMs: number;
  readonly pollMs?: number;
  /**
   * Header ids already bound to other live sessions in the same sessions dir.
   * Callers serialize snapshot -> spawn -> detect per directory, so this only
   * matters for a deferred retry whose snapshot predates other sessions' files.
   */
  readonly claimedSessionIds?: ReadonlySet<string>;
}

/**
 * Polls the sessions dir for a `.jsonl` that was not in the snapshot, whose
 * header cwd is the session's cwd, and whose header id no other live session
 * has claimed. Prime writes the header at process start, so the file normally
 * exists before `session/new` returns.
 */
export async function detectNewPrimeSession(
  input: PrimeSessionDetectInput,
): Promise<PrimeDetectedSession | undefined> {
  const { before, cwd, sinceMs, timeoutMs } = input;
  const pollMs = input.pollMs ?? PRIME_SESSION_DETECT_POLL_MS;
  const claimedSessionIds = input.claimedSessionIds ?? new Set<string>();
  const deadline = Date.now() + timeoutMs;
  const rejected = new Set<string>();
  for (;;) {
    const candidates: Array<PrimeDetectedSession & { readonly orderMs: number }> = [];
    for (const name of await listPrimeSessionFiles(before.sessionsDir)) {
      if (before.files.has(name) || rejected.has(name)) continue;
      const file = nodePath.join(before.sessionsDir, name);
      const header = await readPrimeSessionHeader(file);
      if (header === undefined) {
        // Prime may still be writing the header; look again on the next poll.
        continue;
      }
      if (claimedSessionIds.has(header.id)) {
        rejected.add(name);
        continue;
      }
      const startedMs =
        header.timestampMs ??
        (await stat(file).then(
          (info) => info.mtimeMs,
          () => undefined,
        ));
      if (
        (startedMs !== undefined && startedMs < sinceMs - PRIME_SESSION_DETECT_CLOCK_SKEW_MS) ||
        !(await samePrimeSessionCwd(header.cwd, cwd))
      ) {
        rejected.add(name);
        continue;
      }
      candidates.push({ sessionId: header.id, file, orderMs: startedMs ?? Number.MAX_VALUE });
    }
    if (candidates.length > 0) {
      const [first] = candidates.toSorted((left, right) => left.orderMs - right.orderMs);
      return first ? { sessionId: first.sessionId, file: first.file } : undefined;
    }
    if (Date.now() >= deadline) {
      return undefined;
    }
    await new Promise<void>((resolve) => setTimeout(resolve, pollMs));
  }
}

/**
 * Resolves a persisted session id to its file: an absolute path, then a file
 * named after the id, then a scan of session headers (the header id usually
 * differs from the file basename).
 */
export async function resolvePrimeSessionFile(
  sessionsDir: string,
  sessionId: string,
): Promise<string | undefined> {
  const trimmed = sessionId.trim();
  if (!trimmed) {
    return undefined;
  }
  const isFile = (candidate: string) =>
    stat(candidate).then(
      (info) => info.isFile(),
      () => false,
    );
  if (nodePath.isAbsolute(trimmed) && (await isFile(trimmed))) {
    return trimmed;
  }
  const named = nodePath.join(
    sessionsDir,
    trimmed.endsWith(PRIME_SESSION_FILE_EXTENSION)
      ? trimmed
      : `${trimmed}${PRIME_SESSION_FILE_EXTENSION}`,
  );
  if (await isFile(named)) {
    return named;
  }
  for (const name of await listPrimeSessionFiles(sessionsDir)) {
    const file = nodePath.join(sessionsDir, name);
    const header = await readPrimeSessionHeader(file);
    if (header?.id === trimmed) {
      return file;
    }
  }
  return undefined;
}
