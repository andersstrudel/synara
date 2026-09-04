import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { Effect, Layer } from "effect";
import type * as Acp from "@agentclientprotocol/sdk";
import { ChildProcessSpawner } from "effect/unstable/process";
import { afterEach, describe, expect, it, vi } from "vitest";

import * as AcpErrors from "./AcpErrors.ts";
import {
  AcpSessionRuntime,
  type AcpSessionRuntimeOptions,
  type AcpSessionRuntimeShape,
} from "./AcpSessionRuntime.ts";
import {
  buildPrimeAcpSpawnInput,
  buildPrimeModelFlag,
  buildPrimeRpcDiscoveryArgs,
  detectNewPrimeSession,
  formatPrimeContextWindow,
  formatPrimeUpstreamProviderName,
  getPrimeSupportedThinkingLevels,
  makePrimeAcpRuntime,
  mapPrimeCommands,
  mapPrimeSkills,
  orderPrimeModelSlugs,
  parsePrimeCommands,
  parsePrimeModelRegistry,
  parsePrimeRpcResponses,
  parsePrimeSessionHeader,
  parsePrimeUserSettings,
  readPrimeAuthProviders,
  readPrimeUserSettings,
  resolvePrimeAcpAuthMethodId,
  resolvePrimeAgentDir,
  resolvePrimeSessionFile,
  resolvePrimeSessionsDir,
  runPrimeAcpCompactionCommand,
  serializePrimeRpcRequests,
  snapshotPrimeSessionFiles,
} from "./PrimeAcpSupport.ts";

// Captured from `prime-agent 0.9.1 --mode rpc --no-session --offline` on a
// machine with Anthropic, OpenAI (Codex), and Cerebras credentials.
const PRIME_RPC_MODELS_DATA = {
  models: [
    {
      id: "claude-fable-5-1",
      name: "Claude Fable 5.1",
      api: "anthropic-messages",
      provider: "anthropic",
      baseUrl: "https://api.anthropic.com",
      reasoning: true,
      thinkingLevelMap: { off: null, xhigh: "xhigh", max: "max" },
      input: ["text", "image"],
      cost: { input: 10, output: 50, cacheRead: 0.25, cacheWrite: 12.5 },
      contextWindow: 1_000_000,
      maxTokens: 128_000,
    },
    {
      id: "claude-haiku-4-5",
      name: "Claude Haiku 4.5 (latest)",
      api: "anthropic-messages",
      provider: "anthropic",
      reasoning: true,
      input: ["text", "image"],
      cost: { input: 1, output: 5, cacheRead: 0.1, cacheWrite: 1.25 },
      contextWindow: 200_000,
      maxTokens: 64_000,
    },
    {
      id: "gpt-oss-120b",
      name: "GPT OSS 120B",
      api: "openai-completions",
      provider: "cerebras",
      reasoning: false,
      input: ["text"],
      cost: { input: 0.35, output: 0.75, cacheRead: 0, cacheWrite: 0 },
      contextWindow: 131_072,
      maxTokens: 40_960,
    },
    {
      id: "gpt-5.6-sol",
      name: "GPT-5.6 Sol",
      api: "openai-codex-responses",
      provider: "openai-codex",
      reasoning: true,
      thinkingLevelMap: { xhigh: "xhigh", minimal: null, max: "max" },
      input: ["text", "image"],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      contextWindow: 272_000,
      maxTokens: 128_000,
    },
    {
      id: "qwen-3.8-27b",
      name: "Qwen 3.8 27B",
      api: "openai-completions",
      provider: "cerebras",
      reasoning: true,
      thinkingLevelMap: {
        off: "none",
        minimal: "low",
        low: "low",
        medium: "medium",
        high: "high",
        xhigh: null,
        max: null,
      },
      input: ["text", "image"],
      cost: { input: 0.99, output: 1.49, cacheRead: 0, cacheWrite: 0 },
      contextWindow: 131_072,
      maxTokens: 40_960,
    },
  ],
};

const PRIME_RPC_STATE_DATA = {
  model: PRIME_RPC_MODELS_DATA.models[4],
  thinkingLevel: "high",
  isStreaming: false,
  sessionId: "01a06d2f-5637-76a4-8b0c-8d175a3fb6b2",
};

// `get_commands` shape from `prime-agent 0.9.1 --mode rpc`: skills carry
// `source: "skill"` plus `sourceInfo.{source,scope,path}`, where `source` is
// "builtin" for skills shipped inside the package and "auto" for discovered ones.
const PRIME_RPC_COMMANDS_DATA = {
  commands: [
    {
      name: "skill:websearch",
      description: "Search the web",
      source: "skill",
      sourceInfo: {
        source: "auto",
        scope: "user",
        path: "/home/test/.prime/agent/skills/websearch/SKILL.md",
      },
    },
    {
      name: "skill:compact",
      description: "Compact the session",
      source: "skill",
      sourceInfo: {
        source: "builtin",
        scope: "user",
        path: "/opt/prime-agent/dist/skills/compact/SKILL.md",
      },
    },
    { name: "review", source: "prompt" },
    { name: "", description: "ignored: empty name" },
  ],
};

const PRIME_USER_SETTINGS = {
  defaultProvider: "cerebras",
  defaultModel: "qwen-3.8-27b",
  recentModels: ["cerebras/qwen-3.8-27b", "openai-codex/gpt-5.6-sol", "missing/model"],
  defaultThinkingLevel: "high",
};

function sessionHeader(id: string, cwd: string, timestamp = new Date().toISOString()): string {
  return `${JSON.stringify({ type: "session", version: 3, id, timestamp, cwd, rlmDepth: 0 })}\n`;
}

function rpcLine(id: string, command: string, data: unknown): string {
  return JSON.stringify({ id, type: "response", command, success: true, data });
}

describe("buildPrimeAcpSpawnInput", () => {
  it("builds the default Prime ACP command", () => {
    expect(buildPrimeAcpSpawnInput(undefined, "/tmp/project", "full-access")).toMatchObject({
      command: "prime-agent",
      args: ["--mode", "acp", "--cwd", "/tmp/project"],
      cwd: "/tmp/project",
    });
  });

  it("passes the model, thinking level, and resume target as process-start flags", () => {
    const spawn = buildPrimeAcpSpawnInput(
      { binaryPath: "/usr/local/bin/prime-agent" },
      "/tmp/project",
      "full-access",
      undefined,
      { model: "cerebras/qwen-3.8-27b:high", resumeSessionId: "01a06d2f-5637" },
    );

    expect(spawn).toMatchObject({
      command: "/usr/local/bin/prime-agent",
      args: [
        "--mode",
        "acp",
        "--cwd",
        "/tmp/project",
        "--model",
        "cerebras/qwen-3.8-27b:high",
        "--resume",
        "01a06d2f-5637",
      ],
      cwd: "/tmp/project",
    });
  });

  it("scopes the agent dir through the environment, keeps upstream provider keys, and strips Synara authority", () => {
    const spawn = buildPrimeAcpSpawnInput(
      { agentDir: "/custom/prime/agent" },
      "/tmp/project",
      "approval-required",
      {
        HOME: "/real/home",
        ANTHROPIC_API_KEY: "upstream-key",
        OPENAI_API_KEY: "upstream-openai-key",
        SYNARA_AGENT_GATEWAY_BOOTSTRAP_TOKEN: "bootstrap-must-not-propagate",
      },
    );

    expect(spawn.args).toEqual(["--mode", "acp", "--cwd", "/tmp/project"]);
    expect(spawn.env).toMatchObject({
      HOME: "/real/home",
      PRIME_AGENT_CODING_AGENT_DIR: "/custom/prime/agent",
      // Prime's model registry reads upstream API keys from the environment
      // (like Pi), so the host's provider credentials must reach the child.
      ANTHROPIC_API_KEY: "upstream-key",
      OPENAI_API_KEY: "upstream-openai-key",
    });
    expect(spawn.env?.SYNARA_AGENT_GATEWAY_BOOTSTRAP_TOKEN).toBeUndefined();
  });

  it("leaves the agent dir to the CLI default when unset", () => {
    const spawn = buildPrimeAcpSpawnInput({ agentDir: "  " }, "/tmp/project", "full-access", {
      HOME: "/real/home",
    });
    expect(spawn.env?.PRIME_AGENT_CODING_AGENT_DIR).toBeUndefined();
  });
});

describe("Prime model flag and directories", () => {
  it("formats provider/id with an optional thinking level", () => {
    expect(buildPrimeModelFlag("cerebras/qwen-3.8-27b", undefined)).toBe("cerebras/qwen-3.8-27b");
    expect(buildPrimeModelFlag("cerebras/qwen-3.8-27b", "xhigh")).toBe(
      "cerebras/qwen-3.8-27b:xhigh",
    );
    expect(buildPrimeModelFlag("  ", "high")).toBeUndefined();
  });

  it("resolves the agent and sessions directories from settings, env, then defaults", () => {
    expect(
      resolvePrimeAgentDir("/explicit", { PRIME_AGENT_CODING_AGENT_DIR: "/env" }, "/home"),
    ).toBe("/explicit");
    expect(resolvePrimeAgentDir(undefined, { PRIME_AGENT_CODING_AGENT_DIR: "/env" }, "/home")).toBe(
      "/env",
    );
    expect(resolvePrimeAgentDir("", {}, "/home")).toBe(path.join("/home", ".prime", "agent"));
    expect(resolvePrimeSessionsDir("/agent", { PRIME_AGENT_SESSION_DIR: "/sessions" })).toBe(
      "/sessions",
    );
    expect(resolvePrimeSessionsDir("/agent", {})).toBe(path.join("/agent", "sessions"));
  });
});

describe("makePrimeAcpRuntime", () => {
  it("selects on-demand authentication without requiring advertised auth methods", async () => {
    const fakeRuntime = {} as AcpSessionRuntimeShape;
    let capturedOptions: AcpSessionRuntimeOptions | undefined;
    const layerSpy = vi.spyOn(AcpSessionRuntime, "layer").mockImplementation((options) => {
      capturedOptions = options;
      return Layer.succeed(AcpSessionRuntime, fakeRuntime);
    });

    try {
      const runtime = await Effect.runPromise(
        makePrimeAcpRuntime({
          childProcessSpawner: {} as ChildProcessSpawner.ChildProcessSpawner["Service"],
          primeSettings: { binaryPath: "/opt/prime-agent" },
          runtimeMode: "full-access",
          cwd: "/tmp/project",
          clientInfo: { name: "Synara", version: "0.0.0" },
          spawnOptions: { model: "anthropic/claude-fable-5-1:max" },
        }).pipe(Effect.scoped),
      );

      expect(runtime).toBe(fakeRuntime);
      expect(capturedOptions?.authPolicy).toBe("on-demand");
      expect(capturedOptions?.validateInitializeResult).toBeUndefined();
      expect(capturedOptions?.resumeSessionId).toBeUndefined();
      expect(capturedOptions?.spawn).toMatchObject({
        command: "/opt/prime-agent",
        args: [
          "--mode",
          "acp",
          "--cwd",
          "/tmp/project",
          "--model",
          "anthropic/claude-fable-5-1:max",
        ],
      });
    } finally {
      layerSpy.mockRestore();
    }
  });

  it("never authenticates: an auth-required session setup surfaces login guidance", async () => {
    const error = await Effect.runPromise(
      resolvePrimeAcpAuthMethodId({ protocolVersion: 1 } as Acp.InitializeResponse).pipe(
        Effect.flip,
      ),
    );
    expect(error).toBeInstanceOf(AcpErrors.AcpRequestError);
    expect(error.message).toContain("Run `prime-agent` and use /login");
  });

  it("compacts through the native /compact prompt", async () => {
    const prompts: Array<Acp.PromptRequest["prompt"]> = [];
    const response = await Effect.runPromise(
      runPrimeAcpCompactionCommand({
        prompt: (params) => {
          prompts.push(params.prompt);
          return Effect.succeed({ stopReason: "end_turn" } as Acp.PromptResponse);
        },
      }),
    );
    expect(response.stopReason).toBe("end_turn");
    expect(prompts).toEqual([[{ type: "text", text: "/compact" }]]);
  });
});

describe("Prime RPC discovery", () => {
  it("builds the offline RPC probe command and newline-delimited requests", () => {
    expect(buildPrimeRpcDiscoveryArgs("/tmp/probe")).toEqual([
      "--mode",
      "rpc",
      "--no-session",
      "--offline",
      "--cwd",
      "/tmp/probe",
    ]);
    expect(
      serializePrimeRpcRequests([
        { id: "models", type: "get_available_models" },
        { id: "state", type: "get_state" },
      ]),
    ).toBe('{"id":"models","type":"get_available_models"}\n{"id":"state","type":"get_state"}\n');
  });

  it("keeps only response lines and tolerates event noise", () => {
    const responses = parsePrimeRpcResponses(
      [
        '{"type":"event","name":"ready"}',
        "not json",
        rpcLine("models", "get_available_models", PRIME_RPC_MODELS_DATA),
        JSON.stringify({ id: "commands", type: "response", success: false, error: "boom" }),
        "",
      ].join("\n"),
    );
    expect(responses.get("models")).toMatchObject({ id: "models", success: true });
    expect(responses.get("commands")).toEqual({ id: "commands", success: false, error: "boom" });
    expect(responses.size).toBe(2);
  });
});

describe("parsePrimeModelRegistry", () => {
  const models = parsePrimeModelRegistry({
    models: PRIME_RPC_MODELS_DATA,
    state: PRIME_RPC_STATE_DATA,
    settings: parsePrimeUserSettings(PRIME_USER_SETTINGS),
  });

  it("orders the current default first, then recent models, then alphabetically", () => {
    expect(models.map((model) => model.slug)).toEqual([
      "cerebras/qwen-3.8-27b",
      "openai-codex/gpt-5.6-sol",
      "anthropic/claude-fable-5-1",
      "anthropic/claude-haiku-4-5",
      "cerebras/gpt-oss-120b",
    ]);
  });

  it("describes models with upstream provider, context window, and vision", () => {
    expect(models[0]).toMatchObject({
      slug: "cerebras/qwen-3.8-27b",
      name: "Qwen 3.8 27B",
      description: "cerebras · 131K context · vision",
      upstreamProviderId: "cerebras",
      upstreamProviderName: "Cerebras",
      defaultReasoningEffort: "high",
    });
    expect(models.find((model) => model.slug === "anthropic/claude-fable-5-1")).toMatchObject({
      description: "anthropic · 1M context · vision",
      upstreamProviderName: "Anthropic",
    });
    expect(models.find((model) => model.slug === "cerebras/gpt-oss-120b")).toMatchObject({
      description: "cerebras · 131K context",
    });
    expect(models.find((model) => model.slug === "openai-codex/gpt-5.6-sol")).toMatchObject({
      upstreamProviderId: "openai-codex",
      upstreamProviderName: "OpenAI Codex",
    });
  });

  it("labels upstream providers the way prime-agent's own picker does", () => {
    // Mirrors prime-agent's BUILT_IN_PROVIDER_DISPLAY_NAMES.
    expect(
      Object.fromEntries(
        [
          "openai",
          "openrouter",
          "xai",
          "google",
          "google-vertex",
          "zai",
          "deepseek",
          "prime-inference",
          "amazon-bedrock",
          "kimi-coding",
          "minimax",
          "groq",
          "mistral",
          "cerebras",
          "anthropic",
        ].map((providerId) => [providerId, formatPrimeUpstreamProviderName(providerId)]),
      ),
    ).toEqual({
      openai: "OpenAI",
      openrouter: "OpenRouter",
      xai: "xAI",
      google: "Google Gemini",
      "google-vertex": "Google Vertex AI",
      zai: "ZAI",
      deepseek: "DeepSeek",
      "prime-inference": "Prime Inference",
      "amazon-bedrock": "Amazon Bedrock",
      "kimi-coding": "Kimi For Coding",
      minimax: "MiniMax",
      groq: "Groq",
      mistral: "Mistral",
      cerebras: "Cerebras",
      anthropic: "Anthropic",
    });
    // Not in Prime's table (Prime labels it via its OAuth entry); kept distinct from `openai`.
    expect(formatPrimeUpstreamProviderName("openai-codex")).toBe("OpenAI Codex");
    // Unknown ids (e.g. a user-defined models.json provider) fall back to a capitalized slug.
    expect(formatPrimeUpstreamProviderName("my-proxy")).toBe("My-proxy");
  });

  it("filters thinking levels the model maps to null and mirrors Pi's option labels", () => {
    const qwen = models.find((model) => model.slug === "cerebras/qwen-3.8-27b");
    expect(qwen?.supportedReasoningEfforts).toEqual([
      { value: "off", label: "Off", description: "No extra reasoning" },
      { value: "minimal", label: "Minimal", description: "Light reasoning" },
      { value: "low", label: "Low", description: "Faster reasoning" },
      { value: "medium", label: "Medium", description: "Balanced reasoning" },
      { value: "high", label: "High", description: "Deeper reasoning" },
    ]);
    const fable = models.find((model) => model.slug === "anthropic/claude-fable-5-1");
    expect(fable?.supportedReasoningEfforts?.map((effort) => effort.value)).toEqual([
      "minimal",
      "low",
      "medium",
      "high",
      "xhigh",
      "max",
    ]);
    expect(fable?.defaultReasoningEffort).toBe("high");
    // No thinkingLevelMap: every level is valid.
    const haiku = models.find((model) => model.slug === "anthropic/claude-haiku-4-5");
    expect(haiku?.supportedReasoningEfforts?.map((effort) => effort.value)).toEqual([
      "off",
      "minimal",
      "low",
      "medium",
      "high",
      "xhigh",
      "max",
    ]);
    expect(haiku?.optionDescriptors).toBeUndefined();
  });

  it("omits reasoning controls for non-reasoning models", () => {
    const oss = models.find((model) => model.slug === "cerebras/gpt-oss-120b");
    expect(oss?.supportedReasoningEfforts).toBeUndefined();
    expect(oss?.defaultReasoningEffort).toBeUndefined();
    expect(getPrimeSupportedThinkingLevels({ reasoning: false })).toEqual([]);
  });

  it("drops the default reasoning effort when the model rejects that level", () => {
    const [sol] = parsePrimeModelRegistry({
      models: { models: [PRIME_RPC_MODELS_DATA.models[3]] },
      state: { thinkingLevel: "minimal" },
    });
    expect(sol?.supportedReasoningEfforts?.map((effort) => effort.value)).not.toContain("minimal");
    expect(sol?.defaultReasoningEffort).toBeUndefined();
  });

  it("falls back to settings.json for the default when get_state is unavailable", () => {
    const ordered = parsePrimeModelRegistry({
      models: PRIME_RPC_MODELS_DATA,
      state: undefined,
      settings: parsePrimeUserSettings({
        defaultProvider: "anthropic",
        defaultModel: "claude-haiku-4-5",
        defaultThinkingLevel: "medium",
        recentModels: ["cerebras/gpt-oss-120b"],
      }),
    });
    expect(ordered.map((model) => model.slug).slice(0, 2)).toEqual([
      "anthropic/claude-haiku-4-5",
      "cerebras/gpt-oss-120b",
    ]);
    expect(ordered[0]?.defaultReasoningEffort).toBe("medium");
  });

  it("ignores malformed registry entries and duplicate slugs", () => {
    expect(
      parsePrimeModelRegistry({
        models: {
          models: [
            { id: "x", provider: "p", reasoning: true, input: ["text"] },
            { id: "x", provider: "p", name: "duplicate" },
            { name: "missing id", provider: "p" },
            "not an object",
          ],
        },
        state: "bogus",
      }).map((model) => model.slug),
    ).toEqual(["p/x"]);
    expect(parsePrimeModelRegistry({ models: null, state: null })).toEqual([]);
  });

  it("orders slugs deterministically", () => {
    expect(
      orderPrimeModelSlugs(["c/z", "a/y", "b/x"], {
        defaultSlug: "b/x",
        recentModels: ["c/z", "b/x", "unknown/model"],
      }),
    ).toEqual(["b/x", "c/z", "a/y"]);
  });

  it("formats context windows in K and M", () => {
    expect(formatPrimeContextWindow(131_072)).toBe("131K");
    expect(formatPrimeContextWindow(272_000)).toBe("272K");
    expect(formatPrimeContextWindow(1_000_000)).toBe("1M");
    expect(formatPrimeContextWindow(1_500_000)).toBe("1.5M");
  });
});

describe("Prime commands", () => {
  it("maps discovered commands behind the synthetic compact command", () => {
    const commands = parsePrimeCommands(PRIME_RPC_COMMANDS_DATA);
    expect(commands).toEqual([
      {
        name: "skill:websearch",
        description: "Search the web",
        source: "skill",
        path: "/home/test/.prime/agent/skills/websearch/SKILL.md",
        origin: "auto",
        scope: "user",
      },
      {
        name: "skill:compact",
        description: "Compact the session",
        source: "skill",
        path: "/opt/prime-agent/dist/skills/compact/SKILL.md",
        origin: "builtin",
        scope: "user",
      },
      { name: "review", source: "prompt" },
    ]);
    expect(mapPrimeCommands(commands)).toEqual([
      { name: "compact", description: "Compact the conversation context" },
      { name: "skill:websearch", description: "Search the web" },
      { name: "skill:compact", description: "Compact the session" },
      { name: "review" },
    ]);
    // The same answer feeds the skills listing: built-ins take Prime's own scope.
    expect(mapPrimeSkills(commands)).toEqual([
      {
        name: "websearch",
        description: "Search the web",
        path: "/home/test/.prime/agent/skills/websearch/SKILL.md",
        enabled: true,
        scope: "user",
      },
      {
        name: "compact",
        description: "Compact the session",
        path: "/opt/prime-agent/dist/skills/compact/SKILL.md",
        enabled: true,
        scope: "prime",
      },
    ]);
  });

  it("never duplicates the compact command", () => {
    expect(mapPrimeCommands([{ name: "Compact", description: "native" }])).toEqual([
      { name: "compact", description: "Compact the conversation context" },
    ]);
    expect(parsePrimeCommands({ commands: "nope" })).toEqual([]);
  });

  it("maps skill commands to skill descriptors with Prime's origin as the scope", () => {
    const commands = parsePrimeCommands({
      commands: [
        {
          name: "skill:refine",
          description: "Refine the harness",
          source: "skill",
          sourceInfo: {
            path: "/opt/prime-agent/dist/skills/refine/SKILL.md",
            source: "builtin",
            scope: "user",
          },
        },
        {
          name: "skill:code-review",
          description: "Review changes",
          source: "skill",
          sourceInfo: {
            path: "/home/test/.agents/skills/code-review/SKILL.md",
            source: "auto",
            scope: "user",
          },
        },
        {
          name: "skill:local",
          source: "skill",
          sourceInfo: {
            path: "/repo/.prime/agent/skills/local/SKILL.md",
            source: "auto",
            scope: "project",
          },
        },
        { name: "skill:pathless", description: "cannot be shown", source: "skill" },
        { name: "skill:Refine", source: "skill", sourceInfo: { path: "/dup/SKILL.md" } },
        {
          name: "review",
          source: "prompt",
          sourceInfo: { path: "/home/test/.prime/agent/prompts/review.md" },
        },
      ],
    });
    expect(mapPrimeSkills(commands)).toEqual([
      {
        name: "refine",
        description: "Refine the harness",
        path: "/opt/prime-agent/dist/skills/refine/SKILL.md",
        enabled: true,
        scope: "prime",
      },
      {
        name: "code-review",
        description: "Review changes",
        path: "/home/test/.agents/skills/code-review/SKILL.md",
        enabled: true,
        scope: "user",
      },
      {
        name: "local",
        path: "/repo/.prime/agent/skills/local/SKILL.md",
        enabled: true,
        scope: "project",
      },
    ]);
  });
});

describe("Prime session files", () => {
  const tempDirs: string[] = [];
  afterEach(async () => {
    await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
  });

  async function makeSessionsDir(): Promise<{
    readonly root: string;
    readonly sessionsDir: string;
  }> {
    const root = await mkdtemp(path.join(os.tmpdir(), "synara-prime-sessions-"));
    tempDirs.push(root);
    const sessionsDir = path.join(root, "sessions");
    await mkdir(sessionsDir, { recursive: true });
    return { root, sessionsDir };
  }

  it("parses the header line Prime writes at process start", () => {
    expect(
      parsePrimeSessionHeader(
        '{"type":"session","version":3,"id":"01a0-header","timestamp":"2026-09-04T16:10:13.943Z","cwd":"/work","rlmDepth":0}',
      ),
    ).toEqual({
      id: "01a0-header",
      cwd: "/work",
      timestampMs: Date.parse("2026-09-04T16:10:13.943Z"),
    });
    expect(parsePrimeSessionHeader('{"type":"message"}')).toBeUndefined();
    expect(parsePrimeSessionHeader("garbage")).toBeUndefined();
  });

  it("detects the new session file for the session cwd and ignores pre-existing files", async () => {
    const { root, sessionsDir } = await makeSessionsDir();
    const cwd = path.join(root, "work");
    await mkdir(cwd, { recursive: true });
    await writeFile(path.join(sessionsDir, "old.jsonl"), sessionHeader("old-id", cwd));
    const before = await snapshotPrimeSessionFiles(sessionsDir);
    expect(before.files).toEqual(new Set(["old.jsonl"]));
    const sinceMs = Date.now();

    const detection = detectNewPrimeSession({ before, cwd, sinceMs, timeoutMs: 2_000, pollMs: 10 });
    await new Promise((resolve) => setTimeout(resolve, 30));
    // A file for another cwd must not be claimed by this session.
    await writeFile(
      path.join(sessionsDir, "other-cwd.jsonl"),
      sessionHeader("other-id", path.join(root, "elsewhere")),
    );
    await writeFile(
      path.join(sessionsDir, "01a0-file.jsonl"),
      `${sessionHeader("01a0-header", cwd)}{"type":"thinking_level_change"}\n`,
    );

    await expect(detection).resolves.toEqual({
      sessionId: "01a0-header",
      file: path.join(sessionsDir, "01a0-file.jsonl"),
    });
  });

  it("returns undefined when no matching session file appears in time", async () => {
    const { root, sessionsDir } = await makeSessionsDir();
    const before = await snapshotPrimeSessionFiles(sessionsDir);
    await expect(
      detectNewPrimeSession({
        before,
        cwd: path.join(root, "work"),
        sinceMs: Date.now(),
        timeoutMs: 50,
        pollMs: 10,
      }),
    ).resolves.toBeUndefined();
  });

  it("skips session files that predate the spawn", async () => {
    const { root, sessionsDir } = await makeSessionsDir();
    const cwd = path.join(root, "work");
    await mkdir(cwd, { recursive: true });
    const before = await snapshotPrimeSessionFiles(sessionsDir);
    await writeFile(
      path.join(sessionsDir, "stale.jsonl"),
      sessionHeader("stale-id", cwd, new Date(Date.now() - 60_000).toISOString()),
    );
    await expect(
      detectNewPrimeSession({ before, cwd, sinceMs: Date.now(), timeoutMs: 50, pollMs: 10 }),
    ).resolves.toBeUndefined();
  });

  it("never re-binds a header id another live session already claimed", async () => {
    const { root, sessionsDir } = await makeSessionsDir();
    const cwd = path.join(root, "work");
    await mkdir(cwd, { recursive: true });
    const before = await snapshotPrimeSessionFiles(sessionsDir);
    const sinceMs = Date.now();
    // The claimed file is the earlier one, so it would win on timestamp alone.
    await writeFile(
      path.join(sessionsDir, "claimed.jsonl"),
      sessionHeader("claimed-id", cwd, new Date(sinceMs).toISOString()),
    );
    await writeFile(
      path.join(sessionsDir, "mine.jsonl"),
      sessionHeader("my-id", cwd, new Date(sinceMs + 5).toISOString()),
    );

    await expect(
      detectNewPrimeSession({
        before,
        cwd,
        sinceMs,
        timeoutMs: 200,
        pollMs: 10,
        claimedSessionIds: new Set(["claimed-id"]),
      }),
    ).resolves.toEqual({ sessionId: "my-id", file: path.join(sessionsDir, "mine.jsonl") });
    await expect(
      detectNewPrimeSession({
        before,
        cwd,
        sinceMs,
        timeoutMs: 50,
        pollMs: 10,
        claimedSessionIds: new Set(["claimed-id", "my-id"]),
      }),
    ).resolves.toBeUndefined();
  });

  it("resolves a session by absolute path, basename, or header id", async () => {
    const { sessionsDir } = await makeSessionsDir();
    const byName = path.join(sessionsDir, "named.jsonl");
    await writeFile(byName, sessionHeader("named-header", "/work"));
    const byHeader = path.join(sessionsDir, "file-basename.jsonl");
    await writeFile(byHeader, sessionHeader("header-only", "/work"));

    await expect(resolvePrimeSessionFile(sessionsDir, byName)).resolves.toBe(byName);
    await expect(resolvePrimeSessionFile(sessionsDir, "named")).resolves.toBe(byName);
    await expect(resolvePrimeSessionFile(sessionsDir, "named.jsonl")).resolves.toBe(byName);
    await expect(resolvePrimeSessionFile(sessionsDir, "header-only")).resolves.toBe(byHeader);
    await expect(resolvePrimeSessionFile(sessionsDir, "unknown")).resolves.toBeUndefined();
    await expect(resolvePrimeSessionFile(sessionsDir, "  ")).resolves.toBeUndefined();
  });

  it("reads Prime's user settings and auth providers from the agent dir", async () => {
    const { root } = await makeSessionsDir();
    await writeFile(path.join(root, "settings.json"), JSON.stringify(PRIME_USER_SETTINGS));
    await writeFile(
      path.join(root, "auth.json"),
      JSON.stringify({ anthropic: { type: "oauth" }, cerebras: { type: "api_key" } }),
    );

    await expect(readPrimeUserSettings(root)).resolves.toEqual({
      defaultProvider: "cerebras",
      defaultModel: "qwen-3.8-27b",
      defaultThinkingLevel: "high",
      recentModels: ["cerebras/qwen-3.8-27b", "openai-codex/gpt-5.6-sol", "missing/model"],
    });
    await expect(readPrimeAuthProviders(root)).resolves.toEqual(["anthropic", "cerebras"]);
    await expect(readPrimeUserSettings(path.join(root, "missing"))).resolves.toEqual({
      recentModels: [],
    });
    await expect(readPrimeAuthProviders(path.join(root, "missing"))).resolves.toEqual([]);
  });
});
