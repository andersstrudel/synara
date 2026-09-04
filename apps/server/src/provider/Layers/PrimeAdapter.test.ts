// FILE: PrimeAdapter.test.ts
// Purpose: Adapter/runtime contract tests for Prime Agent session start, resume,
// turn lifecycle, compaction, and RPC discovery.
// Layer: Provider adapter tests

import {
  appendFileSync,
  chmodSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import type * as Acp from "@agentclientprotocol/sdk";
import { ApprovalRequestId, type ProviderRuntimeEvent, ThreadId } from "@synara/contracts";
import { Deferred, Effect, Fiber, Layer, Semaphore, Stream } from "effect";
import { afterEach, describe, expect, it } from "vitest";

import { ServerConfig } from "../../config.ts";
import * as AcpErrors from "../acp/AcpErrors.ts";
import type { AcpSessionRuntimeShape } from "../acp/AcpSessionRuntime.ts";
import { makePrimeAcpRuntime, type PrimeRpcDiscoveryResult } from "../acp/PrimeAcpSupport.ts";
import { PrimeAdapter } from "../Services/PrimeAdapter.ts";
import {
  buildPrimeResumeCursor,
  makeCachedPrimeModelDiscovery,
  makePrimeAdapterLive,
  parsePrimeResume,
  resolvePrimeAdapterTimeouts,
  resolvePrimeStartModel,
  validatePrimeRuntimeMode,
} from "./PrimeAdapter.ts";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const mockAgentPath = path.join(__dirname, "../../../scripts/acp-mock-agent.ts");

const FAST_TIMEOUTS = {
  turnIdleMs: 30_000,
  toolIdleMs: 60_000,
  sessionDetectMs: 20,
  resumeStartRetryDelayMs: 10,
};

interface JsonRpcRequest {
  readonly method?: string;
  readonly params?: Record<string, unknown>;
}

interface PrimeMockAgent {
  readonly tempDir: string;
  readonly binaryPath: string;
  readonly agentDir: string;
  readonly sessionsDir: string;
  readonly cwd: string;
  readArgs: () => ReadonlyArray<ReadonlyArray<string>>;
  readRequests: () => ReadonlyArray<JsonRpcRequest>;
  /** Header ids of every session file in the sessions dir (one per fresh spawn). */
  readSessionIds: () => ReadonlyArray<string>;
}

const tempDirs: string[] = [];
afterEach(() => {
  for (const dir of tempDirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

function readJsonLines<T>(filePath: string): ReadonlyArray<T> {
  let content: string;
  try {
    content = readFileSync(filePath, "utf8");
  } catch {
    return [];
  }
  return content
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line) as T);
}

/**
 * Stands in for `prime-agent`: records its argv, mimics Prime writing a
 * session header with a fresh id at process start (unless resuming; the
 * header id differs from the file basename like the real CLI), then execs the
 * shared ACP mock agent with `session/load` disabled like the real CLI.
 *
 * With a `sessionScript`, the wrapper also relays stdin and, each time a
 * `session/prompt` passes through, appends `sessionScript[promptIndex]` to the
 * session file before the agent sees the prompt — the way Prime persists
 * usage entries while a turn is running.
 */
function makePrimeMockAgent(
  options: { readonly sessionScript?: ReadonlyArray<ReadonlyArray<unknown>> } = {},
): PrimeMockAgent {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), "synara-prime-adapter-"));
  tempDirs.push(tempDir);
  let sessionScriptPath: string | null = null;
  if (options.sessionScript !== undefined) {
    sessionScriptPath = path.join(tempDir, "session-script.json");
    writeFileSync(sessionScriptPath, JSON.stringify(options.sessionScript), "utf8");
  }
  const agentDir = path.join(tempDir, "agent");
  const sessionsDir = path.join(agentDir, "sessions");
  const cwd = path.join(tempDir, "work");
  const binDir = path.join(tempDir, "bin");
  mkdirSync(sessionsDir, { recursive: true });
  mkdirSync(cwd, { recursive: true });
  mkdirSync(binDir, { recursive: true });
  const argsLogPath = path.join(tempDir, "args.jsonl");
  const requestLogPath = path.join(tempDir, "requests.jsonl");
  const wrapperPath = path.join(binDir, "prime-agent.mjs");
  writeFileSync(
    wrapperPath,
    [
      'import { spawn } from "node:child_process";',
      'import { appendFileSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";',
      'import path from "node:path";',
      "const args = process.argv.slice(2);",
      `appendFileSync(${JSON.stringify(argsLogPath)}, JSON.stringify(args) + "\\n");`,
      'const cwd = args[args.indexOf("--cwd") + 1];',
      'const sessionsDir = path.join(process.env.PRIME_AGENT_CODING_AGENT_DIR, "sessions");',
      `const sessionScriptPath = ${JSON.stringify(sessionScriptPath)};`,
      "let sessionFile;",
      'if (!args.includes("--resume")) {',
      "  mkdirSync(sessionsDir, { recursive: true });",
      "  const sessionUuid = crypto.randomUUID();",
      "  sessionFile = path.join(sessionsDir, `${sessionUuid}.jsonl`);",
      "  writeFileSync(",
      "    sessionFile,",
      '    JSON.stringify({ type: "session", version: 3, id: `prime-header-${sessionUuid}`, timestamp: new Date().toISOString(), cwd, rlmDepth: 0 }) + "\\n",',
      "  );",
      "} else if (sessionScriptPath !== null) {",
      "  // A resumed session appends to the file whose header id matches.",
      '  const resumeId = args[args.indexOf("--resume") + 1];',
      "  for (const name of readdirSync(sessionsDir)) {",
      '    if (!name.endsWith(".jsonl")) continue;',
      "    const candidate = path.join(sessionsDir, name);",
      '    const [firstLine] = readFileSync(candidate, "utf8").split("\\n");',
      "    if (JSON.parse(firstLine).id === resumeId) {",
      "      sessionFile = candidate;",
      "      break;",
      "    }",
      "  }",
      "}",
      `const child = spawn(process.execPath, [${JSON.stringify(mockAgentPath)}], {`,
      '  stdio: [sessionScriptPath === null ? "inherit" : "pipe", "inherit", "inherit"],',
      `  env: { ...process.env, SYNARA_ACP_REQUEST_LOG_PATH: ${JSON.stringify(requestLogPath)}, SYNARA_ACP_SUPPORT_SESSION_LOAD: "0" },`,
      "});",
      "if (sessionScriptPath !== null) {",
      '  const script = JSON.parse(readFileSync(sessionScriptPath, "utf8"));',
      "  let promptIndex = 0;",
      '  let pending = "";',
      '  process.stdin.on("data", (chunk) => {',
      '    pending += chunk.toString("utf8");',
      '    let newline = pending.indexOf("\\n");',
      "    while (newline !== -1) {",
      "      const line = pending.slice(0, newline + 1);",
      "      pending = pending.slice(newline + 1);",
      '      if (line.includes(\'"method":"session/prompt"\')) {',
      "        const entries = script[promptIndex] ?? [];",
      "        promptIndex += 1;",
      "        if (sessionFile !== undefined && entries.length > 0) {",
      '          appendFileSync(sessionFile, entries.map((entry) => JSON.stringify(entry) + "\\n").join(""));',
      "        }",
      "      }",
      "      child.stdin.write(line);",
      '      newline = pending.indexOf("\\n");',
      "    }",
      "  });",
      '  process.stdin.on("end", () => child.stdin.end());',
      "}",
      'process.on("SIGTERM", () => child.kill("SIGTERM"));',
      'child.on("exit", (code) => process.exit(code ?? 0));',
      "",
    ].join("\n"),
    "utf8",
  );
  const binaryPath = path.join(binDir, "prime-agent");
  writeFileSync(
    binaryPath,
    `#!/bin/sh\nexec ${JSON.stringify(process.execPath)} ${JSON.stringify(wrapperPath)} "$@"\n`,
    "utf8",
  );
  chmodSync(binaryPath, 0o755);
  return {
    tempDir,
    binaryPath,
    agentDir,
    sessionsDir,
    cwd,
    readArgs: () => readJsonLines<ReadonlyArray<string>>(argsLogPath),
    readRequests: () => readJsonLines<JsonRpcRequest>(requestLogPath),
    readSessionIds: () =>
      readdirSync(sessionsDir)
        .filter((name) => name.endsWith(".jsonl"))
        .flatMap((name) => {
          const header = readJsonLines<{ readonly id?: string }>(path.join(sessionsDir, name))[0];
          return header?.id === undefined ? [] : [header.id];
        }),
  };
}

/** The header id of the single session file a fresh mock spawn wrote. */
function readSoleSessionId(agent: PrimeMockAgent): string {
  const sessionIds = agent.readSessionIds();
  const [sessionId] = sessionIds;
  if (sessionId === undefined || sessionIds.length > 1) {
    throw new Error(`Expected exactly one Prime session file, found ${sessionIds.length}`);
  }
  return sessionId;
}

function writePrimeSessionFile(agent: PrimeMockAgent, sessionId: string): string {
  const file = path.join(agent.sessionsDir, `${crypto.randomUUID()}.jsonl`);
  writeFileSync(
    file,
    `${JSON.stringify({
      type: "session",
      version: 3,
      id: sessionId,
      timestamp: new Date().toISOString(),
      cwd: agent.cwd,
      rlmDepth: 0,
    })}\n`,
    "utf8",
  );
  return file;
}

function makePrimeAdapterTestLayer(
  settings: { readonly binaryPath?: string; readonly agentDir?: string },
  options: Parameters<typeof makePrimeAdapterLive>[1] = {},
) {
  return makePrimeAdapterLive(settings, { timeouts: FAST_TIMEOUTS, ...options }).pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "prime-adapter-test-" })),
    Layer.provideMerge(NodeServices.layer),
  );
}

function makeLifecycleAcpRuntime(
  prompt: AcpSessionRuntimeShape["prompt"] = () =>
    Effect.succeed({ stopReason: "end_turn" } as Acp.PromptResponse),
  onCancel: () => void = () => undefined,
): AcpSessionRuntimeShape {
  const registerHandler = () => Effect.void;
  return {
    handleRequestPermission: registerHandler,
    handleElicitation: registerHandler,
    handleReadTextFile: registerHandler,
    handleWriteTextFile: registerHandler,
    handleCreateTerminal: registerHandler,
    handleTerminalOutput: registerHandler,
    handleTerminalWaitForExit: registerHandler,
    handleTerminalKill: registerHandler,
    handleTerminalRelease: registerHandler,
    handleSessionUpdate: registerHandler,
    handleElicitationComplete: registerHandler,
    handleExtRequest: registerHandler,
    handleExtNotification: registerHandler,
    start: () =>
      Effect.succeed({
        sessionId: "prime-acp-session",
        initializeResult: {} as Acp.InitializeResponse,
        sessionSetupResult: {} as Acp.NewSessionResponse,
        modelConfigId: undefined,
        sessionSetupMethod: "new",
      }),
    awaitExit: Effect.never,
    getEvents: () => Stream.never,
    sessionUpdatesEnqueuedCount: Effect.succeed(0),
    supportsSessionFork: Effect.succeed(false),
    supportsSessionRecovery: Effect.succeed(false),
    getModeState: Effect.succeed(undefined),
    getSessionEpoch: () => Effect.succeed(0 as never),
    getPendingSessionNotificationCount: () => Effect.succeed(0),
    getConfigOptions: Effect.succeed([]),
    getAvailableCommands: Effect.succeed([]),
    awaitLoadReplayReady: Effect.void,
    prompt,
    cancel: Effect.sync(onCancel),
    setMode: () => Effect.succeed({} as Acp.SetSessionModeResponse),
    setConfigOption: () => Effect.succeed({} as Acp.SetSessionConfigOptionResponse),
    setModel: () => Effect.void,
    forkSession: () => Effect.succeed({} as Acp.ForkSessionResponse),
    request: () => Effect.succeed({}),
    notify: () => Effect.void,
  } as AcpSessionRuntimeShape;
}

function collectRuntimeEvents(stream: Stream.Stream<ProviderRuntimeEvent>) {
  const events: ProviderRuntimeEvent[] = [];
  const collector = Stream.runForEach(stream, (event) =>
    Effect.sync(() => {
      events.push(event);
    }),
  ).pipe(Effect.forkChild);
  const waitFor = (predicate: (event: ProviderRuntimeEvent) => boolean, timeoutMs = 15_000) =>
    Effect.gen(function* () {
      const startedAt = Date.now();
      while (!events.some(predicate)) {
        if (Date.now() - startedAt > timeoutMs) {
          throw new Error(
            `Timed out waiting for runtime event. Seen: ${events.map((event) => event.type).join(", ")}`,
          );
        }
        yield* Effect.sleep(20);
      }
      return events.find(predicate)!;
    });
  return { events, collector, waitFor };
}

function promptTexts(requests: ReadonlyArray<JsonRpcRequest>): string[] {
  return requests
    .filter((request) => request.method === "session/prompt")
    .map((request) => {
      const prompt = (request.params?.prompt ?? []) as ReadonlyArray<{
        readonly type: string;
        readonly text?: string;
      }>;
      return prompt.at(-1)?.text ?? "";
    });
}

function rpcDiscoveryStub(
  responses: ReadonlyArray<readonly [string, unknown]>,
  onCall: () => void = () => undefined,
): (input: unknown) => Effect.Effect<PrimeRpcDiscoveryResult> {
  return () =>
    Effect.sync(() => {
      onCall();
      return {
        responses: new Map(responses.map(([id, data]) => [id, { id, success: true, data }])),
        stderr: "",
        exitCode: 0,
      };
    });
}

describe("resolvePrimeAdapterTimeouts", () => {
  it("uses the production defaults when overrides are absent", () => {
    expect(resolvePrimeAdapterTimeouts({})).toEqual({
      turnIdleMs: 30 * 60 * 1000,
      toolIdleMs: 60 * 60 * 1000,
      sessionDetectMs: 4_000,
      resumeStartRetryDelayMs: 1_500,
    });
  });

  it("uses valid environment overrides", () => {
    expect(
      resolvePrimeAdapterTimeouts({
        SYNARA_PRIME_TURN_IDLE_TIMEOUT_MS: "1234",
        SYNARA_PRIME_TOOL_IDLE_TIMEOUT_MS: "5678",
      }),
    ).toMatchObject({ turnIdleMs: 1234, toolIdleMs: 5678 });
  });
});

describe("Prime resume cursor", () => {
  it("round-trips the session header id", () => {
    expect(parsePrimeResume(buildPrimeResumeCursor("01a0-header"))).toEqual({
      sessionId: "01a0-header",
    });
    expect(parsePrimeResume({ schemaVersion: 2, sessionId: "01a0-header" })).toBeUndefined();
    expect(parsePrimeResume({ schemaVersion: 1, sessionId: "  " })).toBeUndefined();
    expect(parsePrimeResume("01a0-header")).toBeUndefined();
  });
});

describe("resolvePrimeStartModel", () => {
  it("formats the selection as provider/id with the thinking level", () => {
    expect(
      resolvePrimeStartModel({
        model: "cerebras/qwen-3.8-27b",
        options: { thinkingLevel: "xhigh" },
      }),
    ).toBe("cerebras/qwen-3.8-27b:xhigh");
    expect(resolvePrimeStartModel({ model: "anthropic/claude-fable-5-1" })).toBe(
      "anthropic/claude-fable-5-1",
    );
    expect(resolvePrimeStartModel(undefined)).toBeUndefined();
  });
});

describe("validatePrimeRuntimeMode", () => {
  it("fails closed for approval-required and accepts the other modes", async () => {
    await expect(
      Effect.runPromise(validatePrimeRuntimeMode("approval-required")),
    ).rejects.toMatchObject({ _tag: "ProviderAdapterValidationError", operation: "startSession" });
    await expect(Effect.runPromise(validatePrimeRuntimeMode("auto"))).resolves.toBeUndefined();
    await expect(
      Effect.runPromise(validatePrimeRuntimeMode("full-access")),
    ).resolves.toBeUndefined();
  });
});

describe("makeCachedPrimeModelDiscovery", () => {
  it("caches successful discovery per binary and agent dir", async () => {
    let discoveryCalls = 0;
    const results = await Effect.runPromise(
      Effect.gen(function* () {
        const discoveryLock = yield* Semaphore.make(1);
        const discoverModels = makeCachedPrimeModelDiscovery({
          discoveryLock,
          discover: () =>
            Effect.sync(() => {
              discoveryCalls += 1;
              return { models: [], source: "prime-rpc", cached: false };
            }),
        });
        return [
          yield* discoverModels({ binaryPath: " prime-agent ", agentDir: undefined }),
          yield* discoverModels({ binaryPath: "prime-agent", agentDir: undefined }),
          yield* discoverModels({ binaryPath: "prime-agent", agentDir: "/other/agent" }),
        ];
      }),
    );

    expect(discoveryCalls).toBe(2);
    expect(results.map((result) => result.cached)).toEqual([false, true, false]);
  });

  it("retries discovery after an error result and coalesces concurrent calls", async () => {
    let discoveryCalls = 0;
    const results = await Effect.runPromise(
      Effect.gen(function* () {
        const discoveryLock = yield* Semaphore.make(1);
        const discoverModels = makeCachedPrimeModelDiscovery({
          discoveryLock,
          discover: () =>
            Effect.gen(function* () {
              discoveryCalls += 1;
              yield* Effect.sleep(10);
              return discoveryCalls === 1
                ? { models: [], source: "prime.unavailable", cached: false, error: "not ready" }
                : { models: [], source: "prime-rpc", cached: false };
            }),
        });
        const target = { binaryPath: "prime-agent", agentDir: undefined };
        const first = yield* discoverModels(target);
        const rest = yield* Effect.all([discoverModels(target), discoverModels(target)], {
          concurrency: "unbounded",
        });
        return [first, ...rest];
      }),
    );

    expect(discoveryCalls).toBe(2);
    expect(results[0]?.error).toBe("not ready");
    expect(results.slice(1).map((result) => result.cached)).toEqual([false, true]);
  });

  it("bypasses a fresh cache entry when forceReload is set", async () => {
    let discoveryCalls = 0;
    const results = await Effect.runPromise(
      Effect.gen(function* () {
        const discoveryLock = yield* Semaphore.make(1);
        const discoverModels = makeCachedPrimeModelDiscovery({
          discoveryLock,
          discover: () =>
            Effect.sync(() => {
              discoveryCalls += 1;
              return { models: [], source: "prime-rpc", cached: false };
            }),
        });
        const target = { binaryPath: "prime-agent", agentDir: undefined };
        return [
          yield* discoverModels(target),
          yield* discoverModels(target, { forceReload: true }),
          yield* discoverModels(target),
        ];
      }),
    );

    expect(discoveryCalls).toBe(2);
    expect(results.map((result) => result.cached)).toEqual([false, false, true]);
  });
});

describe("Prime adapter lifecycle (fake runtime)", () => {
  it("rejects approval-required before spawning anything", async () => {
    let runtimeCreated = false;
    const agent = makePrimeMockAgent();
    const layer = makePrimeAdapterTestLayer(
      { agentDir: agent.agentDir },
      {
        makeAcpRuntime: () =>
          Effect.sync(() => {
            runtimeCreated = true;
            return makeLifecycleAcpRuntime();
          }),
      },
    );
    await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const error = yield* adapter
          .startSession({
            provider: "prime",
            threadId: ThreadId.makeUnsafe("thread-prime-approval"),
            runtimeMode: "approval-required",
            cwd: agent.cwd,
          })
          .pipe(Effect.flip);
        expect(error).toMatchObject({
          _tag: "ProviderAdapterValidationError",
          operation: "startSession",
        });
        expect(runtimeCreated).toBe(false);
      }).pipe(Effect.provide(layer)),
    );
  });

  it("interrupts an active turn through session/cancel and rejects overlapping sends", async () => {
    const promptStarted = await Effect.runPromise(Deferred.make<void>());
    let cancelCalls = 0;
    const runtime = makeLifecycleAcpRuntime(
      () => Deferred.succeed(promptStarted, undefined).pipe(Effect.andThen(Effect.never)),
      () => {
        cancelCalls += 1;
      },
    );
    const agent = makePrimeMockAgent();
    await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const threadId = ThreadId.makeUnsafe("thread-prime-interrupt");
        yield* adapter.startSession({
          provider: "prime",
          threadId,
          runtimeMode: "full-access",
          cwd: agent.cwd,
        });
        const turn = yield* adapter.sendTurn({ threadId, input: "first", attachments: [] });
        yield* Deferred.await(promptStarted);
        const duplicateError = yield* adapter
          .sendTurn({ threadId, input: "second", attachments: [] })
          .pipe(Effect.flip);
        expect(duplicateError).toMatchObject({
          _tag: "ProviderAdapterValidationError",
          operation: "sendTurn",
        });
        expect(
          (yield* adapter.listSessions()).find((session) => session.threadId === threadId),
        ).toMatchObject({ status: "running", activeTurnId: turn.turnId });

        yield* adapter.interruptTurn(threadId, turn.turnId);
        const readySession = (yield* adapter.listSessions()).find(
          (session) => session.threadId === threadId,
        );
        expect(readySession?.status).toBe("ready");
        expect(readySession?.activeTurnId).toBeUndefined();
        expect(cancelCalls).toBe(1);
        yield* adapter.stopSession(threadId);
        expect(yield* adapter.hasSession(threadId)).toBe(false);
      }).pipe(
        Effect.provide(
          makePrimeAdapterTestLayer(
            { agentDir: agent.agentDir },
            { makeAcpRuntime: () => Effect.succeed(runtime) },
          ),
        ),
      ),
    );
  });

  it("retries a resumed start that fails with a process error, but never a fresh start", async () => {
    const agent = makePrimeMockAgent();
    writePrimeSessionFile(agent, "prime-header-retry");
    let attempts = 0;
    const layer = makePrimeAdapterTestLayer(
      { agentDir: agent.agentDir },
      {
        makeAcpRuntime: () =>
          Effect.suspend((): Effect.Effect<AcpSessionRuntimeShape, AcpErrors.AcpError> => {
            attempts += 1;
            return attempts < 3
              ? Effect.fail(
                  new AcpErrors.AcpRequestError({
                    code: -32000,
                    errorMessage: "Session worker is stopping",
                  }),
                )
              : Effect.succeed(makeLifecycleAcpRuntime());
          }),
      },
    );
    await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const threadId = ThreadId.makeUnsafe("thread-prime-resume-retry");
        const session = yield* adapter.startSession({
          provider: "prime",
          threadId,
          runtimeMode: "full-access",
          cwd: agent.cwd,
          resumeCursor: buildPrimeResumeCursor("prime-header-retry"),
        });
        expect(attempts).toBe(3);
        expect(session.resumeCursor).toEqual(buildPrimeResumeCursor("prime-header-retry"));
        yield* adapter.stopSession(threadId);

        // A fresh start has no session-worker lease to wait out; it fails at once.
        attempts = 0;
        const freshError = yield* adapter
          .startSession({
            provider: "prime",
            threadId: ThreadId.makeUnsafe("thread-prime-fresh-no-retry"),
            runtimeMode: "full-access",
            cwd: agent.cwd,
          })
          .pipe(Effect.flip);
        expect(freshError._tag).toBe("ProviderAdapterProcessError");
        expect(attempts).toBe(1);
      }).pipe(Effect.provide(layer)),
    );
  });

  it("surfaces the failure once the resumed-start retries are exhausted", async () => {
    const agent = makePrimeMockAgent();
    writePrimeSessionFile(agent, "prime-header-exhausted");
    let attempts = 0;
    const layer = makePrimeAdapterTestLayer(
      { agentDir: agent.agentDir },
      {
        makeAcpRuntime: () =>
          Effect.suspend(() => {
            attempts += 1;
            return Effect.fail(
              new AcpErrors.AcpRequestError({
                code: -32000,
                errorMessage: "Session worker is stopping",
              }),
            );
          }),
      },
    );
    await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const threadId = ThreadId.makeUnsafe("thread-prime-resume-exhausted");
        const error = yield* adapter
          .startSession({
            provider: "prime",
            threadId,
            runtimeMode: "full-access",
            cwd: agent.cwd,
            resumeCursor: buildPrimeResumeCursor("prime-header-exhausted"),
          })
          .pipe(Effect.flip);
        expect(error).toMatchObject({
          _tag: "ProviderAdapterProcessError",
          detail: "Session worker is stopping",
        });
        expect(attempts).toBe(3);
        expect(yield* adapter.hasSession(threadId)).toBe(false);
      }).pipe(Effect.provide(layer)),
    );
  });

  it("publishes the session id after the first turn when detection missed it at start", async () => {
    const agent = makePrimeMockAgent();
    await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const { collector, waitFor } = collectRuntimeEvents(adapter.streamEvents);
        const collectorFiber = yield* collector;
        const threadId = ThreadId.makeUnsafe("thread-prime-late-session-file");
        const session = yield* adapter.startSession({
          provider: "prime",
          threadId,
          runtimeMode: "full-access",
          cwd: agent.cwd,
        });
        // The fake runtime spawns nothing, so no session file exists yet and
        // the bounded post-start detection comes up empty.
        expect(session.resumeCursor).toBeUndefined();

        writePrimeSessionFile(agent, "prime-header-late");
        const firstTurn = yield* adapter.sendTurn({ threadId, input: "hello", attachments: [] });
        expect(firstTurn.resumeCursor).toBeUndefined();
        yield* waitFor(
          (event) => event.type === "turn.completed" && event.turnId === firstTurn.turnId,
        );
        expect(
          (yield* adapter.listSessions()).find((entry) => entry.threadId === threadId)
            ?.resumeCursor,
        ).toEqual(buildPrimeResumeCursor("prime-header-late"));

        const secondTurn = yield* adapter.sendTurn({ threadId, input: "again", attachments: [] });
        expect(secondTurn.resumeCursor).toEqual(buildPrimeResumeCursor("prime-header-late"));
        yield* waitFor(
          (event) => event.type === "turn.completed" && event.turnId === secondTurn.turnId,
        );

        yield* adapter.stopSession(threadId);
        yield* Fiber.interrupt(collectorFiber);
      }).pipe(
        Effect.provide(
          makePrimeAdapterTestLayer(
            { agentDir: agent.agentDir },
            { makeAcpRuntime: () => Effect.succeed(makeLifecycleAcpRuntime()) },
          ),
        ),
      ),
    );
  });

  it("answers unexpected permission and elicitation requests fail-closed", async () => {
    type PermissionHandler = Parameters<AcpSessionRuntimeShape["handleRequestPermission"]>[0];
    type ElicitationHandler = Parameters<AcpSessionRuntimeShape["handleElicitation"]>[0];
    let permissionHandler: PermissionHandler | undefined;
    let elicitationHandler: ElicitationHandler | undefined;
    const runtime: AcpSessionRuntimeShape = {
      ...makeLifecycleAcpRuntime(),
      handleRequestPermission: (handler) =>
        Effect.sync(() => {
          permissionHandler = handler;
        }),
      handleElicitation: (handler) =>
        Effect.sync(() => {
          elicitationHandler = handler;
        }),
    };
    const agent = makePrimeMockAgent();
    await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const threadId = ThreadId.makeUnsafe("thread-prime-no-approvals");
        yield* adapter.startSession({
          provider: "prime",
          threadId,
          runtimeMode: "full-access",
          cwd: agent.cwd,
        });
        if (permissionHandler === undefined || elicitationHandler === undefined) {
          throw new Error("Prime must register fail-closed ACP responders at start");
        }

        // prime-agent never sends these; a future release that did must not
        // hang on a request Synara has no UI for.
        expect(
          yield* permissionHandler({
            sessionId: "prime-acp-session",
            toolCall: { toolCallId: "call-1", title: "rm -rf build" },
            options: [{ optionId: "allow", name: "Allow", kind: "allow_once" }],
          } as Acp.RequestPermissionRequest),
        ).toEqual({ outcome: { outcome: "cancelled" } });
        expect(
          yield* elicitationHandler({
            sessionId: "prime-acp-session",
            mode: "form",
            message: "Which branch?",
          } as Acp.CreateElicitationRequest),
        ).toEqual({ action: "decline" });

        const approvalError = yield* adapter
          .respondToRequest(threadId, ApprovalRequestId.makeUnsafe("approval-1"), "accept")
          .pipe(Effect.flip);
        expect(approvalError).toMatchObject({
          _tag: "ProviderAdapterRequestError",
          method: "session/request_permission",
        });
        const userInputError = yield* adapter
          .respondToUserInput(threadId, ApprovalRequestId.makeUnsafe("input-1"), {})
          .pipe(Effect.flip);
        expect(userInputError).toMatchObject({
          _tag: "ProviderAdapterRequestError",
          method: "session/elicitation",
        });

        yield* adapter.stopSession(threadId);
      }).pipe(
        Effect.provide(
          makePrimeAdapterTestLayer(
            { agentDir: agent.agentDir },
            { makeAcpRuntime: () => Effect.succeed(runtime) },
          ),
        ),
      ),
    );
  });
});

describe("Prime RPC discovery through the adapter", () => {
  const modelsData = {
    models: [
      {
        id: "qwen-3.8-27b",
        name: "Qwen 3.8 27B",
        provider: "cerebras",
        reasoning: true,
        thinkingLevelMap: { xhigh: null, max: null },
        input: ["text", "image"],
        contextWindow: 131_072,
      },
      {
        id: "claude-fable-5-1",
        name: "Claude Fable 5.1",
        provider: "anthropic",
        reasoning: true,
        thinkingLevelMap: { off: null },
        input: ["text", "image"],
        contextWindow: 1_000_000,
      },
    ],
  };
  const stateData = { model: modelsData.models[1], thinkingLevel: "medium" };
  const commandsData = {
    commands: [{ name: "skill:websearch", description: "Search the web", source: "skill" }],
  };

  it("lists models from get_available_models/get_state and caches for five minutes", async () => {
    let discoveryCalls = 0;
    const agent = makePrimeMockAgent();
    const results = await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const first = yield* adapter.listModels!({ provider: "prime", agentDir: agent.agentDir });
        const second = yield* adapter.listModels!({ provider: "prime", agentDir: agent.agentDir });
        return [first, second];
      }).pipe(
        Effect.provide(
          makePrimeAdapterTestLayer(
            {},
            {
              runRpcDiscovery: rpcDiscoveryStub(
                [
                  ["models", modelsData],
                  ["state", stateData],
                ],
                () => {
                  discoveryCalls += 1;
                },
              ),
            },
          ),
        ),
      ),
    );

    expect(discoveryCalls).toBe(1);
    expect(results[0]).toMatchObject({ source: "prime-rpc", cached: false });
    expect(results[0]?.models.map((model) => model.slug)).toEqual([
      "anthropic/claude-fable-5-1",
      "cerebras/qwen-3.8-27b",
    ]);
    expect(results[0]?.models[0]).toMatchObject({
      description: "anthropic · 1M context · vision",
      defaultReasoningEffort: "medium",
    });
    expect(results[1]?.cached).toBe(true);
  });

  it("reports an unreachable CLI as an empty list with an error and no static fallback", async () => {
    let discoveryCalls = 0;
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        yield* adapter.listModels!({ provider: "prime", binaryPath: "/missing/prime-agent" });
        return yield* adapter.listModels!({
          provider: "prime",
          binaryPath: "/missing/prime-agent",
        });
      }).pipe(
        Effect.provide(
          makePrimeAdapterTestLayer(
            {},
            {
              runRpcDiscovery: () =>
                Effect.sync(() => {
                  discoveryCalls += 1;
                  return {
                    responses: new Map(),
                    stderr: "spawn /missing/prime-agent ENOENT",
                    exitCode: 1,
                  };
                }),
            },
          ),
        ),
      ),
    );

    // Failures are not cached, so the second call probes again.
    expect(discoveryCalls).toBe(2);
    expect(result).toMatchObject({
      models: [],
      source: "prime.unavailable",
      cached: false,
      error: "spawn /missing/prime-agent ENOENT",
    });
  });

  it("lists commands from get_commands behind the synthetic compact command", async () => {
    const seenInputs: Array<{ readonly cwd: string | undefined; readonly requests: unknown }> = [];
    const agent = makePrimeMockAgent();
    const results = await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const first = yield* adapter.listCommands!({ provider: "prime", cwd: agent.cwd });
        const second = yield* adapter.listCommands!({ provider: "prime", cwd: agent.cwd });
        return [first, second];
      }).pipe(
        Effect.provide(
          makePrimeAdapterTestLayer(
            {},
            {
              runRpcDiscovery: (input) => {
                seenInputs.push({ cwd: input.cwd, requests: input.requests });
                return rpcDiscoveryStub([["commands", commandsData]])(input);
              },
            },
          ),
        ),
      ),
    );

    expect(seenInputs).toEqual([
      { cwd: path.resolve(agent.cwd), requests: [{ id: "commands", type: "get_commands" }] },
    ]);
    expect(results[0]).toEqual({
      commands: [
        { name: "compact", description: "Compact the conversation context" },
        { name: "skill:websearch", description: "Search the web" },
      ],
      source: "prime-rpc",
      cached: false,
    });
    expect(results[1]?.cached).toBe(true);
  });

  it("lists native skills from the same get_commands answer and shares the command cache", async () => {
    let discoveryCalls = 0;
    const agent = makePrimeMockAgent();
    const skillsData = {
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
        { name: "review", source: "prompt" },
      ],
    };
    const results = await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const skills = yield* adapter.listSkills!({ provider: "prime", cwd: agent.cwd });
        const commands = yield* adapter.listCommands!({ provider: "prime", cwd: agent.cwd });
        return [skills, commands] as const;
      }).pipe(
        Effect.provide(
          makePrimeAdapterTestLayer(
            {},
            {
              runRpcDiscovery: rpcDiscoveryStub([["commands", skillsData]], () => {
                discoveryCalls += 1;
              }),
            },
          ),
        ),
      ),
    );

    expect(discoveryCalls).toBe(1);
    expect(results[0]).toEqual({
      skills: [
        {
          name: "refine",
          description: "Refine the harness",
          path: "/opt/prime-agent/dist/skills/refine/SKILL.md",
          enabled: true,
          scope: "prime",
        },
      ],
      source: "prime-rpc",
      cached: false,
    });
    expect(results[1]).toEqual({
      commands: [
        { name: "compact", description: "Compact the conversation context" },
        { name: "skill:refine", description: "Refine the harness" },
        { name: "review" },
      ],
      source: "prime-rpc",
      cached: true,
    });
  });

  it("re-runs command discovery when forceReload bypasses the shared cache", async () => {
    let discoveryCalls = 0;
    const agent = makePrimeMockAgent();
    const results = await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        return [
          yield* adapter.listCommands!({ provider: "prime", cwd: agent.cwd }),
          yield* adapter.listSkills!({ provider: "prime", cwd: agent.cwd, forceReload: true }),
          yield* adapter.listCommands!({ provider: "prime", cwd: agent.cwd }),
        ];
      }).pipe(
        Effect.provide(
          makePrimeAdapterTestLayer(
            {},
            {
              runRpcDiscovery: rpcDiscoveryStub([["commands", commandsData]], () => {
                discoveryCalls += 1;
              }),
            },
          ),
        ),
      ),
    );

    expect(discoveryCalls).toBe(2);
    expect(results.map((result) => result.cached)).toEqual([false, false, true]);
  });
});

describe("Prime adapter with the ACP mock agent", () => {
  it("starts fresh with --model, publishes the detected session id, sends turns, and compacts", async () => {
    const agent = makePrimeMockAgent();
    await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const { events, collector, waitFor } = collectRuntimeEvents(adapter.streamEvents);
        const collectorFiber = yield* collector;
        const threadId = ThreadId.makeUnsafe("thread-prime-fresh");

        const session = yield* adapter.startSession({
          provider: "prime",
          threadId,
          runtimeMode: "full-access",
          cwd: agent.cwd,
          modelSelection: {
            provider: "prime",
            model: "cerebras/qwen-3.8-27b",
            options: { thinkingLevel: "high" },
          },
        });

        const sessionId = readSoleSessionId(agent);
        expect(session.model).toBe("cerebras/qwen-3.8-27b");
        expect(session.resumeCursor).toEqual(buildPrimeResumeCursor(sessionId));
        expect(agent.readArgs()).toEqual([
          ["--mode", "acp", "--cwd", agent.cwd, "--model", "cerebras/qwen-3.8-27b:high"],
        ]);
        const started = yield* waitFor((event) => event.type === "thread.started");
        expect(started).toMatchObject({ payload: { providerThreadId: sessionId } });
        const sessionNew = agent.readRequests().find((request) => request.method === "session/new");
        expect(sessionNew?.params).toMatchObject({ cwd: agent.cwd, mcpServers: [] });
        expect(agent.readRequests().some((request) => request.method === "session/load")).toBe(
          false,
        );

        const turn = yield* adapter.sendTurn({ threadId, input: "hello prime", attachments: [] });
        expect(turn.resumeCursor).toEqual(buildPrimeResumeCursor(sessionId));
        const completed = yield* waitFor(
          (event) => event.type === "turn.completed" && event.turnId === turn.turnId,
        );
        expect(completed).toMatchObject({
          payload: { state: "completed", stopReason: "end_turn" },
        });
        expect(events.some((event) => event.type === "content.delta")).toBe(true);

        const planTurn = yield* adapter.sendTurn({
          threadId,
          input: "design it",
          attachments: [],
          interactionMode: "plan",
        });
        yield* waitFor(
          (event) => event.type === "turn.completed" && event.turnId === planTurn.turnId,
        );

        yield* adapter.compactThread!(threadId);
        yield* waitFor(
          (event) =>
            event.type === "thread.state.changed" &&
            (event.payload as { readonly state?: string }).state === "compacted",
        );

        const prompts = promptTexts(agent.readRequests());
        expect(prompts[0]).toBe("hello prime");
        expect(prompts[1]).toMatch(/^Prime Agent plan mode is active\./u);
        expect(prompts[1]).toContain("User request:\ndesign it");
        expect(prompts[2]).toBe("/compact");

        yield* adapter.stopSession(threadId);
        yield* waitFor((event) => event.type === "session.exited");
        yield* Fiber.interrupt(collectorFiber);
      }).pipe(
        Effect.provide(
          makePrimeAdapterTestLayer({ binaryPath: agent.binaryPath, agentDir: agent.agentDir }),
        ),
      ),
    );
  });

  it("resumes with --resume plus a fresh session/new and keeps the session id", async () => {
    const agent = makePrimeMockAgent();
    const resumeSessionId = "prime-header-resume";
    writePrimeSessionFile(agent, resumeSessionId);
    await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const threadId = ThreadId.makeUnsafe("thread-prime-resume");
        const input = {
          provider: "prime" as const,
          threadId,
          runtimeMode: "full-access" as const,
          cwd: agent.cwd,
          resumeCursor: buildPrimeResumeCursor(resumeSessionId),
        };
        const session = yield* adapter.startSession(input);

        expect(session.resumeCursor).toEqual(buildPrimeResumeCursor(resumeSessionId));
        expect(adapter.didResumeSession?.(input, session)).toBe(true);
        expect(agent.readArgs()).toEqual([
          ["--mode", "acp", "--cwd", agent.cwd, "--resume", resumeSessionId],
        ]);
        const methods = agent.readRequests().map((request) => request.method);
        expect(methods).toContain("session/new");
        expect(methods).not.toContain("session/load");
        expect(methods).not.toContain("authenticate");

        yield* adapter.stopSession(threadId);
      }).pipe(
        Effect.provide(
          makePrimeAdapterTestLayer({ binaryPath: agent.binaryPath, agentDir: agent.agentDir }),
        ),
      ),
    );
  });

  it("falls back to a fresh session when the resume target no longer exists", async () => {
    const agent = makePrimeMockAgent();
    await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const threadId = ThreadId.makeUnsafe("thread-prime-resume-missing");
        const input = {
          provider: "prime" as const,
          threadId,
          runtimeMode: "full-access" as const,
          cwd: agent.cwd,
          resumeCursor: buildPrimeResumeCursor("prime-header-deleted"),
        };
        const session = yield* adapter.startSession(input);

        expect(session.resumeCursor).toEqual(buildPrimeResumeCursor(readSoleSessionId(agent)));
        expect(adapter.didResumeSession?.(input, session)).toBe(false);
        expect(agent.readArgs()).toEqual([["--mode", "acp", "--cwd", agent.cwd]]);

        yield* adapter.stopSession(threadId);
      }).pipe(
        Effect.provide(
          makePrimeAdapterTestLayer({ binaryPath: agent.binaryPath, agentDir: agent.agentDir }),
        ),
      ),
    );
  });

  it("binds concurrent fresh starts in one cwd to distinct session files", async () => {
    const agent = makePrimeMockAgent();
    await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const { collector, waitFor } = collectRuntimeEvents(adapter.streamEvents);
        const collectorFiber = yield* collector;
        const threadIds = [
          ThreadId.makeUnsafe("thread-prime-concurrent-a"),
          ThreadId.makeUnsafe("thread-prime-concurrent-b"),
        ];

        // Both starts race inside the detection window; each Prime process
        // writes its own session file for the same cwd.
        const sessions = yield* Effect.all(
          threadIds.map((threadId) =>
            adapter.startSession({
              provider: "prime",
              threadId,
              runtimeMode: "full-access",
              cwd: agent.cwd,
            }),
          ),
          { concurrency: "unbounded" },
        );

        const publishedIds = sessions.map(
          (session) => parsePrimeResume(session.resumeCursor)?.sessionId,
        );
        expect(new Set(publishedIds).size).toBe(2);
        expect(publishedIds.toSorted()).toEqual(agent.readSessionIds().toSorted());
        for (const [index, threadId] of threadIds.entries()) {
          const started = yield* waitFor(
            (event) => event.type === "thread.started" && event.threadId === threadId,
          );
          expect(started).toMatchObject({ payload: { providerThreadId: publishedIds[index] } });
        }

        yield* Effect.forEach(threadIds, (threadId) => adapter.stopSession(threadId));
        yield* Fiber.interrupt(collectorFiber);
      }).pipe(
        Effect.provide(
          makePrimeAdapterTestLayer({ binaryPath: agent.binaryPath, agentDir: agent.agentDir }),
        ),
      ),
    );
  });

  it("lets fresh starts in different cwds proceed without waiting on each other", async () => {
    const agent = makePrimeMockAgent();
    // Prime keeps every project's sessions in one dir; the other cwd shares it.
    const otherCwd = path.join(agent.tempDir, "work-other");
    mkdirSync(otherCwd, { recursive: true });
    const completed: string[] = [];
    await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const start = (threadId: ThreadId, cwd: string) =>
          Effect.gen(function* () {
            const session = yield* adapter.startSession({
              provider: "prime",
              threadId,
              runtimeMode: "full-access",
              cwd,
            });
            completed.push(cwd);
            return session;
          });

        // The start in the mock's own cwd spawns nothing, so it holds its
        // session lock for the whole detection window before giving up.
        const stalledThreadId = ThreadId.makeUnsafe("thread-prime-lock-stalled");
        const stalled = yield* Effect.forkChild(start(stalledThreadId, agent.cwd));
        yield* Effect.sleep(100);
        // A start in another cwd binds its own file at once.
        const otherThreadId = ThreadId.makeUnsafe("thread-prime-lock-other");
        const other = yield* start(otherThreadId, otherCwd);
        expect(completed).toEqual([otherCwd]);
        expect(parsePrimeResume(other.resumeCursor)?.sessionId).toBe(readSoleSessionId(agent));

        const stalledSession = yield* Fiber.join(stalled);
        expect(stalledSession.resumeCursor).toBeUndefined();
        expect(completed).toEqual([otherCwd, agent.cwd]);

        yield* adapter.stopSession(otherThreadId);
        yield* adapter.stopSession(stalledThreadId);
      }).pipe(
        Effect.provide(
          makePrimeAdapterTestLayer(
            { binaryPath: agent.binaryPath, agentDir: agent.agentDir },
            {
              timeouts: { ...FAST_TIMEOUTS, sessionDetectMs: 2_000 },
              makeAcpRuntime: (input) =>
                input.cwd === agent.cwd
                  ? Effect.succeed(makeLifecycleAcpRuntime())
                  : makePrimeAcpRuntime(input),
            },
          ),
        ),
      ),
    );
  });
});

// Synthetic Prime session v3 entries for the session-file usage tests below.
function usageBlock(input: number, output: number, costTotal: number) {
  return {
    input,
    output,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: input + output,
    cost: { input: costTotal, output: 0, cacheRead: 0, cacheWrite: 0, total: costTotal },
  };
}

function userEntry(id: string, parentId: string | null, text: string) {
  return {
    type: "message",
    id,
    parentId,
    timestamp: new Date().toISOString(),
    message: { role: "user", content: [{ type: "text", text }], timestamp: Date.now() },
  };
}

function assistantEntry(
  id: string,
  parentId: string,
  usage: ReturnType<typeof usageBlock>,
  content: ReadonlyArray<Record<string, unknown>> = [{ type: "text", text: "Hi there" }],
) {
  return {
    type: "message",
    id,
    parentId,
    timestamp: new Date().toISOString(),
    message: {
      role: "assistant",
      content,
      api: "openai-completions",
      provider: "cerebras",
      model: "qwen-3.8-27b",
      stopReason: "stop",
      responseId: `resp-${id}`,
      timestamp: Date.now(),
      usage,
    },
  };
}

function compactionEntry(id: string, parentId: string, firstKeptEntryId: string) {
  return {
    type: "compaction",
    id,
    parentId,
    timestamp: new Date().toISOString(),
    firstKeptEntryId,
    tokensBefore: 7_531,
    summary: "S".repeat(400),
    details: { readFiles: [], modifiedFiles: [] },
    fromHook: false,
  };
}

function usageEvents(events: ReadonlyArray<ProviderRuntimeEvent>) {
  return events.flatMap((event) => (event.type === "thread.token-usage.updated" ? [event] : []));
}

describe("Prime session-file usage through the adapter", () => {
  // Prime's ACP mode never sends usage_update; the adapter tails the session
  // file instead. Entries below are synthetic but in Prime's session v3 shape.
  const registryData = {
    models: [
      {
        id: "qwen-3.8-27b",
        name: "Qwen 3.8 27B",
        provider: "cerebras",
        reasoning: true,
        input: ["text"],
        contextWindow: 131_072,
      },
    ],
  };
  const discoveryStub = () => rpcDiscoveryStub([["models", registryData]]);

  it("reports exact context usage during a turn with the registry's context window and cost", async () => {
    const agent = makePrimeMockAgent({
      sessionScript: [
        [
          userEntry("u1", null, "hello prime"),
          assistantEntry("a1", "u1", usageBlock(7_153, 378, 0.0471), [
            { type: "text", text: "Reading" },
            { type: "toolCall", id: "call-1", name: "read", arguments: { path: "README.md" } },
          ]),
        ],
      ],
    });
    await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const { events, collector, waitFor } = collectRuntimeEvents(adapter.streamEvents);
        const collectorFiber = yield* collector;
        const threadId = ThreadId.makeUnsafe("thread-prime-usage-turn");
        yield* adapter.startSession({
          provider: "prime",
          threadId,
          runtimeMode: "full-access",
          cwd: agent.cwd,
          modelSelection: { provider: "prime", model: "cerebras/qwen-3.8-27b" },
        });
        yield* waitFor((event) => event.type === "thread.started");
        expect(usageEvents(events)).toHaveLength(0);

        const turn = yield* adapter.sendTurn({ threadId, input: "hello prime", attachments: [] });
        const usage = yield* waitFor(
          (event) => event.type === "thread.token-usage.updated" && event.turnId === turn.turnId,
        );
        if (usage.type !== "thread.token-usage.updated") throw new Error("unreachable");
        expect(usage.payload.usage).toEqual({
          usedTokens: 7_531,
          usedPercent: (7_531 / 131_072) * 100,
          maxTokens: 131_072,
          totalProcessedTokens: 7_531,
          inputTokens: 7_153,
          cachedInputTokens: 0,
          outputTokens: 378,
          lastUsedTokens: 7_531,
          lastInputTokens: 7_153,
          lastCachedInputTokens: 0,
          lastOutputTokens: 378,
          toolUses: 1,
          compactsAutomatically: true,
        });
        expect(usage.raw).toMatchObject({
          source: "prime.session-file.entry",
          method: "session/file",
        });
        // Raw payloads carry the entry minus its content.
        expect(usage.raw?.payload).toMatchObject({
          type: "message",
          id: "a1",
          message: { role: "assistant", contentBlocks: 2 },
        });

        const completed = yield* waitFor(
          (event) => event.type === "turn.completed" && event.turnId === turn.turnId,
        );
        expect(completed).toMatchObject({
          payload: { state: "completed", cumulativeCostUsd: 0.0471 },
        });
        // The turn's usage is published before its completion.
        expect(events.indexOf(usage)).toBeLessThan(events.indexOf(completed));
        expect(usageEvents(events)).toHaveLength(1);

        yield* adapter.stopSession(threadId);
        yield* waitFor((event) => event.type === "session.exited");
        yield* Fiber.interrupt(collectorFiber);
      }).pipe(
        Effect.provide(
          makePrimeAdapterTestLayer(
            { binaryPath: agent.binaryPath, agentDir: agent.agentDir },
            { runRpcDiscovery: discoveryStub() },
          ),
        ),
      ),
    );
  });

  it("bills sub-agent usage attributed to the turn into its totals and cost, not its context", async () => {
    const agent = makePrimeMockAgent({
      sessionScript: [
        [
          userEntry("u1", null, "hello prime"),
          assistantEntry("a1", "u1", usageBlock(7_153, 378, 0.0471)),
          // An RLM child ran under a1: Prime bills it to a1's usage block while
          // a1's totalTokens (the context reading) stays as it was.
          {
            type: "child_usage_attributed",
            id: "x1",
            parentId: "a1",
            timestamp: new Date().toISOString(),
            targetId: "a1",
            childUsage: usageBlock(9_000, 900, 0.02),
            aggregateUsage: { ...usageBlock(16_153, 1_278, 0.0671), totalTokens: 7_531 },
            origin: { kind: "rlm", depth: 1 },
          },
        ],
      ],
    });
    await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const { events, collector, waitFor } = collectRuntimeEvents(adapter.streamEvents);
        const collectorFiber = yield* collector;
        const threadId = ThreadId.makeUnsafe("thread-prime-usage-child");
        yield* adapter.startSession({
          provider: "prime",
          threadId,
          runtimeMode: "full-access",
          cwd: agent.cwd,
        });

        const turn = yield* adapter.sendTurn({ threadId, input: "hello prime", attachments: [] });
        const completed = yield* waitFor(
          (event) => event.type === "turn.completed" && event.turnId === turn.turnId,
        );
        expect(completed).toMatchObject({
          payload: { state: "completed", cumulativeCostUsd: 0.0471 + 0.02 },
        });
        const usage = usageEvents(events);
        expect(usage).toHaveLength(1);
        expect(usage[0]?.turnId).toBe(turn.turnId);
        expect(usage[0]?.payload.usage).toMatchObject({
          usedTokens: 7_531,
          totalProcessedTokens: 7_531 + 9_900,
          inputTokens: 7_153 + 9_000,
          outputTokens: 378 + 900,
          lastUsedTokens: 7_531,
        });

        yield* adapter.stopSession(threadId);
        yield* Fiber.interrupt(collectorFiber);
      }).pipe(
        Effect.provide(
          makePrimeAdapterTestLayer(
            { binaryPath: agent.binaryPath, agentDir: agent.agentDir },
            { runRpcDiscovery: discoveryStub() },
          ),
        ),
      ),
    );
  });

  it("reports a resumed thread's current context as soon as the session starts", async () => {
    const agent = makePrimeMockAgent();
    const resumeSessionId = "prime-header-usage-resume";
    const file = writePrimeSessionFile(agent, resumeSessionId);
    appendFileSync(
      file,
      [
        userEntry("u1", null, "hello prime"),
        assistantEntry("a1", "u1", usageBlock(7_153, 378, 0.0471)),
        userEntry("u2", "a1", "and again"),
        assistantEntry("a2", "u2", usageBlock(12_628, 471, 0.0132)),
      ]
        .map((entry) => `${JSON.stringify(entry)}\n`)
        .join(""),
    );
    await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const { events, collector, waitFor } = collectRuntimeEvents(adapter.streamEvents);
        const collectorFiber = yield* collector;
        const threadId = ThreadId.makeUnsafe("thread-prime-usage-resume");
        yield* adapter.startSession({
          provider: "prime",
          threadId,
          runtimeMode: "full-access",
          cwd: agent.cwd,
          resumeCursor: buildPrimeResumeCursor(resumeSessionId),
        });

        const started = yield* waitFor((event) => event.type === "thread.started");
        const usage = yield* waitFor((event) => event.type === "thread.token-usage.updated");
        if (usage.type !== "thread.token-usage.updated") throw new Error("unreachable");
        expect(usage.turnId).toBeUndefined();
        expect(usage.payload.usage).toMatchObject({
          usedTokens: 13_099,
          maxTokens: 131_072,
          totalProcessedTokens: 7_531 + 13_099,
          inputTokens: 7_153 + 12_628,
          outputTokens: 378 + 471,
          lastUsedTokens: 13_099,
          toolUses: 0,
        });
        expect(events.indexOf(started)).toBeLessThan(events.indexOf(usage));

        yield* adapter.stopSession(threadId);
        yield* Fiber.interrupt(collectorFiber);
      }).pipe(
        Effect.provide(
          makePrimeAdapterTestLayer(
            { binaryPath: agent.binaryPath, agentDir: agent.agentDir },
            { runRpcDiscovery: discoveryStub() },
          ),
        ),
      ),
    );
  });

  it("publishes a compaction row and estimate when Prime compacts on its own mid-turn", async () => {
    const agent = makePrimeMockAgent({
      sessionScript: [
        [
          userEntry("u1", null, "hello prime"),
          assistantEntry("a1", "u1", usageBlock(7_153, 378, 0.0471)),
          // Auto-compaction: Prime folds the context and keeps answering in the
          // same turn, so all of this lands in the file at once.
          compactionEntry("c1", "a1", "u1"),
          assistantEntry("a2", "c1", usageBlock(3_000, 100, 0.01)),
        ],
      ],
    });
    await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const { events, collector, waitFor } = collectRuntimeEvents(adapter.streamEvents);
        const collectorFiber = yield* collector;
        const threadId = ThreadId.makeUnsafe("thread-prime-usage-auto-compaction");
        yield* adapter.startSession({
          provider: "prime",
          threadId,
          runtimeMode: "full-access",
          cwd: agent.cwd,
        });

        const turn = yield* adapter.sendTurn({ threadId, input: "hello prime", attachments: [] });
        yield* waitFor((event) => event.type === "turn.completed" && event.turnId === turn.turnId);

        const compacted = events.find(
          (event) =>
            event.type === "item.completed" &&
            (event.payload as { readonly itemType?: string }).itemType === "context_compaction",
        );
        expect(compacted).toBeDefined();
        expect(compacted?.payload).toMatchObject({
          status: "completed",
          detail: "from 7,531 tokens",
        });
        // Peak reading, then the row, then the estimate, then the exact answer.
        const usage = usageEvents(events);
        expect(usage.map((event) => event.payload.usage.usedTokens)).toEqual([7_531, 105, 3_100]);
        expect(events.indexOf(compacted!)).toBeGreaterThan(events.indexOf(usage[0]!));
        expect(events.indexOf(compacted!)).toBeLessThan(events.indexOf(usage[1]!));
        expect(usage.every((event) => event.turnId === turn.turnId)).toBe(true);

        yield* adapter.stopSession(threadId);
        yield* Fiber.interrupt(collectorFiber);
      }).pipe(
        Effect.provide(
          makePrimeAdapterTestLayer(
            { binaryPath: agent.binaryPath, agentDir: agent.agentDir },
            { runRpcDiscovery: discoveryStub() },
          ),
        ),
      ),
    );
  });

  it("publishes the post-compaction estimate after the compaction outcome, then exact usage", async () => {
    const agent = makePrimeMockAgent({
      sessionScript: [
        [
          userEntry("u1", null, "hello prime"),
          assistantEntry("a1", "u1", usageBlock(7_153, 378, 0.0471)),
        ],
        // /compact: Prime appends the compaction entry while the prompt runs.
        [compactionEntry("c1", "a1", "u1")],
        [
          userEntry("u2", "c1", "continue"),
          assistantEntry("a2", "u2", usageBlock(9_000, 100, 0.02)),
        ],
      ],
    });
    await Effect.runPromise(
      Effect.gen(function* () {
        const adapter = yield* PrimeAdapter;
        const { events, collector, waitFor } = collectRuntimeEvents(adapter.streamEvents);
        const collectorFiber = yield* collector;
        const threadId = ThreadId.makeUnsafe("thread-prime-usage-compaction");
        yield* adapter.startSession({
          provider: "prime",
          threadId,
          runtimeMode: "full-access",
          cwd: agent.cwd,
        });

        const firstTurn = yield* adapter.sendTurn({
          threadId,
          input: "hello prime",
          attachments: [],
        });
        const firstCompleted = yield* waitFor(
          (event) => event.type === "turn.completed" && event.turnId === firstTurn.turnId,
        );
        const exactBefore = usageEvents(events);
        expect(exactBefore).toHaveLength(1);
        expect(exactBefore[0]?.payload.usage.usedTokens).toBe(7_531);

        yield* adapter.compactThread!(threadId);
        const compacted = yield* waitFor(
          (event) =>
            event.type === "thread.state.changed" &&
            (event.payload as { readonly state?: string }).state === "compacted",
        );
        // Summary (400 chars -> 100) + kept "hello prime" (3) + "Hi there" (2).
        const estimate = yield* waitFor(
          (event) =>
            event.type === "thread.token-usage.updated" && event.payload.usage.usedTokens === 105,
        );
        if (estimate.type !== "thread.token-usage.updated") throw new Error("unreachable");
        expect(estimate.turnId).toBeUndefined();
        expect(estimate.payload.usage).toMatchObject({
          usedTokens: 105,
          maxTokens: 131_072,
          totalProcessedTokens: 7_531,
          lastUsedTokens: 7_531,
        });
        // The estimate follows the compaction outcome (a completed compaction
        // invalidates whatever snapshot preceded it on the meter), and nothing
        // was published between the first turn and the outcome.
        expect(events.indexOf(estimate)).toBeGreaterThan(events.indexOf(compacted));
        expect(
          usageEvents(events.slice(events.indexOf(firstCompleted), events.indexOf(compacted))),
        ).toHaveLength(0);

        const secondTurn = yield* adapter.sendTurn({
          threadId,
          input: "continue",
          attachments: [],
        });
        const exact = yield* waitFor(
          (event) =>
            event.type === "thread.token-usage.updated" && event.turnId === secondTurn.turnId,
        );
        if (exact.type !== "thread.token-usage.updated") throw new Error("unreachable");
        expect(exact.payload.usage).toMatchObject({
          usedTokens: 9_100,
          maxTokens: 131_072,
          totalProcessedTokens: 7_531 + 9_100,
          lastUsedTokens: 9_100,
        });
        const secondCompleted = yield* waitFor(
          (event) => event.type === "turn.completed" && event.turnId === secondTurn.turnId,
        );
        expect(secondCompleted).toMatchObject({ payload: { cumulativeCostUsd: 0.0471 + 0.02 } });

        yield* adapter.stopSession(threadId);
        yield* Fiber.interrupt(collectorFiber);
      }).pipe(
        Effect.provide(
          makePrimeAdapterTestLayer(
            { binaryPath: agent.binaryPath, agentDir: agent.agentDir },
            { runRpcDiscovery: discoveryStub() },
          ),
        ),
      ),
    );
  });
});
