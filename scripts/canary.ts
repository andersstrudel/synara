// FILE: canary.ts
// Purpose: Maintains and launches an isolated, frozen Synara Canary checkout.
// Layer: Local developer tooling

import { spawn, spawnSync } from "node:child_process";
import * as FS from "node:fs";
import * as OS from "node:os";
import * as Path from "node:path";
import { fileURLToPath } from "node:url";

export type CanaryCommand = "setup" | "update" | "start" | "stop" | "status" | "rollback";

export interface CanaryPaths {
  readonly home: string;
  readonly source: string;
  readonly state: string;
  readonly pid: string;
  readonly log: string;
}

interface CanaryState {
  readonly currentCommit: string;
  readonly previousCommit: string | null;
  readonly trackedRef: string;
  readonly updatedAt: string;
}

interface ParsedCanaryArgs {
  readonly command: CanaryCommand;
  readonly ref: string | null;
}

const SCRIPT_PATH = fileURLToPath(import.meta.url);
const REPO_ROOT = Path.resolve(Path.dirname(SCRIPT_PATH), "..");
const DEFAULT_REF = "main";
const COMMIT_PATTERN = /^[0-9a-f]{40}$/iu;

export function resolveCanaryPaths(
  env: NodeJS.ProcessEnv = process.env,
  homeDirectory = OS.homedir(),
): CanaryPaths {
  const home = Path.resolve(
    env.SYNARA_CANARY_HOME?.trim() || Path.join(homeDirectory, ".synara-canary"),
  );
  const cacheBase = env.XDG_CACHE_HOME?.trim() || Path.join(homeDirectory, ".cache");
  const source = Path.resolve(
    env.SYNARA_CANARY_SOURCE?.trim() || Path.join(cacheBase, "synara-canary", "source"),
  );
  return {
    home,
    source,
    state: Path.join(home, "canary-state.json"),
    pid: Path.join(home, "canary.pid"),
    log: Path.join(home, "canary.log"),
  };
}

export function parseCanaryArgs(argv: ReadonlyArray<string>): ParsedCanaryArgs {
  const rawCommand = argv[0] ?? "status";
  if (
    !(["setup", "update", "start", "stop", "status", "rollback"] as const).includes(
      rawCommand as CanaryCommand,
    )
  ) {
    throw new Error(`Unknown Canary command: ${rawCommand}`);
  }
  let ref: string | null = null;
  for (let index = 1; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--ref") {
      const value = argv[index + 1]?.trim();
      if (!value) throw new Error("Missing value for --ref.");
      ref = value;
      index += 1;
      continue;
    }
    throw new Error(`Unknown Canary argument: ${String(argument)}`);
  }
  return { command: rawCommand as CanaryCommand, ref };
}

export function resolveCanaryRef(input: ParsedCanaryArgs, trackedRef: string | null): string {
  return input.ref ?? (input.command === "update" ? trackedRef : null) ?? DEFAULT_REF;
}

export function canaryCloneArgs(originUrl: string, source: string): ReadonlyArray<string> {
  // The cleanliness guard runs immediately after cloning. A --no-checkout clone
  // reports every tracked file as deleted, so it is indistinguishable from a
  // user-modified managed checkout at that point.
  return ["clone", "--", originUrl, source];
}

export function canaryStartArgs(): ReadonlyArray<string> {
  // Invoke the desktop launcher directly. `bun run --cwd apps/desktop start`
  // adds a short-lived package-script process in front of the launcher, so the
  // PID persisted by Canary goes stale while Electron is still running. The
  // direct launcher remains alive for Electron's lifetime and also preserves
  // Canary's flavor, home, updater policy, and commit identity.
  return ["apps/desktop/scripts/start-electron.mjs"];
}

const CANARY_DISPLAY_NAME = "Synara Canary";
const CANARY_LAUNCHER_BUNDLE_ID = "com.emanueledipietro.synara.canary.launcher";

/** The Electron bundle `apps/desktop/scripts/electron-launcher.mjs` builds for the Canary flavor. */
export function canaryRuntimeAppBundlePath(source: string): string {
  return Path.join(source, "apps/desktop/.electron-runtime", `${CANARY_DISPLAY_NAME}.app`);
}

export function canaryRuntimeExecutablePath(source: string): string {
  return Path.join(canaryRuntimeAppBundlePath(source), "Contents/MacOS/Electron");
}

/** Where the double-clickable app lives so Spotlight, Launchpad and the Dock can find it. */
export function canaryLauncherAppPath(homeDirectory = OS.homedir()): string {
  return Path.join(homeDirectory, "Applications", `${CANARY_DISPLAY_NAME}.app`);
}

function xmlEscape(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", `'\\''`)}'`;
}

// A thin app bundle whose executable execs the runtime bundle. The runtime
// bundle itself lives under the managed source checkout, which Spotlight and
// Launchpad do not index and which `canary:update` may replace; this wrapper
// keeps a stable, indexed entry point with the Synara icon.
export function canaryLauncherBundleFiles(input: { readonly runtimeExecutablePath: string }): {
  readonly infoPlist: string;
  readonly launcherScript: string;
} {
  const infoPlist = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key>
  <string>${xmlEscape(CANARY_DISPLAY_NAME)}</string>
  <key>CFBundleDisplayName</key>
  <string>${xmlEscape(CANARY_DISPLAY_NAME)}</string>
  <key>CFBundleIdentifier</key>
  <string>${CANARY_LAUNCHER_BUNDLE_ID}</string>
  <key>CFBundleExecutable</key>
  <string>${xmlEscape(CANARY_DISPLAY_NAME)}</string>
  <key>CFBundleIconFile</key>
  <string>icon.icns</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>CFBundleVersion</key>
  <string>1</string>
  <key>CFBundleShortVersionString</key>
  <string>1.0</string>
  <key>LSMinimumSystemVersion</key>
  <string>12.0</string>
  <key>NSHighResolutionCapable</key>
  <true/>
</dict>
</plist>
`;
  const launcherScript = `#!/bin/sh
# Generated by scripts/canary.ts. Opens the Synara Canary runtime bundle that
# \`bun run canary:setup\` builds; \`bun run canary:update\` refreshes it in place.
RUNTIME=${shellQuote(input.runtimeExecutablePath)}
if [ ! -x "$RUNTIME" ]; then
  osascript -e 'display alert "Synara Canary is not built" message "Run bun run canary:setup in the Synara repository, then open Synara Canary again."' >/dev/null 2>&1
  exit 1
fi
exec "$RUNTIME" "$@"
`;
  return { infoPlist, launcherScript };
}

function installCanaryLauncher(paths: CanaryPaths): string | null {
  if (process.platform !== "darwin") {
    return null;
  }
  const launcherPath = canaryLauncherAppPath();
  const contents = Path.join(launcherPath, "Contents");
  const files = canaryLauncherBundleFiles({
    runtimeExecutablePath: canaryRuntimeExecutablePath(paths.source),
  });
  FS.mkdirSync(Path.join(contents, "MacOS"), { recursive: true });
  FS.mkdirSync(Path.join(contents, "Resources"), { recursive: true });
  FS.writeFileSync(Path.join(contents, "Info.plist"), files.infoPlist);
  FS.writeFileSync(Path.join(contents, "MacOS", CANARY_DISPLAY_NAME), files.launcherScript, {
    mode: 0o755,
  });
  const iconPath = Path.join(paths.source, "apps/desktop/resources/icon.icns");
  if (FS.existsSync(iconPath)) {
    FS.copyFileSync(iconPath, Path.join(contents, "Resources", "icon.icns"));
  }
  // Nudge Launch Services so Spotlight and Launchpad pick the bundle up now
  // rather than on the next scan. Best effort: the bundle works without it.
  spawnSync("touch", [launcherPath], { stdio: "ignore" });
  return launcherPath;
}

function run(command: string, args: ReadonlyArray<string>, cwd: string): void {
  const result = spawnSync(command, [...args], {
    cwd,
    env: process.env,
    stdio: "inherit",
    shell: process.platform === "win32",
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} exited with ${String(result.status)}.`);
  }
}

function capture(command: string, args: ReadonlyArray<string>, cwd: string): string {
  const result = spawnSync(command, [...args], {
    cwd,
    env: process.env,
    encoding: "utf8",
    shell: process.platform === "win32",
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(
      `${command} ${args.join(" ")} exited with ${String(result.status)}: ${result.stderr.trim()}`,
    );
  }
  return result.stdout.trim();
}

function readState(paths: CanaryPaths): CanaryState | null {
  try {
    const state = JSON.parse(FS.readFileSync(paths.state, "utf8")) as Partial<CanaryState>;
    if (
      typeof state.currentCommit !== "string" ||
      !COMMIT_PATTERN.test(state.currentCommit) ||
      (state.previousCommit !== null &&
        (typeof state.previousCommit !== "string" || !COMMIT_PATTERN.test(state.previousCommit))) ||
      typeof state.trackedRef !== "string" ||
      typeof state.updatedAt !== "string"
    ) {
      return null;
    }
    return state as CanaryState;
  } catch {
    return null;
  }
}

function writeState(paths: CanaryPaths, state: CanaryState): void {
  FS.mkdirSync(paths.home, { recursive: true });
  const temporaryPath = `${paths.state}.tmp`;
  FS.writeFileSync(temporaryPath, `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });
  FS.renameSync(temporaryPath, paths.state);
}

function readPid(paths: CanaryPaths): number | null {
  try {
    const pid = Number(FS.readFileSync(paths.pid, "utf8").trim());
    return Number.isInteger(pid) && pid > 0 ? pid : null;
  } catch {
    return null;
  }
}

function isRunning(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function stopCanary(paths: CanaryPaths): void {
  const pid = readPid(paths);
  if (pid === null || !isRunning(pid)) {
    FS.rmSync(paths.pid, { force: true });
    stopCanaryRuntimeProcesses(paths);
    return;
  }
  if (process.platform === "win32") {
    spawnSync("taskkill", ["/PID", String(pid), "/T", "/F"], { stdio: "ignore" });
  } else {
    try {
      process.kill(-pid, "SIGTERM");
    } catch {
      process.kill(pid, "SIGTERM");
    }
  }
  FS.rmSync(paths.pid, { force: true });
  stopCanaryRuntimeProcesses(paths);
}

// The PID file only covers `canary:start`. Instances opened from the app icon,
// and an Electron main that has left the launcher's process group, are found
// by their executable path instead so an update never races a live build.
function stopCanaryRuntimeProcesses(paths: CanaryPaths): void {
  if (process.platform !== "darwin") {
    return;
  }
  spawnSync("pkill", ["-TERM", "-f", canaryRuntimeExecutablePath(paths.source)], {
    stdio: "ignore",
  });
}

function resolveOriginUrl(): string {
  return capture("git", ["remote", "get-url", "origin"], REPO_ROOT);
}

function ensureManagedSource(paths: CanaryPaths): void {
  if (FS.existsSync(Path.join(paths.source, ".git"))) return;
  if (FS.existsSync(paths.source)) {
    const entries = FS.readdirSync(paths.source);
    if (entries.length > 0) {
      throw new Error(`Canary source path exists but is not a Git checkout: ${paths.source}`);
    }
  }
  FS.mkdirSync(Path.dirname(paths.source), { recursive: true });
  run("git", canaryCloneArgs(resolveOriginUrl(), paths.source), REPO_ROOT);
}

function assertManagedSourceIsClean(paths: CanaryPaths): void {
  const status = capture("git", ["status", "--porcelain", "--untracked-files=no"], paths.source);
  if (status.length > 0) {
    throw new Error(
      `Synara Canary's managed source has tracked local changes. Refusing to overwrite ${paths.source}.`,
    );
  }
}

function fetchRef(paths: CanaryPaths, ref: string): string {
  run("git", ["fetch", "--prune", "origin", ref], paths.source);
  const commit = capture("git", ["rev-parse", "FETCH_HEAD"], paths.source);
  if (!COMMIT_PATTERN.test(commit)) throw new Error(`Invalid fetched commit: ${commit}`);
  return commit;
}

function checkout(paths: CanaryPaths, commit: string): void {
  run("git", ["checkout", "--detach", "--force", commit], paths.source);
}

function build(paths: CanaryPaths): void {
  run("bun", ["install", "--frozen-lockfile"], paths.source);
  run("bun", ["run", "build:desktop"], paths.source);
  run("bun", ["run", "release:smoke"], paths.source);
}

function currentSourceCommit(paths: CanaryPaths): string | null {
  if (!FS.existsSync(Path.join(paths.source, ".git"))) return null;
  try {
    const commit = capture("git", ["rev-parse", "HEAD"], paths.source);
    return COMMIT_PATTERN.test(commit) ? commit : null;
  } catch {
    return null;
  }
}

function startCanary(paths: CanaryPaths): void {
  const existingPid = readPid(paths);
  if (existingPid !== null && isRunning(existingPid)) {
    console.log(`Synara Canary is already running (pid ${String(existingPid)}).`);
    return;
  }
  const commit = currentSourceCommit(paths);
  if (
    commit === null ||
    !FS.existsSync(Path.join(paths.source, "apps/desktop/dist-electron/main.js"))
  ) {
    throw new Error("Synara Canary is not built. Run `bun run canary:setup` first.");
  }
  FS.mkdirSync(paths.home, { recursive: true });
  const launcherPath = installCanaryLauncher(paths);
  const env = { ...process.env };
  delete env.VITE_DEV_SERVER_URL;
  delete env.ELECTRON_RENDERER_PORT;
  delete env.SYNARA_AUTH_TOKEN;
  Object.assign(env, {
    SYNARA_DESKTOP_FLAVOR: "canary",
    SYNARA_DISABLE_AUTO_UPDATE: "1",
    SYNARA_HOME: paths.home,
    SYNARA_COMMIT_HASH: commit,
  });
  const logDescriptor = FS.openSync(paths.log, "a", 0o600);
  try {
    FS.writeSync(logDescriptor, `\n[${new Date().toISOString()}] Starting ${commit}\n`);
    const child = spawn("bun", [...canaryStartArgs()], {
      cwd: paths.source,
      env,
      detached: true,
      stdio: ["ignore", logDescriptor, logDescriptor],
      shell: process.platform === "win32",
    });
    if (child.pid === undefined) {
      throw new Error("Synara Canary failed to return a process id.");
    }
    child.unref();
    FS.writeFileSync(paths.pid, `${String(child.pid)}\n`, { mode: 0o600 });
    console.log(`Started Synara Canary at ${commit.slice(0, 12)} (pid ${String(child.pid)}).`);
    console.log(`Log: ${paths.log}`);
    if (launcherPath !== null) {
      console.log(`App: ${launcherPath}`);
    }
  } finally {
    FS.closeSync(logDescriptor);
  }
}

function updateCanary(paths: CanaryPaths, ref: string): void {
  ensureManagedSource(paths);
  assertManagedSourceIsClean(paths);
  const previousCommit = currentSourceCommit(paths);
  const targetCommit = fetchRef(paths, ref);
  if (
    previousCommit === targetCommit &&
    FS.existsSync(Path.join(paths.source, "apps/desktop/dist-electron/main.js"))
  ) {
    const previousState = readState(paths);
    writeState(paths, {
      currentCommit: targetCommit,
      previousCommit: previousState?.previousCommit ?? null,
      trackedRef: ref,
      updatedAt: new Date().toISOString(),
    });
    startCanary(paths);
    return;
  }
  stopCanary(paths);
  try {
    checkout(paths, targetCommit);
    build(paths);
  } catch (error) {
    if (previousCommit !== null && previousCommit !== targetCommit) {
      console.error(`Canary update failed; restoring ${previousCommit.slice(0, 12)}.`);
      checkout(paths, previousCommit);
      build(paths);
      startCanary(paths);
    }
    throw error;
  }
  const previousState = readState(paths);
  writeState(paths, {
    currentCommit: targetCommit,
    previousCommit:
      previousCommit !== null && previousCommit !== targetCommit
        ? previousCommit
        : (previousState?.previousCommit ?? null),
    trackedRef: ref,
    updatedAt: new Date().toISOString(),
  });
  startCanary(paths);
}

function rollbackCanary(paths: CanaryPaths): void {
  const state = readState(paths);
  if (state?.previousCommit === null || state?.previousCommit === undefined) {
    throw new Error("Synara Canary has no previous successful commit to restore.");
  }
  assertManagedSourceIsClean(paths);
  stopCanary(paths);
  const rollbackCommit = state.previousCommit;
  try {
    checkout(paths, rollbackCommit);
    build(paths);
  } catch (error) {
    console.error(`Canary rollback failed; restoring ${state.currentCommit.slice(0, 12)}.`);
    checkout(paths, state.currentCommit);
    build(paths);
    startCanary(paths);
    throw error;
  }
  writeState(paths, {
    currentCommit: rollbackCommit,
    previousCommit: state.currentCommit,
    trackedRef: state.trackedRef,
    updatedAt: new Date().toISOString(),
  });
  startCanary(paths);
}

function printStatus(paths: CanaryPaths): void {
  const state = readState(paths);
  const pid = readPid(paths);
  const running = pid !== null && isRunning(pid);
  console.log(`Synara Canary: ${running ? `running (pid ${String(pid)})` : "stopped"}`);
  console.log(`Source: ${paths.source}`);
  console.log(`Data: ${paths.home}`);
  console.log(`Log: ${paths.log}`);
  if (process.platform === "darwin") {
    console.log(`App: ${canaryLauncherAppPath()}`);
  }
  console.log(`Commit: ${state?.currentCommit ?? currentSourceCommit(paths) ?? "not installed"}`);
  console.log(`Tracked ref: ${state?.trackedRef ?? "not configured"}`);
}

export function runCanaryCommand(input: ParsedCanaryArgs, paths = resolveCanaryPaths()): void {
  if (input.command === "setup" || input.command === "update") {
    updateCanary(paths, resolveCanaryRef(input, readState(paths)?.trackedRef ?? null));
    return;
  }
  if (input.command === "start") {
    startCanary(paths);
    return;
  }
  if (input.command === "stop") {
    stopCanary(paths);
    return;
  }
  if (input.command === "rollback") {
    rollbackCanary(paths);
    return;
  }
  printStatus(paths);
}

const isMain = process.argv[1] !== undefined && Path.resolve(process.argv[1]) === SCRIPT_PATH;
if (isMain) {
  try {
    runCanaryCommand(parseCanaryArgs(process.argv.slice(2)));
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
