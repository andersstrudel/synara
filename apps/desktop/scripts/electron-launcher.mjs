// This file mostly exists because we want dev mode to say "Synara (Dev)" instead of "electron"

import { spawnSync } from "node:child_process";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import {
  resolveSynaraDesktopFlavor,
  SYNARA_SOURCE_DESKTOP_BUILD_MARKER,
  synaraDesktopIdentity,
} from "@synara/shared/desktopIdentity";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const desktopFlavor = resolveSynaraDesktopFlavor({
  // Packaged apps launch their bundled main directly; this launcher is source-only.
  isDevelopment: true,
  requestedFlavor: process.env.SYNARA_DESKTOP_FLAVOR,
});
const desktopIdentity = synaraDesktopIdentity(desktopFlavor);
const APP_DISPLAY_NAME = desktopIdentity.displayName;
const APP_BUNDLE_ID = desktopIdentity.bundleId;
const LAUNCHER_VERSION = 2;
const MICROPHONE_USAGE_DESCRIPTION =
  "Synara needs microphone access so you can record voice notes and transcribe them into the chat composer.";

const __dirname = dirname(fileURLToPath(import.meta.url));
export const desktopDir = resolve(__dirname, "..");

function setPlistString(plistPath, key, value) {
  const replaceResult = spawnSync("plutil", ["-replace", key, "-string", value, plistPath], {
    encoding: "utf8",
  });
  if (replaceResult.status === 0) {
    return;
  }

  const insertResult = spawnSync("plutil", ["-insert", key, "-string", value, plistPath], {
    encoding: "utf8",
  });
  if (insertResult.status === 0) {
    return;
  }

  const details = [replaceResult.stderr, insertResult.stderr].filter(Boolean).join("\n");
  throw new Error(`Failed to update plist key "${key}" at ${plistPath}: ${details}`.trim());
}

function patchMainBundleInfoPlist(appBundlePath, iconPath) {
  const infoPlistPath = join(appBundlePath, "Contents", "Info.plist");
  setPlistString(infoPlistPath, "CFBundleDisplayName", APP_DISPLAY_NAME);
  setPlistString(infoPlistPath, "CFBundleName", APP_DISPLAY_NAME);
  setPlistString(infoPlistPath, "CFBundleIdentifier", APP_BUNDLE_ID);
  setPlistString(infoPlistPath, "CFBundleIconFile", "icon.icns");
  setPlistString(infoPlistPath, "NSMicrophoneUsageDescription", MICROPHONE_USAGE_DESCRIPTION);

  const resourcesDir = join(appBundlePath, "Contents", "Resources");
  copyFileSync(iconPath, join(resourcesDir, "icon.icns"));
  copyFileSync(iconPath, join(resourcesDir, "electron.icns"));
}

function patchHelperBundleInfoPlists(appBundlePath) {
  const frameworksDir = join(appBundlePath, "Contents", "Frameworks");
  if (!existsSync(frameworksDir)) {
    return;
  }

  for (const entry of readdirSync(frameworksDir, { withFileTypes: true })) {
    if (!entry.isDirectory() || !entry.name.endsWith(".app")) {
      continue;
    }
    if (!entry.name.startsWith("Electron Helper")) {
      continue;
    }

    const helperPlistPath = join(frameworksDir, entry.name, "Contents", "Info.plist");
    if (!existsSync(helperPlistPath)) {
      continue;
    }

    const suffix = entry.name.replace("Electron Helper", "").replace(".app", "").trim();
    const helperName = suffix
      ? `${APP_DISPLAY_NAME} Helper ${suffix}`
      : `${APP_DISPLAY_NAME} Helper`;
    const helperIdSuffix = suffix.replace(/[()]/g, "").trim().toLowerCase().replace(/\s+/g, "-");
    const helperBundleId = helperIdSuffix
      ? `${APP_BUNDLE_ID}.helper.${helperIdSuffix}`
      : `${APP_BUNDLE_ID}.helper`;

    setPlistString(helperPlistPath, "CFBundleDisplayName", helperName);
    setPlistString(helperPlistPath, "CFBundleName", helperName);
    setPlistString(helperPlistPath, "CFBundleIdentifier", helperBundleId);
  }
}

function readJson(path) {
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}

// Electron loads `Contents/Resources/app` when the bundle is opened without
// arguments (Finder, Spotlight, the Dock). Without it the runtime bundle shows
// Electron's own "run a local app" splash instead of Synara. The generated entry
// recreates the working directory and environment that `start-electron.mjs`
// passes when it spawns Electron, then hands over to the built desktop main.
// It is rewritten on every launch so it never lags behind the source checkout.
export function writeRuntimeSelfLauncher(appBundlePath, options) {
  const appDir = join(appBundlePath, "Contents", "Resources", "app");
  mkdirSync(appDir, { recursive: true });
  writeFileSync(
    join(appDir, "package.json"),
    `${JSON.stringify({ name: "synara-runtime-launcher", private: true, main: "main.js" }, null, 2)}\n`,
  );
  writeFileSync(join(appDir, "main.js"), renderRuntimeSelfLauncher(options));
}

export function renderRuntimeSelfLauncher({
  desktopDirectory,
  flavor,
  displayName,
  homeDirectoryName,
  buildMarker = SYNARA_SOURCE_DESKTOP_BUILD_MARKER,
}) {
  const launch = {
    desktopDirectory,
    flavor,
    displayName,
    homeDirectoryName,
    buildMarker,
    // Canary pins its updater off and reports the built commit; both normally
    // come from scripts/canary.ts, so they are only filled in when absent.
    environment: flavor === "canary" ? { SYNARA_DISABLE_AUTO_UPDATE: "1" } : {},
    rebuildHint:
      flavor === "canary"
        ? "Run `bun run canary:update` in the Synara repository, then open Synara Canary again."
        : "Run `bun run build:desktop` in the Synara repository, then open the app again.",
  };
  return `"use strict";
// Generated by apps/desktop/scripts/electron-launcher.mjs. Do not edit.
//
// Electron runs this entry whenever the runtime bundle is opened without
// arguments (Finder, Spotlight, the Dock). It fills in the working directory
// and environment that scripts/start-electron.mjs passes when it spawns
// Electron itself, then loads the built desktop main.
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const launch = ${JSON.stringify(launch, null, 2)};

function fillEnvironment() {
  const env = process.env;
  delete env.ELECTRON_RUN_AS_NODE;
  if (!env.SYNARA_DESKTOP_FLAVOR) env.SYNARA_DESKTOP_FLAVOR = launch.flavor;
  if (!env.SYNARA_HOME || env.SYNARA_HOME.trim() === "") {
    env.SYNARA_HOME = path.join(os.homedir(), launch.homeDirectoryName);
  }
  if (!env.SYNARA_SOURCE_DESKTOP_BUILD_MARKER) {
    env.SYNARA_SOURCE_DESKTOP_BUILD_MARKER = launch.buildMarker;
  }
  for (const [key, value] of Object.entries(launch.environment)) {
    if (env[key] === undefined) env[key] = value;
  }
  if (launch.flavor === "canary" && env.SYNARA_COMMIT_HASH === undefined) {
    try {
      const state = JSON.parse(
        fs.readFileSync(path.join(env.SYNARA_HOME, "canary-state.json"), "utf8"),
      );
      if (typeof state.currentCommit === "string" && state.currentCommit.length > 0) {
        env.SYNARA_COMMIT_HASH = state.currentCommit;
      }
    } catch {
      // No state file yet: the app still starts, it just has no commit to report.
    }
  }
}

const builtMainPath = path.join(launch.desktopDirectory, "dist-electron", "main.js");

function builtMainProblem() {
  let source;
  try {
    source = fs.readFileSync(builtMainPath, "utf8");
  } catch {
    return launch.displayName + " is not built.";
  }
  if (!source.includes(launch.buildMarker)) {
    return launch.displayName + " has a stale desktop build.";
  }
  return null;
}

fillEnvironment();
const problem = builtMainProblem();
if (problem === null) {
  process.chdir(launch.desktopDirectory);
  require(builtMainPath);
} else {
  const { app, dialog } = require("electron");
  app.whenReady().then(() => {
    dialog.showErrorBox(problem, launch.rebuildHint);
    app.quit();
  });
}
`;
}

export function copyMacAppBundle(sourceAppBundlePath, targetAppBundlePath) {
  const copyResult = spawnSync("ditto", [sourceAppBundlePath, targetAppBundlePath], {
    encoding: "utf8",
  });
  if (copyResult.error) {
    throw new Error(
      `Failed to copy macOS Electron app bundle from ${sourceAppBundlePath} to ${targetAppBundlePath}: ${copyResult.error.message}`,
      { cause: copyResult.error },
    );
  }
  if (copyResult.status !== 0) {
    const details = [copyResult.stderr, copyResult.stdout].filter(Boolean).join("\n").trim();
    throw new Error(
      `Failed to copy macOS Electron app bundle from ${sourceAppBundlePath} to ${targetAppBundlePath} (ditto exit ${copyResult.status}): ${details}`.trim(),
    );
  }
}

function buildMacLauncher(electronBinaryPath) {
  const sourceAppBundlePath = resolve(electronBinaryPath, "../../..");
  const runtimeDir = join(desktopDir, ".electron-runtime");
  const targetAppBundlePath = join(runtimeDir, `${APP_DISPLAY_NAME}.app`);
  const targetBinaryPath = join(targetAppBundlePath, "Contents", "MacOS", "Electron");
  const iconPath = join(desktopDir, "resources", "icon.icns");
  const metadataPath = join(runtimeDir, "metadata.json");

  mkdirSync(runtimeDir, { recursive: true });

  const expectedMetadata = {
    launcherVersion: LAUNCHER_VERSION,
    sourceAppBundlePath,
    sourceAppMtimeMs: statSync(sourceAppBundlePath).mtimeMs,
    iconMtimeMs: statSync(iconPath).mtimeMs,
  };

  const selfLauncher = {
    desktopDirectory: desktopDir,
    flavor: desktopFlavor,
    displayName: APP_DISPLAY_NAME,
    homeDirectoryName: desktopIdentity.defaultHomeDirectoryName,
  };

  const currentMetadata = readJson(metadataPath);
  if (
    existsSync(targetBinaryPath) &&
    currentMetadata &&
    JSON.stringify(currentMetadata) === JSON.stringify(expectedMetadata)
  ) {
    writeRuntimeSelfLauncher(targetAppBundlePath, selfLauncher);
    return targetBinaryPath;
  }

  rmSync(targetAppBundlePath, { recursive: true, force: true });
  copyMacAppBundle(sourceAppBundlePath, targetAppBundlePath);
  patchMainBundleInfoPlist(targetAppBundlePath, iconPath);
  patchHelperBundleInfoPlists(targetAppBundlePath);
  writeRuntimeSelfLauncher(targetAppBundlePath, selfLauncher);
  writeFileSync(metadataPath, `${JSON.stringify(expectedMetadata, null, 2)}\n`);

  return targetBinaryPath;
}

export function resolveElectronPath() {
  const require = createRequire(import.meta.url);
  const electronBinaryPath = require("electron");

  if (process.platform !== "darwin") {
    return electronBinaryPath;
  }

  return buildMacLauncher(electronBinaryPath);
}
