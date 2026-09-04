import {
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readlinkSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { copyMacAppBundle, writeRuntimeSelfLauncher } from "./electron-launcher.mjs";

describe("macOS Electron launcher copy", { skip: process.platform !== "darwin" }, () => {
  it("keeps framework symlink targets relative after relocation", (t) => {
    const root = mkdtempSync(join(tmpdir(), "synara-electron-launcher-"));
    const source = join(root, "source", "Electron.app");
    const target = join(root, "runtime", "Synara (Dev).app");
    const framework = join(source, "Contents", "Frameworks", "Electron Framework.framework");

    mkdirSync(join(framework, "Versions", "A", "Resources"), { recursive: true });
    writeFileSync(join(framework, "Versions", "A", "Resources", "icudtl.dat"), "icu");
    symlinkSync("A", join(framework, "Versions", "Current"));
    symlinkSync("Versions/Current/Resources", join(framework, "Resources"));

    copyMacAppBundle(source, target);
    rmSync(join(root, "source"), { recursive: true });

    const copiedResources = join(
      target,
      "Contents",
      "Frameworks",
      "Electron Framework.framework",
      "Resources",
    );
    assert.equal(lstatSync(copiedResources).isSymbolicLink(), true);
    assert.equal(readlinkSync(copiedResources), "Versions/Current/Resources");
    assert.equal(readFileSync(join(copiedResources, "icudtl.dat"), "utf8"), "icu");
  });
});

describe("runtime bundle self-launcher", () => {
  it("writes an entry Electron runs when the bundle is opened from Finder", () => {
    const root = mkdtempSync(join(tmpdir(), "synara-self-launcher-"));
    const bundle = join(root, "Synara Canary.app");
    mkdirSync(join(bundle, "Contents", "Resources"), { recursive: true });

    writeRuntimeSelfLauncher(bundle, {
      desktopDirectory: "/opt/synara/apps/desktop",
      flavor: "canary",
      displayName: "Synara Canary",
      homeDirectoryName: ".synara-canary",
      buildMarker: "marker-v2",
    });

    const appDir = join(bundle, "Contents", "Resources", "app");
    assert.equal(JSON.parse(readFileSync(join(appDir, "package.json"), "utf8")).main, "main.js");
    const main = readFileSync(join(appDir, "main.js"), "utf8");
    // Parses as CommonJS and carries the values the script would otherwise pass.
    assert.doesNotThrow(() => new Function("require", "process", main));
    assert.match(main, /"desktopDirectory": "\/opt\/synara\/apps\/desktop"/u);
    assert.match(main, /"flavor": "canary"/u);
    assert.match(main, /"SYNARA_DISABLE_AUTO_UPDATE": "1"/u);
    assert.match(main, /canary-state\.json/u);
    assert.match(main, /"buildMarker": "marker-v2"/u);
    assert.match(main, /process\.chdir\(launch\.desktopDirectory\)/u);
    rmSync(root, { recursive: true, force: true });
  });
});
