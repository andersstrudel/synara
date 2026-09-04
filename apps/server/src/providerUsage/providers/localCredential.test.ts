// FILE: providerUsage/providers/localCredential.test.ts
// Purpose: Local-login providers without a personal quota API still surface a
// connected Settings card, and stay needs-auth when no credential file exists.

import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import nodePath from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { droidUsageFetcher, piUsageFetcher, primeUsageFetcher } from "./localCredential";

const NOW_MS = 1_780_000_000_000;
const tempDirs: string[] = [];

function makeHome(): string {
  const homeDir = mkdtempSync(nodePath.join(os.tmpdir(), "synara-local-usage-"));
  tempDirs.push(homeDir);
  return homeDir;
}

afterEach(() => {
  for (const dir of tempDirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

describe("local credential usage fetchers", () => {
  it("reports needs-auth when no local login is present", async () => {
    const homeDir = makeHome();
    const ctx = { homeDir, env: {}, platform: "linux" as const, nowMs: NOW_MS };
    expect((await droidUsageFetcher.fetch(ctx)).status).toBe("needs-auth");
    expect((await piUsageFetcher.fetch(ctx)).status).toBe("needs-auth");
    expect((await primeUsageFetcher.fetch(ctx)).status).toBe("needs-auth");
  });

  it("surfaces signed-in Droid and Pi without inventing quota bars", async () => {
    const homeDir = makeHome();
    mkdirSync(nodePath.join(homeDir, ".factory"), { recursive: true });
    writeFileSync(nodePath.join(homeDir, ".factory", "auth.json"), JSON.stringify({ token: "d" }));
    mkdirSync(nodePath.join(homeDir, ".pi", "agent"), { recursive: true });
    writeFileSync(
      nodePath.join(homeDir, ".pi", "agent", "auth.json"),
      JSON.stringify({ provider: "openai" }),
    );

    const ctx = { homeDir, env: {}, platform: "linux" as const, nowMs: NOW_MS };
    const droid = await droidUsageFetcher.fetch(ctx);
    const pi = await piUsageFetcher.fetch(ctx);

    expect(droid.status).toBe("ok");
    expect(pi.status).toBe("ok");
    expect(droid.limits).toEqual([]);
    expect(pi.usageLines[0]?.label).toBe("Limits");
  });

  it("counts Prime as signed in once /login stored an upstream credential", async () => {
    const homeDir = makeHome();
    mkdirSync(nodePath.join(homeDir, ".prime", "agent"), { recursive: true });
    writeFileSync(
      nodePath.join(homeDir, ".prime", "agent", "auth.json"),
      JSON.stringify({ anthropic: { type: "api_key" }, openai: { type: "oauth" } }),
    );

    const ctx = { homeDir, env: {}, platform: "linux" as const, nowMs: NOW_MS };
    const prime = await primeUsageFetcher.fetch(ctx);

    expect(prime.status).toBe("ok");
    expect(prime.limits).toEqual([]);
    expect(prime.usageLines[0]?.label).toBe("Limits");
    expect(await primeUsageFetcher.cacheKey?.(ctx)).toBe("file:prime:anthropic,openai");
  });

  it("keeps Prime needs-auth when auth.json exists but holds no provider", async () => {
    const homeDir = makeHome();
    mkdirSync(nodePath.join(homeDir, ".prime", "agent"), { recursive: true });
    writeFileSync(nodePath.join(homeDir, ".prime", "agent", "auth.json"), "{}");

    const ctx = { homeDir, env: {}, platform: "linux" as const, nowMs: NOW_MS };
    expect((await primeUsageFetcher.fetch(ctx)).status).toBe("needs-auth");
  });

  it("reads Prime credentials from PRIME_AGENT_CODING_AGENT_DIR when set", async () => {
    const homeDir = makeHome();
    const agentDir = nodePath.join(homeDir, "custom-prime-agent");
    mkdirSync(agentDir, { recursive: true });
    writeFileSync(nodePath.join(agentDir, "auth.json"), JSON.stringify({ cerebras: {} }));

    const ctx = {
      homeDir,
      env: { PRIME_AGENT_CODING_AGENT_DIR: agentDir },
      platform: "linux" as const,
      nowMs: NOW_MS,
    };
    expect((await primeUsageFetcher.fetch(ctx)).status).toBe("ok");
  });
});
