// FILE: providerUsage/providers/localCredential.ts
// Purpose: Usage fetchers for providers that expose a local login but no
// individual live quota API (Droid, Pi, Prime). Connected accounts still
// appear in Settings → Usage; unsigned ones stay needs-auth.

import nodePath from "node:path";

import type { ProviderKind } from "@synara/contracts";

import { getDroidApiKeyEnv } from "../../provider/acp/DroidAcpSupport";
import { readPrimeAuthProviders, resolvePrimeAgentDir } from "../../provider/acp/PrimeAcpSupport";
import { credentialFingerprint, readJsonFile } from "../credentials";
import { asRecord, buildSnapshot, needsAuthSnapshot } from "../parse";
import type { ProviderUsageContext, ProviderUsageFetcher } from "../types";

async function jsonObjectHasKeys(path: string): Promise<boolean> {
  const parsed = asRecord(await readJsonFile(path));
  return parsed !== null && Object.keys(parsed).length > 0;
}

async function resolveDroidSignedIn(ctx: ProviderUsageContext): Promise<string | null> {
  const apiKey = getDroidApiKeyEnv(ctx.env);
  if (apiKey) return `api:${credentialFingerprint(apiKey)}`;
  const factoryHome = nodePath.join(ctx.homeDir, ".factory");
  for (const fileName of ["auth.json", "session.json", "credentials.json"]) {
    const filePath = nodePath.join(factoryHome, fileName);
    if (await jsonObjectHasKeys(filePath)) {
      return `file:${fileName}`;
    }
  }
  return null;
}

async function resolvePiSignedIn(ctx: ProviderUsageContext): Promise<string | null> {
  const authPath = nodePath.join(ctx.homeDir, ".pi", "agent", "auth.json");
  if (await jsonObjectHasKeys(authPath)) return "file:pi";
  return null;
}

// Prime keeps one credential per upstream provider in `<agentDir>/auth.json`,
// written by `/login`. This is the same lookup the provider health check uses,
// so the usage card and the provider status agree about sign-in. The upstream
// provider ids are not secrets, so they can key the cache: adding a provider
// through `/login` refreshes the card before the TTL expires.
async function resolvePrimeSignedIn(ctx: ProviderUsageContext): Promise<string | null> {
  const agentDir = resolvePrimeAgentDir(undefined, ctx.env, ctx.homeDir);
  const providers = await readPrimeAuthProviders(agentDir);
  if (providers.length === 0) return null;
  return `file:prime:${providers.toSorted().join(",")}`;
}

function localCredentialFetcher(input: {
  provider: ProviderKind;
  source: string;
  detail: string;
  resolveSignedIn: (ctx: ProviderUsageContext) => Promise<string | null>;
}): ProviderUsageFetcher {
  return {
    provider: input.provider,
    async cacheKey(ctx) {
      return (await input.resolveSignedIn(ctx)) ?? `${ctx.homeDir}:none`;
    },
    async fetch(ctx) {
      const signedIn = await input.resolveSignedIn(ctx);
      if (!signedIn) {
        return needsAuthSnapshot(input.provider, ctx.nowMs, input.source);
      }
      return buildSnapshot({
        provider: input.provider,
        nowMs: ctx.nowMs,
        status: "ok",
        source: input.source,
        usageLines: [{ label: "Limits", value: input.detail }],
      });
    },
  };
}

export const droidUsageFetcher = localCredentialFetcher({
  provider: "droid",
  source: "droid-local",
  detail:
    "Droid is signed in locally. Individual rate limits stay in the Droid `/limits` command; Factory has no public personal quota API.",
  resolveSignedIn: resolveDroidSignedIn,
});

export const piUsageFetcher = localCredentialFetcher({
  provider: "pi",
  source: "pi-local",
  detail:
    "Pi is signed in locally. Remaining limits stay with each configured model provider; Pi has no single quota API.",
  resolveSignedIn: resolvePiSignedIn,
});

export const primeUsageFetcher = localCredentialFetcher({
  provider: "prime",
  source: "prime-local",
  detail:
    "Prime is signed in locally. Remaining limits stay with each upstream provider added through /login; Prime has no single quota API.",
  resolveSignedIn: resolvePrimeSignedIn,
});
