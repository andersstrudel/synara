import type { ProviderModelDescriptor } from "@synara/contracts";
import { describe, expect, it } from "vitest";

import {
  getRuntimeAwareModelCapabilities,
  resolveRuntimeModelDescriptor,
} from "./runtimeModelCapabilities";

describe("resolveRuntimeModelDescriptor", () => {
  it("matches a Claude model by its resolved canonical id", () => {
    const runtimeModels: ReadonlyArray<ProviderModelDescriptor> = [
      {
        slug: "sonnet",
        resolvedModel: "claude-sonnet-5",
        name: "Claude Sonnet 5",
        supportsAutoMode: false,
      },
    ];

    expect(
      resolveRuntimeModelDescriptor({
        provider: "claudeAgent",
        model: "claude-sonnet-5",
        runtimeModels,
      }),
    ).toBe(runtimeModels[0]);
  });
});

describe("getRuntimeAwareModelCapabilities", () => {
  const primeRuntimeModel: ProviderModelDescriptor = {
    slug: "openai/gpt-5.5",
    name: "GPT-5.5",
    upstreamProviderId: "openai",
    upstreamProviderName: "OpenAI",
    supportedReasoningEfforts: [
      { value: "low", label: "Low" },
      { value: "medium", label: "Medium" },
      { value: "high", label: "High" },
    ],
    defaultReasoningEffort: "medium",
  };

  it("exposes Prime fast mode when the runtime descriptor advertises it", () => {
    const caps = getRuntimeAwareModelCapabilities({
      provider: "prime",
      model: primeRuntimeModel.slug,
      runtimeModel: { ...primeRuntimeModel, supportsFastMode: true },
    });

    expect(caps.supportsFastMode).toBe(true);
    expect(caps.reasoningEffortLevels.map((level) => level.value)).toEqual([
      "low",
      "medium",
      "high",
    ]);
  });

  it("hides Prime fast mode when the runtime descriptor omits it", () => {
    expect(
      getRuntimeAwareModelCapabilities({
        provider: "prime",
        model: primeRuntimeModel.slug,
        runtimeModel: primeRuntimeModel,
      }).supportsFastMode,
    ).toBe(false);
    expect(
      getRuntimeAwareModelCapabilities({
        provider: "prime",
        model: primeRuntimeModel.slug,
        runtimeModel: { ...primeRuntimeModel, supportsFastMode: false },
      }).supportsFastMode,
    ).toBe(false);
  });

  it("never falls back to the static table for Prime fast mode before discovery resolves", () => {
    expect(
      getRuntimeAwareModelCapabilities({
        provider: "prime",
        model: primeRuntimeModel.slug,
      }).supportsFastMode,
    ).toBe(false);
  });
});
