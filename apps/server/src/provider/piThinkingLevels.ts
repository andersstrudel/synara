import type { PiThinkingLevel } from "@synara/contracts";

export interface PiThinkingLevelDescriptor {
  readonly value: PiThinkingLevel;
  readonly label: string;
  readonly description: string;
}

/**
 * Thinking-level ladder shared by the Pi-lineage harnesses (Pi and Prime
 * Agent). Both adapters advertise it through `supportedReasoningEfforts`, so
 * the web thinking picker renders identically for either provider.
 */
export const PI_THINKING_LEVEL_DESCRIPTORS: ReadonlyArray<PiThinkingLevelDescriptor> = [
  { value: "off", label: "Off", description: "No extra reasoning" },
  { value: "minimal", label: "Minimal", description: "Light reasoning" },
  { value: "low", label: "Low", description: "Faster reasoning" },
  { value: "medium", label: "Medium", description: "Balanced reasoning" },
  { value: "high", label: "High", description: "Deeper reasoning" },
  { value: "xhigh", label: "Extra High", description: "Extra-high reasoning" },
  { value: "max", label: "Max", description: "Maximum reasoning" },
];
