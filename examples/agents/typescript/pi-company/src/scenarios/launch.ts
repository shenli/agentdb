import type { Channels } from "../channels.ts";
import type { Client } from "agentdb";
import { appendConfigOverride } from "../config.ts";
import type { ConfigPatch, AgentRole } from "../types.ts";

/**
 * One step in a scripted scenario. `delayMs` is the wait *before* the action
 * fires (relative to the prior step's completion). Two action shapes:
 *   - `ceo_message` — broadcast a CEO directive on `announcements`.
 *   - `config_update` — append an `agent_override` to `_config:agents`.
 *     Mirrors what the human player would do via the `update_agent_config`
 *     tool in interactive mode.
 */
export type ScenarioStep =
  | { delayMs: number; action: { type: "ceo_message"; text: string } }
  | { delayMs: number; action: { type: "config_update"; role: AgentRole; patch: ConfigPatch } };

/**
 * The canonical "launch a vector search service" demo run. Drives the
 * five-agent company hands-off: CEO directives, a parallel exploration
 * nudge, a hot config-style swap on Marketing, then a wrap-up.
 *
 * Total wall-clock floor: ~3 minutes (sum of `delayMs`); actual run time
 * is longer because each directive triggers LLM calls per agent.
 */
export const launchScenario: ScenarioStep[] = [
  {
    delayMs: 0,
    action: {
      type: "ceo_message",
      text: "Launch our new vector search service this quarter. Engineering and Marketing, propose plans.",
    },
  },
  {
    delayMs: 60_000,
    action: {
      type: "ceo_message",
      text: "Engineer, also explore approach B in parallel.",
    },
  },
  {
    delayMs: 45_000,
    action: {
      type: "config_update",
      role: "marketing",
      patch: {
        systemPromptSuffix: "Be terse and punchy. Aim for under 80 chars per line.",
      },
    },
  },
  {
    delayMs: 15_000,
    action: {
      type: "ceo_message",
      text: "Marketing, redo the campaign with the new style.",
    },
  },
  {
    delayMs: 30_000,
    action: {
      type: "ceo_message",
      text: "Sales, give me the prospect status.",
    },
  },
  {
    delayMs: 30_000,
    action: {
      type: "ceo_message",
      text: "Wrap it up. CTO, summary please.",
    },
  },
];

/**
 * Execute a {@link ScenarioStep} array sequentially. Each step waits its
 * `delayMs` first, then dispatches. Errors propagate to the caller — the
 * entry script's `try/finally` records `run_ended` regardless.
 */
export async function runScenario(
  steps: ScenarioStep[],
  channels: Channels,
  client: Client,
  tenant: string,
): Promise<void> {
  for (const step of steps) {
    await new Promise<void>((resolve) => setTimeout(resolve, step.delayMs));
    if (step.action.type === "ceo_message") {
      await channels.publish("announcements", {
        kind: "directive",
        from: "ceo",
        text: step.action.text,
      });
      console.log(`> CEO: ${step.action.text}`);
    } else if (step.action.type === "config_update") {
      await appendConfigOverride(client, tenant, step.action.role, step.action.patch);
      console.log(
        `> CEO: (config update) ${step.action.role} <- ${JSON.stringify(step.action.patch).slice(0, 120)}`,
      );
    }
  }
}
