import type { Client } from "agentdb";
import type { AgentConfig, AgentRole, ConfigPatch, AgentEvent } from "./types.ts";

const CONFIG_SESSION = "_config:agents";

type EmployeeRole = Exclude<AgentRole, "ceo">;

/**
 * Code-side defaults for every employee/contractor role. Per spec § 5.5
 * these are the authoritative starting point — runtime overrides on the
 * `_config:agents` session are layered on top via {@link mergePatch} at
 * agent-instantiation time. The CEO has no entry; the human player owns
 * that role.
 */
export const CODE_DEFAULTS: Record<EmployeeRole, AgentConfig> = {
  cto: {
    role: "cto",
    systemPrompt:
      "You are the CTO. You receive directives from the CEO and delegate engineering work. Stay strategic, not in the weeds. When you receive a directive, decide whether to draft a feature spec yourself or delegate to the Engineer.",
    tools: ["delegate_to_engineer", "report_to_ceo", "hire_contractor"],
    model: "claude-haiku-4-5",
  },
  engineer: {
    role: "engineer",
    systemPrompt:
      "You are a senior engineer. Receive specs from the CTO. Produce technical docs (specs, RFCs, design notes) using write_doc. Submit for review when ready.\n\nDelegation rule: hire a contractor ONLY when (a) you are already mid-task on something the CEO has not explicitly said to drop, AND (b) the new request is parallelizable. If you're idle or can absorb the new work, do it yourself. Hiring contractors costs LLM tokens and creates downstream coordination work; default to handling things yourself.",
    tools: ["write_doc", "submit_for_review", "hire_contractor", "report_to_ceo"],
    model: "claude-haiku-4-5",
  },
  marketing: {
    role: "marketing",
    systemPrompt:
      "You are head of marketing. Draft campaigns and copy. Coordinate with sales on messaging.\n\nDelegation rule: hire a contractor only when you're already mid-task and the new request needs specialized focus (e.g., punchy taglines while you're drafting a long-form campaign). If you can do the work yourself without context-switching cost, do it.",
    tools: ["draft_campaign", "report_to_ceo", "hire_contractor"],
    model: "claude-haiku-4-5",
  },
  sales: {
    role: "sales",
    systemPrompt:
      "You are head of sales. Build prospect lists and draft outreach. Stay aligned with marketing's messaging. Hire a contractor for prospect research at scale.",
    tools: ["add_prospect", "draft_outreach", "report_to_ceo"],
    model: "claude-haiku-4-5",
  },
  contractor: {
    role: "contractor",
    systemPrompt:
      "You are a contractor brought in to handle a specific task. Stay narrowly focused on the task brief. When done, post your deliverable via report_to_ceo.",
    tools: ["write_doc", "report_to_ceo"],
    model: "claude-haiku-4-5",
  },
};

/**
 * Shallow-merge a {@link ConfigPatch} onto an {@link AgentConfig}. Patch
 * fields overwrite whole base fields — no deep merging. Spec § 5.5
 * deliberately keeps the merge shape simple so a runtime override of
 * `tools` replaces the array rather than appending to it.
 */
export function mergePatch(base: AgentConfig, patch: ConfigPatch): AgentConfig {
  return { ...base, ...patch };
}

/**
 * Reduce the events of a `_config:agents` session into the override
 * patch for one role. Pure: no side effects, no IO. Last write wins on
 * a per-field basis. Non-`agent_override` events and overrides for
 * other roles are ignored.
 */
export function reduceConfigEvents(events: AgentEvent[], role: AgentRole): ConfigPatch {
  let patch: ConfigPatch = {};
  for (const ev of events) {
    const p: any = (ev as any).payload ?? ev;
    if (p?.kind === "agent_override" && p.role === role && p.patch) {
      patch = { ...patch, ...p.patch };
    }
  }
  return patch;
}

/**
 * Read the `_config:agents` session for this tenant, reduce-over-events
 * to produce the override patch for the given role, merge with the code
 * defaults. Called at agent-instantiation time and on each agent-loop
 * iteration so hot config updates take effect.
 */
export async function loadAgentConfig(
  client: Client,
  tenant: string,
  role: EmployeeRole,
): Promise<AgentConfig> {
  const defaults = CODE_DEFAULTS[role];
  const session = client.session(CONFIG_SESSION, { tenant });
  const resp = await session.fetchRecent(1024);
  const recent = (resp.recent ?? []) as AgentEvent[];
  const patch = reduceConfigEvents(recent, role);
  return mergePatch(defaults, patch);
}

/**
 * Append an `agent_override` event to the config session. Used by the
 * CEO's `update_agent_config` tool (Task 9).
 */
export async function appendConfigOverride(
  client: Client,
  tenant: string,
  role: AgentRole,
  patch: ConfigPatch,
): Promise<void> {
  const session = client.session(CONFIG_SESSION, { tenant });
  await session.append({ kind: "agent_override", role, patch } as any, { fetchLastN: 0 });
}
