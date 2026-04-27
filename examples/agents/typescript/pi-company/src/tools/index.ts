import type { CompanyTool } from "../agent.ts";
import { addProspect } from "./add_prospect.ts";
import { announce } from "./announce.ts";
import { delegateToEngineer } from "./delegate_to_engineer.ts";
import { directMessage } from "./direct_message.ts";
import { draftCampaign } from "./draft_campaign.ts";
import { draftOutreach } from "./draft_outreach.ts";
import { hireContractor } from "./hire_contractor.ts";
import { reportToCeo } from "./report_to_ceo.ts";
import { submitForReview } from "./submit_for_review.ts";
import { updateAgentConfig } from "./update_agent_config.ts";
import { writeDoc } from "./write_doc.ts";

/**
 * The complete tool registry, keyed by canonical tool name. Alphabetical so
 * the LLM-visible tool list serializes deterministically across runs.
 *
 * Per-role subsetting happens in {@link filterTools}; the registry itself is
 * shared across all agents in the run.
 */
export const ALL_TOOLS: Record<string, CompanyTool> = {
  add_prospect: addProspect,
  announce,
  delegate_to_engineer: delegateToEngineer,
  direct_message: directMessage,
  draft_campaign: draftCampaign,
  draft_outreach: draftOutreach,
  hire_contractor: hireContractor,
  report_to_ceo: reportToCeo,
  submit_for_review: submitForReview,
  update_agent_config: updateAgentConfig,
  write_doc: writeDoc,
};

/**
 * Per-role default tool name lists. Must align with `CODE_DEFAULTS` in
 * `config.ts`; the agent loop's `filterTools` call drops names that aren't
 * yet in `ALL_TOOLS`, so listing `hire_contractor` / `update_agent_config`
 * here before Tasks 8/9 land is harmless — those entries simply don't
 * resolve until the registry gains them.
 */
export const ROLE_TOOLS: Record<string, string[]> = {
  ceo: ["announce", "direct_message", "update_agent_config"],
  cto: ["delegate_to_engineer", "report_to_ceo", "hire_contractor"],
  engineer: ["write_doc", "submit_for_review", "hire_contractor"],
  marketing: ["draft_campaign", "report_to_ceo", "hire_contractor"],
  sales: ["add_prospect", "draft_outreach", "report_to_ceo"],
  contractor: ["write_doc", "report_to_ceo"],
};

/**
 * Subset the registry to a known role's tool set. Names absent from
 * `ALL_TOOLS` are silently skipped — see `ROLE_TOOLS` doc above for why.
 */
export function filterTools(names: string[]): Record<string, CompanyTool> {
  const out: Record<string, CompanyTool> = {};
  for (const n of names) {
    if (ALL_TOOLS[n]) out[n] = ALL_TOOLS[n];
  }
  return out;
}
