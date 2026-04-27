import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";

const params = Type.Object({
  prospect_id: Type.String(),
  body: Type.String({ description: "The outreach copy." }),
});

export const draftOutreach: CompanyTool<typeof params> = {
  name: "draft_outreach",
  description: "Draft outreach copy for a prospect. Returns a draft ID.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    const draftId = `outreach-${Date.now()}`;
    await ctx.memory.recordEvent({
      kind: "deliverable",
      type: "outreach",
      draftId,
      prospectId: args.prospect_id,
      body: args.body,
    });
    await ctx.channels.publish("gtm", {
      kind: "outreach_drafted",
      from: ctx.role,
      draftId,
      prospect_id: args.prospect_id,
    });
    return { text: `drafted outreach ${draftId} for ${args.prospect_id}` };
  },
};
