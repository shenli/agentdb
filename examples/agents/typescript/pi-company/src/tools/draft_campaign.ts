import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";

const params = Type.Object({
  title: Type.String(),
  body: Type.String({ description: "The campaign copy." }),
});

export const draftCampaign: CompanyTool<typeof params> = {
  name: "draft_campaign",
  description: "Draft a marketing campaign. Returns a campaign ID.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    const campaignId = `campaign-${Date.now()}`;
    await ctx.memory.recordEvent({
      kind: "deliverable",
      type: "campaign",
      campaignId,
      title: args.title,
      body: args.body,
    });
    await ctx.channels.publish("gtm", {
      kind: "campaign_drafted",
      from: ctx.role,
      campaignId,
      title: args.title,
    });
    return { text: `drafted campaign ${campaignId}: ${args.title}` };
  },
};
