import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";

const params = Type.Object({
  name: Type.String(),
  company: Type.String(),
  notes: Type.Optional(Type.String()),
});

export const addProspect: CompanyTool<typeof params> = {
  name: "add_prospect",
  description: "Add a sales prospect record. Returns a prospect ID.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    const prospectId = `prospect-${Date.now()}`;
    await ctx.memory.recordEvent({
      kind: "deliverable",
      type: "prospect",
      prospectId,
      name: args.name,
      company: args.company,
      notes: args.notes ?? "",
    });
    await ctx.channels.publish("gtm", {
      kind: "prospect_added",
      from: ctx.role,
      prospectId,
      name: args.name,
      company: args.company,
    });
    return {
      text: `added prospect ${prospectId}: ${args.name} (${args.company})`,
    };
  },
};
