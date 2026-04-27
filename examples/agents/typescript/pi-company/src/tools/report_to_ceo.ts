import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";

const params = Type.Object({
  text: Type.String({
    description: "The report text. Be specific about progress, blockers, or asks.",
  }),
});

export const reportToCeo: CompanyTool<typeof params> = {
  name: "report_to_ceo",
  description: "Post a status report to the CEO via the announcements channel.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    await ctx.memory.recordEvent({
      kind: "tool_call",
      tool: "report_to_ceo",
      text: args.text,
    });
    await ctx.channels.publish("announcements", {
      kind: "report",
      from: ctx.role,
      text: args.text,
    });
    return { text: `reported to CEO: ${args.text.slice(0, 60)}` };
  },
};
