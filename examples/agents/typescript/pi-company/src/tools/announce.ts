import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";

const params = Type.Object({
  text: Type.String({ description: "The announcement text." }),
});

export const announce: CompanyTool<typeof params> = {
  name: "announce",
  description: "Post a message to the company-wide announcements channel.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    const text = args.text;
    await ctx.memory.recordEvent({ kind: "tool_call", tool: "announce", text });
    await ctx.channels.publish("announcements", {
      kind: "announcement",
      from: ctx.role,
      text,
    });
    return { text: `posted to announcements: ${text.slice(0, 60)}` };
  },
};
