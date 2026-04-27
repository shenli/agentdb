import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";
import { Memory } from "../memory.ts";

const params = Type.Object({
  role: Type.Union(
    [
      Type.Literal("cto"),
      Type.Literal("engineer"),
      Type.Literal("marketing"),
      Type.Literal("sales"),
    ],
    { description: "The target employee role." },
  ),
  text: Type.String({ description: "The message body." }),
});

export const directMessage: CompanyTool<typeof params> = {
  name: "direct_message",
  description: "Send a private message to one specific employee role.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    await ctx.memory.recordEvent({
      kind: "tool_call",
      tool: "direct_message",
      target: args.role,
      text: args.text,
    });
    const targetMemory = new Memory(ctx.client, ctx.run, args.role);
    await targetMemory.recordEvent({
      kind: "peer_message",
      from: ctx.role,
      text: args.text,
    });
    return { text: `sent to ${args.role}: ${args.text.slice(0, 60)}` };
  },
};
