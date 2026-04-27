import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";

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

/**
 * Send a private message to a specific employee.
 *
 * Delivery is via the target's personal mailbox channel (`directs:{role}`)
 * — the target subscribes to it and wakes on the publish, just like any
 * other channel message. Sessions are durable memory; channels are how
 * agents wake up. Direct messages need to wake the target, so they ride
 * the channel layer.
 *
 * The target's own session records the incoming message (via the agent
 * loop's `peer_message` event) when the agent processes the wake-up; we
 * don't write to the target's session here.
 */
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
    await ctx.channels.publish(`directs:${args.role}`, {
      kind: "direct_message",
      from: ctx.role,
      text: args.text,
    });
    return { text: `sent to ${args.role}: ${args.text.slice(0, 60)}` };
  },
};
