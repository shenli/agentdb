import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";

const params = Type.Object({
  task: Type.String({ description: "The task to delegate to the Engineer." }),
});

export const delegateToEngineer: CompanyTool<typeof params> = {
  name: "delegate_to_engineer",
  description:
    "Delegate an engineering task to the Engineer role. Posts to the engineering channel.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    await ctx.memory.recordEvent({
      kind: "tool_call",
      tool: "delegate_to_engineer",
      task: args.task,
    });
    await ctx.channels.publish("engineering", {
      kind: "delegation",
      from: ctx.role,
      task: args.task,
    });
    return { text: `delegated to engineer: ${args.task.slice(0, 60)}` };
  },
};
