import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool, ToolContext } from "../agent.ts";
import { appendConfigOverride } from "../config.ts";
import type { AgentRole, ConfigPatch } from "../types.ts";

const params = Type.Object({
  role: Type.Union(
    [
      Type.Literal("cto"),
      Type.Literal("engineer"),
      Type.Literal("marketing"),
      Type.Literal("sales"),
      Type.Literal("contractor"),
    ],
    { description: "The role whose config to override." },
  ),
  patch: Type.Object(
    {
      systemPrompt: Type.Optional(Type.String({ description: "Replace the entire system prompt." })),
      systemPromptSuffix: Type.Optional(Type.String({ description: "Append to the existing system prompt." })),
      tools: Type.Optional(Type.Array(Type.String(), { description: "Override the tool list." })),
      model: Type.Optional(Type.String({ description: "Override the LLM model id." })),
    },
    { description: "The config fields to override." },
  ),
});

/**
 * CEO-only tool. Appends an `agent_override` event to the
 * `_config:agents` session. Per spec § 5.5, the next iteration of the
 * target agent's loop reloads config and picks up the patch — no
 * restart, no out-of-band channel.
 */
export const updateAgentConfig: CompanyTool<typeof params> = {
  name: "update_agent_config",
  description:
    "(CEO-only) Hot-tune any agent's configuration: system prompt, tools, model. The next agent loop iteration picks up the override. Stored as an event-sourced override in the _config:agents session.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx: ToolContext) => {
    await appendConfigOverride(
      ctx.client,
      ctx.run.tenant,
      args.role as AgentRole,
      args.patch as ConfigPatch,
    );
    const summary = JSON.stringify(args.patch);
    return {
      text: `updated ${args.role} config: ${summary.slice(0, 80)}`,
    };
  },
};
