import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";
import { appendPlanRevised, type Step } from "../plan.ts";

const stepSchema = Type.Object({
  id: Type.String({ description: "Short slug like 's1', 's2'." }),
  text: Type.String({ description: "One clear sentence describing the step." }),
  assigned: Type.Union([
    Type.Literal("cto"),
    Type.Literal("engineer"),
    Type.Literal("marketing"),
    Type.Literal("sales"),
  ]),
  status: Type.Union([
    Type.Literal("pending"),
    Type.Literal("in_progress"),
    Type.Literal("done"),
    Type.Literal("failed"),
    Type.Literal("cancelled"),
  ]),
});

const params = Type.Object({
  reason: Type.String({
    description:
      "Why this revision (e.g., 'initial plan from goal', 'engineer reported s1 blocked, splitting').",
  }),
  steps: Type.Array(stepSchema, {
    description: "Replacement step list. 3-7 steps recommended.",
  }),
});

/**
 * (CEO autonomous-mode only) Replace the current plan.
 *
 * Appends a `plan_revised` event to `{runId}:_plan` and broadcasts a
 * `plan_revised` payload on `announcements` so employees see the new
 * plan in their wake-up context.
 */
export const updatePlan: CompanyTool<typeof params> = {
  name: "update_plan",
  description:
    "(CEO autonomous-mode only) Replace the current plan with a revised step list. Use this on first turn to lay out the initial plan, and any time you learn something that changes what should happen next.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    await appendPlanRevised(ctx.client, ctx.run, args.steps as Step[], args.reason);
    await ctx.channels.publish("announcements", {
      kind: "plan_revised",
      from: ctx.role,
      reason: args.reason,
      step_count: args.steps.length,
    });
    return {
      text: `plan revised (${args.steps.length} steps): ${args.reason.slice(0, 80)}`,
    };
  },
};
