import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";
import { appendStepDispatched, loadCurrentPlan } from "../plan.ts";

const params = Type.Object({
  step_id: Type.String({
    description: "The id of the step to dispatch (must exist in the current plan).",
  }),
});

/**
 * (CEO autonomous-mode only) Hand a step to the assigned employee.
 *
 * Reads the current plan to look up the step's text and assignee, appends
 * a `step_dispatched` event, then publishes a structured payload to the
 * employee's mailbox channel `directs:{assigned}` so they wake up.
 *
 * If the step_id isn't in the current plan, returns an error-shaped
 * result (no exception — pi-mono surfaces the text back to the LLM and
 * lets the CEO retry).
 */
export const dispatchStep: CompanyTool<typeof params> = {
  name: "dispatch_step",
  description:
    "(CEO autonomous-mode only) Hand a step to the assigned employee. The employee will execute and call report_to_ceo with the step_id when done.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    const plan = await loadCurrentPlan(ctx.client, ctx.run, 0);
    const step = plan.steps.find((s) => s.id === args.step_id);
    if (!step) {
      return {
        text: `step ${args.step_id} not found in current plan; valid ids: ${plan.steps.map((s) => s.id).join(", ") || "(plan empty)"}`,
      };
    }
    await appendStepDispatched(ctx.client, ctx.run, step.id, step.assigned, step.text);
    await ctx.channels.publish(`directs:${step.assigned}`, {
      kind: "step_dispatched",
      from: "ceo",
      step_id: step.id,
      text: step.text,
    });
    return {
      text: `dispatched ${step.id} to ${step.assigned}: ${step.text.slice(0, 60)}`,
    };
  },
};
