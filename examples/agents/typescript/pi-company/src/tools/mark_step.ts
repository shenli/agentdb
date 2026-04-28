import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";
import { appendStepCompleted } from "../plan.ts";

const params = Type.Object({
  step_id: Type.String({ description: "The id of the step to mark." }),
  status: Type.Union(
    [Type.Literal("ok"), Type.Literal("failed"), Type.Literal("cancelled")],
    { description: "Override status: ok=done, failed=failed, cancelled=cancelled." },
  ),
  note: Type.Optional(Type.String({ description: "Optional explanation." })),
});

/**
 * (CEO autonomous-mode only) Override a step's status without an employee
 * report. Appends a `step_completed` event with `from="ceo"`.
 */
export const markStep: CompanyTool<typeof params> = {
  name: "mark_step",
  description:
    "(CEO autonomous-mode only) Override a step's status without waiting for an employee report.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    const summary = args.note ?? `(ceo-marked ${args.status})`;
    await appendStepCompleted(ctx.client, ctx.run, args.step_id, args.status, summary, "ceo");
    return { text: `marked ${args.step_id} as ${args.status}` };
  },
};
