import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";
import { appendStepCompleted } from "../plan.ts";
import type { AgentRole } from "../types.ts";

const params = Type.Object({
  text: Type.String({
    description: "The report text. Be specific about progress, blockers, or asks.",
  }),
  step_id: Type.Optional(
    Type.String({
      description:
        "Optional. Set to the dispatched step's id when reporting on an autonomous-mode step. The CEO uses this to mark the step complete and re-plan.",
    }),
  ),
  status: Type.Optional(
    Type.Union([
      Type.Literal("ok"),
      Type.Literal("failed"),
      Type.Literal("blocked"),
    ], { description: "Required when step_id is set. Defaults to 'ok'." }),
  ),
});

/**
 * Post a report to the CEO.
 *
 * Two modes:
 *
 *   1. Scripted mode (no step_id): broadcasts a `report` payload on the
 *      `announcements` channel — the human CEO sees it in the TUI.
 *
 *   2. Autonomous mode (step_id provided): also appends a `step_completed`
 *      event to the `_plan` session AND publishes a `step_completed`
 *      payload to the `directs:ceo` mailbox channel so the autonomous
 *      CEO wakes and re-deliberates. Defaults `status="ok"`.
 */
export const reportToCeo: CompanyTool<typeof params> = {
  name: "report_to_ceo",
  description:
    "Post a report to the CEO. In autonomous mode, pass step_id (and optional status) to mark a dispatched step complete.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    await ctx.memory.recordEvent({
      kind: "tool_call",
      tool: "report_to_ceo",
      text: args.text,
      step_id: args.step_id,
      status: args.status,
    });

    if (args.step_id) {
      const status = args.status ?? "ok";
      await appendStepCompleted(
        ctx.client,
        ctx.run,
        args.step_id,
        status,
        args.text,
        ctx.role as AgentRole,
      );
      await ctx.channels.publish("directs:ceo", {
        kind: "step_completed",
        from: ctx.role,
        step_id: args.step_id,
        status,
        summary: args.text,
      });
      return {
        text: `step ${args.step_id} reported (${status}): ${args.text.slice(0, 60)}`,
      };
    }

    await ctx.channels.publish("announcements", {
      kind: "report",
      from: ctx.role,
      text: args.text,
    });
    return { text: `reported to CEO: ${args.text.slice(0, 60)}` };
  },
};
