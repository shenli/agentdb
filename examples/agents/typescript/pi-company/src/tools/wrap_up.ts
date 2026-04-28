import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";

const params = Type.Object({
  reason: Type.String({
    description:
      "Why you're wrapping up (e.g., 'goal achieved', 'further effort would not help').",
  }),
  summary: Type.String({
    description: "What was accomplished during the run. 1-3 sentences.",
  }),
});

/**
 * (CEO autonomous-mode-only) End the run.
 *
 * Appends a `run_ended` event with `cause="wrap_up"` to the run's
 * `_meta` session and returns `terminate: true` so pi-mono short-circuits
 * post-batch LLM calls. The autonomous CEO loop polls `_meta` (via
 * `run_ended` events) to break out of its wake cycle.
 */
export const wrapUp: CompanyTool<typeof params> = {
  name: "wrap_up",
  description:
    "(CEO autonomous-mode only) End the run. Use when the goal is achieved or further effort would be counterproductive.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    const metaName = `${ctx.run.runId}:_meta`;
    const sess = ctx.client.session(metaName, { tenant: ctx.run.tenant });
    await sess.append(
      {
        kind: "run_ended",
        cause: "wrap_up",
        reason: args.reason,
        summary: args.summary,
        ended_at: Date.now(),
      } as any,
      { fetchLastN: 0 },
    );
    return {
      text: `run wrapped up: ${args.reason}`,
      terminate: true,
    };
  },
};
