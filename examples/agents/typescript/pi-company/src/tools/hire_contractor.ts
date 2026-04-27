import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool, ToolContext } from "../agent.ts";

const params = Type.Object({
  task_brief: Type.String({
    description:
      "What you want the contractor to do. Be specific about the task and expected output.",
  }),
});

/**
 * Headline AgentDB primitive: rent a contractor on demand.
 *
 * Mechanics:
 *   1. Fork the calling agent's session (O(1) MVCC fork via AgentDB) so
 *      the contractor inherits the full event log up to this point.
 *   2. Append a `contractor_hired` event to the parent's session so the
 *      audit trail records the spawn point + child-session ID.
 *   3. Spawn a fresh `Agent` configured with the `contractor` role's
 *      narrow tool set (write_doc + report_to_ceo), pinned to the forked
 *      child session via `sessionNameOverride`. The contractor's own
 *      `parentSession` field is set so its `report_to_ceo` (and any
 *      future report-back tool) can address the hiring agent's session
 *      directly.
 *   4. Fire-and-forget `bootstrapTask(task_brief)`: the contractor wakes
 *      itself with a synthetic peer-message-shaped payload and runs one
 *      autonomous turn. No subscriptions are wired — the contractor is
 *      single-shot, scoped to this task.
 *
 * Why fire-and-forget: parent agents continue their main work in
 * parallel. Contractor outputs land via channel broadcasts (announcements,
 * engineering) and on the forked session log; the parent doesn't need
 * the return value of the contractor's run to keep going.
 *
 * Built-in safety: `ROLE_TOOLS.contractor` is `["write_doc",
 * "report_to_ceo"]` — `hire_contractor` is NOT in the contractor's tool
 * set, so contractors cannot recursively hire. Depth-limit logic is
 * therefore unnecessary in v1.
 *
 * Lazy-imports of `agent.ts`/`config.ts`/`./index.ts` are intentional:
 * `agent.ts` imports `tools/index.ts` indirectly via `ToolContext`'s
 * registry typing in some call paths — and `tools/index.ts` imports
 * every tool, including this one. A top-level import would create a
 * cycle that breaks Node's `--experimental-strip-types` resolution.
 */
export const hireContractor: CompanyTool<typeof params> = {
  name: "hire_contractor",
  description:
    "Rent a contractor agent on demand for a specific task. The contractor inherits your full context (forked session) and runs in parallel. Use this when you face a hard sub-task you'd rather delegate while continuing your main thread.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx: ToolContext) => {
    const taskBrief = args.task_brief;
    const tag = `contractor-${Date.now()}`;

    // 1. Fork the parent session.
    const childSession = await ctx.memory.forkSession(tag);

    // 2. Record the hire on the parent session for the audit trail.
    await ctx.memory.recordEvent({
      kind: "contractor_hired",
      tag,
      childSession,
      task_brief: taskBrief,
    });

    // 3. Build the contractor agent. Lazy-imports to avoid the
    //    tools/index ↔ agent.ts cycle.
    const { Agent } = await import("../agent.ts");
    const { CODE_DEFAULTS } = await import("../config.ts");
    const { ROLE_TOOLS, filterTools } = await import("./index.ts");

    const contractorConfig = {
      ...CODE_DEFAULTS.contractor,
      systemPrompt: `${CODE_DEFAULTS.contractor.systemPrompt}\n\nYou were hired by ${ctx.role}. Your specific task: ${taskBrief}`,
    };

    const contractorTools = filterTools(ROLE_TOOLS.contractor ?? ["write_doc", "report_to_ceo"]);

    const contractorAgent = new Agent({
      client: ctx.client,
      run: ctx.run,
      role: "contractor",
      channels: ctx.channels,
      // Contractors are single-shot — no ongoing subscriptions. They
      // wake once via bootstrapTask, do their work, and stop.
      subscriptions: [],
      toolRegistry: contractorTools,
      configOverride: contractorConfig,
      taskBrief,
      // Crucial: pin Memory at the forked child session.
      sessionNameOverride: childSession,
      // So the contractor can report back to the hirer.
      parentSession: ctx.memory.sessionName,
    });

    // 4. Fire-and-forget bootstrap. Errors are swallowed: the contractor
    //    runs in the background and any failure (LLM hiccup, tool error)
    //    surfaces in the contractor's own session log via Agent's
    //    recordSafe path. The parent agent must not block on this.
    void (async () => {
      try {
        await contractorAgent.start();
        await contractorAgent.bootstrapTask(taskBrief);
      } catch {
        // Already recorded by Agent.recordSafe / handleIncoming's
        // prompt_error path. Don't propagate to the unhandled rejection
        // handler — fire-and-forget is the whole point.
      }
    })();

    return {
      text: `hired contractor ${tag} on session ${childSession} for task: ${taskBrief.slice(0, 60)}`,
    };
  },
};
