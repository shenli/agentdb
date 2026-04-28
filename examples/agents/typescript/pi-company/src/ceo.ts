import type { Client } from "agentdb";
import type { Channels } from "./channels.ts";
import type { RunIdentity } from "./types.ts";
import type { CompanyTool, ToolContext } from "./agent.ts";
import { ALL_TOOLS, CEO_AUTONOMOUS_TOOLS } from "./tools/index.ts";
import { loadCurrentPlan, type PlanState } from "./plan.ts";

export interface Budgets {
  maxRevisions: number;
  maxMinutes: number;
  maxLlmCalls: number;
  stepTimeoutSec: number;
}

export const DEFAULT_BUDGETS: Budgets = {
  maxRevisions: 8,
  maxMinutes: 15,
  maxLlmCalls: 50,
  stepTimeoutSec: 180,
};

export type RunTurn = (
  toolsByName: Record<string, CompanyTool>,
  ctx: ToolContext,
) => Promise<{ terminate?: boolean }>;

export interface AutonomousCEODeps {
  client: Client;
  channels: Channels;
  run: RunIdentity;
  goal: string;
  budgets: Budgets;
  runTurn: RunTurn;
}

/**
 * The autonomous CEO loop.
 *
 * Lifecycle:
 *   1. `boot()` runs the initial-plan turn (LLM is asked to call
 *      update_plan + dispatch_step on the goal).
 *   2. `wakeLoop()` (Task 12) subscribes to `directs:ceo` and
 *      re-deliberates whenever a `step_completed` event arrives.
 *   3. The loop terminates on `wrap_up` (terminate=true returned from
 *      runTurn) or any budget cap.
 */
export class AutonomousCEO {
  private readonly deps: AutonomousCEODeps;
  private revisions = 0;
  private llmCalls = 0;
  private startedAt = 0;
  private terminated = false;
  private terminationCause: string = "";

  constructor(deps: AutonomousCEODeps) {
    this.deps = deps;
    this.startedAt = Date.now();
  }

  get isTerminated(): boolean { return this.terminated; }
  get cause(): string { return this.terminationCause; }

  async boot(): Promise<void> {
    const ctx = this.makeCtx();
    (ctx as any)._ceoPrompt = this.bootPrompt();
    const tools = this.toolsByName();
    await this.driveTurn("(boot)", tools, ctx);
    this.revisions = await this.countPlanRevisedEvents();
  }

  // ---- internals ----

  protected bootPrompt(): string {
    return [
      `You are the CEO of an AI-only company. Your goal:`,
      ``,
      `  ${this.deps.goal}`,
      ``,
      `Your team:`,
      `  - cto: oversight, can review docs, sets engineering direction`,
      `  - engineer: writes code, designs, hires contractors via fork`,
      `  - marketing: drafts campaigns and outreach`,
      `  - sales: builds prospect lists, drafts customer messages`,
      ``,
      `Your tools (autonomous mode):`,
      `  - update_plan(steps, reason): replace the current plan`,
      `  - dispatch_step(step_id): hand a step to an employee — they execute and call report_to_ceo`,
      `  - mark_step(step_id, status, note): override a step's status without an employee report`,
      `  - announce(channel, text): broadcast to a channel`,
      `  - direct_message(to, text): peer message`,
      `  - update_agent_config(role, patch): hot-tune an agent`,
      `  - wrap_up(reason, summary): end the run`,
      ``,
      `Workflow:`,
      `  1. Call update_plan with an initial 3-7 step plan.`,
      `  2. Call dispatch_step on the first pending step.`,
      `  3. Wait. You will be re-prompted when an employee reports back.`,
      ``,
      `Each step shape:`,
      `  { id: "s1", text: "<one sentence>", assigned: "<cto|engineer|marketing|sales>", status: "pending" }`,
      ``,
      `Do NOT call wrap_up in your first turn. Plan first, then dispatch.`,
    ].join("\n");
  }

  protected async driveTurn(
    _label: string,
    tools: Record<string, CompanyTool>,
    ctx: ToolContext,
  ): Promise<void> {
    this.llmCalls += 1;
    const out = await this.deps.runTurn(tools, ctx);
    if (out.terminate) {
      this.terminated = true;
      this.terminationCause = "wrap_up";
    }
  }

  protected toolsByName(): Record<string, CompanyTool> {
    const out: Record<string, CompanyTool> = {};
    for (const name of CEO_AUTONOMOUS_TOOLS) {
      const t = ALL_TOOLS[name];
      if (t) out[name] = t;
    }
    return out;
  }

  protected makeCtx(): ToolContext {
    const memoryStub = {
      recordEvent: async () => {},
      sessionName: `${this.deps.run.runId}:ceo`,
      run: this.deps.run,
      role: "ceo" as const,
      loadHistory: async () => [],
      forkSession: async () => "",
    };
    return {
      memory: memoryStub as any,
      channels: this.deps.channels,
      client: this.deps.client,
      run: this.deps.run,
      role: "ceo",
    };
  }

  protected async snapshotPlan(): Promise<PlanState> {
    return await loadCurrentPlan(this.deps.client, this.deps.run, 20);
  }

  protected async countPlanRevisedEvents(): Promise<number> {
    const sess = this.deps.client.session(`${this.deps.run.runId}:_plan`, { tenant: this.deps.run.tenant });
    const resp = await sess.append({ kind: "_noop" } as any, { fetchLastN: 1024 });
    return (resp.recent ?? []).filter((e: any) => (e.payload?.kind ?? e.kind) === "plan_revised").length;
  }
}
