import type { Client } from "agentdb";
import type { Channels } from "./channels.ts";
import type { RunIdentity } from "./types.ts";
import type { CompanyTool, ToolContext } from "./agent.ts";
import { ALL_TOOLS, CEO_AUTONOMOUS_TOOLS } from "./tools/index.ts";
import { loadCurrentPlan, appendStepTimeout, type PlanState } from "./plan.ts";

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
 *   2. `wakeLoop()` subscribes to `directs:ceo` and re-deliberates
 *      whenever a `step_completed` event arrives.
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

  async start(): Promise<void> {
    await this.boot();
    if (this.budgetCheckTerminate()) {
      await this.recordRunEnded();
      return;
    }
    if (!this.terminated) await this.wakeLoop();
    await this.recordRunEnded();
  }

  /**
   * Subscribe to `directs:ceo` and re-deliberate on every payload arriving
   * on it (typically `step_completed` from a `report_to_ceo` call). Each
   * iteration runs one CEO LLM turn with a wake prompt.
   *
   * Uses Promise.race between the next channel message and a step-timeout
   * timer so the CEO is woken if an employee goes silent past `stepTimeoutSec`.
   */
  protected async wakeLoop(): Promise<void> {
    const iter = this.deps.channels.subscribe("directs:ceo")[Symbol.asyncIterator]();
    while (!this.terminated && !this.budgetCheckTerminate()) {
      const next = iter.next();
      const timeoutMs = this.deps.budgets.stepTimeoutSec * 1000;
      let timer: ReturnType<typeof setTimeout> | null = null;
      const timeoutPromise = new Promise<{ timedOut: true }>((resolve) => {
        timer = setTimeout(() => resolve({ timedOut: true }), timeoutMs);
      });
      const winner = await Promise.race([
        next.then((r) => ({ kind: "msg" as const, result: r })),
        timeoutPromise.then((t) => ({ kind: "timeout" as const, t })),
      ]);
      if (timer) clearTimeout(timer);
      if (winner.kind === "msg") {
        if (winner.result.done) break;
        await this.driveWakeTurn();
      } else {
        await this.synthesizeStepTimeout();
        await this.driveWakeTurn();
      }
    }
  }

  /**
   * Find the most-recently dispatched step whose status is still
   * in_progress (per `_plan` reduction) and append a `step_timeout`
   * event for it. If no such step exists, no-op.
   */
  private async synthesizeStepTimeout(): Promise<void> {
    const plan = await this.snapshotPlan();
    const inProgress = plan.steps.filter((s) => s.status === "in_progress");
    if (inProgress.length === 0) return;
    const step = inProgress[inProgress.length - 1];
    await appendStepTimeout(this.deps.client, this.deps.run, step.id);
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

  private async driveWakeTurn(): Promise<void> {
    const ctx = this.makeCtx();
    (ctx as any)._ceoPrompt = this.wakePrompt(await this.snapshotPlan());
    const tools = this.toolsByName();
    await this.driveTurn("(wake)", tools, ctx);
    this.revisions = await this.countPlanRevisedEvents();
    this.budgetCheckTerminate();
  }

  /**
   * Build the wake prompt. Public for inspection / production wiring.
   */
  protected wakePrompt(plan: PlanState): string {
    const planText = plan.steps.length
      ? plan.steps.map((s) => `  ${s.id} [${s.status}] (${s.assigned}) ${s.text}`).join("\n")
      : "  (empty plan)";
    const obsText = plan.observations.length
      ? plan.observations.map((o) => `  - ${o.from} ${o.kind}: ${JSON.stringify(o.payload).slice(0, 120)}${o.relates_to_step ? ` (re ${o.relates_to_step})` : ""}`).join("\n")
      : "  (none)";
    const remaining = this.budgetsRemaining();
    return [
      `Goal: ${this.deps.goal}`,
      ``,
      `Current plan:`,
      planText,
      ``,
      `Recent observations:`,
      obsText,
      ``,
      `Budget remaining:`,
      `  - revisions: ${remaining.revisions}/${this.deps.budgets.maxRevisions}`,
      `  - time: ${remaining.minutes.toFixed(1)}/${this.deps.budgets.maxMinutes} min`,
      `  - LLM calls: ${remaining.llmCalls}/${this.deps.budgets.maxLlmCalls}`,
      ``,
      `Decide what to do next: dispatch_step / update_plan / mark_step / announce / direct_message / update_agent_config / wrap_up.`,
      `Pick the smallest next action. You will be re-prompted on the next step_completed.`,
    ].join("\n");
  }

  private budgetsRemaining(): { revisions: number; minutes: number; llmCalls: number } {
    const elapsedSec = (Date.now() - this.startedAt) / 1000;
    return {
      revisions: Math.max(0, this.deps.budgets.maxRevisions - this.revisions),
      minutes: Math.max(0, this.deps.budgets.maxMinutes - elapsedSec / 60),
      llmCalls: Math.max(0, this.deps.budgets.maxLlmCalls - this.llmCalls),
    };
  }

  /**
   * Returns true if we should stop. Sets `terminationCause` when so.
   */
  protected budgetCheckTerminate(): boolean {
    if (this.terminated) return true;
    const elapsedMs = Date.now() - this.startedAt;
    if (this.revisions >= this.deps.budgets.maxRevisions) {
      this.terminated = true;
      this.terminationCause = "revision_budget_exhausted";
      return true;
    }
    if (elapsedMs >= this.deps.budgets.maxMinutes * 60_000) {
      this.terminated = true;
      this.terminationCause = "time_budget_exhausted";
      return true;
    }
    if (this.llmCalls >= this.deps.budgets.maxLlmCalls) {
      this.terminated = true;
      this.terminationCause = "call_budget_exhausted";
      return true;
    }
    return false;
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
    const resp = await sess.fetchRecent(1024);
    return (resp.recent ?? []).filter((e: any) => (e.payload?.kind ?? e.kind) === "plan_revised").length;
  }

  /**
   * Append `run_ended` to `_meta`. Skip when wrap_up tool already wrote it
   * (terminationCause === "wrap_up").
   */
  private async recordRunEnded(): Promise<void> {
    if (this.terminationCause === "wrap_up") return;
    const sess = this.deps.client.session(`${this.deps.run.runId}:_meta`, { tenant: this.deps.run.tenant });
    await sess.append(
      { kind: "run_ended", cause: this.terminationCause || "unknown", ended_at: Date.now() } as any,
      { fetchLastN: 0 },
    );
  }
}
