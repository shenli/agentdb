import { test } from "node:test";
import assert from "node:assert/strict";
import { AutonomousCEO, DEFAULT_BUDGETS, type Budgets } from "../src/ceo.ts";
import { PLAN_SESSION_SUFFIX, appendPlanRevised } from "../src/plan.ts";

class FakeSession {
  appended: any[] = [];
  recent: any[] = [];
  async append(event: any, _opts: any) {
    this.appended.push(event);
    if (event.kind === "_noop") return { seq: this.appended.length, recent: this.recent };
    this.recent.push({ payload: event, ts_ms: Date.now() });
    return { seq: this.appended.length, recent: [] };
  }
}
class FakeClient {
  sessions = new Map<string, FakeSession>();
  session(id: string, _opts?: any) {
    if (!this.sessions.has(id)) this.sessions.set(id, new FakeSession());
    return this.sessions.get(id)!;
  }
  broadcasts: any[] = [];
  async broadcast(b: any) { this.broadcasts.push(b); }
}

class FakeChannels {
  client: FakeClient;
  run: any;
  constructor(client: FakeClient, run: any) { this.client = client; this.run = run; }
  async publish(name: string, payload: unknown) {
    await this.client.broadcast({ channel: `${this.run.runId}:${name}`, payload, tenant: this.run.tenant });
  }
  subscribe(_name: string) {
    return { async *[Symbol.asyncIterator]() { /* no events */ } };
  }
}

class ScriptedLLM {
  turns: Array<Array<{ tool: string; args: any }>>;
  callCount = 0;
  constructor(turns: Array<Array<{ tool: string; args: any }>>) { this.turns = turns; }
  async runTurn(toolByName: Record<string, any>, ctx: any): Promise<{ terminate?: boolean }> {
    const turn = this.turns[this.callCount++] ?? [];
    let terminate = false;
    for (const tc of turn) {
      const tool = toolByName[tc.tool];
      if (!tool) throw new Error(`unscripted tool: ${tc.tool}`);
      const result = await tool.handler(tc.args, ctx);
      if (result.terminate) terminate = true;
    }
    return { terminate };
  }
}

function makeBudgets(over: Partial<Budgets> = {}): Budgets {
  return { ...DEFAULT_BUDGETS, ...over };
}

test("boot() calls update_plan + dispatch_step from the scripted LLM", async () => {
  const client = new FakeClient();
  const run = { tenant: "t", runId: "r1" };
  const channels = new FakeChannels(client, run);
  const llm = new ScriptedLLM([
    [
      { tool: "update_plan", args: {
        reason: "initial",
        steps: [
          { id: "s1", text: "scope mvp", assigned: "engineer", status: "pending" },
          { id: "s2", text: "draft launch", assigned: "marketing", status: "pending" },
        ],
      }},
      { tool: "dispatch_step", args: { step_id: "s1" } },
    ],
  ]);

  const ceo = new AutonomousCEO({
    client: client as any,
    channels: channels as any,
    run,
    goal: "ship a meditation app",
    budgets: makeBudgets(),
    runTurn: (tools, ctx) => llm.runTurn(tools, ctx),
  });

  await ceo.boot();

  const planSess = client.sessions.get(`r1:${PLAN_SESSION_SUFFIX}`)!;
  assert.ok(planSess.appended.some((e: any) => e.kind === "plan_revised"));
  assert.ok(planSess.appended.some((e: any) => e.kind === "step_dispatched"));
});
