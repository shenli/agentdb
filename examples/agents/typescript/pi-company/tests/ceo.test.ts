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

class ManualFakeChannels {
  client: FakeClient;
  run: any;
  private mailboxQueue: any[] = [];
  private resolver: ((v: IteratorResult<any>) => void) | null = null;
  constructor(client: FakeClient, run: any) { this.client = client; this.run = run; }
  async publish(name: string, payload: unknown) {
    await this.client.broadcast({ channel: `${this.run.runId}:${name}`, payload, tenant: this.run.tenant });
  }
  pushToMailbox(payload: any) {
    if (this.resolver) {
      const r = this.resolver;
      this.resolver = null;
      r({ value: { tenant: this.run.tenant, channel: `${this.run.runId}:directs:ceo`, payload, ts_ms: Date.now(), sender: "engineer" }, done: false });
    } else {
      this.mailboxQueue.push(payload);
    }
  }
  closeMailbox() {
    if (this.resolver) {
      const r = this.resolver;
      this.resolver = null;
      r({ value: undefined, done: true });
    }
  }
  subscribe(name: string) {
    const queue = this.mailboxQueue;
    const self = this;
    if (name !== "directs:ceo") return { async *[Symbol.asyncIterator]() {} };
    return {
      [Symbol.asyncIterator]() {
        return {
          next: (): Promise<IteratorResult<any>> => {
            if (queue.length > 0) {
              const payload = queue.shift();
              return Promise.resolve({
                value: { tenant: self.run.tenant, channel: `${self.run.runId}:directs:ceo`, payload, ts_ms: Date.now(), sender: "engineer" },
                done: false,
              });
            }
            return new Promise((resolve) => { self.resolver = resolve; });
          },
        };
      },
    };
  }
}

test("start() runs boot then wakes once on a step_completed payload, terminates on wrap_up", async () => {
  const client = new FakeClient();
  const run = { tenant: "t", runId: "r1" };
  const channels = new ManualFakeChannels(client, run);
  const llm = new ScriptedLLM([
    [
      { tool: "update_plan", args: {
        reason: "initial",
        steps: [{ id: "s1", text: "do it", assigned: "engineer", status: "pending" }],
      }},
      { tool: "dispatch_step", args: { step_id: "s1" } },
    ],
    [
      { tool: "wrap_up", args: { reason: "done", summary: "shipped" } },
    ],
  ]);

  const ceo = new AutonomousCEO({
    client: client as any,
    channels: channels as any,
    run,
    goal: "ship",
    budgets: makeBudgets(),
    runTurn: (tools, ctx) => llm.runTurn(tools, ctx),
  });

  setTimeout(() => {
    channels.pushToMailbox({ kind: "step_completed", from: "engineer", step_id: "s1", status: "ok", summary: "done" });
  }, 10);

  await ceo.start();
  assert.equal(ceo.cause, "wrap_up");
  assert.equal(llm.callCount, 2);

  const metaSess = client.sessions.get("r1:_meta");
  assert.ok(metaSess);
  assert.ok(metaSess.appended.some((e: any) => e.kind === "run_ended" && e.cause === "wrap_up"));
});

test("budget cap (maxLlmCalls) terminates the loop", async () => {
  const client = new FakeClient();
  const run = { tenant: "t", runId: "r1" };
  const channels = new ManualFakeChannels(client, run);
  const llm = new ScriptedLLM(
    Array.from({ length: 20 }, () => [{ tool: "update_plan", args: {
      reason: "loop",
      steps: [{ id: "s1", text: "x", assigned: "engineer", status: "pending" }],
    }}])
  );

  const ceo = new AutonomousCEO({
    client: client as any,
    channels: channels as any,
    run,
    goal: "ship",
    budgets: makeBudgets({ maxLlmCalls: 3, stepTimeoutSec: 1 }),
    runTurn: (tools, ctx) => llm.runTurn(tools, ctx),
  });

  const interval = setInterval(() => {
    channels.pushToMailbox({ kind: "step_completed", from: "engineer", step_id: "s1", status: "ok", summary: "x" });
  }, 5);

  try {
    await ceo.start();
  } finally {
    clearInterval(interval);
  }

  assert.equal(ceo.cause, "call_budget_exhausted");
  const metaSess = client.sessions.get("r1:_meta")!;
  const ended = metaSess.appended.find((e: any) => e.kind === "run_ended");
  assert.ok(ended);
  assert.equal(ended.cause, "call_budget_exhausted");
});
