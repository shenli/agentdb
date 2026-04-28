import { test } from "node:test";
import assert from "node:assert/strict";
import { dispatchStep } from "../../src/tools/dispatch_step.ts";
import { PLAN_SESSION_SUFFIX, appendPlanRevised } from "../../src/plan.ts";

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

function makeCtx(client: FakeClient) {
  return {
    client: client as any,
    run: { tenant: "t", runId: "r1" },
    role: "ceo" as any,
    memory: { recordEvent: async () => {}, sessionName: "r1:ceo" } as any,
    channels: {
      publish: async (name: string, payload: any) => {
        await client.broadcast({ channel: `r1:${name}`, payload, tenant: "t" });
      },
      subscribe: () => ({ async *[Symbol.asyncIterator]() {} }),
      run: { tenant: "t", runId: "r1" },
    } as any,
  };
}

test("dispatch_step writes step_dispatched + publishes to directs:{assigned}", async () => {
  const client = new FakeClient();
  await appendPlanRevised(client as any, { tenant: "t", runId: "r1" }, [
    { id: "s1", text: "scope mvp", assigned: "engineer", status: "pending" },
  ], "initial");

  const result = await dispatchStep.handler({ step_id: "s1" }, makeCtx(client));

  const planSess = client.sessions.get(`r1:${PLAN_SESSION_SUFFIX}`)!;
  const dispatched = planSess.appended.find((e: any) => e.kind === "step_dispatched");
  assert.ok(dispatched);
  assert.equal(dispatched.step_id, "s1");
  assert.equal(dispatched.assigned, "engineer");

  const mb = client.broadcasts.find((b) => b.channel === "r1:directs:engineer");
  assert.ok(mb);
  assert.equal(mb.payload.kind, "step_dispatched");
  assert.equal(mb.payload.step_id, "s1");
  assert.ok((result as any).text?.includes("s1"));
});

test("dispatch_step on unknown step_id returns an error result without broadcasting", async () => {
  const client = new FakeClient();
  await appendPlanRevised(client as any, { tenant: "t", runId: "r1" }, [
    { id: "s1", text: "x", assigned: "engineer", status: "pending" },
  ], "initial");
  const result = await dispatchStep.handler({ step_id: "ghost" }, makeCtx(client));
  assert.ok((result as any).text?.toLowerCase().includes("not found"));
  assert.ok(!client.broadcasts.some((b) => b.channel.startsWith("r1:directs:")));
});
