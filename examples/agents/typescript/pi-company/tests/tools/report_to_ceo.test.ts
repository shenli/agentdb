import { test } from "node:test";
import assert from "node:assert/strict";
import { reportToCeo } from "../../src/tools/report_to_ceo.ts";
import { PLAN_SESSION_SUFFIX } from "../../src/plan.ts";

class FakeSession {
  appended: any[] = [];
  async append(event: any, _opts: any) {
    this.appended.push(event);
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

function makeCtx(client: FakeClient, role: any = "engineer") {
  return {
    client: client as any,
    run: { tenant: "t", runId: "r1" },
    role,
    memory: { recordEvent: async () => {}, sessionName: `r1:${role}` } as any,
    channels: {
      publish: async (name: string, payload: any) => {
        await client.broadcast({ channel: `r1:${name}`, payload, tenant: "t" });
      },
      subscribe: () => ({ async *[Symbol.asyncIterator]() {} }),
      run: { tenant: "t", runId: "r1" },
    } as any,
  };
}

test("report_to_ceo without step_id broadcasts to announcements (legacy behavior)", async () => {
  const client = new FakeClient();
  const ctx = makeCtx(client);
  const result = await reportToCeo.handler({ text: "halfway done" } as any, ctx as any);
  assert.equal(client.broadcasts.length, 1);
  assert.equal(client.broadcasts[0].channel, "r1:announcements");
  assert.equal(client.broadcasts[0].payload.kind, "report");
  assert.ok((result as any).text?.includes("halfway"));
  assert.ok(!client.sessions.has(`r1:${PLAN_SESSION_SUFFIX}`));
});

test("report_to_ceo with step_id appends step_completed to _plan AND publishes to directs:ceo", async () => {
  const client = new FakeClient();
  const ctx = makeCtx(client);
  await reportToCeo.handler(
    { text: "scoped to 3 screens", step_id: "s1", status: "ok" } as any,
    ctx as any,
  );
  const planSess = client.sessions.get(`r1:${PLAN_SESSION_SUFFIX}`);
  assert.ok(planSess, "should have created the _plan session");
  assert.equal(planSess.appended.length, 1);
  assert.equal(planSess.appended[0].kind, "step_completed");
  assert.equal(planSess.appended[0].step_id, "s1");
  assert.equal(planSess.appended[0].status, "ok");

  const ceoBroadcast = client.broadcasts.find((b) => b.channel === "r1:directs:ceo");
  assert.ok(ceoBroadcast, "should publish to directs:ceo mailbox");
  assert.equal(ceoBroadcast.payload.kind, "step_completed");
  assert.equal(ceoBroadcast.payload.step_id, "s1");
});

test("report_to_ceo with step_id defaults status to 'ok' when absent", async () => {
  const client = new FakeClient();
  const ctx = makeCtx(client);
  await reportToCeo.handler({ text: "done", step_id: "s2" } as any, ctx as any);
  const planSess = client.sessions.get(`r1:${PLAN_SESSION_SUFFIX}`)!;
  assert.equal(planSess.appended[0].status, "ok");
});
