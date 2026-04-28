import { test } from "node:test";
import assert from "node:assert/strict";
import { markStep } from "../../src/tools/mark_step.ts";
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
}

function makeCtx(client: FakeClient) {
  return {
    client: client as any,
    run: { tenant: "t", runId: "r1" },
    role: "ceo" as any,
    memory: { recordEvent: async () => {}, sessionName: "r1:ceo" } as any,
    channels: null as any,
  };
}

test("mark_step appends step_completed with from='ceo'", async () => {
  const client = new FakeClient();
  await markStep.handler(
    { step_id: "s2", status: "cancelled", note: "no longer needed" },
    makeCtx(client),
  );
  const planSess = client.sessions.get(`r1:${PLAN_SESSION_SUFFIX}`)!;
  const ev = planSess.appended[0];
  assert.equal(ev.kind, "step_completed");
  assert.equal(ev.step_id, "s2");
  assert.equal(ev.status, "cancelled");
  assert.equal(ev.from, "ceo");
  assert.ok(ev.summary?.includes("no longer needed"));
});
