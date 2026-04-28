import { test } from "node:test";
import assert from "node:assert/strict";
import { updatePlan } from "../../src/tools/update_plan.ts";
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

function makeCtx(client: FakeClient) {
  return {
    client: client as any,
    run: { runId: "r1" },
    role: "ceo" as any,
    memory: { recordEvent: async () => {}, sessionName: "r1:ceo" } as any,
    channels: {
      publish: async (name: string, payload: any) => {
        await client.broadcast({ channel: `r1:${name}`, payload });
      },
      subscribe: () => ({ async *[Symbol.asyncIterator]() {} }),
      run: { runId: "r1" },
    } as any,
  };
}

test("update_plan appends plan_revised to _plan and broadcasts on announcements", async () => {
  const client = new FakeClient();
  await updatePlan.handler(
    {
      reason: "initial plan",
      steps: [
        { id: "s1", text: "scope mvp", assigned: "engineer", status: "pending" },
        { id: "s2", text: "draft launch", assigned: "marketing", status: "pending" },
      ],
    },
    makeCtx(client),
  );

  const planSess = client.sessions.get(`r1:${PLAN_SESSION_SUFFIX}`)!;
  assert.equal(planSess.appended[0].kind, "plan_revised");
  assert.equal(planSess.appended[0].steps.length, 2);
  assert.equal(planSess.appended[0].reason, "initial plan");

  const ann = client.broadcasts.find((b) => b.channel === "r1:announcements");
  assert.ok(ann);
  assert.equal(ann.payload.kind, "plan_revised");
});
