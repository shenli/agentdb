import { test } from "node:test";
import assert from "node:assert/strict";
import { wrapToolForObservation, summarizePayload } from "../src/observability.ts";
import type { CompanyTool, ToolContext } from "../src/agent.ts";
import { Type } from "@mariozechner/pi-ai";
import { PLAN_SESSION_SUFFIX } from "../src/plan.ts";

class FakeSession {
  appended: any[] = [];
  recent: any[] = [];
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

const fakeTool: CompanyTool<ReturnType<typeof Type.Object<{ x: any }>>> = {
  name: "fake_tool",
  description: "test",
  parameters: Type.Object({ x: Type.String() }),
  handler: async (args: any, _ctx: ToolContext) => ({ text: `got ${args.x}` }),
};

function makeCtx(client: FakeClient, role: any = "engineer"): ToolContext {
  return {
    client: client as any,
    run: { tenant: "t", runId: "r1" },
    role,
    memory: { recordEvent: async () => {}, sessionName: "r1:engineer" } as any,
    channels: null as any,
  };
}

test("wrapToolForObservation passes through the result unchanged", async () => {
  const client = new FakeClient();
  const wrapped = wrapToolForObservation(fakeTool, "engineer", client as any, { tenant: "t", runId: "r1" });
  const result = await wrapped.handler({ x: "hello" }, makeCtx(client));
  assert.equal(result.text, "got hello");
});

test("wrapped tool appends an observation to _plan with truncated payload", async () => {
  const client = new FakeClient();
  const wrapped = wrapToolForObservation(fakeTool, "engineer", client as any, { tenant: "t", runId: "r1" });
  await wrapped.handler({ x: "hello" }, makeCtx(client));
  const planSess = client.sessions.get(`r1:${PLAN_SESSION_SUFFIX}`);
  assert.ok(planSess, "should have created the _plan session");
  assert.equal(planSess.appended.length, 1);
  const ev = planSess.appended[0];
  assert.equal(ev.kind, "observation");
  assert.equal(ev.from, "engineer");
  assert.equal(ev.toolName, "fake_tool");
});

test("wrapped tool propagates handler errors and still writes an error observation", async () => {
  const throwingTool: CompanyTool = {
    ...fakeTool,
    handler: async () => { throw new Error("boom"); },
  };
  const client = new FakeClient();
  const wrapped = wrapToolForObservation(throwingTool, "engineer", client as any, { tenant: "t", runId: "r1" });
  await assert.rejects(() => wrapped.handler({ x: "hi" }, makeCtx(client)), /boom/);
  const planSess = client.sessions.get(`r1:${PLAN_SESSION_SUFFIX}`)!;
  const ev = planSess.appended[0];
  assert.ok((ev.payload as any)._error?.includes("boom"), "error observation should include error message");
});

test("relates_to_step is forwarded when params include step_id", async () => {
  const client = new FakeClient();
  const wrapped = wrapToolForObservation(fakeTool, "engineer", client as any, { tenant: "t", runId: "r1" });
  await wrapped.handler({ x: "hi", step_id: "s7" }, makeCtx(client));
  const planSess = client.sessions.get(`r1:${PLAN_SESSION_SUFFIX}`)!;
  assert.equal(planSess.appended[0].relates_to_step, "s7");
});

test("summarizePayload truncates long strings", () => {
  const long = "a".repeat(500);
  const out = summarizePayload({ note: long }) as any;
  assert.equal(out._truncated, true);
  assert.ok(out.preview.length <= 240);
});
