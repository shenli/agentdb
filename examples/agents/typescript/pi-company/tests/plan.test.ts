import { test } from "node:test";
import assert from "node:assert/strict";
import { reducePlanEvents } from "../src/plan.ts";
import type { AgentEvent } from "../src/types.ts";

test("reducePlanEvents returns empty plan for empty events", () => {
  const out = reducePlanEvents([]);
  assert.deepEqual(out.steps, []);
  assert.deepEqual(out.observations, []);
});

const ev = (payload: any, ts_ms = Date.now()): AgentEvent => ({ payload, ts_ms } as any);

test("plan_revised defines the step list", () => {
  const events = [
    ev({ kind: "plan_revised", reason: "initial", steps: [
      { id: "s1", text: "scope mvp", assigned: "engineer", status: "pending" },
      { id: "s2", text: "draft launch copy", assigned: "marketing", status: "pending" },
    ]}),
  ];
  const out = reducePlanEvents(events);
  assert.equal(out.steps.length, 2);
  assert.equal(out.steps[0].id, "s1");
  assert.equal(out.steps[0].status, "pending");
});

test("step_dispatched flips status to in_progress", () => {
  const events = [
    ev({ kind: "plan_revised", reason: "x", steps: [
      { id: "s1", text: "t", assigned: "engineer", status: "pending" },
    ]}),
    ev({ kind: "step_dispatched", step_id: "s1", assigned: "engineer", text: "t" }),
  ];
  const out = reducePlanEvents(events);
  assert.equal(out.steps[0].status, "in_progress");
});

test("step_completed with status=ok flips to done", () => {
  const events = [
    ev({ kind: "plan_revised", reason: "x", steps: [
      { id: "s1", text: "t", assigned: "engineer", status: "pending" },
    ]}),
    ev({ kind: "step_dispatched", step_id: "s1", assigned: "engineer", text: "t" }),
    ev({ kind: "step_completed", step_id: "s1", status: "ok", summary: "ok", from: "engineer" }),
  ];
  const out = reducePlanEvents(events);
  assert.equal(out.steps[0].status, "done");
});

test("step_completed with status=failed flips to failed", () => {
  const events = [
    ev({ kind: "plan_revised", reason: "x", steps: [
      { id: "s1", text: "t", assigned: "engineer", status: "pending" },
    ]}),
    ev({ kind: "step_completed", step_id: "s1", status: "failed", summary: "x", from: "engineer" }),
  ];
  const out = reducePlanEvents(events);
  assert.equal(out.steps[0].status, "failed");
});

test("step_completed for unknown step_id is ignored", () => {
  const events = [
    ev({ kind: "plan_revised", reason: "x", steps: [
      { id: "s1", text: "t", assigned: "engineer", status: "pending" },
    ]}),
    ev({ kind: "step_completed", step_id: "ghost", status: "ok", summary: "x", from: "engineer" }),
  ];
  const out = reducePlanEvents(events);
  assert.equal(out.steps[0].status, "pending");
});

test("a second plan_revised replaces the prior step list", () => {
  const events = [
    ev({ kind: "plan_revised", reason: "v1", steps: [
      { id: "s1", text: "old", assigned: "engineer", status: "pending" },
    ]}),
    ev({ kind: "plan_revised", reason: "v2", steps: [
      { id: "s2", text: "new a", assigned: "engineer", status: "pending" },
      { id: "s3", text: "new b", assigned: "marketing", status: "pending" },
    ]}),
  ];
  const out = reducePlanEvents(events);
  assert.equal(out.steps.length, 2);
  assert.equal(out.steps[0].id, "s2");
});

test("observation events accumulate without affecting steps", () => {
  const events = [
    ev({ kind: "plan_revised", reason: "x", steps: [
      { id: "s1", text: "t", assigned: "engineer", status: "pending" },
    ]}),
    ev({ kind: "observation", from: "engineer", toolName: "write_doc", payload: { title: "a" } }),
    ev({ kind: "observation", from: "engineer", toolName: "write_doc", payload: { title: "b" }, relates_to_step: "s1" }),
  ];
  const out = reducePlanEvents(events);
  assert.equal(out.steps[0].status, "pending");
  assert.equal(out.observations.length, 2);
  assert.equal(out.observations[1].relates_to_step, "s1");
});

test("step_timeout marks an in-progress step as failed", () => {
  const events = [
    ev({ kind: "plan_revised", reason: "x", steps: [
      { id: "s1", text: "t", assigned: "engineer", status: "pending" },
    ]}),
    ev({ kind: "step_dispatched", step_id: "s1", assigned: "engineer", text: "t" }),
    ev({ kind: "step_timeout", step_id: "s1" }),
  ];
  const out = reducePlanEvents(events);
  assert.equal(out.steps[0].status, "failed");
});

test("step_timeout after step_completed is ignored", () => {
  const events = [
    ev({ kind: "plan_revised", reason: "x", steps: [
      { id: "s1", text: "t", assigned: "engineer", status: "pending" },
    ]}),
    ev({ kind: "step_completed", step_id: "s1", status: "ok", summary: "ok", from: "engineer" }),
    ev({ kind: "step_timeout", step_id: "s1" }),
  ];
  const out = reducePlanEvents(events);
  assert.equal(out.steps[0].status, "done");
});

test("non-plan events are ignored", () => {
  const events = [
    ev({ kind: "_noop" }),
    ev({ kind: "directive", text: "hello" }),
  ];
  const out = reducePlanEvents(events);
  assert.deepEqual(out.steps, []);
  assert.deepEqual(out.observations, []);
});

import {
  appendPlanRevised,
  appendStepDispatched,
  appendStepCompleted,
  appendObservation,
  appendStepTimeout,
  loadCurrentPlan,
  PLAN_SESSION_SUFFIX,
} from "../src/plan.ts";

class FakeSession {
  appended: any[] = [];
  recent: any[] = [];
  async append(event: any, opts: any) {
    this.appended.push({ event, opts });
    return { seq: this.appended.length, recent: [] };
  }
  async fetchRecent(n: number) {
    return { recent: this.recent.slice(-n) };
  }
}

class FakeClient {
  sessions = new Map<string, FakeSession>();
  session(id: string, _opts?: any) {
    if (!this.sessions.has(id)) this.sessions.set(id, new FakeSession());
    return this.sessions.get(id)!;
  }
}

test("appendPlanRevised writes a plan_revised event to {runId}:_plan", async () => {
  const client = new FakeClient();
  await appendPlanRevised(client as any, { tenant: "t", runId: "r1" }, [
    { id: "s1", text: "do it", assigned: "engineer", status: "pending" },
  ], "initial");
  const sess = client.sessions.get(`r1:${PLAN_SESSION_SUFFIX}`);
  assert.ok(sess);
  assert.equal(sess.appended.length, 1);
  assert.equal(sess.appended[0].event.kind, "plan_revised");
  assert.equal(sess.appended[0].event.reason, "initial");
  assert.equal(sess.appended[0].event.steps.length, 1);
});

test("appendStepDispatched + appendStepCompleted both target {runId}:_plan", async () => {
  const client = new FakeClient();
  const run = { tenant: "t", runId: "r1" };
  await appendStepDispatched(client as any, run, "s1", "engineer", "do it");
  await appendStepCompleted(client as any, run, "s1", "ok", "did it", "engineer");
  const sess = client.sessions.get(`r1:${PLAN_SESSION_SUFFIX}`)!;
  assert.equal(sess.appended.length, 2);
  assert.equal(sess.appended[0].event.kind, "step_dispatched");
  assert.equal(sess.appended[1].event.kind, "step_completed");
  assert.equal(sess.appended[1].event.from, "engineer");
});

test("appendObservation includes relates_to_step when provided", async () => {
  const client = new FakeClient();
  const run = { tenant: "t", runId: "r1" };
  await appendObservation(client as any, run, "engineer", "write_doc", { title: "x" }, "s1");
  const sess = client.sessions.get(`r1:${PLAN_SESSION_SUFFIX}`)!;
  assert.equal(sess.appended[0].event.kind, "observation");
  assert.equal(sess.appended[0].event.relates_to_step, "s1");
});

test("appendStepTimeout writes a step_timeout event to {runId}:_plan", async () => {
  const client = new FakeClient();
  await appendStepTimeout(client as any, { tenant: "t", runId: "r1" }, "s1");
  const sess = client.sessions.get(`r1:${PLAN_SESSION_SUFFIX}`)!;
  assert.equal(sess.appended.length, 1);
  assert.equal(sess.appended[0].event.kind, "step_timeout");
  assert.equal(sess.appended[0].event.step_id, "s1");
});

test("loadCurrentPlan reduces session events and truncates observations", async () => {
  const client = new FakeClient();
  const run = { tenant: "t", runId: "r1" };
  const sess = client.session(`r1:${PLAN_SESSION_SUFFIX}`, { tenant: "t" }) as any as FakeSession;
  sess.recent = [
    { payload: { kind: "plan_revised", reason: "v1", steps: [
      { id: "s1", text: "t", assigned: "engineer", status: "pending" },
    ]}},
    ...Array.from({ length: 25 }, (_, i) => ({
      payload: { kind: "observation", from: "engineer", toolName: "write_doc", payload: { i } },
    })),
  ];
  const out = await loadCurrentPlan(client as any, run, 10);
  assert.equal(out.steps.length, 1);
  assert.equal(out.observations.length, 10);
});
