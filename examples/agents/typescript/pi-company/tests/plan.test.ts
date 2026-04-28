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
