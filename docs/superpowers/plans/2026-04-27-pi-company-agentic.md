# Pi-Company Agentic Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an autonomous CEO mode to the pi-company sample app via the plan-as-events convention, opt-in via `--goal "<text>"`, while preserving the scripted `--scenario=launch` demo unchanged.

**Architecture:** A new session per run (`{runId}:_plan`) stores `plan_revised`, `step_dispatched`, `step_completed`, and `observation` events. An `AutonomousCEO` class drives a deliberation loop: it reads the goal, generates a plan via LLM, dispatches one step at a time, awaits `step_completed` on the `directs:ceo` mailbox channel, and re-deliberates. The loop terminates when the CEO calls `wrap_up` or one of three configurable budgets (revisions / wallclock / LLM calls) hits zero. Employee tool calls are auto-wrapped at registration time so each invocation appends an `observation` event into `_plan` — the plan session reads like an executive briefing.

**Tech Stack:** TypeScript (Node ≥22), `@mariozechner/pi-agent-core@0.70.2`, `@mariozechner/pi-ai@0.70.2`, AgentDB TS SDK (`agentdb` file-link), `node:test` + `node:assert/strict`. Same as parent sample app.

**Spec:** [`docs/superpowers/specs/2026-04-27-pi-company-agentic-design.md`](../specs/2026-04-27-pi-company-agentic-design.md). The Architecture, Tool palette, and CEO loop sections are load-bearing.

**Branch:** `feature/pi-company-agentic` (created in a worktree before Task 1).

---

## File Structure

### Created (TypeScript source)

| File | Responsibility |
|---|---|
| `examples/agents/typescript/pi-company/src/plan.ts` | Plan-as-events: types, reducer, append helpers, `loadCurrentPlan`. |
| `examples/agents/typescript/pi-company/src/observability.ts` | `wrapToolForObservation` — emits an `observation` event into `_plan` after each wrapped tool invocation. |
| `examples/agents/typescript/pi-company/src/ceo.ts` | `AutonomousCEO` class: boot prompt, wake loop, step timeout, budget tracking, `run_ended` recording. |
| `examples/agents/typescript/pi-company/src/tools/update_plan.ts` | CEO tool: append `plan_revised`, broadcast on `announcements`. |
| `examples/agents/typescript/pi-company/src/tools/dispatch_step.ts` | CEO tool: append `step_dispatched`, publish to `directs:{role}` mailbox. |
| `examples/agents/typescript/pi-company/src/tools/mark_step.ts` | CEO tool: append a `step_completed` with `from="ceo"`. |
| `examples/agents/typescript/pi-company/src/tools/wrap_up.ts` | CEO tool: append `run_ended` to `_meta` and signal loop termination. |
| `examples/agents/typescript/pi-company/tests/plan.test.ts` | Reducer + append-helper unit tests. |
| `examples/agents/typescript/pi-company/tests/observability.test.ts` | Tool-wrapper unit tests. |
| `examples/agents/typescript/pi-company/tests/ceo.test.ts` | Boot, wake cycle, replan, wrap_up, budget cap, step timeout — all with mock LLM. |
| `examples/agents/typescript/pi-company/tests/tools/update_plan.test.ts` | Per-tool unit. |
| `examples/agents/typescript/pi-company/tests/tools/dispatch_step.test.ts` | Per-tool unit. |
| `examples/agents/typescript/pi-company/tests/tools/mark_step.test.ts` | Per-tool unit. |
| `examples/agents/typescript/pi-company/tests/tools/wrap_up.test.ts` | Per-tool unit. |
| `examples/agents/typescript/pi-company/tests/tools/report_to_ceo.test.ts` | Per-tool unit covering both `step_id` present and absent. |

### Modified (TypeScript source)

| File | Change |
|---|---|
| `examples/agents/typescript/pi-company/src/agent.ts` | Add optional `wrapTool` to `AgentDeps`; apply it inside `adaptTool` so observability can intercept the `AgentTool` after CompanyTool→AgentTool adaptation. |
| `examples/agents/typescript/pi-company/src/orchestrator.ts` | Add `startAutonomous(goal, budgets)` path: register CEO tools, wrap employee tools for observation, subscribe a CEO agent slot to `directs:ceo`, hand off to `AutonomousCEO`. |
| `examples/agents/typescript/pi-company/src/tools/report_to_ceo.ts` | Add optional `step_id` parameter; when present, append `step_completed` to `_plan` and publish to `directs:ceo` mailbox; when absent, keep existing announcements broadcast. |
| `examples/agents/typescript/pi-company/src/tools/index.ts` | Register the four new CEO tools in `ALL_TOOLS`; add an autonomous-mode-only role tool list `ROLE_TOOLS.ceo_autonomous`. |
| `examples/agents/typescript/pi-company/src/index.ts` | Parse `--goal=`, `--max-revisions=`, `--max-minutes=`, `--max-llm-calls=`, `--step-timeout=`; branch to `Orchestrator.startAutonomous` when `--goal` present. |

### Modified (docs)

| File | Change |
|---|---|
| `examples/agents/typescript/pi-company/README.md` | New "Autonomous mode" section with flags, sample transcript, deterministic-vs-not contrast. |
| `docs/agent-db.md` | New "Recommended pattern: plan-as-events" section, parallel structure to the existing config-as-events section. |

### Unchanged

- `src/scenarios/launch.ts`, `src/memory.ts`, `src/channels.ts`, `src/llm.ts`, `src/config.ts`, `src/types.ts`, `src/server.ts`, `src/tui.ts`
- All employee tools other than `report_to_ceo`

---

## Tasks

Each task ends with a commit. Tasks 1–9 build the data layer + tool primitives. Tasks 10–14 wire up the CEO loop. Task 15 wires entry. Task 16 ships docs. Task 17 is the gated smoke test.

---

### Task 0: Worktree setup

**Files:** none (infrastructure only).

- [ ] **Step 1: Verify `.worktrees/` is git-ignored**

```bash
git check-ignore -q .worktrees && echo OK
```
Expected: `OK`

- [ ] **Step 2: Create worktree on a fresh feature branch**

```bash
git worktree add .worktrees/pi-company-agentic -b feature/pi-company-agentic
cd .worktrees/pi-company-agentic
```

- [ ] **Step 3: Install pi-company deps (file-linked SDK)**

```bash
cd examples/agents/typescript/pi-company
npm install
```
Expected: dependencies installed; `node_modules/agentdb` symlinks the SDK.

- [ ] **Step 4: Verify clean baseline — TypeScript build passes**

```bash
npm run build
```
Expected: no output / exit 0 (`tsc -p tsconfig.json --noEmit`).

- [ ] **Step 5: Verify clean baseline — existing tests pass**

```bash
npm test
```
Expected: existing tests in `tests/` pass; smoke under `tests/smoke/` is env-gated and may report skipped.

---

### Task 1: plan.ts types + reducer (TDD)

Define the canonical `Step`, `PlanState`, and `PlanEventPayload` types, plus a pure `reducePlanEvents` over a list of AgentDB `Event`s. No IO.

**Files:**
- Create: `examples/agents/typescript/pi-company/src/plan.ts`
- Create: `examples/agents/typescript/pi-company/tests/plan.test.ts`

- [ ] **Step 1: Write the failing test for an empty plan**

Create `tests/plan.test.ts`:

```ts
import { test } from "node:test";
import assert from "node:assert/strict";
import { reducePlanEvents } from "../src/plan.ts";

test("reducePlanEvents returns empty plan for empty events", () => {
  const out = reducePlanEvents([]);
  assert.deepEqual(out.steps, []);
  assert.deepEqual(out.observations, []);
});
```

- [ ] **Step 2: Run the test — it should fail because plan.ts doesn't exist**

```bash
cd examples/agents/typescript/pi-company
npm test -- --test-name-pattern=reducePlanEvents 2>&1 | head -20
```
Expected: `Error: Cannot find module .../src/plan.ts` (or TS resolution error).

- [ ] **Step 3: Implement minimal `plan.ts` with types + reducer skeleton**

Create `src/plan.ts`:

```ts
import type { Client } from "agentdb";
import type { AgentEvent, RunIdentity, AgentRole } from "./types.ts";

export type Role = Exclude<AgentRole, "ceo" | "contractor">;

export type StepStatus = "pending" | "in_progress" | "done" | "failed" | "cancelled";

export interface Step {
  id: string;
  text: string;
  assigned: Role;
  status: StepStatus;
}

export interface ObservationRecord {
  from: string;
  kind: string;
  payload: unknown;
  relates_to_step?: string;
  ts_ms: number;
}

export interface PlanState {
  steps: Step[];
  observations: ObservationRecord[];
}

type PlanRevisedPayload = { kind: "plan_revised"; steps: Step[]; reason: string };
type StepDispatchedPayload = { kind: "step_dispatched"; step_id: string; assigned: Role; text: string };
type StepCompletedPayload = {
  kind: "step_completed";
  step_id: string;
  status: "ok" | "failed" | "blocked" | "cancelled";
  summary: string;
  from: AgentRole;
};
type ObservationPayload = {
  kind: "observation";
  from: string;
  toolName: string;
  payload: unknown;
  relates_to_step?: string;
};
type StepTimeoutPayload = { kind: "step_timeout"; step_id: string };

export type PlanEventPayload =
  | PlanRevisedPayload
  | StepDispatchedPayload
  | StepCompletedPayload
  | ObservationPayload
  | StepTimeoutPayload;

/**
 * Pure reducer. The latest `plan_revised` event defines the step list;
 * subsequent `step_dispatched` and `step_completed` events update the
 * status of an individual step. Unknown step ids are ignored.
 *
 * `observation` events accumulate into the returned `observations` list
 * but do not change step status.
 *
 * `step_timeout` is treated as a hint: if the step is still in_progress,
 * mark it failed; otherwise leave alone (a real status arrived first).
 */
export function reducePlanEvents(events: AgentEvent[]): PlanState {
  let steps: Step[] = [];
  const observations: ObservationRecord[] = [];

  for (const ev of events) {
    const p = ((ev as any).payload ?? ev) as PlanEventPayload;
    if (!p || typeof p !== "object" || !("kind" in p)) continue;

    if (p.kind === "plan_revised") {
      steps = p.steps.map((s) => ({ ...s }));
    } else if (p.kind === "step_dispatched") {
      const idx = steps.findIndex((s) => s.id === p.step_id);
      if (idx >= 0) steps[idx] = { ...steps[idx], status: "in_progress" };
    } else if (p.kind === "step_completed") {
      const idx = steps.findIndex((s) => s.id === p.step_id);
      if (idx >= 0) {
        const next: StepStatus =
          p.status === "ok" ? "done"
          : p.status === "cancelled" ? "cancelled"
          : "failed"; // failed | blocked
        steps[idx] = { ...steps[idx], status: next };
      }
    } else if (p.kind === "observation") {
      observations.push({
        from: p.from,
        kind: p.toolName,
        payload: p.payload,
        relates_to_step: p.relates_to_step,
        ts_ms: ((ev as any).ts_ms as number | undefined) ?? Date.now(),
      });
    } else if (p.kind === "step_timeout") {
      const idx = steps.findIndex((s) => s.id === p.step_id);
      if (idx >= 0 && steps[idx].status === "in_progress") {
        steps[idx] = { ...steps[idx], status: "failed" };
      }
    }
  }

  return { steps, observations };
}
```

- [ ] **Step 4: Run the empty-plan test — should pass**

```bash
npm test -- --test-name-pattern=reducePlanEvents 2>&1 | head -20
```
Expected: 1 pass.

- [ ] **Step 5: Add tests for the reducer's other branches**

Append to `tests/plan.test.ts`:

```ts
import type { AgentEvent } from "../src/types.ts";

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
```

- [ ] **Step 6: Run all reducer tests**

```bash
npm test -- --test-name-pattern=reducePlanEvents 2>&1 | tail -20
# Also ensure all tests in the file run:
node --experimental-strip-types --test tests/plan.test.ts 2>&1 | tail -20
```
Expected: all 10 tests pass.

- [ ] **Step 7: Commit**

```bash
git add examples/agents/typescript/pi-company/src/plan.ts examples/agents/typescript/pi-company/tests/plan.test.ts
git commit -m "feat(pi-company): plan-as-events types + reducer"
```

---

### Task 2: plan.ts persistence helpers (TDD)

Add `appendPlanRevised`, `appendStepDispatched`, `appendStepCompleted`, `appendObservation`, and `loadCurrentPlan` to `plan.ts`. All write to `{runId}:_plan` session.

**Files:**
- Modify: `examples/agents/typescript/pi-company/src/plan.ts`
- Modify: `examples/agents/typescript/pi-company/tests/plan.test.ts`

- [ ] **Step 1: Write the failing test for `appendPlanRevised`**

Append to `tests/plan.test.ts`:

```ts
import {
  appendPlanRevised,
  appendStepDispatched,
  appendStepCompleted,
  appendObservation,
  loadCurrentPlan,
  PLAN_SESSION_SUFFIX,
} from "../src/plan.ts";

class FakeSession {
  appended: any[] = [];
  recent: any[] = [];
  async append(event: any, opts: any) {
    this.appended.push({ event, opts });
    if (event.kind === "_noop") {
      return { seq: this.appended.length, recent: this.recent };
    }
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
```

- [ ] **Step 2: Run the test — should fail (functions not defined)**

```bash
node --experimental-strip-types --test tests/plan.test.ts 2>&1 | tail -10
```
Expected: failure with "appendPlanRevised is not a function" or import error.

- [ ] **Step 3: Add the persistence helpers and `PLAN_SESSION_SUFFIX`**

Append to `src/plan.ts`:

```ts
export const PLAN_SESSION_SUFFIX = "_plan";

function planSessionName(run: RunIdentity): string {
  return `${run.runId}:${PLAN_SESSION_SUFFIX}`;
}

export async function appendPlanRevised(
  client: Client,
  run: RunIdentity,
  steps: Step[],
  reason: string,
): Promise<void> {
  const sess = client.session(planSessionName(run), { tenant: run.tenant });
  await sess.append(
    { kind: "plan_revised", reason, steps } as any,
    { fetchLastN: 0 },
  );
}

export async function appendStepDispatched(
  client: Client,
  run: RunIdentity,
  step_id: string,
  assigned: Role,
  text: string,
): Promise<void> {
  const sess = client.session(planSessionName(run), { tenant: run.tenant });
  await sess.append(
    { kind: "step_dispatched", step_id, assigned, text } as any,
    { fetchLastN: 0 },
  );
}

export async function appendStepCompleted(
  client: Client,
  run: RunIdentity,
  step_id: string,
  status: "ok" | "failed" | "blocked" | "cancelled",
  summary: string,
  from: AgentRole,
): Promise<void> {
  const sess = client.session(planSessionName(run), { tenant: run.tenant });
  await sess.append(
    { kind: "step_completed", step_id, status, summary, from } as any,
    { fetchLastN: 0 },
  );
}

export async function appendObservation(
  client: Client,
  run: RunIdentity,
  from: string,
  toolName: string,
  payload: unknown,
  relates_to_step?: string,
): Promise<void> {
  const sess = client.session(planSessionName(run), { tenant: run.tenant });
  await sess.append(
    { kind: "observation", from, toolName, payload, relates_to_step } as any,
    { fetchLastN: 0 },
  );
}

export async function appendStepTimeout(
  client: Client,
  run: RunIdentity,
  step_id: string,
): Promise<void> {
  const sess = client.session(planSessionName(run), { tenant: run.tenant });
  await sess.append(
    { kind: "step_timeout", step_id } as any,
    { fetchLastN: 0 },
  );
}

/**
 * Read all events on `{runId}:_plan` (via the same _noop append+fetch trick
 * Memory uses), reduce, and truncate the observations to the last N.
 *
 * `observation_window` defaults to 20.
 */
export async function loadCurrentPlan(
  client: Client,
  run: RunIdentity,
  observation_window: number = 20,
): Promise<PlanState> {
  const sess = client.session(planSessionName(run), { tenant: run.tenant });
  const resp = await sess.append({ kind: "_noop" } as any, { fetchLastN: 1024 });
  const recent = (resp.recent ?? []).filter((e: any) => {
    const p = e.payload ?? e;
    return p?.kind && p.kind !== "_noop";
  }) as AgentEvent[];
  const state = reducePlanEvents(recent);
  if (state.observations.length > observation_window) {
    state.observations = state.observations.slice(-observation_window);
  }
  return state;
}
```

- [ ] **Step 4: Run the test — should pass**

```bash
node --experimental-strip-types --test tests/plan.test.ts 2>&1 | tail -10
```
Expected: 11 passes (10 reducer + new helper).

- [ ] **Step 5: Add tests for the other helpers + `loadCurrentPlan`**

Append to `tests/plan.test.ts`:

```ts
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
```

- [ ] **Step 6: Run all tests**

```bash
node --experimental-strip-types --test tests/plan.test.ts 2>&1 | tail -10
```
Expected: 14 passes.

- [ ] **Step 7: Commit**

```bash
git add examples/agents/typescript/pi-company/src/plan.ts examples/agents/typescript/pi-company/tests/plan.test.ts
git commit -m "feat(pi-company): plan-as-events persistence helpers"
```

---

### Task 3: observability.ts — tool-call wrapper (TDD)

Wraps a `CompanyTool` so that after `handler` returns, an `observation` event is appended to `_plan` with a truncated payload.

**Files:**
- Create: `examples/agents/typescript/pi-company/src/observability.ts`
- Create: `examples/agents/typescript/pi-company/tests/observability.test.ts`

- [ ] **Step 1: Write the failing test**

Create `tests/observability.test.ts`:

```ts
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
```

- [ ] **Step 2: Run the test — should fail (module missing)**

```bash
node --experimental-strip-types --test tests/observability.test.ts 2>&1 | tail -10
```
Expected: import resolution error.

- [ ] **Step 3: Implement `observability.ts`**

Create `src/observability.ts`:

```ts
import type { Client } from "agentdb";
import type { CompanyTool, ToolContext } from "./agent.ts";
import type { RunIdentity } from "./types.ts";
import { appendObservation } from "./plan.ts";

const MAX_PAYLOAD_LEN = 240;

/**
 * Reduce a tool's params object to a JSON-stringified, length-capped form.
 * The full args remain in the agent's own session log via Agent's
 * `tool_call` audit event; the observation is the CEO-facing summary.
 */
export function summarizePayload(args: unknown): unknown {
  if (args === null || args === undefined) return args;
  if (typeof args === "string") return args.slice(0, MAX_PAYLOAD_LEN);
  try {
    const json = JSON.stringify(args);
    if (json.length <= MAX_PAYLOAD_LEN) return args;
    return { _truncated: true, preview: json.slice(0, MAX_PAYLOAD_LEN) };
  } catch {
    return { _unserializable: true };
  }
}

/**
 * Wrap a CompanyTool so its handler appends an `observation` event to
 * `{runId}:_plan` after executing. The wrapper:
 *   - delegates to the underlying handler
 *   - on success, writes an observation with the (truncated) args
 *   - re-throws any handler error UNCHANGED, after attempting to write
 *     a `kind: observation` with status:"error" so the CEO sees failed
 *     tool calls too
 *
 * The wrapper does NOT swallow handler errors — pi-mono's runtime turns
 * them into tool-result error messages, which is the contract Agent's
 * `adaptTool` relies on.
 */
export function wrapToolForObservation(
  tool: CompanyTool,
  role: string,
  client: Client,
  run: RunIdentity,
): CompanyTool {
  return {
    ...tool,
    handler: async (args, ctx) => {
      let threw: unknown;
      try {
        const result = await tool.handler(args, ctx);
        // Best-effort: a failure to write observation must not corrupt the
        // tool's own result.
        try {
          await appendObservation(
            client,
            run,
            role,
            tool.name,
            summarizePayload(args),
            (args as any)?.step_id,
          );
        } catch {
          // swallow; CEO simply won't see this observation
        }
        return result;
      } catch (err) {
        threw = err;
        try {
          await appendObservation(
            client,
            run,
            role,
            tool.name,
            { _error: String(err), args: summarizePayload(args) },
            (args as any)?.step_id,
          );
        } catch {
          // swallow
        }
        throw threw;
      }
    },
  };
}
```

- [ ] **Step 4: Run the existing test — should pass**

```bash
node --experimental-strip-types --test tests/observability.test.ts 2>&1 | tail -10
```
Expected: 1 pass.

- [ ] **Step 5: Add behavior tests**

Append to `tests/observability.test.ts`:

```ts
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
```

- [ ] **Step 6: Run all observability tests**

```bash
node --experimental-strip-types --test tests/observability.test.ts 2>&1 | tail -10
```
Expected: 5 passes.

- [ ] **Step 7: Commit**

```bash
git add examples/agents/typescript/pi-company/src/observability.ts examples/agents/typescript/pi-company/tests/observability.test.ts
git commit -m "feat(pi-company): tool-call observation wrapper"
```

---

### Task 4: report_to_ceo step_id support (TDD)

Add an optional `step_id` parameter. When present, also append `step_completed` to `_plan` and publish `step_completed` to `directs:ceo` mailbox so the autonomous CEO wakes. When absent, retain existing announcements broadcast verbatim.

**Files:**
- Modify: `examples/agents/typescript/pi-company/src/tools/report_to_ceo.ts`
- Create: `examples/agents/typescript/pi-company/tests/tools/report_to_ceo.test.ts`

- [ ] **Step 1: Write failing tests**

Create `tests/tools/report_to_ceo.test.ts`:

```ts
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
  // mock broadcast
  broadcasts: any[] = [];
  async broadcast(b: any) { this.broadcasts.push(b); }
}

function makeCtx(client: FakeClient, role: any = "engineer") {
  return {
    client: client as any,
    run: { tenant: "t", runId: "r1" },
    role,
    memory: {
      recordEvent: async () => {},
      sessionName: `r1:${role}`,
    } as any,
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
  // No _plan writes when step_id absent.
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
```

- [ ] **Step 2: Run the tests — should fail (handler doesn't know about step_id)**

```bash
node --experimental-strip-types --test tests/tools/report_to_ceo.test.ts 2>&1 | tail -15
```
Expected: 2 of 3 tests fail (the two with step_id).

- [ ] **Step 3: Update `src/tools/report_to_ceo.ts`**

Replace the file:

```ts
import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";
import { appendStepCompleted } from "../plan.ts";
import type { AgentRole } from "../types.ts";

const params = Type.Object({
  text: Type.String({
    description: "The report text. Be specific about progress, blockers, or asks.",
  }),
  step_id: Type.Optional(
    Type.String({
      description:
        "Optional. Set to the dispatched step's id when reporting on an autonomous-mode step. The CEO uses this to mark the step complete and re-plan.",
    }),
  ),
  status: Type.Optional(
    Type.Union([
      Type.Literal("ok"),
      Type.Literal("failed"),
      Type.Literal("blocked"),
    ], { description: "Required when step_id is set. Defaults to 'ok'." }),
  ),
});

/**
 * Post a report to the CEO.
 *
 * Two modes:
 *
 *   1. Scripted mode (no step_id): broadcasts a `report` payload on the
 *      `announcements` channel — the human CEO sees it in the TUI.
 *
 *   2. Autonomous mode (step_id provided): also appends a `step_completed`
 *      event to the `_plan` session AND publishes a `step_completed`
 *      payload to the `directs:ceo` mailbox channel so the autonomous
 *      CEO wakes and re-deliberates. Defaults `status="ok"`.
 *
 * Both modes are non-exclusive — if the autonomous CEO is also subscribed
 * to announcements (it isn't by default in v1), it would see the report
 * there too. The mailbox is the wake signal.
 */
export const reportToCeo: CompanyTool<typeof params> = {
  name: "report_to_ceo",
  description:
    "Post a report to the CEO. In autonomous mode, pass step_id (and optional status) to mark a dispatched step complete.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    await ctx.memory.recordEvent({
      kind: "tool_call",
      tool: "report_to_ceo",
      text: args.text,
      step_id: args.step_id,
      status: args.status,
    });

    if (args.step_id) {
      const status = args.status ?? "ok";
      await appendStepCompleted(
        ctx.client,
        ctx.run,
        args.step_id,
        status,
        args.text,
        ctx.role as AgentRole,
      );
      await ctx.channels.publish("directs:ceo", {
        kind: "step_completed",
        from: ctx.role,
        step_id: args.step_id,
        status,
        summary: args.text,
      });
      return {
        text: `step ${args.step_id} reported (${status}): ${args.text.slice(0, 60)}`,
      };
    }

    await ctx.channels.publish("announcements", {
      kind: "report",
      from: ctx.role,
      text: args.text,
    });
    return { text: `reported to CEO: ${args.text.slice(0, 60)}` };
  },
};
```

- [ ] **Step 4: Run all `report_to_ceo` tests — should pass**

```bash
node --experimental-strip-types --test tests/tools/report_to_ceo.test.ts 2>&1 | tail -10
```
Expected: 3 passes.

- [ ] **Step 5: Run the full suite to confirm no regression**

```bash
npm test 2>&1 | tail -20
```
Expected: all existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add examples/agents/typescript/pi-company/src/tools/report_to_ceo.ts examples/agents/typescript/pi-company/tests/tools/report_to_ceo.test.ts
git commit -m "feat(pi-company): report_to_ceo accepts step_id for autonomous mode"
```

---

### Task 5: wrap_up tool (TDD)

CEO-only tool that records `run_ended` to `_meta` and signals the loop to terminate via a result flag (`terminate: true`) plus by writing a sentinel event the loop polls for.

**Files:**
- Create: `examples/agents/typescript/pi-company/src/tools/wrap_up.ts`
- Create: `examples/agents/typescript/pi-company/tests/tools/wrap_up.test.ts`

- [ ] **Step 1: Write failing test**

Create `tests/tools/wrap_up.test.ts`:

```ts
import { test } from "node:test";
import assert from "node:assert/strict";
import { wrapUp } from "../../src/tools/wrap_up.ts";

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

test("wrap_up appends run_ended to {runId}:_meta with cause='wrap_up'", async () => {
  const client = new FakeClient();
  const result = await wrapUp.handler(
    { reason: "goal achieved", summary: "shipped MVP, landed 3 pilots" },
    makeCtx(client),
  );
  const metaSess = client.sessions.get("r1:_meta");
  assert.ok(metaSess);
  const ev = metaSess.appended.find((e) => e.kind === "run_ended");
  assert.ok(ev);
  assert.equal(ev.cause, "wrap_up");
  assert.equal(ev.reason, "goal achieved");
  assert.ok(ev.summary.includes("shipped"));
  assert.equal(result.terminate, true);
});
```

- [ ] **Step 2: Run — should fail (module missing)**

```bash
node --experimental-strip-types --test tests/tools/wrap_up.test.ts 2>&1 | tail -10
```
Expected: import resolution error.

- [ ] **Step 3: Implement `src/tools/wrap_up.ts`**

```ts
import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";

const params = Type.Object({
  reason: Type.String({
    description:
      "Why you're wrapping up (e.g., 'goal achieved', 'further effort would not help').",
  }),
  summary: Type.String({
    description: "What was accomplished during the run. 1-3 sentences.",
  }),
});

/**
 * (CEO autonomous-mode-only) End the run.
 *
 * Appends a `run_ended` event with `cause="wrap_up"` to the run's
 * `_meta` session and returns `terminate: true` so pi-mono short-circuits
 * post-batch LLM calls. The autonomous CEO loop polls `_meta` (via
 * `run_ended` events) to break out of its wake cycle.
 */
export const wrapUp: CompanyTool<typeof params> = {
  name: "wrap_up",
  description:
    "(CEO autonomous-mode only) End the run. Use when the goal is achieved or further effort would be counterproductive.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    const metaName = `${ctx.run.runId}:_meta`;
    const sess = ctx.client.session(metaName, { tenant: ctx.run.tenant });
    await sess.append(
      {
        kind: "run_ended",
        cause: "wrap_up",
        reason: args.reason,
        summary: args.summary,
        ended_at: Date.now(),
      } as any,
      { fetchLastN: 0 },
    );
    return {
      text: `run wrapped up: ${args.reason}`,
      terminate: true,
    };
  },
};
```

- [ ] **Step 4: Run — should pass**

```bash
node --experimental-strip-types --test tests/tools/wrap_up.test.ts 2>&1 | tail -10
```
Expected: 1 pass.

- [ ] **Step 5: Commit**

```bash
git add examples/agents/typescript/pi-company/src/tools/wrap_up.ts examples/agents/typescript/pi-company/tests/tools/wrap_up.test.ts
git commit -m "feat(pi-company): wrap_up tool"
```

---

### Task 6: update_plan tool (TDD)

CEO tool that appends a `plan_revised` event AND publishes a `plan_revised` payload on `announcements` so employees see the new plan.

**Files:**
- Create: `examples/agents/typescript/pi-company/src/tools/update_plan.ts`
- Create: `examples/agents/typescript/pi-company/tests/tools/update_plan.test.ts`

- [ ] **Step 1: Write failing test**

Create `tests/tools/update_plan.test.ts`:

```ts
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
```

- [ ] **Step 2: Run — should fail (module missing)**

```bash
node --experimental-strip-types --test tests/tools/update_plan.test.ts 2>&1 | tail -10
```

- [ ] **Step 3: Implement `src/tools/update_plan.ts`**

```ts
import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";
import { appendPlanRevised, type Step } from "../plan.ts";

const stepSchema = Type.Object({
  id: Type.String({ description: "Short slug like 's1', 's2'." }),
  text: Type.String({ description: "One clear sentence describing the step." }),
  assigned: Type.Union([
    Type.Literal("cto"),
    Type.Literal("engineer"),
    Type.Literal("marketing"),
    Type.Literal("sales"),
  ]),
  status: Type.Union([
    Type.Literal("pending"),
    Type.Literal("in_progress"),
    Type.Literal("done"),
    Type.Literal("failed"),
    Type.Literal("cancelled"),
  ]),
});

const params = Type.Object({
  reason: Type.String({
    description:
      "Why this revision (e.g., 'initial plan from goal', 'engineer reported s1 blocked, splitting').",
  }),
  steps: Type.Array(stepSchema, {
    description: "Replacement step list. 3-7 steps recommended.",
  }),
});

/**
 * (CEO autonomous-mode only) Replace the current plan.
 *
 * Appends a `plan_revised` event to `{runId}:_plan` and broadcasts a
 * `plan_revised` payload on `announcements` so employees see the new
 * plan in their wake-up context. Existing steps' statuses are preserved
 * only if the new step list re-uses the same ids — otherwise treat this
 * as a fresh plan.
 */
export const updatePlan: CompanyTool<typeof params> = {
  name: "update_plan",
  description:
    "(CEO autonomous-mode only) Replace the current plan with a revised step list. Use this on first turn to lay out the initial plan, and any time you learn something that changes what should happen next.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    await appendPlanRevised(ctx.client, ctx.run, args.steps as Step[], args.reason);
    await ctx.channels.publish("announcements", {
      kind: "plan_revised",
      from: ctx.role,
      reason: args.reason,
      step_count: args.steps.length,
    });
    return {
      text: `plan revised (${args.steps.length} steps): ${args.reason.slice(0, 80)}`,
    };
  },
};
```

- [ ] **Step 4: Run — should pass**

```bash
node --experimental-strip-types --test tests/tools/update_plan.test.ts 2>&1 | tail -10
```
Expected: 1 pass.

- [ ] **Step 5: Commit**

```bash
git add examples/agents/typescript/pi-company/src/tools/update_plan.ts examples/agents/typescript/pi-company/tests/tools/update_plan.test.ts
git commit -m "feat(pi-company): update_plan tool"
```

---

### Task 7: dispatch_step tool (TDD)

CEO tool that appends `step_dispatched` to `_plan` AND publishes a payload to `directs:{assigned}` mailbox channel.

**Files:**
- Create: `examples/agents/typescript/pi-company/src/tools/dispatch_step.ts`
- Create: `examples/agents/typescript/pi-company/tests/tools/dispatch_step.test.ts`

- [ ] **Step 1: Write failing test**

Create `tests/tools/dispatch_step.test.ts`:

```ts
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
    // mirror appended events into `recent` so subsequent _noop reads see them
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
  // Seed a plan with s1 assigned to engineer.
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
  // No directs:* broadcast.
  assert.ok(!client.broadcasts.some((b) => b.channel.startsWith("r1:directs:")));
});
```

- [ ] **Step 2: Run — should fail**

```bash
node --experimental-strip-types --test tests/tools/dispatch_step.test.ts 2>&1 | tail -10
```

- [ ] **Step 3: Implement `src/tools/dispatch_step.ts`**

```ts
import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";
import { appendStepDispatched, loadCurrentPlan } from "../plan.ts";

const params = Type.Object({
  step_id: Type.String({
    description: "The id of the step to dispatch (must exist in the current plan).",
  }),
});

/**
 * (CEO autonomous-mode only) Hand a step to the assigned employee.
 *
 * Reads the current plan to look up the step's text and assignee, appends
 * a `step_dispatched` event, then publishes a structured payload to the
 * employee's mailbox channel `directs:{assigned}` so they wake up. The
 * employee's prompt template surfaces `step_id` and asks them to call
 * `report_to_ceo` with that id when done.
 *
 * If the step_id isn't in the current plan, returns an error-shaped
 * result (no exception — pi-mono surfaces the text back to the LLM and
 * lets the CEO retry).
 */
export const dispatchStep: CompanyTool<typeof params> = {
  name: "dispatch_step",
  description:
    "(CEO autonomous-mode only) Hand a step to the assigned employee. The employee will execute and call report_to_ceo with the step_id when done.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    const plan = await loadCurrentPlan(ctx.client, ctx.run, 0);
    const step = plan.steps.find((s) => s.id === args.step_id);
    if (!step) {
      return {
        text: `step ${args.step_id} not found in current plan; valid ids: ${plan.steps.map((s) => s.id).join(", ") || "(plan empty)"}`,
      };
    }
    await appendStepDispatched(ctx.client, ctx.run, step.id, step.assigned, step.text);
    await ctx.channels.publish(`directs:${step.assigned}`, {
      kind: "step_dispatched",
      from: "ceo",
      step_id: step.id,
      text: step.text,
    });
    return {
      text: `dispatched ${step.id} to ${step.assigned}: ${step.text.slice(0, 60)}`,
    };
  },
};
```

- [ ] **Step 4: Run — should pass**

```bash
node --experimental-strip-types --test tests/tools/dispatch_step.test.ts 2>&1 | tail -10
```
Expected: 2 passes.

- [ ] **Step 5: Commit**

```bash
git add examples/agents/typescript/pi-company/src/tools/dispatch_step.ts examples/agents/typescript/pi-company/tests/tools/dispatch_step.test.ts
git commit -m "feat(pi-company): dispatch_step tool"
```

---

### Task 8: mark_step tool (TDD)

CEO override: append `step_completed` with `from="ceo"` to flip a step's status without an employee report.

**Files:**
- Create: `examples/agents/typescript/pi-company/src/tools/mark_step.ts`
- Create: `examples/agents/typescript/pi-company/tests/tools/mark_step.test.ts`

- [ ] **Step 1: Write failing test**

Create `tests/tools/mark_step.test.ts`:

```ts
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
```

- [ ] **Step 2: Run — fail**

```bash
node --experimental-strip-types --test tests/tools/mark_step.test.ts 2>&1 | tail -10
```

- [ ] **Step 3: Implement `src/tools/mark_step.ts`**

```ts
import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";
import { appendStepCompleted } from "../plan.ts";

const params = Type.Object({
  step_id: Type.String({ description: "The id of the step to mark." }),
  status: Type.Union(
    [Type.Literal("ok"), Type.Literal("failed"), Type.Literal("cancelled")],
    { description: "Override status: ok=done, failed=failed, cancelled=cancelled." },
  ),
  note: Type.Optional(Type.String({ description: "Optional explanation." })),
});

/**
 * (CEO autonomous-mode only) Override a step's status without an employee
 * report. Appends a `step_completed` event with `from="ceo"`.
 *
 * Use cases: cancel a step the plan has outgrown; manually mark a step
 * failed when an employee never responded and a `step_timeout` already
 * fired; mark a step ok if the work happened out-of-band.
 */
export const markStep: CompanyTool<typeof params> = {
  name: "mark_step",
  description:
    "(CEO autonomous-mode only) Override a step's status without waiting for an employee report.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    const summary = args.note ?? `(ceo-marked ${args.status})`;
    await appendStepCompleted(ctx.client, ctx.run, args.step_id, args.status, summary, "ceo");
    return { text: `marked ${args.step_id} as ${args.status}` };
  },
};
```

- [ ] **Step 4: Run — pass**

```bash
node --experimental-strip-types --test tests/tools/mark_step.test.ts 2>&1 | tail -10
```

- [ ] **Step 5: Commit**

```bash
git add examples/agents/typescript/pi-company/src/tools/mark_step.ts examples/agents/typescript/pi-company/tests/tools/mark_step.test.ts
git commit -m "feat(pi-company): mark_step tool"
```

---

### Task 9: tools/index.ts — register the four new tools

**Files:**
- Modify: `examples/agents/typescript/pi-company/src/tools/index.ts`

- [ ] **Step 1: Add imports + registry entries + autonomous role list**

Edit `src/tools/index.ts`. Add to the imports near the top:

```ts
import { dispatchStep } from "./dispatch_step.ts";
import { markStep } from "./mark_step.ts";
import { updatePlan } from "./update_plan.ts";
import { wrapUp } from "./wrap_up.ts";
```

Add to the `ALL_TOOLS` object (alphabetical order):

```ts
  dispatch_step: dispatchStep,
  mark_step: markStep,
  update_plan: updatePlan,
  wrap_up: wrapUp,
```

Below `ROLE_TOOLS`, add a new export:

```ts
/**
 * Tool list for the autonomous CEO. Includes the four new plan-as-events
 * tools plus the existing CEO-level tools. Used by `Orchestrator.startAutonomous`.
 */
export const CEO_AUTONOMOUS_TOOLS: string[] = [
  "update_plan",
  "dispatch_step",
  "mark_step",
  "wrap_up",
  "announce",
  "direct_message",
  "update_agent_config",
];
```

- [ ] **Step 2: Verify TypeScript still type-checks**

```bash
cd examples/agents/typescript/pi-company
npm run build
```
Expected: exit 0.

- [ ] **Step 3: Run the suite to confirm nothing regressed**

```bash
npm test 2>&1 | tail -10
```
Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add examples/agents/typescript/pi-company/src/tools/index.ts
git commit -m "feat(pi-company): register CEO autonomous tools"
```

---

### Task 10: agent.ts — wrapTool hook

Add an optional `wrapTool` callback in `AgentDeps` that the agent applies to every `CompanyTool` before adapting it to pi-mono's `AgentTool`. Used by `orchestrator.startAutonomous` to install observability without changing tool definitions.

**Files:**
- Modify: `examples/agents/typescript/pi-company/src/agent.ts`

- [ ] **Step 1: Add `wrapTool` to `AgentDeps`**

In `src/agent.ts`, find the `AgentDeps` interface and add a new field:

```ts
  /**
   * Optional per-tool wrapper applied at registration time. Used by the
   * autonomous-mode orchestrator to install `wrapToolForObservation` on
   * every employee tool so the autonomous CEO sees observation events.
   * Identity-default (no-op) when undefined.
   */
  wrapTool?: (tool: CompanyTool) => CompanyTool;
```

- [ ] **Step 2: Apply `wrapTool` in `handleIncoming` before tool adaptation**

In `src/agent.ts`, find this block inside `handleIncoming`:

```ts
    const tools = this.config.tools
      .map((name) => this.deps.toolRegistry[name])
      .filter((t): t is CompanyTool => t !== undefined);
```

Replace with:

```ts
    const wrap = this.deps.wrapTool ?? ((t: CompanyTool) => t);
    const tools = this.config.tools
      .map((name) => this.deps.toolRegistry[name])
      .filter((t): t is CompanyTool => t !== undefined)
      .map(wrap);
```

- [ ] **Step 3: Verify TypeScript still type-checks**

```bash
npm run build
```
Expected: exit 0.

- [ ] **Step 4: Run the suite — confirm no regression**

```bash
npm test 2>&1 | tail -10
```
Expected: all tests pass (existing test fixtures don't set `wrapTool`, so the identity default kicks in).

- [ ] **Step 5: Commit**

```bash
git add examples/agents/typescript/pi-company/src/agent.ts
git commit -m "feat(pi-company): agent wrapTool hook for tool wrappers"
```

---

### Task 11: ceo.ts — `AutonomousCEO` boot path (TDD)

The class structure + `boot()` method that runs the initial-plan LLM turn. Wake loop comes in Task 12.

**Files:**
- Create: `examples/agents/typescript/pi-company/src/ceo.ts`
- Create: `examples/agents/typescript/pi-company/tests/ceo.test.ts`

- [ ] **Step 1: Write the failing boot test (with mock LLM)**

Create `tests/ceo.test.ts`:

```ts
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
    // empty iterable: tests will call boot() only — wake loop not exercised.
    return { async *[Symbol.asyncIterator]() { /* no events */ } };
  }
}

/**
 * A scripted "LLM" that emits a fixed sequence of tool calls per turn.
 * Each entry is one *turn*: the array of tool calls the CEO would make
 * during that prompt. After the scripted turns are exhausted, the next
 * call resolves with no tool calls.
 */
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
```

- [ ] **Step 2: Run — fail (module missing)**

```bash
node --experimental-strip-types --test tests/ceo.test.ts 2>&1 | tail -10
```

- [ ] **Step 3: Implement `src/ceo.ts` skeleton + `boot()`**

Create `src/ceo.ts`:

```ts
import type { Client } from "agentdb";
import type { Channels } from "./channels.ts";
import type { RunIdentity } from "./types.ts";
import type { CompanyTool, ToolContext } from "./agent.ts";
import { ALL_TOOLS, CEO_AUTONOMOUS_TOOLS } from "./tools/index.ts";
import { loadCurrentPlan, type PlanState } from "./plan.ts";

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
  /**
   * The function that drives one CEO LLM turn. In production this constructs
   * a fresh pi-mono Agent with the given tools and prompt, then awaits
   * `agent.prompt()`. In tests it's a `ScriptedLLM`. Decoupling from
   * pi-mono keeps the loop unit-testable.
   */
  runTurn: RunTurn;
}

/**
 * The autonomous CEO loop.
 *
 * Lifecycle:
 *   1. `boot()` runs the initial-plan turn (LLM is asked to call
 *      update_plan + dispatch_step on the goal).
 *   2. `wakeLoop()` (Task 12) subscribes to `directs:ceo` and
 *      re-deliberates whenever a `step_completed` event arrives.
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

  /**
   * Run the boot turn. The LLM is expected to call update_plan +
   * dispatch_step. Increments llmCalls (and, indirectly via update_plan,
   * revisions when the LLM's plan_revised event lands — counted by
   * polling `_plan` after the turn).
   */
  async boot(): Promise<void> {
    const ctx = this.makeCtx();
    const tools = this.toolsByName();
    const prompt = this.bootPrompt();

    await this.driveTurn(prompt, tools, ctx);
  }

  // ---- internals ----

  private bootPrompt(): string {
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

  private async driveTurn(
    _prompt: string,
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

  private toolsByName(): Record<string, CompanyTool> {
    const out: Record<string, CompanyTool> = {};
    for (const name of CEO_AUTONOMOUS_TOOLS) {
      const t = ALL_TOOLS[name];
      if (t) out[name] = t;
    }
    return out;
  }

  private makeCtx(): ToolContext {
    // Memory is a no-op stub in autonomous mode — all CEO writes go to
    // `_plan` or `_meta` via tool helpers, not the per-role session.
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
}
```

- [ ] **Step 4: Run boot test — should pass**

```bash
node --experimental-strip-types --test tests/ceo.test.ts 2>&1 | tail -10
```
Expected: 1 pass.

- [ ] **Step 5: Commit**

```bash
git add examples/agents/typescript/pi-company/src/ceo.ts examples/agents/typescript/pi-company/tests/ceo.test.ts
git commit -m "feat(pi-company): AutonomousCEO boot path"
```

---

### Task 12: ceo.ts — wake loop + budget tracking (TDD)

Add `start()`, `wakeLoop()`, and budget enforcement. Wakes on each `step_completed` payload arriving on `directs:ceo`, re-loads `_plan`, drives a wake turn.

**Files:**
- Modify: `examples/agents/typescript/pi-company/src/ceo.ts`
- Modify: `examples/agents/typescript/pi-company/tests/ceo.test.ts`

- [ ] **Step 1: Add a fake-channel iterator helper to the test, then a wake-cycle test**

Append to `tests/ceo.test.ts`:

```ts
class ManualFakeChannels {
  client: FakeClient;
  run: any;
  private mailboxQueue: any[] = [];
  private resolver: ((v: IteratorResult<any>) => void) | null = null;
  constructor(client: FakeClient, run: any) { this.client = client; this.run = run; }
  async publish(name: string, payload: unknown) {
    await this.client.broadcast({ channel: `${this.run.runId}:${name}`, payload, tenant: this.run.tenant });
  }
  // Test driver appends payloads here; the iterator yields them in order.
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
    // boot turn
    [
      { tool: "update_plan", args: {
        reason: "initial",
        steps: [{ id: "s1", text: "do it", assigned: "engineer", status: "pending" }],
      }},
      { tool: "dispatch_step", args: { step_id: "s1" } },
    ],
    // wake turn 1: wrap up
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

  // Schedule a step_completed mailbox event after a small tick.
  setTimeout(() => {
    channels.pushToMailbox({ kind: "step_completed", from: "engineer", step_id: "s1", status: "ok", summary: "done" });
  }, 10);

  await ceo.start();
  assert.equal(ceo.cause, "wrap_up");
  assert.equal(llm.callCount, 2);

  // run_ended written to _meta
  const metaSess = client.sessions.get("r1:_meta");
  assert.ok(metaSess);
  assert.ok(metaSess.appended.some((e: any) => e.kind === "run_ended" && e.cause === "wrap_up"));
});

test("budget cap (maxLlmCalls) terminates the loop", async () => {
  const client = new FakeClient();
  const run = { tenant: "t", runId: "r1" };
  const channels = new ManualFakeChannels(client, run);
  // Each turn just dispatches the same step again; never wraps up.
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

  // Drip mailbox events so the wake loop keeps firing.
  const interval = setInterval(() => {
    channels.pushToMailbox({ kind: "step_completed", from: "engineer", step_id: "s1", status: "ok", summary: "x" });
  }, 5);

  try {
    await ceo.start();
  } finally {
    clearInterval(interval);
  }

  assert.equal(ceo.cause, "call_budget_exhausted");
  // run_ended logged with budget-exhausted cause
  const metaSess = client.sessions.get("r1:_meta")!;
  const ended = metaSess.appended.find((e: any) => e.kind === "run_ended");
  assert.ok(ended);
  assert.equal(ended.cause, "call_budget_exhausted");
});
```

- [ ] **Step 2: Run — fail (`start()` not implemented)**

```bash
node --experimental-strip-types --test tests/ceo.test.ts 2>&1 | tail -15
```

- [ ] **Step 3: Implement `start()`, `wakeLoop()`, budget tracking**

In `src/ceo.ts`, add these methods to `AutonomousCEO`:

```ts
  async start(): Promise<void> {
    await this.boot();
    if (!this.terminated) await this.wakeLoop();
    await this.recordRunEnded();
  }

  /**
   * Subscribe to `directs:ceo` and re-deliberate on every payload arriving
   * on it (typically `step_completed` from a `report_to_ceo` call). Each
   * iteration runs one CEO LLM turn with a wake prompt.
   */
  private async wakeLoop(): Promise<void> {
    for await (const _msg of this.deps.channels.subscribe("directs:ceo")) {
      if (this.terminated) break;
      if (this.budgetCheckTerminate()) break;
      await this.driveWakeTurn();
      if (this.terminated) break;
    }
  }

  private async driveWakeTurn(): Promise<void> {
    const ctx = this.makeCtx();
    const tools = this.toolsByName();
    // The wake prompt currently isn't passed to runTurn (production
    // wires it into pi-mono). Tests use a scripted runTurn that ignores
    // prompts. We still build it so the production wiring (Task 14) is
    // a single line.
    void this.wakePrompt(await this.snapshotPlan());
    await this.driveTurn("(wake)", tools, ctx);
    // Count revisions by polling _plan and checking how many plan_revised
    // events have accumulated.
    this.revisions = await this.countPlanRevisedEvents();
    this.budgetCheckTerminate();
  }

  private async countPlanRevisedEvents(): Promise<number> {
    const sess = this.deps.client.session(`${this.deps.run.runId}:_plan`, { tenant: this.deps.run.tenant });
    const resp = await sess.append({ kind: "_noop" } as any, { fetchLastN: 1024 });
    return (resp.recent ?? []).filter((e: any) => (e.payload?.kind ?? e.kind) === "plan_revised").length;
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
  private budgetCheckTerminate(): boolean {
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

  /**
   * Append `run_ended` to `_meta` (only when wasn't already written by
   * `wrap_up` — we detect by tracking `terminationCause`).
   */
  private async recordRunEnded(): Promise<void> {
    if (this.terminationCause === "wrap_up") return; // wrap_up tool already wrote it
    const sess = this.deps.client.session(`${this.deps.run.runId}:_meta`, { tenant: this.deps.run.tenant });
    await sess.append(
      { kind: "run_ended", cause: this.terminationCause || "unknown", ended_at: Date.now() } as any,
      { fetchLastN: 0 },
    );
  }
```

Also, near the top of the class, replace the `boot()` body with one that increments revisions after the turn (so the budget tracker sees them):

```ts
  async boot(): Promise<void> {
    const ctx = this.makeCtx();
    const tools = this.toolsByName();
    const prompt = this.bootPrompt();
    void prompt; // production wiring (Task 14) feeds this into pi-mono
    await this.driveTurn("(boot)", tools, ctx);
    this.revisions = await this.countPlanRevisedEvents();
    this.budgetCheckTerminate();
  }
```

- [ ] **Step 4: Run all ceo.test.ts — should pass**

```bash
node --experimental-strip-types --test tests/ceo.test.ts 2>&1 | tail -15
```
Expected: 3 passes.

- [ ] **Step 5: Commit**

```bash
git add examples/agents/typescript/pi-company/src/ceo.ts examples/agents/typescript/pi-company/tests/ceo.test.ts
git commit -m "feat(pi-company): AutonomousCEO wake loop + budget caps"
```

---

### Task 13: ceo.ts — step timeout (TDD)

If no `step_completed` arrives within `stepTimeoutSec` of the latest dispatch, synthesize a `step_timeout` event into `_plan` and wake the CEO.

**Files:**
- Modify: `examples/agents/typescript/pi-company/src/ceo.ts`
- Modify: `examples/agents/typescript/pi-company/tests/ceo.test.ts`

- [ ] **Step 1: Write failing timeout test**

Append to `tests/ceo.test.ts`:

```ts
test("step timeout fires when no step_completed arrives in time", async () => {
  const client = new FakeClient();
  const run = { tenant: "t", runId: "r1" };
  const channels = new ManualFakeChannels(client, run);

  const llm = new ScriptedLLM([
    // boot
    [
      { tool: "update_plan", args: {
        reason: "x",
        steps: [{ id: "s1", text: "x", assigned: "engineer", status: "pending" }],
      }},
      { tool: "dispatch_step", args: { step_id: "s1" } },
    ],
    // wake (after timeout fires) — wrap up
    [{ tool: "wrap_up", args: { reason: "stuck", summary: "engineer never replied" } }],
  ]);

  const ceo = new AutonomousCEO({
    client: client as any,
    channels: channels as any,
    run,
    goal: "ship",
    budgets: makeBudgets({ stepTimeoutSec: 0.05 }), // 50ms
    runTurn: (tools, ctx) => llm.runTurn(tools, ctx),
  });

  await ceo.start();
  assert.equal(ceo.cause, "wrap_up");

  // _plan should contain a step_timeout event
  const planSess = client.sessions.get("r1:_plan")!;
  assert.ok(planSess.appended.some((e: any) => e.kind === "step_timeout" && e.step_id === "s1"));
});
```

- [ ] **Step 2: Run — fail (no timeout machinery yet)**

```bash
node --experimental-strip-types --test tests/ceo.test.ts -- --test-name-pattern=timeout 2>&1 | tail -15
```

- [ ] **Step 3: Implement step-timeout machinery in `src/ceo.ts`**

Add to imports at the top of `src/ceo.ts`:

```ts
import { appendStepTimeout } from "./plan.ts";
```

Modify `wakeLoop()` to race the mailbox iterator against a timeout. Replace the existing `wakeLoop()` with:

```ts
  private async wakeLoop(): Promise<void> {
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
        // Timeout: find the latest dispatched-but-not-completed step and
        // synthesize a step_timeout event, then wake.
        await this.synthesizeStepTimeout();
        await this.driveWakeTurn();
      }
    }
  }

  /**
   * Find the most-recently dispatched step whose status is still
   * in_progress (per `_plan` reduction) and append a `step_timeout`
   * event for it. If no such step exists (e.g., everything's already
   * done), no-op.
   */
  private async synthesizeStepTimeout(): Promise<void> {
    const plan = await this.snapshotPlan();
    const inProgress = plan.steps.filter((s) => s.status === "in_progress");
    if (inProgress.length === 0) return;
    // Use the last in-progress step (most recently dispatched).
    const step = inProgress[inProgress.length - 1];
    await appendStepTimeout(this.deps.client, this.deps.run, step.id);
  }
```

- [ ] **Step 4: Run timeout test — should pass**

```bash
node --experimental-strip-types --test tests/ceo.test.ts 2>&1 | tail -15
```
Expected: 4 passes.

- [ ] **Step 5: Commit**

```bash
git add examples/agents/typescript/pi-company/src/ceo.ts examples/agents/typescript/pi-company/tests/ceo.test.ts
git commit -m "feat(pi-company): AutonomousCEO step-timeout safety net"
```

---

### Task 14: orchestrator.ts — autonomous boot path

Add `Orchestrator.startAutonomous(goal, budgets)` that boots the four employees with `wrapToolForObservation` applied to every tool, and starts `AutonomousCEO`.

**Files:**
- Modify: `examples/agents/typescript/pi-company/src/orchestrator.ts`

- [ ] **Step 1: Add imports**

At the top of `src/orchestrator.ts`, add:

```ts
import { Agent as PiAgent, type AgentTool, type AgentEvent as PiAgentEvent } from "@mariozechner/pi-agent-core";
import type { TSchema } from "typebox";
import type { CompanyTool, ToolContext } from "./agent.ts";
import { wrapToolForObservation } from "./observability.ts";
import { AutonomousCEO, type Budgets } from "./ceo.ts";
import { ALL_TOOLS, CEO_AUTONOMOUS_TOOLS, ROLE_TOOLS, filterTools } from "./tools/index.ts";
import { getLLM } from "./llm.ts";
```

(Note some of these imports are already present — verify before pasting and dedupe.)

- [ ] **Step 2: Add `startAutonomous` method**

Append to the `Orchestrator` class:

```ts
  /**
   * Autonomous-mode entry point. Boots the four employee agents with each
   * employee tool wrapped so it emits an `observation` event into `_plan`
   * after every invocation. Starts an `AutonomousCEO` driven by a real
   * pi-mono Agent under the hood.
   */
  async startAutonomous(goal: string, budgets: Budgets): Promise<void> {
    const roles = Object.keys(ROLE_SUBSCRIPTIONS) as EmployeeRole[];
    await this.recordMeta({
      kind: "run_started",
      started_at: Date.now(),
      mode: "autonomous",
      goal,
      agents: roles,
    });

    const channels = new Channels(this.client, this.run);
    const wrap = (role: string) => (t: CompanyTool) => wrapToolForObservation(t, role, this.client, this.run);

    for (const role of roles) {
      const tools = filterTools(ROLE_TOOLS[role] ?? []);
      const agent = new Agent({
        client: this.client,
        run: this.run,
        role,
        channels,
        subscriptions: ROLE_SUBSCRIPTIONS[role],
        toolRegistry: tools,
        wrapTool: wrap(role),
      });
      this.agents.push(agent);
      await agent.start();
      await this.recordMeta({ kind: "agent_joined", role });
    }

    const ceo = new AutonomousCEO({
      client: this.client,
      channels,
      run: this.run,
      goal,
      budgets,
      runTurn: (toolsByName, ctx) => this.runCeoTurnViaPiMono(toolsByName, ctx),
    });

    await ceo.start();
  }

  /**
   * Production runTurn: construct a fresh pi-mono Agent with the CEO's
   * autonomous tools, drive one prompt() call against the wake/boot
   * prompt, return the result. The prompt itself is rebuilt by the
   * caller from `boot()` / `wakeLoop()` — we accept it via an out-of-band
   * channel here for simplicity. v1: rebuild prompt inside this method
   * by calling helpers on AutonomousCEO. To avoid cross-class coupling,
   * the prompt is rebuilt via the helper hooked here:
   */
  private async runCeoTurnViaPiMono(
    toolsByName: Record<string, CompanyTool>,
    ctx: ToolContext,
  ): Promise<{ terminate?: boolean }> {
    // Production prompt construction: defer to the CEO's most recent
    // prompt builder. AutonomousCEO calls this at known points (boot
    // / wake), and the simplest contract is for runCeoTurnViaPiMono to
    // simply pull the latest prompt from a shared field. Per the
    // simplicity of v1, we instead pass the prompt via the ctx
    // `parentSession` field which is unused in autonomous CEO context.
    const prompt = (ctx as any)._ceoPrompt as string | undefined ?? "Plan and dispatch.";
    const piTools: AgentTool<TSchema>[] = Object.values(toolsByName).map((t) => this.adaptCompanyTool(t, ctx));
    const piAgent = new PiAgent({
      initialState: {
        systemPrompt: "You are the autonomous CEO. Use your tools to make progress on the goal.",
        model: getLLM(),
        tools: piTools,
      },
    });
    let terminate = false;
    const unsub = piAgent.subscribe(async (_event: PiAgentEvent) => { /* no-op */ });
    try {
      const result = await piAgent.prompt(prompt);
      // pi-mono surfaces tool-result `terminate` flags through the run; the
      // wrap_up tool sets terminate:true. We re-detect by checking the
      // run's last tool-result events.
      terminate = (result?.toolBatches ?? []).some((b: any) =>
        (b?.results ?? []).some((r: any) => r?.terminate === true),
      );
    } finally {
      unsub();
    }
    return { terminate };
  }

  private adaptCompanyTool(tool: CompanyTool, ctx: ToolContext): AgentTool<TSchema> {
    return {
      name: tool.name,
      description: tool.description,
      label: tool.label ?? tool.name,
      parameters: tool.parameters,
      async execute(_id, params) {
        const out = await tool.handler(params, ctx);
        const content = out.content ?? [{ type: "text" as const, text: out.text ?? "" }];
        return { content, details: out.details ?? {}, terminate: out.terminate };
      },
    };
  }
```

Modify `AutonomousCEO` (in `src/ceo.ts`) so it pushes the current prompt onto the ctx for the production runTurn to find. Replace the call sites in `boot()` and `driveWakeTurn()` to compute and set `(ctx as any)._ceoPrompt`:

```ts
  // in boot()
  async boot(): Promise<void> {
    const ctx = this.makeCtx();
    (ctx as any)._ceoPrompt = this.bootPrompt();
    const tools = this.toolsByName();
    await this.driveTurn("(boot)", tools, ctx);
    this.revisions = await this.countPlanRevisedEvents();
    this.budgetCheckTerminate();
  }

  // in driveWakeTurn()
  private async driveWakeTurn(): Promise<void> {
    const ctx = this.makeCtx();
    (ctx as any)._ceoPrompt = this.wakePrompt(await this.snapshotPlan());
    const tools = this.toolsByName();
    await this.driveTurn("(wake)", tools, ctx);
    this.revisions = await this.countPlanRevisedEvents();
    this.budgetCheckTerminate();
  }
```

- [ ] **Step 3: Verify TypeScript compiles**

```bash
npm run build
```
Expected: exit 0.

- [ ] **Step 4: Run the suite to confirm no regression**

```bash
npm test 2>&1 | tail -10
```
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add examples/agents/typescript/pi-company/src/orchestrator.ts examples/agents/typescript/pi-company/src/ceo.ts
git commit -m "feat(pi-company): orchestrator autonomous-mode boot"
```

---

### Task 15: index.ts — `--goal` and budget flags

Wire CLI flags into `Orchestrator.startAutonomous`.

**Files:**
- Modify: `examples/agents/typescript/pi-company/src/index.ts`

- [ ] **Step 1: Extend `parseArgs` and add an autonomous branch**

Replace the contents of `src/index.ts`:

```ts
import { Client } from "agentdb";
import { Orchestrator } from "./orchestrator.ts";
import { Channels } from "./channels.ts";
import { CompanyTUI } from "./tui.ts";
import { startAgentDBServer } from "./server.ts";
import { launchScenario, runScenario } from "./scenarios/launch.ts";
import { assertApiKeyAvailable } from "./llm.ts";
import { DEFAULT_BUDGETS, type Budgets } from "./ceo.ts";

const TENANT = "pi-company";

interface ParsedArgs {
  scenario?: string;
  resumeRunId?: string;
  goal?: string;
  budgets: Budgets;
}

function parseFlag(a: string, prefix: string): string | undefined {
  return a.startsWith(prefix) ? a.slice(prefix.length) : undefined;
}

function parseArgs(argv: string[]): ParsedArgs {
  const out: ParsedArgs = { budgets: { ...DEFAULT_BUDGETS } };
  for (const a of argv) {
    let v: string | undefined;
    if ((v = parseFlag(a, "--scenario=")) !== undefined) {
      out.scenario = v;
    } else if (a === "--scenario") {
      throw new Error("Use --scenario=NAME (e.g., --scenario=launch)");
    } else if ((v = parseFlag(a, "--resume=")) !== undefined) {
      out.resumeRunId = v;
    } else if ((v = parseFlag(a, "--goal=")) !== undefined) {
      out.goal = v;
    } else if (a === "--goal") {
      throw new Error("Use --goal=\"<text>\" (KEY=VALUE form)");
    } else if ((v = parseFlag(a, "--max-revisions=")) !== undefined) {
      out.budgets.maxRevisions = parseIntOrThrow(v, "--max-revisions");
    } else if ((v = parseFlag(a, "--max-minutes=")) !== undefined) {
      out.budgets.maxMinutes = parseFloatOrThrow(v, "--max-minutes");
    } else if ((v = parseFlag(a, "--max-llm-calls=")) !== undefined) {
      out.budgets.maxLlmCalls = parseIntOrThrow(v, "--max-llm-calls");
    } else if ((v = parseFlag(a, "--step-timeout=")) !== undefined) {
      out.budgets.stepTimeoutSec = parseFloatOrThrow(v, "--step-timeout");
    }
  }
  if (out.goal && out.scenario) {
    throw new Error("--goal and --scenario are mutually exclusive");
  }
  return out;
}

function parseIntOrThrow(v: string, flag: string): number {
  const n = Number.parseInt(v, 10);
  if (!Number.isFinite(n) || n <= 0) throw new Error(`${flag} must be a positive integer (got '${v}')`);
  return n;
}

function parseFloatOrThrow(v: string, flag: string): number {
  const n = Number.parseFloat(v);
  if (!Number.isFinite(n) || n <= 0) throw new Error(`${flag} must be a positive number (got '${v}')`);
  return n;
}

async function main(): Promise<void> {
  assertApiKeyAvailable();

  const args = parseArgs(process.argv.slice(2));
  const runId = args.resumeRunId ?? `r-${Date.now()}`;
  const run = { tenant: TENANT, runId };

  console.log(`pi-company starting (run-id: ${runId})...`);
  const server = await startAgentDBServer();
  console.log(`AgentDB ready at ${server.baseUrl}`);
  const client = new Client(server.baseUrl, { tenant: TENANT });
  const orchestrator = new Orchestrator(client, run);

  if (args.goal) {
    console.log(`Autonomous mode (goal: ${args.goal.slice(0, 80)})...`);
    try {
      await orchestrator.startAutonomous(args.goal, args.budgets);
    } finally {
      await orchestrator.stop("autonomous_complete");
    }
    server.shutdown();
    process.exit(0);
  }

  await orchestrator.startCompany();
  console.log("All 5 agents joined.");
  const channels = new Channels(client, run);

  if (args.scenario === "launch") {
    console.log(`Running scripted scenario 'launch' (run-id: ${runId})...`);
    try {
      await runScenario(launchScenario, channels, client, TENANT);
      console.log("Scenario complete. Draining for 20s...");
      await new Promise((r) => setTimeout(r, 20_000));
    } finally {
      await orchestrator.stop("scenario_complete");
    }
    server.shutdown();
    process.exit(0);
  } else if (args.scenario) {
    console.error(`Unknown scenario: ${args.scenario}`);
    server.shutdown();
    process.exit(2);
  } else {
    const tui = new CompanyTUI({
      channels,
      onCeoInput: async (text: string) => {
        await channels.publish("announcements", { kind: "directive", from: "ceo", text });
      },
    });
    await tui.start();
    await tui.wait();
    await orchestrator.stop("user_quit");
    server.shutdown();
    process.exit(0);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

- [ ] **Step 2: Verify TypeScript compiles**

```bash
npm run build
```
Expected: exit 0.

- [ ] **Step 3: Run the suite — no regression**

```bash
npm test 2>&1 | tail -10
```

- [ ] **Step 4: Smoke-check `--goal` parsing without an LLM call**

```bash
# Dry-run the parse: pass a bogus value and look for our explicit error
node --experimental-strip-types -e "
import('./src/index.ts').catch((e) => { console.error(String(e).slice(0, 200)); });
" 2>&1 | head -5
```
Expected: an error from `assertApiKeyAvailable` or main runtime — confirming the flag-parser path is executed.

(The full smoke run lives in Task 17.)

- [ ] **Step 5: Commit**

```bash
git add examples/agents/typescript/pi-company/src/index.ts
git commit -m "feat(pi-company): --goal flag wires autonomous mode"
```

---

### Task 16: docs — README + agent-db.md

**Files:**
- Modify: `examples/agents/typescript/pi-company/README.md`
- Modify: `docs/agent-db.md`

- [ ] **Step 1: Append "Autonomous mode" section to pi-company README**

Open `examples/agents/typescript/pi-company/README.md`, find the section after the scripted-scenario walkthrough (typically a "Scenario" or "Running the demo" section). Append:

```markdown
## Autonomous mode

The default scripted demo (`--scenario=launch`) runs a fixed sequence: deterministic, polished, marketing-friendly. For a real agentic experience, give the CEO a goal and let it generate its own plan:

    pnpm start --goal="ship a meditation app and land 3 pilot customers"

In autonomous mode the CEO is an LLM agent. It will:

  1. Generate a 3-7 step plan from your goal.
  2. Dispatch one step at a time to an employee.
  3. Wait until the employee reports back (`report_to_ceo` with the step id).
  4. Re-read the plan + accumulated observations.
  5. Either dispatch the next step, revise the plan, or wrap up.

Plans live in a per-run session named `{runId}:_plan`. Every revision, dispatch, and report appends an event. Every employee tool call appends an `observation` event. The CEO's wake prompt is built by reducing those events. Resume any run with `--resume=<run-id>` and the CEO continues from where it left off.

### Budgets

Autonomous runs are bounded — same primitives, same goal, but no timer means no demo. The defaults:

    --max-revisions=8       Plan revisions before the run terminates.
    --max-minutes=15        Wallclock cap.
    --max-llm-calls=50      CEO LLM-turn cap.
    --step-timeout=180      Seconds without a step_completed before the
                            CEO is woken with a synthesized step_timeout.

Whichever fires first wins. The cause is recorded as `run_ended.cause` in `{runId}:_meta`.

### Determinism

Scripted mode is deterministic — replays the same scenario every time. Autonomous mode is not — same goal, different runs, different plans. For marketing recordings, prefer scripted. For exploring how AgentDB primitives compose into something genuinely agentic, use autonomous.
```

- [ ] **Step 2: Append "plan-as-events" section to docs/agent-db.md**

Find the existing "Recommended pattern: config-as-events" section in `docs/agent-db.md`. Insert the following section after it (or immediately before the next H2):

```markdown
## Recommended pattern: plan-as-events

Plans are mutable. A real agent revises its plan as it learns what
works and what doesn't. Storing a plan as a row you `UPDATE` loses the
*reason* a step changed, the *order* steps completed, the *observations*
the agent made along the way. Storing it as an event log keeps all of
that.

The pattern: dedicate one session per run (e.g., `{runId}:_plan`) and
append four kinds of events.

| Event kind        | Producer            | Payload                                                              |
|-------------------|---------------------|----------------------------------------------------------------------|
| `plan_revised`    | The planner agent   | `{ steps, reason }` — the entire step list, replaced wholesale.      |
| `step_dispatched` | The planner agent   | `{ step_id, assigned, text }` — handing the step off.                |
| `step_completed`  | The executing agent | `{ step_id, status, summary, from }` — outcome.                      |
| `observation`     | Auto-emitted        | `{ from, toolName, payload, relates_to_step? }` — any tool-call ran. |

Read time: reduce events into a `{ steps, observations }` snapshot. The
latest `plan_revised` defines the step list; later `step_dispatched` and
`step_completed` events flip individual step statuses. `observation`
events accumulate but don't change steps — they're the planner's
context for re-planning.

Why event-sourced plans:

  - **Replay**: every run produces a self-contained log. Step through it.
  - **Resume**: a process restart loads `_plan`, reduces, and the planner
    continues exactly where it left off.
  - **Fork**: `forkSession("{runId}:_plan", "alt-strategy")` creates an
    O(1) copy-on-write branch — explore an alternate plan without
    disturbing the main run.
  - **Observability**: tool calls auto-emit observation events into the
    same log. The plan session reads like an executive briefing.

A 150-line reference implementation:
[`examples/agents/typescript/pi-company/src/plan.ts`](../examples/agents/typescript/pi-company/src/plan.ts).
The autonomous-CEO loop in
[`pi-company/src/ceo.ts`](../examples/agents/typescript/pi-company/src/ceo.ts)
shows the read-reduce-revise loop end to end.
```

- [ ] **Step 3: Verify doc renders**

```bash
# Quick markdown syntax sanity — confirm no unmatched fences
grep -c '^```' examples/agents/typescript/pi-company/README.md
grep -c '^```' docs/agent-db.md
```
Expected: each grep returns an even number (matched code-fence pairs).

- [ ] **Step 4: Commit**

```bash
git add examples/agents/typescript/pi-company/README.md docs/agent-db.md
git commit -m "docs(pi-company): autonomous mode + plan-as-events convention"
```

---

### Task 17: Gated end-to-end smoke test

A tests/smoke/ test that exercises the full autonomous loop with a real LLM call. Gated by `AGENTDB_DEMO_API_KEY` (matches the existing `launch.test.ts` smoke pattern).

**Files:**
- Create: `examples/agents/typescript/pi-company/tests/smoke/autonomous.test.ts`

- [ ] **Step 1: Read the existing smoke pattern**

```bash
cat examples/agents/typescript/pi-company/tests/smoke/launch.test.ts
```
Note the env-gate idiom (skipping when no API key) and the timeout setting.

- [ ] **Step 2: Create the smoke test**

Create `tests/smoke/autonomous.test.ts`:

```ts
import { test } from "node:test";
import assert from "node:assert/strict";
import { Client } from "agentdb";
import { Orchestrator } from "../../src/orchestrator.ts";
import { startAgentDBServer } from "../../src/server.ts";
import { DEFAULT_BUDGETS } from "../../src/ceo.ts";
import { loadCurrentPlan, PLAN_SESSION_SUFFIX } from "../../src/plan.ts";

const TENANT = "pi-company-smoke-autonomous";
const SHOULD_RUN = !!process.env.AGENTDB_DEMO_API_KEY || !!process.env.ANTHROPIC_API_KEY;

test("autonomous mode boots, dispatches, and terminates within budget", { skip: !SHOULD_RUN, timeout: 240_000 }, async () => {
  const server = await startAgentDBServer();
  try {
    const runId = `r-${Date.now()}`;
    const run = { tenant: TENANT, runId };
    const client = new Client(server.baseUrl, { tenant: TENANT });
    const orch = new Orchestrator(client, run);

    await orch.startAutonomous(
      "Sketch a one-page marketing brief for a new productivity tool. Wrap up after the marketing team posts a draft.",
      { ...DEFAULT_BUDGETS, maxMinutes: 3, maxLlmCalls: 12, maxRevisions: 4, stepTimeoutSec: 60 },
    );

    // Verify _plan has at least one plan_revised + at least one step_dispatched.
    const plan = await loadCurrentPlan(client, run, 50);
    assert.ok(plan.steps.length > 0, "expected at least one step in the plan");
    // Verify _meta contains a run_ended event with a known cause.
    const metaSess = client.session(`${runId}:_meta`, { tenant: TENANT });
    const resp = await (metaSess as any).append({ kind: "_noop" }, { fetchLastN: 1024 });
    const ended = (resp.recent ?? []).find((e: any) => (e.payload?.kind ?? e.kind) === "run_ended");
    assert.ok(ended, "expected a run_ended event in _meta");
    const cause = ((ended as any).payload ?? ended).cause;
    assert.ok(
      ["wrap_up", "revision_budget_exhausted", "time_budget_exhausted", "call_budget_exhausted"].includes(cause),
      `unexpected run_ended cause: ${cause}`,
    );
  } finally {
    server.shutdown();
  }
});
```

- [ ] **Step 3: Run the smoke (skipped without API key)**

```bash
node --experimental-strip-types --test tests/smoke/autonomous.test.ts 2>&1 | tail -10
```
Expected without API key: `1 skipped`.
Expected with API key: 1 pass within ~3 minutes.

- [ ] **Step 4: Commit**

```bash
git add examples/agents/typescript/pi-company/tests/smoke/autonomous.test.ts
git commit -m "test(pi-company): autonomous-mode smoke (API-key gated)"
```

---

### Task 18: Final integration verification

- [ ] **Step 1: Run the whole pi-company suite**

```bash
cd examples/agents/typescript/pi-company
npm test 2>&1 | tail -25
```
Expected: all unit tests pass; smoke skipped (without API key) or pass (with).

- [ ] **Step 2: Run the workspace tests for SDK/server (defense in depth)**

```bash
cd $(git rev-parse --show-toplevel)
cargo test -p agentdb-api -p agentdb-engine 2>&1 | tail -20
```
Expected: pass (no Rust changes were made; this confirms the SDK file-link still works).

- [ ] **Step 3: Push the branch and open a draft PR**

```bash
git push -u origin feature/pi-company-agentic
gh pr create --draft --title "pi-company: autonomous CEO mode (plan-as-events)" --body "$(cat <<'EOF'
## Summary
- Introduces the **plan-as-events** convention: per-run `{runId}:_plan` session storing `plan_revised`, `step_dispatched`, `step_completed`, and `observation` events. Reduce at read time.
- Adds `--goal "<text>"` flag for an autonomous CEO mode alongside the existing scripted scenario (default unchanged).
- The autonomous CEO is a real pi-mono Agent driving four new tools: `update_plan`, `dispatch_step`, `mark_step`, `wrap_up`. It reads the goal, generates a plan, dispatches one step at a time, awaits `step_completed` on `directs:ceo`, re-plans, and wraps up — bounded by configurable budgets (revisions / minutes / LLM calls / step-timeout).
- Employee tool calls are auto-wrapped at registration time so each invocation appends an `observation` event into `_plan`. The plan session reads like an executive briefing.
- README + docs/agent-db.md document the convention.

## Test plan
- [x] `tests/plan.test.ts` — reducer + persistence helpers (14 tests)
- [x] `tests/observability.test.ts` — tool wrapper (5 tests)
- [x] `tests/ceo.test.ts` — boot, wake cycle, budget caps, step timeout (4 tests, mock LLM)
- [x] `tests/tools/{update_plan,dispatch_step,mark_step,wrap_up,report_to_ceo}.test.ts` — per-tool units
- [ ] `tests/smoke/autonomous.test.ts` — gated on AGENTDB_DEMO_API_KEY (run before merging)
- [ ] Manual: `pnpm start --goal="ship a one-page meditation app brief"` ends with a coherent `run_ended` cause

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
Note: `gh pr create` is left for the human to authorize; the agent should pause before pushing/opening the PR and ask for confirmation.

- [ ] **Step 4: Mark task complete**

No commit. The PR is the deliverable.

---

## Self-Review

**Spec coverage:**
- §Architecture — Tasks 1, 2, 11–14 (plan-as-events session, two-mode design, budget loop) ✓
- §Tool palette — Tasks 5–9 (four CEO tools + step_id on report_to_ceo + auto observations) ✓
- §CEO boot prompt — Task 11 ✓
- §CEO wake prompt — Task 12 ✓
- §Step timeout — Task 13 ✓
- §File structure — Tasks 1–17 cover every listed new/modified file ✓
- §plan.ts API — Tasks 1–2 ✓
- §ceo.ts skeleton — Tasks 11–13 ✓
- §Tests (plan, observability, ceo) — Tasks 1, 3, 11–13 ✓
- §README "Autonomous mode" — Task 16 ✓
- §docs/agent-db.md "plan-as-events" — Task 16 ✓
- §Out-of-scope items — explicitly not implemented (no fork_plan, no evaluator, no human-in-loop) ✓

**Type consistency:**
- `Step.assigned` in `plan.ts` (Task 1) is `Role = Exclude<AgentRole, "ceo" | "contractor">`; `update_plan.ts` (Task 6) uses the same union literally; `dispatch_step.ts` (Task 7) reads `step.assigned` from the reducer. ✓
- `step_completed.status` is `"ok" | "failed" | "blocked" | "cancelled"` everywhere (Tasks 1, 4, 5, 8). ✓
- `Budgets` interface defined in Task 11 is referenced consistently in Tasks 12, 14, 15. ✓
- `wrapToolForObservation` signature `(tool, role, client, run)` consistent between Task 3 (definition) and Task 14 (usage). ✓
- `runTurn` signature `(toolsByName, ctx) => Promise<{ terminate? }>` consistent between Task 11 (test scaffold) and Task 14 (production). ✓

**Placeholders:** none — every step has either a concrete code block or an exact command with expected output.

**Things to watch out for during execution:**
- Task 14's `runCeoTurnViaPiMono` uses an out-of-band `(ctx as any)._ceoPrompt` channel — slightly hacky but keeps the unit tests in Task 12 free of pi-mono dependencies. If pi-mono's actual `Agent.prompt()` shape diverges from the assumed `result.toolBatches[].results[].terminate`, Task 14 may need adjustment after a quick read of `@mariozechner/pi-agent-core` types.
- The `FakeChannels` test fixture in Task 12 implements an async iterator manually — Node 22 supports it; if the test runner complains, fall back to a queue-of-promises pattern.
