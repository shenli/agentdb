# Pi-Company Agentic Mode — Design Spec

**Status:** Draft
**Date:** 2026-04-27
**Scope:** `examples/agents/typescript/pi-company/` — add an autonomous CEO mode alongside the existing scripted scenario.

---

## Problem

The current pi-company sample app drives the demo via a hardcoded `scenarios/launch.ts` — a list of `setTimeout`-scheduled CEO messages and config updates. The CEO does not actually decide anything: every action is pre-scripted.

This makes the demo polished but undermines the AgentDB pitch. AgentDB positions itself as an *agent-native* operational database. A scripted CEO is exactly the wrong demonstration of "agent-native" — it could run on Postgres + a cron table.

We want a sample app that proves AgentDB's primitives (event-sourced sessions, fork, hybrid retrieval, pub/sub, hot config) compose into something genuinely agentic: a CEO that *generates its own plan, dispatches steps, observes outcomes, and revises the plan* — all backed by event logs the user can inspect, replay, and resume.

## Goals

1. **Add an autonomous CEO mode** that takes a free-text goal and runs without further human input until it wraps up or hits a budget cap.
2. **Establish the plan-as-events convention** — a parallel pattern to the existing config-as-events pattern. Plans are stored as event logs in a dedicated session, reduced at read time.
3. **Preserve the scripted demo** as the default — autonomous mode is opt-in via `--goal`.
4. **Document the convention** in `docs/agent-db.md` and showcase it in the pi-company README, including a real autonomous-run transcript.

## Non-goals

- Replacing pi-mono (`@mariozechner/pi-agent-core` stays).
- Adding new model providers (current `getLLM()` palette is sufficient).
- Changing config-as-events semantics.
- Reworking employee tool surface beyond a single `step_id` parameter on `report_to_ceo`.

## Architecture

### Two CEO modes, one set of primitives

| Mode | Invocation | CEO behavior |
|------|------------|--------------|
| Scripted (default, unchanged) | `pnpm start --scenario=launch` | `scenarios/launch.ts` drives via `setTimeout` |
| Autonomous (new) | `pnpm start --goal "<text>"` | LLM-driven CEO agent loop |

Both modes share the same employee agents, tools, channels, sessions, and config-as-events behavior. The only difference is who writes the plan.

### Plan-as-events convention

A new session per run, named `{runId}:_plan`, stores four event kinds:

| Kind | Producer | Payload |
|------|----------|---------|
| `plan_revised` | CEO (via `update_plan` tool) | `{ steps: Step[], reason: string }` |
| `step_dispatched` | CEO (via `dispatch_step` tool) | `{ step_id, assigned, text }` |
| `step_completed` | Employee (via `report_to_ceo` tool) or CEO (via `mark_step` tool) | `{ step_id, status: "ok"\|"failed"\|"blocked"\|"cancelled", summary, from }` |
| `observation` | Auto-emitted on any employee tool call | `{ from, kind, payload, relates_to_step? }` |

A `Step` has shape:
```ts
{
  id: string,
  text: string,
  assigned: "engineer" | "marketing" | "sales" | "cto",
  status: "pending" | "in_progress" | "done" | "failed"
}
```

`reducePlanEvents(events)` returns the current `Step[]` (latest `plan_revised` defines the list; subsequent `step_dispatched` and `step_completed` events update individual step statuses) plus the recent `observation` history.

### CEO loop (B from brainstorm)

```
boot:
  CEO LLM call → update_plan(initial_steps, reason) + dispatch_step(s1)
  CEO subscribes to directs:ceo channel
  CEO sleeps until step_completed arrives or step-timeout fires

wake cycle (loop):
  trigger: step_completed event lands in directs:ceo mailbox
  read _plan session, reduce to current plan + recent observations
  build wake prompt (goal, current plan, events since last wake, budgets)
  CEO LLM call → 1+ tool calls (update_plan, dispatch_step, mark_step, or wrap_up)
  CEO sleeps until next step_completed or timeout
  loop ends when wrap_up called OR budget exhausted

step-timeout safety net:
  if no step_completed within --step-timeout (default 180s) of the latest dispatch,
  orchestrator synthesizes a step_timeout observation event and wakes the CEO anyway
```

The CEO does NOT subscribe to general channels (`announcements`, `engineering`, etc). Its only wake signal is the `directs:ceo` mailbox. Employee chatter is reflected via `observation` events the CEO reads when it next wakes — not pushed to it. This keeps the CEO deliberative rather than reactive, and bounds LLM cost.

### Budgets and termination

| Budget | Default | Flag |
|--------|---------|------|
| Max plan revisions | 8 | `--max-revisions=N` |
| Max wallclock | 15 minutes | `--max-minutes=N` |
| Max LLM calls | 50 | `--max-llm-calls=N` |
| Per-step timeout | 180 seconds | `--step-timeout=Ns` |

Run terminates on:
- `wrap_up` tool call → `run_ended { cause: "wrap_up", summary }`
- Any budget hits zero → `run_ended { cause: "revision_budget_exhausted" | "time_budget_exhausted" | "call_budget_exhausted" }`

The cause is logged to `{runId}:_meta` and printed at exit.

## Tool palette

### New CEO-only tools

```ts
update_plan({
  steps: Step[],
  reason: string
})
// Appends plan_revised event to {runId}:_plan
// Publishes "plan_revised" to announcements channel for employee visibility
// (Replaces the full plan; partial-update semantics use mark_step.)

dispatch_step({
  step_id: string  // must exist in current reduced plan
})
// Appends step_dispatched event to {runId}:_plan
// Publishes structured payload {step_id, text} to directs:{assigned} mailbox channel
// (Employee's mailbox handler picks this up and includes step_id in the wake prompt.)

mark_step({
  step_id: string,
  status: "ok" | "failed" | "cancelled",
  note?: string
})
// Appends a step_completed event with from="ceo", summary=note
// (For when the CEO wants to override status without an employee report —
//  e.g., cancel a step whose assigned employee is busy elsewhere.
//  reducePlanEvents maps "ok"→done, "failed"→failed, "cancelled"→cancelled.)

wrap_up({
  reason: string,
  summary: string
})
// Appends run_ended {cause: "wrap_up", reason, summary} to {runId}:_meta
// Terminates the autonomous loop
```

### Modified employee tool

```ts
report_to_ceo({
  step_id?: string,    // NEW: optional in scripted mode, expected in autonomous mode
  status: "ok" | "failed" | "blocked",
  summary: string
})
// When step_id present:
//   - appends step_completed event to {runId}:_plan
//   - publishes "step_completed" payload to directs:ceo mailbox
// When step_id absent (scripted mode):
//   - appends free-form report event to {runId}:ceo session (current behavior)
```

### Auto-emitted observations

Wraps every employee tool that mutates state. Wrapping is applied uniformly at tool-registration time in autonomous mode (in scripted mode it's a no-op so scripted runs are unaffected).

```ts
function wrapToolForObservation<S, D>(
  tool: AgentTool<S, D>,
  role: Role,
  client: AgentDBClient,
  run: string
): AgentTool<S, D> {
  return {
    ...tool,
    execute: async (id, params, signal, onUpdate) => {
      const result = await tool.execute(id, params, signal, onUpdate);
      // Truncated payload — full args remain in employee's session
      const payload = summarizeArgs(tool.name, params);
      await appendObservation(client, run, role, tool.name, payload, params.step_id);
      return result;
    }
  };
}
```

Wrapped tools (autonomous mode only): `announce`, `direct_message`, `delegate_to_engineer`, `write_doc`, `submit_for_review`, `add_prospect`, `draft_outreach`, `draft_campaign`, `hire_contractor`. `report_to_ceo` is NOT wrapped (it already writes its own structured event).

### CEO's full autonomous-mode palette

| Tool | Purpose |
|------|---------|
| `update_plan` | Replace plan |
| `dispatch_step` | Hand a step to an employee |
| `mark_step` | Override a step's status |
| `wrap_up` | End the run |
| `announce` | Existing — broadcast to channels |
| `direct_message` | Existing — peer-to-peer message |
| `update_agent_config` | Existing — hot-tune an agent's config |

## Boot prompt (autonomous CEO)

```
You are the CEO of an AI-only company. Your goal:

  <user-supplied goal>

Your team:
  - cto: oversight, can review docs, sets engineering direction
  - engineer: writes code, designs, hires contractors via fork
  - marketing: drafts campaigns and outreach
  - sales: builds prospect lists, drafts customer messages

You have these tools:
  - update_plan(steps, reason): replace the current plan
  - dispatch_step(step_id): hand a step to an employee (they will execute and report back)
  - mark_step(step_id, status, note): override a step's status without an employee report
  - announce(channel, text): broadcast to a channel
  - direct_message(to, text): peer message
  - update_agent_config(role, patch): hot-tune an agent's behavior
  - wrap_up(reason, summary): end the run

Workflow:
  1. Call update_plan with an initial 3-7 step plan.
  2. Call dispatch_step on the first pending step.
  3. Wait. (You will be re-prompted when an employee reports back.)

Each step should have:
  - id: short slug like "s1", "s2"
  - text: one clear sentence
  - assigned: cto | engineer | marketing | sales
  - status: "pending"

Do NOT call wrap_up in your first turn. Plan first, then dispatch.
```

## Wake prompt (autonomous CEO)

```
Goal: <original goal>

Current plan:
  s1 [done] (engineer)    Scope the meditation app MVP
  s2 [pending] (engineer) Implement basic UI
  s3 [pending] (marketing) Draft launch campaign
  ...

Since your last decision:
  - engineer reported step_completed: s1, status=ok
    Summary: "Scoped to 3 screens: home, session timer, settings. ~400 LoC TS."
  - engineer observation: hired contractor "ui-designer" via hire_contractor
  - engineer observation: wrote doc "meditation-app-scope.md"

Budget remaining:
  - revisions: 6/8
  - time: 11m 23s / 15m
  - LLM calls: 42/50

Decide what to do next. Options:
  - dispatch_step(<id>): hand the next pending step to an employee
  - update_plan(steps, reason): revise the plan based on what you've learned
  - mark_step(id, status): override a step's status (e.g., cancel)
  - announce / direct_message / update_agent_config: communicate or tune
  - wrap_up(reason, summary): if the goal is met or further effort is futile

Pick the smallest next action. You will be re-prompted on the next step_completed.
```

## File structure

### New files

```
examples/agents/typescript/pi-company/src/
├── plan.ts                   # plan-as-events helpers (~150 LoC)
├── ceo.ts                    # AutonomousCEO class (~250 LoC)
├── observability.ts          # tool-call observation wrapper (~80 LoC)
└── tools/
    ├── update_plan.ts        # CEO tool
    ├── dispatch_step.ts      # CEO tool
    ├── mark_step.ts          # CEO tool
    └── wrap_up.ts            # CEO tool
```

### Modified files

| File | Change |
|------|--------|
| `src/index.ts` | Add `--goal`, `--max-revisions`, `--max-minutes`, `--max-llm-calls`, `--step-timeout` flags; branch to `AutonomousCEO` when `--goal` present |
| `src/orchestrator.ts` | Extract employee-boot logic for reuse; in autonomous mode, subscribe CEO agent to `directs:ceo`, register CEO tools, wrap employee tools for observation |
| `src/agent.ts` | Accept optional `wrapTool` hook so observability.ts can intercept tool execution at registration time |
| `src/tools/report_to_ceo.ts` | Accept optional `step_id`; when present, write `step_completed` event to `_plan` and publish to `directs:ceo` |
| `src/tools/index.ts` | Register new CEO tools (autonomous-mode-only) |
| `README.md` | Add "Autonomous mode" section with flags and sample transcript |
| `docs/agent-db.md` | Add "Recommended pattern: plan-as-events" section |

### Unchanged files

- `src/scenarios/launch.ts` — scripted demo intact
- `src/config.ts`, `src/memory.ts`, `src/channels.ts`, `src/llm.ts`
- All employee tools other than `report_to_ceo`

## plan.ts API

```ts
export type StepStatus = "pending" | "in_progress" | "done" | "failed" | "cancelled";

export interface Step {
  id: string;
  text: string;
  assigned: "cto" | "engineer" | "marketing" | "sales";
  status: StepStatus;
}

export interface PlanState {
  steps: Step[];
  recent_observations: ObservationEvent[];
  history: PlanEvent[];
}

export function reducePlanEvents(events: PlanEvent[]): PlanState;

export async function appendPlanRevised(
  client: AgentDBClient,
  run: string,
  steps: Step[],
  reason: string
): Promise<void>;

export async function appendStepDispatched(
  client: AgentDBClient,
  run: string,
  step_id: string,
  assigned: Step["assigned"],
  text: string
): Promise<void>;

export async function appendStepCompleted(
  client: AgentDBClient,
  run: string,
  step_id: string,
  status: "ok" | "failed" | "blocked" | "cancelled",
  summary: string,
  from: "ceo" | "engineer" | "marketing" | "sales" | "cto"
): Promise<void>;

export async function appendObservation(
  client: AgentDBClient,
  run: string,
  from: string,
  kind: string,
  payload: unknown,
  relates_to_step?: string
): Promise<void>;

export async function loadCurrentPlan(
  client: AgentDBClient,
  run: string,
  observation_window: number
): Promise<PlanState>;
```

Session naming: `{runId}:_plan`. Mirrors `_config:agents` naming for the config-as-events convention.

## ceo.ts skeleton

```ts
export class AutonomousCEO {
  constructor(
    private client: AgentDBClient,
    private channels: Channels,
    private run: string,
    private goal: string,
    private budgets: { maxRevisions: number; maxMinutes: number; maxLlmCalls: number; stepTimeoutSec: number }
  ) {}

  async start(): Promise<void> {
    // 1. subscribe to directs:ceo
    // 2. emit boot prompt → expect update_plan + dispatch_step
    // 3. enter wake loop
  }

  private async wakeLoop(): Promise<void> {
    while (!this.terminated && !this.budgetExhausted()) {
      const event = await Promise.race([
        this.nextStepCompleted(),
        this.stepTimeout(),
      ]);
      await this.handleWake(event);
    }
    await this.recordRunEnded();
  }

  private async handleWake(event: WakeEvent): Promise<void> {
    const plan = await loadCurrentPlan(this.client, this.run, /*observation_window=*/20);
    const prompt = buildWakePrompt(this.goal, plan, event, this.budgetsRemaining());
    await this.runLlmTurn(prompt);  // increments llmCalls; tools execute side-effects
  }
}
```

## Tests

### `plan.test.ts`

- `reducePlanEvents` returns empty plan for empty events
- `plan_revised` defines steps; subsequent `plan_revised` replaces them
- `step_dispatched` flips the step's status to `in_progress`
- `step_completed` flips the step's status to `done`/`failed`
- `step_completed` for an unknown `step_id` is ignored gracefully (no throw)
- `observation` events accumulate but do not affect step list
- `loadCurrentPlan` truncates to last N observations

### `observability.test.ts`

- `wrapToolForObservation` calls the underlying tool's `execute` and returns its result unchanged
- After execution, an `observation` event is appended to `_plan` with correct `from`, `kind`, summarized payload
- `relates_to_step` is set when the params object includes `step_id`
- Wrapping a tool that throws still propagates the throw; no observation written

### `ceo.test.ts`

- Boot: with mock LLM that emits `[update_plan, dispatch_step]`, after `start()` returns from boot, `_plan` contains one `plan_revised` and one `step_dispatched`
- Wake on `step_completed`: mock LLM that emits `[dispatch_step]`, after a `step_completed` event arrives, the CEO publishes a new `step_dispatched`
- Replan: mock LLM that emits `[update_plan, dispatch_step]`, the CEO writes a new `plan_revised`
- Wrap-up: mock LLM that emits `[wrap_up]`, the loop terminates and `run_ended` is written with `cause: "wrap_up"`
- Budget cap (revisions): mock LLM that always emits `update_plan`, after 8 revisions the loop terminates with `cause: "revision_budget_exhausted"`
- Step timeout: no `step_completed` arrives within `step-timeout`, CEO is woken with a synthesized `step_timeout` observation

## Pitch & docs

### `pi-company/README.md` — new "Autonomous mode" section

```markdown
## Autonomous mode

The default scripted demo runs a fixed scenario. For a real agentic experience,
provide a goal and let the CEO generate its own plan:

    pnpm start --goal "ship a meditation app and land 3 pilot customers"

The CEO will:
  1. Generate a 3-7 step plan from your goal.
  2. Dispatch one step to an employee at a time.
  3. Wait for the employee to report back.
  4. Re-read the plan + accumulated observations.
  5. Either dispatch the next step, revise the plan, or wrap up.

Sample transcript: see [docs/autonomous-transcript.md](./docs/autonomous-transcript.md).

Budgets bound the run:

    --max-revisions=8       max plan revisions
    --max-minutes=15        max wallclock
    --max-llm-calls=50      max CEO LLM calls
    --step-timeout=180      seconds to wait for a step before giving up

Autonomous mode is non-deterministic — the same goal won't produce the same
plan twice. Scripted mode is deterministic and recommended for marketing
recordings.
```

### `docs/agent-db.md` — new "plan-as-events" section

Parallel structure to existing config-as-events. Headline: *plans are mutable; storing them as event logs preserves the reason for each revision and lets you replay, resume, or fork any run.*

Body covers:
- Why plans aren't rows
- The four event kinds (`plan_revised`, `step_dispatched`, `step_completed`, `observation`)
- Reduce-over-events pattern
- Benefits: replay, resume, fork, observability
- Pointer to `pi-company/src/plan.ts` for a 150-LoC implementation

### Landing-page headline

> *"AgentDB stores agents' plans the same way it stores their conversations — as event logs. The CEO agent revises its plan in response to what employees actually deliver. Resume any run, replay any decision, fork any plan."*

## Out of scope, noted as future

| Future feature | Why deferred |
|----------------|--------------|
| `fork_plan` tool (speculative branching, A/B plan comparison) | Strongest AgentDB pitch but needs concurrent-plan runtime, comparison UI, merge semantics. Current `_plan` event log is *designed* to make this feasible — single session, all events scoped, fork primitive already exists. Natural follow-up PR. |
| Goal-evaluator agent | Considered (Option C in termination). `wrap_up` heuristic is simpler and good enough as a starting point. Add later if it proves unreliable. |
| Human-in-the-loop intervention (stdin commands during a run) | Considered. Adds UI surface area. Add later if users ask for it. |
| Multi-goal runs / goal queues | Out of scope. One goal per run keeps the demo legible. |
| Cross-run learning (retrieval over historical `_plan` sessions) | Hybrid retrieval (BM25 + HNSW) over past plans would enable this — natural future showcase for retrieval primitives. |

## Risks

1. **LLM-driven plans drift from goal.** Mitigation: budgets, prompt explicitly references the goal at each wake, and plan re-deliberation is bounded.
2. **Employees forget to call `report_to_ceo`.** Mitigation: per-step timeout synthesizes a `step_timeout` observation that wakes the CEO anyway, so the loop never deadlocks.
3. **Demo non-determinism breaks marketing recordings.** Mitigation: scripted mode is unchanged and remains the default; autonomous mode is opt-in and explicitly documented as non-deterministic.
4. **Observation log inflation.** Mitigation: observations summarize args (truncated payloads); CEO's wake prompt includes only the last N observations (default 20).
5. **Token cost from frequent re-deliberation.** Mitigation: `--max-llm-calls` budget; CEO is woken only on `step_completed` (not on every channel event); employees still drive most of the work.

## Estimated scope

~600-900 LoC TypeScript:
- `plan.ts` (~150)
- `ceo.ts` (~250)
- `observability.ts` (~80)
- 4 new CEO tools (~150 total)
- `index.ts` flag wiring + `orchestrator.ts` autonomous-boot path (~80)
- `report_to_ceo.ts` step_id branch (~30)
- README + docs/agent-db.md text (~150)
- Tests (~200)

One PR. No subdivision needed at this scope.
