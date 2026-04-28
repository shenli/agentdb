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

export const PLAN_SESSION_SUFFIX = "_plan";

function planSessionName(run: RunIdentity): string {
  return `${run.runId}:${PLAN_SESSION_SUFFIX}`;
}

/**
 * Append a `plan_revised` event to `{runId}:_plan`. Replaces the prior
 * step list at read time (the reducer applies last-write-wins on the
 * latest `plan_revised`).
 */
export async function appendPlanRevised(
  client: Client,
  run: RunIdentity,
  steps: Step[],
  reason: string,
): Promise<void> {
  const sess = client.session(planSessionName(run));
  await sess.append(
    { kind: "plan_revised", reason, steps } as any,
    { fetchLastN: 0 },
  );
}

/**
 * Append a `step_dispatched` event to `{runId}:_plan`. Call this when a
 * step is handed off to a sub-agent; the reducer flips the step's status
 * to `in_progress`.
 */
export async function appendStepDispatched(
  client: Client,
  run: RunIdentity,
  step_id: string,
  assigned: Role,
  text: string,
): Promise<void> {
  const sess = client.session(planSessionName(run));
  await sess.append(
    { kind: "step_dispatched", step_id, assigned, text } as any,
    { fetchLastN: 0 },
  );
}

/**
 * Append a `step_completed` event to `{runId}:_plan`. Call this when a
 * sub-agent finishes a step; the reducer maps the outcome status to a
 * terminal `StepStatus` (`done`, `failed`, or `cancelled`).
 */
export async function appendStepCompleted(
  client: Client,
  run: RunIdentity,
  step_id: string,
  status: "ok" | "failed" | "blocked" | "cancelled",
  summary: string,
  from: AgentRole,
): Promise<void> {
  const sess = client.session(planSessionName(run));
  await sess.append(
    { kind: "step_completed", step_id, status, summary, from } as any,
    { fetchLastN: 0 },
  );
}

/**
 * Append an `observation` event to `{runId}:_plan`. Call this after any
 * tool call to record what a sub-agent produced; observations accumulate
 * and are surfaced to the CEO via `loadCurrentPlan`.
 */
export async function appendObservation(
  client: Client,
  run: RunIdentity,
  from: string,
  toolName: string,
  payload: unknown,
  relates_to_step?: string,
): Promise<void> {
  const sess = client.session(planSessionName(run));
  await sess.append(
    { kind: "observation", from, toolName, payload, relates_to_step } as any,
    { fetchLastN: 0 },
  );
}

/**
 * Append a `step_timeout` event to `{runId}:_plan`. Call this when the
 * CEO's timeout watchdog fires; the reducer marks the step `failed` if
 * it is still `in_progress`.
 */
export async function appendStepTimeout(
  client: Client,
  run: RunIdentity,
  step_id: string,
): Promise<void> {
  const sess = client.session(planSessionName(run));
  await sess.append(
    { kind: "step_timeout", step_id } as any,
    { fetchLastN: 0 },
  );
}

/**
 * Read all events on `{runId}:_plan` via the server's pure-read
 * `fetch_recent` endpoint, reduce, and truncate the observations to the last N.
 *
 * `observationWindow` defaults to 20.
 */
export async function loadCurrentPlan(
  client: Client,
  run: RunIdentity,
  observationWindow: number = 20,
): Promise<PlanState> {
  const sess = client.session(planSessionName(run));
  const resp = await sess.fetchRecent(1024);
  const recent = (resp.recent ?? []) as AgentEvent[];
  const state = reducePlanEvents(recent);
  if (state.observations.length > observationWindow) {
    return { ...state, observations: state.observations.slice(-observationWindow) };
  }
  return state;
}
