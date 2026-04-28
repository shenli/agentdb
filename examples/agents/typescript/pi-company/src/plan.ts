import type { AgentEvent, AgentRole } from "./types.ts";

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
