import { test } from "node:test";
import assert from "node:assert/strict";
import { Client } from "agentdb";
import { Orchestrator } from "../../src/orchestrator.ts";
import { startAgentDBServer } from "../../src/server.ts";
import { DEFAULT_BUDGETS } from "../../src/ceo.ts";
import { loadCurrentPlan } from "../../src/plan.ts";

const SMOKE = process.env.AGENTDB_PI_COMPANY_SMOKE === "1";

const skipReason = !SMOKE
  ? "set AGENTDB_PI_COMPANY_SMOKE=1 to run the autonomous-mode smoke test"
  : undefined;

test(
  "autonomous mode boots, dispatches, and terminates within budget",
  { skip: skipReason, timeout: 240_000 },
  async () => {
    const server = await startAgentDBServer();
    try {
      const runId = `r-${Date.now()}`;
      const run = { runId };
      const client = new Client({
        baseUrl: server.baseUrl,
        auth: server.auth,
      });
      const orch = new Orchestrator(client, run);

      await orch.startAutonomous(
        "Sketch a one-page marketing brief for a new productivity tool. Wrap up after the marketing team posts a draft.",
        {
          ...DEFAULT_BUDGETS,
          maxMinutes: 3,
          maxLlmCalls: 12,
          maxRevisions: 4,
          stepTimeoutSec: 60,
        },
      );

      // Verify _plan has at least one plan_revised (a plan was made)
      const plan = await loadCurrentPlan(client, run, 50);
      assert.ok(plan.steps.length > 0, "expected at least one step in the plan");

      // Verify _meta contains a run_ended event with a known cause
      const metaSess = client.session(`${runId}:_meta`);
      const resp = await metaSess.fetchRecent(1024);
      const ended = (resp.recent ?? []).find(
        (e: any) => (e.payload?.kind ?? e.kind) === "run_ended",
      );
      assert.ok(ended, "expected a run_ended event in _meta");
      const cause = ((ended as any).payload ?? ended).cause;
      assert.ok(
        [
          "wrap_up",
          "revision_budget_exhausted",
          "time_budget_exhausted",
          "call_budget_exhausted",
        ].includes(cause),
        `unexpected run_ended cause: ${cause}`,
      );
    } finally {
      server.shutdown();
    }
  },
);
