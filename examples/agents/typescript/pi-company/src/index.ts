import { Client } from "agentdb";
import { Orchestrator } from "./orchestrator.ts";
import { Channels } from "./channels.ts";
import { CompanyTUI } from "./tui.ts";
import { startAgentDBServer } from "./server.ts";
import { launchScenario, runScenario } from "./scenarios/launch.ts";
import { assertApiKeyAvailable } from "./llm.ts";
import { DEFAULT_BUDGETS, type Budgets } from "./ceo.ts";

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
  const run = { runId };

  console.log(`pi-company starting (run-id: ${runId})...`);
  const server = await startAgentDBServer();
  console.log(`AgentDB ready at ${server.baseUrl}`);
  const client = new Client({
    baseUrl: server.baseUrl,
    auth: server.auth,
  });
  const orchestrator = new Orchestrator(client, run);

  if (args.goal) {
    console.log(`Autonomous mode (goal: ${args.goal.slice(0, 80)})...`);
    try {
      await orchestrator.startAutonomous(args.goal, args.budgets);
    } finally {
      orchestrator.stopAgents();
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
      await runScenario(launchScenario, channels, client);
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
