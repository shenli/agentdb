import { Client } from "agentdb";
import { Orchestrator } from "./orchestrator.ts";
import { Channels } from "./channels.ts";
import { CompanyTUI } from "./tui.ts";
import { startAgentDBServer } from "./server.ts";
import { launchScenario, runScenario } from "./scenarios/launch.ts";
import { assertApiKeyAvailable } from "./llm.ts";

const TENANT = "pi-company";

interface ParsedArgs {
  scenario?: string;
  resumeRunId?: string;
}

/**
 * Minimal flag parser. Only supports `--scenario=NAME` and `--resume=ID`
 * (KEY=VALUE form). Plain space-separated `--scenario NAME` is rejected
 * with a clear error so the user doesn't silently get an undefined arg.
 */
function parseArgs(argv: string[]): ParsedArgs {
  const out: ParsedArgs = {};
  for (const a of argv) {
    if (a.startsWith("--scenario=")) {
      out.scenario = a.split("=")[1];
    } else if (a === "--scenario") {
      throw new Error("Use --scenario=NAME (e.g., --scenario=launch)");
    } else if (a.startsWith("--resume=")) {
      out.resumeRunId = a.split("=")[1];
    }
  }
  return out;
}

async function main(): Promise<void> {
  // Fail fast if no API key — before spawning cargo.
  assertApiKeyAvailable();

  const args = parseArgs(process.argv.slice(2));
  const runId = args.resumeRunId ?? `r-${Date.now()}`;
  const run = { tenant: TENANT, runId };

  console.log(`pi-company starting (run-id: ${runId})...`);

  const server = await startAgentDBServer();
  console.log(`AgentDB ready at ${server.baseUrl}`);

  const client = new Client(server.baseUrl, { tenant: TENANT });
  const orchestrator = new Orchestrator(client, run);
  await orchestrator.startCompany();
  console.log("All 5 agents joined.");

  const channels = new Channels(client, run);

  if (args.scenario === "launch") {
    console.log(`Running scripted scenario 'launch' (run-id: ${runId})...`);
    try {
      await runScenario(launchScenario, channels, client, TENANT);
      // Give agents a few seconds to drain after the last directive.
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
    // Interactive mode: TUI prompt drives CEO directives.
    const tui = new CompanyTUI({
      channels,
      onCeoInput: async (text: string) => {
        await channels.publish("announcements", { kind: "directive", from: "ceo", text });
      },
    });
    await tui.start();
    // tui.start() returns once subscriptions are wired up; wait() blocks
    // until the channel loops settle (i.e. process is shutting down).
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
