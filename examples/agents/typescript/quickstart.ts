/**
 * Quickstart agent — three turns of "thought-act-observe" with
 * working memory backed by AgentDB.
 *
 * Each turn does the canonical agent flow:
 *
 *   1. Pull the recent context (last N events) for the session.
 *   2. Decide an action based on that context.
 *   3. Append the new thought / action / observation to the session
 *      so the next turn — or a forked branch — sees it.
 *
 * Steps 1 + 3 happen in one round trip via `append + fetchLastN`.
 * That's the AgentDB hot-path primitive and the reason this whole
 * demo runs in <100ms against a local dev server.
 *
 * Run me:
 *
 *     # In one terminal:
 *     cargo run -p agentdb-server -- serve --dev
 *
 *     # In another:
 *     cd clients/typescript && npm install && npm run build
 *     node --experimental-strip-types examples/agents/typescript/quickstart.ts
 *
 * Or compile and run:
 *
 *     npx tsc --target es2022 --module nodenext --moduleResolution nodenext \
 *         --outDir /tmp/agentdb-quickstart \
 *         examples/agents/typescript/quickstart.ts \
 *         --paths '{"agentdb":["./clients/typescript/dist/src/index.js"]}'
 *     node /tmp/agentdb-quickstart/quickstart.js
 *
 * Expected output: three turns printed, each showing the working
 * memory growing by one event. Then a fork that branches off the
 * session and shows the child sees the parent's history at fork
 * time.
 */

import { AgentDB } from "../../../clients/typescript/dist/index.js";

const SERVER_URL = process.env.AGENTDB_URL ?? "http://127.0.0.1:7878";
const USER = process.env.AGENTDB_USER ?? "demo";
const PASSWORD = process.env.AGENTDB_PASSWORD ?? "demo-secret";
const WORKSPACE = process.env.AGENTDB_WORKSPACE ?? process.env.AGENTDB_TENANT ?? "quickstart-ts";
const SESSION_ID = "quickstart-agent-ts";

interface ChatPayload {
  role: string;
  text?: string;
  name?: string;
  result?: number;
  context_size?: number;
}

function thoughtForTurn(turn: number, recentTexts: string[]): ChatPayload {
  if (turn === 0) {
    return { role: "user", text: "What's 2 + 2?" };
  }
  if (turn === 1) {
    return {
      role: "assistant",
      text: "Looking back, the user asked '2 + 2'. The answer is 4.",
    };
  }
  return {
    role: "tool",
    name: "calculator",
    result: 4,
    context_size: recentTexts.length,
  };
}

async function main(): Promise<void> {
  const db = new AgentDB({
    baseUrl: SERVER_URL,
    auth: { user: USER, password: PASSWORD },
  });
  if (!(await db.healthReady())) {
    console.error(
      `AgentDB not ready at ${SERVER_URL}. Start it with:\n` +
        `    cargo run -p agentdb-server -- serve --dev`
    );
    process.exit(1);
  }

  const workspace = await db.workspace(WORKSPACE);
  const agent = await workspace.agent("quickstart-agent-ts");
  const sess = await agent.session(SESSION_ID);
  console.log(`# Workspace: ${workspace.id}`);
  console.log(`# Agent:    ${agent.id}`);
  console.log(`# Session:  ${sess.id}`);

  for (let turn = 0; turn < 3; turn++) {
    const newEvent = thoughtForTurn(turn, []);
    const result = await sess.append(newEvent, { fetchLastN: 5 });
    console.log(`--- turn ${turn} (seq=${result.seq}) ---`);
    for (const ev of result.recent) {
      const p = ev.payload as ChatPayload;
      const actor = p.role ?? "?";
      const text = p.text ?? p.result ?? "(no text)";
      console.log(`  [${String(ev.seq).padStart(3)}] ${actor.padStart(9)}: ${text}`);
    }
    console.log();
  }

  // Fork: branch the agent off into an alternate timeline.
  console.log("# Forking into an alternate timeline...");
  const branch = await sess.fork(`${SESSION_ID}-branch-a`);
  const branchResult = await branch.append(
    { role: "user", text: "What if it were 3 + 3 instead?" },
    { fetchLastN: 10 }
  );
  console.log(`--- branch turn 0 (seq=${branchResult.seq}) ---`);
  for (const ev of branchResult.recent) {
    const p = ev.payload as ChatPayload;
    const actor = p.role ?? "?";
    const text = p.text ?? p.result ?? "(no text)";
    console.log(`  [${String(ev.seq).padStart(3)}] ${actor.padStart(9)}: ${text}`);
  }
}

main().catch((err) => {
  console.error("agent run failed:", err);
  process.exit(1);
});
