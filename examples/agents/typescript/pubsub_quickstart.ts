/**
 * Multi-agent pub/sub demo on AgentDB (TypeScript).
 *
 * One producer task appends `task_started` / `task_done` pairs to a
 * shared session; one reactor task subscribes to the same session
 * with a `kind: ["task_done"]` filter and prints each task_done as
 * it arrives. Both run in the same process here for simplicity, but
 * they could just as easily be separate services pointing at the
 * same AgentDB.
 *
 * Run me:
 *
 *     # Terminal 1
 *     cargo run -p agentdb-server -- serve --dev
 *
 *     # Terminal 2 (after building the SDK once)
 *     cd clients/typescript && npm install && npm run build && cd ../..
 *     node --experimental-strip-types examples/agents/typescript/pubsub_quickstart.ts
 *
 * Expected output:
 *
 *     reactor: waiting for task_done events…
 *     reactor: saw task_done #0 (seq=2)
 *     reactor: saw task_done #1 (seq=4)
 *     reactor: saw task_done #2 (seq=6)
 */

import { Client } from "../../../clients/typescript/dist/src/index.js";

const SERVER_URL = process.env.AGENTDB_URL ?? "http://127.0.0.1:7878";
const TOKEN = process.env.AGENTDB_TOKEN;
const SESSION = "agent-coord-ts";

interface TaskPayload {
  kind: string;
  i: number;
}

async function producer(): Promise<void> {
  const client = new Client({ baseUrl: SERVER_URL, token: TOKEN });
  const sess = client.session(SESSION);
  // Tiny delay so the reactor's subscribe handshake lands first.
  // For a production-grade gap-free join, use `fromSeq` on the
  // reactor side instead.
  await new Promise((r) => setTimeout(r, 500));
  for (let i = 0; i < 3; i++) {
    await sess.append({ kind: "task_started", i });
    await new Promise((r) => setTimeout(r, 300));
    await sess.append({ kind: "task_done", i });
  }
}

async function reactor(): Promise<void> {
  const client = new Client({ baseUrl: SERVER_URL, token: TOKEN });
  const sess = client.session(SESSION);
  console.log("reactor: waiting for task_done events…");
  let seen = 0;
  for await (const evt of sess.subscribe({ kind: ["task_done"] })) {
    const p = evt.payload as TaskPayload;
    console.log(`reactor: saw task_done #${p.i} (seq=${evt.seq})`);
    seen += 1;
    if (seen >= 3) return;
  }
}

async function main(): Promise<void> {
  const ready = await new Client({ baseUrl: SERVER_URL }).healthReady();
  if (!ready) {
    console.error(
      `AgentDB not ready at ${SERVER_URL}. Start it with:\n` +
        `    cargo run -p agentdb-server -- serve --dev`
    );
    process.exit(1);
  }
  // Kick off the producer in the background, run the reactor in
  // the foreground. The reactor returns after seeing 3 events,
  // which lets the process exit cleanly.
  producer().catch((e) => console.error("producer failed:", e));
  await reactor();
}

main().catch((err) => {
  console.error("pubsub demo failed:", err);
  process.exit(1);
});
