/**
 * Two-agent typing-indicator demo on AgentDB (TypeScript, Phase 2).
 *
 * Demonstrates the spec § 2 typing-indicator use case using ephemeral
 * broadcast channels:
 *
 *   - A publisher emits "typing" pings every 500ms, then a final
 *     "stopped_typing" event.
 *   - A reactor subscribes to the same channel and prints every
 *     event as it arrives.
 *
 * Broadcast is ephemeral by design — if the reactor were not yet
 * attached when a ping was published, that ping would be dropped
 * (no replay, no per-subscriber acks). For typing indicators that
 * trade-off is exactly what you want.
 *
 * Run me:
 *
 *     # Terminal 1
 *     cargo run -p agentdb-server -- serve --dev
 *
 *     # Terminal 2 (after building the SDK once)
 *     cd clients/typescript && npm install && npm run build && cd ../..
 *     node --experimental-strip-types \
 *          examples/agents/typescript/broadcast_typing.ts
 *
 * Expected output (interleaving may vary):
 *
 *     reactor: subscribing to room:42
 *     publisher: sent typing ping 0
 *     reactor: alice → typing
 *     publisher: sent typing ping 1
 *     reactor: alice → typing
 *     ...
 *     publisher: done
 *     reactor: alice → stopped_typing
 */

import { Client } from "../../../clients/typescript/dist/src/index.js";

const SERVER_URL = process.env.AGENTDB_URL ?? "http://127.0.0.1:7878";
const JWT_TOKEN = process.env.AGENTDB_JWT_TOKEN;
if (!JWT_TOKEN) {
  console.error("AGENTDB_JWT_TOKEN is required for broadcast/channel operations");
  process.exit(1);
}
const CHANNEL = "room:42";

interface TypingPayload {
  kind: "typing" | "stopped_typing";
  agent: string;
  at?: number;
}

async function typingPublisher(): Promise<void> {
  const client = new Client({ baseUrl: SERVER_URL, token: JWT_TOKEN });
  for (let i = 0; i < 5; i++) {
    await client.broadcast({
      channel: CHANNEL,
      payload: { kind: "typing", agent: "alice", at: Date.now() },
    });
    console.log(`publisher: sent typing ping ${i}`);
    await new Promise((r) => setTimeout(r, 500));
  }
  await client.broadcast({
    channel: CHANNEL,
    payload: { kind: "stopped_typing", agent: "alice" },
  });
  console.log("publisher: done");
}

async function typingReactor(): Promise<void> {
  const client = new Client({ baseUrl: SERVER_URL, token: JWT_TOKEN });
  console.log(`reactor: subscribing to ${CHANNEL}`);
  for await (const msg of client.subscribeChannel({ channel: CHANNEL })) {
    const p = msg.payload as TypingPayload;
    console.log(`reactor: ${p.agent} → ${p.kind}`);
    if (p.kind === "stopped_typing") return;
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
  // Reactor first; give it a brief moment to attach to the channel
  // before the publisher fires. Broadcast is ephemeral — anything
  // published before the SSE handshake completes is dropped.
  const consumer = typingReactor();
  await new Promise((r) => setTimeout(r, 200));
  await typingPublisher();
  await consumer;
}

main().catch((err) => {
  console.error("typing-indicator demo failed:", err);
  process.exit(1);
});
