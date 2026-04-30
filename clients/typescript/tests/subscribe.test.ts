/**
 * End-to-end smoke for the TypeScript SDK's `subscribe()`.
 *
 * Same fixture pattern as smoke.test.ts: spawn `agentdb serve --in-memory`
 * on an ephemeral port and exercise the V2 SDK against it. Skipped when the
 * server binary hasn't been built. Set `AGENTDB_TYPESCRIPT_SMOKE=1` to run the
 * live server cases.
 *
 * Also covers the synchronous validation paths (no server needed) so
 * those run on every `npm test` regardless of binary availability.
 */

import test from "node:test";
import assert from "node:assert/strict";
import { spawn, ChildProcess } from "node:child_process";
import { existsSync } from "node:fs";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { Client, SubscriptionClosed } from "../src/index.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, "..", "..", "..", "..");
const SERVER_BIN = path.join(REPO_ROOT, "target", "debug", "agentdb");

async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      if (typeof addr === "object" && addr !== null) {
        const port = addr.port;
        srv.close(() => resolve(port));
      } else {
        srv.close();
        reject(new Error("could not allocate port"));
      }
    });
    srv.on("error", reject);
  });
}

async function spawnServer(): Promise<{
  baseUrl: string;
  proc: ChildProcess;
}> {
  const restPort = await freePort();
  const proc = spawn(
    SERVER_BIN,
    [
      "serve",
      "--in-memory",
      "--bind",
      `127.0.0.1:${restPort}`,
    ],
    {
      stdio: ["ignore", "ignore", "ignore"],
      env: {
        ...process.env,
        RUST_LOG: "warn",
      },
    }
  );
  const baseUrl = `http://127.0.0.1:${restPort}`;
  const healthClient = new Client({ baseUrl });
  const deadline = Date.now() + 15_000;
  let ready = false;
  while (Date.now() < deadline) {
    if (await healthClient.healthReady()) {
      ready = true;
      break;
    }
    if (proc.exitCode !== null) {
      throw new Error(`server exited before ready (rc=${proc.exitCode})`);
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  if (!ready) {
    proc.kill();
    throw new Error("server didn't become ready within 15s");
  }

  return { baseUrl, proc };
}

const skipReason = !existsSync(SERVER_BIN)
  ? `server binary not found at ${SERVER_BIN}; run \`cargo build -p agentdb-server\` first`
  : process.env["AGENTDB_TYPESCRIPT_SMOKE"] !== "1"
    ? "set AGENTDB_TYPESCRIPT_SMOKE=1 to run live server smoke tests"
  : undefined;

// --- Validation tests (no server) ----------------------------------

test("Client constructor rejects token + getToken combined", () => {
  assert.throws(
    () =>
      new Client({
        baseUrl: "http://127.0.0.1:7878",
        token: "x",
        getToken: () => "y",
      }),
    /token.*getToken/
  );
});

test("SubscriptionClosed carries error code + last_seq", () => {
  const e = new SubscriptionClosed("lag_too_far", { lastSeq: 42 });
  assert.equal(e.errorCode, "lag_too_far");
  assert.equal(e.lastSeq, 42);
  assert.equal(e.statusCode, 0);
  assert.match(e.message, /lag_too_far/);
});

// --- Live smoke (gated by server binary) ---------------------------

test("subscribe() end-to-end", { skip: skipReason }, async (t) => {
  const { baseUrl, proc } = await spawnServer();
  t.after(() => {
    proc.kill();
  });

  let counter = 0;
  async function newSession(label: string) {
    counter += 1;
    const client = new Client({ baseUrl });
    const tenant = await client.createTenant(`${label}-${counter}`);
    const agent = await client.createAgent(String(tenant.tenant_id), { alias: `${label}-agent` });
    return await client.createSession(String(tenant.tenant_id), String(agent.agent_id), { alias: label });
  }

  await t.test("session.subscribe replays a session SSE snapshot", async () => {
    const writer = await newSession("s-sub-replay");
    for (let i = 0; i < 3; i++) {
      await writer.append({ kind: "x", i });
    }
    const seen: number[] = [];
    for await (const evt of writer.subscribe({ fromSeq: 0 })) {
      seen.push(evt.seq);
      if (seen.length >= 3) break;
    }
    assert.deepEqual(seen, [0, 1, 2]);
  });

  await t.test("tenant watch sees events from any session", async () => {
    const first = await newSession("s-a");
    const client = new Client({ baseUrl });
    const second = await client.createSession(first.tenantId, first.agentId!, { alias: "s-b" });
    await first.append({ kind: "p", t: 1 });
    await second.append({ kind: "p", t: 2 });
    await first.append({ kind: "p", t: 3 });
    await second.append({ kind: "p", t: 4 });
    const seen = await client.watch(first.tenantId, { scope: "tenant", limit: 10 });
    const sids = new Set(seen.map((item) => item.event.sessionId));
    assert.ok(sids.has(first.sessionId) && sids.has(second.sessionId));
  });
});
