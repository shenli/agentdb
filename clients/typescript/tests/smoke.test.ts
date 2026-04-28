/**
 * End-to-end smoke test for the agentdb TypeScript SDK.
 *
 * Spawns `agentdb serve --dev` as a subprocess on an ephemeral port
 * with `AGENTDB_REQUIRE_AUTH=true`, creates a test user via the admin
 * API, then exercises the SDK against it. Run with:
 *
 *   npm test
 *
 * (which builds first, then runs `node --test dist/tests`).
 *
 * Skipped automatically if the server binary hasn't been built —
 * running this in CI without `cargo build -p agentdb-server`
 * first is a no-op rather than a hard failure.
 */

import test from "node:test";
import assert from "node:assert/strict";
import { spawn, ChildProcess } from "node:child_process";
import { existsSync } from "node:fs";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { Client, Session } from "../src/index.js";

const __filename = fileURLToPath(import.meta.url);
// dist/tests/smoke.test.js → up four levels = repo root
// (dist → typescript → clients → repo root)
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
  testUser: string;
  testPassword: string;
  testTenant: string;
}> {
  const restPort = await freePort();
  const mongoPort = await freePort();
  const proc = spawn(
    SERVER_BIN,
    [
      "serve",
      "--dev",
      "--addr",
      `127.0.0.1:${restPort}`,
      "--mongo-addr",
      `127.0.0.1:${mongoPort}`,
    ],
    {
      stdio: ["ignore", "ignore", "ignore"],
      env: {
        ...process.env,
        RUST_LOG: "warn",
        AGENTDB_REQUIRE_AUTH: "true",
      },
    }
  );
  const baseUrl = `http://127.0.0.1:${restPort}`;
  const healthClient = new Client({ baseUrl });
  // Poll readiness for up to 15 seconds (cold cargo build can be slow).
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

  // Create a test user via the admin API (no admin auth required in dev mode).
  const testUser = "smoke-test-user";
  const testPassword = "smoke-password-123";
  const testTenant = "smoke-tenant";
  const resp = await fetch(`${baseUrl}/api/admin/users`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      user_id: testUser,
      tenant_external: testTenant,
      password: testPassword,
    }),
  });
  if (resp.status !== 201 && resp.status !== 409) {
    const body = await resp.text();
    throw new Error(`Failed to create test user: ${resp.status} ${body}`);
  }

  return { baseUrl, proc, testUser, testPassword, testTenant };
}

const skipReason = !existsSync(SERVER_BIN)
  ? `server binary not found at ${SERVER_BIN}; run \`cargo build -p agentdb-server\` first`
  : undefined;

test("agentdb SDK end-to-end", { skip: skipReason }, async (t) => {
  const { baseUrl, proc, testUser, testPassword } = await spawnServer();
  t.after(() => {
    proc.kill();
  });

  const auth = { user: testUser, password: testPassword };

  await t.test("append round-trips a single event", async () => {
    const client = new Client({ baseUrl, auth });
    const sess = client.session("agent-smoke-1");
    const result = await sess.append(
      { role: "user", text: "hello" },
      { fetchLastN: 5 }
    );
    assert.ok(result.seq >= 1);
    assert.equal(result.recent.length, 1);
    const payload = result.recent[0]!.payload as Record<string, string>;
    assert.equal(payload.role, "user");
    assert.equal(payload.text, "hello");
  });

  await t.test("multiple appends accumulate in order", async () => {
    const client = new Client({ baseUrl, auth });
    const sess = client.session("agent-smoke-2");
    await sess.append({ i: 1 });
    await sess.append({ i: 2 });
    const last = await sess.append({ i: 3 }, { fetchLastN: 10 });
    const seqs = last.recent.map((e) => (e.payload as { i: number }).i);
    assert.deepEqual(seqs, [1, 2, 3]);
  });

  await t.test("fork creates a child that inherits parent events", async () => {
    const client = new Client({ baseUrl, auth });
    const parent = client.session("agent-smoke-fork-parent");
    await parent.append({ i: 1 });
    await parent.append({ i: 2 });
    const child = await parent.fork("agent-smoke-fork-child");
    assert.ok(child instanceof Session);
    assert.equal(child.sessionId, "agent-smoke-fork-child");
    const res = await child.append({ i: 99 }, { fetchLastN: 10 });
    const last = res.recent[res.recent.length - 1]!;
    assert.equal((last.payload as { i: number }).i, 99);
  });

  await t.test("fetchRecent on empty session returns empty array", async () => {
    const client = new Client({ baseUrl, auth });
    const sess = client.session("agent-smoke-fetch-empty");
    const result = await sess.fetchRecent(5);
    assert.deepEqual(result.recent, []);
  });

  await t.test("fetchRecent after 3 appends returns last 2 in order", async () => {
    const client = new Client({ baseUrl, auth });
    const sess = client.session("agent-smoke-fetch-3");
    await sess.append({ i: 1 });
    await sess.append({ i: 2 });
    await sess.append({ i: 3 });
    const result = await sess.fetchRecent(2);
    assert.equal(result.recent.length, 2);
    const payloads = result.recent.map((e) => (e.payload as { i: number }).i);
    assert.deepEqual(payloads, [2, 3]);
  });

  await t.test("fetchRecent(0) returns empty array", async () => {
    const client = new Client({ baseUrl, auth });
    const sess = client.session("agent-smoke-fetch-zero");
    await sess.append({ x: 1 });
    const result = await sess.fetchRecent(0);
    assert.deepEqual(result.recent, []);
  });

  await t.test("health_ready probe returns true when up", async () => {
    const client = new Client({ baseUrl });
    assert.equal(await client.healthReady(), true);
  });
});

test("constructing a Client with token and getToken combined throws", () => {
  assert.throws(
    () => new Client({ baseUrl: "http://127.0.0.1:7878", token: "x", getToken: () => "y" }),
    /token.*getToken/
  );
});
