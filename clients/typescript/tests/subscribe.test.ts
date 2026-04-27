/**
 * End-to-end smoke for the TypeScript SDK's `subscribe()`.
 *
 * Same fixture pattern as smoke.test.ts: spawn `agentdb serve --dev`
 * on an ephemeral port, exercise the SDK against it. Skipped when
 * the server binary hasn't been built (so a fresh checkout's
 * `npm test` doesn't fail on missing cargo output).
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

async function spawnServer(): Promise<{ baseUrl: string; proc: ChildProcess }> {
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
      env: { ...process.env, RUST_LOG: "warn" },
    }
  );
  const baseUrl = `http://127.0.0.1:${restPort}`;
  const client = new Client(baseUrl);
  const deadline = Date.now() + 15_000;
  let ready = false;
  while (Date.now() < deadline) {
    if (await client.healthReady()) {
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
  : undefined;

// --- Validation tests (no server) ----------------------------------

test("Client constructor rejects jwt + getToken combined", () => {
  assert.throws(
    () =>
      new Client("http://127.0.0.1:7878", {
        jwt: "x",
        getToken: () => "y",
      }),
    /jwt.*getToken/
  );
});

test("client.subscribe() throws when tenant unset", async () => {
  const client = new Client("http://127.0.0.1:7878");
  await assert.rejects(async () => {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    for await (const _ of client.subscribe()) {
      break;
    }
  }, /tenant/);
});

test("session.subscribe() throws when tenant unset", async () => {
  const client = new Client("http://127.0.0.1:7878");
  const sess = client.session("agent-1");
  await assert.rejects(async () => {
    for await (const _ of sess.subscribe()) {
      break;
    }
  }, /tenant/);
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

  await t.test("session.subscribe yields events appended after open", async () => {
    const client = new Client(baseUrl, { tenant: "t-1" });
    const sess = client.session("s-sub-1");

    const seen: number[] = [];
    const iter = sess.subscribe();
    const consumer = (async () => {
      for await (const evt of iter) {
        seen.push(evt.seq);
        if (seen.length >= 3) return;
      }
    })();
    // Give the SSE handshake a beat to land before we start writing.
    await new Promise((r) => setTimeout(r, 500));

    const writer = client.session("s-sub-1");
    for (let i = 1; i <= 3; i++) {
      await writer.append({ kind: "msg", n: i });
    }
    await Promise.race([
      consumer,
      new Promise((_, rej) =>
        setTimeout(() => rej(new Error("timeout")), 10_000)
      ),
    ]);
    assert.deepEqual(
      [...seen].sort((a, b) => a - b),
      [1, 2, 3]
    );
  });

  await t.test("session.subscribe replays from from_seq, then tails live", async () => {
    const client = new Client(baseUrl, { tenant: "t-1" });
    const writer = client.session("s-sub-replay");
    for (let i = 0; i < 3; i++) {
      await writer.append({ kind: "x", i });
    }
    const reader = client.session("s-sub-replay");
    const seen: number[] = [];
    for await (const evt of reader.subscribe({ fromSeq: 1 })) {
      seen.push(evt.seq);
      if (seen.length >= 3) break;
    }
    assert.deepEqual(seen, [1, 2, 3]);
  });

  await t.test("client.subscribe (tenant-wide) sees events from any session", async () => {
    const client = new Client(baseUrl, { tenant: "t-fanout" });
    const seen: { sid: string; seq: number }[] = [];
    const iter = client.subscribe();
    const consumer = (async () => {
      for await (const evt of iter) {
        seen.push({ sid: evt.session ?? "?", seq: evt.seq });
        if (seen.length >= 4) return;
      }
    })();
    await new Promise((r) => setTimeout(r, 500));
    await client.session("s-a").append({ kind: "p", t: 1 });
    await client.session("s-b").append({ kind: "p", t: 2 });
    await client.session("s-a").append({ kind: "p", t: 3 });
    await client.session("s-b").append({ kind: "p", t: 4 });
    await Promise.race([
      consumer,
      new Promise((_, rej) =>
        setTimeout(() => rej(new Error("timeout")), 10_000)
      ),
    ]);
    assert.equal(seen.length, 4);
    const sids = new Set(seen.map((e) => e.sid));
    assert.ok(sids.has("s-a") && sids.has("s-b"));
  });
});
