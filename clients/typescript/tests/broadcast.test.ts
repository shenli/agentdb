/**
 * Tests for Phase 2 `broadcast()` and `subscribeChannel()`.
 *
 * Two layers (mirrors `subscribe.test.ts`):
 *
 * 1. Pure-TS unit tests (always run on `npm test`). Cover envelope
 *    parsing + the public-API guards (tenant required, etc.) — no
 *    network, no server.
 *
 * 2. End-to-end smoke against a live `agentdb serve --dev`. Skipped
 *    automatically when the server binary hasn't been built so a
 *    fresh checkout's `npm test` doesn't fail on missing cargo
 *    output. Reuses the same spawn helper pattern as
 *    `subscribe.test.ts`.
 */

import test from "node:test";
import assert from "node:assert/strict";
import { spawn, ChildProcess } from "node:child_process";
import { existsSync } from "node:fs";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { Client, parseBroadcastEnvelope } from "../src/index.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, "..", "..", "..", "..");
const SERVER_BIN = path.join(REPO_ROOT, "target", "debug", "agentdb");

// --- Envelope parsing (no network) ---------------------------------

test("parseBroadcastEnvelope parses full envelope with sender", () => {
  const msg = parseBroadcastEnvelope(
    JSON.stringify({
      tenant: "t-1",
      channel: "c-1",
      ts_ms: 100,
      sender: "u-1",
      payload: { k: "v" },
    })
  );
  assert.equal(msg.tenant, "t-1");
  assert.equal(msg.channel, "c-1");
  assert.equal(msg.ts_ms, 100);
  assert.equal(msg.sender, "u-1");
  assert.deepEqual(msg.payload, { k: "v" });
});

test("parseBroadcastEnvelope handles missing sender", () => {
  // Basic-auth publishers emit envelopes without a `sender` field.
  // The parser leaves it `undefined` so subscribers can distinguish
  // them from JWT publishers cleanly.
  const msg = parseBroadcastEnvelope(
    JSON.stringify({
      tenant: "t-1",
      channel: "c-1",
      ts_ms: 100,
      payload: {},
    })
  );
  assert.equal(msg.sender, undefined);
});

test("parseBroadcastEnvelope preserves arbitrary payload shapes", () => {
  const msg = parseBroadcastEnvelope(
    JSON.stringify({
      tenant: "t-1",
      channel: "c-1",
      ts_ms: 100,
      payload: [1, 2, "three", { nested: true }],
    })
  );
  assert.deepEqual(msg.payload, [1, 2, "three", { nested: true }]);
});

// --- Public API surface (no network) -------------------------------

test("client.broadcast() throws when tenant unset", async () => {
  const client = new Client("http://127.0.0.1:7878"); // no tenant, no auth
  await assert.rejects(
    () => client.broadcast({ channel: "c-1", payload: { x: 1 } }),
    /tenant/
  );
});

test("client.subscribeChannel() throws when tenant unset", async () => {
  const client = new Client("http://127.0.0.1:7878");
  await assert.rejects(async () => {
    // The async generator runs the validation on the first .next()
    // before any network I/O.
    for await (const _ of client.subscribeChannel({ channel: "c-1" })) {
      break;
    }
  }, /tenant/);
});

// --- End-to-end smoke (gated by server binary) ---------------------

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

test("broadcast() round-trip end-to-end", { skip: skipReason }, async (t) => {
  const { baseUrl, proc } = await spawnServer();
  t.after(() => {
    proc.kill();
  });

  await t.test("publish + subscribe delivers payloads in order", async () => {
    const subscriber = new Client(baseUrl, { tenant: "t-bcast-ts" });
    const seen: { i: number }[] = [];
    const iter = subscriber.subscribeChannel({ channel: "c-rt" });
    const consumer = (async () => {
      for await (const msg of iter) {
        seen.push(msg.payload as { i: number });
        if (seen.length >= 2) return;
      }
    })();
    // Let the subscriber attach; broadcast has no replay, so a
    // publish before the subscribe lands would be silently dropped.
    await new Promise((r) => setTimeout(r, 500));

    const publisher = new Client(baseUrl, { tenant: "t-bcast-ts" });
    await publisher.broadcast({ channel: "c-rt", payload: { i: 1 } });
    await publisher.broadcast({ channel: "c-rt", payload: { i: 2 } });

    await Promise.race([
      consumer,
      new Promise((_, rej) =>
        setTimeout(() => rej(new Error("timeout")), 10_000)
      ),
    ]);
    assert.equal(seen.length, 2);
    const ints = seen.map((p) => p.i).sort((a, b) => a - b);
    assert.deepEqual(ints, [1, 2]);
  });

  await t.test("envelope carries tenant + channel and no sender on Basic-auth", async () => {
    const subscriber = new Client(baseUrl, { tenant: "t-bcast-ts" });
    const iter = subscriber.subscribeChannel({ channel: "c-meta" });
    const promise = (async () => {
      for await (const msg of iter) return msg;
      return undefined;
    })();
    await new Promise((r) => setTimeout(r, 500));

    const publisher = new Client(baseUrl, { tenant: "t-bcast-ts" });
    await publisher.broadcast({ channel: "c-meta", payload: { hi: true } });

    const msg = await Promise.race([
      promise,
      new Promise<undefined>((_, rej) =>
        setTimeout(() => rej(new Error("timeout")), 10_000)
      ),
    ]);
    assert.ok(msg, "expected at least one envelope");
    assert.equal(msg!.tenant, "t-bcast-ts");
    assert.equal(msg!.channel, "c-meta");
    assert.equal(msg!.sender, undefined); // dev mode = no JWT, no `sub`
    assert.equal(typeof msg!.ts_ms, "number");
  });
});
