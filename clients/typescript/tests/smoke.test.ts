/**
 * End-to-end smoke test for the agentdb TypeScript SDK.
 *
 * Spawns `agentdb serve --in-memory` as a subprocess on an ephemeral
 * port, then exercises the V2 SDK against it. Run with:
 *
 *   npm test
 *
 * (which builds first, then runs `node --test dist/tests`).
 *
 * Skipped by default. Set `AGENTDB_TYPESCRIPT_SMOKE=1` after building the
 * server binary to run it.
 */

import test from "node:test";
import assert from "node:assert/strict";
import { spawn, ChildProcess } from "node:child_process";
import { existsSync } from "node:fs";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  Client,
  Session,
  SessionEventKind,
  agentStatus,
  contextPackBuilt,
  contextSourceRefs,
  modelMessage,
  toolCall,
  toolResult,
  userMessage,
} from "../src/index.js";

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

  return { baseUrl, proc };
}

const skipReason = !existsSync(SERVER_BIN)
  ? `server binary not found at ${SERVER_BIN}; run \`cargo build -p agentdb-server\` first`
  : process.env["AGENTDB_TYPESCRIPT_SMOKE"] !== "1"
    ? "set AGENTDB_TYPESCRIPT_SMOKE=1 to run live server smoke tests"
  : undefined;

test("agentdb SDK end-to-end", { skip: skipReason }, async (t) => {
  const { baseUrl, proc } = await spawnServer();
  t.after(() => {
    proc.kill();
  });

  let counter = 0;
  async function newSession(label: string): Promise<Session> {
    counter += 1;
    const client = new Client({ baseUrl });
    const tenant = await client.createTenant(`${label}-${counter}`);
    const agent = await client.createAgent(String(tenant.tenant_id), { alias: `${label}-agent` });
    return await client.createSession(String(tenant.tenant_id), String(agent.agent_id), { alias: label });
  }

  await t.test("append round-trips a single event", async () => {
    const sess = await newSession("agent-smoke-1");
    const result = await sess.append(
      { role: "user", text: "hello" },
      { fetchLastN: 5 }
    );
    assert.ok(result.seq >= 0);
    assert.equal(result.recent.length, 1);
    const payload = result.recent[0]!.payload as Record<string, string>;
    assert.equal(payload.role, "user");
    assert.equal(payload.text, "hello");
  });

  await t.test("multiple appends accumulate in order", async () => {
    const sess = await newSession("agent-smoke-2");
    await sess.append({ i: 1 });
    await sess.append({ i: 2 });
    const last = await sess.append({ i: 3 }, { fetchLastN: 10 });
    const seqs = last.recent.map((e) => (e.payload as { i: number }).i);
    assert.deepEqual(seqs, [1, 2, 3]);
  });

  await t.test("fork creates a child that inherits parent events", async () => {
    const parent = await newSession("agent-smoke-fork-parent");
    await parent.append({ i: 1 });
    await parent.append({ i: 2 });
    const child = await parent.fork({ atSeq: 1, alias: "agent-smoke-fork-child" });
    assert.ok(child instanceof Session);
    assert.match(child.sessionId, /^ses_/);
    const res = await child.append({ i: 99 }, { fetchLastN: 10 });
    const last = res.recent[res.recent.length - 1]!;
    assert.equal((last.payload as { i: number }).i, 99);
  });

  await t.test("harness recovers a session from persisted IDs", async () => {
    const firstClient = new Client({ baseUrl });
    const tenant = await firstClient.createTenant("agent-smoke-recovery");
    const tenantId = String(tenant.tenant_id);
    const agent = await firstClient.createAgent(tenantId, { alias: "agent-smoke-recovery-agent" });
    const agentId = String(agent.agent_id);
    const session = await firstClient.createSession(tenantId, agentId, { alias: "agent-smoke-recovery" });

    await session.append(
      userMessage("summarize the sandbox output"),
      { kind: SessionEventKind.UserMessage }
    );
    const call = await session.append(
      toolCall("call_1", "sandbox.exec", { cmd: "python report.py" }),
      { kind: SessionEventKind.ToolCall }
    );
    const artifact = await firstClient.putArtifact(
      tenantId,
      Buffer.from("generated report\nline 1\nline 2\n"),
      { contentType: "text/plain" }
    );
    await session.append(
      toolResult("call_1", "Generated report with two lines."),
      {
        kind: SessionEventKind.ToolResult,
        artifactRefs: [
          {
            artifact_id: String(artifact.artifact_id),
            content_type: String(artifact.content_type),
            bytes: Number(artifact.bytes),
            sha256: String(artifact.sha256),
          },
        ],
        sourceRefs: [{ event_id: call.eventId }],
      }
    );
    const context = await session.context({ include: ["recent_events"], maxEvents: 10 });
    assert.ok(Array.isArray(context.events) && context.events.length > 0);
    const contextEvent = await session.append(
      contextPackBuilt("ctx_recovery_1", ["recent_events"], {
        maxEvents: 10,
        selectedEventCount: (context.events as unknown[]).length,
      }),
      {
        kind: SessionEventKind.ContextPackBuilt,
        sourceRefs: contextSourceRefs(context),
      }
    );
    await session.append(
      modelMessage("The report has two generated lines.", {
        contextPackId: "ctx_recovery_1",
      }),
      {
        kind: SessionEventKind.ModelMessage,
        sourceRefs: [{ event_id: contextEvent.eventId }],
      }
    );

    const beforeCrash = await session.fetch({ fromSeq: 0, limit: 100 });
    const lastSeqBeforeCrash = beforeCrash[beforeCrash.length - 1]!.seq;

    // Simulate a fresh harness process: only persisted IDs are carried forward.
    const recoveryClient = new Client({ baseUrl });
    const recovered = recoveryClient.session(tenantId, session.sessionId, { agentId });
    const replayed = await recovered.replay({ fromSeq: 0, limit: 100 });
    assert.deepEqual(
      replayed.map((event) => event.kind),
      ["user_message", "tool_call", "tool_result", "context_pack_built", "model_message"]
    );

    const resumed = await recovered.append(
      agentStatus("recovered", { lastSeenSeq: lastSeqBeforeCrash }),
      { kind: SessionEventKind.AgentStatus }
    );
    assert.equal(resumed.seq, lastSeqBeforeCrash + 1);
  });

  await t.test("fetchRecent on empty session returns empty array", async () => {
    const sess = await newSession("agent-smoke-fetch-empty");
    const result = await sess.fetchRecent(5);
    assert.deepEqual(result.recent, []);
  });

  await t.test("fetchRecent after 3 appends returns last 2 in order", async () => {
    const sess = await newSession("agent-smoke-fetch-3");
    await sess.append({ i: 1 });
    await sess.append({ i: 2 });
    await sess.append({ i: 3 });
    const result = await sess.fetchRecent(2);
    assert.equal(result.recent.length, 2);
    const payloads = result.recent.map((e) => (e.payload as { i: number }).i);
    assert.deepEqual(payloads, [2, 3]);
  });

  await t.test("fetchRecent(0) returns empty array", async () => {
    const sess = await newSession("agent-smoke-fetch-zero");
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
