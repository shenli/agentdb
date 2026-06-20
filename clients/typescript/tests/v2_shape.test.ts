import test from "node:test";
import assert from "node:assert/strict";

import { AgentDB, Client, appendResultRef, artifactRef, eventRef } from "../src/index.js";

function json(body: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

test("V2 SDK maps the core flow to tenant-scoped routes", async () => {
  const calls: string[] = [];
  const mockFetch = async (url: string | URL) => {
    const raw = String(url);
    calls.push(raw);
    if (raw.endsWith("/api/tenants")) return json({ tenant_id: "ten_1" });
    if (raw.endsWith("/agents")) return json({ agent_id: "agt_1" });
    if (raw.endsWith("/sessions")) return json({ session_id: "ses_1", agent_id: "agt_1" });
    if (raw.endsWith("/events")) return json({ event_id: "evt_1", seq: 0 });
    if (raw.endsWith("/append-and-fetch")) return json({ event_id: "evt_2", seq: 1, recent: [] });
    if (raw.endsWith("/fork")) return json({ session_id: "ses_child", agent_id: "agt_1" });
    if (raw.includes("/replay-prefix")) return json({ events: [] });
    if (raw.includes("/replay")) return json({ events: [] });
    if (raw.endsWith("/context")) return json({ events: [], records: [], skills: [] });
    if (raw.includes("/watch")) return json([]);
    if (raw.endsWith("/artifacts")) return json({ artifact_id: "art_1" });
    if (raw.endsWith("/records/search")) return json({ matches: [] });
    if (raw.endsWith("/records")) return json({ record_id: "rec_1" });
    if (raw.endsWith("/skills")) return json({ skill_id: "skl_1" });
    if (raw.endsWith("/skills/bind")) return json({ skill_id: "skl_1" });
    return json({});
  };

  const c = new Client({ baseUrl: "http://x", fetch: mockFetch as typeof globalThis.fetch });
  const tenant = await c.createTenant("acme");
  const agent = await c.createAgent(String(tenant.tenant_id));
  const session = await c.createSession(String(tenant.tenant_id), String(agent.agent_id));
  await session.append({ hello: "world" });
  await session.appendAndFetch({ hello: "again" });
  await session.fork({ atSeq: 0, alias: "branch" });
  await session.replay();
  await session.materializeReplayPrefix();
  await session.context({ include: ["recent_events", "records"] });
  await c.watch(String(tenant.tenant_id));
  await c.putArtifact(String(tenant.tenant_id), new Blob(["blob"]));
  await c.writeRecord(String(tenant.tenant_id), { namespace: ["mem"], text: "x" });
  await c.searchRecords(String(tenant.tenant_id), { namespace: ["mem"], text: "x" });
  const skill = await c.registerSkill(String(tenant.tenant_id), { name: "tool", version: "1" });
  await c.bindSkill(String(tenant.tenant_id), String(agent.agent_id), String(skill.skill_id));

  assert.ok(calls.some((url) => url.includes("/api/tenants/ten_1/agents")));
  assert.ok(calls.some((url) => url.includes("/api/tenants/ten_1/sessions/ses_1/events")));
  assert.ok(calls.some((url) => url.includes("/append-and-fetch")));
  assert.ok(calls.some((url) => url.includes("/fork")));
  assert.ok(calls.some((url) => url.includes("/replay")));
  assert.ok(calls.some((url) => url.includes("/replay-prefix")));
  assert.ok(calls.some((url) => url.includes("/context")));
  assert.ok(calls.some((url) => url.includes("/watch")));
});

test("friendly SDK binds workspace agent session aliases and canonical ids", async () => {
  const calls: { url: string; body?: unknown }[] = [];
  const mockFetch = async (url: string | URL, init?: RequestInit) => {
    const raw = String(url);
    calls.push({
      url: raw,
      body: typeof init?.body === "string" ? JSON.parse(init.body) : undefined,
    });
    if (raw.endsWith("/api/me")) {
      return json({
        principal_id: "usr_1",
        default_workspace: "acme",
        workspaces: [{ name: "acme", tenant_id: "ten_1", role: "writer" }],
      });
    }
    if (raw.endsWith("/api/tenants")) return json({ tenant_id: "ten_1" });
    if (raw.endsWith("/agents")) return json({ agent_id: "agt_1" });
    if (raw.endsWith("/sessions")) return json({ session_id: "ses_1", agent_id: "agt_1" });
    if (raw.endsWith("/append-and-fetch")) return json({ event_id: "evt_fetch", seq: 1, recent: [] });
    if (raw.endsWith("/events") && init?.method === "GET") return json([]);
    if (raw.endsWith("/events")) return json({ event_id: "evt_1", seq: 0 });
    if (raw.includes("/replay")) {
      return json({
        events: [
          {
            tenant_id: "ten_1",
            agent_id: "agt_1",
            session_id: "ses_1",
            event_id: "evt_1",
            seq: 0,
            kind: "tool_result",
            payload: { tool_call_id: "call_1" },
            artifact_refs: [{ artifact_id: "art_1" }],
            source_refs: [{ event_id: "evt_call" }],
            credential_refs: [{ cred_ref: "vault://agent/acme/github" }],
            metadata_key_id: "tenant:ten_1:default",
            metadata_key_version: "v1",
          },
        ],
      });
    }
    if (raw.endsWith("/context")) return json({ events: [], records: [], skills: [] });
    if (raw.endsWith("/artifacts")) {
      return json({ artifact_id: "art_1", content_type: "text/plain", bytes: 4, sha256: "abcd" });
    }
    if (raw.endsWith("/records/search")) return json({ matches: [] });
    if (raw.endsWith("/records")) return json({ record_id: "rec_1" });
    if (raw.endsWith("/skills")) return json({ skill_id: "skl_1" });
    if (raw.endsWith("/skills/bind")) return json({ skill_id: "skl_1" });
    if (raw.includes("/watch")) return json([]);
    return json({});
  };

  const db = new AgentDB({ baseUrl: "http://x", fetch: mockFetch as typeof globalThis.fetch });
  const workspace = await db.workspace("acme");
  const agent = await workspace.agent("sales-agent");
  const session = await agent.session("ticket-42");

  const user = await session.appendUser("hello", { fetchLastN: 5 });
  const call = await session.toolCall("call_1", "sandbox.exec", { cmd: "python report.py" }, {
    sourceRefs: [eventRef(user)],
    credentialRefs: [{ cred_ref: "vault://agent/acme/github", provider: "github" }],
  });
  const artifact = await session.artifacts.put(new Blob(["blob"]), { contentType: "text/plain" });
  await session.toolResult("call_1", "Generated report.", {
    artifacts: [artifact],
    sourceRefs: [eventRef(call)],
  });
  const context = await session.context({ include: ["recent_events"] });
  const contextEvent = await session.contextPackBuilt("ctx_1", ["recent_events"], {
    selectedEventCount: Array.isArray(context.events) ? context.events.length : 0,
  });
  await session.modelMessage("done", { contextPackId: "ctx_1", sourceRefs: [eventRef(contextEvent)] });
  await session.agentStatus("recovered", { lastSeenSeq: 4 });
  const recovered = agent.sessionById(session.id);
  const replayed = await recovered.replay();
  await db.sessionById(session.id, { tenantId: workspace.id, agentId: agent.id }).fetch();
  await workspace.records.write({ namespace: ["mem"], text: "x" });
  await workspace.records.search({ namespace: ["mem"], text: "x" });
  const skill = await workspace.skills.register({ name: "tool", version: "1" });
  await agent.bindSkill(String(skill.skill_id));
  await workspace.watch({ scope: "agent", agentId: agent.id });

  assert.equal(user.ref().event_id, "evt_fetch");
  assert.deepEqual(appendResultRef(call), { event_id: "evt_1" });
  assert.deepEqual(artifactRef(artifact), {
    artifact_id: "art_1",
    content_type: "text/plain",
    bytes: 4,
    sha256: "abcd",
  });
  assert.deepEqual(replayed[0]?.artifactRefs, [{ artifact_id: "art_1" }]);
  assert.deepEqual(replayed[0]?.sourceRefs, [{ event_id: "evt_call" }]);
  assert.deepEqual(replayed[0]?.credentialRefs, [{ cred_ref: "vault://agent/acme/github" }]);
  assert.equal(replayed[0]?.metadataKeyId, "tenant:ten_1:default");
  assert.ok(
    calls.some((call) => {
      const body = call.body as Record<string, unknown> | undefined;
      return Array.isArray(body?.credential_refs)
        && (body.credential_refs[0] as Record<string, unknown> | undefined)?.cred_ref
          === "vault://agent/acme/github";
    })
  );
  assert.ok(calls.some((call) => call.url.includes("/api/tenants/ten_1/agents/agt_1/sessions")));
  assert.ok(calls.some((call) => call.url.includes("/api/tenants/ten_1/sessions/ses_1/events")));
});

test("friendly SDK can resolve workspace from auth user-info", async () => {
  const calls: { url: string; body?: unknown }[] = [];
  const mockFetch = async (url: string | URL, init?: RequestInit) => {
    const raw = String(url);
    calls.push({
      url: raw,
      body: typeof init?.body === "string" ? JSON.parse(init.body) : undefined,
    });
    if (raw.endsWith("/api/me")) {
      return json({
        principal_id: "usr_1",
        default_workspace: "prod",
        workspaces: [{ name: "prod", tenant_id: "ten_prod", role: "writer" }],
      });
    }
    if (raw.endsWith("/agents") && init?.method === "GET") return json({ agents: [] });
    if (raw.endsWith("/agents")) return json({ agent_id: "agt_1" });
    if (raw.endsWith("/sessions") && init?.method === "GET") return json({ sessions: [] });
    if (raw.endsWith("/sessions")) return json({ session_id: "ses_1", agent_id: "agt_1" });
    return json({});
  };

  const db = new AgentDB({ baseUrl: "http://x", token: "tok_1", fetch: mockFetch as typeof globalThis.fetch });
  const workspace = await db.workspace();
  const agent = await workspace.agent("support-agent");
  const session = await agent.session("ticket-42");

  assert.equal(workspace.id, "ten_prod");
  assert.equal(agent.id, "agt_1");
  assert.equal(session.id, "ses_1");
  assert.ok(!calls.some((call) => call.url.endsWith("/api/tenants") && call.body !== undefined));
});

test("bearer token is sent when configured", async () => {
  const authHeaders: string[] = [];
  const mockFetch = async (_url: string | URL, init?: RequestInit) => {
    const headers = new Headers(init?.headers);
    authHeaders.push(headers.get("authorization") ?? "");
    return json({
      principal_id: "usr_1",
      tenant_id: "ten_1",
    });
  };

  const c = new Client({ baseUrl: "http://x", token: "tok_1", fetch: mockFetch as typeof globalThis.fetch });
  await c.userInfo();

  assert.equal(authHeaders[0], "Bearer tok_1");
});

test("record search carries the last tenant commit sequence", async () => {
  const bodies: unknown[] = [];
  const mockFetch = async (url: string | URL, init?: RequestInit) => {
    const raw = String(url);
    bodies.push(typeof init?.body === "string" ? JSON.parse(init.body) : undefined);
    if (raw.endsWith("/records")) {
      return json({ record_id: "rec_1" }, 201, { "x-agentdb-record-commit-seq": "17" });
    }
    if (raw.endsWith("/records/search")) return json({ matches: [] });
    return json({});
  };

  const c = new Client({ baseUrl: "http://x", fetch: mockFetch as typeof globalThis.fetch });
  await c.writeRecord("ten_1", { namespace: ["mem"], text: "x" });
  await c.searchRecords("ten_1", { namespace: ["mem"], text: "x" });
  await c.searchRecords("ten_1", {
    namespace: ["mem"],
    text: "x",
    min_visible_record_commit_seq: 99,
  });

  assert.equal((bodies[1] as Record<string, unknown>).min_visible_record_commit_seq, 17);
  assert.equal((bodies[2] as Record<string, unknown>).min_visible_record_commit_seq, 99);
});

test("append requires an agent id", async () => {
  const c = new Client({ baseUrl: "http://x", fetch: (async () => json({})) as typeof globalThis.fetch });
  await assert.rejects(() => c.session("ten_1", "ses_1").append({ x: 1 }), /agentId/);
});

test("append rejects raw secret payload fields before request", async () => {
  const calls: unknown[] = [];
  const mockFetch = async (_url: string | URL, init?: RequestInit) => {
    calls.push(typeof init?.body === "string" ? JSON.parse(init.body) : undefined);
    return json({ event_id: "evt_1", seq: 0 });
  };

  const c = new Client({ baseUrl: "http://x", fetch: mockFetch as typeof globalThis.fetch });
  const session = c.session("ten_1", "ses_1", { agentId: "agt_1" });

  await assert.rejects(
    () => session.append({ input: [{ access_token: "raw" }] }),
    /input\[0\]\.access_token/
  );
  await session.append(
    { input: [{ repo: "agentdb" }] },
    { credentialRefs: [{ cred_ref: "vault://agent/acme/github" }] }
  );

  assert.equal(calls.length, 1);
  assert.deepEqual((calls[0] as Record<string, unknown>).credential_refs, [
    { cred_ref: "vault://agent/acme/github" },
  ]);
});

test("constructor rejects token + getToken combined", () => {
  assert.throws(
    () => new Client({ baseUrl: "http://x", token: "x", getToken: () => "y" }),
    /token.*getToken/
  );
});
