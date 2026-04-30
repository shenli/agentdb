import test from "node:test";
import assert from "node:assert/strict";

import { Client } from "../src/index.js";

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
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
    if (raw.includes("/replay")) return json({ events: [] });
    if (raw.endsWith("/context")) return json({ events: [], records: [], skills: [] });
    if (raw.includes("/watch")) return json([]);
    if (raw.endsWith("/artifacts")) return json({ artifact_id: "art_1" });
    if (raw.endsWith("/records/search")) return json({ records: [] });
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
  assert.ok(calls.some((url) => url.includes("/context")));
  assert.ok(calls.some((url) => url.includes("/watch")));
});

test("append requires an agent id", async () => {
  const c = new Client({ baseUrl: "http://x", fetch: (async () => json({})) as typeof globalThis.fetch });
  await assert.rejects(() => c.session("ten_1", "ses_1").append({ x: 1 }), /agentId/);
});

test("constructor rejects token + getToken combined", () => {
  assert.throws(
    () => new Client({ baseUrl: "http://x", token: "x", getToken: () => "y" }),
    /token.*getToken/
  );
});
