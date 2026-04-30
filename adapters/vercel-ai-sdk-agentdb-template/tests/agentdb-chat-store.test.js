import test from "node:test";
import assert from "node:assert/strict";
import { AgentDBChatStore, VERCEL_AI_AGENTDB_KINDS } from "../src/agentdb-chat-store.js";

class FakeSession {
  constructor() {
    this.events = [];
  }

  async append(payload, options) {
    this.events.push({ kind: options.kind, payload, options });
    return { eventId: "evt_1", seq: this.events.length - 1, recent: [] };
  }

  async replay() {
    return this.events.map((event, index) => ({
      eventId: `evt_${index}`,
      seq: index,
      kind: event.kind,
      payload: event.payload,
    }));
  }
}

class FakeClient {
  constructor() {
    this.fakeSession = new FakeSession();
    this.records = [];
  }

  session(tenantId, sessionId, options) {
    this.sessionArgs = { tenantId, sessionId, options };
    return this.fakeSession;
  }

  async writeRecord(tenantId, body) {
    this.records.push({ tenantId, body });
    return { record_id: "rec_1", ...body };
  }

  async searchRecords(_tenantId, _body) {
    return this.records.map((record) => ({ metadata: record.body.metadata }));
  }
}

test("appendMessage stores a Vercel AI message event", async () => {
  const client = new FakeClient();
  const store = new AgentDBChatStore(client, {
    tenantId: "ten_1",
    agentId: "agt_1",
    sessionId: "ses_1",
  });

  await store.appendMessage({ id: "m1", role: "user", content: "hello" });

  assert.equal(client.sessionArgs.tenantId, "ten_1");
  assert.equal(client.fakeSession.events[0].kind, VERCEL_AI_AGENTDB_KINDS.message);
  assert.deepEqual(await store.loadMessages(), [{ id: "m1", role: "user", content: "hello" }]);
});

test("saveStreamState stores resumable stream state as a searchable record", async () => {
  const client = new FakeClient();
  const store = new AgentDBChatStore(client, {
    tenantId: "ten_1",
    agentId: "agt_1",
    sessionId: "ses_1",
  });

  await store.saveStreamState("stream-1", { status: "open" });

  assert.deepEqual(client.records[0].body.namespace, VERCEL_AI_AGENTDB_KINDS.streamStateNamespace);
  assert.equal(await store.loadStreamState("stream-1").then((state) => state.status), "open");
});
