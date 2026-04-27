import { test } from "node:test";
import assert from "node:assert/strict";
import { hireContractor } from "../../src/tools/hire_contractor.ts";
import { Memory } from "../../src/memory.ts";
import { Channels } from "../../src/channels.ts";

class FakeSession {
  id: string;
  appendCalls: any[] = [];
  forkCalls: any[] = [];
  constructor(id: string) {
    this.id = id;
  }
  async append(event: any, opts: any) {
    this.appendCalls.push({ event, opts });
    return { seq: 1, recent: [] };
  }
  async fork(childId: string) {
    this.forkCalls.push(childId);
    return new FakeSession(childId);
  }
}

class FakeClient {
  sessions: Map<string, FakeSession> = new Map();
  broadcasts: any[] = [];
  subscribeCalls: any[] = [];

  session(id: string, _opts?: any) {
    if (!this.sessions.has(id)) this.sessions.set(id, new FakeSession(id));
    return this.sessions.get(id);
  }

  async broadcast(args: any) {
    this.broadcasts.push(args);
  }

  async *subscribeChannel(args: any): AsyncIterable<any> {
    this.subscribeCalls.push(args);
    // Empty stream: caller can break out immediately.
  }
}

test("hire_contractor forks the parent session and records contractor_hired", async () => {
  const fc = new FakeClient();
  const memory = new Memory(fc as any, { tenant: "pi-company", runId: "r-1" }, "engineer");
  const channels = new Channels(fc as any, { tenant: "pi-company", runId: "r-1" });
  const ctx = {
    memory,
    channels,
    client: fc as any,
    run: { tenant: "pi-company", runId: "r-1" },
    role: "engineer" as const,
  };

  const result = await hireContractor.handler(
    { task_brief: "explore approach B for indexing" },
    ctx,
  );

  // The parent session was forked.
  const parentSession = fc.sessions.get("r-1:engineer");
  assert.ok(parentSession, "parent session should exist");
  assert.equal(parentSession!.forkCalls.length, 1, "expected exactly one fork");
  const childId = parentSession!.forkCalls[0];
  assert.ok(
    childId.startsWith("r-1:engineer:contractor-"),
    `unexpected child session ID: ${childId}`,
  );

  // A contractor_hired event was recorded on the parent session.
  const hiredEvents = parentSession!.appendCalls.filter(
    (c) => c.event.kind === "contractor_hired",
  );
  assert.equal(hiredEvents.length, 1);
  assert.equal(hiredEvents[0].event.task_brief, "explore approach B for indexing");
  assert.ok(
    hiredEvents[0].event.childSession.startsWith("r-1:engineer:contractor-"),
  );

  // The handler returned a string-shaped result that mentions "contractor".
  assert.ok(result, "handler should return a result");
  const resultText = (result as any).text ?? String(result);
  assert.ok(
    resultText.includes("contractor"),
    `expected result to mention contractor: ${resultText}`,
  );
});

test("hire_contractor child session ID has run-scoped naming", async () => {
  const fc = new FakeClient();
  const memory = new Memory(
    fc as any,
    { tenant: "pi-company", runId: "r-1" },
    "marketing",
  );
  const channels = new Channels(fc as any, { tenant: "pi-company", runId: "r-1" });
  const ctx = {
    memory,
    channels,
    client: fc as any,
    run: { tenant: "pi-company", runId: "r-1" },
    role: "marketing" as const,
  };
  await hireContractor.handler({ task_brief: "punchy taglines" }, ctx);
  const parent = fc.sessions.get("r-1:marketing");
  assert.ok(parent);
  const childId = parent!.forkCalls[0];
  assert.ok(
    /^r-1:marketing:contractor-\d+$/.test(childId),
    `unexpected child format: ${childId}`,
  );
});
