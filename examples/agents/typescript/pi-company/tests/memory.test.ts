import { test } from "node:test";
import assert from "node:assert/strict";
import { Memory } from "../src/memory.ts";

interface AppendCall {
  sessionId: string;
  event: any;
  fetchLastN: number | undefined;
}

interface ForkCall {
  parentId: string;
  childId: string;
}

class FakeSession {
  readonly sessionId: string;
  private readonly tracker: FakeClient;

  constructor(sessionId: string, tracker: FakeClient) {
    this.sessionId = sessionId;
    this.tracker = tracker;
  }

  async append(event: unknown, opts: { fetchLastN?: number } = {}): Promise<{ seq: number; recent: any[] }> {
    this.tracker.appendCalls.push({
      sessionId: this.sessionId,
      event,
      fetchLastN: opts.fetchLastN,
    });
    return { seq: this.tracker.appendCalls.length, recent: this.tracker.appendResponse };
  }

  async fork(childSessionId: string): Promise<FakeSession> {
    this.tracker.forkCalls.push({
      parentId: this.sessionId,
      childId: childSessionId,
    });
    return new FakeSession(childSessionId, this.tracker);
  }
}

class FakeClient {
  appendCalls: AppendCall[] = [];
  forkCalls: ForkCall[] = [];
  appendResponse: any[] = [];

  session(sessionId: string, _opts?: unknown): FakeSession {
    return new FakeSession(sessionId, this);
  }
}

test("Memory.sessionName composes runId and role", () => {
  const fc = new FakeClient();
  const mem = new Memory(fc as any, { runId: "r-1" }, "engineer");
  assert.equal(mem.sessionName, "r-1:engineer");
});

test("Memory.recordEvent appends an event with the given kind", async () => {
  const fc = new FakeClient();
  const mem = new Memory(fc as any, { runId: "r-1" }, "engineer");
  await mem.recordEvent({ kind: "directive", text: "build it" });
  assert.equal(fc.appendCalls.length, 1);
  const call = fc.appendCalls[0]!;
  assert.equal(call.sessionId, "r-1:engineer");
  assert.deepEqual(call.event, { kind: "directive", text: "build it" });
  assert.equal(call.fetchLastN, 0);
});

test("Memory.forkSession returns a child session ID with run-scoped naming", async () => {
  const fc = new FakeClient();
  const mem = new Memory(fc as any, { runId: "r-1" }, "engineer");
  const child = await mem.forkSession("contractor-1");
  assert.equal(child, "r-1:engineer:contractor-1");
  assert.equal(fc.forkCalls.length, 1);
  assert.equal(fc.forkCalls[0]!.parentId, "r-1:engineer");
  assert.equal(fc.forkCalls[0]!.childId, "r-1:engineer:contractor-1");
});
