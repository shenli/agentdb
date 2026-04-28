import { test } from "node:test";
import assert from "node:assert/strict";
import { wrapUp } from "../../src/tools/wrap_up.ts";

class FakeSession {
  appended: any[] = [];
  async append(event: any, _opts: any) {
    this.appended.push(event);
    return { seq: this.appended.length, recent: [] };
  }
}
class FakeClient {
  sessions = new Map<string, FakeSession>();
  session(id: string, _opts?: any) {
    if (!this.sessions.has(id)) this.sessions.set(id, new FakeSession());
    return this.sessions.get(id)!;
  }
}

function makeCtx(client: FakeClient) {
  return {
    client: client as any,
    run: { runId: "r1" },
    role: "ceo" as any,
    memory: { recordEvent: async () => {}, sessionName: "r1:ceo" } as any,
    channels: null as any,
  };
}

test("wrap_up appends run_ended to {runId}:_meta with cause='wrap_up'", async () => {
  const client = new FakeClient();
  const result = await wrapUp.handler(
    { reason: "goal achieved", summary: "shipped MVP, landed 3 pilots" },
    makeCtx(client),
  );
  const metaSess = client.sessions.get("r1:_meta");
  assert.ok(metaSess);
  const ev = metaSess.appended.find((e) => e.kind === "run_ended");
  assert.ok(ev);
  assert.equal(ev.cause, "wrap_up");
  assert.equal(ev.reason, "goal achieved");
  assert.ok(ev.summary.includes("shipped"));
  assert.equal(result.terminate, true);
});
