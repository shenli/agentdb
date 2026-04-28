import { test } from "node:test";
import assert from "node:assert/strict";
import { updateAgentConfig } from "../../src/tools/update_agent_config.ts";

class FakeSession {
  appendCalls: any[] = [];
  async append(event: any, opts: any) {
    this.appendCalls.push({ event, opts });
    return { seq: 1, recent: [] };
  }
}

class FakeClient {
  sessions: Map<string, FakeSession> = new Map();
  session(id: string, _opts?: any) {
    if (!this.sessions.has(id)) this.sessions.set(id, new FakeSession());
    return this.sessions.get(id);
  }
}

test("update_agent_config appends an agent_override event to _config:agents", async () => {
  const fc = new FakeClient();
  const ctx = {
    memory: null as any,
    channels: null as any,
    client: fc as any,
    run: { runId: "r-1" },
    role: "ceo" as const,
  };

  const result = await updateAgentConfig.handler(
    {
      role: "engineer",
      patch: { systemPromptSuffix: "be terse" },
    },
    ctx,
  );

  // Verify the config session received the override event.
  const configSession = fc.sessions.get("_config:agents");
  assert.ok(configSession, "config session should exist after the update");
  assert.equal(configSession.appendCalls.length, 1);
  const ev = configSession.appendCalls[0].event;
  assert.equal(ev.kind, "agent_override");
  assert.equal(ev.role, "engineer");
  assert.deepEqual(ev.patch, { systemPromptSuffix: "be terse" });

  // Result is the standard tool result shape.
  const resultText = (result as any).text ?? String(result);
  assert.ok(resultText.includes("engineer"), `expected result to mention role: ${resultText}`);
});

test("update_agent_config supports multiple field patches", async () => {
  const fc = new FakeClient();
  const ctx = {
    memory: null as any,
    channels: null as any,
    client: fc as any,
    run: { runId: "r-1" },
    role: "ceo" as const,
  };

  await updateAgentConfig.handler(
    {
      role: "marketing",
      patch: { systemPromptSuffix: "be punchy", model: "gpt-4o-mini" },
    },
    ctx,
  );

  const configSession = fc.sessions.get("_config:agents")!;
  const ev = configSession.appendCalls[0].event;
  assert.deepEqual(ev.patch, { systemPromptSuffix: "be punchy", model: "gpt-4o-mini" });
});
