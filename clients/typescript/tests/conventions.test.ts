import test from "node:test";
import assert from "node:assert/strict";

import {
  SessionEventKind,
  agentStatus,
  approvalRequested,
  approvalResult,
  artifactCreated,
  contextPackBuilt,
  contextSourceRefs,
  modelMessage,
  modelThought,
  toolCall,
  toolError,
  toolResult,
  userMessage,
} from "../src/index.js";

test("session event kind values match conventions", () => {
  assert.equal(SessionEventKind.UserMessage, "user_message");
  assert.equal(SessionEventKind.ModelMessage, "model_message");
  assert.equal(SessionEventKind.ToolCall, "tool_call");
  assert.equal(SessionEventKind.ToolResult, "tool_result");
  assert.equal(SessionEventKind.AgentStatus, "agent_status");
});

test("message payload builders compact optional fields", () => {
  assert.deepEqual(userMessage("hello"), { role: "user", text: "hello" });
  assert.deepEqual(modelMessage("hi", { model: "model-a" }), {
    role: "assistant",
    text: "hi",
    model: "model-a",
  });
  assert.deepEqual(modelThought("inspect output", { visibility: "audit" }), {
    summary: "inspect output",
    visibility: "audit",
  });
});

test("tool payload builders match pairing contract", () => {
  assert.deepEqual(toolCall("call_1", "sandbox.exec", { cmd: "pytest" }), {
    tool_call_id: "call_1",
    tool: "sandbox.exec",
    input: { cmd: "pytest" },
  });
  assert.deepEqual(toolResult("call_1", "tests passed", { durationMs: 12 }), {
    tool_call_id: "call_1",
    status: "ok",
    summary: "tests passed",
    duration_ms: 12,
  });
  assert.deepEqual(
    toolError("call_1", "tests timed out", {
      code: "TIMEOUT",
      message: "deadline exceeded",
      retryable: true,
    }),
    {
      tool_call_id: "call_1",
      retryable: true,
      summary: "tests timed out",
      error: { code: "TIMEOUT", message: "deadline exceeded" },
    }
  );
});

test("artifact approval context and status builders match conventions", () => {
  assert.deepEqual(artifactCreated("art_1", "stdout", { contentType: "text/plain" }), {
    artifact_id: "art_1",
    summary: "stdout",
    content_type: "text/plain",
  });
  assert.deepEqual(approvalRequested("appr_1", "send_email", "Send email"), {
    approval_id: "appr_1",
    action: "send_email",
    summary: "Send email",
  });
  assert.deepEqual(approvalResult("appr_1", "approved", { type: "user", id: "u1" }), {
    approval_id: "appr_1",
    decision: "approved",
    actor: { type: "user", id: "u1" },
  });
  assert.deepEqual(
    contextPackBuilt("ctx_1", ["recent_events", "records"], {
      maxEvents: 20,
      selectedEventCount: 3,
    }),
    {
      context_pack_id: "ctx_1",
      include: ["recent_events", "records"],
      max_events: 20,
      selected_event_count: 3,
    }
  );
  assert.deepEqual(agentStatus("recovered", { lastSeenSeq: 41 }), {
    status: "recovered",
    last_seen_seq: 41,
  });
});

test("contextSourceRefs extracts events records and skills", () => {
  const context = {
    events: [{ event_id: "evt_1" }, { eventId: "evt_2" }, { seq: 3 }],
    records: [{ record_id: "rec_1" }],
    skills: [{ skill_id: "skl_1" }],
  };
  assert.deepEqual(contextSourceRefs(context), [
    { event_id: "evt_1" },
    { event_id: "evt_2" },
    { record_id: "rec_1" },
    { skill_id: "skl_1" },
  ]);
  assert.deepEqual(contextSourceRefs(context, { maxRefs: 2 }), [
    { event_id: "evt_1" },
    { event_id: "evt_2" },
  ]);
});
