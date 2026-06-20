import test from "node:test";
import assert from "node:assert/strict";

import {
  SessionEventKind,
  ADAPTER_READINESS_CHECKS,
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
  validateAdapterEvents,
} from "../src/index.js";

test("session event kind values match conventions", () => {
  assert.ok(ADAPTER_READINESS_CHECKS.includes("tool_call_pairing"));
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
  const sanitized = toolResult("call_1", "Authorization: Bearer abcdefghijklmnopqrstuvwxyz12345", {
    output: {
      stdout:
        "password=hunter2 sk-abcdefghijklmnopqrstuvwxyz ghp_abcdefghijklmnopqrstuvwxyz123456 AKIA1234567890ABCDEF",
    },
  });
  assert.equal(sanitized.summary, "Authorization: Bearer [REDACTED]");
  assert.equal(
    (sanitized.output as Record<string, unknown>).stdout,
    "password=[REDACTED] [REDACTED] [REDACTED] [REDACTED]"
  );
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

test("validateAdapterEvents accepts ready sequences", () => {
  const events = [
    {
      event_id: "evt_1",
      seq: 0,
      kind: "tool_call",
      payload: { tool_call_id: "call_1" },
    },
    {
      event_id: "evt_2",
      seq: 1,
      kind: "tool_result",
      payload: { tool_call_id: "call_1", summary: "ok" },
      source_refs: [{ event_id: "evt_1" }],
      artifact_refs: [{ artifact_id: "art_1" }],
    },
    {
      event_id: "evt_3",
      seq: 2,
      kind: "context_pack_built",
      payload: { context_pack_id: "ctx_1" },
      source_refs: [{ event_id: "evt_1" }],
    },
    {
      event_id: "evt_4",
      seq: 3,
      kind: "model_message",
      payload: { text: "done", context_pack_id: "ctx_1" },
      source_refs: [{ event_id: "evt_3" }],
    },
    {
      event_id: "evt_5",
      seq: 4,
      kind: "agent_status",
      payload: { status: "recovered", last_seen_seq: 3 },
    },
  ];
  assert.deepEqual(validateAdapterEvents(events), []);
});

test("validateAdapterEvents reports common adapter gaps", () => {
  const warnings = validateAdapterEvents([
    {
      kind: "tool_call",
      payload: { tool_call_id: "call_1" },
    },
    {
      kind: "model_message",
      payload: { text: "done", context_pack_id: "ctx_1" },
    },
  ]);
  assert.ok(warnings.includes("tool_call_without_result_or_error:call_1"));
  assert.ok(warnings.includes("model_message_with_context_pack_without_source_refs"));
  assert.ok(warnings.includes("model_message_without_context_pack_built"));
  assert.ok(warnings.includes("missing_recovered_agent_status"));
  assert.ok(warnings.includes("events_missing_agentdb_seq"));
});
