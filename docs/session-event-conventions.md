# Session Event Conventions

Status: draft

Date: 2026-04-30

## 1. Purpose

AgentDB stores ordered session events with arbitrary `kind` and JSON `payload`.
That extensibility is intentional: AgentDB should not become a closed workflow
engine or model runtime.

Agent-platform integrations still need a shared vocabulary for the common
events that every harness, tool adapter, dashboard, evaluator, and replay viewer
needs to understand. This document defines advisory conventions for those
events.

These conventions are:

- stable enough for SDK examples, dashboards, and framework adapters,
- schema-versioned and extensible,
- not a closed enum enforced by the runtime,
- centered on durable session truth rather than model-specific prompt format.

The Python and TypeScript SDKs export `SessionEventKind`, payload builders, and
`context_source_refs` / `contextSourceRefs` helpers for these conventions. Those
helpers are client-side conveniences for adapters; the server still accepts
custom `kind` values and adapter-specific payload fields.

## 2. Envelope Rules

All conventions use the existing event envelope:

```json
{
  "tenant_id": "ten_...",
  "agent_id": "agt_...",
  "session_id": "ses_...",
  "event_id": "evt_...",
  "seq": 42,
  "kind": "tool_result",
  "schema_version": "2026-04-28",
  "payload": {},
  "artifact_refs": [],
  "source_refs": [],
  "policy_version": 1
}
```

Rules:

- `kind` chooses the convention.
- `payload` carries convention-specific fields.
- `artifact_refs` points to large raw outputs, generated files, screenshots,
  logs, traces, or binary payloads. Do not silently inline large hands output in
  `payload`.
- `source_refs` points to the session events, artifacts, records, or skills that
  caused or informed this event.
- `schema_version` is the runtime envelope schema version. Payloads that need
  their own independent version should include `payload.schema_version`.
- Unknown payload fields must be ignored by readers.
- Missing optional payload fields must not make replay impossible.

## 3. Reference Rules

Use references to keep the durable timeline navigable.

### 3.1 Event causality

When an event directly responds to another event, include that event in
`source_refs`.

Examples:

- `tool_result` references the corresponding `tool_call`.
- `tool_error` references the corresponding `tool_call`.
- `model_message` references the `context_pack_built` event or the input events
  used for the model call.
- `approval_result` references `approval_requested`.

```json
{
  "kind": "tool_result",
  "source_refs": [{ "event_id": "evt_tool_call" }],
  "payload": {
    "tool_call_id": "call_123",
    "status": "ok",
    "summary": "Generated report"
  }
}
```

### 3.2 Large output

Put large output bytes in artifacts and reference them from the event.

```json
{
  "kind": "tool_result",
  "artifact_refs": [
    {
      "artifact_id": "art_report",
      "content_type": "text/plain",
      "bytes": 3481230,
      "sha256": "..."
    }
  ],
  "payload": {
    "tool_call_id": "call_123",
    "status": "ok",
    "summary": "Full report is attached as an artifact."
  }
}
```

### 3.3 Derived context

When an event contains a summary, embedding source, extracted fact, memory
record, or model context derived from earlier material, preserve source refs.

```json
{
  "kind": "context_pack_built",
  "source_refs": [
    { "event_id": "evt_user" },
    { "event_id": "evt_tool_result" },
    { "record_id": "rec_relevant_fact" }
  ],
  "payload": {
    "context_pack_id": "ctx_2026_04_30_001",
    "max_events": 20,
    "max_bytes": 1000000
  }
}
```

## 4. Core Event Kinds

### 4.1 `user_message`

Human or upstream platform input.

Required payload fields:

| Field | Type | Notes |
|---|---|---|
| `text` | string | User-visible text, or a short summary when raw content is in artifacts. |

Optional payload fields:

| Field | Type | Notes |
|---|---|---|
| `role` | string | Usually `user`; included for chat-framework mapping. |
| `user_id` | string | Platform user or actor identifier. |
| `channel` | string | UI, API, Slack, webhook, scheduler, etc. |
| `metadata` | object | Adapter-specific details. |

Example:

```json
{
  "kind": "user_message",
  "payload": {
    "role": "user",
    "text": "Summarize the sandbox output.",
    "channel": "web"
  }
}
```

### 4.2 `model_message`

Model-visible assistant output.

Required payload fields:

| Field | Type | Notes |
|---|---|---|
| `text` | string | Assistant text or a summary with artifacts for large output. |

Optional payload fields:

| Field | Type | Notes |
|---|---|---|
| `role` | string | Usually `assistant`. |
| `model` | string | Model identifier if known. |
| `provider` | string | Model provider if useful for audit. |
| `finish_reason` | string | Stop reason from the provider. |
| `usage` | object | Token or cost counters. |
| `context_pack_id` | string | Context pack used for this model call. |
| `metadata` | object | Adapter-specific details. |

Example:

```json
{
  "kind": "model_message",
  "source_refs": [{ "event_id": "evt_context_pack" }],
  "payload": {
    "role": "assistant",
    "text": "The report has two generated lines.",
    "model": "example-model",
    "context_pack_id": "ctx_2026_04_30_001"
  }
}
```

### 4.3 `model_thought`

Optional private planner output when a platform chooses to persist it.

This event may contain sensitive reasoning or planning material. Platforms may
disable it, redact it, encrypt it, or keep only summaries.

Required payload fields:

| Field | Type | Notes |
|---|---|---|
| `summary` | string | Short durable summary. |

Optional payload fields:

| Field | Type | Notes |
|---|---|---|
| `text` | string | Full private/planner text when policy allows. |
| `visibility` | string | Suggested values: `private`, `audit`, `debug`. |
| `model` | string | Model identifier if known. |
| `metadata` | object | Adapter-specific details. |

Example:

```json
{
  "kind": "model_thought",
  "payload": {
    "summary": "Need to inspect the generated report before answering.",
    "visibility": "audit"
  }
}
```

### 4.4 `tool_call`

Requested hand/tool/sandbox operation.

Required payload fields:

| Field | Type | Notes |
|---|---|---|
| `tool_call_id` | string | Stable ID for pairing with result/error events. |
| `tool` | string | Tool, MCP method, sandbox action, or external capability name. |
| `input` | object | Structured input. Redact or artifact-reference large/sensitive input. |

Optional payload fields:

| Field | Type | Notes |
|---|---|---|
| `hand_id` | string | Sandbox, MCP server, resource, or execution environment. |
| `timeout_ms` | number | Requested timeout. |
| `idempotency_key` | string | Tool-side idempotency key when distinct from AgentDB event idempotency. |
| `metadata` | object | Adapter-specific details. |

Example:

```json
{
  "kind": "tool_call",
  "payload": {
    "tool_call_id": "call_123",
    "tool": "sandbox.exec",
    "hand_id": "sandbox_primary",
    "input": { "cmd": "python report.py" },
    "timeout_ms": 30000
  }
}
```

### 4.5 `tool_result`

Successful hand/tool/sandbox operation result.

Required payload fields:

| Field | Type | Notes |
|---|---|---|
| `tool_call_id` | string | Must match the originating `tool_call`. |
| `status` | string | Use `ok` for successful completion. |
| `summary` | string | Short human/model-readable result summary. |

Optional payload fields:

| Field | Type | Notes |
|---|---|---|
| `output` | object | Small structured output. Large output belongs in artifacts. |
| `duration_ms` | number | Execution duration. |
| `exit_code` | number | Process exit code when relevant. |
| `metadata` | object | Adapter-specific details. |

Reference rules:

- `source_refs` should include the originating `tool_call` event.
- `artifact_refs` should include large stdout/stderr/files/logs/screenshots.

Example:

```json
{
  "kind": "tool_result",
  "source_refs": [{ "event_id": "evt_tool_call" }],
  "artifact_refs": [{ "artifact_id": "art_stdout", "content_type": "text/plain" }],
  "payload": {
    "tool_call_id": "call_123",
    "status": "ok",
    "summary": "Generated report with two lines.",
    "duration_ms": 412
  }
}
```

### 4.6 `tool_error`

Failed hand/tool/sandbox operation.

Required payload fields:

| Field | Type | Notes |
|---|---|---|
| `tool_call_id` | string | Must match the originating `tool_call`. |
| `error` | object | Structured error. |
| `retryable` | boolean | Whether retry may succeed without changing input. |
| `summary` | string | Short human/model-readable failure summary. |

Recommended `payload.error` fields:

| Field | Type | Notes |
|---|---|---|
| `code` | string | Stable adapter/tool error code. |
| `message` | string | Safe diagnostic message. |

Optional payload fields:

| Field | Type | Notes |
|---|---|---|
| `duration_ms` | number | Execution duration. |
| `exit_code` | number | Process exit code when relevant. |
| `metadata` | object | Adapter-specific details. |

Example:

```json
{
  "kind": "tool_error",
  "source_refs": [{ "event_id": "evt_tool_call" }],
  "payload": {
    "tool_call_id": "call_123",
    "retryable": true,
    "summary": "Sandbox timed out while generating the report.",
    "error": {
      "code": "SANDBOX_TIMEOUT",
      "message": "execution exceeded 30000 ms"
    }
  }
}
```

### 4.7 `artifact_created`

Generated or uploaded artifact metadata event.

The artifact object itself is stored through the artifact API. This event records
why it exists in the session timeline.

Required payload fields:

| Field | Type | Notes |
|---|---|---|
| `artifact_id` | string | Created artifact ID. |
| `summary` | string | Why this artifact matters. |

Optional payload fields:

| Field | Type | Notes |
|---|---|---|
| `name` | string | File/display name. |
| `content_type` | string | MIME type. |
| `bytes` | number | Size in bytes. |
| `sha256` | string | Integrity hash. |
| `purpose` | string | stdout, screenshot, patch, generated_file, trace, etc. |
| `metadata` | object | Adapter-specific details. |

Reference rules:

- Include the artifact in `artifact_refs`.
- Include the producing event in `source_refs` when known.

Example:

```json
{
  "kind": "artifact_created",
  "source_refs": [{ "event_id": "evt_tool_call" }],
  "artifact_refs": [{ "artifact_id": "art_report", "content_type": "text/plain" }],
  "payload": {
    "artifact_id": "art_report",
    "name": "report.txt",
    "summary": "Report generated by sandbox.exec.",
    "purpose": "generated_file"
  }
}
```

### 4.8 `approval_requested`

Human or policy approval checkpoint.

Required payload fields:

| Field | Type | Notes |
|---|---|---|
| `approval_id` | string | Stable approval request ID. |
| `action` | string | Proposed action. |
| `summary` | string | Human-readable approval request. |

Optional payload fields:

| Field | Type | Notes |
|---|---|---|
| `risk` | string | Suggested values: `low`, `medium`, `high`. |
| `expires_at_ms` | number | Optional approval deadline. |
| `metadata` | object | Adapter-specific details. |

Example:

```json
{
  "kind": "approval_requested",
  "payload": {
    "approval_id": "appr_123",
    "action": "send_email",
    "summary": "Send the drafted follow-up email to the customer.",
    "risk": "medium"
  }
}
```

### 4.9 `approval_result`

Decision for a prior approval request.

Required payload fields:

| Field | Type | Notes |
|---|---|---|
| `approval_id` | string | Matches `approval_requested`. |
| `decision` | string | Suggested values: `approved`, `denied`, `expired`, `canceled`. |
| `actor` | object | Human, policy, or system actor that made the decision. |

Optional payload fields:

| Field | Type | Notes |
|---|---|---|
| `reason` | string | Explanation for denial or override. |
| `metadata` | object | Adapter-specific details. |

Reference rules:

- `source_refs` should include the `approval_requested` event.

Example:

```json
{
  "kind": "approval_result",
  "source_refs": [{ "event_id": "evt_approval_requested" }],
  "payload": {
    "approval_id": "appr_123",
    "decision": "approved",
    "actor": { "type": "user", "id": "usr_456" }
  }
}
```

### 4.10 `context_pack_built`

Optional trace of what durable state was selected for model context.

This event should not replace the context itself as source of truth. It records
the derived view a harness used for a specific model call.

Required payload fields:

| Field | Type | Notes |
|---|---|---|
| `context_pack_id` | string | Harness-generated context pack ID. |
| `include` | array | Included source classes, such as `recent_events`, `records`, `skills`. |

Optional payload fields:

| Field | Type | Notes |
|---|---|---|
| `max_events` | number | Event budget requested. |
| `max_bytes` | number | Byte budget requested. |
| `token_budget` | number | Token budget requested by harness. |
| `selected_event_count` | number | Number of events included. |
| `selected_record_count` | number | Number of records included. |
| `prompt_cache_key` | string | Harness/provider cache key if useful. |
| `metadata` | object | Adapter-specific details. |

Reference rules:

- `source_refs` should include representative events, records, artifacts, or
  skills that shaped the context pack.
- If the context pack body is large, store it as an artifact and reference it.

Example:

```json
{
  "kind": "context_pack_built",
  "source_refs": [
    { "event_id": "evt_user" },
    { "record_id": "rec_memory" },
    { "skill_id": "skl_tool_manifest" }
  ],
  "payload": {
    "context_pack_id": "ctx_2026_04_30_001",
    "include": ["recent_events", "records", "skills"],
    "max_events": 20,
    "max_bytes": 1000000,
    "selected_event_count": 12
  }
}
```

### 4.11 `agent_status`

Lifecycle/status marker for the agent harness.

Required payload fields:

| Field | Type | Notes |
|---|---|---|
| `status` | string | Suggested values: `woke`, `running`, `sleeping`, `paused`, `blocked`, `retrying`, `recovered`, `done`, `failed`. |

Optional payload fields:

| Field | Type | Notes |
|---|---|---|
| `summary` | string | Short status explanation. |
| `last_seen_seq` | number | Last sequence processed before wake/recovery. |
| `reason` | string | Failure/block/sleep reason. |
| `metadata` | object | Adapter-specific details. |

Example:

```json
{
  "kind": "agent_status",
  "payload": {
    "status": "recovered",
    "last_seen_seq": 41,
    "summary": "Harness restarted and resumed from durable session history."
  }
}
```

## 5. Adapter Mapping Notes

### 5.1 OpenAI Agents SDK

Map session items to `user_message`, `model_message`, `tool_call`,
`tool_result`, and `tool_error`. Map interruptions or approvals to
`approval_requested` and `approval_result`. Preserve provider trace/span IDs in
`payload.metadata`.

### 5.2 LangGraph

Map checkpoint writes and state transitions to session events or searchable
records depending on replay needs. Tool invocations should use `tool_call` and
`tool_result`/`tool_error`; large state snapshots should use artifacts.

### 5.3 Vercel AI SDK

Map chat messages to `user_message` and `model_message`. Map tool parts to
`tool_call`, `tool_result`, and `tool_error`. Map resumable stream state to
`agent_status` and watch/subscription cursors.

### 5.4 MCP

Map MCP tool invocations to `tool_call`; results and errors to `tool_result` and
`tool_error`; resources or prompts used by the harness to `source_refs`,
`artifact_refs`, searchable records, or skills depending on durability and
replay needs.

## 6. Validation Checklist

An adapter using these conventions should satisfy:

- It never relies on the model context window as the only durable record.
- Every tool result/error can be paired with its originating tool call.
- Large tool outputs are stored as artifacts and referenced from events.
- Model outputs can be traced to the context pack or input events that produced
  them.
- Human/policy approvals have explicit request and result events.
- Resume/replay can reconstruct the sequence of user input, model output, tool
  calls, tool results, artifacts, approvals, and status transitions.
- Unknown fields do not break readers.
