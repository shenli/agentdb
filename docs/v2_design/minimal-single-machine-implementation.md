# Minimal Single-Machine V2 Implementation Guide

Status: draft

Date: 2026-04-28

This document is the implementation handoff for AI coding agents. It narrows the
V2 design into a local, single-machine prototype that validates AgentDB's runtime
primitives without implementing distributed scale or a managed service.

## 1. Goal

Build a minimal AgentDB V2 runtime database that can run with:

```bash
agentdb serve --dev --v2
```

The prototype should prove these runtime primitives:

- tenant-scoped runtime objects,
- ordered session event logs,
- append-and-fetch for agent loops,
- artifact refs for large payloads,
- scoped searchable records for memory and knowledge use cases,
- skill version pinning,
- simple policy enforcement,
- replay, fork, and durable subscribe semantics,
- enough adapter-facing primitives to support one framework proof adapter.

Prefer correctness and clear boundaries over broad feature coverage.

## 2. Non-Goals

- distributed consensus,
- multi-node routing,
- cross-region writes,
- managed-service control plane,
- MongoDB-compatible V2 primary API,
- general-purpose document database behavior,
- built-in LLM calls,
- built-in memory extraction, scoring, or forgetting,
- tool execution side effects,
- full vector database implementation,
- full dashboard before runtime correctness,
- framework adapters before native V2 primitives are stable,
- exactly-once network delivery for subscriptions,
- full policy language or complex inheritance,
- artifact deduplication in the first cut,
- derivative-only retention in the first cut,
- Go/Golang SDK or client support in the V2 prototype.

## 3. SDK Priorities

V2 SDK priority is:

1. Python SDK/client.
2. TypeScript SDK/client.
3. Other SDKs only after the Python and TypeScript paths are useful.

Do not build or maintain a Go/Golang SDK or client for the V2 prototype. Go can
use the HTTP API directly if needed, but official client ergonomics should focus
on Python first because most agent frameworks and agent developers use Python.
TypeScript comes second because browser/server agent apps, Vercel AI SDK, and
dashboard integrations need a good TS path.

## 4. Repository Boundaries

Use the existing crate layout.

| Area | Responsibility |
|---|---|
| `crates/agentdb-core` | V2 runtime types, invariants, service logic, policy checks |
| `crates/agentdb-storage` | V2 storage traits, key encoding, local repository helpers |
| `crates/agentdb-index` | text search hooks; vector hooks only if caller supplies embeddings |
| `crates/agentdb-api` | HTTP DTOs, routes, SSE subscription endpoint, structured errors |
| `crates/agentdb-wire` | wire DTOs shared by API/SDK if appropriate |
| `crates/agentdb-dashboard` | session-first UI after core APIs stabilize |
| `crates/agentdb-meta` | metadata persistence if reused by the local server |
| `bin/agentdb-server` | `serve --dev --v2` wiring and local data directory configuration |

Do not let framework adapters or dashboard needs define the core model. They must
map onto runtime primitives.

## 5. Local Storage Choice

For the first single-machine V2, use the existing local storage stack where
possible. If there is no clear V2-ready local backend, use a simple embedded KV
or metadata store already present in the repo plus a filesystem/object-store-like
artifact content store.

Artifact metadata belongs in the same local metadata store as tenants, agents,
sessions, events, skills, policies, idempotency records, and searchable records.
Only artifact content bytes should live in the separate artifact content store.
The API should not expose whether small artifact content was physically inlined
or spilled to the filesystem layout.

Required local data layout:

```text
{data_dir}/
  v2/
    meta/        # events, artifact metadata, records, idempotency, indexes
    artifacts/   # artifact content blobs by tenant/artifact_id
    tmp/         # temporary uploads before atomic rename
```

Implementation rules:

- All logical keys are tenant-first.
- Artifact metadata is committed with the metadata/event store.
- Artifact content is written through the local artifact content backend. For the
  filesystem backend, write to a temporary file, verify, then atomically rename
  into place.
- The storage engine decides whether small artifact content may be inlined or
  placed in the content store. The runtime API remains the same.
- Event append and session `next_seq` update must be atomic from the runtime's
  perspective.
- A restart must preserve events, sessions, artifacts, and idempotency results.

## 6. Core Data Types

Use typed string IDs with prefixes:

```text
ten_<uuidv7-or-ulid>
agt_<uuidv7-or-ulid>
ses_<uuidv7-or-ulid>
evt_<uuidv7-or-ulid>
art_<uuidv7-or-ulid>
rec_<uuidv7-or-ulid>
skl_<uuidv7-or-ulid>
pol_<uuidv7-or-ulid>
```

### 6.1 Event Envelope

Every persisted event must use an envelope like:

```json
{
  "tenant_id": "ten_...",
  "agent_id": "agt_...",
  "session_id": "ses_...",
  "event_id": "evt_...",
  "seq": 42,
  "created_at_ms": 1777334400000,
  "kind": "tool_result",
  "schema_version": "2026-04-28",
  "payload": {},
  "artifact_refs": [],
  "source_refs": [],
  "idempotency_key": "optional caller key",
  "policy_version": 1
}
```

Required invariants:

- `(tenant_id, session_id, seq)` is unique.
- `seq` is monotonic and contiguous per session.
- `event_id` is globally unique but not used for session ordering.
- `payload` must be valid JSON.
- Large payloads must use artifact refs once policy thresholds require it.

### 6.2 Session

Minimum fields:

```json
{
  "tenant_id": "ten_...",
  "agent_id": "agt_...",
  "session_id": "ses_...",
  "alias": "optional-human-name",
  "status": "active",
  "lifecycle": "hot",
  "next_seq": 43,
  "forked_from": { "session_id": "ses_parent", "at_seq": 41 },
  "created_at_ms": 1777334400000,
  "last_active_at_ms": 1777334400000,
  "policy_version": 1
}
```

### 6.3 Artifact

Minimum fields:

```json
{
  "tenant_id": "ten_...",
  "artifact_id": "art_...",
  "content_type": "application/json",
  "bytes": 1234,
  "sha256": "...",
  "content_ref": "v2/artifacts/ten_.../art_.../blob",
  "placement": "content_store",
  "source": { "session_id": "ses_...", "seq": 42 },
  "created_at_ms": 1777334400000,
  "redacted": false
}
```

### 6.4 Searchable Record

Searchable records support framework memory and knowledge use cases. AgentDB does
not decide memory semantics.

```json
{
  "tenant_id": "ten_...",
  "record_id": "rec_...",
  "namespace": ["agent", "support"],
  "scope": { "agent_id": "agt_...", "customer_id": "cust_123" },
  "text": "optional searchable text",
  "metadata": {},
  "tags": [],
  "source": { "session_id": "ses_...", "seq": 42 },
  "created_at_ms": 1777334400000,
  "expires_at_ms": null
}
```

For the first cut, implement text search with filters. Vector search may be a
caller-provided embedding field and a pluggable index hook, not a built-in model
call.

### 6.5 Skill Version

```json
{
  "tenant_id": "ten_...",
  "skill_id": "skl_...",
  "name": "draft-outreach",
  "version": "1.0.0",
  "manifest": {},
  "artifact_refs": [],
  "created_at_ms": 1777334400000
}
```

Session events that use a skill must record exact `skill_id` and `version`.

## 7. Logical Storage Keys

Use tenant-first logical keys even on a single machine:

```text
record/tenant/{tenant_id}
record/tenant_alias/{external_key} -> tenant_id

record/agent/{tenant_id}/{agent_id}
record/agent_alias/{tenant_id}/{alias} -> agent_id

record/session/{tenant_id}/{session_id}
record/session_alias/{tenant_id}/{alias} -> session_id
record/session_by_agent/{tenant_id}/{agent_id}/{session_id}

event/{tenant_id}/{session_id}/{seq} -> EventEnvelope
event_by_id/{tenant_id}/{event_id} -> {session_id, seq}

artifact_meta/{tenant_id}/{artifact_id} -> Artifact metadata
artifact_content/{tenant_id}/{artifact_id}/blob -> content backend ref

search_record/{tenant_id}/{record_id} -> SearchableRecord
search_scope/{tenant_id}/{scope_hash}/{record_id}

skill/{tenant_id}/{skill_id}/{version} -> SkillVersion
skill_name/{tenant_id}/{name}/{version} -> skill_id
skill_binding/{tenant_id}/{agent_id}/{skill_id}/{version}

policy/{tenant_id}/{policy_id}/{version}
policy_attachment/{tenant_id}/{target_type}/{target_id}/{policy_id}

idempotency/{tenant_id}/{operation}/{target}/{key} -> result_hash + result
```

## 8. Runtime API Contract

Use `/v2` endpoints for the prototype. Request/response DTOs may live in
`agentdb-api` or `agentdb-wire`, but must stay aligned with runtime types.

### 8.1 Tenants

```text
POST /v2/tenants
GET  /v2/tenants/{tenant_id}
```

Create request:

```json
{ "external_key": "acme", "policy": {} }
```

### 8.2 Agents

```text
POST /v2/tenants/{tenant_id}/agents
GET  /v2/tenants/{tenant_id}/agents/{agent_id}
```

### 8.3 Sessions And Events

```text
POST /v2/tenants/{tenant_id}/agents/{agent_id}/sessions
GET  /v2/tenants/{tenant_id}/sessions/{session_id}
POST /v2/tenants/{tenant_id}/sessions/{session_id}/events
POST /v2/tenants/{tenant_id}/sessions/{session_id}/append-and-fetch
GET  /v2/tenants/{tenant_id}/sessions/{session_id}/events?from_seq=&limit=
GET  /v2/tenants/{tenant_id}/sessions/{session_id}/replay
POST /v2/tenants/{tenant_id}/sessions/{session_id}/fork
```

Append response:

```json
{ "event_id": "evt_...", "seq": 42 }
```

Append-and-fetch response:

```json
{
  "event_id": "evt_...",
  "seq": 42,
  "recent": []
}
```

### 8.4 Artifacts

```text
POST /v2/tenants/{tenant_id}/artifacts
GET  /v2/tenants/{tenant_id}/artifacts/{artifact_id}
GET  /v2/tenants/{tenant_id}/artifacts/{artifact_id}/blob
```

The first cut may use buffered upload. Streaming upload can come later.

Artifact upload returns metadata and an artifact ID. The caller does not choose
physical placement. The storage engine may inline small content or store it in
the local artifact content layout; both must read back through the same API.

### 8.5 Searchable Records

```text
POST   /v2/tenants/{tenant_id}/records
GET    /v2/tenants/{tenant_id}/records/search
DELETE /v2/tenants/{tenant_id}/records/{record_id}
```

Search must support namespace, scope, tags, text query, source refs, and limit.

### 8.6 Skills

```text
POST /v2/tenants/{tenant_id}/skills
POST /v2/tenants/{tenant_id}/agents/{agent_id}/skills/bind
GET  /v2/tenants/{tenant_id}/agents/{agent_id}/skills
```

### 8.7 Subscribe

```text
GET /v2/tenants/{tenant_id}/sessions/{session_id}/subscribe?from_seq=N
```

Use Server-Sent Events for the first cut.

Rules:

- replay existing events from `from_seq`,
- then live-tail newly appended events,
- delivery is at-least-once,
- clients dedupe by `(session_id, seq)`,
- include heartbeat comments if the connection is idle.

## 9. Concurrency And Idempotency

For the first cut, serialize appends per session.

Append rules:

- Acquire a per-session append lock or equivalent transaction guard.
- Read `next_seq`.
- Persist event at `event/{tenant_id}/{session_id}/{next_seq}`.
- Update session `next_seq = next_seq + 1` and `last_active_at_ms`.
- Notify subscribers after durable write succeeds.

Idempotency rules:

- Mutating APIs may accept `Idempotency-Key`.
- The idempotency scope is `(tenant_id, operation, target, key)`.
- Repeating the same payload returns the original result.
- Reusing the key with a different payload returns structured conflict.
- P0 must implement idempotency for event append and append-and-fetch.

## 10. Fork Semantics

Fork is metadata-first and copy-free.

Fork request:

```json
{ "alias": "try-shorter-copy", "at_seq": 42 }
```

Rules:

- Child session records `forked_from = { parent_session_id, at_seq }`.
- Child session has its own `session_id` and starts child-local `seq` at `0`.
- Replay for child returns parent events through `at_seq`, then child events.
- Fetching child-local events returns only child events unless `include_inherited`
  is explicitly set.
- Artifacts are referenced, not copied.
- Searchable records are not automatically copied in P0; callers may write new
  records that reference parent or child sessions.

## 11. Minimal Policy Model

Use built-in defaults plus optional tenant policy and optional agent override.

Precedence:

```text
agent policy override > tenant policy > built-in defaults
```

Minimum policy fields:

```json
{
  "max_inline_event_bytes": 262144,
  "artifact_required_after_bytes": 1048576,
  "max_artifact_bytes": 16777216,
  "max_append_events_per_minute": 600,
  "indexing": { "default": "metadata_only" }
}
```

P0 enforcement:

- inline event payload size,
- artifact max size,
- basic append rate limit,
- indexing mode for searchable records.

## 12. Context Builder

Expose a simple context endpoint after append/fetch works:

```text
POST /v2/tenants/{tenant_id}/sessions/{session_id}/context
```

Request:

```json
{
  "max_events": 20,
  "max_bytes": 1000000,
  "include": ["recent_events", "skills", "records"],
  "record_query": { "namespace": ["agent", "support"], "text": "renewal" }
}
```

Initial behavior:

- include latest events in sequence order,
- include active bound skill manifests,
- include searchable records only when an explicit query/scope is provided,
- include artifact summaries and metadata, not raw blobs,
- do not generate summaries or embeddings internally.

## 13. Structured Errors

Every API error should return:

```json
{
  "code": "EVENT_TOO_LARGE",
  "message": "Inline event payload exceeds policy limit",
  "retryable": false,
  "suggestion": "Store the payload as an artifact and append an artifact ref"
}
```

Minimum error codes:

- `TENANT_NOT_FOUND`,
- `AGENT_NOT_FOUND`,
- `SESSION_NOT_FOUND`,
- `ARTIFACT_NOT_FOUND`,
- `EVENT_TOO_LARGE`,
- `ARTIFACT_TOO_LARGE`,
- `HASH_MISMATCH`,
- `IDEMPOTENCY_CONFLICT`,
- `POLICY_DENIED`,
- `INVALID_CURSOR`,
- `RATE_LIMITED`,
- `INTERNAL`.

## 14. Test Plan

### 14.1 Unit Tests

- ID prefix validation.
- Tenant-first key encoding.
- Event envelope serialization round trip.
- Artifact hash verification.
- Policy size thresholds.
- Source ref serialization.
- Skill version uniqueness.
- Idempotency conflict detection.

### 14.2 Core Integration Tests

- Create tenant, agent, and session.
- First append returns `seq = 0`.
- Multiple appends return contiguous sequence numbers.
- Append-and-fetch includes the appended event.
- Fetch from seq returns ordered events.
- Fork session preserves parent cutoff.
- Child append does not mutate parent.
- Replay on child returns inherited parent events plus child events.
- Artifact put stores metadata and blob.
- Event can reference artifact.
- Searchable record can reference session seq.
- Skill bind then `skill_used` event records exact version.
- Subscription from old seq replays existing events.
- Reconnect from `last_seq + 1` does not duplicate prior events.
- Idempotent append returns same result on retry.
- Conflicting idempotent append returns conflict.

### 14.3 API Tests

- All V2 endpoints reject missing tenant scope.
- Tenant mismatch returns authorization or scope error.
- Oversized inline event is rejected or requires artifact.
- Artifact hash mismatch is rejected.
- Structured error fields are present.
- SSE subscribe receives appended events.

### 14.4 Restart Tests

- Events survive server restart.
- `next_seq` survives server restart.
- Artifact metadata and content survive server restart.
- Idempotency records survive server restart.
- Replay after restart returns the same ordered timeline.

## 15. Build Order For AI Coding Agents

1. Add V2 typed IDs and runtime types in `agentdb-core`.
2. Add V2 storage keys and repository helpers.
3. Implement tenant, agent, and session create/get.
4. Implement atomic event append and fetch by sequence.
5. Implement append-and-fetch.
6. Add idempotency records for append.
7. Implement local artifact put/get/head with SHA-256 verification.
8. Add artifact ref validation during event append.
9. Implement searchable record write/search with simple text matching.
10. Implement skill register/bind/list.
11. Implement session fork metadata and replay merge.
12. Implement SSE subscribe from sequence with in-process fanout.
13. Add minimal policy checks.
14. Add context builder.
15. Add API routes and DTOs.
16. Add restart and end-to-end tests.
17. Add dashboard views only after core APIs are stable.

## 16. Framework Adapter Proof

After the native V2 primitives are stable, implement one proof adapter. Prefer the
adapter whose persistence interface maps most cleanly to the completed primitives.

Validation rule:

```text
The adapter should be mostly mapping code. If it implements ordering,
idempotency, artifact offload, subscriptions, lifecycle, or search policy itself,
AgentDB is missing a primitive.
```
