# V2 Session Layer Gap Audit

Status: draft

Date: 2026-04-30

## 1. Purpose

This audit checks the current AgentDB V2 prototype against the product thesis:

> AgentDB is the durable session database for AI agent platforms.

The target architecture separates:

- **Brains**: models, harnesses, planners, memory strategies, context builders.
- **Hands**: sandboxes, MCP servers, tools, browsers, filesystems, external
  systems.
- **Sessions**: durable ordered execution state, artifacts, searchable records,
  policies, subscriptions, replay, fork lineage, and audit history.

AgentDB should own the session layer. It should make harnesses restartable and
hands replaceable without requiring AgentDB to become a model runtime, workflow
engine, sandbox, or tool executor.

## 2. Ideal Session-Layer Contract

A platform harness should be able to express its lifecycle through a small set
of stable operations:

```text
wake(session_id)
get_events(session_id, range/filter)
append_event(session_id, event)
append_and_fetch(session_id, event, context_window)
subscribe(session_id, from_seq)
fork_session(parent_session_id, at_seq)
build_context(session_id, budget/query)
put_artifact(session_id?, bytes, metadata)
write_record(session_id?, namespace, source_refs, content)
resolve_skills(agent_id/session_id)
watch(tenant_id | agent_id | session_id, cursor)
```

The names do not need to be literal API routes, but the behavior must be
obvious in HTTP, SDKs, examples, and adapter code.

The critical invariant is:

```text
The durable session log is the source of truth. Model context is a derived view.
```

## 3. Current Coverage

The current prototype already covers most of the foundational primitives.

| Capability | Current state |
|---|---|
| Tenant-scoped native API | Implemented under `/api/tenants/{tenant_id}/...`. |
| Agent/session creation | Implemented in Rust API and Python/TypeScript SDKs. |
| Ordered event append | Implemented with per-session `seq`. |
| Append-and-fetch | Implemented as `/sessions/{session_id}/append-and-fetch` and SDK helpers. |
| Idempotency | Implemented for mutating SDK calls and runtime mutation paths. |
| Event fetch | Implemented as `/sessions/{session_id}/events?from_seq=&limit=`. |
| Replay | Implemented as `/sessions/{session_id}/replay`. |
| Fork | Implemented as `/sessions/{session_id}/fork`. |
| Subscribe | Implemented as replay-style SSE from `from_seq`. |
| Watch | Implemented as tenant/session/agent watch cursors. |
| Context pack | Implemented as `/sessions/{session_id}/context`. |
| Artifact metadata/blob | Implemented with Python/TypeScript SDK accessors. |
| Searchable records | Implemented with scoped write/search APIs. |
| Skills | Implemented with register/bind/list behavior. |
| Legacy surface cleanup | Mongo wire facade, Go SDK, tenantless routes, and broadcast helpers are removed from the V2 prototype surface. |

This means the next work should not be another broad primitive milestone. The
more important step is to make the existing primitives unmistakably usable as a
session database for real harnesses.

## 4. Product Gaps

### 4.1 `wake(session_id)` is a concept, not yet an explicit ergonomic path

The pieces exist: SDKs can reconstruct a `Session` handle from tenant/session
IDs, fetch events, replay, context, subscribe, and append. But harness recovery
is not presented as one clear flow.

Impact:

- Framework authors have to infer the recovery recipe.
- Examples optimize for first-run creation rather than restart after a crash.
- It is easy to miss which cursor or sequence should be stored by the harness.

Recommended next step:

- Add explicit SDK/documentation examples named around `wake` or `resume`.
- Add a smoke test where a new client process resumes an existing session by ID,
  reads recent events/context, appends a recovery event, and subscribes from the
  next sequence.

### 4.2 Event conventions for brains and hands are underspecified

AgentDB stores arbitrary event kinds and payloads, which is correct for
extensibility. But agent-platform interoperability needs a recommended core
event vocabulary for common harness actions.

Minimum useful conventions:

| Event kind | Purpose |
|---|---|
| `user_message` | Human or upstream platform input. |
| `model_message` | Model-visible assistant output. |
| `model_thought` | Optional private/planner output when a platform chooses to persist it. |
| `tool_call` | Requested hand/tool/sandbox operation. |
| `tool_result` | Tool success, summary, output refs, and structured result. |
| `tool_error` | Tool failure, retryability, diagnostic summary, output refs. |
| `artifact_created` | Generated or uploaded file/blob/log/screenshot. |
| `approval_requested` | Human/policy approval checkpoint. |
| `approval_result` | Approval decision and actor metadata. |
| `context_pack_built` | Optional trace of what context was presented to a model. |
| `agent_status` | Sleep, wake, pause, done, blocked, retrying. |

Impact:

- Adapters will invent incompatible schemas for the same concepts.
- Dashboard and evaluation views cannot reliably group tool calls/results.
- MCP/OpenAI/LangGraph/Vercel adapters need more mapping code than necessary.

Recommended next step:

- Add a small event convention spec with required fields and source/artifact ref
  guidance.
- Keep it advisory and schema-versioned, not a closed enum in the runtime.

### 4.3 Context packs need source-accountability guidance

The context endpoint exists. The product thesis now says context is a derived
view over durable session truth. The API should make that provenance obvious.

The implementation should continue to return selected events, records, skills,
and policy hints. The docs/examples should additionally emphasize:

- context packs are reproducible best-effort views, not the canonical log,
- every included summary or record should point back to source refs where
  possible,
- harnesses may transform context packs for model-specific prompt formats,
- raw artifacts should remain referenced, not silently expanded.

Recommended next step:

- Document a `context_pack_built` event convention.
- Add examples that show context source refs alongside prompt construction.

### 4.4 Tool and sandbox replacement needs an end-to-end smoke scenario

The design says hands are replaceable, but tests mostly validate individual
primitives. A platform-level scenario would prove the architecture:

1. Create tenant, agent, session.
2. Append `user_message`.
3. Append `tool_call` for a sandbox command.
4. Store a large simulated tool output as an artifact.
5. Append `tool_result` referencing the artifact.
6. Build context and append `model_message`.
7. Drop the client object to simulate harness crash.
8. Recreate the session handle from IDs.
9. Replay/fetch context and append `agent_status: recovered`.
10. Subscribe from the last seen sequence and verify no duplicate/lost events.

Recommended next step:

- Add this as a Python and TypeScript SDK smoke test or example.
- Keep it in-memory/local so it runs quickly in CI.

### 4.5 Adapter-facing API names are close, but the docs should bridge them

The docs now use conceptual names like `wake`, `get_events`, `append_event`, and
`build_context`. The actual SDKs expose idiomatic methods:

| Concept | Python | TypeScript |
|---|---|---|
| wake/resume handle | `client.session(...)` | `client.session(...)` |
| get events | `session.fetch(...)` | `session.fetch(...)` |
| append event | `session.append(...)` | `session.append(...)` |
| append and fetch | `session.append_and_fetch(...)` | `session.appendAndFetch(...)` |
| subscribe | `session.subscribe(...)` | `session.subscribe(...)` |
| fork | `session.fork(...)` | `session.fork(...)` |
| build context | `session.context(...)` | `session.context(...)` |
| watch | `client.watch(...)` | `client.watch(...)` |

Recommended next step:

- Add a short "Concept to SDK mapping" section in SDK READMEs.
- Consider `resume_session` / `wake_session` aliases only if examples show
  `client.session(...)` is not clear enough.

## 5. Prioritized Roadmap

### P0: Harness recovery smoke path

Status: implemented in the follow-up to this audit.

Add one documented smoke scenario that proves a harness can recover with only
tenant, agent, and session IDs.

Deliverables:

- Python example/test.
- TypeScript example/test.
- README snippet showing the sequence cursor rule.

Exit criteria:

- Existing session can be reconstructed from IDs.
- Recovery flow fetches/replays durable history.
- Append after recovery produces the next contiguous sequence.
- Subscribe from `last_seq + 1` does not replay old events.

### P1: Advisory event convention spec

Status: implemented in `session-event-conventions.md`.

Document recommended event kinds and minimal payload fields for brain/hand
interoperability.

Deliverables:

- `docs/v2_design/session-event-conventions.md`.
- Source/artifact ref guidance for tool calls/results/errors.
- Schema-version guidance.

Exit criteria:

- OpenAI Agents SDK, LangGraph, Vercel AI SDK, and MCP adapter events can map to
  the conventions without runtime-specific special cases.

### P2: Context provenance examples

Clarify how context packs relate to durable execution truth.

Deliverables:

- Context example in Python/TypeScript SDK docs.
- Optional `context_pack_built` event convention.

Exit criteria:

- A reader can tell which events/records/artifacts/skills were used to build the
  model input and how to recover the source material later.

### P3: Adapter readiness checklist

Before adding another adapter, write down the checklist each adapter must pass.

Required checks:

- Does it use AgentDB ordering instead of inventing a separate sequence?
- Does it store large outputs as artifacts?
- Does it preserve tool call/result/error pairing?
- Does it support resume from session ID and sequence/cursor?
- Does it avoid making model context the source of truth?
- Does it preserve source refs for records and summaries?

## 6. Recommendation

The first implementation follow-up should be P0: a harness recovery smoke path
and SDK documentation update. That follow-up is now included alongside this
audit.

That is the smallest change that proves the revised thesis in executable form.
It exercises the existing primitives together without introducing another broad
runtime subsystem, and it gives adapter authors the recovery recipe they need.
