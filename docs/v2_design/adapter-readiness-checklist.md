# V2 Adapter Readiness Checklist

Status: draft

Date: 2026-04-30

## 1. Purpose

Framework adapters are the validation workload for AgentDB's durable session
database thesis. A good adapter should mostly map framework concepts onto
AgentDB primitives. It should not reimplement ordering, recovery, artifact
storage, context provenance, or replay semantics outside AgentDB.

This checklist is the gate for adapters such as LangGraph, OpenAI Agents SDK,
Vercel AI SDK, MCP, or custom platform harnesses.

## 2. Required Checks

| Check | Required behavior |
|---|---|
| AgentDB ordering | Adapter uses AgentDB `seq`/`event_id` for replay ordering instead of inventing a separate durable sequence. |
| Tool pairing | Every `tool_call` has a later `tool_result` or `tool_error` with the same `tool_call_id`. |
| Large output artifacts | Large stdout/stderr/files/logs/screenshots are stored as artifacts and referenced from events. |
| Resume cursor | Adapter persists tenant, agent, session, and last handled `seq`/cursor so a fresh harness can recover. |
| Context provenance | Adapter records `context_pack_built` when it builds model context and links model output back to that context event. |
| Source refs | Derived summaries, records, artifacts, context packs, approvals, and model outputs preserve useful `source_refs`. |
| Approval pairing | Human/policy approval flows use `approval_requested` and `approval_result` with matching `approval_id`. |
| Runtime source of truth | Adapter does not make model prompt text, framework memory, or trace spans the only durable record. |

## 3. SDK Helper

Python and TypeScript SDKs expose advisory validators:

```python
from agentdb import validate_adapter_events

warnings = validate_adapter_events(events)
assert warnings == []
```

```ts
import { validateAdapterEvents } from "agentdb";

const warnings = validateAdapterEvents(events);
if (warnings.length) throw new Error(warnings.join("\n"));
```

The validator is intentionally best-effort. It checks common event-sequence
mistakes:

- missing AgentDB sequence numbers,
- unpaired tool calls,
- tool results/errors without a prior call,
- large inline string output without artifact refs,
- context packs without source refs,
- model messages that name a context pack but do not reference its event,
- missing `agent_status: recovered`,
- recovered status events without `last_seen_seq`.

It does not replace integration tests against a live AgentDB server.

## 4. Adapter Test Recipe

Every adapter should include at least one smoke test with this shape:

1. Create or resume an AgentDB session.
2. Append a user/input event.
3. Produce a tool call.
4. Store a simulated large tool output as an artifact.
5. Append a tool result referencing the call and artifact.
6. Build context.
7. Append `context_pack_built` with refs from the context response.
8. Append a model output that references the context event.
9. Simulate harness restart by rebuilding the session handle from IDs.
10. Replay or fetch durable history.
11. Append `agent_status: recovered` with `last_seen_seq`.
12. Subscribe or watch from `last_seen_seq + 1`.
13. Run `validate_adapter_events` / `validateAdapterEvents` on the recorded
    sequence and require no warnings.

## 5. Framework Notes

### 5.1 OpenAI Agents SDK

- Map session items to AgentDB events.
- Preserve tool call IDs.
- Map interruptions to approval events when human/policy decision is required.
- Keep provider traces as metadata or records, not as the only durable timeline.

### 5.2 LangGraph

- Use AgentDB session events for ordered execution timeline.
- Use records for searchable memory/checkpoint metadata when needed.
- Use artifacts for large state snapshots or serialized checkpoints.
- Keep graph checkpoint IDs in payload metadata.

### 5.3 Vercel AI SDK

- Map messages to `user_message` / `model_message`.
- Map tool parts to `tool_call`, `tool_result`, and `tool_error`.
- Store stream resume state as AgentDB sequence/cursor state.

### 5.4 MCP

- Map tool invocations and results to AgentDB tool events.
- Preserve MCP server/resource identifiers in metadata.
- Store large resources as artifacts or records depending on whether they are
  raw evidence or searchable memory.
