# agentdb (Python)

Python client for AgentDB, the runtime database for agent sessions, artifacts,
records, replay, and context.

## Install

```bash
pip install -e clients/python
pip install -e 'clients/python[async]'  # AsyncClient + httpx
```

## Start A Local Server

```bash
cargo run -p agentdb-server -- serve --dev
```

## Sync Quick Start

```python
from agentdb import AgentDB

db = AgentDB("http://127.0.0.1:7878")
workspace = db.workspace("acme")
agent = workspace.agent("sales-agent")
session = agent.session("ticket-42")

result = session.append_user("hello", fetch_last_n=5)
print(result.seq, [event.payload for event in result.recent])
```

`tenant_id` is infrastructure state. Normal agent code binds a workspace once
and then works with agents, sessions, artifacts, and records from that
handle. Raw tenant-scoped calls remain available through `Client` for internal
tests and admin-style tooling.

`workspace("acme")`, `workspace.agent("sales-agent")`, and
`agent.session("ticket-42")` use application-facing names/aliases. AgentDB still
assigns canonical opaque IDs (`ten_*`, `agt_*`, `ses_*`) and exposes them on the
returned handles as `.id`. Use `agent.session_by_id(session_id)` or
`agent.session(session_id=session_id)` when recovering from a persisted canonical
session ID.

## Async Quick Start

```python
import asyncio
from agentdb import AsyncClient

async def main():
    async with AsyncClient("http://127.0.0.1:7878") as client:
        tenant = await client.create_tenant("acme-async")
        agent = await client.create_agent(tenant["tenant_id"])
        session = await client.create_session(tenant["tenant_id"], agent["agent_id"])
        result = await session.append({"role": "user", "text": "hello"}, fetch_last_n=5)
        print(result.seq, [event.payload for event in result.recent])

asyncio.run(main())
```

## Core Surface

- `AgentDB.workspace`, `AgentDB.session_by_id`
- `Workspace.agent`, `Workspace.agent_by_id`, `Workspace.session_by_id`, `Workspace.changes`
- `Workspace.artifacts`, `Workspace.records`
- `AgentHandle.session`, `AgentHandle.session_by_id`
- `SessionHandle.append`, `append_user`, `model_message`, `tool_call`, `tool_result`, `context_pack_built`
- `SessionHandle.fetch`, `fetch_recent`, `replay`, `materialize_replay_prefix`, `fork`, `context`
- `SessionHandle.artifacts`, `records`
- `event_ref`, `artifact_ref`

IDs are opaque typed strings such as `ten_*`, `agt_*`, `ses_*`, `evt_*`,
`art_*`, `rec_*`, and `skl_*`. Mutating SDK calls generate an
`Idempotency-Key` only when `idempotency_key` is supplied. This keeps the
default append path eligible for server-side same-session batching; reuse an
explicit key when the caller may retry a mutation.

For records, `write_record` captures the server's
`X-AgentDB-Record-Commit-Seq` header per tenant. Later `search_records` calls
automatically include `min_visible_record_commit_seq` unless the call already
sets it. Use `sharding_key` when you want the owner-local LSI-like index; omit
it for GSI-like global discovery.

## Harness Recovery

AgentDB sessions are durable state outside the harness process. A restarted
harness should keep the tenant, agent, session, and last-seen sequence IDs, then
rebuild a session handle and continue from durable history:

```python
from agentdb import AgentDB

db = AgentDB("http://127.0.0.1:7878")
agent = db.workspace(tenant_id=tenant_id).agent_by_id(agent_id)
session = agent.session_by_id(session_id)

history = session.replay(from_seq=0, limit=100)
context = session.context(include=["recent_events", "records"])

result = session.agent_status("recovered", last_seen_seq=last_seen_seq)

for event in session.replay(from_seq=last_seen_seq + 1, limit=100):
    handle(event)
```

Concept mapping:

| Durable-session concept | Python SDK |
|---|---|
| wake/resume handle | `agent.session_by_id(...)` |
| get events | `session.fetch(...)` |
| append event | `session.append(...)` |
| append and fetch | `session.append_and_fetch(...)` |
| fork session | `session.fork(...)` |
| build context | `session.context(...)` |
| workspace/session changefeed | `workspace.changes(...)` |

Use `from_seq=last_seen_seq + 1` when reconnecting a subscription to avoid
replaying events the harness has already processed. Persisting the last handled
`seq` is the harness's cursor rule.

## Event Conventions

The runtime accepts arbitrary event kinds. The SDK exports advisory convention
helpers for common brain/hand/session events:

```python
from agentdb import event_ref

user = session.append_user("run the report")
call = session.tool_call(
    "call_1",
    "sandbox.exec",
    {"cmd": "python report.py"},
    source_refs=[event_ref(user)],
)
session.tool_result("call_1", "Generated report.", source_refs=[event_ref(call)])
```

Helpers are intentionally client-side only. They do not prevent custom event
kinds or adapter-specific payload fields.

Append payloads reject raw credential-shaped field names such as `api_key`,
`access_token`, `password`, and `private_key` before sending the request. Pass
opaque vault or broker references through `credential_refs` instead. The
`tool_result` helper also redacts common credential tokens from inline summary,
output, and metadata strings; put large raw stdout/stderr in artifacts.

## Context Provenance

Context is a derived view over the durable session log. When a harness builds
context for a model call, record a `context_pack_built` event with source refs,
then link the model output to that event:

```python
from agentdb import (
    context_source_refs,
    event_ref,
)

context = session.context(include=["recent_events", "records"], max_events=20)
context_event = session.context_pack_built(
    "ctx_1",
    ["recent_events", "records"],
    max_events=20,
    selected_event_count=len(context.get("events", [])),
    selected_record_count=len(context.get("records", [])),
    source_refs=context_source_refs(context, max_refs=50),
)

session.model_message(
    "The report is ready.",
    context_pack_id="ctx_1",
    source_refs=[event_ref(context_event)],
)
```

This keeps the prompt/context shape model-specific while preserving the
recoverable events and records that produced it.

## Adapter Readiness

Adapters can run a lightweight convention check over a recorded event sequence:

```python
from agentdb import validate_adapter_events

warnings = validate_adapter_events(events)
if warnings:
    raise AssertionError("\n".join(warnings))
```

The validator is advisory. It catches common issues such as unpaired tool calls,
large inline tool outputs without artifacts, missing context provenance, missing
resume status, and event sequences that do not carry AgentDB `seq` values.

## Testing

```bash
python3 -m pytest -q clients/python/tests
```

Live smoke tests are skipped when `target/debug/agentdb` is absent.
