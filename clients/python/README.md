# agentdb (Python)

Python client for AgentDB V2, the runtime database for agent sessions,
artifacts, records, skills, replay, and context.

## Install

```bash
pip install -e clients/python
pip install -e 'clients/python[async]'  # AsyncClient + httpx
```

## Start A Local Server

```bash
cargo run -p agentdb-server -- serve --in-memory --bind 127.0.0.1:8080
```

## Sync Quick Start

```python
from agentdb import AgentDB

db = AgentDB("http://127.0.0.1:8080")
workspace = db.workspace("acme")
agent = workspace.agent("sales-agent")
session = agent.session("ticket-42")

result = session.append_user("hello", fetch_last_n=5)
print(result.seq, [event.payload for event in result.recent])
```

`tenant_id` is infrastructure state. Normal agent code binds a workspace once
and then works with agents, sessions, artifacts, records, and skills from that
handle. Raw tenant-scoped calls remain available through `Client` for internal
tests and admin-style tooling.

## Async Quick Start

```python
import asyncio
from agentdb import AsyncClient

async def main():
    async with AsyncClient("http://127.0.0.1:8080") as client:
        tenant = await client.create_tenant("acme-async")
        agent = await client.create_agent(tenant["tenant_id"])
        session = await client.create_session(tenant["tenant_id"], agent["agent_id"])
        result = await session.append({"role": "user", "text": "hello"}, fetch_last_n=5)
        print(result.seq, [event.payload for event in result.recent])

asyncio.run(main())
```

## Core Surface

- `AgentDB.workspace`
- `Workspace.agent`, `Workspace.session`, `Workspace.watch`
- `Workspace.artifacts`, `Workspace.records`, `Workspace.skills`
- `AgentHandle.session`, `AgentHandle.bind_skill`
- `SessionHandle.append`, `append_user`, `model_message`, `tool_call`, `tool_result`
- `SessionHandle.fetch`, `fetch_recent`, `replay`, `fork`, `subscribe`, `context`
- `SessionHandle.artifacts`, `records`, `skills`

IDs are opaque typed strings such as `ten_*`, `agt_*`, `ses_*`, `evt_*`,
`art_*`, `rec_*`, and `skl_*`. Mutating SDK calls generate an
`Idempotency-Key` automatically unless one is supplied.

## Harness Recovery

AgentDB sessions are durable state outside the harness process. A restarted
harness should keep the tenant, agent, session, and last-seen sequence IDs, then
rebuild a session handle and continue from durable history:

```python
from agentdb import AgentDB

db = AgentDB("http://127.0.0.1:8080")
agent = db.workspace(tenant_id=tenant_id).agent(agent_id=agent_id)
session = agent.session(session_id=session_id)

history = session.replay(from_seq=0, limit=100)
context = session.context(include=["recent_events", "records", "skills"])

result = session.agent_status("recovered", last_seen_seq=last_seen_seq)

for event in session.subscribe(from_seq=last_seen_seq + 1):
    handle(event)
```

Concept mapping:

| Durable-session concept | Python SDK |
|---|---|
| wake/resume handle | `agent.session(session_id=...)` |
| get events | `session.fetch(...)` |
| append event | `session.append(...)` |
| append and fetch | `session.append_and_fetch(...)` |
| subscribe from sequence | `session.subscribe(from_seq=...)` |
| fork session | `session.fork(...)` |
| build context | `session.context(...)` |
| workspace/session watch | `workspace.watch(...)` |

Use `from_seq=last_seen_seq + 1` when reconnecting a subscription to avoid
replaying events the harness has already processed. Persisting the last handled
`seq` is the harness's cursor rule.

## Event Conventions

The runtime accepts arbitrary event kinds. The SDK exports advisory convention
helpers for common brain/hand/session events:

```python
from agentdb import SessionEventKind, tool_call, tool_result, user_message

session.append(
    user_message("run the report"),
    kind=SessionEventKind.USER_MESSAGE.value,
)
call = session.append(
    tool_call("call_1", "sandbox.exec", {"cmd": "python report.py"}),
    kind=SessionEventKind.TOOL_CALL.value,
)
session.append(
    tool_result("call_1", "Generated report."),
    kind=SessionEventKind.TOOL_RESULT.value,
    source_refs=[{"event_id": call.event_id}],
)
```

For common events, `SessionHandle` also provides shortcuts:

```python
session.append_user("run the report")
call = session.tool_call("call_1", "sandbox.exec", {"cmd": "python report.py"})
session.tool_result("call_1", "Generated report.", source_refs=[{"event_id": call.event_id}])
```

Helpers are intentionally client-side only. They do not prevent custom event
kinds or adapter-specific payload fields.

## Context Provenance

Context is a derived view over the durable session log. When a harness builds
context for a model call, record a `context_pack_built` event with source refs,
then link the model output to that event:

```python
from agentdb import (
    SessionEventKind,
    context_pack_built,
    context_source_refs,
    model_message,
)

context = session.context(include=["recent_events", "records"], max_events=20)
context_event = session.append(
    context_pack_built(
        "ctx_1",
        ["recent_events", "records"],
        max_events=20,
        selected_event_count=len(context.get("events", [])),
        selected_record_count=len(context.get("records", [])),
    ),
    kind=SessionEventKind.CONTEXT_PACK_BUILT.value,
    source_refs=context_source_refs(context, max_refs=50),
)

session.model_message(
    "The report is ready.",
    context_pack_id="ctx_1",
    source_refs=[{"event_id": context_event.event_id}],
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
