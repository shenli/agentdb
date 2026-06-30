# LLMS.md — AgentDB for LLM agents

This file is written for an LLM agent that's learning AgentDB
in one ingest. Sections are short, copy-pasteable, and ordered
by how often a working agent needs them. Human devs may prefer
[`README.md`](README.md) and
[`docs/architecture/implementation-architecture.md`](docs/architecture/implementation-architecture.md).

If you (the agent) only read one section: go to **§3 The five
operations**.

> **Why not `AGENTS.md`?** That filename is the established
> Claude Code / Cursor convention for "instructions to an LLM
> coding *in* this repo" (build commands, crate layout, etc.).
> Runtime-API instructions for an LLM *using* AgentDB live
> here in `LLMS.md` so both can coexist without overloading
> one filename with two audiences.

---

## 1. What AgentDB is

AgentDB is an **agent-native runtime database**. It stores the
events your agent produces (thoughts, tool calls, tool outputs,
user turns) in **sessions**, plus artifacts, searchable records,
skills, fork lineage, replay streams, and context packs. One
runtime surface, one agent memory model.

You talk to it through the native HTTP API:

- **HTTP** at `http://<host>:7878` or the configured bind port.
  Every official SDK uses this API for writes, reads, replay,
  search, artifacts, and context.

AgentDB also has an optional MongoDB client compatibility listener:

- **MongoDB wire adapter** at the configured `--mongo-bind` port
  (for example `mongodb://127.0.0.1:7879/?directConnection=true`).
  This is MongoDB client compatibility, not MongoDB database
  compatibility. Use it only for fixed AgentDB runtime collections
  and AgentDB extension commands.

---

## 2. Core concepts

| term | one-line definition |
|---|---|
| **workspace / tenant** | Top-level isolation unit. SDKs expose this as a workspace handle; the server stores it as a tenant. |
| **agent** | Runtime actor under a workspace. Sessions belong to agents. |
| **session** | A linear sequence of events. The unit of working memory for an agent. SDKs let you create sessions by stable aliases while the server assigns canonical `ses_*` IDs. |
| **event** | One typed JSON envelope you appended. Server assigns `event_id`, `seq`, and `created_at_ms`; your data lives under `payload`. |
| **`append-and-fetch`** | The hot-path primitive: write one event, return the last N events of the same session, in one round trip. |
| **fork** | Create a child session that inherits the parent's events at fork time, MVCC-backed. Use to branch into "what if" timelines without copying. |
| **records** | Searchable durable facts linked back to source events or artifacts. |
| **artifacts** | Larger binary/text objects referenced from events instead of being inlined into payloads. |
| **context pack** | A recorded bundle of selected context with source references, so prompts are reproducible. |

---

## 3. The five operations

> Defaults below assume the dev-mode server: `agentdb serve --dev`.
> Prefer the SDK handles for new agent code.

### 3.1 Append an event (and read recent context)

The single most-used operation. Every turn writes *something*
and almost always wants the recent history for context.

**Python**:

```python
from agentdb import AgentDB

db = AgentDB("http://127.0.0.1:7878")
workspace = db.workspace("demo")
agent = workspace.agent("support-agent")
session = agent.session("ticket-42")

result = session.append(
    {"role": "user", "text": "checkout failed"},
    fetch_last_n=5,
)
```

**TypeScript**:

```ts
import { AgentDB } from "agentdb";

const db = new AgentDB({ baseUrl: "http://127.0.0.1:7878" });
const workspace = await db.workspace("demo");
const agent = await workspace.agent("support-agent");
const session = await agent.session("ticket-42");

const result = await session.append(
  { role: "user", text: "checkout failed" },
  { fetchLastN: 5 }
);
```

`fetch_last_n: 0` skips the read-back (cheaper if you don't
need context).

### 3.2 Fork a session

Branch into an alternate timeline. The child sees the parent's
events at fork time; subsequent writes diverge.

```python
branch = session.fork("ticket-42-branch-a")
branch.append({"role": "user", "text": "try the refund path"})
```

### 3.3 Search records

Use records for durable facts you want to retrieve later. Records
can carry source refs back to the events or artifacts that produced
them.

```python
record = workspace.records.write(
    namespace=["support"],
    text="Checkout retry is safe after inventory lock timeout.",
    source_refs=[result.ref()],
)
matches = workspace.records.search(
    namespace=["support"],
    text="checkout retry safe",
    limit=5,
)
```

### 3.4 Build context

Context packs record what an agent selected for a prompt. That
makes future replay/debugging possible because the prompt context
has provenance.

```python
pack = session.context(max_events=20, include=["recent_events", "records"])
```

### 3.5 Health probe

Before you start writing, check the server is up. `/health/ready`
returns 200 when listeners are bound and at least one tablet is
claimed.

```
GET /health/ready
```

---

## 4. SDK shortcuts (Python, TypeScript)

Every SDK wraps the HTTP surface in the same shape. Pick one:

**Python** (`pip install agentdb`, or `pip install 'agentdb[async]'`):

```python
from agentdb import AgentDB

db = AgentDB("http://127.0.0.1:7878")
session = db.workspace("demo").agent("agent-1").session("ticket-42")
result = session.append({"role": "user", "text": "hi"}, fetch_last_n=5)
child = session.fork("ticket-42-branch-a")
```

**TypeScript** (`npm install agentdb` once published; for now
build from `clients/typescript/`):

```ts
import { AgentDB } from "agentdb";
const db = new AgentDB({ baseUrl: "http://127.0.0.1:7878" });
const workspace = await db.workspace("demo");
const agent = await workspace.agent("agent-1");
const session = await agent.session("ticket-42");
const result = await session.append({ role: "user", text: "hi" },
                                    { fetchLastN: 5 });
const child = await session.fork("ticket-42-branch-a");
```

---

## 5. MongoDB client compatibility

Prefer the native SDKs for full integrations. Use the MongoDB listener
for inspection, simple event/record writes, and clients that already
have a MongoDB driver.

Start the listener:

```bash
cargo run -p agentdb-server -- serve --dev --mongo-bind 127.0.0.1:7879
```

Safe MongoDB-client rules:

- Always connect with `directConnection=true`.
- If username/password auth is configured, use
  `authMechanism=PLAIN&authSource=%24external`.
- Do not generate SCRAM, role, user-management, or database-auth
  code. AgentDB's MongoDB listener supports only basic `PLAIN`
  authentication.
- Bind to localhost unless separate infrastructure provides
  access control and TLS.
- The MongoDB database name is the AgentDB tenant external key.
- Use only fixed collections: `sessions`, `events`, `records`,
  `artifacts`, `skills`.
- Do not create arbitrary collections such as `users`, `orders`,
  or `tasks`.
- Do not rely on `createIndexes`; it is accepted as a no-op.
- Do not generate `updateOne`, `deleteOne`, transactions, change
  streams, or arbitrary aggregation pipelines.
- Write events with `db.events.insertOne(...)` or
  `agentdbAppendAndFetch`.
- Use `db.runCommand(...)` for fork, replay, context, lifecycle,
  and restore.

Example with `mongosh`:

```javascript
use demo

db.events.insertOne({
  session_alias: "ticket-42",
  kind: "message",
  payload: {
    role: "user",
    content: "checkout failed"
  }
})

db.runCommand({
  agentdbAppendAndFetch: "events",
  session_alias: "ticket-42",
  kind: "message",
  payload: {
    role: "assistant",
    content: "I'll inspect the order state."
  },
  fetch_last_n: 20
})
```

Mental mapping:

```text
use demo                         -> tenant external_key "demo"
db.events.insertOne(...)         -> append one AgentDB event
session_alias                    -> create/reuse a session alias
omitted agent_id                 -> create/reuse agent alias "mongo-default"
db.runCommand({agentdbFork...})  -> AgentDB fork primitive
```

Detailed rules live in
[`docs/reference/mongodb-compatibility.md`](docs/reference/mongodb-compatibility.md).

---

## 6. Common mistakes

- **Forgetting `fetch_last_n` on the very first append.** A
  fresh session returns one event in `recent` (the one you just
  wrote). That's correct — it's not a bug.
- **Using `fork` and then writing to the parent.** Forks branch
  forward; both parent and child can keep accepting writes
  independently. Pick the right session_id on every subsequent
  call.
- **Mixing tenant in the path AND in auth.** When auth is on,
  the `Authorization` header IS the tenant identity. The path
  path and the tenant identity in auth must agree. Prefer SDK
  handles so this is managed for you.
- **Throwing away canonical IDs.** Aliases are for humans and
  apps. Persist the canonical `workspace.id`, `agent.id`, and
  `session.id` when you need exact recovery after restart.
- **Treating AgentDB as full MongoDB.** MongoDB client compatibility
  is a limited adapter. Do not generate arbitrary collections,
  indexes, update/delete, transactions, or change streams.

---

## 7. Errors you'll see

Every error response carries `{"error": {"code", "kind",
"message"}}` plus an HTTP status. SDKs map these to typed
exceptions. The ones an agent should know:

| HTTP | Meaning | What to do |
|---|---|---|
| 400 | bad request body | fix the body shape |
| 401 | missing/invalid auth | add `Authorization` |
| 403 | wrong tenant | re-check credentials match the session |
| 404 | tenant or session unknown | check the names |
| 421 | routing drift on a forwarded request | retry once; if persistent, ask an operator |
| 429 | quota exhausted | back off `retry_after_ms` and retry |
| 503 | node draining or fault injected | retry on a different node |

---

## 8. Where to go next

- **Try it locally**: `cargo run -p agentdb-server -- serve --dev`
  prints a banner with copy-pasteable curl commands.
- **Working examples**:
  [`examples/agents/python/quickstart.py`](examples/agents/python/quickstart.py)
  is a 60-line three-turn agent loop;
  [`examples/agents/python/async_quickstart.py`](examples/agents/python/async_quickstart.py)
  shows the async variant with parallel writes;
  [`examples/mongodb/node/quickstart.js`](examples/mongodb/node/quickstart.js)
  shows safe MongoDB client compatibility usage.
- **Why this database exists** (motivation, requirements,
  principles): [`docs/architecture/agentdb-product-thesis.md`](docs/architecture/agentdb-product-thesis.md).
- **What the architecture commits to** (identity, layering,
  real-time, deployment): [`docs/architecture/design.md`](docs/architecture/design.md).
- **What the code does today**:
  [`docs/architecture/implementation-architecture.md`](docs/architecture/implementation-architecture.md).
- **OpenAPI spec**: `GET /openapi.json` on a running server.
