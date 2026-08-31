# LLMS.md — AgentDB for LLM agents

This file is written for an LLM agent that's learning AgentDB
in one ingest. Sections are short, copy-pasteable, and ordered
by how often a working agent needs them. Human devs may prefer
[`README.md`](README.md) and
[`docs/design/architecture.md`](docs/design/architecture.md).

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

Branch into an alternate timeline. The child inherits the
parent's history up to the fork point (MVCC-backed, no copy);
subsequent writes diverge.

```python
branch = session.fork("ticket-42-branch-a")
branch.append({"role": "user", "text": "try the refund path"})
```

Know where inherited history shows up: `append`/`fetch` on the
child return only events written to the child itself (its working
tail), while **replay** (`GET .../sessions/{id}/replay`) and
**context packs** return the full merged history — inherited
parent prefix plus child events. Use replay/context when the
branch needs the pre-fork conversation.

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

Search is eventually consistent by default (an async indexer
feeds the global index). For read-your-writes — search right
after a write — pass the write's commit token: the HTTP write
response carries `X-AgentDB-Record-Commit-Seq`, and search
accepts it back as `min_visible_record_commit_seq`, which
forces a consistent view at least that fresh. SDK record
handles carry this token for you.

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

## 5. Common mistakes

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

---

## 6. Errors you'll see

Every error response is a flat JSON object:

```json
{"code": "SESSION_NOT_FOUND", "message": "…", "retryable": false}
```

`code` is a stable SCREAMING_SNAKE identifier, `retryable` tells
you whether the same request can simply be retried, and some
errors add a `suggestion` string with the fix. 429 and 503
responses also carry a `Retry-After` header (seconds) — honor it
before retrying. SDKs map these to typed exceptions. The ones an
agent should know:

| HTTP | Meaning | What to do |
|---|---|---|
| 400 | bad request body | fix the body shape |
| 401 | missing/invalid auth | add `Authorization` |
| 403 | wrong tenant for this bearer token | re-check credentials match the session |
| 404 | tenant or session unknown | check the names |
| 409 | alias/version/idempotency conflict | read the existing resource instead |
| 421 | routing drift on a forwarded request | retry once; if persistent, ask an operator |
| 422 | request understood but denied (quota policy, size caps) | read `message`; don't blind-retry |
| 429 | quota exhausted | back off `Retry-After` seconds and retry |
| 503 | node draining or fault injected | honor `Retry-After`, then retry (another node if you have one) |

---

## 7. Where to go next

- **Try it locally**: `cargo run -p agentdb-server -- serve --dev`
  prints a banner with copy-pasteable curl commands.
- **Working examples**:
  [`examples/agents/python/quickstart.py`](examples/agents/python/quickstart.py)
  is a 60-line three-turn agent loop;
  [`examples/agents/python/async_quickstart.py`](examples/agents/python/async_quickstart.py)
  shows the async variant with parallel writes.
- **Why this database exists** (motivation, requirements,
  principles): [`docs/design/product-thesis.md`](docs/design/product-thesis.md).
- **What the architecture commits to** (identity, layering,
  real-time, deployment): [`docs/design/design.md`](docs/design/design.md).
- **What the code does today**:
  [`docs/design/architecture.md`](docs/design/architecture.md).
- **Full HTTP surface**: [`docs/reference/`](docs/reference/) and
  the route list in
  [`crates/agentdb-api/src/routes.rs`](crates/agentdb-api/src/routes.rs).
