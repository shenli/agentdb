# LLMS.md — AgentDB for LLM agents

This file is written for an LLM agent that's learning AgentDB
in one ingest. Sections are short, copy-pasteable, and ordered
by how often a working agent needs them. Human devs may prefer
[`README.md`](README.md) and [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

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

AgentDB is an **agent-native operational database**. It stores
the events your agent produces (thoughts, tool calls, tool
outputs, user turns) in **sessions**. The same database also
serves the **retrieval** an agent needs at the next turn (text
+ vector search) over those same events. One system, one wire.

You talk to it two ways:

- **REST** at `http://<host>:7878` — the canonical surface for
  writes (`append_and_fetch`) and forks. Every official SDK
  uses this.
- **MongoDB wire protocol** at `mongodb://<host>:7879` — every
  Mongo driver works. Use it for retrieval (`$search`,
  `$vectorSearch`) and ad-hoc reads.

There is no Postgres tier, no separate vector store, no
external indexer. Append, retrieve, fork — same database.

---

## 2. Core concepts

| term | one-line definition |
|---|---|
| **tenant** | Top-level isolation unit. Auth-bound (Basic Auth → tenant). One tenant per app/customer/etc. |
| **session** | A linear sequence of events under a tenant. The unit of working memory for an agent. Identified by an external `session_id` you choose. |
| **event** | One JSON object you appended. Server assigns `seq` (monotonic) and `ts_ms`. Your data lives under `payload`. |
| **`append_and_fetch`** | The hot-path primitive: write one event, return the last N events of the same session, in one round trip. |
| **fork** | Create a child session that inherits the parent's events at fork time, MVCC-backed. Use to branch into "what if" timelines without copying. |
| **hybrid retrieval** | `$search` (BM25 over `payload.<field>`) + `$vectorSearch` (cosine over `payload.<embedding-field>`) over the same event log. |
| **tier** | Each session is `Warm` → `Cold` → `Archive` over its lifetime. Reads are tier-transparent: you don't query differently. |

---

## 3. The five operations

> Defaults below assume the dev-mode server: `agentdb serve --dev`,
> tenant `demo`, session `agent-1`. Swap as needed.

### 3.1 Append an event (and read recent context)

The single most-used operation. Every turn writes *something*
and almost always wants the recent history for context.

**REST**:

```
POST /api/tenants/{tenant}/sessions/{session}/append_and_fetch
Content-Type: application/json

{
  "event":       <any JSON>,
  "fetch_last_n": 5,
  "actor_id":     null
}
```

**Response**:

```json
{
  "seq": 7,
  "recent": [
    {"seq": 3, "ts_ms": 1714060000000, "payload": {...}, "actor_id": null},
    {"seq": 4, "ts_ms": 1714060000123, "payload": {...}, "actor_id": null},
    ...
  ]
}
```

`fetch_last_n: 0` skips the read-back (cheaper if you don't
need context).

**Auth-bound shorthand** (when `AGENTDB_REQUIRE_AUTH=true`):
`POST /api/sessions/{session}/append_and_fetch` — tenant comes
from your `Authorization: Basic <user:password>` header.

### 3.2 Fork a session

Branch into an alternate timeline. The child sees the parent's
events at fork time; subsequent writes diverge.

```
POST /api/sessions/{parent_session}/fork
Content-Type: application/json

{ "new_session_id": "agent-1-branch-a" }
```

**Response**:

```json
{
  "new_session_id":      "agent-1-branch-a",
  "parent_session_id":   "agent-1",
  "parent_seq_at_fork":  6
}
```

After the fork, write to the child via `append_and_fetch` with
`session_id = "agent-1-branch-a"`. The fork is metadata-only;
no events are copied.

### 3.3 Text search over past events (BM25)

Use the Mongo wire. Path is `payload.<field>` because everything
you appended lands under `payload`.

```js
db.events.aggregate([
  { $search: {
      text: { query: "refund", path: "payload.text" },
      limit: 10
  }}
])
```

Returns a cursor of event documents whose `payload.text` field
matches the query. Order is BM25 score, descending.

### 3.4 Vector search over past events

Same wire, same collection. Embed the query yourself; AgentDB
doesn't run a model.

```js
db.events.aggregate([
  { $vectorSearch: {
      queryVector:   [0.13, -0.42, ..., 0.07],   // 16+ dims
      path:          "payload.embedding",
      limit:          5,
      numCandidates: 50
  }}
])
```

Returns a cursor of event documents ranked by cosine similarity
to `queryVector`.

### 3.5 Health probe

Before you start writing, check the server is up. `/health/ready`
returns 200 when listeners are bound and at least one tablet is
claimed.

```
GET /health/ready
```

---

## 4. SDK shortcuts (Python, TypeScript, Go)

Every SDK wraps the REST surface in the same shape. Pick one:

**Python** (`pip install agentdb`, or `pip install 'agentdb[async]'`):

```python
from agentdb import Client
client = Client("http://127.0.0.1:7878")
session = client.session("agent-1", tenant="demo")
result = session.append({"role": "user", "text": "hi"}, fetch_last_n=5)
child = session.fork("agent-1-branch-a")
```

**TypeScript** (`npm install agentdb` once published; for now
build from `clients/typescript/`):

```ts
import { Client } from "agentdb";
const client = new Client("http://127.0.0.1:7878");
const session = client.session("agent-1", { tenant: "demo" });
const result = await session.append({ role: "user", text: "hi" },
                                    { fetchLastN: 5 });
const child = await session.fork("agent-1-branch-a");
```

**Go** (`go get github.com/shenli/agentdb/clients/go/agentdb`):

```go
client := agentdb.New("http://127.0.0.1:7878",
    agentdb.WithDefaultTenant("demo"))
sess := client.Session("agent-1")
res, _ := sess.Append(ctx,
    map[string]any{"role": "user", "text": "hi"},
    agentdb.AppendOptions{FetchLastN: 5})
child, _ := sess.Fork(ctx, "agent-1-branch-a")
```

---

## 5. Common mistakes

- **Searching the wrong path.** Your data is at
  `payload.<field>`, not `<field>`. `$search.text.path` should
  be `"payload.text"`, not `"text"`.
- **Forgetting `fetch_last_n` on the very first append.** A
  fresh session returns one event in `recent` (the one you just
  wrote). That's correct — it's not a bug.
- **Using `fork` and then writing to the parent.** Forks branch
  forward; both parent and child can keep accepting writes
  independently. Pick the right session_id on every subsequent
  call.
- **Mixing tenant in the path AND in auth.** When auth is on,
  the `Authorization` header IS the tenant identity. The path
  variant `/api/sessions/{sid}/append_and_fetch` is the
  v2-clean shape. The legacy `/api/tenants/{tid}/...` still
  works but the auth middleware enforces match.
- **Treating Mongo wire reads as a separate database.** It's
  the same data — events you wrote via REST are immediately
  searchable via `$search` / `$vectorSearch`. No syncing step.

---

## 6. Errors you'll see

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

## 7. Where to go next

- **Try it locally**: `cargo run -p agentdb-server -- serve --dev`
  prints a banner with copy-pasteable curl commands.
- **Working examples**:
  [`examples/agents/python/quickstart.py`](examples/agents/python/quickstart.py)
  is a 60-line three-turn agent loop;
  [`examples/agents/python/async_quickstart.py`](examples/agents/python/async_quickstart.py)
  shows the async variant with parallel writes.
- **Full architecture** (for human devs):
  [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
- **Design rationale**: [`docs/agent-db.md`](docs/agent-db.md)
  explains *why* an agent-native database differs from an
  OLTP DB.
- **OpenAPI spec**: `GET /openapi.json` on a running server.
