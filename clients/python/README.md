# agentdb (Python)

Python client for [AgentDB](https://github.com/shenli/agentdb) — agent-native
operational database.

## Install

From source (recommended while AgentDB is pre-1.0):

```bash
pip install -e clients/python
```

PyPI publish lands once the server hits 0.1.0 stable.

## Five-line hello world

Start the server in dev mode (zero config, ephemeral state):

```bash
cargo run -p agentdb-server -- serve --dev
```

In another shell:

```python
from agentdb import Client

client = Client("http://127.0.0.1:7878")
session = client.session("agent-1", tenant="demo")
result = session.append({"role": "user", "text": "hello"}, fetch_last_n=5)
print("new seq:", result.seq)
for ev in result.recent:
    print(ev.seq, ev.payload)
```

## Auth mode

When the server is started with `AGENTDB_REQUIRE_AUTH=true`, supply
credentials at construction. Tenant comes from the credentials —
no need to pass it explicitly.

```python
client = Client("https://prod.example.com", user="alice", password="hunter2")
session = client.session("agent-1")  # tenant auth-derived
session.append({"role": "tool", "text": "did a thing"})
```

## Fork

```python
parent = client.session("agent-1", tenant="demo")
parent.append({"role": "user", "text": "hello"})
child = parent.fork("agent-1-branch-a")  # returns a Session pointing at the new id
child.append({"role": "tool", "text": "branch off"})
```

The child sees a frozen snapshot of the parent's events at fork
time, MVCC-backed — see the architecture doc for the semantics.

## Errors

Every server-surfaced failure raises an `AgentDBError` subclass:

| HTTP | Class           | Typical cause                                  |
|------|-----------------|------------------------------------------------|
| 400  | `BadRequest`    | malformed body, tenant resolution failed       |
| 401  | `Unauthorized`  | missing/invalid `Authorization`                |
| 403  | `Forbidden`     | authed user can't act on this tenant/session   |
| 404  | `NotFound`      | tenant or session doesn't exist                |
| 409  | `Conflict`      | resource already exists / state collision      |
| 421  | `Misdirected`   | multi-node routing drift                        |
| 429  | `RateLimited`   | quota exhausted; carries `retry_after_ms`      |
| 5xx  | `ServerError`   | forwarding failure, fault injection, draining |

Catch `AgentDBError` at the top of an agent loop for blanket
handling, or a subclass when reacting to a specific category.

## Why no `Session.recent(n)` for pure reads?

`append + fetch_last_n` is intentional — every agent turn writes
*something* (a thought, a tool result, an observation) and wants
the recent-N for context. Combining the two saves a round trip
and is the documented hot path.

For genuine read-only queries (dashboards, replays, audit), point
a MongoDB driver at the wire port:

```python
import pymongo
mongo = pymongo.MongoClient("mongodb://127.0.0.1:7879")
events = list(mongo["demo"]["events"].find({"session_id": "agent-1"}).limit(10))
```

## Testing the SDK

The smoke test spawns `agentdb serve --dev` as a subprocess and
round-trips an event. Build the server first:

```bash
cargo build -p agentdb-server
pytest clients/python
```

The test is skipped if `cargo build` hasn't been run.
