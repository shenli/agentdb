# AgentDB

The **agent-native operational database**. AgentDB stores your
agent's events (thoughts, tool calls, user turns) in sessions,
and serves the retrieval the agent needs at the next turn —
text + vector search — over the same events. One system, one
wire, no separate vector store.

🚀 [Install](#install) · 🏃 [Quickstart](#quickstart--your-first-agent-in-three-commands) · 🤖 [For AI agents](#-for-ai-agents-using-agentdb) · 🏗 [Architecture](docs/ARCHITECTURE.md)

---

## What it's for

You're building agents — chatbots, research assistants, sales
bots, multi-step automations. Each agent session emits a
stream of events: user turns, model outputs, tool calls,
observations. You need a place to store that stream durably,
retrieve past events as context for the next turn, scale to
many concurrent agents, and react to other agents' events in
real time.

Most teams glue this together: Postgres for events, pgvector
for vectors, Redis for the session cache, an event-bus for
real-time. AgentDB is one system that does all of it.

## What's different

- **Sessions, not rows.** A session is the agent's working
  memory — an append-only event log with a built-in lifecycle
  (warm → cold → archive) and copy-free fork ("what if I
  tried this branch instead?"). Retrieval is scoped to a
  session by default.
- **Hybrid retrieval over your own log.** BM25 text + HNSW
  vector search across the events you appended. One write,
  two indexes; no second database to sync.
- **Multi-tenant from the start.** Per-tenant isolation,
  quotas, and admission control are baked in. Built for
  platforms that host many users' agents — not retrofitted
  from "single Postgres per team."
- **Real-time event subscriptions.** Stream a session's
  events to a browser or service as they land, with
  replay-from-seq + gap-free reconnection. Plus a separate
  ephemeral broadcast channel for transient signals (typing
  indicators, presence pings).
- **One-round-trip primitives.** Operations like
  `append_and_fetch` (write an event AND read the last N for
  context, in one call) match the agent loop's actual shape.

Operator-grade extras — live dashboard, Prometheus metrics,
S3-backed storage, multi-node clustering, structured audit
log — live alongside. See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## 🤖 For AI agents using AgentDB

If you're an LLM-driven agent reading this repo to learn the
API, jump straight to **[`LLMS.md`](LLMS.md)** — a single-page
manual designed to be ingested in one shot. It covers the five
operations you'll actually use, the request / response shapes,
and the common mistakes.

(Naming: `AGENTS.md` at the repo root is the established
Claude Code / Cursor convention for "how to *develop in* this
repo" — build commands, crate layout, MSRV. The runtime-API
manual lives separately in `LLMS.md` so the two audiences
don't trip over the same filename.)

---

## Install

Three options, fastest first:

```bash
# A. Precompiled binary (Linux + macOS, x86_64 + aarch64).
#    Detects platform, downloads from GitHub Releases, installs to
#    ~/.local/bin (override with AGENTDB_PREFIX=/usr/local/bin).
curl -sSL https://raw.githubusercontent.com/shenli/agentdb/main/scripts/install.sh | sh

# B. Docker — multi-arch image on GHCR.
docker run --rm -p 7878:7878 -p 7879:7879 ghcr.io/shenli/agentdb:latest serve --dev

# C. From source. `rust-toolchain.toml` pins the version, so
#    rustup auto-installs the right toolchain on first build.
cargo install --git https://github.com/shenli/agentdb agentdb-server
```

Option A and B require a published release; until v0.1.0 ships,
use option C or build from a clone (`cargo build -p agentdb-server`).

## Quickstart — your first agent in three commands

```bash
# 1. Start the server (zero config, ephemeral state)
agentdb serve --dev
# (or: cargo run -p agentdb-server -- serve --dev)

# 2. In another shell, install a client SDK
pip install -e clients/python                                          # Python
(cd clients/typescript && npm install && npm run build)                # TypeScript
(cd examples/agents/go && go build .)                                  # Go (uses local replace)

# 3. Run a sample agent loop in your language of choice
python examples/agents/python/quickstart.py
node --experimental-strip-types examples/agents/typescript/quickstart.ts
(cd examples/agents/go && go run .)
```

You should see three turns of working memory grow, then a fork
that branches the session. That's the canonical AgentDB shape:
write one event, read the recent N back in the same round trip.

The `serve --dev` startup banner prints listen addresses,
dashboard URL, and a copy-pasteable `curl` so you can poke at
the REST surface without reading further. Open
<http://127.0.0.1:7878/dashboard> for the live operator view.

### Five-line client

**Python** ([`clients/python/`](clients/python/README.md)) — sync:

```python
from agentdb import Client

client = Client("http://127.0.0.1:7878")
session = client.session("agent-1", tenant="demo")
result = session.append({"role": "user", "text": "hello"}, fetch_last_n=5)
print("seq:", result.seq, [(e.seq, e.payload) for e in result.recent])
```

…or async (`pip install 'agentdb[async]'`) for agent frameworks
that are async-first:

```python
from agentdb import AsyncClient

async with AsyncClient("http://127.0.0.1:7878") as client:
    session = client.session("agent-1", tenant="demo")
    result = await session.append({"role": "user", "text": "hello"},
                                  fetch_last_n=5)
```

**TypeScript / Node** ([`clients/typescript/`](clients/typescript/README.md)):

```ts
import { Client } from "agentdb";

const client = new Client("http://127.0.0.1:7878");
const session = client.session("agent-1", { tenant: "demo" });
const result = await session.append(
  { role: "user", text: "hello" },
  { fetchLastN: 5 }
);
console.log("seq:", result.seq, result.recent.map((e) => e.payload));
```

**Go** ([`clients/go/`](clients/go/README.md)):

```go
import "github.com/shenli/agentdb/clients/go/agentdb"

client := agentdb.New("http://127.0.0.1:7878",
    agentdb.WithDefaultTenant("demo"))
sess := client.Session("agent-1")
res, _ := sess.Append(ctx,
    map[string]any{"role": "user", "text": "hello"},
    agentdb.AppendOptions{FetchLastN: 5})
fmt.Println("seq:", res.Seq, "recent:", len(res.Recent))
```

A Rust client crate is the natural follow-up; in the meantime
any MongoDB driver speaks the wire port at
`mongodb://127.0.0.1:7879`.

### Operator subcommands

Once the server is up, the same `agentdb` binary doubles as an
admin CLI for the running process:

```bash
agentdb cluster-status     # leader + nodes + tablets in one call
agentdb show-scheduler
agentdb show-tablets
agentdb show-nodes

agentdb drain              # graceful shutdown
agentdb evict-node <node>  # remove a dead node from the control plane

agentdb dump-config        # what the server would run with
```

For production-shaped config, run `agentdb serve` without `--dev`
and set the relevant env vars (see [Configuration](#configuration-env-vars)
below). Env vars still work on every invocation — the clap flags are
optional overrides, not replacements.

### Synthetic workload

```bash
cd workload && go run ./cmd/agentdb-workload -config config.yaml
```

Hit the Go workload's control server on `:9090` (`/demo/burst`,
`/demo/fork`, `/demo/cold`) for scripted load shaping.

### Docker compose (SlateDB + MinIO)

A production-shape stack with MinIO as the S3-compatible backend
lives under [`examples/compose/`](examples/compose/):

```bash
cd examples/compose
docker compose up --build
# REST + dashboard + /metrics on :7878, Mongo wire on :7879,
# MinIO console on :9001 (minio / minio123)
docker compose exec agentdb agentdb show-scheduler
```

Single-node by default; multi-node works (see the compose
`README.md` for how to add a peer). Use `agentdb serve --dev`
for the fastest local dev loop; the compose stack is for
operators evaluating the production storage path without
provisioning real S3.

### Prometheus metrics

Every running server exposes `/metrics` in Prometheus text format:

```bash
curl -s http://127.0.0.1:7878/metrics | grep agentdb_appends_total
# agentdb_appends_total{box="0"} 42
# agentdb_appends_total{box="1"} 37
# ...
```

Point Prometheus / Grafana Agent / VictoriaMetrics at it — no
extra config needed. See
[`docs/ARCHITECTURE.md` § Observability](docs/ARCHITECTURE.md#observability)
for the full metric catalog.

Per-box latency histograms (`agentdb_append_latency_seconds`,
`agentdb_admission_wait_seconds`) ride the same endpoint.
`GET /api/admin/boxes/:id/latency` and `agentdb show-latency <box>`
return pre-computed p50/p95/p99 for ad-hoc inspection without a
Prometheus query.

### Production shape (SlateDB + S3)

```bash
# Prereqs: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY in env
AGENTDB_OBJECT_STORE=s3 \
AGENTDB_S3_BUCKET=my-bucket \
AGENTDB_S3_REGION=us-east-1 \
AGENTDB_KV_BACKEND=slatedb \
AGENTDB_SLATEDB_PATH=agentdb/slate \
cargo run --release -p agentdb-server
```

See [`docs/S3.md`](docs/S3.md) for the full MinIO / S3 recipe.

---

## API tour

### REST (native)

```
POST /api/tenants/{tenant}/sessions/{session}/append_and_fetch
POST /api/sessions/{session}/fork
POST /api/admin/tier/force                # target: "warm"|"cold"|"archive"
POST /api/admin/boxes/:id/inject_fault
POST /api/admin/boxes/:id/quota           # max_qps, max_tenant_qps, burst, max_warm_sessions
POST /api/admin/boxes/:id/webhook         # {"url": "..."} or {"url": null}
GET  /api/admin/boxes                     # per-box snapshot
GET  /api/admin/boxes/:id
GET  /api/admin/sessions/:session_id?tenant_id=...
GET  /api/stream                          # SSE live event feed
```

### MongoDB wire protocol (OP_MSG subset)

Point an official Mongo driver at `mongodb://127.0.0.1:7879/` and
use the tenant as your database name:

```js
// insert
db.getSiblingDB("t-1").events.insertOne({ session_id: "s-1", kind: "message", text: "hi" })

// find by session
db.getSiblingDB("t-1").events.find({ session_id: "s-1" }).limit(50)

// hybrid retrieval via $search
db.getSiblingDB("t-1").events.aggregate([
  { $search: { text: { path: "payload.text", query: "refund" } } },
  { $limit: 10 }
])
```

Supported commands: `hello`, `insert`, `find`, `aggregate` (with
`$search` + `$vectorSearch`), `update`, `delete`,
`listDatabases`, `listCollections`. Transactions return a
standard "not implemented" error.

Because AgentDB is an event log, `update` and `delete` don't
literally mutate or remove events. `update` appends a
**correction event** (merged payload + `_update_of: <seq>`
marker); `delete` appends a **tombstone event**
(`_tombstone_of: <seq>`). Both require a `session_id` in
the filter. **`find` default-folds** both so
`insertOne → updateOne → find` returns the post-update
payload as you'd expect; `{raw: true}` in the find body
opts out and returns the un-folded log. See
`docs/ARCHITECTURE.md § Mongo CRUD semantics` for the full
contract.

### Real-time subscriptions (SSE)

Server-Sent Events stream of events appended to a session, with
`from_seq` replay for gap-free reconnect. Browser-direct via JWT
(no application backend in the loop); backend-direct via Basic
auth.

```
GET /api/tenants/{tenant}/sessions/{session}/subscribe
       ?from_seq=<u64>            # optional; default = "live tail only"
       &kind=<string>             # optional; repeatable kind filter (OR'd)
       &heartbeat_secs=<u64>      # optional; SSE keepalive interval (default 20)
       &access_token=<jwt>        # browsers using EventSource

GET /api/tenants/{tenant}/subscribe
       ?session=<id>              # optional; repeatable session whitelist
       &kind=<kind>               # optional; repeatable kind filter
       &from_seq_per_session=s-1:42,s-2:7   # optional; per-session catch-up
       &heartbeat_secs=<u64>
       &access_token=<jwt>
```

**TypeScript** (browser-direct, `getToken` refreshes on every
reconnect so `jwt_expired` is recovered transparently):

```ts
import { Client } from "agentdb";

const client = new Client("https://agentdb.example.com", {
  tenant: "t-1",
  getToken: async () => await myAuth.getJwt(),
});
const session = client.session("s-1");

let lastSeen: number | undefined;
for await (const event of session.subscribe({ fromSeq: lastSeen })) {
  ui.render(event);
  lastSeen = event.seq;
}
```

**Python** (sync; backend or scripts):

```python
from agentdb import Client

client = Client("http://127.0.0.1:7878", tenant="t-1")
session = client.session("s-1")
for event in session.subscribe(from_seq=42):
    handle(event)
```

**Go**:

```go
import "github.com/shenli/agentdb/clients/go/agentdb"

client := agentdb.New("http://127.0.0.1:7878",
    agentdb.WithDefaultTenant("t-1"))
sess := client.Session("s-1")
events, errs := sess.Subscribe(ctx, agentdb.SessionSubscribeOptions{})
for evt := range events {
    handle(evt)
}
if err := <-errs; err != nil { /* SubscriptionClosed or transport error */ }
```

#### Why this exists, and why it's free

`append_and_fetch` already writes a durable, seq-numbered event to
the session log. The subscribe endpoint just tails that log live
(in-process broadcast bus → SSE) with `from_seq=N` for gap-free
reconnect. No replication slot, no separate fanout cluster, no
auxiliary table. The same primitive serves the write and the
change feed.

Comparison with Supabase Realtime's Postgres Changes:

- Supabase: 4-hop pipeline (table → WAL → logical decode →
  Realtime cluster → WS) at separate billing.
- AgentDB: 1-hop (append → bus → SSE) in the database process.
- AgentDB has gap-free `from_seq` replay; Supabase doesn't.

#### JWT browser-direct

Set per-tenant JWT and CORS via env vars at server boot:

```
AGENTDB_TENANT_T_1_JWT_HMAC_SECRET=<secret>          # OR
AGENTDB_TENANT_T_1_JWT_JWKS_URL=https://auth.example.com/.well-known/jwks.json
AGENTDB_TENANT_T_1_JWT_ISSUER=https://auth.example.com
AGENTDB_TENANT_T_1_JWT_AUDIENCE=agentdb              # optional
AGENTDB_TENANT_T_1_CORS_ORIGINS=https://app.example.com,https://staging.example.com
```

JWT claims required: `iss`, `sub`, `tenant`, `sessions` (`"*"`
or `["s-1", ...]`), `exp`. The official SDKs auto-reconnect with
`from_seq` on `jwt_expired` — gap-free across token refresh.

See [`examples/browser/`](examples/browser/) for a runnable demo
(static HTML + EventSource + a tiny `mint_token.py` helper).

#### Ephemeral broadcast channels

For transient signals (typing indicators, cursor positions, agent-status
pings) that should NOT be persisted in the session log, use the broadcast
endpoints:

```
POST /api/tenants/{tenant}/channels/{channel}/publish
GET  /api/tenants/{tenant}/channels/{channel}/subscribe
```

Tenant-scoped channels, in-memory fan-out. No durability, no replay — slow
subscribers silently drop messages. Per-channel JWT auth via the `channels`
claim (alongside `sessions`):

```json
{ "tenant": "t-1", "sessions": "*", "channels": ["room:42", "room:43"] }
```

**SDKs:**

```ts
import { Client } from "agentdb";

const client = new Client("https://agentdb.example.com", { tenant: "t-1" });

await client.broadcast({ channel: "room:42", payload: { kind: "typing" } });
for await (const msg of client.subscribeChannel({ channel: "room:42" })) {
  handle(msg);
}
```

```python
from agentdb import Client

client = Client("http://127.0.0.1:7878", tenant="t-1")

client.broadcast(channel="room:42", payload={"kind": "typing"})
for msg in client.subscribe_channel(channel="room:42"):
    print(msg.payload)
```

```go
import "github.com/shenli/agentdb/clients/go/agentdb"

client := agentdb.New("http://127.0.0.1:7878",
    agentdb.WithDefaultTenant("t-1"))

_ = client.Broadcast(ctx, "room:42", payload)
msgs, errs := client.SubscribeChannel(ctx, "room:42")
for msg := range msgs { handle(msg) }
if err := <-errs; err != nil { /* transport error */ }
```

If you need durability or replay, use `append_and_fetch` + the durable
session subscribe instead — the two paths intentionally don't blur.

See [`examples/agents/typescript/broadcast_typing.ts`](examples/agents/typescript/broadcast_typing.ts) for a runnable typing-indicator demo.

---

## Configuration (env vars)

| Variable | Default | What it does |
|---|---|---|
| `AGENTDB_ADDR` | `127.0.0.1:7878` | REST bind address |
| `AGENTDB_MONGO_ADDR` | `127.0.0.1:7879` | Mongo wire bind address |
| `AGENTDB_BOXES` | `4` | Number of logical boxes (FNV-routed from tenant id) |
| `AGENTDB_KV_BACKEND` | `rocksdb` | `rocksdb` (local) or `slatedb` (object-store-backed) |
| `AGENTDB_DATA` | `./data/rocksdb` | RocksDB path (ignored when backend=slatedb) |
| `AGENTDB_SLATEDB_PATH` | `agentdb/slate` | SlateDB prefix within the object store |
| `AGENTDB_OBJECT_STORE` | `local` | `local` (filesystem) or `s3` |
| `AGENTDB_OBJECTS` | `./data/objects` | Local object store root (ignored when `=s3`) |
| `AGENTDB_S3_BUCKET` | — | Required when `AGENTDB_OBJECT_STORE=s3` |
| `AGENTDB_S3_REGION` | `us-east-1` | S3 region |
| `AGENTDB_S3_ENDPOINT` | — | MinIO / custom S3 endpoint (forces path-style) |
| `AGENTDB_TICK_SECS` | `2` | Lifecycle sweep interval |
| `AGENTDB_T_COLD_SECS` | `60` | Warm → cold idle threshold |
| `AGENTDB_T_ARCHIVE_SECS` | `600` | Cold → archive idle threshold |
| `AGENTDB_SEGMENT_GC_TICK_SECS` | `5` | Segment GC sweep interval |
| `AGENTDB_SEGMENT_GC_GRACE_SECS` | `30` | Grace period before GC reclaims an unpinned segment |
| `AGENTDB_TICK_MS` | `1000` | SSE `MetricsTick` broadcast interval |
| `AGENTDB_HNSW_EF` | `100` | HNSW `ef_construction` + `ef_search` (vector recall knob) |
| `AGENTDB_META_BACKEND` | `sqlite` | `sqlite` (default) or `postgres`. Selects the meta-store dialect |
| `AGENTDB_META_PATH` | `meta.sqlite` | SQLite file path (ignored on postgres) |
| `AGENTDB_META_URL` | — | Required when `AGENTDB_META_BACKEND=postgres` (`postgres://user:pw@host/db`) |
| `AGENTDB_NODE_REAPER_TICK_SECS` | `10` | How often the leader scans `nodes` for failure + GC |
| `AGENTDB_NODE_DEAD_AFTER_SECS` | `30` | Stale `alive` rows older than this flip to `dead` |
| `AGENTDB_NODE_GC_AFTER_SECS` | `3600` | `dead` rows older than this are removed from `nodes` |
| `AGENTDB_NODE_AZ` | — | Availability zone label (e.g. `us-east-1a`). Placement prefers cross-AZ targets when two or more AZs are declared |
| `AGENTDB_LOCK_GC_TICK_SECS` | `60` | How often the lock-map GC sweeps idle session/tenant lock entries |
| `AGENTDB_PLACEMENT_TICK_SECS` | `5` | How often the leader scans `tablets` for unowned rows to assign |
| `AGENTDB_RECONCILER_TICK_SECS` | `3` | How often each node picks up new placements via `add_box` |
| `AGENTDB_CLUSTER_TABLETS` | `AGENTDB_BOXES` | Tablets to seed on a fresh cluster (cluster-wide, not per-process) |

AWS creds use the standard env chain (`AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`).

---

## Repo layout

```
bin/agentdb-server           main binary (serves REST + Mongo wire)
crates/agentdb-storage       KvStore trait, RocksKvStore, SlateKvStore
crates/agentdb-core          types, registries, primitives, lifecycle,
                             fork, tiering, MVCC read contexts, admission
crates/agentdb-index         per-box BM25 + HNSW vector indexes
crates/agentdb-wire          MongoDB wire protocol subset (OP_MSG)
crates/agentdb-api           native REST, SSE, webhook delivery
crates/agentdb-dashboard     static HTML/CSS/JS for /dashboard
workload/                    Go workload generator + demo control server
scripts/                     smoke.sh, smoke-s3.sh, perf runners
docs/                        design, architecture, performance, S3 guide
```

---

## Reading order

- **Why this database exists** (motivation, workload requirements,
  design principles):
  [`docs/agent-db.md`](docs/agent-db.md)
- **What the architecture commits to** (identity model, layering,
  clock, failure model, real-time, deployment, operability):
  [`docs/DESIGN.md`](docs/DESIGN.md)
- **What the code actually does today** (crate layout, request
  flow, storage layout):
  [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- **How fast it runs**:
  [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md) — laptop numbers
  for throughput, vector search, tier transitions, admission
  overhead, snapshot reads, KV backend comparison
- **How to deploy against S3 / MinIO**:
  [`docs/S3.md`](docs/S3.md) — SlateDB + MinIO / S3 walkthrough

---

## Tests

```bash
cargo test --workspace                                # ~222 tests across 20 binaries
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
bash scripts/smoke.sh                                 # boot server, append, fetch dashboard
bash scripts/smoke-s3.sh                              # same, but against MinIO + SlateDB
cd workload && go build ./... && go vet ./...
cargo bench -p agentdb-index --bench vector_bench     # HNSW vs brute-force vector search
```

---

## Dashboard screenshot

<!-- TODO: capture with server running + workload driving traffic.
     Drop the PNG at docs/screenshots/dashboard.png and reference it
     here. The box grid shows warm/cold/archive dots per box; the
     right-side event feed streams TierChange / ForkCreated /
     FaultInjected / QuotaHit / SegmentGcRun entries. -->

![dashboard](docs/screenshots/dashboard.png)

---

## Status

> AgentDB is an open-source prototype. It runs as a single
> process or a small multi-node cluster, with a pluggable
> storage backend. The goal is to prove the agent-native
> primitives compose — not to replace your Postgres.

### What works today

The agent-native primitives (append-and-fetch, fork, hybrid
retrieval, three-tier lifecycle, MVCC snapshot reads, HLC,
pluggable KV) are all implemented `[implemented]`. Around them,
the project has grown the platform pieces a real deployment needs:

- **Multi-node clustering.** `[not battle-tested]` SQLite or
  Postgres control plane (tenants, nodes, tablets, shards,
  scheduler leader). Tablet leases with fencing tokens;
  scheduler-driven placement + reconciliation; cross-AZ-aware
  assignment. Cross-node request forwarding for both REST and
  Mongo wire.
- **Auth & isolation.** `[implemented]` Optional SCRAM-SHA-256
  on the Mongo wire and HTTP Basic on REST, with auth-bound
  tenant identity. Optional admin-plane bearer auth with RBAC
  scopes (`admin` / `readonly`) and an audit log of every
  admin request. TLS termination on both ports (or
  `--dev-tls` for ephemeral self-signed).
- **Real-time pub/sub.** `[implemented]` Durable, replayable
  session-event SSE with `from_seq` replay + gap-free
  reconnect, plus an ephemeral broadcast channel for transient
  signals (typing, cursor positions, presence pings).
- **Hot-tablet handling.** `[not battle-tested]` Sliding
  rate-window detector, manual or automatic shed
  (`AGENTDB_AUTO_SHED_ENABLED=true`) that chains routing with
  per-session warm-data migration, dynamic tablet creation at
  runtime.
- **Operability.** `[not battle-tested]` Live dashboard,
  Prometheus `/metrics` with per-box latency histograms,
  structured `/health/ready` with runbook URLs, point-in-time
  backup (manual + leader-gated periodic), config hot-reload,
  slow-query log, global per-tenant rate limits, full admin CLI.
- **Packaging.** `[not battle-tested]` Multi-stage `Dockerfile`,
  compose stack with MinIO, release pipeline producing
  per-platform binaries, install script.
- **Client SDKs.** `[implemented]` Python (sync + async),
  TypeScript / Node, Go — all stdlib-only or single-dep, with
  a matching quickstart example each.

### Explicit non-goals

These will not be added — by design — and the wrong adopter
should choose a different tool:

- **Cross-box (distributed) transactions.** The design is
  single-tablet, 2PC-free.
- **LLM / embedding generation.** Caller's responsibility;
  AgentDB stores the events and serves retrieval.
- **General-purpose search.** The BM25 indexer is
  intentionally vanilla (whitespace + lowercase, no
  language models). Use Elasticsearch / Tantivy / Lucene
  for real search workloads.

### Known limitations

What's missing or partial in the current prototype, with
honest framing:

- **Cross-node Mongo wire forwarding under auth.** Cross-node
  forwarding works in unauth mode (the dev default). With
  `AGENTDB_REQUIRE_AUTH=true`, SASL session state can't
  cross a hop, so the handler returns
  `NotWritablePrimary` with the owning peer's address and
  the driver reconnects. Auth-aware forwarding needs an
  inter-node trust model.
- **Cross-node / cold-session warm migration.**
  `migrate-session` (and `shed --migrate-data`) handle
  warm sessions co-located in the same process's KV.
  Cross-node migration of warm sessions, and migration of
  cold/archive sessions, would need snapshot shipping +
  object-store path rewrites.
- **Cross-node `create-tablet`.** The admin endpoint
  creates the tablet on the node receiving the call.
  Spawning on a specific peer requires posting to that
  peer's admin URL; automatic placement-aware creation
  is future work.
- **Placement policy beyond cross-AZ.** Cross-AZ
  preference works. Anti-affinity (don't co-locate
  tablets X and Y) and node-weight awareness
  (heterogeneous hardware) are not yet implemented.
- **Tablet deletion / shrink.** Once created, a tablet
  stays. Shrinking the pool would require data migration.
- **Admin-plane mTLS.** Bearer-token auth + RBAC ships;
  mTLS-based admin auth is a follow-up.
- **`listDatabases` / `listCollections` cross-tenant
  fanout.** Still tenant-hash routed in a multi-node
  cluster. Most agent workloads don't hit this.
- **ANN at 100M+ vectors per node.** HNSW is single-node,
  in-process. Vertical scaling within a single tablet
  has a ceiling.

The full picture lives in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
("What the prototype does NOT do") and [`docs/DESIGN.md`](docs/DESIGN.md)
(§9 explicit non-goals, §10 open questions).

---

## License

MIT OR Apache-2.0 (see [`LICENSE`](LICENSE)).
