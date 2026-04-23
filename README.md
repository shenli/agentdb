# AgentDB

A prototype **agent-native operational database**. The premise and
design rationale live in [`docs/agent-db.md`](docs/agent-db.md); this
repo is the showcase-grade prototype of what the primitives look like
when you actually build them.

> AgentDB is a prototype. It's a single process with a pluggable
> storage backend, not a distributed system. The goal is to prove the
> agent-native primitives compose — not to replace your Postgres.

---

## What it does differently

| | |
|---|---|
| **`append_and_fetch`** | Write one event and return the last N in the same round trip — the hot-path primitive for an agent scribbling context between tool calls. |
| **Session fork** | Cross-session parent pointer so children see a frozen snapshot of the parent's events at fork time, without copying. MVCC-backed. |
| **Hybrid retrieval** | BM25 text + HNSW cosine vector search, planned together with a tenant/session filter in one query. |
| **3-tier lifecycle** | Warm → cold → archive, with a pluggable object store underneath. Tier transitions emit webhooks so you can fan out derived data (summaries, embeddings, archival copies). |
| **Per-box isolation** | Tenants FNV-route to boxes. Each box has its own metrics, quota, token-bucket admission control, fault injection, webhook URL. |
| **Pluggable KV** | RocksDB (local disk) or SlateDB (object store — S3 / MinIO). Swap via one env var. |
| **Live dashboard** | Alpine.js page at `/dashboard`, driven by `/api/admin/boxes` + a Server-Sent Events stream of tier changes, forks, fault injections, rate-limit hits, and per-second metric ticks. |

---

## Quickstart

The `agentdb` binary has a clap CLI with subcommands. `agentdb serve`
runs the server; a handful of admin subcommands wrap the operator
endpoints so you don't have to curl them manually.

```bash
# 1. Build once
cargo build -p agentdb-server

# 2. Dev mode — ephemeral tempdirs, zero-config launch
./target/debug/agentdb serve --dev
```

Dev mode uses a fresh temp directory for RocksDB + object store +
meta SQLite; state doesn't persist across restart. Perfect for a
first-time-user walkthrough and for local testing.

```bash
# 3. In another shell — hit the admin endpoints through the CLI
./target/debug/agentdb show-scheduler
./target/debug/agentdb show-tablets
./target/debug/agentdb show-nodes

# 4. Or gracefully drain before shutdown
./target/debug/agentdb drain

# 5. See what config the server would run with
./target/debug/agentdb dump-config

# 6. Dashboard at http://127.0.0.1:7878/dashboard
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

The compose stack is single-node — multi-node is blocked on the
M20 shard/tablet refactor. Use `agentdb serve --dev` for local
development; the compose stack is for operators evaluating the
production storage path without provisioning real S3.

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
[`docs/ARCHITECTURE.md` § Prometheus exposition](docs/ARCHITECTURE.md#prometheus-exposition-m27-piece)
for the full metric catalog.

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
`$search`), `listDatabases`. Transactions, updates, and deletes
return a standard "not implemented" error.

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

- **What the prototype is supposed to be**:
  [`docs/agent-db.md`](docs/agent-db.md) — requirements, design
  principles, architecture, implementation trade-offs
- **What the code actually does today**:
  [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — crate layout,
  request flow, storage layout, per-milestone additions
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

Prototype with a working control plane. Every single-process agent-
native primitive is implemented (append-and-fetch, fork, hybrid
retrieval, three-tier lifecycle, box isolation, admission control,
lifecycle webhooks, MVCC snapshot reads, HLC, pluggable KV). The
M18+ milestones added:

- **SQLite control plane** (tenants, nodes, tablets, shards, scheduler leader)
- **Tablet leases** with fencing-token epoch — writes fail fast on lease loss
- **Node membership + heartbeat** with stale-node detection
- **Scheduler leader election** via singleton lease (SQLite
  equivalent of `pg_advisory_lock`)
- **Stale-lease reaper** — the elected leader cleans up dead
  owners' tablet rows
- **Node drain** (`POST /api/admin/drain`) with drain-aware lease
  renewal for clean handoff without SIGTERM
- **Per-tenant locking** + per-box admission semaphore
- **Shard hash + `LocationProxy`** with operator-visible
  assignment management (admin endpoint + CLI)
- **M20 session-shard write routing** — `POST /api/admin/shards/:id`
  actually moves future writes to the targeted box
- **M22 reaper-driven shard reassignment** — when a tablet's lease
  expires, its orphaned shards round-robin to surviving tablets
- **M20 multi-node forwarding** — both writes (POST
  append_and_fetch, POST fork) and session-scoped reads (GET
  admin session detail) forward to the tablet's owning node
  via HTTP, preserving auth and HLC, with a loop-guard
  (`X-AgentDB-Forwarded` → 421 on routing drift). Mongo
  `find`/`aggregate` with a pinned `session_id` return
  `code: 10107 NotWritablePrimary` with the owning node's
  address (real forwarding deferred — see Architecture)
- **M19 SCRAM-SHA-256 auth** (optional, `AGENTDB_REQUIRE_AUTH=true`)
  — Mongo wire advertises SCRAM; REST `/api/tenants/*` requires
  Basic auth; user→tenant ACL; `agentdb user add|list|delete`
- **M25 hot-tablet detection + shed + dynamic tablet
  creation** — sliding rate window on every box;
  scheduler-leader detector flags saturated tablets;
  `POST /api/admin/tablets/:id/shed` redistributes shards
  between existing tablets;
  `POST /api/admin/tablets/create` grows the box pool at
  runtime (no process restart). Full relief loop:
  `show-hot-tablets` → `create-tablet` → `shed-tablet
  <hot> --to <new>`
- **Full admin CLI** (`agentdb serve --dev`, `drain`, `show-*`,
  `set-shard`, `user *`, `shed-tablet`, `create-tablet`,
  `dump-config`)
- **Docker packaging** (multi-stage `Dockerfile`, compose stack
  with MinIO)
- **Prometheus `/metrics`** exposition
- **Structured `/health/ready`** body with per-reason `doc_url`
- **Runbook library** for every REST error code

**Still out of scope:**

- **Cross-node Mongo wire forwarding.** Mongo clients
  targeting a session on a peer get `code: 10107
  NotWritablePrimary` with the owner's address so they can
  reconnect, but there's no server-side proxy. Binary
  stateful protocol (SASL state doesn't transfer); separate
  PR.
- **Tenant-wide reads.** `find` without a session filter,
  `listDatabases`, `listCollections` still route by tenant
  hash. An override moves one session, not the whole tenant.
- **Child-inherits-parent-box.** A forked child lives on
  whatever box its parent was on; the child's `session_id`
  hashes independently and reads routed by that hash may
  miss it.
- **Quiesce / data-handoff on reassignment.** Moves via
  `set_shard` or the reaper change *future* routing; data already
  accepted on the old box stays there (append-only event streams
  remain durable wherever they were written, but cross-box reads
  that don't consult the shard map will miss them). Reads still
  route by tenant-hash.
- **Admin-plane auth.** `/api/admin/*` endpoints stay open —
  operators are trusted on the loopback interface. mTLS or a
  bearer token is a follow-up PR.
- **TLS termination.** Basic auth over plaintext is weak; the
  prototype assumes the caller terminates TLS at an ingress.
- **M22 sophisticated policy** — cross-AZ preference, anti-
  affinity, node-weight awareness. Current reassignment is
  round-robin across live tablets.
- **Cross-node `create-tablet`.** The admin endpoint
  creates the tablet on the node receiving the call. To
  spawn a tablet on a different node, post to that node's
  admin URL. Automatic placement (e.g. "put the new
  tablet wherever load is lowest") is future work.
- **Tablet deletion / shrink.** Once created, a tablet
  stays. Shrinking requires data migration.
- **Auto-shed policy.** The detector flags; an operator
  calls `shed`. Automatic policy needs calibrated
  thresholds that only production data can provide.
- **Cross-box (distributed) transactions** — design is single-box
  2PC-free.
- **Real ANN at 100M+ vectors** — HNSW is single-node, in-process.
- **LLM / embedding generation** — caller's responsibility;
  webhook signal only.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) "Control plane
(M18 and later)" and "What the prototype does NOT do" for the full
picture.

---

## License

MIT OR Apache-2.0 (see [`LICENSE`](LICENSE)).
