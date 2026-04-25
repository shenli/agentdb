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
| **Live dashboard** | Tabbed Alpine.js page at `/dashboard`: overview (box grid + SSE event feed), slow queries, audit log, token mint/revoke, tenant-quota editor. Takes an admin bearer token in the header input and stores it in `localStorage`. |

---

## Quickstart — your first agent in three commands

```bash
# 1. Start the server (zero config, ephemeral state)
cargo run -p agentdb-server -- serve --dev

# 2. In another shell, install a client SDK
pip install -e clients/python                # Python
# or
(cd clients/typescript && npm install && npm run build)   # TypeScript

# 3. Run a sample agent loop
python examples/agents/python/quickstart.py
# or
node --experimental-strip-types examples/agents/typescript/quickstart.ts
```

You should see three turns of working memory grow, then a fork
that branches the session. That's the canonical AgentDB shape:
write one event, read the recent N back in the same round trip.

The `serve --dev` startup banner prints listen addresses,
dashboard URL, and a copy-pasteable `curl` so you can poke at
the REST surface without reading further. Open
<http://127.0.0.1:7878/dashboard> for the live operator view.

### Five-line client

**Python** ([`clients/python/`](clients/python/README.md)):

```python
from agentdb import Client

client = Client("http://127.0.0.1:7878")
session = client.session("agent-1", tenant="demo")
result = session.append({"role": "user", "text": "hello"}, fetch_last_n=5)
print("seq:", result.seq, [(e.seq, e.payload) for e in result.recent])
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

Other languages (Go, Rust client crate) are tracked as follow-ups;
in the meantime any MongoDB driver speaks the wire port at
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

Single-node by default; multi-node works since the M20 shard/
tablet refactor (see the compose `README.md` for how to add a
peer). Use `agentdb serve --dev` for the fastest local dev loop;
the compose stack is for operators evaluating the production
storage path without provisioning real S3.

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
- **Admin-plane bearer auth** (optional,
  `AGENTDB_REQUIRE_ADMIN_AUTH=true`) — `/api/admin/*` requires
  `Authorization: Bearer <token>`. Health/metrics stay open.
  First-mint bootstrap exception lets operators obtain the
  initial token without out-of-band DB access.
  `agentdb admin-token add|list|revoke`; admin CLI
  subcommands pick up `$AGENTDB_ADMIN_TOKEN` automatically.
  Tokens carry an RBAC scope: `admin` (default) or
  `readonly` (GET-only; mutations return 403). Scope-403s
  show up in the audit log with the actor_token_id
- **TLS termination** (both ports) — REST
  (`--tls-cert` / `--tls-key` + env pair) terminates
  HTTPS via rustls + axum-server. Mongo wire
  (`--mongo-tls-cert` / `--mongo-tls-key`) terminates
  TLS via tokio-rustls; `AGENTDB_MONGO_TLS_REUSE_REST=1`
  reuses the REST cert so operators don't provision two.
  `agentdb serve --dev-tls` generates one ephemeral cert
  for both ports
- **M25 hot-tablet detection + shed + dynamic tablet
  creation** — sliding rate window on every box;
  scheduler-leader detector flags saturated tablets;
  `POST /api/admin/tablets/:id/shed` redistributes shards
  between existing tablets;
  `POST /api/admin/tablets/create` grows the box pool at
  runtime (no process restart). Full relief loop:
  `show-hot-tablets` → `create-tablet` → `shed-tablet
  <hot> --to <new>`
- **Warm-session data handoff** — per-session via
  `POST /api/admin/sessions/:sid/migrate`, or bulk via
  `shed-tablet --migrate-data` which chains the routing
  move with a per-session warm rekey for every session
  on the moved shards (cold/archive + fork parents
  reported in `skipped_sessions`)
- **Auto-shed scheduler** (optional,
  `AGENTDB_AUTO_SHED_ENABLED=true`) — background task
  fires shed-with-migrate against the least-loaded
  sibling whenever the hot-tablet detector flags a
  tablet; per-tablet cooldown
  (`AGENTDB_AUTO_SHED_COOLDOWN_SECS`) prevents thrash
- **Full admin CLI** (`agentdb serve --dev`, `drain`, `show-*`,
  `set-shard`, `user *`, `shed-tablet [--migrate-data]`,
  `create-tablet`, `admin-token *`, `migrate-session`,
  `dump-config`)
- **Docker packaging** (multi-stage `Dockerfile`, compose stack
  with MinIO)
- **Prometheus `/metrics`** exposition
- **Per-box latency histograms** (append, admission-wait) on
  `/metrics` and via `GET /api/admin/boxes/:id/latency` +
  `agentdb show-latency <box>`
- **Admin audit log** — every `/api/admin/*` request
  (including 401 attempts) persists to the meta store.
  Query via `GET /api/admin/audit` or `agentdb audit-log`
  with actor / path-prefix / time-window filters
- **Global per-tenant rate limits** — cap a tenant's
  aggregate QPS across every box on this node.
  `PUT /api/admin/tenant-quotas/:id` or
  `agentdb set-tenant-quota <id> --max-qps N --burst M`.
  Denials surface as `429` with `scope: "tenant_global"`
  and as `agentdb_tenant_quota_denies_total{tenant="..."}`
  on `/metrics`
- **Slow-query log** — bounded ring buffer of every
  append / find / aggregate that exceeds
  `AGENTDB_SLOW_QUERY_THRESHOLD_MS`. Records carry
  tenant, session, latency and a short detail. Surfaced
  via `GET /api/admin/slow-queries` and
  `agentdb show-slow-queries`;
  `agentdb_slow_queries_recorded_total{kind="..."}` on
  `/metrics`
- **Point-in-time backup** — live RocksDB checkpoint +
  SQLite VACUUM INTO into a destination directory.
  `POST /api/admin/backup` or
  `agentdb backup --out /abs/path`. Restore is offline —
  copy the files into the target dirs and restart.
  Optional scheduler via `AGENTDB_BACKUP_INTERVAL_SECS`
  + `AGENTDB_BACKUP_DIR` + `AGENTDB_BACKUP_RETAIN` runs
  leader-gated periodic backups and prunes older copies.
  Status + counters on `GET /api/admin/backup/status`
  and `agentdb_backup_runs_total{failures,…}` on
  `/metrics`
- **Config hot-reload** — `POST /api/admin/reload` (or
  `SIGHUP`, or `agentdb reload`) re-reads env + meta and
  swaps live values for the slow-query threshold + buffer
  capacity, tracing filter (`RUST_LOG`), and the tenant-
  quota registry without a restart
- **Structured `/health/ready`** body with per-reason `doc_url`
- **Runbook library** for every REST error code

**Still out of scope:**

- **Cross-node Mongo wire forwarding under auth.** With
  auth off (the default), node-A forwards session-pinned
  Mongo commands to the owning peer and relays the reply
  end-to-end (counters:
  `agentdb_wire_forwarded_total` /
  `agentdb_wire_forward_failures_total`). With
  `AGENTDB_REQUIRE_AUTH=true`, SASL session state can't
  cross a forward, so the handler returns
  `code: 10107 NotWritablePrimary` with the peer's
  address — the driver reconnects there. Auth-aware
  forwarding needs a shared-secret / mTLS inter-node
  trust model; future PR.
- **Tenant-wide reads.** `find` without a session filter,
  `listDatabases`, `listCollections` still route by tenant
  hash. An override moves one session, not the whole tenant.
- **Child-inherits-parent-box.** A forked child lives on
  whatever box its parent was on; the child's `session_id`
  hashes independently and reads routed by that hash may
  miss it.
- **Automatic data handoff on shed.** `shed` moves future
  routing; operators call `migrate-session` per affected
  session to move the already-written events. Coupling the
  two is future work.
- **Cross-node / cold-session handoff.** `migrate-session`
  is warm-only and intra-process (shared KV). Cross-node
  would need snapshot shipping; cold/archive needs
  object-store path rewrites.
- **Admin-plane mTLS.** Bearer-token auth + RBAC scopes
  ship; mTLS-based admin auth is a follow-up.
- **M22 sophisticated placement policy** — cross-AZ
  preference, anti-affinity, node-weight awareness.
  Current reassignment is round-robin across live
  tablets.
- **Cross-node `create-tablet`.** The admin endpoint
  creates the tablet on the node receiving the call. To
  spawn a tablet on a different node, post to that node's
  admin URL. Automatic placement is future work.
- **Tablet deletion / shrink.** Once created, a tablet
  stays. Shrinking requires data migration.
- **Cross-box (distributed) transactions** — design is
  single-box, 2PC-free.
- **Real ANN at 100M+ vectors** — HNSW is single-node,
  in-process.
- **LLM / embedding generation** — caller's responsibility;
  webhook signal only.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) "Control plane
(M18 and later)" and "What the prototype does NOT do" for the full
picture.

---

## License

MIT OR Apache-2.0 (see [`LICENSE`](LICENSE)).
