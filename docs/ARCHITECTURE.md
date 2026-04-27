# AgentDB Architecture

This is the **HOW** doc — what the running system actually
does, in code terms. The companion docs:

- [`agent-db.md`](agent-db.md) — **WHY** (motivation,
  workload requirements, design principles).
- [`DESIGN.md`](DESIGN.md) — **WHAT** the architecture
  commits to (identity, layering, clock, failure model,
  real-time, deployment).

This file describes the prototype's implementation: which
crate owns each capability, how requests flow through the
system, how the components fit together, and what the
prototype intentionally does *not* do. For the *why* behind
any architectural choice, follow the cross-link to DESIGN.md.

---

## Crate layout

```
bin/agentdb-server         the binary; wires every subsystem together
crates/agentdb-storage     KvStore trait + RocksKvStore + SlateKvStore
crates/agentdb-core        types, registries, primitives, lifecycle, fork,
                           MVCC read contexts, admission, HLC
crates/agentdb-index       per-box BM25 + HNSW vector indexes
crates/agentdb-meta        SQLite / Postgres control plane (tenants, nodes,
                           tablets, shards, scheduler, sessions locators,
                           users, admin tokens, audit log, quotas)
crates/agentdb-wire        MongoDB wire-protocol subset (OP_MSG)
crates/agentdb-api         native REST, SSE, broadcast, webhook delivery
crates/agentdb-dashboard   static HTML / CSS / JS at /dashboard

clients/python             Python SDK (sync + async)
clients/typescript         TypeScript / Node SDK
clients/go                 Go SDK

examples/agents/{python,typescript,go}   per-language quickstarts
examples/compose                          docker-compose stack with MinIO
workload                                  Go load generator
```

Dependency direction is strict: `storage` knows nothing about
agentdb; `core` depends on `storage` + `meta`; `index`,
`wire`, and `api` depend on `core`; `dashboard` depends on
nothing. The binary in `bin/agentdb-server` pulls them
together.

---

## System overview

A running cluster is a small number of `agentdb-server`
processes (**nodes**) sharing a control plane and an object
store, with each node hosting some number of **tablets** —
the runtime unit that owns a slice of the keyspace. Clients
talk to any node; if the node doesn't own the right tablet,
it forwards.

```
                        ┌────────────────────────────┐
                        │  Clients                   │
                        │  ─────────                 │
                        │  Python / TS / Go SDK      │
                        │  Browser (SSE + broadcast) │
                        │  MongoDB drivers           │
                        │  curl / OpenAPI consumers  │
                        └────────────┬───────────────┘
                                     │ REST 7878  ·  Mongo wire 7879
                  ┌──────────────────┼──────────────────┐
                  ▼                  ▼                  ▼
        ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
        │  agentdb-server  │  │  agentdb-server  │  │  agentdb-server  │
        │  (node A)        │  │  (node B)        │  │  (node C)        │
        │                  │  │                  │  │                  │
        │  REST  +  Mongo  │  │  REST  +  Mongo  │  │  REST  +  Mongo  │
        │  api   +  wire   │  │  api   +  wire   │  │  api   +  wire   │
        │                  │  │                  │  │                  │
        │  ┌────────────┐  │  │  ┌────────────┐  │  │  ┌────────────┐  │
        │  │ tablets:   │  │  │  │ tablets:   │  │  │  │ tablets:   │  │
        │  │ 0, 1       │  │  │  │ 2          │  │  │  │ 3          │  │
        │  │  - core    │  │  │  │  - core    │  │  │  │  - core    │  │
        │  │  - index   │  │  │  │  - index   │  │  │  │  - index   │  │
        │  │  - lease   │  │  │  │  - lease   │  │  │  │  - lease   │  │
        │  └────────────┘  │  │  └────────────┘  │  │  └────────────┘  │
        │                  │  │                  │  │                  │
        │   RocksDB / SlateDB (warm)              │  │   ...           │
        └─────────┬────────┘  └────────┬─────────┘  └────────┬─────────┘
                  │                    │                     │
                  └────────────┬───────┴─────────────────────┘
                               │
                  ┌────────────┴────────────────────┐
                  │                                 │
        ┌─────────▼──────────┐         ┌────────────▼──────────────────┐
        │  Meta store        │         │  Object store                 │
        │  (SQLite/Postgres) │         │  (S3 / MinIO / Ceph / local)  │
        │                    │         │                               │
        │  tenants, nodes,   │         │  segments  (cold sessions)    │
        │  tablets, shards,  │         │  WAL       (durable writes)   │
        │  scheduler leader, │         │  indexes   (snapshot dumps)   │
        │  session locators, │         │                               │
        │  users, audit, …   │         │                               │
        └────────────────────┘         └───────────────────────────────┘
```

Five things to keep in mind reading code:

1. **The data plane lives on each node**, in `agentdb-core`'s
   `Box_` runtime: per-tablet KV reads/writes, per-session
   locks, in-memory BM25 + HNSW indexes, lifecycle engine.
2. **The control plane lives in the meta store**, accessed
   via `agentdb-meta`. Tablet ownership, scheduler election,
   shard assignments, tenants, users, quotas, audit log. Hot
   path never hits the meta store synchronously (see DESIGN.md
   §11(a)) — every node caches the routing tables locally and
   refreshes them periodically + on Postgres `LISTEN/NOTIFY`.
3. **Object storage is the durable record.** Cold-tier
   session segments and WAL entries persist there; warm KV is
   a cache. A failed node's tablets recover by reading from
   object storage on a peer.
4. **Real-time is its own subsystem.** A browser-or-service
   SSE subscriber attaches to a node and follows the durable
   event stream from a chosen `from_seq`; ephemeral broadcasts
   on a separate channel route through an in-process bus.
5. **Two wire protocols, one data plane.** REST (`agentdb-api`)
   and Mongo wire (`agentdb-wire`) both terminate in the same
   `Box_` primitives. Adding an interface (e.g. native gRPC)
   wouldn't change the data plane.

Where to drill deeper:

| Topic | Section |
|---|---|
| How a single request gets served | [Request flow](#request-flow) |
| What's stored where | [Storage layer](#storage-layer) |
| `append_and_fetch`, `fork`, MVCC | [Data plane primitives](#data-plane-primitives) |
| BM25 + vector | [Hybrid retrieval](#hybrid-retrieval) |
| Multi-node, lease, placement, routing | [Control plane](#control-plane) |
| Cross-node REST + Mongo wire forwarding | [Multi-node forwarding](#multi-node-forwarding) |
| SSE event subscribe + ephemeral broadcast | [Real-time](#real-time) |
| Auth, RBAC, TLS, audit | [Security](#security) |
| Backup, hot-reload, slow-query, hot-tablet | [Operations](#operations) |
| Metrics, tracing, dashboard | [Observability](#observability) |
| Why X isn't there | [Non-goals](#non-goals) |

---

## Request flow

Most requests are a single round trip terminating in one
`Box_`. A few cross multiple tablets (unpinned reads,
broadcast subscriptions); the routing layer decides.

### Single-session write (REST `POST /api/sessions/:sid/append_and_fetch`)

```
client
   │  1. POST with payload
   ▼
agentdb-api
   │  2. auth → AuthedTenant ext (auth_middleware.rs)
   │  3. resolve session_locator (meta) → tenant + tablet
   │  4. is the tablet local?
   ▼
   ├── yes ────────────────────► agentdb-core::Box_
   │                                 │  per-session lock
   │                                 │  HLC.now() + seq
   │                                 │  RocksDB / SlateDB write
   │                                 │  in-memory index update
   │                                 │  fetch last N
   │                                 ▼
   │                              broadcast event to subscribers
   │                                 │  (BroadcastBus → SSE consumers)
   │                                 ▼
   │                              return AppendResult
   │
   └── no  ────────────────────► forwarding::forward_append_and_fetch
                                     │  HTTP POST to peer node's REST
                                     ▼  (Authorization, X-AgentDB-HLC,
                                         X-AgentDB-Forwarded passed through)
                                  peer terminates locally as above,
                                  reply relayed verbatim
```

### Single-session read (REST recent / Mongo `find` with `session_id`)

Same routing as the write path. Reads open a `ReadCtx` that
pins a RocksDB snapshot + segment paths + an HLC cutoff
(`agentdb-core::read_ctx`); fold the event log through
`agentdb-core::fold` to apply correction + tombstone events,
then return the materialized view.

### Tenant-wide read (Mongo `find` with no `session_id`)

`agentdb-wire/src/handlers/find.rs` enumerates the tenant's
sessions via `session_locators::list_by_tenant_external`,
splits targets into local boxes vs. peer-owned tablets,
fans out, and merges. Per-peer fanout uses
`forward::pinned_find` (one synthetic OP_MSG per session).

### Hybrid retrieval (Mongo `aggregate` with `$search` / `$vectorSearch`)

The request lands on the tenant-hash box; for each session
the locator says is local, the box's BM25 (Tantivy) or HNSW
(`hnsw_rs`) index serves the query. Results merge across
sessions; an over-fetch factor compensates for cross-session
recall loss. See [Hybrid retrieval](#hybrid-retrieval).

### Real-time subscribe (SSE)

`GET /api/tenants/:t/subscribe` (or `…/sessions/:s/subscribe`):
the handler in `agentdb-api::stream` opens the bus
subscription, optionally replays from `from_seq` by reading
KV/segments, then transitions to live tail. JWT auth on the
subscribe path; `jwt_expired` close frame triggers SDK-side
reconnect.

### Ephemeral broadcast

`POST /api/tenants/:t/channels/:c/publish` writes to the
node-local `BroadcastBus` (`agentdb-api::broadcast`). The
matching `subscribe` stream sends the messages to attached
SSE consumers. No durability, no replay, no cross-node
fanout.

### Webhook delivery (lifecycle hook)

When a session transitions tier (warm → cold → archive), the
lifecycle engine emits a `TierChange` event and an HTTP
webhook (`agentdb-api::webhook`). Consumers can build
derivatives (summaries, embeddings) and write them back via
the same `append_and_fetch` path.

---

## Storage layer

Three tiers, all pluggable.

### Local KV — `agentdb-storage`

`KvStore` trait with two implementations:

- **`RocksKvStore`** (default) — embedded RocksDB. The fast
  path for warm sessions on a single-node deployment.
- **`SlateKvStore`** — SlateDB on top of an `object_store`.
  Same API; warm data persists on S3-compatible storage with
  a configurable in-memory cache. Production shape; an
  operator who restarts a process loses no warm state.

Backend selected via `AGENTDB_KV_BACKEND` (`rocksdb` |
`slatedb`).

### Object store — `object_store` crate

Pluggable via `AGENTDB_OBJECT_STORE` (`local` | `s3`).
S3-compatible endpoints (AWS, MinIO, Ceph, Cloudflare R2)
work via `AGENTDB_S3_ENDPOINT` and the standard AWS
credential chain.

Object store holds:

- **Cold session segments.** When a session transitions to
  cold, the lifecycle engine packs its events into a single
  `SegmentFile` and writes one object per session under
  `segment/{tablet_id}/{session_id}/{branch}.bin`.
- **Index snapshots.** Periodic dumps of per-box BM25 + HNSW
  indexes under `indexes/{tablet_id}/...`. On restart the
  process loads the latest snapshot and replays any events
  appended after.
- **Tablet metadata snapshots.** Per-tablet catalog (session
  registry, segment index, HLC checkpoint). Lets a fresh
  node replay a tablet's state from object storage alone.
- **Backup output.** `agentdb backup --out /path` writes
  RocksDB checkpoint + meta-store dump there.

### Meta store — `agentdb-meta`

Dual-dialect (SQLite default; Postgres via
`AGENTDB_META_BACKEND=postgres` + `AGENTDB_META_URL`).
Hand-rolled migrations under `crates/agentdb-meta/migrations`,
forward-only, with side-by-side `.sql` + `.pg.sql` variants
that the runner picks by dialect.

Tables:

| Table | Purpose |
|---|---|
| `tenants` | Global tenant id space |
| `users` | SCRAM-SHA-256 credentials, one per tenant |
| `nodes` | Cluster membership + heartbeat (with `availability_zone`, `mongo_address`) |
| `tablets` | Runtime ownership; `epoch` is the fencing token |
| `shards` | 16384-row shard → tablet assignment (NULL = default hash) |
| `scheduler_leader` | Singleton row holding the current leader's lease |
| `session_locators` | Canonical session → tenant + tablet record |
| `tenant_quotas` | Global per-tenant rate limits |
| `admin_tokens` | Bearer tokens + RBAC scope |
| `admin_audit` | Append-only log of every `/api/admin/*` request |

`sqlx::Any` rewrites `?` placeholders to `$N` for Postgres.
Postgres triggers `NOTIFY` on `shards`, `nodes`, `tablets`
changes; a `change_notifier::subscribe` stream forwards them
to consumers (`LocationProxy`, `NodeAddressProxy`) as
"refresh now" hints on top of the existing periodic polling.
SQLite has no NOTIFY; consumers fall through to the periodic
loop.

### Key layout

KV keys are prefixed by tablet id. Tenant lives inside
records, never in the key path:

```
session/{tablet_id}/{session_id}                    -> Session
event/{tablet_id}/{session_id}/{branch}/{seq}       -> Event
segment/{tablet_id}/{session_id}/{branch}           -> SegmentFile
session_index/{session_id}                           -> tablet_id (lookup)
```

The `session_index` row exists for operations that arrive
with only a session id and no tablet context. Hot-path
writes go through `hash(session_id) % S` and skip the lookup.

### Tier-to-storage mapping

| Tier | Lives in | Read path |
|---|---|---|
| Warm | KV (RocksDB or SlateDB) | direct lookup |
| Cold | Object store (`segment/...`) | one object fetch + parse |
| Archive | Object store (cold class — S3 IA / Glacier IR) | hedged read; same path as cold |

See DESIGN.md §2 for the full layered-storage rationale.

---

## Data plane primitives

`agentdb-core` owns the verbs.

### `append_and_fetch` (`primitives.rs`)

The hot path. One call:

1. Acquire the per-session write lock
   (`Box_::session_write_lock`, sharded `DashMap` keyed by
   session id).
2. Mint HLC timestamp (`HlcClock::now()`).
3. Allocate next `seq` for the session.
4. Persist the event (KV append, with WAL semantics).
5. Update in-memory BM25 + HNSW indexes incrementally.
6. Optionally read back the last N events.
7. Release the lock.
8. Asynchronously broadcast the event to any SSE subscribers
   on this node (`BroadcastBus::publish`).

A new tenant or session is auto-minted on first write
(populating `tenants`, `session_locators`, and the per-box
`SessionRegistry`).

### `fork` (`fork.rs`)

Creates a child session that sees the parent's events at fork
time without copying. Pure metadata: a new `Session` row with
a `forked_from: ForkRef { parent_session_id, parent_seq }`
pointer. Reads against the child merge the parent's events
(up to `parent_seq`) with the child's own writes via
`fold_events`.

### MVCC snapshot reads (`read_ctx.rs`)

`ReadCtx::open` pins:

- A RocksDB snapshot (local-level isolation).
- A set of segment paths (pinned against segment GC via
  `SegmentPinRegistry`).
- An HLC cutoff for cross-tablet coherence.

Fanout reads pass the same cutoff so each tablet returns its
view as-of the same HLC time.

### HLC and causal consistency (`hlc.rs`)

Hybrid logical clock — `(physical_ns: u64, logical: u32)`,
12 bytes per event. Every event carries an `hlc_ts`,
assigned under the session lock. HLC propagates via the
`X-AgentDB-HLC` header on REST and via `$clusterTime` on
Mongo wire. `AGENTDB_HLC_MAX_OFFSET_SECS` (default 10s)
rejects inbound clocks that diverge from local wall-clock by
more than the bound — prevents one rogue client from
poisoning the global HLC.

### Lifecycle (`lifecycle.rs`, `tiering.rs`)

Per-session state machine: **warm** (idle < `T_COLD_SECS`,
default 60s) → **cold** (idle < `T_ARCHIVE_SECS`, default
600s) → **archive**. The `LifecycleEngine` ticks every
`AGENTDB_TICK_SECS` (default 2s), inspects per-box session
last-active timestamps, and fires transitions.

A cold transition packs the session's events into one
segment (`segment::write_segment`), writes it to object
storage, and removes the warm KV rows. An archive transition
is metadata-only; the segment stays in object storage but
its tier classification flips. Reads are tier-transparent —
`scan_session_all_tiers` walks KV first, falls back to the
segment.

Tier transitions emit:

- An SSE `TierChange` event for dashboard consumers.
- A webhook POST (when configured per-box) for derivative
  pipelines.

### Admission control (`admission.rs`)

Per-tablet token bucket, keyed by tenant. `Box_::admit`
consumes a token before a write proceeds. Bucket
configuration: default capacity `DEFAULT_ADMISSION_CAPACITY`,
overridable per-box via the admin endpoint. Denials surface
as HTTP 429 with `scope: "tenant_per_box"` and a
`retry_after_ms` hint.

A second layer — global per-tenant rate limits — sits on top
(see [Operations](#operations)).

---

## Hybrid retrieval

`agentdb-index` owns the per-box indexes; `agentdb-wire`
exposes them via Mongo `aggregate`.

### Text — Tantivy / BM25

`agentdb-index::text` wraps a Tantivy index per box. At
ingest time the indexer walks every event's `payload`
recursively: every JSON string at any nested path (e.g.
`payload.text`, `payload.assistant.response`) becomes a
tokenized term stream at that path. Tokenizer is whitespace +
lowercase — no stemming, no language models.

Query shape (Mongo wire):

```js
db.events.aggregate([
  { $search: { text: { query: "refund", path: "payload.text" }, limit: 10 } }
])
```

### Vector — HNSW (`hnsw_rs`)

`agentdb-index::vector` keeps an HNSW graph per (box, path).
Numeric arrays of length 2–2048 in the payload become entries
at their JSON path. Cosine distance; `ef_search` tunable via
`AGENTDB_HNSW_EF` (default 100).

```js
db.events.aggregate([
  { $vectorSearch: {
      queryVector: [...], path: "payload.embedding",
      limit: 5, numCandidates: 50
  }}
])
```

The indexer skips arrays outside the 2–2048 dim range so
non-semantic numeric payloads (latency arrays, token-id
sequences) don't trigger vector indexing.

### Persistent + incremental

Both indexes are **incrementally updated on every append**
(no lazy build) and **persistently snapshotted** to object
storage under `indexes/{tablet_id}/...`. Restart replays
events appended after the last snapshot. The
`IndexSnapshotJob` controls cadence; bounded recovery time
in seconds for warm session counts.

### Filtered fanout

Cross-session retrieval (within one tenant) fans out across
the local boxes that host that tenant's sessions. Each box
returns its top-`K · f` candidates (over-fetch factor `f`,
default 2); the coordinator reranks `K · f · M` and returns
top-K.

Cross-node fanout uses `forward::pinned_find` for the Mongo
wire path.

### Hybrid (BM25 + vector merge)

Currently client-side: callers run both `$search` and
`$vectorSearch` and merge ranks (typically RRF). A
server-side `$rankFusion` stage is a documented follow-up
(DESIGN.md §10 / `docs/follow-ups.md`).

---

## Control plane

The "small, read-mostly, never-on-the-hot-path" backbone
that lets a multi-node cluster work.

### Meta store

See [Storage layer / Meta store](#storage-layer). Central
fact: every node + every client caches routing tables
locally; meta is consulted only on cache miss, on scheduler
events, and on the periodic refresh.

### Lease + fencing

Each tablet has one owner at a time, holding a time-bounded
lease in the meta store (`tablets.lease_expires_at_ms`).
Mechanics:

- Owner renews every `AGENTDB_LEASE_RENEW_SECS` (10s) via
  CAS:
  ```sql
  UPDATE tablets SET lease_expires_at_ms = ? + 30000
  WHERE tablet_id = ? AND owner_node_id = ? AND lease_expires_at_ms > ?
  ```
  Update succeeds only if the owner still holds the lease.
- Lease duration: `AGENTDB_LEASE_DURATION_SECS` (30s).
- Before every write batch the owner checks `is_live()` in
  memory; expired → fail-fast with `Error::LeaseLost` →
  HTTP 503 `code: "lease_lost"`.
- If the meta store is unreachable for
  `LEASE_DURATION − LEASE_RENEW` (≈20s), the owner
  proactively stops writing (it cannot prove its lease is
  valid).

Every write tags its target object-store path with the
tablet's monotonic `epoch` (incremented on each ownership
transfer). Stale-epoch writes are rejected at the storage
layer regardless of the sender's state. Lease + fencing is
the correctness backstop, not just a fast path; design
rationale in DESIGN.md §4.5.

### Scheduler election (`scheduler.rs`)

Singleton lease in `scheduler_leader`. Followers periodically
attempt to acquire; the leader runs the placement +
reconciler loops and the auto-shed scheduler. Leader-only
work: bootstrap empty cluster, reassign orphaned shards,
trigger auto-shed.

### Stale-lease reaper (`reaper.rs`)

Leader-side. Scans `tablets` for rows whose
`lease_expires_at_ms` is in the past. For each, waits an
extra `safety_margin_secs` (5s) past expiry — the lease may
have just expired but the old owner hasn't finished its last
write yet — then clears `owner_node_id` (incrementing
`epoch`) so a peer can claim. Orphaned shards round-robin to
surviving tablets.

### Node lifecycle

Each `agentdb serve` upserts a `nodes` row at boot with
`address`, `mongo_address`, `availability_zone`, and bumps
`last_heartbeat_ms` every 5s. The `NodeReaper` scans for
stale heartbeats; nodes idle past
`AGENTDB_NODE_DEAD_AFTER_SECS` (30s) flip to `dead`, and
stay around for `AGENTDB_NODE_GC_AFTER_SECS` (1h) before
hard removal. Operators can `agentdb evict-node <id>` to
force removal of a known-dead node.

`POST /api/admin/drain` (or `agentdb drain`, or SIGTERM)
sets the drain flag, stops lease renewal, and lets running
requests finish before exit. Renewal-aware lease loop avoids
a stale-lease window during graceful shutdown.

### Session locator

`session_locators` is the canonical "where does session X
live" record (one row per session id). Populated on mint;
consulted by:

- Read fanout (Mongo wire `find`, REST admin reads).
- Fork resolution (parent's locator gives the child its
  tenant + initial tablet).
- Auth-bound mutation routes (`/api/sessions/:sid/...`) —
  the locator's tenant must match the authenticated user's.

The locator decouples session identity from the routing key:
sessions can be migrated between tablets (via shard override
or cooperative handoff) without breaking client-supplied ids.

### Tablet placement and reconciliation

Two leader-side loops in `tablet_placement.rs` and
`tablet_reconciler.rs`:

- **Placement.** Compares "tablets that need owners" with
  "alive nodes," prefers cross-AZ targets when multiple AZs
  are declared (`AGENTDB_NODE_AZ`), and assigns one row per
  unowned tablet to the least-loaded eligible node.
- **Reconciliation.** Each node periodically scans its
  assignments; new assignments trigger `add_box` (claim the
  lease, build the in-memory `Box_`, start lifecycle/index
  loops). Removed assignments trigger `relinquish` (flush
  state, drop the lease).

Tablet split is shard-level: reassign a subset of a tablet's
shards to a new tablet via `shards::set_assignment`. No data
movement inside the unchanged shards.

### Per-session locking

Per-tablet `DashMap<session_id, Arc<Mutex>>` (`Box_::session_locks`),
LRU-evicted by the lock-map GC. Two writes to the same
session serialize; two writes to different sessions in the
same tablet proceed in parallel.

### Shard hash + `LocationProxy`

`agentdb-core::shard` hashes session ids to one of
`SHARD_COUNT` (16384) shards via FNV-1a with a fixed seed.
`LocationProxy` caches the shard → tablet assignments;
refresh every `AGENTDB_SHARD_REFRESH_SECS` (30s) plus
`LISTEN/NOTIFY` push.

---

## Multi-node forwarding

Both protocols forward when the request targets a tablet
this node doesn't own.

### REST (`agentdb-api::forwarding`)

The API tier resolves the target tablet via the locator + the
`NodeAddressProxy` (cached `tablet_id → NodeTarget` from
`tablets ⨝ nodes`). Local target → handle locally. Remote
target → forward via `forwarding::forward_request`:

- Forwarded headers: `Authorization`, `X-AgentDB-HLC`.
- Loop guard: every forward adds `X-AgentDB-Forwarded: 1`. A
  node that receives a request with that header **and** still
  routes Remote returns **421 Misdirected Request** instead
  of forwarding again.

Covered endpoints: `append_and_fetch`, `fork`, admin session
GET / migrate.

### Mongo wire (`agentdb-wire::forward`)

Two paths:

1. **Pinned-session commands** (find/update/delete/insert
   with a session id): `routing_decision` returns the peer's
   `mongo_address`; `forward_op_msg` ships the original
   OP_MSG frame end-to-end and relays the reply.
2. **Unpinned `find` fanout**: per peer-resident session,
   build a synthetic pinned-find frame via
   `forward::pinned_find` and merge results with local
   reads.

Auth interaction: SCRAM session state is per-connection.
With `AGENTDB_REQUIRE_AUTH=true`, the wire forwarder cannot
relay auth across a hop; the dispatcher skips forwarding and
returns `code: 10107 NotWritablePrimary` with the peer's
address. The driver reconnects directly. Auth-aware
forwarding needs an inter-node trust model and is future
work.

### Cooperative handoff

A tablet whose owner is leaving (drain) pre-publishes the
target node id in `tablets.handoff_target_node_id`; the
designated successor picks up the assignment without going
through the reaper's lease-expiry path. Reduces failover
latency in the planned-shutdown case.

---

## Real-time

Two complementary primitives, each with a dedicated subsystem.

### Phase 1 — Durable session-event subscribe (`agentdb-api::stream`)

`GET /api/tenants/:t/subscribe` and
`GET /api/tenants/:t/sessions/:s/subscribe` open a
long-lived SSE stream. Each frame carries one event envelope:
`{tenant, session, seq, ts_ms, payload, kind?, actor_id?}`.

**Replay + live tail in one connection.** A subscriber may
specify `from_seq=N` (per-session) or
`from_seq_per_session=sid:seq,...` (tenant-wide). The handler
streams the historical events from KV/segments, then
transitions into the live tail without dropping events at
the boundary — internally it pre-subscribes to the bus
before the catch-up scan starts, so any append during the
scan is buffered and delivered after the historical chunk.

**Auth via JWT.** Bearer token, separate from the Basic-Auth
write path (browser-direct subscribers don't store
passwords). On expiry the server emits a terminal
`event: error` frame with `error: jwt_expired, last_seq: N`;
SDKs in Python / TypeScript / Go transparently fetch a fresh
token via the caller's `get_token` callback and reconnect
with `from_seq = N + 1`. The consumer never sees a gap.

**Backpressure.** Each subscriber has a bounded queue. Lag
overflow → terminal `lag_too_far` frame; subscriber decides
whether to reconnect from a fresher seq or give up. Catch-up
errors → terminal `catchup_failed`.

**Multi-node.** A subscriber connects to any node; the
node's stream handler forwards the subscribe to the tablet's
owning node when the request targets a peer-owned session.

### Phase 2 — Ephemeral broadcast (`agentdb-api::broadcast`)

`POST /api/tenants/:t/channels/:c/publish` and
`GET /api/tenants/:t/channels/:c/subscribe`. Channels are
ephemeral: no durability, no replay, no `from_seq`.

**`BroadcastBus`.** In-process tokio broadcast channels with
a fixed per-channel capacity (default a few hundred
messages). When a subscriber's buffer is full the **server
drops oldest queued messages for that subscriber** rather
than back-pressuring publishers — a stuck subscriber doesn't
slow the system. Trade-off explicit: if you need durability,
use the per-session append + subscribe path.

**Auth via JWT with `channels` claim.** Subscriber's JWT
declares which channels it may join (e.g.
`{"channels": ["room:42", "presence:*"]}`). Glob matching is
server-side; unauthorized channel → `403
channel_not_in_jwt`.

**`no_echo` filter.** Publishers commonly subscribe to the
same channel they publish on; the SDK can pass `?no_echo=1`
so the server filters out the subscriber's own publishes
(per-connection id correlation), avoiding echo loops.

**Multi-node.** Phase 2 broadcasts are in-process per node;
a publisher and subscriber on different nodes don't see each
other's traffic. Acceptable for the launch use cases (typing
indicators, presence pings tend to be co-located). Cross-
node broadcast is a documented follow-up (DESIGN.md §10.7).

### When to use which

| Property | Phase 1 (subscribe) | Phase 2 (broadcast) |
|---|---|---|
| Durable | yes | no |
| Replayable | yes (`from_seq`) | no |
| Gap-free | yes | no (lag → drop) |
| Per-session ordered | yes | n/a |
| Auth scope | per-tenant + per-session | per-channel JWT claim |
| Multi-node fanout | yes | no |
| Use case | tool-call results, audit, agent reactors | typing, cursors, presence pings |

---

## Security

Optional and additive — none of this fires unless explicitly
enabled.

### REST: HTTP Basic auth + auth-bound tenant

`AGENTDB_REQUIRE_AUTH=true` enables `basic_auth_middleware`
on every tenant-bound mutation route. PBKDF2 against a
SCRAM-SHA-256 stored key (one round per request, ~5–10 ms;
caching is a future PR). Per-tenant ACL: a user can only
access their tenant.

The middleware injects an `AuthedTenant { external_id,
user_id }` request extension. Handlers consume it as the
canonical tenant identity (`agentdb-api::handlers`,
`agentdb-api::fork`). On the v2-shape route family
`/api/sessions/:sid/...`, tenant comes from the extension —
no path or body tenant id is trusted.

User management: `agentdb user add|list|delete` (CLI) or
`POST /api/admin/users`.

### Mongo wire: SCRAM-SHA-256

When auth is required, `hello` advertises `SCRAM-SHA-256`.
`saslStart` / `saslContinue` carry out the standard challenge
with the user's stored `(salt, iterations, stored_key,
server_key)`. Per-connection state lives in
`WireState::sasl_sessions`. After auth, the command's `$db`
must match the user's tenant (except `admin`, always
allowed); cross-tenant access returns `code: 13
Unauthorized`.

### Admin-plane bearer

`AGENTDB_REQUIRE_ADMIN_AUTH=true` enables
`admin_auth_middleware` on `/api/admin/*`. Tokens hashed
SHA-256 in `admin_tokens`; mint via `agentdb admin-token
add` (plaintext returned once), revoke via `agentdb
admin-token revoke`.

Tokens carry RBAC scope: `admin` (default) or `readonly`
(GET-only; mutations return 403). Scope-403s record in the
audit log with `actor_token_id`.

**Bootstrap exception.** When no active tokens exist,
`POST /api/admin/tokens` is allowed without auth so an
operator can mint the first one. The moment a token is
active, subsequent mints require auth.

### TLS (both ports)

REST: `--tls-cert` / `--tls-key` (or env) terminates HTTPS
via rustls + axum-server. Mongo wire: `--mongo-tls-cert` /
`--mongo-tls-key` terminates TLS via tokio-rustls.
`AGENTDB_MONGO_TLS_REUSE_REST=1` reuses the REST cert so
operators don't provision two. `agentdb serve --dev-tls`
generates one ephemeral self-signed cert covering both ports
(localhost + 127.0.0.1) for smoke testing.

### Audit log

Every `/api/admin/*` request — including 401 attempts and
403 RBAC denials — appends a row to `admin_audit`
(method, path, actor token id, status, timestamp). Query via
`GET /api/admin/audit` or `agentdb audit-log`, filterable by
actor / path-prefix / time window.

### Threat model in scope

- Tenant isolation under shared infrastructure.
- Admin-plane access control (read vs admin).
- Wire-level confidentiality + integrity via TLS.
- Auditability of administrative actions.

Not in scope: mTLS-based admin auth (bearer-only today),
end-to-end encryption inside the database, key rotation
automation, network-level isolation (assumed at the ingress).

---

## Operations

The administrative surface a real deployment needs.

### Admin CLI

`agentdb` is one binary. `agentdb serve` starts the server;
the rest are HTTP-client wrappers around `/api/admin/*`:

| Command | Purpose |
|---|---|
| `cluster-status` | Leader + nodes (with AZ) + tablet placement summary |
| `show-{scheduler,tablets,nodes,shards}` | Per-subsystem snapshots |
| `set-shard <id> --tablet <n>` | Pin one shard to a tablet |
| `show-tablet-load`, `show-hot-tablets` | Per-tablet rate + saturation |
| `shed-tablet <id> [--to <n>] [--migrate-data]` | Move shards (and warm sessions) off |
| `create-tablet` | Grow the box pool at runtime |
| `migrate-session <sid>` | Move one warm session between tablets |
| `drain` | Graceful shutdown |
| `evict-node <id>` | Force-remove a known-dead node |
| `user add|list|delete` | SCRAM users |
| `admin-token add|list|revoke` | Bearer tokens |
| `set-tenant-quota`, `clear-tenant-quota`, `show-tenant-quota` | Per-tenant rate limits |
| `show-slow-queries` | Recent slow appends/finds |
| `show-latency <box>` | Per-box append + admission histograms |
| `audit-log` | Recent admin requests |
| `backup --out <dir>` | Manual point-in-time backup |
| `reload` | Config hot-reload |
| `dump-config` | Effective config with provenance |

All commands honor `AGENTDB_ADMIN_TOKEN` for bearer auth.

### Backup + restore

`agentdb backup --out /abs/path` runs a live RocksDB
checkpoint + meta-store dump (SQLite `VACUUM INTO` or
`pg_dump`) to the destination directory. Restore is offline:
copy the checkpoint into `AGENTDB_DATA`, the meta dump into
`AGENTDB_META_PATH`, and restart.

Optional periodic backup (leader-gated): set
`AGENTDB_BACKUP_INTERVAL_SECS`, `AGENTDB_BACKUP_DIR`,
`AGENTDB_BACKUP_RETAIN`; the server runs the same flow on a
timer and prunes older copies. Status + counters on
`/api/admin/backup/status` + `agentdb_backup_runs_total{...}`.

### Config hot-reload

`POST /api/admin/reload` (or SIGHUP, or `agentdb reload`)
re-reads env + meta and swaps live values for the
slow-query threshold + buffer capacity, tracing filter
(`RUST_LOG`), and the tenant-quota registry. Other settings
are set-on-boot.

### Hot-tablet handling

Sliding rate window per box (`agentdb-core::rate_window`)
tracks appends_per_sec + admission_wait_ratio. The
leader-side `HotTabletDetector` flips a tablet `cool → hot`
when wait-ratio exceeds `AGENTDB_HOT_WAIT_RATIO` (0.3) for
`AGENTDB_HOT_CONSECUTIVE` samples (3, ≈30s). Admin endpoints
expose the snapshot.

Three remediation paths:

1. **Manual shed.** `agentdb shed-tablet <hot> --to <new>
   [--migrate-data]` redistributes overridden shards from the
   hot tablet to a less-loaded sibling, optionally chasing
   the routing change with per-session warm-data migration.
2. **Auto-shed.** With `AGENTDB_AUTO_SHED_ENABLED=true`, a
   leader-side scheduler fires `shed-with-migrate` against
   the least-loaded sibling whenever the detector flags a
   tablet. Per-tablet cooldown
   (`AGENTDB_AUTO_SHED_COOLDOWN_SECS`) prevents thrash.
3. **Grow.** `agentdb create-tablet` adds a new tablet at
   runtime (no process restart) so a shed has somewhere to go.

### Slow-query log

Bounded ring buffer of every append / find / aggregate that
exceeds `AGENTDB_SLOW_QUERY_THRESHOLD_MS` (default 100 ms).
Records carry tenant, session, latency, kind, and a short
detail. Surfaced via `GET /api/admin/slow-queries`,
`agentdb show-slow-queries`, and the
`agentdb_slow_queries_recorded_total{kind="..."}` counter.

### Per-tenant rate limits

Token-bucket per tenant, configured via
`agentdb set-tenant-quota <tenant> --max-qps N --burst M`.
Caps a tenant's aggregate QPS across every box on this node.
Denials surface as HTTP 429 with `scope: "tenant_global"`
and `agentdb_tenant_quota_denies_total{tenant="..."}`.

### Mongo CRUD semantics

Because AgentDB is an event log, `update` and `delete` don't
literally mutate or remove events:

- `update` → appends a **correction event** with a merged
  payload + `_update_of: <seq>` marker.
- `delete` → appends a **tombstone event** with
  `_tombstone_of: <seq>`.

Both require a `session_id` in the filter. `find` default-
folds both, so `insertOne → updateOne → find` returns the
post-update payload as a Mongo user expects. `{raw: true}`
in the find body opts out and returns the un-folded log.

---

## Observability

### Metrics

`GET /metrics` exposes Prometheus text format, no extra
config needed. Per-box counters (`agentdb_appends_total`,
`agentdb_query_find_total`, `agentdb_quota_hits_total{kind}`,
`agentdb_forwarded_writes_total`, …) plus per-box latency
histograms for `agentdb_append_latency_seconds` and
`agentdb_admission_wait_seconds`.

`GET /api/admin/boxes/:id/latency` returns pre-computed
p50/p95/p99 from the same histogram for ad-hoc inspection
without a Prometheus query.

### Tracing

OpenTelemetry plumbed end-to-end. Every request carries a
trace id through wire/api → core → storage → forwarding;
fanout reads emit child spans per peer; hedged reads emit
both primary and secondary spans. Exporter configured via
the standard `OTEL_*` env vars; off by default.

W3C trace-context (`traceparent` header) propagates across
cross-node forwards. A single REST call that fans out across
three nodes appears as one connected trace tree.

### Health endpoints

- `/health/live` — process is up. Always 200. Targets the
  K8s livenessProbe; a failure here means restart.
- `/health/ready` — should receive traffic. Returns 200 when
  every box with a configured lease reports `is_live()` and
  the process is not draining. 503 with a structured body
  carrying per-reason `code` + `doc_url` (linked to the
  runbook library under `docs/runbooks/`).

### Dashboard

`/dashboard` serves a tabbed Alpine.js page (in
`agentdb-dashboard`) with: overview (box grid + SSE event
feed), slow queries, audit log, token mint/revoke, tenant-
quota editor. Takes an admin bearer token in the header
input and stores it in `localStorage`.

### REST error taxonomy

Every error response is `{"error": {"code", "kind",
"message", "doc_url"}}` plus an HTTP status. The
`error::ErrorCode` enum is checked by the OpenAPI snapshot
test (`tests/openapi_snapshot.rs`); every code with a
runbook URL points at a one-page runbook in
`docs/runbooks/`. SDKs map status + code to typed exceptions.

### OpenAPI

`GET /openapi.json` returns the full REST spec generated by
`utoipa`. The `tests/openapi_snapshot.rs` snapshot test pins
the surface — a PR that changes a route's signature must
regenerate the snapshot, making API breaks visible in
review.

---

## Configuration

The full env-var catalog lives next to the code that reads
each value (most subsystems have a `from_env()` constructor).
A few load-bearing ones, by area:

| Variable | Default | Area |
|---|---|---|
| `AGENTDB_ADDR` | `127.0.0.1:7878` | REST listener |
| `AGENTDB_MONGO_ADDR` | `127.0.0.1:7879` | Mongo wire listener |
| `AGENTDB_BOXES` | `4` | Tablets seeded per process |
| `AGENTDB_KV_BACKEND` | `rocksdb` | `rocksdb` \| `slatedb` |
| `AGENTDB_OBJECT_STORE` | `local` | `local` \| `s3` |
| `AGENTDB_META_BACKEND` | `sqlite` | `sqlite` \| `postgres` |
| `AGENTDB_REQUIRE_AUTH` | `false` | Tenant Basic auth + SCRAM |
| `AGENTDB_REQUIRE_ADMIN_AUTH` | `false` | Admin bearer auth |
| `AGENTDB_T_COLD_SECS` | `60` | Warm → cold idle threshold |
| `AGENTDB_T_ARCHIVE_SECS` | `600` | Cold → archive idle threshold |
| `AGENTDB_LEASE_DURATION_SECS` | `30` | Tablet lease duration |
| `AGENTDB_AUTO_SHED_ENABLED` | `false` | Hot-tablet auto-shed scheduler |

`agentdb dump-config` prints the effective configuration with
provenance (default / env / file / flag). For tunable
philosophy and the deployment-mode story, see DESIGN.md §12
(deployment modes) and §13 (operational surface).

---

## Non-goals

What the prototype intentionally does not do:

- **Cross-box (distributed) transactions.** The design is
  single-tablet, 2PC-free.
- **LLM / embedding generation.** Caller's responsibility;
  AgentDB stores and serves, doesn't compute.
- **General-purpose search.** Vanilla BM25 (whitespace +
  lowercase). For language models / stemming / faceting, use
  Elasticsearch / Tantivy directly / Lucene.
- **High-cardinality SQL analytics.** No JOIN, no GROUP BY
  across the corpus. Dump events into a columnar store
  (DuckDB, ClickHouse) for that.
- **Production deployment of the prototype.** No SLA, no
  prod customers, no 24/7 on-call. Run it where you're
  willing to accept that.

What's missing or partial today (with intent to ship):

- **Cross-node Mongo wire forwarding under auth.** Cross-
  node forwarding works in unauth mode; auth-mode SASL
  state can't cross hops yet.
- **Cross-node / cold-session warm migration.**
  `migrate-session` (and `shed --migrate-data`) handle
  warm sessions co-located in the same process's KV.
  Cross-node + cold/archive migration would need snapshot
  shipping + object-store path rewrites.
- **Cross-node `create-tablet`.** Admin endpoint creates on
  the receiving node; placement-aware creation across the
  cluster is future work.
- **Placement policy beyond cross-AZ.** Anti-affinity
  (don't co-locate tablets X and Y) and node-weight
  awareness (heterogeneous hardware) not yet in.
- **Tablet deletion / shrink.** Once created, a tablet
  stays. Shrinking would require data migration.
- **Admin-plane mTLS.** Bearer-token auth + RBAC ships;
  mTLS is a follow-up.
- **`listDatabases` / `listCollections` cross-tablet
  fanout.** Still tenant-hash-routed.
- **Hybrid retrieval rank fusion (RRF) on the server.**
  Client-side merge today; server-side `$rankFusion` is a
  documented follow-up.
- **Cross-node ephemeral broadcast.** Phase 2 broadcasts
  are in-process per node.
- **ANN at 100M+ vectors per node.** HNSW is single-node,
  in-process; vertical ceiling per tablet.

For the architectural rationale behind any non-goal, see
DESIGN.md §9 (explicit non-goals) and §10 (open questions).
