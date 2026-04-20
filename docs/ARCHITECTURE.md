# AgentDB Architecture

AgentDB is a prototype agent-native operational database. Its design
goals are spelled out at length in [agent-db.md](./agent-db.md); this
document maps the design concepts to the code you can actually read and
poke.

## Crate layout

```
bin/agentdb-server           the binary: wires everything together
crates/agentdb-storage       KvStore trait + RocksDB implementation
crates/agentdb-core          types, registries, primitives, lifecycle, fork
crates/agentdb-index         per-box BM25 + HNSW vector indexes
crates/agentdb-wire          MongoDB wire protocol subset (OP_MSG)
crates/agentdb-api           native REST: append_and_fetch, fork, admin
crates/agentdb-dashboard     static HTML/CSS/JS served at /dashboard
workload/                    Go workload generator + demo control server
```

The dependency direction is strict: `storage` has no knowledge of
agentdb; `core` depends on `storage`; `index`, `wire`, and `api` depend
on `core`; `dashboard` depends on nothing (static assets). The binary
pulls them together.

## Request flow

```
                     +-----------------+
  Mongo driver  ---> |  agentdb-wire   | ---+
                     |   (OP_MSG)      |    |
                     +-----------------+    |
                                            v
                                       +---------+        +----------+
  REST / SSE    ---> agentdb-api  ---> |  Box_   | -----> | RocksKv  |
  dashboard                            | (core)  |        +----------+
                                       +---------+             |
                                            |                  v
                                            |            +----------+
                                            +----------> | object_  |
                                           segments      | store    |
                                                         +----------+
```

Both the Mongo wire layer and the REST API route a request to a **box**
using FNV-1a of the tenant ID modulo the configured box count. The box
is the unit of concurrency control — every write takes the box's
`write_lock` (a `tokio::sync::Mutex`), so writes inside one box are
serialized but different boxes run in parallel.

## The box

`Box_` (`crates/agentdb-core/src/box_runtime.rs`) is the unit of
physical isolation described in §3.5 of the design doc. Each box owns:

- a `write_lock` for serializing mutations
- `BoxMetrics` counters (appends, forks, tier transitions, quota hits,
  warm/cold gauges)
- an optional `InjectedFault` (latency or error, expiring after N
  seconds) used by the dashboard's chaos button
- a `Quota` (soft caps on warm sessions and QPS)
- a reference to the shared `KvStore` and `ObjectStore`

Boxes do NOT own their KV or object store — they share one. The "box"
is a logical partition of the keyspace, not a separate storage engine.
This is the prototype-level approximation of the box architecture; a
production system would give each box its own shards and caches.

## Storage layout (RocksDB keys)

The prototype uses a single RocksDB instance keyed by hierarchical
prefixes. All keys are length-prefixed big-endian encoded so prefix
scans are lexicographically ordered.

```
tenant/{box_id}/{tenant_external_id}           -> Tenant
tenant_by_internal/{box_id}/{internal_id}      -> tenant_external_id
session/{box_id}/{tenant_internal_id}/{external} -> Session
session_by_internal/{box_id}/{tenant_internal_id}/{internal} -> external
event/{box_id}/{tenant_internal}/{session_internal}/{branch}/{seq} -> Event
segment/{box_id}/{tenant_internal}/{session_internal}/{branch} -> SegmentFile
```

Indexes are NOT stored in RocksDB. `PerBoxIndexes` is built lazily per
box by walking all events (warm KV + cold segment files) on the first
query and cached until server restart. New events appended after the
build do not flow into the cached index — rebuilding incrementally is
a known limitation carried forward from the prototype's lazy design.

Keeping session events under `{tenant_internal}/{session_internal}`
gives us O(1) session scans — the §4.1 "session clustering" property
is a prefix-range read, not a compaction strategy, because the
prototype doesn't need compaction yet.

## The append_and_fetch primitive

`append_and_fetch` (core/src/primitives.rs) is the single highest-value
native primitive described in §2.7. In one lock-scope it:

1. routes to a box (FNV-1a of tenant ID)
2. applies any active fault (latency or error)
3. enforces the warm-sessions quota
4. finds or mints the tenant and session
5. bumps the box's warm-sessions gauge if the session is new
6. appends the event at `seq = last_seq + 1`
7. records the append metric
8. returns the last N events (merged with the parent session's tail if
   this is a fork, see below)

Because it's a single call, the client-visible round trip is O(1). The
MongoDB `insert` command goes through the same path but doesn't take
the `fetch_last_n` parameter — pure insert for driver compatibility.

## Fork: cross-session parent pointer

`fork_session` (core/src/fork.rs) does NOT branch events in place. It
records the child session's `ForkRef { parent_session_internal_id,
parent_seq_at_fork }`. `fetch_last_n_merged` walks the chain: it reads
the child's own events first, and if the child is short it walks up to
the parent and reads up to `parent_seq_at_fork`.

This is the copy-on-write story from §2.8 implemented without MVCC.
Children get snapshot semantics because parent events in the bounded
range never change (the segment behind them is immutable). A write to
the parent after fork never affects the child's view — the child's
`parent_seq_at_fork` is frozen at fork time.

## Lifecycle: warm → cold

Each box runs a `LifecycleEngine` background task that ticks every
`AGENTDB_TICK_SECS` (default 2s). On each tick it scans sessions in
the box and demotes sessions whose `last_active_at` is older than
`AGENTDB_T_COLD_SECS` (default 60s):

1. Collect all live events from KV into a `SegmentFile` (protobuf-ish
   binary with schema version tag).
2. Write the segment to object storage under
   `objects/segments/{box_id}/{tenant}/{session}/{branch}.seg`.
3. Delete the per-event KV entries.
4. Update the session record: `tier = Cold`, `segment_path` set.

Reads of a cold session automatically go through the segment file,
concatenated with any WAL events that arrived after the segment was
written (the merge path).

An admin endpoint `POST /api/admin/tier/force` can force either
direction — used by the dashboard "Cool session" button and by tests
that need deterministic tier timing.

## Hybrid retrieval

`HybridPlanner` (core/src/planner.rs) runs a query against the per-box
indexes (`PerBoxIndexes` in agentdb-index). The planner is
deliberately tiny — it supports three sources:

- `Text(query)` → BM25 inverted-index lookup
- `Vector(embedding, k)` → cosine-similarity kNN via HNSW
  (`instant-distance` backend), with a brute-force bypass below 64
  vectors per path for deterministic small-N behavior
- `Filter(tenant_id, session_id)` → KV scan

A query can combine Text + Filter or Vector + Filter; the planner runs
the filter first and intersects. This is the §4.4 "pre-filter"
execution order — chosen because the tenant filter already prunes the
candidate set before the ANN or brute-force scan.

## MVCC / snapshot reads

Read primitives in the prototype are snapshot-consistent. A
`ReadCtx` (in `crates/agentdb-core/src/read_ctx.rs`) pins a RocksDB
snapshot + a set of segment paths referenced by any Cold sessions
read through it. Handlers in `agentdb-api` and `agentdb-wire` open one
`ReadCtx` per request; the HybridPlanner, `fetch_last_n_merged`, and
`scan_session_events` all read through it.

Writers are unchanged — they still serialize through each box's
`write_lock`. MVCC in this prototype only provides **read-side**
isolation; there is no multi-version write tracking or conflict
detection.

Segment files on the object store are not deleted inline by
`reactivate`. A per-box `SegmentGc` task sweeps every 5s and deletes
segments that (a) are not referenced by any current session record
and (b) are not pinned by any open `ReadCtx` and (c) are older than a
30s grace window. Configure via `AGENTDB_SEGMENT_GC_TICK_SECS` /
`AGENTDB_SEGMENT_GC_GRACE_SECS`.

Still out of scope: client-held snapshot handles (`openSnapshot` over
the wire), cross-box transactional snapshots, write-side MVCC, and
versioned indexes (the text/vector index is unversioned and the
planner post-filters hits by snapshot-visible `next_seq`).

## Observability (M7)

Every mutation path records to `BoxMetrics`:

- `appends`, `forks`, `tier_transitions`, `quota_hits`, `faults` —
  monotonically-increasing counters
- `warm_sessions`, `cold_sessions` — gauges (bumped/dropped at
  session creation and tier transition)
- `inserts`, `query_find`, `query_aggregate` — wire-protocol counters

`GET /api/admin/boxes` returns a per-box snapshot. `GET /api/stream` is
the Server-Sent Events channel; the server broadcasts:

- `MetricsTick` every 1s with gauge + counter snapshots per box
- `TierChange` on forced tier transition
- `ForkCreated` on successful fork
- `FaultInjected` on fault injection

The dashboard (`crates/agentdb-dashboard/static/app.js`) consumes both
the snapshot endpoint and the SSE stream to render box tiles + event
feed live.

## What the prototype does NOT do

The design doc describes a much larger system. The prototype
intentionally cuts these:

- **No real distributed KV.** RocksDB is a single-node store;
  "box" is a logical partition, not a shard.
- **No real object-store scaling.** The object_store crate abstracts it
  but the prototype writes to a local directory.
- **ANN is in-memory and non-persistent.** HNSW via `instant-distance`
  is built lazily per box on first query and rebuilt from scratch on
  server restart. Incremental updates and persisted snapshots are
  future work.
- **No LLM hooks, no lifecycle webhooks, no derived-data pipeline.**
- **No Percolator / 2PC.** Transactions are single-box, single-lock.
- **Admission control is token-bucket based and process-local.**
  Per-box and per-tenant-in-box QPS + burst via `Quota.max_qps` /
  `Quota.max_tenant_qps` / `Quota.burst`. Global process-wide ceiling
  and distributed rate limiting are out of scope. Denials surface as
  HTTP 429 (REST) or Mongo error code 82 (wire) and emit a `QuotaHit`
  SSE event carrying `kind` = `"box"` or `"tenant"` plus the tenant
  external id.

The prototype exists to demonstrate that the agent-native primitives
(append_and_fetch, fork, hybrid retrieval, box isolation, lifecycle,
native observability) compose into something sensible. Every one of
those bullets above is a fair amount of real work.
