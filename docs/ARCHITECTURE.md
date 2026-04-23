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

- a **per-tenant lock map** (`tenant_locks`) so different tenants on
  one box run in parallel while preserving ordering within each
  tenant (M23; replaced the original single `write_lock` mutex)
- an **admission semaphore** bounding in-flight writes to
  `AGENTDB_ADMISSION_CAPACITY` (default 64), complementing the QPS
  token bucket
- an optional **tablet lease** (M21) that writes check on every
  request — a stale lease short-circuits with `NodeDraining` /
  `LeaseLost`
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

## Pluggable KV backend

The `KvStore` trait lives in `crates/agentdb-storage`. The default
implementation is `RocksKvStore` (local disk). A second
implementation, `SlateKvStore`, writes to an `object_store` backend
and enables running AgentDB against S3. Pick via
`AGENTDB_KV_BACKEND=rocksdb|slatedb`. See `docs/S3.md` for the S3
deployment walkthrough.

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

## Lifecycle: warm → cold → archive

Each box runs a `LifecycleEngine` background task that ticks every
`AGENTDB_TICK_SECS` (default 2s). Two sweep passes per tick:

**Pass 1 (warm→cold):** sessions idle longer than `AGENTDB_T_COLD_SECS`
(default 60s) are demoted:

1. Collect all live events from KV into a `SegmentFile` (protobuf-ish
   binary with schema version tag).
2. Write the segment to object storage under
   `objects/segments/{box_id}/{tenant}/{session}/{branch}.seg`.
3. Delete the per-event KV entries.
4. Update the session record: `tier = Cold`, `segment_path` set.

**Pass 2 (cold→archive):** cold sessions idle an additional
`AGENTDB_T_ARCHIVE_SECS` (default 600s) are flipped to `Tier::Archive`.
The prototype implements this as a metadata-only flip on the `Session`
record — the segment file stays where it is. A production deployment
would use this hook to copy the segment to a cheaper object-storage
class (S3 Intelligent-Tiering / Glacier Instant); that copy is
explicitly out of prototype scope.

Reads of cold and archive sessions go through the same segment-based
path via `primitives::scan_session_all_tiers`: a tier match selects
either the KV prefix scan (warm) or a hedged segment read
(`read_segment_hedged`, per-tier delay — 50 ms cold / 200 ms archive).
M12 initially shipped archive as partial; M17 closed the three
remaining gaps so that `fetch_last_n_merged` on a forked-archived
parent, `plan_locality_bound` on an archived session, and Mongo wire
`find` on archived sessions all return semantically-correct results.
The archive tier now affects **latency**, not **semantics** — which is
exactly the contract in `docs/agent-db-v2.md` §6.

An admin endpoint `POST /api/admin/tier/force` can force any tier
transition (warm↔cold↔archive). Multi-step transitions (warm→archive
or archive→warm) walk one hop at a time and emit one SSE `TierChange`
event + one webhook per hop.

## Hybrid retrieval

`HybridPlanner` (core/src/planner.rs) runs a query against the per-box
indexes (`PerBoxIndexes` in agentdb-index). The planner is
deliberately tiny — it supports three sources:

- `Text(query)` → BM25 inverted-index lookup
- `Vector(embedding, k)` → cosine-similarity kNN via HNSW
  (`hnsw_rs` backend — swapped from `instant-distance` in M16 to get
  incremental insert without full rebuilds), with a brute-force bypass
  below 64 vectors per path for deterministic small-N behavior
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

## HLC and causal consistency (M14)

AgentDB carries a process-global **Hybrid Logical Clock** (`HlcClock`,
`crates/agentdb-core/src/hlc.rs`) that assigns a causality-respecting
timestamp to every event. Design rationale lives in
`docs/agent-db-v2.md` §4 and
`docs/superpowers/notes/2026-04-21-hlc-clock-decision.md`; this section
documents what the prototype actually does.

**Timestamp shape.** `HlcTimestamp { physical_ns: u64, logical: u32 }`
— 12 bytes per event, stored on `Event::hlc_ts`. Segment schema
bumped to `SEGMENT_SCHEMA_VERSION = 2`.

**Allocation.** `append_and_fetch` calls `registry.hlc().now()` inside
the session lock, immediately before allocating `seq`. Per-session
`(seq, hlc_ts)` is jointly strictly monotonic.

**Snapshot reads.** `ReadCtx::open` captures `box_.hlc().current()` as
the `hlc_cutoff`. `fetch_last_n`, `scan_session_events`, and the fork
path in `fetch_last_n_merged` filter events by `ev.hlc_ts <= cutoff`.
For KV reads this is redundant with the RocksDB snapshot pin; for
segment-sourced reads (cold / archive / fork parent), HLC is the only
isolation mechanism — RocksDB snapshots don't cover object storage.

**Wire propagation.** The Mongo wire dispatcher absorbs inbound
`$clusterTime` via `HlcClock::update` before running the command and
injects a fresh HLC-backed `$clusterTime` (with zero-signature sub-doc
for Rust-driver compat) on every response. The REST layer does the
same via an axum middleware and the `X-AgentDB-HLC: physical_ns.logical`
header. Both paths short-circuit with a 400 / Mongo code 71 if the
inbound timestamp violates the max-offset guard.

**Max-offset guard.** `AGENTDB_HLC_MAX_OFFSET_SECS` (default 10,
matching CockroachDB's `max_offset`) bounds how far in the future an
inbound remote may be. Violations increment `HlcClock::rejects_count()`
and fail the request.

**Admin surface.** `GET /api/admin/hlc` returns the current HLC, wall
clock, offset, configured max-offset, and reject count. Useful on-call
tool for diagnosing clock skew or rogue peers.

**What HLC does not give.** Not external consistency (a skewed reader
may observe T2 < T1 when wall-clock commit was T1 < T2; acceptable for
agent workloads, not for banking). Not a concurrency primitive — write
conflicts are still prevented by per-session locks. Not a clock-repair
mechanism — it bounds drift damage to the max-offset window, but
monitoring must fix the cause.

## Agent schema (M15)

Agent attribution is a schema feature, not a first-class API resource.
Design rationale in `docs/agent-db-v2.md` §2.3; this section documents
what the prototype does.

**Fields.**

- `Event::actor_id: Option<String>` — free-form identifier of the agent
  that wrote this event. `#[serde(default, skip_serializing_if =
  "Option::is_none")]` keeps the wire format tight when absent.
- `Session::primary_agent_id: Option<String>` — the owning agent for
  single-agent sessions. Multi-agent sessions (orchestrator + workers)
  may leave this unset and disambiguate at the event level via
  `actor_id`.

Both fields are additive and optional: segments written before M15
(i.e. v2 segments whose `Event` payload lacks the field) deserialize
cleanly with the new code via serde defaults. This is the repo's
policy for on-disk format evolution: **required fields bump the
`SEGMENT_SCHEMA_VERSION`; additive `Option<T>` fields do not**.

**Ingress paths.**

- REST `AppendAndFetchRequest` carries an optional `actor_id`;
  `handlers::append_and_fetch` routes through
  `agentdb_core::append_and_fetch_with_actor`. A sibling
  `append_and_fetch` preserves the old signature for call sites that
  don't know about agents, keeping the change non-breaking across the
  50+ internal uses.
- Mongo `insert` extracts a top-level `actor_id` string field from the
  inserted document (removing it from the payload reaching `Event`)
  and threads it through the same `_with_actor` path.

**Cross-session retrieval.**

`PerBoxIndexes::agent_events: BTreeMap<String, Vec<DocId>>` maps each
`actor_id` to the events carrying it. Built at lazy-index-init time
next to the text and vector indexes. `BTreeMap` (not `HashMap`) for
stable iteration and predictable memory at high agent cardinality.

`HybridPlanner::scan_by_agent(actor_id)` resolves the index entries
through the `ReadCtx`, inheriting snapshot visibility (`next_seq`
filter) and HLC cutoff filtering for free. Events carry `score = 1.0`
as a sentinel since there is no ranking for an exact-match agent
lookup. Defense-in-depth: a resolved event whose `actor_id` no
longer matches the query is filtered out after resolve (guards
against index/storage drift).

**Not in v1:** an agent-as-resource API (registration endpoints,
per-agent quotas, agent lifecycle). That is a v2+ product decision
per design §10.4; promoting `actor_id` from metadata to resource
would force opinions on what an "agent" is (LangGraph node, crew
member, tool-calling loop), which the database should not hold.

## Hedged reads (M15)

Object storage has fat read tails (seconds, occasionally tens of
seconds on S3). Cold- and archive-tier segment reads use a
**hedged-request-with-cooperative-cancel** pattern from "The Tail at
Scale" to bound p99.9.

**Primitive.** `agentdb_core::hedge::hedged(primary, delay, secondary_factory)`:
fires the primary at t=0; if it hasn't completed by `delay`, fires the
secondary via `secondary_factory()`. First to complete wins; the
loser is cancelled by Rust's cooperative future-drop. This is
latency-only hedging — a primary error **before** the delay is
returned directly, no retry. Error-driven retry is a separate
feature, deliberately out of scope.

**Applied sites.** `segment::read_segment_hedged` wraps the existing
`read_segment` and is called from:

- `primitives::fetch_last_n_merged` (fork-parent cold branch)
- `HybridPlanner::plan_locality_bound` (cold-session scan)
- `HybridPlanner::resolve` (warm-miss fallback onto segment)

Warm-tier reads are **not** hedged — they go directly through the
pinned RocksDB snapshot, which already has predictable latency.

**Configuration.**

- `AGENTDB_HEDGE_COLD_MS` — cold-tier delay, default 50.
- `AGENTDB_HEDGE_ARCHIVE_MS` — archive-tier delay, default 200.
- `AGENTDB_HEDGE_ENABLED` — emergency kill-switch (`false` /
  `0` / `FALSE` / `False` disables hedging, making reads fall back
  to a single plain `read_segment`).

**Metrics.** Three counters on `BoxMetrics`, all surfaced in
`MetricsSnapshot`:

- `hedged_triggers` — secondary was fired (primary exceeded the delay).
- `hedged_primary_wins` — primary finished first after the secondary
  had been fired (primary was slow but still beat the hedge).
- `hedged_secondary_wins` — secondary finished first; these are the
  latency-saved cases.

`hedged_triggers == hedged_primary_wins + hedged_secondary_wins` by
construction.

**What hedged reads do not give.** They don't accelerate a
consistently-slow object store — if both primary and secondary are
bottlenecked at the same tail, both return slowly. They don't retry
errors. And they don't hedge the warm-tier KV path, which has its
own latency characteristics.

## REST error taxonomy (M13)

Every `/api/*` REST error returns a Stripe-style JSON body with
stable machine-readable fields:

```json
{
  "code": "quota_exceeded",
  "type": "rate_limit_error",
  "message": "rate limit exceeded (Tenant)",
  "scope": "tenant",
  "retry_after_ms": 1000
}
```

Codes (with HTTP status + typed extras):

| code | type | HTTP | Extras |
|---|---|---|---|
| `not_found` | `invalid_request_error` | 404 | `resource` (`tenant` / `session` / `box`), `id` |
| `invalid_request` | `invalid_request_error` | 400 | `field?` |
| `quota_exceeded` | `rate_limit_error` | 429 | `scope`, `retry_after_ms` (also `Retry-After: 1` header) |
| `fault_injected` | `api_error` | 503 | `box_id`, `fault_kind` (`latency` / `error`) |
| `storage_error` | `api_error` | 500 | — |
| `serialization_error` | `api_error` | 500 | — |

The `message` field is human-readable and may change between
releases — callers should branch on `code`, not `message`. The
authoritative schemas live at `/openapi.json`.

The Mongo wire protocol returns errors in its own format
(`{ok: 0, errmsg, code: N}` where `N` follows the Mongo error code
table); the two surfaces do not share a taxonomy.

One asymmetry: axum's built-in `Json<T>` extractor rejects a
malformed request body with plain-text 400 before our
`IntoResponse` is invoked. These responses carry an HTTP status
but not a Stripe-style body. The `serialization_error` code only
applies to server-side JSON failures (reading segments, decoding
persisted state). Wiring a custom extractor rejection into
`Error::Invalid` for request-body decode failures is a future
improvement.

## OpenAPI specification (M13)

The REST surface is described by an OpenAPI 3.x document served
unauthenticated at `GET /openapi.json`. The spec is generated from
`#[utoipa::path(...)]` annotations on each handler plus
`#[derive(ToSchema)]` on each request/response type, aggregated by
`agentdb_api::ApiDoc`. A snapshot test
(`crates/agentdb-api/tests/openapi_snapshot.rs`) locks the
generated output against a checked-in file
(`tests/snapshots/openapi.json`, ~1500 lines) so any surface change
is visible in the PR diff.

Not covered by OpenAPI: the Mongo wire protocol (not HTTP) and
SSE event bodies (OpenAPI does not model event streams well — the
endpoint is listed, but event shapes are defined only via the
`StreamEvent` schema for SDK consumers).

## Persistent + incremental indexing (M16)

Pre-M16 indexing had two problems:

- **Lazy rebuild on first query.** New events appended between server
  starts were invisible to queries until the next full rebuild —
  M14/M15 sidestepped by rebuilding the index on every restart (ouch
  at scale).
- **`instant-distance` rebuilt the full HNSW graph on every insert.**
  BoTE estimated ~33 min cold-start at v2-target scale; intolerable.

M16 fixes both. Three components.

**Backend swap (`hnsw_rs`).** Incremental HNSW: `insert()` is O(log N)
amortized directly on the live graph, no rebuild. `VectorIndex` trait
is unchanged; all existing callers work without modification. Small-N
brute-force bypass survives for deterministic tiny-dataset behavior.
Spike note: `docs/superpowers/notes/2026-04-22-hnsw-backend-spike.md`.

**Incremental insert on the write path.** `append_and_fetch` now calls
`Box_::index_event` under the session lock immediately after the KV
write. Events are searchable the moment they're acknowledged — no
staleness window. Metric: `events_indexed` on `BoxMetrics`.

**Persistent snapshots + recovery.** The whole point of M16.

- **Snapshot format.** `IndexSnapshot` captures BM25 postings,
  normalized vector entries per path, and the agent-events index, all
  serialized as BSON (same format segments use) at
  `indexes/{box_id:04x}/snap-{hlc_physical_ns:016x}.bson`. HNSW graph
  itself is NOT serialized — the first query at each path rebuilds
  the graph from the restored entries. This keeps the on-disk format
  stable across `hnsw_rs` version upgrades and avoids the tempdir
  gymnastics `file_dump` would require.

- **Periodic job.** One task per box (`IndexSnapshotJob`), defaults to
  hourly (`AGENTDB_INDEX_SNAPSHOT_SECS`, default 3600). Captures the
  current HLC, serializes the live `PerBoxIndexes`, uploads, then
  prunes to retain the last N (`AGENTDB_INDEX_SNAPSHOT_RETAIN`,
  default 3). Metrics: `index_snapshots_written`,
  `index_snapshot_failures`.

- **Recovery on startup.** `Box_::indexes_built` now follows this
  flow on first access:

  1. Discover the latest snapshot via `list_snapshots` (lex order on
     the fixed-width hex HLC name == newest first).
  2. Deserialize, restore `PerBoxIndexes`.
  3. Walk every session's events, replaying only those with
     `hlc_ts > snapshot_cutoff` via `PerBoxIndexes::replay_events_after`.
  4. If no snapshot exists OR load fails, fall back to the full-KV-scan
     rebuild (pre-M16 behavior). Degraded but correct.

**Integration tests** in `crates/agentdb-core/tests/index_snapshot_recovery.rs`
cover all three paths: snapshot-only recovery, snapshot + post-cutoff
replay, and no-snapshot fallback.

**Env surface:**

| Var | Default | Meaning |
|---|---|---|
| `AGENTDB_INDEX_SNAPSHOT_SECS` | `3600` | Gap between periodic snapshot attempts |
| `AGENTDB_INDEX_SNAPSHOT_RETAIN` | `3` | How many snapshots to keep per box |
| `AGENTDB_HNSW_EF` | `100` | HNSW `ef_construction` + `ef_search` (unchanged from pre-M16; honored on both fresh and snapshot-restored indexes) |

**Deferred to follow-ups:** `agentdb drain` CLI command that triggers
a final snapshot before shutdown (Phase F of the M16 plan, but the
drain CLI is §15.5-scope operational surface that hasn't landed yet);
cross-box fanout (M23); pre-built HNSW snapshot (optimization if
rebuild-on-first-query becomes a p99 issue — currently not measured).

## Tracing and OpenTelemetry (M15 Part A)

Every request produces a span tree observable via the `tracing` crate
and optionally exported over OTLP to a collector (Jaeger, Tempo, etc).

**Tree shape (REST `append_and_fetch` example).**

```
rest.append_and_fetch            tenant, session, actor, fetch_last_n
 └── append_and_fetch            seq, hlc_physical_ns (recorded mid-span)
      └── fetch_last_n_merged    session, tier, fork, n
           └── read_segment_hedged  (only if tier is Cold/Archive)
                                   path, tier, hedge_triggered, hedge_winner
```

Mongo wire gets the equivalent with a root `mongo.dispatch` span that
records `command` (e.g. `find`, `insert`) and `db` (the tenant
namespace). Hybrid retrieval paths produce `planner.plan` (with
`strategy` + `hits` fields) or `planner.scan_by_agent` (`actor`,
`candidates`, `hits`).

**Two init modes (server binary, `bin/agentdb-server/src/tracing_init.rs`):**

- **`AGENTDB_OTLP_ENDPOINT` unset** (dev): `tracing_subscriber::fmt`
  pretty-prints spans + events to stdout. Zero external dependencies.
- **`AGENTDB_OTLP_ENDPOINT=http://host:4317`** (prod): OTLP/gRPC
  exporter batches spans to the named collector while stdout keeps the
  same human-readable output for `docker compose logs` visibility.

`init()` returns a `TracerGuard` RAII handle; dropping it flushes any
pending batched spans. `main()` keeps the guard alive for the lifetime
of the process.

**Configuration env vars.**

| Var | Default | Meaning |
|---|---|---|
| `AGENTDB_OTLP_ENDPOINT` | *(empty)* | Collector address; empty disables OTLP export |
| `AGENTDB_TRACE_SERVICE_NAME` | `"agentdb"` | OTel resource `service.name` attribute |
| `RUST_LOG` | `info` | Standard `EnvFilter` syntax (e.g. `info,agentdb_core=debug`) |

**Library-side: no OTel dependency.** Instrumented functions use
`#[tracing::instrument]` only — no `opentelemetry::` imports in
`agentdb-core` / `-wire` / `-api`. The OTel pipeline lives entirely in
the server binary. This keeps the libraries usable with any
`tracing_subscriber::Layer` (fmt, Jaeger-direct, in-memory test
recorder — the M15 integration test in
`crates/agentdb-api/tests/tracing_spans.rs` uses the last).

**Deferred.** W3C `traceparent` header extract/inject (Task A4 Step
2+3) is out of scope for this prototype and is a follow-up when
distributed traces across HTTP clients become valuable. Admin
endpoints are not yet instrumented — the hot path is covered, and
admin can gain spans as debugging needs them.

## Observability (M7)

Every mutation path records to `BoxMetrics`:

- `appends`, `forks`, `tier_transitions`, `quota_hits`, `faults` —
  monotonically-increasing counters
- `warm_sessions`, `cold_sessions` — gauges (bumped/dropped at
  session creation and tier transition)
- `inserts`, `query_find`, `query_aggregate` — wire-protocol counters
- `hedged_triggers`, `hedged_primary_wins`, `hedged_secondary_wins` —
  counters for cold/archive segment hedging (M15). See
  "Hedged reads" above.
- `events_indexed`, `index_snapshots_written`, `index_snapshot_failures`
  — counters for persistent + incremental indexing (M16). See
  "Persistent + incremental indexing" above.

`GET /api/admin/boxes` returns a per-box snapshot. `GET /api/stream` is
the Server-Sent Events channel; the server broadcasts:

- `MetricsTick` every 1s with gauge + counter snapshots per box
- `TierChange` on forced tier transition
- `ForkCreated` on successful fork
- `FaultInjected` on fault injection

The dashboard (`crates/agentdb-dashboard/static/app.js`) consumes both
the snapshot endpoint and the SSE stream to render box tiles + event
feed live.

## Control plane (M18 and later)

The sections above describe the original single-process prototype —
one process, one set of boxes, state persisted via RocksDB +
object_store, no coordination needed. M18 onward layer a **control
plane** on top of that core without rewriting it: a second SQLite
database (the "meta store") tracks cluster-wide state that every
process agrees on, and a bundle of background loops keep it fresh.

### Meta store (`crates/agentdb-meta`)

SQLite-backed cluster-wide metadata. One file (`meta.sqlite`) holds
five tables:

| Table | What it tracks |
|---|---|
| `tenants` | Global tenant id space — one row per external tenant id. Superset of the per-box `TenantRegistry`. |
| `nodes` | Cluster membership + heartbeat. Each running `agentdb serve` process upserts a row and bumps `last_heartbeat_ms` every 5s. |
| `tablets` | Runtime ownership units. Every box owns a tablet (today `tablet_id == box_id`); each row carries a monotonic `epoch` for fencing. |
| `scheduler_leader` | Singleton row (enforced via `CHECK (id = 1)`) holding the current scheduler leader's lease. |
| `shards` | 16384-row shard → tablet assignment. Default NULL means "route via hash"; operators can pin specific shards. |

Migrations are hand-rolled (`migrations.rs`), idempotent, forward-
only. Both SQLite (local dev) and Postgres (future production) are
target dialects; the crate currently implements only SQLite but the
helper functions keep dialect-agnostic SQL so a Postgres backend is
additive.

### Leases + fencing (M21)

Each box holds a lease on its tablet row, acquired via
`tablets::assume_ownership` (a CAS on `epoch` — the row's epoch
is the fencing token). A background `LeaseRenewalJob` refreshes
the lease every `AGENTDB_LEASE_RENEW_SECS` (default 10s); the
lease duration is `AGENTDB_LEASE_DURATION_SECS` (30s) so a missed
renewal window is tolerable. Writers check `is_live()` on every
request — a stale lease returns `Error::LeaseLost` immediately,
which surfaces as HTTP 503 `code: "lease_lost"` with a doc_url.

### Node heartbeat + staleness (M22 piece 1)

`agentdb_meta::nodes::upsert` on boot; `heartbeat` every
`AGENTDB_HEARTBEAT_SECS` (default 5s). The
`/api/admin/nodes?stale_threshold_ms=N` endpoint flags rows whose
`last_heartbeat_ms` is older than the threshold — operators see
stale nodes without needing a separate probe.

### Scheduler election (M22 piece 2)

Every node runs a `LeaderElection` loop (`agentdb_core::scheduler`)
that alternates between "try-acquire every `AGENTDB_LEADER_ATTEMPT_SECS`
(2s default)" and "renew every `AGENTDB_LEADER_RENEW_SECS` (5s)".
Exactly one process in the cluster wins; others sit as followers.
`GET /api/admin/scheduler` shows the current leader plus the local
node's view.

### Stale-lease reaper (M22 piece 3)

The leader runs a `StaleTabletReaper` that periodically scans the
`tablets` table (`AGENTDB_REAPER_TICK_SECS`, default 10s) for
entries whose lease expired longer than `AGENTDB_REAPER_SAFETY_MARGIN_SECS`
(5s) ago, clears the owner, and bumps the epoch. Followers' loops
no-op — `is_leader()` checks the scheduler state on every tick.

### Drain (M22 piece 4)

`POST /api/admin/drain` flips a process-level atomic
(`BoxRegistry::draining`). Subsequent writes return
`Error::NodeDraining` immediately; the lease-renewal loop stops
renewing, so after the lease duration + safety margin the reaper on
another node clears the rows. Operators get a clean handoff without
SIGTERM. No un-drain — restart the process to reverse.
`docs/runbooks/node_draining.md` has the full procedure.

### Per-tenant locking + admission semaphore (M23)

`Box_::write_lock: Mutex<()>` (serializes every tenant) is gone.
Replaced by:

- **Per-tenant lock map** (`tenant_locks: RwLock<HashMap<String, Arc<Mutex<()>>>>`)
  keyed by external tenant id. Different tenants on one box run in
  parallel; a single tenant still serializes. Preserves the
  mint-race invariant that the old single mutex provided.
- **Admission semaphore** (`tokio::sync::Semaphore`, cap via
  `AGENTDB_ADMISSION_CAPACITY`, default 64) bounding concurrent
  writes per box. Waits rather than rejects — complements the QPS
  token bucket, which rejects.

### Shard hash + LocationProxy + write-path routing (M20)

`agentdb_core::shard::shard_id(session_external) -> u16` is FNV-1a
(64-bit) modulo 16384. `LocationProxy` holds a cached lookup
`shard_id → Option<tablet_id>`, refreshed from the `shards` table
every `AGENTDB_SHARD_REFRESH_SECS` (30s). Operators pin shards via
`POST /api/admin/shards/:id` (immediately refreshes the proxy) and
inspect assignments via `GET /api/admin/shards/:id` or
`GET /api/admin/shards?session_id=...`.

**Write-path integration.** At server start, the proxy is attached
to the `BoxRegistry` via `attach_location`. `BoxRegistry::route_session(tenant, session)`
then consults the proxy before falling back to tenant-hash
routing:

1. If the proxy has an override for the session's shard, look up
   the tablet id.
2. If that tablet id is within this node's box range
   (`0..num_boxes`), route to `boxes[tablet_id]`. Since
   `tablet_id == box_id` on a single-node cluster, this is an
   identity map today; the indirection is there so multi-node
   adds cross-node forwarding without changing callers.
3. Otherwise (no override, or override points at a remote/stale
   tablet) fall back to `route_tenant(tenant)` — the pre-M20
   hash behavior. The fallback is deliberate: the write path
   must never panic on a stale override, and falling back
   guarantees writes land somewhere durable even if the
   operator's override is wrong.

`append_and_fetch_with_actor` uses `route_session`; every other
caller (fork, admin endpoints, tier/lifecycle background jobs)
stays on `route_tenant` because it operates on tenant-wide state.

### Reaper-driven shard reassignment (M22)

When `clear_expired_ownership` returns a non-empty set of reaped
tablets, the reaper does a second pass:

1. `tablets::list_all` → keep only rows with `owner_node_id.is_some()`
   and `lease_expires_at_ms > now`. That's the "live" set.
2. For each reaped tablet, `shards::list_by_tablet` returns the
   shard ids that pointed at it.
3. Round-robin those shards across the live set, one
   `set_assignment` call per shard. The counter
   `shard_reassignments_total` increments once per move.
4. Refresh the local `LocationProxy` immediately so this node's
   write path picks up the new routing on the next request
   (rather than waiting up to 30s for the periodic refresh).

If the live set is empty (every tablet is dead — the whole
cluster is down), the reaper logs a WARN and leaves overrides
alone. Writes for affected shards fall through `route_session`'s
safety net to tenant-hash routing.

Round-robin is deliberately simple; load-aware reassignment
requires per-tablet metrics that don't exist yet (M25 work).

### Clap CLI (M26 piece 1)

The binary is now `agentdb` (was `agentdb-server`). Subcommands:

```
agentdb serve [--dev]       long-running server
agentdb dump-config         print effective env + defaults as JSON
agentdb drain               POST /api/admin/drain
agentdb show-tablets        GET  /api/admin/tablets
agentdb show-scheduler      GET  /api/admin/scheduler
agentdb show-nodes          GET  /api/admin/nodes
agentdb show-shard <sid>    GET  /api/admin/shards?session_id=...
agentdb set-shard <id> ...  POST /api/admin/shards/:id
agentdb list-shard-overrides GET /api/admin/shards/list
```

`--dev` sets up an ephemeral tempdir for KV + object store + meta;
every storage env var is auto-overridden so `agentdb serve --dev` is
a zero-config quickstart. Admin CLI subcommands exit 1 on 4xx and
2 on 5xx for shell-pipeline branching.

### Docker + compose (M26 piece 2)

`Dockerfile` is multi-stage (Rust builder → debian-slim runtime,
non-root user, tini PID 1). `examples/compose/docker-compose.yml`
boots a single AgentDB node backed by MinIO for S3-compat; the
AgentDB process runs the production-shape storage path (SlateDB on
S3). Multi-node compose is blocked on M20 — see the compose
`README.md` for the reason.

### Prometheus exposition (M27 piece)

`GET /metrics` returns the standard Prometheus text-format body —
one HELP/TYPE block per metric plus per-box samples labelled by
`box`. No `prometheus` crate dep; the format is trivial. Scraping
an AgentDB node needs no extra config beyond pointing at
`http://<node>:7878/metrics`.

### Structured `/health/ready` (M27 piece)

Failure body is a `reasons: []` array of `{code, message, doc_url}`
objects, one per distinct problem (so "node draining *and* a
lease expired" surface both signals, not just one). `code` is a
typed enum — `node_draining`, `lease_not_live` today, easy to add
more. Every code carries a runbook URL (same contract as REST
error `doc_url`).

### Runbooks (M27 piece)

`docs/runbooks/` carries one file per user-facing error code. Every
REST error response (including `lease_lost`, `fault_injected`,
`quota_exceeded`, `node_draining`, `storage_error`, `invalid_request`,
`not_found`, `serialization_error`) embeds a `doc_url` pointing at
the relevant file. The structure is always Symptom → Diagnose →
Verify → Remediate → Prevent so operators skimming one runbook
know the shape of every other.

## What the prototype does NOT do

The design doc describes a much larger system. The prototype
intentionally cuts these:

- **No real distributed KV.** RocksDB is a single-node store;
  "box" is a logical partition, not a shard.
- **Single-node writer model.** The `object_store` crate now supports
  S3 end-to-end via the `SlateKvStore` backend
  (`AGENTDB_KV_BACKEND=slatedb AGENTDB_OBJECT_STORE=s3`), but AgentDB
  still assumes a single writer per box — multi-node coordination,
  write fencing, and leader election are not implemented.
- **ANN is in-memory and non-persistent.** HNSW via `instant-distance`
  is built lazily per box on first query and rebuilt from scratch on
  server restart. Incremental updates and persisted snapshots are
  future work.
- **Lifecycle webhooks** fire fire-and-forget HTTP POSTs on every
  tier transition (per-box URL via `POST /api/admin/boxes/:id/webhook`,
  no retries, 5s timeout, no auth beyond HTTPS). LLM hooks and
  derivative-data pipelines are out of scope — the integrator runs
  them behind the webhook.
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
