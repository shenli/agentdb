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

- a **per-session lock map** (`session_locks`) on the hot
  path so different sessions of one tenant run in parallel
  (M26 Phase 2; the prior per-tenant lock now narrowly
  serializes only the get_or_mint flow)
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
session-scoped read gaps; **M26 Phase 4** closes the last one — the
in-memory text/vector indexes' `build` and `replay_events_after`
paths now treat `Tier::Archive` identically to `Tier::Cold`,
reading the same segment via `read_segment` instead of skipping the
session. After this, `fetch_last_n_merged` on a forked-archived
parent, `plan_locality_bound` on an archived session, Mongo wire
`find` on archived sessions, and unhinted text/vector retrieval all
return semantically-correct results. The archive tier now affects
**latency**, not **semantics** — which is exactly the contract in
`docs/agent-db-v2.md` §6.

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

**W3C trace-context propagation across nodes.** Every cross-node
forward emits a `traceparent` header, and an axum middleware on the
receiving side (`agentdb_api::trace_context::trace_parent_middleware`)
extracts it and attaches it as the parent of the local
`rest.request` span. Multi-node `append_and_fetch` and similar
forwarded paths therefore appear as one connected trace tree spanning
both nodes, rather than two disconnected ones. The propagator is
hand-rolled (no extra crate) and covers the standard `00-…` format;
unknown versions and malformed headers fall through to "this request
starts a new trace."

**Spans added on infrastructure paths.** Beyond the request-tree
shape above, the following control-plane operations also emit spans:
`forward.request` (cross-node REST forward, with target node id and
HTTP status), `placement.sweep_once` and `reconciler.reconcile_once`
(multi-node tablet placement loops), `lease.acquire` / `lease.renew`
(per-tablet ownership), and `node_reaper.sweep_once` (membership
state transitions). When something goes wrong in a multi-node
deployment these are typically what you want to see first.

**Library-side OTel surface.** `agentdb-api` carries
`opentelemetry` + `tracing-opentelemetry` solely for the propagator;
business-logic crates (`agentdb-core`, `agentdb-wire`,
`agentdb-meta`) still use `#[tracing::instrument]` exclusively. The
OTel SDK + exporter live entirely in the server binary, so libraries
remain usable with any `tracing_subscriber::Layer` (fmt,
Jaeger-direct, in-memory test recorder — see
`crates/agentdb-api/tests/tracing_spans.rs` and
`tests/trace_propagation.rs`).

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
feed live. The page is tabbed: Overview (boxes + events), Slow queries
(`GET /api/admin/slow-queries`), Audit log
(`GET /api/admin/audit`), Tokens (mint / list / revoke with RBAC
scope), and Tenant quotas (list / PUT / DELETE). Each admin tab
polls its endpoint every ~5s and only when visible.

**Auth.** An `<input>` in the header accepts an admin bearer
token, which the dashboard persists in `localStorage` under
`agentdb.admin.token` and sends as `Authorization: Bearer ...`
on every admin fetch. On an auth-less dev server the input
stays empty and requests go through plaintext. Non-2xx
responses surface a human-readable hint inline (401 → "set an
admin token above", 403 → "this token lacks the required
scope", 503 → "meta store not configured"); no silent failures.

## Control plane (M18 and later)

The sections above describe the original single-process prototype —
one process, one set of boxes, state persisted via RocksDB +
object_store, no coordination needed. M18 onward layer a **control
plane** on top of that core without rewriting it: a second SQLite
database (the "meta store") tracks cluster-wide state that every
process agrees on, and a bundle of background loops keep it fresh.

### Meta store (`crates/agentdb-meta`)

Dual-dialect cluster-wide metadata. `AGENTDB_META_BACKEND`
selects:

- **`sqlite`** (default) — file at `AGENTDB_META_PATH`
  (defaults to `meta.sqlite`). Zero-dep, great for dev /
  single-node.
- **`postgres`** — URL at `AGENTDB_META_URL`
  (`postgres://user:pass@host/db`). Wider ops tooling,
  LISTEN/NOTIFY-driven change propagation.

Tables both backends carry:

| Table | What it tracks |
|---|---|
| `tenants` | Global tenant id space — one row per external tenant id. Superset of the per-box `TenantRegistry`. |
| `nodes` | Cluster membership + heartbeat. Each running `agentdb serve` process upserts a row and bumps `last_heartbeat_ms` every 5s. `address` + `mongo_address` are advertised for cross-node forwarding. |
| `tablets` | Runtime ownership units. Each row carries a monotonic `epoch` for fencing. |
| `scheduler_leader` | Singleton row (enforced via `CHECK (id = 1)`) holding the current scheduler leader's lease. |
| `shards` | 16384-row shard → tablet assignment. Default NULL means "route via hash". |
| `users` | SCRAM-SHA-256 creds, one per tenant. |
| `admin_tokens` | Admin bearer tokens + their RBAC scope. |
| `admin_audit` | Append-only log of every `/api/admin/*` request. |
| `tenant_quotas` | Global per-tenant rate limits. |
| `session_locators` | Canonical session lookup record (M26 Phase 1): one row per external session id pointing at the tenant + tablet + shard. Populated on mint; not yet consulted on the routing path — see "Session locator (M26 Phase 1)" below. |

Migrations are hand-rolled (`migrations.rs`), idempotent,
forward-only, and carry **two SQL variants side-by-side** —
`NNN_name.sql` (SQLite) + `NNN_name.pg.sql` (Postgres). The
runner picks the right file based on the backend discriminant.

Runtime queries go through `sqlx::Any`, which rewrites `?`
placeholders to `$N` for Postgres. The small number of
genuinely dialect-specific operations (`VACUUM INTO` vs
`pg_dump` in `backup()`, `LISTEN/NOTIFY`) branch on
`MetaDialect`.

**Change propagation.**
Migration 009 attaches Postgres triggers to `shards`, `nodes`,
`tablets` that `NOTIFY agentdb_<table>_changed` with the PK
of the changed row. The `change_notifier::subscribe` stream
forwards those to consumers (LocationProxy, NodeAddressProxy)
as a "refresh now" hint on top of the existing periodic
polling. SQLite has no NOTIFY primitive; the stream is inert
and callers fall back to the periodic refresh loops they
already run.

### Leases + fencing (M21)

Each box holds a lease on its tablet row, acquired via
`tablets::assume_ownership` (a CAS on `epoch` — the row's epoch
is the fencing token). A background `LeaseRenewalJob` refreshes
the lease every `AGENTDB_LEASE_RENEW_SECS` (default 10s); the
lease duration is `AGENTDB_LEASE_DURATION_SECS` (30s) so a missed
renewal window is tolerable. Writers check `is_live()` on every
request — a stale lease returns `Error::LeaseLost` immediately,
which surfaces as HTTP 503 `code: "lease_lost"` with a doc_url.

### Node lifecycle

Rows in `nodes` walk a small state machine managed jointly by
each node's own heartbeat loop and the leader-owned
`NodeReaper`:

```
boot                                shutdown (SIGTERM/SIGINT)
  │                                        │
  v                                        v
 alive ───heartbeat stale────> dead ──gc_after──> removed
  │                                  ^             (row gone)
  │   drain (op cmd)                 │
  └─────> draining ────stop heartbeat──┘
  │
  └─────> leaving ─────────────────────┘
          (graceful shutdown)
```

- **Registration.** Every `agentdb serve` process calls
  `agentdb_meta::nodes::upsert` on boot with
  `state = "alive"`, advertising both its REST address and
  (optionally) its Mongo wire listener address for cross-node
  forwarding.
- **Heartbeat.** A background tokio task calls
  `agentdb_meta::nodes::heartbeat` every
  `AGENTDB_HEARTBEAT_SECS` (default 5s) to bump
  `last_heartbeat_ms`. Transient meta errors are logged and
  retried on the next tick; they don't abort the loop.
- **Failure detection.** The scheduler leader runs
  `NodeReaper` (`AGENTDB_NODE_REAPER_TICK_SECS`, default 10s).
  On each tick it flips every `state = 'alive'` row whose
  `last_heartbeat_ms` is older than
  `AGENTDB_NODE_DEAD_AFTER_SECS` (default 30s) to
  `state = 'dead'`. The original heartbeat timestamp is
  preserved so operators can see how long the node has been
  gone and the GC pass can use it as an age signal.
- **Dead-row GC.** The same tick also removes any
  `state = 'dead'` row whose `last_heartbeat_ms` is older than
  `AGENTDB_NODE_GC_AFTER_SECS` (default 3600s). Keeps the
  `nodes` table bounded across deployments without letting dead
  rows vanish before an operator has a chance to see them.
- **Graceful shutdown.** On SIGTERM / SIGINT the server flips
  its own row to `state = 'leaving'` before exiting. Peers
  that hold a cached `NodeAddressProxy` invalidate on the next
  `pg_notify` delivery (Postgres) or next periodic refresh
  (SQLite), so cross-node writes route around a shutting-down
  node immediately instead of waiting for heartbeat-stale
  detection.
- **Change propagation.** The migration-009 `pg_notify`
  triggers on `nodes`/`shards`/`tablets` drive an
  `agentdb_meta::change_notifier` stream subscribed on each
  node. On every `Channel::Nodes` event, `NodeAddressProxy`
  force-refreshes its cache. On SQLite the stream is inert —
  the periodic refresh loops cover correctness.
- **Operator eviction.** `DELETE /api/admin/nodes/{id}` drops
  a row manually. Returns HTTP 409 on still-alive rows (drain
  first); otherwise it's idempotent (removing an unknown id
  returns `removed: false` with 200).

The `/api/admin/nodes?stale_threshold_ms=N` endpoint still
surfaces `is_stale` as a computed flag for short-lived
monitoring probes — that's independent of the state-machine
transitions above. `/api/admin/scheduler` reports
`nodes_marked_dead_total` + `nodes_gc_removed_total` from the
current leader's `NodeReaper`.

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

### Session locator (M26 Phase 1)

The 2026-04-24 design review flagged that session identity is
still effectively `(box_id, tenant_id, session_external)` —
tenant is load-bearing, which contradicts the v2 commitment to
*sessions are globally unique* and *tenant is auth-bound*. The
`session_locators` table is the first concrete step away from
that:

```
session_locators(
  session_id          PRIMARY KEY,    -- the globally-unique external id
  tenant_external_id,                 -- auth-bound tenant identity (M26 Phase 1b)
  tenant_internal_id,                 -- per-process registry id (informational)
  tablet_id,                          -- where it currently lives (NULL = unplaced)
  shard_id,                           -- denormalised FNV(session_id) % SHARD_COUNT
  created_at_ms
)
```

`tenant_external_id` is the authoritative tenant identity used
by `fork`'s tenant-resolution path; `tenant_internal_id` is
preserved for diagnostics but is **not** globally meaningful
(it's the per-process `TenantRegistry`'s local id, which is
NOT cross-cluster stable until the tenant table itself becomes
the source of truth in a later milestone).

#### Locator-driven write routing

`BoxRegistry::route_session_for_write(tenant, session)` is the
write-side counterpart to the locator-driven read fanout. It
asks `session_locators::get(session)` first; if a row with
`tablet_id` is present, the write is routed to that tablet
(local box or `RouteDecision::Remote` via `NodeAddressProxy`,
depending on which node owns it). Locator misses fall through
to the pre-existing `route_session_for_io` chain — shard
override (`LocationProxy`) → tenant-hash — so pre-M26 sessions
and brand-new mints behave exactly as before.

The REST `append_and_fetch` handler and the inner
`primitives::append_and_fetch_with_actor` both consult the
same helper, so a write that crosses a node boundary is
detected once at the API layer (forwards to the peer) and the
inner routing won't second-guess the decision after a
concurrent locator update — TOCTOU between the two routing
calls degrades to the legacy route rather than failing the
write.

After this lands, both reads (PR #74) and writes go through
the locator on the hot path. `session_locator` is now the
canonical "where does session X live" record across the
entire data path — the v2 promise from `agent-db-v2.md`
that "writes route by session, not by tenant hash" is
finally true on the wire, not just on the API surface.

#### Locator-driven fanout (Phase 3)

`session_locators::list_by_tenant_external` enumerates every
session of a tenant regardless of which local box hosts it.
The Mongo wire `find` handler uses this to fan out unpinned
queries: the targets are computed as the union of the
locator-listed sessions on local tablets and the tenant-hash
box's local-KV-listed sessions (the latter for back-compat
with sessions that predate locator population). Each session
is read against the box that actually hosts it, so an unpinned
`find` after a shard override or tablet handoff still returns
events from every relevant box on this node.

Cross-node sessions (locator points at a tablet whose box
isn't in this process's registry) are skipped with a tracing
warn — wire-level cross-node Mongo forwarding for read traffic
is the explicit Phase 3 follow-up. Pinned-session queries that
target a peer-owned shard still produce
`NotWritablePrimary` (code 10107) so drivers reconnect to the
right peer.

Populated by `agentdb_core::primitives::populate_session_locator`
on every newly-minted session: REST `append_and_fetch`,
Mongo wire writes (which go through the same primitive), and
the REST `/api/sessions/:id/fork` handler all populate the
child's locator before returning. The upsert is
`INSERT OR IGNORE`, so repeat traffic is one cheap meta
roundtrip; the first call wins, leaving `update_tablet`
(handoff/migration) as the only path that retargets a row.

This PR populates locators but does NOT yet consult them on
the routing path. That's deliberate: the populate→consume
split lets each follow-up M26 phase land in isolation. Future
phases (per
`docs/2026-04-24-next-step-design.md`):

- **Phase 2**: per-session lock map keyed on session id, replacing
  the per-tenant write lock that's currently the dominant
  intra-tenant concurrency bottleneck.
- **Phase 3**: route session-scoped reads via the locator;
  cross-tablet `find` / `aggregate` fans out across the tenant's
  locator-listed tablets and merges.
- **Phase 4**: include archive-tier segments in indexed
  retrieval so unhinted text/vector queries don't quietly drop
  archived data.

A locator is a control-plane record; it's purposely small so
that even "tens of millions of sessions" fits comfortably in
the existing meta store.

### Tablet placement and reconciliation

The previous version of this section described every node
hard-claiming `[0..AGENTDB_BOXES)` at boot, which forced
single-node deployments because two nodes would race on the
same tablet ids. That changed. Today each `agentdb serve`
process boots with an empty `BoxRegistry`; the scheduler-leader
runs `TabletPlacement`, the per-node `TabletReconciler` picks
up assignments. The split mirrors §2.5 of `agent-db-v2.md`.

```
control plane:
  nodes table     ←─ heartbeat        ←─ each agentdb process
                          │
  tablets table    ──assign_to──▶  ←── round-robin from leader's
                          │            TabletPlacement loop
                          ▼
  add_box(tablet_id)  ◀── TabletReconciler on each node
                          │
                          ▼
       Box_ + LeaseManager + LeaseRenewalJob
```

- **Bootstrap.** When the leader's first placement tick finds
  an empty `tablets` table, it seeds
  `[0..AGENTDB_BOXES)` rows (env defaults to 4). The same env
  is now cluster-wide rather than per-process.
- **Placement.** `AGENTDB_PLACEMENT_TICK_SECS` (default 5s).
  Each tick runs three phases on the leader. (1) **Bootstrap**
  seeds `[0..AGENTDB_CLUSTER_TABLETS)` if the table is empty.
  (2) **Rebalance**: count tablets per alive node, compute
  `target = ceil(total_alive_owned / num_alive)`. For every
  alive node owning more than `target`, the leader writes a
  cooperative-handoff signal (`handoff_target_node_id`) on
  each excess tablet, picking the currently-least-loaded peer
  per signal so multi-target distribution comes out even.
  Ownership is **not** transferred here — that happens when
  the source's `TabletReconciler` observes the signal,
  drops its in-process `Box_`, and atomically calls
  `tablets::complete_handoff` (which clears the signal,
  zeros the lease, bumps the epoch, and points the row at
  the recorded target). Until completion, the source keeps
  serving writes; there's no NULL-owner gap and in-flight
  writes don't see `LeaseLost`. The rebalance is suppressed
  when `max_count - min_count ≤ 1` so the unavoidable +1
  imbalance from a non-divisible total doesn't cause
  perpetual reshuffling. (3) **Assign-unowned**: every
  `owner_node_id IS NULL` tablet (failover orphan) is
  assigned to the currently-least-loaded alive node (ties
  broken by node-id). `tablets::assign_to` writes the owner
  without granting a lease — that's the receiving node's job.
- **Reconciliation.** `AGENTDB_RECONCILER_TICK_SECS` (default
  3s). Each node lists tablets where it's been promised
  ownership and calls `BoxRegistry::add_box`, which acquires
  the lease via `LeaseManager::acquire` (epoch++ via
  `assume_ownership`). Already-running boxes are a no-op.
- **Failover.** `StaleTabletReaper` clears expired leases
  (`owner_node_id` → NULL), the next placement tick assigns
  to a live node, and that node's reconciler picks it up.
- **Initial sync pass at boot.** The server runs one
  `placement::sweep_once` (if leader) and one
  `reconciler::reconcile_once` synchronously before binding
  the HTTP listener so `/health/ready` reports the
  post-reconcile box set rather than a transient empty
  registry.

What's deliberately **not** here yet: load metrics beyond
"tablet count" — request rate, lock waits, segment GC
backlog. Today's rebalance balances on ownership count
alone, which is enough to keep one cold node from staying
cold but doesn't react to non-uniform per-tablet load.
Tablet-level load signals are the natural follow-up.

#### Cross-AZ failure-domain awareness

Per `agent-db-v2.md` §2.7, each node declares its AZ at boot
via `AGENTDB_NODE_AZ` (written into `nodes.availability_zone`,
migration 013). Two scheduler-policy hooks read it:

- **Assign-unowned**: ranks alive nodes by `(AZ tablet
  count, per-node tablet count, node-id)`. Lower wins on
  every key. With more than one AZ represented, the
  AZ-load tier dominates, so unowned tablets spread
  evenly across AZs.
- **Rebalance handoff target picker**: filters peers to
  those in a *different* AZ from the source first. Among
  that set, the existing lowest-projected-load logic
  picks. If no cross-AZ peer exists (single-AZ cluster, or
  no AZs declared at all), the filter empties and the
  picker falls back to "any alive peer" — single-AZ
  behavior is unchanged.

Net effect: a single-AZ outage's blast radius drops from
"every tablet in that AZ loses its writer until the AZ
recovers" to "every tablet in that AZ loses its writer for
the failover window" (target p95 < 30s, design doc §5.2).
No quorum writes, no Paxos — durability stays anchored on
the multi-AZ object store, and the scheduler picks
diverse failover candidates as a matter of policy.

### Per-session locking + admission semaphore

The 2026-04-24 design review flagged that even with the M23
per-tenant lock map, one tenant's many concurrent sessions
all serialized on the same lock — a structural bottleneck
for agent workloads where a single tenant routinely drives
hundreds of in-flight sessions. M26 Phase 2 splits that lock
into two:

- **`tenant_locks`** (`RwLock<HashMap<String, Arc<Mutex<()>>>>`)
  is now narrowly the **mint serialization** lock. The
  `get_or_mint` flow advances per-tenant `next_session_id`
  state in the local KV; two concurrent first-time mints
  would race on that counter without it. Held only across
  the get-or-mint phase of each write.
- **`session_locks`** (same shape, keyed by external session
  id) is the **hot-path write lock**. Held for the duration
  of the append + index update. Two writes to the same
  session serialize; writes to different sessions of the
  same tenant proceed in parallel. The append flow is
  two-phase: `tenant_locks → drop → session_locks`, with a
  re-fetch of the session row under the session lock so
  concurrent appenders that both passed phase 1 don't
  overwrite each other's `next_seq`.
- **Admission semaphore** (`tokio::sync::Semaphore`, cap via
  `AGENTDB_ADMISSION_CAPACITY`, default 64) bounding concurrent
  writes per box. Waits rather than rejects — complements the QPS
  token bucket, which rejects.

Both lock maps are swept by a periodic background task
(`agentdb_core::LockGc`, default tick
`AGENTDB_LOCK_GC_TICK_SECS=60`) that drops entries whose
`Arc<Mutex>` has no other holders. Re-acquisition after a
prune is identical to first-touch acquisition — the next
`session_write_lock`/`tenant_write_lock` call reinserts a
fresh entry. Without this, the maps grow monotonically with
every distinct external id ever touched, leaking memory
proportional to the historical (rather than working) set.
Counters: `LockGc::session_locks_pruned_total()` and
`tenant_locks_pruned_total()`.

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

### Cross-node forwarding (M20 multi-node)

Before this slice, `route_session` silently fell back to
tenant-hash on the local node whenever a shard override
pointed at a tablet this node didn't own. Safe on a single-
node cluster; silently wrong on multi-node. Two PRs close
the gap — the first shipped write-path forwarding, this one
extends reads and generalizes the helper.

**Routing primitives.**

- **`NodeAddressProxy`** (`agentdb_core::forwarding`). Caches
  `tablet_id → NodeTarget { node_id, address }` from joining
  the `tablets` and `nodes` tables every
  `AGENTDB_NODE_ADDR_REFRESH_SECS` (default 10s — shorter
  than LocationProxy's 30s because node-address changes are
  failover-critical).
- **`RouteDecision`** enum returned by
  `BoxRegistry::route_session_for_io`:
  `Local(Arc<Box_>)` or
  `Remote { target: NodeTarget, tablet_id: i64 }`. Same
  decision powers writes and reads; the method was named
  `route_session_for_write` in the first slice and renamed
  when reads adopted it.
- **`agentdb_api::forwarding::forward_request`** — generic
  HTTP forwarder taking method + path + optional JSON body.
  `forward_append_and_fetch`, `forward_admin_session_get`,
  `forward_fork` are thin wrappers that build the URL.

**Safety properties preserved on every forwarded request:**

- **Tenant-scope ACL.** `Authorization` header forwarded
  verbatim; the remote node's Basic Auth middleware verifies
  independently and rejects cross-tenant access with its own
  403.
- **HLC merge.** `X-AgentDB-HLC` forwarded in both
  directions so the cluster clock converges across the hop.
- **Loop guard.** Every forward adds
  `X-AgentDB-Forwarded: 1`. A node that receives a request
  with that header *and* still routes Remote returns **421
  Misdirected Request** instead of forwarding again.
- **No internal retry.** Forwarding failures surface as 503
  immediately. Operators see slow failover rather than it
  being masked.

**Covered endpoints:**

| Endpoint | Method | Cross-node behavior |
|---|---|---|
| `/api/tenants/:tid/sessions/:sid/append_and_fetch` | POST | forwarded |
| `/api/admin/sessions/:sid?tenant_id=...` | GET | forwarded |
| `/api/sessions/:sid/fork` | POST | forwarded |
| Mongo `find` with `{session_id: S}` filter | — | 421-equivalent: `code: 10107 NotWritablePrimary` with owner address |
| Mongo `aggregate` pinning `session_id = S` | — | same as find |

**Metrics** (per box, credited to box 0 on the originating
node):
`agentdb_forwarded_writes_total`,
`agentdb_forwarded_write_failures_total`,
`agentdb_forwarded_reads_total`,
`agentdb_forwarded_read_failures_total`.

**Scope limits (still out):**

- **Mongo wire forwarding.** Binary, stateful per-connection
  protocol — SASL state doesn't transfer across a forwarded
  hop without an inter-node trust model. Mongo clients get
  `code: 10107` with the owner's address and reconnect;
  real forwarding is future work.
- **Tenant-wide reads** (`find` without a session filter,
  `listDatabases`, `listCollections`). Still tenant-hash
  routed. An override moves one session, not the whole
  tenant.
- **Child-inherits-parent-box.** A forked child session
  lives on whatever box its parent was on; the child's
  `session_id` hashes to a potentially different shard, so
  reads routed by shard hash may miss it. Closing this gap
  requires either migrating the child's session record at
  fork time or teaching the read path to follow the parent
  pointer.

### Hot-tablet detection + shed (M25)

The control plane knows how to *move* load between tablets
(M20 shard overrides, M22 reaper reassignment). What was
missing was the *signal* — which tablet is hot, and when? —
and a convenient bulk operation to act on it.

**Rate window.** `BoxMetrics` carries a 60-second sliding
window bucketed into six 10-second slots. Every
`record_append` and `record_admission_wait` fetch-adds into
the current slot; every ~10 seconds the window rolls one
slot forward. Readers sum across slots and compute:

- `appends_per_sec` = total appends / effective window sec
- `admission_wait_ratio` = total waits / max(total appends, 1)

Hot-path cost per record is one atomic fetch_add; no locks.
The rate window is `Option<RateWindow>` on `BoxMetrics` so
legacy `Default::default()` paths (tests) don't pay the
allocation.

**Detector.** A background task on the scheduler leader
samples each tablet's rate window every
`AGENTDB_HOT_DETECT_TICK_SECS` (default 10s). A tablet flips
`cool → hot` when its `admission_wait_ratio >
AGENTDB_HOT_WAIT_RATIO` (default 0.3) for
`AGENTDB_HOT_CONSECUTIVE` samples in a row (default 3,
≈30 seconds of sustained saturation). A single sample back
under threshold flips it `hot → cool` — asymmetric
hysteresis favors false negatives over false positives for
an operator-facing signal. Followers keep an empty
snapshot; admin endpoints on followers report "no hot
tablets."

Output lives in memory only (`Arc<RwLock<DetectorSnapshot>>`).
Hot/cool is a transient runtime signal, not a durable fact;
no SQLite writes.

**Admin surface.**

| Endpoint | Purpose |
|---|---|
| `GET /api/admin/tablets/hot` | Currently-flagged tablets + last sample time |
| `GET /api/admin/tablets/load` | Every tablet with `{ appends_per_sec, admission_wait_ratio, is_hot }` |
| `POST /api/admin/tablets/:id/shed` | Move a fraction of this tablet's overridden shards to another |
| `POST /api/admin/tablets/create` | Grow the box pool by one new tablet at runtime |

Shed body: `{ "to_tablet_id": i64 | null, "fraction": f64 | null }`.
`to_tablet_id = null` → server picks the least-loaded
tablet from the detector snapshot. `fraction` defaults to
0.5, clamps to (0, 1]. Returns the list of moved shard ids.

Shed does **not** migrate data. Old events stay on the
source tablet. Same semantics as `set_shard` — same
rationale (the "quiesce + handoff" gap is acknowledged in
ARCHITECTURE.md and is real-split-specific future work).

**Dynamic tablet creation** (`POST /api/admin/tablets/create`).
Body: `{ "tablet_id": i64 | null }`. When null, the server
picks `max(existing) + 1`. Calls
`BoxRegistry::add_box(tablet_id)` which:

1. Idempotently inserts the tablet row in meta.
2. Acquires the lease via the existing `LeaseManager::acquire`.
3. Builds a `Box_` and pushes it under a write lock on
   the `boxes` Vec.

The Vec lives behind `parking_lot::RwLock` (added in this
PR — replaces the old `Vec<Arc<Box_>>`); hot-path readers
take a read lock, clone the `Arc<Box_>` they need, and
drop the lock. Reads incur one atomic per request — not
measurable under normal load. Writes only happen when
operators call `create` — zero steady-state contention.

Operator workflow for relief under load:

1. `agentdb show-hot-tablets` — identify the saturated tablet
2. `agentdb create-tablet` — grow the pool by one
3. `agentdb shed-tablet <hot_id> --to <new_id>` — move half
   of the hot tablet's shards to the new one

**CLI.** `agentdb show-hot-tablets`, `agentdb
show-tablet-load`, `agentdb shed-tablet <id> [--to N]
[--fraction F]`, `agentdb create-tablet [--id N]`.

**Out of scope for this PR:**
- **Cross-node new-tablet placement.** `create-tablet`
  acquires the lease locally. To create a tablet on a
  different node, post to that node's admin URL.
- **Auto-shed / auto-create.** Detector flags; operator
  acts. Autonomy needs calibrated thresholds + anti-thrash
  logic that only production data can provide.
- **Shed with data handoff.** `POST /api/admin/tablets/:id/shed`
  accepts `{ "migrate_data": true }` to also rekey every
  warm session on the moved shards. Cold/archive + fork
  parents land in `skipped_sessions`; the rest land in
  `migrated_sessions` with event counts. Bare shed stays
  routing-only.
- **Auto-shed scheduler.** With
  `AGENTDB_AUTO_SHED_ENABLED=true`, a background task
  reads the detector's hot list on a tick (default 10s)
  and fires shed-with-migrate against the least-loaded
  sibling tablet. Per-tablet cooldown
  (`AGENTDB_AUTO_SHED_COOLDOWN_SECS`, default 300s)
  prevents thrash when the shed doesn't immediately cool
  the source.
- **Tablet deletion.** Once created, a tablet stays.
  Shrinking requires data migration.
- **Per-session hot-key detection.** Requires per-session
  metrics the prototype doesn't track.

### Warm-session data handoff

Closes the single biggest correctness gap from the M20 /
M22 / M25 era: when `shed` (or `set_shard`) reassigns a
shard, previously-written events stay on the old tablet,
so session-scoped reads routed to the new tablet miss
them.

**Design.**
`agentdb_core::handoff::migrate_warm_session(registry,
kv, tenants, sessions, tenant_external,
session_external, to_box_id, now_ms)` atomically rekeys
a warm session from its current box to the destination
box, in a single `write_batch` over the shared KV store:

1. Locate the source: scan the registry's current boxes
   for a matching `(tenant, session)`.
2. Refuse if `tier != Warm` (cold/archive segments embed
   `box_id` in their object-store paths; copying is out
   of scope).
3. Refuse if the session is a fork parent (child
   sessions carry `parent_session_internal_id` pointing
   into the source's session registry; cross-box fork
   trees aren't supported yet).
4. Mint or resolve the tenant + session on the
   destination. `tenant_internal_id` is per-box, so
   dest's internal id is different from source's —
   every migrated event's key has both ids remapped.
5. Scan the source's `Events` prefix; build a
   `WriteOp::Put(new_key) + WriteOp::Delete(old_key)`
   pair per event, plus the session record
   replace/delete pair.
6. `kv.write_batch(ops)` — atomic at the RocksDB level.
   Either every event is under the new keys or nothing
   is.
7. Replay events into the dest box's `PerBoxIndexes`
   (best-effort; the lazy-build path rebuilds from KV on
   first read if this step fails).

**Admin surface.**

| Endpoint | Purpose |
|---|---|
| `POST /api/admin/sessions/:sid/migrate` | Move one warm session; body `{ tenant_id, to_tablet_id }` |

Returns `{ events_migrated, from_tablet_id, to_tablet_id }`.
Errors: 404 (session or tenant not found), 409 (cold/archive,
or has forked children).

**CLI.** `agentdb migrate-session <sid> --tenant t --to N`.

**Operator workflow for full shed-with-data-handoff:**

```bash
# See what's hot
agentdb show-hot-tablets

# Move future traffic off the hot one
agentdb shed-tablet <hot> --to <new>

# Move the already-written data for each affected session
for sid in $(agentdb show-shard <session_hint> | jq -r ...); do
  agentdb migrate-session $sid --tenant t-1 --to <new>
done
```

Out of scope:
- **Cold / archive migration** — would need to copy the
  object-store segment or rewrite its path.
- **Cross-node migration** — intra-process (shared KV)
  only; cross-node would need snapshot shipping.
- **Fork-parent migration** — refused; children's
  parent pointers are box-local.
- **Quiesce / concurrent-write safety.** A writer that
  appends during the scan-window may have its event
  land on source after the scan and survive the
  Delete. Operators should migrate idle sessions only.

### Latency histograms

Every box carries two fixed-bucket histograms alongside
the existing counter metrics:

- `append_latency` — full
  `append_and_fetch_with_actor` duration (admission wait
  + tenant lock + fork + event commit + fetch tail).
  Recorded only on the success path.
- `admission_wait_latency` — time spent blocked on the
  per-box admission semaphore. Recorded only when a
  request actually waited (fast-path excluded) so the
  histogram reflects *pressure*, not uncontended writes.

**Buckets** (upper bound inclusive):
1 ms, 2 ms, 5 ms, 10 ms, 20 ms, 50 ms, 100 ms, 200 ms,
500 ms, 1 s, 2 s, 5 s, 10 s, +Inf. Fourteen lock-free
`AtomicU64` buckets + a running count + sum; `record(ns)`
is one array index + three fetch-adds. No allocation.

**Exposition.** Prometheus histogram shape: `_bucket{le}` +
`_sum` + `_count`. Bucket upper bounds in seconds so
standard histogram queries work out of the box.

**Admin.** `GET /api/admin/boxes/:id/latency` returns
`{ box_id, append, admission_wait }` where each view
carries `{ count, sum_ns, p50_ns, p95_ns, p99_ns }`.
Percentiles are linearly interpolated inside the hit
bucket.

**CLI.** `agentdb show-latency <box_id>`.

Out of scope: per-session histograms (needs a bounded
top-K sketch), read-side latency, `/metrics` labels
beyond `box` (tenant labels would balloon the cardinality).

### Admin audit log

Every request that reaches the admin-auth middleware (on
`/api/admin/*`) is persisted to the meta store's
`admin_audit` table, including 401 rejections. One row
records: timestamp, actor token id (NULL when the request
failed auth or took the bootstrap mint exception), method,
path, query string, response status, and client IP
(X-Forwarded-For leftmost entry — NULL when there's no
reverse proxy).

Reads are `GET /api/admin/audit` with filters: `limit`,
`since_ms`, `until_ms`, `actor_id`, `path_prefix`. Rows
return newest first; clients paginate by passing the
oldest `ts_ms` from a page as the next `until_ms`.
`agentdb audit-log` wraps this from the CLI.

Writes are fire-and-forget: the middleware spawns the
insert after `next.run(req)` so a slow SQLite write never
blocks the client response. Insert failures log at warn
and are otherwise invisible.

Out of scope: request-body hashing (bodies are tiny and
the interesting operations — who revoked which token, who
migrated which session — are visible from method + path +
query); time- or count-based retention (operators prune
the meta store directly today); surfacing the audit log
on `/metrics` (row counts are a gauge, but tailing is a
worse UX than the JSON endpoint).

### Cross-node Mongo wire forwarding

When a Mongo client connects to node-A but its session
`S` is owned by node-B, the wire server forwards the
entire OP_MSG frame to node-B and streams the reply back.
Pre-v2.1 this case returned
`code: 10107 NotWritablePrimary` with the peer address so
the driver could reconnect — that still works when auth
is required (see below).

How it works:

- Every node advertises its Mongo wire listener address
  in `nodes.mongo_address` (migration 008). `NodeTarget`
  carries it through the `NodeAddressProxy` cache.
- Before dispatching an OP_MSG, the wire server inspects
  the command (`find` / `insert` / `update` / `delete` /
  `aggregate`). For commands carrying a top-level
  `session_id` (single session only — mixed-session
  inserts fall through to local handling where they'd be
  rejected anyway), it calls
  `BoxRegistry::route_session_for_io`.
- If the route is remote AND the peer advertised a Mongo
  address, `forward::forward_op_msg` dials the peer's
  Mongo port, writes the raw frame, reads the framed
  reply, and relays it back to the client. No cursor
  continuation support: each command is its own forward.
- Counters: `agentdb_wire_forwarded_total` and
  `agentdb_wire_forward_failures_total` on `/metrics`.

**Auth-required mode is NOT forwarded.** SASL session
state is per-connection and doesn't transfer across a
forward — so when `AGENTDB_REQUIRE_AUTH=true`, the wire
handler keeps returning the legacy NotWritablePrimary
error and drivers reconnect to the owner. Proper
forwarding under SCRAM needs a separate inter-node trust
model (shared secret, mTLS, or a synthetic internal auth
identity) and is deferred.

**Loop safety.** When a forwarded frame reaches node-B,
node-B's own `routing_decision` returns `None` (the
session routes locally) so the request is dispatched
normally — no infinite forwarding ping-pong.

**Timeouts.** `forward_op_msg` bounds each forward to
30s (`DEFAULT_FORWARD_TIMEOUT`) so a misbehaving peer
can't stall the client connection indefinitely. Failed
forwards increment the failures counter and fall through
to local dispatch (which emits the NotWritablePrimary
signal, giving the driver a path to reconnect).

### Admin token RBAC

Admin tokens now carry a scope. Two levels in this PR:

- `admin` — full access, any HTTP method, any
  `/api/admin/*` route. The default, and the scope the
  bootstrap mint always produces.
- `readonly` — `GET` only. `POST` / `PUT` / `DELETE`
  return `403 Forbidden`; the audit log records the
  attempt with the actor_token_id, so oncall can see
  "alice tried to drain the node but her token was
  readonly."

The scope column (`admin_tokens.scope`) was added in
migration 007 with a `'admin'` default, so every pre-
existing token keeps working across upgrade — no
re-mint required.

Scope is checked after the existing hash-verify pass in
`admin_auth_middleware`. An `admin` scope authorizes any
method; `readonly` authorizes only `GET`. HEAD/OPTIONS
aren't used by the admin surface today and are refused
defensively — a future route addition never quietly lets
a readonly token through.

Mint carries an optional `scope` field in the body
(default `admin`). CLI: `agentdb admin-token add
--scope readonly --description grafana`. The list /
summary surface includes the scope alongside description.

Out of scope: finer-grained scopes (e.g. "tablets only",
"audit only") — two levels covers the common
"oncall monitors" split and keeps the ACL table simple.
Future scopes can widen the column without another
migration.

### Config hot-reload

`POST /api/admin/reload` (or `SIGHUP` on Unix, or
`agentdb reload`) re-reads env + meta and pushes fresh
values into the live subsystems without a restart.

Currently reloaded:

- `AGENTDB_SLOW_QUERY_THRESHOLD_MS` — `SlowQueryBuffer`
  stores the threshold in an atomic; update takes effect
  on the very next request.
- `AGENTDB_SLOW_QUERY_BUFFER_CAPACITY` — shrinking evicts
  oldest to fit; growing just raises the cap.
- `RUST_LOG` — swapped via `tracing_subscriber::reload`.
  Invalid filters are rejected without disturbing the
  running filter.
- `tenant_quotas` meta table — `TenantQuotaRegistry::hydrate`
  reconciles with `DB is the source of truth`:  new rows
  get fresh buckets, removed rows drop their bucket,
  unchanged rows keep their in-memory state (including
  accumulated deny counters).

Intentionally NOT reloaded:

- `AGENTDB_ADMISSION_CAPACITY` — resizing a running Tokio
  semaphore mid-flight is a footgun.
- TLS certs — rotation is a distinct concern (future PR).
- Meta / KV / object-store backends — not hot-swappable.

The reload target is an `Arc<dyn Fn() -> Future<...>>`
extension attached to axum. The server binary installs
the real closure; tests inject a fake. The SIGHUP
handler shares the same closure so signals and HTTP
take the same code path.

### Per-tenant rate limits

Per-box admission already had two token buckets: a global
box cap (`Quota::max_qps`) and a per-box-per-tenant cap
(`Quota::max_tenant_qps`). Those protect the box from a
noisy tenant. What they do **not** do is cap a single
tenant's aggregate impact across every box.

The global per-tenant rate limit closes that gap. One
`TokenBucket` per tenant, keyed by the external tenant id
(the string), held in an in-memory `TenantQuotaRegistry`
on the `BoxRegistry`. The persistent source of truth is
the meta store's `tenant_quotas` table (migration 006).
Server startup hydrates the registry from meta before the
HTTP listener binds, so no cold-start window leaks
unthrottled traffic from a quota'd tenant.

`append_and_fetch_with_actor` consults the registry before
the per-box admission semaphore — denial fails fast
without parking a semaphore permit. Denials surface as
`429 TooManyRequests` with
`scope: "tenant_global"` in the error body.

**Admin surface.** `/api/admin/tenant-quotas` — PUT / GET /
DELETE by tenant id; list via the index endpoint. CLI:
`agentdb set-tenant-quota <id> --max-qps N --burst M`,
`agentdb show-tenant-quota [id]`,
`agentdb clear-tenant-quota <id>`.

**Observability.** `agentdb_tenant_quota_denies_total{tenant="..."}`
on `/metrics`, one series per tenant that has denied since
process start. Cardinality is operator-controlled:
unconfigured tenants never deny and never produce a series.

**Scope.** Per-node, not cluster-global. Multi-node
deployments divide the quota by node count manually;
cluster-global coordination needs a shared token source
(out of scope for the prototype).

### Slow-query log

Latency histograms answer "how fast is the system?"; the
slow-query log answers "which specific requests were slow,
and for whom?" — it captures every request whose elapsed
time clears an operator-configured threshold.

Implementation is a bounded in-memory ring buffer
(`SlowQueryBuffer`) attached to the `BoxRegistry` via
`OnceLock`, symmetrical with the tenant-quota and location
proxies. Each record carries timestamp, kind
(`append` / `find` / `aggregate`), box id, tenant id,
session id, latency, and a short detail string (payload
bytes for append, collection + filter excerpt + hit count
for find/aggregate). Writes are lock-taking but the hot
path on every request is the threshold compare: one atomic
load, one branch.

Env configuration:

- `AGENTDB_SLOW_QUERY_THRESHOLD_MS` — default 100; `0`
  records every request (debug mode).
- `AGENTDB_SLOW_QUERY_BUFFER_CAPACITY` — default 1024; when
  full the oldest record is evicted.
- `AGENTDB_SLOW_QUERY_DISABLE` — any non-empty value skips
  the attach at startup entirely. Cheaper than setting a
  very high threshold if operators don't want any
  instrumentation at all.

**Admin surface.**
`GET /api/admin/slow-queries?kind=&tenant=&limit=&after_id=`
returns rows newest-first. Pagination: pass the largest id
from a page as `after_id` to skip it. CLI:
`agentdb show-slow-queries`.

**Observability.**
`agentdb_slow_queries_recorded_total{kind="..."}` on
`/metrics`. Fixed cardinality (three kinds).

Out of scope: persistence across restart (the buffer is in
memory — by design, so operators never see stale records
that predate a config change); per-tenant cardinality on
the counter (would require adding a tenant label, which
operators can already get by scraping the JSON endpoint);
wire-protocol insert (the REST `append_and_fetch` path and
the Mongo `insert` handler both go through
`append_and_fetch_with_actor`, so inserts are already
covered).

### Point-in-time backup

`POST /api/admin/backup` with `{"out_dir": "/abs/path"}`
takes a consistent snapshot of the node's persistent
state while the server is live. No drain, no lock, no
pause. The output directory MUST NOT pre-exist — the
endpoint creates it and writes three things:

- `kv/` — a RocksDB checkpoint produced via the native
  `Checkpoint::create_checkpoint` API. SSTs hard-link
  where possible (same device), so the checkpoint costs
  near-zero IO even on terabyte KVs.
- `meta.sqlite` — the meta store snapshotted via
  `VACUUM INTO`. Contains `schema_migrations` so
  reopening does not re-run any migration.
- `manifest.json` — version, created-at-ms, byte sizes,
  originating `node_id`.

Both storage-layer primitives are additions on the
underlying backends (`KvStore::checkpoint`,
`MetaStore::backup`), each with pre-checks that refuse
to overwrite an existing target so an operator typo
never silently destroys prior data.

**Restore is offline.** Stop the server, copy the
contents of `kv/` into the destination KV directory and
`meta.sqlite` into the destination meta path, then
start the server. The freshly-opened stores rebuild
in-memory indexes lazily on first access — no special
"restore mode" is required. Cross-node restore (pointing
another node at this backup) works as long as the new
node id is distinct from the one in the manifest; the
lease mechanism re-acquires ownership.

**Admin surface.** CLI wrapper:
`agentdb backup --out /abs/path`.

**Scheduled backups.** When `AGENTDB_BACKUP_INTERVAL_SECS`
and `AGENTDB_BACKUP_DIR` are both set, the server starts
a background loop that fires one backup per interval on
the **scheduler leader** (multi-node clusters don't
produce duplicate copies — followers sleep). Each run
writes to `<base>/backup-<ms>/`; after success, a
retention pass keeps the newest
`AGENTDB_BACKUP_RETAIN` dirs (default 7; `0` disables
retention). Failures bump a counter but never remove
prior copies — a failed prune never destroys the last
good backup.

Observability: `GET /api/admin/backup/status` returns
last-success / last-failure info and counters. `/metrics`
emits `agentdb_backup_runs_total`,
`agentdb_backup_failures_total`, and
`agentdb_backup_last_success_ts_seconds`. Only the
process running the scheduler attaches the status
handle; followers return defaults, so a dashboard polling
any node never has to special-case "who has the
scheduler."

Out of scope: object-store segments (cold/archive
blobs) — those are content-addressed and immutable, so
operators back up S3 / GCS separately using the
provider's own tooling; the manifest records which
segments exist via the meta tablets table and the
existing index snapshots, so a backup+restore on a
fresh bucket is recoverable as long as the segments are
restored alongside. Cron-style precision (specific times
of day / weekdays) — operators who need that use systemd
timers or their platform's scheduler to fire
`agentdb backup` directly; the interval loop is the
zero-dependency option.

### TLS termination

**REST.** HTTP by default. Supply `--tls-cert PATH
--tls-key PATH` (or `AGENTDB_TLS_CERT` / `AGENTDB_TLS_KEY`
env vars) to serve HTTPS via rustls + axum-server.

**Mongo wire.** Plaintext by default. Supply
`--mongo-tls-cert PATH --mongo-tls-key PATH` (or
`AGENTDB_MONGO_TLS_CERT` / `AGENTDB_MONGO_TLS_KEY`) to
terminate TLS on the wire listener. The same PEM pair can
be reused across both ports by setting
`AGENTDB_MONGO_TLS_REUSE_REST=1` alongside the REST cert —
saves operators provisioning a second cert for the same
hostname. `agentdb serve --dev-tls` generates one
ephemeral cert and applies it to both listeners.

Under the hood the wire server exposes two entry points
side-by-side: `agentdb_wire::serve` for plaintext and
`agentdb_wire::serve_tls` for TLS. The connection-handling
function is generic over `AsyncRead + AsyncWrite`, so the
framing + dispatch path is identical for both; the TLS
variant just wraps each accepted socket in a
`tokio_rustls::TlsAcceptor` handshake task before handing
off. Handshake failures log at debug (one message per bad
client) so a stream of malicious probes doesn't flood warn.

Both flag pairs and env pairs are mutually exclusive —
setting one without the other is a startup error so
operators don't accidentally boot plaintext under a name
that suggests otherwise. `rcgen`-based dev mode
(`--dev-tls`) generates an ephemeral self-signed cert at
startup covering `localhost`, `127.0.0.1`, `::1`. Clients
must skip cert verification for the dev cert; the cert
lives only in memory and each invocation gets a fresh one.

On the crypto provider — rustls 0.23 requires an explicit
`CryptoProvider::install_default()` before any handshake.
We install `aws_lc_rs` once at TLS entry time. The
provider is process-wide.

**Out of scope:**
- **mTLS.** Client-cert auth isn't wired. Bearer + Basic
  Auth remain the auth layer; TLS adds confidentiality
  on top.
- **Certificate rotation.** Certs are loaded once at
  startup. SIGHUP reload is follow-up work.

### Mongo CRUD semantics (update + delete)

AgentDB is an event log, so Mongo's `update` and `delete`
don't literally mutate or remove rows — doing so would
rewrite history. The Mongo wire handlers translate both
into appends:

- **`update`** appends a **correction event** per matched
  event. The correction carries the merged payload (post-
  `$set` / `$unset` / `$inc`) plus `_update_of: <seq>`,
  pointing at the original's sequence number. Response
  shape: `{ok: 1, n: <matched>, nModified: <corrections
  appended>}`. Drivers that just check `ok` work as
  expected; drivers that branch on `nModified` get a
  truthful count of corrections.
- **`delete`** appends a **tombstone event** per matched
  event. The tombstone's payload is
  `{_tombstone_of: <seq>}`. Response shape: `{ok: 1, n:
  <tombstones appended>}`. The raw log preserves
  everything; the read path folds it out (see below).

#### Materialized read view

By default, `find` on `events` returns a **folded** view:

- Events carrying `_update_of: N` collapse onto event
  `N` (their merged payload replaces the target's).
  Latest correction wins when more than one targets the
  same seq.
- Events referenced by a `_tombstone_of: N` marker
  disappear from the output, along with the tombstone
  itself. Tombstoning a later correction falls back to
  the previous correction (or the original if none
  remain).
- Correction events and tombstones themselves never
  appear in the folded view.

Opt out with `{raw: true}` in the find command body —
this returns the un-folded log, correction markers and
all. Intended for debugging and log-forensic work; most
clients want the folded default.

The write handlers (`update` / `delete`) also filter
against the folded view, so chained mutations compose:
`updateOne({v:0}→{v:1})` followed by `deleteOne({v:1})`
removes the event entirely, as a Mongo user would
expect.

The folding primitive is `agentdb_core::fold_events` —
two-pass, O(n), pure. Unit-tested in isolation from any
transport.

Aggregate (`$search` / `$vectorSearch`) also filters out
correction and tombstone events from the hit set so a
search for "status: shipped" doesn't return the correction
event alongside the original it was updating. This is a
single-pass filter, not a full cross-reference fold — a
hit whose later tombstone doesn't itself match the search
will still appear. Full reconciliation would require
loading every hit's full session log and is deferred.

Both commands require the filter to carry a top-level
`session_id` equality. An unscoped
`updateMany({status: "x"})` would scan every session — a
subtle performance footgun on an event log at any
reasonable scale. Scope enforcement is in the handler; no
way to opt out.

Rejected shapes:

- Replacement documents on `update` (no `$`-operator).
  Only `$set`, `$unset`, `$inc` are honored; a full
  replacement doesn't map onto append semantics.
- `upsert: true`. Inserts go through the `insert` command
  directly.
- `delete.limit` values other than `0` (all) or `1`
  (first). Matches the MongoDB wire protocol's own
  restriction.

Metrics: `agentdb_updates_total` and
`agentdb_deletes_total` counters per box, increment on
every `update` / `delete` command (independent of how
many events it touches).

### SCRAM-SHA-256 auth (M19)

Off by default (backward-compatible). Enable with
`AGENTDB_REQUIRE_AUTH=true`. When on:

- **Users live in the meta store** (`users` table, migration
  003). Credentials are stored as `(iterations, salt_b64,
  stored_key_b64, server_key_b64)` per RFC 5802; the password
  itself never touches disk. `agentdb-meta::scram` is a pure
  crypto module — derive, verify, constant-time compare.

- **Mongo wire.** `hello` advertises `SCRAM-SHA-256` when auth
  is configured. `saslStart` parses the client-first message,
  looks up the user, and returns `server-first` (salt + iter +
  combined nonce). `saslContinue` parses `client-final`,
  verifies the client proof, and returns `server-final`
  (server signature). Failures are uniformly `code: 18
  AuthenticationFailed` — no user-enumeration.

  Per-connection state lives in `WireState::sasl_sessions`
  (`Arc<Mutex<HashMap<conn_id, SaslSession>>>`). Non-auth
  commands check `AuthStatus` on every dispatch; while
  `require_auth == true`, unauthenticated connections get
  `AuthenticationFailed` until `saslStart`/`saslContinue`
  completes. After auth, the command's `$db` must match the
  authenticated user's tenant (except `admin`, always
  allowed) — cross-tenant access returns `code: 13
  Unauthorized`.

- **REST.** A Basic-Auth middleware layers only on
  `/api/tenants/*`. Admin-plane routes (`/api/admin/*`,
  `/health/*`, `/metrics`) stay open — the threat model
  assumes operators run admin behind network isolation.
  PBKDF2 runs per request (~5–10 ms @ 4096 iters); a future
  PR adds a short-TTL cache.

- **Admin endpoints.** `POST /api/admin/users` (create),
  `GET /api/admin/users` (list, summaries only), `DELETE
  /api/admin/users/:id` (idempotent 204).

- **CLI.** `agentdb user add|list|delete`. `add` reads the
  password from `--password` or, if omitted, from stdin.

Out of scope (explicitly): SCRAM-SHA-1 (MongoDB ≥ 4.0
defaults to SHA-256), TLS termination (assumed at the
ingress), Argon2 at-rest hashing.

### Admin-plane bearer auth

Companion to M19 (tenant auth). Closes the last "wide-open
endpoint" gap: `/api/admin/*` now requires a Bearer token
when `AGENTDB_REQUIRE_ADMIN_AUTH=true`.

**Token store.** Migration 004 adds `admin_tokens`
`(token_id, token_hash_b64, description, created_at_ms,
revoked_at_ms)`. `agentdb_meta::admin_tokens` does the
CRUD. Mint generates 32 random bytes, hashes with SHA-256,
persists only the hash. The plaintext is returned ONCE and
never stored — if the operator loses it, they mint a new
one and revoke the old.

Verification is constant-time against the stored hash set
(rows are tiny at prototype scale; bumps to a hash-indexed
lookup later if it ever matters).

**Middleware.** Layered only on the `/api/admin/*`
subtree — `/health/*`, `/metrics`, `/openapi.json`,
`/dashboard`, `/api/stream` always stay open so operators
can probe without credentials. When admin auth is
configured:

- Absent / malformed `Authorization: Bearer <...>` → 401.
- Token not in active set (unknown or revoked) → 401.
- Match → pass through.

**Bootstrap exception.** When no active tokens exist,
`POST /api/admin/tokens` is allowed without auth so an
operator can mint the first one. The moment a token is
active, subsequent mints require auth. Every other admin
endpoint 401s during bootstrap. Rationale: the alternative
(mint via direct SQLite access) is awkward and doesn't
generalize to a server behind a proxy.

**CLI.** `agentdb admin-token add [--description X]`
prints the plaintext to stdout ONCE. All admin CLI
subcommands read `AGENTDB_ADMIN_TOKEN` and attach it as
`Bearer` — one export covers a whole session.

**Operator bootstrap:**

```bash
# Server up with AGENTDB_REQUIRE_ADMIN_AUTH=true and no tokens yet.
TOKEN=$(agentdb admin-token add --description bootstrap | jq -r .token)
export AGENTDB_ADMIN_TOKEN="$TOKEN"
agentdb show-tablets          # now authenticated
```

Out of scope this PR: mTLS / cert-based admin auth,
scoped tokens (everything is full-access today), audit log
of admin actions.

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
agentdb user add <uid> --tenant <tid>  POST /api/admin/users
agentdb user list           GET /api/admin/users
agentdb user delete <uid>   DELETE /api/admin/users/:uid
agentdb show-hot-tablets    GET /api/admin/tablets/hot
agentdb show-tablet-load    GET /api/admin/tablets/load
agentdb shed-tablet <id>    POST /api/admin/tablets/:id/shed
agentdb create-tablet       POST /api/admin/tablets/create
agentdb admin-token add     POST /api/admin/tokens
agentdb admin-token list    GET  /api/admin/tokens
agentdb admin-token revoke <id>  DELETE /api/admin/tokens/:id
agentdb migrate-session <sid> --tenant T --to N
                            POST /api/admin/sessions/:sid/migrate
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
