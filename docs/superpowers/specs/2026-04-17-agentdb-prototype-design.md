# AgentDB Prototype — Design Spec

**Date:** 2026-04-17
**Author:** Li Shen
**Status:** Draft — pending user review
**Source doc:** `docs/agent-db.md`

## 1. Purpose and scope

### 1.1 What we are building

A single-node AgentDB prototype in Rust that exposes a **MongoDB-compatible wire protocol** plus a small native REST API for agent-specific primitives, backed by a pluggable storage layer (RocksDB today, SlateDB later). A Go workload generator simulates a customer-support chatbot platform to drive realistic traffic. A live web dashboard visualises the system while it runs.

Goal is a **showcase**: make the architectural ideas from `docs/agent-db.md` tangible for collaborators, not a production system.

### 1.2 Concepts showcased

All seven differentiating concepts from the source document:

1. **Append-and-fetch-last-N** — atomic write + read-last-N primitive
2. **Session lifecycle** — warm → cold → archive with actual storage tier movement
3. **Hybrid retrieval** — vector + structured filter + full-text in one query plan
4. **Context fork** — copy-on-write subagent spawn
5. **Document model** — nested field indexing, schema-free
6. **Box architecture** — many logical tenants on shared physical resources with isolation
7. **Tunable transactions** — default single-doc atomic, opt-in multi-doc batched

### 1.3 Success criteria

A ten-minute demo that delivers all of:

- Start `agentdb-server`, `agentdb-workload`, and open the dashboard
- Watch ~1,000 real tenants across ~10 logical boxes, with synthetic metadata for ~100,000 additional cold tenants visualised in the box grid
- See sessions transition Warm → Cold → Archive live on the dashboard
- Run a hybrid retrieval (`$vectorSearch` + filter) in a single Mongo aggregation from `mongosh`; query plan appears in the trace feed
- Fork a session; branch graph updates; metrics show bytes-unique-to-child growing while bytes-inherited stays constant
- Inject a fault into one box; dashboard shows that box turning red while siblings continue unaffected
- Connect with `mongosh` live and poke around a real session

### 1.4 Explicitly out of scope

- Multi-process / real fault isolation across OS boundaries
- Production S3 (pluggable via `object_store`, but local FS is the default)
- Authentication, TLS, access control
- Durability guarantees beyond RocksDB defaults
- Real LLM calls (workload is scripted state machines)
- Full MongoDB wire protocol compatibility — targeted subset only
- Performance benchmarking (separate effort post-prototype)
- Chaos / crash testing

## 2. Architecture overview

```
┌─────────────────────────────────────────────────────────────┐
│  agentdb-workload (Go)                                      │
│  Scripted chatbot tenants → Mongo driver + REST client      │
└──────────────────┬──────────────────────────────────────────┘
                   │ MongoDB wire + HTTP
                   ▼
┌─────────────────────────────────────────────────────────────┐
│  agentdb-server (Rust, single process)                      │
│                                                             │
│  ┌──────────────┐    ┌───────────────────────────────────┐  │
│  │ agentdb-wire │    │ agentdb-api                       │  │
│  │ Mongo OP_MSG │    │ Native REST (fork, append_fetch,  │  │
│  │ subset       │    │ admin, SSE) + static dashboard    │  │
│  └──────┬───────┘    └────────────────┬──────────────────┘  │
│         │                             │                     │
│         ▼                             ▼                     │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ agentdb-core                                          │  │
│  │ Document model • Session/Branch • Lifecycle engine    │  │
│  │ Box registry • Tenant routing • Transactions          │  │
│  └──────┬──────────────────────────────────┬─────────────┘  │
│         │                                  │                │
│         ▼                                  ▼                │
│  ┌─────────────────────┐      ┌────────────────────────┐    │
│  │ agentdb-index       │      │ agentdb-storage        │    │
│  │ TextIndex (BM25)    │      │ KvStore trait          │    │
│  │ VectorIndex (brute) │      │ → RocksKvStore (now)   │    │
│  │ SecondaryIndex      │      │ → SlateKvStore (later) │    │
│  │ HybridPlanner       │      │ object_store tiering   │    │
│  └─────────────────────┘      └───────────┬────────────┘    │
└────────────────────────────────────────────┼────────────────┘
                                             ▼
                        ┌────────────────────────────────────┐
                        │  RocksDB (local) + object_store    │
                        │  ./data/rocksdb, ./data/objects/   │
                        └────────────────────────────────────┘
```

## 3. Repository layout

```
agentdb/
├── crates/
│   ├── agentdb-core/          # doc model, session/branch types, lifecycle
│   ├── agentdb-storage/       # KvStore trait, RocksKvStore, object_store tiering
│   ├── agentdb-index/         # secondary, FTS (BM25 inverted), vector (cosine), hybrid planner
│   ├── agentdb-wire/          # MongoDB wire protocol subset
│   ├── agentdb-api/           # axum REST + SSE + static dashboard
│   └── agentdb-dashboard/     # static HTML/CSS/JS (no build step)
├── bin/
│   └── agentdb-server/        # main binary
├── workload/                  # Go module, separate toolchain
│   ├── go.mod
│   ├── cmd/agentdb-workload/
│   └── internal/persona/
├── tests/integration/         # end-to-end scenarios
├── scripts/smoke.sh           # CI smoke test
└── docs/
    ├── agent-db.md
    └── superpowers/specs/
```

## 4. Data model

### 4.1 Core entities

| Entity | Purpose | Storage |
|---|---|---|
| **Document** | JSON/BSON, unbounded nesting, schema-free | keyed in `cf_state` or `cf_events` |
| **Collection** | Named bag of documents (Mongo-familiar) | logical; collection name part of key prefix |
| **Session** | First-class object with lifecycle metadata | `cf_meta` |
| **Event** | Append-only record inside a session | `cf_events` |
| **State doc** | Per-session mutable document | `cf_state` |
| **Tenant** | Logical owner; maps to exactly one box | `cf_meta` |
| **Box** | Logical partition of ~100-1,000 tenants | in-process runtime object |
| **Branch** | COW lineage for sessions | `cf_meta` |

### 4.2 Key design

All data lives in one logical keyspace, partitioned by a 1-byte namespace tag:

```
key = ns_tag(1B) | box_id(2B) | tenant_id(8B) | session_id(8B) | branch_id(4B) | seq(8B)

ns_tag: 0x01 = events  0x02 = state  0x03 = secondary  0x04 = meta
```

Lexicographic ordering guarantees locality:
- Events of a session are contiguous
- Sessions of a tenant are contiguous
- Tenants of a box are contiguous

Consequences:
- `session_load` = single prefix scan (one seek + sequential iteration)
- `append` = single `put`
- `tenant_scan` / `box_scan` = prefix scans
- No cross-shard coordination for any common operation

Namespace tags are folded into the key rather than relying on RocksDB column families so the abstraction works identically against SlateDB (which uses prefix-based namespacing).

### 4.3 External vs internal IDs

External callers (Mongo wire, REST, workload) pass **opaque string IDs** for tenants and sessions (e.g., `"t-42"`, `"s-9"`). The server maintains a bidirectional mapping in `cf_meta`:

- `external_string → internal_u64` (forward, for writes/queries)
- `internal_u64 → external_string` (reverse, for responses)

Internal u64s are used in keys (8 bytes each) for compactness and fast comparison. New IDs are minted on first reference via a per-box monotonic counter. BoxId is also internal u16, assigned at box creation.

### 4.4 DocId

`DocId` used in the index layer is the 8-byte internal `seq` scoped within a box. Given a `DocId` and a box, the key-prefix scan resolves the full `(tenant_id, session_id, branch_id)` from `cf_meta` if needed. For the common query path, the planner works directly with `(BoxId, DocId)` pairs and only materialises full documents at projection time.

## 5. Storage layer

### 5.1 Pluggable `KvStore` trait

```rust
pub trait KvStore: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    fn delete(&self, key: &[u8]) -> Result<()>;
    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter>;
    fn write_batch(&self, ops: Vec<WriteOp>) -> Result<()>;
    fn snapshot(&self) -> Result<Box<dyn KvSnapshot>>;
}
```

### 5.2 Implementations

| Impl | Status | Notes |
|---|---|---|
| `RocksKvStore` | Prototype default | Uses `rust-rocksdb`. Fast local warm tier. |
| `SlateKvStore` | Post-prototype stretch | Uses [slatedb](https://github.com/slatedb/slatedb). Proves "LSM on object storage" thesis from source doc §3.1. |

### 5.3 Tiering

Three tiers, all exposed through the same query path:

**Warm.** All live reads/writes land in the KV store (RocksDB default). Per-session LRU informs tier decisions.

**Cold.** Session migrates to `object_store`. Lifecycle engine reads all `cf_events`, `cf_state`, `cf_secondary` entries for `(box, tenant, session)`, writes a single BSON segment to `object_store` at `boxes/{box_id}/cold/{tenant_id}/{session_id}/segment-{epoch}.bson`, writes segment footer to `cf_meta`, deletes warm entries. Reactivation reverses this.

**Archive.** From cold, the raw segment moves to a cheaper prefix `boxes/{box}/archive/...`, and a derived-data sidecar is generated and stored:
```
{
  summary: "...",           // joined-event summary (demo: deterministic fake)
  embedding: [f32; 384],    // deterministic hash-based embedding for demo
  extracted_entities: [...]
}
```
The sidecar stays in `cf_meta` and in the per-box FTS/vector indexes so hybrid queries continue to hit archived sessions. Raw segment bytes become "ghost data" — fetchable via admin endpoint but not indexed.

### 5.4 `object_store` integration

The `object_store` crate abstracts local FS / S3 / MinIO / Azure / GCS. Default demo config: local `./data/object_store/`. Swap to MinIO or S3 via config file, no code change.

### 5.5 Derived-data generation

For the prototype, summary and embedding generation are **deterministic fake** functions:
- Summary: concatenate first N event payloads, truncate
- Embedding: hash event payloads into a fixed-dim `[f32; 384]` via a seeded RNG

The lifecycle engine calls a `DerivedGenerator` trait, which in production could be an LLM call. Keeping the trait makes the architectural boundary from source doc §2.5 explicit (DB schedules and stores, application computes).

## 6. Indexing

### 6.1 Traits

```rust
pub trait TextIndex {
    fn insert(&mut self, doc_id: DocId, text: &str);
    fn search(&self, q: &str, k: usize, filter: Option<DocIdFilter>) -> Vec<(DocId, f32)>;
}

pub trait VectorIndex {
    fn insert(&mut self, doc_id: DocId, vec: &[f32]);
    fn knn(&self, q: &[f32], k: usize, filter: Option<DocIdFilter>) -> Vec<(DocId, f32)>;
}
```

### 6.2 Prototype implementations

- **`InMemoryInverted`** — `HashMap<Token, Vec<(DocId, tf)>>` with BM25 scoring. ~250 LOC. Per-box.
- **`InMemoryFlatVec`** — `Vec<(DocId, [f32; DIM])>` with brute-force cosine top-K. ~100 LOC. Per-box.
- **Secondary index** — `cf_secondary` entries keyed by `(indexed_path, value, doc_id)`, maintained synchronously on write.

**Index population by tier.** Warm entries are added synchronously on write. Cold sessions keep their FTS/vector entries in the in-memory indexes — the segment file in object storage is the raw data, but the derived index entries remain memory-resident so queries never need to touch object storage to decide relevance. Archive sessions are represented only by their derived-data sidecar (summary text + single pre-computed embedding), each contributing one entry to FTS and vector indexes. On server startup, the indexes are rebuilt by iterating `cf_meta` per box: warm sessions scan `cf_events`, cold sessions read index-only metadata from `cf_meta`, archive sessions load their sidecar. At prototype volumes (tens of thousands of docs per box) the rebuild takes ≤2 seconds.

Both indexes scale fine at prototype volumes. Traits enable swapping in tantivy / HNSW without touching consumers.

### 6.3 Hybrid planner

Three strategies, chosen at plan time by filter shape inspection:

1. **Locality-bound pre-filter.** Filter includes `tenant_id=` or `session_id=`. Fetch doc_ids via prefix scan, score inline. The common agent case and the architectural headline.
2. **Selective pre-filter.** Filter hits a secondary index with low cardinality. Resolve filter first, then score and rank.
3. **Over-fetch post-filter.** Broad or no filter. Ask index for `k * over_fetch` candidates, apply filter in memory, truncate.

The chosen strategy and the latency it produced are emitted as a `QueryPlan` event on the SSE stream so the dashboard makes the decision visible.

## 7. Query surface

### 7.1 MongoDB wire protocol (primary)

Implemented subset — enough for `mongosh`, `pymongo`, and the Go `mongo-go-driver`:

| Command | Behaviour |
|---|---|
| `hello` / `isMaster` | Stub single-node topology; no auth challenge |
| `find` + `getMore` | Filter (incl. nested paths), projection, limit, skip, sort; cursors |
| `insert` / `update` / `delete` | Single- and multi-doc. Operators: `$set`, `$unset`, `$inc`, `$push` |
| `aggregate` | Stages: `$match`, `$project`, `$limit`, `$skip`, `$sort`, `$vectorSearch`, `$search` |
| `startTransaction` / `commitTransaction` / `abortTransaction` | Single-shard batched write |
| `listDatabases` / `listCollections` | For browsing |
| `ping` | Health |

Explicitly **not** supported: auth flows, replica-set / sharding metadata, change streams, `$where` / server-side JS, most aggregation stages. Unsupported commands return a typed error that drivers surface cleanly.

### 7.2 Custom aggregation stages

Mirror the shape of Atlas's Search stages so examples look familiar:

```js
{ $vectorSearch: {
    queryVector: <float[]>, path: "embedding",
    numCandidates: 100, limit: 10,
    filter: { tenant_id: "t-42", "payload.kind": "message" }
}}

{ $search: {
    text: { query: "refund policy", path: "payload.text" },
    filter: { session_id: "s-9", ts: { $gte: ISODate("...") } }
}}
```

### 7.3 Native REST (agent-native primitives)

```
POST /api/sessions/{session_id}/append_and_fetch
     body: { event: Document, fetch_last_n: usize }
     resp: { seq: u64, recent: [Event; N] }

POST /api/sessions/{session_id}/fork
     body: { from_branch_id?: u32 }
     resp: { new_session_id, branch_id, parent_branch_id }

POST /api/admin/tier/force
     body: { session_id, target: "Warm" | "Cold" | "Archive" }
     resp: { old_tier, new_tier, bytes_moved, duration_ms }

POST /api/admin/boxes/{box_id}/quota
     body: { max_warm_sessions?, max_qps? }

POST /api/admin/boxes/{box_id}/inject_fault
     body: { kind: "latency" | "error", duration_s }

GET  /api/admin/boxes
GET  /api/admin/boxes/{box_id}
GET  /api/admin/sessions/{session_id}

GET  /api/stream                 # SSE stream of live events
GET  /dashboard                  # static HTML entry point
```

These endpoints exist as REST because their return semantics don't map cleanly to Mongo commands (atomic write+read return values, fork metadata, operator surface).

## 8. Transactions

Three layers, matching source doc §2.3:

1. **Default: single-doc atomic.** Every `insert`/`update`/`delete` is one `KvStore::put` or one `write_batch`. No coordination cost.
2. **Opt-in: multi-doc batched via Mongo wire.** `startTransaction` / `commitTransaction` map to an in-process accumulator; commit is one `write_batch`. Single-shard by construction. Abort drops the accumulator.
3. **Snapshot read.** `KvStore::snapshot()` provides a point-in-time view. Used for `readConcern: "snapshot"`, replay, and fork.

Not supported: serializable isolation, cross-box transactions, server-side read-modify-write. Clear error returned if attempted.

## 9. Fork (copy-on-write)

### 9.1 Data model

```rust
struct Branch {
    id: u32,
    parent_id: Option<u32>,
    session_id: SessionId,
    created_at_seq: u64,   // parent's seq at fork point
}
```

Stored in `cf_meta`. Fork is a metadata-only operation: one write.

### 9.2 Writes

Go only to the child `branch_id`. No reference to parent data.

### 9.3 Reads

Walk the branch chain leaf → root. For each branch, scan keys with that `branch_id`. Merge results by `seq`:
- Child entries take precedence per seq
- Parent entries visible only up to `created_at_seq` as seen by this child
- Leafmost branch wins on dedup

### 9.4 Cost

- Fork itself: one metadata write, zero data copy
- Divergence: proportional to child writes
- Read merge: O(chain_depth × N) prefix scans, bounded and cheap at realistic chain depths (1-3)

Composes with the hybrid planner's locality-bound strategy — fork + hybrid retrieval Just Works.

## 10. Box architecture

### 10.1 Runtime object

```rust
struct Box {
    id: BoxId,                       // u16
    kv: Arc<dyn KvStore>,            // shared physical store; isolation via key prefix
    indexes: Arc<PerBoxIndexes>,     // in-memory FTS + vector, per-box only
    lifecycle: LifecycleEngine,      // own tokio task, 1s tick
    quota: Quota,
    metrics: BoxMetrics,
    fault: Option<InjectedFault>,    // for demo fault-injection
}
```

### 10.2 Routing

Each request carries `tenant_id`. Router consults an in-memory `tenant_id → box_id` cache hydrated lazily from `cf_meta`. Requests for a box are serialised through a bounded-concurrency tokio task pool owned by the box, so one box's faults can't starve another's queue.

### 10.3 Quota

Per-box counters: `warm_sessions`, `qps` (rolling window), `bytes_written_per_min`. Soft cap → lifecycle engine aggressively demotes idle sessions. Hard cap → new writes return `QuotaExceeded`; reads still serve.

### 10.4 Fault injection

`POST /api/admin/boxes/{id}/inject_fault` makes that box's handlers return errors or sleep for N seconds. The dashboard shows only that box turning red. Single-process so this is cooperative isolation, not OS-level — explicitly labelled as such in the UI tooltip.

### 10.5 Synthetic cold tenants (stretch mode)

Config flag `synthetic_cold_tenants_per_box: u32`. If set, the box reports N extra tenants in metrics with `tier=Cold`, no actual data. They appear as grey dots in the dashboard grid, demonstrating the long-tail visual. Labelled "synthetic" in the UI so the claim stays honest.

## 11. Lifecycle engine

Per-box tokio task on a 1-second tick. State machine:

```
           append / read
Warm ─────────────────────► Warm           (touches last_active_at)
  │
  │ idle > T_cold (e.g., 60s in demo, tunable)
  ▼
Cold ──────── read ──────► Warm            (reactivation)
  │
  │ idle > T_archive (e.g., 5m in demo)
  ▼
Archive ──── read of raw ─► Warm           (slow path, admin endpoint)
             └── hybrid query hits derived sidecar — stays Archive
```

Each tick: scan `cf_meta` for sessions past a threshold, schedule a tier move. Moves serialised per session, parallel across sessions, bounded by a per-box concurrency cap to protect object_store bandwidth during cold-start storms (source doc §4.8).

Demo thresholds (`T_cold`, `T_archive`) are small so transitions happen on human timescales during the demo.

## 12. Workload generator (Go)

### 12.1 Why Go

- Goroutine-per-tenant is the natural concurrency model for a chatbot simulator
- Scales effortlessly to 10k+ concurrent tenants, letting the stretch-mode long-tail story land
- Official `mongo-go-driver` is mature; `net/http` + `encoding/json` handle REST
- Single static binary for distribution
- Workload is pure client code with no shared types with the DB, so toolchain separation is clean

### 12.2 Persona config

```go
type TenantPersona struct {
    TenantID               string
    TicketRatePerMin       float64
    ConcurrentSessionsCap  int
    MessageIntervalMs      [2]int  // min, max
    ToolCallProb           float64
    MemoryRecallProb       float64
    EscalationProb         float64
    SessionLen             [2]int
    BurstSchedule          []BurstWindow
}
```

### 12.3 Session state machine

```
NEW → WAITING_USER → AGENT_THINKING → {TOOL_CALL | MEMORY_RECALL} → AGENT_REPLY
                                         ↓
                                    ESCALATED (fork)
                                         ↓
                                    RESOLVED → idle → cold → archive
```

Each transition emits one event via `append_and_fetch`. `MEMORY_RECALL` runs a hybrid query against past tickets for that tenant. `ESCALATED` calls `/api/sessions/{id}/fork` and continues under the child branch.

### 12.4 Demo control endpoints

The workload binary listens on port 9090 for stage directions:
```
POST /demo/burst?tenant=T       # trigger a load spike
POST /demo/fork?session=S       # force a fork
POST /demo/cold?tenant=T        # idle into cold tier
POST /demo/hybrid?tenant=T      # fire a visible hybrid retrieval
```

The dashboard includes buttons that hit these endpoints so a presenter can narrate and trigger the described behaviour in one motion.

### 12.5 Config

YAML file: personas list, global rates, concurrency caps, pass-through of `synthetic_cold_tenants_per_box` for the server.

## 13. Dashboard

### 13.1 Delivery

Served by `agentdb-api` at `/dashboard`. Static HTML/CSS/JS with [Alpine.js](https://alpinejs.dev/) via CDN. No build step. ~500 LOC total including CSS. Live data via `/api/stream` SSE.

### 13.2 Layout

1. **Top bar.** Active boxes, total tenants (real + synthetic in muted colour), aggregate QPS, storage-by-tier stacked bar, p50/p99 query latency.
2. **Box grid (middle-left).** Tile per box sized by recent activity; tenants as coloured dots (green=warm, blue=cold, grey=archive; red overlay for injected fault). Click to drill in.
3. **Session detail (middle-right).** Branch graph, event count, last activity, current tier, bytes-by-tier, bytes-unique-to-this-branch for forks.
4. **Event feed (bottom).** Scrolling log of last-N-seconds events: tier transitions, forks, quota hits, query plans with strategy/latency, faults. Filterable by kind.

### 13.3 Demo buttons

Shortcut buttons wired to the workload's `/demo/*` endpoints: trigger burst, force fork, idle tenant, fire hybrid query. Lets a presenter narrate and make the corresponding dashboard reaction happen in one motion.

## 14. Observability

### 14.1 Metrics (`metrics` crate)

Per box and global:
- **Counters** — `inserts`, `updates`, `deletes`, `queries_by_type`, `forks`, `tier_transitions`, `quota_hits`
- **Histograms** — `query_latency` (labelled by strategy), `session_load_latency`, `tier_transition_duration`, `append_latency`
- **Gauges** — `sessions_by_tier`, `bytes_by_tier`, `queue_depth`

### 14.2 Tracing (`tracing` crate)

Key structured events:
- `agentdb.query.plan`
- `agentdb.tier.change`
- `agentdb.fork.created`
- `agentdb.quota.hit`
- `agentdb.fault.injected`

A `tracing_subscriber::Layer` forwards tagged events onto the SSE stream so structured logs and dashboard events share one source.

### 14.3 SSE schema

```rust
enum StreamEvent {
    MetricsTick(MetricsSnapshot),
    BoxMetricsTick(BoxId, BoxMetricsSnapshot),
    TierChange { session_id, from, to, bytes, duration_ms },
    QueryPlan { box_id, strategy, latency_ms, k, n_returned, filter_shape },
    ForkCreated { session_id, branch_id, parent_branch_id },
    QuotaHit { box_id, kind },
    FaultInjected { box_id, kind, duration_s },
}
```

## 15. Testing strategy

### 15.1 Layers

1. **Unit tests per crate.** Key encoding / decoding, branch-chain read merge, BM25 scoring, cosine top-K, quota logic, filter selectivity estimator.
2. **Integration tests (`tests/integration/`).** Full in-process server, end-to-end scenarios:
   - `test_session_load_locality` — 1,000 events single prefix scan
   - `test_tier_lifecycle` — warm → cold → reactivate → archive; hybrid query hits derived sidecar
   - `test_fork_cow` — metadata-only cost, divergence proportional to child writes
   - `test_hybrid_planner_locality_bound` — session-filter chooses locality-bound strategy
   - `test_box_fault_isolation` — fault on A, traffic on B unaffected
   - `test_mongo_wire_with_driver` — real `mongodb` Rust driver, `insert`/`find`/`aggregate` incl. `$vectorSearch`
3. **Smoke demo (`scripts/smoke.sh`).** Start server + small workload, 10-second run, a handful of queries via `mongosh`, SSE stream alive. CI-friendly.

### 15.2 Out of scope

Chaos, durability, benchmarking, real S3 (integration tests use `object_store` local FS backend).

## 16. Milestones

Estimated 12-14 working days. Each milestone produces visibly working functionality.

| # | Days | Deliverable |
|---|---|---|
| **M1** | 1-2 | Workspace scaffold; `KvStore` trait + `RocksKvStore`; key encoder; unit tests |
| **M2** | 3 | Core types (Document, Session, Branch, Box); tenant→box routing; skeletal REST + `append_and_fetch` |
| **M3** | 4-5 | `agentdb-wire`: Mongo subset (`hello`, `find`+`getMore`, insert/update/delete, listDbs/Collections); `mongosh` works |
| **M4** | 6-7 | `TextIndex` / `VectorIndex` traits + in-memory impls; secondary index; hybrid planner (3 strategies); `$vectorSearch` / `$search` aggregation stages |
| **M5** | 8-9 | Lifecycle engine; `object_store`-backed cold/archive tiers; derived-data generation; tier-force admin |
| **M6** | 10-11 | Fork (branch chain, metadata-only CoW); Mongo transaction commands |
| **M7** | 12-14 | Workload generator in Go with demo CLI + control endpoints; SSE stream; dashboard; fault injection; smoke script |

### 16.1 Stretch (post-M7)

- `SlateKvStore` implementation — proves "LSM on object storage" thesis
- MinIO deployment script
- Real BGE-small embeddings via `candle` crate
- Benchmarks and scale tests

## 17. Open questions to revisit during implementation

- **Branch-chain read cost at high depth.** If a session is forked repeatedly (parent → child → grandchild), read merging grows linearly. At what depth does this start to hurt? Expect fine at ≤5; plan to instrument.
- **Cold-tier segment format.** BSON concatenation is simple but lacks random access within a segment. If reactivation latency matters, consider a simple index footer. Defer until measured.
- **Synthetic cold tenants rendering.** Showing 100k dots in the dashboard grid is a non-trivial render. Likely need aggregation (group tenants into blocks at high zoom out, expand on click).
- **Mongo wire protocol edge cases.** Some drivers require `buildInfo`, `getLog`, or specific error codes. Fix as they show up during M3.

## 18. References

- Source design: `docs/agent-db.md`
- [SlateDB](https://github.com/slatedb/slatedb) — target for pluggable storage later
- [object_store](https://docs.rs/object_store/) — cold/archive tier abstraction
- [Alpine.js](https://alpinejs.dev/) — dashboard reactivity
- Atlas `$vectorSearch` / `$search` — aggregation stage shape we mimic
