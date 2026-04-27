# AgentDB Design

Li Shen · [shenli3514@gmail.com](mailto:shenli3514@gmail.com)

## Preface

This is the canonical design document for AgentDB — the
current architectural commitment, the model the codebase is
built around, and the source of truth when implementation and
intent disagree.

**Three docs, three jobs.** Don't mix them up:

- [`docs/agent-db.md`](agent-db.md) — **WHY**. Motivation,
  workload requirements, design principles. Stable; doesn't
  evolve as the implementation evolves.
- [`docs/DESIGN.md`](DESIGN.md) — **WHAT** (this file).
  Current architectural commitments: identity model, physical
  layering, clock, failure model, performance contract,
  real-time, API surface, deployment, operability. Evolves
  with the design.
- [`docs/ARCHITECTURE.md`](ARCHITECTURE.md) — **HOW**, today.
  Current implementation state, milestone-by-milestone changes,
  crate layout, request flow. Evolves with the codebase.

For the historical record of the architecture reviews and
decision notes that produced this design, see:

- `docs/superpowers/notes/2026-04-20-tablet-unit-review-response.md`
- `docs/superpowers/notes/2026-04-21-architecture-review-decision-matrix.md`
- `docs/superpowers/notes/2026-04-21-hlc-clock-decision.md`
- `docs/superpowers/specs/2026-04-25-pubsub-design.md` (durable session-event subscribe)
- `docs/superpowers/specs/2026-04-26-pubsub-phase2-design.md` (ephemeral broadcast channels)

---

## 1. Identity model

### 1.1 Tenant — logical, implicit, auth-bound

A tenant is a logical boundary for quota, isolation, and billing. It is **not** present in request payloads. Tenant identity is resolved at connection accept time:

- **Mongo wire:** SCRAM-authenticated user maps 1:1 to a tenant. The Mongo `$db` field is a sub-namespace *inside* the tenant, not the tenant selector itself.
- **Native REST:** bearer token in `Authorization` maps to a tenant. No tenant parameter in request bodies.

Internal code paths still thread `tenant_id` for quota attribution, isolation checks, and billing accounting — but it never appears on the client-visible surface.

### 1.2 Session — the core data unit

A session is the unit of append, fork, locality, lifecycle, and retrieval. Session IDs are **globally unique** (UUID v7 or KSUID-shaped). Two consequences:

- A session identifier is self-contained — no `(tenant_id, session_id)` composite is needed for identity or routing.
- Migration between tenants (rare) is a metadata operation, not an ID rewrite.

A session is bound to exactly one **shard** for its entire lifetime, where `shard = hash(session_id) % S` and `S` is a fixed constant (v1 default: 16384, following Redis Cluster precedent). The shard's current owning **tablet** is resolved via an assignment table. Details of the shard/tablet/node layers are in §1.4–§1.6.

### 1.3 Agent — metadata dimension, not resource

Agent attribution is a schema feature, not a first-class API resource. Specifically:

- **Events carry `actor_id`.** The writing agent's identifier. Free-form string; no pre-registration.
- **Sessions carry `primary_agent_id`.** The owning agent for single-agent sessions; may be absent for multi-agent sessions.
- **Secondary indexes on `agent_id`** enable the cross-session "all events for agent X" query (agent-db.md §2.5).

Agent is **not**:

- A registration resource with its own endpoints.
- A quota boundary (tenants are; agents are not).
- A tablet-selection input in v1.

This preserves agent-db.md §3.9's scope discipline (which has now moved into this doc as §9 explicit non-goals) while making agent attribution cheap and queryable. Promoting agent to a full resource is a v2+ product decision that awaits clear multi-agent-framework direction.

### 1.4 Shard — the fixed logical partition

A **shard** is a hash bucket. The universe of shards has a fixed cardinality `S` (v1: 16384) and never changes for the life of a cluster. Each session maps to exactly one shard via `shard = hash(session_id) % S`, and that mapping is immutable — adding or removing physical capacity never re-hashes sessions.

Fixed `S` is what avoids the classic `hash % N` failure where adding one node reshuffles ~99% of the keyspace. Since `S` is constant, only the shard-to-tablet assignment changes when the cluster resizes, not the session-to-shard mapping.

Changing `S` in the future is possible but expensive (a true reshuffle). `S = 16384` was chosen to cover clusters up to ~2000 nodes without ever needing to grow; `S = 65536` supports ~10000. Picking `S` once and never touching it is the explicit v1 stance.

Hash function: XXHash3 or FNV-1a over the full `session_id` bytes, with a fixed seed written into cluster config (the seed must not vary across processes, or the same `session_id` would route to different shards — a Rust `SipHash` default would do exactly this and must be avoided).

### 1.5 Tablet — the runtime unit

A **tablet** is the runtime vehicle that owns a set of shards. Its responsibilities:

- A KV-key prefix for its sessions and events.
- Tablet-local indexes (text, vector).
- An admission-control semaphore.
- A lifecycle engine for warm→cold→archive transitions of sessions in the tablet.
- A session-lock pool keyed by session id (per-session mutexes).
- Its own metadata (session registry, segment catalog, index pointers, HLC checkpoint) — persisted as AgentDB's own data (§2.3).

**Shard-to-tablet is an arbitrary mapping, not a contiguous range.** A tablet may own any subset of the `S` shards. Typical deployments keep the subset contiguous for administrative simplicity (easier to visualize), but the system does not require it — and non-contiguous ownership arises naturally after shard-level migration, e.g., relocating scattered hot shards off a busy node.

**Three scheduling granularities flow from this design:**

| Granularity | Mechanism | Typical cost | Use case |
|---|---|---|---|
| **Tablet** | Update `tablet_assignment` — one row | Migrate an entire tablet's warm state | Node load-balancing, drain, failover |
| **Shard** | Update `shard_assignment` — N rows for N shards | Migrate the sessions in those shards | Disperse scattered hotspots |
| **Session** | Add a row to the override table | Migrate one session's data | Truly pathological single-session hotspot (rare) |

The common case is tablet-level. Shard-level handles the hot-session-scattered-across-a-tablet scenario. Session-level is an escape hatch for outliers.

**Tablet split** is a shard operation: reassign a subset of a tablet's shards to a new tablet. No data movement inside either tablet's retained sessions — only the migrating shards copy across.

**Tenant → tablets is one-to-many via shard fanout.** A tenant's sessions distribute across many shards (by hash), and those shards are distributed across many tablets. A hot tenant therefore spreads across hardware automatically — the single-tenant-hotspot failure mode of the original `box = hash(tenant)` model is structurally eliminated.

### 1.6 Node — the physical host

A **node** is the process / container / VM that hosts one or more tablets. Nodes are:

- The unit of failure (process crash, machine loss).
- The unit of deployment (rolling upgrades, capacity changes).
- The unit of observability (a node reports its tablets' metrics).

Nodes do not own data directly — data lives in object storage and in the tablets' warm caches. A node dying means its tablets need to be failed-over to other nodes; the data is intact because the durable record is object storage (§4.1).

**Tablet-to-node assignment** is recorded in `tablet_assignment` (Postgres, §2.3). This is Layer 2 of the routing; Layer 1 is `shard_assignment` (shard → tablet). Nodes can host different numbers of tablets to support heterogeneous hardware (a bigger machine hosts more tablets — the weighting is explicit, not implicit in a hash function).

```
       hash(session_id) % S
  session ──────────────────▶ shard  (fixed, S = 16384)
                                │
                     shard_assignment[shard]
                                │
                                ▼
                           tablet  (runtime unit; owns an arbitrary subset of shards)
                                │
                   tablet_assignment[tablet]
                                │
                                ▼
                             node    (physical host; hosts ≥1 tablet)
```

Both assignment tables live in the global control plane (§2.3). They are read-mostly, written only by the scheduler. Clients and tablet runtimes cache them locally; the control plane being unavailable for minutes does not stop ongoing traffic (§4.4).

### 1.7 Failure domains — cross-AZ scheduling without Paxos

Spanner replicates tablets across zones via Paxos for synchronous cross-zone redundancy. AgentDB explicitly does not pay that cost (§4, §9). But we get most of the benefit for free, because **object storage is already replicated across AZs** (S3 and compatible services all do this by default). The only component that is single-AZ at any moment is the **runtime writer** — the tablet's lock pool, warm KV cache, and in-flight admission queue.

This turns cross-AZ protection into a **scheduler policy question, not a replication-protocol question**:

- Each node declares its AZ/zone in its Postgres membership row.
- The scheduler's failover logic has a preference: when a tablet needs a new owner, prefer candidates in an AZ different from the failed owner's AZ.
- Steady-state placement: across the cluster, no single AZ holds more than `ceil(tablets / AZs)` tablets, within bounds.

Consequences:

- **Single-AZ outage blast-radius** drops from "every tablet in that AZ loses its writer until AZ recovers" to "every tablet in that AZ loses its writer for the failover window (target p95 < 30s, §4.2)."
- **No Paxos, no quorum writes, no TrueTime**: the cost is a few lines of scheduler policy.
- Data durability is handled entirely by object storage, which is already multi-AZ at the storage layer.

Multi-region is still a non-goal (§9). AZ-awareness is the minimum failure-domain discipline; regions require very different write-path semantics and are deliberately out of scope for v1 and v2.

---

## 2. Physical architecture

### 2.1 Layered storage

- **Object storage** is the durable, primary store for segment files, persistent index snapshots, and tablet metadata snapshots (§2.3).
- **Local KV (RocksDB today; pluggable `KvStore` trait)** caches warm data, the WAL, and tablet metadata.
- **HA Postgres** holds the small global control plane: tenant registry, shard assignment, tablet assignment, cluster config (§2.3).
- **Redis is optional.** Per-node token buckets + in-process caches suffice for most deployments. Redis is a scale-out lever, not a required dependency.

The pluggable `KvStore` (`RocksKvStore` for local, `SlateKvStore` for object-store-backed) survives unchanged.

### 2.2 Key layout

Keys are tablet-prefixed; tenant is inside records, not in the key path:

```
session/{tablet_id}/{session_id}                     -> Session
event/{tablet_id}/{session_id}/{branch}/{seq}        -> Event
segment/{tablet_id}/{session_id}/{branch}            -> SegmentFile
session_index/{session_id}                              -> tablet_id   (lookup for operations with only session_id)
```

The `session_index` entry is written at session creation and read by any operation that arrives with a session id but no tablet context. The hot path (append) routes via `hash(session_id)` and skips the lookup.

Tenant appears as a field in `Session` and `Event` records for audit and quota. It is never load-bearing for routing.

### 2.3 Metadata storage — two layers

Metadata splits into two layers with different durability, latency, and scaling profiles. Treating them uniformly (one Postgres or one Raft group for everything) is the PD-class mistake we explicitly avoid (§11).

**Layer 1 — Global control plane.** HA Postgres (primary + standby, or any managed Postgres: RDS / Aurora / Neon / CloudSQL). Holds the small, globally-consistent state:

- **Tenant registry** — auth identity, quota, billing attribution. ~1 GB at Type III scale.
- **Shard assignment** — 16384-entry array: which tablet owns each shard. Rewritten when tablets split / migrate / rebalance.
- **Tablet assignment** — which node owns each tablet. Rewritten on failover, drain, rebalance.
- **Node membership** — one row per node, `last_heartbeat` updated every 10s. Scheduler scans for stale rows to trigger failover.
- **Cluster config** — `S`, hash seed, tick intervals, max-offset, quota defaults.

**Total size: a few GB at Type III scale.** A single Postgres instance handles this forever. Change propagation uses `LISTEN / NOTIFY` (for immediate push) with 1s polling as a reconnect fallback. No etcd, no Consul, no ZooKeeper — deliberate component-count discipline.

Hot path never hits Postgres. Every node and every client caches the assignment tables locally; routing decisions are purely local reads. Postgres receives writes only on scheduler events and the 10s-periodic heartbeat update (100 writes/s at 1000 nodes — trivial).

**Layer 2 — Per-tablet metadata (dogfooded).** The per-tablet catalogs (session registry, segment catalog, index pointers, HLC checkpoint, fork refs) are stored **using AgentDB's own storage engine**: local `KvStore` for warm access, periodic snapshots to object storage under `metadata/{tablet_id}/...`. This is the same write path and durability story as event data.

Three reasons this is the right choice:

1. **Access pattern match.** Per-tablet metadata is per-tablet prefix-scanned, append-dominant, with warm/cold tiering needs — identical to event data. Reusing the event engine avoids a second storage implementation.
2. **Durability match.** Object storage (multi-AZ replicated) is the ultimate source of truth for both. A tablet can be fully rebuilt on a new node from object storage alone.
3. **Design self-consistency.** If AgentDB's storage model cannot host its own metadata, the model is not suitable for the workloads it claims to serve. Dogfooding is a falsification test.

**Disaster-recovery invariant.** Every segment on object storage carries a header with the session's identity (tenant_id, session_id, fork ref, max HLC). If the Postgres control plane is totally lost, the global state is recoverable by scanning object storage prefixes — slow (TB-scale scan), but not a data-loss event. Only the tenant-registry auth data is Postgres-only; that one is backup-restored from regular Postgres backups (RPO minutes).

**Cluster membership via `LISTEN / NOTIFY`, not gossip.** At cluster sizes up to ~1000 nodes, Postgres + LISTEN/NOTIFY handles membership with simpler operations and fewer failure modes than SWIM/Serf-style gossip. Gossip's convergence window becomes a correctness hazard for tablet ownership transitions, and its scaling ceiling (low hundreds before degradation) matches AgentDB's v1/v2 target. Above 1000 nodes, federate into sub-clusters before reaching for gossip.

### 2.4 Write path

```
Request → auth resolves tenant
        → shard      = hash(session_id) % S         (Layer 1; S fixed)
        → tablet = shard_assignment[shard]        (Layer 2; cached)
        → node      = tablet_assignment[tablet]  (Layer 3; cached)
        → forward to node if not local
        → acquire per-session lock (inside tablet)
        → admission control (tablet semaphore)
        → HLC = hlc.now()
        → seq = session.next_seq()
        → WAL write to object storage
        → update warm KV
        → update tablet-local index incrementally
        → release lock
        → return
```

Key differences from the prototype:

- **Session lock replaces box lock.** Two sessions on the same tablet proceed in parallel.
- **HLC is assigned inside the lock** so order within a session is monotonic in both `seq` and `hlc_ts`.
- **Incremental index update** is in the write path — no more lazy build on first query.

### 2.5 Read path

```
Request → auth resolves tenant
        → open ReadCtx (pins RocksDB snapshot + segment paths + HLC cutoff)
        → if session pinned: resolve shard → tablet → node, single-tablet read
          else:              fanout to all tablets holding the tenant's shards, merge+rerank
        → each tablet: KV scan + segment scan, post-filter by HLC cutoff
        → cold/archive segment reads use hedged-request pattern (see §5.3)
        → close ReadCtx
        → return
```

### 2.6 Location Proxy — the routing cache

Following Spanner's pattern, the mapping from `session_id → shard → tablet → node` is served by a dedicated component named the **Location Proxy**, not by ad-hoc caches scattered across the codebase. Naming it explicitly matters: it is the one place where routing staleness is handled, and centralizing that logic prevents each caller from inventing its own invalidation scheme.

Responsibilities:

- Compute `shard = hash(session_id) % S` (pure function, no I/O).
- Look up `tablet` and `node` from cached assignment tables, refreshed via Postgres `LISTEN/NOTIFY`.
- On stale routing (destination node responds "not my tablet"), issue one refresh from Postgres and retry; second failure returns an error to the caller.
- Expose metrics: `location_proxy_cache_hits`, `location_proxy_refreshes`, `location_proxy_stale_retries`.

Every client library and every in-cluster component reaches routing state through the Location Proxy. Nothing talks to Postgres for routing in the hot path; Postgres only pushes change events via LISTEN/NOTIFY.

---

## 3. Clock and consistency

### 3.1 HLC as the clock

AgentDB uses Hybrid Logical Clock (HLC) for causality-respecting total order across tablets with bounded drift from wall clock. Rationale and options-comparison are in the clock-decision note; summary here.

**Timestamp shape:** `(physical_ns: u64, logical: u32)` — 12 bytes per event. Storage overhead on a 10 KB event is 0.12%.

**Every event carries an `hlc_ts`.** Assigned under the session lock at append time; monotonic both intra-session (by seq) and across all events a single process ever emits.

**HLC propagates on every RPC.** Inbound HLC does a merge against local clock. Outbound RPC carries current local HLC.

**Mongo `$clusterTime` maps to HLC directly.** Mongo 3.6+ drivers get causal consistency for free.

**Max-offset guard:** inbound HLC with physical component > `local_wall_clock + AGENTDB_HLC_MAX_OFFSET_SECS` (default 10s) is rejected, request fails, counter increments. Prevents a single rogue clock from poisoning the global HLC.

### 3.2 Snapshot reads

A `ReadCtx` pins:

- A RocksDB snapshot (local-level isolation).
- A set of segment paths (pinned against the segment GC).
- An HLC cutoff (the "snapshot time" for cross-tablet coherence).

Read filtering applies `event.hlc_ts <= ctx.hlc_cutoff`. Fanout across tablets propagates the cutoff so each tablet returns its view as-of the same HLC time.

### 3.3 What HLC does not give (explicit)

- **Not external consistency.** An observer with a skewed clock may see `T2 < T1` when real-world commit order was `T1 < T2`. Agent workloads accept this; banking does not. AgentDB is not a banking database.
- **Not a concurrency primitive.** HLC orders reads. Write conflicts are prevented by per-session locks.
- **Not a clock-repair mechanism.** HLC bounds the effect of drift; operational monitoring must fix the cause.

### 3.4 Transaction posture

Unchanged in shape from the original design, sharpened in detail:

- **Default write: single-session, single-document atomic.** Per-session lock is the serialization point. No 2PC, no global commit.
- **Default read: snapshot-consistent via ReadCtx + HLC cutoff.**
- **Opt-in strong isolation: deferred.** The API shape is reserved; no implementation in v1. When it ships, it will use HLC + a tablet-scoped 2PC variant, restricted to single-tablet multi-session transactions. Cross-tablet serializable transactions remain out of scope.

---

## 4. Failure model

### 4.1 Object storage as the durable source of truth

The durability contract is: **if an event's WAL object has been persisted to object storage, the event survives any node failure.** Object storage (S3, MinIO, Ceph) already replicates under the hood; AgentDB inherits this and does not layer additional consensus.

Consequences:

- **No per-tablet Paxos / Raft in v1.** A tablet has one writer at a time; loss of that writer is recovered by a failover process, not by quorum replication.
- **Warm KV is a cache, not a log.** RocksDB state can be rebuilt from object-storage segments plus the object-storage-backed WAL.
- **Failover = tablet ownership transfer.** Metadata service records which node owns which tablet; on failure, a new node takes ownership and replays recent WAL objects to rebuild warm state.

### 4.2 The failover window

**Target:** recover an unhealthy tablet onto a new node in seconds, not minutes. The window is bounded by:

- WAL tail replay time (recent object-storage WAL entries).
- Warm-state rebuild from segments (bounded by working-set size per tablet).
- Index rebuild — eliminated once indexes are persistent (§5.1).

Concrete v1 target: p95 failover < 30 seconds for a tablet at the median warm size.

### 4.3 What's deferred

- **Multi-writer per tablet.** Not in v1. A tablet is single-writer; hot-session split (deferred; see §10.1) is the escape for single-session write bottlenecks.
- **Multi-region active-active.** Not in v1 or v2.
- **Byzantine-tolerant protocols.** Never.
- **TrueTime / commit-wait.** Rejected permanently for AgentDB; the external-consistency guarantee it provides is not worth the infrastructure cost for this workload.

### 4.4 Graceful degradation when the control plane is unavailable

The single hardest property of this design is that **no single metadata component is on the hot path**. This keeps blast-radius bounded when the control plane (Postgres) or one of its components fails:

| Component fails | Effect on data plane | Effect on control plane |
|---|---|---|
| Postgres primary down | None — tablet runtimes serve from cached assignments, HLC continues, object storage writes continue | New tenant registration, tablet migration, failover decisions are paused until standby takes over (seconds) |
| Postgres standby failover in progress | None — clients and runtimes use cached assignment | Writes to control plane queue or retry |
| Node crash | Clients routed to that node get connection errors; retry after cache refresh finds new owner (after failover window) | Scheduler observes stale heartbeat, assigns tablets to other nodes |
| Object storage transient failure | Writes fail-fast (durability cannot be promised); warm reads serve from KV cache | None |
| Object storage lost entirely (hypothetical) | Data-loss event | Control plane unaffected (held in Postgres) |
| Postgres lost entirely (hypothetical) | Tablet runtimes keep serving from cached state until assignments need changes | Tenant registry is lost; recover from Postgres backup (RPO minutes), or worst-case rebuild from object-storage segment headers |

The invariant: **every component can be unavailable without causing an immediate total outage, and recovery paths are bounded-time.**

This is the PD-avoidance property (§11). It does not come from any single clever mechanism; it comes from the systematic rule that the control plane is a backup synchronization point, not an online dependency.

### 4.5 Tablet ownership — leases and fencing tokens

The v1 design intentionally omits Paxos/Raft at the tablet level (§4.1, §9). What keeps the single-writer model safe from split-brain during failover is a **lease + fencing** discipline modeled on Chubby, ZooKeeper, CockroachDB Range leases, and TiKV Region leases. Martin Kleppmann's 2016 analysis ("How to do distributed locking") is the canonical argument: for single-owner correctness, the load-bearing primitive is the **fencing token**, not the consensus protocol.

**Lease mechanics.**

- Tablet ownership is a time-bounded lease held by one node at a time. Lease duration: `LEASE_DURATION_SECS` (default 30).
- The owner renews every `LEASE_RENEW_SECS` (default 10) via an atomic `UPDATE tablet_ownership SET expires_at = now() + 30s WHERE tablet_id = $t AND holder = $self AND expires_at > now()`. The update succeeds only if the owner still validly holds the lease.
- Before every write batch, the owner verifies in-memory that its lease has not expired. Expired → stop writing immediately, fail-fast pending requests, surrender the tablet.
- If Postgres is unreachable longer than `LEASE_DURATION_SECS − LEASE_RENEW_SECS` (default 20s), the owner proactively stops writing. It cannot prove its lease is still valid; the conservative action is silence.

**Scheduler reassignment rule.** When the scheduler observes a node stop heartbeating, it does **not** immediately reassign that node's tablets. It waits until `old_lease_expiry + SAFETY_MARGIN_SECS` (default 5s) before transferring ownership. This guarantees the old owner has either voluntarily stopped or its lease has provably expired — no concurrent writers can exist.

**Fencing tokens.** Every tablet carries a monotonically increasing **epoch** stored in Postgres alongside the lease. On every ownership change — voluntary transfer, failover, lease expiry — the scheduler atomically increments the epoch in the same transaction that records the new holder.

Every write from a tablet owner carries its epoch:

- **Object storage paths** include the epoch: `wal/{tablet_id}/epoch={N}/...`, `segments/{tablet_id}/epoch={N}/...`. Readers honor only the latest epoch's objects for any given logical position.
- **Metadata updates** in Postgres are gated by `WHERE current_epoch = $self_epoch`. Stale-epoch updates fail at the conditional check.
- **Tablet-to-tablet RPCs** (rare: cross-tablet read fanout) carry the caller's epoch; receivers verify it against Postgres on cache miss.

An owner whose lease has silently expired and whose epoch has been superseded cannot damage the system. Its reads remain correct (reading at an old HLC snapshot is valid). Its writes fail at either the Postgres conditional update or the read-side epoch filter on object storage. **The lease is the fast path; the fencing token is the correctness backstop.**

**What this buys, what it doesn't.**

- **Gives:** single-writer correctness without consensus; failover safety equivalent to Raft's leader-election property for the single-writer case; lower operational complexity than running consensus per tablet.
- **Does not give:** sub-second failover. Realistic failover window is `LEASE_DURATION + SAFETY_MARGIN + cache refresh` ≈ 30–60s end-to-end. For agent workloads and their LLM-dominated latency budget (agent-db.md §2.1), this is acceptable. For applications that cannot tolerate it, see §10.8.

---

## 5. Performance contract

### 5.1 Indexes are persistent and incremental

The prototype's lazy, in-memory, rebuild-on-restart index architecture does not scale. The revised design requires:

- **Persistent.** Index state serialized to object storage under `indexes/{tablet_id}/...`. Survives process restart. Recovery is bounded-time replay of any WAL events written after the last index snapshot.
- **Incremental.** New events flow into indexes on append. HNSW backend swap from `instant-distance` to a library supporting incremental insert (hnswlib or usearch candidates).
- **Tablet-local.** No global index crossing tablets; queries that need cross-tablet results fan out.

This is elevated to P0 — it is the first big engineering milestone, ahead of the tablet-unit refactor, because the tablet work depends on the storage and recovery story the persistent-index work establishes.

### 5.2 Fanout retrieval with over-fetch

Cross-tablet hybrid retrieval uses **over-fetch + merge + rerank**:

- Each of M tablets returns top-`K·f` candidates (over-fetch factor `f`, default 2).
- Coordinator reranks `K·f·M` candidates globally, returns top-K.
- Recall guarantee depends on `f` and score-distribution variance; benchmarked workload establishes `f`.

Pre-filter execution order is preserved (structured filter before ANN) — carried from agent-db.md §4.4.

### 5.3 Tail latency — hedged reads

Object storage has fat tails (seconds, occasionally tens of seconds on S3). Snapshot reads on cold/archive segments use the **hedged-request with tied cancellation** pattern from "The Tail at Scale":

- Fire primary read.
- After `δ` (per-tier: 50 ms cold, 200 ms archive), fire a second read.
- Whichever completes first wins; the loser is cancelled.

This bounds p99.9 without requiring storage-layer changes. It is v1 plumbing in the cold-segment read path, not a later optimization.

Micro-sharded segments (splitting a segment into fixed-size sub-objects so any single slow read blocks less data) is a follow-up lever, applied only if hedged reads prove insufficient under measured load.

### 5.4 Per-session locking

Write concurrency within a tablet is bounded by a per-tablet semaphore and serialized per-session by a sharded lock map (`DashMap<SessionId, Arc<Mutex<()>>>` with LRU eviction). Two sessions in the same tablet append in parallel; two writes to the same session serialize.

Cross-session atomic writes (rare, opt-in) acquire session locks in ascending session-id order to avoid deadlock.

### 5.5 Admission control

Admission operates at three levels:

- **Per-session.** The session lock naturally bounds single-session throughput.
- **Per-tablet.** Semaphore bounds in-flight writes per tablet. Protects the write pipeline from hotspot sessions on the same tablet.
- **Per-tenant and global.** Token bucket — carried from the prototype's M10 admission control. Enforces quota attribution.

---

## 6. Real-time event subscriptions

The agent-event log isn't useful only as a write target. Many
agent designs need live notification on appends — a fan-out
reactor that picks up new tool-call results, a browser tab
showing a session's progress, a multi-agent design where
agent-B reacts to agent-A's writes, an audit indexer that
ships events to a downstream pipeline. AgentDB exposes this
directly, with two complementary primitives.

### 6.1 Durable, replayable session-event stream

`GET /api/tenants/{t}/subscribe` and
`GET /api/tenants/{t}/sessions/{s}/subscribe` open a
long-lived **Server-Sent Events** stream. Each SSE message
carries one event envelope: `{tenant, session, seq, ts_ms,
payload, kind?, actor_id?}`. The stream is **gap-free**: every
appended event in the matching scope is delivered exactly once
in seq order, with no skipped sequences and no duplicates
across reconnects.

**Replay + live tail in one connection.** A subscriber may
specify `from_seq=N` (per-session) or
`from_seq_per_session=sid:seq,sid:seq,...` (tenant-wide) to
catch up on history before the live tail begins. The server
streams the historical events from KV/object storage, then
seamlessly transitions into the live tail without dropping
events at the boundary. From the consumer's perspective there
is no replay/live distinction — it's one ordered stream.

**Filters.** Optional `session=<id>[,id...]` and
`kind=<k>[,k...]` query parameters narrow the stream
server-side. Filtering happens before fan-out so an inactive
subscriber doesn't pay for events it doesn't want.

**Auth via JWT, with transparent refresh.** The subscribe
endpoints take a bearer JWT (separate from the Basic-Auth
write path — JWT supports browser-direct subscribers without
storing passwords). When the token expires mid-stream, the
server emits a terminal `event: error` frame with
`error: jwt_expired` and `last_seq: N`; SDKs in Python /
TypeScript / Go transparently fetch a fresh token via the
caller's `get_token` callback and reconnect with
`from_seq = N + 1`. The consumer never sees a gap.

**Backpressure and lag policy.** Each subscriber has a
bounded in-memory queue. When a subscriber lags far enough
that the queue would overflow, the server emits
`event: error` with `error: lag_too_far, last_seq: N` and
closes. The subscriber can choose to reconnect with a fresher
`from_seq` (skipping the gap explicitly) or treat it as a
terminal failure. Catch-up failures (`catchup_failed`) are
also explicit terminal frames — better to fail loudly than
silently desync.

**Multi-node behavior.** A subscriber connects to any node;
the node forwards the subscribe to the tablet's owning node,
which is the source of truth for the session's seq counter.
The subscriber sees the same total order regardless of which
node it connected through.

The full design rationale (replay differentiator, mapping to
Supabase Realtime concepts, latency profile) lives in the
preparatory spec at
`docs/superpowers/specs/2026-04-25-pubsub-design.md`.

### 6.2 Ephemeral broadcast channels

Some signals shouldn't be persisted: typing indicators,
cursor positions, presence pings, "agent X is alive" beats.
Writing those into the durable event log inflates storage,
forces compaction, and pollutes session retrieval with noise.
A separate **ephemeral** primitive serves them:

- `POST /api/tenants/{t}/channels/{c}/publish` — fire-and-
  forget. Any JSON payload. Returns 204 on success; nothing
  is stored.
- `GET /api/tenants/{t}/channels/{c}/subscribe` — SSE stream
  of broadcasts on that channel. No replay (there's nothing
  to replay), no `from_seq`, no durability.

**Auth via a `channels` JWT claim.** A subscriber's JWT
declares which channels it may join (e.g.
`{"channels": ["room:42", "presence:*"]}`). Channel-name
glob matching is server-side; a subscriber attempting to
join a channel not in its claim gets `403
channel_not_in_jwt`.

**`BroadcastBus` semantics.** In-process tokio broadcast
channels with a fixed capacity per channel (default: a few
hundred messages). When a subscriber lags such that its
buffer is full, the **server drops the oldest queued
messages for that subscriber and continues** — a stuck
subscriber doesn't degrade publishers or other subscribers.
This is the deliberate trade for "transient signals" — if
durability matters, use the per-session
`append + subscribe` path.

**`no_echo` filter.** Publishers commonly subscribe to the
same channel they publish on; the SDK can pass
`?no_echo=1` so the server filters out the subscriber's
own publishes (correlated by a per-connection id), avoiding
echo-loop traffic in collaborative-editing-style designs.

**Multi-node.** Ephemeral broadcasts are in-process per node.
A publisher and subscriber on different nodes do not see
each other's traffic. This is acceptable for the target
use cases (typing indicators are user-local; cursor
positions go to the same node hosting the session). When
cross-node broadcast becomes a real requirement, the
follow-up design lifts the bus into a multi-node fanout
(via the same Postgres-LISTEN-NOTIFY hub or a dedicated
pubsub backbone).

**Channel naming.** `^[a-zA-Z0-9._:-]{1,256}$`. Conventions
like `room:42`, `presence:agent-7`, or `cursor:doc-id` are
recommended; the server doesn't interpret the name.

The full broadcast design lives at
`docs/superpowers/specs/2026-04-26-pubsub-phase2-design.md`.

### 6.3 When to use which

| Property | Durable subscribe | Ephemeral broadcast |
|---|---|---|
| Durable | yes | no |
| Replayable | yes (`from_seq`) | no |
| Gap-free | yes | no (lag → drop) |
| Per-session ordered | yes | n/a |
| Auth scope | per-tenant + per-session | per-channel JWT claim |
| Multi-node fanout | yes | no |
| Use case | tool-call results, audit, agent reactors | typing, cursors, presence pings |

The two are deliberately separate primitives. Sending a
typing indicator through the durable log would be an order-
of-magnitude space waste; persisting a tool-call result as
a broadcast would lose the audit trail. Picking the right
one is a design choice the writer makes per-event.

### 6.4 Presence (deferred)

Presence ("which agents/users are currently subscribed to
this session?") is the natural extension of broadcast. It
needs a different consistency model — last-write-wins state
synchronization across subscribers, with TTL-based
expiry — and is sketched as a future-work appendix in the
broadcast spec. Not in scope yet.

---

## 7. API surface

### 7.1 Native interface (long-term primary)

A typed RPC surface (gRPC + protobuf, specifics v2+) is the eventual primary interface. v1 ships a native REST surface using the primitives below; the RPC shift is a follow-up.

Primitives (each a single call):

- `append_and_fetch` — single-event append + return last-N (the core high-frequency primitive).
- `fork_session` — copy-on-write fork; returns new session id + parent reference.
- `fetch_last_n` — last-N events, merged across fork chain, snapshot-consistent.
- `scan_session` — full session scan.
- `hybrid_retrieve` — text + vector + structured filter, session-local or cross-tablet fanout.
- `subscribe` — durable, replayable, gap-free SSE event stream (per-session or tenant-wide); see §6.1.
- `broadcast` + `subscribe_channel` — ephemeral pub/sub for transient signals (typing, cursors, presence pings); see §6.2.
- `admin/*` — tier transitions, metrics, HLC inspection, webhook configuration.

Request conventions:

- Tenant is **never** in the request body.
- Authorization header carries the tenant-bound token.
- `X-AgentDB-HLC` header carries the caller's HLC for causal-consistency propagation (mirroring Mongo `$clusterTime`).

### 7.2 Mongo wire (short-term adoption)

Mongo OP_MSG subset, as in the prototype:

- `insert` → maps to append.
- `find` → maps to session scan / filter.
- `aggregate` (limited pipeline) → maps to hybrid retrieve or scan.
- `createCollection` with `tabletKey` metadata → tablet declaration (reserved; v1 uses defaults).
- `$clusterTime` ↔ HLC (bidirectional).

This is **a compatibility layer for adoption**, not the primary contract. Every Mongo feature absent from the subset is an intentional non-goal, not a bug.

### 7.3 DDL posture

No SQL DDL. Schema evolution happens by writing new fields directly (agent-db.md §3.1). Collection-level metadata via `createCollection` (tablet key, indexes) is a deployment-time administrative action — compatible with the no-DDL stance because it does not affect per-document schema evolution.

---

## 8. Observability

### 8.1 Distributed tracing

**OpenTelemetry propagated end-to-end from v1.** Every request carries a trace ID through wire/api → core → storage → tablet fanout coordinator. Fanout spans are children of the parent request span. Hedged reads emit both primary and secondary spans.

Not optional, not deferred: once queries cross tablets + indexes + object store, absence of tracing makes production debugging guesswork.

### 8.2 Metrics

Per-tablet counters and gauges (extending the prototype's `BoxMetrics`):

- Throughput: `appends`, `forks`, `queries`, `tier_transitions`
- State: `warm_sessions`, `cold_sessions`, `archive_sessions`
- Admission: `quota_hits` (with `kind` = tablet | tenant | global)
- Clock: `hlc_physical_offset_ns` gauge, `hlc_offset_rejects` counter
- Tail: `hedged_triggers`, `hedged_winners` (primary vs secondary)
- Failover: `failover_duration`, `failover_count`

### 8.3 Admin + SSE

- `GET /api/admin/tablets` — per-tablet snapshot.
- `GET /api/admin/hlc` — current HLC, wall clock, offset, reject count.
- `GET /api/stream` — SSE for metrics ticks, tier changes, fork events, fault injections, quota hits.

### 8.4 Dashboard

The built-in dashboard (`agentdb-dashboard`) consumes both the snapshot endpoint and the SSE stream. Existing prototype dashboard survives; new panels for HLC skew, hedged-read rate, and failover status.

---

## 9. Explicit non-goals

Writing these down prevents scope drift under future review pressure:

- **TrueTime / atomic clock infrastructure.** HLC is the final choice.
- **Per-tablet Paxos / Raft consensus** for the write path in v1. Object storage is the durable record.
- **Global secondary indexes** spanning tablets. Fanout-merge handles cross-tablet queries.
- **Full MongoDB wire protocol compatibility.** Defined subset only.
- **Agent as first-class resource** with endpoints, registration, lifecycle, per-agent quota. Agent is a metadata dimension.
- **Adaptive (self-rebalancing) tablet.** v2+ consideration.
- **User-supplied affinity-key routing.** v2+ consideration.
- **Multi-region active-active writes.** Permanent non-goal for v1 and v2.
- **Transactional writes to external systems** (LLM calls, message queues). The database emits events; the application acts on them.
- **Separate coordination service** (etcd / ZooKeeper / Consul) in v1. HA Postgres + `LISTEN/NOTIFY` carries this role. Explicit component-count discipline — see §11.
- **Gossip protocol for node membership** in v1. Postgres membership with `LISTEN/NOTIFY` suffices up to ~1000 nodes; beyond that, federate into sub-clusters before introducing gossip.
- **Variable `S` (shard count) in a running cluster.** `S` is picked once at cluster-init time and is immutable for that cluster's lifetime. Growing `S` requires a migration equivalent to creating a new cluster.
- **Separate dev build and prod build of the binary.** The same binary serves all three deployment modes (§12). Differences are config-driven, not compile-time. This prevents silent skew between "what I tested on my laptop" and "what runs in prod."

---

## 10. Open questions and deferred work

Items explicitly parked for a later design cycle:

### 10.1 Auto-split / auto-rebalance for hot tablets

**Hot tablet** (one tablet receiving disproportionate load
from its shards) — partially shipped. The hot-tablet detector,
the manual `shed-tablet` admin endpoint, and the dynamic
tablet creation primitive are all in place; cooperative
handoff is in. What's still deferred is the **scheduler-side
auto-decision**: today an operator inspects the detector
output and runs `agentdb shed-tablet` manually. The natural
follow-up is a leader-side periodic loop that reads the same
signals and applies the shed automatically (with safety
floors and rate limits). Target: the split decision is
driven by scheduler policy, not manual runbook.

**Hot session** (one session within a tablet receiving
runaway load — e.g., a looping agent): not solved by tablet
split because all traffic belongs to a single
`shard = hash(session_id) % S`. Harder case, deferred.
Candidate mechanisms:

- **Per-session override path** (§1.5) routes the hot
  session to an isolated tablet. One row in Postgres
  override table; forwarded via Location Proxy.
- **Session sub-partitioning** (more complex): event
  appends for the same session split across multiple
  back-end storage units, coordinated via HLC. Design must
  land first — does `fetch_last_N` observe events across
  the sub-partition boundary? Does fork chain cross? Does
  retrieval span?

### 10.2 Opt-in cross-tablet transactions

Single-tablet multi-session transactions are feasible with HLC + tablet-scoped 2PC. Cross-tablet transactions require distributed commit, which is out of scope for v1. Placeholder API shape reserved.

### 10.3 Native typed RPC

Primary v2+ interface. Mongo wire becomes a translation layer. Specifics deferred.

### 10.4 Full agent-as-resource

Agent registration endpoints, per-agent quota, agent lifecycle. Deferred pending clear multi-agent-framework direction.

### 10.5 User-supplied affinity key

Session-creation-time tablet hint for workloads with natural cross-session coordination. P2; revisit after benchmark workload produces evidence.

### 10.6 Presence (real-time follow-up)

Last-write-wins, TTL-expiring presence state ("which agents
are subscribed to this session right now?"). Different
consistency model from durable subscribe / ephemeral broadcast; sketched as a
future-work appendix in
`docs/superpowers/specs/2026-04-26-pubsub-phase2-design.md`.

### 10.7 Cross-node ephemeral broadcast

Ephemeral broadcasts (§6.2) are in-process per node — a
publisher and subscriber on different nodes don't see each
other's traffic. Acceptable for the launch use cases (typing
indicators, presence pings — usually the same user's
sessions anchor on one node). When a real cross-node
broadcast use case lands, lift the bus into a multi-node
fanout via Postgres LISTEN/NOTIFY or a dedicated pubsub
backbone. Out of scope until evidence.

### 10.8 Consensus-based tablet replication for sub-second failover

The v1 failure model trades a ~30–60s failover window for operational simplicity: no Paxos/Raft on tablet writes, object storage as the durable record, lease + fencing as the ownership-safety mechanism (§4.5).

Deployments that cannot tolerate this window — financial applications, real-time bidding, safety-critical control — will eventually need tablet-level consensus replication. This is the path Spanner, CockroachDB, and Yugabyte take. Adding it is an **additive v3+ upgrade**, not a v2 commitment. The data model is deliberately shaped to not foreclose it:

- Tablets already carry a monotonically-increasing epoch suitable for Raft-style term numbering.
- The single-writer path is structurally the same thing Raft calls "leader appends"; promoting it to a Raft leader is a contained change.
- Object storage remains the ultimate durability substrate even with consensus — the consensus layer reduces failover time, not durability guarantees.
- Lease + fencing tokens remain the correctness floor even with consensus on top.

Serious production deployments eventually want this. v1 does not. The scope line is explicit.

---

## 11. Durable invariants — lessons from TiKV/PD

The control-plane shape in this document is designed specifically to avoid the scaling anti-patterns that TiKV's Placement Driver (PD) ran into at scale. These invariants are load-bearing: violating any of them reintroduces the PD class of problems, regardless of the terminology in use at the time.

**(a) The central store is never on the data-plane hot path.**
Append, fetch, snapshot read, fanout query — none may contact Postgres synchronously. Routing is resolved via Location Proxy's local cache, computed from hash + cached assignment. Postgres is consulted only by the scheduler and during cache miss/invalidation.

**(b) Node membership is coordinated through a small, read-mostly channel, not through fan-in heartbeats of heavy data.**
Nodes write minimal heartbeats (alive + light load). They do not report tablet-level detail on every beat. PD's pain came from every TiKV reporting full region-list diffs every 10 seconds; this scheme is forbidden.

**(c) Scheduling is predominantly local to a tablet, not centralized.**
Tier transitions, segment GC, index snapshots, per-session fork — the tablet decides these itself. Only cross-tablet rebalancing and failover go through the scheduler leader. The scheduler is not a serialization point for routine operations.

**(d) There is no global timestamp oracle.**
HLC is fully local, with bounded drift. No write ever waits on an RPC to a central timestamp service.

**(e) Metadata schema is shardable from day one.**
Tenant rows partition by `hash(tenant_id)`; assignment tables partition by natural key range. No global auto-increment columns, no cross-row invariants spanning the whole cluster. If Postgres ever grows beyond a single instance, sharding is a routine migration, not a schema retrofit.

**(f) Every layer tolerates its dependency being unavailable.**
Running tablets keep serving if Postgres is down. Postgres keeps recording state if object storage is briefly unavailable. No component's outage causes an immediate total outage (§4.4).

**(g) Bootstrap is idempotent from empty state.**
A fresh cluster can come up with an empty Postgres and empty object storage. A new node can join without requiring pre-seeded state — gossip is not required because cached assignments can be populated on demand from Postgres. Cluster state can be reconstructed from object storage + a Postgres backup after a catastrophic loss, with bounded recovery time.

**(h) Ownership is leased, not owned; every tablet-scoped write carries a fencing token.**
Tablet owners hold a time-bounded Postgres lease and stop writing when the lease expires. Scheduler reassignment waits for lease expiry plus a safety margin. Every write is tagged with a monotonically-increasing epoch; writes with stale epochs are rejected at the storage or metadata layer regardless of the sender's state. This replaces Paxos/Raft's single-leader property for the single-writer model without requiring per-tablet consensus. Mechanics in §4.5.

These eight invariants are the reason AgentDB's metadata architecture looks like Cassandra's `system` keyspace + async scheduler rather than TiKV's PD. They are durable commitments, not implementation convenience; future work should be checked against them before it lands.

The consensus-based alternative (§10.8) remains an explicit upgrade lever for production deployments that outgrow the ~30s failover window these invariants imply.

---

## 12. Deployment modes

The design accumulates real architectural complexity — HA Postgres, object storage, lease + fencing, HLC, tablet scheduler, Location Proxy. None of that complexity should translate into **deployment** complexity. A developer on a laptop should be running a working cluster in under one minute, and a small self-hosted production deployment should be configurable in under ten.

The discipline: **one binary, three modes, all differences are config-driven.** Not separate dev/prod builds. Not feature-flag-gated code paths. Same binary, different environment.

| Mode | Use | External deps | HA | Launch |
|---|---|---|---|---|
| **Embedded / dev** | laptop, CI, demo, teaching, tiny self-host | none | n/a | `agentdb start --dev` |
| **Single-node prod** | small production, edge, initial deployment | Postgres, object storage | data-plane via object storage only | `agentdb start --config prod.toml` |
| **Clustered prod** | real production | HA Postgres, object storage, optional Redis | full (failover, cross-AZ scheduling) | Helm / docker-compose / systemd |

### 12.1 Embedded / dev mode

- **Postgres role** → local SQLite file. SQLite supports the transactions lease + fencing needs; the assignment tables and tenant registry fit easily.
- **Object storage** → local filesystem (`./data/segments/`, `./data/wal/`). The `KvStore` trait and existing `SlateKvStore` pattern already support pluggable backends.
- **Everything else in-process**: HLC, scheduler, Location Proxy, tablet runtime, wire protocol handlers, dashboard.
- **Zero config required.** Defaults cover every knob.

Developer flow:

```bash
agentdb start --dev
# Ready in ~200ms. Mongo wire :27017, REST :8080, dashboard :9000.

mongosh mongodb://localhost:27017
# Insert your first event in < 10 seconds from cold.
```

Precedent: CockroachDB `cockroach start-single-node`, DuckDB's fully-embedded model, SQLite itself, early InfluxDB embedded mode. The embedded-to-clustered path is a well-trodden deployment shape.

### 12.2 Single-node production

One AgentDB process with real external storage:

- **External Postgres** — managed (RDS / CloudSQL / Neon) or self-hosted (Patroni / pg_auto_failover).
- **External object storage** — S3 / MinIO / Ceph / GCS.
- **Single AgentDB node** hosts all tablets.

No node-level HA, but **data-level durability is intact**: S3 is already multi-AZ replicated, so a crash of the AgentDB process just triggers a restart with full recovery from object storage. The failover window for single-node is "how long does a process restart take." For small production workloads this is acceptable.

Configuration is small (≤ 5 required items):

```toml
# prod.toml
postgres_url = "postgres://..."
s3_endpoint  = "https://s3.amazonaws.com"
s3_bucket    = "my-agentdb-data"
cluster_name = "tenant-alpha-prod"
# Everything else: defaults.
```

### 12.3 Clustered production

The full design: multiple AgentDB nodes, HA Postgres, object storage, optional Redis for cross-node rate-limiting. Deployed via Helm (k8s), docker-compose (small bare-metal), or systemd (traditional). `examples/` in the repo ships all three.

`docker compose up` in `examples/compose/` brings up a 3-node cluster with Postgres and MinIO in ~30 seconds — the "second-step" tutorial after dev mode.

### 12.4 Mode invariant

All three modes share the binary, the wire protocol, the schema, the Mongo driver compatibility, the dashboard, the admin CLI. A workload that runs correctly in dev mode runs correctly in clustered mode without code changes — only config changes.

This is why "separate dev build" is an explicit non-goal (§9). Silent skew between a developer's laptop behavior and production behavior is one of the most common ways distributed systems fail in production; we refuse it by construction.

---

## 13. Operational surface

The design's internal complexity is paid for with deliberate investment in operability. Every subsystem we introduced (HLC, lease + fencing, scheduler, Location Proxy) has a corresponding operator-facing affordance. Operators never have to grep logs to understand the system.

### 13.1 Configuration discipline

- **Every parameter has a default.** Dev mode runs with zero flags.
- **Single-node prod** needs ≤ 5 config items (Postgres URL, S3 endpoint, bucket, credentials, cluster name).
- **Clustered prod** may tune timers and thresholds for scale, but need not.
- **Every parameter documented** with default value and "when to change it." No knob is a mystery.
- `agentdb dump-config` prints the effective configuration with provenance (default / env / file / flag) for each value.

### 13.2 Schema migrations

Postgres schema is **embedded in the binary** via `refinery` or `sqlx::migrate!`. Migrations run automatically on first start of a new version. Idempotent, versioned, forward-only. No external migration tool required.

For SQLite (dev mode), the same mechanism runs — identical schema in a SQLite dialect.

### 13.3 Rolling upgrade

- **Assignment-table schema is append-only.** New columns have defaults; old binaries ignore new columns they don't understand.
- **New and old binaries must coexist** across at least one minor-version boundary. CI pipeline includes a cross-version test.
- **Tablet segment headers include a schema version**; readers tolerate one version of difference.
- **Rolling procedure**: drain a node (migrate its tablets to peers) → upgrade binary → node rejoins → scheduler reassigns tablets. The drain-upgrade-join cycle is an `agentdb` CLI command.

### 13.4 Health checks

- `GET /health/live` — process is up and HLC is advancing.
- `GET /health/ready` — Postgres reachable, object storage reachable, every tablet this node should own has a valid lease. Kubernetes-convention; usable by any load balancer.

### 13.5 Admin CLI

Same binary, subcommand form:

- `agentdb tablet list` — tablets on this node, lease state, epoch, warm size, QPS.
- `agentdb tablet failover <id> <target>` — manual ownership transfer.
- `agentdb tablet inspect <id>` — segment catalog, HLC checkpoint, fork chain.
- `agentdb flush-cache` — drop in-memory routing caches.
- `agentdb dump-config` — effective configuration with provenance.
- `agentdb show-lease <tablet>` — current holder, expiry, epoch.
- `agentdb drain` — graceful shutdown; migrate tablets to peers before exit.
- `agentdb hlc` — current HLC, wall clock, offset, reject count.

All of these are available in dev mode too — operators learn the tool on their laptop.

### 13.6 Runbook-friendly error responses

Every error response includes a stable error code and a runbook URL:

```json
{
  "error": "lease_expired",
  "tablet_id": "t_0x1a2b",
  "code": "E0047",
  "doc": "https://agentdb.dev/runbooks/E0047"
}
```

Runbooks live in `docs/runbooks/`, are short (one-page typical), and are structured: *Symptom → Diagnose → Verify → Remediate → Prevent*. Each runbook is linked from the error code that triggers it.

### 13.7 Observability plugs into existing infrastructure

OpenTelemetry tracing (§8.1) and per-tablet metrics (§8.2) work in all three modes. In dev mode they render to the built-in dashboard. In clustered production they flow to the user's existing collector — Jaeger, Tempo, Datadog, Prometheus, whatever. We do not ship a full observability stack; we emit standard formats and let operators plug them into infrastructure they already run.

---
