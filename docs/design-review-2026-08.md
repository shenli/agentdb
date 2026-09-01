# AgentDB Design Review — August 2026

Reviewed at `main` @ `97fc38f`. Scope: design docs (`docs/design/*`), the Rust
workspace (~93k LoC), and a comparison against production lessons from
large-scale distributed KV stores. A rendered architecture diagram of the current
implementation accompanies this review at
[`assets/design-review-2026-08-architecture.html`](assets/design-review-2026-08-architecture.html).

> **Branch status (`dev/simplify-architecture`)**: the "cut now" set from the
> simplification pass below is implemented on this branch — the MongoDB wire
> listener, the SlateDB backend, the legacy quorum log, and the dead SCRAM
> code are removed, and the diagram reflects the post-cut architecture.
> Findings that reference those components describe the reviewed `main`
> commit and are retained as the record of why the cuts were made.

## Verdict

The design thinking is unusually strong. The docs are honest about
current-vs-target state, and several hard calls are correct: virtual shards as
routing slots vs tablets as the only physical unit, no synchronous object-store
writes on the hot path, the DynamoDB-style local/global record-index split, and
the explicit dedicated/shared placement invariant. The biggest problems are not
in the target architecture on paper; they are three places where the
implementation quietly contradicts the product thesis, plus one
scope-allocation issue.

## Current architecture (as implemented)

```text
SDKs / HTTP ──► agentdb-api (axum, static bearer) ──► agentdb-core Runtime ──► KvStore (RocksDB)
Mongo drivers ─► agentdb-wire ── direct calls, bypasses the API layer ──┘         │
                                                                                  ├─► ContentBackend (blobs, fsynced)
   agentdb-meta (SQLite/Postgres control plane)      cluster mode only:           │
   tablets / ranges / replicas / movements   core ─► raft-rs + raft-engine ─► tablet-local KV
```

Load-bearing implementation facts the findings below rest on:

- The authoritative state is RAM: `Runtime.state` is one `tokio::sync::Mutex`
  over 25 `BTreeMap`s holding all tenants, agents, sessions, records,
  artifacts, and aliases (`agentdb-core/src/runtime.rs:899-983`), locked at
  ~108 sites. The KV store is a mirror; startup fully rehydrates and rebuilds
  the text/vector indexes from scratch.
- Append is correctly ordered: seq minted under the state lock, lock dropped,
  one `WriteBatch` (event + export ledger + secondary index + idempotency) via
  the adaptive batcher, then memory updated after the durable write
  (`runtime.rs:7295-7390`, `persist.rs:1650-1700`).
- RocksDB `set_sync` defaults to **false**, gated on
  `AGENTDB_ROCKSDB_SYNC_WRITES` (`agentdb-storage/src/rocks.rs:76-84`); the
  group-commit window is 0 when sync is off (`persist.rs:725-750`).
- The MongoDB wire listener depends only on `agentdb-core` and is started as a
  sibling listener; it does not pass through API auth, DTO validation, or the
  cluster routing/forwarding layer.
- Watch/subscribe endpoints build the entire SSE body as a `String` and return
  a complete response (`agentdb-api/src/routes.rs:8216-8234`); `Runtime::watch`
  scans in-scope sessions under the global state lock (`runtime.rs:7608`).
- Cluster mode exists behind `ReplicatedLog` (single-node, legacy quorum, and
  raft-rs/raft-engine implementations) with a full control plane in
  `agentdb-meta`; the default deployment is single-node and the binary labels
  cluster mode forward-looking.

## Findings

### 1. The durability default contradicts the core promise (highest priority)

The pitch is "durable session log that survives crashes," but a default-config
ack means "in the memtable and unflushed WAL buffer." A power loss or SIGKILL
loses acknowledged appends. Compounding it:

- The group-commit batcher only batches when sync is on, so the design never
  exercises the thesis's own stated win ("batch many sessions into each
  flush", `product-thesis.md`).
- The crash tests cannot catch this: `open_kv` caches the live RocksDB handle
  in a process-global `OnceLock` (`tests/runtime_restart.rs:25-34`), so
  simulated restarts re-hydrate from a warm handle. WAL recovery from a torn
  on-disk state is never exercised. Mature storage engines have shipped
  data-loss bugs in exactly this blind spot (log-recycling recovery after an
  engine upgrade): restart-replay must be tested end-to-end, not assumed.

**Recommendation:** make fsync-on the default, build group commit around it,
and add a kill-9-and-reopen crash harness. Cheapest, highest-leverage
credibility fix in the repo.

> **Branch status**: implemented. Fsync is on by default with a
> backlog-aware group-commit window (idle appends pay only their own sync;
> under 32-way concurrency the durable path measured *faster* than the old
> non-durable one — see `docs/operations/performance.md`). Restart tests now
> close and reopen RocksDB for real (the warm-handle `OnceLock` is gone; the
> batcher writer thread joins deterministically on drop), and
> `ops/local-ci/tests/auto/S14_acknowledged_appends_survive_sigkill.sh`
> SIGKILLs the live server mid-append-stream and asserts every acknowledged
> append survives. Remaining: power-loss falsification of fsync itself needs
> a VM-kill/dm-flakey rig on Linux.

### 2. The global in-RAM `State` is the real scale wall — being built around, not through

Everything serializes on one lock; node capacity is bound to process RSS; the
`AGENTDB_STARTUP_MAX_*` hydration caps are guardrails on a structure that
should not need them. Most telling: `refresh_replicated_state` runs a **full
rehydrate on every Raft apply batch, invoked at the top of every append** in
cluster mode (`runtime.rs:3358-3375`, `7027`) — O(dataset) work on the hot
path.

The sequencing is inverted: large effort has gone into tablet split/merge,
rebalance, snapshots, and catch-up while the state machine those tablets serve
still binds capacity to one process's RAM and one mutex. A replicated tablet
layer under a RAM-authoritative global-lock runtime inherits all of its
limits.

**Recommendation:** before more tablet lifecycle machinery, (a) flip authority
to the KV store with memory as a bounded cache, (b) shard in-memory state by
tenant or session, (c) make Raft apply incremental. This also unblocks
`tablet-storage-engine.md` step 5 (appends as deterministic commands), which
is hard to extract while `runtime.rs` is a 12k-line god object.

### 3. Tenant isolation is a namespace, not a boundary

One installation-wide bearer token grants every tenant; `tenant_id` comes from
the URL and is never bound to the credential (`agentdb-api/src/auth.rs:21-48`).
The docs are honest that this is installation auth, but the thesis makes
multi-tenancy a first-class pillar, and every isolation claim (quota, watch
scope, blast radius, dedicated tablets, per-tenant encryption) sits above an
unauthenticated tenant selector.

The MongoDB listener widens the gap: it bypasses `agentdb-api` entirely
(auth, validation, and — fatally for cluster mode — routing/epoch
forwarding), **auto-creates tenants from the database name**
(`agentdb-wire/src/command.rs:437-465`), and compares its PLAIN password with
`==` rather than constant-time (`server.rs:107`). Separately,
`agentdb-meta/src/scram.rs` + `users.rs` implement SCRAM-SHA-256 but nothing
calls them — dead code implying a security posture that does not exist.

**Recommendations:** per-tenant token scoping as the next auth step (even a
simple token → tenant-set claim); route the wire listener through the same
internal facade as HTTP or refuse to start it in cluster mode; constant-time
compare; no tenant auto-creation; wire up SCRAM or delete it.

### 4. "Watch" is snapshot polling dressed as SSE

Each poll is O(in-scope sessions) under the hot-path state lock — a
noisy-neighbor amplifier that contradicts the "stream live state" positioning.
The right substrate already exists: the durable export ledger is
commit-ordered and cursor-based.

**Recommendation:** back `watch` with ledger cursor reads (no state lock);
layer genuine long-held SSE/broadcast on top later, as `design.md` already
anticipates.

### 5. Smaller findings (several are classic distributed-storage scars)

- **Idempotency store**: in-memory `HashMap` with no TTL/eviction
  (`idempotency.rs:25`) and durable rows with no retention — an unbounded
  queue by another name. Give it a byte quota and retention window — every
  cross-component map is a quota site.
- **Fork write-order**: `fork_session` inserts the child into memory *then*
  persists, with no rollback on persist failure (`runtime.rs:4342-4354`),
  while append correctly does durable-first. Adopt one rule everywhere:
  memory follows durable commit.
- **Raft restart** rebuilds committed entries from raft-engine in one pass —
  paginate by bytes/entries with the chunk size in config (unbounded
  log-tail scans OOM under backlog; the backlog length is the blast radius).
- **Tablet epochs are present — good.** Ensure late messages/snapshots
  addressed to a tombstoned tablet are logged no-ops, not errors —
  post-destroy messages are a normal consequence of async delivery, not
  programmer errors.
- **`std::sync::Mutex` on the BM25/HNSW indexes inside async handlers**
  (`runtime.rs:1364-1369`) blocks worker threads under search load.
- **`mem::transmute` to a `'static` RocksDB snapshot**
  (`rocks.rs:295-315`) is soundness-by-field-drop-order — needs a `// SAFETY:`
  comment at minimum, an owning wrapper ideally.
- **Testing gap vs claims**: 400+ tests and plan-driven fault scenarios are
  genuinely good, but there is no property testing, no deterministic
  simulation, no linearizability checking, and no real process-kill — while
  the docs claim zero RPO under quorum.

### 6. Strategic: the scope is fighting the thesis

The thesis says single-node must be excellent before distributed expands. What
exists today: Mongo wire compat, four export sinks, SlateDB backend,
dashboard, Helm, KMS envelope crypto, record graph edges, BM25+HNSW, and a
large Raft/tablet control plane — while single-node durability defaults to
off, state is RAM-bound, and watch polls under a global lock. Findings 1, 2,
and 4 *are* the "excellent single node." Freeze new distributed surface until
those three land.

## Agent-native principles assessment

Scored against the eight principles in "Agent-Native 基础设施的设计原则"
(Shen, db0.ai, 2026-08). Summary: strong at the primitive layer, mixed at the
resource-model layer — which that article argues is the layer incumbents
cannot retrofit and is therefore where a pre-release product should press.

| # | Principle | Verdict |
|---|---|---|
| 1 | One intent = one call | **Strong** — two retry gaps |
| 2 | Agent-scale capacity & pricing | **Mixed** — wrong protocol signals, inverted idle economics |
| 3 | Account holder ≠ end user | **Partial** — platform-shaped tenancy, no claim/transfer |
| 4 | Branching as a primitive | **Strong** — pointer forks, cost matches the name |
| 5 | Rollback boundaries in the API | **Partial** — boundaries in prose, not machine-readable |
| 6 | Creation→usable without a human | **Mostly strong** — short human-required list |
| 7 | Trust → enforced boundaries | **Weak** — fails the no-docs worst-order test |
| 8 | Agent UX as regression test | **Interface yes, evals no** |

Highlights per principle:

- **P1**: `append-and-fetch` is one intent/one call with a single failure
  boundary (one WriteBatch: event + export ledger + idempotency), correct
  per-agent/per-session attribution, and one-call raw-bytes artifact upload
  (`routes.rs:7004-7050`). Gaps: artifact `Idempotency-Key` is parsed and
  deliberately dropped (`routes.rs:7031` comment) so retried uploads
  double-store; first-touch is a 3-call choreography (tenant→agent→session)
  that only the SDK aliases hide.
- **P2**: machine-readable `X-AgentDB-Backpressure` on every write and
  per-tenant quota policy objects are the right shape; warm/cold/archive
  matches write-once-never-read economics. But there is **no 429 and no
  `Retry-After` anywhere in `agentdb-api`** — quota rejections are 403s, so
  stock agent backoff never engages; `backpressure_for_tenant` scans sessions
  under the global state lock per request; and idle tenants are not cheap
  (everything hydrates to RAM — finding 2 wearing P2's clothes).
- **P3**: SDKs bind workspace from auth and hide tenant IDs — the platform
  shape — but there is no ownership claim/transfer flow (move a tenant,
  rebind attribution/billing, export-import between installations).
- **P4**: fork is a ref write (`forked_from {session_id, at_seq}`), zero
  copy, fork-aware replay, replay-prefix materialization for deep chains —
  the cost matches the name. Caveats: abandoned branches only become cheap
  via lifecycle/GC, and the fork memory-before-persist ordering bug above.
- **P5**: append-only + fork-instead-of-undo sidesteps much of rollback;
  legal hold and derivative-only purge are explicit. Missing: a
  machine-readable reversibility declaration (what GC purges, what a tenant
  delete leaves in indexes/export ledger, object-store orphans unswept).
- **P6**: creates are synchronous, live/ready probes distinct, admin tokens
  and backup are API-driven, `LLMS.md` is the walk-yourself-in artifact.
  Human-required list: mongo listener password (env-only, not API-rotatable),
  loopback admin bootstrap open until first token mint.
- **P7**: fails the test "no docs, worst-order capability composition":
  one installation token reaches every tenant; the wire listener auto-creates
  tenants and bypasses API auth; TLS posture is documentation. The
  raw-secret write-path guards are the one boundary already converted from
  trust to enforcement — the pattern to replicate for tenancy.
- **P8**: `LLMS.md` is ahead of most infra (usage-ordered, split from
  `AGENTS.md`), but nothing measures it: no eval harness runs coding agents
  against a dev server on real tasks (resume, fork-and-explore, artifact
  recovery) as a daily regression. No first-party MCP server. Natural
  dogfood: store the eval harness's own traces in AgentDB.

New actions this pass adds: 429 + `Retry-After` everywhere (custom header
stays); wire artifact idempotency; agent-experience eval suite; machine-
readable reversibility declarations; first-party MCP server.

## Cell architecture: Universe → Cell → Split

Question: does the storage design have a real cell layer, in the Spanner
two-layer sense (Universe → Cell → Split), given the hard-won lesson that
too many chunks/regions crushes the metadata plane?

**What exists.** `cluster-storage-topology.md` Track A has cells as isolation
domains: `cells` rows, `nodes.cell_id`, `placement_scopes.cell_id`, tenant
policy cell assignment, same-cell scheduler placement, per-cell object-store
namespace, and explicit tenant-migration-between-cells as a fenced workflow.
The data-layer invariants are also right: tablets are the only physical unit,
shards are virtual routing slots with no per-shard state, and the scale
guardrails explicitly reject region explosion.

**The gap.** The cell is a *column*, not a *layer*. All metadata lives in one
shared store logically partitioned by `cell_id` (the doc calls per-cell
partitions "optional hardening"), and the metadata cardinality that the
region-explosion lesson warns about has been re-created one level up:

1. **`RouteSnapshot` loads the whole world.** Snapshot construction calls
   `session_locators::list_all(pool)` (`routing_http.rs:601`) plus all
   `shards` rows into every routing snapshot on every refresh. Session
   locators are O(total sessions) — at the workload the product thesis
   projects (agents creating sessions at machine rate), this is the region-
   explosion analog, except worse: per-*session*, not per-range. Systems
   that kept chunks large at the data layer still hit metadata pressure from
   chunk count; AgentDB kept tablets large but made its finest-grained
   metadata row a session.
2. **Per-shard rows.** `shards` is seeded as 16,384 rows
   (`shards.rs:21`) and loaded wholesale; the compressed `tablet_ranges`
   representation exists but the per-shard table is still authoritative at
   prototype scale. A dedicated tenant with one tablet should cost one range
   row, not 16k shard rows per placement scope.
3. **Heartbeat writes into shared metadata.** Every node upserts
   `tablet_stats` per replica per heartbeat and the scheduler scans them —
   the classic placement-driver heartbeat-pressure shape, pointed at one
   shared SQL store.

**Recommended target — make the two layers real:**

```text
Universe (tiny, global, rarely written)
  cells registry + domain classes
  tenant → cell assignment
  global admin/audit
  cardinality: O(tenants + cells)

Cell (self-contained control plane, blast-radius boundary)
  placement scopes, compressed tablet ranges, tablets,
  replicas, movements, snapshots, stats, scheduler lease
  cardinality: O(tablets in cell) — bounded by cell size

Split (= tablet, as already designed)
  Raft group + grouped-RocksDB domain owning contiguous
  shard ranges; split/merge under the existing guardrails
```

Concretely:

- **Routing becomes two lookups**: tenant→cell at the universe (cacheable,
  changes only on explicit migration), then hash(tenant, routing_key) →
  range → tablet inside the cell. No universe-level per-session state.
- **Demote `session_locators` to a pinned-exception table.** The docs
  already say "if absent, compute from the hash" — make absent the norm,
  keep locators only for sessions explicitly pinned (post-split residency,
  manual overrides), and scope them to the cell. Never `list_all` into a
  snapshot; look up point-wise or per-tenant.
- **Promote compressed `tablet_ranges` to authoritative** and stop
  materializing per-shard rows per scope.
- **Aggregate stats in-cell**: the cell scheduler consumes heartbeats;
  only coarse rollups (cell capacity, health) flow up to the universe.
- **Per-cell metadata partitioning stops being "optional hardening"** and
  becomes the target: one physical Postgres can still host multiple cell
  schemas early, but no query may span cells, so a cell's metadata can later
  move out without changing any caller. A shared-metadata outage today takes
  down every cell; after the split it takes down one.

This also composes with finding 2: a cell-scoped control plane only pays off
once the data plane stops being one RAM-resident state machine per process.

### No placement-service single point of failure

Central placement services teach the failure shape to avoid: when timestamp
allocation and routing sit on the placement leader's critical path, a
placement failover stalls every transaction even though the service itself is
Raft-replicated. Replicating the
metadata store shortens the outage; it does not remove the dependency. The
rule that removes it: **the metadata plane serves placement changes, never
requests** — the steady-state data path must touch zero metadata.

What AgentDB already gets right:

- **No TSO analog.** Sequences are per-session, minted by the owning tablet;
  IDs are UUIDv7, node-local; ordinary operations are single-tablet by design
  (cross-tablet 2PC is an explicit non-goal — keep it that way, it is what
  makes no-SPOF cheap).
- **Tablet leadership is self-contained.** Serving leadership comes from the
  tablet's own raft-rs group, not from a metadata-issued lease; the meta
  scheduler lease (`scheduler.rs`) only gates placement work.
- **Routing is cached and fenced.** Nodes route from epoched snapshots
  refreshed asynchronously; stale-epoch writes are rejected or forwarded at
  the tablet. Stale routing is safe, which is what allows serving on cached
  state while metadata is down.

The one verified placement-shaped dependency today: **session create and fork
synchronously upsert `session_locators` into the shared meta store**
(`routes.rs:5604`, `routes.rs:6082`). In cluster mode a metadata outage
therefore blocks new sessions and forks — the highest-frequency operations
that touch the control plane. The fix is the same as the cardinality fix
above: derive placement from the hash, write locators only as pinned
exceptions (and asynchronously, on movement), never inline on create.

Design rules to lock in:

1. **A written degradation contract.** Universe down: all serving and all
   session traffic unaffected; blocked: tenant creation, cell migration,
   cell admin. Cell metadata down: that cell's placement is frozen (no
   splits, moves, replacements); serving, appends, new sessions, and forks
   continue on cached routing. Tablet leader down: Raft election inside the
   tablet, bounded RTO, zero RPO while quorum survives (already documented).
2. **Leases fail safe, not closed.** If the meta store is unreachable and a
   scheduler or tablet lease cannot renew, the meaning is "no placement
   changes may occur," never "stop serving." This is sound because no
   competing owner can be appointed while the same store is down — the CAS
   that would appoint one lives there.
3. **Persist the last routing snapshot on every node** so a node can cold-
   start and serve with stale-but-fenced routing while metadata is
   unavailable; today a fresh node cannot build its first snapshot without
   the meta store.
4. **Never introduce a global sequencer.** The planned cross-tablet export
   frontier must be computed as a merge of per-tablet commit watermarks, not
   allocated from a central counter — that is the one place in the current
   roadmap where a TSO could sneak back in.
5. **Replicate both metadata layers anyway** (universe: small Raft group or
   HA Postgres; cell: per-cell replicated store) — to shorten placement-
   change outages, not to protect the data path, which rules 1-4 already
   decouple.

### Object storage as the recovery root

Decision: Raft replication owns the acknowledged write; object storage stays
off the performance-critical path — but the bucket must be a full recovery
root. "Given only the bucket (and KMS), rebuild the cell." Today that is
half-true: raft snapshots upload with retention/prune and a restore worker
can bootstrap a node, sealed segments and archives land in the content
backend — but the object-store backend implements no listing, and the
manifests that make objects interpretable live only in the KV/meta store.
Lose the metadata and the bucket is an uninterpretable pile of blobs.

The recovery ladder, with the RPO each rung provides:

| Failure | Recovery source | RPO |
|---|---|---|
| One node | Raft catch-up / peer snapshot (exists) | zero while quorum survives |
| Quorum / cell loss | object storage: snapshots + sealed segments + metadata backup | bounded by upload lag |
| Metadata loss | scavenge: rebuild manifests from self-describing objects | same as above |

What has to change to make the lower rungs real:

1. **Self-describing object layout.** Every object carries its own identity —
   tenant, session, seq range, tablet, epoch, format version, checksum — in
   the object itself, not only in a manifest row, and the key layout is a
   versioned durable format contract (treat it like a file format). Recovery
   must never need the metadata store to interpret a bucket.
2. **Listing plus a per-cell root inventory.** Implement listing in
   `ObjectStoreContentBackend` (currently unsupported — the startup orphan
   sweep is explicitly not available there), and periodically write a
   per-cell root inventory object so scavenge is bounded instead of a full
   bucket scan.
3. **Continuous upload with a measured lag.** Raft-snapshot upload cadence,
   seal frequency, and content-mirror backlog together define the cell-loss
   RPO. Expose `recovery_lag_seconds` per tablet as a first-class metric and
   alert on it; tier the cadence (enterprise gets tighter RPO) per the tier
   matrix above. Control-plane metadata (tenant policies, tablet maps) gets
   periodic backups into the same bucket — or is made fully scavengeable.
4. **GC ordering invariant.** upload → verify checksum → commit manifest →
   only then is local state eligible for GC. The recovery guarantee is a
   corollary: anything deleted locally is provably in the bucket. Never GC
   on upload-attempted; only on upload-verified.
5. **Restore as a tested product workflow.** `agentdbctl restore
   --from-bucket` covering node bootstrap (exists for raft snapshots),
   whole-cell rebuild, and manifest scavenge — exercised by periodic
   automated restore drills. An untested restore path is not a recovery
   root; this belongs in the same harness as the kill-9 crash tests.
6. **KMS is part of the root.** Recovery from the bucket requires the KEKs;
   document that dependency per tier, and note the deliberate exception:
   crypto-shredded tenants are unrecoverable by design.

This also completes the single-node story: one node plus a bucket gives
"cheap to run, durable to S3 with a bounded, measured lag" — the natural
middle rung between laptop dev and a 3-node cell.

### Bounding single-node blast radius

The second blast-radius lesson: with chunks and replicas spread randomly across a
big cluster, every store holds slices of nearly every table, so one node
failure touches almost all tenants (leader re-elections everywhere), and any
simultaneous k-node failure almost surely kills some region outright —
random placement maximizes the number of distinct replica sets the cluster
uses. The blast radius of one node scales with the fleet.

The cell design, taken seriously as a *placement* boundary and not just a
metadata one, already inverts this: a tenant lives in exactly one cell, a
cell is 3-5+ nodes, so "tenants affected by node X" is bounded by the
tenants placed in X's cell — a function of cell size, never fleet size.
AgentDB's Track A same-cell scheduling and dedicated-tablet placement are
the right bones. What to add on top:

1. **Balance leaders, not just replicas, inside the cell.** A node failure
   costs one Raft election for each tablet whose *leader* was there;
   followers only lose redundancy. With leaders spread, one node down in a
   5-node cell means ~1/5 of that cell's tablets blip for an election, and
   nothing outside the cell notices.
2. **Copyset-style replica placement for larger cells.** Constrain which
   node triples may host replica sets instead of drawing them freely; this
   caps how many distinct replica sets a k-node correlated failure can
   intersect, at the cost of slower parallel re-replication. For 3-5 node
   cells this is moot (every triple overlaps); it becomes a real decision
   only if cells grow past ~10 nodes — which is itself an argument for
   keeping cells small and adding cells rather than growing them.
3. **Make "which tenants does node X serve?" a first-class query.** With
   dedicated tablets and small cells the answer is a short, enumerable
   list; expose it via the admin API so an operator (or an automated
   incident page) can name the affected tenants at failure time instead of
   inferring them. Random-spread systems cannot answer this question;
   being able to answer it is the observable proof the blast radius is
   actually bounded.
4. **Document the shared-pool exception honestly.** A shared free-tier
   tablet concentrates many tenants on one replica set by design; a node
   failure in the pool touches all of them at once. That is the tier
   tradeoff — bounded RTO through Raft failover rather than bounded tenant
   count — and it should be stated in the tier's SLO rather than hidden.

### Exploit tenant independence; never dedicate compute

AgentDB's tenants do not interact — no cross-tenant reads, transactions,
searches, or feeds exist anywhere in the API. That independence is the
single strongest scalability and availability lever in the design, stronger
than in general-purpose serverless databases which had to engineer keyspace
isolation into a shared SQL layer. It should be treated as a load-bearing
invariant: any future feature that couples tenants on a data path (global
search, cross-tenant analytics, shared indexes) spends this lever and should
be pushed to export sinks instead.

A serverless-database lesson bounds the design from the other side: giving
each tenant a dedicated compute node fails twice even at low traffic — idle
cost, and control-plane pressure from thousands of registered nodes
heartbeating the placement service. The mapping for AgentDB:

1. **Dedicated means a dedicated storage domain, never dedicated compute.**
   A dedicated tablet is a per-tenant Raft group + LSM domain; many tablets
   multiplex on one node. The grouped-RocksDB layout with
   `tablets_per_domain` / `max_open_domains` is the right substrate. The
   control-plane registrant must remain the node, not the tenant: nothing
   per-tenant may hold a long-lived control-plane session.
2. **The tension to resolve: per-paid-tenant tablets recreate region
   explosion via tenancy.** With one dedicated tablet per paid tenant, Raft
   groups tick, heartbeat, and publish `tablet_stats` rows in proportion to
   *tenant count*, not data size — the same pressure curve, one level down.
   At 10k paid tenants that is 10k ticking Raft groups and 10k stats rows
   per heartbeat interval into the cell store.
3. **Park the idle.** The workload thesis itself says most sessions are idle
   most of the time; the tenant-at-rest target should be: KV rows + sealed
   objects only — zero RAM, zero Raft ticks, zero heartbeat rows, closed
   RocksDB domain. Hibernated/quiesced Raft groups are established practice
   for idle ranges; stats should be report-by-exception (publish
   on change past a threshold, not per tick). This is finding 2's fix
   extended to the cluster layer.
4. **Wake latency is an API contract, not a performance bug.** Agents
   tolerate waits; what they need is the machine-readable shape: the idle
   threshold, the wake p99, and whether requests during wake queue or get
   503 + `Retry-After`. Publishing those three numbers makes aggressive
   parking safe — an agent-native client base is precisely the one that
   permits scale-to-zero everywhere (principle 2 of the agent-native
   assessment above).
5. **Availability composes per tenant.** With no cross-tenant paths and
   parked-by-default tenants, a cell's effective failure domain at any
   instant is only its *active* tenants; upgrades and incidents roll cell
   by cell with no fan-out. This is the payoff of spending nothing on
   cross-tenant capability.

### Tier the hierarchy, not just the tablet

The skeleton already exists (`tenant_policy.tier free|pro|enterprise`,
placement class, encryption class, economy/standard/enterprise domain
classes, capacity classes, the shared→dedicated promotion flow), but it is
scattered across dimensions. The organizing rule that makes it coherent: **a
tier is how much of the hierarchy a tenant shares.** Free shares a tablet;
pro owns tablets inside a shared cell; enterprise owns a cell. Blast radius,
parking policy, metadata cost, and price then fall out of the rung instead
of being independent knobs.

| Dimension | Free | Pro | Enterprise |
|---|---|---|---|
| Placement | shared pool tablet | dedicated tablet(s), shared cell | dedicated tablets in a dedicated cell |
| Cell class | economy (single-AZ, preemptible capacity OK) | standard (multi-AZ) | enterprise (multi-AZ, stable capacity, own rollout window) |
| Blast radius | pool tablet + cell | own tablets; cell-level events shared | own cell — one tenant per failure domain |
| Encryption | pool KEK | tenant KEK | tenant KEK, BYOK/KMS, crypto-shred on delete |
| Parking / wake | aggressive: short idle threshold, cold wake, 503 + `Retry-After` | parked, faster wake target | warm head pinned or wake SLO |
| Metadata cost per tenant | ~0 (rows only; no own tablet, no locators) | O(own tablets) | O(own tablets) + cell rows |
| Replication | RF3 (single-AZ acceptable) | RF3 across AZs | RF3 across AZs, PITR/backup SLO |
| Lifecycle defaults | short warm window, quick cold/archive, derivative-only after N days | standard retention | legal hold, long retention, restore SLO |
| Quota / backpressure | strict hard caps, best-effort noisy-neighbor | purchasable throughput | reserved capacity |
| Export / observability | none or sampled | managed consumers | dedicated consumers + audit attribution |
| Mobility | promotion to pro (existing flow) | cell migration to enterprise | BYOC / dedicated universe option |

Three rules to hold while filling this in:

1. **Never tier correctness.** RPO, append atomicity, idempotency, and
   fork semantics are identical at every tier; what tiers are failure-domain
   size, capacity class, latency, retention, and observability. A free tier
   that can lose acknowledged writes poisons the product's core claim.
2. **The free tier's job at agent scale is zero marginal cost.** Millions
   of machine-created tenants (the Lovable/Bolt shape) are viable only if
   an idle free tenant is KV rows plus sealed objects — no tablet, no Raft
   group, no locator, no heartbeat row. The parking design above is what
   makes the free tier economically possible, not an optimization of it.
3. **The tier contract must be machine-readable.** The consumers choosing
   tiers are agent platforms, not humans on a pricing page: expose each
   tier's wake p99, idle threshold, quotas, retention windows, isolation
   class, and reversibility guarantees through a capability descriptor on
   the API (`GET /api/tiers` or per-tenant metadata), so a platform can
   select and verify a tier programmatically. This is principles 2 and 5 of
   the agent-native assessment applied to the pricing surface.

The one rung the current docs do not state: **enterprise = dedicated cell.**
Today cells hold many tenants and only the tablet is dedicated. Giving the
top tier its own cell completes the story from the preceding sections — it
buys per-tenant blast radius, per-tenant metadata partition, per-tenant
object-store namespace and KMS scope, and a rollout boundary an enterprise
can schedule — and it is cheap to offer because a cell is already the unit
of operational replication.

## Metadata self-hosting: shrink the external DB to a bootstrap root

**Motivation (empirical).** The 2026-08-31 S8 investigation showed
operational metadata leaking onto the data path: replica-health rows were
UPDATEd from the raft message pump (two SQL round trips per message), and
the raft tick worker ran three control-plane queries before every tick — a
slow metadata database froze heartbeats and turned transient DB stalls into
60s commit timeouts. The immediate fixes (tick in an SQL-free task, health
deduped and time-boxed) treat the symptom. The disease is how much lives in
the metadata DB at all, and it is the same disease as the PD lesson this
review already cites: a control-plane store whose cardinality grows with
tenants and whose availability sits under every request.

**The three-layer split.** Classify every table `agentdb-meta` holds today
by what actually needs it:

| Layer | Tables (today) | Property | Where it belongs |
|---|---|---|---|
| **Bootstrap root** | `nodes` (membership/addresses), `cells`, `tablets` + `tablet_ranges` + `shards` + `placement_scopes` (placement/routing), `scheduler_leader`, `config`, `schema_migrations` | Needed *before* AgentDB's own raft groups can form or be found; O(nodes + cells + tablets), low write rate | Stays in SQLite/Postgres forever. This is the circularity boundary — a tablet cannot discover its own peers from data stored inside a tablet that is not up yet. Analog: Spanner's membership layer, FoundationDB's coordinators. |
| **Ephemeral operational state** | `tablet_replicas` (health, applied/match indexes), `tablet_stats` (+ write rate, compaction debt), the heartbeat columns of `nodes` | Observed, high-write, reconstructible from the live system in seconds; durability adds nothing but pool pressure | Out of SQL entirely: in-memory on the observing node, exposed through the admin API (which mostly exists), aggregated by the scheduler from live reports. Losing it on restart is *correct* — stale health is worse than no health. |
| **Tenant-scale metadata** | `tenants`, `tenant_policies`, `tenant_quotas`, `session_locators`, `session_overrides`, `admin_tokens`, `admin_audit` (`users` is dead SCRAM residue — delete, don't migrate) | Cardinality grows with tenants and sessions — `session_locators` is O(sessions), the single largest control-plane table | Into AgentDB itself: a **system tenant** on a **system tablet**, stored as records/events. The external DB stays O(nodes + cells + tablets); tenant growth stops pressuring it. |

**Why self-hosting is the right shape here** (not just dogfooding vanity):

- The primitives already exist. The change-notifier becomes AgentDB
  **watch**; metadata durability and DR ride the existing raft +
  recovery-root manifests instead of a separate Postgres backup story;
  per-cell metadata partitioning falls out of the cell design (the system
  tablet is just a tablet in the cell).
- It completes the no-SPOF rule this review set: "metadata never on the
  data path" is enforced structurally when tenant metadata IS data-path
  data, cached and replicated like everything else, and the external DB is
  consulted only at bootstrap/topology-change time.
- It is the two-level design the cited systems converged on: a tiny
  static root (coordinators/membership) below self-hosted metadata
  (Spanner directories, CockroachDB system ranges).

**Costs, stated honestly:**

- **Layering discipline becomes load-bearing.** The system tablet must be
  placeable, recoverable, and elect a leader using ONLY the bootstrap
  root. Every code path that runs before tablets are up must be audited
  for reads of migrated tables. One violation reintroduces the circular
  dependency as a rare, terrible outage.
- **Recovery drills need a metadata variant.** S16 proves data-tablet
  restore from the recovery root; a system-tablet variant must prove the
  metadata tablet restores first and alone, then data tablets restore
  using it.
- **SQL inspection is lost for migrated data.** Mitigation: admin
  endpoints (most exist) plus `agentdbctl` read paths for the system
  tenant.
- **Auth bootstrap ordering.** Admin/tenant tokens moving into the system
  tenant means auth verification depends on the system tablet being up;
  the static installation token (which lives in config, not the DB) must
  remain sufficient to operate a cell whose system tablet is down.

**Staged path** (each stage independently shippable and reversible):

1. **Evict ephemeral state from SQL** — replica health, applied/match
   indexes, tablet stats become in-memory/leader-observed, surfaced via
   the admin API. Biggest immediate win; needs no self-hosting machinery.
   Deletes the highest-write tables outright.
2. **System tenant for tenant-scale metadata** — tenants, policies,
   quotas, locators, tokens move to records under a reserved system
   tenant on a dedicated system tablet; change-notifier subscriptions
   become watch; S16 gains the metadata-restore drill.
3. **Shrink `agentdb-meta` to the bootstrap root** — what remains is
   membership, cells, placement, and the scheduler lease. Single-node
   mode barely changes: the root is already a local SQLite file.

The implementation-grade design (data model, bootstrap ordering, migration
tool, recovery drill, test matrix) is in
[`docs/design/metadata-self-hosting.md`](design/metadata-self-hosting.md).

Design decision still open before stage 2: whether the system tenant's
rows are AgentDB *records* (search/watch for free, but the record schema
becomes a public-ish contract) or a dedicated key range on the system
tablet with purpose-built codecs (tighter, but re-implements watch). Lean
records until a concrete blocker appears — subtraction over invention.

## Simplification pass: cut, quarantine, keep

Anti-over-design rule set: keep what serves the hot loop and the durability
story; delete every *second way* to do the same thing; quarantine evaluation
machinery behind its seams; defer features the product thesis itself lists as
open questions. One protocol surface, one KV backend, one auth model.

**Cut now** (delete from the tree; git remembers):

| What | Why | Cost of keeping |
|---|---|---|
| `agentdb-wire` MongoDB listener (~1.9k LoC + server wiring) | Second protocol surface; bypasses auth/routing/validation (finding 3); contradicts the thesis's own "not a compatibility facade" line. The DynamoDB-style index behavior is semantics, not a facade — it stays. If Mongo compat ever matters for adoption, re-ship it as a separate optional proxy binary outside the trust boundary. | A parallel API to keep correct + the worst enforced-boundary hole |
| `SlateKvStore` backend (`slate.rs`, ~540 LoC + contract tests) | Docs already say "evaluation backend, not the production answer." Its job — proving the `KvStore` abstraction — is done; the lesson is banked. | A whole backend in every KV contract-test matrix |
| Legacy `RaftQuorumReplicatedLog` | Superseded by the raft-rs/raft-engine path. | Two quorum implementations to reason about |
| Dead SCRAM code (`agentdb-meta/scram.rs`, `users.rs`) | Called by nothing; implies a security posture that does not exist. | Misleading surface |
| ClickStack OTel views + Parquet-via-DuckDB connectors | The durable export ledger + managed consumers are core; JSONL (universal) + ClickHouse (one blessed sink) cover the contract. OTel projections and Parquet conversion are downstream tooling — externalize to a separate repo or defer. | Four sink dialects maturing in-tree |
| Mongo listener credentials, plus one of the two external auth mechanisms | Collapse to a single token model with scopes (static install token OR metadata-backed tokens, not both), separate internal Raft token stays. | Three credential systems for one server |

**Quarantine** (keep the seams, freeze the surface):

- Tablet lifecycle *automation* — the `AGENTDB_TABLET_LIFECYCLE_*` /
  movement-scheduler env-var family, auto split/merge policy passes,
  `agentdbctl` cluster commands. The seams (`ReplicatedLog`,
  `TabletKvStore`, `LoggedTabletStateMachine`) are the right boundaries and
  stay; the automation on top keeps growing while the data plane it manages
  is still one RAM-resident state machine (finding 2). Freeze until finding
  2 lands; the Universe→Cell→Split work above will reshape half of it
  anyway (locators, stats, per-shard rows).
- `tools/agentdb-cluster-smoke` — keep runnable, stop expanding.
- `.worktrees/pg` divergent checkout — remove from the working tree.

**Keep** (the moat — do not thin these):

- Session primitives: append-and-fetch, idempotency, pointer fork +
  fork-aware replay, watch/export ledger, warm/cold/archive lifecycle.
- Local/global record-index semantics with commit-seq visibility repair.
- RocksDB `KvStore` + LocalFS/S3 `ContentBackend` + in-memory dev mode.
- Python/TypeScript SDKs, `LLMS.md`/`AGENTS.md`, embedded dashboard
  (cheap, already built), framework adapters as proofs.

**Decide deliberately** (open product questions — pick, don't drift):

- HNSW vector index: the thesis says "not a vector database" and leaves
  built-in retrieval scope open. Either commit to scoped vector retrieval
  as a product feature or drop to metadata/tag/text filtering and integrate
  outward. The rebuild-on-hydrate half of this is resolved: indexes persist
  as watermark-keyed snapshots with delta replay (action 2), and HNSW
  graphs rebuild lazily from snapshotted entries on first query. What
  remains is the product decision, not the engineering debt.
- Record edges: graph traversal is adjacent to the memory-platform scope
  the thesis excludes. Keep only if a real adapter uses it.

Net effect: one protocol (native HTTP), one persistent backend (RocksDB),
one quorum path (raft-rs), one auth model, two export sinks — and the
engineering attention freed lands on findings 1 and 2, which is the point.

### The familiarity counterargument (wire-compat-as-prior)

The strongest case for wire compatibility in agent-native infra: platform
engineers report that with a MySQL-compatible backend they can tell an LLM
"it's MySQL" and the model already knows how to use it — familiarity from
training priors, zero docs needed. This is the flip side of the P8 evidence
(agents act from priors, not docs) and deserves a real answer, not a
reflexive defense of the cut.

The answer: **familiarity multiplies capability only at full fidelity.**
Full MySQL compatibility is deep enough that the model's entire MySQL prior
is true. AgentDB's Mongo listener was deliberately narrow (fixed collections, a
small command subset), so "tell it it's MongoDB" activates the full Mongo
prior while honoring a fraction of it — a *prior trap*: the agent reaches
for arbitrary collections, updates, aggregation, indexes, and hits
unimplemented commands. Partial compat is worse for agents than a small
honest native surface, and the moat operations (append-and-fetch, fork,
replay, watch) have no training prior under any wire protocol — they are
extension commands either way.

Where the familiarity lesson does land for AgentDB:

1. **Adapters are the true compat layer.** The consumer is the agent
   harness, not the generated app, and the interfaces LLMs already know for
   that slot are the OpenAI messages array, the LangGraph checkpointer, and
   MCP tools. Invest familiarity budget there — plus a first-party MCP
   server ("here's an MCP server" is the 2026 equivalent of "it's MySQL")
   and boring, conventional HTTP+JSON idioms in the native API.
2. **If any wire protocol earns a place, it is SQL on the read path.** SQL
   is the deepest prior in any training set, and the ClickHouse export
   already exists: "your session history is queryable as SQL over there"
   gives full-fidelity familiarity for analytics and inspection without
   touching the write path or the trust boundary.
3. **Test it instead of arguing it.** The P8 eval suite can run identical
   tasks through the Mongo listener, the native SDK + `LLMS.md`, and an MCP
   server, and score completion. Whether narrow compat helps or hurts
   agents is measurable for a few dollars before paying for protocol
   maintenance.

Principle to record: *familiarity over novelty, but only at full fidelity —
partial compatibility is a prior trap.*

## Chaos-testing findings (2026-08-30)

The S14-S16 crash/chaos harnesses (`ops/local-ci/tests/auto/`, wired into
the nightly/release runner; scenario records in
`testing-plans/agentdb-project-wide-distributed-tests.md`) found two
pre-existing silent data-loss bugs within their first runs — both on the
tiered-storage path, both invisible to the prior test suite because restart
tests paired each restart with a fresh in-memory content backend:

1. **Startup orphan sweep destroyed tiered history.** Its referenced-set
   held only artifact ids, so every persistent restart deleted all sealed
   segments, archive bundles, and replay prefixes as "orphans". Fixed:
   session-referenced content ids (including archive sources, kept
   conservatively) are now part of the referenced set; pinned by a
   restart regression test.
2. **Merged read dropped post-archive segments.** Archive manifest and
   sealed segments were treated as mutually exclusive in `fetch_from_seq`,
   though the lifecycle seals reopened warm heads on top of an archive.
   Post-archive segments were unreadable, and the next archive compaction
   (which reads through the same path) wrote a bundle without them and
   deleted their blobs — permanent loss on the healthy path, no crash
   required. Fixed: archive, segments, and warm head merge; pinned by a
   lifecycle regression test.

The harnesses also caught a design error in this branch's own cache sweep
(reusing the destructive `list_objects` tmp-cleanup as a periodic scan,
which could destroy in-flight uploads) — now split into a read-only listing
plus an explicit startup-only `sweep_temporary_objects`. Property tests
(proptest) cover the recovery-manifest format; a concurrent flaky-mirror
chaos test covers cache eviction invariants under fault.

## Prioritized actions

| # | Action | Size | Pays off |
|---|---|---|---|
| 1 | fsync-on default + group commit + kill-9 crash harness | S–M | Durability promise becomes true. **Implemented on this branch** (power-loss rig remains) |
| 2 | KV-authoritative state, sharded in-memory cache, incremental apply | L | Removes the scale wall; unblocks tablet step 5. **Stages 1-2 largely implemented** (`dev/kv-authoritative-reads`). Stage 1: metadata maps are a cache with KV point-read fallback at every public funnel, eviction-chaos proven. Stage 2: enumerations (lists, watch scopes, lifecycle/GC sweeps, policy worker, fleet summary, orphan reconcile) read the authoritative KV scans with cache overlay; quota/backpressure counts come from drift-free per-tenant aggregates (exact recompute for admission, 2s-TTL cache for the advisory hot path, invalidated on mutation); and `AGENTDB_STARTUP_HYDRATION=lazy` skips hydrating the tenant/agent/session/artifact/alias maps entirely — validated by running the complete core+api suite under lazy mode (green, and wired into CI as a parity stage). The warm cache now has an eviction policy too: cap-triggered whole-map clears (lazy mode only; caps default to the historical hydration caps), swept on the worker tick and chaos-proven end-to-end by S17 (empty-cache startup + 200ms sweeps + SIGKILL: every acked append survives with payload audit). Records are lazy too: rows are not resident in lazy mode (search materializes hits through the fallback, external-id dedupe works cold, GC pre-warms from the authoritative scan). And the startup index rebuild is gone: the text/vector/global indexes, doc-id maps, and external-id map persist as a snapshot blob keyed by the record-index watermark (written by the index worker after ≥`AGENTDB_RECORD_INDEX_SNAPSHOT_MIN_ADVANCE` commits, default 256; `AGENTDB_RECORD_INDEX_SNAPSHOT=0` is the kill switch); startup restores the snapshot and delta-replays only the records whose commit seq moved past it — updates, deletes, and GC purges included — falling back to the full scan-and-retokenize rebuild on any decode failure. HNSW graphs rebuild lazily from the snapshotted entries on first query. Chaos-proven by S18 (snapshot every worker tick + SIGKILL mid-churn: ~2,200 acked record ops per cycle restore correctly; found and fixed a missing tenant fallback in the API search funnel on its first run). Stage 3 is in too: committed Raft applies no longer trigger a full O(everything) rehydrate — the incremental path (default; `AGENTDB_REPLICATED_APPLY=full` is the kill switch) evicts the large cached sets (funnels reload them through the Stage 1 fallbacks), re-scans the small config sets that have no per-row fallback (skills, bindings, export consumers, record edges, tenant meta), and lets the search indexes converge through the replicated outbox — which is now trimmed once both watermarks pass a mutation, so each drain is proportional to new mutations, not history. Building this exposed a pre-existing replication bug (confirmed against the legacy full-rehydrate path too): the record-family funnels never called `refresh_replicated_state`, so a freshly promoted leader minted record commit sequences from 1, colliding with replicated history; every record/skill/tenant-meta funnel now refreshes at entry, and a failover regression test pins commit-seq monotonicity + cross-failover external-id dedupe. Re-running the S8 real-cluster smoke for this work also caught a regression the durable-by-default slice had shipped unnoticed (cluster smokes had not run since): routing every write through the group-commit batcher's single writer thread let one slow raft commit head-of-line-block every tablet's writes for the full 60s append timeout — bisected to the #288 merge on a clean 28-core box. Fixed by disabling the batcher on replicated runtimes at construction: group commit exists to amortize the RocksDB WAL fsync, and replicated tablets get durability from the raft log, so the batcher bought nothing there and cost the node its parallelism. The tick/outbox SQL gating is fixed too: the raw raft tick now runs in its own task that never touches the metadata database (control-plane SQL only feeds the separate failover-evaluation loop), the per-send-tick peer-URL SQL refresh is gone (the dedicated change-notified refresher owns it), and replica-health reporting moved out of the message send loop — deduped per peer per tick and time-boxed to 2s, ending the two-SQL-round-trips-per-raft-message storm that let a slow metadata DB stall the message pump. S8 retries halved; what remains (a handful of single-retry slow first commits per smoke, absorbed exactly as pre-#288) stays open as a small follow-up. Skills are lazy too now: per-row fallbacks for skills, uniqueness rows, and agent bindings; enumeration and cascade paths warm from tenant-scoped KV scans; skill quota admission counts authoritative rows; the maps join the eviction sweep (`AGENTDB_METADATA_CACHE_MAX_SKILLS`) and the Stage 3 apply-invalidation evicts them instead of re-scanning. That closes the lazy story for every large set — remaining: relaxing the replicated startup (still full-hydrate, snapshots skipped) now that apply is incremental |
| 3 | Watch backed by export-ledger cursors | S | Hot path no longer pays for observers. **Largely implemented on this branch**: watch is frontier-incremental — a poll fetches only events past each session's cursor position, bounded by the page size, instead of every event of every in-scope session; full ledger-backed live push remains the finding-4 end state |
| 4 | Per-tenant token scoping; wire-listener hardening | M | Isolation claims become enforceable. **Token scoping implemented on this branch**: metadata-backed tokens gained a `tenant` scope bound to exactly one existing tenant, enforced by the API middleware on every data route (anything outside `/api/tenants/{bound_tenant}/...` returns 403); `admin` scope now covers the data plane, `readonly` is GET/HEAD anywhere, and the static installation token stays root. `GET /api/me` reports the caller's scope + bound tenant so an SDK can discover its workspace from the credential alone. Verification is memoized in-process (`AGENTDB_AUTH_CACHE_TTL_MS`, default 60s, invalidated on mint/revoke) to keep SQL off the request hot path, and the open-until-first-mint bootstrap now closes the data plane too. Chaos-tested: revocation racing live traffic never grants after the revoke ack and never surfaces 5xx. Wire-listener hardening is moot — the listener was deleted in the simplification pass |
| 5 | Idempotency retention, fork write-order, raft scan pagination | S | Closes known distributed-storage failure shapes. **Implemented on this branch**: idempotency TTL+cap eviction with durable-row cleanup, fork is durable-first with alias reservation, raft replay has a fail-closed byte budget |
| 6 | 429 + Retry-After; wire artifact Idempotency-Key | S | Stock agent retry loops work unmodified. **Implemented on this branch**: active-session quota exhaustion returns 429, all 429/503 responses carry Retry-After, artifact create is idempotent under Idempotency-Key |
| 7 | Agent-experience eval suite (daily regression) | M | LLMS.md stops being an untested hypothesis. **Implemented**: `ops/evals/agent-experience-eval.sh` (nightly/release runner stage) executes every actionable LLMS.md claim against a real dev server — health, the flat error envelope on 404/malformed-body/unknown-route (a new response-mapping middleware now wraps axum's plain-text rejections in the JSON envelope), quota 429 + Retry-After, the full agent loop (append-and-fetch with first-append semantics, fork with precise inheritance semantics, records write→search with the read-your-writes commit token, context packs), and the Python SDK quickstart end-to-end in a fresh venv. Its first runs caught six real divergences, all fixed: plain-text 4xxs, a documented error shape (`{"error":{code,kind,message}}`) that never existed, a phantom `GET /openapi.json` claim, `/api/me` (added in the token-scoping slice) breaking the Python SDK's workspace resolution (server now enriches tenant principals with workspace info; SDK falls back to bootstrap for unbound principals), fork-inheritance wording that overpromised on the hot path, and the undocumented eventual consistency of global record search (now documented with the `X-AgentDB-Record-Commit-Seq` → `min_visible_record_commit_seq` read-your-writes pattern) |
| 8 | Universe/Cell metadata split; locators as pinned exceptions; compressed ranges authoritative | M–L | Metadata cardinality bounded per cell; blast radius contained |
| 9 | Tenant-at-rest = zero marginal runtime cost: parked tablets, hibernating Raft, exception-only stats, documented wake contract | M–L | Dedicated isolation without per-tenant compute or control-plane pressure |
| 10 | Tier matrix as one contract (share-a-tablet / own-tablets / own-a-cell) with a machine-readable tier descriptor | M | Tiers become selectable and verifiable by agent platforms; enterprise gets per-tenant blast radius |
| 11 | Subtraction pass: remove wire listener, SlateDB backend, legacy quorum log, dead auth code; externalize ClickStack/Parquet; freeze tablet automation | M | One protocol, one backend, one quorum path; closes the worst P7 hole; attention lands on findings 1-2 |
| 12 | S3 as recovery root: self-describing objects, listing + cell inventory, measured recovery lag, GC-after-verify, tested restore-from-bucket | M–L | Cell loss and metadata loss become bounded-RPO recoveries instead of disasters. **Partially implemented on this branch**: versioned self-describing recovery manifests with a periodic backup worker, object-store listing, `GET /api/admin/recovery` with `recovery_lag_ms`, `agentdb restore-from-bucket`, and the managed local content cache in mirror mode (TTL eviction after verified upload, touch-on-read, never evicts the only copy — cold content lives only in the object store; see `docs/operations/recovery.md`). Remaining: per-cell inventory, manifest-less scavenge, automated restore drills. |
| 13 | Metadata self-hosting: evict ephemeral state from SQL, system tenant for tenant-scale metadata, shrink agentdb-meta to a bootstrap root | L | **All three stages implemented** (see `docs/design/metadata-self-hosting.md` implementation records). Stage 1: `tablet_replicas` reduced to placement intent, observed state rides per-node `node_operational` summaries, every raft path SQL-free (send, receive, apply, snapshot, movement), merged-view semantics pinned. Stages 2-3: bearer tokens, session locators, and the tenant registry live as records under a reserved system tenant (hidden from and unreachable through the public API; token verify is one point-read keyed on the hash); the dead `session_overrides`/`tenant_quotas`/`admin_audit`/`users` tables are deleted; `tenant_placement_policies` reclassified Layer 1 (scheduler directory); migration 028 drops all Layer-3 tables. `agentdb-meta` is now the bootstrap root: nodes, cells, tablets/ranges/shards/scopes, scheduler lease, config, node summaries. S16 gained the S16-M metadata-restore verification (a pre-loss token must authenticate after a bucket-only restore) and S8 gained a fleet-wide SQL statement budget as the standing tripwire for the hot-loop-SQL defect class |
