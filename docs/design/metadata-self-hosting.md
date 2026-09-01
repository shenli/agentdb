# Metadata Self-Hosting: Detailed Design

Status: **proposed** (design-review action #13). Companion to the
"Metadata self-hosting" section of
[`design-review-2026-08.md`](../design-review-2026-08.md), which carries the
motivation and the three-layer argument; this document is the
implementation-grade design.

Goal: shrink the external metadata database (`agentdb-meta`,
SQLite/Postgres) to a **bootstrap root** whose size is O(nodes + cells +
tablets) and whose availability is never on the data path, by (1) deleting
ephemeral operational state from SQL, and (2) moving tenant-scale metadata
into AgentDB itself under a reserved **system tenant** on a dedicated
**system tablet**.

---

## 1. Where we are

`agentdb-meta` today holds 21 tables spanning three very different kinds of
state (full disposition table in Appendix A). The server binary alone has
~300 `pool()` call sites. Three structural problems:

1. **Cardinality.** `session_locators` grows O(sessions), `tenants` /
   `tenant_policies` / `tenant_quotas` / `admin_tokens` grow O(tenants).
   The control-plane store scales with product success — the PD failure
   mode the design review documents.
2. **Hot-path adjacency.** The S8 investigation (2026-08-31) found replica
   health UPDATEd from the raft message pump and the raft tick gated on
   three control-plane SELECTs. The worker-level fixes landed (#291), but
   the architecture still invites the next leak: any state in SQL is one
   `await` away from a hot loop.
3. **Split durability story.** Data rides raft + recovery-root manifests;
   metadata needs separate Postgres backup/restore. Two DR stories, one of
   them untested by our drills.

## 2. Target architecture

```
┌─────────────────────────────────────────────────────────┐
│ Layer 3: tenant-scale metadata (moves INTO AgentDB)     │
│   tenants, policies, quotas, session locators/overrides,│
│   admin/tenant tokens, audit log                        │
│   → records under the SYSTEM TENANT on the SYSTEM TABLET│
├─────────────────────────────────────────────────────────┤
│ Layer 2: ephemeral operational state (leaves SQL)       │
│   replica health, applied/match indexes, tablet stats,  │
│   heartbeat freshness                                   │
│   → in-memory, node-reported, scheduler-aggregated      │
├─────────────────────────────────────────────────────────┤
│ Layer 1: bootstrap root (stays in SQLite/Postgres)      │
│   nodes, cells, tablets, tablet_ranges, shards,         │
│   placement_scopes, scheduler_leader, config            │
└─────────────────────────────────────────────────────────┘
```

Invariant that makes the layering sound (the **circularity boundary**):

> Everything required to place, elect, and recover the system tablet lives
> in Layer 1. Everything in Layer 3 may be read only after some tablet is
> serving. Layer 2 is reconstructible from live processes within one
> heartbeat period and is never durable.

Single-node deployments keep their shape: Layer 1 is the same local SQLite
file it is today; Layer 3 lives in the same RocksDB the data already uses
(the system tablet is `RUNTIME_PRIMARY_TABLET_ID`'s sibling, or in
non-replicated mode simply a reserved key namespace in the one store).

---

## 3. Stage 1 — evict ephemeral state from SQL

> **Implementation refinements** (2026-08-31, discovered during the build;
> they supersede details below where they differ):
>
> 1. **`tablet_replicas` was two things in one table.** It held placement
>    INTENT (tablet → node → role, written on placement decisions — Layer 1
>    by this design's own definition) *and* observed progress/health. The
>    implementation keeps a lean intent table (`tablet_id, node_id, role,
>    state('active'|'draining'), updated_at_ms`) and evicts only the
>    observed columns. Decision code that needs both reads a **merged
>    view** (intent joined with fresh observations; intent authoritative
>    for role and the draining lifecycle, observations for health and
>    progress, health defaulting to `unknown`).
> 2. **The node heartbeat IS a SQL row, so the summary is too.** There is
>    no node→scheduler RPC to piggyback on: the heartbeat is each node
>    upserting its own `nodes` row. The per-node summary therefore lands in
>    a `node_operational` row (one JSON summary per node, replaced at
>    heartbeat cadence) rather than an in-memory fleet view on the
>    scheduler. This still achieves the goals: write rate O(nodes) per
>    heartbeat interval instead of O(tablets × messages), zero SQL on raft
>    send/receive/apply paths, and staleness handled by `reported_at_ms`
>    cutoffs exactly like node liveness. When stage 2's system tenant
>    lands, this row is a natural early migrant.
> 3. **The message-receive path had its own SQL** (progress + health
>    UPDATEs and a peer-validation SELECT per received raft message —
>    the mirror of the send-side issue #291 fixed). Receive-side writes now
>    go to the in-process registry; peer validation runs against a 5s
>    in-process cache of the intent-derived peer set with a
>    refresh-once-before-reject rule.
> 4. **raft-id → node-id resolution is memoized** in the registry (stable
>    per node), so outbox health reporting does at most one SQL lookup per
>    unknown peer, ever — not per message or per batch.

### 3.1 What is deleted

Tables `tablet_replicas`, `tablet_stats` (and its write-rate/compaction
columns), migration channel `tablet_replica_notify`, and the
`last_heartbeat_ms` write traffic against `nodes` (the column stays for
Layer-1 liveness, but see §3.4 for its new writer).

### 3.2 What replaces them

**Per-node `OperationalState` registry** (in-memory, in the server
process):

```rust
struct OperationalState {
    // What THIS node observes about the raft groups it participates in.
    replicas: RwLock<BTreeMap<(TabletId, RaftNodeId), ReplicaObservation>>,
    // What THIS node measures about tablets it leads.
    tablet_stats: RwLock<BTreeMap<TabletId, TabletStatsObservation>>,
}

struct ReplicaObservation {
    role: RaftRole,
    health: ReplicaHealth,      // healthy | unreachable (outbox-observed)
    applied_index: u64,
    match_index: u64,
    observed_at_ms: i64,
}
```

Writers: the raft outbox (send success/failure → health), the raft groups
themselves (roles/indexes read directly from `RaftRsSharedGroup::status()`
— no copy needed for the local view), and the stats sampler that feeds
compaction/write-rate today.

**Transport: piggyback on the node heartbeat.** Nodes already heartbeat
the control plane. The heartbeat request body gains the node's
`OperationalState` summary (bounded: per-tablet, not per-key). The
scheduler-leader keeps an in-memory `FleetOperationalView` aggregated from
the last heartbeat per node. No new RPC, no gossip protocol, no SQL.

Consumers and their migrations:

| Consumer | Today | After |
|---|---|---|
| Failover candidate selection (tick worker task B) | `tablet_replicas` SELECT (applied_index, health) | Local raft `status()` for own groups + the peer observations that arrive on raft messages; falls back to "campaign if leader silent past election timeout" — which is what raft itself guarantees anyway |
| Scheduler / rebalancer (`tablet_rebalance.rs`) | `tablet_stats` SELECT | `FleetOperationalView` on the scheduler leader; a freshly elected scheduler waits one heartbeat period before making movement decisions |
| Admin API (`/api/admin/tablet-stats`, cluster health) | SQL | Node-local registry for this node + scheduler's fleet view for fleet-wide endpoints (proxy to scheduler leader, which the admin API already knows how to find) |
| Outbox health reporting | 2 SQL round trips per message (deduped/time-boxed in #291) | one in-memory map insert |

### 3.3 Semantics changes (accepted, deliberate)

- Operational state is **lost on process restart** and rebuilt within one
  heartbeat period. This is correct: post-restart, pre-rebuild stale rows
  are what we have today, and they are worse than a brief "unknown".
- Movement decisions stall for up to one heartbeat period after scheduler
  failover. Bounded and harmless: movements are minutes-scale operations.
- `applied_index` visibility for humans moves from SQL to
  `GET /api/admin/tablets` (already exists) — no operational capability is
  lost, only the SQL veneer.

### 3.4 Node liveness

`nodes.last_heartbeat_ms` stays in Layer 1 (liveness feeds failover and is
needed when the reporting node is *down*, so it cannot be node-local). But
its write rate drops to the heartbeat interval per node — O(nodes) rows,
low frequency — and it is the **only** periodic control-plane write left.

### 3.5 Stage-1 tests

- Unit: candidate selection from raft status + message-carried
  observations (the failover matrix from `runtime_quorum.rs` re-pointed at
  the new source).
- S8/S9 must pass unchanged (they observe behavior, not tables).
- New assertion in S8: **zero** SQL statements from the outbox/tick paths
  during steady state (greppable via sqlx statement logging in the
  harness).

---

## 4. Stage 2 — the system tenant

> **Implementation record** (2026-09-01; scope decisions that supersede
> details below):
>
> 1. **All three stages landed in one change**, so the `MetadataStore`
>    trait + dual-implementation migration window was unnecessary: there is
>    a single implementation (`agentdb_api::sysmeta`, free functions over
>    the in-process runtime handle), and no `SqlMetadataStore` ever
>    shipped. No data migration either — pre-users, migration 028 simply
>    drops the old tables.
> 2. **The system tenant rides the primary tablet** rather than a dedicated
>    `SYSTEM_TABLET_ID = 0`. All load-bearing properties hold (raft
>    replication, recovery manifests, KV-authoritative caching); the
>    dedicated-tablet placement rules (pin to durable nodes, move last) are
>    cluster-scale polish deferred until the scheduler grows placement
>    classes.
> 3. **The consumer inventory shrank stage 2 dramatically.**
>    `session_overrides`, `tenant_quotas`, `admin_audit`, and `users` had
>    ZERO consumers — deleted outright, not migrated. `tenant_placement
>    policies` is read by the scheduler for placement decisions, which must
>    work while the system tablet is down — reclassified **Layer 1** by
>    this design's own circularity rule. The migrated surface is exactly:
>    bearer tokens, session locators, and the cluster tenant registry.
> 4. **Token records key on the hash**: `external_id = sha256(plaintext)`,
>    so verification is one point lookup through the record external-id
>    map (in-memory hit, KV fallback) — no scan, no SQL.
> 5. **Bootstrap snapshot starts locator-less**: routing's first snapshot
>    loads before the runtime exists and uses the documented tenant-hash
>    fallback until the first refresh (locators are re-read from records
>    every refresh interval; the SQL LISTEN/NOTIFY locator channel is
>    gone — watch-based invalidation is the noted follow-up).
> 6. The locator `tenant_internal_id` field is retired (always 0);
>    `tenant_external_id` is the identity, as migration 012 intended.
> 7. **The S8 statement budget caught real defects on its first run**
>    (1.26M statements vs a ~150k healthy baseline): heartbeat/lease
>    touches fired the LISTEN/NOTIFY triggers on every row touch, so every
>    refresher on every node re-pulled the full routing set per heartbeat;
>    and failover evaluation polled three full-table loads at raft-tick
>    cadence. Fixed by migration 029 (triggers ignore liveness-only
>    updates), notify debouncing in the three refresher loops, and a
>    second-scale failover-evaluation cadence. The budget is calibrated
>    ~2.5x above the healthy baseline for the smoke's 500ms refresh
>    config.
> 8. **Removing the storm exposed a latent convergence latch** it had been
>    masking: raft message sends had no HTTP timeout (one stalled peer
>    head-of-line blocked every node's sequential outbox flush), and the
>    replicated append's commit wait blocked tokio workers outright
>    (starving the accept loop and ingress that commit depends on). Fixed
>    with bounded per-request send timeouts (3s messages / 120s
>    transfers / 2s connect) and `block_in_place` around the commit
>    wait — both items from `distributed-safety-hardening.md` landed
>    early. Outbox send failures now warn once per flush (they logged
>    only at debug, which made a fleet-wide delivery outage invisible).

### 4.1 Identity and placement

- Reserved tenant id: `ten_00000000-0000-0000-0000-000000000001`
  (constant `SYSTEM_TENANT_ID`), reserved external key `__agentdb_system`.
  Rejected on the public API: creating/deleting/writing it via
  `/api/tenants/...` returns `POLICY_DENIED`; only the in-process system
  client may write it.
- Reserved tablet: `SYSTEM_TABLET_ID = 0` (data tablets start at 1 today).
  Layer 1's `tablets` row for tablet 0 is created at cell bootstrap by the
  first node, exactly like the primary tablet today.
- Placement rules: the scheduler pins the system tablet to the cell's
  three most durable nodes, never co-schedules its replicas onto nodes
  marked draining, and moves it **last** during rebalances. It is a
  normal raft group otherwise — same log, same snapshots, same recovery
  manifests.

### 4.2 Data model: composite records, one per entity

Each migrated table maps to a **record namespace** under the system
tenant, one record per natural key, with the row content as the record's
`metadata` JSON. Multi-table-per-entity state collapses into one composite
record so that every metadata mutation is a **single-record commit** — no
cross-record transactions needed:

| Today (SQL) | System-tenant record |
|---|---|
| `tenants` + `tenant_policies` + `tenant_quotas` row set | namespace `["sys","tenant"]`, `external_id = tenant_id`, metadata `{external_key, policy, quota, placement, created_at_ms, deleted_at_ms}` |
| `session_locators` (+ `session_overrides`) | namespace `["sys","locator"]`, `external_id = session_id`, metadata `{tenant_id, tablet_id, epoch, override?}` |
| `admin_tokens` | namespace `["sys","token"]`, `external_id = token_id`, metadata `{hash_b64, scope, tenant_id?, created_at_ms, revoked_at_ms}` |
| `admin_audit` | **events**, not records: an append-only system session per month (`sys-audit-2026-09`), because audit is a log, not a keyed row set |

Why records and not a bespoke key range: watch, search, idempotent
upsert-by-external-id, per-record commit tokens, and the KV-authoritative
cache/fallback machinery all exist and are chaos-tested (S17/S18). A
bespoke range would re-implement watch and snapshots for one consumer.
The record schema for `["sys",*]` namespaces is marked **internal** in
docs; it is not part of the public API contract even though it is
technically reachable by an operator with the static token.

### 4.3 Read path

`agentdb-meta`'s public module API (`tenants::get`, `session_locators::…`,
`admin_tokens::verify`, …) is the seam. Extract the tenant-scale modules
behind a `MetadataStore` trait with two implementations:

- `SqlMetadataStore` — today's code, kept during migration and for the
  Layer-1 tables permanently.
- `SystemTenantStore` — reads records through an **in-process runtime
  handle** (same process, no HTTP hop) with `min_visible_record_commit_seq`
  read-your-writes on the caller's own mutations.

Caching does not change: every consumer that caches meta rows today
(routing, auth token cache, policy resolution) keeps its cache; only the
miss path changes from SQL to a record point-read — which is a tablet-local
KV get on nodes hosting a system-tablet replica, and one internal RPC to
the system tablet's leader otherwise. Routing reads (`session_locators`)
deserve a **system-tablet follower replica on every node** in small cells
(≤ ~5 nodes) so locator misses stay node-local; large cells accept the RPC
or widen the locator cache TTL.

### 4.4 Write path

All writes go through the system tablet's raft leader via the internal
client (`upsert_record_with_commit` under `SYSTEM_TENANT_ID`). Properties
we get for free: linearizable per-record, idempotent by external id,
commit tokens for read-your-writes. Properties we must NOT need:
cross-record transactions — enforced by the composite-record data model
(§4.2). The one current multi-row transaction (tenant + alias uniqueness)
maps to the existing record external-id uniqueness machinery.

### 4.5 Watch replaces the change-notifier

`change_notifier::Channel::{Tenants…, SessionLocators}` subscriptions
become AgentDB **watch** on the system tenant scoped to the relevant
namespace. `Channel::{Nodes, Tablets, Shards}` remain on the SQL notifier
(Layer 1). The peer-URL refresher and routing invalidation keep their
shape; only the subscription source changes.

### 4.6 Bootstrap and failure ordering

Startup order per node:

1. Open Layer 1 (SQLite/Postgres). Register node, read cells/tablets.
2. Form/join raft groups for owned tablets — **system tablet first**.
3. Once the system tablet has a leader (any node in the cell), Layer-3
   reads go live. Until then:
   - Data-plane requests to *other* tablets proceed using **cached**
     tenant/policy/locator state where present (the caches are already
     KV-authoritative with TTLs).
   - Cache misses that need Layer 3 fail with `SERVICE_UNAVAILABLE` +
     `Retry-After` — the same contract as a draining node.
   - The **static installation token always authenticates** (config, not
     DB) so operators can inspect a cell whose system tablet is down.
4. `health/ready` gains a `system_tablet: ready|degraded` field; a node
   is still "ready" without it (it can serve warm traffic), but the
   dashboard and `agentdbctl` surface the degradation loudly.

Failure analysis: system-tablet leader loss is a normal raft failover
(election timeout, then a follower leads). During the window, Layer-3
*writes* (tenant create, token mint, locator update) queue/fail-retryable;
Layer-3 *reads* serve from cache. This is strictly better than today,
where a Postgres outage takes down the same operations with no cache
discipline and no bounded blast radius.

### 4.7 Migration

Pre-1.0, offline, one-shot — no dual-write machinery:

```
agentdb migrate-metadata --from <sql-url> --data-dir <dir>
```

Reads every Layer-3 row from SQL, writes composite records into the system
tenant (idempotent by external id, safe to re-run), verifies counts +
spot-check hashes, then stamps `config.metadata_layout = "system-tenant"`.
Server startup refuses to run with Layer-3 tables still populated but
layout stamped (split-brain guard). Downgrade = re-run with `--reverse`
while the release still ships `SqlMetadataStore`.

### 4.8 Recovery (the drill that gates this stage)

Extend S16 with a **metadata-restore drill**:

1. Destroy a cell keeping only the object store + Layer-1 dump.
2. Restore Layer 1 (plain SQL restore — tiny).
3. `agentdb restore-from-bucket` the **system tablet first**; verify
   tenants/locators/tokens are readable.
4. Restore data tablets; verify a locator-routed append works end-to-end.

RPO for metadata = the recovery-manifest interval, same as data — one
number, one story, which is the point.

---

## 5. Stage 3 — shrink `agentdb-meta` to the root

Delete the migrated modules and `SqlMetadataStore` for Layer-3 tables;
what remains: `nodes`, `cells`, `tablets`, `tablet_ranges`, `shards`,
`placement_scopes`, `scheduler_leader`, `config`, `migrations`,
`change_notifier` (Layer-1 channels only). Also deleted outright rather
than migrated: `users` (dead SCRAM residue — design-review finding 3).

Post-state invariants, enforced by a lint-style test that greps the server
for `pool()` reachable from request paths:

- No SQL on any request path (auth included — token verification is a
  record read behind the existing 60s cache).
- The external DB is written at: node join/leave, cell/tablet lifecycle,
  scheduler lease renewal, node heartbeat. Nothing else.

---

## 6. Testing plan (delta)

| Test | Stage | Falsifies |
|---|---|---|
| S8 steady-state zero-SQL assertion | 1 | ephemeral state leaked back onto raft paths |
| Failover-candidate unit matrix on raft-status source | 1 | wrong leader choice without `tablet_replicas` |
| S20: system-tablet failover under live tenant-create/token-mint load | 2 | Layer-3 writes lost or duplicated across metadata failover |
| S16-M: metadata-restore drill (§4.8) | 2 | circularity — system tablet not recoverable from root + bucket alone |
| S21: system-tablet-down brownout — warm data traffic keeps flowing, cold misses get 503+Retry-After, static token still works | 2 | hidden Layer-3 dependency on a warm path |
| Migration round-trip (`migrate-metadata` then `--reverse`, diff) | 2 | lossy migration |
| pool-on-request-path lint test | 3 | regression of the whole point |

## 7. Open questions (decide before stage 2 lands)

1. **Locator read locality**: follower replica of the system tablet on
   every node (bounded to small cells) vs. accepting one internal RPC on
   locator-cache miss. Leaning: follower-everywhere for cells ≤ 5 nodes,
   measured before generalizing.
2. **Audit-log retention**: system-tenant audit sessions inherit the
   normal retention machinery — is per-month session + tenant retention
   policy enough, or does audit need legal-hold defaults?
3. **Per-cell vs global system tenant**: this design is per-cell (each
   cell's system tablet holds that cell's tenants), which matches the
   Universe→Cell layering; the Universe-level directory (tenant→cell) is
   Layer 1's `cells`/placement data. Confirm no use case needs a global
   metadata view stronger than "ask each cell".

## Appendix A — full table disposition

| Table | Layer | Disposition |
|---|---|---|
| `nodes` | 1 | stays (heartbeat column write-rate drops in stage 1) |
| `cells` | 1 | stays |
| `tablets`, `tablet_ranges`, `shards`, `placement_scopes` | 1 | stay (routing bootstrap) |
| `scheduler_leader` | 1 | stays (must work with system tablet down) |
| `config`, `schema_migrations` | 1 | stay |
| `tablet_movements`, `tablet_snapshots` | 1 | stay (scheduler work queue; O(active movements), self-cleaning) |
| `tablet_replicas` | 2 | **deleted** (stage 1) |
| `tablet_stats` | 2 | **deleted** (stage 1) |
| `tenants`, `tenant_policies`, `tenant_quotas` | 3 | → `["sys","tenant"]` composite records |
| `session_locators`, `session_overrides` | 3 | → `["sys","locator"]` records |
| `admin_tokens` | 3 | → `["sys","token"]` records |
| `admin_audit` | 3 | → system-tenant audit event sessions |
| `users` | — | **deleted**, not migrated (dead SCRAM residue) |
