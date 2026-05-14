# Distributed Storage Cluster Next Step

Status: Track A and bounded Track B implemented; online split/merge cutover
hardening continues

This document describes the next production-oriented step for AgentDB's
distributed storage cluster. It combines the existing replicated tablet design
with multi-tenant blast-radius control, cell boundaries, availability-zone
placement, and the validation plan needed before cluster mode should be treated
as a supported production surface.

## Summary

AgentDB should continue toward a replicated tablet storage engine:

```text
client request
  -> gateway / any healthy AgentDB node
  -> route to tablet owner / leader
  -> propose tablet command through raft-rs
  -> persist raft log in raft-engine
  -> commit after quorum
  -> apply to tablet-local state
  -> acknowledge after local visibility
  -> seal snapshots / history / artifacts to object storage asynchronously
```

SlateDB should not be the production hot-write answer. It remains useful as an
optional `KvStore` backend for abstraction testing and object-store evaluation,
but the production write path should commit through local replicated tablet
state first.

The next design addition is a **cell**, used here as a short name for an
isolation domain. The exact product/schema name can change. The required
concept is an operational and tenant-placement boundary that limits how many
tenants can be affected by one node pool, object-store namespace, deploy, noisy
tenant, or operator mistake. It is not a storage primitive and does not replace
tablets. Metadata can start as one shared highly-available service with logical
`cell_id` partitioning; per-cell metadata partitions or databases are an
optional hardening step, not the default next step.

Availability zones are physical failure domains inside or across a cell. Tablet
replicas should be placed across AZs when the cell has multi-AZ capacity.

## Goals

- Survive one node failure without data loss for quorum-replicated tablets.
- Limit the tenant-visible effect of one node failure to tablets whose leader or
  owner was on that node.
- Limit operational blast radius by assigning tenants to cells.
- Keep tablets as the only storage, Raft, snapshot, compaction, and movement
  unit.
- Support dedicated paid tenants and shared free-tier pools without mixing their
  physical isolation rules.
- Map tenant tier and priority to isolation-domain classes so low-SLA tenants
  can run on lower-cost capacity while paid tenants stay on stable capacity.
- Spread large tenants across multiple tablets without exposing placement to SDK
  callers.
- Support single-AZ deployment for local, economy, and low-cost clusters while
  clearly documenting that it does not survive AZ failure.
- Place replicas across availability zones when possible.
- Make failover, catch-up, split, merge, drain, and replacement observable and
  testable.
- Keep object storage asynchronous for sealed history, artifacts, snapshots, and
  archive flows.

## Non-Goals

- Do not introduce cells as a second data-sharding layer below tablets.
- Do not make virtual shards independently replicated, leased, snapshotted, or
  compacted.
- Do not depend on synchronous object-store writes for the agent-loop hot path.
- Do not claim full cross-region active-active semantics in this step.
- Do not describe single-AZ clusters as equivalent to multi-AZ production
  deployments.
- Do not automatically move tenants across cells during normal balancing.
- Do not mix dedicated paid tenants into shared pool tablets.

## Terminology

- **Region**: broad cloud or datacenter geography, such as `us-west-2`.
- **Availability zone**: independent physical failure domain inside a region,
  such as `us-west-2a`.
- **Cell / isolation domain**: bounded AgentDB deployment slice with its own
  node pool, tenant assignments, placement scopes, operational rollout boundary,
  and object-store namespace. This document uses `cell` for brevity, but
  `isolation_domain`, `placement_domain`, or `cluster_slice` are viable names if
  they fit the product language better. Metadata isolation can be logical first
  and physically separated later when the operational tradeoff is justified.
- **Domain class**: cost and SLO class for an isolation domain, such as
  `economy`, `standard`, or `enterprise`. This is where instance lifecycle,
  replica policy, rollout risk, and tenant priority are expressed.
- **Deployment profile**: supported topology shape, such as `local`,
  `single_az_cluster`, `multi_az_standard`, or `enterprise`.
- **Node**: one AgentDB server process with stable `node_id`, internal address,
  local fast storage, region, AZ, and cell metadata.
- **Placement scope**: routing namespace for virtual shards. A dedicated paid
  tenant is one placement scope. A shared free-tier pool is also one placement
  scope.
- **Virtual shard**: stable routing slot inside a placement scope. It has no
  owner, Raft group, lease, snapshot, or compaction state.
- **Tablet**: physical storage and serving unit. A tablet owns shard ranges and
  carries Raft replication, local applied state, indexes, snapshots, compaction,
  lifecycle state, and movement.
- **Tenant policy**: per-tenant placement, isolation, quota, lifecycle,
  encryption, and cell assignment metadata.

## Target Topology

```text
region us-west-2

  shared HA metadata service
    cells
    nodes(cell_id, region, availability_zone)
    tenant_policies(cell_id, tier, placement_scope_id)
    placement_scopes(cell_id)
    tablet_ranges / tablets / tablet_replicas

  cell c001
    gateway / load-balancer slice
    metadata scope: cell_id=c001
    object-store bucket or prefix
    nodes:
      node-a az=us-west-2a
      node-b az=us-west-2b
      node-c az=us-west-2c
    placement scopes:
      tenant/acme
      tenant/bigco
      shared/free-pool-001
    tablets:
      tablet 7  replicas: node-a, node-b, node-c
      tablet 8  replicas: node-b, node-c, node-a

  cell c002
    separate gateway slice
    metadata scope: cell_id=c002
    separate object-store bucket or prefix
    independent rollout and scheduler domain
```

Cells are used to limit blast radius. Tablets are used to store and replicate
data. AZs are used to avoid placing quorum replicas in the same physical failure
domain.

## Cell Model

A production cell should contain:

- one AgentDB node pool
- one scheduler/rebalance domain
- one metadata namespace, usually in a shared highly-available metadata service
- one object-store bucket, prefix, account, or encryption namespace
- a set of placement scopes
- a set of tenant assignments
- its own metrics, alerts, dashboards, and rollout gates

The metadata namespace does not need to be a separate database at first. The
default next step should be one shared metadata backend with `cell_id` on the
relevant rows and strict same-cell placement invariants in routing and the
scheduler. A separate schema, partition, or database per cell is a later
hardening option for scale, compliance, or stronger blast-radius requirements.

Implementation note: Track A keeps the shared metadata backend. The current
implementation has logical `cells`, `nodes.cell_id`, `placement_scopes.cell_id`,
and tenant policy `cell_id`/`tier`/`priority` fields. Routing resolves tenant
policy to cell and placement scope before tablet selection. Scheduler
assignment, replacement, and load balancing select only same-cell nodes and use
cell deployment profile plus tenant tier/capacity policy when choosing targets.
Track B now provides bounded grouped RocksDB tablet-store domains, tablet
checkpoint/restore hooks, domain metrics, benchmark coverage, and manual
compaction operations.

An isolation domain should also declare a capacity and SLO profile:

- capacity class: `stable`, `preemptible`, or `mixed`
- tenant tier policy: which tenant tiers may be placed there
- allowed tenant priority range
- default replica policy
- default backup/snapshot policy
- rollout policy and change window

Cells should be small enough that a bad rollout or metadata issue does not hit
the full fleet, but large enough to run quorum and absorb one node failure.

Recommended initial size:

- minimum: 3 nodes per cell
- preferred: 3 AZs with one voter per AZ for baseline cells
- larger cells: 5 or more nodes, still with per-tablet replica placement across
  AZs and load-balanced leaders

The product should also support lower-cost single-AZ cells. A single-AZ cell can
survive one node failure when tablets have quorum across distinct nodes, but it
cannot survive an AZ failure. This is a valid deployment profile for local,
economy, free-tier, internal, and cost-sensitive workloads, but it should not be
marketed as equivalent to multi-AZ production HA.

Tenant movement between cells should be an explicit migration workflow:

```text
prepare tenant migration
  -> create destination placement scope in target cell
  -> snapshot / copy tablet state
  -> catch up committed tail
  -> fence old placement
  -> atomically switch tenant policy cell_id / placement_scope_id
  -> verify reads and writes in target cell
  -> retire old placement after retention window
```

Do not use tenant migration as an ordinary load-balancing mechanism until the
workflow is fully observable and reversible.

## Availability-Zone Placement

AZ placement is replica placement policy, not a tenant API concept.

Baseline rules:

- A tablet with three voters should prefer three distinct AZs.
- In a single-AZ deployment, a tablet with three voters should place those
  voters on three distinct nodes in the same AZ.
- If the cell has only two AZs, prefer `2 + 1` placement only with an explicit
  documented tradeoff: losing the AZ with two voters can lose quorum.
- Avoid placing two voters for the same tablet on the same node.
- Prefer not to place two voters for the same tablet in the same AZ if another
  healthy AZ is available.
- Learners can be placed in a target AZ during replacement, catch-up, or
  migration, then promoted when caught up.
- The scheduler should balance tablet leadership across nodes and AZs, not only
  replica count.

For a serious production target, use three AZs when available:

```text
tablet T voters
  node-a cell=c001 az=a
  node-b cell=c001 az=b
  node-c cell=c001 az=c
```

This survives any one node and usually any one AZ. A two-AZ deployment can be
supported as a lower-availability mode, but it should not be described as
equivalent to three-AZ quorum placement.

Single-AZ quorum placement is still useful:

```text
tablet T voters
  node-a cell=c001 az=a
  node-b cell=c001 az=a
  node-c cell=c001 az=a
```

This survives one node failure, but not loss of `az=a`.

## Deployment Profiles

AgentDB should support a small set of explicit deployment profiles instead of
one ambiguous "cluster" mode.

| Profile | Shape | Capacity | Failure tolerance | Recommended use |
|---|---|---|---|---|
| `local` | one node | stable local disk | no node HA | development, evaluation, small self-hosted installs |
| `single_az_cluster` | three or more nodes in one AZ | stable, mixed, or preemptible by tier | one-node failure when quorum remains; no AZ failure tolerance | free tier, economy domains, internal tools, cost-sensitive workloads |
| `multi_az_standard` | three or more nodes across three AZs | stable | one-node and usually one-AZ failure | paid production default |
| `enterprise` | multi-AZ stable isolated domain | stable isolated pool | one-node and one-AZ failure plus stricter rollout/key controls | enterprise and regulated tenants |

Default recommendations:

- Default developer path: `local`.
- Default low-cost cluster path: `single_az_cluster`.
- Default paid production path: `multi_az_standard`.
- Enterprise/regulated path: `enterprise`.

The API and docs should be precise:

```text
single-AZ cluster: node-failure tolerance, no AZ-failure tolerance
multi-AZ cluster: node-failure tolerance and AZ-failure tolerance when voters
                  are placed across enough AZs
```

## Tenant Placement Classes

AgentDB should use placement policy to control cost and isolation.

| Class | Placement | Blast-radius target |
|---|---|---|
| Free / low-SLA | Shared pool placement scope inside one cell | Can share tablets with other free tenants |
| Paid standard | Dedicated placement scope, one or more dedicated tablets inside one cell | No tablet-level mixing with other tenants |
| Enterprise / regulated | Dedicated cell or small set of cells, tenant encryption class, strict rollout gates | No shared pool, stricter operational isolation |

Dedicated placement means:

- one tenant owns the placement scope
- tablets in that scope contain only that tenant's data
- split/merge never combines tablets from different dedicated tenants
- snapshots, local indexes, sealed segments, backups, and compaction debt stay
  inside that tenant boundary

Shared placement means:

- multiple low-SLA tenants may share one placement scope
- shared tablets must not contain paid, enterprise, regulated, or
  tenant-key-isolated tenants
- noisy-neighbor control comes from per-tenant quotas and tablet-level
  backpressure

## Tenant Tier And Cost Domains

Tenant tier should drive which isolation domains a tenant can use. The domain
controls both blast radius and infrastructure cost.

Example policy:

| Tenant tier | Allowed domain class | Capacity | Replica policy | Notes |
|---|---|---|---|---|
| Free | `economy` | preemptible or mixed | single-AZ or lower availability, explicit durability policy | Best-effort runtime availability |
| Developer / trial | `economy` or `standard` | mixed | single-AZ or multi-AZ depending on product offer | Upgrade path to standard |
| Paid standard | `standard` | stable on-demand/reserved | multi-AZ quorum by default | Normal production tier |
| Enterprise | `enterprise` | stable, isolated pool | multi-AZ quorum, stricter rollout gates | Dedicated or tightly bounded domain |
| Regulated | `regulated` | stable, isolated pool | multi-AZ quorum, stricter key/backup policy | Separate compliance controls |

Free-tier tenants can be placed in an economy isolation domain backed by lower
cost instances, including spot or other preemptible capacity, but this must be a
policy decision with clear guarantees. Preemptible capacity is acceptable for
best-effort availability; it is not equivalent to stable quorum capacity.

Recommended rules:

- Never place paid, enterprise, regulated, or tenant-key-isolated tenants in an
  economy/preemptible domain unless an operator explicitly overrides policy.
- Keep free-tier shared pools separate from paid shared infrastructure.
- If a free-tier domain uses only preemptible nodes, document that multiple
  simultaneous preemptions can make tablets unavailable and can lose unsealed
  local-only committed data unless the tier's durability policy explicitly
  protects it through stable quorum, synchronous external persistence, or a
  comparable mechanism.
- If free-tier data still has a durability promise, keep at least quorum voters
  on stable capacity and use preemptible nodes only as learners, cache workers,
  or overflow API capacity.
- Treat tenant priority as an admission and recovery signal: higher-priority
  tenants get capacity, catch-up, compaction, and replacement work before
  best-effort tenants.
- Use per-tier quotas and backpressure so a noisy free tenant cannot exhaust
  shared domain resources.

This separates three concerns that are easy to conflate:

- cost: whether nodes are stable, mixed, or preemptible
- availability: whether the tier promises quorum survival and fast failover
- durability: whether committed data must survive node and capacity loss without
  relying on best-effort local disks

AgentDB can choose a lower-cost availability profile for free tenants, but the
tenant policy should make that explicit rather than hiding it behind the
placement mechanism.

## Metadata Isolation Levels

Metadata isolation has a real management cost. The design should support levels
instead of requiring a dedicated metadata database per cell on day one.

| Level | Shape | Operational cost | Blast-radius behavior |
|---|---|---|---|
| Shared logical | One HA metadata database, all rows carry `cell_id` | Lowest | Metadata outage can affect all cells; placement bugs are constrained by invariants and tests |
| Shared database, partitioned schema | One metadata service, per-cell schemas or partitions | Medium | Some noisy-query and maintenance isolation; service outage can still be global |
| Dedicated metadata database | Separate metadata database per cell | Highest | Strongest cell-level isolation; more backup, migration, monitoring, and upgrade burden |

Recommended path:

- Start with **shared logical** metadata.
- Make every placement query and mutation cell-scoped.
- Add database constraints and tests where possible so cross-cell movement is
  hard to do accidentally.
- Move to partitioned or dedicated metadata only when a concrete need appears:
  scale bottlenecks, compliance, noisy metadata workloads, or a product tier
  that requires stronger isolation.

This keeps the first implementation manageable while preserving a path to
stronger blast-radius isolation later.

## Metadata Changes

The current metadata model already has placement scopes, tenant policies, nodes,
tablet ranges, tablets, and tablet replicas. The next step should add explicit
cell fields and enforce them in placement code.

Target additions:

```text
cells
  cell_id
  region
  domain_class: economy | standard | enterprise | regulated
  deployment_profile: local | single_az_cluster | multi_az_standard | enterprise
  capacity_class: stable | mixed | preemptible
  metadata_isolation_level: shared_logical | partitioned | dedicated
  state: serving | draining | maintenance | disabled
  metadata_namespace
  object_store_namespace
  rollout_version
  created_at_ms
  updated_at_ms

nodes
  cell_id
  region
  availability_zone
  capacity_class: stable | preemptible
  state
  address
  heartbeat

placement_scopes
  cell_id
  placement_scope_id
  kind: dedicated_tenant | shared_pool
  shard_count
  epoch
  state

tenant_policies
  tenant_id
  tier: free | developer | paid | enterprise | regulated
  priority
  cell_id
  placement_class: dedicated | shared
  placement_scope_id
  availability_class
  durability_class
  encryption_class: tenant | pool
  key_policy_id
  policy_version

tablet_replicas
  tablet_id
  node_id
  cell_id
  availability_zone
  role: voter | learner | log_only
  health
  applied_index
  match_index
```

Hard placement invariants:

- A placement scope belongs to exactly one cell.
- A tablet belongs to exactly one placement scope and therefore one cell.
- All voters for a tablet must be inside the tablet's cell.
- The scheduler must not rebalance tablets across cells.
- Split and merge operate only inside one placement scope and one cell.
- A tenant's default writes route to the cell in `tenant_policies.cell_id`.
- Cross-cell movement requires an explicit migration operation.
- Tenant tier must be allowed by the destination cell's domain class.
- Paid-or-higher tenants must not be placed in economy/preemptible domains by
  automatic placement.
- Paid production tenants should not be placed in `single_az_cluster` domains
  unless the product plan or operator override explicitly accepts no AZ-failure
  tolerance.

## Request Routing

Normal SDK callers should not choose cells, tablets, or shards directly.

Routing flow:

```text
request
  -> authenticate tenant
  -> load tenant policy
  -> resolve cell_id and placement_scope_id
  -> choose routing key: session_id or sharding_key
  -> shard = hash(tenant_id, routing_key) % shard_count
  -> tablet = tablet_map[cell_id, placement_scope_id, shard]
  -> route to tablet owner / Raft leader
```

Gateway behavior:

- Prefer routing to a healthy node in the tenant's cell.
- If the first node is not the owner, it may forward to the owner inside the
  same cell.
- Cross-cell forwarding should be disabled by default except for migration and
  admin workflows.
- Route rejections should include cell, tablet, owner, epoch, and retryability
  metadata for debugging.

## Node Failure Behavior

When one node goes down in a quorum cell:

1. Heartbeats for that node stop.
2. Other nodes mark it stale/dead after the configured threshold.
3. Tablet leaders or owners on that node are lost.
4. Surviving replicas elect or expose a new leader when quorum remains.
5. Ownership/routing metadata is refreshed.
6. Requests for affected tablets may fail or see elevated latency during the
   failover window.
7. Requests for tablets not owned by the failed node should continue normally.
8. Replacement or catch-up work restores replica count later.

Tenant impact depends on placement:

- A tenant with one small tablet on the failed node sees a brief outage or
  degraded latency for that tablet.
- A large tenant spread across many tablets may see partial degradation because
  some tablets were on the failed node and others were not.
- Tenants assigned to other cells should not be affected by the node failure.
- If all cells share one metadata database, that metadata service remains a
  larger global blast radius even if data tablets are cell-scoped. This is an
  acceptable early tradeoff if the database is highly available and all data
  placement is cell-scoped.

This is why cells should isolate node pools, object-store namespaces, placement,
and rollout domains first. Metadata can remain a shared logical service until
scale, compliance, or blast-radius requirements justify stronger isolation.

## Blast-Radius Matrix

| Failure | Expected blast radius with tablets only | Expected blast radius with cells |
|---|---|---|
| One tablet hot | One tablet or shard range | Same |
| One node down | Tablets with leaders/owners/replicas on that node | Same, but only inside one cell |
| One node down in single-AZ cell | Tablets with leaders/owners/replicas on that node; quorum survives if enough voters remain | Same, but only inside that single-AZ cell |
| One AZ down | Tablets without quorum across surviving AZs | Only cells using that AZ; quorum survives if voters span 3 AZs |
| One AZ down in single-AZ cell | Whole cell unavailable | Whole single-AZ cell unavailable |
| Noisy free tenant | Shared pool tablets | Shared pool inside one cell |
| Bad split/merge bug | Affected placement scopes/tablets | Affected cell if rollout is cell-gated |
| Bad binary rollout | Whole cluster if globally rolled out | One cell at a time |
| Metadata DB outage | Whole cluster if metadata is global | One cell if metadata is partitioned per cell |
| Object-store issue | Whole cluster if one bucket/account is global | One cell if object namespace is isolated |

## Storage Engine Direction

The storage direction remains:

- `raft-rs` and `raft-engine` for replicated tablet logs
- tablet-local applied state for session events, records, metadata,
  idempotency, local indexes, and changefeed state
- RocksDB or another local LSM for hot applied state
- object storage for artifacts, sealed history, snapshots, archive, and
  retained cold data
- SlateDB as optional/evaluation storage, not the production hot-write commit
  path

### Storage Architecture Diagram

Hot writes commit through Raft and local tablet state. Object storage is not on
the synchronous write-commit path. An object cache may be added later for
cold/sealed reads, but it should not be required for the initial deployment.

```text
                         WRITE PATH (hot / mutable)

 client / SDK
     |
     v
 gateway / any AgentDB API node
     |
     v
 route by tenant -> isolation domain -> placement scope -> shard -> tablet
     |
     v
 tablet owner / Raft leader
     |
     v
 +-------------------- tablet Raft group --------------------+
 |                                                           |
 |  leader node                 follower node      follower node
 |  +----------------+          +-------------+    +-------------+
 |  | raft-rs        |  ----->  | raft-rs     |    | raft-rs     |
 |  | raft-engine    |  ----->  | raft-engine |    | raft-engine |
 |  | local tablet   |          | local tablet|    | local tablet|
 |  | applied state  |          | applied     |    | applied     |
 |  +----------------+          +-------------+    +-------------+
 |                                                           |
 +---------------------- quorum commit ----------------------+
     |
     v
 apply committed command to tablet-local state
     |
     v
 acknowledge client
```

```text
                         ASYNC SEAL / SNAPSHOT PATH

 tablet-local state / committed log
     |
     v
 background sealer / snapshot worker
     |
     v
 immutable segment / snapshot / artifact object
     |
     v
 optional object cache layer  --->  S3 / object store
          |                               |
          | cache is disposable           | durable source of truth
          | may be omitted at first       |
          v                               v
    fast repeated reads              retained cold data
```

Hot reads should prefer tablet-local applied state when the requested data is
still hot.

```text
                         READ PATH (hot data)

 client / SDK
     |
     v
 gateway / API node
     |
     v
 route to tablet owner / leader or valid local replica
     |
     v
 tablet-local applied state
     |
     v
 return latest session head / recent events / local index result
```

Cold reads use manifests or object references from tablet-local state, then
fetch immutable bytes from S3/object storage directly. A disposable cache in
front of S3 is a future optimization, not a required component.

```text
                         READ PATH (cold / sealed data)

 client / SDK
     |
     v
 gateway / API node
     |
     v
 tablet owner / storage worker
     |
     v
 read manifest / object reference from tablet-local state
     |
     v
 optional object cache
     |
     +---- cache hit ----> return bytes
     |
     +---- cache miss ---> S3 / object store
                            |
                            v
                    validate checksum / version / size
                            |
                            v
                    fill cache and return bytes
```

Combined shape:

```text
             shared metadata DB
        cells / tenants / tablets / replicas
                    |
                    v
+-------------------------------------------------------------+
| isolation domain / cell                                     |
|                                                             |
|  API + storage node A     API + storage node B     node C   |
|  +------------------+     +------------------+     +------+ |
|  | raft-engine      |<--->| raft-engine      |<--->| ...  | |
|  | tablet state     |     | tablet state     |     |      | |
|  | hot cache        |     | hot cache        |     |      | |
|  +------------------+     +------------------+     +------+ |
|                                                             |
|          async snapshots / sealed history / artifacts       |
|                         |                                   |
+-------------------------|-----------------------------------+
                          v
              optional object cache layer
                          |
                          v
                  S3 / object store
```

Core rule:

```text
Raft + local tablet state = hot correctness path
S3 / object store        = cold/sealed durability path
optional object cache    = cold/sealed read acceleration only
```

The main physical storage improvement is to move from logical tablet prefixes
inside one large KV/LSM toward independently managed tablet-local stores. This
applies even in single-node deployments. A single node should not mean one
global LSM tree for all tenants, all tablets, and all indexes.

## LSM And Compaction Strategy

RocksDB and SlateDB are both LSM-shaped storage engines. They are good for
append-heavy workloads, but one large LSM tree creates shared write
amplification, compaction debt, cache pressure, and tail-latency coupling across
unrelated tenants.

AgentDB should solve this by bounding each LSM's responsibility:

- The tablet is the default compaction and applied-state boundary.
- Dedicated paid tenants get tenant-owned tablets and therefore tenant-bounded
  LSM/compaction domains.
- Shared free-tier tenants can share tablet-local stores, but only inside
  explicit shared/economy placement scopes.
- Large or hot tenants split into more tablets before one LSM becomes too large.
- Local indexes live with the tablet so index compaction follows the same
  boundary as the base data.
- Sealed immutable history leaves the hot LSM and moves to object storage.

The storage layout can evolve through profiles:

| Layout | Description | Best use | Tradeoff |
|---|---|---|---|
| Prefix-scoped global LSM | Current simple layout: all tablets share one KV instance with prefixes | Development and early compatibility | Cheapest to manage, worst compaction isolation |
| Grouped tablet LSM | One LSM per small group of low-SLA tablets | Free/economy pools | Fewer handles than per-tablet, some shared compaction debt |
| Tablet-local LSM | One physical LSM per tablet | Paid standard, hot tablets, predictable isolation | More files, memory, and open handles |
| Tenant-local group | One or more LSMs per dedicated tenant, each holding a bounded tablet set | Medium paid tenants | Balances isolation and overhead |
| Tiered hybrid | Hot heads in local LSM, sealed immutable segments in object storage/cache | Long-lived sessions and archives | Requires manifest/merge reads |

The target should be a hybrid policy, not a single universal layout:

- `local` profile can start with prefix-scoped global LSM for simplicity, but
  should support a tablet-local option for production single-node users.
- `single_az_cluster` economy domains can use grouped tablet LSMs to control
  cost.
- `multi_az_standard` paid domains should use tablet-local or tenant-local
  grouped LSMs.
- `enterprise` and regulated domains should prefer tablet-local LSMs or a
  small dedicated tenant-local group with strict size limits.

Implementation note: Track B starts with a bounded `grouped_rocksdb` layout
rather than opening one RocksDB instance for every tablet. Operators set
`storage.tablet.tablets_per_domain` and `storage.tablet.max_open_domains` so
memtables and open-file usage stay capped. A one-tablet domain remains an
explicit high-isolation setting, not the default. Each grouped domain also uses
a smaller RocksDB write-buffer profile than the legacy single-DB runtime path;
the default mutable-memtable budget is roughly:

```text
max_open_domains * domain_write_buffer_size_mib * domain_max_write_buffers
```

The current default is `16 * 16 MiB * 2`, or about 512 MiB before RocksDB
metadata and block cache overheads. Grouped domains also default to one
RocksDB background job and one parallelism unit per open domain; larger values
should be tied to an explicit node CPU budget.

TiKV RFC 0093, "Physical isolation between Region", is the reference model for
the later full tablet-store design: multiple RocksDB instances on one node must
share process-level controls where possible, bound memtable growth explicitly,
name tablet directories with an epoch/applied-index component, checkpoint for
snapshot transfer, and delay old-domain cleanup until existing handles are
gone.

Compaction controls should be explicit:

- Track per-tablet key count, value bytes, storage bytes, write rate, retained
  log debt, tombstones, and compaction debt.
- Trigger split when a tablet crosses size, write-rate, retained-log, or
  compaction-debt thresholds.
- Trigger log compaction only after snapshots are durable enough for recovery.
- Move cold sealed events, old artifacts, and archive bundles out of the hot LSM
  into object storage.
- Keep hot session heads and recent events in the tablet-local LSM.
- Backpressure writes to a tablet with excessive compaction debt rather than
  penalizing unrelated tablets.
- Schedule compaction per tablet/domain with tenant priority so paid tenants do
  not wait behind free-tier compaction.
- Avoid broad range deletes in shared LSMs where they create large tombstone
  debt across unrelated tenants.

Operational guardrails:

- Set a maximum target storage size per hot tablet.
- Set a maximum target write rate per hot tablet.
- Set a maximum compaction debt per tablet before split/backpressure.
- Set a maximum number of tablets per physical LSM for grouped layouts.
- Report p99 append latency together with compaction activity and write
  amplification estimates.
- Expose "largest LSM domain" and "noisiest compaction domain" in admin status.

For single-node deployments, the recommended production shape is:

```text
single node
  tablet 1 -> local LSM A
  tablet 2 -> local LSM B
  tablet 3 -> local LSM C
  shared/economy tablets 4..8 -> grouped LSM D
  sealed history / artifacts -> object storage or local content backend
```

This preserves the easy single-node operational model while avoiding one
monolithic LSM tree as the data set grows.

### LSM Domains As Lifecycle Units

Separated LSM domains are not only a performance optimization. They also define
the practical unit for backup, restore, local balancing, node migration, and
tenant movement. They also affect changefeed production and replay, because
changefeed cursors must remain meaningful across tablet movement, restore,
split, merge, and hot/cold storage transitions.

If a single node stores all tenants in one global LSM, then every operational
workflow becomes coarse and expensive:

- backup must checkpoint a large mixed-tenant store
- restore has to reason about unrelated tenants and tablets
- local disk balancing cannot move one hot tablet without moving the world
- tenant migration requires filtering a large global keyspace
- compaction debt from one tenant can delay backup or restore for another
- testing recovery for one tablet still exercises a global storage image
- changefeed replay may need to scan unrelated keys or reconstruct ordering
  across a mixed global store

If the node stores bounded tablet or tenant-local LSM domains, those workflows
become scoped:

- backup can capture one tablet, tenant-local group, or shared-pool group
- restore can rehydrate a bounded tablet before opening it for traffic
- local balancing can move a tablet store from one disk/path to another
- node replacement can copy tablet stores independently
- split/merge can materialize child/destination stores without rewriting
  unrelated tenants
- tenant migration can copy dedicated tenant tablet stores plus committed log
  tails
- compaction debt and backup pressure are visible per domain
- changefeed outbox and cursor state can stay aligned with the tablet's Raft
  applied index

Recommended lifecycle contract:

```text
tablet-local LSM domain
  manifest.json
  rocksdb/ or other local engine directory
  latest_applied_index
  latest_snapshot_index
  tenant/placement metadata
  checksum and size summary
```

The control plane should know enough to answer:

- which tablet owns this LSM domain?
- which tenant or shared pool does it belong to?
- what Raft index does the local state cover?
- is there a durable snapshot covering that index?
- can this domain be moved, copied, compacted, or deleted?

Backup and restore should be tablet-aware:

```text
backup tablet
  -> fence or snapshot at an applied index
  -> checkpoint the tablet-local LSM domain
  -> record tablet id, placement scope, cell, epoch, applied index, checksum
  -> upload snapshot/checkpoint metadata and optional bytes

restore tablet
  -> create replacement tablet metadata
  -> install checkpoint/snapshot into a new local LSM domain
  -> replay committed Raft tail after snapshot index
  -> verify checksum and applied index
  -> open tablet replica and allow routing only after catch-up
```

Local balancing on a single node should move LSM domains between local storage
paths, not individual keys:

```text
disk0/tablet-0007  -> disk1/tablet-0007
```

Cluster rebalancing should likewise prefer moving bounded tablet stores and
catching up from Raft/snapshots. A global LSM makes this much harder because the
movement unit is no longer aligned with the Raft, tablet, placement, and tenant
unit.

This is the core reason single-node production should still support separated
LSM domains: the same boundary that reduces compaction blast radius also makes
backup, restore, migration, and balancing tractable.

### Changefeed Implications

Storage-engine changes must preserve changefeed semantics. The changefeed
source of truth should be the committed tablet log and deterministic apply
state, not incidental RocksDB/SST/SlateDB file layout.

Recommended model:

```text
Raft committed entry
  -> apply tablet command
  -> write base tablet state
  -> write tablet-local changefeed/outbox row with applied_index
  -> publish watch/changefeed item after local visibility
```

Changefeed cursors should include enough information to survive movement:

```text
changefeed cursor
  cell_id
  placement_scope_id
  tablet_id
  tablet_epoch
  applied_index
  optional sequence within applied_index
```

Required invariants:

- A changefeed item must not be visible before its base state is locally
  visible.
- Changefeed replay for a tablet must be recoverable from a durable snapshot
  plus the committed Raft tail after the snapshot index.
- Backup/restore must include the changefeed cursor/outbox state or rebuild it
  deterministically from the committed tablet log.
- Split should retarget changefeed ownership by shard range and preserve cursor
  continuity through the cutover epoch.
- Merge should merge source changefeed streams into the destination tablet
  without duplicating already published items.
- Tenant migration should fence the source placement, copy/rebuild changefeed
  state, catch up the committed tail, and publish a new epoch before clients
  resume.
- Sealing cold history to object storage must not remove the recent changefeed
  window until all required cursors are past the sealed range or the range is
  replayable from sealed objects.
- Engine acceleration hooks such as SST ingestion, range delete, or file clone
  must update or preserve changefeed manifests and applied-index metadata.

Operational signals:

- per-tablet changefeed lag from applied index to published cursor
- oldest retained changefeed cursor per tablet
- changefeed rebuild progress during restore/migration
- split/merge changefeed cutover epoch
- dropped/expired cursor count by tenant policy

This keeps changefeed as a tablet-level contract. The underlying LSM layout can
change, but clients should see stable ordered progress within the scope promised
by the API.

### Engine Acceleration Hooks

AgentDB should use underlying storage-engine features to speed up lifecycle
operations, but only behind the tablet lifecycle contract. Correctness should
come from tablet epochs, placement metadata, Raft applied indexes, manifests,
checksums, and catch-up verification. Engine-specific features are
optimizations, not the source of truth.

Useful hooks include:

| Capability | Example engines | Useful for | Guardrail |
|---|---|---|---|
| Checkpoint / snapshot | RocksDB checkpoint | fast backup, local restore seed, tablet copy | checkpoint must be tied to tablet id, epoch, and applied index |
| Range delete | RocksDB `DeleteRange`, Pebble range deletion | tombstoning drained tablet ranges, cleanup after split/merge | avoid broad deletes in shared LSMs unless the range is tenant/tablet-exclusive |
| SST ingestion | RocksDB external SST ingestion, Pebble SSTable ingestion | fast restore, split child materialization, bulk migration | ingest only verified SSTs with expected key bounds and schema/version metadata |
| Column families | RocksDB column families | grouping tablet/index families inside one process | do not let one column-family layout become a hidden global compaction domain |
| Manual compaction | RocksDB/Pebble manual compaction | post-restore cleanup, post-delete space reclamation, controlled maintenance | schedule by tenant priority and avoid foreground latency spikes |
| File clone / hard link | filesystem reflink, RocksDB checkpoint hard links | local copy, same-node backup, fast tablet store clone | only within same trust boundary and after manifest/checksum validation |
| Object-store copy/clone | S3 copy, object-store prefix copy, future SlateDB object clone | cold snapshot copy, cross-cell archive movement | immutable object keys, version/checksum validation, no mutable hot writes |

The lifecycle layer should expose these as optional capabilities:

```text
trait TabletStoreLifecycle {
  checkpoint(tablet_id, applied_index) -> CheckpointManifest
  ingest_checkpoint(tablet_id, manifest)
  ingest_sst(tablet_id, sst_manifest)
  delete_range(tablet_id, key_range)
  clone_local(tablet_id, target_path)
  compact(tablet_id, policy)
}
```

Fallback behavior must always exist:

- If SST ingestion is unavailable, restore can replay from snapshot records or
  normal KV writes.
- If range delete is unsafe for a shared LSM, cleanup can delete scoped keys in
  batches.
- If local clone/reflink is unavailable, copy files or stream through the
  checkpoint manifest.
- If object-store clone is unavailable, copy immutable objects by reading and
  rewriting with checksum verification.

Operation-specific guidance:

- **Backup**: prefer engine checkpoint for tablet-local RocksDB domains; upload
  checkpoint manifest and optionally SST files/object references.
- **Restore**: prefer verified checkpoint or SST ingestion, then replay Raft
  tail after `latest_snapshot_index`.
- **Split**: prefer range-filtered snapshot/export into child tablet domains;
  use SST ingestion when the source can emit bounded child SSTs.
- **Merge**: prefer ingesting verified source snapshots into a destination
  domain, then replay each source tail up to cutover.
- **Tenant migration**: prefer copying immutable sealed objects by object-store
  copy/clone and moving hot tablet state through checkpoint plus Raft catch-up.
- **Cleanup**: prefer range delete only when key bounds exactly cover a
  tablet-exclusive range; otherwise use scoped batch deletes.

This gives AgentDB permission to exploit RocksDB, Pebble-like, SlateDB, or
filesystem/object-store features without coupling the product semantics to one
engine's behavior.

Options to evaluate:

- one RocksDB instance per tablet
- one RocksDB instance per node with column families per tablet
- grouped RocksDB instances per small tablet set
- hybrid layout where small shared-pool tablets are grouped and large dedicated
  tenants get isolated stores

The decision should be benchmark-driven. Important metrics:

- append p50/p95/p99/p999 latency
- write amplification
- compaction debt and compaction pause impact
- open file and memory overhead
- tablet snapshot time
- tablet catch-up time
- split/merge copy time
- restart recovery time

## Scheduler And Placement Policy

The scheduler should become cell-aware and AZ-aware.

Required scheduler rules:

- only schedule tablets onto nodes in the tablet's cell
- prefer voter placement across distinct AZs
- for `single_az_cluster`, prefer distinct nodes in the same AZ and mark the
  tablet as not AZ-failure-tolerant in status
- do not create a tablet replica on a draining, dead, or maintenance node
- do not move paid tenants into shared placement scopes
- do not move paid-or-higher tenants into economy/preemptible domains
- prefer stable-capacity voters for tiers with durability or availability
  guarantees
- use preemptible nodes as learners, cache/API capacity, or best-effort free
  voters only when the tier policy allows it
- do not split or merge across cells
- do not merge dedicated tablets from different tenants
- cap movement per tick per cell
- respect lifecycle cooldowns to avoid split/merge oscillation
- prefer replacing unhealthy replicas before load-balancing healthy tablets
- prioritize replacement, catch-up, compaction, and snapshot work by tenant
  tier and priority

Useful scheduler inputs:

- node state and heartbeat age
- node region, AZ, and cell
- cell deployment profile
- node capacity class and preemption risk
- tablet owner, epoch, and lifecycle state
- tablet replica role and health
- raft leader, term, commit index, applied index, and lag
- tablet storage bytes, key count, write rate, and retained log debt
- tenant tier, priority, placement class, availability class, durability class,
  and quota policy
- recent split/merge movement history

## Operational Model

Minimum production operating model:

- bootstrap a cell
- add node to cell
- drain node
- replace failed node
- rebalance within cell
- run rolling restart one cell at a time
- roll out new binary/config one cell at a time
- inspect tablet ownership and Raft health
- inspect tenant-to-cell and tenant-to-placement mapping
- inspect tenant tier, priority, and domain class
- migrate tenant between cells as an explicit workflow

Operators should be able to answer:

- which cell owns this tenant?
- what tier and priority does this tenant have?
- what deployment profile is this tenant on?
- is this tenant on stable, mixed, or preemptible capacity?
- which tablets serve this session?
- which node and AZ own the current write path?
- does the tablet have quorum?
- which replicas are lagging?
- what movement is pending?
- what tenants are in the blast radius of this cell, node, AZ, or tablet?

## Validation Plan

### Unit And Contract Tests

- cell-aware routing chooses only placement scopes in the tenant's cell
- scheduler refuses cross-cell tablet placement
- scheduler prefers distinct AZ voters
- tier policy prevents paid tenants from being placed in economy/preemptible
  domains
- tier policy allows free tenants to use economy domains
- deployment profile policy prevents paid production tenants from being placed
  in single-AZ domains unless explicitly allowed
- single-AZ profile places voters on distinct nodes in one AZ
- multi-AZ profile places voters across distinct AZs when enough AZs exist
- preemptible capacity is not selected for stable-tier voters unless explicitly
  allowed
- split/merge refuses cross-cell and cross-tenant dedicated combinations
- tenant policy changes fence stale routing epochs
- route decisions include cell and AZ information where useful
- SlateDB remains covered by `KvStore` contract tests only, not production
  cluster claims

### Real-Process Cluster Smoke

The existing real-cluster smoke harness now seeds a cell, labels nodes with
distinct AZs, verifies those labels through admin status, exercises ordinary
appends, forces Raft catch-up, and can run owner-failover or deterministic
single-node chaos scenarios. Extend it next with:

- process restart from persisted raft-engine and KV directories
- explicit tablet replacement flow
- split under active writes
- merge after split
- two-cell routing and blast-radius checks
- AZ failure simulation for multi-AZ profiles

### Chaos Tests

Run bounded chaos in nightly validation:

- randomly kill/restart one node at a time while preserving quorum
- inject delayed, dropped, and reordered Raft messages
- pause the scheduler leader
- restart the scheduler leader
- make a follower lag, then catch it up with snapshot transfer
- fail object storage during snapshot/archive paths
- fail metadata reads/writes briefly and verify clear degradation behavior

### Soak And Performance Tests

Run multi-hour workloads with:

- many tenants
- free shared-pool tenants
- paid dedicated tenants
- economy/preemptible free-tier domains
- single-AZ cluster domains
- multi-AZ standard domains
- standard stable paid-tier domains
- one large tenant spread across many tablets
- hot sessions
- append-and-fetch agent loops
- split/merge enabled by policy thresholds
- node replacement while traffic continues

Track:

- acknowledged write loss: must be zero
- duplicate event sequences: must be zero
- session sequence gaps after successful writes: must be zero
- p99 and p999 append latency
- failover duration
- catch-up duration
- split/merge duration
- movement queue age
- raft applied lag
- retained log debt
- compaction debt
- per-cell error rate

## Implementation Plan

Treat the work as two tracks. The first track establishes blast-radius
boundaries without changing the hot storage engine. The second track changes
physical tablet storage and lifecycle operations after the placement model is
observable.

### Track A: Isolation Domain And Placement

Goal: make tenant placement, deployment profile, tier, priority, cell, and AZ
visible and enforceable while keeping the current storage layout.

1. Add metadata fields:
   - `cells.cell_id`
   - `cells.domain_class`
   - `cells.deployment_profile`
   - `cells.capacity_class`
   - `nodes.cell_id`
   - `nodes.capacity_class`
   - `tenant_policies.cell_id`
   - `tenant_policies.tier`
   - `tenant_policies.priority`
2. Keep one shared metadata database. Scope placement queries and mutations by
   `cell_id`; do not create per-cell metadata databases in this cut.
3. Update routing snapshots so tenant policy resolves `cell_id` before
   placement scope and tablet lookup.
4. Update scheduler/rebalance rules:
   - never place tablets outside their cell
   - never place paid-or-higher tenants into economy/preemptible domains unless
     explicitly overridden
   - support `single_az_cluster` by placing voters on distinct nodes in one AZ
   - support `multi_az_standard` by placing voters across AZs when available
5. Expose cell, deployment profile, tier, priority, capacity class, AZ, and
   AZ-failure-tolerance status in admin APIs and `agentdbctl`.
6. Extend the real-cluster smoke harness:
   - seed one single-AZ economy cell
   - seed one multi-AZ standard cell
   - prove paid tenants do not land in the economy cell
   - prove a node failure in one cell does not break writes in the other cell

Exit criteria:

- Same-cell placement invariants are unit-tested.
- Tier/domain compatibility is unit-tested.
- Single-AZ and multi-AZ placement profiles are unit-tested.
- Real-process smoke proves basic cross-cell blast-radius behavior.
- No physical storage-engine migration is required.

### Track B: Tablet-Local Physical Storage

Goal: make tablet storage domains align with backup, restore, migration,
changefeed, compaction, and split/merge lifecycle.

1. Introduce a `TabletStoreLifecycle` abstraction with optional engine
   acceleration hooks:
   - checkpoint
   - restore/ingest checkpoint
   - ingest SST or bulk files when supported
   - delete range when safe
   - local clone/reflink when available
   - manual compaction
2. Prototype one physical layout:
   - grouped RocksDB per tablet set with bounded open domains first
   - RocksDB per tablet only as an explicit `tablets_per_domain = 1`
     high-isolation mode
3. Add tablet-store manifests:
   - tablet id
   - cell id
   - placement scope
   - tablet epoch
   - latest applied index
   - latest snapshot index
   - checksum and size summary

   Current implementation status: `TabletStoreManifest` is serializable and
   checkpoint creation fills `checksum_sha256` and `size_bytes` from a
   deterministic walk of the checkpoint directory. The runtime backup path can
   populate `latest_applied_index` for local tablet checkpoints. When shared
   metadata is configured, online backup enriches local tablet checkpoint
   manifests with `cell_id`, `placement_scope_id`, `tablet_epoch`, and the
   latest retained snapshot index.
4. Implement tablet-aware backup/restore:
   - checkpoint one tablet-local domain
   - restore into a new local domain
   - replay committed Raft tail
   - verify applied index before routing

   Current implementation status: the storage lifecycle can checkpoint and
   restore a single-tablet grouped RocksDB domain when
   `tablets_per_domain = 1`. Restore refuses to overwrite a non-empty
   destination or a domain currently open in the node process.
   `agentdbctl backup create --include-tablet-checkpoints` can include local
   tablet checkpoint manifests in an online backup when the node uses this
   single-tablet-domain layout. Restore validation checks referenced checkpoint
   paths, sizes, and checksums. `agentdbctl restore install-tablet-checkpoint`
   provides an offline local install path for stopped nodes. Core tests cover
   opening a restored tablet store against a log with post-checkpoint entries
   and replaying that tail before serving. Because there are no production users
   to migrate, no prefix-to-grouped online migration path is required.
5. Preserve changefeed semantics:
   - changefeed cursor includes cell, placement scope, tablet, epoch, and
     applied index
   - restore can include or rebuild tablet-local changefeed state
   - split/merge/migration publish cutover epochs without duplicate events

   Current implementation status: routed watch requests run inside the tablet
   context and emitted changefeed items include `cell_id`,
   `placement_scope_id`, `tablet_id`, `tablet_epoch`, and `applied_index` when
   the node can resolve them. Cursor strings are versioned and include the same
   provenance. Split/merge publish cutover epochs remain higher-level lifecycle
   work.
6. Benchmark against the current prefix-scoped layout:
   - append p50/p95/p99/p999
   - write amplification estimate
   - compaction activity and debt
   - checkpoint time
   - restore and catch-up time
   - open file and memory overhead

   Current implementation status: `agentdb-bench tablet-store` provides a
   local storage-layout microbenchmark for `prefix_scoped` and
   `grouped_rocksdb`. It reports append throughput, p50/p95/p99/p999/max latency,
   logical write bytes, a physical-byte/write-amplification estimate, RocksDB
   domain metrics, and checkpoint duration/size/checksum when a single-tablet
   domain checkpoint is requested. For single-tablet grouped domains it also
   times offline local restore into a fresh store root. Admin scheduler status
   exposes open tablet-store domain metrics, including memtable bytes, level-0
   files, pending compaction bytes, running compactions/flushes, and background
   errors. `agentdbctl tablet-store compact --tablet-id <id>` exposes the
   manual compaction hook for local restore/heavy-churn maintenance. On Linux
   the benchmark samples process open file descriptors and resident memory
   before writes, after writes, and after checkpointing. The real-cluster smoke
   harness reports Raft catch-up request duration,
   attempted/sent/skipped counts, and convergence wait time after the catch-up
   snapshot. Online split/merge routing cutover hardening remains higher-level
   control-plane work.

Exit criteria:

- One tablet can be backed up and restored into a new tablet-local store.
- Restored tablet can replay Raft tail and serve after verification.
- Changefeed replay remains ordered and non-duplicating across restore.
- Compaction and storage metrics are visible per tablet/domain.
- New deployments can run grouped tablet-store domains directly; no
  prefix-scoped migration path is required before production testing.

## Rollout Phases

### Phase 1: Cell-Aware Metadata And Placement

- Add `cells` metadata.
- Add `cell_id` to nodes, placement scopes, and tenant policies.
- Add domain class, deployment profile, capacity class, tenant tier, and tenant
  priority metadata.
- Keep one shared metadata backend by default; use logical `cell_id`
  partitioning rather than creating per-cell databases.
- Make routing resolve tenant `cell_id`.
- Make scheduler/rebalance refuse cross-cell and tier-incompatible placement.
- Add AZ-aware replica placement checks.
- Add single-AZ profile checks that use distinct nodes and expose no
  AZ-failure tolerance.
- Expose cell and AZ in admin status.
- Expose tier, priority, domain class, deployment profile, and capacity class in
  admin status.

### Phase 2: Cell-Aware Test Harness

- Keep the real-cluster smoke harness seeding cells and distinct AZ labels.
- Add restart and explicit replacement scenarios on top of existing owner-down,
  node-down chaos, and catch-up coverage.
- Add a two-cell smoke scenario with assertions that unaffected cells keep
  serving when one cell loses a node.
- Add nightly chaos with deterministic seeds.

### Phase 3: Tablet-Local Applied-State Isolation

- Use bounded grouped RocksDB tablet-store domains as the default cluster
  physical layout.
- Use `tablets_per_domain = 1` only for explicit high-isolation
  backup/restore testing.
- Benchmark grouped domains with `agentdb-bench tablet-store`, including
  latency, checkpoint/restore time, resource usage, write-amplification
  estimates, and RocksDB domain metrics.
- Track online split/merge routing cutover hardening separately from initial
  no-existing-user deployments.

### Phase 4: Operational Workflows

- Document and test add-node, drain-node, replace-node, rolling restart, and
  rolling upgrade per cell.
- Use `agentdbctl cells` and `agentdbctl tenant-policies` for cell status and
  tenant placement inspection/updates.
- Expand alert/runbook coverage beyond the initial cluster alert triage for
  quorum loss, lagging replicas, stale routing, tablet-store pressure, and
  checkpoint/snapshot failures.

### Phase 5: Tenant Migration

- Design and implement explicit tenant movement between cells.
- Use snapshot plus committed-tail catch-up.
- Fence source placement before policy cutover.
- Keep rollback and verification steps operator-visible.

## Production Readiness Criteria

Cluster mode should not be called production-supported until:

- one-node failure is covered by deterministic and chaos tests
- one-AZ failure behavior is documented and tested for supported topologies
- cell-scoped blast radius is enforced in scheduler and routing
- tenant placement class invariants are enforced
- split/merge under active writes has real-process coverage
- replacement/catch-up from snapshots has real-process coverage
- operator runbooks exist for add, drain, replace, restart, and upgrade
- alerts exist for leaderless tablets, lagging replicas, stale owners,
  movement backlog, snapshot failures, and metadata degradation
- SlateDB is clearly documented as optional/evaluation for runtime metadata,
  not the hot production commit path

## Remaining Hardening Cut

The remaining useful cut is no longer metadata migration or prefix-layout
migration. The next work should focus on real-process lifecycle validation:

1. Add a two-cell smoke topology with separate placement scopes and tablet
   owners per cell. This first needs a tenant bootstrap path that can create a
   tenant directly in a non-default cell before normal tenant-id routing exists.
2. Prove writes in cell B continue while cell A loses an owner, restarts a
   process, or runs an explicit replacement.
3. Add split and merge under active writes with routed watch cursor assertions
   across the cutover epoch.
4. Add an AZ-failure simulation for multi-AZ cells and document the expected
   single-AZ profile behavior.
5. Add replacement-from-checkpoint coverage in the real-cluster smoke path.
6. Expand runbooks and alerts for movement backlog, metadata degradation,
   object-store degradation, rolling upgrades, and two-cell blast-radius drills.

This cut hardens the distributed cluster behavior around the Track A and Track
B primitives already in place without introducing per-cell metadata databases or
per-tablet RocksDB domains by default.
