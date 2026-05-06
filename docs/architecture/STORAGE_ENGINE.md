# AgentDB Storage Engine Architecture

Status: design note

This document describes the current AgentDB storage engine and the target
direction for improving durability, scalability, and LSM-tree behavior.

## Summary

The current storage engine is a single-node runtime store behind the `KvStore`
trait. It supports local RocksDB and an optional SlateDB-backed object-store
mode. This is enough for prototype and evaluation workloads, but it is not the
right production hot-write architecture for a multi-node AgentDB cluster.

The target architecture is a replicated tablet store:

- Writes commit through a local replicated log before the client receives
  success.
- Paid and isolation-sensitive tenants use dedicated tablets that belong to
  exactly one tenant.
- Free or low-SLA tenants may use shared pool tablets under an explicit
  placement policy.
- Each tablet owns a small local LSM tree or equivalent applied-state store.
- Large tenants are split across virtual tablets instead of growing one large
  tree.
- Object storage is used asynchronously for sealed history, artifacts,
  snapshots, and archives, not as the synchronous append path.
- Local indexes live with the tablet; global indexes are async, partitioned
  projections fed by the committed log/changefeed.

This direction addresses two problems at once: data safety requires quorum
replication between nodes, while LSM write amplification and compaction overhead
require keeping individual trees small and isolated.

## Current Storage Engine

AgentDB currently depends on the `KvStore` abstraction in
`crates/agentdb-storage`. The runtime stores metadata, session events, searchable
records, idempotency keys, and index outbox rows as tenant-scoped logical keys.

The current backends are:

- `RocksKvStore`: local RocksDB runtime metadata and event storage.
- `SlateKvStore`: optional SlateDB runtime metadata over an `object_store`
  backend.
- `LocalFsContentBackend`: local artifact/content bytes.
- `ObjectStoreContentBackend`: S3-compatible artifact/content bytes.
- `InMemoryContentBackend`: test and in-memory runtime support.

The current key layout is tenant-first:

```text
event/{tenant_id}/{session_id}/{seq}
record/session/{tenant_id}/{session_id}
record/searchable/{tenant_id}/{record_id}
record/searchable_index_outbox/{tenant_id}/{commit_seq}/{record_id}
```

This layout makes tenant-scoped prefix scans straightforward. Tenant identity is
still an internal routing, policy, quota, and storage boundary; SDK callers
should not need to pass `tenant_id` directly in ordinary application code.

The target physical storage model should support two placement classes.
Dedicated placement preserves a one-tenant-per-tablet boundary. Shared placement
is a cost optimization for free or low-SLA tenants and must be governed by a
separate policy.

## Current Write Path

The current hot path is single-node:

```text
API request
  -> authenticate and resolve internal tenant scope
  -> runtime command
  -> KvStore write_batch / put
  -> update local runtime state and outbox rows
  -> return success
```

For RocksDB, durability comes from the local RocksDB write path. For SlateDB,
runtime metadata is written through SlateDB to object storage. Artifact bytes
may be written to local filesystem or object storage through `ContentBackend`.

This has several important limitations:

- There is no multi-node consensus on the hot write path.
- A node-local RocksDB instance is not enough for node-loss durability.
- SlateDB over object storage validates the KV abstraction, but it is not a
  replacement for quorum replication.
- Synchronous object-store writes are too slow and variable for ordinary
  session append latency.
- One large LSM tree eventually concentrates write amplification, compaction
  work, and tail-latency risk.

## Role Of SlateDB

SlateDB is useful as an evaluation backend and as proof that AgentDB's storage
abstraction can target object storage. It should not be treated as the final
cluster commit mechanism.

SlateDB is a candidate for:

- testing object-store-backed runtime metadata behavior
- validating cold/archive object-store assumptions
- hardening the `KvStore` abstraction against non-RocksDB backends
- possible future use in cold, archive, snapshot, or read-mostly paths

SlateDB is not currently responsible for:

- quorum replication
- leader election
- tablet membership
- read-after-write session semantics across node failures
- isolating hot tenants into many independently compacted write domains

The production hot path should commit to replicated local state first, then
upload sealed immutable data to object storage asynchronously.

## LSM-Tree Problem

LSM trees are a good fit for write-heavy workloads, but large LSM trees create
two practical problems:

- Write amplification: one logical write can be rewritten several times as data
  moves through memtables, level-0 files, and deeper levels.
- Compaction overhead: large trees require expensive background compaction that
  can consume CPU, I/O bandwidth, cache, and object-store requests.

The wrong design is one monolithic global LSM for all tenants and all data
types. It makes unrelated tenants share compaction debt and makes hot tenants
increase tail latency for everyone.

AgentDB should instead bound the size and responsibility of each LSM tree.

## Target Architecture

The target architecture splits the storage engine into replicated tablets. A
tablet is the physical ownership unit for a range, hash bucket, or virtual
bucket of AgentDB data.

Tablet placement is policy-controlled:

```text
dedicated placement
  tenant A
    tablet A/0001
    tablet A/0002

  tenant B
    tablet B/0001

shared placement
  shared/free-pool tablet S/0001
    tenant F1
    tenant F2
    tenant F3

invalid
  shared/free-pool tablet contains paid tenant A
```

Dedicated tablets are tenant-exclusive:

```text
tenant A dedicated placement
  tablet A/0001
  tablet A/0002

tenant B dedicated placement
  tablet B/0001

invalid
  dedicated tablet X contains tenant A + tenant B
```

This dedicated-placement invariant is load-bearing for paid, enterprise, and
isolation-sensitive tenants. It keeps Raft logs, local LSM files, snapshots,
sealed segments, backup units, and compaction domains inside one tenant's
security and lifecycle boundary. Shared tablets intentionally trade some of
that physical isolation for cost efficiency and must not be used for tenants
whose policy requires tenant-scoped physical isolation.

```text
client append / write
  -> route to tablet leader
  -> propose command through Raft
  -> append durable local Raft log
  -> replicate to quorum
  -> commit index advances
  -> apply command to tablet-local state
  -> return success
  -> emit local index update, changefeed, and global-index outbox
  -> async seal/upload immutable data to object storage
```

The likely Rust components are:

- `raft-rs` for Raft consensus and commit-index behavior.
- `raft-engine` for durable local Multi-Raft log storage.
- AgentDB tablet state machine for session events, records, metadata,
  idempotency, local indexes, and changefeed emission.
- RocksDB, SlateDB, or another LSM implementation for tablet-local applied
  state, depending on the path and workload.
- Object storage for sealed segments, snapshots, archive bundles, and large
  artifact payloads.

`raft-rs` and `raft-engine` are infrastructure components, not a complete
database. AgentDB still owns tablet placement, routing, snapshots, transport,
apply workers, log compaction policy, index maintenance, and recovery.

## Split-LSM Design

Each tablet should own its own applied-state store. That store can be an LSM,
but the important properties are that it is small, independently compacted, and
aligned with the tenant's placement class.

```text
node
  dedicated tenant A / tablet 0001
    raft log group A/0001
    local LSM/state store A/0001
    local record/session indexes A/0001

  dedicated tenant A / tablet 0002
    raft log group A/0002
    local LSM/state store A/0002
    local record/session indexes A/0002

  shared free-pool / tablet 0001
    raft log group shared/0001
    local LSM/state store shared/0001
    local record/session indexes shared/0001
    tenants F1, F2, F3
```

This design reduces LSM risk in several ways:

- Compaction is isolated to one tablet instead of the whole database.
- Hot tenants can be split into more same-tenant tablets without changing the
  developer API.
- Local indexes compact with their owning data partition.
- Backpressure can be applied to a hot tablet without penalizing unrelated
  tablets.
- Tablet movement and rebalancing move bounded units of data.
- Small tablets make snapshots, catch-up, and rebuilds operationally tractable.

The logical tenant boundary remains stable. Physical placement depends on
policy. A paid tenant can map to one dedicated tablet when small and many
dedicated tablets when hot or large. Free tenants can share pool tablets until
policy, usage, or upgrade requires promotion to dedicated placement.

## Placement Classes

Placement is a policy decision, not an SDK concept.

```text
tenant_policy {
  tier: free | pro | enterprise
  placement_class: shared | dedicated
  encryption_class: pool | tenant
  max_active_tablets
  split_threshold
  retention_policy
  sla_class
}
```

Dedicated placement is for paid, enterprise, regulated, high-SLA, or
isolation-sensitive tenants:

- one tenant per tablet
- tenant-scoped encryption policy
- tenant-scoped backup, restore, export, and deletion
- independent split, merge, compaction, and rebalancing
- no shared Raft log, LSM, or snapshot with another tenant

Shared placement is for free or low-SLA tenants:

- many tenants may share one pool tablet
- paid or enterprise tenants must not be placed in shared pool tablets
- quotas and retention should be stricter
- noisy-neighbor protection is best-effort within the pool
- encryption is pool/tablet-level unless a more expensive per-tenant envelope
  scheme is added
- promotion to dedicated placement must be supported

Promotion from shared to dedicated placement should be an explicit lifecycle:

```text
free tenant in shared tablet
  -> policy changes or tenant becomes hot
  -> create dedicated tenant tablet
  -> copy or replay only that tenant's hot keys and manifests
  -> fence tenant routing epoch
  -> route new writes to dedicated tablet
  -> drain old shared-tablet writes
  -> tombstone old shared rows
  -> garbage collect old shared data after retention window
```

## Control Plane Policy And Key Metadata

Placement, quota, retention, and encryption policy should live in the control
plane. Tablet-local state should cache the policy needed for serving, but the
source of truth is the control plane.

```text
control plane
  tenant policy
  placement class
  encryption class
  quota / retention / SLA
  tenant_kek_id or shared_pool_kek_id
  active key version
  allowed old key versions
  tablet map
  tablet epoch
  replica placement

tablet / data plane
  Raft log
  local LSM state
  session events
  records
  local indexes
  manifests
  encrypted payloads
  applied policy and key-version references
```

The control plane should store key references and encryption metadata, not raw
key material. Raw key material belongs in KMS or a dedicated key-management
service.

```text
control plane stores
  kms_key_id
  kek_id
  active_key_version
  wrapped_dek metadata
  encryption policy version

KMS stores or protects
  tenant KEK
  shared pool KEK
  root key material

tablet/object storage stores
  ciphertext
  encryption context
  key version reference
  wrapped DEK when required
```

Control-plane policy records should be versioned. Tablet writes should execute
against an explicit policy and tablet epoch so stale routing, stale placement,
or stale encryption metadata can be rejected or refreshed.

```text
write command includes
  tenant_id
  tablet_id
  tablet_epoch
  placement_class
  encryption_policy_version
  key_version
```

This split keeps placement and key-rotation decisions auditable while avoiding
secret key material in Raft logs, local LSM values, snapshots, or object-store
manifests.

## Tenant-Scoped Encryption

Dedicated tablets simplify encryption and deletion. AgentDB can use a
tenant-scoped key hierarchy without mixing encryption domains inside the same
Raft log, LSM file, snapshot, or sealed segment.

```text
tenant_kek_id
  -> tablet_dek_id
       raft log encryption
       local LSM encryption
       tablet snapshot encryption

  -> segment_dek_id
       sealed session segment encryption
       archive bundle encryption
       artifact blob encryption
```

Dedicated tablet lifecycle must preserve the encryption boundary:

- Creating a tablet chooses the tenant first, then the tablet bucket/range.
- Splitting a tablet creates child tablets with the same `tenant_id`.
- Merging tablets is allowed only when all source tablets have the same
  `tenant_id`.
- Moving or rebalancing a tablet moves one tenant-owned replica set.
- Snapshots and sealed objects remain encrypted under the owning tenant's key
  policy.
- Tenant key rotation and crypto-shredding never need to rewrite or inspect
  another tenant's data.

Shared pool tablets use a different encryption class:

```text
shared_pool_kek_id
  -> shared_tablet_dek_id
       shared tablet Raft log encryption
       shared tablet LSM encryption
       shared tablet snapshot encryption
```

This is cheaper, but it does not provide the same physical key isolation as a
dedicated tablet. If a tenant requires per-tenant key rotation,
crypto-shredding, restore, or export guarantees, the tenant should be promoted
to dedicated placement.

## Index Placement

AgentDB should keep the DynamoDB-like distinction already established in the
record index design:

- Local index: tablet-local, strongly tied to the tablet's committed base data,
  and used when the query includes the shard/routing key.
- Global index: partitioned async projection across tablets, eventually
  consistent, and used when the query does not include a shard/routing key.

For writes, the tablet commit is the source of truth. Local index state is
updated from the same committed command or from a tablet-local replayable outbox.
Global index workers consume committed changefeed/outbox rows and update a
separate partitioned index asynchronously.

Read-after-write in the same session should be guaranteed by the tablet leader's
committed state and session read context, not by waiting for the global index to
catch up.

## Object Storage Role

Object storage should sit behind the hot path:

- sealed session segments
- archive bundles
- large artifact bytes
- Raft or LSM snapshots
- cold read-mostly projections
- backup and restore material

Object storage should not be required before acknowledging ordinary session
appends. Its latency and tail behavior are not appropriate for the synchronous
commit path.

The safe pattern is:

```text
quorum commit first
  -> acknowledge client
  -> background seal/upload
  -> verify content checksum
  -> persist manifest / snapshot metadata
  -> garbage collect old local state after policy allows
```

## Consistency Model

The target storage engine should provide:

- Linearizable writes within a tablet.
- Read-after-write in the same connection/session by routing to the tablet
  leader or a replica that has applied at least the required commit index.
- Eventually consistent global indexes.
- Strong local-index reads when the query includes the tablet/shard key.
- Durable recovery after node failure through Raft log replay and snapshots.

Cross-tablet writes should not be hidden behind an implicit two-phase commit in
the initial design. Most AgentDB operations should be scoped to one tablet. If a
future feature needs atomic multi-tablet behavior, it should be explicit and
designed separately because it changes latency, failure modes, and operational
complexity.

## Implementation Plan

1. Define placement-aware tablet identity and routing.

   Introduce stable physical tablet ids for both dedicated and shared placement.
   Route by internal tenant scope, placement class, and shard key or virtual
   bucket. Keep this internal to the server and SDK adapters.

2. Add control-plane policy and key metadata.

   Store tenant policy, placement class, encryption class, key references, key
   versions, tablet map, tablet epoch, and replica placement in the control
   plane. Store references and wrapped-key metadata only; raw key material stays
   in KMS.

3. Separate tablet-local runtime state.

   Stop treating the runtime KV store as one global write domain. Add a
   tablet-local storage interface so each tablet can own its own applied state,
   local indexes, and replay cursor.

4. Add replicated log abstraction.

   Introduce a `ReplicatedLog` boundary before the applied state machine. Start
   with a single-node implementation that preserves current behavior, then add a
   Raft implementation using `raft-rs` and `raft-engine`.

5. Move append/write commands through the tablet state machine.

   Model appends, record writes, idempotency rows, local-index updates, and
   global-index outbox entries as deterministic commands applied after commit.

6. Add quorum commit before acknowledgement.

   In clustered mode, acknowledge writes only after the Raft group reaches the
   commit index for the command and the local apply path has made it visible to
   the session read context.

7. Add snapshot and catch-up support.

   Snapshot tablet-local applied state, upload snapshots asynchronously, and use
   Raft log replay plus snapshots for follower catch-up and node replacement.

8. Add tablet split and rebalance.

   Split hot or large dedicated tablets by same-tenant virtual bucket/range.
   Split shared pool tablets by pool bucket/range and tenant distribution.
   Rebalance tablets across nodes based on write rate, compaction debt, storage
   size, and read pressure. Never move paid or enterprise tenants into shared
   pool tablets.

9. Add shared-to-dedicated promotion.

   Support policy-driven promotion when a free tenant upgrades, becomes hot, or
   requires tenant-scoped encryption and lifecycle guarantees.

10. Tune LSM compaction per workload.

   Keep append-heavy session logs, mutable metadata, and read-heavy indexes in
   separate column families or stores when needed. Use tiered/universal
   compaction for append-heavy logs and leveled compaction for read-heavy
   indexes only where it pays off.

11. Make object-store upload asynchronous.

   Move sealed segments, snapshots, archive bundles, and large content objects
   through background workers with checksums, retry, and manifest updates.

12. Expose operational signals.

    Add metrics and admin surfaces for tablet size, write rate, Raft lag,
    applied index, compaction debt, global-index lag, snapshot age, and
    object-store upload backlog.

## Non-Goals

- Do not make object storage the synchronous commit path for hot appends.
- Do not build one database-wide LSM tree.
- Do not place paid, enterprise, regulated, or tenant-key-isolated tenants in
  shared pool tablets.
- Do not allow a dedicated tablet, dedicated Raft group, dedicated local LSM,
  dedicated snapshot, or dedicated sealed tablet segment to contain data from
  multiple tenants.
- Do not expose physical tablet ids or internal tenant ids as normal SDK
  concepts.
- Do not add implicit cross-tablet two-phase commit for ordinary session
  operations.
- Do not claim clustered production safety until quorum replication, snapshots,
  catch-up, and operational recovery are implemented.

## Open Decisions

- Dedicated tablet keying inside one tenant: hash bucket, range partition, or
  hybrid virtual bucket layout.
- Shared pool tablet keying across free tenants: pool hash bucket, tenant bucket,
  or hybrid layout.
- Applied-state backend: RocksDB per tablet, shared RocksDB with column families,
  SlateDB for selected paths, or another local LSM implementation.
- Snapshot format and snapshot cadence.
- Compaction strategy by data type.
- Global-index partitioning and repair strategy.
- Follower-read policy and session read-context routing.
- Membership change policy and operator controls.
- KMS integration model, key cache behavior, and key-rotation workflow.
