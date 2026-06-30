# Cluster Deployment

This document describes the **target** deployment model for a real multi-node
AgentDB cluster.

It is intentionally forward-looking. It explains how AgentDB **should** be
deployed once clustered operation becomes a supported product surface. It is
not a claim that the repository already ships a production-ready cluster mode.

For the current support boundary, see [cluster-status.md](cluster-status.md).
For human and AI-agent operations, use [agentdbctl](agentdbctl.md) by default.
Raw admin endpoints remain the backend contract and an escape hatch.

## Purpose

This document answers:

- what components belong in an AgentDB cluster
- what should be shared across nodes
- what should stay node-local
- what topology should operators expect
- what a real cluster bootstrap flow should look like

## Target Cluster Shape

The intended cluster shape is:

```text
clients / SDKs / adapters
        |
        v
   load balancer / gateway
        |
        +-------------------------------+
        |                               |
        v                               v
   agentdb node A                  agentdb node B          ... more nodes
   - HTTP API                      - HTTP API
   - dashboard/admin               - dashboard/admin
   - hot cache / NVMe              - hot cache / NVMe
   - placement workers             - placement workers
        |                               |
        +---------------+---------------+
                        |
                        v
                 shared control plane
                 - node membership
                 - tablet / placement ownership
                 - leases / heartbeats
                 - session locator metadata
                        |
                        v
                 shared durable storage
                 - object storage for cold/archive/artifacts
                 - object-store-backed runtime metadata
                   or another shared runtime backend
```

## Design Intent

A real AgentDB cluster should separate:

- **hot execution locality**
  - node-local CPU, DRAM, and NVMe
- **shared coordination**
  - membership, ownership, leases, routing metadata
- **shared durable truth**
  - object storage and shared runtime metadata

This matches the larger AgentDB direction:

- warm working set stays close to the node
- cold/archive data lives cheaply and durably in object storage
- coordination is narrow and metadata-scoped rather than a heavy global data
  path

## Cluster Components

### 1. AgentDB Nodes

Each node runs the normal AgentDB server process plus clustered control-plane
loops:

- API serving
- routing / forwarding
- lease renewal
- heartbeat
- tablet or placement ownership
- lifecycle work for owned placements

Each node should have:

- a stable `node_id`
- a routable internal address
- a shared internal auth token for node-to-node Raft RPCs
- region / availability-zone metadata
- local fast storage for hot working set

For Track B tablet-local storage, the default cluster layout is bounded grouped
RocksDB domains. This gives the deployment a physical tablet-store boundary
without creating one RocksDB instance and memtable set per tablet:

```toml
[storage.tablet]
layout = "grouped_rocksdb"
root_dir = "/var/lib/agentdb/tablet-stores"
tablets_per_domain = 32
max_open_domains = 16
domain_write_buffer_size_mib = 16
domain_max_write_buffers = 2
domain_parallelism = 1
domain_max_background_jobs = 1
domain_max_open_files = 128
```

`tablets_per_domain = 1` is the high-isolation mode, but it opens more RocksDB
domains and therefore more memtables. Use it only when the node has explicit
memory/file-handle capacity for that placement class. It is also the mode
required by `agentdbctl backup create --include-tablet-checkpoints`, because a
checkpoint must map to exactly one tablet store.

The rough mutable-memtable cap for grouped domains is:

```text
max_open_domains * domain_write_buffer_size_mib * domain_max_write_buffers
```

The default above is about 512 MiB before RocksDB overheads. Increase it only
with matching instance memory and file descriptor limits.

After startup, use `agentdbctl status` or `GET /api/admin/scheduler` to inspect
open tablet-store domains. The status includes open domain count, memtable
bytes, level-0 file count, pending compaction bytes, running
compactions/flushes, and background errors. On AWS, check these values before
raising `max_open_domains` or lowering `tablets_per_domain`; per-tablet domains
can multiply reserved memtable memory quickly. To compact one local tablet-store
range after restore or heavy churn, run `agentdbctl tablet-store compact
--tablet-id <id>` against the node that owns that local tablet.

The AWS Terraform demo exposes these knobs as variables:
`tablet_store_tablets_per_domain`, `tablet_store_max_open_domains`,
`tablet_store_domain_write_buffer_size_mib`,
`tablet_store_domain_max_write_buffers`,
`tablet_store_domain_max_background_jobs`, and
`tablet_store_domain_max_open_files`. For AWS backup/restore experiments, start
with `tablet_store_tablets_per_domain = 1` and reduce
`tablet_store_max_open_domains` to keep the memtable budget inside the instance
memory envelope.

The same Terraform demo can now emit cell/AZ-aware node configs for AWS cluster
experiments. Set `node_count`, pass multiple `subnet_ids` across AZs, and set
`deployment_mode = "cluster"`, `cell_id`, `capacity_class`,
`internal_auth_token`, and a shared Postgres `meta_url`. The generated
`agentdb.toml` binds to the EC2 private IP and writes routing labels so node
heartbeats advertise a reachable private URL instead of `0.0.0.0`.

### 2. Shared Control Plane

The cluster needs a shared metadata plane for:

- registered nodes
- heartbeats
- tablet / placement rows
- lease epochs and expiry
- handoff targets
- session locator metadata when direct hashing is insufficient

This control plane should be small, durable, and externally observable through
admin APIs.

Operationally, this is where cluster truth lives:

- who is in the cluster
- who owns what
- who is stale
- who is draining
- what should take over next

### 3. Shared Durable Storage

Clustered AgentDB should treat durable shared storage as the retained source of
truth for:

- cold session segments
- archive bundles
- artifact blobs
- object-store-backed runtime metadata when using SlateDB-style storage

This storage must outlive any one node.

### 4. Load Balancer / Gateway

Clients should not hardcode one node.

The cluster entry point should be:

- a load balancer
- an internal gateway
- or another stable service address

That layer should route to healthy nodes and retry around draining or stale
members when possible.

## Placement / Tablet Model

The target cluster should route work by **placement** or **tablet** rather than
by whole-tenant node pinning.

Desired properties:

- one session belongs to one placement at a time
- one placement is owned by one node at a time
- one tenant may span many placements
- hot tenants therefore spread across many nodes

This avoids the "entire tenant = one hotspot" failure mode.

## Storage Expectations

### Node-Local Storage

Each node should keep:

- hot caches
- warm mutable session heads
- restore staging area
- local index/cache materializations

This is where NVMe matters.

### Shared Storage

The cluster should keep:

- cold session history
- archive bundles
- artifact content
- persistent runtime metadata not tied to one node disk

This is where object storage matters.

## Recommended Topology

### Minimum Serious Cluster

Target baseline:

- 3 nodes minimum
- one stable control-plane metadata backend
- one shared object store bucket or namespace
- one load balancer

Three nodes gives room for:

- maintenance
- ownership handoff
- one-node failure without total fleet collapse

### Availability Zones

Preferred shape:

- distribute nodes across at least 2 availability zones
- keep the control-plane metadata backend highly available
- keep object storage regional and durable

### Networking

Cluster deployments should assume:

- routable node-to-node traffic
- low enough latency for forwarding and lease renewal
- enough bandwidth that handoff / restore / catch-up is not bottlenecked by a
  weak east-west network

## Bootstrap Sequence

The target bootstrap order should be:

1. Provision the shared control-plane metadata store.
2. Provision the shared object store.
3. Provision node-local disks / NVMe.
4. Start the first AgentDB node with cluster config.
5. Register the node in the control plane.
6. Create initial tablets / placements.
7. Start additional nodes.
8. Allow ownership assignment and routing snapshots to converge.
9. Put the load balancer in front of the fleet.

The intended operator shape is CLI-first:

```bash
agentdbctl cells upsert \
  --cell-id prod-c001 \
  --region us-west-2 \
  --domain-class standard \
  --deployment-profile multi_az_standard \
  --capacity-class stable

agentdbctl nodes register \
  --node-id node-a \
  --address http://10.0.0.10:7878 \
  --cell-id prod-c001 \
  --region us-west-2 \
  --availability-zone us-west-2a \
  --capacity-class stable

agentdbctl cells list
agentdbctl tablets create 0
agentdbctl tablets rebalance --dry-run
agentdbctl tablets rebalance
agentdbctl status
```

Use `agentdbctl raw ...` only for endpoints that are not modeled by the CLI
yet.

## Recovery Timing

Do not tune all HA settings toward the same small number. AgentDB's intended
production tradeoff is:

- fast Raft election and ownership reconciliation for write-path RTO
- slower stale-node marking for dead-node cleanup, replacement, and rebalance
- bounded snapshot/log/catch-up work so recovery does not overload healthy
  nodes

With the defaults, a single owner process failure in a healthy quorum should
recover write availability in seconds to tens of seconds. The real-cluster
smoke asserts the current implementation moves ownership within 45 seconds.
The default 180-second stale-node threshold is deliberately longer and affects
when the node is marked `dead`, when stale stats are deleted, and when
replacement workflows can safely treat the source as failed. It is not the
write-path RTO.

RPO for acknowledged writes is zero while a Raft quorum survives. A client that
does not receive an acknowledgment must retry with idempotency; the write may
or may not have committed before the timeout. If a tablet loses quorum, it is
unavailable until quorum returns or an operator runs an explicit recovery
workflow.

Production tuning guidance:

- reduce `AGENTDB_RAFT_TICK_INTERVAL_MS` only when the network and host
  scheduling jitter are low enough to avoid spurious elections
- keep `AGENTDB_STALE_NODE_THRESHOLD_MS` comfortably above normal network,
  metadata, and GC pauses; lowering it improves cleanup RTO but increases
  false-dead risk
- set `[routing].refresh_interval_ms` low enough for route convergence, but
  avoid excessive metadata polling in large clusters
- treat `AGENTDB_REBALANCE_MAX_MOVEMENTS` and tablet lifecycle action limits as
  pressure controls during recovery, not just placement controls

## Node Configuration Model

Each node should have:

### Shared cluster settings

- cluster or deployment name
- control-plane metadata DSN
- object-store configuration
- Raft timing:
  `AGENTDB_RAFT_TICK_INTERVAL_MS` (default 100 ms) and
  `AGENTDB_RAFT_OWNER_CAMPAIGN_INTERVAL_MS` (default 1 second)
- routing refresh timing (`[routing].refresh_interval_ms`, default 5 seconds)
- scheduler lease/renew timing:
  `AGENTDB_SCHEDULER_LEASE_MS` (default 30 seconds) and
  `AGENTDB_SCHEDULER_RENEW_INTERVAL_MS` (default 10 seconds)
- heartbeat timing (`AGENTDB_NODE_HEARTBEAT_MS`, default 5 seconds)
- stale-node cleanup threshold (`AGENTDB_STALE_NODE_THRESHOLD_MS`, default
  180 seconds)
- ownership reaper timing:
  `AGENTDB_REAPER_TICK_MS` (default 10 seconds) and
  `AGENTDB_REAPER_SAFETY_MARGIN_MS` (default 5 seconds)
- rebalance movement cap (`AGENTDB_REBALANCE_MAX_MOVEMENTS`; `0` disables
  automatic movement)
- tablet lifecycle thresholds:
  `AGENTDB_TABLET_LIFECYCLE_SPLIT_MAX_SHARDS`,
  `AGENTDB_TABLET_LIFECYCLE_MERGE_MAX_SHARDS`,
  `AGENTDB_TABLET_LIFECYCLE_SPLIT_MAX_KEYS`,
  `AGENTDB_TABLET_LIFECYCLE_MERGE_MAX_KEYS`,
  `AGENTDB_TABLET_LIFECYCLE_SPLIT_MAX_STORAGE_BYTES`,
  `AGENTDB_TABLET_LIFECYCLE_MERGE_MAX_STORAGE_BYTES`, and
  `AGENTDB_TABLET_LIFECYCLE_SPLIT_MIN_WRITE_OPS_PER_SEC`
  (`AGENTDB_TABLET_LIFECYCLE_SPLIT_MIN_WRITE_OPS_PER_SEC_MILLI` for milli-op
  precision), `AGENTDB_TABLET_LIFECYCLE_SPLIT_MAX_COMPACTION_DEBT_OPS` for
  retained committed log entries, and `AGENTDB_TABLET_LIFECYCLE_MAX_ACTIONS`;
  split/merge preparation is disabled unless at least one split or merge
  threshold is set above `0`
- tablet lifecycle cooldown (`AGENTDB_TABLET_LIFECYCLE_COOLDOWN_MS`, default
  10 minutes) to avoid immediate repeated movement after a completed lifecycle
  group
- retained-log compaction threshold
  (`AGENTDB_TABLET_LOG_COMPACTION_MIN_DEBT_OPS`; `0` disables local log
  compaction by debt)
- routing refresh intervals
- internal Raft RPC token (`server.internal_auth_token` or
  `AGENTDB_INTERNAL_AUTH_TOKEN`)

### Node-local settings

- `node_id`
- bind address
- advertised internal address
- region / availability zone
- local data/cache paths
- resource limits

## What Cluster Readiness Should Mean

A node should only be considered ready for cluster traffic when:

- it can reach the control-plane metadata store
- it can reach the shared object store
- it has registered itself successfully
- it has current routing metadata
- it is not stale
- it is not draining

Cluster readiness is stricter than single-node `/health/ready`.

## What This Document Does Not Promise

This document does not mean:

- current open-source AgentDB already supports this end to end
- all admin APIs described by the notes are stable
- the scheduler, handoff, and rebalance logic are done

It is the target operator model, not the current product claim.

## Related Docs

- [cluster-status.md](cluster-status.md) - current support boundary
- [cluster-operations.md](cluster-operations.md) - target day-2 cluster
  procedures
- [agentdbctl.md](agentdbctl.md) - recommended CLI for human and AI-agent
  operations
- [deployment.md](deployment.md) - supported deployment choices today
- [../design/architecture.md](../design/architecture.md) - current
  implementation shape
