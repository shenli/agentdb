# Cluster Deployment

This document describes the **target** deployment model for a real multi-node
AgentDB cluster.

It is intentionally forward-looking. It explains how AgentDB **should** be
deployed once clustered operation becomes a supported product surface. It is
not a claim that the repository already ships a production-ready cluster mode.

For the current support boundary, see [CLUSTER_STATUS.md](CLUSTER_STATUS.md).
For human and AI-agent operations, use [agentdbctl](AGENTDBCTL.md) by default.
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
agentdbctl nodes register \
  --node-id node-a \
  --address http://10.0.0.10:7878 \
  --region us-west-2 \
  --availability-zone us-west-2a

agentdbctl tablets create 0
agentdbctl tablets rebalance --dry-run
agentdbctl tablets rebalance
agentdbctl status
```

Use `agentdbctl raw ...` only for endpoints that are not modeled by the CLI
yet.

## Node Configuration Model

Each node should have:

### Shared cluster settings

- cluster or deployment name
- control-plane metadata DSN
- object-store configuration
- lease timing
- heartbeat timing
- rebalance movement cap (`AGENTDB_REBALANCE_MAX_MOVEMENTS`; `0` disables
  automatic movement)
- tablet lifecycle thresholds:
  `AGENTDB_TABLET_LIFECYCLE_SPLIT_MAX_SHARDS`,
  `AGENTDB_TABLET_LIFECYCLE_MERGE_MAX_SHARDS`, and
  `AGENTDB_TABLET_LIFECYCLE_MAX_ACTIONS`; split/merge preparation is disabled
  unless the split or merge threshold is set above `0`
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

- [CLUSTER_STATUS.md](CLUSTER_STATUS.md) - current support boundary
- [CLUSTER_OPERATIONS.md](CLUSTER_OPERATIONS.md) - target day-2 cluster
  procedures
- [AGENTDBCTL.md](AGENTDBCTL.md) - recommended CLI for human and AI-agent
  operations
- [DEPLOYMENT.md](DEPLOYMENT.md) - supported deployment choices today
- [../architecture/ARCHITECTURE.md](../architecture/ARCHITECTURE.md) - current
  implementation shape
