# Cluster Status

This document explains the current state of multi-node and cluster operation in
AgentDB.

## Short Version

AgentDB does **not** currently have a supported, documented, user-facing
cluster deployment mode.

There is distributed-design and routing work in the repository, but there is
not yet a stable operator contract for running a real AgentDB cluster.

## What Exists Today

The codebase and docs already contain pieces that point toward future
distributed operation:

- routing status and routing decision endpoints
- cluster health and readiness summary endpoints
- control-plane metadata helpers
- design work for placement/routing/tablets
- runbooks that mention boxes/tablets and future cluster behavior
- performance notes from prototype multi-box experiments

Those pieces are useful for development direction, but they are **not** the
same thing as a supported cluster product.

## What Is Not Yet Supported

These are not currently documented as stable operator procedures:

- bootstrap a multi-node cluster
- add a node
- remove a node
- replace a failed node
- drain a node
- rebalance tenants, sessions, or placements
- perform a rolling restart
- perform a rolling upgrade
- recover a partially failed cluster

If you are looking for those workflows, the answer today is:

- there is no canonical procedure in this repo
- there is no supported SLA for them
- you should not infer support from prototype routing/admin endpoints

## What Is Implemented Now

The repository now has some read-only cluster-facing visibility endpoints:

- `GET /api/admin/drain`
- `GET /api/admin/nodes`
- `GET /api/admin/tablets`
- `GET /api/admin/routing`
- `GET /api/admin/routing/decision`
- `GET /api/admin/cluster/health`
- `GET /api/admin/cluster/readiness`

And a very small amount of direct metadata admin:

- `POST /api/admin/drain` for one-way local drain activation
- `POST /api/admin/nodes` for explicit node registration/upsert
- `DELETE /api/admin/nodes/{node_id}` for operator cleanup of a stale node row

These help operators and developers inspect control-plane state and clean up
obviously-bad metadata rows, and they provide the first step of planned node
maintenance by refusing new mutations on a drained node. They still do **not**
add the full cluster control plane or day-2 workflows described in the
target-state docs.

## Kubernetes Clarification

Running the single-node Helm chart on a Kubernetes cluster is **not** the same
thing as running an AgentDB cluster.

Today the Helm chart is:

- one StatefulSet
- one AgentDB replica
- one persistent volume

That is still single-node AgentDB, just packaged for Kubernetes evaluation.

## What A Real Cluster Guide Would Need

Before AgentDB can claim cluster deployment support, the docs would need to
cover at least:

### Topology

- cluster membership model
- placement / shard ownership model
- shared metadata and lease model
- routing and failover behavior

### Day-2 Operations

- add node
- remove node
- replace node
- drain node
- start / stop node
- rolling upgrade
- rollback

### Failure Handling

- node lost
- split brain / lease loss
- stale routing metadata
- object-store or metadata-store outage
- rebuild or rebalance behavior

### Operator Visibility

- who owns what
- what is rebuilding
- what is stale
- what is draining
- what traffic is degraded

AgentDB is not there yet as a user-facing product.

## Recommended Wording For Users

The honest operational message today is:

- **supported**: single-node AgentDB
- **evaluation only**: single-node AgentDB packaged with Docker, Compose, or
  Helm
- **future work**: true multi-node clustered AgentDB

Forward-looking target-state docs exist for that future work:

- [CLUSTER_DEPLOYMENT.md](CLUSTER_DEPLOYMENT.md)
- [CLUSTER_OPERATIONS.md](CLUSTER_OPERATIONS.md)

Those documents describe the intended operator model, not a current support
claim.

## Related Docs

- [DEPLOYMENT.md](DEPLOYMENT.md) - deployment choices and current support
- [OPERATIONS.md](OPERATIONS.md) - day-2 single-node operations
- [CLUSTER_DEPLOYMENT.md](CLUSTER_DEPLOYMENT.md) - target cluster topology and
  bootstrap model
- [CLUSTER_OPERATIONS.md](CLUSTER_OPERATIONS.md) - target day-2 cluster
  procedures
- [../architecture/ARCHITECTURE.md](../architecture/ARCHITECTURE.md) - current
  implementation shape
