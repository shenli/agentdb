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
- explicit tablet ownership admin primitives
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
- automatically rebalance tenants, sessions, or placements
- perform a rolling restart
- perform a rolling upgrade
- recover a partially failed cluster

If you are looking for those workflows, the answer today is:

- there is no canonical procedure in this repo
- there is no supported SLA for them
- you should not infer support from prototype routing/admin endpoints

## What Is Implemented Now

The repository now has cluster-facing visibility endpoints:

- `GET /api/admin/scheduler`
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
- `POST /api/admin/nodes/{node_id}/maintenance` for marking nodes `alive`,
  `draining`, or `maintenance`; setting the local node to `alive` clears the
  local drain flag
- `POST /api/admin/tablets/create` for idempotent tablet metadata creation
- `POST /api/admin/tablets/{tablet_id}/assign` for pre-claiming an unowned
  tablet on a fresh `alive` node
- `POST /api/admin/tablets/{tablet_id}/relinquish` for force-clearing current
  ownership and fencing the old epoch
- `POST /api/admin/tablets/{tablet_id}/handoff` for cooperative ownership
  movement
- `POST /api/admin/tablets/{tablet_id}/handoff/complete` for source-owner
  handoff acknowledgement
- `POST /api/admin/tablets/{tablet_id}/handoff/cancel` for retracting a handoff
  signal
- `POST /api/admin/tablets/rebalance` for a dry-run/apply pass that assigns
  unowned tablets, moves tablets away from fresh draining nodes, and evens out
  fresh `alive` node load
- `POST /api/admin/backups` for an online backup manifest and optional
  metadata snapshot
- `POST /api/admin/restores` for backup manifest validation; online destructive
  restore apply is intentionally not supported

These help operators and developers inspect control-plane state and clean up
obviously-bad metadata rows. They also provide the first useful node
maintenance path: drain the local node, mark tablet handoffs, observe movement,
and refuse new mutations on the drained node. They still do **not** add the full
cluster control plane or all day-2 workflows described in the target-state docs.

The server also now runs a small amount of real control-plane background work
when metadata-backed cluster mode is enabled:

- periodic node heartbeat/upsert
- singleton scheduler-leader lease acquisition/renewal
- stale-node detection (`alive` -> `dead`)
- expired-tablet ownership clearing
- automatic tablet rebalance attempts that assign unowned tablets, move fresh
  draining owners through cooperative handoff, and even out fresh `alive` node
  load

This is the first live convergence loop in the product, but it is still a
small subset of the eventual cluster control plane.

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
