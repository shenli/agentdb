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

For human and AI-agent operations, use [agentdbctl](AGENTDBCTL.md). The
endpoint names below document the backend API surface for precision; normal
operator workflows should use the CLI equivalents first:

```bash
agentdbctl status
agentdbctl nodes list
agentdbctl tablets list
agentdbctl drain status
```

The repository now has cluster-facing visibility endpoints:

- `GET /api/admin/scheduler`
- `GET /api/admin/drain`
- `GET /api/admin/nodes`
- `GET /api/admin/tablets`
- `GET /api/admin/routing`
- `GET /api/admin/routing/decision`
- `GET /api/admin/raft`
- `GET /api/admin/cluster/health`
- `GET /api/admin/cluster/readiness`

And a very small amount of direct metadata admin:

- `agentdbctl drain start` for one-way local drain activation
- `agentdbctl nodes register ...` for explicit node registration/upsert
- `agentdbctl nodes delete <node_id>` for operator cleanup of a stale node row
- `agentdbctl nodes maintenance <node_id> alive|draining|maintenance` for node
  maintenance state; setting the local node to `alive` clears the local drain
  flag
- `agentdbctl tablets create <tablet_id>` for idempotent tablet metadata
  creation
- `agentdbctl tablets assign <tablet_id> <node_id>` for pre-claiming an
  unowned tablet on a fresh `alive` node
- `agentdbctl tablets relinquish <tablet_id>` for force-clearing current
  ownership and fencing the old epoch
- `agentdbctl tablets handoff <tablet_id> <target_node_id>` for cooperative
  ownership movement
- `agentdbctl tablets complete-handoff <tablet_id> <expected_source_node_id>`
  for source-owner handoff acknowledgement
- `agentdbctl tablets cancel-handoff <tablet_id>` for retracting a handoff
  signal
- `agentdbctl tablets prepare-replacement <tablet_id> <target_node_id>` to
  register a replacement target as a learner and mark it as the handoff target
- `agentdbctl tablets complete-replacement <tablet_id> <target_node_id>` to
  promote a healthy, caught-up replacement target to owner; use
  `--min-applied-index` when source replica progress is unavailable and
  `--retire-source` when the old owner should be removed from replica metadata
- `agentdbctl tablets rebalance --dry-run` and `agentdbctl tablets rebalance`
  for assigning unowned tablets, moving tablets away from fresh draining nodes,
  and evening out fresh `alive` node load
- `agentdbctl raft status` for local Raft group status, configured replica
  progress, replication lag, and catch-up-needed visibility
- `agentdbctl raft catch-up --dry-run` for previewing snapshot catch-up
  candidates, and `agentdbctl raft catch-up` for manually sending snapshots to
  lagging voter/learner replicas
- `agentdbctl raft snapshots` for retained snapshot metadata, including age,
  expiry, size, checksum, source node, target node, and storage URI when a
  retained object exists
- `agentdbctl raft prune-snapshots --dry-run` and
  `agentdbctl raft prune-snapshots` for retention cleanup of expired retained
  metadata-only snapshots; object-backed retained snapshots are deleted by the
  server prune worker before their metadata is marked deleted
- retained object-backed Raft snapshots can be consumed by the server restore
  worker when a local replica is behind and has a matching retained snapshot
- `agentdbctl backup create ...` for an online backup manifest and optional
  metadata snapshot
- `agentdbctl restore validate <manifest-path>` for backup manifest validation;
  online destructive restore apply is intentionally not supported

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
- [AGENTDBCTL.md](AGENTDBCTL.md) - recommended CLI for human and AI-agent
  operations
- [CLUSTER_DEPLOYMENT.md](CLUSTER_DEPLOYMENT.md) - target cluster topology and
  bootstrap model
- [CLUSTER_OPERATIONS.md](CLUSTER_OPERATIONS.md) - target day-2 cluster
  procedures
- [../architecture/ARCHITECTURE.md](../architecture/ARCHITECTURE.md) - current
  implementation shape
