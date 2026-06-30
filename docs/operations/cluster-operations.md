# Cluster Operations

This document describes the **target** day-2 operator workflows for a real
AgentDB cluster.

It is forward-looking. Use it as the intended operator contract for cluster
mode, not as a claim that all workflows already exist in the current server.

For the current support boundary, see [cluster-status.md](cluster-status.md).
For the recommended human CLI, see [agentdbctl.md](agentdbctl.md).

Operator examples in this document use `agentdbctl` by default. Raw HTTP
endpoints are still the backend contract and can be reached with
`agentdbctl raw`, but humans should not need to hand-write payloads for normal
cluster work.

## Goals

A real AgentDB cluster should eventually make these workflows boring:

- add node
- drain node
- stop node
- replace node
- restart node
- rolling upgrade
- capacity expansion

The system should do this without:

- corrupting session order
- losing placement ownership clarity
- forcing operators to edit raw metadata by hand

## Core Principles

### 1. Drain Before Stop

Planned work on a node should begin with a drain step.

Drain means:

- stop accepting new owned writes
- advertise to clients and peers that the node is leaving service
- stop renewing ownership long enough for handoff to occur
- allow in-flight work to finish or fail clearly

### 2. Ownership Moves Before Process Exit

For a clean maintenance flow, tablets or placements should move away from a
node before or as that node exits.

### 3. Immutable History Minimizes Move Cost

Cold and archive history should stay in shared durable storage. Node movement
should mostly move:

- ownership metadata
- warm working set
- local caches

not rewrite retained history.

### 4. Operator Actions Must Be Observable

Operators should always be able to answer:

- which nodes are healthy
- which nodes are draining
- which placements are stale
- which placements are moving
- which sessions are rebuilding or catching up

## Node Lifecycle States

The target cluster should expose node states such as:

- `joining`
- `ready`
- `draining`
- `stale`
- `down`
- `rebuilding`

These states should be visible through admin APIs and the dashboard.

## Standard Workflows

### Add Node

Use this when:

- capacity is low
- a new AZ is being added
- a replacement host is joining the fleet

Target flow:

1. Provision the new machine or pod with node-local fast storage.
2. Start AgentDB with cluster config and a new stable `node_id`.
3. Confirm the node heartbeats into the control plane.
4. Wait for the node to become `ready`.
5. Assign or rebalance placements onto the new node.
6. Confirm routing snapshots across the fleet converge.

Expected operator checks:

- `agentdbctl nodes list`
- `agentdbctl tablets list`
- `agentdbctl status`

Current admin controls:

- `agentdbctl nodes register ...` can explicitly register or upsert a node.
- `agentdbctl tablets assign <tablet_id> <node_id>` can pre-claim an unowned
  tablet for a fresh `alive` node.
- `agentdbctl tablets rebalance --dry-run` previews assignment and movement.
- `agentdbctl tablets rebalance` assigns unowned tablets, marks load-balancing
  handoffs, and completes healthy caught-up replacements with Raft membership
  promotion/source retirement when the serving API node has the local group.
- The scheduler leader also runs the same rebalance policy automatically when
  metadata-backed cluster mode is enabled. Use `AGENTDB_REBALANCE_MAX_MOVEMENTS`
  to cap movements per scheduler tick; set it to `0` to disable automatic
  movement.

Success criteria:

- node is healthy and visible
- owned placements show the new owner
- client traffic can reach the node through the load balancer

### Drain Node

Use this before:

- planned restart
- OS maintenance
- binary upgrade
- node replacement

Target flow:

1. Mark the node as draining.
2. Stop sending new client traffic to it.
3. Begin cooperative handoff for owned tablets.
4. Wait for in-flight writes to drain.
5. Wait for owned placements to become unowned or reassigned.
6. Stop the process.

Current admin controls:

- `agentdbctl drain start` marks the local node as draining and refuses new
  mutating requests on that node.
- `agentdbctl nodes maintenance <node_id> draining` marks a node as draining in
  the control plane.
- `agentdbctl nodes maintenance <node_id> maintenance` marks a node as out of
  service for planned work.
- `agentdbctl nodes maintenance <node_id> alive` marks a node alive again; for
  the local node, this also clears the local drain flag.
- `agentdbctl tablets handoff <tablet_id> <target_node_id>` marks an owned
  tablet for cooperative transfer to a fresh `alive` target.
- `agentdbctl tablets complete-handoff <tablet_id> <expected_source_node_id>`
  transfers ownership after the source owner has drained the tablet.
- `agentdbctl tablets cancel-handoff <tablet_id>` retracts a pending handoff
  target.
- `agentdbctl tablets rebalance` moves tablets away from fresh draining nodes
  through the same cooperative handoff signal.

This aligns with the existing `node_draining` runbook direction. Stale/dead
owners are intentionally not force-moved by rebalance; lease expiry and the
ownership reaper must first make those tablets unowned, or an operator must use
the explicit `relinquish` endpoint after accepting the fencing impact.

### Stop Node

There are two cases.

#### Planned Stop

Always:

1. drain
2. verify ownership moved or expired cleanly
3. stop process

#### Unplanned Stop

If the node dies:

- surviving Raft voters should elect a new leader when quorum remains
- ownership should be reconciled to the new Raft leader
- client traffic should route elsewhere
- other nodes should mark it stale/dead after the configured cleanup threshold
- replacement/rebalance cleanup should proceed after the dead-node decision

Current behavior: local Raft groups keep ticking on all replicas, surviving
voters can elect a new leader, and all nodes that host a local Raft group can
reconcile tablet ownership to the observed Raft leader. The scheduler leader
marks stale `alive` nodes as `dead` after `AGENTDB_STALE_NODE_THRESHOLD_MS`
(default 180 seconds), and the lease reaper clears expired tablet ownership
with `AGENTDB_REAPER_SAFETY_MARGIN_MS` grace. Cooperative rebalance does not
force-move a dead source; Raft membership retirement is tied to healthy
caught-up replacement completion.

The operational RTO for write availability should be based on Raft election,
ownership reconciliation, route refresh, and client retry. The dead-node
threshold is a cleanup and replacement timer, not the write-path RTO.

### Backup And Restore

Current backup controls:

- `agentdbctl backup create --backup-id <id> --directory <path>` writes an
  online backup manifest under a target directory and includes a metadata
  snapshot when metadata is configured.
- The manifest includes runtime storage summary data for observability. It is
  not a full runtime KV/content backup.
- Add `--include-tablet-checkpoints` to checkpoint local physical tablet-store
  domains. This is supported only for `grouped_rocksdb` with
  `tablets_per_domain = 1`, where each checkpoint maps to exactly one tablet
  store. The manifest records path, checksum, size, and latest applied index.
  When metadata is configured, it also records cell, placement scope, tablet
  epoch, and latest retained snapshot index.
- `agentdbctl restore validate <manifest-path>` validates a backup manifest and
  referenced metadata snapshot plus tablet checkpoint paths, sizes, and
  checksums.
- `agentdbctl restore install-tablet-checkpoint --tablet-id <id>
  --checkpoint-dir <path> --tablet-store-root <path>` installs a checkpoint into
  a stopped node's local `grouped_rocksdb` tablet-store root. It supports only
  `tablets_per_domain = 1` and refuses to overwrite non-empty tablet domains.

Restore apply remains an offline operator procedure. The HTTP restore endpoint
intentionally rejects destructive apply because live restore semantics require a
cluster-wide stop/drain contract and runtime data copy/verification that are not
safe to fake in an online handler.

### Restart Node

Use this for:

- process recovery
- config change
- local maintenance

Target flow:

1. If planned, drain first.
2. Stop the process.
3. Start the process with the same node identity when appropriate.
4. Wait for heartbeat and readiness.
5. Let ownership return only through the normal control-plane process.

Do not treat restart as manual metadata surgery.

### Replace Node

Use this when a host is lost permanently or should be retired.

Target flow:

1. If the old node is still alive, drain it first.
2. Remove it from the active serving fleet.
3. Provision a replacement node.
4. Start the replacement with a new node identity unless there is a strong
   reason to preserve the old one.
5. Reassign placements through the scheduler or handoff mechanism.
6. Verify the old node no longer owns placements.

Key rule:

- replacement should move ownership, not resurrect ambiguous dual ownership

### Remove Node

Removing capacity is a controlled version of node replacement without adding a
direct successor immediately.

Target flow:

1. drain the node
2. move ownership away
3. stop the node
4. remove its registration once the cluster no longer depends on it

### Rolling Upgrade

This is the most important cluster operator workflow.

Target flow:

1. pick one node
2. drain it
3. verify placement handoff or lease expiry
4. stop the node
5. install or start the new binary
6. wait for readiness and metadata convergence
7. repeat on the next node

Guardrails:

- never drain too many nodes at once
- keep enough healthy capacity to absorb the moved placements
- monitor catch-up and restore pressure during the rollout

### Rollback

If a new build is bad:

1. stop rollout immediately
2. keep already-upgraded nodes serving only if they are healthy
3. drain upgraded nodes one by one
4. restart them on the previous binary
5. verify routing convergence and ownership health

## Capacity Expansion

Operators should scale a cluster based on:

- hot placements per node
- warm-head bytes per node
- catch-up or restore backlog
- request latency under load
- object-store and metadata-store pressure

Adding nodes should be the preferred path when:

- hot working set no longer fits comfortably on node-local storage
- a few nodes are consistently overloaded
- handoff/recovery windows are too long

## Failure Handling Expectations

### Node Lost

Expected behavior:

- stale heartbeat detected
- leases expire
- placements become eligible for reassignment
- client traffic routes elsewhere

### Control Plane Unreachable

Expected behavior:

- nodes eventually become stale or stop renewing ownership
- writes may fail fast with lease or routing errors
- operators recover the metadata backend first

### Shared Object Store Unreachable

Expected behavior:

- cold/archive reads and restore paths degrade or fail
- warm traffic may continue for a limited time depending on cache locality
- cluster should surface clear storage-health signals

## Required Admin Surface

A usable cluster mode should expose at least:

- `agentdbctl nodes list`
- `agentdbctl tablets list`
- `agentdbctl raft status`
- `agentdbctl raft catch-up --dry-run`
- `agentdbctl raft snapshots`
- `agentdbctl raft prune-snapshots --dry-run`
- `agentdbctl status`
- node drain status
- node staleness
- ownership epoch / lease visibility
- routing snapshot status
- Raft replica lag and snapshot catch-up status
- retained snapshot age, expiry, and deletion status

Future cluster mode likely also needs:

- add-tablet / create-placement controls
- rebalance controls
- node cordon or maintenance flags

## Dashboard Expectations

A true cluster dashboard should show:

- node fleet health
- tablet / placement ownership
- draining nodes
- stale nodes
- ownership churn
- restore / catch-up backlog
- hot-node pressure

## Current Reality

This is the target operating contract.

The current repository is not yet at the point where operators should expect
all of these workflows to be stable, documented, and supported end to end.

Use [cluster-status.md](cluster-status.md) as the source of truth for what is
actually supported today.

## Related Docs

- [cluster-deployment.md](cluster-deployment.md) - target cluster topology and
  bootstrap model
- [cluster-status.md](cluster-status.md) - current support boundary
- [cluster-alerts.md](cluster-alerts.md) - evaluation alert and triage
  runbook
- [../runbooks/node_draining.md](../runbooks/node_draining.md) - current drain
  error/runbook shape
- [../runbooks/lease_lost.md](../runbooks/lease_lost.md) - lease-loss handling
