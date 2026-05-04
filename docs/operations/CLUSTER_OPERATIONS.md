# Cluster Operations

This document describes the **target** day-2 operator workflows for a real
AgentDB cluster.

It is forward-looking. Use it as the intended operator contract for cluster
mode, not as a claim that all workflows already exist in the current server.

For the current support boundary, see [CLUSTER_STATUS.md](CLUSTER_STATUS.md).

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

- `GET /api/admin/nodes`
- `GET /api/admin/tablets`
- routing snapshot status

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
3. Stop renewing leases or begin cooperative handoff.
4. Wait for in-flight writes to drain.
5. Wait for owned placements to become unowned or reassigned.
6. Stop the process.

This aligns with the existing `node_draining` runbook direction.

### Stop Node

There are two cases.

#### Planned Stop

Always:

1. drain
2. verify ownership moved or expired cleanly
3. stop process

#### Unplanned Stop

If the node dies:

- other nodes should mark it stale after heartbeat expiry
- leases should expire
- ownership should be reassigned or made eligible for reassignment
- client traffic should route elsewhere

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

- `GET /api/admin/nodes`
- `GET /api/admin/tablets`
- node drain status
- node staleness
- ownership epoch / lease visibility
- routing snapshot status

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

Use [CLUSTER_STATUS.md](CLUSTER_STATUS.md) as the source of truth for what is
actually supported today.

## Related Docs

- [CLUSTER_DEPLOYMENT.md](CLUSTER_DEPLOYMENT.md) - target cluster topology and
  bootstrap model
- [CLUSTER_STATUS.md](CLUSTER_STATUS.md) - current support boundary
- [../runbooks/node_draining.md](../runbooks/node_draining.md) - current drain
  error/runbook shape
- [../runbooks/lease_lost.md](../runbooks/lease_lost.md) - lease-loss handling
