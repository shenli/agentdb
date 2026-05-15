# Cluster Smoke Scenarios

This directory contains throwaway smoke scripts for exercising AgentDB as a
real multi-process cluster. The current harness runs AgentDB servers on the
host and uses Compose only for Postgres metadata and MinIO object storage.

The smoke suite is not a production bootstrap flow. It is a failure-mode
exercise for the distributed control plane, Raft ownership, grouped RocksDB
tablet-store lifecycle, and administrative recovery paths.

## Default Topology

The default run creates one cell, one placement scope, one serving tablet, and
three Raft voters. Node labels are still populated so placement and status
surfaces are exercised even on the small topology.

```text
cell default, placement scope default

  AZ local-a          AZ local-b          AZ local-c
  +---------+         +---------+         +---------+
  | node-a  |         | node-b  |         | node-c  |
  | voter   |         | voter   |         | voter   |
  +----+----+         +----+----+         +----+----+
       \                   |                   /
        \                  |                  /
         +-----------------------------------+
                   tablet 1 Raft group
```

Run:

```bash
ops/smoke/scripts/real-cluster-smoke-compose.sh
```

Covered invariants:

- metadata seeding creates a serving tablet with exactly one cell-scoped
  placement scope
- all seeded nodes expose cell, AZ, domain, deployment profile, and capacity
  labels through admin/status APIs
- the owner is resolved from tablet metadata and local Raft observability
- tenant, agent, session, append, and event fetch work through the cluster path
- read-after-write verification checks the exact committed sequence and payload
  sequence, not only the event count
- Raft readiness reports no leaderless tablet, no multi-leader tablet, and no
  commit/applied drift once the cluster is expected to be healthy

## Two-Cell Topology

`--nodes 6 --two-cell` creates two independent cells. Each cell owns its own
placement scope, tablet, and three voters. The shared metadata database remains
single and global.

```text
shared metadata DB
        |
        +-----------------------+
        |                       |
cell default                cell-b
tablet 1                    tablet 2

AZ local-a  local-b  local-c     AZ local-a  local-b  local-c
+------+   +------+  +------+    +------+   +------+  +------+
|node-a|   |node-b|  |node-c|    |node-d|   |node-e|  |node-f|
+--+---+   +--+---+  +--+---+    +--+---+   +--+---+  +--+---+
   \          |         /            \          |         /
    +------------------+              +------------------+
         tablet 1                         tablet 2
```

Run:

```bash
ops/smoke/scripts/real-cluster-smoke-compose.sh --nodes 6 --two-cell
```

Covered invariants:

- routing resolves tenant policy to the expected cell and tablet
- writes in one cell do not require ownership movement in the other cell
- Raft catch-up and readiness are evaluated per tablet
- same-cell placement is maintained for both seeded tablets

## Owner Failover

`--failover` kills the current owner process for tablet 1, waits for surviving
nodes to elect or observe a new Raft leader, and verifies a new exact-sequence
append.

```text
before kill                      after kill

node-a leader/owner              node-a stopped
node-b follower                  node-b leader/owner
node-c follower                  node-c follower

        writes continue with quorum 2/3
```

Run:

```bash
ops/smoke/scripts/real-cluster-smoke-compose.sh --failover
ops/smoke/scripts/real-cluster-smoke-compose.sh --nodes 6 --two-cell --failover
```

Covered invariants:

- a follower does not self-elect only because it receives client writes
- metadata ownership is reconciled to the observed Raft leader
- local Raft ownership reconciliation runs even on nodes that are not the global
  scheduler lease holder
- writes complete after owner loss with RPO 0 for committed entries
- in two-cell mode, cell-b remains writable while cell default fails over

## Deterministic Hardening

`--full-hardening` composes deterministic lifecycle probes. Explicit
replacement runs before split/merge because replacement must target a live
tablet Raft group; split/merge is a metadata cutover probe that can retire the
original tablet from the local runtime view.

Run:

```bash
ops/smoke/scripts/real-cluster-smoke-compose.sh --nodes 6 --two-cell --full-hardening
```

The same probes can be run individually:

```bash
ops/smoke/scripts/real-cluster-smoke.sh --restart-persistence
ops/smoke/scripts/real-cluster-smoke.sh --checkpoint-replacement
ops/smoke/scripts/real-cluster-smoke.sh --az-failure
ops/smoke/scripts/real-cluster-smoke.sh --rolling-restart
ops/smoke/scripts/real-cluster-smoke.sh --split-merge
ops/smoke/scripts/real-cluster-smoke.sh --replacement
```

### Restart Persistence

The current tablet owner is stopped and restarted with the same Raft and KV
directories.

Covered invariants:

- Raft state survives process restart
- a restarted node does not regress committed/applied indexes
- persisted grouped RocksDB tablet-store state remains readable
- a post-restart append reaches the exact expected sequence

### Checkpoint Replacement

The admin backup API creates a tablet-store checkpoint, then the harness
installs it offline into a fresh grouped RocksDB root.

Covered invariants:

- checkpoint metadata includes size and checksum
- checkpoint install restores the expected tablet contents
- grouped RocksDB can isolate checkpoint validation without opening unbounded
  per-tablet RocksDB instances in the live cluster

### One-AZ Failure

All nodes in one AZ are stopped at the same time. With the six-node two-cell
topology, each cell loses one voter and keeps quorum.

```text
AZ local-a down

cell default: node-a X, node-b up, node-c up -> quorum 2/3
cell-b:       node-d X, node-e up, node-f up -> quorum 2/3
```

Covered invariants:

- quorum writes continue while one AZ is down
- readiness during the outage allows the stopped replicas to be unreachable or
  lagging, but still requires no leaderless tablet, no multi-leader tablet, and
  no commit/applied drift on surviving replicas
- restarted AZ nodes can catch up through the snapshot/catch-up path
- full readiness is required again after the AZ is restored

### Rolling Restart

Each node is stopped, restarted, and verified before the next node is touched.

Covered invariants:

- the cluster tolerates sequential process restarts
- ownership can move and settle between restarts
- every append is verified by exact sequence and payload after convergence
- logical append retries carry a stable `Idempotency-Key`, so an indeterminate
  timeout cannot turn one smoke operation into two committed events
- catch-up probes detect lag before the next restart begins

### Split/Merge Metadata Cutover

The harness prepares split/merge metadata while writes are active, then
completes the cutover and checks watch cursor continuity.

Covered invariants:

- prepared tablet-range state remains observable
- writes committed during the prepared split window are not skipped by watches
- merge cutover does not break session event reads

### Explicit Replacement

The harness prepares and completes a handoff from the current owner to another
same-cell node, then stops the old owner and writes through the replacement.

Covered invariants:

- replacement target must be in the same cell
- replacement complete carries an observed source applied-index watermark and
  waits for target catch-up before accepting the handoff
- stale internal snapshot install is idempotent
- committed/applied indexes stay monotonic across the handoff
- the old owner can be stopped without losing the committed write stream

## Random Chaos

`--chaos` repeatedly kills and restarts one node at a time while keeping quorum.
It is intended for nightly runs, not for deterministic debugging.

```bash
ops/smoke/scripts/real-cluster-smoke-compose.sh --chaos --chaos-iterations 48
ops/smoke/scripts/real-cluster-smoke-compose.sh --chaos --chaos-seed 12345
```

Covered invariants:

- bounded single-node failure and rejoin does not lose committed events
- exact sequence verification catches ambiguous timeout handling bugs
- failed followers catch up before the next chaos iteration

## Recovery-Time Expectations

The smoke configs intentionally use short local timings:

- scheduler lease TTL: 5 seconds
- stale-node threshold: 180 seconds
- append HTTP timeout: 75 seconds
- exact-sequence append recovery attempts: 8
- Raft readiness waits: usually 90 to 120 seconds

Expected local RPO is 0 for entries that were acknowledged as committed. RTO is
scenario-dependent:

- owner process loss should recover after Raft election plus owner
  reconciliation, normally within the append retry window
- one-AZ loss should remain writable immediately when each tablet keeps quorum
- a stopped replica may stay reported as unreachable or lagging until it is
  restarted and catch-up completes
- full post-restoration health requires no unreachable replicas, no lagging
  replicas, no leaderless tablets, no multi-leader tablets, and zero
  commit/applied drift

These timings are local smoke expectations, not production SLOs. Production RTO
depends on heartbeat/election configuration, scheduler lease TTL, node failure
detection, object-store latency, snapshot size, and whether the failed domain
removes a Raft quorum.

## Planned Coverage

The following distributed scenarios are intentionally not implemented in this
local harness yet:

- real network partitions with asymmetric packet loss or delayed messages
- disk-full, fsync failure, and corrupted Raft log recovery
- metadata database primary failover during active scheduling
- S3/MinIO outage during snapshot or checkpoint transfer
- simultaneous two-node loss in one three-voter tablet, which should reject
  writes rather than pretend to make progress
- live tenant movement between cells
- online tablet split with automatic child Raft group creation and data
  movement
- physical tablet replacement after an online split has retired the original
  source tablet group
- large snapshot transfer under throttled bandwidth
- mixed-version rolling upgrade and downgrade compatibility
- AWS multi-AZ deployment smoke with real instance termination and load
  balancer routing behavior
