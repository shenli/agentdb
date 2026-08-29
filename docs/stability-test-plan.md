# AgentDB Stability Test Plan

Status: draft for the `stability-test` branch.

## Goals

- Catch data loss, duplicate commit, stale read, index drift, and changefeed
  cursor regressions before release.
- Exercise real multi-process AgentDB clusters with Postgres metadata, MinIO
  object storage, grouped RocksDB tablet storage, and raft-rs replication.
- Keep tests deterministic by default. Random chaos must use an explicit seed
  and report that seed.

## Real-Cluster Suites

Run from the repository root on a host with Docker:

```bash
AGENTDB_STABILITY_SUITE_WORK_DIR=$HOME/work/agentdb-stability-suite \
  ops/smoke/scripts/real-cluster-stability-suite.sh
```

The compose runner bind-mounts Postgres and MinIO data under the smoke work
directory sibling (`$WORK_DIR.deps`) so large state stays under `~/work` on
hosts where `/` is small.

### deterministic-surface

Command:

```bash
AGENTDB_CLUSTER_SMOKE_WORK_DIR=$HOME/work/agentdb-stability-suite/deterministic-surface \
  ops/smoke/scripts/real-cluster-smoke-compose.sh \
  --nodes 6 --two-cell --consistency --record-index --changefeed
```

Coverage:

- Two independent three-node raft groups, one per cell.
- Tenant, agent, and session creation in both cells.
- Gap-free session event order after catch-up on every node.
- Idempotent append replay: same key and same body must return the same
  event id and seq.
- Idempotency conflict: same key and different body must return
  `IDEMPOTENCY_CONFLICT` and must not append another event.
- Record write/search in both cells.
- Local and global record search visibility with commit-sequence fences.
- Record tag filtering, source-record filtering, vector nearest-neighbor
  ranking, delete removal from search, and admin index watermarks.
- Session watch cursor replay, tenant watch fanout across sessions, and durable
  export watch event envelopes.

### deterministic-hardening

Command:

```bash
AGENTDB_CLUSTER_SMOKE_WORK_DIR=$HOME/work/agentdb-stability-suite/deterministic-hardening \
  ops/smoke/scripts/real-cluster-smoke-compose.sh \
  --nodes 6 --two-cell --full-hardening
```

Coverage includes `deterministic-surface` plus:

- Owner restart with existing data directories.
- Tablet checkpoint creation and offline checkpoint install.
- Availability-zone failure with quorum survivors, restart, and catch-up.
- Rolling restart of every node.
- Explicit tablet replacement and handoff.
- Split/merge metadata cutover while validating watch visibility.

### seeded-chaos

Command:

```bash
AGENTDB_CLUSTER_SMOKE_WORK_DIR=$HOME/work/agentdb-stability-suite/seeded-chaos \
  ops/smoke/scripts/real-cluster-smoke-compose.sh \
  --nodes 3 --chaos --chaos-seed 424242 --chaos-iterations 12
```

Coverage:

- Bounded kill/restart iterations with deterministic victim selection.
- Writes while one node is down and after restart.
- Final raft convergence and minimum event-count validation.

## Current Results

Validated on `li-dev` from `~/work/agentdb` on branch `stability-test`:

- `deterministic-surface` passed on a real six-node, two-cell cluster using
  work directories under `~/work`.
- The suite exposed and this branch fixed durable idempotency replay after a
  raft timeout. A retry now consults the persisted idempotency row when the
  in-process cache missed the committed result.
- The suite exposed and this branch fixed global record-index outbox catch-up
  when a record write committed durably but timed out before the runtime
  updated in-memory record state.

Remaining hardening failure:

- Scenario: `deterministic-hardening` reaches split/merge after restart,
  checkpoint replacement, AZ failure, rolling restart, and tablet replacement.
- Observed on `li-dev`: append seq `15` during split prepare retried after
  `"raft-rs command did not commit ... before timeout"` and returned seq `14`.
- Expected: retry for the seq `15` idempotency key must replay seq `15` if the
  first attempt committed, or commit seq `15` if it did not. Returning seq `14`
  indicates stale session sequence state or incorrect idempotency replay around
  split/merge/ownership transition.

This remains a data-consistency blocker for the full hardening suite.

## Next Coverage To Add

- A fault-injection mode that pauses raft responses after durable event commit
  but before HTTP response, so idempotency replay is deterministic rather than
  timing-dependent.
- A persisted metadata stress case with low Postgres connection limits to catch
  metadata pool starvation without depending on incidental 60s stalls.
- Cross-node read-after-write checks for records and events immediately after
  owner handoff.
- Export consumer claim/ack/rewind checks against the real cluster, including
  lease expiry and second-consumer takeover.
- Snapshot/catch-up verification that compares event ids, record ids, and
  export cursors across every replica after restart.
