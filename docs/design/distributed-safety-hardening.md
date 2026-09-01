# Distributed Safety Hardening

## Status

This document defines the next correctness and operability priorities for
AgentDB's clustered runtime. It is a forward-looking hardening plan, not a claim
that clustered AgentDB is a supported production surface today. The current
support boundary remains documented in
[`../operations/cluster-status.md`](../operations/cluster-status.md).

The work is intentionally ordered around failure containment and evidence. It
does not expand AgentDB into a general-purpose database, introduce broad
transactions, or replace the agent-native runtime model.

## Goals

The hardening program should make these statements testable:

1. A node that cannot confirm current Raft authority cannot serve a successful
   linearizable read or accept a write.
2. A surviving quorum preserves every acknowledged session mutation and
   continues from the exact committed session sequence.
3. A node cannot join the wrong cluster, reuse a replaced store identity, or
   silently open a data directory created for another cluster generation.
4. Retried manifest and object-lifecycle operations are idempotent, observable,
   and safe across crashes at every boundary.
5. Storage, replication, indexing, and object-store backlog remain bounded, and
   pressure is attributed to the tenant producing it.
6. Test results state exactly which production path and failure mode they
   exercised.

## Priority Order

| Priority | Work | Completion signal | Status |
|---|---|---|---|
| P0 | Define clustered read semantics and add a Raft read barrier | A partitioned or resumed former leader cannot successfully serve a linearizable read | **Done** — ReadIndex barrier (`RaftRsSharedGroup::read_barrier`) on session reads, classification in [`read-semantics.md`](read-semantics.md), plus `check_quorum` so an isolated leader steps down (and `pre_vote` keeps it down) instead of self-confirming stale reads; S22 verifies the isolated leader leaves the leader role and stays out while partitioned |
| P0 | Add deterministic transport fault injection | Reproducible 2+1, asymmetric, delayed, and healed partition tests run from a fixed seed | **Done** — `agentdb_api::transport_faults` at the production send/recv boundary; S22 isolates a live leader and verifies quorum-loss + election + heal |
| P0 | Add cluster root and durable store identity | Startup fails closed on cluster, node, store-incarnation, or generation mismatch | **Partial** — `<data_dir>/IDENTITY.json` (cluster id, node id, store incarnation, integrity digest) verified before any store opens; startup fails closed on node/cluster mismatch, corruption, or unknown version. Control-plane admission (store-incarnation retirement + bootstrap-generation cross-check) is the remaining follow-up |
| P1 | Make manifest changes content-addressed and reconcilable | Duplicate requests and crash/retry sequences cannot under-count live object references |
| P1 | Connect tenant admission to downstream storage debt | Slow storage throttles the responsible tenant before queues or retained logs become unbounded |
| P1 | Adopt a concise testing-evidence contract | Filtered, concurrent, mutation, and acceptance tests carry explicit evidence |
| P2 | Evaluate manifest-based tablet attachment | A cold/read-mostly tablet can move without copying its complete durable history |
| P2 | Reduce control-plane metadata load in stages | Ephemeral state leaves SQL before durable metadata is moved or self-hosted |

## 1. Clustered Read Semantics

### Current boundary

AgentDB documents quorum commit for clustered tablet writes, but it does not yet
publish an endpoint-by-endpoint clustered read-consistency contract. A local
process believing it is the leader is not by itself proof that it still has
authority: network isolation, scheduler pauses, and delayed Raft messages can
leave a former leader temporarily unaware that a majority elected another
leader.

The contract must classify each read surface as one of:

- **Linearizable**: confirmed against a current quorum before reading.
- **Read-your-writes**: observes at least a caller-provided commit or sequence
  watermark.
- **Snapshot-consistent**: reads from a named, stable snapshot boundary.
- **Eventually consistent**: may return stale data, with the staleness signal or
  repair mechanism documented.

No endpoint should inherit a stronger label merely because it routes to the
node currently recorded as owner.

### Required mechanism

Clustered linearizable reads should use a Raft read barrier such as ReadIndex:

```text
request reaches candidate leader
        |
        v
confirm leadership with quorum
        |
        v
obtain read_index
        |
        v
wait until local applied_index >= read_index
        |
        v
read one stable local snapshot
```

The barrier should have a bounded timeout and a typed retryable failure. It must
not fall back to a local read when quorum confirmation fails.

`append-and-fetch` has a narrower but strict requirement: the returned recent
tail must include the append acknowledged by that response. Internally this can
use the mutation's committed/applied index rather than paying for an unrelated
second quorum round trip.

### Acceptance tests

- Partition one voter from a three-voter tablet while the other two retain
  quorum. The isolated former leader must reject or time out a linearizable read.
- Pause the former leader for longer than an election timeout, elect a new
  leader on the majority, then resume the old process. It must not serve stale
  data before processing the new term and completing a read barrier.
- Commit event sequence `N` on the majority, heal the partition, and require all
  replicas to converge on the same event ID, payload, sequence, commit index,
  and applied index.
- Lose the HTTP response after commit and retry with the same idempotency key.
  Exactly one logical event must exist.

## 2. Deterministic Transport Fault Injection

Process termination tests are necessary but insufficient. They do not model a
live process that can receive client traffic while only some peer links work.

The internal Raft transport should expose test-only link controls keyed by:

```text
(tablet_id, source_node_id, target_node_id)
```

Supported actions should include:

- pass normally
- drop
- delay by a bounded duration
- reject with a typed transport error
- hold and release in deterministic order

The controls belong at the production transport boundary so tests exercise the
same serialization, authentication, routing, outbox, and dispatch path as a
real message. A parallel test transport that bypasses production framing would
not provide the same evidence.

Every scenario should record:

- seed and exact link schedule
- tablet and node identities
- elected leader and term before and after the fault
- commit and applied indexes per replica
- client operation IDs and idempotency keys
- the exact invariant that ended the scenario

The deterministic suite should cover 2+1 partitions, asymmetric partitions,
message delay and reordering, snapshot delay, healing, and follower catch-up.
A nightly job should run at least 100 fixed or recorded seeds before adding a
larger random-chaos budget.

## 3. Cluster Root And Store Identity

Cluster bootstrap needs authority that exists before routing or ordinary
metadata is trusted. Each data directory also needs an identity that survives
process restarts but changes when a store is deliberately replaced.

### Root descriptor

A versioned canonical root descriptor should contain at least:

```text
cluster_id
bootstrap_generation
initial control-plane authority
initial node/store identities or their approved set
credential or provisioning digest, never the credential value
format_version
```

The descriptor should have deterministic canonical bytes and a cryptographic
digest. The locally provisioned descriptor and the committed control-plane
record must agree before the node becomes ready.

### Store identity

Every local data directory should persist:

```text
cluster_id
node_id
store_incarnation
bootstrap_generation
created_at
format_version
```

`store_incarnation` identifies the physical store, not merely the logical node.
Replacing a disk or rebuilding a node creates a new incarnation and requires an
explicit control-plane admission step.

Startup must fail closed when:

- a data directory belongs to another cluster
- a node ID is already associated with another live incarnation
- an old disk attempts to rejoin after its incarnation was retired
- the local bootstrap generation conflicts with committed authority
- the identity file is truncated, malformed, or has an unknown incompatible
  version

These failures should be visible before the external listener reports ready.

## 4. Manifest And Object-Lifecycle Safety

AgentDB's recovery contract already follows the essential order:

```text
upload -> verify -> publish manifest -> only then collect old data
```

The next step is to make every manifest transition independently identifiable
and reconcilable.

### Manifest-change identity

Derive a stable change ID from canonical content such as:

```text
object owner (tenant/session/tablet)
owner epoch
objects added
objects removed
new durable/applied watermark
manifest format version
```

Retries of the same logical transition must use the same identity. The mutation
result should distinguish applied, already applied, rejected as stale, and
unknown outcome.

### Reference ordering

- Establish a durable positive reference before publishing a manifest that
  exposes an object.
- Derive removal from the authoritative applied manifest transition rather
  than accepting an independent decrement that can be replayed twice.
- Delete an object only when no live manifest epoch references it and the
  retention/grace policy allows collection.
- Provide an authoritative query for the currently applied manifest and its
  watermark so clients and workers can reconcile unknown outcomes.

### Crash matrix

Tests should interrupt execution after each boundary:

1. object write started
2. object write completed
3. checksum verified
4. positive reference committed
5. manifest committed
6. old reference removed
7. old object deletion started
8. local/Raft state truncated

A crash may leave a temporary orphan. It must never delete an object still
reachable from an authoritative manifest.

## 5. Pipeline-Aware Tenant Backpressure

Static request quotas do not by themselves prevent collapse when downstream
work drains more slowly than writes arrive. Admission should observe bounded
debt across the write pipeline:

- in-memory mutation and memtable bytes
- retained Raft-log bytes and entries
- pending snapshot or object-upload bytes
- compaction debt
- record-index outbox lag
- export-ledger lag where retention depends on consumers
- object-store request rate and error state

Each resource should retain its own metric and limit. A single admission result
can combine them, but one opaque counter must not hide which stage is full.

Pressure should be charged to the tenant and priority class producing the work.
Global safety caps remain necessary, but one tenant's slow upload or indexing
backlog should not consume all node memory or block unrelated tenants without a
reported reason.

Start with measurement. Inject 500 ms object-store latency and record which
queues, logs, and memory counters grow over a ten-minute workload. Add the first
credit to the stage that becomes unbounded; do not introduce a general
scheduler before the bottleneck is demonstrated.

Operator-visible backpressure should include:

- limiting resource
- tenant and priority class
- current usage and configured limit
- retry-after guidance
- time since the resource last drained

## 6. Testing-Evidence Contract

The repository should keep a short, enforceable contract for correctness tests:

1. A filtered test run declares the expected selected-test count before it
   runs. Zero selected tests is not success.
2. A mutation experiment confirms that the mutation landed and that the test
   traversed the mutated production path.
3. A red result identifies the assertion or invariant that fired, not only the
   exit code.
4. Cross-thread and asynchronous probes declare an observation window and await
   the task or a durable effect. An unobserved detached panic is not evidence.
5. Fixtures do not duplicate the production rule they claim to verify. The
   assertion must consume output from the real producer.
6. Acceptance scripts emit a unique positive completion marker reachable only
   after every required assertion.
7. Every green report states what the run did not cover.
8. Quantified claims record the command, commit, selected count, result, and
   relevant raw evidence.

Random chaos complements deterministic tests; it does not replace them. A
random failure should be converted into a fixed-seed regression before the
finding is considered closed.

## 7. Manifest-Based Tablet Attachment

This is an evaluation track, not a near-term storage-engine replacement.

The hypothesis is that cold or read-mostly tablet state can be represented by
immutable shared objects plus a small authoritative manifest and recent Raft
tail. A new replica could then attach by receiving metadata and fetching blocks
on demand instead of copying the tablet's full durable byte set.

The experiment should start with cold tablets and measure:

- bytes transferred during placement and replacement
- time until the replica can participate as a learner
- time until it can serve at target p99 latency
- object-store GET count and cost
- cache size and warm-up bandwidth
- maximum retained unflushed Raft tail
- recovery time after complete local-cache loss
- compaction read/write amplification

Do not replace the local hot-tablet path unless the same AgentDB workload shows
an operational or cost win. The required benchmark is session-native:

```text
append event -> quorum commit -> apply -> fetch recent session tail
```

Generic point-put throughput is not sufficient evidence.

## 8. Staged Metadata Reduction

AgentDB should not remove its external metadata root in one step. The safe order
is:

1. Remove ephemeral Raft health, peer observations, routing refreshes, and
   high-frequency status from the shared metadata database.
2. Keep those signals node-local or propagate them through bounded,
   purpose-built cluster messages.
3. Move tenant-scale durable metadata into an internal AgentDB system tenant
   only after bucket-only restore and cluster identity are proven.
4. Keep the external metadata backend as a small bootstrap and recovery root
   until an equivalent self-hosted recovery procedure has passed destructive
   drills.

This sequence is expanded in
[`metadata-self-hosting.md`](metadata-self-hosting.md).

## Explicit Non-Goals

This hardening plan does not add:

- a public raw-KV API
- general multi-key OLTP transactions
- a timestamp oracle or deadlock detector
- arbitrary user-key range sharding
- a new storage engine before the current path is measured
- synchronous object-store uploads on the ordinary session append path
- a production-cluster support claim without operator procedures and SLOs

AgentDB remains a runtime-state database for sessions, events, artifacts,
records, skills, context, replay, forks, lifecycle, and export.

## Program Exit Criteria

The first hardening program is complete only when all of the following are
true:

- Clustered read semantics are documented per endpoint and enforced by code.
- A former leader cannot serve a successful linearizable read under the
  deterministic pause and partition scenarios.
- One hundred deterministic partition schedules pass with exact event and
  index convergence.
- Cluster/store identity mismatch tests fail before readiness and before data
  mutation.
- The manifest crash matrix passes without live-object loss or double removal.
- A ten-minute slow-storage workload remains within declared memory, log,
  upload, and index-debt bounds while unrelated tenants continue making
  progress.
- Every acceptance result records its commit, configuration, seed, exact
  completion marker, and uncovered failure classes.

Passing these gates improves the evidence for clustered correctness. It does
not by itself complete add/remove/replace, rolling upgrade, restore, alerting,
or production-SLO work listed in the cluster operations documents.
