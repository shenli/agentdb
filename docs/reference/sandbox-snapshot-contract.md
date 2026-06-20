# Sandbox Snapshot Contract

Status: reference contract

Date: 2026-06-20

AgentDB stores sandbox snapshot references, not sandbox filesystem bytes. A
sandbox service, harness, or platform owns snapshot creation, restore, garbage
collection, and byte storage.

## Ownership

| Concern | Owner |
|---|---|
| Ordered session log, fork graph, replay, lifecycle state | AgentDB |
| `SnapshotRef` metadata on a session | AgentDB |
| Snapshot byte creation, upload, copy-on-write fork, mount, restore | Sandbox service |
| Snapshot object retention, deletion, storage class, encryption | Sandbox service / object store |
| Credential injection | Control plane / egress broker, never sandbox snapshot refs |

AgentDB must never store filesystem bytes, mounted paths, host credentials,
environment secrets, or object-store credentials in the session log. Snapshot
layers are opaque refs such as object keys, content-addressed layer ids, or
filesystem snapshot ids.

## Snapshot Record

After a sandbox successfully captures bytes for a committed session point, it
records:

```text
tenant_id
session_id
at_seq
layers[] = { name, ref_key, kind? }
expires_at_ms?
```

`at_seq` must be `<= session.next_seq`. `layers` must be non-empty. AgentDB
orders refs by `at_seq` and creation time. The sandbox service chooses layer
granularity, for example:

| Layer | Example | Typical retention |
|---|---|---|
| `base` | image digest / VM base snapshot | weeks to indefinite |
| `deps` | dependency cache snapshot | days to weeks |
| `repo` | checked-out source tree | days to weeks |
| `workdir` | generated files / scratch state | hours to days |

## Trigger Policy

The sandbox service decides when to snapshot. Recommended defaults:

- On session lifecycle transition from `Warm` to `Cold`.
- Before `Archive` when replay cost or re-derivation cost is material.
- Before a fork when the parent has filesystem state that child execution needs.
- After large file-producing tool results when re-running the tool would be
  expensive or non-deterministic.
- Opportunistically every N committed events or M minutes for long-running
  sessions, tuned per tenant tier.

AgentDB policy should be treated as input to the sandbox scheduler, not as a
background snapshot worker inside AgentDB. The sandbox scheduler can read tenant
policy, session lifecycle, and watch/export feeds, then call the core snapshot
record operation after bytes are durable.

## Resume Contract

To resume a session:

1. Call AgentDB's snapshot resolution for `(tenant_id, session_id, now_ms)`.
2. If a non-expired ref is returned, restore the listed layers in order.
3. Replay AgentDB events from `replay_from_seq`.
4. If `dangling_skipped` is true, log/metric the expired ref and continue with
   the older ref plus replay.
5. If no ref is returned, start from a clean sandbox and replay from seq `0`.

The restore operation must be idempotent. A retry can restore the same ref and
replay the same tail without changing committed AgentDB history.

## Fork Contract

Fork is coordinated across stores:

1. AgentDB creates the child session with `ForkRef { parent_session_id, at_seq }`.
2. The sandbox creates a copy-on-write filesystem child from the parent snapshot
   at or before `at_seq`, or records that the child will reconstruct from replay.
3. AgentDB's snapshot resolver can inherit an ancestor snapshot when resolving
   the child; replay then applies the child branch from the correct point.

The child must not copy parent events into its own session log. The fork edge is
a pointer in AgentDB and a copy-on-write snapshot relationship in the sandbox
store.

## Byte Lifecycle

The sandbox service owns byte deletion. A snapshot ref may outlive bytes or
point at expired bytes; AgentDB handles this by skipping expired refs and falling
back to older refs plus replay. Operators should expose:

- snapshots created / failed / restored,
- restore latency,
- expired or dangling refs skipped,
- bytes retained by tenant and storage class,
- oldest restorable session age by tenant,
- snapshot GC deletes and failures.

Deletion should be conservative: delete bytes only after the ref's retention
window and any legal hold / audit policy allow it. If object-store lifecycle
rules delete bytes first, restore should surface a dangling-ref metric and fall
back rather than failing the whole session resume.

## Failure Modes

| Failure | Required behavior |
|---|---|
| Snapshot capture fails | Do not record a `SnapshotRef`; continue with replay-only recovery. |
| Snapshot bytes expire | Resolver skips expired refs; sandbox falls back to older ref plus replay. |
| Restore fails for a live ref | Retry restore, then fall back to older ref plus replay if policy allows. |
| Layer missing or corrupt | Treat as dangling/corrupt, emit metric, and fall back. |
| Fork COW fails | Child still exists in AgentDB; sandbox reconstructs from ancestor ref plus replay. |

## Security

Snapshot layers must not contain credentials. Credential material lives in the
control plane / vault and is injected only at egress. Snapshot refs should be
opaque handles, not pre-signed URLs or bearer tokens.
