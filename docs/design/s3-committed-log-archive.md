# S3-Native Committed Tablet-Log Archive

**Status: proposed; not implemented.**

This document defines the missing storage layer between AgentDB's Raft-backed
tablet write path and its object-store recovery path: continuously sealing
committed tablet-log ranges into immutable S3 objects.

The short version is:

```text
Raft/local SSD owns low-latency acknowledgement and the hot tail.
S3 owns immutable committed-log history and recovery checkpoints.
```

S3 is not on the ordinary append acknowledgement path. Acknowledged writes
remain protected by the tablet's Raft quorum. A background archiver copies only
committed entries to S3 in coarse, self-describing objects and publishes a
measured archive watermark. Recovery starts from a verified tablet checkpoint,
replays contiguous S3 log segments, and uses a surviving Raft tail when one is
available.

## What AgentDB Does Today

AgentDB already implements several adjacent pieces, which makes the current
design easy to mistake for continuous log archival:

| Existing mechanism | Trigger | Object-store data | Continuous committed tablet log? |
|---|---|---|---|
| Session lifecycle sealing | A session moves to `cold` or `archive` | Full-fidelity session segment or archive bundle | No. It is session-policy output, not every committed tablet mutation. |
| Content mirror | A content object is written in cluster object-store mode | Artifact, session segment, archive, or replay-prefix bytes | No. It mirrors blobs, not `ReplicatedLog` entries, and its upload queue is currently in memory. |
| Recovery manifest | Periodic whole-runtime export, five minutes by default | Full KV snapshot as JSON Lines | No. It is a periodic full snapshot with a time watermark, not an index-addressed incremental log. |
| Raft snapshot object | A replica needs snapshot catch-up | Full tablet state for a particular source/target/index | No. Upload is coupled to replica lag and movement rather than a regular S3 recovery checkpoint. |
| Local Raft log | Every tablet mutation | Committed commands in `raft-engine` on replica-local storage | This is the source the proposed archiver will consume. It is not currently archived continuously to S3. |

The missing invariant is:

> For each tablet, S3 contains a verifiable, contiguous sequence of committed
> log entries after a retained checkpoint, up to a published
> `archived_commit_index`.

## Goals

- Keep S3 outside the synchronous append path.
- Reduce bucket-only recovery RPO from a periodic whole-state interval to a
  bounded, measured per-tablet archive lag.
- Make recovery incremental: one checkpoint plus log ranges, rather than a
  repeated whole-runtime export.
- Make uploaded objects immutable, self-describing, checksummed, encrypted, and
  safe to publish more than once.
- Resume archival after process restart, leader change, retry, or an extended
  S3 outage without depending on an in-memory upload queue.
- Bound local retained-log bytes and attribute archive debt to tablets and
  tenants before disk exhaustion.
- Preserve AgentDB's existing Raft ordering, fencing, and membership model.

## Non-Goals

- Replacing Raft with S3 or with a second replication protocol.
- Waiting for S3 before acknowledging an ordinary session append.
- Treating object storage as a low-latency mutable KV store.
- Replacing session lifecycle segments. Session segments are a logical,
  session-oriented read representation; tablet-log segments are physical
  recovery material.
- Serving ordinary hot reads from S3.
- Building lazy remote-SST reads as part of the first implementation.
- Providing zero-RPO recovery after simultaneous loss of every Raft replica.
  The initial bucket-only RPO is the configured maximum archive age and must be
  reported explicitly.

## Core Invariants

1. **Raft commit precedes archival.** An archive object contains only entries
   returned by `ReplicatedLog::committed_entries`; speculative or merely
   persisted Raft entries are never uploaded as committed history.
2. **S3 is off the acknowledgement path.** An ordinary append returns after
   quorum commit and apply, independent of S3 latency.
3. **One index has one committed command.** A reader that finds different
   payload hashes for the same tablet and index fails closed; it does not pick
   the newest object by timestamp.
4. **Published ranges are immutable.** Segment objects use create-only writes.
   Retrying an existing key is successful only after its size and digest match.
5. **Ranges are contiguous.** Advancing `archived_commit_index` from `N` to `M`
   requires verified coverage of every index in `[N + 1, M]`.
6. **Recovery metadata is index-based.** Correctness uses tablet id, Raft
   index, term, configuration epoch, format version, and hashes. Wall-clock
   time is observability metadata, not an ordering primitive.
7. **Local GC is recovery-aware.** AgentDB does not discard the only recoverable
   copy of an unarchived committed entry. Log compaction must preserve either a
   retained verified checkpoint or a continuous archived path from an older
   retained checkpoint.
8. **The bucket can be audited without the original node.** Object keys and
   headers carry enough identity to inventory a cluster, tablet, range, and
   encryption requirement.
9. **Leader changes are safe.** Raft term/index provide fencing. A new leader
   reconstructs the S3 watermark, reads committed entries after it, and resumes
   idempotently; no separate writer-token protocol is introduced.

## Write And Archive Path

```text
client append
    -> tablet leader
    -> Raft quorum commit on local SSDs
    -> apply to tablet state
    -> acknowledge client

background archive worker
    -> read committed entries after archived_commit_index
    -> require a contiguous range
    -> seal at target bytes or maximum age
    -> compress and encrypt
    -> create immutable S3 object
    -> HEAD/read-back metadata and verify digest
    -> advance local/replicated archive progress
    -> expose lag and retained-log debt
```

Starting defaults:

| Setting | Default | Reason |
|---|---:|---|
| Target uncompressed segment size | 16 MiB | Amortizes PUT cost while keeping retry and recovery units bounded. |
| Maximum dirty-segment age | 30 seconds | Bounds low-volume bucket-only RPO without issuing a PUT per append. |
| Maximum encoded segment size | 64 MiB | Bounds worker memory and retry cost; a single larger entry is stored alone. |
| Compression | Zstandard, level 1 | Fast recovery-oriented compression; record the codec in every header. |
| Archive worker concurrency | 1 per tablet, globally bounded | Preserves per-tablet order and prevents many tablets from overwhelming S3. |
| Recovery checkpoint interval | 10 minutes or 1 GiB of new archived log | Bounds replay work independently of replica catch-up traffic. |

These are assumptions to measure, not performance claims. Qualification must
record command-size distributions, active tablet count, PUT rate, archive and
restore throughput, CPU, compression ratio, and S3 cost.

### Backlog And S3 Outage Behavior

The Raft log is the durable source queue. The archiver should not copy every
pending command into an in-memory payload queue. After restart or leader
change, it reconstructs the last verified S3 index and rereads subsequent
committed entries from the local replicated log.

During an S3 outage, quorum writes may continue while local replicated-log
retention absorbs the archive debt. Admission must begin throttling before
retention consumes the reserved disk budget. The decision should use:

- `unarchived_entries`
- `unarchived_bytes`
- `oldest_unarchived_age_ms`
- free bytes after the configured Raft/archive reserve
- per-tenant contribution when command ownership can be attributed

The system must never describe the backlog as drained when an upload has merely
exhausted a finite retry count. A failed range remains pending until it is
verified in S3 or an operator deliberately changes the durability policy.

## Object Layout

All production paths include the cluster identity:

```text
agentdb/v1/clusters/{cluster_id}/
  tablets/{tablet_id}/
    log/
      {first_index:020}-{last_index:020}-{first_term}-{sha256}.aglog
    checkpoints/
      {snapshot_index:020}-{snapshot_term}-{sha256}.agsnap
    inventory/
      {generation:020}-{sha256}.json
      current.json
```

Log and checkpoint objects are immutable and content-addressed. `current.json`
is an optional discovery optimization, not the only recovery record. If used,
it must be updated with an object-store conditional write (`If-Match` against
the previously read ETag, or create-only for the first generation). A LIST of
immutable inventory objects must be sufficient if the pointer is missing or an
update outcome is unknown.

The durable checkpoint identity must not contain a source or target node. A
checkpoint belongs to a tablet and index; any eligible replacement replica
should be able to consume it.

## Segment Format

Use a versioned binary envelope rather than JSON arrays of bytes. The header
contains at least:

```text
magic = "AGDBTLOG"
format_version
cluster_id
tablet_id
first_index
last_index
first_term
last_term
entry_count
uncompressed_bytes
encoded_bytes
compression_codec
encryption_key_id
encryption_key_version
payload_sha256
header_sha256
created_at_ms
```

The payload is an ordered sequence of length-prefixed records:

```text
index | term | command_length | command_bytes
```

An entry is never split between objects. A segment is valid only when indexes
are strictly increasing and cover the entire inclusive header range. Decoders
reject unknown required flags, unsupported versions, checksum mismatch,
duplicate indexes, gaps, and trailing bytes.

Object metadata may duplicate the tablet, range, version, and digest for cheap
HEAD inspection, but correctness also validates the encrypted object body; S3
metadata alone is not the durable format contract.

## Publishing And Idempotency

The archiver may race with a previous leader or retry after an unknown PUT
outcome. Publication therefore follows this sequence:

1. Derive the final key from the range identity and encoded-object hash.
2. Issue a create-only PUT.
3. If the object already exists, verify its length, format identity, and digest.
4. Verify a new object by HEAD plus a bounded header read or full checksum,
   according to the configured verification policy.
5. Publish an immutable inventory generation covering the new contiguous high
   watermark.
6. Optionally CAS `current.json` to that generation.
7. Record the verified `archived_commit_index` and emit metrics.

Overlapping exact duplicates can exist after failover. Recovery may ignore an
exact duplicate, but it rejects conflicting payloads for the same committed
index. GC may remove duplicates only after a newer inventory and a restore
verification prove they are not needed.

## Recovery

Bucket-only tablet recovery is:

```text
discover highest valid retained checkpoint
    -> verify checkpoint identity, checksum, and encryption keys
    -> restore tablet state at snapshot_index
    -> discover log objects after snapshot_index
    -> select one contiguous, non-conflicting chain
    -> replay through archived_commit_index
    -> verify applied index and state-machine invariants
    -> join Raft and fetch any surviving hot tail, if available
```

LIST order is not trusted. Recovery sorts and validates ranges itself. A gap is
a hard stop with the exact missing index range in the error. A corrupt newest
checkpoint may fall back to an older retained checkpoint only if a complete log
chain exists from that older index.

The recovery report states:

- checkpoint index and age
- archived commit index and age
- latest known cluster commit index, when available
- recovered entry and byte counts
- duplicates or superseded objects encountered
- effective RPO in entries and milliseconds
- whether a surviving Raft tail closed the remaining gap

## Checkpoints And Local Log Compaction

S3 recovery checkpoints are distinct from target-specific Raft snapshot
transfers. They are created on a regular recovery policy even when all replicas
are healthy.

Track at least:

```text
commit_index
applied_index
archived_commit_index
verified_checkpoint_index
oldest_retained_checkpoint_index
local_log_first_index
```

A safe initial compaction rule is:

```text
compact_through <= verified_checkpoint_index
```

After checkpoint-plus-log recovery is exercised, AgentDB may compact past the
checkpoint while retaining a continuous S3 chain, but it must never advance
the local compaction boundary beyond the verified S3 recovery boundary merely
because a best-effort upload was attempted.

## Content Objects Versus Tablet-Log Objects

The continuous log archive does not make the generic content mirror durable by
itself. Content bytes require separate rules:

- **Session segments, archives, and replay prefixes:** lifecycle work is already
  outside ordinary append acknowledgement. Write and verify the immutable S3
  object before publishing its metadata reference and deleting hot rows.
- **Artifacts:** upload directly to S3, preferably with streaming/multipart
  support, then commit artifact metadata. An asynchronous API must use a durable
  upload-intent state machine rather than an in-memory queue.
- **Local content copies:** treat them as caches only after the object-store copy
  is verified.

This separation prevents `ContentBackend::put` from ambiguously meaning either
"durable on this node" or "recoverable from the bucket."

## API And Internal Interfaces

The public session/event API does not change. Add an internal boundary similar
to:

```rust
pub trait CommittedLogArchive: Send + Sync {
    async fn discover_progress(&self, tablet_id: i64) -> Result<ArchiveProgress>;
    async fn put_segment(&self, segment: EncodedLogSegment) -> Result<PutOutcome>;
    async fn list_segments(
        &self,
        tablet_id: i64,
        after_index: u64,
    ) -> Result<Vec<ArchivedSegmentMeta>>;
    async fn get_segment(&self, object_key: &str) -> Result<EncodedLogSegment>;
}
```

`ReplicatedLog` remains the hot source. The archiver reads
`committed_entries(tablet_id, after_index, limit)`. A later implementation may
add a byte-bounded iterator so selection does not depend on entry count alone.

## Configuration

Proposed configuration:

```toml
[storage.committed_log_archive]
enabled = true
target_segment_bytes = 16777216
max_segment_age_ms = 30000
max_encoded_segment_bytes = 67108864
compression = "zstd"
compression_level = 1
max_concurrent_uploads = 8
max_concurrent_uploads_per_tablet = 1
max_unarchived_bytes_per_tablet = 1073741824
checkpoint_interval_ms = 600000
checkpoint_after_archived_bytes = 1073741824
```

Cluster production configuration should eventually require an object store and
this archive unless the operator explicitly selects a documented
quorum-survivability-only durability class. Development and single-node local
profiles may keep it disabled.

## Observability And Operations

Expose per-tablet gauges and cluster aggregates:

- `agentdb_tablet_commit_index`
- `agentdb_tablet_archived_commit_index`
- `agentdb_tablet_archive_lag_entries`
- `agentdb_tablet_archive_lag_bytes`
- `agentdb_tablet_oldest_unarchived_age_ms`
- `agentdb_tablet_verified_checkpoint_index`
- `agentdb_tablet_archive_put_total{outcome}`
- `agentdb_tablet_archive_put_latency_ms`
- `agentdb_tablet_archive_retry_total{class}`
- `agentdb_tablet_archive_conflict_total`
- `agentdb_tablet_archive_restore_bytes_total`
- `agentdb_tablet_archive_restore_seconds`

Readiness should fail only when the configured durability policy requires it.
S3 unavailability alone does not make a healthy Raft quorum unready, but archive
lag crossing the retained-log disk budget must trigger admission control before
disk exhaustion. Operator output distinguishes:

- Raft durability: acknowledged writes protected by the current quorum
- bucket survivability: writes covered through `archived_commit_index`
- checkpoint recoverability: latest verified checkpoint and tested restore age

## Validation Gates

The feature is not complete until these tests exercise the production object
store adapter, using MinIO locally and S3 in release qualification:

1. Append across several segment boundaries; restore from bucket only and
   compare every tablet key plus commit/applied indexes.
2. Crash after encoding, during PUT, after PUT, during verification, after
   inventory publication, and after an unknown CAS outcome.
3. Stop S3 for 10 minutes while writes continue; verify bounded retained-log
   growth, admission before reserve exhaustion, and automatic drain afterward.
4. Change leaders during upload; accept exact duplicates and reject conflicting
   commands for one index.
5. Return LIST results out of order with duplicates and gaps; recovery must
   sort, de-duplicate, and report the exact missing range.
6. Corrupt the newest segment and checkpoint independently; verify checksum
   rejection and only safe fallback to older material.
7. Lose all local content/cache directories; restore checkpoint, log history,
   lifecycle objects, and artifacts using only the bucket and KMS.
8. Measure PUTs per million appends, archive CPU, compression ratio, retained
   log bytes, steady-state lag, and restore MiB/s under 1 KiB, 16 KiB, and mixed
   AgentDB command distributions.

The initial acceptance SLO should be numerical after measurement. A reasonable
test target is `oldest_unarchived_age_ms <= 30_000` in steady state, with no
S3-induced increase in ordinary append p99 beyond measurement noise.

## Rollout

### Phase 0: Fix Content Durability Semantics

- Persist content upload intents or make lifecycle/artifact S3 publication
  synchronous with their metadata transition.
- Keep failed uploads visible and retryable across restart.
- Separate "local durable" from "bucket verified" status.

### Phase 1: Shadow Archive

- Implement the format and archiver without changing recovery or compaction.
- Compare archived ranges against local committed entries continuously.
- Expose lag, PUT rate, bytes, failures, and estimated cost.

### Phase 2: Bucket Restore

- Create regular content-addressed tablet checkpoints.
- Restore checkpoint plus S3 log in an offline drill.
- Require repeated state-invariant success before relying on the archive for
  RPO.

### Phase 3: Archive-Aware Compaction

- Gate local Raft-log compaction on the verified S3 recovery boundary.
- Add retained-log disk admission and operator remediation.
- Keep rollback available by disabling new uploads while retaining immutable
  objects already written.

### Phase 4: Production Durability Class

- Make the archive standard for production cluster profiles.
- Publish measured RPO, RTO, and cost envelopes.
- Automate bucket-only restore drills and surface their last successful age.

## Decision

AgentDB should adopt continuous committed tablet-log archival as an extension
of Raft, not a replacement for Raft. The existing session tiering, recovery
manifest, content mirror, and lag-triggered snapshot paths are useful inputs;
none currently establishes a continuous, index-addressed S3 history of every
committed tablet mutation.
