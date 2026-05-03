# AgentDB Artifact Storage Optimization

This document captures the artifact-storage design direction that should be
implemented after the current tiered session-storage work lands.

For current runtime architecture, see [`ARCHITECTURE.md`](ARCHITECTURE.md).
For session hot/cold/archive storage, see
[`TIERED_STORAGE_REDESIGN.md`](TIERED_STORAGE_REDESIGN.md).

## Problem

Artifacts are not normal session events.

They are typically:

- much larger than event payloads
- more expensive to transfer
- more likely to be binary
- more likely to benefit from range reads or streaming
- subject to different retention and cost policies than runtime metadata

AgentDB already makes the most important separation:

- artifact metadata stays in the runtime store
- raw artifact bytes live in `ContentBackend`
- session events only carry `artifact_refs`

That is the correct foundation, but it is not enough for large-blob workloads.

## Goals

- Keep session append, replay, and fetch paths small and fast.
- Make artifact bytes cheap to reference and expensive only when actually read.
- Support large object-store-backed blobs without full in-memory buffering.
- Keep artifact lifecycle independent from session metadata lifecycle.
- Work cleanly with local filesystem and object-store backends.

## Non-Goals

- Full CDN/media-delivery pipeline
- Deduplication across arbitrary transformed derivatives in the first slice
- Cross-region artifact replication policy
- Inline artifact preview generation for every content type

## Current Good Decisions

The current design already gets several important things right:

- artifacts are stored out-of-line through `ContentBackend`
- session events hold `artifact_refs`, not raw blob payloads
- integrity is tracked with `sha256`
- raw bytes can be purged independently from artifact metadata
- the same abstraction works for local FS and object-store-backed deployments

Those choices should stay.

## Design Principles

### Metadata And Blob Bytes Must Stay Separate

Artifact metadata belongs in the runtime store because it needs to be:

- searchable
- referenceable from session events
- visible in dashboards and APIs
- retained even if raw bytes are later purged

Raw bytes belong in the content backend because they are:

- large
- expensive to scan
- storage-class-sensitive
- read rarely compared to event metadata

### Blob Access Should Be Lazy

Most runtime operations should not touch artifact bytes at all.

The common path should be:

- write event with `artifact_ref`
- replay event with `artifact_ref`
- inspect metadata

Only explicit blob reads should fetch content bytes.

### Optimize For Object Storage First

For large artifacts, the object-store path is the important one:

- higher latency
- remote transfer costs
- range-read support
- multipart upload support
- storage-class movement

Local filesystem support should remain, but object-store behavior should drive
the large-blob design.

## Recommended Optimizations

### 1. Streaming Upload And Download

Current `ContentBackend` behavior is memory-oriented:

- upload takes a full byte slice
- download returns the full blob into memory

That is acceptable for small blobs but not for large artifacts.

Recommended evolution:

```rust
trait ContentBackend {
    async fn put(&self, ...) -> Result<PutResult>;
    async fn get(&self, ...) -> Result<Vec<u8>>;

    // Future additions:
    async fn put_stream(&self, ...);
    async fn get_stream(&self, ...);
}
```

The existing API can remain for small artifacts. Streaming should be the
preferred path for large uploads and downloads.

### 2. Multipart Upload For Object Stores

Large S3 uploads should use multipart upload instead of a single put.

Benefits:

- lower retry cost for failed uploads
- better throughput for large artifacts
- better operational behavior for multi-GB blobs

The right abstraction is not `S3ContentBackend`, but the already-correct
generalization: `ObjectStoreContentBackend`, with multipart behavior enabled
when the backend supports it.

### 3. Range Reads

Range reads are important for:

- large logs
- trace dumps
- structured artifacts where only the head/tail is needed
- UI preview

Recommended future API shape:

```rust
async fn get_range(
    &self,
    tenant_id: &str,
    artifact_id: &str,
    start: u64,
    end_exclusive: u64,
) -> Result<Vec<u8>>;
```

This should be supported first on object-store backends, then mirrored on local
FS.

### 4. Compression As A Policy Option

Compression is useful for:

- logs
- JSON
- text traces

Compression is usually not useful for:

- already-compressed images
- archives
- media

So compression should be:

- explicit
- content-type-aware
- optional per artifact or per policy

The system should not blindly compress every artifact.

### 5. Content Deduplication

If identical blobs are repeatedly uploaded, AgentDB should eventually be able
to store them once and let multiple artifact metadata records reference the
same underlying object.

This is especially useful for:

- repeated tool outputs
- checkpoints
- duplicated logs from retries

The natural key for this is content hash, but the implementation should be
careful not to change the user-facing artifact identity model.

### 6. Independent Storage Classes

Artifact bytes should support a different lifecycle than session metadata.

Examples:

- metadata stays hot and queryable
- raw bytes move to cheaper object-store classes
- raw bytes may be deleted while metadata remains

This matches the existing `DerivativeOnly` direction and should be preserved.

### 7. Integrity Verification

The runtime already records `sha256`. A future background verifier should:

- confirm object existence
- confirm hash integrity
- surface mismatch or missing-object alerts

This matters more once object-store-backed deployments become common.

## Prioritization

If only a few optimizations are implemented in the next artifact milestone,
they should be:

1. streaming upload and download
2. multipart object-store upload
3. range reads

These provide the highest practical value for large-blob workloads.

## API Direction

Recommended high-level evolution:

- keep the current simple artifact APIs for small payloads
- add explicit large-artifact/streaming paths
- preserve `artifact_ref` as the normal event-level contract
- do not force clients to download bytes just to inspect a session

## Backend Expectations

### LocalFsContentBackend

Should remain:

- simple
- durable enough for single-node local deployments
- feature-compatible where reasonable

### ObjectStoreContentBackend

Should become the primary large-artifact backend.

Near-term expectations:

- S3-compatible storage
- eventual multipart upload support
- eventual range-read support
- lifecycle/storage-class-aware operations

This backend name is intentionally generic because the implementation should be
usable with more than S3 over time.

## Operational Model

Operators should be able to reason about artifacts separately from sessions:

- artifact count
- artifact bytes
- bytes by storage class
- bytes by tenant
- raw-byte purge count
- integrity-check failure count

The dashboard and storage summary APIs should eventually expose these signals.

## Suggested Future Work

### Phase 1

- streaming artifact APIs
- object-store range reads
- multipart upload support

### Phase 2

- artifact storage-class transitions
- better artifact metrics and dashboard surfaces
- partial preview APIs

### Phase 3

- deduplication
- integrity verification worker
- richer artifact retention policy

## Bottom Line

Artifacts should remain:

- cheap to reference
- expensive only when explicitly read
- independently retained and tiered from session history

The current `ContentBackend` split is correct. Future work should focus on
large-blob behavior, not on collapsing artifacts back into normal session
storage.
