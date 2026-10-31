# AgentDB Lifecycle Storage

This document describes AgentDB's warm/cold/archive/derivative-only storage
model for session history and artifact bytes. It is a forward-looking design
document; “today” in the original problem statement describes the design
baseline. For current behavior, see [tiered storage operations](../operations/tiered-storage.md)
and the [architecture map](architecture.md).
For the tablet-local hot storage engine, see
[`tablet-storage-engine.md`](tablet-storage-engine.md). For the product
motivation behind lifecycle-aware runtime storage, see
[`product-thesis.md`](product-thesis.md).

## Problem

AgentDB already models session lifecycle with `Warm`, `Cold`, `Archive`, and
`DerivativeOnly`, but the current runtime mostly treats these as lifecycle
labels rather than distinct storage contracts.

Today:

- session lifecycle transitions are primarily metadata updates
- event reads still come from the same runtime store regardless of lifecycle
- artifact bytes are separated from metadata, but session event history is not
- operator and performance docs still imply a stronger object-store-backed
  tiering model than the current runtime actually implements

This creates a gap between product story and implementation reality.

## Goals

- Make lifecycle state correspond to real storage behavior.
- Keep `Warm` and `Cold` read semantics identical.
- Make `Archive` behavior explicit rather than accidental.
- Preserve append, replay, fork, and context semantics across lifecycle
  transitions.
- Expose lifecycle as an operational system through APIs, metrics, and the
  dashboard.

## Non-Goals

- Distributed placement-aware tier orchestration
- Multi-region archival workflows
- Fully asynchronous restore orchestration in the first slice
- Warehouse-style analytical partitioning across tenants

## Design Principles

### Lifecycle Is A Storage Contract

`Warm`, `Cold`, `Archive`, and `DerivativeOnly` should mean something concrete
about where bytes live, what can be read, and what latency/feature tradeoffs
apply.

### Query Semantics First

`Cold` should affect storage cost and latency, not correctness. Reads that work
against a warm session should continue to work against a cold session.

### Coarse Movement Units

Storage transitions should move sealed session-history units, not individual
event rows. This is the AgentDB equivalent of using partitions as the movement
unit in analytical systems.

### Keep Runtime APIs Stable

Clients should not need to understand whether a session is backed by hot rows,
sealed segments, or archived bundles. The runtime should hide those details
behind one logical session-history interface.

## Lifecycle Semantics

### Warm

- session is appendable
- recent mutable history lives in the hot runtime store
- low-latency reads and writes

### Cold

- session is no longer actively hot
- full-fidelity event history lives in sealed session segments
- reads remain semantically equivalent to warm
- appending creates or restores a warm head without rewriting the full history

### Archive

- session history is retained at lower cost than cold
- storage may move to a cheaper object class or an archive package
- reads still work, but latency and some capabilities may be degraded
- appending does not mutate archived history in place; the runtime reopens a
  warm head above the archived base history
- any degraded behavior must be explicit in API and product documentation

### DerivativeOnly

- raw event bytes and/or artifact bytes are gone
- metadata, references, summaries, and other derived searchable state remain
- full replay is no longer guaranteed

## Storage Model

The redesign separates runtime metadata from session-history data.

### Hot Metadata

The runtime store continues to own:

- tenants
- agents
- sessions
- event envelopes or warm-head rows
- artifact metadata
- records
- policy state

### Session Segments

Cold history is stored as sealed session segments. A segment is a durable,
immutable chunk of one session's event stream.

Suggested segment metadata:

```rust
pub struct SessionSegment {
    pub tenant_id: Id,
    pub session_id: Id,
    pub segment_id: Id,
    pub seq_start: u64,
    pub seq_end: u64,
    pub event_count: u64,
    pub byte_size: u64,
    pub checksum: String,
    pub storage_uri: String,
    pub storage_class: String,
    pub created_at_ms: u64,
}
```

This object is the lifecycle movement unit.

### Archive Manifest

Archive state should not be inferred solely from `session.lifecycle`. It should
be represented explicitly by an archive manifest.

Suggested archive metadata:

```rust
pub struct SessionArchiveManifest {
    pub tenant_id: Id,
    pub session_id: Id,
    pub manifest_id: Id,
    pub source_segments: Vec<Id>,
    pub storage_uri: String,
    pub storage_class: String,
    pub read_mode: String, // full | lazy | summary_only
    pub summary_artifact_id: Option<Id>,
    pub created_at_ms: u64,
}
```

### Session Storage State

Session rows should carry explicit storage state rather than relying only on the
coarse lifecycle enum.

Suggested shape:

```rust
pub struct SessionStorageState {
    pub warm_head_present: bool,
    pub warm_head_seq_start: Option<u64>,
    pub sealed_segment_ids: Vec<Id>,
    pub archive_manifest_id: Option<Id>,
    pub restore_state: Option<String>, // none | pending | restoring | failed
    pub last_transition_at_ms: u64,
}
```

This can evolve, but the key point is that lifecycle becomes inspectable and
operationally meaningful.

## Segment Strategy

Do not model cold storage as one forever-growing session blob.

Preferred model:

- one mutable warm head
- one or more immutable sealed segments
- optional archive package or manifest

Example:

- `seg_001`: `seq 0..199`
- `seg_002`: `seq 200..399`
- warm head: `seq 400..N`

This makes the system easier to:

- transition between tiers
- restore incrementally
- compact later
- reason about operationally

## Read Path Contract

All session-history reads should go through one internal abstraction.

Suggested surface:

```rust
trait SessionHistoryReader {
    async fn read_range(
        &self,
        tenant_id: &Id,
        session_id: &Id,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>>;

    async fn read_tail(
        &self,
        tenant_id: &Id,
        session_id: &Id,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>>;

    async fn read_all(
        &self,
        tenant_id: &Id,
        session_id: &Id,
    ) -> Result<Vec<EventEnvelope>>;
}
```

Behavior by lifecycle:

- `Warm`: read warm head and any needed sealed segments
- `Cold`: read sealed segments directly
- `Archive`: read archive package or archived segments
- `DerivativeOnly`: return only the preserved derived view; full replay is not
  promised

The following runtime features should consume the same logical history reader:

- `fetch`
- `append-and-fetch`
- replay
- fork-parent visibility
- context packing
- dashboard session explorer
- search paths that constrain by session history

## Write Path Contract

Appends always target a warm mutable head.

When a client appends to a cold session:

- the runtime opens a new warm head for the session
- existing cold segments remain sealed
- the session becomes `Warm`

This avoids full-history rehydration in the common case and keeps resumed
sessions cheap to reactivate.

When a client appends to an archived session:

- the runtime does not modify archive bundles in place
- the runtime either reopens a new warm head directly or restores to cold first
- archived history remains immutable base history for replay and reads
- newly appended events land in the warm head

In both cases, the logical session timeline is assembled from:

- older immutable history in sealed segments or archive bundles
- newer mutable history in the warm head

This is the core append contract for tiered session storage: old history stays
immutable, while new activity grows from a fresh warm tail.

## Lifecycle Transitions

### Warm To Cold

1. Seal the warm head into one or more immutable segments.
2. Persist segment metadata.
3. Remove hot event rows for the sealed range.
4. Keep session metadata and lightweight indexes.
5. Mark the session as cold.

### Cold To Warm

Preferred first implementation:

1. Create a new warm head.
2. Keep existing sealed segments in place.
3. Read old history lazily through the session-history reader.

This is cheaper than eager full rehydration and better matches resumed-agent
behavior.

### Cold To Archive

1. Persist an archive manifest or transition storage class.
2. Optionally compact cold segments into an archive package.
3. Mark the session as archived with an explicit read mode.

### Archive To Warm

Two acceptable designs:

- synchronous restore for small/local deployments
- explicit async restore state for slow archive backends

The product must report restore state clearly.

Append-on-archive may reuse this transition, or it may reopen a warm head
without eager full restore. In either design, archive bundles remain immutable
and new writes do not rewrite old archived history.

### Archive To DerivativeOnly

1. Purge raw session-history bytes according to policy.
2. Preserve metadata, refs, summaries, and indexed derivatives.
3. Mark the session as derivative-only.

## Search And Derived Data

Search should not depend on whether raw history is warm or cold.

AgentDB should treat raw session history and derived searchable state as
separate storage concerns.

- Raw session history exists for replay, recovery, fork, and audit.
- Derived searchable state exists for discovery, retrieval, filtering, and
  lightweight context loading.

Tier transitions should move raw history between storage contracts without
implicitly destroying the index/search contract.

Recommended model:

- index on append for warm sessions
- retain index documents when sealing to cold
- compact archive search representation if needed
- preserve only derived/indexed documents in derivative-only state

Derived state may include:

- extracted tool results
- summaries
- source refs
- searchable observations
- embeddings

### Index Behavior By Lifecycle

#### Warm

- Raw history is mutable and appendable.
- Derived search/index documents are built incrementally on append.
- Expensive derivative work may be throttled or batched, but the system should
  not require a later cold/archive pass just to make warm data discoverable.

#### Warm To Cold

- Move raw session-history bytes into sealed immutable segments.
- Keep existing search/index documents live.
- Do not force queries to scan sealed segment blobs just because the raw
  history left the warm store.

Cold should change storage cost and mutability, not basic discoverability.

#### Cold

- Full replay should remain available from sealed segments.
- Search should continue to resolve against derived/indexed state rather than
  depending on direct reads of cold segment blobs.
- Index/search data should remain logically valid even though raw history is no
  longer in the warm mutable path.

#### Cold To Archive

- Compact or move raw session-history bytes into an archive bundle or colder
  storage class.
- Preserve derived search/index state by default.
- Optionally compact the archive-side search representation if the platform
  wants lower index cost for old data.

Archive should not require rebuilding search results by opening full archive
bundles for every query.

#### Archive

- Raw history may have higher latency or reduced read behavior depending on the
  archive contract.
- Search should continue to operate from derived/indexed state.
- Platforms may choose between multiple archive search modes:
  - full index retained
  - compact index retained
  - summary-only search retained

The chosen archive search mode should be explicit in storage policy and product
documentation.

#### Archive To DerivativeOnly

- Purge raw session-history bytes.
- Keep summaries, refs, searchable observations, and other derived/indexed
  state that remains useful without full replay.
- Treat derived/indexed state as the surviving searchable representation of the
  session.

### Design Rules

- Do not treat secondary indexes as an implementation detail of the warm tier.
- Do not require direct scans of archive blobs for ordinary search paths.
- Do not tie search correctness to physical placement of raw history bytes.
- Make any degradation in archive search capability explicit rather than
  accidental.
- Let lifecycle policy decide whether old data keeps a full index, compact
  index, or summary-only representation.

This keeps lifecycle movement independent from the search contract.

## Artifact Content Optimization

Artifacts are not normal session events. They are typically larger, more
expensive to transfer, more likely to be binary, and more likely to benefit from
range reads or streaming. AgentDB should keep the existing separation:

- artifact metadata stays in the runtime store
- raw artifact bytes live in `ContentBackend`
- session events carry `artifact_refs`, not raw blob payloads

That split lets append, replay, fork, and context paths stay small while raw
bytes are fetched only by explicit blob reads.

### Artifact Goals

- Keep session append, replay, and fetch paths small and fast.
- Make artifact bytes cheap to reference and expensive only when actually read.
- Support object-store-backed large blobs without full in-memory buffering.
- Keep artifact lifecycle independent from session metadata lifecycle.
- Preserve metadata even when raw bytes move storage class or are purged.

### Large Blob API Direction

The current `ContentBackend` shape is memory-oriented: upload takes a complete
byte slice and download returns the whole blob. That is acceptable for small
artifacts, but large artifacts need explicit streaming and partial-read paths:

```rust
trait ContentBackend {
    async fn put(&self, ...) -> Result<PutResult>;
    async fn get(&self, ...) -> Result<Vec<u8>>;

    // Future additions:
    async fn put_stream(&self, ...);
    async fn get_stream(&self, ...);
    async fn get_range(&self, ...);
}
```

The existing simple APIs can remain for small payloads. Streaming upload,
streaming download, object-store multipart upload, and range reads should be the
preferred path for large blobs.

### Artifact Lifecycle

Artifact bytes may need a different lifecycle from session metadata:

- metadata stays hot and queryable
- raw bytes move to cheaper object-store classes
- raw bytes may be deleted while metadata and derived state remain
- integrity verification can confirm object existence and `sha256`

This matches the `DerivativeOnly` direction: users should still be able to see
what artifact existed, which event referenced it, and which derived records came
from it, even if the original bytes are gone by policy.

### Artifact Priorities

If artifact work is split into phases, prioritize:

1. streaming upload and download
2. object-store multipart upload
3. range reads and partial preview APIs
4. storage-class transitions and artifact metrics
5. deduplication and integrity-verification workers

## Policy Model

The current lifecycle policy should evolve from threshold-only settings into a
real storage policy.

Suggested additions:

```rust
pub struct LifecyclePolicy {
    pub cold_after_ms: Option<u64>,
    pub archive_after_ms: Option<u64>,
    pub derivative_only_after_ms: Option<u64>,
    pub seal_after_events: Option<u64>,
    pub seal_after_bytes: Option<u64>,
    pub cold_storage_class: Option<String>,
    pub archive_storage_class: Option<String>,
    pub rehydrate_on_append: Option<bool>,
    pub archive_read_mode: Option<String>, // full | lazy | summary_only
}
```

Important policy questions:

- Does archive preserve full replay?
- Is archive restore automatic or explicit?
- Does derivative-only retain event envelopes, only summaries, or neither?
- What latency or availability guarantees apply per tier?

## Operator APIs

Tiered storage should be visible and controllable.

Recommended admin APIs:

```text
POST /api/admin/sessions/{session_id}/lifecycle/transition
GET  /api/admin/sessions/{session_id}/storage
POST /api/admin/sessions/{session_id}/restore
GET  /api/admin/lifecycle/queue
GET  /api/admin/tenants/{tenant_id}/storage-summary
GET  /api/admin/storage/segments
```

Expected session storage response:

- lifecycle
- warm head status
- segment count
- bytes by tier
- archive manifest status
- restore state
- last transition time
- last transition error

## Lifecycle Worker

The current full-session sweep model should be replaced with scheduled
eligibility.

Recommended approach:

- compute next eligible lifecycle transition times on append or transition
- maintain an indexed queue or min-heap of due sessions
- execute transitions with bounded concurrency

This avoids scanning every session on every sweep and reduces interference with
the hot path.

## Dashboard Implications

The dashboard should expose lifecycle as an operational system.

Overview:

- sessions by lifecycle
- bytes by tier
- transition rate
- restore rate and latency
- transition failures
- sessions pending transition

Tenant page:

- warm/cold/archive/derivative-only counts
- largest sessions or tenants by storage
- recent restores
- failed transitions

Session page:

- lifecycle badge
- storage breakdown
- segment list
- archive/restore status
- transition history
- event explorer

## Metrics

Recommended metrics:

- `sessions_by_lifecycle`
- `session_segments_total`
- `session_segment_bytes`
- `lifecycle_transition_duration_ms`
- `lifecycle_transition_failures_total`
- `session_restore_requests_total`
- `session_restore_duration_ms`
- `archive_reads_total`
- `rehydrate_on_append_total`
- `derivative_only_purges_total`

## Migration Strategy

Build this incrementally.

### Phase 1: Make Cold Real

- add session segment metadata
- introduce a session-history reader abstraction
- make cold reads real for session history

### Phase 2: Append To Cold Via New Warm Head

- stop assuming full rehydrate is required
- keep older history sealed

### Phase 3: Reintroduce Operator Control Surface

- admin lifecycle endpoints
- lifecycle metrics
- dashboard storage visibility

### Phase 4: Make Archive Real

- archive manifest
- storage-class movement or archive package
- explicit restore/read-mode contract

### Phase 5: Finalize DerivativeOnly

- raw purge policy
- derived-only retrieval semantics
- search/index guarantees

## Open Questions

- Should `Archive` preserve full-fidelity replay in the product contract, or is
  that only guaranteed for `Warm` and `Cold`?
- Should archive restore be synchronous in single-node mode and asynchronous in
  future remote-storage deployments?
- Should segment sealing happen only on lifecycle transition, or also on size
  thresholds during long-running hot sessions?
- How much event envelope metadata survives into `DerivativeOnly`?

## Summary

The redesign makes AgentDB's tier model real:

- `Warm` is mutable hot runtime state
- `Cold` is full-fidelity sealed session history
- `Archive` is a lower-cost retained form with explicit read semantics
- `DerivativeOnly` is raw bytes removed, derived state retained

The core shift is from lifecycle-as-label to lifecycle-as-storage-contract.
