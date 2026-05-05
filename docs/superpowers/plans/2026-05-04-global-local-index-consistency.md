# Global/Local Index Consistency Implementation Plan

**Source design:** [`../specs/2026-05-04-global-local-index-consistency.md`](../specs/2026-05-04-global-local-index-consistency.md)

**Goal:** Implement DynamoDB-like secondary-index behavior for AgentDB:

- global index behaves like a GSI: cross-placement, asynchronous,
  eventually consistent
- local index behaves like an LSI: scoped to the owner placement and
  read-after-write visible for the writer
- same logical session reads observe their acknowledged writes even when
  derived indexes lag

This plan is written to land incrementally. The first phases can be
implemented against the current single-node `Runtime` record search path.
Later phases lift the same contract to placement/tablet routing and
cross-placement discovery.

## Scope

**In:**

- Commit-token and visibility-fence plumbing for mutating record/event
  paths.
- Local-index applied watermarks.
- LSI-like read behavior for the owner placement.
- Repair path that merges committed base-store tail data when the local
  index is behind the required visibility token.
- Query routing rule: sharding key present uses local index; sharding
  key absent uses global index discovery.
- GSI-like global-index interface and async mutation stream.
- Fanout coordinator behavior that treats the global index as discovery,
  not final truth.
- Tests proving same-session read-after-write under index lag.

**Out:**

- Strongly consistent global secondary index maintenance.
- Cross-placement consensus for index state.
- Replacing the whole current search/index implementation in one PR.
- Exact DynamoDB physical storage layout.
- Full multi-region semantics.

## Acceptance Criteria

- A write acknowledged to logical session `S` is visible to the next read
  from `S`, including search reads that normally use indexes.
- The write path does not wait for global-index apply.
- Local reads can prove whether the local index is current enough by
  comparing a required token with a local-index watermark.
- If the local index lags, the read path repairs visibility from
  committed base storage before replying.
- Cross-placement search uses the global index only to select candidate
  placements and still asks owner placements to enforce local visibility.
- Queries with a sharding key bypass global discovery and route directly
  to the owner placement.
- Tests cover index lag, restart/catch-up, reconnect/token propagation,
  and stale global-index discovery.

---

## Phase A - Define Commit Tokens And Visibility Fences

The consistency contract needs a concrete token before index behavior can
be made testable.

### Task A1: Choose the first commit-token representation

**Decision:** Start with a monotonic runtime commit token represented as:

```rust
pub struct CommitToken {
    pub placement_id: Option<i64>,
    pub scope: CommitScope,
    pub seq: u64,
}

pub enum CommitScope {
    Session(Id),
    Tenant(Id),
}
```

For current single-node runtime code, `placement_id` is `None` and
`seq` is the existing session-local sequence for event writes with
`CommitScope::Session`. For searchable records, use
`CommitScope::Tenant` plus a runtime-local monotonic
`record_commit_seq` until records are attached to a placement/session
commit stream.

Follow-up once HLC/tablet placement is fully wired: replace or augment
this with placement HLC/WAL offset so cross-session placement reads have
a single comparable token.

**Files:**

- Modify: `crates/agentdb-core/src/runtime.rs`
- Modify: `crates/agentdb-core/src/lib.rs`
- Modify: `crates/agentdb-api/src/dto.rs`

- [ ] Add `CommitToken` to `agentdb-core`.
- [ ] Add `last_record_commit_seq` or equivalent to runtime state.
- [ ] Return a commit token from record/event write responses where the
  API shape can tolerate it.
- [ ] Keep wire compatibility by making token fields optional in DTOs.

### Task A2: Add session visibility fence

The fence stores the latest token observed by a logical session when the
route has a `session_id`. For tenant-scoped surfaces such as records,
explicit token propagation is the reliable mechanism because plain HTTP
does not provide durable connection state across retries or reconnects.

**Files:**

- Modify: `crates/agentdb-core/src/runtime.rs`
- Modify: `crates/agentdb-api/src/routes.rs`
- Modify: `clients/typescript/src/client.ts`
- Modify: `clients/python/agentdb/client.py`

- [ ] Add a server-side fence map keyed by logical session id for
  session-scoped routes.
- [ ] Advance the fence after successful session-scoped writes.
- [ ] Let reads carry an explicit `min_visible_token` request field.
- [ ] SDKs echo the latest token for a session when available.
- [ ] If no explicit token is provided, server-side session fence is the
  fallback where the server can identify the logical session.
- [ ] For tenant-scoped record search, require explicit token echo for
  strict read-after-write across reconnects.

### Task A3: Tests for token propagation

- [ ] Unit test: a write advances the server-side fence.
- [ ] API test: write response includes a token.
- [ ] SDK shape test: a follow-up read carries the token.
- [ ] Reconnect-style test: explicit token still enforces visibility
  without relying on connection affinity.

---

## Phase B - Local Index Watermarks

The local index needs an applied watermark so reads can decide whether
the fast path is sufficient.

### Task B1: Add local-index applied watermark

**Files:**

- Modify: `crates/agentdb-core/src/runtime.rs`
- Future placement phase: placement/tablet runtime module once split
  exists.

- [ ] Add `local_index_watermark` to the local index owner.
- [ ] Advance it only after the local index has fully applied a commit.
- [ ] Expose a read helper:

```rust
fn local_index_covers(required: &CommitToken) -> bool;
```

- [ ] Add metrics:
  `local_index_applied_token`, `local_index_lag_tokens`,
  `local_index_repairs_total`.

### Task B2: Make local index apply idempotent

**Files:**

- Modify: `crates/agentdb-core/src/runtime.rs`
- Modify: `crates/agentdb-index/src/text.rs`
- Modify: `crates/agentdb-index/src/vector.rs`

- [ ] Ensure applying the same mutation twice does not duplicate final
  query results.
- [ ] Keep or introduce doc-id mapping that is stable enough for replay
  after restart.
- [ ] Tombstone/delete mutations must be represented in the local index
  or filtered after candidate resolution.

### Task B3: Watermark tests

- [ ] Unit test: local watermark advances after index apply.
- [ ] Unit test: duplicate apply is harmless.
- [ ] Unit test: watermark does not advance if index apply fails.

---

## Phase C - LSI-Like Local Read Path

The local index can be implemented synchronously or asynchronously, but
the externally visible read behavior must be LSI-like.

### Task C1: Implement visibility-repair path

**Files:**

- Modify: `crates/agentdb-core/src/runtime.rs`
- Possible new module: `crates/agentdb-core/src/index_visibility.rs`

Read algorithm for placement-local search:

1. Compute required token from explicit request token or session fence.
2. Check local-index watermark.
3. If watermark covers required token, use the normal indexed path.
4. If watermark is behind, run the indexed path and scan committed base
   data between watermark and required token.
5. Merge missing records/events into candidates.
6. Apply final filters and ranking after merge.

- [ ] Add a helper for fetching committed tail records/events by token
  range.
- [ ] Add merge logic that de-duplicates candidates by stable id.
- [ ] Apply tag/source/namespace filters after repair.
- [ ] Apply text/vector scoring consistently enough that repaired hits
  can appear in top-K when relevant.

### Task C2: Optional short wait for local catch-up

**Files:**

- Modify: `crates/agentdb-core/src/runtime.rs`
- Modify: `config/agentdb.toml`

- [ ] Add config: `index.local_catchup_wait_ms`, default `0` for first
  implementation.
- [ ] If configured, wait briefly for local watermark before repair.
- [ ] Repair remains mandatory when wait expires.

### Task C3: LSI-like behavior tests

- [ ] Force local index watermark behind the write token.
- [ ] Write a searchable record/event.
- [ ] Immediately search from the same logical session.
- [ ] Assert the just-written item appears via repair.
- [ ] Search from another session may observe lag unless it provides a
  token.
- [ ] Delete/update test: repair path does not resurrect tombstoned data.

---

## Phase D - Async Local Index Pipeline

Once repair exists, the local index can safely move out of the strict
write latency path where useful.

### Task D1: Durable local index mutation stream

**Files:**

- Modify: `crates/agentdb-core/src/persist.rs`
- Modify: `crates/agentdb-storage/src/keys.rs`
- Modify: `crates/agentdb-core/src/runtime.rs`

- [ ] Add an index-outbox entry written with the base write batch.
- [ ] Outbox entry includes commit token, operation type, object id,
  tenant/session/namespace, and enough payload/projection to index.
- [ ] Make outbox consumption idempotent.
- [ ] Add cleanup after local index watermark passes the outbox token
  and the mutation is durably snapshotted.

### Task D2: Local index worker

**Files:**

- Possible new module: `crates/agentdb-core/src/local_index_worker.rs`

- [ ] Background task scans outbox entries in commit-token order.
- [ ] Applies mutations to local text/vector/filter indexes.
- [ ] Advances local-index watermark after successful apply.
- [ ] Emits lag metrics and structured errors on failures.

### Task D3: Restart and catch-up tests

- [ ] Write N records with local worker paused.
- [ ] Restart runtime.
- [ ] Worker catches up from durable outbox.
- [ ] Same-session read works before catch-up through repair.
- [ ] Normal indexed read works after catch-up without repair.

---

## Phase E - GSI-Like Global Index Interface

The global index should start as an interface plus one simple
implementation so the query contract is correct before tuning recall.

### Task E1: Define global index trait

**Files:**

- New: `crates/agentdb-core/src/global_index.rs`
- Modify: `crates/agentdb-core/src/lib.rs`

```rust
pub trait GlobalIndex: Send + Sync {
    async fn apply(&self, mutation: GlobalIndexMutation) -> Result<()>;
    async fn discover(&self, query: GlobalIndexQuery) -> Result<GlobalIndexDiscovery>;
    async fn watermarks(&self) -> Result<Vec<GlobalIndexWatermark>>;
}
```

- [ ] Define `GlobalIndexMutation`.
- [ ] Define `GlobalIndexQuery`.
- [ ] Define discovery result as candidate placement ids plus optional
  coarse candidate ids.
- [ ] Define per-placement global-index watermarks.

### Task E2: Add no-op and in-memory implementations

**Files:**

- New: `crates/agentdb-core/src/global_index_memory.rs`

- [ ] `NoopGlobalIndex` returns no narrowing and forces fanout fallback.
- [ ] `InMemoryGlobalIndex` supports tests and single-process demos.
- [ ] Both implementations are explicitly eventually consistent.

### Task E3: Global index mutation producer

**Files:**

- Modify: `crates/agentdb-core/src/local_index_worker.rs`
- Modify: `crates/agentdb-core/src/runtime.rs`

- [ ] Reuse the durable index-outbox stream.
- [ ] Publish global mutations asynchronously after base commit.
- [ ] Do not block write ack on global apply.
- [ ] Add retry/backoff and dead-letter diagnostics for failed global
  applies.

---

## Phase F - Cross-Placement Query Integration

Global index discovery should reduce fanout, while local owners still
enforce visibility.

### Task F0: Implement query routing rule

**Files:**

- Modify: `crates/agentdb-api/src/routes.rs`
- Modify: routing/fanout modules once placement fanout is active.
- Possible new module: `crates/agentdb-core/src/search_coordinator.rs`

- [ ] If a query includes the sharding key, resolve the owner placement
  and use the local-index read path from Phase C.
- [ ] If a query does not include the sharding key, ask
  `GlobalIndex::discover` for candidate placements.
- [ ] Treat tenant and namespace filters as search predicates, not as
  placement routing keys, unless the placement model explicitly shards
  by them.
- [ ] For session-owned data, use `session_id` as the initial sharding
  key.

### Task F1: Coordinator discovery and fanout

**Files:**

- Modify: `crates/agentdb-api/src/routes.rs`
- Modify: routing/fanout modules once placement fanout is active.
- Possible new module: `crates/agentdb-core/src/search_coordinator.rs`

- [ ] For no-sharding-key cross-placement search, ask
  `GlobalIndex::discover`.
- [ ] If discovery returns no candidates or stale watermarks, widen to
  configured fallback fanout.
- [ ] Send required visibility token only to the owner placement for the
  writer's logical session.
- [ ] Each placement runs the Phase C local read algorithm.
- [ ] Coordinator merges and ranks placement results.

### Task F2: Stale global index tests

- [ ] Write to placement A while global index is paused.
- [ ] Search with sharding key routes to A and returns the write.
- [ ] Search without sharding key may miss until global catch-up or
  fallback fanout includes A.
- [ ] Search without sharding key plus a fresh visibility token widens
  fanout or routes to the known owner so read-after-write is preserved.
- [ ] With fallback fanout enabled, no-sharding-key search finds the
  write despite stale global index.

---

## Phase G - API, SDK, And Compatibility

### Task G1: Token DTOs and headers

**Files:**

- Modify: `crates/agentdb-api/src/dto.rs`
- Modify: `crates/agentdb-api/src/routes.rs`
- Modify: `clients/typescript/src/client.ts`
- Modify: `clients/python/agentdb/client.py`

- [ ] Add optional `commit_token` to write responses.
- [ ] Add optional `min_visible_token` to read/search requests.
- [ ] Consider `X-AgentDB-Commit-Token` and
  `X-AgentDB-Min-Visible-Token` for route families where body changes
  are awkward.
- [ ] SDKs keep token management per logical session.

### Task G2: Backward compatibility

- [ ] Existing clients that do not send tokens continue to work.
- [ ] Server-side session fence preserves same-session behavior where
  the server can identify the logical session.
- [ ] Explicit token path is documented as the reliable mode across
  reconnects, retries, and node handoff.

---

## Phase H - Observability And Operations

### Task H1: Metrics

- [ ] `local_index_watermark`
- [ ] `local_index_lag_tokens`
- [ ] `local_index_repairs_total`
- [ ] `local_index_repair_duration_ms`
- [ ] `global_index_watermark`
- [ ] `global_index_lag_tokens`
- [ ] `global_index_apply_failures_total`
- [ ] `global_index_discovery_fallback_total`

### Task H2: Admin/debug surface

- [ ] Add admin endpoint or dashboard panel exposing local and global
  index lag.
- [ ] Include last applied token, last mutation time, and repair count.
- [ ] Add runbook entry for sustained index lag.

---

## Phase I - Documentation

### Task I1: Architecture docs

**Files:**

- Modify: `docs/architecture/ARCHITECTURE.md`
- Modify: `docs/architecture/DESIGN.md`
- Modify: `docs/operations/PERFORMANCE.md`

- [ ] Document GSI-like and LSI-like behavior.
- [ ] Document same-session read-after-write guarantee.
- [ ] Document that global index is not the strong-consistency boundary.
- [ ] Document index lag metrics and operational implications.

### Task I2: API docs

**Files:**

- Modify: `docs/reference/api-sdk.md`
- Modify: SDK READMEs as needed.

- [ ] Document `commit_token` and `min_visible_token`.
- [ ] Explain SDK automatic token propagation.
- [ ] Explain when users may need to persist/echo a token manually.

---

## Risks

- **Token comparability.** Session `seq` is enough for session-local
  event reads, but not enough for general placement-wide record search.
  Mitigation: use a temporary runtime record sequence first, then move
  to placement HLC/WAL offset when the placement stream is the common
  substrate.
- **Repair-path cost.** If local indexing is badly behind, tail scans
  can become expensive. Mitigation: cap lag alerts, make the local index
  worker high priority, and use optional short wait where it improves
  p99 without violating correctness.
- **Ranking repaired vector hits.** Newly written embeddings may need
  scoring outside HNSW. Mitigation: score repaired tail candidates
  exactly against the query vector before final merge.
- **Global-index stale discovery.** A GSI-like index can miss fresh
  placements. Mitigation: session-local reads route to owner placement;
  no-sharding-key reads use fallback fanout when watermarks are stale,
  discovery is empty, or a fresh visibility token requires stronger
  read-after-write behavior than the global index can provide.

## Recommended Landing Order

1. Phase A: tokens and fences.
2. Phase B: local index watermarks.
3. Phase C: LSI-like local repair path.
4. Phase D: async local pipeline.
5. Phase E: global index interface.
6. Phase F: cross-placement fanout integration.
7. Phase G-H-I: API polish, observability, docs alongside the above
   phases.
