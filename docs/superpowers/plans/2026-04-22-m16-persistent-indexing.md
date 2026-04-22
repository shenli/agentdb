# M16 Persistent + Incremental Indexing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Write tests first for each non-trivial step (superpowers:test-driven-development).

**Goal:** Turn `PerBoxIndexes` from "lazy rebuild on first query, lost on restart" into "always-current + fast recovery after restart." Confirmed P0 by the BoTE (cold-start rebuild is ~33 min at v2-target scale; intolerable).

**Scope:**

- Swap `instant-distance` (which rebuilds the full HNSW graph on every insert) for a library that supports O(log N) incremental insert.
- Incremental insert in the write path: every `append_event` updates the per-box index.
- Periodic snapshots of HNSW + BM25 state to object storage under `indexes/{box_id}/...`.
- Recovery on startup: load latest snapshot + replay WAL events newer than the snapshot's HLC, rather than scanning every session.
- Graceful drain: take a final snapshot before shutdown.

**Non-goals (explicitly deferred):**

- Cross-box / cross-tablet index fanout — that is M23 (fanout coordinator).
- Hot-tablet auto-split — M24.
- Consensus-based replication of index state — v3+ (§10.6 of v2 design).
- Re-indexing all historical data after a backend swap — out of scope; documented as a rebuild-from-events procedure.

**Source design:** `docs/agent-db-v2.md` §6.1 (persistent + incremental indexing).
**Plan ref:** `docs/superpowers/plans/2026-04-21-v2-design-implementation.md` §4 M16.
**Acceptance criteria:**
- Restart a box holding 10⁶ vectors → indexes ready for queries in < 10s (vs current O(minutes) full scan).
- Recall@10 unchanged or better vs `instant-distance` baseline.
- Events appended after a snapshot are correctly included in query results via WAL replay.

---

## Phase A — HNSW backend spike

The chosen library decides everything else (Phase B's swap is a straightforward refactor but only if the chosen library matches our needs). Do the evaluation before committing.

### Task A1: Candidate libraries

Candidates (all pure Rust or reasonable FFI):

- **`hnsw_rs`** — pure Rust, incremental insert, `serde` serialization, used in production by Qdrant's test benches. Primary candidate.
- **`usearch`** — C++ core via FFI. Very fast, memory-mapped snapshot files. Secondary candidate; higher build-time cost.
- **`instant-distance` (current)** — no incremental insert. Out.

- [ ] **Step 1: Add `hnsw_rs` as a throwaway dep in `agentdb-index/Cargo.toml` (commented out until spike passes)**

- [ ] **Step 2: Write a spike test in `agentdb-index/tests/hnsw_backend_spike.rs`**

Exercise:
- Insert 10k random 384-d vectors one-at-a-time
- Query top-10 for a held-out vector, measure recall vs brute force
- Serialize to a tempfile, drop, deserialize, re-query, assert identical results
- Insert 1k more vectors incrementally post-load, re-query, assert new vectors appear

If any step fails or the library API doesn't cover the need, try `usearch`. Document outcome in `docs/superpowers/notes/2026-04-22-hnsw-backend-spike.md`.

---

## Phase B — Backend swap

Assuming Phase A picks `hnsw_rs` (otherwise equivalent for `usearch`).

### Task B1: Keep the `VectorIndex` trait API stable; swap the impl

**Files:**
- Modify: `crates/agentdb-index/src/vector.rs`
- Modify: `crates/agentdb-index/Cargo.toml`

- [ ] **Step 1: Add `hnsw_rs` as a real dep**
- [ ] **Step 2: Replace `HnswVectorIndex` internals** (same trait, same public API) to use `hnsw_rs::hnsw::Hnsw` with incremental insert.
- [ ] **Step 3: Keep the brute-force `BRUTE_FORCE_THRESHOLD` path** for small-N determinism.
- [ ] **Step 4: Run existing tests** (`knn_returns_identical_vector_first`, etc.) and fix any API drift.
- [ ] **Step 5: Benchmark vs `instant-distance`** on a 10k-vector synthetic workload. Sign off only if query latency + recall are within 5% of baseline.

### Task B2: Remove `instant-distance` dep after all tests pass

- [ ] Drop `instant-distance` from `Cargo.toml` and re-run full workspace to verify nothing leaks through.

---

## Phase C — Incremental insert on the write path

Today `append_event` does not touch the index; `PerBoxIndexes::build` walks every session from scratch on first query.

### Task C1: Hook into `append_event`

**Files:**
- Modify: `crates/agentdb-core/src/primitives.rs` (in `append_event`)
- Modify: `crates/agentdb-core/src/box_runtime.rs` (expose write-access to indexes)

- [ ] **Step 1: `Box_` exposes an `indexes_write()` method** that acquires the write-guard on `RwLock<Option<PerBoxIndexes>>`. If indexes haven't been built yet (first request ever), trigger an initial build before inserting the new event, same as the existing `indexes_built` read path.

- [ ] **Step 2: After the KV write commits in `append_and_fetch_with_actor`, update indexes**

```rust
if let Some(indexes) = box_.indexes_write().await.as_mut() {
    indexes.index_event(session.internal_id, &event);
}
```

- [ ] **Step 3: Regression test** — append an event, query immediately via `HybridPlanner`, hit should appear (previously this would miss because the index was built at first-query time and never refreshed).

### Task C2: Metrics

- [ ] Counter `events_indexed` on `BoxMetrics`. Increment from the incremental-insert path.

---

## Phase D — Index snapshot to object storage

### Task D1: Serializable `IndexSnapshot`

**Files:**
- New: `crates/agentdb-core/src/index_snapshot.rs`

- [ ] **Step 1: Define the on-disk format**

```rust
pub struct IndexSnapshot {
    pub schema_version: u32,        // 1, bumpable
    pub box_id: u16,
    pub hlc_ts: HlcTimestamp,       // cutoff; events with hlc_ts <= this are in the snapshot
    pub hnsw: HnswSnapshot,         // per-path HNSW state, serialized via library's mechanism
    pub bm25: Bm25Snapshot,         // postings + doc-length tables
    pub agent_events: BTreeMap<String, Vec<DocId>>,
}
```

- [ ] **Step 2: Decide format — bincode or bson?**

Match the existing segment choice (bson). Trade-off: bson is bigger but consistent with segments. Accept the size for uniformity.

- [ ] **Step 3: `write_snapshot(&indexes, &store, path) -> Result<()>`** and **`read_snapshot(&store, path) -> Result<IndexSnapshot>`**. Object path: `indexes/{box_id:04x}/snap-{hlc_ts.physical_ns:016x}.bson`.

### Task D2: Snapshot from the live `PerBoxIndexes`

- [ ] Add `PerBoxIndexes::to_snapshot(&self, box_id, hlc_cutoff) -> IndexSnapshot`.
- [ ] Add `PerBoxIndexes::from_snapshot(snap) -> Self`.

### Task D3: Periodic snapshot job

**Files:**
- New: `crates/agentdb-core/src/index_snapshot_job.rs` (or fold into `lifecycle.rs`)

- [ ] Per-box background task, tick every `AGENTDB_INDEX_SNAPSHOT_SECS` (default 3600 = hourly).
- [ ] Acquires a read guard on indexes, captures the HLC cutoff at that moment, serializes under write lock on its own filesystem stage, uploads to object store.
- [ ] Metrics: `index_snapshots_total`, `index_snapshot_duration_ms`, `index_snapshot_bytes`.
- [ ] Prune old snapshots: keep last N (default 3), delete older via object store.

---

## Phase E — Recovery on startup

### Task E1: Snapshot discovery

- [ ] **`list_latest_snapshot(&store, box_id) -> Result<Option<String>>`** — lists `indexes/{box_id:04x}/` and returns the path of the highest-HLC snapshot. `None` if no snapshots exist.

### Task E2: Recovery path

Modify `indexes_built` (or introduce a new `indexes_recovered` that replaces it):

- [ ] **Step 1: Try load latest snapshot**. If found: materialize `PerBoxIndexes` from it; capture `hlc_cutoff` = snapshot's HLC.
- [ ] **Step 2: Replay WAL events after cutoff**. Scan every session's events with `hlc_ts > cutoff`, call `index_event` on each.
- [ ] **Step 3: If no snapshot**: fall back to the existing full-rebuild path (it still works).
- [ ] **Step 4: Integration test**: write 10k events, snapshot, write 100 more, restart simulation → indexes serve all 10100 events.

### Task E3: Acceptance-criterion benchmark

- [ ] **Throw-away benchmark**: 10⁶ vectors in index; snapshot; "restart" by creating a fresh `Box_` and timing `indexes_built`. Must be < 10s. If not, profile and tune (HNSW build parallelism, snapshot compression, etc.)

---

## Phase F — Graceful drain

### Task F1: Drain snapshot

- [ ] `agentdb drain` CLI command (from §15.5 of design) calls `Box_::snapshot_indexes()` synchronously before exit.
- [ ] Integration test: drain → restart → ready within expected budget (no replay needed since snapshot covers everything).

---

## Phase G — Documentation

### Task G1: ARCHITECTURE.md update

- [ ] New section "Persistent + incremental indexing (M16)" documenting:
  - Why (cold-start blast radius from BoTE)
  - Snapshot format + object-storage path
  - Periodic job + drain-on-shutdown
  - Recovery algorithm (load snapshot → replay WAL)
  - Env vars: `AGENTDB_INDEX_SNAPSHOT_SECS`, snapshot retention

### Task G2: Update the v2 plan's M16 entry if scope changed

---

## Risks

- **Backend library behavioral differences.** `hnsw_rs` may have different recall characteristics than `instant-distance`. Mitigation: B1 Step 5 benchmark gates the swap; if recall regresses, either tune ef/M parameters or fall back to `usearch`.
- **Snapshot size at Type III scale.** 10⁶ vectors × 1536-d × 4 bytes = ~6 GB raw per path. Serialized (with graph overhead) could be ~8 GB. Object-store upload of that size is minutes. Mitigation: compress (zstd), or (v2+) shard snapshots. For prototype, accept uncompressed 10 GB snapshots at top-end scale.
- **Replay cost at high event volumes.** If a snapshot is hours old and a busy tablet has millions of new events, the replay walks all of them. Mitigation: more frequent snapshots (AGENTDB_INDEX_SNAPSHOT_SECS tunable).

## Out of scope

- Vector-quantization / PQ-compressed indexes — future perf lever.
- Multi-tenant index sharing / dedup — structural, not M16.
- Cross-box snapshot replication — M23+.
