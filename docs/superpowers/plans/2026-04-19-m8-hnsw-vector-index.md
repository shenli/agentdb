# M8: HNSW Vector Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the O(n) brute-force `InMemoryFlatVec` in `agentdb-index` with an HNSW-backed `HnswVectorIndex` while preserving the existing `VectorIndex` trait surface and all current test behavior.

**Architecture:** Swap the per-path vector store inside `crates/agentdb-index/src/vector.rs` for an HNSW built on first `knn` call (lazy, to match the existing `PerBoxIndexes::indexes_built` architecture), with a small-N brute-force bypass for correctness on tiny datasets. Use `instant-distance` as the HNSW library. No other crate's code or public interface changes.

**Tech Stack:** Rust 1.75, `instant-distance` 0.6 (HNSW), `parking_lot` 0.12 (Mutex for interior mutability under `&self`), existing `thiserror`/`serde_json` stack.

---

## File Structure

**Modified:**
- `Cargo.toml` (workspace) — add `instant-distance = "0.6"` and make sure `parking_lot` is in workspace deps
- `crates/agentdb-index/Cargo.toml` — depend on `instant-distance` + `parking_lot`
- `crates/agentdb-index/src/vector.rs` — replace `InMemoryFlatVec` with `HnswVectorIndex`, delete brute-force struct
- `crates/agentdb-core/src/indexes.rs` — change the field type from `InMemoryFlatVec` to `HnswVectorIndex`
- `docs/ARCHITECTURE.md` — fix inaccurate "Storage layout" entries
- `docs/PERFORMANCE.md` — add new "Vector search" section with before/after numbers

**Created:**
- `crates/agentdb-index/benches/vector_bench.rs` — `cargo bench --bench vector_bench` for the perf-doc numbers (standalone, does its own inline brute-force baseline for the recall comparison)

---

## Task 1: Add dependencies

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/agentdb-index/Cargo.toml`

- [ ] **Step 1: Add `instant-distance` to workspace dependencies**

Open `Cargo.toml` (workspace root). Under `[workspace.dependencies]`, add one line:

```toml
instant-distance = "0.6"
```

(`parking_lot` is already in workspace deps from M7.)

- [ ] **Step 2: Depend on both from agentdb-index**

Edit `crates/agentdb-index/Cargo.toml`. Change the `[dependencies]` section to:

```toml
[dependencies]
instant-distance = { workspace = true }
parking_lot = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
```

- [ ] **Step 3: Verify the workspace still builds**

Run: `cargo build --workspace`
Expected: clean build (resolves the new crate).

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml crates/agentdb-index/Cargo.toml Cargo.lock
git commit -m "deps(agentdb-index): add instant-distance + parking_lot"
```

---

## Task 2: Implement `HnswVectorIndex` with lazy build

**Files:**
- Modify: `crates/agentdb-index/src/vector.rs`

This task replaces `InMemoryFlatVec` with `HnswVectorIndex` while keeping every existing test passing. We do the port first, then add new tests in Task 4.

- [ ] **Step 1: Read existing file to confirm baseline tests**

Read: `crates/agentdb-index/src/vector.rs`

Confirm these existing tests are the ones we must keep passing unchanged:
- `dim_mismatch_on_insert_returns_error`
- `knn_returns_identical_vector_first`
- `knn_limit_is_respected`
- `unknown_path_returns_empty`
- `paths_are_isolated`
- `query_dim_mismatch_returns_empty`

- [ ] **Step 2: Rewrite `crates/agentdb-index/src/vector.rs` in full**

Replace the entire file with:

```rust
use std::collections::HashMap;

use instant_distance::{Builder, HnswMap, Point, Search};
use parking_lot::Mutex;

use crate::doc_id::DocId;
use crate::error::{IndexError, IndexResult};

pub type Score = f32;

/// Default HNSW `M` parameter (unused by `instant-distance` 0.6 — see
/// note below — but kept in the surface for future library upgrades).
pub const DEFAULT_M: usize = 16;
/// Default `ef_construction` passed to `instant_distance::Builder`.
pub const DEFAULT_EF: usize = 100;
/// Below this N, we skip HNSW entirely and brute-force cosine. Keeps
/// tiny-dataset tests deterministic and avoids HNSW graph-construction
/// corner cases at small N.
pub const BRUTE_FORCE_THRESHOLD: usize = 64;

pub trait VectorIndex: Send + Sync {
    fn insert(&mut self, path: &str, doc_id: DocId, vector: &[f32]) -> IndexResult<()>;
    fn knn(&self, path: &str, query: &[f32], limit: usize) -> Vec<(DocId, Score)>;
}

/// HNSW-backed vector index. One `HnswMap` per path, built lazily on
/// the first `knn` call and cached until a new `insert` arrives (which
/// marks the graph dirty and forces a rebuild on the next query).
pub struct HnswVectorIndex {
    per_path: Mutex<HashMap<String, PathState>>,
    m: usize,
    ef: usize,
}

struct PathState {
    dim: usize,
    entries: Vec<(DocId, Vec<f32>)>, // unit-normalized vectors
    built: Option<HnswMap<NormPoint, DocId>>,
    dirty: bool,
}

#[derive(Clone)]
struct NormPoint(Vec<f32>);

impl Point for NormPoint {
    fn distance(&self, other: &Self) -> f32 {
        // Both points are unit-normalized on insert and on query, so dot
        // product is cosine similarity in [-1, 1]. Convert to a distance
        // where smaller = more similar: 1.0 - cos_sim in [0, 2].
        let sim: f32 = self.0.iter().zip(other.0.iter()).map(|(a, b)| a * b).sum();
        1.0 - sim
    }
}

impl Default for HnswVectorIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl HnswVectorIndex {
    pub fn new() -> Self {
        Self {
            per_path: Mutex::new(HashMap::new()),
            m: DEFAULT_M,
            ef: DEFAULT_EF,
        }
    }

    pub fn with_params(m: usize, ef: usize) -> Self {
        Self {
            per_path: Mutex::new(HashMap::new()),
            m,
            ef,
        }
    }

    /// Reads `AGENTDB_HNSW_M` and `AGENTDB_HNSW_EF` from the environment,
    /// falling back to `DEFAULT_M` and `DEFAULT_EF` if missing or
    /// unparseable.
    pub fn from_env() -> Self {
        let m = std::env::var("AGENTDB_HNSW_M")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_M);
        let ef = std::env::var("AGENTDB_HNSW_EF")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_EF);
        Self::with_params(m, ef)
    }
}

fn normalise(v: &[f32]) -> Vec<f32> {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm <= 1e-12 {
        return v.to_vec();
    }
    v.iter().map(|x| x / norm).collect()
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

impl VectorIndex for HnswVectorIndex {
    fn insert(&mut self, path: &str, doc_id: DocId, vector: &[f32]) -> IndexResult<()> {
        let mut map = self.per_path.lock();
        let state = map.entry(path.to_string()).or_insert_with(|| PathState {
            dim: vector.len(),
            entries: Vec::new(),
            built: None,
            dirty: true,
        });
        if state.dim != vector.len() {
            return Err(IndexError::Invalid(format!(
                "vector dim mismatch at path {path}: expected {}, got {}",
                state.dim,
                vector.len()
            )));
        }
        state.entries.push((doc_id, normalise(vector)));
        state.dirty = true;
        state.built = None;
        Ok(())
    }

    fn knn(&self, path: &str, query: &[f32], limit: usize) -> Vec<(DocId, Score)> {
        if limit == 0 {
            return Vec::new();
        }
        let mut map = self.per_path.lock();
        let Some(state) = map.get_mut(path) else {
            return Vec::new();
        };
        if state.dim != query.len() {
            return Vec::new();
        }
        let q_norm = normalise(query);

        // Small-N: brute force, deterministic, bypasses HNSW.
        if state.entries.len() < BRUTE_FORCE_THRESHOLD {
            let mut ranked: Vec<(DocId, Score)> = state
                .entries
                .iter()
                .map(|(id, v)| (*id, dot(&q_norm, v)))
                .collect();
            ranked.sort_by(|(_, a), (_, b)| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
            ranked.truncate(limit);
            return ranked;
        }

        // Large-N: build `HnswMap` lazily (attaches DocIds as values),
        // then query it.
        //
        // Note: `instant-distance` 0.6 does not expose an `M` knob on
        // `Builder` — only `ef_construction`. We keep `self.m` on the
        // surface for future library upgrades but don't pass it here.
        if state.dirty || state.built.is_none() {
            let _ = self.m;
            let points: Vec<NormPoint> = state
                .entries
                .iter()
                .map(|(_, v)| NormPoint(v.clone()))
                .collect();
            let doc_ids: Vec<DocId> = state.entries.iter().map(|(id, _)| *id).collect();
            let built = Builder::default()
                .ef_construction(self.ef)
                .build(points, doc_ids);
            state.built = Some(built);
            state.dirty = false;
        }

        let built = state.built.as_ref().expect("just built");
        let query_point = NormPoint(q_norm);
        let mut search = Search::default();
        let mut out: Vec<(DocId, Score)> = Vec::with_capacity(limit);
        for map_item in built.search(&query_point, &mut search).take(limit) {
            // MapItem.value is &DocId (the value we built the map with).
            // MapItem.distance is 1 - cos_sim, so we invert for the caller.
            let sim: Score = 1.0 - map_item.distance;
            out.push((*map_item.value, sim));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(session: u64, seq: u64) -> DocId {
        DocId::new(session, seq)
    }

    #[test]
    fn dim_mismatch_on_insert_returns_error() {
        let mut idx = HnswVectorIndex::new();
        idx.insert("p", d(1, 1), &[1.0, 0.0]).unwrap();
        let err = idx.insert("p", d(1, 2), &[1.0, 0.0, 0.0]).unwrap_err();
        assert!(err.to_string().contains("dim mismatch"));
    }

    #[test]
    fn knn_returns_identical_vector_first() {
        let mut idx = HnswVectorIndex::new();
        idx.insert("p", d(1, 1), &[1.0, 0.0, 0.0]).unwrap();
        idx.insert("p", d(1, 2), &[0.0, 1.0, 0.0]).unwrap();
        idx.insert("p", d(1, 3), &[0.0, 0.0, 1.0]).unwrap();
        let hits = idx.knn("p", &[1.0, 0.0, 0.0], 3);
        assert_eq!(hits[0].0, d(1, 1));
        assert!(hits[0].1 > 0.99);
    }

    #[test]
    fn knn_limit_is_respected() {
        let mut idx = HnswVectorIndex::new();
        for i in 0..10 {
            idx.insert("p", d(1, i), &[i as f32, 1.0]).unwrap();
        }
        let hits = idx.knn("p", &[1.0, 1.0], 3);
        assert_eq!(hits.len(), 3);
    }

    #[test]
    fn unknown_path_returns_empty() {
        let idx = HnswVectorIndex::new();
        let hits = idx.knn("none", &[0.0], 10);
        assert!(hits.is_empty());
    }

    #[test]
    fn paths_are_isolated() {
        let mut idx = HnswVectorIndex::new();
        idx.insert("a", d(1, 1), &[1.0, 0.0]).unwrap();
        idx.insert("b", d(1, 2), &[1.0, 0.0]).unwrap();
        let a = idx.knn("a", &[1.0, 0.0], 10);
        let b = idx.knn("b", &[1.0, 0.0], 10);
        assert_eq!(a.len(), 1);
        assert_eq!(b.len(), 1);
        assert_ne!(a[0].0, b[0].0);
    }

    #[test]
    fn query_dim_mismatch_returns_empty() {
        let mut idx = HnswVectorIndex::new();
        idx.insert("p", d(1, 1), &[1.0, 0.0]).unwrap();
        let hits = idx.knn("p", &[1.0], 10);
        assert!(hits.is_empty());
    }
}
```

- [ ] **Step 3: Run the existing unit tests**

Run: `cargo test -p agentdb-index --lib vector::tests`
Expected: all 6 tests pass (all under `BRUTE_FORCE_THRESHOLD`, so they hit the brute-force branch — behavior should be identical to before).

- [ ] **Step 4: Commit**

```bash
git add crates/agentdb-index/src/vector.rs
git commit -m "feat(agentdb-index): introduce HnswVectorIndex with small-N bypass"
```

---

## Task 3: Plumb `HnswVectorIndex` into `PerBoxIndexes`

**Files:**
- Modify: `crates/agentdb-core/src/indexes.rs`

- [ ] **Step 1: Swap the type and constructor**

Edit `crates/agentdb-core/src/indexes.rs`. Change these three lines:

Line 4:
```rust
use agentdb_index::vector::{InMemoryFlatVec, VectorIndex};
```
to:
```rust
use agentdb_index::vector::{HnswVectorIndex, VectorIndex};
```

Line 18 (inside `pub struct PerBoxIndexes`):
```rust
    pub vector: InMemoryFlatVec,
```
to:
```rust
    pub vector: HnswVectorIndex,
```

Line 33 (inside `PerBoxIndexes::new`):
```rust
            vector: InMemoryFlatVec::new(),
```
to:
```rust
            vector: HnswVectorIndex::from_env(),
```

- [ ] **Step 2: Verify the workspace still compiles and tests pass**

Run: `cargo test --workspace --no-fail-fast 2>&1 | tail -40`
Expected: all tests pass. In particular, these two in `indexes.rs` should still work:
- `build_indexes_vector_from_payload_embedding_array`
- `build_indexes_counts_across_sessions_and_tenants`

- [ ] **Step 3: Commit**

```bash
git add crates/agentdb-core/src/indexes.rs
git commit -m "feat(agentdb-core): use HnswVectorIndex in PerBoxIndexes"
```

---

## Task 4: Add HNSW-specific unit tests

**Files:**
- Modify: `crates/agentdb-index/src/vector.rs`

These tests exercise the HNSW-branch code path (N ≥ `BRUTE_FORCE_THRESHOLD`) that the inherited tests can't reach.

- [ ] **Step 1: Add recall test (above threshold, forces HNSW)**

Append to `#[cfg(test)] mod tests { ... }` in `crates/agentdb-index/src/vector.rs`, just before the closing `}`:

```rust
    /// Insert 256 random 128-dim points including a known target. A
    /// query close to the target must put it in the top-5. Seeded for
    /// reproducibility.
    #[test]
    fn knn_recall_on_256_vectors_returns_target_in_top_k() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn pseudo_rand(seed: u64, i: u64) -> f32 {
            let mut h = DefaultHasher::new();
            (seed, i).hash(&mut h);
            // Map hash to [-1.0, 1.0]
            ((h.finish() as f64 / u64::MAX as f64) * 2.0 - 1.0) as f32
        }

        let mut idx = HnswVectorIndex::new();
        let dim = 128;
        // Target vector at seq=999.
        let target: Vec<f32> = (0..dim).map(|j| pseudo_rand(42, j as u64)).collect();
        idx.insert("emb", d(1, 999), &target).unwrap();

        for i in 0..256u64 {
            let v: Vec<f32> = (0..dim).map(|j| pseudo_rand(i + 1000, j as u64)).collect();
            idx.insert("emb", d(1, i), &v).unwrap();
        }

        // Query = slight perturbation of target.
        let query: Vec<f32> = target
            .iter()
            .enumerate()
            .map(|(j, x)| x + pseudo_rand(99, j as u64) * 0.01)
            .collect();
        let hits = idx.knn("emb", &query, 5);
        let top_ids: Vec<DocId> = hits.iter().map(|(id, _)| *id).collect();
        assert!(
            top_ids.contains(&d(1, 999)),
            "target not in top-5: got {:?}",
            top_ids
        );
    }
```

- [ ] **Step 2: Add path-isolation-across-dims test**

Append immediately after the previous test:

```rust
    #[test]
    fn dim_mismatch_across_paths_is_isolated() {
        let mut idx = HnswVectorIndex::new();
        idx.insert("p2", d(1, 1), &[1.0, 0.0]).unwrap();
        idx.insert("p3", d(1, 2), &[1.0, 0.0, 0.0]).unwrap();
        // Neither insert should interfere; both paths queryable with
        // their own dims.
        let a = idx.knn("p2", &[1.0, 0.0], 1);
        let b = idx.knn("p3", &[1.0, 0.0, 0.0], 1);
        assert_eq!(a.len(), 1);
        assert_eq!(b.len(), 1);
        // A query with the wrong dim against a path returns empty, not
        // a cross-path hit.
        let wrong = idx.knn("p2", &[1.0, 0.0, 0.0], 1);
        assert!(wrong.is_empty());
    }
```

- [ ] **Step 3: Add rebuild-after-insert test**

Append:

```rust
    #[test]
    fn insert_after_knn_invalidates_cached_hnsw() {
        let mut idx = HnswVectorIndex::new();
        // Seed above threshold so the HNSW branch fires.
        for i in 0..80u64 {
            let v: Vec<f32> = vec![i as f32, 1.0, 0.0, 0.0, 0.0];
            idx.insert("p", d(1, i), &v).unwrap();
        }
        // First query builds.
        let _ = idx.knn("p", &[1.0, 1.0, 0.0, 0.0, 0.0], 3);
        // Insert a new extremely-close vector.
        idx.insert("p", d(1, 999), &[1.0, 1.0, 0.0, 0.0, 0.0]).unwrap();
        // Second query must see it.
        let hits = idx.knn("p", &[1.0, 1.0, 0.0, 0.0, 0.0], 3);
        let ids: Vec<DocId> = hits.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&d(1, 999)), "new insert not reflected: {:?}", ids);
    }
```

- [ ] **Step 4: Run the new tests**

Run: `cargo test -p agentdb-index --lib vector::tests`
Expected: all 9 tests pass (6 inherited + 3 new).

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-index/src/vector.rs
git commit -m "test(agentdb-index): cover HNSW branch with recall + isolation + dirty-cache tests"
```

---

## Task 5: Fix `docs/ARCHITECTURE.md` key-layout inaccuracies

**Files:**
- Modify: `docs/ARCHITECTURE.md`

- [ ] **Step 1: Remove the two inaccurate index-key lines**

Read the file and find the "Storage layout (RocksDB keys)" code block. The two lines describing index keys:

```
index_posting/{box_id}/{token}/...             -> posting lists
index_vector/{box_id}/{session_internal}/{seq} -> embedding bytes
```

Delete both. Indexes are not stored in RocksDB.

- [ ] **Step 2: Add a clarifying paragraph after the code block**

After the closing ``` of the key-layout code block, add a new paragraph:

```
Indexes are NOT stored in RocksDB. `PerBoxIndexes` is built lazily per
box by walking all events (warm KV + cold segment files) on the first
query and cached until server restart. New events appended after the
build do not flow into the cached index — rebuilding incrementally is
a known limitation carried forward from the prototype's lazy design.
```

- [ ] **Step 3: Verify nothing else references the fake keys**

Run: `grep -n "index_posting\|index_vector" docs/ARCHITECTURE.md`
Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add docs/ARCHITECTURE.md
git commit -m "docs(architecture): drop fake index_vector/index_posting KV keys"
```

---

## Task 6: Benchmark HNSW vs brute-force and update `PERFORMANCE.md`

**Files:**
- Create: `crates/agentdb-index/benches/vector_bench.rs`
- Modify: `crates/agentdb-index/Cargo.toml` (add `criterion` dev-dep + `[[bench]]` entry)
- Modify: `docs/PERFORMANCE.md`

The benchmark does its own inline brute-force baseline so we can measure both without keeping the retired `InMemoryFlatVec` struct around.

- [ ] **Step 1: Add `criterion` as a dev-dep**

Edit `crates/agentdb-index/Cargo.toml`. Change `[dev-dependencies]` to:

```toml
[dev-dependencies]
anyhow = { workspace = true }
criterion = "0.5"

[[bench]]
name = "vector_bench"
harness = false
```

Then run: `cargo build -p agentdb-index --benches`
Expected: `criterion` downloads and compiles.

- [ ] **Step 2: Write the bench file**

Create `crates/agentdb-index/benches/vector_bench.rs` with:

```rust
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::hint::black_box;

use agentdb_index::{
    vector::{HnswVectorIndex, VectorIndex},
    DocId,
};
use criterion::{criterion_group, criterion_main, Criterion};

const N: usize = 10_000;
const DIM: usize = 384;
const QUERIES: usize = 100;
const TOP_K: usize = 10;

fn pseudo_rand(seed: u64, i: u64) -> f32 {
    let mut h = DefaultHasher::new();
    (seed, i).hash(&mut h);
    ((h.finish() as f64 / u64::MAX as f64) * 2.0 - 1.0) as f32
}

fn gen_vecs(n: usize, dim: usize, seed_base: u64) -> Vec<Vec<f32>> {
    (0..n as u64)
        .map(|i| (0..dim as u64).map(|j| pseudo_rand(seed_base + i, j)).collect())
        .collect()
}

fn brute_force_knn(vecs: &[Vec<f32>], query: &[f32], k: usize) -> Vec<(usize, f32)> {
    let mut ranked: Vec<(usize, f32)> = vecs
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let norm_v: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-12);
            let norm_q: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-12);
            let dot: f32 = v.iter().zip(query.iter()).map(|(a, b)| a * b).sum();
            (i, dot / (norm_v * norm_q))
        })
        .collect();
    ranked.sort_by(|(_, a), (_, b)| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    ranked.truncate(k);
    ranked
}

fn bench(c: &mut Criterion) {
    let vecs = gen_vecs(N, DIM, 1);
    let queries = gen_vecs(QUERIES, DIM, 1_000_000);

    // Build the HNSW index once, outside the measured region.
    let mut idx = HnswVectorIndex::new();
    for (i, v) in vecs.iter().enumerate() {
        idx.insert("bench", DocId::new(0, i as u64), v).unwrap();
    }
    // Warm the cache (trigger lazy build).
    let _ = idx.knn("bench", &queries[0], TOP_K);

    c.bench_function("hnsw_knn_10k_x_384", |b| {
        let mut q = 0usize;
        b.iter(|| {
            let res = idx.knn("bench", black_box(&queries[q % QUERIES]), TOP_K);
            q += 1;
            res
        })
    });

    c.bench_function("brute_knn_10k_x_384", |b| {
        let mut q = 0usize;
        b.iter(|| {
            let res = brute_force_knn(&vecs, black_box(&queries[q % QUERIES]), TOP_K);
            q += 1;
            res
        })
    });

    // Recall@10: fraction of brute-force top-10 present in HNSW top-10,
    // averaged over all queries. Printed once; criterion won't graph it.
    let mut total_recall = 0.0;
    for q in &queries {
        let truth = brute_force_knn(&vecs, q, TOP_K);
        let hits = idx.knn("bench", q, TOP_K);
        // Brute index → DocId::new(0, i) → doc_id.seq = i.
        let truth_ids: std::collections::HashSet<u64> = truth.iter().map(|(i, _)| *i as u64).collect();
        let hnsw_ids: std::collections::HashSet<u64> = hits.iter().map(|(id, _)| id.seq).collect();
        let overlap = truth_ids.intersection(&hnsw_ids).count();
        total_recall += overlap as f32 / TOP_K as f32;
    }
    let avg_recall = total_recall / queries.len() as f32;
    eprintln!("recall@10 (averaged over {} queries) = {:.3}", queries.len(), avg_recall);
}

criterion_group!(benches, bench);
criterion_main!(benches);
```

- [ ] **Step 3: Run the benchmark**

Run: `cargo bench -p agentdb-index --bench vector_bench 2>&1 | tee /tmp/m8-bench.log`
Expected: two timing lines (`hnsw_knn_10k_x_384` and `brute_knn_10k_x_384`) plus one `recall@10 ...` line on stderr.

Take note of:
- HNSW median latency
- Brute-force median latency
- The recall@10 number

- [ ] **Step 4: Add "Vector search" section to `docs/PERFORMANCE.md`**

Read `docs/PERFORMANCE.md`. Find the last top-level section before "Gotchas" (the one titled "Session lifecycle — end-to-end"). After that section's content and before the `## Gotchas` header, insert a new section using the numbers you captured in Step 3:

```markdown
## Vector search (knn)

`crates/agentdb-index/benches/vector_bench.rs` builds one index with
10 000 pseudo-random 384-dimension points and runs 100 queries at
top-k = 10. The HNSW index is built once outside the measured region
(lazy-build behavior). Brute-force numbers come from an inline
reference implementation in the same bench.

| impl                    | median latency | vs brute force |
|-------------------------|---------------:|---------------:|
| brute force (reference) | XX.X ms        | 1.00×          |
| HNSW (instant-distance) | X.X ms         | YY×            |

Recall@10 (overlap between HNSW top-10 and brute-force top-10,
averaged over 100 queries): **Z.ZZ**.

Reproduce:
```bash
cargo bench -p agentdb-index --bench vector_bench
```
```

Replace `XX.X`, `X.X`, `YY`, `Z.ZZ` with the numbers from Step 3.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-index/Cargo.toml crates/agentdb-index/benches/vector_bench.rs Cargo.lock docs/PERFORMANCE.md
git commit -m "bench(agentdb-index): add HNSW vs brute-force vector_bench + perf doc"
```

---

## Task 7: Milestone exit gate

- [ ] **Step 1: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: all tests pass.

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 3: Check formatting**

Run: `cargo fmt --all --check`
Expected: clean (no output).

- [ ] **Step 4: Check that Go code is unaffected**

Run: `cd workload && go build ./... && go vet ./... && cd ..`
Expected: clean.

- [ ] **Step 5: Run the smoke script**

Run: `bash scripts/smoke.sh`
Expected: `smoke ok`.

- [ ] **Step 6: If anything failed, fix it**

If any of Steps 1-5 reported problems, fix them inline and commit with message
`fix(m8): <short description>`. Do not mark this task complete until all five run clean.

---

## Self-review checklist (controller)

Before handing off, confirm each spec requirement has a task:

- [x] Swap `InMemoryFlatVec` → `HnswVectorIndex` — Task 2
- [x] Use `instant-distance` — Task 1 + Task 2
- [x] Keep `VectorIndex` trait signatures — Task 2 (no trait changes)
- [x] Per-path isolation, first-vector-wins dim — Task 2 (preserved in code), Task 4 test
- [x] `AGENTDB_HNSW_M` / `AGENTDB_HNSW_EF` env vars — Task 2 (`from_env`)
- [x] Fix `docs/ARCHITECTURE.md` — Task 5
- [x] Update `docs/PERFORMANCE.md` — Task 6
- [x] All existing tests pass — Task 2 Step 3, Task 3 Step 2, Task 7 Step 1
- [x] `parking_lot::Mutex` for interior mutability — Task 2
- [x] Small-N brute-force bypass — Task 2 (`BRUTE_FORCE_THRESHOLD`)
- [x] Clippy + fmt + smoke — Task 7
