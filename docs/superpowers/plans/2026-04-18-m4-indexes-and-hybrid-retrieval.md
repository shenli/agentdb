# AgentDB Prototype — Milestone 4: Indexes + hybrid retrieval + `$vectorSearch`/`$search`

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the architectural headline — "hybrid retrieval in one query plan" — real. Add in-memory text (BM25) and vector (cosine) indexes per box, a small hybrid planner with two strategies (locality-bound and over-fetch post-filter), and Mongo-style `$vectorSearch` / `$search` aggregation stages so callers can run hybrid queries from the `mongodb` Rust driver or `mongosh`.

**Architecture:** New `agentdb-index` crate holds the `TextIndex` and `VectorIndex` traits plus simple in-memory implementations. `agentdb-core` grows a `PerBoxIndexes` struct on each `Box_`, lazily built from `cf_events` on first query, and a `HybridPlanner` that picks a strategy based on the filter shape. `agentdb-wire` adds an `aggregate` command handler that parses a small Mongo pipeline (`$vectorSearch` / `$search` as source stages, `$match` / `$limit` / `$project` as trailing transforms) and delegates to the planner.

**Tech Stack (M4 additions):** No new external crates — the in-memory BM25 and cosine implementations are hand-rolled (~350 lines combined). We use `ahash` transitively via `bson` so no direct dep needed.

**References:**
- Spec: `docs/superpowers/specs/2026-04-17-agentdb-prototype-design.md` §6 (indexing), §6.3 (hybrid planner), §7.1 (aggregation stages)
- Previous milestones: `docs/superpowers/plans/2026-04-17-m1-storage-foundation.md`, `docs/superpowers/plans/2026-04-18-m2-core-types-and-append-and-fetch.md`, `docs/superpowers/plans/2026-04-18-m3-mongo-wire-subset.md`

---

## Scope

**In scope for M4:**
- `agentdb-index` crate with:
  - `TextIndex` trait + `InMemoryInverted` impl (tokenize + BM25 scoring)
  - `VectorIndex` trait + `InMemoryFlatVec` impl (brute-force cosine top-K)
- Per-box index state in `agentdb-core`, lazily built from `cf_events`
- `HybridPlanner` with 2 strategies:
  - **Locality-bound**: filter contains `session_id=` → scan just that session's events and score in memory
  - **Over-fetch post-filter**: use index for top-K*over_fetch candidates, apply filter in memory, truncate to limit
- `aggregate` Mongo command dispatch in `agentdb-wire` with the following pipeline stages:
  - `$vectorSearch`: query vector + path + numCandidates + limit + filter
  - `$search`: text query + path + filter
  - `$match`, `$limit`, `$project` as trailing transforms
- Integration tests using the real `mongodb` Rust driver for one `$vectorSearch` and one `$search` query

**Out of scope (deferred):**
- Secondary index for nested fields (spec §6.2 third strategy) — the planner's selective pre-filter strategy falls back to over-fetch post-filter in M4
- `$sort`, `$skip`, `$unwind`, and most other pipeline stages (M5+)
- Eager index maintenance on append — M4 builds the index lazily on first query per box and does NOT update it on subsequent writes. The `cargo run` demo loop would need to call a refresh endpoint or restart the server. Explicitly tracked as a known limitation
- Index persistence — in-memory only; rebuilt on process restart
- Hybrid queries that combine `$vectorSearch` AND `$search` in one pipeline (either-or in M4)
- Custom text analyzers (tokenization is whitespace + lowercase + ASCII-only; drop tokens shorter than 2 chars)
- Rich distance metrics beyond cosine (Euclidean / dot-product deferred)

**Concurrency contract:** Index build is serialized per box with a `tokio::sync::Mutex<()>` (reuses the existing box write-lock to avoid concurrent rebuilds). Once built, reads are lock-free; the index structure is wrapped in `Arc<RwLock<InnerState>>` and read-only from that point. For M4 "no eager updates" means the read lock is acquired once and released after the query — no writer contention.

---

## File Structure

All paths relative to repo root `/Users/lishen/work/agentdb`.

**Created:**
- `crates/agentdb-index/Cargo.toml`
- `crates/agentdb-index/src/lib.rs`
- `crates/agentdb-index/src/error.rs`
- `crates/agentdb-index/src/doc_id.rs` — `DocId` newtype + sentinel helpers
- `crates/agentdb-index/src/text.rs` — `TextIndex` trait + `InMemoryInverted`
- `crates/agentdb-index/src/vector.rs` — `VectorIndex` trait + `InMemoryFlatVec`
- `crates/agentdb-core/src/indexes.rs` — `PerBoxIndexes`, build-from-events
- `crates/agentdb-core/src/planner.rs` — `HybridPlanner`, strategy enum, result type
- `crates/agentdb-wire/src/handlers/aggregate.rs` — `$vectorSearch`, `$search`, trailing stages

**Modified:**
- `Cargo.toml` (add `agentdb-index` workspace member)
- `crates/agentdb-core/Cargo.toml` (depend on `agentdb-index`)
- `crates/agentdb-core/src/box_runtime.rs` (add `indexes: Arc<PerBoxIndexes>` to `Box_`; lazy-build)
- `crates/agentdb-core/src/lib.rs` (declare `indexes`, `planner` modules; re-export entry types)
- `crates/agentdb-wire/Cargo.toml` (depend on `agentdb-index`; `mongodb` dev-dep already present)
- `crates/agentdb-wire/src/handlers/mod.rs` (`pub mod aggregate;`)
- `crates/agentdb-wire/src/command.rs` (add `aggregate` branch to dispatch)

**Responsibilities:**
- `text.rs` / `vector.rs` — pure data structures and algorithms; no I/O, no storage coupling
- `doc_id.rs` — DocId is (session_id, seq); opaque to outside the crate
- `indexes.rs` — wraps the two indexes per box, knows how to build from `cf_events`
- `planner.rs` — pure logic over filter shape + indexes; returns ranked `(DocId, score, event)` tuples
- `aggregate.rs` — parses Mongo pipeline; drives planner; formats result cursor

---

## Data model notes

### DocId

Per-box monotonic internal id for index entries:
```rust
pub struct DocId {
    pub session_internal_id: u64,
    pub seq: u64,
}
```

Why `(session, seq)` and not an opaque u64? Because the planner's locality-bound strategy needs to filter the candidate set to "events within session X" cheaply — we can construct the filter predicate as `doc.session_internal_id == target_session_id` without a side lookup. At query time we resolve `DocId → Event` via `scan_session_events` (existing M3 primitive) or via a direct prefix lookup in `cf_events`.

### Indexed paths

On build, we walk each event's payload JSON and index the following paths:
- **Text index**: every string-valued leaf, indexed under its dotted path (e.g. `payload.text`, `payload.message`, `payload.kind`)
- **Vector index**: every array-of-numbers leaf where the array length is 2–2048, indexed under its dotted path (e.g. `payload.embedding`)

Rationale: the Mongo aggregation stages specify a `path`. We look that path up as a string key into the indexes. No configuration ceremony.

The path prefix `payload.` is included because the aggregation stages spec says e.g. `path: "payload.text"` — full dotted path from the top-level event document.

### BM25 formula (for reference)

Standard BM25 over `doc_freq` (how many documents contain the term) and `term_freq` in each doc:
```
idf(q) = ln((N - df(q) + 0.5) / (df(q) + 0.5) + 1)
score(q, d) = sum_over_terms [ idf(q) * (tf(q, d) * (k1 + 1)) /
                               (tf(q, d) + k1 * (1 - b + b * |d| / avgdl)) ]
```
Constants: `k1 = 1.2`, `b = 0.75`. Document length `|d|` is token count after tokenization.

### Cosine similarity

Normalize at index time (store unit vectors). At query time, normalize the query; similarity = dot product. Top-K via sort-and-truncate.

---

## Task 1: Scaffold `agentdb-index` crate

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Create: `crates/agentdb-index/Cargo.toml`
- Create: `crates/agentdb-index/src/lib.rs`
- Create: `crates/agentdb-index/src/error.rs`
- Create stubs: `crates/agentdb-index/src/{doc_id,text,vector}.rs`

- [ ] **Step 1: Register the new crate in workspace members**

Edit `Cargo.toml`. In `[workspace] members = [...]` list, add `crates/agentdb-index` in alphabetical position. Final list:
```toml
members = [
    "crates/agentdb-storage",
    "crates/agentdb-core",
    "crates/agentdb-api",
    "crates/agentdb-index",
    "crates/agentdb-wire",
    "bin/agentdb-server",
]
```

- [ ] **Step 2: Write the crate Cargo.toml**

Save `crates/agentdb-index/Cargo.toml`:
```toml
[package]
name = "agentdb-index"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
anyhow = { workspace = true }
```

- [ ] **Step 3: Error type**

Save `crates/agentdb-index/src/error.rs`:
```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("{0}")]
    Invalid(String),
}

pub type IndexResult<T> = std::result::Result<T, IndexError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_formats() {
        let e = IndexError::Invalid("bad path".into());
        assert_eq!(e.to_string(), "bad path");
    }
}
```

- [ ] **Step 4: Stub lib.rs**

Save `crates/agentdb-index/src/lib.rs`:
```rust
//! AgentDB in-memory indexing primitives.
//!
//! Provides `TextIndex` and `VectorIndex` traits and simple in-memory
//! implementations used by the box runtime to score events at query time.

pub mod doc_id;
pub mod error;
pub mod text;
pub mod vector;

pub use doc_id::DocId;
pub use error::{IndexError, IndexResult};
```

Stub the three source files with single-line comments pointing to the tasks that will fill them in:
- `crates/agentdb-index/src/doc_id.rs`: `// Filled in by Task 2.`
- `crates/agentdb-index/src/text.rs`: `// Filled in by Task 3.`
- `crates/agentdb-index/src/vector.rs`: `// Filled in by Task 4.`

- [ ] **Step 5: Verify compile + tests**

Run: `cargo check --workspace`
Expected: clean.

Run: `cargo test -p agentdb-index error::tests`
Expected: 1 passed.

Confirm M3 tests still pass:
Run: `cargo test -p agentdb-storage -p agentdb-core -p agentdb-api -p agentdb-wire`
Expected: 106 tests pass.

- [ ] **Step 6: Format and commit**

Run: `cargo fmt --all && cargo fmt --all --check`
```bash
git add Cargo.toml crates/agentdb-index
git commit -m "chore: scaffold agentdb-index crate"
```

---

## Task 2: `DocId` newtype

**Files:**
- Modify: `crates/agentdb-index/src/doc_id.rs`

- [ ] **Step 1: Write doc_id.rs**

Replace with:
```rust
use serde::{Deserialize, Serialize};

/// Internal document identifier scoped within a box.
///
/// Composed of `(session_internal_id, seq)` so the planner can filter the
/// candidate set to a specific session via cheap field comparison, and
/// so the caller can resolve a hit back to an `Event` through
/// `agentdb-core::primitives::scan_session_events` or a prefix lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DocId {
    pub session_internal_id: u64,
    pub seq: u64,
}

impl DocId {
    pub fn new(session_internal_id: u64, seq: u64) -> Self {
        Self {
            session_internal_id,
            seq,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering_is_lexicographic_over_session_then_seq() {
        let a = DocId::new(10, 1);
        let b = DocId::new(10, 2);
        let c = DocId::new(11, 0);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn roundtrips_through_serde_json() {
        let d = DocId::new(42, 7);
        let s = serde_json::to_string(&d).unwrap();
        let back: DocId = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p agentdb-index doc_id::tests`
Expected: 2 passed.

- [ ] **Step 3: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-index/src/doc_id.rs
git commit -m "feat(index): DocId (session_internal_id, seq)"
```

---

## Task 3: `TextIndex` trait + `InMemoryInverted` (BM25)

**Files:**
- Modify: `crates/agentdb-index/src/text.rs`

- [ ] **Step 1: Write text.rs**

Replace with:
```rust
use std::collections::HashMap;

use crate::doc_id::DocId;

pub type Score = f32;

/// Trait for full-text indexes used by the hybrid planner.
///
/// Implementations are per-box; the `path` parameter threads through so one
/// physical index can serve multiple indexed fields.
pub trait TextIndex: Send + Sync {
    /// Insert a document's text content under the given path.
    fn insert(&mut self, path: &str, doc_id: DocId, text: &str);

    /// Query for the top-`limit` documents matching `query` under `path`.
    /// Returns doc ids + BM25 scores, descending by score.
    fn search(&self, path: &str, query: &str, limit: usize) -> Vec<(DocId, Score)>;
}

/// Simple in-memory BM25 inverted index (per path).
///
/// Uses tf-idf / BM25 scoring with k1=1.2, b=0.75. Tokenization is
/// whitespace + ASCII-lowercase + drop-if-len-lt-2.
pub struct InMemoryInverted {
    per_path: HashMap<String, PathState>,
}

struct PathState {
    /// token -> list of (doc_id, term_freq_in_doc)
    postings: HashMap<String, Vec<(DocId, u32)>>,
    /// doc_id -> document length in tokens
    doc_len: HashMap<DocId, u32>,
    total_tokens: u64,
}

impl Default for InMemoryInverted {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryInverted {
    pub fn new() -> Self {
        Self {
            per_path: HashMap::new(),
        }
    }
}

fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|t| t.len() >= 2)
        .map(|t| t.to_ascii_lowercase())
        .collect()
}

impl TextIndex for InMemoryInverted {
    fn insert(&mut self, path: &str, doc_id: DocId, text: &str) {
        let tokens = tokenize(text);
        if tokens.is_empty() {
            return;
        }
        let state = self
            .per_path
            .entry(path.to_string())
            .or_insert_with(|| PathState {
                postings: HashMap::new(),
                doc_len: HashMap::new(),
                total_tokens: 0,
            });
        state.doc_len.insert(doc_id, tokens.len() as u32);
        state.total_tokens = state.total_tokens.saturating_add(tokens.len() as u64);

        let mut local: HashMap<String, u32> = HashMap::new();
        for t in tokens {
            *local.entry(t).or_insert(0) += 1;
        }
        for (tok, tf) in local {
            state.postings.entry(tok).or_default().push((doc_id, tf));
        }
    }

    fn search(&self, path: &str, query: &str, limit: usize) -> Vec<(DocId, Score)> {
        let Some(state) = self.per_path.get(path) else {
            return Vec::new();
        };
        let q_tokens = tokenize(query);
        if q_tokens.is_empty() {
            return Vec::new();
        }

        let n = state.doc_len.len() as f32;
        if n == 0.0 {
            return Vec::new();
        }
        let avg_dl = (state.total_tokens as f32) / n;
        let k1 = 1.2_f32;
        let b = 0.75_f32;

        let mut scores: HashMap<DocId, f32> = HashMap::new();
        for q in q_tokens {
            let Some(postings) = state.postings.get(&q) else {
                continue;
            };
            let df = postings.len() as f32;
            let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
            for (doc_id, tf) in postings {
                let tf = *tf as f32;
                let dl = *state.doc_len.get(doc_id).unwrap_or(&0) as f32;
                let denom = tf + k1 * (1.0 - b + b * dl / avg_dl.max(1.0));
                let score = idf * (tf * (k1 + 1.0)) / denom.max(1e-9);
                *scores.entry(*doc_id).or_insert(0.0) += score;
            }
        }

        let mut ranked: Vec<(DocId, Score)> = scores.into_iter().collect();
        ranked.sort_by(|(_, a), (_, b)| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
        ranked.truncate(limit);
        ranked
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(session: u64, seq: u64) -> DocId {
        DocId::new(session, seq)
    }

    #[test]
    fn tokenize_splits_on_non_alnum_and_lowercases() {
        let t = tokenize("Hello, World! It's 2026.");
        assert_eq!(t, vec!["hello", "world", "it", "2026"]);
    }

    #[test]
    fn tokenize_drops_short_tokens() {
        let t = tokenize("I am a developer");
        assert_eq!(t, vec!["am", "developer"]);
    }

    #[test]
    fn empty_index_returns_no_hits() {
        let idx = InMemoryInverted::new();
        let hits = idx.search("text", "anything", 10);
        assert!(hits.is_empty());
    }

    #[test]
    fn single_doc_matches_its_own_token() {
        let mut idx = InMemoryInverted::new();
        idx.insert("payload.text", d(1, 1), "refund policy for shipping");
        let hits = idx.search("payload.text", "refund", 10);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, d(1, 1));
        assert!(hits[0].1 > 0.0);
    }

    #[test]
    fn more_matches_score_higher() {
        let mut idx = InMemoryInverted::new();
        idx.insert("p", d(1, 1), "hello world");
        idx.insert("p", d(1, 2), "hello hello world world");
        idx.insert("p", d(1, 3), "unrelated text here");
        let hits = idx.search("p", "hello world", 10);
        assert_eq!(hits[0].0, d(1, 2));
        assert!(hits[0].1 > hits[1].1);
    }

    #[test]
    fn different_paths_are_isolated() {
        let mut idx = InMemoryInverted::new();
        idx.insert("a", d(1, 1), "orange");
        idx.insert("b", d(1, 2), "orange");
        let hits_a = idx.search("a", "orange", 10);
        let hits_b = idx.search("b", "orange", 10);
        assert_eq!(hits_a.len(), 1);
        assert_eq!(hits_b.len(), 1);
        assert_ne!(hits_a[0].0, hits_b[0].0);
    }

    #[test]
    fn limit_truncates_results() {
        let mut idx = InMemoryInverted::new();
        for i in 0..10 {
            idx.insert("p", d(1, i), "banana");
        }
        let hits = idx.search("p", "banana", 3);
        assert_eq!(hits.len(), 3);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p agentdb-index text::tests`
Expected: 7 passed.

- [ ] **Step 3: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-index/src/text.rs
git commit -m "feat(index): InMemoryInverted BM25 text index with per-path isolation"
```

---

## Task 4: `VectorIndex` trait + `InMemoryFlatVec` (cosine)

**Files:**
- Modify: `crates/agentdb-index/src/vector.rs`

- [ ] **Step 1: Write vector.rs**

Replace with:
```rust
use std::collections::HashMap;

use crate::doc_id::DocId;
use crate::error::{IndexError, IndexResult};

pub type Score = f32;

/// Trait for vector indexes used by the hybrid planner.
pub trait VectorIndex: Send + Sync {
    /// Insert a document's vector under the given path.
    ///
    /// Returns an error if the vector dimensionality disagrees with the
    /// path's existing vectors.
    fn insert(&mut self, path: &str, doc_id: DocId, vector: &[f32]) -> IndexResult<()>;

    /// Return the top-`limit` most similar docs under `path` (cosine),
    /// descending by score. Empty if the path is unknown or the query
    /// dimensionality doesn't match.
    fn knn(&self, path: &str, query: &[f32], limit: usize) -> Vec<(DocId, Score)>;
}

/// Brute-force cosine top-K. At prototype scale (tens of thousands of
/// vectors per path, per box) this is microseconds to milliseconds.
pub struct InMemoryFlatVec {
    per_path: HashMap<String, PathState>,
}

struct PathState {
    dim: usize,
    /// doc_id -> unit-normalised vector
    vectors: Vec<(DocId, Vec<f32>)>,
}

impl Default for InMemoryFlatVec {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryFlatVec {
    pub fn new() -> Self {
        Self {
            per_path: HashMap::new(),
        }
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

impl VectorIndex for InMemoryFlatVec {
    fn insert(&mut self, path: &str, doc_id: DocId, vector: &[f32]) -> IndexResult<()> {
        let entry = self.per_path.entry(path.to_string());
        let state = entry.or_insert_with(|| PathState {
            dim: vector.len(),
            vectors: Vec::new(),
        });
        if state.dim != vector.len() {
            return Err(IndexError::Invalid(format!(
                "vector dim mismatch at path {path}: expected {}, got {}",
                state.dim,
                vector.len()
            )));
        }
        state.vectors.push((doc_id, normalise(vector)));
        Ok(())
    }

    fn knn(&self, path: &str, query: &[f32], limit: usize) -> Vec<(DocId, Score)> {
        let Some(state) = self.per_path.get(path) else {
            return Vec::new();
        };
        if state.dim != query.len() || limit == 0 {
            return Vec::new();
        }
        let q = normalise(query);
        let mut ranked: Vec<(DocId, Score)> =
            state.vectors.iter().map(|(id, v)| (*id, dot(&q, v))).collect();
        ranked.sort_by(|(_, a), (_, b)| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
        ranked.truncate(limit);
        ranked
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
        let mut idx = InMemoryFlatVec::new();
        idx.insert("p", d(1, 1), &[1.0, 0.0]).unwrap();
        let err = idx.insert("p", d(1, 2), &[1.0, 0.0, 0.0]).unwrap_err();
        assert!(err.to_string().contains("dim mismatch"));
    }

    #[test]
    fn knn_returns_identical_vector_first() {
        let mut idx = InMemoryFlatVec::new();
        idx.insert("p", d(1, 1), &[1.0, 0.0, 0.0]).unwrap();
        idx.insert("p", d(1, 2), &[0.0, 1.0, 0.0]).unwrap();
        idx.insert("p", d(1, 3), &[0.0, 0.0, 1.0]).unwrap();
        let hits = idx.knn("p", &[1.0, 0.0, 0.0], 3);
        assert_eq!(hits[0].0, d(1, 1));
        assert!(hits[0].1 > 0.99); // perfectly aligned -> ~1.0
    }

    #[test]
    fn knn_limit_is_respected() {
        let mut idx = InMemoryFlatVec::new();
        for i in 0..10 {
            idx.insert("p", d(1, i), &[i as f32, 1.0]).unwrap();
        }
        let hits = idx.knn("p", &[1.0, 1.0], 3);
        assert_eq!(hits.len(), 3);
    }

    #[test]
    fn unknown_path_returns_empty() {
        let idx = InMemoryFlatVec::new();
        let hits = idx.knn("none", &[0.0], 10);
        assert!(hits.is_empty());
    }

    #[test]
    fn paths_are_isolated() {
        let mut idx = InMemoryFlatVec::new();
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
        let mut idx = InMemoryFlatVec::new();
        idx.insert("p", d(1, 1), &[1.0, 0.0]).unwrap();
        let hits = idx.knn("p", &[1.0], 10);
        assert!(hits.is_empty());
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p agentdb-index vector::tests`
Expected: 6 passed.

- [ ] **Step 3: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-index/src/vector.rs
git commit -m "feat(index): InMemoryFlatVec brute-force cosine top-K"
```

---

## Task 5: `PerBoxIndexes` struct + build from events

**Files:**
- Modify: `crates/agentdb-core/Cargo.toml`
- Create: `crates/agentdb-core/src/indexes.rs`
- Modify: `crates/agentdb-core/src/lib.rs`

**Context:** `PerBoxIndexes` composes one `InMemoryInverted` and one `InMemoryFlatVec`. It exposes a `build(kv, tenant_registry, session_registry, box_id)` method that:
1. Enumerates all tenants in the box
2. For each tenant, enumerates sessions
3. For each session, scans events via `scan_session_events`
4. For each event, walks `payload` JSON; string leaves go into text index; numeric-array leaves (length 2..=2048) go into vector index

Path keys use dotted notation rooted at `payload.`: e.g., `payload.text`, `payload.embedding`, `payload.message.body`.

- [ ] **Step 1: Update agentdb-core Cargo.toml**

Edit `crates/agentdb-core/Cargo.toml`. In `[dependencies]` add:
```toml
agentdb-index = { path = "../agentdb-index" }
```

- [ ] **Step 2: Write indexes.rs**

Save to `crates/agentdb-core/src/indexes.rs`:
```rust
use std::sync::Arc;

use agentdb_index::text::{InMemoryInverted, TextIndex};
use agentdb_index::vector::{InMemoryFlatVec, VectorIndex};
use agentdb_index::DocId;
use agentdb_storage::{BoxId, KvStore};

use crate::error::Result;
use crate::primitives::scan_session_events;
use crate::registry::{SessionRegistry, TenantRegistry};
use crate::types::Event;

/// Per-box index state: one text index + one vector index, both in-memory.
pub struct PerBoxIndexes {
    pub text: InMemoryInverted,
    pub vector: InMemoryFlatVec,
    /// Number of events indexed (for diagnostics).
    pub indexed: u64,
}

impl Default for PerBoxIndexes {
    fn default() -> Self {
        Self::new()
    }
}

impl PerBoxIndexes {
    pub fn new() -> Self {
        Self {
            text: InMemoryInverted::new(),
            vector: InMemoryFlatVec::new(),
            indexed: 0,
        }
    }

    /// Scan every session in `box_id`, pull all events, index them.
    pub fn build(
        &mut self,
        kv: &Arc<dyn KvStore>,
        tenants: &TenantRegistry,
        sessions: &SessionRegistry,
        box_id: BoxId,
    ) -> Result<()> {
        let tenant_externals = tenants.list_in_box(box_id)?;
        for tenant_ext in tenant_externals {
            let Some(tenant) = tenants.lookup(box_id, &tenant_ext)? else {
                continue;
            };
            let session_externals = sessions.list_in_tenant(box_id, tenant.internal_id)?;
            for session_ext in session_externals {
                let Some(session) = sessions.lookup(box_id, tenant.internal_id, &session_ext)?
                else {
                    continue;
                };
                for event in scan_session_events(kv, &session)? {
                    self.index_event(session.internal_id, &event);
                }
            }
        }
        Ok(())
    }

    /// Index one event under the indexes' by-path structure. The top-level
    /// prefix is `"payload"` so callers use natural paths like
    /// `"payload.text"` and `"payload.embedding"`.
    fn index_event(&mut self, session_internal_id: u64, event: &Event) {
        let doc_id = DocId::new(session_internal_id, event.seq);
        self.walk(doc_id, "payload", &event.payload);
        self.indexed += 1;
    }

    fn walk(&mut self, doc_id: DocId, path: &str, value: &serde_json::Value) {
        match value {
            serde_json::Value::String(s) => self.text.insert(path, doc_id, s),
            serde_json::Value::Array(a) => {
                if let Some(v) = as_float_vec(a) {
                    if (2..=2048).contains(&v.len()) {
                        let _ = self.vector.insert(path, doc_id, &v);
                    }
                }
            }
            serde_json::Value::Object(map) => {
                for (k, v) in map {
                    let child = format!("{path}.{k}");
                    self.walk(doc_id, &child, v);
                }
            }
            _ => {}
        }
    }
}

fn as_float_vec(a: &[serde_json::Value]) -> Option<Vec<f32>> {
    let mut out = Vec::with_capacity(a.len());
    for v in a {
        let n = v.as_f64()?;
        out.push(n as f32);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_storage::RocksKvStore;
    use serde_json::json;
    use tempfile::TempDir;

    async fn setup_with_events() -> (
        TempDir,
        Arc<dyn KvStore>,
        Arc<crate::BoxRegistry>,
        TenantRegistry,
        SessionRegistry,
    ) {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let registry = Arc::new(crate::BoxRegistry::new(kv.clone(), 4).unwrap());
        let tenants = TenantRegistry::new(kv.clone());
        let sessions = SessionRegistry::new(kv.clone());
        (dir, kv, registry, tenants, sessions)
    }

    #[tokio::test]
    async fn build_indexes_text_from_payload_text_field() {
        let (_dir, kv, registry, tenants, sessions) = setup_with_events().await;
        crate::primitives::append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"text": "refund policy for shipping"}),
            0,
            0,
        )
        .await
        .unwrap();
        let box_id = registry.route_tenant("t-1").id;
        let mut idx = PerBoxIndexes::new();
        idx.build(&kv, &tenants, &sessions, box_id).unwrap();
        let hits = idx.text.search("payload.text", "refund", 10);
        assert_eq!(hits.len(), 1);
    }

    #[tokio::test]
    async fn build_indexes_vector_from_payload_embedding_array() {
        let (_dir, kv, registry, tenants, sessions) = setup_with_events().await;
        crate::primitives::append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"embedding": [1.0, 0.0, 0.0, 0.0]}),
            0,
            0,
        )
        .await
        .unwrap();
        let box_id = registry.route_tenant("t-1").id;
        let mut idx = PerBoxIndexes::new();
        idx.build(&kv, &tenants, &sessions, box_id).unwrap();
        let hits = idx.vector.knn("payload.embedding", &[1.0, 0.0, 0.0, 0.0], 10);
        assert_eq!(hits.len(), 1);
        assert!(hits[0].1 > 0.99);
    }

    #[tokio::test]
    async fn build_indexes_counts_across_sessions_and_tenants() {
        let (_dir, kv, registry, tenants, sessions) = setup_with_events().await;
        for i in 0..3 {
            crate::primitives::append_and_fetch(
                &registry,
                &tenants,
                &sessions,
                "t-1",
                "s-1",
                json!({"text": format!("hello {i}")}),
                0,
                i,
            )
            .await
            .unwrap();
        }
        let box_id = registry.route_tenant("t-1").id;
        let mut idx = PerBoxIndexes::new();
        idx.build(&kv, &tenants, &sessions, box_id).unwrap();
        assert_eq!(idx.indexed, 3);
    }
}
```

- [ ] **Step 3: Update lib.rs**

Edit `crates/agentdb-core/src/lib.rs`. Add `pub mod indexes;` after the existing `pub mod primitives;`. Add a re-export:
```rust
pub use indexes::PerBoxIndexes;
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p agentdb-core indexes::tests`
Expected: 3 passed.

Run: `cargo check --workspace` — expect clean.

- [ ] **Step 5: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core/Cargo.toml crates/agentdb-core/src/indexes.rs crates/agentdb-core/src/lib.rs
git commit -m "feat(core): PerBoxIndexes builds text+vector indexes from event payloads"
```

---

## Task 6: Wire `PerBoxIndexes` into `Box_` with lazy build

**Files:**
- Modify: `crates/agentdb-core/src/box_runtime.rs`

**Context:** Each `Box_` gets a `indexes: Arc<RwLock<Option<PerBoxIndexes>>>`. A new method `Box_::indexes_built(&self, kv, tenants, sessions) -> RwLockReadGuard<PerBoxIndexes>` builds the index on first call (under the box write lock to avoid concurrent rebuilds) and returns a read guard thereafter.

- [ ] **Step 1: Write failing test**

Append inside the `#[cfg(test)] mod tests` block in `crates/agentdb-core/src/box_runtime.rs`:
```rust
    #[tokio::test]
    async fn indexes_built_is_idempotent_and_reflects_events() {
        use crate::primitives::append_and_fetch;
        use crate::registry::{SessionRegistry, TenantRegistry};
        use serde_json::json;

        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let reg = Arc::new(BoxRegistry::new(kv.clone(), 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        append_and_fetch(
            &reg,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"text": "hybrid retrieval"}),
            0,
            0,
        )
        .await
        .unwrap();
        let box_ = reg.route_tenant("t-1");
        let built1 = box_.indexes_built(reg.kv(), &tenants, &sessions).await.unwrap();
        assert_eq!(built1.as_ref().expect("built").indexed, 1);
        drop(built1);
        let built2 = box_.indexes_built(reg.kv(), &tenants, &sessions).await.unwrap();
        assert_eq!(built2.as_ref().expect("built").indexed, 1);
    }
```

- [ ] **Step 2: Run (expect compile failure — `indexes_built` not defined)**

Run: `cargo test -p agentdb-core box_runtime::tests::indexes_built`
Expected: FAIL.

- [ ] **Step 3: Add the field and method**

In `crates/agentdb-core/src/box_runtime.rs`, replace the `Box_` struct and its `new` with:
```rust
pub struct Box_ {
    pub id: BoxId,
    pub write_lock: Mutex<()>,
    indexes: tokio::sync::RwLock<Option<crate::indexes::PerBoxIndexes>>,
}

impl Box_ {
    fn new(id: BoxId) -> Self {
        Self {
            id,
            write_lock: Mutex::new(()),
            indexes: tokio::sync::RwLock::new(None),
        }
    }

    /// Build (on first call) and return a read guard over the box's indexes.
    /// Rebuilds are skipped — subsequent calls reuse the existing instance.
    pub async fn indexes_built<'a>(
        &'a self,
        kv: &Arc<dyn KvStore>,
        tenants: &crate::registry::TenantRegistry,
        sessions: &crate::registry::SessionRegistry,
    ) -> Result<tokio::sync::RwLockReadGuard<'a, Option<crate::indexes::PerBoxIndexes>>> {
        // Check fast path under a read lock.
        {
            let r = self.indexes.read().await;
            if r.is_some() {
                return Ok(r);
            }
        }
        // Build under an exclusive lock. Double-check inside in case of races.
        {
            let mut w = self.indexes.write().await;
            if w.is_none() {
                let mut built = crate::indexes::PerBoxIndexes::new();
                built.build(kv, tenants, sessions, self.id)?;
                *w = Some(built);
            }
        }
        Ok(self.indexes.read().await)
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p agentdb-core box_runtime::tests -- --nocapture`
Expected: 5 passed (4 previous + 1 new).

Run: `cargo test --workspace` — expect all tests still pass.

- [ ] **Step 5: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core/src/box_runtime.rs
git commit -m "feat(core): Box_ lazy-builds PerBoxIndexes on first query"
```

---

## Task 7: Hybrid planner

**Files:**
- Create: `crates/agentdb-core/src/planner.rs`
- Modify: `crates/agentdb-core/src/lib.rs`

**Context:** The planner takes:
- A `Query` describing the source stage: either text (path, query string, limit) or vector (path, query vector, limit, num_candidates)
- A `Filter` describing post-index constraints (reuses the `agentdb-wire` filter? No — to avoid a core→wire dep, we accept a simple predicate closure or re-define a minimal filter. For M4 we accept a `serde_json::Value` document filter with the same shape and reimplement a tiny matcher here.)
- The `PerBoxIndexes` + KvStore + registries

It returns:
- A `Vec<(DocId, f32, Event)>` — scored + materialized results

**Strategy selection:** if the filter contains a top-level `session_id == <string>` equality, use the **locality-bound** strategy: skip the index entirely, scan that session's events via `scan_session_events`, score in-memory, apply the rest of the filter, truncate to limit.

Otherwise use **over-fetch post-filter**: query the index for `limit * over_fetch` (default 3) candidates; materialize each; apply filter; truncate to limit.

- [ ] **Step 1: Write planner.rs**

Save to `crates/agentdb-core/src/planner.rs`:
```rust
use std::sync::Arc;

use agentdb_index::{DocId, PerBoxIndexes};
use agentdb_storage::{encode_key, BoxId, KvStore, Namespace};

use crate::error::Result;
use crate::primitives::{scan_session_events, DEFAULT_BRANCH_ID};
use crate::registry::SessionRegistry;
use crate::types::Event;

#[derive(Debug, Clone)]
pub enum Source {
    Text {
        path: String,
        query: String,
        limit: usize,
    },
    Vector {
        path: String,
        query: Vec<f32>,
        limit: usize,
        num_candidates: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    LocalityBound,
    OverFetchPostFilter,
}

#[derive(Debug, Clone)]
pub struct Hit {
    pub doc_id: DocId,
    pub score: f32,
    pub event: Event,
    pub session_external_id: String,
}

pub struct PlanOutput {
    pub strategy: Strategy,
    pub hits: Vec<Hit>,
}

const DEFAULT_OVER_FETCH: usize = 3;

/// Pure predicate: does a `(session_external, event)` pair match the filter?
pub type FilterFn = Box<dyn Fn(&str, &Event) -> bool + Send + Sync>;

pub struct HybridPlanner<'a> {
    pub kv: &'a Arc<dyn KvStore>,
    pub indexes: &'a PerBoxIndexes,
    pub sessions: &'a SessionRegistry,
    pub box_id: BoxId,
    pub tenant_internal_id: u64,
}

impl<'a> HybridPlanner<'a> {
    /// Hint: if the filter pins the session by external id, use locality-bound.
    pub fn plan(
        &self,
        source: &Source,
        filter: FilterFn,
        session_id_hint: Option<&str>,
    ) -> Result<PlanOutput> {
        if let Some(session_ext) = session_id_hint {
            self.plan_locality_bound(source, filter, session_ext)
        } else {
            self.plan_over_fetch(source, filter)
        }
    }

    fn plan_locality_bound(
        &self,
        source: &Source,
        filter: FilterFn,
        session_ext: &str,
    ) -> Result<PlanOutput> {
        let session = match self
            .sessions
            .lookup(self.box_id, self.tenant_internal_id, session_ext)?
        {
            Some(s) => s,
            None => {
                return Ok(PlanOutput {
                    strategy: Strategy::LocalityBound,
                    hits: Vec::new(),
                });
            }
        };

        let all = scan_session_events(self.kv, &session)?;
        let mut scored: Vec<Hit> = Vec::new();
        for event in all {
            if !filter(&session.external_id, &event) {
                continue;
            }
            let score = score_single(source, &event);
            if score > f32::NEG_INFINITY {
                scored.push(Hit {
                    doc_id: DocId::new(session.internal_id, event.seq),
                    score,
                    event,
                    session_external_id: session.external_id.clone(),
                });
            }
        }
        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        scored.truncate(match source {
            Source::Text { limit, .. } => *limit,
            Source::Vector { limit, .. } => *limit,
        });
        Ok(PlanOutput {
            strategy: Strategy::LocalityBound,
            hits: scored,
        })
    }

    fn plan_over_fetch(&self, source: &Source, filter: FilterFn) -> Result<PlanOutput> {
        let candidates: Vec<(DocId, f32)> = match source {
            Source::Text {
                path,
                query,
                limit,
            } => {
                use agentdb_index::text::TextIndex;
                self.indexes
                    .text
                    .search(path, query, limit * DEFAULT_OVER_FETCH)
            }
            Source::Vector {
                path,
                query,
                limit,
                num_candidates,
            } => {
                use agentdb_index::vector::VectorIndex;
                self.indexes
                    .vector
                    .knn(path, query, (*num_candidates).max(*limit))
            }
        };

        let limit = match source {
            Source::Text { limit, .. } | Source::Vector { limit, .. } => *limit,
        };
        let mut hits: Vec<Hit> = Vec::new();
        for (doc_id, score) in candidates {
            let Some(event) = self.resolve(doc_id)? else {
                continue;
            };
            let Some(session) = self.sessions.lookup_by_internal(
                self.box_id,
                self.tenant_internal_id,
                doc_id.session_internal_id,
            )?
            else {
                continue;
            };
            if !filter(&session.external_id, &event) {
                continue;
            }
            hits.push(Hit {
                doc_id,
                score,
                event,
                session_external_id: session.external_id,
            });
            if hits.len() >= limit {
                break;
            }
        }
        Ok(PlanOutput {
            strategy: Strategy::OverFetchPostFilter,
            hits,
        })
    }

    fn resolve(&self, doc_id: DocId) -> Result<Option<Event>> {
        // (box, tenant, session, branch=0, seq)
        let key = encode_key(
            Namespace::Events,
            self.box_id,
            self.tenant_internal_id,
            doc_id.session_internal_id,
            DEFAULT_BRANCH_ID,
            doc_id.seq,
        );
        let Some(bytes) = self.kv.get(&key)? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&bytes)?))
    }
}

fn score_single(source: &Source, event: &Event) -> f32 {
    // For locality-bound, we don't go through the index — score inline.
    // Text: very simple bag-of-words overlap at the specified path.
    // Vector: cosine between the event's vector at path and the query.
    match source {
        Source::Text { path, query, .. } => {
            let Some(text) = extract_string(&event.payload, path) else {
                return f32::NEG_INFINITY;
            };
            let q = query.to_ascii_lowercase();
            let t = text.to_ascii_lowercase();
            // cheap overlap: count query terms present.
            let mut s = 0.0;
            for term in q
                .split(|c: char| !c.is_ascii_alphanumeric())
                .filter(|s| s.len() >= 2)
            {
                if t.contains(term) {
                    s += 1.0;
                }
            }
            s
        }
        Source::Vector { path, query, .. } => {
            let Some(v) = extract_vec(&event.payload, path) else {
                return f32::NEG_INFINITY;
            };
            cosine(&v, query)
        }
    }
}

fn extract_string<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a str> {
    let mut cur = value;
    // path is prefixed by "payload." conventionally; tolerate both forms.
    let rest = path.strip_prefix("payload.").unwrap_or(path);
    for segment in rest.split('.') {
        cur = cur.get(segment)?;
    }
    cur.as_str()
}

fn extract_vec(value: &serde_json::Value, path: &str) -> Option<Vec<f32>> {
    let mut cur = value;
    let rest = path.strip_prefix("payload.").unwrap_or(path);
    for segment in rest.split('.') {
        cur = cur.get(segment)?;
    }
    let arr = cur.as_array()?;
    let mut out = Vec::with_capacity(arr.len());
    for v in arr {
        out.push(v.as_f64()? as f32);
    }
    Some(out)
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return f32::NEG_INFINITY;
    }
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if na < 1e-12 || nb < 1e-12 {
        return 0.0;
    }
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    dot / (na * nb)
}
```

This references `SessionRegistry::lookup_by_internal` which does not yet exist — add it in the next step.

- [ ] **Step 2: Add `SessionRegistry::lookup_by_internal`**

In `crates/agentdb-core/src/registry.rs`, inside `impl SessionRegistry`, add:
```rust
    /// Lookup a session by its internal id (not its external string). Used by
    /// the planner when resolving DocIds that came back from an index.
    pub fn lookup_by_internal(
        &self,
        box_id: BoxId,
        tenant_id: TenantId,
        internal_id: SessionId,
    ) -> Result<Option<Session>> {
        let key = crate::meta::key_session_record(box_id, tenant_id, internal_id);
        let Some(bytes) = self.kv.get(&key)? else {
            return Ok(None);
        };
        let s: Session = serde_json::from_slice(&bytes)?;
        Ok(Some(s))
    }
```

- [ ] **Step 3: Update lib.rs**

Edit `crates/agentdb-core/src/lib.rs`. Add `pub mod planner;` and re-export the planner's public types:
```rust
pub use planner::{HybridPlanner, Hit, PlanOutput, Source, Strategy};
```

- [ ] **Step 4: Add planner unit tests**

Append to `crates/agentdb-core/src/planner.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::append_and_fetch;
    use crate::registry::{SessionRegistry, TenantRegistry};
    use crate::BoxRegistry;
    use agentdb_storage::RocksKvStore;
    use serde_json::json;
    use tempfile::TempDir;

    async fn setup() -> (
        TempDir,
        Arc<dyn KvStore>,
        Arc<BoxRegistry>,
        Arc<TenantRegistry>,
        Arc<SessionRegistry>,
    ) {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let reg = Arc::new(BoxRegistry::new(kv.clone(), 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv.clone()));
        (dir, kv, reg, tenants, sessions)
    }

    #[tokio::test]
    async fn locality_bound_strategy_is_picked_when_session_hint_present() {
        let (_d, kv, reg, tenants, sessions) = setup().await;
        append_and_fetch(
            &reg,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"text": "refund policy"}),
            0,
            0,
        )
        .await
        .unwrap();

        let tenant = tenants.get_or_mint(reg.route_tenant("t-1").id, "t-1", 0).unwrap();
        let box_id = tenant.box_id;

        // Indexes empty (not built) but locality-bound shouldn't need them.
        let indexes = PerBoxIndexes::new();
        let p = HybridPlanner {
            kv: &kv,
            indexes: &indexes,
            sessions: &sessions,
            box_id,
            tenant_internal_id: tenant.internal_id,
        };
        let out = p
            .plan(
                &Source::Text {
                    path: "payload.text".into(),
                    query: "refund".into(),
                    limit: 10,
                },
                Box::new(|_, _| true),
                Some("s-1"),
            )
            .unwrap();
        assert_eq!(out.strategy, Strategy::LocalityBound);
        assert_eq!(out.hits.len(), 1);
    }

    #[tokio::test]
    async fn over_fetch_strategy_uses_text_index() {
        let (_d, kv, reg, tenants, sessions) = setup().await;
        for i in 0..3 {
            append_and_fetch(
                &reg,
                &tenants,
                &sessions,
                "t-1",
                "s-1",
                json!({"text": format!("refund case {i}")}),
                0,
                i,
            )
            .await
            .unwrap();
        }
        let tenant = tenants.get_or_mint(reg.route_tenant("t-1").id, "t-1", 0).unwrap();
        let mut indexes = PerBoxIndexes::new();
        indexes.build(&kv, &tenants, &sessions, tenant.box_id).unwrap();
        let p = HybridPlanner {
            kv: &kv,
            indexes: &indexes,
            sessions: &sessions,
            box_id: tenant.box_id,
            tenant_internal_id: tenant.internal_id,
        };
        let out = p
            .plan(
                &Source::Text {
                    path: "payload.text".into(),
                    query: "refund".into(),
                    limit: 5,
                },
                Box::new(|_, _| true),
                None,
            )
            .unwrap();
        assert_eq!(out.strategy, Strategy::OverFetchPostFilter);
        assert_eq!(out.hits.len(), 3);
    }

    #[tokio::test]
    async fn over_fetch_strategy_uses_vector_index() {
        let (_d, kv, reg, tenants, sessions) = setup().await;
        append_and_fetch(
            &reg,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"embedding": [1.0, 0.0, 0.0]}),
            0,
            0,
        )
        .await
        .unwrap();
        let tenant = tenants.get_or_mint(reg.route_tenant("t-1").id, "t-1", 0).unwrap();
        let mut indexes = PerBoxIndexes::new();
        indexes.build(&kv, &tenants, &sessions, tenant.box_id).unwrap();
        let p = HybridPlanner {
            kv: &kv,
            indexes: &indexes,
            sessions: &sessions,
            box_id: tenant.box_id,
            tenant_internal_id: tenant.internal_id,
        };
        let out = p
            .plan(
                &Source::Vector {
                    path: "payload.embedding".into(),
                    query: vec![1.0, 0.0, 0.0],
                    limit: 5,
                    num_candidates: 10,
                },
                Box::new(|_, _| true),
                None,
            )
            .unwrap();
        assert_eq!(out.strategy, Strategy::OverFetchPostFilter);
        assert_eq!(out.hits.len(), 1);
        assert!(out.hits[0].score > 0.99);
    }
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p agentdb-core planner::tests`
Expected: 3 passed.

Run: `cargo test --workspace` — all tests pass.

- [ ] **Step 6: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core/src/planner.rs crates/agentdb-core/src/registry.rs crates/agentdb-core/src/lib.rs
git commit -m "feat(core): HybridPlanner with locality-bound and over-fetch strategies"
```

---

## Task 8: `aggregate` command handler in `agentdb-wire`

**Files:**
- Modify: `crates/agentdb-wire/Cargo.toml`
- Create: `crates/agentdb-wire/src/handlers/aggregate.rs`
- Modify: `crates/agentdb-wire/src/handlers/mod.rs`
- Modify: `crates/agentdb-wire/src/command.rs`

**Context:** `aggregate` takes a pipeline (array of stage docs). M4 supports:
- **Source stages (must be first)**: `$vectorSearch`, `$search`
- **Transforms (after source, in order)**: `$match`, `$limit`, `$project`

If the first stage isn't a supported source, return an error reply. If any downstream stage is unsupported, return an error reply with `{ok: 0}`.

Source stage contract:
```js
{ $vectorSearch: { queryVector: [...], path: "payload.embedding", numCandidates: 100, limit: 10, filter: {...} } }
{ $search: { text: { query: "refund", path: "payload.text" }, filter: {...} } }
```
The `filter` sub-field uses the M3 filter syntax (supported by `crate::handlers::filter::Filter`).

Output: a Mongo cursor reply.

- [ ] **Step 1: Add dep on agentdb-index**

Edit `crates/agentdb-wire/Cargo.toml`. In `[dependencies]` add:
```toml
agentdb-index = { path = "../agentdb-index" }
```

- [ ] **Step 2: Write aggregate.rs**

Save to `crates/agentdb-wire/src/handlers/aggregate.rs`:
```rust
use std::sync::Arc;

use agentdb_core::{BoxRegistry, Hit, HybridPlanner, SessionRegistry, Source, TenantRegistry};
use bson::{doc, Bson, Document};
use serde_json::Value;

use crate::bsonx::json_to_bson;
use crate::error::{WireError, WireResult};
use crate::handlers::filter::Filter;

pub async fn aggregate_response(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    db: &str,
    collection: &str,
    pipeline: &[Document],
) -> WireResult<Document> {
    if collection != "events" {
        return Err(WireError::Protocol(format!(
            "aggregate on {collection} not supported in M4; only 'events' indexes exist"
        )));
    }
    let Some(first) = pipeline.first() else {
        return Ok(empty_batch(db, collection));
    };

    // Parse source stage.
    let (source, source_filter) = parse_source(first)?;

    // Parse tail stages.
    let mut post_match: Option<Filter> = None;
    let mut post_limit: Option<i64> = None;
    let mut project: Option<Document> = None;
    for stage in pipeline.iter().skip(1) {
        if let Ok(inner) = stage.get_document("$match") {
            if post_match.is_some() {
                return Err(WireError::Protocol(
                    "multiple $match stages not supported".into(),
                ));
            }
            post_match = Some(Filter::parse(inner)?);
        } else if let Ok(n) = stage.get_i64("$limit") {
            post_limit = Some(n);
        } else if let Ok(n) = stage.get_i32("$limit") {
            post_limit = Some(n as i64);
        } else if let Ok(p) = stage.get_document("$project") {
            project = Some(p.clone());
        } else {
            return Err(WireError::Protocol(format!(
                "unsupported pipeline stage: {stage:?}"
            )));
        }
    }

    // Resolve tenant + box.
    let box_ = registry.route_tenant(db);
    let Some(tenant) = tenants.lookup(box_.id, db)? else {
        return Ok(empty_batch(db, collection));
    };

    // Build indexes lazily.
    let indexes_guard = box_.indexes_built(registry.kv(), tenants, sessions).await?;
    let indexes = indexes_guard.as_ref().expect("built by indexes_built");

    // Session hint — either from the source-stage `filter.session_id` or
    // the downstream `$match.session_id` if present.
    let hint_from_source = source_filter.top_level_eq("session_id").and_then(|v| match v {
        Value::String(s) => Some(s.clone()),
        _ => None,
    });
    let hint_from_post = post_match
        .as_ref()
        .and_then(|f| f.top_level_eq("session_id"))
        .and_then(|v| match v {
            Value::String(s) => Some(s.clone()),
            _ => None,
        });
    let session_hint = hint_from_source.or(hint_from_post);

    // Merge filters into a single predicate for the planner.
    let source_filter_clone = source_filter.clone();
    let post_match_clone = post_match.clone();
    let predicate = Box::new(move |session_external: &str, event: &agentdb_core::Event| {
        let row = serde_json::json!({
            "session_id": session_external,
            "seq": event.seq,
            "ts": event.ts,
            "payload": event.payload,
        });
        if !source_filter_clone.matches(&row) {
            return false;
        }
        if let Some(m) = &post_match_clone {
            if !m.matches(&row) {
                return false;
            }
        }
        true
    });

    let planner = HybridPlanner {
        kv: registry.kv(),
        indexes,
        sessions,
        box_id: tenant.box_id,
        tenant_internal_id: tenant.internal_id,
    };

    let out = planner.plan(&source, predicate, session_hint.as_deref())?;
    let mut hits = out.hits;

    if let Some(limit) = post_limit {
        let cap = if limit <= 0 { usize::MAX } else { limit as usize };
        hits.truncate(cap);
    }

    // Project (trivial: include only listed fields if `{ field: 1 }` map provided).
    let batch: Vec<Bson> = hits
        .into_iter()
        .map(|h| hit_to_bson(&h, project.as_ref()))
        .collect();

    Ok(doc! {
        "cursor": {
            "id": 0_i64,
            "ns": format!("{}.{}", db, collection),
            "firstBatch": Bson::Array(batch),
        },
        "ok": 1.0,
    })
}

fn empty_batch(db: &str, collection: &str) -> Document {
    doc! {
        "cursor": {
            "id": 0_i64,
            "ns": format!("{}.{}", db, collection),
            "firstBatch": Bson::Array(vec![]),
        },
        "ok": 1.0,
    }
}

fn parse_source(stage: &Document) -> WireResult<(Source, Filter)> {
    if let Ok(vs) = stage.get_document("$vectorSearch") {
        let query_arr = vs
            .get_array("queryVector")
            .map_err(|_| WireError::Protocol("$vectorSearch missing queryVector".into()))?;
        let mut query: Vec<f32> = Vec::with_capacity(query_arr.len());
        for v in query_arr {
            match v {
                Bson::Double(d) => query.push(*d as f32),
                Bson::Int32(i) => query.push(*i as f32),
                Bson::Int64(i) => query.push(*i as f32),
                _ => return Err(WireError::Protocol("queryVector entries must be numeric".into())),
            }
        }
        let path = vs
            .get_str("path")
            .map_err(|_| WireError::Protocol("$vectorSearch missing path".into()))?
            .to_string();
        let limit = vs.get_i64("limit").unwrap_or(10).max(1) as usize;
        let num_candidates = vs.get_i64("numCandidates").unwrap_or(100).max(1) as usize;
        let filter = match vs.get_document("filter") {
            Ok(f) => Filter::parse(f)?,
            Err(_) => Filter::parse(&Document::new())?,
        };
        return Ok((
            Source::Vector {
                path,
                query,
                limit,
                num_candidates,
            },
            filter,
        ));
    }

    if let Ok(s) = stage.get_document("$search") {
        let text = s
            .get_document("text")
            .map_err(|_| WireError::Protocol("$search missing text".into()))?;
        let query = text
            .get_str("query")
            .map_err(|_| WireError::Protocol("$search.text missing query".into()))?
            .to_string();
        let path = text
            .get_str("path")
            .map_err(|_| WireError::Protocol("$search.text missing path".into()))?
            .to_string();
        let limit = s.get_i64("limit").unwrap_or(10).max(1) as usize;
        let filter = match s.get_document("filter") {
            Ok(f) => Filter::parse(f)?,
            Err(_) => Filter::parse(&Document::new())?,
        };
        return Ok((Source::Text { path, query, limit }, filter));
    }

    Err(WireError::Protocol(
        "aggregate pipeline must start with $vectorSearch or $search in M4".into(),
    ))
}

fn hit_to_bson(h: &Hit, project: Option<&Document>) -> Bson {
    let mut row = serde_json::json!({
        "session_id": h.session_external_id,
        "seq": h.event.seq,
        "ts": h.event.ts,
        "payload": h.event.payload,
        "_score": h.score,
    });
    if let Some(p) = project {
        if let Some(obj) = row.as_object() {
            let mut kept = serde_json::Map::new();
            for (k, _v) in p.iter() {
                if let Some(v) = obj.get(k) {
                    kept.insert(k.clone(), v.clone());
                }
            }
            row = Value::Object(kept);
        }
    }
    json_to_bson(row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_storage::{KvStore, RocksKvStore};
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn setup() -> (
        TempDir,
        Arc<BoxRegistry>,
        Arc<TenantRegistry>,
        Arc<SessionRegistry>,
    ) {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let registry = Arc::new(BoxRegistry::new(kv.clone(), 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        (dir, registry, tenants, sessions)
    }

    async fn seed_text(registry: &BoxRegistry, tenants: &TenantRegistry, sessions: &SessionRegistry) {
        for i in 0..3 {
            agentdb_core::append_and_fetch(
                registry,
                tenants,
                sessions,
                "t-1",
                "s-1",
                json!({"text": format!("refund case number {i}")}),
                0,
                i,
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn aggregate_search_returns_hits() {
        let (_d, r, t, s) = setup().await;
        seed_text(&r, &t, &s).await;
        let reply = aggregate_response(
            &r,
            &t,
            &s,
            "t-1",
            "events",
            &[doc! {
                "$search": {
                    "text": { "query": "refund", "path": "payload.text" },
                    "limit": 10_i64,
                }
            }],
        )
        .await
        .unwrap();
        let batch = reply
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 3);
    }

    #[tokio::test]
    async fn aggregate_search_with_trailing_limit_truncates() {
        let (_d, r, t, s) = setup().await;
        seed_text(&r, &t, &s).await;
        let reply = aggregate_response(
            &r,
            &t,
            &s,
            "t-1",
            "events",
            &[
                doc! {
                    "$search": {
                        "text": { "query": "refund", "path": "payload.text" },
                        "limit": 10_i64,
                    }
                },
                doc! { "$limit": 2_i64 },
            ],
        )
        .await
        .unwrap();
        let batch = reply
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 2);
    }

    #[tokio::test]
    async fn aggregate_vector_search_returns_top_hit() {
        let (_d, r, t, s) = setup().await;
        agentdb_core::append_and_fetch(
            &r,
            &t,
            &s,
            "t-1",
            "s-1",
            json!({"embedding": [1.0, 0.0, 0.0]}),
            0,
            0,
        )
        .await
        .unwrap();
        let reply = aggregate_response(
            &r,
            &t,
            &s,
            "t-1",
            "events",
            &[doc! {
                "$vectorSearch": {
                    "queryVector": [1.0, 0.0, 0.0],
                    "path": "payload.embedding",
                    "limit": 5_i64,
                    "numCandidates": 10_i64,
                }
            }],
        )
        .await
        .unwrap();
        let batch = reply
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 1);
    }

    #[tokio::test]
    async fn aggregate_first_stage_must_be_source() {
        let (_d, r, t, s) = setup().await;
        seed_text(&r, &t, &s).await;
        let err = aggregate_response(
            &r,
            &t,
            &s,
            "t-1",
            "events",
            &[doc! { "$match": {} }],
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("$vectorSearch or $search"));
    }
}
```

- [ ] **Step 3: Declare the module**

Edit `crates/agentdb-wire/src/handlers/mod.rs`. Add `pub mod aggregate;` line.

- [ ] **Step 4: Dispatch**

Edit `crates/agentdb-wire/src/command.rs`. In the match inside `dispatch`, add an arm for `aggregate` after `insert`:
```rust
"aggregate" => {
    let collection = match body.get_str("aggregate") {
        Ok(s) => s.to_string(),
        Err(_) => {
            return error_reply(&crate::error::WireError::Protocol(
                "aggregate command missing collection name".into(),
            ));
        }
    };
    let pipeline_arr = body.get_array("pipeline").cloned().unwrap_or_default();
    let pipeline: Vec<Document> = pipeline_arr
        .into_iter()
        .filter_map(|b| match b {
            Bson::Document(d) => Some(d),
            _ => None,
        })
        .collect();
    match crate::handlers::aggregate::aggregate_response(
        &state.registry,
        &state.tenants,
        &state.sessions,
        head.db.unwrap_or(""),
        &collection,
        &pipeline,
    )
    .await
    {
        Ok(d) => d,
        Err(e) => error_reply(&e),
    }
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p agentdb-wire handlers::aggregate`
Expected: 4 passed.

Run: `cargo test --workspace` — all tests pass.

- [ ] **Step 6: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-wire crates/agentdb-core
git commit -m "feat(wire): aggregate command with \$search, \$vectorSearch, \$match, \$limit, \$project"
```

---

## Task 9: End-to-end integration tests via the `mongodb` driver

**Files:**
- Modify: `crates/agentdb-wire/tests/driver.rs`

**Context:** Add 2 tests that exercise `$search` and `$vectorSearch` through the real `mongodb` Rust driver.

- [ ] **Step 1: Append tests**

Append at the bottom of `crates/agentdb-wire/tests/driver.rs`:
```rust
#[tokio::test]
async fn text_search_aggregation_returns_hits() {
    let (_dir, port) = spawn().await;
    let cli = client(port).await;
    let db = cli.database("t-search");
    let events = db.collection::<bson::Document>("events");

    events
        .insert_one(doc! { "session_id": "s-1", "text": "refund policy" })
        .await
        .unwrap();
    events
        .insert_one(doc! { "session_id": "s-1", "text": "shipping update" })
        .await
        .unwrap();

    let pipeline = vec![doc! {
        "$search": {
            "text": { "query": "refund", "path": "payload.text" },
            "limit": 10_i64,
        }
    }];
    let cursor = events.aggregate(pipeline).await.unwrap();
    use futures_util::StreamExt;
    let docs: Vec<bson::Document> = cursor.filter_map(|r| async move { r.ok() }).collect().await;
    assert_eq!(docs.len(), 1);
    assert_eq!(
        docs[0]
            .get_document("payload")
            .unwrap()
            .get_str("text")
            .unwrap(),
        "refund policy"
    );
}

#[tokio::test]
async fn vector_search_aggregation_returns_top_hit() {
    let (_dir, port) = spawn().await;
    let cli = client(port).await;
    let db = cli.database("t-vector");
    let events = db.collection::<bson::Document>("events");

    events
        .insert_one(doc! { "session_id": "s-1", "embedding": [1.0, 0.0, 0.0] })
        .await
        .unwrap();
    events
        .insert_one(doc! { "session_id": "s-1", "embedding": [0.0, 1.0, 0.0] })
        .await
        .unwrap();

    let pipeline = vec![doc! {
        "$vectorSearch": {
            "queryVector": [1.0, 0.0, 0.0],
            "path": "payload.embedding",
            "limit": 5_i64,
            "numCandidates": 10_i64,
        }
    }];
    let cursor = events.aggregate(pipeline).await.unwrap();
    use futures_util::StreamExt;
    let docs: Vec<bson::Document> = cursor.filter_map(|r| async move { r.ok() }).collect().await;
    assert!(!docs.is_empty());
    let first_payload = docs[0].get_document("payload").unwrap();
    let emb = first_payload.get_array("embedding").unwrap();
    let as_vec: Vec<f64> = emb.iter().filter_map(|b| b.as_f64()).collect();
    assert_eq!(as_vec, vec![1.0, 0.0, 0.0]);
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p agentdb-wire --test driver`
Expected: 5 passed (3 previous + 2 new).

- [ ] **Step 3: Run the full workspace suite**

Run: `cargo test --workspace`
Expected counts (minimum):
- agentdb-storage: 17 + 11 = 28
- agentdb-core: 36 + 3 (indexes) + 3 (planner) + 1 (box_runtime) = 43
- agentdb-api: 2
- agentdb-wire: 37 + 4 (aggregate) = 41 unit, 5 driver integration
- agentdb-index: 1 (error) + 2 (doc_id) + 7 (text) + 6 (vector) = 16
- total: **135 tests**

Run: `cargo clippy --workspace --all-targets -- -D warnings` — clean.
Run: `cargo fmt --all --check` — clean.

- [ ] **Step 4: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-wire/tests/driver.rs
git commit -m "test(wire): mongodb driver end-to-end coverage for \$search and \$vectorSearch"
```

---

## Task 10: Milestone exit gate

**Files:** none beyond re-running checks.

- [ ] **Step 1: Full workspace test**

Run: `cargo test --workspace`
Expected: all tests pass.

- [ ] **Step 2: Clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings. Fix any in M4 code.

- [ ] **Step 3: Fmt**

Run: `cargo fmt --all --check`
Expected: clean.

## Milestone exit criteria

`cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` returns exit 0.

Manual demo (optional):
```bash
cargo run -p agentdb-server &
mongosh "mongodb://127.0.0.1:7879/?directConnection=true"
> use t-demo
> db.events.insertOne({session_id: "s-1", text: "refund policy for shipping", embedding: [1,0,0,0,0,0,0,0]})
> db.events.insertOne({session_id: "s-1", text: "billing error", embedding: [0,1,0,0,0,0,0,0]})
> db.events.aggregate([{$search: {text: {query: "refund", path: "payload.text"}}}])
> db.events.aggregate([{$vectorSearch: {queryVector: [1,0,0,0,0,0,0,0], path: "payload.embedding", limit: 5, numCandidates: 100}}])
```

## Known limitations to carry into M5

- **Indexes are not updated on append.** The lazy-build + read-only model means events inserted after the first query on a box are invisible until restart or explicit rebuild. M5 or M4.5 should add eager indexing on `append_event` (cheap — just walk the payload and `.insert()` once per path).
- **No persistence.** Indexes are rebuilt on every server restart; acceptable at demo scale but fragile.
- **Secondary index skipped.** The planner's middle strategy from spec §6.3 (selective pre-filter via secondary index on nested fields) is not implemented. Broad filters without a session hint fall back to over-fetch, which is O(N) in total event count.
- **Hybrid (vector + text in one pipeline) not supported.** M4 accepts either `$vectorSearch` or `$search` as the source, not both.
- **`$sort`, `$skip`, `$group`, `$unwind` pipeline stages** not implemented.
- **Analyzer is naive.** Whitespace-split + ASCII-lowercase + drop-short-tokens. No stemming, stop-words, or Unicode normalization.
