//! HNSW-backed vector index.
//!
//! Backend: `hnsw_rs` 0.3 (swapped from `instant-distance` in M16 to get
//! incremental insert + file-based snapshot). See
//! `docs/superpowers/notes/2026-04-22-hnsw-backend-spike.md`.
//!
//! One `Hnsw` per path, built incrementally as `insert()` is called —
//! no rebuild on query. A small-N bypass keeps tiny-dataset tests
//! deterministic.

use std::collections::HashMap;

use hnsw_rs::prelude::{DistCosine, Hnsw};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::doc_id::DocId;
use crate::error::{IndexError, IndexResult};

pub type Score = f32;

/// Default `ef_construction` / `ef_search`. Mirrors the knob exposed by
/// `HnswVectorIndex::with_ef` and `from_env` (`AGENTDB_HNSW_EF`).
pub const DEFAULT_EF: usize = 100;
/// Below this N, we skip HNSW entirely and brute-force cosine. Keeps
/// tiny-dataset tests deterministic and avoids HNSW graph-construction
/// corner cases at small N.
pub const BRUTE_FORCE_THRESHOLD: usize = 64;

/// `hnsw_rs` default HNSW graph connectivity. 16 is the library's
/// convention; we will not tune this until a benchmark motivates it.
const HNSW_MAX_CONNECTIONS: usize = 16;
/// Max levels in the HNSW hierarchy — `hnsw_rs`'s typical default.
const HNSW_MAX_LAYERS: usize = 16;
/// Initial `max_elements` for a fresh `Hnsw`. `hnsw_rs` resizes
/// internally on insert, so this is just a starting hint.
const HNSW_INITIAL_CAPACITY: usize = 1_000;

pub trait VectorIndex: Send + Sync {
    fn insert(&mut self, path: &str, doc_id: DocId, vector: &[f32]) -> IndexResult<()>;
    fn knn(&self, path: &str, query: &[f32], limit: usize) -> Vec<(DocId, Score)>;
}

pub struct HnswVectorIndex {
    per_path: Mutex<HashMap<String, PathState>>,
    ef: usize,
}

struct PathState {
    dim: usize,
    /// Unit-normalized vectors, kept in insertion order. Brute force
    /// uses this directly; HNSW uses `doc_ids[i]` as the inverse
    /// mapping from the `usize` it stores back to our `DocId`.
    entries: Vec<(DocId, Vec<f32>)>,
    /// Built lazily once `entries.len() >= BRUTE_FORCE_THRESHOLD` on a
    /// query; insert populates it incrementally afterward. `None` until
    /// that first crossing — small-N queries bypass HNSW entirely.
    hnsw: Option<Box<Hnsw<'static, f32, DistCosine>>>,
}

impl Default for HnswVectorIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl HnswVectorIndex {
    pub fn new() -> Self {
        Self::with_ef(DEFAULT_EF)
    }

    pub fn with_ef(ef: usize) -> Self {
        Self {
            per_path: Mutex::new(HashMap::new()),
            ef,
        }
    }

    /// Reads `AGENTDB_HNSW_EF` from the environment (applied to both
    /// `ef_construction` and `ef_search`), falling back to `DEFAULT_EF`
    /// if missing or unparseable.
    pub fn from_env() -> Self {
        let ef = std::env::var("AGENTDB_HNSW_EF")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_EF);
        Self::with_ef(ef)
    }

    /// Capture current state as a serializable snapshot (M16 Phase D).
    /// Stores the unit-normalized per-path `entries` only; HNSW graphs
    /// are rebuilt lazily on first query after load (same lazy-build
    /// rule that applies to a fresh instance).
    pub fn to_snapshot(&self) -> VectorSnapshot {
        let map = self.per_path.lock();
        let paths: Vec<VectorPathSnapshot> = map
            .iter()
            .map(|(path, state)| VectorPathSnapshot {
                path: path.clone(),
                dim: state.dim,
                entries: state
                    .entries
                    .iter()
                    .map(|(d, v)| (d.session_internal_id, d.seq, v.clone()))
                    .collect(),
            })
            .collect();
        VectorSnapshot { paths }
    }

    /// Rebuild from a snapshot (M16 Phase D). HNSW graphs are not
    /// pre-built — the first `knn()` call at a path crossing
    /// `BRUTE_FORCE_THRESHOLD` will rebuild from the loaded entries.
    pub fn from_snapshot(snap: &VectorSnapshot, ef: usize) -> Self {
        let mut per_path = HashMap::new();
        for p in &snap.paths {
            let entries: Vec<(DocId, Vec<f32>)> = p
                .entries
                .iter()
                .map(|(s, q, v)| (DocId::new(*s, *q), v.clone()))
                .collect();
            per_path.insert(
                p.path.clone(),
                PathState {
                    dim: p.dim,
                    entries,
                    hnsw: None,
                },
            );
        }
        Self {
            per_path: Mutex::new(per_path),
            ef,
        }
    }
}

/// Serializable snapshot of the vector index. Preserves normalized
/// per-path entries; HNSW structure is rebuilt lazily on first query.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorSnapshot {
    pub paths: Vec<VectorPathSnapshot>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorPathSnapshot {
    pub path: String,
    pub dim: usize,
    /// `(session_internal_id, seq, normalized_vector)`.
    pub entries: Vec<(u64, u64, Vec<f32>)>,
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

/// Build a fresh HNSW, bulk-load the current `entries`, and return it.
/// Called at the boundary where a path crosses `BRUTE_FORCE_THRESHOLD`.
fn build_hnsw_from_entries(
    entries: &[(DocId, Vec<f32>)],
    ef: usize,
) -> Box<Hnsw<'static, f32, DistCosine>> {
    let hnsw: Hnsw<'static, f32, DistCosine> = Hnsw::new(
        HNSW_MAX_CONNECTIONS,
        entries.len().max(HNSW_INITIAL_CAPACITY),
        HNSW_MAX_LAYERS,
        ef,
        DistCosine {},
    );
    for (i, (_, v)) in entries.iter().enumerate() {
        hnsw.insert((v.as_slice(), i));
    }
    Box::new(hnsw)
}

impl VectorIndex for HnswVectorIndex {
    fn insert(&mut self, path: &str, doc_id: DocId, vector: &[f32]) -> IndexResult<()> {
        let mut map = self.per_path.lock();
        let state = map.entry(path.to_string()).or_insert_with(|| PathState {
            dim: vector.len(),
            entries: Vec::new(),
            hnsw: None,
        });
        if state.dim != vector.len() {
            return Err(IndexError::Invalid(format!(
                "vector dim mismatch at path {path}: expected {}, got {}",
                state.dim,
                vector.len()
            )));
        }
        let v = normalise(vector);
        let idx = state.entries.len();
        state.entries.push((doc_id, v.clone()));
        if let Some(hnsw) = state.hnsw.as_ref() {
            // Live incremental insert once the HNSW exists.
            hnsw.insert((v.as_slice(), idx));
        }
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

        // Large-N: build HNSW on first cross of the threshold, then
        // keep using it (incremental inserts have been flowing in).
        if state.hnsw.is_none() {
            state.hnsw = Some(build_hnsw_from_entries(&state.entries, self.ef));
        }
        let hnsw = state.hnsw.as_ref().expect("just built");

        let neighbours = hnsw.search(&q_norm, limit, self.ef);
        let mut out: Vec<(DocId, Score)> = Vec::with_capacity(neighbours.len());
        for n in neighbours {
            let entry_idx = n.d_id;
            let Some((doc_id, _)) = state.entries.get(entry_idx) else {
                // Out of sync between HNSW and entries — drop the hit
                // rather than panic. Indicates a serious bug that
                // deserves investigation in the logs.
                tracing::warn!(
                    path,
                    entry_idx,
                    entries_len = state.entries.len(),
                    "HNSW returned index out of entries bounds"
                );
                continue;
            };
            // DistCosine returns 1 - cos_sim; invert for the caller.
            let score = 1.0 - n.distance;
            out.push((*doc_id, score));
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
    fn incremental_inserts_after_hnsw_build_are_visible() {
        // Cross BRUTE_FORCE_THRESHOLD so HNSW activates; then insert
        // one more and verify it's findable. This is the M16 core
        // invariant that instant-distance couldn't satisfy without
        // a full rebuild.
        let mut idx = HnswVectorIndex::new();
        for i in 0..(BRUTE_FORCE_THRESHOLD as u64 + 10) {
            // Spread vectors around so no duplicate neighbour ambiguity.
            idx.insert("p", d(1, i), &[i as f32, 1.0, 0.0]).unwrap();
        }
        // First query builds HNSW.
        let _ = idx.knn("p", &[50.0, 1.0, 0.0], 3);

        // Post-build incremental insert.
        let new_id = d(99, 99);
        idx.insert("p", new_id, &[1000.0, 1.0, 0.0]).unwrap();

        let hits = idx.knn("p", &[1000.0, 1.0, 0.0], 3);
        assert!(
            hits.iter().any(|(id, _)| *id == new_id),
            "post-HNSW-build insert not found in neighbours: {:?}",
            hits.iter().map(|(id, _)| *id).collect::<Vec<_>>()
        );
    }
}
