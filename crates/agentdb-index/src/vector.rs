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
