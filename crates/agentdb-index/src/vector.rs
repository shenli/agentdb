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
        let mut ranked: Vec<(DocId, Score)> = state
            .vectors
            .iter()
            .map(|(id, v)| (*id, dot(&q, v)))
            .collect();
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
