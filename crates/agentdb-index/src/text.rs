use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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

/// `(session_internal_id, seq, term_frequency_in_doc)` — one entry per
/// (doc, token) pair in a path's postings list.
pub type PostingEntry = (u64, u64, u32);
/// A single term's postings: token -> list of entries for that token.
pub type TokenPostings = (String, Vec<PostingEntry>);
/// `(session_internal_id, seq, doc_len_in_tokens)` — one entry per doc.
pub type DocLenEntry = (u64, u64, u32);

/// Serializable snapshot of a BM25 text index. Structurally mirrors the
/// live state; rebuilds live state on `InMemoryInverted::from_snapshot`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TextSnapshot {
    pub paths: Vec<PathSnapshot>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PathSnapshot {
    pub path: String,
    /// token -> [(session, seq, tf)] — same shape as the live postings list.
    pub postings: Vec<TokenPostings>,
    /// [(session, seq, doc_len)] — same shape as the live doc_len map.
    pub doc_len: Vec<DocLenEntry>,
    pub total_tokens: u64,
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

    /// Capture current state as a serializable snapshot (M16 Phase D).
    pub fn to_snapshot(&self) -> TextSnapshot {
        let mut paths = Vec::with_capacity(self.per_path.len());
        for (path, state) in &self.per_path {
            let postings: Vec<TokenPostings> = state
                .postings
                .iter()
                .map(|(token, list)| {
                    (
                        token.clone(),
                        list.iter()
                            .map(|(d, tf)| (d.session_internal_id, d.seq, *tf))
                            .collect(),
                    )
                })
                .collect();
            let doc_len: Vec<DocLenEntry> = state
                .doc_len
                .iter()
                .map(|(d, l)| (d.session_internal_id, d.seq, *l))
                .collect();
            paths.push(PathSnapshot {
                path: path.clone(),
                postings,
                doc_len,
                total_tokens: state.total_tokens,
            });
        }
        TextSnapshot { paths }
    }

    /// Rebuild from a snapshot (M16 Phase D).
    pub fn from_snapshot(snap: &TextSnapshot) -> Self {
        let mut per_path: HashMap<String, PathState> = HashMap::new();
        for p in &snap.paths {
            let postings: HashMap<String, Vec<(DocId, u32)>> = p
                .postings
                .iter()
                .map(|(tok, list)| {
                    (
                        tok.clone(),
                        list.iter()
                            .map(|(s, q, tf)| (DocId::new(*s, *q), *tf))
                            .collect(),
                    )
                })
                .collect();
            let doc_len: HashMap<DocId, u32> = p
                .doc_len
                .iter()
                .map(|(s, q, l)| (DocId::new(*s, *q), *l))
                .collect();
            per_path.insert(
                p.path.clone(),
                PathState {
                    postings,
                    doc_len,
                    total_tokens: p.total_tokens,
                },
            );
        }
        Self { per_path }
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
