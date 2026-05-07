//! GSI-like record placement index contract.
//!
//! The runtime's local record index is the authoritative read-after-write path
//! when a request is routed to an owner placement. This module defines the
//! eventually-consistent global index used for cross-placement discovery when a
//! caller does not provide a sharding key. It deliberately carries watermarks so
//! callers can tell whether a global result is behind a known commit token.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use crate::errors::Result;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GlobalIndexMutationOp {
    Upsert,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GlobalIndexMutation {
    pub tenant_id: String,
    pub record_id: String,
    pub namespace: Vec<String>,
    pub placement_id: i64,
    pub commit_seq: u64,
    pub op: GlobalIndexMutationOp,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub terms: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalIndexQuery {
    pub tenant_id: String,
    pub namespace: Vec<String>,
    pub terms: Vec<String>,
    pub min_visible_commit_seq: Option<u64>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalIndexHit {
    pub record_id: String,
    pub placement_id: i64,
    pub commit_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalIndexSearchResult {
    pub hits: Vec<GlobalIndexHit>,
    pub watermark: u64,
    pub stale: bool,
}

pub trait GlobalRecordIndex: Send + Sync {
    fn apply(&self, mutation: GlobalIndexMutation) -> Result<()>;
    fn search(&self, query: GlobalIndexQuery) -> Result<GlobalIndexSearchResult>;
    fn watermark(&self, tenant_id: &str) -> Result<u64>;
    fn reset(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct NoopGlobalRecordIndex;

impl GlobalRecordIndex for NoopGlobalRecordIndex {
    fn apply(&self, _mutation: GlobalIndexMutation) -> Result<()> {
        Ok(())
    }

    fn search(&self, query: GlobalIndexQuery) -> Result<GlobalIndexSearchResult> {
        let watermark = 0;
        Ok(GlobalIndexSearchResult {
            hits: vec![],
            watermark,
            stale: query
                .min_visible_commit_seq
                .is_some_and(|seq| seq > watermark),
        })
    }

    fn watermark(&self, _tenant_id: &str) -> Result<u64> {
        Ok(0)
    }
}

#[derive(Debug, Default)]
pub struct InMemoryGlobalRecordIndex {
    inner: Mutex<InMemoryGlobalRecordIndexState>,
}

#[derive(Debug, Default)]
struct InMemoryGlobalRecordIndexState {
    records: BTreeMap<String, GlobalIndexEntry>,
    tenant_watermark: BTreeMap<String, u64>,
}

#[derive(Debug, Clone)]
struct GlobalIndexEntry {
    tenant_id: String,
    record_id: String,
    namespace: Vec<String>,
    placement_id: i64,
    commit_seq: u64,
    terms: BTreeSet<String>,
}

impl GlobalRecordIndex for InMemoryGlobalRecordIndex {
    fn reset(&self) -> Result<()> {
        *self.inner.lock().expect("global record index lock") =
            InMemoryGlobalRecordIndexState::default();
        Ok(())
    }

    fn apply(&self, mutation: GlobalIndexMutation) -> Result<()> {
        let mut inner = self.inner.lock().expect("global record index lock");
        let tenant_watermark = inner
            .tenant_watermark
            .entry(mutation.tenant_id.clone())
            .or_default();
        *tenant_watermark = (*tenant_watermark).max(mutation.commit_seq);

        match mutation.op {
            GlobalIndexMutationOp::Upsert => {
                inner.records.insert(
                    mutation.record_id.clone(),
                    GlobalIndexEntry {
                        tenant_id: mutation.tenant_id,
                        record_id: mutation.record_id,
                        namespace: mutation.namespace,
                        placement_id: mutation.placement_id,
                        commit_seq: mutation.commit_seq,
                        terms: normalized_terms(mutation.terms),
                    },
                );
            }
            GlobalIndexMutationOp::Delete => {
                inner.records.remove(&mutation.record_id);
            }
        }
        Ok(())
    }

    fn search(&self, query: GlobalIndexQuery) -> Result<GlobalIndexSearchResult> {
        let inner = self.inner.lock().expect("global record index lock");
        let watermark = *inner.tenant_watermark.get(&query.tenant_id).unwrap_or(&0);
        let required_terms = normalized_terms(query.terms);
        let limit = query.limit.max(1);
        let mut hits = Vec::with_capacity(limit.min(32));

        for entry in inner.records.values() {
            if hits.len() >= limit {
                break;
            }
            if entry.tenant_id != query.tenant_id || entry.namespace != query.namespace {
                continue;
            }
            if !required_terms.is_subset(&entry.terms) {
                continue;
            }
            hits.push(GlobalIndexHit {
                record_id: entry.record_id.clone(),
                placement_id: entry.placement_id,
                commit_seq: entry.commit_seq,
            });
        }

        Ok(GlobalIndexSearchResult {
            hits,
            watermark,
            stale: query
                .min_visible_commit_seq
                .is_some_and(|seq| seq > watermark),
        })
    }

    fn watermark(&self, tenant_id: &str) -> Result<u64> {
        let inner = self.inner.lock().expect("global record index lock");
        Ok(*inner.tenant_watermark.get(tenant_id).unwrap_or(&0))
    }
}

fn normalized_terms(terms: Vec<String>) -> BTreeSet<String> {
    terms
        .into_iter()
        .map(|term| term.trim().to_ascii_lowercase())
        .filter(|term| !term.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_global_index_discovers_placement_and_tracks_watermark() {
        let index = InMemoryGlobalRecordIndex::default();
        index
            .apply(GlobalIndexMutation {
                tenant_id: "ten_1".to_string(),
                record_id: "rec_1".to_string(),
                namespace: vec!["mem".to_string()],
                placement_id: 7,
                commit_seq: 17,
                op: GlobalIndexMutationOp::Upsert,
                terms: vec!["Alpha".to_string(), "beta".to_string()],
            })
            .unwrap();

        let result = index
            .search(GlobalIndexQuery {
                tenant_id: "ten_1".to_string(),
                namespace: vec!["mem".to_string()],
                terms: vec!["alpha".to_string()],
                min_visible_commit_seq: Some(17),
                limit: 10,
            })
            .unwrap();

        assert_eq!(result.watermark, 17);
        assert!(!result.stale);
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].placement_id, 7);
    }

    #[test]
    fn global_index_reports_stale_when_watermark_lags_commit_token() {
        let index = InMemoryGlobalRecordIndex::default();
        let result = index
            .search(GlobalIndexQuery {
                tenant_id: "ten_1".to_string(),
                namespace: vec!["mem".to_string()],
                terms: vec![],
                min_visible_commit_seq: Some(1),
                limit: 10,
            })
            .unwrap();

        assert!(result.stale);
        assert_eq!(result.watermark, 0);
    }
}
