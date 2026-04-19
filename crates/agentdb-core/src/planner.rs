use std::sync::Arc;

use agentdb_index::DocId;
use agentdb_storage::{encode_key, BoxId, KvStore, Namespace};

use crate::error::Result;
use crate::indexes::PerBoxIndexes;
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
        let session =
            match self
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
            // Exclude both missing-field (NEG_INFINITY) and zero-score (no
            // query terms matched) cases. The OverFetchPostFilter strategy
            // only considers events in the index postings so it naturally
            // skips non-matches; LocalityBound needs this explicit guard.
            if score > 0.0 {
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
            Source::Text { path, query, limit } => {
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
        TempDir,
        Arc<dyn KvStore>,
        Arc<BoxRegistry>,
        Arc<TenantRegistry>,
        Arc<SessionRegistry>,
    ) {
        use object_store::local::LocalFileSystem;
        use object_store::ObjectStore;
        let dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let reg = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv.clone()));
        (dir, obj_dir, kv, reg, tenants, sessions)
    }

    #[tokio::test]
    async fn locality_bound_strategy_is_picked_when_session_hint_present() {
        let (_d, _obj_dir, kv, reg, tenants, sessions) = setup().await;
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

        let tenant = tenants
            .get_or_mint(reg.route_tenant("t-1").id, "t-1", 0)
            .unwrap();
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
        let (_d, _obj_dir, kv, reg, tenants, sessions) = setup().await;
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
        let tenant = tenants
            .get_or_mint(reg.route_tenant("t-1").id, "t-1", 0)
            .unwrap();
        let mut indexes = PerBoxIndexes::new();
        indexes
            .build(&kv, &tenants, &sessions, tenant.box_id)
            .unwrap();
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
        let (_d, _obj_dir, kv, reg, tenants, sessions) = setup().await;
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
        let tenant = tenants
            .get_or_mint(reg.route_tenant("t-1").id, "t-1", 0)
            .unwrap();
        let mut indexes = PerBoxIndexes::new();
        indexes
            .build(&kv, &tenants, &sessions, tenant.box_id)
            .unwrap();
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
