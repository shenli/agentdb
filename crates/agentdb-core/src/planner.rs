use std::sync::Arc;

use agentdb_index::DocId;
use agentdb_storage::{encode_key, BoxId, Namespace};
use object_store::ObjectStore;

use crate::error::Result;
use crate::indexes::PerBoxIndexes;
use crate::primitives::DEFAULT_BRANCH_ID;
use crate::registry::SessionRegistry;
use crate::types::{Event, Tier};

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
    pub ctx: &'a crate::read_ctx::ReadCtx,
    pub store: &'a Arc<dyn ObjectStore>,
    pub indexes: &'a PerBoxIndexes,
    pub sessions: &'a SessionRegistry,
    pub box_id: BoxId,
    pub tenant_internal_id: u64,
}

impl<'a> HybridPlanner<'a> {
    /// Hint: if the filter pins the session by external id, use locality-bound.
    #[tracing::instrument(
        name = "planner.plan",
        skip(self, filter),
        fields(
            session_hint = session_id_hint.unwrap_or(""),
            strategy = tracing::field::Empty,
            hits = tracing::field::Empty,
        ),
    )]
    pub async fn plan(
        &self,
        source: &Source,
        filter: FilterFn,
        session_id_hint: Option<&str>,
    ) -> Result<PlanOutput> {
        let out = if let Some(session_ext) = session_id_hint {
            self.plan_locality_bound(source, filter, session_ext).await?
        } else {
            self.plan_over_fetch(source, filter).await?
        };
        tracing::Span::current()
            .record("strategy", format!("{:?}", out.strategy))
            .record("hits", out.hits.len());
        Ok(out)
    }

    async fn plan_locality_bound(
        &self,
        source: &Source,
        filter: FilterFn,
        session_ext: &str,
    ) -> Result<PlanOutput> {
        let session = match self
            .ctx
            .session(self.sessions, self.tenant_internal_id, session_ext)?
        {
            Some(s) => s,
            None => {
                return Ok(PlanOutput {
                    strategy: Strategy::LocalityBound,
                    hits: Vec::new(),
                });
            }
        };

        // M17: unify across tiers. Archive-tier sessions no longer
        // short-circuit to empty; the underlying segment read is the
        // same as cold, with the per-tier hedge delay picking up the
        // longer archive threshold.
        let all: Vec<Event> =
            crate::primitives::scan_session_all_tiers(self.ctx, self.store, &session).await?;

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

    async fn plan_over_fetch(&self, source: &Source, filter: FilterFn) -> Result<PlanOutput> {
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
            let Some(session) = self.ctx.session_by_internal(
                self.sessions,
                self.tenant_internal_id,
                doc_id.session_internal_id,
            )?
            else {
                continue;
            };
            // M9 visibility filter: drop candidates past the snapshot's next_seq.
            if doc_id.seq >= session.next_seq {
                continue;
            }
            let Some(event) = self.resolve(doc_id, &session).await? else {
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

    /// Return every event attributed to `actor_id` across every session
    /// in this box's tenant. Uses the `agent_events` index built at
    /// lazy-init time (see `PerBoxIndexes`). Resolves each `DocId`
    /// through the `ReadCtx`, honoring snapshot visibility and HLC
    /// cutoff.
    ///
    /// Events carry `score = 1.0` as a sentinel — there is no ranking
    /// for an exact-match agent filter.
    #[tracing::instrument(
        name = "planner.scan_by_agent",
        skip(self),
        fields(
            actor = actor_id,
            candidates = tracing::field::Empty,
            hits = tracing::field::Empty,
        ),
    )]
    pub async fn scan_by_agent(&self, actor_id: &str) -> Result<Vec<Hit>> {
        let cutoff = self.ctx.hlc_cutoff();
        let docs = self.indexes.docs_by_agent(actor_id);
        tracing::Span::current().record("candidates", docs.len());
        let mut hits: Vec<Hit> = Vec::with_capacity(docs.len());
        for &doc_id in docs {
            let Some(session) = self.ctx.session_by_internal(
                self.sessions,
                self.tenant_internal_id,
                doc_id.session_internal_id,
            )?
            else {
                continue;
            };
            if doc_id.seq >= session.next_seq {
                continue;
            }
            let Some(event) = self.resolve(doc_id, &session).await? else {
                continue;
            };
            // Defense in depth against a stale index: the index only
            // grows, so an entry may reference an event whose
            // `actor_id` has since been cleared (not possible today but
            // worth guarding). The cutoff check is the HLC snapshot.
            if event.actor_id.as_deref() != Some(actor_id) {
                continue;
            }
            if event.hlc_ts > cutoff {
                continue;
            }
            hits.push(Hit {
                doc_id,
                score: 1.0,
                event,
                session_external_id: session.external_id,
            });
        }
        tracing::Span::current().record("hits", hits.len());
        Ok(hits)
    }

    async fn resolve(
        &self,
        doc_id: DocId,
        session: &crate::types::Session,
    ) -> Result<Option<Event>> {
        let key = encode_key(
            Namespace::Events,
            self.box_id,
            self.tenant_internal_id,
            doc_id.session_internal_id,
            DEFAULT_BRANCH_ID,
            doc_id.seq,
        );
        if let Some(bytes) = self.ctx.get(&key)? {
            return Ok(Some(serde_json::from_slice(&bytes)?));
        }
        // Warm miss — if the session is cold, read from its segment.
        let Some(path) = session.segment_path.as_deref() else {
            return Ok(None);
        };
        // A concurrent `reactivate` could have deleted the segment between our
        // KV read and this object-store read. Treat that as a miss rather than
        // propagating an error to the client. Hedged for tail protection.
        let seg = match crate::segment::read_segment_hedged(
            self.store,
            path,
            Tier::Cold,
            self.ctx.metrics(),
        )
        .await
        {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        Ok(seg
            .events
            .into_iter()
            .find(|e| e.seq == doc_id.seq && e.seq < session.next_seq))
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
    use crate::read_ctx::ReadCtx;
    use crate::registry::{SessionRegistry, TenantRegistry};
    use crate::BoxRegistry;
    use agentdb_storage::{KvStore, RocksKvStore};
    use serde_json::json;
    use tempfile::TempDir;

    async fn setup_with_store() -> (
        TempDir,
        TempDir,
        Arc<dyn KvStore>,
        Arc<dyn ObjectStore>,
        Arc<BoxRegistry>,
        Arc<TenantRegistry>,
        Arc<SessionRegistry>,
    ) {
        use object_store::local::LocalFileSystem;
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let reg = Arc::new(BoxRegistry::new(kv.clone(), store.clone(), 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv.clone()));
        (kv_dir, obj_dir, kv, store, reg, tenants, sessions)
    }

    #[tokio::test]
    async fn locality_bound_strategy_is_picked_when_session_hint_present() {
        let (_kd, _od, kv, store, reg, tenants, sessions) = setup_with_store().await;
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
        let box_ = reg.route_tenant("t-1");
        let ctx = ReadCtx::open(&box_, &kv).unwrap();
        let p = HybridPlanner {
            ctx: &ctx,
            store: &store,
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
            .await
            .unwrap();
        assert_eq!(out.strategy, Strategy::LocalityBound);
        assert_eq!(out.hits.len(), 1);
    }

    #[tokio::test]
    async fn over_fetch_strategy_uses_text_index() {
        let (_kd, _od, kv, store, reg, tenants, sessions) = setup_with_store().await;
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
            .build(&kv, &store, &tenants, &sessions, tenant.box_id)
            .await
            .unwrap();
        let box_ = reg.route_tenant("t-1");
        let ctx = ReadCtx::open(&box_, &kv).unwrap();
        let p = HybridPlanner {
            ctx: &ctx,
            store: &store,
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
            .await
            .unwrap();
        assert_eq!(out.strategy, Strategy::OverFetchPostFilter);
        assert_eq!(out.hits.len(), 3);
    }

    #[tokio::test]
    async fn over_fetch_strategy_uses_vector_index() {
        let (_kd, _od, kv, store, reg, tenants, sessions) = setup_with_store().await;
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
            .build(&kv, &store, &tenants, &sessions, tenant.box_id)
            .await
            .unwrap();
        let box_ = reg.route_tenant("t-1");
        let ctx = ReadCtx::open(&box_, &kv).unwrap();
        let p = HybridPlanner {
            ctx: &ctx,
            store: &store,
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
            .await
            .unwrap();
        assert_eq!(out.strategy, Strategy::OverFetchPostFilter);
        assert_eq!(out.hits.len(), 1);
        assert!(out.hits[0].score > 0.99);
    }

    #[tokio::test]
    async fn vector_search_over_fetch_strategy_finds_cold_session_event() {
        let (_kd, _od, kv, store, reg, tenants, sessions) = setup_with_store().await;
        append_and_fetch(
            &reg,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"embedding": [1.0, 0.0, 0.0]}),
            0,
            100,
        )
        .await
        .unwrap();

        let tenant = tenants
            .get_or_mint(reg.route_tenant("t-1").id, "t-1", 0)
            .unwrap();
        let mut session = sessions
            .lookup(tenant.box_id, tenant.internal_id, "s-1")
            .unwrap()
            .unwrap();
        let box_ = reg.route_tenant("t-1");
        {
            let guard = box_.write_lock.lock().await;
            crate::tiering::cold_transition(&kv, &store, &sessions, &guard, &mut session)
                .await
                .unwrap();
        }

        let mut indexes = PerBoxIndexes::new();
        indexes
            .build(&kv, &store, &tenants, &sessions, tenant.box_id)
            .await
            .unwrap();

        let ctx = ReadCtx::open(&box_, &kv).unwrap();
        let p = HybridPlanner {
            ctx: &ctx,
            store: &store,
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
            .await
            .unwrap();
        assert_eq!(out.hits.len(), 1);
        assert!(out.hits[0].score > 0.99);
    }

    #[tokio::test]
    async fn planner_filters_out_candidates_beyond_snapshot_next_seq() {
        let (_kd, _od, kv, store, reg, tenants, sessions) = setup_with_store().await;
        for i in 1..=5u64 {
            append_and_fetch(
                &reg,
                &tenants,
                &sessions,
                "t-1",
                "s-1",
                json!({"text": format!("alpha {i}")}),
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
            .build(&kv, &store, &tenants, &sessions, tenant.box_id)
            .await
            .unwrap();

        let box_ = reg.route_tenant("t-1");
        let ctx = ReadCtx::open(&box_, &kv).unwrap();
        // Post-snapshot append must NOT appear in planner results.
        append_and_fetch(
            &reg,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"text": "alpha 6"}),
            0,
            6,
        )
        .await
        .unwrap();
        // Rebuild to simulate index seeing the newer event (planner visibility
        // is the guard, not the index).
        let mut indexes2 = PerBoxIndexes::new();
        indexes2
            .build(&kv, &store, &tenants, &sessions, tenant.box_id)
            .await
            .unwrap();

        let p = HybridPlanner {
            ctx: &ctx,
            store: &store,
            indexes: &indexes2,
            sessions: &sessions,
            box_id: tenant.box_id,
            tenant_internal_id: tenant.internal_id,
        };
        let out = p
            .plan(
                &Source::Text {
                    path: "payload.text".into(),
                    query: "alpha".into(),
                    limit: 10,
                },
                Box::new(|_, _| true),
                None,
            )
            .await
            .unwrap();
        // Original 5 events visible; the 6th is post-snapshot and filtered out.
        assert_eq!(
            out.hits.len(),
            5,
            "visibility filter must drop post-snapshot candidates"
        );
    }

    #[tokio::test]
    async fn scan_by_agent_returns_cross_session_events_for_one_agent() {
        // Two sessions attributed to agent_a (3 events each), one session
        // attributed to agent_b (2 events). Scanning for agent_a should
        // return 6 events across both agent_a sessions; agent_b returns 2;
        // an unknown agent returns nothing.
        let (_kd, _od, kv, store, reg, tenants, sessions) = setup_with_store().await;

        for session in ["a-1", "a-2"] {
            for i in 1..=3u64 {
                crate::primitives::append_and_fetch_with_actor(
                    &reg,
                    &tenants,
                    &sessions,
                    "t-agent",
                    session,
                    json!({"n": i}),
                    Some("agent_a".into()),
                    0,
                    i,
                )
                .await
                .unwrap();
            }
        }
        for i in 1..=2u64 {
            crate::primitives::append_and_fetch_with_actor(
                &reg,
                &tenants,
                &sessions,
                "t-agent",
                "b-1",
                json!({"n": i}),
                Some("agent_b".into()),
                0,
                i,
            )
            .await
            .unwrap();
        }

        let tenant = tenants
            .get_or_mint(reg.route_tenant("t-agent").id, "t-agent", 0)
            .unwrap();

        let mut indexes = PerBoxIndexes::new();
        indexes
            .build(&kv, &store, &tenants, &sessions, tenant.box_id)
            .await
            .unwrap();

        // Index sanity: both agents present, sizes match expectations.
        assert_eq!(indexes.docs_by_agent("agent_a").len(), 6);
        assert_eq!(indexes.docs_by_agent("agent_b").len(), 2);
        assert_eq!(indexes.docs_by_agent("agent_c").len(), 0);

        let box_ = reg.route_tenant("t-agent");
        let ctx = ReadCtx::open(&box_, &kv).unwrap();
        let p = HybridPlanner {
            ctx: &ctx,
            store: &store,
            indexes: &indexes,
            sessions: &sessions,
            box_id: tenant.box_id,
            tenant_internal_id: tenant.internal_id,
        };

        let a_hits = p.scan_by_agent("agent_a").await.unwrap();
        assert_eq!(a_hits.len(), 6, "all agent_a events across both sessions");
        for h in &a_hits {
            assert_eq!(h.event.actor_id.as_deref(), Some("agent_a"));
            assert!(h.session_external_id == "a-1" || h.session_external_id == "a-2");
        }

        let b_hits = p.scan_by_agent("agent_b").await.unwrap();
        assert_eq!(b_hits.len(), 2);
        for h in &b_hits {
            assert_eq!(h.event.actor_id.as_deref(), Some("agent_b"));
            assert_eq!(h.session_external_id, "b-1");
        }

        let c_hits = p.scan_by_agent("agent_c").await.unwrap();
        assert!(c_hits.is_empty());
    }

    #[tokio::test]
    async fn scan_by_agent_respects_hlc_cutoff_against_post_snapshot_writes() {
        // An event written after the ReadCtx opens must not appear through
        // scan_by_agent even if the index has been rebuilt since. This
        // mirrors the M14 HLC-cutoff contract on fetch_last_n.
        let (_kd, _od, kv, store, reg, tenants, sessions) = setup_with_store().await;

        // Warm up: one pre-snapshot event for agent_x.
        crate::primitives::append_and_fetch_with_actor(
            &reg,
            &tenants,
            &sessions,
            "t-agent",
            "s-1",
            json!({"n": 1}),
            Some("agent_x".into()),
            0,
            1,
        )
        .await
        .unwrap();

        let tenant = tenants
            .get_or_mint(reg.route_tenant("t-agent").id, "t-agent", 0)
            .unwrap();
        let box_ = reg.route_tenant("t-agent");
        let ctx = ReadCtx::open(&box_, &kv).unwrap();

        // Post-snapshot writes under the same agent.
        for i in 2..=4u64 {
            crate::primitives::append_and_fetch_with_actor(
                &reg,
                &tenants,
                &sessions,
                "t-agent",
                "s-1",
                json!({"n": i}),
                Some("agent_x".into()),
                0,
                i,
            )
            .await
            .unwrap();
        }

        // Rebuild indexes so they DO contain the post-snapshot events;
        // the HLC cutoff on the ctx is what must filter them out.
        let mut indexes = PerBoxIndexes::new();
        indexes
            .build(&kv, &store, &tenants, &sessions, tenant.box_id)
            .await
            .unwrap();
        assert_eq!(indexes.docs_by_agent("agent_x").len(), 4);

        let p = HybridPlanner {
            ctx: &ctx,
            store: &store,
            indexes: &indexes,
            sessions: &sessions,
            box_id: tenant.box_id,
            tenant_internal_id: tenant.internal_id,
        };
        let hits = p.scan_by_agent("agent_x").await.unwrap();
        assert_eq!(
            hits.len(),
            1,
            "ctx opened after 1 event; post-snapshot events must be filtered"
        );
        assert_eq!(hits[0].event.payload["n"], 1);
    }

    #[tokio::test]
    async fn locality_bound_text_search_works_on_archived_session() {
        // M17 regression: pre-M17 the planner's `Tier::Archive` branch
        // returned empty, so text/vector queries hinted at a specific
        // archived session found nothing. Now archive reads through
        // the segment uniformly with cold.
        use crate::read_ctx::ReadCtx;
        let (_kd, _od, kv, store, reg, tenants, sessions) = setup_with_store().await;
        append_and_fetch(
            &reg,
            &tenants,
            &sessions,
            "t-arc",
            "s-arc",
            json!({"text": "refund policy archive"}),
            0,
            0,
        )
        .await
        .unwrap();
        let box_ = reg.route_tenant("t-arc");
        let tenant = tenants
            .get_or_mint(reg.route_tenant("t-arc").id, "t-arc", 0)
            .unwrap();
        let mut session = sessions
            .lookup(box_.id, tenant.internal_id, "s-arc")
            .unwrap()
            .unwrap();
        // Cold + Archive transitions.
        {
            let guard = box_.write_lock.lock().await;
            crate::tiering::cold_transition(
                &kv,
                reg.object_store(),
                &sessions,
                &guard,
                &mut session,
            )
            .await
            .unwrap();
        }
        session.tier = crate::types::Tier::Archive;
        sessions.save(&session).unwrap();

        // Indexes are intentionally empty — the locality-bound path
        // doesn't use them for archive; it reads the segment directly.
        let indexes = PerBoxIndexes::new();
        let ctx = ReadCtx::open(&box_, &kv).unwrap();
        let p = HybridPlanner {
            ctx: &ctx,
            store: &store,
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
                    limit: 10,
                },
                Box::new(|_, _| true),
                Some("s-arc"),
            )
            .await
            .unwrap();
        assert_eq!(
            out.hits.len(),
            1,
            "archived-session locality-bound search must hit (M17)"
        );
    }
}
