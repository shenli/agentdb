use agentdb_core::{BoxRegistry, Hit, HybridPlanner, SessionRegistry, Source, TenantRegistry};
use bson::{doc, Bson, Document};
use serde_json::Value;

use crate::bsonx::json_to_bson;
use crate::error::{WireError, WireResult};
use crate::handlers::filter::Filter;

/// Cap incoming limit/candidate values to prevent `limit * over_fetch`
/// overflow in the planner. 100K is far beyond realistic demand at prototype
/// scale and leaves room for over-fetch multipliers.
const MAX_STAGE_LIMIT: usize = 100_000;

fn clamp_limit(raw: i64) -> usize {
    let x = raw.max(1) as usize;
    x.min(MAX_STAGE_LIMIT)
}

pub async fn aggregate_response(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    db: &str,
    collection: &str,
    pipeline: &[Document],
) -> WireResult<Document> {
    registry.route_tenant(db).metrics.record_query_aggregate();
    if collection != "events" {
        return Err(WireError::Protocol(format!(
            "aggregate on {collection} not supported; only 'events' is indexed"
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
    let indexes_guard = box_
        .indexes_built(registry.kv(), registry.object_store(), tenants, sessions)
        .await?;
    let indexes = indexes_guard.as_ref().expect("built by indexes_built");

    // Session hint — either from the source-stage `filter.session_id` or
    // the downstream `$match.session_id` if present.
    let hint_from_source = source_filter
        .top_level_eq("session_id")
        .and_then(|v| match v {
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

    // Open a single ReadCtx for the aggregate: the planner scans warm events
    // through this snapshot and pins any cold segments it touches for the
    // duration of the call.
    let ctx = agentdb_core::ReadCtx::open(&box_, registry.kv())?;
    let planner = HybridPlanner {
        ctx: &ctx,
        store: registry.object_store(),
        indexes,
        sessions,
        box_id: tenant.box_id,
        tenant_internal_id: tenant.internal_id,
    };

    let out = planner
        .plan(&source, predicate, session_hint.as_deref())
        .await?;
    let mut hits = out.hits;

    if let Some(limit) = post_limit {
        let cap = if limit <= 0 {
            usize::MAX
        } else {
            limit as usize
        };
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
                _ => {
                    return Err(WireError::Protocol(
                        "queryVector entries must be numeric".into(),
                    ))
                }
            }
        }
        let path = vs
            .get_str("path")
            .map_err(|_| WireError::Protocol("$vectorSearch missing path".into()))?
            .to_string();
        let limit = clamp_limit(vs.get_i64("limit").unwrap_or(10));
        let num_candidates = clamp_limit(vs.get_i64("numCandidates").unwrap_or(100));
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
        let limit = clamp_limit(s.get_i64("limit").unwrap_or(10));
        let filter = match s.get_document("filter") {
            Ok(f) => Filter::parse(f)?,
            Err(_) => Filter::parse(&Document::new())?,
        };
        return Ok((Source::Text { path, query, limit }, filter));
    }

    Err(WireError::Protocol(
        "aggregate pipeline must start with $vectorSearch or $search".into(),
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
    use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
    use agentdb_storage::{KvStore, RocksKvStore};
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn setup() -> (
        TempDir,
        TempDir,
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
        let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        (dir, obj_dir, registry, tenants, sessions)
    }

    async fn seed_text(
        registry: &BoxRegistry,
        tenants: &TenantRegistry,
        sessions: &SessionRegistry,
    ) {
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
        let (_d, _od, r, t, s) = setup().await;
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
        let (_d, _od, r, t, s) = setup().await;
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
        let (_d, _od, r, t, s) = setup().await;
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
        let (_d, _od, r, t, s) = setup().await;
        seed_text(&r, &t, &s).await;
        let err = aggregate_response(&r, &t, &s, "t-1", "events", &[doc! { "$match": {} }])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("$vectorSearch or $search"));
    }
}
