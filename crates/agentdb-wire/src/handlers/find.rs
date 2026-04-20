use agentdb_core::{scan_session_events, BoxRegistry, Event, ReadCtx, SessionRegistry, TenantRegistry};
use bson::{doc, Bson, Document};
use serde_json::Value;

use crate::bsonx::json_to_bson;
use crate::error::{WireError, WireResult};
use crate::handlers::filter::Filter;

pub fn find_response(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    db: &str,
    collection: &str,
    filter_doc: &Document,
    limit: Option<i64>,
) -> WireResult<Document> {
    let box_ = registry.route_tenant(db);
    box_.metrics.record_query_find();
    let filter = Filter::parse(filter_doc)?;
    let tenant = match tenants.lookup(box_.id, db)? {
        Some(t) => t,
        None => return Ok(empty_batch(db, collection)),
    };
    let ctx = ReadCtx::open(&box_, registry.kv())?;
    let mut batch: Vec<Value> = Vec::new();
    let cap = limit.unwrap_or(i64::MAX).max(0) as usize;

    match collection {
        "events" => {
            let session_externals = sessions.list_in_tenant(tenant.box_id, tenant.internal_id)?;
            // If the filter targets a specific session, only scan that one.
            let scoped: Vec<String> = match filter.top_level_eq("session_id") {
                Some(Value::String(s)) => vec![s.clone()],
                _ => session_externals,
            };
            for s_ext in scoped {
                let Some(session) = ctx.session(sessions, tenant.internal_id, &s_ext)? else {
                    continue;
                };
                for ev in scan_session_events(&ctx, &session)? {
                    let row = project_event(&session.external_id, ev);
                    if filter.matches(&row) {
                        batch.push(row);
                        if batch.len() >= cap {
                            break;
                        }
                    }
                }
                if batch.len() >= cap {
                    break;
                }
            }
        }
        "sessions" => {
            let session_externals = sessions.list_in_tenant(tenant.box_id, tenant.internal_id)?;
            for s_ext in session_externals {
                let Some(session) = ctx.session(sessions, tenant.internal_id, &s_ext)? else {
                    continue;
                };
                let row = serde_json::to_value(&session)?;
                if filter.matches(&row) {
                    batch.push(row);
                    if batch.len() >= cap {
                        break;
                    }
                }
            }
        }
        other => {
            return Err(WireError::Protocol(format!(
                "unknown collection {other}; only 'events' and 'sessions' exist"
            )));
        }
    }

    let first_batch: Vec<Bson> = batch.into_iter().map(json_to_bson).collect();
    Ok(doc! {
        "cursor": {
            "id": 0_i64,
            "ns": format!("{}.{}", db, collection),
            "firstBatch": Bson::Array(first_batch),
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

fn project_event(session_id: &str, ev: Event) -> Value {
    serde_json::json!({
        "session_id": session_id,
        "seq": ev.seq,
        "ts": ev.ts,
        "payload": ev.payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_storage::{KvStore, RocksKvStore};
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn setup_with_events() -> (
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
        for seq in 1..=3u64 {
            agentdb_core::append_and_fetch(
                &registry,
                &tenants,
                &sessions,
                "t-1",
                "s-1",
                json!({"kind": "msg", "i": seq}),
                0,
                seq,
            )
            .await
            .unwrap();
        }
        (dir, obj_dir, registry, tenants, sessions)
    }

    #[tokio::test]
    async fn find_events_returns_all_for_tenant() {
        let (_dir, _obj_dir, registry, tenants, sessions) = setup_with_events().await;
        let resp = find_response(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "events",
            &doc! {},
            None,
        )
        .unwrap();
        let batch = resp
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 3);
    }

    #[tokio::test]
    async fn find_events_with_session_id_filter_scans_only_that_session() {
        let (_dir, _obj_dir, registry, tenants, sessions) = setup_with_events().await;
        // create a second session
        for _ in 0..2 {
            agentdb_core::append_and_fetch(
                &registry,
                &tenants,
                &sessions,
                "t-1",
                "s-2",
                json!({"kind": "other"}),
                0,
                0,
            )
            .await
            .unwrap();
        }
        let resp = find_response(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "events",
            &doc! { "session_id": "s-1" },
            None,
        )
        .unwrap();
        let batch = resp
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 3);
    }

    #[tokio::test]
    async fn find_respects_limit() {
        let (_dir, _obj_dir, registry, tenants, sessions) = setup_with_events().await;
        let resp = find_response(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "events",
            &doc! {},
            Some(2),
        )
        .unwrap();
        let batch = resp
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 2);
    }

    #[tokio::test]
    async fn find_unknown_tenant_returns_empty() {
        let (_dir, _obj_dir, registry, tenants, sessions) = setup_with_events().await;
        let resp = find_response(
            &registry,
            &tenants,
            &sessions,
            "nope",
            "events",
            &doc! {},
            None,
        )
        .unwrap();
        let batch = resp
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn find_on_sessions_collection_returns_session_records() {
        let (_dir, _obj_dir, registry, tenants, sessions) = setup_with_events().await;
        let resp = find_response(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "sessions",
            &doc! {},
            None,
        )
        .unwrap();
        let batch = resp
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 1);
        let rec = batch[0].as_document().unwrap();
        assert_eq!(rec.get_str("external_id").unwrap(), "s-1");
    }
}
