use agentdb_core::{
    scan_session_all_tiers, BoxRegistry, Event, ReadCtx, SessionRegistry, TenantRegistry,
};
use bson::{doc, Bson, Document};
use serde_json::Value;

use crate::bsonx::json_to_bson;
use crate::error::{WireError, WireResult};
use crate::handlers::filter::Filter;

pub async fn find_response(
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
    let tenant_opt = tenants.lookup(box_.id, db)?;
    // Admit unconditionally: unknown tenants still pay the per-box gate
    // via sentinel 0 (skips the per-tenant gate). Prevents spam against
    // random db names from bypassing rate limits.
    let admit_id = tenant_opt.as_ref().map(|t| t.internal_id).unwrap_or(0);
    box_.admit(admit_id)
        .map_err(|d| agentdb_core::Error::TooManyRequests {
            scope: d.scope,
            retry_after_ms: 1000,
        })?;
    let tenant = match tenant_opt {
        Some(t) => t,
        None => return Ok(empty_batch(db, collection)),
    };
    let filter = Filter::parse(filter_doc)?;
    let ctx = ReadCtx::open(&box_, registry.kv())?;
    let store = registry.object_store();
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
                // M17: tier-aware scan. Warm goes through the KV
                // snapshot; cold/archive read through the segment with
                // per-tier hedge delay.
                for ev in scan_session_all_tiers(&ctx, store, &session).await? {
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
        .await
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
        .await
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
        .await
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
        .await
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
        .await
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

    #[tokio::test]
    async fn find_events_on_cold_session_returns_segment_contents() {
        // M17 regression: before this milestone, find over `events` on
        // a cold session returned empty because scan_session_events
        // only walked KV (already evicted on tier transition).
        let (_dir, _obj_dir, registry, tenants, sessions) = setup_with_events().await;
        let box_ = registry.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let mut session = sessions
            .lookup(box_.id, tenant.internal_id, "s-1")
            .unwrap()
            .unwrap();
        {
            let guard = box_.tenant_write_lock("t-1").lock_owned().await;
            agentdb_core::cold_transition(
                registry.kv(),
                registry.object_store(),
                &sessions,
                &guard,
                &mut session,
            )
            .await
            .unwrap();
        }
        assert_eq!(session.tier, agentdb_core::Tier::Cold);

        let resp = find_response(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "events",
            &doc! {},
            None,
        )
        .await
        .unwrap();
        let batch = resp
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(
            batch.len(),
            3,
            "cold session should surface 3 events via segment read"
        );
    }

    #[tokio::test]
    async fn find_events_on_archive_session_returns_segment_contents() {
        // M17 core regression: find on an archived session returned
        // empty pre-M17 because `Tier::Archive => Vec::new()`. Now it
        // must return the same events as the cold case.
        let (_dir, _obj_dir, registry, tenants, sessions) = setup_with_events().await;
        let box_ = registry.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let mut session = sessions
            .lookup(box_.id, tenant.internal_id, "s-1")
            .unwrap()
            .unwrap();
        // Cold then flip to Archive (metadata-only in this prototype).
        {
            let guard = box_.tenant_write_lock("t-1").lock_owned().await;
            agentdb_core::cold_transition(
                registry.kv(),
                registry.object_store(),
                &sessions,
                &guard,
                &mut session,
            )
            .await
            .unwrap();
        }
        session.tier = agentdb_core::Tier::Archive;
        sessions.save(&session).unwrap();

        let resp = find_response(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "events",
            &doc! {},
            None,
        )
        .await
        .unwrap();
        let batch = resp
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(
            batch.len(),
            3,
            "archive session should surface 3 events via unified segment read (M17)"
        );
    }
}
