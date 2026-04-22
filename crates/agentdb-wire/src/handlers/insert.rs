use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use bson::{doc, Bson, Document};

use crate::bsonx::bson_to_json;
use crate::error::{WireError, WireResult};

pub async fn insert_response(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    db: &str,
    collection: &str,
    docs: &[Document],
) -> WireResult<Document> {
    registry.route_tenant(db).metrics.record_insert();
    if collection != "events" {
        return Err(WireError::Protocol(format!(
            "insert into {collection} not supported; only 'events' accepts writes"
        )));
    }

    let mut inserted = 0i32;
    let mut write_errors: Vec<Bson> = Vec::new();
    for (idx, d) in docs.iter().enumerate() {
        let Some(session_id) = d.get_str("session_id").ok() else {
            write_errors.push(bson::Bson::Document(doc! {
                "index": idx as i32,
                "code": 2_i32, // BadValue
                "errmsg": "document is missing required field 'session_id'",
            }));
            continue;
        };

        let mut payload_doc = d.clone();
        payload_doc.remove("session_id");
        // If the inserted doc has a top-level `actor_id` string, extract
        // it and lift it to `Event::actor_id`. Mirrors the REST surface's
        // explicit `actor_id` field on AppendAndFetchRequest. See
        // `docs/agent-db-v2.md` §2.3.
        let actor_id = payload_doc
            .remove("actor_id")
            .and_then(|b| b.as_str().map(str::to_string));
        let payload = bson_to_json(Bson::Document(payload_doc));

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        match agentdb_core::append_and_fetch_with_actor(
            registry, tenants, sessions, db, session_id, payload, actor_id, 0, now,
        )
        .await
        {
            Ok(_) => inserted += 1,
            Err(e) => {
                let code: i32 = match &e {
                    agentdb_core::Error::TooManyRequests { .. } => 82, // ExceededTimeLimit
                    _ => 11,                                           // generic
                };
                write_errors.push(bson::Bson::Document(doc! {
                    "index": idx as i32,
                    "code": code,
                    "errmsg": e.to_string(),
                }));
            }
        }
    }

    let mut resp = doc! {
        "n": inserted,
        "ok": 1.0,
    };
    if !write_errors.is_empty() {
        resp.insert("writeErrors", Bson::Array(write_errors));
    }
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_storage::{KvStore, RocksKvStore};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn context() -> (
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

    #[tokio::test]
    async fn insert_one_event_succeeds_and_n_is_one() {
        let (_d, _od, r, t, s) = context();
        let resp = insert_response(
            &r,
            &t,
            &s,
            "t-1",
            "events",
            &[doc! { "session_id": "s-1", "kind": "hello" }],
        )
        .await
        .unwrap();
        assert_eq!(resp.get_i32("n").unwrap(), 1);
        assert_eq!(resp.get_f64("ok").unwrap(), 1.0);
    }

    #[tokio::test]
    async fn insert_document_missing_session_id_reports_write_error() {
        let (_d, _od, r, t, s) = context();
        let resp = insert_response(
            &r,
            &t,
            &s,
            "t-1",
            "events",
            &[doc! { "kind": "no-session-id-here" }],
        )
        .await
        .unwrap();
        assert_eq!(resp.get_i32("n").unwrap(), 0);
        let errs = resp.get_array("writeErrors").unwrap();
        assert_eq!(errs.len(), 1);
        assert!(errs[0]
            .as_document()
            .unwrap()
            .get_str("errmsg")
            .unwrap()
            .contains("session_id"));
    }

    #[tokio::test]
    async fn insert_with_actor_id_records_it_on_the_event() {
        use agentdb_core::{fetch_last_n, ReadCtx};
        let (_d, _od, r, t, s) = context();
        insert_response(
            &r,
            &t,
            &s,
            "t-1",
            "events",
            &[doc! {
                "session_id": "s-agent",
                "actor_id": "agent-orchestrator",
                "tool": "search",
            }],
        )
        .await
        .unwrap();

        let box_ = r.route_tenant("t-1");
        let tenant = t.get_or_mint(box_.id, "t-1", 0).unwrap();
        let session = s
            .lookup(box_.id, tenant.internal_id, "s-agent")
            .unwrap()
            .unwrap();
        let ctx = ReadCtx::open(&box_, r.kv()).unwrap();
        let events = fetch_last_n(&ctx, &session, 10).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].actor_id.as_deref(),
            Some("agent-orchestrator"),
            "actor_id not lifted from inserted doc to Event.actor_id"
        );
        // And it must not also remain in the payload.
        assert!(
            events[0].payload.get("actor_id").is_none(),
            "actor_id leaked into payload: {:?}",
            events[0].payload
        );
    }

    #[tokio::test]
    async fn insert_into_unknown_collection_errors_out() {
        let (_d, _od, r, t, s) = context();
        let err = insert_response(&r, &t, &s, "t-1", "widgets", &[doc! {}])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("widgets"));
    }
}
