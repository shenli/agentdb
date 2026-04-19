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
        let payload = bson_to_json(Bson::Document(payload_doc));

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        match agentdb_core::append_and_fetch(
            registry, tenants, sessions, db, session_id, payload, 0, now,
        )
        .await
        {
            Ok(_) => inserted += 1,
            Err(e) => write_errors.push(bson::Bson::Document(doc! {
                "index": idx as i32,
                "code": 11_i32, // UserNotFound / generic
                "errmsg": e.to_string(),
            })),
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
        Arc<BoxRegistry>,
        Arc<TenantRegistry>,
        Arc<SessionRegistry>,
    ) {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let registry = Arc::new(BoxRegistry::new(kv.clone(), 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        (dir, registry, tenants, sessions)
    }

    #[tokio::test]
    async fn insert_one_event_succeeds_and_n_is_one() {
        let (_d, r, t, s) = context();
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
        let (_d, r, t, s) = context();
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
    async fn insert_into_unknown_collection_errors_out() {
        let (_d, r, t, s) = context();
        let err = insert_response(&r, &t, &s, "t-1", "widgets", &[doc! {}])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("widgets"));
    }
}
