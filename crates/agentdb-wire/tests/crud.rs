//! Real mongo-driver tests for the append-on-update /
//! tombstone-on-delete semantics.

use std::sync::Arc;

use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use agentdb_wire::{serve, WireState};
use bson::doc;
use mongodb::options::ClientOptions;
use mongodb::Client;
use tempfile::TempDir;

async fn spawn() -> (TempDir, TempDir, u16) {
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
    let state = WireState::new(registry, tenants, sessions);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = serve(listener, state).await;
    });
    (dir, obj_dir, port)
}

async fn client(port: u16) -> Client {
    let uri = format!("mongodb://127.0.0.1:{port}/?directConnection=true");
    let mut options = ClientOptions::parse(&uri).await.unwrap();
    options.credential = None;
    Client::with_options(options).unwrap()
}

#[tokio::test]
async fn update_one_appends_a_correction_event() {
    let (_d, _o, port) = spawn().await;
    let cli = client(port).await;
    let db = cli.database("t-1");
    let events = db.collection::<bson::Document>("events");

    // Seed an event with a mutable-looking `status` field.
    events
        .insert_one(doc! { "session_id": "s-1", "status": "pending", "order_id": 42 })
        .await
        .unwrap();

    // Mongo updateOne: bump status.
    let res = events
        .update_one(
            doc! { "session_id": "s-1", "payload.status": "pending" },
            doc! { "$set": { "status": "shipped" } },
        )
        .await
        .unwrap();
    assert_eq!(res.matched_count, 1);
    assert_eq!(res.modified_count, 1);

    // Now the log has two events: the original + the
    // correction. The correction carries the merged payload
    // AND a `_update_of` marker pointing at the original seq.
    use futures_util::StreamExt;
    let cursor = events.find(doc! { "session_id": "s-1" }).await.unwrap();
    let docs: Vec<bson::Document> = cursor.filter_map(|r| async move { r.ok() }).collect().await;
    assert_eq!(docs.len(), 2, "expected original + correction: {docs:?}");

    let correction = docs
        .iter()
        .find(|d| {
            d.get_document("payload")
                .map(|p| p.get_i64("_update_of").is_ok() || p.get_i32("_update_of").is_ok())
                .unwrap_or(false)
        })
        .expect("no event carries `_update_of`");
    let payload = correction.get_document("payload").unwrap();
    assert_eq!(payload.get_str("status").unwrap(), "shipped");
    // The merged payload keeps the order_id from the original.
    assert!(
        payload.get_i64("order_id").is_ok() || payload.get_i32("order_id").is_ok(),
        "correction should carry merged fields: {payload:?}"
    );
}

#[tokio::test]
async fn update_many_touches_every_match_in_the_session() {
    let (_d, _o, port) = spawn().await;
    let cli = client(port).await;
    let events = cli.database("t-many").collection::<bson::Document>("events");
    for i in 0..3 {
        events
            .insert_one(doc! { "session_id": "s-1", "n": i, "done": false })
            .await
            .unwrap();
    }
    let res = events
        .update_many(
            doc! { "session_id": "s-1", "payload.done": false },
            doc! { "$set": { "done": true } },
        )
        .await
        .unwrap();
    assert_eq!(res.matched_count, 3);
    assert_eq!(res.modified_count, 3);
}

#[tokio::test]
async fn update_without_session_id_filter_is_rejected() {
    let (_d, _o, port) = spawn().await;
    let cli = client(port).await;
    let events = cli
        .database("t-unscoped")
        .collection::<bson::Document>("events");
    events
        .insert_one(doc! { "session_id": "s-1", "status": "x" })
        .await
        .unwrap();

    // No session_id in the filter — the handler must refuse.
    let err = events
        .update_one(
            doc! { "payload.status": "x" },
            doc! { "$set": { "status": "y" } },
        )
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.to_lowercase().contains("session_id"),
        "rejection should mention session_id; got: {msg}",
    );
}

#[tokio::test]
async fn update_replacement_doc_is_rejected() {
    let (_d, _o, port) = spawn().await;
    let cli = client(port).await;
    let events = cli
        .database("t-replace")
        .collection::<bson::Document>("events");
    events
        .insert_one(doc! { "session_id": "s-1", "status": "x" })
        .await
        .unwrap();
    // A replacement doc (no $-operator) is sent via
    // `replace_one`. The handler must refuse: replacing a
    // whole event doesn't map onto append-on-update.
    let err = events
        .replace_one(
            doc! { "session_id": "s-1" },
            doc! { "session_id": "s-1", "something": "else" },
        )
        .await
        .unwrap_err();
    let msg = format!("{err:#}").to_lowercase();
    assert!(
        msg.contains("replacement") || msg.contains("$set"),
        "expected rejection mentioning replacement updates; got: {msg}"
    );
}

#[tokio::test]
async fn delete_one_appends_a_tombstone() {
    let (_d, _o, port) = spawn().await;
    let cli = client(port).await;
    let events = cli
        .database("t-del")
        .collection::<bson::Document>("events");
    events
        .insert_one(doc! { "session_id": "s-1", "kind": "keep" })
        .await
        .unwrap();
    events
        .insert_one(doc! { "session_id": "s-1", "kind": "drop" })
        .await
        .unwrap();

    let res = events
        .delete_one(doc! { "session_id": "s-1", "payload.kind": "drop" })
        .await
        .unwrap();
    assert_eq!(res.deleted_count, 1);

    use futures_util::StreamExt;
    let cursor = events.find(doc! { "session_id": "s-1" }).await.unwrap();
    let docs: Vec<bson::Document> = cursor.filter_map(|r| async move { r.ok() }).collect().await;
    assert_eq!(docs.len(), 3, "original 2 + tombstone 1: {docs:?}");
    let has_tombstone = docs.iter().any(|d| {
        d.get_document("payload")
            .map(|p| {
                p.get_i64("_tombstone_of").is_ok() || p.get_i32("_tombstone_of").is_ok()
            })
            .unwrap_or(false)
    });
    assert!(has_tombstone, "no tombstone event found: {docs:?}");
}

#[tokio::test]
async fn delete_many_tombstones_all_matches() {
    let (_d, _o, port) = spawn().await;
    let cli = client(port).await;
    let events = cli
        .database("t-delmany")
        .collection::<bson::Document>("events");
    for _ in 0..3 {
        events
            .insert_one(doc! { "session_id": "s-1", "kind": "drop" })
            .await
            .unwrap();
    }
    let res = events
        .delete_many(doc! { "session_id": "s-1", "payload.kind": "drop" })
        .await
        .unwrap();
    assert_eq!(res.deleted_count, 3);
}

#[tokio::test]
async fn delete_on_unknown_session_returns_zero() {
    let (_d, _o, port) = spawn().await;
    let cli = client(port).await;
    let events = cli
        .database("t-empty")
        .collection::<bson::Document>("events");
    // Tenant exists (mint by inserting into another session);
    // session `s-missing` does not.
    events
        .insert_one(doc! { "session_id": "s-existing", "x": 1 })
        .await
        .unwrap();
    let res = events
        .delete_many(doc! { "session_id": "s-missing", "payload.x": 1 })
        .await
        .unwrap();
    assert_eq!(res.deleted_count, 0);
}
