//! Materialized read view: `find` default-folds correction +
//! tombstone events into a latest-wins view, so clients that
//! wrote via `update` / `delete` see the expected result.
//!
//! Opt out with `{raw: true}` in the find command body.

use std::sync::Arc;

use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use agentdb_wire::{serve, WireState};
use bson::doc;
use futures_util::StreamExt;
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
async fn find_after_update_sees_the_correction_folded_in() {
    let (_d, _o, port) = spawn().await;
    let cli = client(port).await;
    let events = cli.database("t-1").collection::<bson::Document>("events");

    events
        .insert_one(doc! { "session_id": "s-1", "status": "pending", "order": 42 })
        .await
        .unwrap();
    events
        .update_one(
            doc! { "session_id": "s-1", "payload.status": "pending" },
            doc! { "$set": { "status": "shipped" } },
        )
        .await
        .unwrap();

    // Default folded view: exactly one event, with the
    // post-update payload.
    let cursor = events.find(doc! { "session_id": "s-1" }).await.unwrap();
    let docs: Vec<bson::Document> = cursor.filter_map(|r| async move { r.ok() }).collect().await;
    assert_eq!(docs.len(), 1, "folded view should have one event: {docs:?}");
    let payload = docs[0].get_document("payload").unwrap();
    assert_eq!(payload.get_str("status").unwrap(), "shipped");
    // The `_update_of` marker is stripped from the folded view.
    assert!(payload.get("_update_of").is_none());
}

#[tokio::test]
async fn find_after_delete_hides_the_event() {
    let (_d, _o, port) = spawn().await;
    let cli = client(port).await;
    let events = cli.database("t-del").collection::<bson::Document>("events");

    events
        .insert_one(doc! { "session_id": "s-1", "kind": "keep" })
        .await
        .unwrap();
    events
        .insert_one(doc! { "session_id": "s-1", "kind": "drop" })
        .await
        .unwrap();
    events
        .delete_one(doc! { "session_id": "s-1", "payload.kind": "drop" })
        .await
        .unwrap();

    let cursor = events.find(doc! { "session_id": "s-1" }).await.unwrap();
    let docs: Vec<bson::Document> = cursor.filter_map(|r| async move { r.ok() }).collect().await;
    assert_eq!(docs.len(), 1, "only the non-dropped event remains: {docs:?}");
    let payload = docs[0].get_document("payload").unwrap();
    assert_eq!(payload.get_str("kind").unwrap(), "keep");
}

#[tokio::test]
async fn find_raw_true_exposes_corrections_and_tombstones() {
    // Use a raw `runCommand` to pass the non-standard `raw`
    // flag; the driver's typed `find()` doesn't expose it.
    let (_d, _o, port) = spawn().await;
    let cli = client(port).await;
    let events = cli
        .database("t-raw")
        .collection::<bson::Document>("events");

    events
        .insert_one(doc! { "session_id": "s-1", "status": "pending" })
        .await
        .unwrap();
    events
        .update_one(
            doc! { "session_id": "s-1", "payload.status": "pending" },
            doc! { "$set": { "status": "done" } },
        )
        .await
        .unwrap();

    let db = cli.database("t-raw");
    let reply = db
        .run_command(doc! {
            "find": "events",
            "filter": { "session_id": "s-1" },
            "raw": true,
        })
        .await
        .unwrap();
    let batch = reply
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 2, "raw mode should return original + correction: {batch:?}");
    let has_marker = batch.iter().any(|b| {
        b.as_document()
            .and_then(|d| d.get_document("payload").ok())
            .map(|p| p.get("_update_of").is_some())
            .unwrap_or(false)
    });
    assert!(has_marker, "raw view must expose the _update_of marker");
}

#[tokio::test]
async fn update_then_delete_hides_everything() {
    let (_d, _o, port) = spawn().await;
    let cli = client(port).await;
    let events = cli
        .database("t-chain")
        .collection::<bson::Document>("events");

    events
        .insert_one(doc! { "session_id": "s-1", "v": 0 })
        .await
        .unwrap();
    events
        .update_one(
            doc! { "session_id": "s-1", "payload.v": 0 },
            doc! { "$set": { "v": 1 } },
        )
        .await
        .unwrap();
    // Delete by matching v: 0 — the filter sees the folded
    // view, so it matches the folded (v=1) event only if the
    // caller used the post-update filter. Let's use v: 1.
    events
        .delete_one(doc! { "session_id": "s-1", "payload.v": 1 })
        .await
        .unwrap();

    let cursor = events.find(doc! { "session_id": "s-1" }).await.unwrap();
    let docs: Vec<bson::Document> = cursor.filter_map(|r| async move { r.ok() }).collect().await;
    assert!(
        docs.is_empty(),
        "after update + delete the event is gone: {docs:?}"
    );
}
