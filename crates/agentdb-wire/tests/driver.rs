use std::sync::Arc;

use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use agentdb_wire::{serve, WireState};
use bson::doc;
use mongodb::options::ClientOptions;
use mongodb::Client;
use tempfile::TempDir;

async fn spawn() -> (TempDir, u16) {
    let dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let registry = Arc::new(BoxRegistry::new(kv.clone(), 4).unwrap());
    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv));
    let state = WireState {
        registry,
        tenants,
        sessions,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = serve(listener, state).await;
    });
    (dir, port)
}

async fn client(port: u16) -> Client {
    let uri = format!("mongodb://127.0.0.1:{port}/?directConnection=true");
    let mut options = ClientOptions::parse(&uri).await.unwrap();
    // Avoid auth handshake
    options.credential = None;
    Client::with_options(options).unwrap()
}

#[tokio::test]
async fn hello_over_real_driver() {
    let (_dir, port) = spawn().await;
    let cli = client(port).await;
    let admin = cli.database("admin");
    let reply = admin.run_command(doc! { "ping": 1 }).await.unwrap();
    assert_eq!(reply.get_f64("ok").unwrap(), 1.0);
}

#[tokio::test]
async fn insert_then_find_round_trip() {
    let (_dir, port) = spawn().await;
    let cli = client(port).await;
    let db = cli.database("t-42");
    let events = db.collection::<bson::Document>("events");

    events
        .insert_one(doc! { "session_id": "s-1", "kind": "hello" })
        .await
        .unwrap();
    events
        .insert_one(doc! { "session_id": "s-1", "kind": "world" })
        .await
        .unwrap();

    let cursor = events.find(doc! { "session_id": "s-1" }).await.unwrap();
    use futures_util::StreamExt;
    let docs: Vec<bson::Document> = cursor.filter_map(|r| async move { r.ok() }).collect().await;
    assert_eq!(docs.len(), 2);
    let kinds: Vec<&str> = docs
        .iter()
        .map(|d| d.get_document("payload").unwrap().get_str("kind").unwrap())
        .collect();
    assert!(kinds.contains(&"hello"));
    assert!(kinds.contains(&"world"));
}

#[tokio::test]
async fn list_databases_sees_tenant_after_write() {
    let (_dir, port) = spawn().await;
    let cli = client(port).await;
    let events = cli.database("t-abc").collection::<bson::Document>("events");
    events
        .insert_one(doc! { "session_id": "s-1", "x": 1 })
        .await
        .unwrap();

    let dbs = cli.list_database_names().await.unwrap();
    assert!(dbs.contains(&"t-abc".to_string()));
}

#[tokio::test]
async fn text_search_aggregation_returns_hits() {
    let (_dir, port) = spawn().await;
    let cli = client(port).await;
    let db = cli.database("t-search");
    let events = db.collection::<bson::Document>("events");

    events
        .insert_one(doc! { "session_id": "s-1", "text": "refund policy" })
        .await
        .unwrap();
    events
        .insert_one(doc! { "session_id": "s-1", "text": "shipping update" })
        .await
        .unwrap();

    let pipeline = vec![doc! {
        "$search": {
            "text": { "query": "refund", "path": "payload.text" },
            "limit": 10_i64,
        }
    }];
    let cursor = events.aggregate(pipeline).await.unwrap();
    use futures_util::StreamExt;
    let docs: Vec<bson::Document> = cursor.filter_map(|r| async move { r.ok() }).collect().await;
    assert_eq!(docs.len(), 1);
    assert_eq!(
        docs[0]
            .get_document("payload")
            .unwrap()
            .get_str("text")
            .unwrap(),
        "refund policy"
    );
}

#[tokio::test]
async fn vector_search_aggregation_returns_top_hit() {
    let (_dir, port) = spawn().await;
    let cli = client(port).await;
    let db = cli.database("t-vector");
    let events = db.collection::<bson::Document>("events");

    events
        .insert_one(doc! { "session_id": "s-1", "embedding": [1.0, 0.0, 0.0] })
        .await
        .unwrap();
    events
        .insert_one(doc! { "session_id": "s-1", "embedding": [0.0, 1.0, 0.0] })
        .await
        .unwrap();

    let pipeline = vec![doc! {
        "$vectorSearch": {
            "queryVector": [1.0, 0.0, 0.0],
            "path": "payload.embedding",
            "limit": 5_i64,
            "numCandidates": 10_i64,
        }
    }];
    let cursor = events.aggregate(pipeline).await.unwrap();
    use futures_util::StreamExt;
    let docs: Vec<bson::Document> = cursor.filter_map(|r| async move { r.ok() }).collect().await;
    assert!(!docs.is_empty());
    let first_payload = docs[0].get_document("payload").unwrap();
    let emb = first_payload.get_array("embedding").unwrap();
    let as_vec: Vec<f64> = emb.iter().filter_map(|b| b.as_f64()).collect();
    assert_eq!(as_vec, vec![1.0, 0.0, 0.0]);
}
