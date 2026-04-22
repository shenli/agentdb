use std::sync::Arc;

use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use agentdb_wire::{serve, WireState};
use bson::doc;
use mongodb::options::ClientOptions;
use mongodb::Client;
use object_store::ObjectStore;
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
    (dir, obj_dir, port)
}

async fn spawn_with_http() -> (TempDir, TempDir, u16, u16) {
    use object_store::local::LocalFileSystem;
    let kv_dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let registry = Arc::new(BoxRegistry::new(kv.clone(), store.clone(), 4).unwrap());
    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv));

    let mongo_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mongo_port = mongo_listener.local_addr().unwrap().port();
    let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_port = http_listener.local_addr().unwrap().port();

    let wire_state = WireState {
        registry: registry.clone(),
        tenants: tenants.clone(),
        sessions: sessions.clone(),
    };
    let http_state = agentdb_api::AppState {
        registry: registry.clone(),
        tenants: tenants.clone(),
        sessions: sessions.clone(),
        stream: agentdb_api::StreamBus::default(),
        http: agentdb_api::webhook::new_client(),
    };

    tokio::spawn(async move {
        let _ = serve(mongo_listener, wire_state).await;
    });
    let app = agentdb_api::router(http_state);
    tokio::spawn(async move {
        let _ = axum::serve(http_listener, app).await;
    });

    (kv_dir, obj_dir, mongo_port, http_port)
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
    let (_dir, _obj_dir, port) = spawn().await;
    let cli = client(port).await;
    let admin = cli.database("admin");
    let reply = admin.run_command(doc! { "ping": 1 }).await.unwrap();
    assert_eq!(reply.get_f64("ok").unwrap(), 1.0);
}

#[tokio::test]
async fn insert_then_find_round_trip() {
    let (_dir, _obj_dir, port) = spawn().await;
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
    let (_dir, _obj_dir, port) = spawn().await;
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
    let (_dir, _obj_dir, port) = spawn().await;
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
    let (_dir, _obj_dir, port) = spawn().await;
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

#[tokio::test]
async fn cold_session_is_still_queryable_via_search() {
    let (_kv, _obj, port, http_port) = spawn_with_http().await;
    let cli = client(port).await;
    let db = cli.database("t-cold");
    let events = db.collection::<bson::Document>("events");

    events
        .insert_one(doc! { "session_id": "s-1", "text": "refund policy" })
        .await
        .unwrap();

    // Force the session cold via HTTP admin.
    let rest = reqwest::Client::new();
    let resp: serde_json::Value = rest
        .post(format!("http://127.0.0.1:{http_port}/api/admin/tier/force"))
        .json(&serde_json::json!({
            "tenant_id": "t-cold",
            "session_id": "s-1",
            "target": "Cold"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["tier"], "cold");

    // $search should still find the event via index + cold resolve fallback.
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

    // Reactivate.
    let warm: serde_json::Value = rest
        .post(format!("http://127.0.0.1:{http_port}/api/admin/tier/force"))
        .json(&serde_json::json!({
            "tenant_id": "t-cold",
            "session_id": "s-1",
            "target": "Warm"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(warm["tier"], "warm");
}

#[tokio::test]
async fn fork_child_merges_parent_tail_via_http_append_and_fetch() {
    let (_kv, _obj, mongo_port, http_port) = spawn_with_http().await;
    let cli = client(mongo_port).await;
    let db = cli.database("t-fork");
    let events = db.collection::<bson::Document>("events");

    // Seed the parent with 2 events via the driver.
    events
        .insert_one(doc! { "session_id": "parent", "i": 1 })
        .await
        .unwrap();
    events
        .insert_one(doc! { "session_id": "parent", "i": 2 })
        .await
        .unwrap();

    // Fork via REST.
    let rest = reqwest::Client::new();
    let fork: serde_json::Value = rest
        .post(format!(
            "http://127.0.0.1:{http_port}/api/sessions/parent/fork"
        ))
        .json(&serde_json::json!({
            "tenant_id": "t-fork",
            "new_session_id": "child"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(fork["parent_seq_at_fork"], 3);

    // Child appends, requests fetch_last_n=5 via HTTP REST (the mongodb driver
    // doesn't invoke append_and_fetch; we use the REST endpoint directly).
    let resp: serde_json::Value = rest
        .post(format!(
            "http://127.0.0.1:{http_port}/api/tenants/t-fork/sessions/child/append_and_fetch"
        ))
        .json(&serde_json::json!({"event": {"c": 1}, "fetch_last_n": 5}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["recent"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn transaction_wire_commands_are_rejected_with_not_implemented() {
    // Multi-doc transactions are not supported in M6. The server responds
    // with ok:0 so the driver surfaces the limitation as a client error
    // rather than silently accepting a fake transaction that provides no
    // atomicity. A real application should detect this error and fall back
    // to single-doc writes rather than trusting cross-op atomicity.
    let (_dir, _obj, port) = spawn().await;
    let cli = client(port).await;
    let admin = cli.database("admin");
    let result = admin
        .run_command(doc! {
            "startTransaction": 1,
            "lsid": { "id": bson::oid::ObjectId::new() },
            "txnNumber": 1_i64,
            "autocommit": false,
        })
        .await;
    assert!(result.is_err(), "startTransaction should be rejected");
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("not implemented") || msg.contains("CommandNotFound"),
        "error should indicate command is not supported; got: {msg}"
    );
}

#[tokio::test]
async fn cluster_time_is_returned_and_advances_per_command() {
    // M14 Phase E: every response carries $clusterTime. Successive
    // commands should see monotonically non-decreasing cluster times.
    let (_dir, _obj_dir, port) = spawn().await;
    let cli = client(port).await;
    let admin = cli.database("admin");

    let r1 = admin.run_command(doc! { "ping": 1 }).await.unwrap();
    let ct1 = r1
        .get_document("$clusterTime")
        .expect("response must carry $clusterTime");
    let ts1 = ct1
        .get_timestamp("clusterTime")
        .expect("$clusterTime.clusterTime must be a Timestamp");
    let sig1 = ct1
        .get_document("signature")
        .expect("$clusterTime must include signature");
    assert_eq!(
        sig1.get_i64("keyId").unwrap(),
        0,
        "v1 ships an unsigned cluster time"
    );
    assert!(
        sig1.get_binary_generic("hash").is_ok(),
        "signature.hash must be BinData even if empty"
    );

    // Next command — cluster time must not go backward.
    let r2 = admin.run_command(doc! { "ping": 1 }).await.unwrap();
    let ts2 = r2
        .get_document("$clusterTime")
        .unwrap()
        .get_timestamp("clusterTime")
        .unwrap();
    assert!(
        (ts2.time, ts2.increment) >= (ts1.time, ts1.increment),
        "cluster time regressed: {:?} -> {:?}",
        ts1,
        ts2
    );
}
