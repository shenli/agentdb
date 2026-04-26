//! M25 dynamic-tablet end-to-end: create a new tablet at
//! runtime via REST, verify it's routable, and verify the
//! shed → new-tablet chain works without restarting the
//! server.

use std::sync::Arc;
use std::time::Duration;

use agentdb_api::{router, AppState, StreamBus};
use agentdb_core::{
    BoxRegistry, HotTabletConfig, HotTabletDetector, LeaderElection, LeaderElectionConfig,
    LocationProxy, SessionRegistry, TenantRegistry,
};
use agentdb_meta::{shards as meta_shards, MetaStore};
use agentdb_storage::{KvStore, RocksKvStore};
use axum::Extension;
use serde_json::json;
use tempfile::TempDir;

/// Bring up a meta-backed single-node server so `add_box` can
/// acquire leases. Starts with num_boxes=2. Returns the base
/// URL + registry handle (for metric assertions) + the meta
/// store (to set shard overrides directly).
async fn spawn_dynamic_node() -> (
    TempDir,
    TempDir,
    tempfile::NamedTempFile,
    String,
    Arc<BoxRegistry>,
    Arc<MetaStore>,
    Arc<LocationProxy>,
) {
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    let data = TempDir::new().unwrap();
    let obj = TempDir::new().unwrap();
    let meta_file = tempfile::NamedTempFile::new().unwrap();
    let meta_path = meta_file.path().to_string_lossy().to_string();
    let meta = Arc::new(MetaStore::open_sqlite(&meta_path).await.unwrap());
    meta_shards::seed_shards(meta.pool(), agentdb_core::SHARD_COUNT as i64, 0)
        .await
        .unwrap();

    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(data.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj.path()).unwrap());
    let registry = Arc::new(
        BoxRegistry::new_with_meta(
            kv.clone(),
            store,
            2,
            Arc::new(agentdb_core::HlcClock::default()),
            meta.clone(),
            "dyn-test-node".into(),
        )
        .await
        .unwrap(),
    );
    let location = LocationProxy::new(meta.clone()).await.unwrap();
    registry.attach_location(location.clone()).ok();

    let election = LeaderElection::start(
        meta.clone(),
        "dyn-test-node".into(),
        LeaderElectionConfig {
            lease_duration: Duration::from_secs(10),
            renew_interval: Duration::from_millis(100),
            attempt_interval: Duration::from_millis(50),
        },
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(election.is_leader());

    let detector = HotTabletDetector::start(
        registry.clone(),
        election,
        HotTabletConfig {
            tick: Duration::from_millis(100),
            wait_ratio_threshold: 0.3,
            consecutive_samples: 2,
            min_appends_per_sec: 0.0,
        },
    );

    let state = AppState {
        registry: registry.clone(),
        tenants: Arc::new(TenantRegistry::new(kv.clone())),
        sessions: Arc::new(SessionRegistry::new(kv)),
        stream: StreamBus::default(),
        event_bus: agentdb_api::EventBus::default(),
        http: agentdb_api::webhook::new_client(),
        auth: None,
        admin_auth: None,
    };
    let app = router(state)
        .layer(Extension(meta.clone()))
        .layer(Extension(detector))
        .layer(Extension(location.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (
        data,
        obj,
        meta_file,
        format!("http://{addr}"),
        registry,
        meta,
        location,
    )
}

#[tokio::test]
async fn create_tablet_grows_the_pool_and_new_tablet_is_routable() {
    let (_d, _o, _m, base, registry, meta, location) = spawn_dynamic_node().await;
    assert_eq!(registry.num_boxes(), 2, "start with 2 boxes");

    // Create a new tablet via the admin endpoint. Expect
    // id=2 (max existing is 1, so next is 2).
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/admin/tablets/create"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "create failed: {}",
        resp.text().await.unwrap()
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["tablet_id"].as_i64(), Some(2));
    assert_eq!(body["owner_node_id"].as_str(), Some("dyn-test-node"));

    // Registry now has 3 boxes.
    assert_eq!(registry.num_boxes(), 3);
    // The new box is reachable via box_by_id.
    let new_box = registry.box_by_id(2).expect("box 2 must exist");
    assert_eq!(new_box.id, 2);

    // /api/admin/tablets lists it.
    let resp = client
        .get(format!("{base}/api/admin/tablets"))
        .send()
        .await
        .unwrap();
    let rows: serde_json::Value = resp.json().await.unwrap();
    let ids: Vec<i64> = rows
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|r| r["tablet_id"].as_i64())
        .collect();
    assert!(ids.contains(&2), "admin/tablets must include id 2");

    // Route a session to the new tablet and verify the
    // write lands on box 2.
    let session = "s-on-new-tablet";
    let shard_id = agentdb_core::shard_id(session) as i64;
    meta_shards::set_assignment(meta.pool(), shard_id, Some(2), 0)
        .await
        .unwrap();
    location.refresh().await.unwrap();

    let before = new_box.metrics.snapshot().appends;
    let url = format!("{base}/api/tenants/t-1/sessions/{session}/append_and_fetch");
    let resp = client
        .post(&url)
        .json(&json!({"event": {"text": "on-new"}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let after = new_box.metrics.snapshot().appends;
    assert_eq!(
        after - before,
        1,
        "write should have landed on the newly-created tablet's box"
    );
}

#[tokio::test]
async fn create_tablet_with_explicit_id_and_duplicate_errors() {
    let (_d, _o, _m, base, _reg, _meta, _loc) = spawn_dynamic_node().await;
    let client = reqwest::Client::new();

    // Explicit id=5 succeeds.
    let resp = client
        .post(format!("{base}/api/admin/tablets/create"))
        .json(&json!({"tablet_id": 5}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["tablet_id"].as_i64(), Some(5));

    // Repeat with the same id → 409 Conflict.
    let resp = client
        .post(format!("{base}/api/admin/tablets/create"))
        .json(&json!({"tablet_id": 5}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        409,
        "duplicate create must be 409 Conflict, got {}",
        resp.status()
    );
}
