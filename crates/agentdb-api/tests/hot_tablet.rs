//! M25 hot-tablet end-to-end: burst writes to one session,
//! verify the detector flags that session's tablet, then
//! exercise the shed endpoint and confirm new writes split
//! across tablets.
//!
//! Unlike `multi_node.rs`, this spawns a single node — the
//! detector logic is per-node and doesn't require a peer.

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

/// Helper: force the box's admission semaphore down to 1 so
/// even a modest burst generates admission waits (which is
/// what the detector keys off). Returns the spawned server's
/// base URL.
async fn spawn_saturable_node() -> (
    TempDir,
    TempDir,
    tempfile::NamedTempFile,
    String,
    Arc<BoxRegistry>,
    Arc<HotTabletDetector>,
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
    // Admission capacity 1: any concurrent second request
    // waits — guaranteed to generate admission-wait signal.
    let registry = Arc::new(
        BoxRegistry::new_with_capacity(
            kv.clone(),
            store,
            4,
            Arc::new(agentdb_core::HlcClock::default()),
            1,
        )
        .unwrap(),
    );
    let location = LocationProxy::new(meta.clone()).await.unwrap();
    registry.attach_location(location.clone()).ok();

    let election = LeaderElection::start(
        meta.clone(),
        "hot-tablet-test".into(),
        LeaderElectionConfig {
            lease_duration: Duration::from_secs(10),
            renew_interval: Duration::from_millis(100),
            attempt_interval: Duration::from_millis(50),
        },
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(election.is_leader());

    // Run the detector with tight knobs so the test finishes
    // in a reasonable wall-clock window.
    let detector = HotTabletDetector::start(
        registry.clone(),
        election,
        HotTabletConfig {
            tick: Duration::from_millis(100),
            wait_ratio_threshold: 0.1, // low threshold to flip fast
            consecutive_samples: 2,
            min_appends_per_sec: 0.0, // disable APS floor
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
        .layer(Extension(detector.clone()))
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
        detector,
        location,
    )
}

#[tokio::test]
async fn detector_flags_tablet_after_burst_and_shed_moves_shards() {
    let (_d, _o, _m, base, registry, detector, location) = spawn_saturable_node().await;

    // Pick a session and pin it to tablet 2. Burst writes to
    // that session will saturate box 2.
    let session = "s-burst";
    let shard_id = agentdb_core::shard_id(session) as i64;
    // Use a helper to poke the meta store directly — we
    // didn't keep a MetaStore ref around above; instead, use
    // the LocationProxy's refresh path: point shard_id at
    // tablet 2 via the admin endpoint.
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/admin/shards/{shard_id}"))
        .json(&json!({ "tablet_id": 2 }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "set_shard failed");
    location.refresh().await.unwrap();

    // Make one write via HTTP so the path is exercised end-
    // to-end (validates routing + handler + metrics wiring).
    let url = format!("{base}/api/tenants/t-1/sessions/{session}/append_and_fetch");
    let resp = client
        .post(&url)
        .json(&json!({"event": {"warmup": true}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    // Prime the rate window directly for the target box.
    // Triggering real admission-wait via HTTP is flaky in-
    // process because reqwest's connection pool plus shared
    // tokio runtime rarely produces true concurrency — the
    // detector logic is what matters here, and unit tests
    // already prove the rate window rolls correctly under
    // real concurrency. Simulating saturation via direct
    // calls keeps this integration test deterministic.
    let hot_box = registry.box_by_id(2).unwrap();
    for _ in 0..200 {
        hot_box.metrics.record_append();
        hot_box.metrics.record_admission_wait();
    }

    // Give the detector time for 3+ ticks (100ms each) so
    // the `consecutive_samples: 2` hysteresis is satisfied.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Hot list must include tablet 2.
    let resp = client
        .get(format!("{base}/api/admin/tablets/hot"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let hot = body["hot"].as_array().expect("hot array");
    assert!(
        hot.iter().any(|h| h["tablet_id"].as_i64() == Some(2)),
        "tablet 2 should be flagged hot; detector saw: {body}"
    );

    // Use the admin /api/admin/shards/list endpoint to
    // confirm our shard is actually on tablet 2 before shed.
    let overrides = client
        .get(format!("{base}/api/admin/shards/list"))
        .send()
        .await
        .unwrap();
    let overrides_body: serde_json::Value = overrides.json().await.unwrap();
    let shards_on_2: Vec<i64> = overrides_body["overrides"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter(|o| o["tablet_id"].as_i64() == Some(2))
        .filter_map(|o| o["shard_id"].as_i64())
        .collect();
    assert_eq!(
        shards_on_2,
        vec![shard_id],
        "pre-shed: only our burst shard should be on tablet 2"
    );

    // POST shed with to=3 (explicit destination) so we don't
    // depend on detector's "least-loaded" pick timing.
    let shed = client
        .post(format!("{base}/api/admin/tablets/2/shed"))
        .json(&json!({"to_tablet_id": 3, "fraction": 1.0}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        shed.status().as_u16(),
        200,
        "shed failed: {}",
        shed.text().await.unwrap()
    );
    let shed_body: serde_json::Value = shed.json().await.unwrap();
    assert_eq!(shed_body["to_tablet_id"].as_i64(), Some(3));
    let moved = shed_body["moved_shards"].as_array().expect("moved_shards");
    assert_eq!(moved.len(), 1);
    assert_eq!(moved[0].as_i64(), Some(shard_id));

    // After shed + proxy refresh, new writes route to tablet 3.
    let _ = detector.hot_tablets(); // silence unused warning if any
    location.refresh().await.unwrap();
    let appends_before_3 = registry.box_by_id(3).unwrap().metrics.snapshot().appends;
    let resp = client
        .post(&url)
        .json(&json!({"event": {"after": "shed"}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let appends_after_3 = registry.box_by_id(3).unwrap().metrics.snapshot().appends;
    assert_eq!(
        appends_after_3 - appends_before_3,
        1,
        "post-shed write should land on tablet 3"
    );
}

#[tokio::test]
async fn shed_with_migrate_data_moves_events_atomically() {
    let (_d, _o, _m, base, registry, _detector, location) = spawn_saturable_node().await;

    // Write 8 events on the default tenant-hash box.
    let client = reqwest::Client::new();
    let session = "s-shed-migrate";
    let url = format!("{base}/api/tenants/t-1/sessions/{session}/append_and_fetch");
    for i in 1..=8 {
        let r = client
            .post(&url)
            .json(&json!({"event": {"i": i}, "fetch_last_n": 0}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status().as_u16(), 200);
    }
    let from_box = registry.route_tenant("t-1").id;
    let to_box = (from_box + 1) % 4;

    // Set an override so the session's shard is on from_box
    // (needed so shed sees it in list_by_tablet). Pin the
    // shard to from_box explicitly.
    let shard_id = agentdb_core::shard_id(session) as i64;
    let r = client
        .post(format!("{base}/api/admin/shards/{shard_id}"))
        .json(&json!({ "tablet_id": from_box }))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    location.refresh().await.unwrap();

    // Shed the one shard off from_box → to_box with
    // migrate_data=true. The response should list our
    // session as migrated.
    let r = client
        .post(format!("{base}/api/admin/tablets/{from_box}/shed"))
        .json(&json!({"to_tablet_id": to_box, "fraction": 1.0, "migrate_data": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status().as_u16(),
        200,
        "shed w/ migrate failed: {}",
        r.text().await.unwrap()
    );
    let body: serde_json::Value = r.json().await.unwrap();
    let migrated = body["migrated_sessions"].as_array().unwrap();
    assert!(
        migrated.iter().any(|m| m["session_id"].as_str() == Some(session)),
        "shed should have migrated {session}; got {body}"
    );
    let m = migrated
        .iter()
        .find(|m| m["session_id"].as_str() == Some(session))
        .unwrap();
    assert_eq!(m["events_migrated"].as_u64(), Some(8));

    // One more write — lands on the new box and the full
    // history is visible.
    let r = client
        .post(&url)
        .json(&json!({"event": {"i": 9}, "fetch_last_n": 30}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["recent"].as_array().unwrap().len(), 9);
}
