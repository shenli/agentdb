//! Auto-shed scheduler end-to-end: fires shed-with-migrate
//! against a hot tablet once the detector flags it, and
//! respects the per-tablet cooldown on the same source.

use std::sync::Arc;
use std::time::Duration;

use agentdb_api::{auto_shed::AutoShedConfig, router, AppState, StreamBus};
use agentdb_core::{
    BoxRegistry, HotTabletConfig, HotTabletDetector, LeaderElection, LeaderElectionConfig,
    LocationProxy, SessionRegistry, TenantRegistry,
};
use agentdb_meta::{shards as meta_shards, MetaStore};
use agentdb_storage::{KvStore, RocksKvStore};
use axum::Extension;
use serde_json::json;
use tempfile::TempDir;

async fn spawn_with_autoshed() -> (
    TempDir,
    TempDir,
    tempfile::NamedTempFile,
    String,
    Arc<BoxRegistry>,
    Arc<HotTabletDetector>,
    Arc<LocationProxy>,
    Arc<agentdb_api::auto_shed::AutoShedJob>,
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
        BoxRegistry::new_with_capacity(
            kv.clone(),
            store,
            4,
            Arc::new(agentdb_core::HlcClock::default()),
            1, // admission capacity = 1 so bursts produce waits
        )
        .unwrap(),
    );
    let location = LocationProxy::new(meta.clone()).await.unwrap();
    registry.attach_location(location.clone()).ok();

    let election = LeaderElection::start(
        meta.clone(),
        "autoshed-test".into(),
        LeaderElectionConfig {
            lease_duration: Duration::from_secs(10),
            renew_interval: Duration::from_millis(100),
            attempt_interval: Duration::from_millis(50),
        },
    );
    tokio::time::sleep(Duration::from_millis(200)).await;

    let detector = HotTabletDetector::start(
        registry.clone(),
        election,
        HotTabletConfig {
            tick: Duration::from_millis(100),
            wait_ratio_threshold: 0.1,
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
        jwt_config: agentdb_api::jwt::JwtConfig::default(),
        cors_config: agentdb_api::cors::CorsConfig::default(),
        broadcast_bus: agentdb_api::BroadcastBus::default(),
    };
    let app = router(state.clone())
        .layer(Extension(meta.clone()))
        .layer(Extension(detector.clone()))
        .layer(Extension(location.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Start the auto-shed scheduler with tiny timings for
    // fast test signal.
    let job = agentdb_api::auto_shed::AutoShedJob::start(
        state,
        detector.clone(),
        meta.clone(),
        AutoShedConfig {
            tick: Duration::from_millis(100),
            cooldown: Duration::from_millis(600),
            fraction: 1.0,
        },
    );

    (
        data,
        obj,
        meta_file,
        format!("http://{addr}"),
        registry,
        detector,
        location,
        job,
    )
}

#[tokio::test]
async fn auto_shed_fires_when_tablet_is_flagged_hot() {
    let (_d, _o, _m, base, registry, _detector, location, job) =
        spawn_with_autoshed().await;
    let client = reqwest::Client::new();
    let session = "s-auto";

    // Pin the session's shard to tablet 2. Writes for this
    // session then saturate box 2.
    let shard_id = agentdb_core::shard_id(session) as i64;
    let r = client
        .post(format!("{base}/api/admin/shards/{shard_id}"))
        .json(&json!({"tablet_id": 2}))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    location.refresh().await.unwrap();

    // One warmup write so the session exists on box 2.
    let url = format!("{base}/api/tenants/t-1/sessions/{session}/append_and_fetch");
    client
        .post(&url)
        .json(&json!({"event": {"i": 0}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap();

    // Saturate box 2's metrics directly (same approach the
    // other hot-tablet integration tests use — in-process
    // HTTP concurrency is flaky).
    let hot_box = registry.box_by_id(2).unwrap();
    for _ in 0..500 {
        hot_box.metrics.record_append();
        hot_box.metrics.record_admission_wait();
    }

    // Wait for detector → flag → scheduler → fire.
    // detector tick + consecutive=2 + scheduler tick +
    // the shed + migrate runtime.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline && job.total() == 0 {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        job.total() >= 1,
        "auto-shed should have fired at least once"
    );

    // The shard should now be reassigned off tablet 2.
    let r = client
        .get(format!("{base}/api/admin/shards/{shard_id}"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert_ne!(
        body["tablet_id"].as_i64(),
        Some(2),
        "auto-shed should have moved the shard off tablet 2"
    );
}

#[tokio::test]
async fn auto_shed_respects_cooldown() {
    let (_d, _o, _m, _base, registry, _detector, _location, job) =
        spawn_with_autoshed().await;
    // Saturate box 2 continuously so the detector keeps it
    // flagged. Under cooldown (600ms in the test config),
    // auto-shed should fire at most twice in a 1-second
    // window: once immediately, once after the cooldown.
    let hot_box = registry.box_by_id(2).unwrap();
    let feeder_box = hot_box.clone();
    let feeder = tokio::spawn(async move {
        for _ in 0..40 {
            for _ in 0..50 {
                feeder_box.metrics.record_append();
                feeder_box.metrics.record_admission_wait();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(1200)).await;
    feeder.abort();
    let fires = job.total();
    // Cooldown = 600ms, scheduler tick = 100ms, detector
    // tick = 100ms + 2 consecutive. First fire at ~300ms;
    // next eligible at ~900ms; so in a 1200ms window we
    // expect 2 fires at most (maybe 3 if timing is tight).
    assert!(
        (1..=4).contains(&fires),
        "auto-shed fires should be bounded by cooldown; got {fires}"
    );
}
