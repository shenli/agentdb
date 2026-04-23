//! M20 write-path integration test: `POST /api/admin/shards/:id`
//! actually changes where future writes land.
//!
//! Before M20's hot-path integration, setting an override via the
//! admin endpoint only changed what `GET /api/admin/shards/:id`
//! returned — the append path still hashed tenant→box directly.
//! This test exercises the end-to-end wiring:
//!
//! 1. Start with a 4-box registry and a LocationProxy attached.
//! 2. Observe which box an `append_and_fetch(tenant, session)` hits
//!    by default (tenant-hash fallback).
//! 3. `POST /api/admin/shards/<shard_id>` to redirect that shard to
//!    a *different* box.
//! 4. Re-issue the write; verify it lands on the new box by
//!    comparing per-box append counters.
//! 5. Revert the override; verify writes go back to the original.

use std::sync::Arc;

use agentdb_api::{router, AppState, StreamBus};
use agentdb_core::{BoxRegistry, LocationProxy, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use serde_json::json;
use tempfile::TempDir;

async fn spawn() -> (
    TempDir,
    TempDir,
    tempfile::NamedTempFile,
    String,
    Arc<BoxRegistry>,
    Arc<LocationProxy>,
) {
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    let dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let meta_file = tempfile::NamedTempFile::new().unwrap();
    let meta_path = meta_file.path().to_string_lossy().to_string();
    let meta = Arc::new(
        agentdb_meta::MetaStore::open_sqlite(&meta_path)
            .await
            .unwrap(),
    );
    agentdb_meta::migrations::apply(meta.pool()).await.unwrap();
    agentdb_meta::shards::seed_shards(
        meta.pool(),
        agentdb_core::SHARD_COUNT as i64,
        0,
    )
    .await
    .unwrap();

    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
    let proxy = LocationProxy::new(meta.clone()).await.unwrap();
    registry.attach_location(proxy.clone()).ok();

    let state = AppState {
        registry: registry.clone(),
        tenants: Arc::new(TenantRegistry::new(kv.clone())),
        sessions: Arc::new(SessionRegistry::new(kv)),
        stream: StreamBus::default(),
        http: agentdb_api::webhook::new_client(),
        auth: None,
    };
    let app = router(state)
        .layer(axum::Extension(meta.clone()))
        .layer(axum::Extension(proxy.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (
        dir,
        obj_dir,
        meta_file,
        format!("http://{addr}"),
        registry,
        proxy,
    )
}

#[tokio::test]
async fn shard_override_actually_reroutes_future_writes() {
    let (_dir, _obj_dir, _meta_file, base, registry, _proxy) = spawn().await;
    let client = reqwest::Client::new();

    let tenant = "t-route";
    let session = "s-route-me";
    let url = format!("{base}/api/tenants/{tenant}/sessions/{session}/append_and_fetch");
    let body = json!({"event": {"text": "hi"}, "fetch_last_n": 0});

    // Baseline: with no override, writes land on the tenant-hash box.
    let default_box = registry.route_tenant(tenant).id;
    let appends_before: Vec<u64> = (0..4)
        .map(|i| registry.box_by_id(i).unwrap().metrics.snapshot().appends)
        .collect();

    client
        .post(&url)
        .json(&body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let appends_after_default: Vec<u64> = (0..4)
        .map(|i| registry.box_by_id(i).unwrap().metrics.snapshot().appends)
        .collect();
    assert_eq!(
        appends_after_default[default_box as usize] - appends_before[default_box as usize],
        1,
        "first write (no override) lands on the tenant-hash box"
    );

    // Pick a target box that's different from the default.
    let target_box: i64 = ((default_box as i64) + 1) % 4;
    let shard_id = agentdb_core::shard_id(session);
    let resp = client
        .post(format!("{base}/api/admin/shards/{shard_id}"))
        .json(&json!({"tablet_id": target_box}))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "set_shard failed: {}",
        resp.text().await.unwrap()
    );

    // Second write — must land on target_box.
    client
        .post(&url)
        .json(&body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let appends_after_override: Vec<u64> = (0..4)
        .map(|i| registry.box_by_id(i).unwrap().metrics.snapshot().appends)
        .collect();
    assert_eq!(
        appends_after_override[target_box as usize] - appends_after_default[target_box as usize],
        1,
        "write after override lands on target box (target={target_box}, default={default_box})"
    );
    assert_eq!(
        appends_after_override[default_box as usize] - appends_after_default[default_box as usize],
        0,
        "default box does not receive the override write"
    );

    // Revert the override — pass tablet_id: null.
    let resp = client
        .post(format!("{base}/api/admin/shards/{shard_id}"))
        .json(&json!({"tablet_id": null}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "revert set_shard failed");

    // Third write — back to the default box.
    client
        .post(&url)
        .json(&body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let appends_final: Vec<u64> = (0..4)
        .map(|i| registry.box_by_id(i).unwrap().metrics.snapshot().appends)
        .collect();
    assert_eq!(
        appends_final[default_box as usize] - appends_after_override[default_box as usize],
        1,
        "after revert, writes route back to the tenant-hash box"
    );
}
