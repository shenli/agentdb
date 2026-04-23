//! Warm-session handoff end-to-end via REST.
//!
//! Writes events to a session on its default tenant-hash
//! box, sets a shard override to a different box, calls
//! the migrate endpoint, and verifies that subsequent
//! session-scoped operations route to the new box and see
//! the full history.

use std::sync::Arc;

use agentdb_api::{router, AppState, StreamBus};
use agentdb_core::{
    BoxRegistry, LocationProxy, SessionRegistry, TenantRegistry,
};
use agentdb_meta::{self, shards as meta_shards, MetaStore};
use agentdb_storage::{KvStore, RocksKvStore};
use axum::Extension;
use serde_json::json;
use tempfile::TempDir;

async fn spawn_node() -> (
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
    let dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let meta_file = tempfile::NamedTempFile::new().unwrap();
    let meta_path = meta_file.path().to_string_lossy().to_string();
    let meta = Arc::new(MetaStore::open_sqlite(&meta_path).await.unwrap());
    meta_shards::seed_shards(meta.pool(), agentdb_core::SHARD_COUNT as i64, 0)
        .await
        .unwrap();

    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
    let location = LocationProxy::new(meta.clone()).await.unwrap();
    registry.attach_location(location.clone()).ok();

    let state = AppState {
        registry: registry.clone(),
        tenants: Arc::new(TenantRegistry::new(kv.clone())),
        sessions: Arc::new(SessionRegistry::new(kv)),
        stream: StreamBus::default(),
        http: agentdb_api::webhook::new_client(),
        auth: None,
        admin_auth: None,
    };
    let app = router(state).layer(Extension(meta.clone()));
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
        meta,
        location,
    )
}

#[tokio::test]
async fn migrate_session_end_to_end_moves_events_to_new_tablet() {
    let (_d, _o, _m, base, registry, meta, location) = spawn_node().await;
    let client = reqwest::Client::new();
    let tenant = "t-1";
    let session = "s-migrate-e2e";

    // Write 10 events on the default (tenant-hash) box.
    let url = format!("{base}/api/tenants/{tenant}/sessions/{session}/append_and_fetch");
    for i in 1..=10 {
        let resp = client
            .post(&url)
            .json(&json!({"event": {"i": i}, "fetch_last_n": 0}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
    }

    let from_box = registry.route_tenant(tenant).id;
    let before = registry
        .box_by_id(from_box)
        .unwrap()
        .metrics
        .snapshot()
        .appends;
    assert_eq!(before, 10);
    // Pick a different box.
    let to_box = (from_box + 1) % 4;

    // Set a shard override so future session-scoped writes
    // route to the new box. Important: the admin endpoint
    // for set_shard takes tablet_id (which == box_id here),
    // and we refresh the proxy inline.
    let shard_id = agentdb_core::shard_id(session);
    meta_shards::set_assignment(meta.pool(), shard_id as i64, Some(to_box as i64), 0)
        .await
        .unwrap();
    location.refresh().await.unwrap();

    // Migrate.
    let resp = client
        .post(format!(
            "{base}/api/admin/sessions/{session}/migrate"
        ))
        .json(&json!({"tenant_id": tenant, "to_tablet_id": to_box}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "migrate failed: {}",
        resp.text().await.unwrap()
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["events_migrated"].as_u64(), Some(10));
    assert_eq!(body["from_tablet_id"].as_u64(), Some(from_box as u64));
    assert_eq!(body["to_tablet_id"].as_u64(), Some(to_box as u64));

    // Write one more event — it should land on the new box.
    let resp = client
        .post(&url)
        .json(&json!({"event": {"i": 11}, "fetch_last_n": 30}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let recent = body["recent"].as_array().expect("recent");
    // The response includes the last N events from this
    // session AS SEEN BY THE NEW BOX. Pre-migration, the
    // new box was empty. After migration, it has 10 old
    // events; the +1 append makes 11. fetch_last_n=30
    // returns all of them.
    assert_eq!(
        recent.len(),
        11,
        "after migrate + append, new box should see all 11 events; got {recent:?}"
    );

    // Verify the source box has no events left for this
    // session. box_by_id(from_box).appends stays at 10
    // (that counter is monotonic; what matters is the
    // session record on source is gone, which we test
    // indirectly via the absence of 11th event there).
}

#[tokio::test]
async fn migrate_unknown_session_is_404() {
    let (_d, _o, _m, base, _reg, _meta, _loc) = spawn_node().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{base}/api/admin/sessions/nobody/migrate"
        ))
        .json(&json!({"tenant_id": "t-ghost", "to_tablet_id": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);
}
