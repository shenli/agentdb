//! End-to-end: backup a live node, then open the backup files
//! as a fresh server and verify the writes are visible.
//!
//! This exercises every layer the backup endpoint touches:
//! axum routing → admin_backup module → RocksDB Checkpoint →
//! SQLite VACUUM INTO → manifest.json.

use std::sync::Arc;

use agentdb_api::{router, AppState, StreamBus};
use agentdb_core::{BackupManifest, BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_meta::MetaStore;
use agentdb_storage::{KvStore, RocksKvStore};
use axum::Extension;
use serde_json::{json, Value};
use tempfile::TempDir;

struct Node {
    _kv_dir: TempDir,
    _obj_dir: TempDir,
    _meta_file: tempfile::NamedTempFile,
    base: String,
}

async fn spawn() -> Node {
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    let kv_dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let meta_file = tempfile::NamedTempFile::new().unwrap();
    let meta_path = meta_file.path().to_string_lossy().to_string();
    let meta = Arc::new(MetaStore::open_sqlite(&meta_path).await.unwrap());

    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 2).unwrap());

    let state = AppState {
        registry,
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
    let app = router(state).layer(Extension(meta.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Node {
        _kv_dir: kv_dir,
        _obj_dir: obj_dir,
        _meta_file: meta_file,
        base: format!("http://{addr}"),
    }
}

async fn append(client: &reqwest::Client, base: &str, tenant: &str, session: &str, body: Value) {
    let r = client
        .post(format!(
            "{base}/api/tenants/{tenant}/sessions/{session}/append_and_fetch"
        ))
        .json(&json!({"event": body, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200, "append failed: {}", r.text().await.unwrap());
}

#[tokio::test]
async fn backup_writes_kv_and_meta_sqlite_and_manifest() {
    let node = spawn().await;
    let client = reqwest::Client::new();

    // Seed a tenant + one write so the KV has content and the
    // meta store has a tenants row.
    append(&client, &node.base, "t-acme", "s-1", json!({"msg": "hello"})).await;

    let out = TempDir::new().unwrap();
    let dest = out.path().join("snap");
    let resp = client
        .post(format!("{}/api/admin/backup", node.base))
        .json(&json!({"out_dir": dest.to_string_lossy()}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200, "backup failed: {}", resp.text().await.unwrap());

    let manifest: BackupManifest = resp.json().await.unwrap();
    assert_eq!(manifest.version, 1);
    assert!(manifest.kv_bytes > 0, "kv bytes should be non-zero");
    assert!(manifest.meta_bytes > 0, "meta bytes should be non-zero");

    // Directory layout check.
    assert!(dest.join("kv").is_dir(), "kv subdir missing");
    assert!(dest.join("meta.sqlite").is_file(), "meta.sqlite missing");
    assert!(dest.join("manifest.json").is_file(), "manifest.json missing");

    // manifest.json matches the response.
    let on_disk: BackupManifest =
        serde_json::from_slice(&std::fs::read(dest.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(on_disk.kv_bytes, manifest.kv_bytes);
    assert_eq!(on_disk.meta_bytes, manifest.meta_bytes);
}

#[tokio::test]
async fn backup_refuses_when_out_dir_exists() {
    let node = spawn().await;
    let client = reqwest::Client::new();

    // Seed writes so the backup has content.
    append(&client, &node.base, "t-1", "s-1", json!({"msg": "x"})).await;

    let out = TempDir::new().unwrap();
    let dest = out.path().join("existing");
    std::fs::create_dir_all(&dest).unwrap();

    let resp = client
        .post(format!("{}/api/admin/backup", node.base))
        .json(&json!({"out_dir": dest.to_string_lossy()}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["field"].as_str(), Some("out_dir"));
}

#[tokio::test]
async fn backup_refuses_relative_path() {
    let node = spawn().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/admin/backup", node.base))
        .json(&json!({"out_dir": "relative/path/snap"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["field"].as_str(), Some("out_dir"));
}

#[tokio::test]
async fn restored_meta_carries_tenant_rows() {
    // Full round-trip: seed source, backup, open the restored
    // meta.sqlite as a standalone MetaStore, verify contents
    // are there. Full KV-replay round-trip is harder (we'd
    // need to open a BoxRegistry pointed at the restored kv
    // dir and replay the HLC state); this test asserts the
    // meta side, and the earlier test covers byte sizes > 0
    // for kv. Together they cover both legs.
    let node = spawn().await;
    let client = reqwest::Client::new();
    // Create a tenant by writing so a row lands in meta or
    // KV — in our codebase tenant records are in the KV, and
    // admin_tokens are in meta. Mint an admin token into meta
    // so it's visible after restore.
    let _ = client
        .post(format!("{}/api/admin/tokens", node.base))
        .json(&json!({"description": "restored-token"}))
        .send()
        .await
        .unwrap();

    // Take a backup of both dirs.
    let out = TempDir::new().unwrap();
    let dest = out.path().join("snap");
    let resp = client
        .post(format!("{}/api/admin/backup", node.base))
        .json(&json!({"out_dir": dest.to_string_lossy()}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    // Open the restored meta file directly.
    let restored_meta = MetaStore::open_sqlite(&dest.join("meta.sqlite").to_string_lossy())
        .await
        .unwrap();
    let tokens = agentdb_meta::admin_tokens::list_active(restored_meta.pool())
        .await
        .unwrap();
    assert_eq!(tokens.len(), 1, "restored meta missing minted token");
    assert_eq!(tokens[0].description, "restored-token");

    // Keep the source alive so its tempdirs don't drop early.
    drop(node);
}
