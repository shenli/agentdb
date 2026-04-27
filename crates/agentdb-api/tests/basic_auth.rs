//! M19: REST Basic Auth end-to-end.
//!
//! Spawns an AppState with auth enabled, creates a user via the
//! admin endpoint, then exercises the `/api/tenants/*` routes
//! with various Authorization headers. Each test asserts either
//! the 2xx happy path or a specific 4xx.

use std::sync::Arc;

use agentdb_api::{router, AppState, AuthConfig, StreamBus};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_meta::MetaStore;
use agentdb_storage::{KvStore, RocksKvStore};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde_json::json;
use tempfile::TempDir;

async fn spawn_with_auth() -> (TempDir, TempDir, tempfile::NamedTempFile, String) {
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    let dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let meta_file = tempfile::NamedTempFile::new().unwrap();
    let meta_path = meta_file.path().to_string_lossy().to_string();
    let meta = Arc::new(MetaStore::open_sqlite(&meta_path).await.unwrap());

    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
    let state = AppState {
        registry: registry.clone(),
        tenants: Arc::new(TenantRegistry::new(kv.clone())),
        sessions: Arc::new(SessionRegistry::new(kv)),
        stream: StreamBus::default(),
        event_bus: agentdb_api::EventBus::default(),
        http: agentdb_api::webhook::new_client(),
        auth: Some(AuthConfig {
            meta: Some(meta.clone()),
        }),
        admin_auth: None,
        jwt_config: agentdb_api::jwt::JwtConfig::default(),
        cors_config: agentdb_api::cors::CorsConfig::default(),
    };
    let app = router(state).layer(axum::Extension(meta.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (dir, obj_dir, meta_file, format!("http://{addr}"))
}

fn basic(user: &str, pass: &str) -> String {
    format!("Basic {}", B64.encode(format!("{user}:{pass}").as_bytes()))
}

async fn create_user(base: &str, user_id: &str, tenant: &str, password: &str) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/admin/users"))
        .json(&json!({
            "user_id": user_id,
            "tenant_external": tenant,
            "password": password,
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "admin create user failed: {}",
        resp.text().await.unwrap()
    );
}

#[tokio::test]
async fn missing_auth_header_is_401() {
    let (_d, _o, _m, base) = spawn_with_auth().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{base}/api/tenants/t-1/sessions/s/append_and_fetch"
        ))
        .json(&json!({"event": {"text": "hi"}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401);
    assert!(
        resp.headers().contains_key("www-authenticate"),
        "401 response must carry WWW-Authenticate"
    );
}

#[tokio::test]
async fn wrong_password_is_401() {
    let (_d, _o, _m, base) = spawn_with_auth().await;
    create_user(&base, "alice", "t-alice", "pencil").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{base}/api/tenants/t-alice/sessions/s/append_and_fetch"
        ))
        .header("Authorization", basic("alice", "WRONG"))
        .json(&json!({"event": {"text": "hi"}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401);
}

#[tokio::test]
async fn correct_auth_matching_tenant_is_200() {
    let (_d, _o, _m, base) = spawn_with_auth().await;
    create_user(&base, "alice", "t-alice", "pencil").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{base}/api/tenants/t-alice/sessions/s/append_and_fetch"
        ))
        .header("Authorization", basic("alice", "pencil"))
        .json(&json!({"event": {"text": "hi"}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "expected 200, got {status}: {body}");
}

#[tokio::test]
async fn cross_tenant_access_is_403() {
    let (_d, _o, _m, base) = spawn_with_auth().await;
    create_user(&base, "alice", "t-alice", "pencil").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{base}/api/tenants/t-bob/sessions/s/append_and_fetch"
        ))
        .header("Authorization", basic("alice", "pencil"))
        .json(&json!({"event": {"text": "hi"}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 403);
}

#[tokio::test]
async fn admin_endpoints_stay_open() {
    let (_d, _o, _m, base) = spawn_with_auth().await;
    // No Authorization header; admin-plane route should still
    // work (admin auth is a separate future PR).
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/api/admin/users"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
}

#[tokio::test]
async fn admin_delete_user_is_204_and_idempotent() {
    let (_d, _o, _m, base) = spawn_with_auth().await;
    create_user(&base, "ghost", "t-ghost", "p").await;

    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("{base}/api/admin/users/ghost"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 204);

    // Second delete: no row, still 204.
    let resp = client
        .delete(format!("{base}/api/admin/users/ghost"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 204);
}

#[tokio::test]
async fn duplicate_user_id_is_409() {
    let (_d, _o, _m, base) = spawn_with_auth().await;
    create_user(&base, "alice", "t-alice", "pencil").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/admin/users"))
        .json(&json!({
            "user_id": "alice",
            "tenant_external": "t-alice",
            "password": "other",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 409);
}
