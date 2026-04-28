//! M27: v2 SSE routes — auth-derived tenant, no tenant in URL.

use std::sync::Arc;

use agentdb_api::{router, AppState, AuthConfig, StreamBus};
use agentdb_core::{BoxRegistry, HlcClock, SessionRegistry, TenantRegistry};
use agentdb_meta::MetaStore;
use agentdb_storage::{KvStore, RocksKvStore};
use axum::Extension;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde_json::json;
use tempfile::TempDir;

struct Env {
    _kv_dir: TempDir,
    _obj_dir: TempDir,
    _meta_file: tempfile::NamedTempFile,
    base: String,
}

async fn spawn() -> Env {
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
    let registry = Arc::new(
        BoxRegistry::new_with_meta(
            kv.clone(),
            store,
            4,
            Arc::new(HlcClock::default()),
            meta.clone(),
            "node-a".into(),
        )
        .await
        .unwrap(),
    );
    let state = AppState {
        registry,
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
        broadcast_bus: agentdb_api::BroadcastBus::default(),
    };
    let app = router(state).layer(Extension(meta.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Env {
        _kv_dir: kv_dir,
        _obj_dir: obj_dir,
        _meta_file: meta_file,
        base: format!("http://{addr}"),
    }
}

fn basic(user: &str, pass: &str) -> String {
    format!("Basic {}", B64.encode(format!("{user}:{pass}").as_bytes()))
}

async fn create_user(base: &str, user_id: &str, tenant: &str, password: &str) {
    let resp = reqwest::Client::new()
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

/// Append one event under the legacy tenant-explicit route to
/// populate a session and its locator. Returns once the row
/// exists in `session_locators` so subsequent subscribe can
/// resolve it.
async fn seed_session(base: &str, user: &str, pass: &str, tenant: &str, session: &str) {
    reqwest::Client::new()
        .post(format!(
            "{base}/api/tenants/{tenant}/sessions/{session}/append_and_fetch"
        ))
        .header("Authorization", basic(user, pass))
        .json(&json!({"event": {"seed": true}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

#[tokio::test]
async fn v2_session_subscribe_without_auth_is_401() {
    let env = spawn().await;
    let resp = reqwest::Client::new()
        .get(format!("{}/api/sessions/sess-x/subscribe", env.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401);
}

#[tokio::test]
async fn v2_session_subscribe_with_auth_owned_session_succeeds() {
    let env = spawn().await;
    create_user(&env.base, "alice", "t-alice", "pencil").await;
    seed_session(&env.base, "alice", "pencil", "t-alice", "sess-1").await;

    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/sessions/sess-1/subscribe?from_seq=0",
            env.base
        ))
        .header("Authorization", basic("alice", "pencil"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert!(
        resp.headers()
            .get("content-type")
            .map(|v| v.to_str().unwrap_or(""))
            .unwrap_or("")
            .starts_with("text/event-stream"),
        "expected SSE content-type"
    );
}

#[tokio::test]
async fn v2_session_subscribe_with_other_tenants_session_is_403() {
    let env = spawn().await;
    create_user(&env.base, "alice", "t-alice", "pencil").await;
    create_user(&env.base, "bob", "t-bob", "pencil").await;
    // Seed a session under tenant t-alice
    seed_session(&env.base, "alice", "pencil", "t-alice", "sess-a").await;

    // bob (tenant t-bob) tries to subscribe to t-alice's session
    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/sessions/sess-a/subscribe?from_seq=0",
            env.base
        ))
        .header("Authorization", basic("bob", "pencil"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 403);
}

#[tokio::test]
async fn v2_tenant_subscribe_without_auth_is_401() {
    let env = spawn().await;
    let resp = reqwest::Client::new()
        .get(format!("{}/api/subscribe", env.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401);
}

#[tokio::test]
async fn v2_tenant_subscribe_with_auth_succeeds() {
    let env = spawn().await;
    create_user(&env.base, "alice", "t-alice", "pencil").await;
    seed_session(&env.base, "alice", "pencil", "t-alice", "sess-1").await;

    let resp = reqwest::Client::new()
        .get(format!("{}/api/subscribe", env.base))
        .header("Authorization", basic("alice", "pencil"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
}
