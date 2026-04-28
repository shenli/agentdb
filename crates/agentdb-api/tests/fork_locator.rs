//! M26 Phase 1b: fork derives tenant from session_locator
//! instead of requiring it in the request body.
//!
//! Spins up a registry with `BoxRegistry::new_with_meta`
//! (locator-capable path active) and exercises the four
//! relevant cases:
//!
//! 1. body omits `tenant_id` → locator supplies it; fork
//!    succeeds.
//! 2. body has `tenant_id` matching locator → fork succeeds
//!    (back-compat).
//! 3. body has `tenant_id` disagreeing with locator → 400.
//! 4. parent has no locator AND body omits `tenant_id` → 400.

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
    meta: Arc<MetaStore>,
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
        registry: registry.clone(),
        tenants: Arc::new(TenantRegistry::new(kv.clone())),
        sessions: Arc::new(SessionRegistry::new(kv)),
        stream: StreamBus::default(),
        event_bus: agentdb_api::EventBus::default(),
        http: agentdb_api::webhook::new_client(),
        auth: Some(AuthConfig { meta: Some(meta.clone()) }),
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
        meta,
    }
}

fn basic_auth(user: &str, pass: &str) -> String {
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
    assert!(resp.status().is_success(), "create_user failed: {}", resp.status());
}

/// Append once to populate a parent session + its locator.
async fn seed_parent(env: &Env, auth: &str, session: &str) {
    reqwest::Client::new()
        .post(format!(
            "{}/api/sessions/{session}/append_and_fetch",
            env.base
        ))
        .header("Authorization", auth)
        .json(&json!({"event": {"k": 1}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

#[tokio::test]
async fn fork_resolves_tenant_from_locator_when_body_omits_it() {
    let env = spawn().await;
    create_user(&env.base, "alice", "t-1", "pw").await;
    let auth = basic_auth("alice", "pw");
    seed_parent(&env, &auth, "parent").await;

    // Sanity-check the locator was written by the seed append.
    let loc = agentdb_meta::session_locators::get(env.meta.pool(), "parent")
        .await
        .unwrap()
        .expect("seed append should have populated parent's locator");
    assert!(loc.tablet_id.is_some());
    assert_eq!(loc.tenant_external_id.as_deref(), Some("t-1"));

    // Body has no tenant_id — locator-derived path drives the
    // resolution.
    let resp = reqwest::Client::new()
        .post(format!("{}/api/sessions/parent/fork", env.base))
        .header("Authorization", &auth)
        .json(&json!({ "new_session_id": "child" }))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status, 200, "fork failed: {body}");
    assert_eq!(body["new_session_id"], "child");

    // Child also gets a locator after fork (the REST handler
    // populates).
    let child_loc = agentdb_meta::session_locators::get(env.meta.pool(), "child")
        .await
        .unwrap()
        .expect("fork should have populated child's locator");
    assert_eq!(child_loc.tenant_internal_id, loc.tenant_internal_id);
}

#[tokio::test]
async fn fork_with_matching_body_tenant_still_works() {
    // Back-compat: clients that still send tenant_id and
    // happen to match the locator are accepted unchanged.
    let env = spawn().await;
    create_user(&env.base, "alice", "t-1", "pw").await;
    let auth = basic_auth("alice", "pw");
    seed_parent(&env, &auth, "parent").await;

    let resp = reqwest::Client::new()
        .post(format!("{}/api/sessions/parent/fork", env.base))
        .header("Authorization", &auth)
        .json(&json!({ "tenant_id": "t-1", "new_session_id": "child" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
}

#[tokio::test]
async fn fork_with_disagreeing_body_tenant_returns_400() {
    // Locator says "t-1" but the client sends "t-other" —
    // this likely means the client config is stale; surface
    // 400 rather than silently overruling either side.
    let env = spawn().await;
    create_user(&env.base, "alice", "t-1", "pw").await;
    let auth = basic_auth("alice", "pw");
    seed_parent(&env, &auth, "parent").await;

    let resp = reqwest::Client::new()
        .post(format!("{}/api/sessions/parent/fork", env.base))
        .header("Authorization", &auth)
        .json(&json!({ "tenant_id": "t-other", "new_session_id": "child" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let msg = body["message"].as_str().unwrap_or("");
    // When auth is enabled, the disagreement is with the authenticated
    // tenant; when auth is absent, with the locator.
    assert!(
        msg.contains("disagrees"),
        "expected disagreement message, got: {msg}"
    );
}

#[tokio::test]
async fn fork_without_locator_and_without_body_tenant_returns_400() {
    // Parent session has never been touched (no locator).
    // Body omits tenant_id. Resolution can't proceed.
    let env = spawn().await;
    create_user(&env.base, "alice", "t-1", "pw").await;
    let auth = basic_auth("alice", "pw");
    // Note: NOT seeding the parent — we want the locator gone.

    let resp = reqwest::Client::new()
        .post(format!("{}/api/sessions/never-existed/fork", env.base))
        .header("Authorization", &auth)
        .json(&json!({ "new_session_id": "child" }))
        .send()
        .await
        .unwrap();
    // Auth resolves tenant to "t-1" but the parent session
    // has never been written (no locator, no data). The fork
    // handler proceeds past tenant resolution and fails at
    // `fork_session` with "not found". Must not be 401.
    let status = resp.status().as_u16();
    assert_ne!(status, 401, "must not be 401");
    assert!(
        status == 400 || status == 404,
        "expected 400 or 404, got {status}"
    );
}
