//! M26 closeout: tenant identity is auth-bound, not client-
//! supplied, on every mutation route.
//!
//! Two surfaces to lock down:
//!
//! 1. `POST /api/sessions/:sid/fork` — was previously mounted
//!    *outside* the auth subtree (security gap: anyone with TCP
//!    access to the API port could fork any session). It's now
//!    under basic_auth_middleware, and the handler verifies the
//!    parent session's locator-recorded tenant matches the
//!    authenticated user's tenant.
//!
//! 2. `POST /api/sessions/:sid/append_and_fetch` — new auth-bound
//!    route added in this milestone. No tenant in path or body;
//!    handler derives tenant from credentials and cross-checks
//!    against the locator.
//!
//! For each surface this file pins down the four contract cases:
//! missing auth → 401, wrong tenant → 403, body-vs-auth drift →
//! 400 (fork only), and the happy path → 200.

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

/// Append one event via the v2 session-keyed route to
/// populate a session and its locator. Returns once the row
/// exists in `session_locators` so subsequent fork/find can
/// resolve it.
async fn seed_session(base: &str, user: &str, pass: &str, _tenant: &str, session: &str) {
    reqwest::Client::new()
        .post(format!(
            "{base}/api/sessions/{session}/append_and_fetch"
        ))
        .header("Authorization", basic(user, pass))
        .json(&json!({"event": {"seed": true}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

// ---------- fork ----------

#[tokio::test]
async fn fork_without_auth_header_is_401() {
    let env = spawn().await;
    create_user(&env.base, "alice", "t-alice", "pencil").await;
    seed_session(&env.base, "alice", "pencil", "t-alice", "s-parent").await;

    // Pre-PR, fork was mounted outside the auth subtree, so
    // this would have succeeded with no credentials. Now it
    // has to 401.
    let resp = reqwest::Client::new()
        .post(format!("{}/api/sessions/s-parent/fork", env.base))
        .json(&json!({"new_session_id": "s-child"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401);
}

#[tokio::test]
async fn fork_with_auth_owned_by_user_succeeds() {
    let env = spawn().await;
    create_user(&env.base, "alice", "t-alice", "pencil").await;
    seed_session(&env.base, "alice", "pencil", "t-alice", "s-parent").await;

    let resp = reqwest::Client::new()
        .post(format!("{}/api/sessions/s-parent/fork", env.base))
        .header("Authorization", basic("alice", "pencil"))
        .json(&json!({"new_session_id": "s-child"}))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "expected 200; got {status}: {body}");
}

#[tokio::test]
async fn fork_with_auth_owned_by_other_tenant_is_403() {
    let env = spawn().await;
    create_user(&env.base, "alice", "t-alice", "pencil").await;
    create_user(&env.base, "bob", "t-bob", "pickle").await;
    // Parent session belongs to alice (tenant t-alice).
    seed_session(&env.base, "alice", "pencil", "t-alice", "s-alices").await;

    // bob authenticates as himself (tenant t-bob), but tries to
    // fork alice's session. The locator says t-alice; the
    // authed tenant is t-bob → 403.
    let resp = reqwest::Client::new()
        .post(format!("{}/api/sessions/s-alices/fork", env.base))
        .header("Authorization", basic("bob", "pickle"))
        .json(&json!({"new_session_id": "s-bobs-attempt"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 403);
}

#[tokio::test]
async fn fork_with_body_tenant_disagreeing_with_auth_is_400() {
    let env = spawn().await;
    create_user(&env.base, "alice", "t-alice", "pencil").await;
    seed_session(&env.base, "alice", "pencil", "t-alice", "s-parent").await;

    // Authed as alice (t-alice), but body claims t-bob. The
    // body is just a stale client claim — return 400 so the
    // client can update, rather than silently overruling.
    let resp = reqwest::Client::new()
        .post(format!("{}/api/sessions/s-parent/fork", env.base))
        .header("Authorization", basic("alice", "pencil"))
        .json(&json!({
            "new_session_id": "s-child",
            "tenant_id": "t-bob",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
}

// ---------- session-keyed append_and_fetch ----------

#[tokio::test]
async fn session_keyed_append_without_auth_is_401() {
    let env = spawn().await;
    create_user(&env.base, "alice", "t-alice", "pencil").await;
    seed_session(&env.base, "alice", "pencil", "t-alice", "s-1").await;

    let resp = reqwest::Client::new()
        .post(format!("{}/api/sessions/s-1/append_and_fetch", env.base))
        .json(&json!({"event": {"text": "no creds"}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401);
}

#[tokio::test]
async fn session_keyed_append_with_auth_happy_path() {
    let env = spawn().await;
    create_user(&env.base, "alice", "t-alice", "pencil").await;
    // Seed via the legacy route so the locator exists.
    seed_session(&env.base, "alice", "pencil", "t-alice", "s-1").await;

    // Then write via the new auth-bound route — no tenant in
    // path or body.
    let resp = reqwest::Client::new()
        .post(format!("{}/api/sessions/s-1/append_and_fetch", env.base))
        .header("Authorization", basic("alice", "pencil"))
        .json(&json!({"event": {"text": "hi"}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "expected 200; got {status}: {body}");
}

#[tokio::test]
async fn session_keyed_append_for_other_tenants_session_is_403() {
    let env = spawn().await;
    create_user(&env.base, "alice", "t-alice", "pencil").await;
    create_user(&env.base, "bob", "t-bob", "pickle").await;
    // Session owned by alice's tenant.
    seed_session(&env.base, "alice", "pencil", "t-alice", "s-shared").await;

    // bob authenticates and tries to append. Auth says t-bob,
    // locator says t-alice → 403.
    let resp = reqwest::Client::new()
        .post(format!(
            "{}/api/sessions/s-shared/append_and_fetch",
            env.base
        ))
        .header("Authorization", basic("bob", "pickle"))
        .json(&json!({"event": {"text": "spoof"}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 403);
}

