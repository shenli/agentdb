//! Admin-plane bearer-token auth end-to-end.
//!
//! Spawns a single server with admin_auth enabled. Verifies:
//!   - bootstrap mint without a token succeeds when no tokens exist
//!   - subsequent admin calls without a token → 401
//!   - subsequent admin calls with the minted token → 200
//!   - a revoked token → 401
//!   - health / metrics stay open regardless
//!   - the second mint (tokens now exist) requires auth

use std::sync::Arc;

use agentdb_api::{admin_auth_middleware::AdminAuthConfig, router, AppState, StreamBus};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_meta::MetaStore;
use agentdb_storage::{KvStore, RocksKvStore};
use axum::Extension;
use serde_json::json;
use tempfile::TempDir;

async fn spawn_admin_auth_node() -> (TempDir, TempDir, tempfile::NamedTempFile, String) {
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
        registry,
        tenants: Arc::new(TenantRegistry::new(kv.clone())),
        sessions: Arc::new(SessionRegistry::new(kv)),
        stream: StreamBus::default(),
        event_bus: agentdb_api::EventBus::default(),
        http: agentdb_api::webhook::new_client(),
        auth: None,
        admin_auth: Some(AdminAuthConfig {
            meta: Some(meta.clone()),
        }),
    };
    let app = router(state).layer(Extension(meta.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (dir, obj_dir, meta_file, format!("http://{addr}"))
}

/// Shortcut: mint a bootstrap token, return its plaintext.
async fn mint_bootstrap(base: &str) -> String {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/admin/tokens"))
        .json(&json!({"description": "bootstrap"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "bootstrap mint should succeed without auth: {}",
        resp.text().await.unwrap()
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    body["token"]
        .as_str()
        .expect("plaintext token in body")
        .to_string()
}

#[tokio::test]
async fn admin_endpoint_without_bearer_is_401_when_auth_on() {
    let (_d, _o, _m, base) = spawn_admin_auth_node().await;
    // Mint a bootstrap token first so the admin endpoints
    // are now protected. Otherwise the bootstrap exception
    // would let /api/admin/tokens through.
    let _ = mint_bootstrap(&base).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/api/admin/tablets"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401);
}

#[tokio::test]
async fn bootstrap_mint_without_bearer_succeeds_when_no_tokens_exist() {
    let (_d, _o, _m, base) = spawn_admin_auth_node().await;
    let token = mint_bootstrap(&base).await;
    assert!(token.starts_with("agentdb_"));
}

#[tokio::test]
async fn second_mint_requires_bearer() {
    let (_d, _o, _m, base) = spawn_admin_auth_node().await;
    let _ = mint_bootstrap(&base).await;

    // Second mint without token → 401.
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/admin/tokens"))
        .json(&json!({"description": "second"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401);
}

#[tokio::test]
async fn valid_token_lets_admin_request_through() {
    let (_d, _o, _m, base) = spawn_admin_auth_node().await;
    let token = mint_bootstrap(&base).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/api/admin/tablets"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "authenticated admin request should succeed: {}",
        resp.text().await.unwrap()
    );
}

#[tokio::test]
async fn revoked_token_is_401() {
    let (_d, _o, _m, base) = spawn_admin_auth_node().await;
    let token = mint_bootstrap(&base).await;

    // Get the token_id from GET /api/admin/tokens so we can
    // revoke it.
    let client = reqwest::Client::new();
    let list = client
        .get(format!("{base}/api/admin/tokens"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(list.status().as_u16(), 200);
    let list_body: serde_json::Value = list.json().await.unwrap();
    let token_id = list_body[0]["token_id"]
        .as_str()
        .expect("token_id in list")
        .to_string();

    // Revoke self.
    let resp = client
        .delete(format!("{base}/api/admin/tokens/{token_id}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 204);

    // Subsequent requests with the same token → 401.
    let resp = client
        .get(format!("{base}/api/admin/tablets"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        401,
        "revoked token must not authenticate"
    );
}

#[tokio::test]
async fn health_and_metrics_always_open_regardless_of_auth() {
    let (_d, _o, _m, base) = spawn_admin_auth_node().await;
    let _ = mint_bootstrap(&base).await; // make admin auth binding

    let client = reqwest::Client::new();
    // /health/live — should be 200 with no auth.
    let r = client.get(format!("{base}/health/live")).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 200, "/health/live must stay open");
    // /metrics — should be 200 with no auth.
    let r = client.get(format!("{base}/metrics")).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 200, "/metrics must stay open");
    // /openapi.json — should be 200 with no auth.
    let r = client
        .get(format!("{base}/openapi.json"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200, "/openapi.json must stay open");
}

#[tokio::test]
async fn admin_auth_off_is_pass_through() {
    // Control case: when AppState::admin_auth is None, admin
    // endpoints stay open (backward compat).
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    let dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
    let state = AppState {
        registry,
        tenants: Arc::new(TenantRegistry::new(kv.clone())),
        sessions: Arc::new(SessionRegistry::new(kv)),
        stream: StreamBus::default(),
        event_bus: agentdb_api::EventBus::default(),
        http: agentdb_api::webhook::new_client(),
        auth: None,
        admin_auth: None,
    };
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/api/admin/tablets"))
        .send()
        .await
        .unwrap();
    // With admin_auth: None, admin endpoints respond normally
    // (503 here because meta Extension isn't attached; the
    // key point is that we did NOT get a 401).
    assert_ne!(
        resp.status().as_u16(),
        401,
        "admin_auth: None must not 401; got {}",
        resp.status()
    );
}
