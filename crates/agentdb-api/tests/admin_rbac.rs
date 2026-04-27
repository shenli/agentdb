//! End-to-end RBAC for admin bearer tokens.
//!
//! Covers the admin / readonly scope split plus the audit
//! log's visibility of scope-403 attempts.

use std::sync::Arc;

use agentdb_api::{admin_auth_middleware::AdminAuthConfig, router, AppState, StreamBus};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_meta::MetaStore;
use agentdb_storage::{KvStore, RocksKvStore};
use axum::Extension;
use serde_json::{json, Value};
use tempfile::TempDir;

async fn spawn() -> (TempDir, TempDir, tempfile::NamedTempFile, Arc<MetaStore>, String) {
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
    let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 2).unwrap());
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
        jwt_config: agentdb_api::jwt::JwtConfig::default(),
        cors_config: agentdb_api::cors::CorsConfig::default(),
    };
    let app = router(state).layer(Extension(meta.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (dir, obj_dir, meta_file, meta, format!("http://{addr}"))
}

/// Bootstrap an admin token (scope defaults to admin via the
/// mint request).
async fn mint_admin(base: &str) -> String {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/admin/tokens"))
        .json(&json!({"description": "bootstrap"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["scope"].as_str(), Some("admin"), "bootstrap default: {v}");
    v["token"].as_str().unwrap().to_string()
}

/// Mint a readonly token using an existing admin token. Returns
/// the plaintext.
async fn mint_readonly(base: &str, admin_token: &str) -> String {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/admin/tokens"))
        .header("Authorization", format!("Bearer {admin_token}"))
        .json(&json!({"description": "grafana", "scope": "readonly"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200, "mint readonly failed: {}", resp.text().await.unwrap());
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["scope"].as_str(), Some("readonly"));
    v["token"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn readonly_token_can_get_but_not_post() {
    let (_d, _o, _m, _meta, base) = spawn().await;
    let admin_tok = mint_admin(&base).await;
    let ro_tok = mint_readonly(&base, &admin_tok).await;

    let client = reqwest::Client::new();
    // Readonly GET on /api/admin/tablets → 200.
    let r = client
        .get(format!("{base}/api/admin/tablets"))
        .header("Authorization", format!("Bearer {ro_tok}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200, "readonly GET should succeed");

    // Readonly POST on /api/admin/drain → 403.
    let r = client
        .post(format!("{base}/api/admin/drain"))
        .header("Authorization", format!("Bearer {ro_tok}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 403, "readonly POST must 403: {}", r.text().await.unwrap());
}

#[tokio::test]
async fn readonly_token_cannot_mint_another_token() {
    let (_d, _o, _m, _meta, base) = spawn().await;
    let admin_tok = mint_admin(&base).await;
    let ro_tok = mint_readonly(&base, &admin_tok).await;

    let client = reqwest::Client::new();
    // Readonly POST on /api/admin/tokens → 403. Critically,
    // the readonly token cannot escalate by minting an admin
    // token for itself.
    let r = client
        .post(format!("{base}/api/admin/tokens"))
        .header("Authorization", format!("Bearer {ro_tok}"))
        .json(&json!({"description": "escalate", "scope": "admin"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 403);
}

#[tokio::test]
async fn readonly_token_cannot_revoke_tokens() {
    let (_d, _o, _m, _meta, base) = spawn().await;
    let admin_tok = mint_admin(&base).await;
    let ro_tok = mint_readonly(&base, &admin_tok).await;

    // Look up the readonly token id via the admin token.
    let client = reqwest::Client::new();
    let v: Value = client
        .get(format!("{base}/api/admin/tokens"))
        .header("Authorization", format!("Bearer {admin_tok}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let tokens = v.as_array().unwrap();
    let ro_id = tokens
        .iter()
        .find(|t| t["scope"].as_str() == Some("readonly"))
        .unwrap()["token_id"]
        .as_str()
        .unwrap()
        .to_string();

    let r = client
        .delete(format!("{base}/api/admin/tokens/{ro_id}"))
        .header("Authorization", format!("Bearer {ro_tok}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 403);
}

#[tokio::test]
async fn admin_token_can_do_anything() {
    let (_d, _o, _m, _meta, base) = spawn().await;
    let admin_tok = mint_admin(&base).await;

    let client = reqwest::Client::new();
    let r = client
        .get(format!("{base}/api/admin/tablets"))
        .header("Authorization", format!("Bearer {admin_tok}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);

    // GET on audit log also OK.
    let r = client
        .get(format!("{base}/api/admin/audit"))
        .header("Authorization", format!("Bearer {admin_tok}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);
}

#[tokio::test]
async fn scope_403_is_audited_with_actor() {
    let (_d, _o, _m, meta, base) = spawn().await;
    let admin_tok = mint_admin(&base).await;
    let ro_tok = mint_readonly(&base, &admin_tok).await;

    // Trigger a deliberate scope-403.
    let client = reqwest::Client::new();
    let r = client
        .post(format!("{base}/api/admin/drain"))
        .header("Authorization", format!("Bearer {ro_tok}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 403);

    // Audit writes are fire-and-forget; poll briefly.
    let mut rejected: Option<agentdb_meta::audit::AuditRecord> = None;
    for _ in 0..40 {
        let rows = agentdb_meta::audit::list(meta.pool(), &Default::default())
            .await
            .unwrap();
        if let Some(r) = rows
            .into_iter()
            .find(|r| r.path == "/api/admin/drain" && r.status == 403)
        {
            rejected = Some(r);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    let record = rejected.expect("scope-403 row missing from audit log");
    assert_eq!(record.method, "POST");
    assert!(
        record.actor_token_id.is_some(),
        "a scope-403 still has an authenticated actor",
    );
}

#[tokio::test]
async fn token_list_surfaces_scope() {
    let (_d, _o, _m, _meta, base) = spawn().await;
    let admin_tok = mint_admin(&base).await;
    let _ = mint_readonly(&base, &admin_tok).await;

    let client = reqwest::Client::new();
    let v: Value = client
        .get(format!("{base}/api/admin/tokens"))
        .header("Authorization", format!("Bearer {admin_tok}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let scopes: Vec<&str> = v
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["scope"].as_str().unwrap())
        .collect();
    assert!(scopes.contains(&"admin"));
    assert!(scopes.contains(&"readonly"));
}

#[tokio::test]
async fn second_mint_without_explicit_scope_defaults_to_admin() {
    // Back-compat check: callers that don't know about scope
    // keep working — mint without a `scope` field in the body
    // defaults to admin server-side.
    let (_d, _o, _m, _meta, base) = spawn().await;
    let admin_tok = mint_admin(&base).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/admin/tokens"))
        .header("Authorization", format!("Bearer {admin_tok}"))
        .json(&json!({"description": "legacy-client"})) // no scope field
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["scope"].as_str(), Some("admin"), "default should be admin: {v}");
}
