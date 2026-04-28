//! End-to-end tests: one per REST error code. Asserts HTTP status,
//! headers, and the Stripe-style JSON body shape.
//!
//! Migrated to v2 session routes with Basic auth.

use std::sync::Arc;

use agentdb_api::{router, AppState, AuthConfig, StreamBus};
use agentdb_core::{BoxRegistry, HlcClock, SessionRegistry, TenantRegistry};
use agentdb_meta::MetaStore;
use agentdb_storage::{KvStore, RocksKvStore};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde_json::json;
use serial_test::serial;
use tempfile::TempDir;

async fn spawn() -> (TempDir, TempDir, tempfile::NamedTempFile, String, Arc<BoxRegistry>) {
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
    let registry = Arc::new(
        BoxRegistry::new_with_meta(
            kv.clone(),
            store,
            4,
            Arc::new(HlcClock::default()),
            meta.clone(),
            "test-node".into(),
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
    let app = router(state).layer(axum::Extension(meta));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (dir, obj_dir, meta_file, format!("http://{addr}"), registry)
}

fn basic(user: &str, pass: &str) -> String {
    format!("Basic {}", B64.encode(format!("{user}:{pass}").as_bytes()))
}

async fn create_user(base: &str, user_id: &str, tenant: &str) {
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/admin/users"))
        .json(&json!({
            "user_id": user_id,
            "tenant_external": tenant,
            "password": "pw",
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "create_user failed: {}", resp.status());
}

#[tokio::test]
#[serial]
async fn not_found_emits_typed_body() {
    let (_dir, _obj_dir, _meta_file, base, _registry) = spawn().await;
    let client = reqwest::Client::new();
    let r = client
        .get(format!(
            "{base}/api/admin/sessions/nonexistent?tenant_id=t-x"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["code"], "not_found");
    assert_eq!(body["type"], "invalid_request_error");
    assert!(body["message"].as_str().is_some_and(|m| !m.is_empty()));
    assert!(body["resource"].is_string());
    assert!(body["id"].is_string());
}

#[tokio::test]
#[serial]
async fn invalid_request_emits_typed_body() {
    let (_dir, _obj_dir, _meta_file, base, _registry) = spawn().await;
    let client = reqwest::Client::new();
    let r = client
        .post(format!("{base}/api/admin/tier/force"))
        .json(&json!({"tenant_id": "t-x", "session_id": "s-x", "target": "moon"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["code"], "invalid_request");
    assert_eq!(body["type"], "invalid_request_error");
    assert_eq!(body["field"], "target");
}

#[tokio::test]
#[serial]
async fn quota_exceeded_emits_typed_body_and_retry_after_header() {
    let (_dir, _obj_dir, _meta_file, base, registry) = spawn().await;
    let client = reqwest::Client::new();
    let box_id = registry.route_tenant("t-rl").id;
    create_user(&base, "u-rl", "t-rl").await;
    let auth = basic("u-rl", "pw");

    // Tight per-tenant limit so a small burst trips it.
    client
        .post(format!("{base}/api/admin/boxes/{box_id}/quota"))
        .json(&json!({"max_tenant_qps": 1, "burst": 1}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let url = format!("{base}/api/sessions/s/append_and_fetch");
    let body = json!({"event": {"text": "seed"}, "fetch_last_n": 0});

    // Mint the tenant (first request uses sentinel 0, bypasses per-tenant).
    client.post(&url).header("Authorization", &auth).json(&body).send().await.unwrap();

    let mut saw_429 = false;
    for _ in 0..5 {
        let r = client.post(&url).header("Authorization", &auth).json(&body).send().await.unwrap();
        if r.status() == 429 {
            assert_eq!(
                r.headers().get("retry-after").and_then(|v| v.to_str().ok()),
                Some("1")
            );
            let payload: serde_json::Value = r.json().await.unwrap();
            assert_eq!(payload["code"], "quota_exceeded");
            assert_eq!(payload["type"], "rate_limit_error");
            assert!(payload["scope"].is_string());
            assert!(payload["retry_after_ms"].is_number());
            saw_429 = true;
            break;
        }
    }
    assert!(saw_429, "expected a 429 within 5 requests");
}

#[tokio::test]
#[serial]
async fn fault_injected_emits_typed_body() {
    let (_dir, _obj_dir, _meta_file, base, registry) = spawn().await;
    let client = reqwest::Client::new();
    let box_id = registry.route_tenant("t-fault").id;
    create_user(&base, "u-fault", "t-fault").await;
    let auth = basic("u-fault", "pw");

    client
        .post(format!("{base}/api/admin/boxes/{box_id}/inject_fault"))
        .json(&json!({"kind": "error", "duration_s": 10}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let r = client
        .post(format!("{base}/api/sessions/s/append_and_fetch"))
        .header("Authorization", &auth)
        .json(&json!({"event":{"text":"hi"},"fetch_last_n":0}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["code"], "fault_injected");
    assert_eq!(body["type"], "api_error");
    assert_eq!(body["box_id"], box_id);
    assert_eq!(body["fault_kind"], "error");
}
