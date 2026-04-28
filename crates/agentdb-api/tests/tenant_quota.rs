//! End-to-end: global per-tenant rate limits.
//!
//! Spawns a server with a meta store + hydrated
//! TenantQuotaRegistry. Verifies that:
//!   - PUT seeds the bucket and subsequent writes deny with 429
//!     + `scope: tenant_global`
//!   - the bucket refills over time
//!   - DELETE removes enforcement
//!   - the deny counter on GET and on /metrics reflects activity
//!   - an unconfigured tenant is never denied

use std::sync::Arc;

use agentdb_api::{router, AppState, AuthConfig, StreamBus};
use agentdb_core::{BoxRegistry, HlcClock, SessionRegistry, TenantRegistry, TenantQuotaRegistry};
use agentdb_meta::MetaStore;
use agentdb_storage::{KvStore, RocksKvStore};
use axum::Extension;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde_json::{json, Value};
use tempfile::TempDir;

async fn create_user_helper(base: &str, user_id: &str, tenant: &str, password: &str) {
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
    assert!(resp.status().is_success(), "create_user failed: {}", resp.text().await.unwrap());
}

fn basic(user: &str, pass: &str) -> String {
    format!("Basic {}", B64.encode(format!("{user}:{pass}").as_bytes()))
}

async fn spawn() -> (
    TempDir,
    TempDir,
    tempfile::NamedTempFile,
    Arc<MetaStore>,
    String,
) {
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
            2,
            Arc::new(HlcClock::default()),
            meta.clone(),
            "quota-test-node".into(),
        )
        .await
        .unwrap(),
    );
    let quotas = Arc::new(TenantQuotaRegistry::new());
    registry
        .attach_tenant_quotas(quotas.clone())
        .map_err(|_| ())
        .unwrap();

    let state = AppState {
        registry,
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
    (dir, obj_dir, meta_file, meta, format!("http://{addr}"))
}

async fn append(client: &reqwest::Client, base: &str, tenant: &str, auth: &str) -> reqwest::Response {
    client
        .post(format!("{base}/api/sessions/s-1/append_and_fetch"))
        .header("Authorization", auth)
        .json(&json!({"event": {"msg": "hello"}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn unconfigured_tenant_is_never_throttled() {
    let (_d, _o, _m, _meta, base) = spawn().await;
    let client = reqwest::Client::new();
    create_user_helper(&base, "free-tier-user", "free-tier", "pass").await;
    let auth = basic("free-tier-user", "pass");
    for _ in 0..10 {
        let r = append(&client, &base, "free-tier", &auth).await;
        assert_eq!(r.status().as_u16(), 200);
    }
}

#[tokio::test]
async fn configured_tenant_denies_after_burst_with_429() {
    let (_d, _o, _m, _meta, base) = spawn().await;
    let client = reqwest::Client::new();
    create_user_helper(&base, "noisy-user", "noisy", "pass").await;
    let auth = basic("noisy-user", "pass");

    // Mint the tenant with a permissive initial write.
    assert_eq!(append(&client, &base, "noisy", &auth).await.status().as_u16(), 200);

    // Configure a tight quota.
    let r = client
        .put(format!("{base}/api/admin/tenant-quotas/noisy"))
        .json(&json!({"max_qps": 1, "burst": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);

    // First post-quota write spends the burst token.
    assert_eq!(append(&client, &base, "noisy", &auth).await.status().as_u16(), 200);
    // Second should 429.
    let r = append(&client, &base, "noisy", &auth).await;
    assert_eq!(r.status().as_u16(), 429, "expected tenant_global deny");
    let body: Value = r.json().await.unwrap();
    assert_eq!(
        body["scope"].as_str(),
        Some("tenant_global"),
        "error body should carry scope=tenant_global: {body}",
    );
}

#[tokio::test]
async fn bucket_refills_over_time() {
    let (_d, _o, _m, _meta, base) = spawn().await;
    let client = reqwest::Client::new();
    create_user_helper(&base, "refill-user", "refill", "pass").await;
    let auth = basic("refill-user", "pass");

    assert_eq!(append(&client, &base, "refill", &auth).await.status().as_u16(), 200);
    let r = client
        .put(format!("{base}/api/admin/tenant-quotas/refill"))
        .json(&json!({"max_qps": 20, "burst": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);

    // Drain the bucket.
    assert_eq!(append(&client, &base, "refill", &auth).await.status().as_u16(), 200);
    assert_eq!(append(&client, &base, "refill", &auth).await.status().as_u16(), 429);

    // 20 qps = 50ms per token. Wait 150ms to be safe.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(
        append(&client, &base, "refill", &auth).await.status().as_u16(),
        200,
        "token should have refilled"
    );
}

#[tokio::test]
async fn delete_removes_enforcement() {
    let (_d, _o, _m, _meta, base) = spawn().await;
    let client = reqwest::Client::new();
    create_user_helper(&base, "clear-me-user", "clear-me", "pass").await;
    let auth = basic("clear-me-user", "pass");

    assert_eq!(append(&client, &base, "clear-me", &auth).await.status().as_u16(), 200);
    client
        .put(format!("{base}/api/admin/tenant-quotas/clear-me"))
        .json(&json!({"max_qps": 1, "burst": 1}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(append(&client, &base, "clear-me", &auth).await.status().as_u16(), 200);
    assert_eq!(append(&client, &base, "clear-me", &auth).await.status().as_u16(), 429);

    let r = client
        .delete(format!("{base}/api/admin/tenant-quotas/clear-me"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 204);
    for _ in 0..5 {
        assert_eq!(
            append(&client, &base, "clear-me", &auth).await.status().as_u16(),
            200,
            "quota cleared; should never deny"
        );
    }
}

#[tokio::test]
async fn deny_counter_surfaces_on_get_and_metrics() {
    let (_d, _o, _m, _meta, base) = spawn().await;
    let client = reqwest::Client::new();
    create_user_helper(&base, "counted-user", "counted", "pass").await;
    let auth = basic("counted-user", "pass");

    assert_eq!(append(&client, &base, "counted", &auth).await.status().as_u16(), 200);
    client
        .put(format!("{base}/api/admin/tenant-quotas/counted"))
        .json(&json!({"max_qps": 1, "burst": 1}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    // Drain + deny three times.
    let _ = append(&client, &base, "counted", &auth).await;
    for _ in 0..3 {
        assert_eq!(append(&client, &base, "counted", &auth).await.status().as_u16(), 429);
    }

    // GET shows the counter.
    let v: Value = client
        .get(format!("{base}/api/admin/tenant-quotas/counted"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["deny_count"].as_u64(), Some(3), "got {v}");

    // /metrics exposes `agentdb_tenant_quota_denies_total`
    // with a tenant label.
    let body = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        body.contains("agentdb_tenant_quota_denies_total{tenant=\"counted\"} 3"),
        "expected per-tenant deny gauge in /metrics; got:\n{body}"
    );
}

#[tokio::test]
async fn get_returns_404_for_unconfigured_tenant() {
    let (_d, _o, _m, _meta, base) = spawn().await;
    let client = reqwest::Client::new();
    let r = client
        .get(format!("{base}/api/admin/tenant-quotas/never-set"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 404);
}

#[tokio::test]
async fn put_rejects_zero_values_with_400() {
    let (_d, _o, _m, _meta, base) = spawn().await;
    let client = reqwest::Client::new();
    let r = client
        .put(format!("{base}/api/admin/tenant-quotas/acme"))
        .json(&json!({"max_qps": 0, "burst": 10}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 400);
    let r = client
        .put(format!("{base}/api/admin/tenant-quotas/acme"))
        .json(&json!({"max_qps": 10, "burst": 0}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 400);
}
