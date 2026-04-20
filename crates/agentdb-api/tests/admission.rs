//! End-to-end admission-control tests over the REST API.

use std::sync::Arc;

use agentdb_api::{router, AppState, StreamBus};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use serde_json::json;
use serial_test::serial;
use tempfile::TempDir;

async fn spawn() -> (TempDir, TempDir, String, Arc<BoxRegistry>) {
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    let dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
    let state = AppState {
        registry: registry.clone(),
        tenants: Arc::new(TenantRegistry::new(kv.clone())),
        sessions: Arc::new(SessionRegistry::new(kv)),
        stream: StreamBus::default(),
    };
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (dir, obj_dir, format!("http://{addr}"), registry)
}

#[tokio::test]
#[serial]
async fn per_tenant_limit_emits_429_with_retry_after() {
    let (_dir, _obj_dir, base, registry) = spawn().await;
    let client = reqwest::Client::new();

    // Find which box the tenant routes to and configure that one.
    let box_id = registry.route_tenant("t-rl").id;

    client
        .post(format!("{base}/api/admin/boxes/{box_id}/quota"))
        .json(&json!({
            "max_tenant_qps": 2,
            "burst": 2,
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // First request mints the tenant; its admit uses sentinel 0 and bypasses
    // the per-tenant gate. After the mint, the second request onward goes
    // through the per-tenant bucket (capacity 2).
    let url = format!("{base}/api/tenants/t-rl/sessions/s/append_and_fetch");
    let body = json!({"event": {"text": "hi"}, "fetch_last_n": 0});

    let mut statuses: Vec<u16> = Vec::new();
    let mut saw_retry_after = false;
    for _ in 0..7 {
        let r = client.post(&url).json(&body).send().await.unwrap();
        let code = r.status().as_u16();
        if code == 429 && r.headers().get("retry-after").is_some() {
            saw_retry_after = true;
        }
        statuses.push(code);
    }

    let ok_count = statuses.iter().filter(|&&s| s == 200).count();
    let throttled = statuses.iter().filter(|&&s| s == 429).count();
    assert!(
        ok_count >= 2 && ok_count <= 3,
        "expected ~burst(=2)+sentinel(1)=3 admits, got {:?}",
        statuses
    );
    assert!(throttled >= 4, "expected throttles, got {:?}", statuses);
    assert!(saw_retry_after, "no 429 carried Retry-After: {:?}", statuses);
}

#[tokio::test]
#[serial]
async fn refill_allows_new_requests_after_sleep() {
    let (_dir, _obj_dir, base, registry) = spawn().await;
    let client = reqwest::Client::new();
    let box_id = registry.route_tenant("t-rf").id;
    client
        .post(format!("{base}/api/admin/boxes/{box_id}/quota"))
        .json(&json!({"max_tenant_qps": 10, "burst": 1}))
        .send()
        .await
        .unwrap();
    let url = format!("{base}/api/tenants/t-rf/sessions/s/append_and_fetch");
    let body = json!({"event": {"text": "a"}, "fetch_last_n": 0});

    // Sentinel-mint first request (bypasses per-tenant gate).
    let r = client.post(&url).json(&body).send().await.unwrap();
    assert_eq!(r.status(), 200);
    // Drain the burst=1 bucket.
    let r = client.post(&url).json(&body).send().await.unwrap();
    assert_eq!(r.status(), 200);
    // Third is throttled.
    let r = client.post(&url).json(&body).send().await.unwrap();
    assert_eq!(r.status(), 429);
    // Wait for refill: 10 qps → 100ms per token.
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    let r = client.post(&url).json(&body).send().await.unwrap();
    assert_eq!(r.status(), 200);
}

#[tokio::test]
#[serial]
async fn admin_endpoints_are_not_rate_limited() {
    let (_dir, _obj_dir, base, registry) = spawn().await;
    let client = reqwest::Client::new();
    let box_id = registry.route_tenant("t-admin").id;
    client
        .post(format!("{base}/api/admin/boxes/{box_id}/quota"))
        .json(&json!({"max_qps": 1, "burst": 1}))
        .send()
        .await
        .unwrap();
    for _ in 0..10 {
        let r = client
            .get(format!("{base}/api/admin/boxes"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
    }
}
