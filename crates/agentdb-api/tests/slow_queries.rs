//! End-to-end: slow-query recording and the admin read
//! surface.
//!
//! The test attaches a buffer with `threshold_ms=0` so every
//! request records (0 is "disabled" via threshold compare;
//! we pass 1 to guarantee inclusion of sub-ms append paths).

use std::sync::Arc;

use agentdb_api::{router, AppState, AuthConfig, StreamBus};
use agentdb_core::{BoxRegistry, HlcClock, SessionRegistry, SlowQueryBuffer, TenantRegistry};
use agentdb_meta::MetaStore;
use agentdb_storage::{KvStore, RocksKvStore};
use axum::Extension;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde_json::{json, Value};
use tempfile::TempDir;

async fn spawn(threshold_ms: u64, capacity: usize) -> (TempDir, TempDir, tempfile::NamedTempFile, String, Arc<SlowQueryBuffer>) {
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
            "slow-query-test-node".into(),
        )
        .await
        .unwrap(),
    );
    let buf = Arc::new(SlowQueryBuffer::new(threshold_ms, capacity));
    registry
        .attach_slow_queries(buf.clone())
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
    (dir, obj_dir, meta_file, format!("http://{addr}"), buf)
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
    assert!(resp.status().is_success(), "create_user failed: {}", resp.text().await.unwrap());
}

fn basic(user: &str, pass: &str) -> String {
    format!("Basic {}", B64.encode(format!("{user}:{pass}").as_bytes()))
}

async fn append(client: &reqwest::Client, base: &str, tenant: &str, session: &str, auth: &str) {
    let r = client
        .post(format!("{base}/api/sessions/{session}/append_and_fetch"))
        .header("Authorization", auth)
        .json(&json!({"event": {"msg": "hi"}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200, "append failed for {tenant}/{session}: {}", r.text().await.unwrap());
}

#[tokio::test]
async fn append_gets_recorded_when_above_threshold() {
    // Threshold 0 records every request, regardless of how
    // fast — keeps the test deterministic across machines.
    let (_d, _o, _m, base, _buf) = spawn(0, 64).await;
    let client = reqwest::Client::new();
    create_user(&base, "t1-user", "t-1", "pass").await;
    let auth = basic("t1-user", "pass");
    append(&client, &base, "t-1", "s-1", &auth).await;

    let v: Value = client
        .get(format!("{base}/api/admin/slow-queries"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let arr = v.as_array().expect("slow-queries body is an array");
    assert!(
        !arr.is_empty(),
        "expected at least one record; got: {v}"
    );
    let first = &arr[0];
    assert_eq!(first["kind"].as_str(), Some("append"));
    assert_eq!(first["tenant_id"].as_str(), Some("t-1"));
    assert_eq!(first["session_id"].as_str(), Some("s-1"));
    // latency_ms is unsigned; we only assert the field is
    // present and parseable — a fast machine can legitimately
    // record < 1ms which still satisfies "threshold = 0".
    assert!(first["latency_ms"].as_u64().is_some(), "latency_ms missing: {first}");
    assert!(
        first["detail"]
            .as_str()
            .unwrap_or("")
            .contains("payload_bytes="),
        "detail should summarize the payload: {first}",
    );
}

#[tokio::test]
async fn fast_requests_are_not_recorded_when_below_threshold() {
    // threshold 60s — nothing in a hot-path test will be slow.
    let (_d, _o, _m, base, buf) = spawn(60_000, 64).await;
    let client = reqwest::Client::new();
    create_user(&base, "t1-user", "t-1", "pass").await;
    let auth = basic("t1-user", "pass");
    for _ in 0..5 {
        append(&client, &base, "t-1", "s-1", &auth).await;
    }
    assert_eq!(buf.len(), 0, "no appends should clear a 60s threshold");
}

#[tokio::test]
async fn endpoint_returns_empty_array_when_no_buffer_attached() {
    // Spin up a server without attaching a buffer — simulates
    // `AGENTDB_SLOW_QUERY_DISABLE=1`. The admin endpoint
    // responds with `[]` rather than a 5xx so dashboards
    // don't special-case disabled mode.
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
            "slow-query-nobuf-node".into(),
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
    let base = format!("http://{addr}");

    let client = reqwest::Client::new();
    create_user(&base, "t1-user", "t-1", "pass").await;
    let auth = basic("t1-user", "pass");
    append(&client, &base, "t-1", "s-1", &auth).await;

    let v: Value = client
        .get(format!("{base}/api/admin/slow-queries"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn filter_by_tenant_is_honored() {
    let (_d, _o, _m, base, _buf) = spawn(0, 64).await;
    let client = reqwest::Client::new();
    create_user(&base, "alpha-user", "alpha", "pass").await;
    create_user(&base, "beta-user", "beta", "pass2").await;
    let auth_alpha = basic("alpha-user", "pass");
    let auth_beta = basic("beta-user", "pass2");
    append(&client, &base, "alpha", "s-alpha", &auth_alpha).await;
    append(&client, &base, "beta", "s-beta", &auth_beta).await;

    let v: Value = client
        .get(format!("{base}/api/admin/slow-queries?tenant=alpha"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let arr = v.as_array().unwrap();
    assert!(!arr.is_empty());
    assert!(
        arr.iter().all(|r| r["tenant_id"].as_str() == Some("alpha")),
        "tenant filter must be exact: {v}",
    );
}

#[tokio::test]
async fn metrics_surface_records_per_kind_counter() {
    let (_d, _o, _m, base, _buf) = spawn(0, 64).await;
    let client = reqwest::Client::new();
    create_user(&base, "t1-user", "t-1", "pass").await;
    let auth = basic("t1-user", "pass");
    append(&client, &base, "t-1", "s-1", &auth).await;
    append(&client, &base, "t-1", "s-2", &auth).await;

    let body = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    // Exposition emits one series per kind regardless of
    // whether the counter is zero, so the "append" series
    // must show a positive count.
    let line = body
        .lines()
        .find(|l| l.starts_with("agentdb_slow_queries_recorded_total{kind=\"append\"}"))
        .expect("missing per-kind append counter line");
    let n: u64 = line.rsplit_once(' ').unwrap().1.parse().unwrap();
    assert!(n >= 2, "expected >= 2 append records, got line: {line}");
}

#[tokio::test]
async fn ring_evicts_oldest_when_over_capacity() {
    let (_d, _o, _m, base, buf) = spawn(0, 3).await;
    let client = reqwest::Client::new();
    create_user(&base, "t1-user", "t-1", "pass").await;
    let auth = basic("t1-user", "pass");
    for i in 0..6 {
        append(&client, &base, "t-1", &format!("s-{i}"), &auth).await;
    }
    assert_eq!(buf.len(), 3, "ring size must clamp to capacity");
    let v: Value = client
        .get(format!("{base}/api/admin/slow-queries"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    // Newest three session ids survive; listing is newest-first.
    let sessions: Vec<&str> = arr.iter().map(|r| r["session_id"].as_str().unwrap()).collect();
    assert_eq!(sessions, vec!["s-5", "s-4", "s-3"]);
}
