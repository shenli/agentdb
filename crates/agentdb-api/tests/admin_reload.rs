//! End-to-end: `POST /api/admin/reload` flips live
//! subsystem config without restarting the server.
//!
//! This test injects a fake reload engine so we don't have
//! to spin up the full tracing subscriber + binary-side
//! orchestrator. The handler itself is tiny; what matters
//! is that the axum plumbing (Extension extractor,
//! ServiceUnavailable when missing) is correct.

use std::sync::Arc;

use agentdb_api::admin_reload::{ReloadHandle, ReloadSummary};
use agentdb_api::{router, AppState, StreamBus};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use axum::Extension;
use serde_json::Value;
use tempfile::TempDir;

fn make_state() -> (TempDir, TempDir, AppState) {
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    let dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
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
        admin_auth: None,
        jwt_config: agentdb_api::jwt::JwtConfig::default(),
        cors_config: agentdb_api::cors::CorsConfig::default(),
        broadcast_bus: agentdb_api::BroadcastBus::default(),
    };
    (dir, obj_dir, state)
}

async fn spawn_with_reload(engine: ReloadHandle) -> (TempDir, TempDir, String) {
    let (dir, obj_dir, state) = make_state();
    let app = router(state).layer(Extension(engine));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (dir, obj_dir, format!("http://{addr}"))
}

#[tokio::test]
async fn reload_returns_summary_when_engine_is_installed() {
    let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let c = counter.clone();
    let engine: ReloadHandle = Arc::new(move || {
        let c = c.clone();
        Box::pin(async move {
            c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(ReloadSummary {
                slow_query_threshold_ms: 42,
                slow_query_buffer_capacity: 7,
                tracing_filter: "debug".into(),
                tenant_quotas_added: vec!["acme".into()],
                tenant_quotas_removed: vec![],
                reloaded_at_ms: 1_000,
            })
        })
    });
    let (_d, _o, base) = spawn_with_reload(engine).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/admin/reload"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["slow_query_threshold_ms"].as_u64(), Some(42));
    assert_eq!(v["slow_query_buffer_capacity"].as_u64(), Some(7));
    assert_eq!(v["tracing_filter"].as_str(), Some("debug"));
    assert_eq!(
        v["tenant_quotas_added"].as_array().unwrap().len(),
        1
    );
    assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn reload_propagates_engine_error_as_500() {
    let engine: ReloadHandle = Arc::new(move || {
        Box::pin(async move { Err("simulated".into()) })
    });
    let (_d, _o, base) = spawn_with_reload(engine).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/admin/reload"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 500);
    let body = resp.text().await.unwrap();
    assert!(body.contains("simulated"), "got: {body}");
}

#[tokio::test]
async fn reload_returns_503_when_engine_not_installed() {
    // Server with no reload extension layered in.
    let (_d, _o, state) = make_state();
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/admin/reload"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 503);
}
