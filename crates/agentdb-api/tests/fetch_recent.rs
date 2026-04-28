//! Integration tests for the read-only `fetch_recent` endpoint.
//!
//! Covers:
//! - Empty session (tenant/session not yet minted) → empty `recent`
//! - Session with N events; `fetch_recent {n: 3}` returns the last 3 in order
//! - `n: 0` → empty array regardless of session size
//! - `n` larger than the session size → returns all events

use std::sync::Arc;

use agentdb_api::{router, AppState, StreamBus};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use serde_json::{json, Value};
use tempfile::TempDir;

struct Env {
    _kv_dir: TempDir,
    _obj_dir: TempDir,
    base: String,
}

async fn spawn() -> Env {
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    let kv_dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
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
        jwt_config: agentdb_api::jwt::JwtConfig::default(),
        cors_config: agentdb_api::cors::CorsConfig::default(),
        broadcast_bus: agentdb_api::BroadcastBus::default(),
    };
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Env {
        _kv_dir: kv_dir,
        _obj_dir: obj_dir,
        base: format!("http://{addr}"),
    }
}

/// Append one event to the (tenant, session) pair via the legacy route.
async fn append(base: &str, tenant: &str, session: &str, payload: Value) {
    reqwest::Client::new()
        .post(format!(
            "{base}/api/tenants/{tenant}/sessions/{session}/append_and_fetch"
        ))
        .json(&json!({"event": payload, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

/// Call the legacy-route fetch_recent and return the parsed JSON body.
async fn fetch_recent(base: &str, tenant: &str, session: &str, n: usize) -> Value {
    let resp = reqwest::Client::new()
        .post(format!(
            "{base}/api/tenants/{tenant}/sessions/{session}/fetch_recent"
        ))
        .json(&json!({"n": n}))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(status, 200, "fetch_recent returned {status}: {body}");
    body
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A session that has never had an event appended still returns
/// `{ "recent": [] }` rather than an error.
#[tokio::test]
async fn empty_session_returns_empty_recent() {
    let env = spawn().await;
    let body = fetch_recent(&env.base, "t-1", "s-new", 10).await;
    assert_eq!(body["recent"], json!([]), "expected empty recent array");
}

/// After appending 5 events, `n: 3` returns the last 3 in ascending
/// sequence order (sorted by ts, which aligns with seq in tests).
#[tokio::test]
async fn fetch_last_3_of_5_events() {
    let env = spawn().await;
    for i in 1u64..=5 {
        append(&env.base, "t-2", "s-5", json!({"i": i})).await;
    }
    let body = fetch_recent(&env.base, "t-2", "s-5", 3).await;
    let recent = body["recent"].as_array().unwrap();
    assert_eq!(recent.len(), 3, "expected 3 events, got: {recent:?}");
    // The last 3 events should have seqs 3, 4, 5.
    let seqs: Vec<u64> = recent
        .iter()
        .map(|e| e["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, vec![3, 4, 5], "unexpected seqs: {seqs:?}");
}

/// `n: 0` must return an empty array even when the session has events.
#[tokio::test]
async fn n_zero_returns_empty() {
    let env = spawn().await;
    for i in 1u64..=3 {
        append(&env.base, "t-3", "s-z", json!({"i": i})).await;
    }
    let body = fetch_recent(&env.base, "t-3", "s-z", 0).await;
    assert_eq!(body["recent"], json!([]), "n=0 should return empty");
}

/// When `n` exceeds the number of stored events, all events are returned.
#[tokio::test]
async fn n_larger_than_session_returns_all() {
    let env = spawn().await;
    for i in 1u64..=2 {
        append(&env.base, "t-4", "s-all", json!({"i": i})).await;
    }
    let body = fetch_recent(&env.base, "t-4", "s-all", 100).await;
    let recent = body["recent"].as_array().unwrap();
    assert_eq!(recent.len(), 2, "expected all 2 events, got: {recent:?}");
    let seqs: Vec<u64> = recent
        .iter()
        .map(|e| e["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, vec![1, 2]);
}

/// The M26 session-keyed variant (`/api/sessions/:sid/fetch_recent`)
/// returns the same result for a session seeded via the legacy route.
/// Without auth configured, the endpoint resolves the tenant from the
/// session locator — but with no meta store the locator is empty and
/// the request will get a 400. We verify it at least reaches the
/// handler (not a 404) and returns a structured error.
#[tokio::test]
async fn session_keyed_route_reachable() {
    let env = spawn().await;
    // Seed a session so *something* exists.
    append(&env.base, "t-5", "s-keyed", json!({"seed": true})).await;

    // Without a meta store, session locator lookup returns None, so
    // the handler returns 400 (cannot resolve tenant). 404 would mean
    // the route is not registered.
    let resp = reqwest::Client::new()
        .post(format!("{}/api/sessions/s-keyed/fetch_recent", env.base))
        .json(&json!({"n": 10}))
        .send()
        .await
        .unwrap();
    assert_ne!(resp.status().as_u16(), 404, "session-keyed route not registered");
    assert_eq!(resp.status().as_u16(), 400, "expected 400 without meta store");
}
