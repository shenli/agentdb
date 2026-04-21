use std::sync::Arc;

use agentdb_api::{router, AppState, StreamBus};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use serde_json::json;
use serial_test::serial;
use tempfile::TempDir;

async fn spawn() -> (TempDir, TempDir, String) {
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    let dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let state = AppState {
        registry: Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap()),
        tenants: Arc::new(TenantRegistry::new(kv.clone())),
        sessions: Arc::new(SessionRegistry::new(kv)),
        stream: StreamBus::default(),
        http: agentdb_api::webhook::new_client(),
    };
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (dir, obj_dir, format!("http://{addr}"))
}

#[tokio::test]
#[serial]
async fn append_and_fetch_endpoint_round_trip() {
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::new();

    let resp1: serde_json::Value = client
        .post(format!(
            "{base}/api/tenants/t-1/sessions/s-1/append_and_fetch"
        ))
        .json(&json!({"event": {"kind": "hello"}, "fetch_last_n": 5}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp1["seq"], 1);
    assert_eq!(resp1["recent"].as_array().unwrap().len(), 1);
    assert_eq!(resp1["recent"][0]["payload"]["kind"], "hello");

    let resp2: serde_json::Value = client
        .post(format!(
            "{base}/api/tenants/t-1/sessions/s-1/append_and_fetch"
        ))
        .json(&json!({"event": {"kind": "followup"}, "fetch_last_n": 5}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp2["seq"], 2);
    let recent = resp2["recent"].as_array().unwrap();
    assert_eq!(recent.len(), 2);
    assert_eq!(recent[0]["payload"]["kind"], "hello");
    assert_eq!(recent[1]["payload"]["kind"], "followup");
}

#[tokio::test]
#[serial]
async fn fetch_last_n_respects_bound() {
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::new();

    for i in 1..=4 {
        client
            .post(format!(
                "{base}/api/tenants/t-1/sessions/s-bound/append_and_fetch"
            ))
            .json(&json!({"event": {"i": i}, "fetch_last_n": 0}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    let resp: serde_json::Value = client
        .post(format!(
            "{base}/api/tenants/t-1/sessions/s-bound/append_and_fetch"
        ))
        .json(&json!({"event": {"i": 5}, "fetch_last_n": 2}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["seq"], 5);
    let recent = resp["recent"].as_array().unwrap();
    assert_eq!(recent.len(), 2);
    assert_eq!(recent[0]["payload"]["i"], 4);
    assert_eq!(recent[1]["payload"]["i"], 5);
}

#[tokio::test]
#[serial]
async fn admin_force_tier_cold_then_warm_round_trips() {
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::new();

    // Seed an event so the session exists.
    client
        .post(format!(
            "{base}/api/tenants/t-1/sessions/s-1/append_and_fetch"
        ))
        .json(&json!({"event": {"text": "hi"}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let cold: serde_json::Value = client
        .post(format!("{base}/api/admin/tier/force"))
        .json(&json!({
            "tenant_id": "t-1",
            "session_id": "s-1",
            "target": "Cold"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cold["tier"], "cold");

    let warm: serde_json::Value = client
        .post(format!("{base}/api/admin/tier/force"))
        .json(&json!({
            "tenant_id": "t-1",
            "session_id": "s-1",
            "target": "Warm"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(warm["tier"], "warm");
}

#[tokio::test]
#[serial]
async fn fork_endpoint_creates_child_and_fetch_last_n_merges_parent_tail() {
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::new();

    // Seed parent with 2 events.
    for i in 1..=2u64 {
        client
            .post(format!(
                "{base}/api/tenants/t-1/sessions/parent/append_and_fetch"
            ))
            .json(&json!({"event": {"i": i}, "fetch_last_n": 0}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    // Fork.
    let fork: serde_json::Value = client
        .post(format!("{base}/api/sessions/parent/fork"))
        .json(&json!({
            "tenant_id": "t-1",
            "new_session_id": "child"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(fork["new_session_id"], "child");
    assert_eq!(fork["parent_session_id"], "parent");
    assert_eq!(fork["parent_seq_at_fork"], 3);

    // Append to child, request fetch_last_n=5 — should include parent tail + child event.
    let resp: serde_json::Value = client
        .post(format!(
            "{base}/api/tenants/t-1/sessions/child/append_and_fetch"
        ))
        .json(&json!({"event": {"c": 1}, "fetch_last_n": 5}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let recent = resp["recent"].as_array().unwrap();
    assert_eq!(recent.len(), 3);
}
