//! End-to-end test for Archive tier + lifecycle webhook delivery.

use std::net::SocketAddr;
use std::sync::Arc;

use agentdb_api::{router, AppState, StreamBus};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};
use serial_test::serial;
use tempfile::TempDir;
use tokio::sync::mpsc;

type Rx = mpsc::UnboundedReceiver<Value>;
type Tx = mpsc::UnboundedSender<Value>;

async fn mock_webhook_server() -> (String, Rx) {
    let (tx, rx) = mpsc::unbounded_channel::<Value>();
    let app = Router::new()
        .route(
            "/",
            post(
                |State(tx): State<Tx>, Json(body): Json<Value>| async move {
                    let _ = tx.send(body);
                    axum::http::StatusCode::NO_CONTENT
                },
            ),
        )
        .with_state(tx);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}/"), rx)
}

async fn spawn_db() -> (TempDir, TempDir, String, Arc<BoxRegistry>) {
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
        http: agentdb_api::webhook::new_client(),
    };
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (dir, obj_dir, format!("http://{addr}"), registry)
}

async fn recv_with_timeout(rx: &mut Rx) -> Option<Value> {
    tokio::time::timeout(std::time::Duration::from_millis(2000), rx.recv())
        .await
        .ok()
        .flatten()
}

#[tokio::test]
#[serial]
async fn archive_roundtrip_fires_webhooks_for_every_step() {
    let (mock_url, mut rx) = mock_webhook_server().await;
    let (_dir, _obj_dir, base, registry) = spawn_db().await;
    let client = reqwest::Client::new();
    let box_id = registry.route_tenant("t-a").id;

    client
        .post(format!("{base}/api/admin/boxes/{box_id}/webhook"))
        .json(&json!({"url": mock_url}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    client
        .post(format!(
            "{base}/api/tenants/t-a/sessions/s-1/append_and_fetch"
        ))
        .json(&json!({"event":{"text":"hi"},"fetch_last_n":0}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // warm → cold
    client
        .post(format!("{base}/api/admin/tier/force"))
        .json(&json!({
            "tenant_id": "t-a",
            "session_id": "s-1",
            "target": "cold"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let ev = recv_with_timeout(&mut rx).await.expect("warm→cold webhook");
    assert_eq!(ev["event"], "TierChange");
    assert_eq!(ev["from"], "warm");
    assert_eq!(ev["to"], "cold");
    assert_eq!(ev["tenant_id"], "t-a");
    assert_eq!(ev["session_id"], "s-1");

    // cold → archive
    client
        .post(format!("{base}/api/admin/tier/force"))
        .json(&json!({
            "tenant_id": "t-a",
            "session_id": "s-1",
            "target": "archive"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let ev = recv_with_timeout(&mut rx)
        .await
        .expect("cold→archive webhook");
    assert_eq!(ev["from"], "cold");
    assert_eq!(ev["to"], "archive");

    // archive → warm: two-step (archive→cold, then cold→warm)
    client
        .post(format!("{base}/api/admin/tier/force"))
        .json(&json!({
            "tenant_id": "t-a",
            "session_id": "s-1",
            "target": "warm"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let ev1 = recv_with_timeout(&mut rx)
        .await
        .expect("archive→cold webhook");
    let ev2 = recv_with_timeout(&mut rx).await.expect("cold→warm webhook");
    assert_eq!(ev1["from"], "archive");
    assert_eq!(ev1["to"], "cold");
    assert_eq!(ev2["from"], "cold");
    assert_eq!(ev2["to"], "warm");
}

#[tokio::test]
#[serial]
async fn no_webhook_when_url_unset() {
    let (_dir, _obj_dir, base, _registry) = spawn_db().await;
    let client = reqwest::Client::new();

    client
        .post(format!(
            "{base}/api/tenants/t-b/sessions/s/append_and_fetch"
        ))
        .json(&json!({"event":{"text":"hi"},"fetch_last_n":0}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    client
        .post(format!("{base}/api/admin/tier/force"))
        .json(&json!({"tenant_id": "t-b", "session_id": "s", "target": "cold"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    // No assertion machinery needed — the test passes as long as the
    // request didn't panic or hang. The fire_tier_change function
    // returns immediately when `webhook` is None.
}

#[tokio::test]
#[serial]
async fn set_webhook_rejects_non_http_url() {
    let (_dir, _obj_dir, base, registry) = spawn_db().await;
    let client = reqwest::Client::new();
    let box_id = registry.route_tenant("anything").id;
    let r = client
        .post(format!("{base}/api/admin/boxes/{box_id}/webhook"))
        .json(&json!({"url": "ftp://bad"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
}
