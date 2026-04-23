//! End-to-end: the admin_auth_middleware writes one audit row
//! per request that reaches `/api/admin/*`, and the audit log
//! endpoint returns them newest-first with the documented filters.
//!
//! The writes are fire-and-forget (spawned). The tests use a
//! short bounded poll to wait for the row rather than a fixed
//! sleep.

use std::sync::Arc;

use agentdb_api::{admin_auth_middleware::AdminAuthConfig, router, AppState, StreamBus};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_meta::MetaStore;
use agentdb_storage::{KvStore, RocksKvStore};
use axum::Extension;
use serde_json::json;
use tempfile::TempDir;

async fn spawn_admin_auth_node() -> (
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
    let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 2).unwrap());
    let state = AppState {
        registry,
        tenants: Arc::new(TenantRegistry::new(kv.clone())),
        sessions: Arc::new(SessionRegistry::new(kv)),
        stream: StreamBus::default(),
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
    (dir, obj_dir, meta_file, meta, format!("http://{addr}"))
}

async fn mint_bootstrap(base: &str) -> String {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/admin/tokens"))
        .json(&json!({"description": "bootstrap"}))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    body["token"].as_str().unwrap().to_string()
}

/// Poll until the audit log has at least `want` rows, or the
/// attempt budget runs out. Audit writes are `tokio::spawn`ed
/// from the middleware, so there's no way to synchronously
/// observe them — this is the shape every test that checks
/// audit rows needs.
async fn wait_for_rows(meta: &MetaStore, want: usize) -> Vec<agentdb_meta::audit::AuditRecord> {
    for _ in 0..40 {
        let rows = agentdb_meta::audit::list(meta.pool(), &Default::default())
            .await
            .unwrap();
        if rows.len() >= want {
            return rows;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("audit row count did not reach {want} within budget");
}

#[tokio::test]
async fn successful_admin_request_is_audited_with_actor() {
    let (_d, _o, _m, meta, base) = spawn_admin_auth_node().await;
    let token = mint_bootstrap(&base).await;
    // Token-list is a handy GET for a signed-in actor.
    let client = reqwest::Client::new();
    let r = client
        .get(format!("{base}/api/admin/tokens"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);

    // Expect at least two rows: the bootstrap mint (actor
    // None, the bootstrap exception path) + the token list
    // above (actor Some).
    let rows = wait_for_rows(&meta, 2).await;
    // Newest first.
    let latest = &rows[0];
    assert_eq!(latest.method, "GET");
    assert_eq!(latest.path, "/api/admin/tokens");
    assert_eq!(latest.status, 200);
    assert!(
        latest.actor_token_id.is_some(),
        "authenticated request should have a non-null actor"
    );
}

#[tokio::test]
async fn bootstrap_mint_is_audited_with_null_actor() {
    let (_d, _o, _m, meta, base) = spawn_admin_auth_node().await;
    let _ = mint_bootstrap(&base).await;
    let rows = wait_for_rows(&meta, 1).await;
    let r = rows
        .iter()
        .find(|r| r.path == "/api/admin/tokens" && r.method == "POST")
        .expect("bootstrap mint row");
    assert_eq!(r.status, 200);
    assert!(
        r.actor_token_id.is_none(),
        "bootstrap mint bypasses auth; actor must be NULL",
    );
}

#[tokio::test]
async fn unauthorized_attempt_is_audited() {
    let (_d, _o, _m, meta, base) = spawn_admin_auth_node().await;
    // Make auth binding so the middleware rejects rather than
    // bootstrapping.
    let _ = mint_bootstrap(&base).await;

    let client = reqwest::Client::new();
    let r = client
        .get(format!("{base}/api/admin/tablets"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 401);

    let rows = wait_for_rows(&meta, 2).await;
    let rejected = rows
        .iter()
        .find(|r| r.path == "/api/admin/tablets" && r.status == 401)
        .expect("401 row for unauthorized tablet list");
    assert!(
        rejected.actor_token_id.is_none(),
        "failed auth has no actor"
    );
}

#[tokio::test]
async fn audit_endpoint_returns_rows_with_filters() {
    let (_d, _o, _m, meta, base) = spawn_admin_auth_node().await;
    let token = mint_bootstrap(&base).await;

    // Produce a mix of rows.
    let client = reqwest::Client::new();
    for _ in 0..3 {
        client
            .get(format!("{base}/api/admin/tablets"))
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
            .unwrap();
    }
    client
        .get(format!("{base}/api/admin/nodes"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();

    // Make sure all writes landed before we read via the
    // HTTP endpoint (the endpoint call itself also writes).
    let _ = wait_for_rows(&meta, 5).await;

    // Read via the public endpoint with a path-prefix filter.
    let resp = client
        .get(format!("{base}/api/admin/audit?path_prefix=/api/admin/tablets"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("audit response is an array");
    assert!(
        arr.iter()
            .all(|r| r["path"].as_str().unwrap().starts_with("/api/admin/tablets")),
        "path_prefix filter must be honored: {body}",
    );
    // Three tablet GETs were made before the audit call.
    assert!(arr.len() >= 3, "expected at least 3 rows, got {}", arr.len());

    // limit=1 → one row back.
    let resp = client
        .get(format!("{base}/api/admin/audit?limit=1"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body.as_array().unwrap().len(), 1);
}
