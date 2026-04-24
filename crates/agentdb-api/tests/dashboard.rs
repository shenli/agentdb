//! Smoke test: the /dashboard tree actually serves our
//! static assets and they include the tabs + endpoint URLs
//! we expect. This catches "mismatched route in HTML" bugs
//! before they hit an operator.

use std::sync::Arc;

use agentdb_api::{router, AppState, StreamBus};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use tempfile::TempDir;

async fn spawn() -> (TempDir, TempDir, String) {
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
        http: agentdb_api::webhook::new_client(),
        auth: None,
        admin_auth: None,
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
async fn dashboard_index_html_has_every_tab() {
    let (_d, _o, base) = spawn().await;
    let html = reqwest::get(format!("{base}/dashboard/"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    // Every tab button lives in the header nav; missing one
    // would mean operators can't reach the feature even if
    // the backend endpoint exists.
    for label in ["Overview", "Slow queries", "Audit log", "Tokens", "Tenant quotas"] {
        assert!(
            html.contains(label),
            "index.html missing tab '{label}':\n{}",
            &html[..html.len().min(400)]
        );
    }
    // The admin-token input is load-bearing — without it,
    // a dashboard served against an auth-enabled server
    // cannot authenticate. Guard against accidental removal.
    assert!(
        html.contains("admin bearer token"),
        "admin-token input should be present in the header"
    );
}

#[tokio::test]
async fn dashboard_app_js_references_every_admin_endpoint() {
    let (_d, _o, base) = spawn().await;
    let js = reqwest::get(format!("{base}/dashboard/app.js"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    for endpoint in [
        "/api/admin/boxes",
        "/api/admin/slow-queries",
        "/api/admin/audit",
        "/api/admin/tokens",
        "/api/admin/tenant-quotas",
    ] {
        assert!(
            js.contains(endpoint),
            "app.js should reference {endpoint}; otherwise that tab can't fetch"
        );
    }
    // Authorization: Bearer wiring — test that we pass the
    // token the operator pastes into the header.
    assert!(
        js.contains("Authorization") && js.contains("Bearer"),
        "app.js must build an Authorization: Bearer header"
    );
}

#[tokio::test]
async fn dashboard_styles_includes_table_and_scope_badge_classes() {
    let (_d, _o, base) = spawn().await;
    let css = reqwest::get(format!("{base}/dashboard/styles.css"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    // The admin tabs render tables + scope badges; make sure
    // the stylesheet agrees with the markup.
    for selector in ["table.data", ".scope-admin", ".scope-readonly", ".panel-head", ".row-403"] {
        assert!(css.contains(selector), "styles.css missing `{selector}`");
    }
}
