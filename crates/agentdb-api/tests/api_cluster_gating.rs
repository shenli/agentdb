//! Deployment-mode route gating: the cluster / distributed control-plane routes
//! must be mounted only when `deployment.mode = "cluster"`. Single-node
//! deployments never expose them (404), while the shared admin routes stay
//! mounted in both modes.

mod common;

use agentdb_api::AppState;
use agentdb_core::Runtime;
use common::spawn_raw;
use reqwest::StatusCode;

/// A representative sample of the cluster / distributed control-plane routes.
/// (method, path) pairs; every one should be absent in single-node mode.
fn cluster_routes() -> Vec<(&'static str, &'static str)> {
    vec![
        ("GET", "/api/admin/nodes"),
        ("GET", "/api/admin/cells"),
        ("GET", "/api/admin/tablets"),
        ("GET", "/api/admin/tablet-ranges"),
        ("GET", "/api/admin/tablet-movements"),
        ("GET", "/api/admin/routing"),
        ("GET", "/api/admin/raft"),
        ("GET", "/api/admin/scheduler"),
        ("GET", "/api/admin/cluster/health"),
        ("GET", "/api/admin/raft/snapshots"),
        ("POST", "/api/internal/raft/messages"),
        ("POST", "/api/internal/raft/snapshots"),
        ("POST", "/api/internal/tablet-movements/snapshot/export"),
    ]
}

async fn status(app: &common::TestApp, method: &str, path: &str) -> StatusCode {
    let url = format!("{}{}", app.base, path);
    let req = match method {
        "GET" => app.client.get(url),
        "POST" => app.client.post(url).json(&serde_json::json!({})),
        other => panic!("unhandled method {other}"),
    };
    req.send().await.unwrap().status()
}

#[tokio::test]
async fn cluster_routes_are_not_mounted_in_single_node_mode() {
    // Default AppState is single-node (cluster_mode = false).
    let app = spawn_raw(AppState::new(Runtime::in_memory())).await;

    for (method, path) in cluster_routes() {
        assert_eq!(
            status(&app, method, path).await,
            StatusCode::NOT_FOUND,
            "{method} {path} should be 404 in single-node mode",
        );
    }

    // Sanity: shared, non-cluster surfaces stay mounted in single-node mode.
    assert_eq!(
        status(&app, "GET", "/health/ready").await,
        StatusCode::OK,
        "health route must stay mounted",
    );
    assert_ne!(
        status(&app, "GET", "/api/admin/recovery").await,
        StatusCode::NOT_FOUND,
        "non-cluster admin route must stay mounted in single-node mode",
    );
}

#[tokio::test]
async fn cluster_routes_are_mounted_in_cluster_mode() {
    let app = spawn_raw(AppState::new(Runtime::in_memory()).with_cluster_mode(true)).await;

    for (method, path) in cluster_routes() {
        assert_ne!(
            status(&app, method, path).await,
            StatusCode::NOT_FOUND,
            "{method} {path} should be mounted in cluster mode",
        );
    }
}
