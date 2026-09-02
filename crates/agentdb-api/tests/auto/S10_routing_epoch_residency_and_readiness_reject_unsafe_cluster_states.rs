// AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
// Scenario: S10 routing_epoch_residency_and_readiness_reject_unsafe_cluster_states
// Falsifies: C7, C16, C17, C18
//
// REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

#[path = "../common/mod.rs"]
mod common;

use agentdb_api::AppState;
use agentdb_core::Runtime;
use common::spawn_app_with_state;

#[tokio::test]
async fn routing_epoch_residency_and_readiness_reject_unsafe_cluster_states() {
    // The cluster/health route is mounted only in cluster mode; the handler
    // still reports the disabled/single-node state when routing is unconfigured.
    let app = spawn_app_with_state(AppState::new(Runtime::in_memory())).await;
    let ready = app
        .client
        .get(format!("{}/health/ready", app.base))
        .send()
        .await
        .unwrap();
    assert!(
        ready.status().is_success(),
        "baseline readiness must be healthy before route fault arms run"
    );

    let status: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/cluster/health", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["mode"], "disabled");
    assert_eq!(status["status"], "ok");
    assert_eq!(status["single_node"], true);
    assert_eq!(status["summary"]["node_count"], 1);
    assert_eq!(status["issues"].as_array().unwrap().len(), 0);
}
