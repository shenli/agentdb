//! Transport fault injection control plane: the internal-token-gated
//! endpoints that scenarios use to install, inspect, release, and clear
//! deterministic link faults. The message-path effects are exercised by
//! the real-cluster S22 scenario; these tests pin the control surface.

mod common;

use std::sync::Arc;

use agentdb_api::{transport_faults::FaultTable, AppState};
use agentdb_core::Runtime;
use common::spawn_app_with_state;
use serde_json::json;

const TOKEN_HEADER: &str = "X-AgentDB-Internal-Token";

fn faulted_state() -> (AppState, Arc<FaultTable>) {
    let table = Arc::new(FaultTable::new());
    let mut state = AppState::new(Runtime::in_memory()).with_internal_auth_token("secret");
    state.transport_faults = Some(table.clone());
    (state, table)
}

#[tokio::test]
async fn fault_control_round_trip() {
    let (state, table) = faulted_state();
    let app = spawn_app_with_state(state).await;
    let base = format!("{}/api/internal/raft/faults", app.base);

    // Install a rule set.
    let response = app
        .client
        .put(&base)
        .header(TOKEN_HEADER, "secret")
        .json(&json!({
            "rules": [
                {"key": "drop-b", "tablet_id": 1, "to": 20, "direction": "send", "action": "drop"},
                {"key": "slow", "direction": "both", "action": "delay", "ms": 250},
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let snapshot: serde_json::Value = response.json().await.unwrap();
    assert_eq!(snapshot["rules"].as_array().unwrap().len(), 2);

    // The installed rules act on evaluation.
    let msg = agentdb_storage::RaftRsWireMessage {
        tablet_id: 1,
        from: 10,
        to: 20,
        payload: vec![],
    };
    assert_eq!(
        table.evaluate(1, 10, 20, true, &msg),
        agentdb_api::transport_faults::FaultVerdict::Drop
    );

    // Counters are visible.
    let snapshot: serde_json::Value = app
        .client
        .get(&base)
        .header(TOKEN_HEADER, "secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(snapshot["matched"]["drop-b"], 1);

    // Clear wipes rules and reports discarded holds.
    let cleared: serde_json::Value = app
        .client
        .delete(&base)
        .header(TOKEN_HEADER, "secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cleared["cleared"], true);
    assert_eq!(
        table.evaluate(1, 10, 20, true, &msg),
        agentdb_api::transport_faults::FaultVerdict::Pass
    );
}

#[tokio::test]
async fn fault_control_requires_token_and_gate() {
    // Gate off: even with the right token the endpoint 404s.
    let state = AppState::new(Runtime::in_memory()).with_internal_auth_token("secret");
    let app = spawn_app_with_state(state).await;
    let response = app
        .client
        .get(format!("{}/api/internal/raft/faults", app.base))
        .header(TOKEN_HEADER, "secret")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    // Gate on, wrong token: unauthorized before any table access.
    let (state, _table) = faulted_state();
    let app = spawn_app_with_state(state).await;
    let response = app
        .client
        .get(format!("{}/api/internal/raft/faults", app.base))
        .header(TOKEN_HEADER, "wrong")
        .send()
        .await
        .unwrap();
    assert_ne!(response.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn release_reports_missing_infrastructure() {
    let (state, table) = faulted_state();
    table.replace(vec![agentdb_api::transport_faults::FaultRule {
        key: "park".into(),
        tablet_id: None,
        from: None,
        to: None,
        direction: agentdb_api::transport_faults::FaultDirection::Both,
        action: agentdb_api::transport_faults::FaultAction::Hold,
    }]);
    let msg = agentdb_storage::RaftRsWireMessage {
        tablet_id: 7,
        from: 1,
        to: 2,
        payload: vec![9],
    };
    table.evaluate(7, 1, 2, true, &msg);
    let app = spawn_app_with_state(state).await;

    let released: serde_json::Value = app
        .client
        .post(format!("{}/api/internal/raft/faults/release", app.base))
        .header(TOKEN_HEADER, "secret")
        .json(&json!({"key": "park"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    // No raft registry on this app: the held message cannot be re-sent,
    // and the endpoint says so instead of silently dropping it.
    assert_eq!(released["resent"], 0);
    assert_eq!(released["errors"].as_array().unwrap().len(), 1);
}
