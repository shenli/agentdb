//! `POST /api/sessions/open` — one-call idempotent resolve-or-create of a
//! (workspace, agent, session) triple.

mod common;

use common::spawn_app;
use reqwest::StatusCode;
use serde_json::json;

async fn open(app: &common::TestApp, ws: &str, agent: &str, session: &str) -> reqwest::Response {
    app.client
        .post(format!("{}/api/sessions/open", app.base))
        .json(&json!({ "workspace": ws, "agent": agent, "session": session }))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn open_creates_the_triple_and_is_idempotent() {
    let app = spawn_app().await;

    let first: serde_json::Value = open(&app, "acme", "sales-agent", "ticket-42")
        .await
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    let session_id = first["session_id"].as_str().unwrap().to_string();
    let tenant_id = first["tenant_id"].as_str().unwrap().to_string();
    let agent_id = first["agent_id"].as_str().unwrap().to_string();
    assert!(session_id.starts_with("ses_"), "session_id: {session_id}");
    assert!(tenant_id.starts_with("ten_"), "tenant_id: {tenant_id}");
    assert!(agent_id.starts_with("agt_"), "agent_id: {agent_id}");
    assert_eq!(first["alias"], "ticket-42");
    assert_eq!(first["next_seq"], 0);

    // Repeating the exact call returns the same rows — no duplicates.
    let again: serde_json::Value = open(&app, "acme", "sales-agent", "ticket-42")
        .await
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(again["session_id"], session_id);
    assert_eq!(again["tenant_id"], tenant_id);
    assert_eq!(again["agent_id"], agent_id);
}

#[tokio::test]
async fn open_reuses_the_workspace_and_agent_for_a_new_session() {
    let app = spawn_app().await;

    let a: serde_json::Value = open(&app, "acme", "sales-agent", "ticket-1")
        .await
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let b: serde_json::Value = open(&app, "acme", "sales-agent", "ticket-2")
        .await
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    // Same workspace + agent → same tenant_id/agent_id, distinct sessions.
    assert_eq!(a["tenant_id"], b["tenant_id"]);
    assert_eq!(a["agent_id"], b["agent_id"]);
    assert_ne!(a["session_id"], b["session_id"]);
    assert_eq!(b["alias"], "ticket-2");
}

#[tokio::test]
async fn open_rejects_a_session_alias_owned_by_another_agent() {
    let app = spawn_app().await;

    open(&app, "acme", "agent-a", "shared-alias")
        .await
        .error_for_status()
        .unwrap();

    // Session aliases are unique per tenant; a different agent cannot claim it.
    let resp = open(&app, "acme", "agent-b", "shared-alias").await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "ALIAS_CONFLICT");
}

#[tokio::test]
async fn open_requires_non_empty_fields() {
    let app = spawn_app().await;

    let resp = app
        .client
        .post(format!("{}/api/sessions/open", app.base))
        .json(&json!({ "workspace": "acme", "agent": "", "session": "s1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}
