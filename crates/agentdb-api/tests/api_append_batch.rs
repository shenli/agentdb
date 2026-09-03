//! `POST /api/tenants/{t}/sessions/{s}/events/batch` — bulk append (importer
//! ingest path): many ordered events in one write batch.

mod common;

use common::{spawn_app, TestApp};
use serde_json::{json, Value};

struct Session {
    tenant_id: String,
    agent_id: String,
    session_id: String,
}

async fn open(app: &TestApp, workspace: &str, agent: &str, session: &str) -> Session {
    let s: Value = app
        .client
        .post(format!("{}/api/sessions/open", app.base))
        .json(&json!({ "workspace": workspace, "agent": agent, "session": session }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    Session {
        tenant_id: s["tenant_id"].as_str().unwrap().to_string(),
        agent_id: s["agent_id"].as_str().unwrap().to_string(),
        session_id: s["session_id"].as_str().unwrap().to_string(),
    }
}

async fn batch(app: &TestApp, s: &Session, events: Value) -> reqwest::Response {
    app.client
        .post(format!(
            "{}/api/tenants/{}/sessions/{}/events/batch",
            app.base, s.tenant_id, s.session_id
        ))
        .json(&json!({ "agent_id": s.agent_id, "events": events }))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn batch_appends_events_in_one_call_with_contiguous_seqs() {
    let app = spawn_app().await;
    let s = open(&app, "acme", "a1", "s1").await;

    let events: Vec<Value> = (0..5)
        .map(|i| json!({ "kind": "message", "payload": { "n": i } }))
        .collect();
    let resp = batch(&app, &s, json!(events)).await;
    assert!(resp.status().is_success(), "batch failed: {}", resp.status());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["count"], 5);
    let appended = body["appended"].as_array().unwrap();
    assert_eq!(appended.len(), 5);
    for (i, a) in appended.iter().enumerate() {
        assert_eq!(a["seq"], i as u64);
    }

    // Events are readable back in order via the changefeed.
    let page: Value = app
        .client
        .get(format!(
            "{}/api/tenants/{}/changes?scope=session&session_id={}&limit=100",
            app.base, s.tenant_id, s.session_id
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let event_count = page["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|it| it["object_type"] == "event")
        .count();
    assert_eq!(event_count, 5);

    // A second batch continues the seq sequence.
    let resp = batch(&app, &s, json!([{ "kind": "message", "payload": { "n": 5 } }])).await;
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["appended"][0]["seq"], 5);
}

#[tokio::test]
async fn batch_rejects_oversized_request() {
    let app = spawn_app().await;
    let s = open(&app, "acme", "a1", "s1").await;
    let events: Vec<Value> = (0..1025)
        .map(|_| json!({ "kind": "m", "payload": {} }))
        .collect();
    let resp = batch(&app, &s, json!(events)).await;
    assert_eq!(resp.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
}
