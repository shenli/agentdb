//! End-to-end smoke tests for the V2.0 HTTP routes.
//!
//! These tests drive the router over real HTTP via `reqwest`, exercising the
//! same path that an external client would: serde DTO → handler → runtime →
//! JSON response. The runtime is in-memory, so each test gets a clean state.

mod common;

use common::{spawn_app, TestApp};
use serde_json::json;

#[tokio::test]
async fn create_tenant_agent_session_append_fetch_round_trip() {
    let app = spawn_app().await;

    let t: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({"external_key": "acme", "policy": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid = t["tenant_id"].as_str().unwrap().to_string();

    let a: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/agents", app.base))
        .json(&json!({"alias": "rep-1", "config": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let aid = a["agent_id"].as_str().unwrap().to_string();

    let s: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/agents/{aid}/sessions",
            app.base
        ))
        .json(&json!({"alias": null}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let sid = s["session_id"].as_str().unwrap().to_string();

    let r: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {"i": 1},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["seq"].as_u64().unwrap(), 0);

    // fetch_events returns Vec<EventEnvelope> directly (not wrapped).
    let events: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events?from_seq=0&limit=10",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let events = events.as_array().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["seq"].as_u64().unwrap(), 0);
    assert_eq!(events[0]["kind"], "x");
}

#[tokio::test]
async fn append_and_fetch_returns_recent() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let r: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/append-and-fetch",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {},
            "artifact_refs": [],
            "source_refs": [],
            "last": 5,
            "max_bytes": 1_000_000
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["seq"].as_u64().unwrap(), 0);
    let recent = r["recent"].as_array().unwrap();
    assert_eq!(recent.len(), 1);
}

#[tokio::test]
async fn idempotency_key_replays_result() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let body = json!({
        "agent_id": aid,
        "kind": "x",
        "payload": {"v": 1},
        "artifact_refs": [],
        "source_refs": []
    });

    let r1: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .header("Idempotency-Key", "k1")
        .json(&body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let r2: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .header("Idempotency-Key", "k1")
        .json(&body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(r1["event_id"], r2["event_id"]);
    assert_eq!(r1["seq"], r2["seq"]);
}

#[tokio::test]
async fn idempotency_key_conflict_on_payload_change() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let r1 = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .header("Idempotency-Key", "k1")
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {"v": 1},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap();
    assert!(r1.status().is_success());

    let r2 = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .header("Idempotency-Key", "k1")
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {"v": 2},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status().as_u16(), 409);
    let body: serde_json::Value = r2.json().await.unwrap();
    assert_eq!(body["code"], "IDEMPOTENCY_CONFLICT");
}

#[tokio::test]
async fn unknown_tenant_returns_404() {
    let app = spawn_app().await;
    let bogus = "ten_018f0000-0000-7000-8000-000000000000";
    let r = app
        .client
        .get(format!("{}/api/tenants/{bogus}", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 404);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["code"], "TENANT_NOT_FOUND");
}

#[tokio::test]
async fn create_agent_under_unknown_tenant_returns_404() {
    let app = spawn_app().await;
    let bogus = "ten_018f0000-0000-7000-8000-000000000000";
    let r = app
        .client
        .post(format!("{}/api/tenants/{bogus}/agents", app.base))
        .json(&json!({"alias": null, "config": {}}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 404);
}

#[tokio::test]
async fn append_to_unknown_session_returns_404() {
    let app = spawn_app().await;
    let (tid, _aid, _sid) = bootstrap_session(&app).await;
    let bogus = "ses_018f0000-0000-7000-8000-000000000000";
    let r = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{bogus}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": "agt_018f0000-0000-7000-8000-000000000000",
            "kind": "x",
            "payload": {},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 404);
}

async fn bootstrap_session(app: &TestApp) -> (String, String, String) {
    let t: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({"external_key": "test", "policy": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid = t["tenant_id"].as_str().unwrap().to_string();

    let a: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/agents", app.base))
        .json(&json!({"alias": null, "config": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let aid = a["agent_id"].as_str().unwrap().to_string();

    let s: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/agents/{aid}/sessions",
            app.base
        ))
        .json(&json!({"alias": null}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let sid = s["session_id"].as_str().unwrap().to_string();
    (tid, aid, sid)
}
