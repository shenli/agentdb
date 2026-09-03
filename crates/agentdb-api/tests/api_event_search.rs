//! `GET /api/tenants/{t}/events/search` — scoped, text-only search over the
//! durable event log (a core capability; record search is a separate Labs
//! feature).

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

async fn append(app: &TestApp, s: &Session, kind: &str, payload: Value) {
    let url = format!(
        "{}/api/tenants/{}/sessions/{}/events",
        app.base, s.tenant_id, s.session_id
    );
    let r = app
        .client
        .post(url)
        .json(&json!({ "agent_id": s.agent_id, "kind": kind, "payload": payload }))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "append failed: {}", r.status());
}

async fn search(app: &TestApp, tenant: &str, query: &str) -> Value {
    app.client
        .get(format!(
            "{}/api/tenants/{}/events/search?{}",
            app.base, tenant, query
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test]
async fn search_matches_event_kind_and_payload_text() {
    let app = spawn_app().await;
    let s = open(&app, "acme", "a1", "s1").await;
    append(&app, &s, "tool_call", json!({ "tool": "search_flights" })).await;
    append(&app, &s, "message", json!({ "text": "booked the hotel" })).await;
    append(&app, &s, "message", json!({ "text": "unrelated chatter" })).await;

    // Payload text match.
    let page = search(&app, &s.tenant_id, "q=hotel").await;
    let matches = page["matches"].as_array().unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0]["event"]["kind"], "message");
    assert_eq!(matches[0]["event"]["payload"]["text"], "booked the hotel");
    assert!(matches[0]["score"].as_u64().unwrap() >= 1);

    // Event-kind match.
    let page = search(&app, &s.tenant_id, "q=tool_call").await;
    let matches = page["matches"].as_array().unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0]["event"]["kind"], "tool_call");

    // No hit → empty matches, not an error.
    let page = search(&app, &s.tenant_id, "q=nonexistentterm").await;
    assert!(page["matches"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn search_ranks_by_token_coverage() {
    let app = spawn_app().await;
    let s = open(&app, "acme", "a1", "s1").await;
    append(&app, &s, "message", json!({ "text": "deploy the service" })).await;
    append(&app, &s, "message", json!({ "text": "deploy the service to staging now" })).await;

    let page = search(&app, &s.tenant_id, "q=deploy+staging").await;
    let matches = page["matches"].as_array().unwrap();
    assert_eq!(matches.len(), 2, "both mention deploy");
    // The event containing both tokens ranks first.
    assert_eq!(matches[0]["event"]["payload"]["text"], "deploy the service to staging now");
    assert!(
        matches[0]["score"].as_u64().unwrap() > matches[1]["score"].as_u64().unwrap()
    );
}

#[tokio::test]
async fn search_scopes_to_agent_and_session() {
    let app = spawn_app().await;
    let a = open(&app, "acme", "agent-a", "s1").await;
    let b = open(&app, "acme", "agent-b", "s2").await;
    append(&app, &a, "message", json!({ "text": "shared keyword" })).await;
    append(&app, &b, "message", json!({ "text": "shared keyword" })).await;

    // Tenant scope sees both.
    let page = search(&app, &a.tenant_id, "q=keyword").await;
    assert_eq!(page["matches"].as_array().unwrap().len(), 2);

    // Agent scope sees only agent-a's event.
    let page = search(&app, &a.tenant_id, &format!("q=keyword&agent_id={}", a.agent_id)).await;
    let matches = page["matches"].as_array().unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0]["event"]["agent_id"], a.agent_id);

    // Session scope sees only s2's event.
    let page = search(&app, &b.tenant_id, &format!("q=keyword&session_id={}", b.session_id)).await;
    let matches = page["matches"].as_array().unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0]["event"]["session_id"], b.session_id);
}

#[tokio::test]
async fn search_kind_filter_and_limit_apply() {
    let app = spawn_app().await;
    let s = open(&app, "acme", "a1", "s1").await;
    append(&app, &s, "tool_call", json!({ "text": "alpha" })).await;
    append(&app, &s, "message", json!({ "text": "alpha" })).await;
    append(&app, &s, "message", json!({ "text": "alpha" })).await;

    // kind filter restricts to tool_call.
    let page = search(&app, &s.tenant_id, "q=alpha&kind=tool_call").await;
    let matches = page["matches"].as_array().unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0]["event"]["kind"], "tool_call");

    // limit caps the result count.
    let page = search(&app, &s.tenant_id, "q=alpha&limit=2").await;
    assert_eq!(page["matches"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn search_unknown_agent_is_404() {
    let app = spawn_app().await;
    let s = open(&app, "acme", "a1", "s1").await;
    append(&app, &s, "message", json!({ "text": "hello" })).await;

    let resp = app
        .client
        .get(format!(
            "{}/api/tenants/{}/events/search?q=hello&agent_id=agt_doesnotexist000000000000000",
            app.base, s.tenant_id
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}
