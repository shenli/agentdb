//! Fork defaults: omitting `at_seq` forks at the parent's current tip (inherits
//! the full history); an explicit `at_seq` rewinds-and-branches.

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

async fn append(app: &TestApp, s: &Session, kind: &str) {
    let r = app
        .client
        .post(format!(
            "{}/api/tenants/{}/sessions/{}/events",
            app.base, s.tenant_id, s.session_id
        ))
        .json(&json!({ "agent_id": s.agent_id, "kind": kind, "payload": {} }))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "append failed: {}", r.status());
}

async fn fork(app: &TestApp, s: &Session, body: Value) -> String {
    let child: Value = app
        .client
        .post(format!(
            "{}/api/tenants/{}/sessions/{}/fork",
            app.base, s.tenant_id, s.session_id
        ))
        .json(&body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    child["session_id"].as_str().unwrap().to_string()
}

async fn replay_len(app: &TestApp, tenant: &str, child: &str) -> usize {
    let r: Value = app
        .client
        .get(format!(
            "{}/api/tenants/{}/sessions/{}/replay?from_seq=0&limit=1000",
            app.base, tenant, child
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let evs = r
        .get("events")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    evs.len()
}

#[tokio::test]
async fn fork_without_at_seq_inherits_full_history() {
    let app = spawn_app().await;
    let s = open(&app, "acme", "a1", "parent").await;
    append(&app, &s, "p0").await;
    append(&app, &s, "p1").await;

    // No at_seq -> fork at the tip: child replay = 2 inherited + 1 child = 3.
    let child = fork(&app, &s, json!({ "alias": "child-tip" })).await;
    let child_s = Session {
        tenant_id: s.tenant_id.clone(),
        agent_id: s.agent_id.clone(),
        session_id: child.clone(),
    };
    append(&app, &child_s, "c0").await;
    assert_eq!(
        replay_len(&app, &s.tenant_id, &child).await,
        3,
        "tip fork inherits both parent events"
    );
}

#[tokio::test]
async fn fork_with_explicit_at_seq_rewinds() {
    let app = spawn_app().await;
    let s = open(&app, "acme", "a1", "parent2").await;
    append(&app, &s, "p0").await;
    append(&app, &s, "p1").await;

    // Explicit at_seq=0 -> inherit only seq 0; child replay = 1 inherited + 1 = 2.
    let child = fork(&app, &s, json!({ "at_seq": 0, "alias": "child-rewind" })).await;
    let child_s = Session {
        tenant_id: s.tenant_id.clone(),
        agent_id: s.agent_id.clone(),
        session_id: child.clone(),
    };
    append(&app, &child_s, "c0").await;
    assert_eq!(
        replay_len(&app, &s.tenant_id, &child).await,
        2,
        "explicit at_seq=0 rewinds to the first event"
    );
}
