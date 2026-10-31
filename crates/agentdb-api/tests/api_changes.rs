//! `GET /api/tenants/{t}/changes` — the one public changefeed over the export
//! ledger.

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

async fn append(app: &TestApp, s: &Session, n: i64) {
    let url = format!(
        "{}/api/tenants/{}/sessions/{}/events",
        app.base, s.tenant_id, s.session_id
    );
    let r = app
        .client
        .post(url)
        .json(&json!({ "agent_id": s.agent_id, "kind": "message", "payload": { "n": n } }))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "append failed: {}", r.status());
}

async fn changes(app: &TestApp, tenant: &str, query: &str) -> Value {
    app.client
        .get(format!(
            "{}/api/tenants/{}/changes?{}",
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
async fn changes_returns_committed_events_and_a_resumable_cursor() {
    let app = spawn_app().await;
    let s = open(&app, "acme", "a1", "s1").await;
    for n in 0..3 {
        append(&app, &s, n).await;
    }

    let page = changes(&app, &s.tenant_id, "limit=100").await;
    let items = page["items"].as_array().unwrap();
    let event_count = items
        .iter()
        .filter(|it| it["object_type"] == "event")
        .count();
    assert_eq!(
        event_count, 3,
        "all three appended events should be in the feed"
    );
    let next = page["next_cursor"]
        .as_str()
        .expect("a next_cursor")
        .to_string();

    // Resuming from the tail yields no further events.
    let tail = changes(
        &app,
        &s.tenant_id,
        &format!("from_cursor={}", urlencode(&next)),
    )
    .await;
    let tail_events = tail["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|it| it["object_type"] == "event")
        .count();
    assert_eq!(
        tail_events, 0,
        "resuming from next_cursor returns no new events"
    );
}

#[tokio::test]
async fn changes_carries_the_fork_edge_control_item() {
    let app = spawn_app().await;
    let s = open(&app, "acme", "a1", "s1").await;
    append(&app, &s, 0).await;

    // Fork the session.
    let fork: Value = app
        .client
        .post(format!(
            "{}/api/tenants/{}/sessions/{}/fork",
            app.base, s.tenant_id, s.session_id
        ))
        .json(&json!({ "at_seq": 0, "alias": "branch" }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(fork["session_id"].is_string());

    let page = changes(&app, &s.tenant_id, "limit=100").await;
    let types: Vec<String> = page["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|it| it["object_type"].as_str().map(str::to_string))
        .collect();
    let has_fork_edge = types.iter().any(|t| t == "fork_edge");
    assert!(
        has_fork_edge,
        "the fork_edge control item must appear in the changefeed; got: {types:?}",
    );
}

#[tokio::test]
async fn changes_backfill_reports_snapshot_and_completion() {
    let app = spawn_app().await;
    let s = open(&app, "acme", "a1", "s1").await;
    append(&app, &s, 0).await;

    let page = changes(&app, &s.tenant_id, "backfill=true&limit=100").await;
    assert!(
        page.get("snapshot_high_watermark").is_some(),
        "backfill must echo a snapshot_high_watermark",
    );
    assert_eq!(
        page["complete"], true,
        "a small backfill under the limit is complete",
    );
}

fn urlencode(s: &str) -> String {
    // Minimal percent-encoding for cursor query values.
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            other => format!("%{:02X}", other as u32),
        })
        .collect()
}
