//! `fetch_recent` on the append endpoint: `POST .../events` optionally returns
//! the recent tail in the same call, folding in `append-and-fetch`.

mod common;

use common::spawn_app;
use serde_json::{json, Value};

#[tokio::test]
async fn append_with_fetch_recent_returns_the_tail() {
    let app = spawn_app().await;

    // One-call setup via the open endpoint.
    let session: Value = app
        .client
        .post(format!("{}/api/sessions/open", app.base))
        .json(&json!({ "workspace": "acme", "agent": "a1", "session": "s1" }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tenant_id = session["tenant_id"].as_str().unwrap();
    let agent_id = session["agent_id"].as_str().unwrap().to_string();
    let session_id = session["session_id"].as_str().unwrap();
    let events_url = format!(
        "{}/api/tenants/{}/sessions/{}/events",
        app.base, tenant_id, session_id
    );

    let append = |payload: Value, fetch_recent: Option<u64>| {
        let mut body = json!({ "agent_id": agent_id, "kind": "message", "payload": payload });
        if let Some(n) = fetch_recent {
            body["fetch_recent"] = json!(n);
        }
        app.client.post(&events_url).json(&body).send()
    };

    // Three plain appends.
    for i in 0..3 {
        let r = append(json!({ "n": i }), None).await.unwrap();
        assert!(r.status().is_success(), "plain append failed: {}", r.status());
    }

    // Append with fetch_recent=2 → the response carries the recent tail.
    let with_tail: Value = append(json!({ "n": 3 }), Some(2))
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(with_tail["event_id"].is_string());
    let recent = with_tail["recent"]
        .as_array()
        .expect("fetch_recent response must include a recent tail");
    assert_eq!(recent.len(), 2, "expected the last 2 events");

    // A plain append still returns just the append result (no tail).
    let plain: Value = append(json!({ "n": 4 }), None)
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(plain["event_id"].is_string());
    let empty = plain
        .get("recent")
        .and_then(Value::as_array)
        .is_none_or(|a| a.is_empty());
    assert!(empty, "plain append must not carry a recent tail");
}
