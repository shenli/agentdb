// AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
// Scenario: S7 export_and_watch_cursors_are_gap_free_and_sink_ack_safe
// Falsifies: C11, C12, C17
//
// REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

#[path = "../common/mod.rs"]
mod common;

use common::spawn_app;
use serde_json::json;

async fn seed(app: &common::TestApp) -> (String, String, String) {
    let tenant: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({"external_key": "auto-export", "policy": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid = tenant["tenant_id"].as_str().unwrap().to_string();
    let agent: serde_json::Value = app
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
    let aid = agent["agent_id"].as_str().unwrap().to_string();
    let session: serde_json::Value = app
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
    (
        tid,
        aid,
        session["session_id"].as_str().unwrap().to_string(),
    )
}

#[tokio::test]
async fn export_and_watch_cursors_are_gap_free_and_sink_ack_safe() {
    let app = spawn_app().await;
    let (tid, aid, sid) = seed(&app).await;
    for n in 0..6_u64 {
        app.client
            .post(format!(
                "{}/api/tenants/{tid}/sessions/{sid}/events",
                app.base
            ))
            .json(&json!({
                "agent_id": aid,
                "kind": "message",
                "payload": {"n": n},
                "artifact_refs": [],
                "source_refs": []
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    let mut cursor = None;
    let mut seen = Vec::new();
    loop {
        let url = match &cursor {
            Some(cursor) => format!(
                "{}/api/tenants/{tid}/exports/watch?scope=session&session_id={sid}&limit=2&from_cursor={cursor}",
                app.base
            ),
            None => format!(
                "{}/api/tenants/{tid}/exports/watch?scope=session&session_id={sid}&limit=2",
                app.base
            ),
        };
        let page: serde_json::Value = app
            .client
            .get(url)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let items = page["items"].as_array().unwrap();
        if items.is_empty() {
            break;
        }
        for item in items {
            let cursor = item["cursor"].as_str().unwrap().to_string();
            assert!(
                !seen.iter().any(|prior: &String| prior == &cursor),
                "cursor must not duplicate: {cursor}"
            );
            seen.push(cursor);
        }
        cursor = page["next_cursor"].as_str().map(str::to_string);
        if seen.len() >= 6 {
            break;
        }
    }
    assert_eq!(seen.len(), 6);

    let created: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/exports/consumers", app.base))
        .json(&json!({
            "name": "auto-clickhouse",
            "scope": "tenant",
            "include": ["events"]
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(created["last_acked_cursor"].is_null());
    let claimed: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/exports/consumers/auto-clickhouse/claim",
            app.base
        ))
        .json(&json!({"lease_owner": "worker-a", "lease_ms": 60_000}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(claimed["lease_owner"], "worker-a");
    let last_cursor = seen.last().unwrap();
    let acked: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/exports/consumers/auto-clickhouse/ack",
            app.base
        ))
        .json(&json!({
            "lease_owner": "worker-a",
            "cursor": last_cursor,
            "snapshot_high_watermark": last_cursor
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(acked["last_acked_cursor"], json!(last_cursor));
}
