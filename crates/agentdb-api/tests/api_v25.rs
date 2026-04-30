mod common;

use common::spawn_app;
use serde_json::json;

async fn seed(app: &common::TestApp) -> (String, String, String) {
    let tenant: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({"external_key": "api-v25", "policy": {}}))
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
    let sid = session["session_id"].as_str().unwrap().to_string();
    (tid, aid, sid)
}

async fn append(app: &common::TestApp, tid: &str, aid: &str, sid: &str, kind: &str, n: u64) {
    app.client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": kind,
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

#[tokio::test]
async fn fork_replay_and_subscribe_snapshot_work_over_http() {
    let app = spawn_app().await;
    let (tid, aid, sid) = seed(&app).await;
    append(&app, &tid, &aid, &sid, "parent", 0).await;
    append(&app, &tid, &aid, &sid, "parent", 1).await;
    append(&app, &tid, &aid, &sid, "parent", 2).await;

    let child: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/fork",
            app.base
        ))
        .json(&json!({"at_seq": 1, "alias": "child"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let child_sid = child["session_id"].as_str().unwrap();
    append(&app, &tid, &aid, child_sid, "child", 10).await;

    let replay: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{child_sid}/replay",
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
    let kinds: Vec<_> = replay["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| event["kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, vec!["parent", "parent", "child"]);

    let sse = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/subscribe?from_seq=1",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(sse.contains("event: event"));
    assert!(sse.contains("\"seq\":1"));
    assert!(sse.contains("\"seq\":2"));
    assert!(!sse.contains("\"seq\":0"));
}

#[tokio::test]
async fn context_and_watch_work_over_http() {
    let app = spawn_app().await;
    let (tid, aid, sid) = seed(&app).await;
    append(&app, &tid, &aid, &sid, "message", 1).await;
    append(&app, &tid, &aid, &sid, "message", 2).await;
    app.client
        .post(format!("{}/api/tenants/{tid}/records", app.base))
        .json(&json!({
            "namespace": ["mem"],
            "text": "renewal note",
            "policy": {"mode": "summary_only", "fields": [], "vector_allowed": false}
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let context: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/context",
            app.base
        ))
        .json(&json!({
            "max_events": 1,
            "max_bytes": 8000,
            "include": ["recent_events", "records", "policy"],
            "record_query": {"namespace": ["mem"], "text": "renewal"}
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(context["events"].as_array().unwrap().len(), 1);
    assert_eq!(context["records"].as_array().unwrap().len(), 1);
    assert!(context["policy"].is_object());

    let items: serde_json::Value = app
        .client
        .get(format!("{}/api/tenants/{tid}/watch?scope=tenant", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(items.as_array().unwrap().len(), 2);
    let cursor = items[0]["cursor"].as_str().unwrap();
    let after: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/watch?scope=agent&agent_id={aid}&from_cursor={cursor}",
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
    assert_eq!(after.as_array().unwrap().len(), 1);
}
