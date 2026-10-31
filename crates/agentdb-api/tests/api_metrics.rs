//! `GET /metrics` — Prometheus text-format runtime metrics.

mod common;

use common::spawn_app;
use serde_json::json;

#[tokio::test]
async fn metrics_serves_prometheus_text_with_fleet_gauges() {
    let app = spawn_app().await;

    // Create some state so the counts are non-trivial.
    app.client
        .post(format!("{}/api/sessions/open", app.base))
        .json(&json!({ "workspace": "acme", "agent": "a1", "session": "s1" }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let resp = app
        .client
        .get(format!("{}/metrics", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let content_type = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        content_type.starts_with("text/plain"),
        "content-type: {content_type}",
    );

    let body = resp.text().await.unwrap();
    assert!(body.contains("agentdb_up 1"), "body:\n{body}");
    assert!(body.contains("# TYPE agentdb_tenants gauge"));
    assert!(body.contains("agentdb_build_info{version="));
    assert!(body.contains("agentdb_sessions_by_lifecycle{lifecycle=\"warm\"}"));
    // One tenant and one session were just created.
    assert!(
        body.contains("\nagentdb_tenants 1\n"),
        "expected 1 tenant:\n{body}"
    );
    assert!(
        body.contains("\nagentdb_sessions 1\n"),
        "expected 1 session:\n{body}"
    );
}
