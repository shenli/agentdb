//! `labs` switch route gating: the Labs routes (records/search, context,
//! caller-driven lifecycle / replay-prefix, artifact verify-lifecycle, policy
//! sweeps) are mounted only when `labs` is enabled. The default surface 404s
//! them. Core routes stay mounted in both modes.

mod common;

use agentdb_api::AppState;
use agentdb_core::Runtime;
use common::{spawn_raw, TestApp};
use reqwest::StatusCode;
use serde_json::{json, Value};

/// Open a real (workspace, agent, session) via the core `open` route so we hit
/// Labs routes with valid ids — a bad id would 404 on its own and mask the
/// mount gating.
async fn open_ids(app: &TestApp) -> (String, String) {
    let s: Value = app
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
    (
        s["tenant_id"].as_str().unwrap().to_string(),
        s["session_id"].as_str().unwrap().to_string(),
    )
}

/// (method, path) for a representative Labs route, given real ids.
fn labs_routes(t: &str, s: &str) -> Vec<(&'static str, String)> {
    vec![
        ("POST", format!("/api/tenants/{t}/records")),
        (
            "GET",
            format!("/api/tenants/{t}/records/search?namespace=mem"),
        ),
        ("POST", format!("/api/tenants/{t}/records/upsert")),
        ("POST", format!("/api/tenants/{t}/sessions/{s}/context")),
        (
            "POST",
            format!("/api/tenants/{t}/sessions/{s}/replay-prefix"),
        ),
        ("POST", format!("/api/tenants/{t}/sessions/{s}/lifecycle")),
        ("GET", format!("/api/tenants/{t}/sessions/{s}/storage")),
        ("POST", format!("/api/tenants/{t}/policy/gc-sweep")),
        (
            "GET",
            format!("/api/tenants/{t}/artifacts/verify-lifecycle"),
        ),
    ]
}

async fn status(app: &TestApp, method: &str, path: &str) -> StatusCode {
    let url = format!("{}{}", app.base, path);
    let req = match method {
        "GET" => app.client.get(url),
        "POST" => app.client.post(url).json(&json!({})),
        other => panic!("unhandled method {other}"),
    };
    req.send().await.unwrap().status()
}

#[tokio::test]
async fn labs_routes_are_not_mounted_by_default() {
    let app = spawn_raw(AppState::new(Runtime::in_memory())).await; // labs = false
    let (t, s) = open_ids(&app).await;

    for (method, path) in labs_routes(&t, &s) {
        assert_eq!(
            status(&app, method, &path).await,
            StatusCode::NOT_FOUND,
            "{method} {path} should be 404 when labs is off",
        );
    }

    // Core routes stay mounted.
    assert_eq!(
        status(&app, "GET", "/health/ready").await,
        StatusCode::OK,
        "health must stay mounted",
    );
    assert_ne!(
        status(&app, "GET", &format!("/api/tenants/{t}/sessions/{s}")).await,
        StatusCode::NOT_FOUND,
        "core session get must stay mounted",
    );
    assert_ne!(
        status(
            &app,
            "GET",
            &format!("/api/tenants/{t}/sessions/{s}/replay")
        )
        .await,
        StatusCode::NOT_FOUND,
        "core replay must stay mounted (only replay-prefix is Labs)",
    );
}

#[tokio::test]
async fn labs_routes_are_mounted_when_enabled() {
    let app = spawn_raw(AppState::new(Runtime::in_memory()).with_labs(true)).await;
    let (t, s) = open_ids(&app).await;

    for (method, path) in labs_routes(&t, &s) {
        assert_ne!(
            status(&app, method, &path).await,
            StatusCode::NOT_FOUND,
            "{method} {path} should be mounted when labs is on",
        );
    }
}
