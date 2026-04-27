//! Phase 2 Task 6: CORS integration tests for the channel routes.
//!
//! Verifies that the per-tenant CORS middleware (Phase 1) covers the
//! Phase 2 publish + subscribe routes after they were lifted out of
//! the Basic-auth `tenant_routes` subtree into the dedicated
//! `channel_routes` subtree.

mod common;
use common::TestServer;

#[tokio::test]
async fn allowed_origin_echoed_on_publish() {
    let server = TestServer::start_with_cors("t-1", &["https://app.example.com"]).await;
    let resp = reqwest::Client::new()
        .post(format!(
            "{}/api/tenants/t-1/channels/c-1/publish",
            server.base
        ))
        .header("Origin", "https://app.example.com")
        .json(&serde_json::json!({"k": "v"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.headers()
            .get("access-control-allow-origin")
            .expect("ACAO header present")
            .to_str()
            .unwrap(),
        "https://app.example.com"
    );
}

#[tokio::test]
async fn allowed_origin_echoed_on_subscribe() {
    let server = TestServer::start_with_cors("t-1", &["https://app.example.com"]).await;
    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/tenants/t-1/channels/c-1/subscribe",
            server.base
        ))
        .header("Origin", "https://app.example.com")
        .timeout(std::time::Duration::from_millis(500))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.headers()
            .get("access-control-allow-origin")
            .expect("ACAO header present")
            .to_str()
            .unwrap(),
        "https://app.example.com"
    );
}

#[tokio::test]
async fn options_preflight_returns_204_with_methods_and_headers() {
    let server = TestServer::start_with_cors("t-1", &["https://app.example.com"]).await;
    let resp = reqwest::Client::new()
        .request(
            reqwest::Method::OPTIONS,
            format!(
                "{}/api/tenants/t-1/channels/c-1/publish",
                server.base
            ),
        )
        .header("Origin", "https://app.example.com")
        .header("Access-Control-Request-Method", "POST")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    let methods = resp
        .headers()
        .get("access-control-allow-methods")
        .expect("ACAM header present")
        .to_str()
        .unwrap();
    assert!(
        methods.contains("POST"),
        "expected ACAM to include POST for channel publish, got: {}",
        methods
    );
}
