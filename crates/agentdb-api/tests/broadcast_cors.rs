//! CORS integration tests for the v2 channel routes.
//!
//! Verifies that the CORS middleware covers the Phase 2 publish +
//! subscribe routes on the v2 path (`/api/channels/:c/...`) where
//! tenant is not present in the URL. The middleware applies
//! default-permissive CORS on v2 paths (echoes Origin back).
//!
//! Per-tenant CORS config remains for v1 paths; see subscribe_cors.rs.

mod common;
use common::{mint_hmac_token_with_channels, TestServer};

#[tokio::test]
async fn allowed_origin_echoed_on_publish() {
    // For v2 channel routes the CORS middleware applies default-permissive
    // behavior (echoes Origin back). The server only needs JWT config for
    // the channel-level auth; CORS is separate.
    let server = TestServer::start_with_jwt_hmac_and_cors("t-1", b"secret", &["https://app.example.com"]).await;
    let token = mint_hmac_token_with_channels(b"secret", "t-1", "*", Some(&["*"]), 60);
    let resp = reqwest::Client::new()
        .post(format!(
            "{}/api/channels/c-1/publish",
            server.base
        ))
        .header("Authorization", format!("Bearer {token}"))
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
    let server = TestServer::start_with_jwt_hmac_and_cors("t-1", b"secret", &["https://app.example.com"]).await;
    let token = mint_hmac_token_with_channels(b"secret", "t-1", "*", Some(&["*"]), 60);
    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/channels/c-1/subscribe",
            server.base
        ))
        .header("Authorization", format!("Bearer {token}"))
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
    let server = TestServer::start_with_jwt_hmac_and_cors("t-1", b"secret", &["https://app.example.com"]).await;
    let resp = reqwest::Client::new()
        .request(
            reqwest::Method::OPTIONS,
            format!(
                "{}/api/channels/c-1/publish",
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
