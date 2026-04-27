//! Integration tests for the per-tenant CORS middleware on the
//! subscribe routes (Task 10-2).
//!
//! Covers the four observable behaviors:
//!   1. Allowlisted origin → echoed via Access-Control-Allow-Origin.
//!   2. Non-allowlisted origin → header omitted (browser blocks).
//!   3. OPTIONS preflight → 204 with method/header advertisements.
//!   4. No Origin header → no CORS header on response.

mod common;

use common::TestServer;

#[tokio::test]
async fn allowed_origin_is_echoed() {
    let server = TestServer::start_with_cors("t-1", &["https://app.example.com"]).await;
    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/tenants/t-1/sessions/s-1/subscribe",
            server.base
        ))
        .header("Origin", "https://app.example.com")
        .send()
        .await
        .expect("send GET subscribe");
    assert_eq!(
        resp.headers()
            .get("access-control-allow-origin")
            .expect("CORS header present")
            .to_str()
            .unwrap(),
        "https://app.example.com"
    );
}

#[tokio::test]
async fn disallowed_origin_omits_cors_header() {
    let server = TestServer::start_with_cors("t-1", &["https://app.example.com"]).await;
    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/tenants/t-1/sessions/s-1/subscribe",
            server.base
        ))
        .header("Origin", "https://evil.example.com")
        .send()
        .await
        .expect("send GET subscribe");
    assert!(resp.headers().get("access-control-allow-origin").is_none());
}

#[tokio::test]
async fn options_preflight_returns_204() {
    let server = TestServer::start_with_cors("t-1", &["https://app.example.com"]).await;
    let resp = reqwest::Client::new()
        .request(
            reqwest::Method::OPTIONS,
            format!(
                "{}/api/tenants/t-1/sessions/s-1/subscribe",
                server.base
            ),
        )
        .header("Origin", "https://app.example.com")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .expect("send OPTIONS preflight");
    assert_eq!(resp.status(), 204);
    let methods = resp
        .headers()
        .get("access-control-allow-methods")
        .expect("Access-Control-Allow-Methods present")
        .to_str()
        .unwrap();
    assert!(methods.contains("GET"));
    let headers = resp
        .headers()
        .get("access-control-allow-headers")
        .expect("Access-Control-Allow-Headers present")
        .to_str()
        .unwrap();
    assert!(headers.to_lowercase().contains("authorization"));
}

#[tokio::test]
async fn no_origin_header_means_no_cors_header() {
    let server = TestServer::start_with_cors("t-1", &["https://app.example.com"]).await;
    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/tenants/t-1/sessions/s-1/subscribe",
            server.base
        ))
        .send()
        .await
        .expect("send GET subscribe");
    assert_eq!(resp.status(), 200);
    assert!(resp.headers().get("access-control-allow-origin").is_none());
}
