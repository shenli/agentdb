//! M27 Task 4 — integration tests for the v2 channel routes.
//!
//! `/api/channels/:channel/publish` and `/api/channels/:channel/subscribe`
//! derive the tenant from the JWT `tenant` claim rather than carrying it in
//! the URL. Tests cover the five required cases:
//!
//! 1. Missing JWT → 401
//! 2. Valid JWT + allowed channel → 2xx (publish) / 200 + SSE (subscribe)
//! 3. Valid JWT + disallowed channel → 403
//! 4. Query-token SSE happy path → 200 + `text/event-stream`
//! 5. Dual auth (header + query token simultaneously) → 400

mod common;

use common::{mint_hmac_token_with_channels, TestServer};

#[tokio::test]
async fn v2_publish_without_jwt_is_401() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/channels/ch-x/publish", server.base))
        .json(&serde_json::json!({"hello": "world"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401);
}

#[tokio::test]
async fn v2_publish_with_valid_jwt_succeeds() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token = mint_hmac_token_with_channels(b"secret", "t-1", "*", Some(&["ch-x"]), 60);
    let resp = reqwest::Client::new()
        .post(format!("{}/api/channels/ch-x/publish", server.base))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .json(&serde_json::json!({"hello": "world"}))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "expected 2xx, got {}",
        resp.status()
    );
}

#[tokio::test]
async fn v2_publish_with_jwt_for_disallowed_channel_is_403() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token = mint_hmac_token_with_channels(b"secret", "t-1", "*", Some(&["other-ch"]), 60);
    let resp = reqwest::Client::new()
        .post(format!("{}/api/channels/ch-x/publish", server.base))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .json(&serde_json::json!({"hello": "world"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 403);
}

#[tokio::test]
async fn v2_subscribe_with_query_token_succeeds() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token = mint_hmac_token_with_channels(b"secret", "t-1", "*", Some(&["ch-x"]), 60);
    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/channels/ch-x/subscribe?access_token={token}",
            server.base
        ))
        .timeout(std::time::Duration::from_millis(500))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let ct = resp
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap_or(""))
        .unwrap_or("");
    assert!(
        ct.starts_with("text/event-stream"),
        "expected SSE content-type, got {ct}"
    );
}

#[tokio::test]
async fn v2_subscribe_with_dual_auth_is_400() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token = mint_hmac_token_with_channels(b"secret", "t-1", "*", Some(&["ch-x"]), 60);
    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/channels/ch-x/subscribe?access_token={token}",
            server.base
        ))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {token}"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
}
