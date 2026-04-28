//! Task 8 — JWT integration tests for the Phase 2 broadcast endpoints.
//!
//! Proves the `channels` claim plumbed through `extract_subscribe_jwt`
//! gates publish + subscribe end-to-end:
//!
//! * wildcard (`channels: "*"`) — publish + subscribe both succeed,
//! * explicit list — only listed channels are accepted, others 403,
//! * missing `channels` claim — every channel is denied (403).
//!
//! Bodies decode as `serde_json::Value` so the assertion can pin the
//! exact `code` field (`channel_not_in_jwt`), guarding against silent
//! drift if the error code is renamed.

mod common;
use common::{mint_hmac_token_with_channels, TestServer};

#[tokio::test]
async fn channels_wildcard_allows_publish_and_subscribe() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token = mint_hmac_token_with_channels(b"secret", "t-1", "*", Some(&["*"]), 60);

    let pub_resp = reqwest::Client::new()
        .post(format!(
            "{}/api/channels/c-1/publish",
            server.base
        ))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"ok": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(pub_resp.status(), 204);

    // Short timeout — `subscribe` keeps the SSE stream open indefinitely;
    // we only need to confirm the headers came back 200.
    let sub_resp = reqwest::Client::new()
        .get(format!(
            "{}/api/channels/c-1/subscribe",
            server.base
        ))
        .header("Authorization", format!("Bearer {token}"))
        .timeout(std::time::Duration::from_millis(500))
        .send()
        .await
        .unwrap();
    assert_eq!(sub_resp.status(), 200);
}

#[tokio::test]
async fn channels_list_excludes_other_channels() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token = mint_hmac_token_with_channels(b"secret", "t-1", "*", Some(&["c-1"]), 60);

    // Allowed channel: 204.
    let resp_allowed = reqwest::Client::new()
        .post(format!(
            "{}/api/channels/c-1/publish",
            server.base
        ))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp_allowed.status(), 204);

    // Disallowed channel: 403 channel_not_in_jwt.
    let resp_denied = reqwest::Client::new()
        .post(format!(
            "{}/api/channels/c-other/publish",
            server.base
        ))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp_denied.status(), 403);
    let body: serde_json::Value = resp_denied.json().await.unwrap();
    assert_eq!(body["code"], "channel_not_in_jwt");
}

#[tokio::test]
async fn missing_channels_claim_returns_403() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    // sessions=* but channels claim is None — every publish should be denied.
    let token = mint_hmac_token_with_channels(b"secret", "t-1", "*", None, 60);

    let resp = reqwest::Client::new()
        .post(format!(
            "{}/api/channels/c-1/publish",
            server.base
        ))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "channel_not_in_jwt");
}
