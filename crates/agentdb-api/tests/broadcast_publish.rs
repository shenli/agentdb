//! Phase 2 Task 4 — `POST .../channels/:channel/publish` handler.
//!
//! Covers the happy path (204 + envelope on the bus), channel-name
//! validation, body-size cap, and the "no JWT config → fall through
//! with `sender = None`" path.

mod common;

use agentdb_api::broadcast_bus::ChannelKey;
use common::TestServer;

#[tokio::test]
async fn publish_returns_204_and_emits_to_bus() {
    let server = TestServer::start().await;
    let mut sub = server
        .state
        .broadcast_bus
        .subscribe(&ChannelKey {
            tenant: "t-1".into(),
            channel: "c-1".into(),
        })
        .await;

    let resp = reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-1/publish", server.base))
        .json(&serde_json::json!({"kind": "typing"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    let env = tokio::time::timeout(std::time::Duration::from_millis(500), sub.recv())
        .await
        .expect("envelope within 500ms")
        .expect("recv ok");
    assert_eq!(env.tenant, "t-1");
    assert_eq!(env.channel, "c-1");
    assert_eq!(env.payload["kind"], "typing");
    assert!(env.sender.is_none()); // no JWT, no sub
}

#[tokio::test]
async fn publish_with_invalid_channel_name_returns_400() {
    // Channel names with spaces are invalid per spec § 5.7.
    // Use percent-encoded space (%20) so the URL parses but the decoded
    // path fails validation.
    let server = TestServer::start().await;
    let resp = reqwest::Client::new()
        .post(format!(
            "{}/api/tenants/t-1/channels/with%20space/publish",
            server.base
        ))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "channel_name_invalid");
}

#[tokio::test]
async fn publish_too_large_returns_413() {
    let server = TestServer::start().await;
    let big = "x".repeat(20 * 1024); // 20 KiB > 16 KiB cap
    let resp = reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-1/publish", server.base))
        .json(&serde_json::json!({"data": big}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413);
}

#[tokio::test]
async fn publish_with_basic_auth_falls_through_no_jwt_no_sender() {
    // TestServer::start() has no JWT config — extractor returns Ok(None);
    // sender stays None.
    let server = TestServer::start().await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-1/publish", server.base))
        .json(&serde_json::json!({"hello": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
}
