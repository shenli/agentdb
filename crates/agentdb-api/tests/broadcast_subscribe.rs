//! Phase 2 Task 5 — `GET .../channels/:channel/subscribe` SSE handler.
//!
//! Covers fan-out (every subscriber sees every publish), per-channel
//! isolation, and cross-tenant isolation. JWT + no_echo + lag-policy
//! tests come later (Tasks 8 / 9).

mod common;
use common::{mint_hmac_token_with_channels, TestServer};

#[tokio::test]
async fn multiple_subscribers_each_receive_every_publish() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token = mint_hmac_token_with_channels(b"secret", "t-1", "*", Some(&["*"]), 60);

    let mut s1 = server.open_sse_with_bearer("/api/channels/c-1/subscribe", &token).await;
    let mut s2 = server.open_sse_with_bearer("/api/channels/c-1/subscribe", &token).await;

    // Brief settle so both subscribers are attached to the bus
    // before the first publish — open_sse returns once the response
    // headers land, but the unfold task may not have polled `recv`
    // yet.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    server
        .post_json_bearer(
            "/api/channels/c-1/publish",
            serde_json::json!({"i": 1}),
            &token,
        )
        .await;
    server
        .post_json_bearer(
            "/api/channels/c-1/publish",
            serde_json::json!({"i": 2}),
            &token,
        )
        .await;

    for sub in [&mut s1, &mut s2] {
        let mut got = vec![];
        for _ in 0..2 {
            let frame = sub.next_event().await;
            let env: serde_json::Value = serde_json::from_str(&frame.data).unwrap();
            got.push(env["payload"]["i"].as_u64().unwrap());
        }
        got.sort();
        assert_eq!(got, vec![1, 2]);
    }
}

#[tokio::test]
async fn subscribers_on_different_channels_are_isolated() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token = mint_hmac_token_with_channels(b"secret", "t-1", "*", Some(&["*"]), 60);

    let mut s = server.open_sse_with_bearer("/api/channels/c-1/subscribe", &token).await;

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Publish to c-2 — s (subscribed to c-1) must NOT see it.
    server
        .post_json_bearer(
            "/api/channels/c-2/publish",
            serde_json::json!({"x": 1}),
            &token,
        )
        .await;

    // Then publish to c-1 — s sees it.
    server
        .post_json_bearer(
            "/api/channels/c-1/publish",
            serde_json::json!({"x": 2}),
            &token,
        )
        .await;

    let frame = s.next_event().await;
    let env: serde_json::Value = serde_json::from_str(&frame.data).unwrap();
    assert_eq!(env["payload"]["x"], 2);
}

#[tokio::test]
async fn cross_tenant_isolation_holds() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token_t1 = mint_hmac_token_with_channels(b"secret", "t-1", "*", Some(&["*"]), 60);

    let mut s = server.open_sse_with_bearer("/api/channels/c-1/subscribe", &token_t1).await;

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Inject a cross-tenant envelope directly onto the bus — the HTTP
    // v2 publish route requires a JWT whose tenant claim resolves to a
    // configured tenant (t-other has no JWT config in this server), so
    // we bypass HTTP for the "t-other" injection.
    let key_other = agentdb_api::ChannelKey {
        tenant: "t-other".into(),
        channel: "c-1".into(),
    };
    server
        .state
        .broadcast_bus
        .publish(
            &key_other,
            agentdb_api::BroadcastEnvelope {
                tenant: "t-other".into(),
                channel: "c-1".into(),
                ts_ms: 1,
                sender: None,
                payload: serde_json::json!({"leak": true}),
            },
        )
        .await;

    server
        .post_json_bearer(
            "/api/channels/c-1/publish",
            serde_json::json!({"leak": false}),
            &token_t1,
        )
        .await;

    let frame = s.next_event().await;
    let env: serde_json::Value = serde_json::from_str(&frame.data).unwrap();
    assert_eq!(env["payload"]["leak"], false);
    assert_eq!(env["tenant"], "t-1");
}
