//! Phase 2 Task 5 — `GET .../channels/:channel/subscribe` SSE handler.
//!
//! Covers fan-out (every subscriber sees every publish), per-channel
//! isolation, and cross-tenant isolation. JWT + no_echo + lag-policy
//! tests come later (Tasks 8 / 9).

mod common;
use common::TestServer;

#[tokio::test]
async fn multiple_subscribers_each_receive_every_publish() {
    let server = TestServer::start().await;
    let mut s1 = server.open_sse("/api/tenants/t-1/channels/c-1/subscribe").await;
    let mut s2 = server.open_sse("/api/tenants/t-1/channels/c-1/subscribe").await;

    // Brief settle so both subscribers are attached to the bus
    // before the first publish — open_sse returns once the response
    // headers land, but the unfold task may not have polled `recv`
    // yet.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-1/publish", server.base))
        .json(&serde_json::json!({"i": 1}))
        .send()
        .await
        .unwrap();
    reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-1/publish", server.base))
        .json(&serde_json::json!({"i": 2}))
        .send()
        .await
        .unwrap();

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
    let server = TestServer::start().await;
    let mut s = server.open_sse("/api/tenants/t-1/channels/c-1/subscribe").await;

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Publish to c-2 — s (subscribed to c-1) must NOT see it.
    reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-2/publish", server.base))
        .json(&serde_json::json!({"x": 1}))
        .send()
        .await
        .unwrap();

    // Then publish to c-1 — s sees it.
    reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-1/publish", server.base))
        .json(&serde_json::json!({"x": 2}))
        .send()
        .await
        .unwrap();

    let frame = s.next_event().await;
    let env: serde_json::Value = serde_json::from_str(&frame.data).unwrap();
    assert_eq!(env["payload"]["x"], 2);
}

#[tokio::test]
async fn cross_tenant_isolation_holds() {
    let server = TestServer::start().await;
    let mut s = server.open_sse("/api/tenants/t-1/channels/c-1/subscribe").await;

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    reqwest::Client::new()
        .post(format!("{}/api/tenants/t-other/channels/c-1/publish", server.base))
        .json(&serde_json::json!({"leak": true}))
        .send()
        .await
        .unwrap();
    reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-1/publish", server.base))
        .json(&serde_json::json!({"leak": false}))
        .send()
        .await
        .unwrap();

    let frame = s.next_event().await;
    let env: serde_json::Value = serde_json::from_str(&frame.data).unwrap();
    assert_eq!(env["payload"]["leak"], false);
    assert_eq!(env["tenant"], "t-1");
}
