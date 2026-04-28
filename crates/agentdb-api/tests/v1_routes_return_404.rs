//! M28 v1 removal: verify that all previously-deprecated v1 paths
//! now return 404. This is the safest regression guard — if someone
//! accidentally re-registers a v1 route the test will catch it.
//!
//! Routes verified:
//!   - POST /api/tenants/:tid/sessions/:sid/append_and_fetch
//!   - POST /api/tenants/:tid/sessions/:sid/fetch_recent
//!   - GET  /api/tenants/:tid/sessions/:sid/subscribe
//!   - GET  /api/tenants/:tid/subscribe
//!   - POST /api/tenants/:tid/channels/:cid/publish
//!   - GET  /api/tenants/:tid/channels/:cid/subscribe

mod common;

use common::TestServer;

#[tokio::test]
async fn v1_append_and_fetch_is_404() {
    let server = TestServer::start().await;
    let resp = reqwest::Client::new()
        .post(format!(
            "{}/api/tenants/t-1/sessions/s-1/append_and_fetch",
            server.base
        ))
        .json(&serde_json::json!({"event": {}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404, "v1 append_and_fetch must 404");
}

#[tokio::test]
async fn v1_fetch_recent_is_404() {
    let server = TestServer::start().await;
    let resp = reqwest::Client::new()
        .post(format!(
            "{}/api/tenants/t-1/sessions/s-1/fetch_recent",
            server.base
        ))
        .json(&serde_json::json!({"fetch_last_n": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404, "v1 fetch_recent must 404");
}

#[tokio::test]
async fn v1_session_subscribe_is_404() {
    let server = TestServer::start().await;
    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/tenants/t-1/sessions/s-1/subscribe",
            server.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404, "v1 session subscribe must 404");
}

#[tokio::test]
async fn v1_tenant_subscribe_is_404() {
    let server = TestServer::start().await;
    let resp = reqwest::Client::new()
        .get(format!("{}/api/tenants/t-1/subscribe", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404, "v1 tenant subscribe must 404");
}

#[tokio::test]
async fn v1_channel_publish_is_404() {
    let server = TestServer::start().await;
    let resp = reqwest::Client::new()
        .post(format!(
            "{}/api/tenants/t-1/channels/c-1/publish",
            server.base
        ))
        .json(&serde_json::json!({"hello": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404, "v1 channel publish must 404");
}

#[tokio::test]
async fn v1_channel_subscribe_is_404() {
    let server = TestServer::start().await;
    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/tenants/t-1/channels/c-1/subscribe",
            server.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404, "v1 channel subscribe must 404");
}
