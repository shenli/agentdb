//! Pub/sub Phase 1, Task 2 verification: every successful
//! `append_and_fetch` writes one envelope to the process-wide
//! `EventBus`. Subscribers must see the same `seq`, `tenant`,
//! `session`, `kind`, and payload as the durable record.

use std::time::Duration;

use serde_json::json;
use serial_test::serial;

mod common;
use common::TestServer;

#[tokio::test]
#[serial]
async fn append_emits_envelope_with_matching_seq_and_payload() {
    let server = TestServer::start().await;
    let mut rx = server.state.event_bus.subscribe();

    let resp = server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            json!({"event": {"kind": "message", "text": "hi"}, "fetch_last_n": 0}),
        )
        .await;
    assert_eq!(resp.status(), 200);

    let env = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("envelope within 500ms")
        .expect("recv ok");

    assert_eq!(env.tenant, "t-1");
    assert_eq!(env.session, "s-1");
    assert_eq!(env.seq, 1);
    assert_eq!(env.kind.as_deref(), Some("message"));
    assert_eq!(env.payload["text"], "hi");
}

/// The spec (§6.7) says emission MUST happen after the durable
/// write succeeds. We can't observe the kv write directly, but we
/// can prove the bus and the "what would a follow-up read return"
/// are consistent: emit once, then re-read with `fetch_last_n=10`
/// and assert the response includes an event whose seq matches the
/// envelope we already received. If emission fired before the
/// commit, the second request might see a smaller stream than the
/// envelope claimed.
#[tokio::test]
#[serial]
async fn emission_happens_after_durable_write() {
    let server = TestServer::start().await;
    let mut rx = server.state.event_bus.subscribe();

    server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            json!({"event": {"kind": "x"}, "fetch_last_n": 0}),
        )
        .await;
    let env = rx.recv().await.expect("recv ok");

    let resp = server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            json!({"event": {"kind": "probe"}, "fetch_last_n": 10}),
        )
        .await;
    let body: serde_json::Value = resp.json().await.expect("json");
    let returned = body["recent"].as_array().expect("recent array");
    assert!(
        returned.iter().any(|e| e["seq"] == env.seq),
        "expected the durable read to include the seq we already saw on the bus; got {returned:?}"
    );
}
