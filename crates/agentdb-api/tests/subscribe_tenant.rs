//! Tenant-wide subscribe SSE endpoint. Spec §6.5.
//!
//! Phase 1 Task 7: subscribe to events across all sessions of a
//! tenant, with optional `session=` whitelist, `kind=` filter, and
//! a per-session catch-up cursor encoded as
//! `from_seq_per_session=s-1:N,s-2:M`.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::TestServer;

/// Bound each `next_event()` so a missed wake-up surfaces as an
/// assertion miss instead of hanging CI.
async fn next_with_timeout(sse: &mut common::SseStream) -> common::SseFrame {
    tokio::time::timeout(Duration::from_secs(5), sse.next_event())
        .await
        .expect("sse next_event timed out")
}

#[tokio::test]
async fn tenant_wide_subscription_delivers_events_from_multiple_sessions() {
    let server = TestServer::start().await;
    let mut sse = server.open_sse("/api/tenants/t-1/subscribe").await;
    sse.skip_subscribed_event().await;

    server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            serde_json::json!({"event": {"kind": "msg"}, "fetch_last_n": 0}),
        )
        .await;
    server
        .post_json(
            "/api/tenants/t-1/sessions/s-2/append_and_fetch",
            serde_json::json!({"event": {"kind": "msg"}, "fetch_last_n": 0}),
        )
        .await;

    let mut by_session: HashMap<String, u64> = HashMap::new();
    for _ in 0..2 {
        let e = next_with_timeout(&mut sse).await;
        let env: serde_json::Value = serde_json::from_str(&e.data).unwrap();
        by_session.insert(
            env["session"].as_str().unwrap().into(),
            env["seq"].as_u64().unwrap(),
        );
    }
    assert_eq!(by_session.get("s-1"), Some(&1));
    assert_eq!(by_session.get("s-2"), Some(&1));
}

#[tokio::test]
async fn tenant_wide_with_session_filter_excludes_others() {
    let server = TestServer::start().await;
    let mut sse = server
        .open_sse("/api/tenants/t-1/subscribe?session=s-1")
        .await;
    sse.skip_subscribed_event().await;

    server
        .post_json(
            "/api/tenants/t-1/sessions/s-2/append_and_fetch",
            serde_json::json!({"event": {"kind": "msg"}, "fetch_last_n": 0}),
        )
        .await;
    server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            serde_json::json!({"event": {"kind": "msg"}, "fetch_last_n": 0}),
        )
        .await;
    let evt = next_with_timeout(&mut sse).await;
    let env: serde_json::Value = serde_json::from_str(&evt.data).unwrap();
    assert_eq!(env["session"], "s-1");
}

#[tokio::test]
async fn tenant_wide_does_not_leak_other_tenants() {
    let server = TestServer::start().await;
    let mut sse = server.open_sse("/api/tenants/t-1/subscribe").await;
    sse.skip_subscribed_event().await;

    // Append to a different tenant — should NOT be delivered.
    server
        .post_json(
            "/api/tenants/t-other/sessions/s-x/append_and_fetch",
            serde_json::json!({"event": {"kind": "msg"}, "fetch_last_n": 0}),
        )
        .await;
    server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            serde_json::json!({"event": {"kind": "msg"}, "fetch_last_n": 0}),
        )
        .await;
    let evt = next_with_timeout(&mut sse).await;
    let env: serde_json::Value = serde_json::from_str(&evt.data).unwrap();
    assert_eq!(env["tenant"], "t-1");
    assert_eq!(env["session"], "s-1");
}

#[tokio::test]
async fn tenant_wide_from_seq_per_session_replays_then_tails() {
    let server = TestServer::start().await;
    // Pre-populate two sessions
    for _ in 0..3 {
        server
            .post_json(
                "/api/tenants/t-1/sessions/s-1/append_and_fetch",
                serde_json::json!({"event": {"kind": "msg"}, "fetch_last_n": 0}),
            )
            .await;
    }
    for _ in 0..2 {
        server
            .post_json(
                "/api/tenants/t-1/sessions/s-2/append_and_fetch",
                serde_json::json!({"event": {"kind": "msg"}, "fetch_last_n": 0}),
            )
            .await;
    }
    // Subscribe replaying s-1 from seq 2 and s-2 from seq 1.
    let mut sse = server
        .open_sse("/api/tenants/t-1/subscribe?from_seq_per_session=s-1:2,s-2:1")
        .await;
    sse.skip_subscribed_event().await;

    let mut seen: HashMap<String, Vec<u64>> = HashMap::new();
    // Catch-up: s-1 seqs 2,3 + s-2 seqs 1,2 = 4 frames.
    for _ in 0..4 {
        let e = next_with_timeout(&mut sse).await;
        let env: serde_json::Value = serde_json::from_str(&e.data).unwrap();
        seen.entry(env["session"].as_str().unwrap().into())
            .or_default()
            .push(env["seq"].as_u64().unwrap());
    }
    let mut s1 = seen.remove("s-1").unwrap();
    s1.sort();
    let mut s2 = seen.remove("s-2").unwrap();
    s2.sort();
    assert_eq!(s1, vec![2, 3]);
    assert_eq!(s2, vec![1, 2]);

    // Live tail: append once to each session, both arrive.
    server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            serde_json::json!({"event": {"kind": "msg"}, "fetch_last_n": 0}),
        )
        .await;
    server
        .post_json(
            "/api/tenants/t-1/sessions/s-2/append_and_fetch",
            serde_json::json!({"event": {"kind": "msg"}, "fetch_last_n": 0}),
        )
        .await;
    let mut got: HashMap<String, u64> = HashMap::new();
    for _ in 0..2 {
        let e = next_with_timeout(&mut sse).await;
        let env: serde_json::Value = serde_json::from_str(&e.data).unwrap();
        got.insert(
            env["session"].as_str().unwrap().into(),
            env["seq"].as_u64().unwrap(),
        );
    }
    assert_eq!(got.get("s-1"), Some(&4));
    assert_eq!(got.get("s-2"), Some(&3));
}
