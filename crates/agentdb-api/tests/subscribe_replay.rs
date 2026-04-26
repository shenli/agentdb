//! Per-session subscribe with `?from_seq=N`. Spec §4.1, §6.3.
//!
//! Phase 1 Task 5: gap-free replay. When a client passes
//! `from_seq=N`, the server first streams events from the
//! durable log in `[N, live_seq)`, then switches to live tail
//! while dropping any envelope with `seq < live_seq` to avoid
//! duplicates.
//!
//! These tests cover the headline differentiator vs Supabase
//! Realtime: gap-free reconnect.

mod common;

use std::time::Duration;

use common::TestServer;

/// Wrap each `next_event()` in a 5s timeout: a missed wake-up
/// turns into an assertion miss rather than a hung CI run.
async fn next_with_timeout(sse: &mut common::SseStream) -> common::SseFrame {
    tokio::time::timeout(Duration::from_secs(5), sse.next_event())
        .await
        .expect("sse next_event timed out")
}

#[tokio::test]
async fn from_seq_replays_then_tails_with_no_gap() {
    let server = TestServer::start().await;

    for i in 1..=5 {
        let resp = server
            .post_json(
                "/api/tenants/t-1/sessions/s-1/append_and_fetch",
                serde_json::json!({"event": {"kind": "msg", "n": i}, "fetch_last_n": 0}),
            )
            .await;
        assert!(resp.status().is_success(), "append failed: {}", resp.status());
    }

    let mut sse = server
        .open_sse("/api/tenants/t-1/sessions/s-1/subscribe?from_seq=3")
        .await;
    let first = next_with_timeout(&mut sse).await;
    assert_eq!(first.event_name.as_deref(), Some("subscribed"));
    let payload: serde_json::Value = serde_json::from_str(&first.data).unwrap();
    assert_eq!(payload["live_seq"], 6);

    // Catch-up: should receive seq 3, 4, 5.
    let mut seqs = Vec::new();
    for _ in 0..3 {
        let e = next_with_timeout(&mut sse).await;
        let env: serde_json::Value = serde_json::from_str(&e.data).unwrap();
        seqs.push(env["seq"].as_u64().unwrap());
    }
    assert_eq!(seqs, vec![3, 4, 5]);

    // Append 2 more — must arrive via live tail.
    for i in 6..=7 {
        let resp = server
            .post_json(
                "/api/tenants/t-1/sessions/s-1/append_and_fetch",
                serde_json::json!({"event": {"kind": "msg", "n": i}, "fetch_last_n": 0}),
            )
            .await;
        assert!(resp.status().is_success());
    }
    for expected in 6..=7u64 {
        let e = next_with_timeout(&mut sse).await;
        let env: serde_json::Value = serde_json::from_str(&e.data).unwrap();
        assert_eq!(env["seq"].as_u64().unwrap(), expected);
    }
}

#[tokio::test]
async fn race_between_catchup_and_live_does_not_duplicate_or_drop() {
    let server = TestServer::start().await;

    for i in 1..=3 {
        let resp = server
            .post_json(
                "/api/tenants/t-1/sessions/s-1/append_and_fetch",
                serde_json::json!({"event": {"kind": "msg", "n": i}, "fetch_last_n": 0}),
            )
            .await;
        assert!(resp.status().is_success());
    }
    let mut sse = server
        .open_sse("/api/tenants/t-1/sessions/s-1/subscribe?from_seq=1")
        .await;
    sse.skip_subscribed_event().await;

    let post_handle = {
        let server = server.clone();
        tokio::spawn(async move {
            for i in 4..=5 {
                server
                    .post_json(
                        "/api/tenants/t-1/sessions/s-1/append_and_fetch",
                        serde_json::json!({"event": {"kind": "msg", "n": i}, "fetch_last_n": 0}),
                    )
                    .await;
            }
        })
    };
    post_handle.await.unwrap();

    let mut all_seqs = Vec::new();
    for _ in 0..5 {
        let e = next_with_timeout(&mut sse).await;
        let env: serde_json::Value = serde_json::from_str(&e.data).unwrap();
        all_seqs.push(env["seq"].as_u64().unwrap());
    }
    all_seqs.sort();
    assert_eq!(all_seqs, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn from_seq_above_live_seq_is_pure_live_tail() {
    let server = TestServer::start().await;
    let resp = server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            serde_json::json!({"event": {"kind": "msg"}, "fetch_last_n": 0}),
        )
        .await;
    assert!(resp.status().is_success());
    let mut sse = server
        .open_sse("/api/tenants/t-1/sessions/s-1/subscribe?from_seq=999")
        .await;
    sse.skip_subscribed_event().await;
    let resp = server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            serde_json::json!({"event": {"kind": "msg"}, "fetch_last_n": 0}),
        )
        .await;
    assert!(resp.status().is_success());
    let evt = next_with_timeout(&mut sse).await;
    let env: serde_json::Value = serde_json::from_str(&evt.data).unwrap();
    assert_eq!(env["seq"].as_u64().unwrap(), 2);
}
