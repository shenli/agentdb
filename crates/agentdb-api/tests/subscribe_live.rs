//! Per-session subscribe (live tail). Spec §6.1, §6.2, §6.4.
//!
//! Phase 1 Task 4: connect to `GET
//! /api/tenants/{t}/sessions/{s}/subscribe`, expect a `subscribed`
//! control event, then receive every appended event in seq order.
//! Replay/lag/JWT/CORS land in later tasks.

mod common;

use std::time::Duration;

use common::TestServer;

/// The harness uses a fresh `reqwest::Client` per call inside
/// `post_json` and `open_sse`, so we wrap each `next_event()` in
/// a 5s timeout: a missed wake-up turns into an assertion miss
/// rather than a hung CI run.
async fn next_with_timeout(sse: &mut common::SseStream) -> common::SseFrame {
    tokio::time::timeout(Duration::from_secs(5), sse.next_event())
        .await
        .expect("sse next_event timed out")
}

#[tokio::test]
async fn subscribe_receives_appended_events_in_order() {
    let server = TestServer::start().await;

    let mut sse = server
        .open_sse("/api/tenants/t-1/sessions/s-1/subscribe")
        .await;

    let first = next_with_timeout(&mut sse).await;
    assert_eq!(first.event_name.as_deref(), Some("subscribed"));

    for i in 1..=3 {
        let resp = server
            .post_json(
                "/api/tenants/t-1/sessions/s-1/append_and_fetch",
                serde_json::json!({"event": {"kind": "msg", "n": i}, "fetch_last_n": 0}),
            )
            .await;
        assert!(
            resp.status().is_success(),
            "append failed: {}",
            resp.status()
        );
    }

    let mut received = Vec::new();
    for _ in 0..3 {
        let evt = next_with_timeout(&mut sse).await;
        let env: serde_json::Value = serde_json::from_str(&evt.data).unwrap();
        received.push(env["seq"].as_u64().unwrap());
    }
    assert_eq!(received, vec![1, 2, 3]);
}

#[tokio::test]
async fn subscribe_filters_by_kind() {
    let server = TestServer::start().await;
    let mut sse = server
        .open_sse("/api/tenants/t-1/sessions/s-1/subscribe?kind=keep")
        .await;
    sse.skip_subscribed_event().await;

    server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            serde_json::json!({"event": {"kind": "skip"}, "fetch_last_n": 0}),
        )
        .await;
    server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            serde_json::json!({"event": {"kind": "keep"}, "fetch_last_n": 0}),
        )
        .await;
    let evt = next_with_timeout(&mut sse).await;
    let env: serde_json::Value = serde_json::from_str(&evt.data).unwrap();
    assert_eq!(env["kind"], "keep");
}

/// Spec §6.4: repeated `kind` query keys are OR'd. Verifies the
/// `axum_extra::Query` extractor decodes `?kind=a&kind=b` into a
/// two-element Vec, and that the live-tail loop drops the
/// in-between `c` event but delivers `a` (seq 1) and `b` (seq 3).
#[tokio::test]
async fn subscribe_kind_filter_accepts_multiple_values() {
    let server = TestServer::start().await;
    let mut sse = server
        .open_sse("/api/tenants/t-1/sessions/s-1/subscribe?kind=a&kind=b")
        .await;
    sse.skip_subscribed_event().await;

    for k in ["a", "c", "b"] {
        let resp = server
            .post_json(
                "/api/tenants/t-1/sessions/s-1/append_and_fetch",
                serde_json::json!({"event": {"kind": k}, "fetch_last_n": 0}),
            )
            .await;
        assert!(resp.status().is_success(), "append failed: {}", resp.status());
    }

    let mut got: Vec<(u64, String)> = Vec::new();
    for _ in 0..2 {
        let evt = next_with_timeout(&mut sse).await;
        let env: serde_json::Value = serde_json::from_str(&evt.data).unwrap();
        got.push((
            env["seq"].as_u64().unwrap(),
            env["kind"].as_str().unwrap().to_string(),
        ));
    }
    assert_eq!(got, vec![(1, "a".into()), (3, "b".into())]);
}
