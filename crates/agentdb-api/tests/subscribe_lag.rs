//! Per-session subscribe lag-too-far close behavior. Spec §6.6.
//!
//! Migrated to v2 route `/api/sessions/{s}/subscribe` with Basic auth.

mod common;

use std::time::Duration;

use agentdb_api::event_bus::EventEnvelope;
use agentdb_core::hlc::HlcTimestamp;
use common::{seed_basic_user, TestServer};

fn synthetic_env(seq: u64) -> EventEnvelope {
    EventEnvelope {
        tenant: "t-1".into(),
        session: "s-1".into(),
        seq,
        ts_ms: 1_000 + seq,
        hlc_ts: HlcTimestamp {
            physical_ns: 0,
            logical: 0,
        },
        actor_id: None,
        kind: Some("msg".into()),
        payload: serde_json::json!({"n": seq}),
    }
}

#[tokio::test]
async fn slow_subscriber_receives_lag_too_far_then_close() {
    // Force lag with a tiny `EventBus` capacity. The bus uses
    // overflow-mode broadcast (drop-oldest), so once we push past
    // `cap` envelopes faster than the subscriber drains them, the
    // next `recv()` returns `RecvError::Overflowed`.
    let server = TestServer::start_with_basic_auth_and_event_bus_capacity(4).await;
    let auth = seed_basic_user(&server, "alice", "t-1", "pw").await;

    let mut sse = server
        .open_sse_with_basic("/api/sessions/s-1/subscribe", &auth)
        .await;
    sse.skip_subscribed_event().await;

    // Drown the bus directly via the in-process handle. Going
    // through HTTP would take seconds per thousand requests; here
    // we synthesize envelopes that the live-tail filter will accept
    // (matching tenant + session) and pump them in faster than the
    // SSE writer can drain. With cap=4 + 5_000 emits, overflow is
    // certain regardless of how aggressively axum flushes.
    for i in 1..=5_000u64 {
        server.state.event_bus.emit(synthetic_env(i));
    }

    // Drain frames until we see the `error: lag_too_far` envelope
    // followed by EOF. We accept some envelope frames first
    // (whatever the receiver managed to drain before overflow).
    let mut saw_error = false;
    let mut saw_close = false;
    for _ in 0..200 {
        match tokio::time::timeout(Duration::from_secs(5), sse.try_next_event())
            .await
            .expect("try_next_event timed out")
        {
            Some(frame) if frame.event_name.as_deref() == Some("error") => {
                let body: serde_json::Value = serde_json::from_str(&frame.data).unwrap();
                assert_eq!(body["error"], "lag_too_far");
                // `last_delivered_seq` may be 0 if overflow happens
                // before the first deliverable envelope makes it
                // through, or >=1 once at least one event was
                // forwarded. Either is a valid recovery cursor.
                assert!(body["last_delivered_seq"].is_u64());
                saw_error = true;
            }
            Some(_) => continue,
            None => {
                saw_close = true;
                break;
            }
        }
    }
    assert!(saw_error, "expected lag_too_far error frame");
    assert!(saw_close, "expected SSE stream close after error frame");
}
