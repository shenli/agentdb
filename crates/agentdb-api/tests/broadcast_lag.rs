//! Task 9 — Phase 2 lag-policy integration test.
//!
//! Spec § 5.5: when the broadcast channel overflows (per-channel
//! capacity 256, `set_overflow=true`), the SSE handler silently skips
//! lost messages and KEEPS the stream open. This is the inverse of
//! Phase 1's `lag_too_far` close — broadcast is ephemeral with no
//! replay, so there is no recovery to do; the subscriber just keeps
//! receiving fresh envelopes.

mod common;
use agentdb_api::{BroadcastEnvelope, ChannelKey};
use common::TestServer;

#[tokio::test]
async fn slow_subscriber_loses_messages_but_stream_stays_open() {
    let server = TestServer::start().await;
    let mut sse = server
        .open_sse("/api/tenants/t-1/channels/c-1/subscribe")
        .await;

    // Pump well over per-channel capacity (256). Bypass HTTP for speed:
    // emit directly via the bus so we don't bottleneck on reqwest. We
    // intentionally do NOT poll the SSE response body during this burst
    // — keeping the reqwest body buffer + axum's internal channel +
    // TCP socket buffer all unread is what eventually backpressures the
    // SSE handler's `unfold`, which stops calling `sub.recv()`, which
    // is what lets the broadcast bus's per-channel capacity (256)
    // actually overflow. With a large enough burst the socket buffer
    // fills regardless of OS defaults and overflow is forced.
    let key = ChannelKey {
        tenant: "t-1".into(),
        channel: "c-1".into(),
    };
    // Big payload + big count so we comfortably exceed any reasonable
    // socket-buffer + bus-capacity total even on hosts with generous
    // TCP buffers (macOS / Linux defaults).
    let filler = "x".repeat(2048);
    for i in 0..50_000u64 {
        server
            .state
            .broadcast_bus
            .publish(
                &key,
                BroadcastEnvelope {
                    tenant: "t-1".into(),
                    channel: "c-1".into(),
                    ts_ms: i,
                    sender: None,
                    payload: serde_json::json!({"i": i, "pad": filler}),
                },
            )
            .await;
    }

    // Read until quiescent — count what arrives. Stream must NOT have
    // closed (no error frame, no EOF). The 200ms-per-iter quiet window
    // drains any backlog the SSE writer has buffered before we move on.
    let mut received = 0;
    while let Ok(Some(_)) = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        sse.try_next_event(),
    )
    .await
    {
        received += 1;
    }
    assert!(received > 0, "expected some messages to be delivered");
    assert!(
        received < 50_000,
        "expected to lose some messages to overflow (capacity 256), got {received} of 50000"
    );

    // The stream is still alive — publish one more and verify it
    // arrives. If the handler had closed on overflow this would hang
    // and the outer 2s timeout would fail the test. Skip past any
    // last-moment backlog frames that arrived between the drain loop
    // and the final publish (the bus is async — the writer may still
    // be flushing the tail of the burst when we return from the
    // quiet-window break).
    server
        .state
        .broadcast_bus
        .publish(
            &key,
            BroadcastEnvelope {
                tenant: "t-1".into(),
                channel: "c-1".into(),
                ts_ms: 9999,
                sender: None,
                payload: serde_json::json!({"final": true}),
            },
        )
        .await;
    let final_payload = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let frame = sse.next_event().await;
            let env: serde_json::Value = serde_json::from_str(&frame.data).unwrap();
            if env["payload"]["final"] == true {
                return env;
            }
            // Stragglers from the burst — keep looking for the marker.
        }
    })
    .await
    .expect("stream still alive after overflow");
    assert_eq!(final_payload["payload"]["final"], true);
}
