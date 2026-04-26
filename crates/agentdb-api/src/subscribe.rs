//! Phase 1 Tasks 4 + 5 + 6: per-session subscribe SSE endpoint.
//!
//! Connects the on-process [`crate::EventBus`] to an HTTP SSE
//! response. Per spec §6.1/§6.2/§6.3/§6.4:
//!
//! 1. Open the receiver BEFORE we resolve `live_seq`, so any
//!    event appended after the resolve is buffered for us.
//! 2. Snapshot `live_seq = session.next_seq` via
//!    [`crate::handlers::resolve_session_next_seq`].
//! 3. Emit a `subscribed` control frame with `{tenant, session,
//!    live_seq}` so the client knows the last seq the server saw
//!    at attach time.
//! 4. If `from_seq < live_seq`, run a catch-up scan over the
//!    half-open range `[from_seq, live_seq)` against the durable
//!    log and stream those events first.
//! 5. Tail the bus, dropping envelopes with `seq < live_seq`
//!    (already delivered via catch-up) or that don't match
//!    `(tenant, session)` or the optional `kind=` filter list.
//!
//! The dedup invariant: catch-up streams events in
//! `[from_seq, live_seq)`; live tail drops `seq < live_seq`. So no
//! event is delivered twice, and none is dropped — assuming the
//! bus subscribe happened before the `live_seq` snapshot, which
//! it does (step 1 before step 2).
//!
//! ## Error frames
//!
//! Two error conditions terminate the stream with an explicit
//! `event: error` frame so the SDK can reconnect gap-free instead
//! of guessing what happened (spec §6.6):
//!
//! - **`lag_too_far`** — the subscriber fell behind the bus
//!   capacity. We capture the last successfully delivered `seq`
//!   and emit it so the client reconnects with
//!   `from_seq = last_delivered_seq + 1`.
//! - **`catchup_failed`** — the durable-log scan in
//!   `[from_seq, live_seq)` returned a KV/snapshot error. Failing
//!   loud is preferable to silently switching to live tail and
//!   leaving a gap in `[from_seq, live_seq)`.
//!
//! Both reuse [`error_frame`] for a consistent wire shape. Task 9d
//! (`jwt_expired`) plugs into the same helper.
//!
//! Multi-node 421, JWT, and CORS are still later tasks — this file
//! stays focused on the per-session subscribe path.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Path, State};
// `axum_extra::extract::Query` uses `serde_html_form` and decodes
// repeated query keys (`?kind=a&kind=b`) into `Vec<String>`. The
// default axum `Query` (backed by `serde_urlencoded`) cannot, so
// we deliberately shadow it here. Spec §6.4: "Multiple
// `kind=foo&kind=bar` parameters are OR'd."
use axum_extra::extract::Query;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::stream::{self, Stream, StreamExt};
use serde::Deserialize;

use crate::event_bus::EventEnvelope;
use crate::AppState;

/// Query string for `GET …/subscribe`. All fields are optional.
///
/// `kind` is repeated-key (`?kind=foo&kind=bar`) per spec §6.4
/// and decoded via `serde_html_form`. An empty value
/// (`?kind=` alone) deserializes to a single empty string, which
/// [`kind_matches`] explicitly drops on the way in via
/// [`normalize_kind_filter`] so it neither matches everything nor
/// matches nothing surprisingly.
#[derive(Debug, Deserialize)]
pub struct SubscribeQuery {
    /// Catch-up cursor (Task 5). When present and below the
    /// snapshotted `live_seq`, the server replays events in
    /// `[from_seq, live_seq)` from the durable log before
    /// switching to live tail. When absent, defaults to
    /// `live_seq` so callers get pure live tail.
    #[serde(default)]
    pub from_seq: Option<u64>,
    /// Repeated-key allow-list. Empty vec ⇒ "deliver every event".
    #[serde(default)]
    pub kind: Vec<String>,
    /// Override the keep-alive interval (default 20s). Tests can
    /// drop this to a small value to assert keep-alive cadence
    /// without a long sleep; production clients leave it default.
    #[serde(default)]
    pub heartbeat_secs: Option<u64>,
}

/// Drop empty / whitespace-only entries so a stray `?kind=` (or
/// `?kind=&kind=foo`) behaves like "no filter on the empty slot"
/// rather than silently disabling the filter or matching nothing.
fn normalize_kind_filter(raw: Vec<String>) -> Vec<String> {
    raw.into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

pub async fn subscribe_session(
    State(state): State<AppState>,
    Path((tenant, session)): Path<(String, String)>,
    Query(q): Query<SubscribeQuery>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // Subscribe FIRST so any event appended between here and the
    // `live_seq` snapshot is buffered for us — closes the
    // attach-vs-append race that gap-free reconnect needs to win
    // (spec §6.3 / §6.4).
    let rx = state.event_bus.subscribe();
    let live_seq = crate::handlers::resolve_session_next_seq(&state, &tenant, &session)
        .await
        .unwrap_or(0);

    let subscribed_payload = serde_json::json!({
        "tenant": tenant,
        "session": session,
        "live_seq": live_seq,
    });
    let subscribed_evt = stream::once(async move {
        Ok::<_, Infallible>(
            Event::default()
                .event("subscribed")
                .data(subscribed_payload.to_string()),
        )
    });

    let kind_filter = normalize_kind_filter(q.kind);

    // Catch-up phase. `from_seq < live_seq` ⇒ the client missed
    // events in `[from_seq, live_seq)`; replay them from the
    // durable log. `from_seq` defaults to `live_seq` so an absent
    // query parameter is "pure live tail, no catch-up".
    //
    // Spec §6.6: on scan failure we send `catchup_failed` and
    // close, rather than silently degrading to live tail (which
    // would leave the client with a gap in `[from_seq, live_seq)`
    // that nothing on the wire signals).
    let from_seq = q.from_seq.unwrap_or(live_seq);
    let keepalive_secs = q.heartbeat_secs.unwrap_or(20);

    let catchup_envs: Vec<EventEnvelope> = if from_seq < live_seq {
        match crate::handlers::scan_events_from_seq(&state, &tenant, &session, from_seq, live_seq)
            .await
        {
            Ok(events) => events
                .into_iter()
                .map(|ev| crate::event_bus::envelope_from_event(&tenant, &session, &ev))
                .collect(),
            Err(e) => {
                // Spec §6.6: catch-up failure is loud — we emit a
                // `subscribed` (so the client sees the cursor we
                // tried for) followed by an `error: catchup_failed`
                // frame, then close. Silently degrading to live tail
                // would leave a gap in `[from_seq, live_seq)` that
                // the wire doesn't signal.
                tracing::warn!(
                    error = ?e,
                    tenant = %tenant,
                    session = %session,
                    from_seq,
                    live_seq,
                    "subscribe catch-up scan failed; closing with catchup_failed",
                );
                let err = error_frame(
                    "catchup_failed",
                    serde_json::json!({"from_seq": from_seq, "live_seq": live_seq}),
                );
                let err_stream = stream::once(async move { Ok::<_, Infallible>(err) });
                let combined = subscribed_evt.chain(err_stream).left_stream();
                return Sse::new(combined)
                    .keep_alive(KeepAlive::new().interval(Duration::from_secs(keepalive_secs)));
            }
        }
    } else {
        Vec::new()
    };

    let kind_filter_for_catchup = kind_filter.clone();
    let catchup_stream = stream::iter(catchup_envs.into_iter().filter_map(move |env| {
        if !kind_matches(&env, &kind_filter_for_catchup) {
            return None;
        }
        let payload = serde_json::to_string(&env).ok()?;
        Some(Ok::<_, Infallible>(Event::default().data(payload)))
    }));

    let want_tenant = tenant.clone();
    let want_session = session.clone();
    // `last_delivered_seq` reflects the highest seq the client has
    // actually received via this SSE stream when the live phase
    // begins. If catch-up ran (`from_seq < live_seq`), the catch-up
    // phase delivered all events with `seq < live_seq`, so the
    // high-water mark is `live_seq - 1`. If catch-up didn't run
    // (`from_seq >= live_seq`, pure live tail), nothing has been
    // delivered yet. Spec §6.6: the close frame on `lag_too_far`
    // tells the client to reconnect with
    // `from_seq = last_delivered_seq + 1`; if we left this at 0
    // after a non-empty catch-up, the SDK would replay the entire
    // `[0, live_seq)` window on reconnect.
    let initial_last_delivered_seq = if from_seq < live_seq {
        live_seq.saturating_sub(1)
    } else {
        0
    };
    let initial = LiveState {
        rx,
        live_seq,
        last_delivered_seq: initial_last_delivered_seq,
        closed_after_error: false,
    };
    // Subscriber-side filtering. Spec §6.7: Phase 1 uses a single
    // bus and accepts O(n_subs × n_events). Per-(tenant, session)
    // keyed buses are the upgrade path when this becomes the
    // bottleneck.
    let live = stream::unfold(initial, move |mut state| {
        let kind_filter = kind_filter.clone();
        let want_tenant = want_tenant.clone();
        let want_session = want_session.clone();
        async move {
            // Previous iteration emitted the lag_too_far frame;
            // close the stream now.
            if state.closed_after_error {
                return None;
            }
            loop {
                match state.rx.recv().await {
                    Ok(env) => {
                        if env.tenant != want_tenant || env.session != want_session {
                            continue;
                        }
                        if env.seq < state.live_seq {
                            // Already delivered via catch-up scan
                            // (or would have been if from_seq
                            // covered it). Drop to keep the
                            // dedup invariant.
                            continue;
                        }
                        if !kind_matches(&env, &kind_filter) {
                            continue;
                        }
                        let payload = serde_json::to_string(&env).unwrap_or_else(|_| "{}".into());
                        state.last_delivered_seq = env.seq;
                        return Some((Ok(Event::default().data(payload)), state));
                    }
                    Err(async_broadcast::RecvError::Overflowed(missed)) => {
                        // Client fell behind. Tell them what the
                        // last seq we delivered was so they can
                        // reconnect with `from_seq = N + 1` and
                        // recover gap-free.
                        tracing::warn!(
                            tenant = %want_tenant,
                            session = %want_session,
                            missed,
                            last_delivered_seq = state.last_delivered_seq,
                            "subscribe lag_too_far; closing stream",
                        );
                        let frame = error_frame(
                            "lag_too_far",
                            serde_json::json!({
                                "last_delivered_seq": state.last_delivered_seq,
                            }),
                        );
                        state.closed_after_error = true;
                        return Some((Ok(frame), state));
                    }
                    // Bus shut down. End the SSE response cleanly —
                    // clients reconnect with the EventSource retry
                    // policy.
                    Err(async_broadcast::RecvError::Closed) => return None,
                }
            }
        }
    });

    let combined = subscribed_evt.chain(catchup_stream).chain(live);
    Sse::new(combined.right_stream())
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(keepalive_secs)))
}

/// Live-tail iterator state carried through `stream::unfold`.
///
/// `last_delivered_seq` is the seq of the most recent envelope
/// returned to the client (zero before the first delivery), so the
/// `lag_too_far` close frame can tell the client where to resume
/// from. `closed_after_error` is the one-shot flag that makes
/// `unfold` end the stream after emitting the error frame: SSE has
/// no graceful "send this frame then close" primitive, so we hand
/// out the error envelope, mark the state, and return `None` on
/// the next poll.
struct LiveState {
    rx: async_broadcast::Receiver<EventEnvelope>,
    live_seq: u64,
    last_delivered_seq: u64,
    closed_after_error: bool,
}

/// Build the JSON body for an error frame: `{"error": <error>,
/// ...extras}`. Split out from [`error_frame`] so the body shape is
/// unit-testable without an SSE round-trip — `axum::response::sse::Event`
/// doesn't expose its event/data fields. Non-object `extras` is
/// ignored so callers can pass `serde_json::Value::Null` for the
/// no-extras case.
fn build_error_body(error: &str, extras: serde_json::Value) -> String {
    let mut body = serde_json::json!({"error": error});
    if let serde_json::Value::Object(map) = extras {
        for (k, v) in map {
            body[k] = v;
        }
    }
    body.to_string()
}

/// Build an `event: error` frame with `data:` set to a JSON object
/// `{"error": <error>, ...extras}`. Shared by `lag_too_far` and
/// `catchup_failed`; the next planned reuse is Task 9d
/// (`jwt_expired`). `extras` is merged into the body — pass an
/// `Object` like `json!({"last_delivered_seq": 7})` and its keys
/// land alongside `error`. Non-object `extras` is ignored so callers
/// can pass `serde_json::Value::Null` when there are no extras.
fn error_frame(error: &str, extras: serde_json::Value) -> Event {
    Event::default()
        .event("error")
        .data(build_error_body(error, extras))
}

/// Empty filter ⇒ deliver everything. Otherwise the envelope's
/// `kind` (extracted from `payload.kind` at emit time) must match
/// at least one allow-listed string.
fn kind_matches(env: &EventEnvelope, kind_filter: &[String]) -> bool {
    if kind_filter.is_empty() {
        return true;
    }
    env.kind
        .as_ref()
        .is_some_and(|k| kind_filter.iter().any(|f| f == k))
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_core::hlc::HlcTimestamp;

    fn env(kind: Option<&str>) -> EventEnvelope {
        EventEnvelope {
            tenant: "t".into(),
            session: "s".into(),
            seq: 1,
            ts_ms: 0,
            hlc_ts: HlcTimestamp {
                physical_ns: 0,
                logical: 0,
            },
            actor_id: None,
            kind: kind.map(str::to_string),
            payload: serde_json::json!({}),
        }
    }

    #[test]
    fn empty_filter_matches_anything() {
        assert!(kind_matches(&env(None), &[]));
        assert!(kind_matches(&env(Some("x")), &[]));
    }

    #[test]
    fn filter_matches_only_listed_kinds() {
        let f = vec!["keep".to_string()];
        assert!(kind_matches(&env(Some("keep")), &f));
        assert!(!kind_matches(&env(Some("skip")), &f));
        assert!(!kind_matches(&env(None), &f));
    }

    #[test]
    fn build_error_body_merges_extras() {
        let s = build_error_body(
            "catchup_failed",
            serde_json::json!({"from_seq": 5, "live_seq": 10}),
        );
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["error"], "catchup_failed");
        assert_eq!(v["from_seq"], 5);
        assert_eq!(v["live_seq"], 10);
    }

    #[test]
    fn build_error_body_handles_lag_too_far_shape() {
        let s = build_error_body(
            "lag_too_far",
            serde_json::json!({"last_delivered_seq": 42}),
        );
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["error"], "lag_too_far");
        assert_eq!(v["last_delivered_seq"], 42);
    }

    #[test]
    fn build_error_body_ignores_non_object_extras() {
        // Defensive: callers may pass `Value::Null` when no extras
        // apply; that should yield just `{"error": ...}` rather
        // than failing or producing a malformed body.
        let s = build_error_body("some_err", serde_json::Value::Null);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["error"], "some_err");
        assert!(v.as_object().unwrap().len() == 1);
    }

    #[test]
    fn normalize_kind_filter_drops_empty_and_whitespace() {
        assert_eq!(normalize_kind_filter(vec![]), Vec::<String>::new());
        // A bare `?kind=` deserializes to `vec![""]`; treat as
        // "no entry on that slot" rather than match-everything or
        // match-nothing.
        assert_eq!(
            normalize_kind_filter(vec!["".into()]),
            Vec::<String>::new()
        );
        assert_eq!(
            normalize_kind_filter(vec!["  ".into(), "a".into(), "".into(), " b ".into()]),
            vec!["a".to_string(), "b".into()]
        );
    }
}
