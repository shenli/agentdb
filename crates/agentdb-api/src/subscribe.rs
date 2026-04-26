//! Phase 1 Tasks 4 + 5: per-session subscribe SSE endpoint.
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
//! The lag-too-far error frame, multi-node 421, JWT, and CORS are
//! all later tasks — this file deliberately stays small.

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

    // Catch-up phase. `from_seq < live_seq` ⇒ the client missed
    // events in `[from_seq, live_seq)`; replay them from the
    // durable log. `from_seq` defaults to `live_seq` so an absent
    // query parameter is "pure live tail, no catch-up".
    //
    // On scan failure we degrade to "live tail only" with a warn —
    // dropping the whole subscription would be worse than a
    // potential gap, and the client can still reconnect. The
    // resolve helper's warn covers most KV-error cases too.
    let from_seq = q.from_seq.unwrap_or(live_seq);
    let catchup_envs: Vec<EventEnvelope> = if from_seq < live_seq {
        match crate::handlers::scan_events_from_seq(&state, &tenant, &session, from_seq, live_seq)
            .await
        {
            Ok(events) => events
                .into_iter()
                .map(|ev| crate::event_bus::envelope_from_event(&tenant, &session, &ev))
                .collect(),
            Err(e) => {
                tracing::warn!(
                    error = ?e,
                    tenant = %tenant,
                    session = %session,
                    from_seq,
                    live_seq,
                    "subscribe catch-up scan failed; falling back to live tail",
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

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
    // Subscriber-side filtering. Spec §6.7: Phase 1 uses a single
    // bus and accepts O(n_subs × n_events). Per-(tenant, session)
    // keyed buses are the upgrade path when this becomes the
    // bottleneck.
    let live = stream::unfold((rx, live_seq), move |(mut rx, live_seq)| {
        let kind_filter = kind_filter.clone();
        let want_tenant = want_tenant.clone();
        let want_session = want_session.clone();
        async move {
            loop {
                match rx.recv().await {
                    Ok(env) => {
                        if env.tenant != want_tenant || env.session != want_session {
                            continue;
                        }
                        if env.seq < live_seq {
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
                        return Some((Ok(Event::default().data(payload)), (rx, live_seq)));
                    }
                    // Sender dropped or stream closed. End the
                    // SSE response cleanly — clients reconnect
                    // with the EventSource retry policy.
                    Err(_) => return None,
                }
            }
        }
    });

    let stream = subscribed_evt.chain(catchup_stream).chain(live);
    let keepalive_secs = q.heartbeat_secs.unwrap_or(20);
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(keepalive_secs)))
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
