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
//! - **`jwt_expired`** — the JWT's `exp` was reached after the
//!   stream opened (Task 9d). Spec §6.13 step 5: emit the close
//!   frame with the current high-water mark so the SDK reconnects
//!   gap-free with a refreshed token. Checked once per delivery
//!   (no background timer): if events are flowing the close fires
//!   on the next post-`exp` event; if no events flow, the
//!   keep-alive comments hold the connection open until one does.
//!   Phase 1 doesn't promise sub-second precision on the close.
//!
//! All three reuse [`error_frame`] for a consistent wire shape.
//!
//! Multi-node 421, JWT, and CORS are still later tasks — this file
//! stays focused on the per-session subscribe path.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::HeaderMap;
// `axum_extra::extract::Query` uses `serde_html_form` and decodes
// repeated query keys (`?kind=a&kind=b`) into `Vec<String>`. The
// default axum `Query` (backed by `serde_urlencoded`) cannot, so
// we deliberately shadow it here. Spec §6.4: "Multiple
// `kind=foo&kind=bar` parameters are OR'd."
use axum_extra::extract::Query;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::stream::{self, StreamExt};
use serde::Deserialize;

use axum::Extension;

use crate::auth_middleware::AuthedTenant;
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
    /// JWT-as-query-param for browser EventSource clients which
    /// can't set custom headers (spec §6.13). Mutually exclusive
    /// with `Authorization: Bearer …`; both supplied ⇒ 400.
    #[serde(default)]
    pub access_token: Option<String>,
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
    headers: HeaderMap,
) -> axum::response::Response {
    subscribe_session_inner(state, tenant, session, q, headers).await
}

async fn subscribe_session_inner(
    state: AppState,
    tenant: String,
    session: String,
    q: SubscribeQuery,
    headers: HeaderMap,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    // Multi-node 421 redirect (spec §6.8). Phase 1 doesn't proxy
    // SSE across nodes — if the owning tablet is on a peer, return
    // 421 with the owner's address so the SDK reconnects directly.
    // Mirrors `append_and_fetch`'s routing pattern but tells the
    // client to redirect rather than forwarding the request.
    if let agentdb_core::RouteDecision::Remote { target, .. } =
        state.registry.route_session_for_io(&tenant, &session)
    {
        // TODO(M27 Task 5): once /api/tenants/{t}/.../subscribe is deleted,
        // this redirect should target the v2 URL `/api/sessions/{s}/subscribe`.
        // Both v1 and v2 callers funnel through this inner helper, so we'd
        // also need to thread the caller's route shape into the inner if we
        // want path fidelity per call. For now the redirect uses the v1 URL
        // because v1 still exists for both shapes during the transition.
        let path = format!(
            "/api/tenants/{}/sessions/{}/subscribe",
            tenant, session
        );
        return crate::forwarding::subscribe_not_owner_reply(&target, &path);
    }

    // JWT enforcement (spec §6.13). Returns Ok(None) if the tenant
    // has no JWT config — falls through to whatever auth the route's
    // middleware applies (typically Basic). Otherwise Some(claims) on
    // verify success, or an ApiError on dual-auth / missing / invalid /
    // expired token.
    let jwt_claims = match crate::subscribe_auth::extract_subscribe_jwt(
        &state,
        &headers,
        q.access_token.as_deref(),
        &tenant,
    )
    .await
    {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    if let Some(claims) = &jwt_claims {
        if !claims.sessions.allows(&session) {
            return crate::error::ApiError::Coded(
                axum::http::StatusCode::FORBIDDEN,
                crate::error::ErrorCode::SessionNotInJwt,
                format!("jwt sessions claim does not allow session {session}"),
            )
            .into_response();
        }
    }

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
                let combined = subscribed_evt.chain(err_stream);
                return Sse::new(combined)
                    .keep_alive(KeepAlive::new().interval(Duration::from_secs(keepalive_secs)))
                    .into_response();
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
        jwt_exp_secs: jwt_claims.as_ref().map(|c| c.exp),
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
                        // Spec §6.13 step 5: token may have expired
                        // mid-stream. Checked AFTER all filters so a
                        // dropped envelope doesn't trigger a spurious
                        // close, and BEFORE delivery so the post-`exp`
                        // event is replaced by the close frame rather
                        // than leaking past the expiry boundary.
                        if let Some(exp) = state.jwt_exp_secs {
                            if now_secs() >= exp {
                                tracing::info!(
                                    tenant = %want_tenant,
                                    session = %want_session,
                                    last_delivered_seq = state.last_delivered_seq,
                                    "subscribe jwt_expired; closing stream",
                                );
                                let body = build_error_body(
                                    "jwt_expired",
                                    serde_json::json!({
                                        "last_seq": state.last_delivered_seq,
                                    }),
                                );
                                state.closed_after_error = true;
                                return Some((
                                    Ok(Event::default().event("error").data(body)),
                                    state,
                                ));
                            }
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
    Sse::new(combined)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(keepalive_secs)))
        .into_response()
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
    /// JWT `exp` claim (UNIX seconds), or `None` if the tenant
    /// requires no JWT. When present and `now_secs() >= exp` on a
    /// post-filter delivery, the unfold emits a `jwt_expired` close
    /// frame in place of the next envelope (spec §6.13 step 5).
    jwt_exp_secs: Option<u64>,
}

/// Wall-clock UNIX seconds. Used for mid-stream JWT expiry checks
/// (spec §6.13 step 5). A clock before the epoch is impossible on
/// real hardware; the saturating fallback keeps us from panicking
/// even in the pathological case.
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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

/// Query string for `GET /api/tenants/{tenant_id}/subscribe`. All
/// fields are optional (spec §6.5).
///
/// `session` and `kind` are repeated-key vectors decoded via
/// `serde_html_form` (matching [`SubscribeQuery`]). `session` is a
/// whitelist — empty ⇒ no whitelist, every session of the tenant.
/// `kind` is OR'd. `from_seq_per_session` encodes a per-session
/// catch-up cursor as a comma-separated `id:seq` list (e.g.
/// `s-1:42,s-2:7`); session ids must not contain `,` or `:`.
#[derive(Debug, Deserialize)]
pub struct TenantSubscribeQuery {
    /// Repeated-key whitelist of session ids. Empty vec ⇒ deliver
    /// every session of the tenant.
    #[serde(default)]
    pub session: Vec<String>,
    /// Repeated-key kind filter. Empty vec ⇒ deliver every kind.
    #[serde(default)]
    pub kind: Vec<String>,
    /// Per-session catch-up cursor map encoded as
    /// `s-1:42,s-2:7`. Sessions not in the map get pure live
    /// tail (no replay). Sessions in the map but not in the
    /// `session` whitelist (when present) are dropped.
    #[serde(default)]
    pub from_seq_per_session: Option<String>,
    /// Override the keep-alive interval (default 20s). Same
    /// semantics as the per-session endpoint.
    #[serde(default)]
    pub heartbeat_secs: Option<u64>,
    /// JWT-as-query-param for browser EventSource clients which
    /// can't set custom headers (spec §6.13). Mutually exclusive
    /// with `Authorization: Bearer …`; both supplied ⇒ 400.
    #[serde(default)]
    pub access_token: Option<String>,
}

/// Multi-session live-tail state. Mirrors [`LiveState`] but tracks
/// per-session cursors instead of a single `live_seq`/`last_delivered_seq`
/// pair: catch-up may have run for several sessions independently,
/// and the `lag_too_far` close frame needs to report a *map* of
/// reconnect cursors (`last_delivered_seq_by_session`) so the SDK
/// can resume each session at the right point.
struct TenantLiveState {
    rx: async_broadcast::Receiver<EventEnvelope>,
    /// Snapshotted `live_seq` per session at attach time. Live
    /// envelopes with `seq < live_seq` for that session are dropped
    /// — they were already delivered via catch-up. Sessions absent
    /// from this map either had no catch-up cursor or had their
    /// catch-up scan fail (degraded to live-only).
    live_seq_by_session: HashMap<String, u64>,
    /// Highest seq delivered to the client for each session. Sent
    /// in the `lag_too_far` body so the SDK can reconnect with a
    /// `from_seq_per_session` cursor that resumes gap-free.
    last_delivered_seq_by_session: HashMap<String, u64>,
    /// One-shot flag: previous iteration emitted the `lag_too_far`
    /// frame, so the next poll ends the stream.
    closed_after_error: bool,
    /// JWT `exp` claim (UNIX seconds), or `None` if the tenant
    /// requires no JWT. Same semantics as [`LiveState::jwt_exp_secs`]
    /// but checked per-envelope on the tenant-wide stream; the close
    /// body uses the per-session high-water-mark map so reconnect
    /// resumes every session gap-free.
    jwt_exp_secs: Option<u64>,
}

/// Decode the `from_seq_per_session=s-1:42,s-2:7` query parameter
/// into a `{session_id: from_seq}` map. Malformed pairs are
/// silently dropped (better lenient than 400-on-typo for an
/// optional cursor). Session ids containing `,` or `:` are out of
/// scope — the spec constrains ids to safe chars.
fn parse_from_seq_per_session(s: Option<&str>) -> HashMap<String, u64> {
    let mut out = HashMap::new();
    let Some(s) = s else { return out };
    for pair in s.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        if let Some((k, v)) = pair.split_once(':') {
            if let Ok(seq) = v.trim().parse::<u64>() {
                out.insert(k.trim().to_string(), seq);
            }
        }
    }
    out
}

/// Build the JSON body for the tenant-wide `subscribed` control
/// frame. `catchup_failed_sessions` is omitted entirely when empty
/// so happy-path clients see exactly the same shape as before.
fn build_subscribed_payload(
    tenant: &str,
    live_seq_by_session: &HashMap<String, u64>,
    catchup_failed_sessions: &[String],
) -> serde_json::Value {
    let mut payload = serde_json::json!({
        "tenant": tenant,
        "live_seq_by_session": live_seq_by_session,
    });
    if !catchup_failed_sessions.is_empty() {
        payload["catchup_failed_sessions"] = serde_json::json!(catchup_failed_sessions);
    }
    payload
}

pub async fn subscribe_tenant(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Query(q): Query<TenantSubscribeQuery>,
    headers: HeaderMap,
) -> axum::response::Response {
    subscribe_tenant_inner(state, tenant, q, headers).await
}

async fn subscribe_tenant_inner(
    state: AppState,
    tenant: String,
    q: TenantSubscribeQuery,
    headers: HeaderMap,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    let kind_filter = normalize_kind_filter(q.kind);
    let session_whitelist: Option<HashSet<String>> = if q.session.is_empty() {
        None
    } else {
        Some(q.session.iter().cloned().collect())
    };
    let from_seq_map = parse_from_seq_per_session(q.from_seq_per_session.as_deref());
    let keepalive_secs = q.heartbeat_secs.unwrap_or(20);

    // Multi-node 421 redirect (spec §6.8). The tenant-wide stream
    // can't be served for sessions on peer nodes — Phase 1 doesn't
    // proxy SSE. We only check sessions the client explicitly
    // named (whitelist or per-session catch-up cursor); enumerating
    // every session in a tenant would be expensive and racy. If
    // the client gave no specific sessions, we serve locally and
    // accept the gap (envelopes from peer-owned sessions don't
    // reach this node's bus, so the client just sees an empty
    // stream — correct given Phase 1's no-cross-node-fanout).
    let mut sessions_to_check: HashSet<&String> = HashSet::new();
    if let Some(ws) = &session_whitelist {
        sessions_to_check.extend(ws.iter());
    }
    sessions_to_check.extend(from_seq_map.keys());
    if !sessions_to_check.is_empty() {
        let mut remote_owners: HashSet<String> = HashSet::new();
        for sid in &sessions_to_check {
            if let agentdb_core::RouteDecision::Remote { target, .. } =
                state.registry.route_session_for_io(&tenant, sid)
            {
                remote_owners.insert(target.address);
            }
        }
        if !remote_owners.is_empty() {
            let mut addrs: Vec<String> = remote_owners.into_iter().collect();
            addrs.sort();
            return crate::forwarding::subscribe_not_owner_multi_reply(addrs);
        }
    }

    // JWT enforcement (spec §6.13). Same fall-through-on-no-config
    // semantics as the per-session endpoint. For tenant-wide we then
    // cross-check the claim against every session the client named
    // (whitelist or per-session catch-up cursor); if no specific
    // sessions were named the claim must be `sessions: "*"`,
    // otherwise the subscriber would silently see no events for
    // sessions outside its claim — better to fail loud.
    let jwt_claims = match crate::subscribe_auth::extract_subscribe_jwt(
        &state,
        &headers,
        q.access_token.as_deref(),
        &tenant,
    )
    .await
    {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    if let Some(claims) = &jwt_claims {
        let mut named: HashSet<&str> = q.session.iter().map(String::as_str).collect();
        for k in from_seq_map.keys() {
            named.insert(k.as_str());
        }
        if named.is_empty() {
            // No explicit session list → require wildcard claim.
            let is_wildcard = matches!(
                claims.sessions,
                crate::jwt::SessionsClaim::Wildcard(ref s) if s == "*"
            );
            if !is_wildcard {
                return crate::error::ApiError::Coded(
                    axum::http::StatusCode::FORBIDDEN,
                    crate::error::ErrorCode::SessionNotInJwt,
                    "tenant-wide subscribe without specific sessions requires sessions=\"*\""
                        .into(),
                )
                .into_response();
            }
        } else {
            for sid in &named {
                if !claims.sessions.allows(sid) {
                    return crate::error::ApiError::Coded(
                        axum::http::StatusCode::FORBIDDEN,
                        crate::error::ErrorCode::SessionNotInJwt,
                        format!("jwt sessions claim does not allow session {sid}"),
                    )
                    .into_response();
                }
            }
        }
    }

    // Subscribe FIRST so any envelope appended between here and
    // the per-session `live_seq` snapshots is buffered (same race
    // closure as the per-session endpoint).
    let rx = state.event_bus.subscribe();

    // Resolve a `live_seq` per requested catch-up session and
    // accumulate catch-up envelopes. Tenant-wide is multi-session,
    // so we degrade lenient: a per-session scan failure logs and
    // skips that session's replay (rather than tearing down the
    // whole stream the way the single-session endpoint does).
    let mut live_seq_by_session: HashMap<String, u64> = HashMap::new();
    let mut catchup: Vec<EventEnvelope> = Vec::new();
    // Sessions whose catch-up scan returned an error. We surface
    // these to the client in the `subscribed` frame so the SDK can
    // distinguish "no events yet" from "we have a gap, please
    // re-subscribe with a different `from_seq`."
    let mut catchup_failed_sessions: Vec<String> = Vec::new();
    for (session_id, from_seq) in &from_seq_map {
        if let Some(whitelist) = &session_whitelist {
            if !whitelist.contains(session_id) {
                continue;
            }
        }
        let live_seq = crate::handlers::resolve_session_next_seq(&state, &tenant, session_id)
            .await
            .unwrap_or(0);
        live_seq_by_session.insert(session_id.clone(), live_seq);
        if *from_seq < live_seq {
            match crate::handlers::scan_events_from_seq(
                &state, &tenant, session_id, *from_seq, live_seq,
            )
            .await
            {
                Ok(events) => {
                    for ev in events {
                        catchup.push(crate::event_bus::envelope_from_event(
                            &tenant, session_id, &ev,
                        ));
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = ?e,
                        tenant = %tenant,
                        session = %session_id,
                        "tenant catch-up scan failed; degrading to live-only for this session",
                    );
                    // Drop the snapshot entirely so live-tail dedup
                    // (`live_seq_by_session.get(...)`) defaults to
                    // "no snapshot → no dedup" and delivers every
                    // matching live envelope. This also keeps the
                    // `subscribed` frame from claiming a fake
                    // `live_seq=0` for the failed session.
                    live_seq_by_session.remove(session_id);
                    catchup_failed_sessions.push(session_id.clone());
                }
            }
        }
    }

    // `subscribed` control frame. Multi-session shape: report the
    // snapshotted `live_seq` for every session we resolved, plus a
    // (possibly omitted) list of sessions whose catch-up scan
    // failed so the client can react explicitly.
    let subscribed_payload =
        build_subscribed_payload(&tenant, &live_seq_by_session, &catchup_failed_sessions);
    let subscribed_evt = stream::once(async move {
        Ok::<_, Infallible>(
            Event::default()
                .event("subscribed")
                .data(subscribed_payload.to_string()),
        )
    });

    // Sort catch-up by `(session, seq)` so frames within a session
    // are ordered and inter-session interleaving is deterministic.
    catchup.sort_by(|a, b| (a.session.as_str(), a.seq).cmp(&(b.session.as_str(), b.seq)));
    // Pre-seed `last_delivered_seq_by_session` with `live_seq - 1`
    // for sessions where catch-up actually ran, so a `lag_too_far`
    // immediately after attach still reports a cursor that lets
    // the SDK resume gap-free.
    let mut initial_last_delivered: HashMap<String, u64> = HashMap::new();
    for (session_id, &live_seq) in &live_seq_by_session {
        if let Some(&from_seq) = from_seq_map.get(session_id) {
            if from_seq < live_seq && live_seq > 0 {
                initial_last_delivered.insert(session_id.clone(), live_seq - 1);
            }
        }
    }

    let kind_filter_for_catchup = kind_filter.clone();
    let catchup_stream = stream::iter(catchup.into_iter().filter_map(move |env| {
        if !kind_matches(&env, &kind_filter_for_catchup) {
            return None;
        }
        let payload = serde_json::to_string(&env).ok()?;
        Some(Ok::<_, Infallible>(Event::default().data(payload)))
    }));

    let want_tenant = tenant.clone();
    let want_sessions = session_whitelist.clone();
    let kind_filter_for_live = kind_filter.clone();
    let initial = TenantLiveState {
        rx,
        live_seq_by_session: live_seq_by_session.clone(),
        last_delivered_seq_by_session: initial_last_delivered,
        closed_after_error: false,
        jwt_exp_secs: jwt_claims.as_ref().map(|c| c.exp),
    };

    let live = stream::unfold(initial, move |mut state| {
        let kind_filter = kind_filter_for_live.clone();
        let want_tenant = want_tenant.clone();
        let want_sessions = want_sessions.clone();
        async move {
            if state.closed_after_error {
                return None;
            }
            loop {
                match state.rx.recv().await {
                    Ok(env) => {
                        if env.tenant != want_tenant {
                            continue;
                        }
                        if let Some(ws) = &want_sessions {
                            if !ws.contains(&env.session) {
                                continue;
                            }
                        }
                        if let Some(&live_seq) = state.live_seq_by_session.get(&env.session) {
                            if env.seq < live_seq {
                                // Already delivered via catch-up.
                                continue;
                            }
                        }
                        if !kind_matches(&env, &kind_filter) {
                            continue;
                        }
                        // Spec §6.13 step 5: token expired mid-stream.
                        // Same post-filter / pre-deliver placement as
                        // the per-session endpoint; close body carries
                        // the per-session high-water-mark map so the
                        // SDK reconnects every session gap-free with
                        // a refreshed token.
                        if let Some(exp) = state.jwt_exp_secs {
                            if now_secs() >= exp {
                                tracing::info!(
                                    tenant = %want_tenant,
                                    "tenant subscribe jwt_expired; closing stream",
                                );
                                let body = build_error_body(
                                    "jwt_expired",
                                    serde_json::json!({
                                        "last_delivered_seq_by_session":
                                            &state.last_delivered_seq_by_session,
                                    }),
                                );
                                state.closed_after_error = true;
                                return Some((
                                    Ok(Event::default().event("error").data(body)),
                                    state,
                                ));
                            }
                        }
                        let payload = match serde_json::to_string(&env) {
                            Ok(s) => s,
                            Err(_) => continue,
                        };
                        state
                            .last_delivered_seq_by_session
                            .insert(env.session.clone(), env.seq);
                        return Some((Ok(Event::default().data(payload)), state));
                    }
                    Err(async_broadcast::RecvError::Overflowed(missed)) => {
                        tracing::warn!(
                            tenant = %want_tenant,
                            missed,
                            "tenant subscribe lag_too_far; closing stream",
                        );
                        let frame = error_frame(
                            "lag_too_far",
                            serde_json::json!({
                                "last_delivered_seq_by_session":
                                    &state.last_delivered_seq_by_session,
                            }),
                        );
                        state.closed_after_error = true;
                        return Some((Ok(frame), state));
                    }
                    Err(async_broadcast::RecvError::Closed) => return None,
                }
            }
        }
    });

    let combined = subscribed_evt.chain(catchup_stream).chain(live);
    Sse::new(combined)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(keepalive_secs)))
        .into_response()
}

/// v2: `/api/sessions/{session_id}/subscribe` — auth-derived tenant.
///
/// Mirrors `append_and_fetch_session_keyed`'s shape: takes
/// `Option<Extension<AuthedTenant>>` rather than the non-optional
/// form so the `(None, Some(locator))` branch in
/// `resolve_session_tenant` can fall back to the locator's tenant
/// when the request has no auth header. The tenant-wide v2
/// counterpart `subscribe_tenant_keyed` uses the non-optional form
/// because there's no per-session locator to fall back to.
#[tracing::instrument(
    name = "rest.subscribe_session_keyed",
    skip(state, headers, authed),
    fields(session = %session),
)]
pub async fn subscribe_session_keyed(
    State(state): State<AppState>,
    Path(session): Path<String>,
    Query(q): Query<SubscribeQuery>,
    headers: HeaderMap,
    authed: Option<Extension<AuthedTenant>>,
) -> axum::response::Response {
    let tenant = match crate::auth_middleware::resolve_session_tenant(
        &state,
        &session,
        authed.as_deref(),
        "subscribe",
    )
    .await
    {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    subscribe_session_inner(state, tenant, session, q, headers).await
}

/// v2: `/api/subscribe` — auth-derived tenant, no path params.
/// Tenant is the bearer token's claim; no locator cross-check
/// needed because the URL doesn't address any specific session.
#[tracing::instrument(
    name = "rest.subscribe_tenant_keyed",
    skip(state, headers, authed),
    fields(tenant = %authed.external_id),
)]
pub async fn subscribe_tenant_keyed(
    State(state): State<AppState>,
    Query(q): Query<TenantSubscribeQuery>,
    headers: HeaderMap,
    Extension(authed): Extension<AuthedTenant>,
) -> axum::response::Response {
    subscribe_tenant_inner(state, authed.external_id, q, headers).await
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
    fn parse_from_seq_per_session_decodes_pairs() {
        let m = parse_from_seq_per_session(Some("s-1:42,s-2:7"));
        assert_eq!(m.get("s-1"), Some(&42));
        assert_eq!(m.get("s-2"), Some(&7));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn parse_from_seq_per_session_handles_none_and_empty() {
        assert!(parse_from_seq_per_session(None).is_empty());
        assert!(parse_from_seq_per_session(Some("")).is_empty());
        assert!(parse_from_seq_per_session(Some(",,")).is_empty());
    }

    #[test]
    fn parse_from_seq_per_session_drops_malformed() {
        // Missing colon and non-numeric seq are silently dropped —
        // an optional cursor is better degraded leniently than 400'd.
        let m = parse_from_seq_per_session(Some("missingcolon,s-1:abc,s-2:5"));
        assert_eq!(m.len(), 1);
        assert_eq!(m.get("s-2"), Some(&5));
    }

    #[test]
    fn parse_from_seq_per_session_trims_whitespace() {
        let m = parse_from_seq_per_session(Some(" s-1 : 42 , s-2 : 7 "));
        assert_eq!(m.get("s-1"), Some(&42));
        assert_eq!(m.get("s-2"), Some(&7));
    }

    #[test]
    fn build_subscribed_payload_omits_empty_failed_sessions() {
        let mut map = HashMap::new();
        map.insert("s-1".to_string(), 5);
        let payload = build_subscribed_payload("t-1", &map, &[]);
        assert_eq!(payload["tenant"], "t-1");
        assert_eq!(payload["live_seq_by_session"]["s-1"], 5);
        assert!(payload.get("catchup_failed_sessions").is_none());
    }

    #[test]
    fn build_subscribed_payload_includes_non_empty_failed_sessions() {
        let map = HashMap::new();
        let failed = vec!["s-3".to_string(), "s-4".to_string()];
        let payload = build_subscribed_payload("t-1", &map, &failed);
        let arr = payload["catchup_failed_sessions"].as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0], "s-3");
        assert_eq!(arr[1], "s-4");
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
