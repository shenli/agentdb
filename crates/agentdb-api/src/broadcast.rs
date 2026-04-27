//! Phase 2 ephemeral broadcast handlers (spec § 5.1, § 5.5, § 5.7).
//!
//! `POST .../publish` (this task) and `GET .../subscribe` (Task 5).
//! Auth reuses Phase 1's [`crate::subscribe_auth::extract_subscribe_jwt`]
//! — same dual-mode header/query token, same `Ok(None)` fall-through
//! when the tenant has no JWT config (then existing Basic auth applies,
//! just like Phase 1 subscribe routes).

use std::convert::Infallible;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures_util::stream;
use serde::Deserialize;

use crate::broadcast_bus::{validate_channel_name, BroadcastEnvelope, ChannelKey};
use crate::error::{ApiError, ErrorCode};
use crate::AppState;

/// 16 KiB cap on broadcast bodies (spec § 5.5).
pub const MAX_BODY_BYTES: usize = 16 * 1024;

/// Local `now_ms` helper. The bus's internal version is private; we
/// mint our own here rather than widen its surface for one caller.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `POST /api/tenants/:tenant_id/channels/:channel/publish`.
///
/// Spec § 5.1, § 5.3, § 5.5, § 5.7. Validation order is fail-cheapest-
/// first: channel-name check, body-size cap, JWT verification, JSON
/// parse, then enqueue + 204.
pub async fn publish(
    State(state): State<AppState>,
    Path((tenant, channel)): Path<(String, String)>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if !validate_channel_name(&channel) {
        return ApiError::Coded(
            StatusCode::BAD_REQUEST,
            ErrorCode::ChannelNameInvalid,
            "channel name fails validation".into(),
        )
        .into_response();
    }
    if body.len() > MAX_BODY_BYTES {
        // TODO(Task 14): mint a dedicated `BodyTooLarge` ErrorCode +
        // runbook. `InvalidRequest` is the closest existing variant
        // while we're still in Phase 2 plumbing.
        return ApiError::Coded(
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorCode::InvalidRequest,
            format!("body exceeds {MAX_BODY_BYTES} bytes"),
        )
        .into_response();
    }

    // JWT enforcement (only when tenant has JWT config).
    // `?access_token=` query support is reserved for the SSE GET in
    // Task 5; publish is header-only because senders aren't browsers
    // hitting EventSource.
    let claims = match crate::subscribe_auth::extract_subscribe_jwt(
        &state, &headers, None, &tenant,
    )
    .await
    {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    if let Some(c) = &claims {
        let allowed = c
            .channels
            .as_ref()
            .map(|ch| ch.allows(&channel))
            .unwrap_or(false);
        if !allowed {
            return ApiError::Coded(
                StatusCode::FORBIDDEN,
                ErrorCode::ChannelNotInJwt,
                format!("jwt channels claim does not allow channel {channel}"),
            )
            .into_response();
        }
    }

    // Parse body as JSON. Empty body is accepted as JSON `null`
    // would be — but reqwest's `.json(&{})` always sends `{}`, and
    // a missing body is a malformed broadcast either way.
    let payload: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::Coded(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidRequest,
                format!("malformed json body: {e}"),
            )
            .into_response();
        }
    };

    let env = BroadcastEnvelope {
        tenant: tenant.clone(),
        channel: channel.clone(),
        ts_ms: now_ms(),
        sender: claims.as_ref().map(|c| c.sub.clone()),
        payload,
    };
    let key = ChannelKey { tenant, channel };
    state.broadcast_bus.publish(&key, env).await;

    StatusCode::NO_CONTENT.into_response()
}

/// Query string for `GET …/channels/:channel/subscribe`. All fields
/// are optional. Decoded via `axum_extra::extract::Query` (i.e.
/// `serde_html_form`) for parity with the per-session subscribe
/// endpoint, even though Phase 2 has no repeated-key parameters
/// today; future filters (e.g. tag allow-lists) are likely.
#[derive(Debug, Deserialize)]
pub struct SubscribeChannelQuery {
    /// SSE keep-alive interval in seconds. Default 20 — matches the
    /// per-session endpoint so client retry budgets stay consistent
    /// across both subscribe shapes.
    #[serde(default)]
    pub heartbeat_secs: Option<u64>,
    /// EventSource-friendly auth (when `Authorization` header isn't
    /// usable). Mutually exclusive with `Authorization: Bearer …` —
    /// `extract_subscribe_jwt` enforces that.
    #[serde(default)]
    pub access_token: Option<String>,
    /// When true, drop envelopes whose `sender` matches this
    /// subscriber's JWT `sub` (spec § 5.8). Only effective when both
    /// publisher and subscriber use JWT auth — Basic-auth publishers
    /// emit `sender: None` and so never match.
    #[serde(default)]
    pub no_echo: Option<bool>,
}

/// `GET /api/tenants/:tenant_id/channels/:channel/subscribe`.
///
/// Spec § 5.1, § 5.2, § 5.5, § 5.8. Validation order matches
/// [`publish`]: channel-name → JWT verify → JWT channel allow-list →
/// open subscription → stream envelopes.
///
/// Lag policy (spec § 5.5) is the inverse of Phase 1's
/// `subscribe_session`: broadcast is ephemeral with no replay, so
/// `RecvError::Overflowed(_)` is silently skipped (the subscriber
/// missed the older messages — there's nothing to recover, only
/// fresher ones to keep delivering). No `lag_too_far` close frame.
pub async fn subscribe_channel(
    State(state): State<AppState>,
    Path((tenant, channel)): Path<(String, String)>,
    axum_extra::extract::Query(q): axum_extra::extract::Query<SubscribeChannelQuery>,
    headers: HeaderMap,
) -> Response {
    if !validate_channel_name(&channel) {
        return ApiError::Coded(
            StatusCode::BAD_REQUEST,
            ErrorCode::ChannelNameInvalid,
            "channel name fails validation".into(),
        )
        .into_response();
    }

    let claims = match crate::subscribe_auth::extract_subscribe_jwt(
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
    if let Some(c) = &claims {
        let allowed = c
            .channels
            .as_ref()
            .map(|ch| ch.allows(&channel))
            .unwrap_or(false);
        if !allowed {
            return ApiError::Coded(
                StatusCode::FORBIDDEN,
                ErrorCode::ChannelNotInJwt,
                format!("jwt channels claim does not allow channel {channel}"),
            )
            .into_response();
        }
    }

    // Self-echo filter (spec § 5.8). When `no_echo=true` AND the
    // subscriber authenticated with a JWT, capture its `sub` so the
    // unfold can drop matching senders. Without a JWT we have no
    // identity to match on, so the flag silently no-ops — which
    // matches the spec's "only effective when JWT in use".
    let no_echo = q.no_echo.unwrap_or(false);
    let echo_filter_sub: Option<String> = if no_echo {
        claims.as_ref().map(|c| c.sub.clone())
    } else {
        None
    };

    let key = ChannelKey {
        tenant,
        channel,
    };
    let sub = state.broadcast_bus.subscribe(&key).await;

    // `BroadcastSubscription`'s `Drop` decrements the per-channel
    // subscriber counter, so client disconnect (which ends this
    // stream and drops `sub`) cleans up automatically — no extra
    // bookkeeping here.
    let live = stream::unfold((sub, echo_filter_sub), move |(mut sub, drop_sender)| async move {
        loop {
            match sub.recv().await {
                Ok(env) => {
                    if let Some(d) = &drop_sender {
                        if env.sender.as_deref() == Some(d.as_str()) {
                            continue;
                        }
                    }
                    let payload = match serde_json::to_string(&env) {
                        Ok(s) => s,
                        Err(_) => continue,
                    };
                    return Some((
                        Ok::<_, Infallible>(Event::default().data(payload)),
                        (sub, drop_sender),
                    ));
                }
                // Spec § 5.5: ephemeral, no replay → silently skip
                // overflow. Different from Phase 1's `lag_too_far`
                // close: there's nothing for the subscriber to
                // recover with, so dropping the gap and continuing
                // to deliver fresher messages is the correct shape.
                Err(async_broadcast::RecvError::Overflowed(_)) => continue,
                // Bus shut down (process exit). End the stream
                // cleanly; clients reconnect.
                Err(async_broadcast::RecvError::Closed) => return None,
            }
        }
    });

    let keepalive_secs = q.heartbeat_secs.unwrap_or(20);
    Sse::new(live)
        .keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(keepalive_secs)))
        .into_response()
}
