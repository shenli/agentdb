//! Phase 2 ephemeral broadcast handlers (spec § 5.1, § 5.5, § 5.7).
//!
//! `POST .../publish` (this task) and `GET .../subscribe` (Task 5).
//! Auth reuses Phase 1's [`crate::subscribe_auth::extract_subscribe_jwt`]
//! — same dual-mode header/query token, same `Ok(None)` fall-through
//! when the tenant has no JWT config (then existing Basic auth applies,
//! just like Phase 1 subscribe routes).

use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

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
