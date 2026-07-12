//! Coarse installation-level HTTP authentication.
//!
//! `server.api_auth_token` protects every external `/api/*` route. Admin-plane
//! tokens stored in the metadata database additionally authorize
//! `/api/admin/*` and support read-only scope. Internal Raft routes retain their
//! separate rotating token and are intentionally excluded here.

use agentdb_meta::admin_tokens::{self, AdminScope};
use axum::{
    body::Body,
    extract::{Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use subtle::ConstantTimeEq;

use crate::state::AppState;

pub async fn authorize_external_api(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if !path.starts_with("/api/") || path.starts_with("/api/internal/") {
        return next.run(request).await;
    }

    let presented = bearer_token(request.headers()).map(str::to_string);
    if static_token_matches(&state, presented.as_deref()) {
        return next.run(request).await;
    }

    if path.starts_with("/api/admin/") {
        return authorize_admin_request(state, request, next, presented.as_deref()).await;
    }

    if state.api_auth_token.is_some() || state.api_auth_previous_token.is_some() {
        return auth_error(
            StatusCode::UNAUTHORIZED,
            "a valid API bearer token is required",
        );
    }

    next.run(request).await
}

async fn authorize_admin_request(
    state: AppState,
    request: Request<Body>,
    next: Next,
    presented: Option<&str>,
) -> Response {
    let Some(meta) = state.auth_meta.as_ref() else {
        if state.api_auth_token.is_some() || state.api_auth_previous_token.is_some() {
            return auth_error(
                StatusCode::UNAUTHORIZED,
                "a valid admin bearer token is required",
            );
        }
        return next.run(request).await;
    };

    if let Some(presented) = presented {
        match admin_tokens::verify(meta.pool(), presented).await {
            Ok(Some((_token_id, AdminScope::Admin))) => return next.run(request).await,
            Ok(Some((_token_id, AdminScope::Readonly))) => {
                if matches!(*request.method(), Method::GET | Method::HEAD) {
                    return next.run(request).await;
                }
                return auth_error(StatusCode::FORBIDDEN, "the admin bearer token is read-only");
            }
            Ok(None) => {}
            Err(error) => {
                tracing::error!(?error, "failed to verify admin bearer token");
                return auth_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "admin authentication metadata is unavailable",
                );
            }
        }
    }

    match admin_tokens::count_all(meta.pool()).await {
        Ok(0) if state.api_auth_token.is_none() && state.api_auth_previous_token.is_none() => {
            // Local/bootstrap compatibility: the admin API remains open only
            // until the first metadata-backed token is ever minted.
            // Non-loopback startup separately requires a static API token.
            next.run(request).await
        }
        Ok(_) => auth_error(
            StatusCode::UNAUTHORIZED,
            "a valid admin bearer token is required",
        ),
        Err(error) => {
            tracing::error!(?error, "failed to read admin authentication state");
            auth_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "admin authentication metadata is unavailable",
            )
        }
    }
}

fn bearer_token(headers: &axum::http::HeaderMap) -> Option<&str> {
    let raw = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = raw.split_once(' ')?;
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty()).then_some(token)
}

fn static_token_matches(state: &AppState, presented: Option<&str>) -> bool {
    let Some(presented) = presented else {
        return false;
    };
    let current = state
        .api_auth_token
        .as_deref()
        .is_some_and(|expected| constant_time_eq(expected, presented));
    let previous = state
        .api_auth_previous_token
        .as_deref()
        .is_some_and(|expected| constant_time_eq(expected, presented));
    current | previous
}

fn constant_time_eq(expected: &str, presented: &str) -> bool {
    expected.as_bytes().ct_eq(presented.as_bytes()).unwrap_u8() == 1
}

fn auth_error(status: StatusCode, message: &'static str) -> Response {
    let mut response = (
        status,
        Json(serde_json::json!({
            "code": if status == StatusCode::FORBIDDEN { "forbidden" } else { "unauthorized" },
            "message": message,
            "retryable": false
        })),
    )
        .into_response();
    if status == StatusCode::UNAUTHORIZED {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer realm=\"agentdb\""),
        );
    }
    response
}
