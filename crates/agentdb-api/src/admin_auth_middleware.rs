//! Admin-plane bearer-token auth (post-M25 security patch).
//!
//! Layered on `/api/admin/*`. Behavior depends on
//! `AppState::admin_auth`:
//!
//! - `None` → pass-through (backward compat; dev mode and
//!   `agentdb serve --dev`).
//! - `Some { meta }` + no active tokens exist → pass-through
//!   for `POST /api/admin/tokens` only (bootstrap); every
//!   other admin route 401s. Once the first token is
//!   active, mint also requires auth.
//! - `Some { meta }` + tokens exist → `Authorization:
//!   Bearer <token>` required; look up, verify hash, allow.
//!
//! Always open: `/health/*`, `/metrics`, `/openapi.json`,
//! `/dashboard`, `/api/stream`. Those don't live under
//! `/api/admin/` so this middleware never sees them.

use std::sync::Arc;

use agentdb_meta::{admin_tokens, MetaStore};
use axum::extract::{Request, State};
use axum::http::header;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::error::ApiError;
use crate::AppState;

/// Path prefix that exempts the bootstrap mint from the
/// "active tokens exist" gate. Matches exactly one route.
const BOOTSTRAP_MINT_PATH: &str = "/api/admin/tokens";
const BOOTSTRAP_MINT_METHOD: &str = "POST";

pub async fn admin_auth_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let Some(cfg) = &state.admin_auth else {
        return next.run(req).await;
    };
    let Some(meta) = cfg.meta.clone() else {
        return ApiError::Internal("admin auth configured without meta store".into())
            .into_response();
    };

    let path = req.uri().path().to_string();
    let method = req.method().as_str().to_string();

    // Extract the Bearer token, if any. `None` means no
    // Authorization header (or malformed) — treated as
    // unauthenticated.
    let presented: Option<String> = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer ").map(|rest| rest.trim().to_string()));

    // Bootstrap exception: if no tokens are active, let the
    // first POST /api/admin/tokens through unauthenticated.
    // Every other admin route stays 401.
    if path == BOOTSTRAP_MINT_PATH && method == BOOTSTRAP_MINT_METHOD {
        match admin_tokens::count_active(meta.pool()).await {
            Ok(0) => return next.run(req).await,
            Ok(_) => {} // tokens exist — fall through to normal auth check
            Err(e) => {
                return ApiError::Internal(format!("admin auth: count_active: {e}"))
                    .into_response()
            }
        }
    }

    let Some(token) = presented else {
        return ApiError::Unauthorized("missing Bearer token".into()).into_response();
    };

    match admin_tokens::verify(meta.pool(), &token).await {
        Ok(Some(_token_id)) => next.run(req).await,
        Ok(None) => ApiError::Unauthorized("invalid or revoked bearer token".into())
            .into_response(),
        Err(e) => {
            ApiError::Internal(format!("admin auth: verify: {e}")).into_response()
        }
    }
}

/// Config carried on [`AppState`]. `meta` is `Option` for
/// symmetry with the existing auth config shape; a `Some`
/// admin_auth with `None` meta is an invariant violation
/// (500).
#[derive(Clone)]
pub struct AdminAuthConfig {
    pub meta: Option<Arc<MetaStore>>,
}
