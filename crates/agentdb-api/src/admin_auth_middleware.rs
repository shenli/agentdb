//! Admin-plane bearer-token auth (post-M25 security patch).
//!
//! Layered on `/api/admin/*`. Behavior depends on
//! `AppState::admin_auth`:
//!
//! - `None` → pass-through (backward compat; dev mode and
//!   `agentdb serve --dev`). No audit is written — without an
//!   admin-auth config there is no meta store to write to.
//! - `Some { meta }` + no active tokens exist → pass-through
//!   for `POST /api/admin/tokens` only (bootstrap); every
//!   other admin route 401s. Once the first token is
//!   active, mint also requires auth.
//! - `Some { meta }` + tokens exist → `Authorization:
//!   Bearer <token>` required; look up, verify hash, allow.
//!
//! RBAC (scope enforcement): each token carries a scope.
//!
//! - `admin` — any method on `/api/admin/*`.
//! - `readonly` — `GET` only; `POST/PUT/DELETE` return 403.
//!
//! The check runs after token verification, before dispatch.
//! A scope-403 is still audited with the actor_token_id so
//! operators can see "alice tried to revoke a token but
//! couldn't" in the audit log.
//!
//! Audit: every request that reaches the middleware (when
//! admin_auth is configured) writes one row to
//! `admin_audit`, whether authenticated, bootstrap, 401,
//! or scope-403. The insert happens after the downstream
//! response so we can log the status code; the middleware
//! swallows insert errors (but logs them) rather than
//! failing the request.
//!
//! Always open: `/health/*`, `/metrics`, `/openapi.json`,
//! `/dashboard`, `/api/stream`. Those don't live under
//! `/api/admin/` so this middleware never sees them.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_meta::{admin_tokens, audit, MetaStore};
use agentdb_meta::admin_tokens::AdminScope;
use axum::extract::{Request, State};
use axum::http::{header, Method};
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
    let query = req.uri().query().map(|s| s.to_string());
    let method = req.method().as_str().to_string();
    let client_ip = req
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        // Take the leftmost entry — that's the originator per
        // the XFF convention when multiple proxies chain.
        .and_then(|s| s.split(',').next())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    // Extract the Bearer token, if any. `None` means no
    // Authorization header (or malformed) — treated as
    // unauthenticated.
    let presented: Option<String> = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer ").map(|rest| rest.trim().to_string()));

    // Decide the request's fate. `actor` is `Some(token_id,
    // scope)` when auth succeeded, `None` for bootstrap /
    // rejected requests. `reject` is `Some` when we already
    // shaped a 4xx/5xx response (auth failure, scope block).
    let (actor, mut reject): (Option<(String, AdminScope)>, Option<Response>) = {
        if path == BOOTSTRAP_MINT_PATH && method == BOOTSTRAP_MINT_METHOD {
            match admin_tokens::count_active(meta.pool()).await {
                Ok(0) => (None, None),
                Ok(_) => authenticate(&meta, presented.as_deref()).await,
                Err(e) => (
                    None,
                    Some(
                        ApiError::Internal(format!("admin auth: count_active: {e}"))
                            .into_response(),
                    ),
                ),
            }
        } else {
            authenticate(&meta, presented.as_deref()).await
        }
    };

    // Scope check: readonly tokens can only GET.
    if reject.is_none() {
        if let Some((_, scope)) = &actor {
            if !scope_permits(*scope, req.method()) {
                reject = Some(
                    ApiError::Forbidden(format!(
                        "this token has scope '{}' and cannot perform a {} request",
                        scope.as_str(),
                        method,
                    ))
                    .into_response(),
                );
            }
        }
    }

    let actor_token_id = actor.map(|(id, _)| id);

    let response = match reject {
        Some(r) => r,
        None => next.run(req).await,
    };

    let status = response.status().as_u16();
    let ts_ms = now_ms();
    // Fire-and-forget audit write. We spawn because the
    // response has already been shaped; the body flows back
    // to the client regardless of whether the audit row
    // lands. Failures to write are logged (once, at warn)
    // but do not affect the client.
    tokio::spawn(write_audit_row(
        meta,
        ts_ms,
        actor_token_id,
        method,
        path,
        query,
        status,
        client_ip,
    ));
    response
}

/// Verify the presented token and return the (actor, reject)
/// pair. `reject` is `Some` on any auth failure; `None` means
/// the request should flow through.
async fn authenticate(
    meta: &MetaStore,
    presented: Option<&str>,
) -> (Option<(String, AdminScope)>, Option<Response>) {
    let Some(token) = presented else {
        return (
            None,
            Some(ApiError::Unauthorized("missing Bearer token".into()).into_response()),
        );
    };
    match admin_tokens::verify(meta.pool(), token).await {
        Ok(Some((id, scope))) => (Some((id, scope)), None),
        Ok(None) => (
            None,
            Some(
                ApiError::Unauthorized("invalid or revoked bearer token".into())
                    .into_response(),
            ),
        ),
        Err(e) => (
            None,
            Some(ApiError::Internal(format!("admin auth: verify: {e}")).into_response()),
        ),
    }
}

/// Decide whether `scope` permits `method` on an admin-plane
/// request. Admin tokens can do anything; readonly tokens
/// can only GET. HEAD and OPTIONS aren't used by the admin
/// surface today — refuse to make a decision about them
/// (returns false) so a future route addition doesn't quietly
/// let a readonly token through.
fn scope_permits(scope: AdminScope, method: &Method) -> bool {
    match scope {
        AdminScope::Admin => true,
        AdminScope::Readonly => method == Method::GET,
    }
}

#[allow(clippy::too_many_arguments)]
async fn write_audit_row(
    meta: Arc<MetaStore>,
    ts_ms: i64,
    actor_token_id: Option<String>,
    method: String,
    path: String,
    query: Option<String>,
    status: u16,
    client_ip: Option<String>,
) {
    if let Err(e) = audit::insert(
        meta.pool(),
        ts_ms,
        actor_token_id,
        method,
        path,
        query,
        status,
        client_ip,
    )
    .await
    {
        tracing::warn!(error = %e, "admin audit write failed");
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Config carried on [`AppState`]. `meta` is `Option` for
/// symmetry with the existing auth config shape; a `Some`
/// admin_auth with `None` meta is an invariant violation
/// (500).
#[derive(Clone)]
pub struct AdminAuthConfig {
    pub meta: Option<Arc<MetaStore>>,
}
