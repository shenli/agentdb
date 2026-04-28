//! M19: HTTP Basic Auth middleware for tenant-bound mutation
//! routes. Originally tenant-only; M26 closeout extended it
//! to cover session-keyed routes (`/api/sessions/:sid/...`)
//! whose tenant is auth-derived rather than path-supplied.
//!
//! When auth is disabled (AppState::auth is None), this is a
//! straight pass-through and downstream handlers see no
//! `AuthedTenant` extension. When auth is enabled:
//!
//! 1. Read `Authorization: Basic <b64>`; 401 if absent/malformed.
//! 2. Decode to `(username, password)`; 401 if not UTF-8.
//! 3. Look up the user in the meta store; 401 if missing.
//! 4. Derive SaltedPassword with the stored salt + iterations,
//!    compare the resulting StoredKey in constant time; 401 on
//!    mismatch.
//! 5. If the path is `/api/tenants/{tid}/...`, verify
//!    `tid == user.tenant_external`; 403 on mismatch. (The
//!    `/api/sessions/{sid}/...` shapes have no path tenant —
//!    handlers do the locator-vs-authed-tenant check there
//!    because they already need to look the session up.)
//! 6. Insert [`AuthedTenant`] into request extensions so
//!    handlers can derive tenant from auth instead of trusting
//!    the path or body.
//!
//! PBKDF2 per-request is ~5–10 ms on commodity hardware. For
//! hot paths that's noticeable; a later PR can add a short-TTL
//! cache of `(user_id, computed_stored_key)` keyed by password
//! hash so the second request on the same connection skips
//! PBKDF2. Pending real usage data first.

use agentdb_meta::scram::{self, Credentials};
use agentdb_meta::users;
use axum::extract::{Request, State};
use axum::http::header;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use subtle::ConstantTimeEq;

use crate::error::ApiError;
use crate::AppState;

/// Auth-derived tenant identity. Inserted into request
/// extensions by [`basic_auth_middleware`] when auth succeeds.
/// Handlers consume this via `Extension<AuthedTenant>` to derive
/// tenant from credentials rather than from path/body, which is
/// the M26 acceptance contract: tenant identity is auth-bound,
/// not client-supplied.
///
/// Absence of the extension means auth was disabled (or the
/// route is mounted outside the auth boundary, which is a bug);
/// handlers fall back to legacy path/body tenant resolution in
/// that case so single-process tests and pre-auth deployments
/// keep working.
#[derive(Debug, Clone)]
pub struct AuthedTenant {
    pub external_id: String,
    pub user_id: String,
}

/// Axum middleware entry point. Registered on every tenant-
/// bound mutation subtree (`/api/tenants/:tid/*` AND
/// `/api/sessions/:sid/*`); admin-plane routes and health
/// endpoints skip it.
pub async fn basic_auth_middleware(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Response {
    let Some(auth_cfg) = &state.auth else {
        // No auth configured — pass through unchanged. Handlers
        // see no AuthedTenant extension and fall back to legacy
        // tenant resolution.
        return next.run(req).await;
    };
    let meta = match &auth_cfg.meta {
        Some(m) => m.clone(),
        None => return ApiError::Internal("auth configured without meta store".into()).into_response(),
    };

    // Path form: /api/tenants/{tenant_id}/... — only this
    // shape carries a path tenant; /api/sessions/... shapes
    // do not, and handlers verify tenant from the locator.
    let path = req.uri().path().to_string();
    let tenant_from_path = extract_tenant_id(&path);

    let header = match req.headers().get(header::AUTHORIZATION) {
        Some(h) => h.clone(),
        None => return ApiError::Unauthorized("missing Authorization header".into()).into_response(),
    };
    let header_str = match header.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ApiError::Unauthorized("non-ascii Authorization".into()).into_response(),
    };
    let (user_id, password) = match parse_basic(&header_str) {
        Ok(pair) => pair,
        Err(msg) => return ApiError::Unauthorized(msg.into()).into_response(),
    };

    let user = match users::get(meta.pool(), &user_id).await {
        Ok(Some(u)) => u,
        _ => return ApiError::Unauthorized("auth failed".into()).into_response(),
    };
    if !verify_password(&password, &user.credentials) {
        return ApiError::Unauthorized("auth failed".into()).into_response();
    }
    if let Some(tid) = tenant_from_path {
        if tid != user.tenant_external {
            return ApiError::Forbidden(format!(
                "user {user_id} is bound to tenant {} but request targets {tid}",
                user.tenant_external
            ))
            .into_response();
        }
    }
    // Auth succeeded. Hand the tenant identity to downstream
    // handlers via an extension so they don't have to re-derive
    // it from the path. Path-tenant verification (above) is
    // belt-and-braces with the handlers' own checks.
    req.extensions_mut().insert(AuthedTenant {
        external_id: user.tenant_external.clone(),
        user_id,
    });
    next.run(req).await
}

/// Verify a password by reconstructing `StoredKey` from the
/// stored `(salt, iterations)` and comparing in constant time.
/// Matches what `scram::derive_credentials_with_salt` does at
/// user-create time.
fn verify_password(password: &str, stored: &Credentials) -> bool {
    let derived =
        scram::derive_credentials_with_salt(password, &stored.salt, stored.iterations);
    derived
        .stored_key
        .ct_eq(&stored.stored_key)
        .unwrap_u8()
        == 1
}

/// Parse an `Authorization: Basic <b64>` header. Returns
/// `(user, pass)` or an error message suitable for 401 bodies.
fn parse_basic(header: &str) -> Result<(String, String), &'static str> {
    let rest = header.strip_prefix("Basic ").ok_or("not Basic scheme")?;
    let decoded = B64.decode(rest.trim()).map_err(|_| "malformed base64")?;
    let s = std::str::from_utf8(&decoded).map_err(|_| "credentials not UTF-8")?;
    let (u, p) = s.split_once(':').ok_or("credentials missing colon")?;
    if u.is_empty() {
        return Err("empty username");
    }
    Ok((u.to_string(), p.to_string()))
}

/// Extract `{tenant_id}` from a `/api/tenants/{tid}/...` path.
/// Returns `None` if the path doesn't match that prefix (which
/// means auth isn't applicable — the middleware should only be
/// registered on tenant routes, but defend in depth).
fn extract_tenant_id(path: &str) -> Option<&str> {
    let rest = path.strip_prefix("/api/tenants/")?;
    match rest.split_once('/') {
        Some((tid, _)) => Some(tid),
        None => Some(rest),
    }
}

/// Resolves the tenant for a session-keyed v2 route by combining the
/// auth-derived tenant (when present) and the session locator's
/// recorded tenant. Used by `append_and_fetch_session_keyed`,
/// `fetch_recent_session_keyed`, `subscribe_session_keyed`, and
/// (M27 Task 4) the v2 channel handlers.
///
/// `route_hint` is woven into the 400 error message's recovery hint
/// (e.g., `"append_and_fetch"`, `"fetch_recent"`, `"subscribe"`) so a
/// developer hitting a pre-M26 session sees a path forward.
///
/// Returns `Ok(tenant_external_id)` on success, or `Err(Response)`
/// on the 400 / 403 / 500 error paths.
pub async fn resolve_session_tenant(
    state: &AppState,
    session: &str,
    authed: Option<&AuthedTenant>,
    route_hint: &'static str,
) -> Result<String, axum::response::Response> {
    use axum::response::IntoResponse;

    let authed_tenant = authed.map(|a| a.external_id.as_str());
    let authed_user = authed.map(|a| a.user_id.as_str());
    let locator_tenant = match state.registry.meta_store() {
        Some(meta) => match agentdb_meta::session_locators::get(meta.pool(), session).await {
            Ok(Some(loc)) => loc.tenant_external_id,
            Ok(None) => None,
            Err(e) => {
                return Err(ApiError::Core(agentdb_core::Error::Invalid {
                    field: None,
                    message: format!("session locator lookup failed: {e}"),
                })
                .into_response());
            }
        },
        None => None,
    };
    match (authed_tenant, locator_tenant.as_deref()) {
        (Some(a), Some(l)) if a != l => {
            Err(ApiError::Forbidden(format!(
                "user {} is bound to tenant {a} but session {session} belongs to tenant {l}",
                authed_user.unwrap_or("?")
            ))
            .into_response())
        }
        (Some(a), _) => Ok(a.to_string()),
        (None, Some(l)) => Ok(l.to_string()),
        (None, None) => Err(ApiError::Core(agentdb_core::Error::Invalid {
            field: Some("tenant".into()),
            message: format!(
                "cannot resolve tenant for session {session}: no Authorization header, \
                 no session locator. Use /api/tenants/<tid>/sessions/<sid>/{route_hint} \
                 for pre-M26 sessions, or enable auth"
            ),
        })
        .into_response()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_accepts_well_formed_header() {
        let hdr = format!(
            "Basic {}",
            B64.encode("alice:pencil".as_bytes())
        );
        let (u, p) = parse_basic(&hdr).unwrap();
        assert_eq!(u, "alice");
        assert_eq!(p, "pencil");
    }

    #[test]
    fn parse_basic_rejects_wrong_scheme() {
        let hdr = "Bearer whatever";
        assert!(parse_basic(hdr).is_err());
    }

    #[test]
    fn parse_basic_rejects_malformed_b64() {
        assert!(parse_basic("Basic not-b64!!").is_err());
    }

    #[test]
    fn parse_basic_rejects_missing_colon() {
        let hdr = format!("Basic {}", B64.encode(b"alicepencil"));
        assert!(parse_basic(&hdr).is_err());
    }

    #[test]
    fn extract_tenant_id_handles_various_path_shapes() {
        assert_eq!(extract_tenant_id("/api/tenants/t-1/sessions/s"), Some("t-1"));
        assert_eq!(extract_tenant_id("/api/tenants/t-42"), Some("t-42"));
        assert_eq!(extract_tenant_id("/api/admin/users"), None);
        assert_eq!(extract_tenant_id("/"), None);
    }

    #[test]
    fn verify_password_accepts_correct_password() {
        let creds = scram::derive_credentials("hunter2", 4096);
        assert!(verify_password("hunter2", &creds));
    }

    #[test]
    fn verify_password_rejects_wrong_password() {
        let creds = scram::derive_credentials("hunter2", 4096);
        assert!(!verify_password("other", &creds));
    }
}
