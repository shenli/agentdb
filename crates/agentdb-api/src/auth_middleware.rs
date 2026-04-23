//! M19: HTTP Basic Auth middleware for `/api/tenants/:tid/*`.
//!
//! When auth is disabled (AppState::auth is None), this is a
//! straight pass-through. When auth is enabled:
//!
//! 1. Read `Authorization: Basic <b64>`; 401 if absent/malformed.
//! 2. Decode to `(username, password)`; 401 if not UTF-8.
//! 3. Look up the user in the meta store; 401 if missing.
//! 4. Derive SaltedPassword with the stored salt + iterations,
//!    compare the resulting StoredKey in constant time; 401 on
//!    mismatch.
//! 5. Extract `:tenant_id` from the path; 403 if it doesn't
//!    match the authenticated user's tenant.
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

/// Axum middleware entry point. Registered on the `/api/tenants/*`
/// subtree only; admin-plane routes and health endpoints skip it.
pub async fn basic_auth_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let Some(auth_cfg) = &state.auth else {
        // No auth configured — pass through unchanged.
        return next.run(req).await;
    };
    let meta = match &auth_cfg.meta {
        Some(m) => m.clone(),
        None => return ApiError::Internal("auth configured without meta store".into()).into_response(),
    };

    // Path form: /api/tenants/{tenant_id}/...
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
    // Auth succeeded. Pass through; downstream handlers don't
    // need the user identity (the tenant in the path is now
    // trusted because we verified it matches).
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
