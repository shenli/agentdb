//! JWT extraction for subscribe endpoints (spec § 6.13).
//!
//! Accepts a JWT via either `Authorization: Bearer <jwt>` header OR
//! `?access_token=<jwt>` query param. Both supplied → 400. Tenant has
//! no JWT config → returns `Ok(None)` so the caller falls through to
//! existing Basic auth.

use axum::http::{HeaderMap, StatusCode};

use crate::error::{ApiError, ErrorCode};
use crate::jwt::{verify_for_tenant, AgentDbClaims, JwtError};
use crate::AppState;

/// Extract and verify a JWT for the given tenant.
///
/// - `Ok(Some(claims))` — JWT verified successfully.
/// - `Ok(None)` — tenant has no JWT config (caller may fall back to Basic auth).
/// - `Err(ApiError)` — JWT supplied but invalid / expired / dual-supply / missing
///   when JWT is required for this tenant.
pub async fn extract_subscribe_jwt(
    state: &AppState,
    headers: &HeaderMap,
    access_token: Option<&str>,
    tenant: &str,
) -> Result<Option<AgentDbClaims>, ApiError> {
    if !state.jwt_config.per_tenant.contains_key(tenant) {
        return Ok(None);
    }

    let header_token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .map(str::to_string);

    let token = match (header_token, access_token) {
        (Some(_), Some(_)) => {
            return Err(ApiError::Coded(
                StatusCode::BAD_REQUEST,
                ErrorCode::BadRequestDualAuth,
                "supply jwt via Authorization header OR ?access_token=, not both".into(),
            ));
        }
        (Some(t), None) => t,
        (None, Some(t)) => t.to_string(),
        (None, None) => {
            return Err(ApiError::Coded(
                StatusCode::UNAUTHORIZED,
                ErrorCode::JwtInvalid,
                "missing jwt".into(),
            ));
        }
    };

    let claims = verify_for_tenant(&state.jwt_config, tenant, &token)
        .await
        .map_err(map_jwt_err)?;
    Ok(Some(claims))
}

fn map_jwt_err(e: JwtError) -> ApiError {
    match e {
        JwtError::Expired => ApiError::Coded(
            StatusCode::UNAUTHORIZED,
            ErrorCode::JwtExpired,
            "jwt expired".into(),
        ),
        JwtError::SessionForbidden => ApiError::Coded(
            StatusCode::FORBIDDEN,
            ErrorCode::SessionNotInJwt,
            "session not in jwt".into(),
        ),
        // TenantNotConfigured shouldn't reach here (we early-returned above);
        // treat as JwtInvalid if it does.
        JwtError::TenantNotConfigured
        | JwtError::Invalid(_)
        | JwtError::IssuerMismatch
        | JwtError::TenantMismatch => ApiError::Coded(
            StatusCode::UNAUTHORIZED,
            ErrorCode::JwtInvalid,
            format!("invalid jwt: {e}"),
        ),
    }
}
