//! Admin bearer-token admin endpoints.
//!
//! - `POST /api/admin/tokens` — mint a new token. Returns
//!   the plaintext ONCE. Persists only the SHA-256 hash.
//! - `GET /api/admin/tokens` — list active tokens (summaries).
//! - `DELETE /api/admin/tokens/:id` — soft-revoke.
//!
//! Auth is handled by the `admin_auth_middleware` layered on
//! `/api/admin/*`. The middleware's bootstrap exception lets
//! the first mint happen before any token exists.

use std::sync::Arc;

use agentdb_meta::{admin_tokens, MetaStore};
use axum::extract::{Extension, Path};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::ApiError;

#[derive(Debug, Clone, Deserialize, Default, ToSchema)]
pub struct MintTokenRequest {
    /// Free-form label so operators can tell tokens apart.
    /// Shown in the list endpoint; never used for auth.
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MintTokenResponse {
    pub token_id: String,
    /// **The plaintext token.** Returned ONCE at mint time;
    /// the server never carries it again. Callers must
    /// capture it immediately or issue a new one.
    pub token: String,
    pub description: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TokenSummaryView {
    pub token_id: String,
    pub description: String,
    pub created_at_ms: i64,
    pub revoked_at_ms: Option<i64>,
}

impl From<admin_tokens::TokenSummary> for TokenSummaryView {
    fn from(t: admin_tokens::TokenSummary) -> Self {
        Self {
            token_id: t.token_id,
            description: t.description,
            created_at_ms: t.created_at_ms,
            revoked_at_ms: t.revoked_at_ms,
        }
    }
}

#[utoipa::path(
    post,
    path = "/api/admin/tokens",
    tag = "admin",
    request_body = MintTokenRequest,
    responses(
        (status = 200, description = "New token minted; plaintext returned once", body = MintTokenResponse),
        (status = 401, description = "Admin auth required (except for the bootstrap mint when no tokens exist)"),
        (status = 503, description = "Meta store not configured"),
    ),
)]
pub async fn mint_token(
    meta: Option<Extension<Arc<MetaStore>>>,
    Json(body): Json<MintTokenRequest>,
) -> Result<Json<MintTokenResponse>, ApiError> {
    let Some(Extension(meta)) = meta else {
        return Err(ApiError::ServiceUnavailable(
            "meta store not configured on this server".into(),
        ));
    };
    let description = body.description.unwrap_or_default();
    let now_ms = crate::stream::now_ms() as i64;
    let minted = admin_tokens::mint(meta.pool(), &description, now_ms)
        .await
        .map_err(|e| ApiError::Internal(format!("mint token: {e}")))?;
    Ok(Json(MintTokenResponse {
        token_id: minted.summary.token_id,
        token: minted.plaintext,
        description: minted.summary.description,
        created_at_ms: minted.summary.created_at_ms,
    }))
}

#[utoipa::path(
    get,
    path = "/api/admin/tokens",
    tag = "admin",
    responses(
        (status = 200, description = "Active (non-revoked) tokens", body = Vec<TokenSummaryView>),
        (status = 401, description = "Admin auth required"),
        (status = 503, description = "Meta store not configured"),
    ),
)]
pub async fn list_tokens(
    meta: Option<Extension<Arc<MetaStore>>>,
) -> Result<Json<Vec<TokenSummaryView>>, ApiError> {
    let Some(Extension(meta)) = meta else {
        return Err(ApiError::ServiceUnavailable(
            "meta store not configured on this server".into(),
        ));
    };
    let all = admin_tokens::list_active(meta.pool())
        .await
        .map_err(|e| ApiError::Internal(format!("list tokens: {e}")))?;
    Ok(Json(all.into_iter().map(TokenSummaryView::from).collect()))
}

#[utoipa::path(
    delete,
    path = "/api/admin/tokens/{token_id}",
    tag = "admin",
    params(("token_id" = String, Path, description = "Token id to revoke")),
    responses(
        (status = 204, description = "Token revoked (or was already absent / revoked)"),
        (status = 401, description = "Admin auth required"),
        (status = 503, description = "Meta store not configured"),
    ),
)]
pub async fn revoke_token(
    meta: Option<Extension<Arc<MetaStore>>>,
    Path(token_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let Some(Extension(meta)) = meta else {
        return Err(ApiError::ServiceUnavailable(
            "meta store not configured on this server".into(),
        ));
    };
    let now_ms = crate::stream::now_ms() as i64;
    admin_tokens::revoke(meta.pool(), &token_id, now_ms)
        .await
        .map_err(|e| ApiError::Internal(format!("revoke token: {e}")))?;
    Ok(StatusCode::NO_CONTENT)
}
