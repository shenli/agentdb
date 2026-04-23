//! Admin surface for managing SCRAM-SHA-256 user accounts (M19).
//!
//! Endpoints:
//!   POST   /api/admin/users                 create
//!   GET    /api/admin/users                 list (summaries only)
//!   DELETE /api/admin/users/:user_id        delete (idempotent)
//!
//! These are admin-plane endpoints — they stay **unauthenticated**
//! in this PR. The threat model assumes the admin port is
//! restricted by network isolation (loopback, private VPC). A
//! separate PR adds admin-plane mTLS or bearer auth.

use std::sync::Arc;

use agentdb_meta::{users, MetaStore};
use axum::extract::{Extension, Path};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::ApiError;

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateUserRequest {
    /// SCRAM username. Used as both the auth identity and the
    /// stable id for `DELETE /api/admin/users/:user_id`.
    pub user_id: String,
    /// External tenant id this user can access. After SCRAM
    /// handshake completes, this connection is clamped to this
    /// tenant — requests targeting any other tenant are rejected
    /// with 403.
    pub tenant_external: String,
    /// Plaintext password. Goes through PBKDF2 on the server
    /// and is never persisted. Must be sent over TLS in any
    /// deployment past "localhost dev".
    pub password: String,
    /// PBKDF2 iteration count. Defaults to the RFC floor (4096)
    /// when omitted. Higher values slow down per-auth PBKDF2
    /// work; set for high-security deployments.
    #[serde(default)]
    pub iterations: Option<u32>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CreateUserResponse {
    pub user_id: String,
    pub tenant_external: String,
    pub created_at_ms: i64,
}

#[utoipa::path(
    post,
    path = "/api/admin/users",
    tag = "admin",
    request_body = CreateUserRequest,
    responses(
        (status = 201, description = "User created", body = CreateUserResponse),
        (status = 409, description = "user_id already exists"),
        (status = 503, description = "Meta store not configured"),
    ),
)]
pub async fn create_user(
    meta: Option<Extension<Arc<MetaStore>>>,
    Json(body): Json<CreateUserRequest>,
) -> Result<(StatusCode, Json<CreateUserResponse>), ApiError> {
    let Some(Extension(meta)) = meta else {
        return Err(ApiError::ServiceUnavailable(
            "meta store not configured on this server".into(),
        ));
    };
    let iter = body
        .iterations
        .unwrap_or(agentdb_meta::scram::DEFAULT_ITERATIONS);
    let now_ms = crate::stream::now_ms() as i64;
    let user = users::create(
        meta.pool(),
        &body.user_id,
        &body.tenant_external,
        &body.password,
        iter,
        now_ms,
    )
    .await
    .map_err(|e| match e {
        agentdb_meta::MetaError::Conflict(msg) => ApiError::Conflict(msg),
        other => ApiError::Internal(format!("create user: {other}")),
    })?;
    Ok((
        StatusCode::CREATED,
        Json(CreateUserResponse {
            user_id: user.user_id,
            tenant_external: user.tenant_external,
            created_at_ms: user.created_at_ms,
        }),
    ))
}

/// Response envelope for `GET /api/admin/users`. Wraps
/// `users::UserSummary` so utoipa can generate a schema without
/// making the meta crate depend on utoipa.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UserListItem {
    pub user_id: String,
    pub tenant_external: String,
    pub created_at_ms: i64,
}

impl From<users::UserSummary> for UserListItem {
    fn from(u: users::UserSummary) -> Self {
        Self {
            user_id: u.user_id,
            tenant_external: u.tenant_external,
            created_at_ms: u.created_at_ms,
        }
    }
}

#[utoipa::path(
    get,
    path = "/api/admin/users",
    tag = "admin",
    responses(
        (status = 200, description = "Users by created_at_ms ascending", body = Vec<UserListItem>),
        (status = 503, description = "Meta store not configured"),
    ),
)]
pub async fn list_users(
    meta: Option<Extension<Arc<MetaStore>>>,
) -> Result<Json<Vec<UserListItem>>, ApiError> {
    let Some(Extension(meta)) = meta else {
        return Err(ApiError::ServiceUnavailable(
            "meta store not configured on this server".into(),
        ));
    };
    let all = users::list(meta.pool())
        .await
        .map_err(|e| ApiError::Internal(format!("list users: {e}")))?;
    Ok(Json(all.into_iter().map(UserListItem::from).collect()))
}

#[utoipa::path(
    delete,
    path = "/api/admin/users/{user_id}",
    tag = "admin",
    params(("user_id" = String, Path, description = "SCRAM username")),
    responses(
        (status = 204, description = "User deleted (or did not exist)"),
        (status = 503, description = "Meta store not configured"),
    ),
)]
pub async fn delete_user(
    meta: Option<Extension<Arc<MetaStore>>>,
    Path(user_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let Some(Extension(meta)) = meta else {
        return Err(ApiError::ServiceUnavailable(
            "meta store not configured on this server".into(),
        ));
    };
    users::delete(meta.pool(), &user_id)
        .await
        .map_err(|e| ApiError::Internal(format!("delete user: {e}")))?;
    // Idempotent: 204 whether or not a row was actually removed.
    // An endpoint that 404s on re-delete encourages callers to
    // build fragile "check-then-delete" patterns.
    Ok(StatusCode::NO_CONTENT)
}
