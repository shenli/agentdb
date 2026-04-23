//! Admin-plane audit listing endpoint.
//!
//! The *writes* come from the admin_auth_middleware — every
//! request that reaches the middleware produces one row,
//! including 401 rejections. This module exposes the reads:
//! `GET /api/admin/audit` with filters that match the
//! [`agentdb_meta::audit::AuditFilter`] shape.
//!
//! Auth: layered under the same bearer-token middleware as
//! the rest of `/api/admin/*`. Listing the audit log is
//! itself an auditable action — operators see their own
//! reads reflected in subsequent listings.

use std::sync::Arc;

use agentdb_meta::{audit, MetaStore};
use axum::extract::{Extension, Query};
use axum::Json;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::ApiError;

#[derive(Debug, Clone, Deserialize, Default, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct AuditQuery {
    /// Cap on returned rows. Defaults to
    /// [`agentdb_meta::audit::DEFAULT_LIMIT`]; clamped to
    /// [`agentdb_meta::audit::MAX_LIMIT`].
    #[serde(default)]
    pub limit: Option<i64>,
    /// Inclusive lower bound on `ts_ms` (wall-clock, UTC).
    #[serde(default)]
    pub since_ms: Option<i64>,
    /// Exclusive upper bound on `ts_ms`. Pass the oldest
    /// `ts_ms` from a previous page to paginate.
    #[serde(default)]
    pub until_ms: Option<i64>,
    /// Exact match on an admin token id.
    #[serde(default)]
    pub actor_id: Option<String>,
    /// Literal prefix on `path`. LIKE wildcards are escaped
    /// so operators can paste a real URL prefix.
    #[serde(default)]
    pub path_prefix: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AuditRecordView {
    pub id: i64,
    pub ts_ms: i64,
    pub actor_token_id: Option<String>,
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub status: u16,
    pub client_ip: Option<String>,
}

impl From<audit::AuditRecord> for AuditRecordView {
    fn from(r: audit::AuditRecord) -> Self {
        Self {
            id: r.id,
            ts_ms: r.ts_ms,
            actor_token_id: r.actor_token_id,
            method: r.method,
            path: r.path,
            query: r.query,
            status: r.status,
            client_ip: r.client_ip,
        }
    }
}

#[utoipa::path(
    get,
    path = "/api/admin/audit",
    tag = "admin",
    params(AuditQuery),
    responses(
        (status = 200, description = "Audit rows (newest first)", body = Vec<AuditRecordView>),
        (status = 401, description = "Admin auth required"),
        (status = 503, description = "Meta store not configured"),
    ),
)]
pub async fn list_audit(
    meta: Option<Extension<Arc<MetaStore>>>,
    Query(q): Query<AuditQuery>,
) -> Result<Json<Vec<AuditRecordView>>, ApiError> {
    let Some(Extension(meta)) = meta else {
        return Err(ApiError::ServiceUnavailable(
            "meta store not configured on this server".into(),
        ));
    };
    let filter = audit::AuditFilter {
        limit: q.limit,
        since_ms: q.since_ms,
        until_ms: q.until_ms,
        actor_token_id: q.actor_id,
        path_prefix: q.path_prefix,
    };
    let rows = audit::list(meta.pool(), &filter)
        .await
        .map_err(|e| ApiError::Internal(format!("list audit: {e}")))?;
    Ok(Json(rows.into_iter().map(AuditRecordView::from).collect()))
}
