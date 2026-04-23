//! Admin surface for global per-tenant rate limits.
//!
//! - `GET    /api/admin/tenant-quotas`              list all
//! - `GET    /api/admin/tenant-quotas/:tenant_id`   get one
//! - `PUT    /api/admin/tenant-quotas/:tenant_id`   upsert
//! - `DELETE /api/admin/tenant-quotas/:tenant_id`   clear
//!
//! Writes touch two places: the meta store (persistent source
//! of truth, survives restart) and the in-memory
//! [`TenantQuotaRegistry`] on [`agentdb_core::BoxRegistry`]
//! (hot path enforcement). Meta wins: meta writes before the
//! in-memory update so a partial failure leaves the system
//! with the persistent config as the authority.

use std::sync::Arc;

use agentdb_meta::{tenant_quotas, MetaStore};
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::ApiError;
use crate::AppState;

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct SetTenantQuotaRequest {
    pub max_qps: u64,
    pub burst: u64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TenantQuotaView {
    pub tenant_id: String,
    pub max_qps: u64,
    pub burst: u64,
    /// Wall-clock ms when the quota was last set via PUT.
    pub updated_at_ms: i64,
    /// Running count of denials this process has served for
    /// this tenant since the bucket was (re)configured. Not
    /// persisted — resets on restart. Operators watching
    /// `agentdb_tenant_quota_denies_total{tenant="..."}` on
    /// `/metrics` get the same number across restarts.
    #[serde(default)]
    pub deny_count: u64,
}

#[utoipa::path(
    get,
    path = "/api/admin/tenant-quotas",
    tag = "admin",
    responses(
        (status = 200, description = "Every configured tenant quota", body = Vec<TenantQuotaView>),
        (status = 401, description = "Admin auth required"),
        (status = 503, description = "Meta store not configured"),
    ),
)]
pub async fn list_tenant_quotas(
    State(state): State<AppState>,
    meta: Option<Extension<Arc<MetaStore>>>,
) -> Result<Json<Vec<TenantQuotaView>>, ApiError> {
    let Some(Extension(meta)) = meta else {
        return Err(ApiError::ServiceUnavailable(
            "meta store not configured on this server".into(),
        ));
    };
    let rows = tenant_quotas::list_all(meta.pool())
        .await
        .map_err(|e| ApiError::Internal(format!("list tenant quotas: {e}")))?;
    // Merge per-tenant deny counters from the in-memory
    // registry. Tenants present in meta but not in the
    // registry (e.g. startup race) still list; deny_count = 0.
    let denies: std::collections::HashMap<String, u64> = state
        .registry
        .tenant_quotas()
        .map(|q| q.deny_snapshot().into_iter().collect())
        .unwrap_or_default();
    Ok(Json(
        rows.into_iter()
            .map(|q| TenantQuotaView {
                deny_count: denies.get(&q.tenant_id).copied().unwrap_or(0),
                tenant_id: q.tenant_id,
                max_qps: q.max_qps,
                burst: q.burst,
                updated_at_ms: q.updated_at_ms,
            })
            .collect(),
    ))
}

#[utoipa::path(
    get,
    path = "/api/admin/tenant-quotas/{tenant_id}",
    tag = "admin",
    params(("tenant_id" = String, Path, description = "External tenant id")),
    responses(
        (status = 200, description = "The configured quota", body = TenantQuotaView),
        (status = 404, description = "No quota set for this tenant"),
        (status = 401, description = "Admin auth required"),
        (status = 503, description = "Meta store not configured"),
    ),
)]
pub async fn get_tenant_quota(
    State(state): State<AppState>,
    meta: Option<Extension<Arc<MetaStore>>>,
    Path(tenant_id): Path<String>,
) -> Result<Json<TenantQuotaView>, ApiError> {
    let Some(Extension(meta)) = meta else {
        return Err(ApiError::ServiceUnavailable(
            "meta store not configured on this server".into(),
        ));
    };
    let Some(q) = tenant_quotas::get(meta.pool(), &tenant_id)
        .await
        .map_err(|e| ApiError::Internal(format!("get tenant quota: {e}")))?
    else {
        return Err(ApiError::Core(agentdb_core::Error::NotFound {
            resource: agentdb_core::Resource::Tenant,
            id: format!("{tenant_id} (no quota configured)"),
        }));
    };
    let deny_count = state
        .registry
        .tenant_quotas()
        .and_then(|r| {
            r.deny_snapshot()
                .into_iter()
                .find(|(t, _)| t == &tenant_id)
                .map(|(_, c)| c)
        })
        .unwrap_or(0);
    Ok(Json(TenantQuotaView {
        tenant_id: q.tenant_id,
        max_qps: q.max_qps,
        burst: q.burst,
        updated_at_ms: q.updated_at_ms,
        deny_count,
    }))
}

#[utoipa::path(
    put,
    path = "/api/admin/tenant-quotas/{tenant_id}",
    tag = "admin",
    params(("tenant_id" = String, Path, description = "External tenant id")),
    request_body = SetTenantQuotaRequest,
    responses(
        (status = 200, description = "Quota upserted", body = TenantQuotaView),
        (status = 400, description = "max_qps or burst is zero"),
        (status = 401, description = "Admin auth required"),
        (status = 503, description = "Meta store not configured"),
    ),
)]
pub async fn put_tenant_quota(
    State(state): State<AppState>,
    meta: Option<Extension<Arc<MetaStore>>>,
    Path(tenant_id): Path<String>,
    Json(body): Json<SetTenantQuotaRequest>,
) -> Result<Json<TenantQuotaView>, ApiError> {
    if body.max_qps == 0 {
        return Err(ApiError::Core(agentdb_core::Error::Invalid {
            field: Some("max_qps".into()),
            message: "max_qps must be > 0".into(),
        }));
    }
    if body.burst == 0 {
        return Err(ApiError::Core(agentdb_core::Error::Invalid {
            field: Some("burst".into()),
            message: "burst must be > 0".into(),
        }));
    }
    let Some(Extension(meta)) = meta else {
        return Err(ApiError::ServiceUnavailable(
            "meta store not configured on this server".into(),
        ));
    };
    let now_ms = crate::stream::now_ms() as i64;
    tenant_quotas::upsert(meta.pool(), &tenant_id, body.max_qps, body.burst, now_ms)
        .await
        .map_err(|e| ApiError::Internal(format!("upsert tenant quota: {e}")))?;
    // Meta write landed; update the in-memory registry so
    // subsequent requests see the new bucket.
    if let Some(reg) = state.registry.tenant_quotas() {
        reg.set(
            &tenant_id,
            agentdb_core::TenantQuotaConfig {
                max_qps: body.max_qps,
                burst: body.burst,
            },
        );
    }
    Ok(Json(TenantQuotaView {
        tenant_id,
        max_qps: body.max_qps,
        burst: body.burst,
        updated_at_ms: now_ms,
        deny_count: 0,
    }))
}

#[utoipa::path(
    delete,
    path = "/api/admin/tenant-quotas/{tenant_id}",
    tag = "admin",
    params(("tenant_id" = String, Path, description = "External tenant id")),
    responses(
        (status = 204, description = "Quota cleared (or was already absent)"),
        (status = 401, description = "Admin auth required"),
        (status = 503, description = "Meta store not configured"),
    ),
)]
pub async fn delete_tenant_quota(
    State(state): State<AppState>,
    meta: Option<Extension<Arc<MetaStore>>>,
    Path(tenant_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let Some(Extension(meta)) = meta else {
        return Err(ApiError::ServiceUnavailable(
            "meta store not configured on this server".into(),
        ));
    };
    tenant_quotas::delete(meta.pool(), &tenant_id)
        .await
        .map_err(|e| ApiError::Internal(format!("delete tenant quota: {e}")))?;
    if let Some(reg) = state.registry.tenant_quotas() {
        reg.clear(&tenant_id);
    }
    Ok(StatusCode::NO_CONTENT)
}
