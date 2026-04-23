//! `GET /api/admin/slow-queries` — readonly view over the
//! in-memory slow-query ring buffer.
//!
//! Rows stay in memory — restart drops them. Filters:
//!   - `kind` — `append` / `find` / `aggregate`
//!   - `tenant` — exact match on external tenant id
//!   - `limit` — default 100, clamped to 10_000
//!   - `after_id` — pagination (return rows with id > this)
//!
//! When the buffer isn't attached (legacy / test paths), the
//! response is an empty array rather than an error — "nothing
//! has been slow" and "slow-query recording is disabled" are
//! the same thing from the operator's perspective.

use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;
use utoipa::IntoParams;

use crate::error::ApiError;
use crate::AppState;

#[derive(Debug, Clone, Deserialize, Default, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct SlowQueryQuery {
    /// One of `append`, `find`, `aggregate`. Omit for all.
    #[serde(default)]
    pub kind: Option<agentdb_core::SlowQueryKind>,
    /// Exact external tenant id. Omit for all tenants.
    #[serde(default)]
    pub tenant: Option<String>,
    /// Max rows to return. Default 100, clamped to 10_000.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Return only rows whose id is strictly greater than
    /// this value. Pair with the largest id from a previous
    /// page for pagination.
    #[serde(default)]
    pub after_id: Option<u64>,
}

#[utoipa::path(
    get,
    path = "/api/admin/slow-queries",
    tag = "admin",
    params(SlowQueryQuery),
    responses(
        (status = 200, description = "Recent slow-query records (newest first)", body = Vec<agentdb_core::SlowQueryRecord>),
        (status = 401, description = "Admin auth required"),
    ),
)]
pub async fn list_slow_queries(
    State(state): State<AppState>,
    Query(q): Query<SlowQueryQuery>,
) -> Result<Json<Vec<agentdb_core::SlowQueryRecord>>, ApiError> {
    let Some(buf) = state.registry.slow_queries() else {
        return Ok(Json(Vec::new()));
    };
    let filter = agentdb_core::SlowQueryFilter {
        kind: q.kind,
        tenant_id: q.tenant,
        limit: q.limit,
        after_id: q.after_id,
    };
    Ok(Json(buf.snapshot(&filter)))
}
