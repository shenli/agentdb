//! `POST /api/admin/reload` — hot-reload reloadable config
//! without restarting the server.
//!
//! The server binary owns the reloadable subsystems (tracing
//! filter in particular, since it's global-state; and the
//! tenant-quota + slow-query handles on the BoxRegistry). The
//! API hands off to a `ReloadEngine` trait implemented there,
//! so tests can stand in a fake and the handler stays
//! reusable.
//!
//! Targets currently reloaded on a single call:
//!   - `AGENTDB_SLOW_QUERY_THRESHOLD_MS`
//!   - `AGENTDB_SLOW_QUERY_BUFFER_CAPACITY`
//!   - `RUST_LOG` / tracing `EnvFilter` (if set; otherwise
//!     the existing filter is kept)
//!   - `tenant_quotas` table — re-read from meta, applied via
//!     [`agentdb_core::TenantQuotaRegistry::hydrate`].
//!
//! Targets intentionally NOT reloaded:
//!   - `AGENTDB_ADMISSION_CAPACITY` — resizing a running
//!     Tokio semaphore mid-flight is a footgun.
//!   - TLS certs — handle rotation separately (a future PR).
//!   - Meta / KV / object-store backends — they're not
//!     hot-swappable.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::extract::{Extension, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::ApiError;
use crate::AppState;

/// Future returned by a reload — boxed so the handle can be
/// an object-safe `Arc<dyn Fn() -> _>`. Keeps this module
/// decoupled from tracing-subscriber + binary internals.
pub type ReloadFuture =
    Pin<Box<dyn Future<Output = Result<ReloadSummary, String>> + Send>>;

/// Opaque reload handle carried on the app as a request
/// extension. `None` means reload isn't wired (test
/// registries, or a binary path that didn't install it).
pub type ReloadHandle = Arc<dyn Fn() -> ReloadFuture + Send + Sync>;

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ReloadSummary {
    /// Current slow-query threshold in ms, post-reload.
    pub slow_query_threshold_ms: u64,
    /// Current slow-query buffer capacity, post-reload.
    pub slow_query_buffer_capacity: u64,
    /// Current tracing filter string, post-reload. Empty
    /// when no filter change was applied.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tracing_filter: String,
    /// Tenants whose quota was added in this reload (new
    /// rows in the meta table since the last hydration).
    #[serde(default)]
    pub tenant_quotas_added: Vec<String>,
    /// Tenants whose quota was removed (deleted from the
    /// meta table since the last hydration).
    #[serde(default)]
    pub tenant_quotas_removed: Vec<String>,
    /// Wall-clock ms when the reload completed.
    pub reloaded_at_ms: i64,
}

#[utoipa::path(
    post,
    path = "/api/admin/reload",
    tag = "admin",
    responses(
        (status = 200, description = "Reload successful", body = ReloadSummary),
        (status = 401, description = "Admin auth required"),
        (status = 500, description = "Reload encountered an unrecoverable error (running config preserved)"),
        (status = 503, description = "Reload engine not installed"),
    ),
)]
pub async fn post_reload(
    State(_state): State<AppState>,
    engine: Option<Extension<ReloadHandle>>,
) -> Result<Json<ReloadSummary>, ApiError> {
    let Some(Extension(engine)) = engine else {
        return Err(ApiError::ServiceUnavailable(
            "reload engine not installed on this server".into(),
        ));
    };
    match (engine)().await {
        Ok(summary) => Ok(Json(summary)),
        Err(e) => Err(ApiError::Internal(format!("reload: {e}"))),
    }
}
