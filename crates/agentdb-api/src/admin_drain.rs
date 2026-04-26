//! `POST /api/admin/drain` — flip this process's drain switch.
//! `GET  /api/admin/drain` — inspect the switch.
//!
//! Drain is **permanent for the process's lifetime**. There is no
//! undrain path; operators restart the process to reverse a drain.
//! See `docs/runbooks/node_draining.md` for the operational
//! contract.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::Serialize;
use utoipa::ToSchema;

use crate::AppState;

#[derive(Debug, Serialize, ToSchema)]
pub struct DrainStatus {
    /// Node id of this process. `None` for legacy-path registries
    /// (no control plane); those registries can't be drained.
    pub node_id: Option<String>,
    /// Whether this process is currently draining.
    pub is_draining: bool,
    /// Wall-clock ms of the first drain call on this process, or
    /// `None` before the first call.
    pub drained_at_ms: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DrainUnavailable {
    pub message: String,
}

#[utoipa::path(
    get,
    path = "/api/admin/drain",
    tag = "admin",
    responses(
        (status = 200, description = "Drain state of this process", body = DrainStatus),
    ),
)]
pub async fn get_drain(State(state): State<AppState>) -> Json<DrainStatus> {
    Json(DrainStatus {
        node_id: state.registry.node_id().map(|s| s.to_string()),
        is_draining: state.registry.is_draining(),
        drained_at_ms: state.registry.drained_at_ms(),
    })
}

#[utoipa::path(
    post,
    path = "/api/admin/drain",
    tag = "admin",
    responses(
        (status = 200, description = "Drain activated (idempotent on repeat)", body = DrainStatus),
        (status = 503, description = "This process has no control-plane node id — cannot drain a legacy-path registry", body = DrainUnavailable),
    ),
)]
pub async fn post_drain(
    State(state): State<AppState>,
    meta: Option<Extension<Arc<agentdb_meta::MetaStore>>>,
) -> Result<Json<DrainStatus>, (StatusCode, Json<DrainUnavailable>)> {
    let Some(node_id) = state.registry.node_id().map(|s| s.to_string()) else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(DrainUnavailable {
                message: "this process has no control-plane node id — drain requires a meta-backed registry".into(),
            }),
        ));
    };
    let now = now_ms();
    state.registry.drain(now);
    // Best-effort update of the nodes table so /api/admin/nodes
    // surfaces the draining state to operators. If meta isn't
    // layered as an axum Extension (tests that skip it) we just
    // skip the update — the drain still took effect locally.
    if let Some(Extension(meta)) = meta {
        if let Err(err) =
            agentdb_meta::nodes::set_state(meta.pool(), &node_id, "draining", now).await
        {
            tracing::warn!(
                %node_id,
                %err,
                "drain: failed to record draining state in meta nodes table"
            );
        }
    }
    Ok(Json(DrainStatus {
        node_id: Some(node_id),
        is_draining: true,
        drained_at_ms: state.registry.drained_at_ms(),
    }))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_core::{BoxRegistry, HlcClock, SessionRegistry, TenantRegistry};
    use agentdb_meta::MetaStore;
    use agentdb_storage::{KvStore, RocksKvStore};
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use tempfile::TempDir;

    async fn state_with_meta_backed_registry() -> (TempDir, TempDir, AppState) {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let meta = Arc::new(MetaStore::open_sqlite(":memory:").await.unwrap());
        let reg = Arc::new(
            BoxRegistry::new_with_meta(
                kv.clone(),
                store,
                1,
                Arc::new(HlcClock::default()),
                meta,
                "drain-test-node".into(),
            )
            .await
            .unwrap(),
        );
        (
            kv_dir,
            obj_dir,
            AppState {
                registry: reg,
                tenants: Arc::new(TenantRegistry::new(kv.clone())),
                sessions: Arc::new(SessionRegistry::new(kv)),
                stream: crate::stream::StreamBus::default(),
                event_bus: crate::event_bus::EventBus::default(),
                http: crate::webhook::new_client(),
                auth: None,
        admin_auth: None,
            },
        )
    }

    async fn state_with_legacy_registry() -> (TempDir, TempDir, AppState) {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let reg = Arc::new(BoxRegistry::new(kv.clone(), store, 1).unwrap());
        (
            kv_dir,
            obj_dir,
            AppState {
                registry: reg,
                tenants: Arc::new(TenantRegistry::new(kv.clone())),
                sessions: Arc::new(SessionRegistry::new(kv)),
                stream: crate::stream::StreamBus::default(),
                event_bus: crate::event_bus::EventBus::default(),
                http: crate::webhook::new_client(),
                auth: None,
        admin_auth: None,
            },
        )
    }

    #[tokio::test]
    async fn get_drain_on_fresh_process_reports_not_draining() {
        let (_kv, _obj, state) = state_with_meta_backed_registry().await;
        let Json(st) = get_drain(State(state)).await;
        assert_eq!(st.node_id.as_deref(), Some("drain-test-node"));
        assert!(!st.is_draining);
        assert!(st.drained_at_ms.is_none());
    }

    #[tokio::test]
    async fn post_drain_flips_state_and_is_idempotent() {
        let (_kv, _obj, state) = state_with_meta_backed_registry().await;
        let Json(first) = post_drain(State(state.clone()), None)
            .await
            .expect("first drain ok");
        assert!(first.is_draining);
        let first_ts = first.drained_at_ms.expect("timestamp captured");

        // Second POST — idempotent; drained_at_ms must match.
        let Json(second) = post_drain(State(state.clone()), None)
            .await
            .expect("second drain ok");
        assert!(second.is_draining);
        assert_eq!(second.drained_at_ms, Some(first_ts));

        // GET reflects the state too.
        let Json(via_get) = get_drain(State(state)).await;
        assert!(via_get.is_draining);
        assert_eq!(via_get.drained_at_ms, Some(first_ts));
    }

    #[tokio::test]
    async fn post_drain_refuses_on_legacy_registry_without_node_id() {
        let (_kv, _obj, state) = state_with_legacy_registry().await;
        let (code, body) = post_drain(State(state), None).await.expect_err("503");
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.0.message.contains("node id"));
    }
}
