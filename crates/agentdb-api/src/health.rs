//! Process health endpoints. Minimal versions — see the M27 plan for
//! the full operational surface.
//!
//! - `GET /health/live`: the process is up. Always 200. Target of the
//!   Kubernetes livenessProbe; a failure here should trigger restart,
//!   not route-away.
//! - `GET /health/ready`: this process should receive traffic. Returns
//!   200 only if every box with a configured lease reports
//!   `is_live()`. Boxes with `lease() == None` (pre-M21 or dev mode
//!   without a control plane) are treated as always ready. Target of
//!   the Kubernetes readinessProbe; a failure here should cause peers
//!   to route around this node without restarting it.

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;

use crate::AppState;

#[derive(Debug, Serialize)]
pub struct LiveResponse {
    pub status: &'static str,
}

#[derive(Debug, Serialize)]
pub struct ReadyResponse {
    pub status: &'static str,
    /// Box ids whose lease is configured but not live. Empty on 200.
    pub unhealthy_boxes: Vec<u16>,
    /// Box ids with no configured lease (legacy / dev). Diagnostic only.
    pub leaseless_boxes: Vec<u16>,
}

pub async fn live() -> Json<LiveResponse> {
    Json(LiveResponse { status: "ok" })
}

pub async fn ready(
    State(state): State<AppState>,
) -> Result<Json<ReadyResponse>, (StatusCode, Json<ReadyResponse>)> {
    let mut unhealthy = Vec::new();
    let mut leaseless = Vec::new();
    for i in 0..state.registry.num_boxes() {
        let Some(b) = state.registry.box_by_id(i) else {
            continue;
        };
        match b.lease() {
            Some(lease) if !lease.is_live() => unhealthy.push(i),
            Some(_) => {}
            None => leaseless.push(i),
        }
    }
    if unhealthy.is_empty() {
        Ok(Json(ReadyResponse {
            status: "ok",
            unhealthy_boxes: unhealthy,
            leaseless_boxes: leaseless,
        }))
    } else {
        Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ReadyResponse {
                status: "lease_not_live",
                unhealthy_boxes: unhealthy,
                leaseless_boxes: leaseless,
            }),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_core::{BoxRegistry, HlcClock, SessionRegistry, TenantRegistry};
    use agentdb_storage::{KvStore, RocksKvStore};
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn state_with_legacy_registry() -> (TempDir, TempDir, AppState) {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let reg = Arc::new(BoxRegistry::new(kv.clone(), store, 2).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        let stream = crate::stream::StreamBus::default();
        let http = crate::webhook::new_client();
        (
            kv_dir,
            obj_dir,
            AppState {
                registry: reg,
                tenants,
                sessions,
                stream,
                http,
            },
        )
    }

    #[tokio::test]
    async fn live_always_reports_ok() {
        let Json(body) = live().await;
        assert_eq!(body.status, "ok");
    }

    #[tokio::test]
    async fn ready_returns_ok_when_no_leases_configured() {
        // Legacy registry — no boxes have a lease. We report ok with
        // every box listed under `leaseless_boxes` for diagnostics.
        let (_kv_dir, _obj_dir, state) = state_with_legacy_registry();
        let res = ready(State(state)).await.expect("ok");
        assert_eq!(res.status, "ok");
        assert!(res.unhealthy_boxes.is_empty());
        assert_eq!(res.leaseless_boxes, vec![0u16, 1u16]);
    }

    #[tokio::test]
    async fn ready_returns_503_when_a_configured_lease_is_not_live() {
        use agentdb_meta::MetaStore;
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
                2,
                Arc::new(HlcClock::default()),
                meta,
                "node-a".into(),
            )
            .await
            .unwrap(),
        );
        // Force box 0's lease to look expired.
        let b = reg.box_by_id(0).unwrap();
        b.lease().unwrap().expire_for_test();

        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        let state = AppState {
            registry: reg,
            tenants,
            sessions,
            stream: crate::stream::StreamBus::default(),
            http: crate::webhook::new_client(),
        };
        let (code, body) = ready(State(state)).await.expect_err("503");
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.0.unhealthy_boxes, vec![0u16]);
    }
}
