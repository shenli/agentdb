//! Process health endpoints.
//!
//! - `GET /health/live`: the process is up. Always 200. Target of the
//!   Kubernetes livenessProbe; a failure here should trigger restart,
//!   not route-away.
//! - `GET /health/ready`: this process should receive traffic.
//!   Returns 200 when every box with a configured lease reports
//!   `is_live()` and the process is not draining. Otherwise 503 with
//!   a structured body listing the specific reasons (drain, expired
//!   leases) so alert/log pipelines can diagnose without parsing a
//!   status string. Target of the Kubernetes readinessProbe — a
//!   failure here should route traffic around this node without
//!   restarting it.

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;
use utoipa::ToSchema;

use crate::AppState;

#[derive(Debug, Serialize, ToSchema)]
pub struct LiveResponse {
    pub status: &'static str,
}

/// Stable machine-readable reason codes for `/health/ready` 503s.
/// Future reasons (e.g. meta-store unreachable, scheduler leader
/// election loop stuck) land here without breaking existing clients.
#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReadyReasonCode {
    /// This process's drain flag is set (see `POST /api/admin/drain`).
    NodeDraining,
    /// One or more boxes have a configured lease that is no longer
    /// live. The `unhealthy_boxes` field lists them.
    LeaseNotLive,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ReadyReason {
    pub code: ReadyReasonCode,
    /// Human-readable description. Stable per code; operators can
    /// string-match but the `code` is the authoritative signal.
    pub message: String,
    /// Runbook URL — same shape as REST-error `doc_url`. Empty when
    /// the code has no published runbook yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc_url: Option<&'static str>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ReadyResponse {
    /// `"ok"` on 200, `"unready"` on 503. Kept for back-compat with
    /// scripts that eyeball a single field; new code should read
    /// `reasons`.
    pub status: &'static str,
    /// Each distinct reason this process is not ready. Empty on 200.
    #[serde(default)]
    pub reasons: Vec<ReadyReason>,
    /// Box ids whose lease is configured but not live. Empty unless
    /// one of `reasons` is `lease_not_live`.
    pub unhealthy_boxes: Vec<u16>,
    /// Box ids with no configured lease (legacy / dev mode).
    /// Diagnostic only.
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

    let mut reasons: Vec<ReadyReason> = Vec::new();
    if state.registry.is_draining() {
        reasons.push(ReadyReason {
            code: ReadyReasonCode::NodeDraining,
            message: "this node has been drained via POST /api/admin/drain".into(),
            doc_url: Some(
                "https://github.com/shenli/agentdb/blob/main/docs/runbooks/node_draining.md",
            ),
        });
    }
    if !unhealthy.is_empty() {
        reasons.push(ReadyReason {
            code: ReadyReasonCode::LeaseNotLive,
            message: format!(
                "boxes with expired leases: {}",
                unhealthy
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            doc_url: Some(
                "https://github.com/shenli/agentdb/blob/main/docs/runbooks/lease_lost.md",
            ),
        });
    }

    if reasons.is_empty() {
        Ok(Json(ReadyResponse {
            status: "ok",
            reasons,
            unhealthy_boxes: unhealthy,
            leaseless_boxes: leaseless,
        }))
    } else {
        Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ReadyResponse {
                status: "unready",
                reasons,
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
                auth: None,
            },
        )
    }

    async fn state_with_meta_registry() -> (TempDir, TempDir, AppState) {
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
                "node-health-test".into(),
            )
            .await
            .unwrap(),
        );
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        (
            kv_dir,
            obj_dir,
            AppState {
                registry: reg,
                tenants,
                sessions,
                stream: crate::stream::StreamBus::default(),
                http: crate::webhook::new_client(),
                auth: None,
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
        let (_kv, _obj, state) = state_with_legacy_registry();
        let res = ready(State(state)).await.expect("ok");
        assert_eq!(res.status, "ok");
        assert!(res.reasons.is_empty());
        assert!(res.unhealthy_boxes.is_empty());
        assert_eq!(res.leaseless_boxes, vec![0u16, 1u16]);
    }

    #[tokio::test]
    async fn ready_503_when_lease_expired_carries_structured_reason() {
        let (_kv, _obj, state) = state_with_meta_registry().await;
        state
            .registry
            .box_by_id(0)
            .unwrap()
            .lease()
            .unwrap()
            .expire_for_test();
        let (code, body) = ready(State(state)).await.expect_err("503");
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.0.status, "unready");
        assert_eq!(body.0.unhealthy_boxes, vec![0u16]);
        // Exactly one reason — lease_not_live.
        assert_eq!(body.0.reasons.len(), 1);
        assert!(matches!(
            body.0.reasons[0].code,
            ReadyReasonCode::LeaseNotLive
        ));
        assert!(body.0.reasons[0].doc_url.unwrap().contains("lease_lost.md"));
    }

    #[tokio::test]
    async fn ready_503_when_draining_carries_structured_reason() {
        let (_kv, _obj, state) = state_with_meta_registry().await;
        state.registry.drain(1);
        let (code, body) = ready(State(state)).await.expect_err("503");
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.0.status, "unready");
        assert_eq!(body.0.reasons.len(), 1);
        assert!(matches!(
            body.0.reasons[0].code,
            ReadyReasonCode::NodeDraining
        ));
        assert!(body.0.reasons[0]
            .doc_url
            .unwrap()
            .contains("node_draining.md"));
    }

    #[tokio::test]
    async fn ready_503_can_accumulate_multiple_reasons() {
        let (_kv, _obj, state) = state_with_meta_registry().await;
        state.registry.drain(1);
        state
            .registry
            .box_by_id(0)
            .unwrap()
            .lease()
            .unwrap()
            .expire_for_test();
        let (_, body) = ready(State(state)).await.expect_err("503");
        let codes: Vec<_> = body.0.reasons.iter().map(|r| r.code).collect();
        assert_eq!(codes.len(), 2);
        assert!(codes
            .iter()
            .any(|c| matches!(c, ReadyReasonCode::NodeDraining)));
        assert!(codes
            .iter()
            .any(|c| matches!(c, ReadyReasonCode::LeaseNotLive)));
    }
}
