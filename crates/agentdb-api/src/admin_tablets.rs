//! `GET /api/admin/tablets` — every tablet row from the control plane,
//! with its current owner node id and epoch.
//!
//! Uses the same `Option<Extension<Arc<MetaStore>>>` pattern as
//! `admin_nodes` so the route stays registered whether or not the
//! process is wired to a control plane — routers built without the
//! meta extension (e.g. most tests) get a graceful 503.

use std::sync::Arc;

use agentdb_meta::{tablets, MetaStore};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::Serialize;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TabletView {
    pub tablet_id: i64,
    /// Node id currently holding the lease, if any. `None` means the
    /// tablet row exists but has never been claimed.
    pub owner_node_id: Option<String>,
    /// Monotonic counter that bumps on every successful ownership
    /// change; used as the fencing token on storage-path writes
    /// (follow-up PR wires that).
    pub epoch: i64,
    /// Wall-clock ms at which the current lease expires. `0` when the
    /// tablet has never been claimed.
    pub lease_expires_at_ms: i64,
    pub created_at_ms: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TabletsUnavailable {
    pub message: String,
}

#[utoipa::path(
    get,
    path = "/api/admin/tablets",
    tag = "admin",
    responses(
        (status = 200, description = "Every tablet row, ordered by tablet_id", body = Vec<TabletView>),
        (status = 503, description = "Control plane not configured on this process", body = TabletsUnavailable),
    ),
)]
pub async fn list_tablets(
    meta: Option<Extension<Arc<MetaStore>>>,
) -> Result<Json<Vec<TabletView>>, (StatusCode, Json<TabletsUnavailable>)> {
    let Some(Extension(meta)) = meta else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(TabletsUnavailable {
                message: "meta store is not configured on this server".into(),
            }),
        ));
    };
    match tablets::list_all(meta.pool()).await {
        Ok(rows) => Ok(Json(
            rows.into_iter()
                .map(|t| TabletView {
                    tablet_id: t.tablet_id,
                    owner_node_id: t.owner_node_id,
                    epoch: t.epoch,
                    lease_expires_at_ms: t.lease_expires_at_ms,
                    created_at_ms: t.created_at_ms,
                })
                .collect(),
        )),
        Err(e) => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(TabletsUnavailable {
                message: format!("meta query failed: {e}"),
            }),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn meta_with_tablets(owned: &[(i64, Option<&str>)]) -> Arc<MetaStore> {
        let meta = Arc::new(MetaStore::open_sqlite(":memory:").await.unwrap());
        for (id, owner) in owned {
            tablets::get_or_create(meta.pool(), *id, 100).await.unwrap();
            if let Some(node_id) = owner {
                tablets::assume_ownership(meta.pool(), *id, 0, node_id, 1000)
                    .await
                    .unwrap();
            }
        }
        meta
    }

    #[tokio::test]
    async fn returns_503_when_meta_extension_is_absent() {
        let (code, body) = list_tablets(None).await.expect_err("503");
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.0.message.contains("not configured"));
    }

    #[tokio::test]
    async fn returns_tablets_in_id_order_with_owner_info() {
        let meta = meta_with_tablets(&[(2, Some("node-a")), (1, None), (3, Some("node-b"))]).await;
        let Json(rows) = list_tablets(Some(Extension(meta))).await.expect("ok");
        let ids: Vec<_> = rows.iter().map(|r| r.tablet_id).collect();
        assert_eq!(ids, vec![1i64, 2, 3]);
        assert_eq!(rows[0].owner_node_id, None);
        assert_eq!(rows[0].epoch, 0);
        assert_eq!(rows[1].owner_node_id.as_deref(), Some("node-a"));
        assert_eq!(rows[1].epoch, 1);
        assert_eq!(rows[1].lease_expires_at_ms, 1000);
        assert_eq!(rows[2].owner_node_id.as_deref(), Some("node-b"));
    }
}
