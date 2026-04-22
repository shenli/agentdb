//! `GET /api/admin/nodes` — operator-visible list of cluster
//! membership. Reads from the `nodes` table (M18) via an `Extension`
//! carrying the MetaStore. When the MetaStore extension is absent
//! (tests using `router()` without layering one in), the handler
//! returns 503 — the endpoint is always registered, so the router
//! shape doesn't depend on whether a control plane is configured.

use std::sync::Arc;

use agentdb_meta::{nodes, MetaStore};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::Serialize;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct NodeView {
    pub node_id: String,
    pub address: String,
    pub state: String,
    pub last_heartbeat_ms: i64,
    pub joined_at_ms: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct NodesUnavailable {
    pub message: String,
}

#[utoipa::path(
    get,
    path = "/api/admin/nodes",
    tag = "admin",
    responses(
        (status = 200, description = "Every registered node, ordered by node_id", body = Vec<NodeView>),
        (status = 503, description = "Control plane not configured on this process", body = NodesUnavailable),
    ),
)]
pub async fn list_nodes(
    meta: Option<Extension<Arc<MetaStore>>>,
) -> Result<Json<Vec<NodeView>>, (StatusCode, Json<NodesUnavailable>)> {
    let Some(Extension(meta)) = meta else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(NodesUnavailable {
                message: "meta store is not configured on this server".into(),
            }),
        ));
    };
    match nodes::list_all(meta.pool()).await {
        Ok(rows) => Ok(Json(
            rows.into_iter()
                .map(|n| NodeView {
                    node_id: n.node_id,
                    address: n.address,
                    state: n.state,
                    last_heartbeat_ms: n.last_heartbeat_ms,
                    joined_at_ms: n.joined_at_ms,
                })
                .collect(),
        )),
        Err(e) => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(NodesUnavailable {
                message: format!("meta query failed: {e}"),
            }),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn meta_with(rows: &[(&str, &str, &str, i64)]) -> Arc<MetaStore> {
        let meta = Arc::new(MetaStore::open_sqlite(":memory:").await.unwrap());
        for (id, addr, state, ts) in rows {
            nodes::upsert(meta.pool(), id, addr, state, *ts)
                .await
                .unwrap();
        }
        meta
    }

    #[tokio::test]
    async fn returns_503_when_meta_extension_is_absent() {
        let (code, body) = list_nodes(None).await.expect_err("503");
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.0.message.contains("not configured"));
    }

    #[tokio::test]
    async fn returns_registered_rows_in_id_order() {
        let meta = meta_with(&[
            ("node-b", "b-addr", "alive", 200),
            ("node-a", "a-addr", "alive", 100),
        ])
        .await;
        let Json(rows) = list_nodes(Some(Extension(meta))).await.expect("ok");
        let ids: Vec<_> = rows.iter().map(|r| r.node_id.as_str()).collect();
        assert_eq!(ids, ["node-a", "node-b"]);
        assert_eq!(rows[0].address, "a-addr");
        assert_eq!(rows[0].state, "alive");
        assert_eq!(rows[0].last_heartbeat_ms, 100);
    }
}
