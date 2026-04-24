//! `GET /api/admin/nodes` — operator-visible list of cluster
//! membership. Reads from the `nodes` table (M18) via an `Extension`
//! carrying the MetaStore. When the MetaStore extension is absent
//! (tests using `router()` without layering one in), the handler
//! returns 503 — the endpoint is always registered, so the router
//! shape doesn't depend on whether a control plane is configured.
//!
//! Optional `?stale_threshold_ms=N` query parameter. Each returned
//! row carries `is_stale`, set when
//! `now_ms - last_heartbeat_ms > stale_threshold_ms`. Default
//! threshold is 15000 ms — three times the default heartbeat period.
//!
//! `DELETE /api/admin/nodes/:node_id` is the operator-recovery
//! path: manually evict a node row. Refuses to remove rows that
//! are still `alive` (operator is expected to drain the node
//! first, or wait for the node reaper to flip it to `dead`).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_meta::{nodes, MetaStore};
use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

const DEFAULT_STALE_THRESHOLD_MS: i64 = 15_000;

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct NodeView {
    pub node_id: String,
    pub address: String,
    /// Mongo wire listener address advertised for cross-node
    /// forwarding. `None` when the node predates migration
    /// 008 or didn't set `AGENTDB_NODE_MONGO_ADDRESS`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mongo_address: Option<String>,
    pub state: String,
    pub last_heartbeat_ms: i64,
    pub joined_at_ms: i64,
    /// True when this node's last heartbeat is older than the
    /// configured `stale_threshold_ms` (default 15000). Diagnostic —
    /// the scheduler that M22 will add does the actual failover
    /// decision; this flag is just for operator visibility.
    pub is_stale: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct NodesUnavailable {
    pub message: String,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ListNodesQuery {
    /// Nodes whose last heartbeat is older than this many milliseconds
    /// are flagged `is_stale = true`. Default 15000.
    pub stale_threshold_ms: Option<i64>,
}

#[utoipa::path(
    get,
    path = "/api/admin/nodes",
    tag = "admin",
    params(ListNodesQuery),
    responses(
        (status = 200, description = "Every registered node, ordered by node_id", body = Vec<NodeView>),
        (status = 503, description = "Control plane not configured on this process", body = NodesUnavailable),
    ),
)]
pub async fn list_nodes(
    meta: Option<Extension<Arc<MetaStore>>>,
    Query(query): Query<ListNodesQuery>,
) -> Result<Json<Vec<NodeView>>, (StatusCode, Json<NodesUnavailable>)> {
    let Some(Extension(meta)) = meta else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(NodesUnavailable {
                message: "meta store is not configured on this server".into(),
            }),
        ));
    };
    let threshold = query
        .stale_threshold_ms
        .unwrap_or(DEFAULT_STALE_THRESHOLD_MS);
    let now = now_ms();
    match nodes::list_all(meta.pool()).await {
        Ok(rows) => Ok(Json(
            rows.into_iter()
                .map(|n| {
                    let is_stale = now - n.last_heartbeat_ms > threshold;
                    NodeView {
                        node_id: n.node_id,
                        address: n.address,
                        mongo_address: n.mongo_address,
                        state: n.state,
                        last_heartbeat_ms: n.last_heartbeat_ms,
                        joined_at_ms: n.joined_at_ms,
                        is_stale,
                    }
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

/// Body shape for `DELETE /api/admin/nodes/:node_id` responses.
/// `removed` is `true` when a row matching the path param existed
/// before the call (so the operator sees the difference between
/// "id not in table" and "id was already removed in a previous
/// call").
#[derive(Debug, Serialize, ToSchema)]
pub struct DeleteNodeResponse {
    pub node_id: String,
    pub removed: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DeleteNodeError {
    pub message: String,
}

#[utoipa::path(
    delete,
    path = "/api/admin/nodes/{node_id}",
    tag = "admin",
    params(("node_id" = String, Path, description = "Node id to evict")),
    responses(
        (status = 200, description = "Row removed (or already absent)", body = DeleteNodeResponse),
        (status = 409, description = "Node is still state='alive'; drain it first", body = DeleteNodeError),
        (status = 503, description = "Control plane not configured on this process", body = DeleteNodeError),
    ),
)]
pub async fn delete_node(
    meta: Option<Extension<Arc<MetaStore>>>,
    Path(node_id): Path<String>,
) -> Result<Json<DeleteNodeResponse>, (StatusCode, Json<DeleteNodeError>)> {
    let Some(Extension(meta)) = meta else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(DeleteNodeError {
                message: "meta store is not configured on this server".into(),
            }),
        ));
    };
    // Refuse to remove an `alive` row. If the operator wants to
    // take a live node out of the cluster they should drain it
    // first (POST /api/admin/drain); that transitions it to
    // "draining" and stops traffic, and the node reaper will
    // eventually flip it to "dead" when heartbeats stop. This
    // also avoids a footgun where deleting the currently-running
    // leader's row deletes the row its own heartbeat would just
    // re-upsert, causing write churn.
    match nodes::get(meta.pool(), &node_id).await {
        Ok(Some(n)) if n.state == "alive" => {
            return Err((
                StatusCode::CONFLICT,
                Json(DeleteNodeError {
                    message: format!(
                        "node '{node_id}' is still state='alive'; drain it before removing"
                    ),
                }),
            ));
        }
        Ok(_) => {}
        Err(e) => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(DeleteNodeError {
                    message: format!("meta query failed: {e}"),
                }),
            ));
        }
    }
    match nodes::remove(meta.pool(), &node_id).await {
        Ok(removed) => {
            tracing::info!(%node_id, removed, "admin evicted node via DELETE /api/admin/nodes");
            Ok(Json(DeleteNodeResponse { node_id, removed }))
        }
        Err(e) => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(DeleteNodeError {
                message: format!("meta delete failed: {e}"),
            }),
        )),
    }
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

    async fn meta_with(rows: &[(&str, &str, &str, i64)]) -> Arc<MetaStore> {
        let meta = Arc::new(MetaStore::open_sqlite(":memory:").await.unwrap());
        for (id, addr, state, ts) in rows {
            nodes::upsert(meta.pool(), id, addr, None, state, *ts)
                .await
                .unwrap();
        }
        meta
    }

    fn no_filter() -> Query<ListNodesQuery> {
        Query(ListNodesQuery {
            stale_threshold_ms: None,
        })
    }

    #[tokio::test]
    async fn returns_503_when_meta_extension_is_absent() {
        let (code, body) = list_nodes(None, no_filter()).await.expect_err("503");
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.0.message.contains("not configured"));
    }

    #[tokio::test]
    async fn returns_registered_rows_in_id_order_fresh_heartbeats_not_stale() {
        // Heartbeats at "now" — within threshold, must not be stale.
        let now = now_ms();
        let meta = meta_with(&[
            ("node-b", "b-addr", "alive", now),
            ("node-a", "a-addr", "alive", now),
        ])
        .await;
        let Json(rows) = list_nodes(Some(Extension(meta)), no_filter())
            .await
            .expect("ok");
        let ids: Vec<_> = rows.iter().map(|r| r.node_id.as_str()).collect();
        assert_eq!(ids, ["node-a", "node-b"]);
        assert!(!rows[0].is_stale);
        assert!(!rows[1].is_stale);
    }

    #[tokio::test]
    async fn flags_rows_with_last_heartbeat_older_than_threshold() {
        // One row heartbeated "now", the other 60s ago. With a 5s
        // threshold the old row is stale, the fresh one isn't.
        let now = now_ms();
        let meta = meta_with(&[
            ("node-a", "addr", "alive", now),
            ("node-b", "addr", "alive", now - 60_000),
        ])
        .await;
        let Json(rows) = list_nodes(
            Some(Extension(meta)),
            Query(ListNodesQuery {
                stale_threshold_ms: Some(5_000),
            }),
        )
        .await
        .expect("ok");
        assert!(!rows[0].is_stale, "node-a heartbeated now — fresh");
        assert!(rows[1].is_stale, "node-b 60s old, threshold 5s — stale");
    }

    #[tokio::test]
    async fn delete_returns_503_when_meta_extension_is_absent() {
        let (code, body) = delete_node(None, Path("any-node".into()))
            .await
            .expect_err("503");
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.0.message.contains("not configured"));
    }

    #[tokio::test]
    async fn delete_rejects_alive_node_with_409() {
        let now = now_ms();
        let meta = meta_with(&[("alive-node", "a", "alive", now)]).await;
        let (code, body) = delete_node(Some(Extension(meta)), Path("alive-node".into()))
            .await
            .expect_err("409");
        assert_eq!(code, StatusCode::CONFLICT);
        assert!(body.0.message.to_lowercase().contains("drain"));
    }

    #[tokio::test]
    async fn delete_removes_dead_node_and_reports_true() {
        let now = now_ms();
        let meta = Arc::new(MetaStore::open_sqlite(":memory:").await.unwrap());
        nodes::upsert(meta.pool(), "gone", "a", None, "dead", now)
            .await
            .unwrap();
        let Json(resp) = delete_node(Some(Extension(meta.clone())), Path("gone".into()))
            .await
            .expect("ok");
        assert_eq!(resp.node_id, "gone");
        assert!(resp.removed);
        assert!(nodes::get(meta.pool(), "gone").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_reports_false_when_id_unknown() {
        let meta = Arc::new(MetaStore::open_sqlite(":memory:").await.unwrap());
        let Json(resp) = delete_node(Some(Extension(meta)), Path("ghost".into()))
            .await
            .expect("ok");
        // The path is idempotent: removing an id that was never
        // in the table is success with removed=false, not an
        // error. Matches the spirit of DELETE.
        assert!(!resp.removed);
    }

    #[tokio::test]
    async fn delete_allows_draining_and_leaving_and_dead() {
        // The conflict check only fires on state='alive'. Every
        // other state is fair game for manual eviction.
        let now = now_ms();
        for state in ["draining", "leaving", "dead"] {
            let meta = Arc::new(MetaStore::open_sqlite(":memory:").await.unwrap());
            nodes::upsert(meta.pool(), "n", "addr", None, state, now)
                .await
                .unwrap();
            let Json(resp) = delete_node(Some(Extension(meta)), Path("n".into()))
                .await
                .unwrap_or_else(|_| panic!("delete should succeed for state={state}"));
            assert!(resp.removed, "state={state} → should report removed=true");
        }
    }

    #[tokio::test]
    async fn default_threshold_is_used_when_query_is_absent() {
        // Heartbeat 30s ago — stale under the 15s default, fresh
        // under a 60s override.
        let now = now_ms();
        let meta = meta_with(&[("node-a", "addr", "alive", now - 30_000)]).await;
        let Json(with_default) = list_nodes(Some(Extension(meta.clone())), no_filter())
            .await
            .expect("ok");
        assert!(with_default[0].is_stale);

        let Json(with_override) = list_nodes(
            Some(Extension(meta)),
            Query(ListNodesQuery {
                stale_threshold_ms: Some(60_000),
            }),
        )
        .await
        .expect("ok");
        assert!(!with_override[0].is_stale);
    }
}
