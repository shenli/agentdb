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

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_meta::{nodes, MetaStore};
use axum::extract::Query;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

const DEFAULT_STALE_THRESHOLD_MS: i64 = 15_000;

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct NodeView {
    pub node_id: String,
    pub address: String,
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
            nodes::upsert(meta.pool(), id, addr, state, *ts)
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
