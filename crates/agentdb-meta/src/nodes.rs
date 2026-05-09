//! Cluster node membership + heartbeat. Follow-up PRs add LISTEN/NOTIFY
//! for change propagation (Postgres only).

use sqlx::AnyPool;

use crate::error::{MetaError, MetaResult};

/// (node_id, address, mongo_address, state, last_heartbeat_ms,
/// joined_at_ms, availability_zone, region). Aliased so the
/// `query_as` annotations don't have to spell the tuple at
/// every call site.
type NodeRow = (
    String,
    String,
    Option<String>,
    String,
    i64,
    i64,
    Option<String>,
    Option<String>,
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub node_id: String,
    pub address: String,
    /// Legacy peer listener address retained for migration compatibility.
    /// New V2 deployments do not use this field.
    pub mongo_address: Option<String>,
    pub state: String,
    pub last_heartbeat_ms: i64,
    pub joined_at_ms: i64,
    /// Availability zone declared at boot via
    /// `AGENTDB_NODE_AZ`. `None` for single-AZ deployments
    /// and pre-migration-013 nodes — placement falls back
    /// to load-only balancing in either case.
    pub availability_zone: Option<String>,
    /// Durable region metadata used by residency-aware routing.
    /// Pre-migration rows may leave this unset; routing falls back
    /// to `availability_zone` so older single-label deployments keep
    /// their previous behavior.
    pub region: Option<String>,
}

#[allow(clippy::too_many_arguments)]
pub async fn upsert(
    pool: &AnyPool,
    node_id: &str,
    address: &str,
    mongo_address: Option<&str>,
    state: &str,
    now_ms: i64,
    availability_zone: Option<&str>,
    region: Option<&str>,
) -> MetaResult<()> {
    // SQLite's ON CONFLICT upsert syntax.
    sqlx::query(
        "INSERT INTO nodes (node_id, address, mongo_address, state, last_heartbeat_ms, joined_at_ms, availability_zone, region)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (node_id) DO UPDATE SET
              address = excluded.address,
              mongo_address = excluded.mongo_address,
              state = excluded.state,
              last_heartbeat_ms = excluded.last_heartbeat_ms,
              availability_zone = excluded.availability_zone,
              region = excluded.region",
    )
    .bind(node_id)
    .bind(address)
    .bind(mongo_address)
    .bind(state)
    .bind(now_ms)
    .bind(now_ms)
    .bind(availability_zone)
    .bind(region)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn heartbeat(pool: &AnyPool, node_id: &str, now_ms: i64) -> MetaResult<()> {
    let rows = sqlx::query("UPDATE nodes SET last_heartbeat_ms = $1 WHERE node_id = $2")
        .bind(now_ms)
        .bind(node_id)
        .execute(pool)
        .await?
        .rows_affected();
    if rows == 0 {
        return Err(MetaError::NotFound(format!("node {node_id}")));
    }
    Ok(())
}

/// Update just the `state` column (and bump `last_heartbeat_ms` so
/// observers don't simultaneously see "draining" and "stale").
/// Unlike [`upsert`], this preserves the existing `address` —
/// `POST /api/admin/drain` uses it to flip state to `"draining"`
/// without accidentally wiping the advertised address.
pub async fn set_state(pool: &AnyPool, node_id: &str, state: &str, now_ms: i64) -> MetaResult<()> {
    let rows =
        sqlx::query("UPDATE nodes SET state = $1, last_heartbeat_ms = $2 WHERE node_id = $3")
            .bind(state)
            .bind(now_ms)
            .bind(node_id)
            .execute(pool)
            .await?
            .rows_affected();
    if rows == 0 {
        return Err(MetaError::NotFound(format!("node {node_id}")));
    }
    Ok(())
}

pub async fn get(pool: &AnyPool, node_id: &str) -> MetaResult<Option<Node>> {
    let row: Option<NodeRow> = sqlx::query_as(
        "SELECT node_id, address, mongo_address, state, last_heartbeat_ms, joined_at_ms, availability_zone, region \
           FROM nodes WHERE node_id = $1",
    )
    .bind(node_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(into_node))
}

/// Every registered node, ordered by `node_id`. Point-in-time
/// snapshot — a node can be updated between this return and the
/// caller acting on it. Used by `GET /api/admin/nodes` and by the
/// scheduler's membership scan.
pub async fn list_all(pool: &AnyPool) -> MetaResult<Vec<Node>> {
    let rows: Vec<NodeRow> = sqlx::query_as(
        "SELECT node_id, address, mongo_address, state, last_heartbeat_ms, joined_at_ms, availability_zone, region
           FROM nodes ORDER BY node_id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_node).collect())
}

fn into_node(row: NodeRow) -> Node {
    let (n, a, m, s, h, j, az, region) = row;
    Node {
        node_id: n,
        address: a,
        mongo_address: m,
        state: s,
        last_heartbeat_ms: h,
        joined_at_ms: j,
        availability_zone: az,
        region,
    }
}

/// Flip `state` from `alive` to `dead` for every node whose last
/// heartbeat is older than `deadline_ms`. Returns the list of
/// node ids that were transitioned this call (empty when nothing
/// was stale). Idempotent: a row already at `dead`/`leaving`/`gc`
/// is left alone.
///
/// Used by the leader-owned failure detector in agentdb-core to
/// surface membership changes. `last_heartbeat_ms` is NOT
/// touched — we want the original timestamp preserved so GC and
/// observability can see how long the node has been gone.
pub async fn mark_dead_if_stale(pool: &AnyPool, deadline_ms: i64) -> MetaResult<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "UPDATE nodes SET state = 'dead' \
           WHERE state = 'alive' AND last_heartbeat_ms < $1 \
         RETURNING node_id",
    )
    .bind(deadline_ms)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(n,)| n).collect())
}

/// Delete every row whose `state = 'dead'` and whose last
/// heartbeat is older than `gc_deadline_ms`. Returns the removed
/// node ids. Idempotent across ticks — a row is gone on the
/// second call, so the RETURNING set is empty.
///
/// The GC threshold should be much larger than the dead-detection
/// threshold so operators have time to observe, debug, or recover
/// a node before it disappears from the control-plane.
pub async fn gc_dead(pool: &AnyPool, gc_deadline_ms: i64) -> MetaResult<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "DELETE FROM nodes \
           WHERE state = 'dead' AND last_heartbeat_ms < $1 \
         RETURNING node_id",
    )
    .bind(gc_deadline_ms)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(n,)| n).collect())
}

/// Drop a specific node row. Returns `true` when a row was
/// removed, `false` when the id wasn't present. Used by the
/// `DELETE /api/admin/nodes/:id` operator-recovery path.
pub async fn remove(pool: &AnyPool, node_id: &str) -> MetaResult<bool> {
    let rows = sqlx::query("DELETE FROM nodes WHERE node_id = $1")
        .bind(node_id)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(rows > 0)
}

/// Nodes whose last heartbeat is older than `threshold_ms`. Used by
/// the scheduler to detect dead nodes and start failover.
pub async fn stale(pool: &AnyPool, threshold_ms: i64) -> MetaResult<Vec<Node>> {
    let rows: Vec<NodeRow> = sqlx::query_as(
        "SELECT node_id, address, mongo_address, state, last_heartbeat_ms, joined_at_ms, availability_zone, region
           FROM nodes WHERE last_heartbeat_ms < $1 ORDER BY last_heartbeat_ms",
    )
    .bind(threshold_ms)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_node).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> AnyPool {
        crate::test_pool_sqlite_memory().await
    }

    #[tokio::test]
    async fn upsert_and_get() {
        let p = pool().await;
        upsert(
            &p,
            "node-a",
            "127.0.0.1:9000",
            None,
            "alive",
            100,
            None,
            None,
        )
        .await
        .unwrap();
        let n = get(&p, "node-a").await.unwrap().unwrap();
        assert_eq!(n.node_id, "node-a");
        assert_eq!(n.address, "127.0.0.1:9000");
        assert_eq!(n.state, "alive");
        assert_eq!(n.last_heartbeat_ms, 100);
    }

    #[tokio::test]
    async fn upsert_updates_existing_row() {
        let p = pool().await;
        upsert(&p, "node-a", "old-addr", None, "alive", 100, None, None)
            .await
            .unwrap();
        upsert(
            &p,
            "node-a",
            "new-addr",
            Some("new-mongo"),
            "draining",
            200,
            None,
            None,
        )
        .await
        .unwrap();
        let n = get(&p, "node-a").await.unwrap().unwrap();
        assert_eq!(n.address, "new-addr");
        assert_eq!(n.mongo_address.as_deref(), Some("new-mongo"));
        assert_eq!(n.state, "draining");
        assert_eq!(n.last_heartbeat_ms, 200);
        assert_eq!(n.availability_zone, None);
        assert_eq!(n.region, None);
        // joined_at should remain the original.
        assert_eq!(n.joined_at_ms, 100);
    }

    #[tokio::test]
    async fn upsert_persists_region_and_availability_zone() {
        let p = pool().await;
        upsert(
            &p,
            "node-a",
            "addr",
            None,
            "alive",
            100,
            Some("us-west-2a"),
            Some("us-west-2"),
        )
        .await
        .unwrap();

        let n = get(&p, "node-a").await.unwrap().unwrap();
        assert_eq!(n.availability_zone.as_deref(), Some("us-west-2a"));
        assert_eq!(n.region.as_deref(), Some("us-west-2"));
    }

    #[tokio::test]
    async fn heartbeat_advances_timestamp() {
        let p = pool().await;
        upsert(&p, "node-a", "addr", None, "alive", 100, None, None)
            .await
            .unwrap();
        heartbeat(&p, "node-a", 500).await.unwrap();
        let n = get(&p, "node-a").await.unwrap().unwrap();
        assert_eq!(n.last_heartbeat_ms, 500);
    }

    #[tokio::test]
    async fn heartbeat_errors_on_unknown_node() {
        let p = pool().await;
        let err = heartbeat(&p, "ghost", 500).await.unwrap_err();
        assert!(matches!(err, MetaError::NotFound(_)));
    }

    #[tokio::test]
    async fn set_state_preserves_address_and_bumps_heartbeat() {
        let p = pool().await;
        upsert(&p, "node-a", "addr-keep", None, "alive", 100, None, None)
            .await
            .unwrap();
        set_state(&p, "node-a", "draining", 500).await.unwrap();
        let n = get(&p, "node-a").await.unwrap().unwrap();
        assert_eq!(n.state, "draining");
        assert_eq!(n.address, "addr-keep", "address must be preserved");
        assert_eq!(n.last_heartbeat_ms, 500);
    }

    #[tokio::test]
    async fn set_state_errors_on_unknown_node() {
        let p = pool().await;
        let err = set_state(&p, "ghost", "draining", 500).await.unwrap_err();
        assert!(matches!(err, MetaError::NotFound(_)));
    }

    #[tokio::test]
    async fn list_all_returns_every_node_in_id_order() {
        let p = pool().await;
        upsert(&p, "node-b", "bb", None, "alive", 100, None, None)
            .await
            .unwrap();
        upsert(&p, "node-a", "aa", Some("amongo"), "alive", 100, None, None)
            .await
            .unwrap();
        upsert(&p, "node-c", "cc", None, "draining", 200, None, None)
            .await
            .unwrap();
        let all = list_all(&p).await.unwrap();
        let ids: Vec<_> = all.iter().map(|n| n.node_id.as_str()).collect();
        assert_eq!(ids, ["node-a", "node-b", "node-c"]);
    }

    #[tokio::test]
    async fn stale_detects_nodes_below_threshold() {
        let p = pool().await;
        upsert(&p, "healthy", "a", None, "alive", 1000, None, None)
            .await
            .unwrap();
        upsert(&p, "dead", "b", None, "alive", 50, None, None)
            .await
            .unwrap();
        let s = stale(&p, 500).await.unwrap();
        let ids: Vec<_> = s.iter().map(|n| n.node_id.as_str()).collect();
        assert_eq!(ids, ["dead"]);
    }

    #[tokio::test]
    async fn mark_dead_if_stale_transitions_and_reports() {
        let p = pool().await;
        upsert(&p, "fresh", "a", None, "alive", 1000, None, None)
            .await
            .unwrap();
        upsert(&p, "gone-1", "b", None, "alive", 100, None, None)
            .await
            .unwrap();
        upsert(&p, "gone-2", "c", None, "alive", 120, None, None)
            .await
            .unwrap();
        // deadline 500 → both gone-1/gone-2 flip; fresh stays alive.
        let mut changed = mark_dead_if_stale(&p, 500).await.unwrap();
        changed.sort();
        assert_eq!(changed, vec!["gone-1", "gone-2"]);

        assert_eq!(get(&p, "fresh").await.unwrap().unwrap().state, "alive");
        assert_eq!(get(&p, "gone-1").await.unwrap().unwrap().state, "dead");
        // Heartbeat timestamp is preserved so GC + operators can
        // tell how long the node has been gone.
        assert_eq!(
            get(&p, "gone-1").await.unwrap().unwrap().last_heartbeat_ms,
            100
        );

        // Idempotent: a second call returns an empty list because
        // the stale rows are now state='dead' and the predicate
        // only fires on state='alive'.
        let again = mark_dead_if_stale(&p, 500).await.unwrap();
        assert!(again.is_empty());
    }

    #[tokio::test]
    async fn mark_dead_if_stale_ignores_non_alive_states() {
        let p = pool().await;
        upsert(&p, "leaving", "a", None, "leaving", 100, None, None)
            .await
            .unwrap();
        upsert(&p, "draining", "b", None, "draining", 100, None, None)
            .await
            .unwrap();
        let changed = mark_dead_if_stale(&p, 500).await.unwrap();
        assert!(
            changed.is_empty(),
            "only state='alive' rows should transition; got {changed:?}"
        );
    }

    #[tokio::test]
    async fn gc_dead_removes_long_gone_rows_only() {
        let p = pool().await;
        // Three dead rows with different ages, plus one alive.
        upsert(&p, "just-died", "a", None, "alive", 500, None, None)
            .await
            .unwrap();
        upsert(&p, "ancient", "b", None, "alive", 10, None, None)
            .await
            .unwrap();
        upsert(&p, "alive", "c", None, "alive", 1000, None, None)
            .await
            .unwrap();
        mark_dead_if_stale(&p, 900).await.unwrap();

        // GC with a 100 deadline: "ancient" (hb=10) and
        // "just-died" (hb=500) both qualify. But we want only
        // ancient to drop, so set threshold between them.
        let removed = gc_dead(&p, 100).await.unwrap();
        assert_eq!(removed, vec!["ancient"]);
        assert!(get(&p, "just-died").await.unwrap().is_some());
        assert!(get(&p, "alive").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn gc_dead_skips_alive_and_leaving_rows() {
        // Paranoia: even with an absurdly aggressive gc_deadline
        // that would cover every timestamp in the table, rows
        // whose state != 'dead' must be left alone.
        let p = pool().await;
        upsert(&p, "alive", "a", None, "alive", 10, None, None)
            .await
            .unwrap();
        upsert(&p, "leaving", "b", None, "leaving", 10, None, None)
            .await
            .unwrap();
        let removed = gc_dead(&p, i64::MAX).await.unwrap();
        assert!(removed.is_empty());
        assert!(get(&p, "alive").await.unwrap().is_some());
        assert!(get(&p, "leaving").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn remove_reports_whether_row_existed() {
        let p = pool().await;
        upsert(&p, "node-a", "a", None, "alive", 100, None, None)
            .await
            .unwrap();
        assert!(remove(&p, "node-a").await.unwrap());
        assert!(get(&p, "node-a").await.unwrap().is_none());
        // Second remove returns false — already gone.
        assert!(!remove(&p, "node-a").await.unwrap());
        // Removing a never-existed id also reports false.
        assert!(!remove(&p, "ghost").await.unwrap());
    }
}
