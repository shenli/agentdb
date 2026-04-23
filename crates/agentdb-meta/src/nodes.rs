//! Cluster node membership + heartbeat. Follow-up PRs add LISTEN/NOTIFY
//! for change propagation (Postgres only).

use sqlx::sqlite::SqlitePool;

use crate::error::{MetaError, MetaResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub node_id: String,
    pub address: String,
    pub state: String,
    pub last_heartbeat_ms: i64,
    pub joined_at_ms: i64,
}

pub async fn upsert(
    pool: &SqlitePool,
    node_id: &str,
    address: &str,
    state: &str,
    now_ms: i64,
) -> MetaResult<()> {
    // SQLite's ON CONFLICT upsert syntax.
    sqlx::query(
        "INSERT INTO nodes (node_id, address, state, last_heartbeat_ms, joined_at_ms)
              VALUES (?, ?, ?, ?, ?)
         ON CONFLICT (node_id) DO UPDATE SET
              address = excluded.address,
              state = excluded.state,
              last_heartbeat_ms = excluded.last_heartbeat_ms",
    )
    .bind(node_id)
    .bind(address)
    .bind(state)
    .bind(now_ms)
    .bind(now_ms)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn heartbeat(pool: &SqlitePool, node_id: &str, now_ms: i64) -> MetaResult<()> {
    let rows = sqlx::query("UPDATE nodes SET last_heartbeat_ms = ? WHERE node_id = ?")
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
pub async fn set_state(
    pool: &SqlitePool,
    node_id: &str,
    state: &str,
    now_ms: i64,
) -> MetaResult<()> {
    let rows = sqlx::query("UPDATE nodes SET state = ?, last_heartbeat_ms = ? WHERE node_id = ?")
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

pub async fn get(pool: &SqlitePool, node_id: &str) -> MetaResult<Option<Node>> {
    let row: Option<(String, String, String, i64, i64)> = sqlx::query_as(
        "SELECT node_id, address, state, last_heartbeat_ms, joined_at_ms FROM nodes WHERE node_id = ?",
    )
    .bind(node_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(n, a, s, h, j)| Node {
        node_id: n,
        address: a,
        state: s,
        last_heartbeat_ms: h,
        joined_at_ms: j,
    }))
}

/// Every registered node, ordered by `node_id`. Point-in-time
/// snapshot — a node can be updated between this return and the
/// caller acting on it. Used by `GET /api/admin/nodes` and by the
/// scheduler's membership scan.
pub async fn list_all(pool: &SqlitePool) -> MetaResult<Vec<Node>> {
    let rows: Vec<(String, String, String, i64, i64)> = sqlx::query_as(
        "SELECT node_id, address, state, last_heartbeat_ms, joined_at_ms
           FROM nodes ORDER BY node_id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(n, a, s, h, j)| Node {
            node_id: n,
            address: a,
            state: s,
            last_heartbeat_ms: h,
            joined_at_ms: j,
        })
        .collect())
}

/// Nodes whose last heartbeat is older than `threshold_ms`. Used by
/// the scheduler to detect dead nodes and start failover.
pub async fn stale(pool: &SqlitePool, threshold_ms: i64) -> MetaResult<Vec<Node>> {
    let rows: Vec<(String, String, String, i64, i64)> = sqlx::query_as(
        "SELECT node_id, address, state, last_heartbeat_ms, joined_at_ms
           FROM nodes WHERE last_heartbeat_ms < ? ORDER BY last_heartbeat_ms",
    )
    .bind(threshold_ms)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(n, a, s, h, j)| Node {
            node_id: n,
            address: a,
            state: s,
            last_heartbeat_ms: h,
            joined_at_ms: j,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations;

    async fn pool() -> SqlitePool {
        let p = SqlitePool::connect("sqlite::memory:").await.unwrap();
        migrations::apply(&p).await.unwrap();
        p
    }

    #[tokio::test]
    async fn upsert_and_get() {
        let p = pool().await;
        upsert(&p, "node-a", "127.0.0.1:9000", "alive", 100)
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
        upsert(&p, "node-a", "old-addr", "alive", 100)
            .await
            .unwrap();
        upsert(&p, "node-a", "new-addr", "draining", 200)
            .await
            .unwrap();
        let n = get(&p, "node-a").await.unwrap().unwrap();
        assert_eq!(n.address, "new-addr");
        assert_eq!(n.state, "draining");
        assert_eq!(n.last_heartbeat_ms, 200);
        // joined_at should remain the original.
        assert_eq!(n.joined_at_ms, 100);
    }

    #[tokio::test]
    async fn heartbeat_advances_timestamp() {
        let p = pool().await;
        upsert(&p, "node-a", "addr", "alive", 100).await.unwrap();
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
        upsert(&p, "node-a", "addr-keep", "alive", 100)
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
        upsert(&p, "node-b", "bb", "alive", 100).await.unwrap();
        upsert(&p, "node-a", "aa", "alive", 100).await.unwrap();
        upsert(&p, "node-c", "cc", "draining", 200).await.unwrap();
        let all = list_all(&p).await.unwrap();
        let ids: Vec<_> = all.iter().map(|n| n.node_id.as_str()).collect();
        assert_eq!(ids, ["node-a", "node-b", "node-c"]);
    }

    #[tokio::test]
    async fn stale_detects_nodes_below_threshold() {
        let p = pool().await;
        upsert(&p, "healthy", "a", "alive", 1000).await.unwrap();
        upsert(&p, "dead", "b", "alive", 50).await.unwrap();
        let s = stale(&p, 500).await.unwrap();
        let ids: Vec<_> = s.iter().map(|n| n.node_id.as_str()).collect();
        assert_eq!(ids, ["dead"]);
    }
}
