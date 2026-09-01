//! Tablet replica placement INTENT.
//!
//! Rows say where each tablet is *supposed* to have replicas and in what
//! role/lifecycle state; they change only on placement decisions (scheduler,
//! movements, node removal). Observed progress and health — which used to be
//! columns here, written from raft hot paths — now ride the per-node
//! [`crate::node_operational`] summaries at heartbeat cadence; use
//! [`crate::node_operational::merged_replica_view_all`] (or `_by_tablet`)
//! when a decision needs intent joined with observations.

use sqlx::AnyPool;

use crate::error::{MetaError, MetaResult};

/// Placement intent row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaIntent {
    pub tablet_id: i64,
    pub node_id: String,
    pub role: String,
    /// Lifecycle intent: `active` or `draining` (being replaced/removed).
    pub state: String,
    pub updated_at_ms: i64,
}

/// Merged intent + observation view. Shape-compatible with the pre-stage-1
/// `tablet_replicas` row so decision code keeps its vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletReplica {
    pub tablet_id: i64,
    pub node_id: String,
    pub role: String,
    /// Observed health (`unknown` when no fresh report covers the replica),
    /// or the intent state `draining` when the replica is being drained.
    pub health: String,
    pub applied_index: i64,
    pub match_index: i64,
    pub updated_at_ms: i64,
}

pub async fn upsert_intent(
    pool: &AnyPool,
    tablet_id: i64,
    node_id: &str,
    role: &str,
    now_ms: i64,
) -> MetaResult<()> {
    validate_role(role)?;
    sqlx::query(
        "INSERT INTO tablet_replicas (tablet_id, node_id, role, state, updated_at_ms)
         VALUES ($1, $2, $3, 'active', $4)
         ON CONFLICT (tablet_id, node_id) DO UPDATE SET
            role = excluded.role,
            state = 'active',
            updated_at_ms = excluded.updated_at_ms",
    )
    .bind(tablet_id)
    .bind(node_id)
    .bind(role)
    .bind(now_ms)
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark a replica's lifecycle intent (`active` or `draining`).
pub async fn set_state(
    pool: &AnyPool,
    tablet_id: i64,
    node_id: &str,
    state: &str,
    now_ms: i64,
) -> MetaResult<bool> {
    validate_state(state)?;
    let rows = sqlx::query(
        "UPDATE tablet_replicas SET state = $1, updated_at_ms = $2
          WHERE tablet_id = $3 AND node_id = $4",
    )
    .bind(state)
    .bind(now_ms)
    .bind(tablet_id)
    .bind(node_id)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

pub async fn list_intent_by_tablet(
    pool: &AnyPool,
    tablet_id: i64,
) -> MetaResult<Vec<ReplicaIntent>> {
    let rows: Vec<(i64, String, String, String, i64)> = sqlx::query_as(
        "SELECT tablet_id, node_id, role, state, updated_at_ms
           FROM tablet_replicas
          WHERE tablet_id = $1
          ORDER BY node_id",
    )
    .bind(tablet_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_intent).collect())
}

pub async fn list_intent_all(pool: &AnyPool) -> MetaResult<Vec<ReplicaIntent>> {
    let rows: Vec<(i64, String, String, String, i64)> = sqlx::query_as(
        "SELECT tablet_id, node_id, role, state, updated_at_ms
           FROM tablet_replicas
          ORDER BY tablet_id, node_id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_intent).collect())
}

pub async fn delete(pool: &AnyPool, tablet_id: i64, node_id: &str) -> MetaResult<bool> {
    let rows = sqlx::query("DELETE FROM tablet_replicas WHERE tablet_id = $1 AND node_id = $2")
        .bind(tablet_id)
        .bind(node_id)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(rows > 0)
}

pub async fn delete_by_tablet(pool: &AnyPool, tablet_id: i64) -> MetaResult<u64> {
    let rows = sqlx::query("DELETE FROM tablet_replicas WHERE tablet_id = $1")
        .bind(tablet_id)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(rows)
}

fn validate_role(role: &str) -> MetaResult<()> {
    match role {
        "voter" | "learner" | "log_only" => Ok(()),
        other => Err(MetaError::InvalidInput(format!(
            "invalid tablet replica role `{other}`"
        ))),
    }
}

fn validate_state(state: &str) -> MetaResult<()> {
    match state {
        "active" | "draining" => Ok(()),
        other => Err(MetaError::InvalidInput(format!(
            "invalid tablet replica state `{other}`"
        ))),
    }
}

fn into_intent(
    (tablet_id, node_id, role, state, updated_at_ms): (i64, String, String, String, i64),
) -> ReplicaIntent {
    ReplicaIntent {
        tablet_id,
        node_id,
        role,
        state,
        updated_at_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> AnyPool {
        crate::test_pool_sqlite_memory().await
    }

    #[tokio::test]
    async fn intent_roundtrip_and_state() {
        let p = pool().await;
        upsert_intent(&p, 7, "node-a", "voter", 100).await.unwrap();
        upsert_intent(&p, 7, "node-b", "learner", 100)
            .await
            .unwrap();
        let rows = list_intent_by_tablet(&p, 7).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.state == "active"));

        assert!(set_state(&p, 7, "node-a", "draining", 200).await.unwrap());
        let rows = list_intent_by_tablet(&p, 7).await.unwrap();
        assert_eq!(
            rows.iter().find(|r| r.node_id == "node-a").unwrap().state,
            "draining"
        );
        // Re-upserting reactivates.
        upsert_intent(&p, 7, "node-a", "voter", 300).await.unwrap();
        let rows = list_intent_by_tablet(&p, 7).await.unwrap();
        assert_eq!(
            rows.iter().find(|r| r.node_id == "node-a").unwrap().state,
            "active"
        );
    }

    #[tokio::test]
    async fn invalid_role_and_state_rejected() {
        let p = pool().await;
        assert!(upsert_intent(&p, 7, "node-a", "boss", 100).await.is_err());
        upsert_intent(&p, 7, "node-a", "voter", 100).await.unwrap();
        assert!(set_state(&p, 7, "node-a", "sleepy", 100).await.is_err());
    }

    #[tokio::test]
    async fn delete_paths() {
        let p = pool().await;
        upsert_intent(&p, 7, "node-a", "voter", 100).await.unwrap();
        upsert_intent(&p, 7, "node-b", "voter", 100).await.unwrap();
        assert!(delete(&p, 7, "node-a").await.unwrap());
        assert!(!delete(&p, 7, "node-a").await.unwrap());
        assert_eq!(delete_by_tablet(&p, 7).await.unwrap(), 1);
    }
}
