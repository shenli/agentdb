//! Tablet replica placement and health metadata.
//!
//! This is control-plane state only. The future Raft implementation owns the
//! actual log replication; these rows let routing, placement, and operators see
//! where each tablet is expected to have voters/learners and how far they have
//! caught up.

use sqlx::AnyPool;

use crate::error::{MetaError, MetaResult};

type TabletReplicaRow = (i64, String, String, String, i64, i64, i64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletReplica {
    pub tablet_id: i64,
    pub node_id: String,
    pub role: String,
    pub health: String,
    pub applied_index: i64,
    pub match_index: i64,
    pub updated_at_ms: i64,
}

#[allow(clippy::too_many_arguments)]
pub async fn upsert(
    pool: &AnyPool,
    tablet_id: i64,
    node_id: &str,
    role: &str,
    health: &str,
    applied_index: i64,
    match_index: i64,
    now_ms: i64,
) -> MetaResult<()> {
    validate_role(role)?;
    validate_health(health)?;
    validate_index(applied_index, "applied_index")?;
    validate_index(match_index, "match_index")?;
    sqlx::query(
        "INSERT INTO tablet_replicas
            (tablet_id, node_id, role, health, applied_index, match_index, updated_at_ms)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (tablet_id, node_id) DO UPDATE SET
            role = excluded.role,
            health = excluded.health,
            applied_index = excluded.applied_index,
            match_index = excluded.match_index,
            updated_at_ms = excluded.updated_at_ms",
    )
    .bind(tablet_id)
    .bind(node_id)
    .bind(role)
    .bind(health)
    .bind(applied_index)
    .bind(match_index)
    .bind(now_ms)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_progress(
    pool: &AnyPool,
    tablet_id: i64,
    node_id: &str,
    applied_index: i64,
    match_index: i64,
    now_ms: i64,
) -> MetaResult<bool> {
    validate_index(applied_index, "applied_index")?;
    validate_index(match_index, "match_index")?;
    let rows = sqlx::query(
        "UPDATE tablet_replicas
            SET applied_index = $1, match_index = $2, updated_at_ms = $3
          WHERE tablet_id = $4 AND node_id = $5",
    )
    .bind(applied_index)
    .bind(match_index)
    .bind(now_ms)
    .bind(tablet_id)
    .bind(node_id)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

pub async fn set_health(
    pool: &AnyPool,
    tablet_id: i64,
    node_id: &str,
    health: &str,
    now_ms: i64,
) -> MetaResult<bool> {
    validate_health(health)?;
    let rows = sqlx::query(
        "UPDATE tablet_replicas
            SET health = $1, updated_at_ms = $2
          WHERE tablet_id = $3 AND node_id = $4",
    )
    .bind(health)
    .bind(now_ms)
    .bind(tablet_id)
    .bind(node_id)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

pub async fn list_by_tablet(pool: &AnyPool, tablet_id: i64) -> MetaResult<Vec<TabletReplica>> {
    let rows: Vec<TabletReplicaRow> = sqlx::query_as(
        "SELECT tablet_id, node_id, role, health, applied_index, match_index, updated_at_ms
           FROM tablet_replicas
          WHERE tablet_id = $1
          ORDER BY node_id",
    )
    .bind(tablet_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_replica).collect())
}

pub async fn list_all(pool: &AnyPool) -> MetaResult<Vec<TabletReplica>> {
    let rows: Vec<TabletReplicaRow> = sqlx::query_as(
        "SELECT tablet_id, node_id, role, health, applied_index, match_index, updated_at_ms
           FROM tablet_replicas
          ORDER BY tablet_id, node_id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_replica).collect())
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

fn validate_health(health: &str) -> MetaResult<()> {
    match health {
        "unknown" | "healthy" | "lagging" | "unreachable" | "draining" => Ok(()),
        other => Err(MetaError::InvalidInput(format!(
            "invalid tablet replica health `{other}`"
        ))),
    }
}

fn validate_index(index: i64, name: &str) -> MetaResult<()> {
    if index < 0 {
        return Err(MetaError::InvalidInput(format!(
            "{name} must be non-negative"
        )));
    }
    Ok(())
}

fn into_replica(
    (tablet_id, node_id, role, health, applied_index, match_index, updated_at_ms): TabletReplicaRow,
) -> TabletReplica {
    TabletReplica {
        tablet_id,
        node_id,
        role,
        health,
        applied_index,
        match_index,
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
    async fn upsert_and_list_replicas() {
        let p = pool().await;
        upsert(&p, 7, "node-b", "learner", "lagging", 3, 5, 100)
            .await
            .unwrap();
        upsert(&p, 7, "node-a", "voter", "healthy", 10, 10, 100)
            .await
            .unwrap();

        let replicas = list_by_tablet(&p, 7).await.unwrap();
        assert_eq!(replicas.len(), 2);
        assert_eq!(replicas[0].node_id, "node-a");
        assert_eq!(replicas[0].role, "voter");
        assert_eq!(replicas[1].node_id, "node-b");
    }

    #[tokio::test]
    async fn upsert_replaces_existing_replica() {
        let p = pool().await;
        upsert(&p, 7, "node-a", "learner", "lagging", 1, 2, 100)
            .await
            .unwrap();
        upsert(&p, 7, "node-a", "voter", "healthy", 5, 6, 200)
            .await
            .unwrap();

        let replica = list_by_tablet(&p, 7).await.unwrap().remove(0);
        assert_eq!(replica.role, "voter");
        assert_eq!(replica.health, "healthy");
        assert_eq!(replica.applied_index, 5);
        assert_eq!(replica.match_index, 6);
        assert_eq!(replica.updated_at_ms, 200);
    }

    #[tokio::test]
    async fn set_progress_and_health_update_existing_row() {
        let p = pool().await;
        upsert(&p, 7, "node-a", "learner", "lagging", 1, 2, 100)
            .await
            .unwrap();

        assert!(set_progress(&p, 7, "node-a", 7, 8, 200).await.unwrap());
        assert!(set_health(&p, 7, "node-a", "healthy", 300).await.unwrap());

        let replica = list_by_tablet(&p, 7).await.unwrap().remove(0);
        assert_eq!(replica.applied_index, 7);
        assert_eq!(replica.match_index, 8);
        assert_eq!(replica.health, "healthy");
        assert_eq!(replica.updated_at_ms, 300);
    }

    #[tokio::test]
    async fn validation_rejects_unknown_values() {
        let p = pool().await;
        let err = upsert(&p, 7, "node-a", "primary", "healthy", 1, 1, 100)
            .await
            .unwrap_err();
        assert!(matches!(err, MetaError::InvalidInput(_)));

        let err = upsert(&p, 7, "node-a", "voter", "bad", 1, 1, 100)
            .await
            .unwrap_err();
        assert!(matches!(err, MetaError::InvalidInput(_)));
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let p = pool().await;
        upsert(&p, 7, "node-a", "voter", "healthy", 1, 1, 100)
            .await
            .unwrap();
        assert!(delete(&p, 7, "node-a").await.unwrap());
        assert!(!delete(&p, 7, "node-a").await.unwrap());
    }

    #[tokio::test]
    async fn delete_by_tablet_removes_all_replicas_for_tablet() {
        let p = pool().await;
        upsert(&p, 7, "node-a", "voter", "healthy", 1, 1, 100)
            .await
            .unwrap();
        upsert(&p, 7, "node-b", "learner", "lagging", 0, 0, 100)
            .await
            .unwrap();
        upsert(&p, 8, "node-a", "voter", "healthy", 1, 1, 100)
            .await
            .unwrap();

        assert_eq!(delete_by_tablet(&p, 7).await.unwrap(), 2);
        assert!(list_by_tablet(&p, 7).await.unwrap().is_empty());
        assert_eq!(list_by_tablet(&p, 8).await.unwrap().len(), 1);
    }
}
