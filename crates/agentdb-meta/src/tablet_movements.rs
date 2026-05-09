//! Tablet split/merge state movement work queue.
//!
//! Split/merge range cutover is metadata-only. These rows describe the
//! source-to-destination state copy/replay work that must advance destination
//! `tablet_replicas` before a safe cutover.

use sqlx::AnyPool;

use crate::error::{MetaError, MetaResult};

type TabletMovementRow = (
    String,
    String,
    i64,
    i64,
    String,
    i64,
    i64,
    i64,
    String,
    i64,
    i64,
    Option<i64>,
);

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TabletMovement {
    pub movement_id: String,
    pub movement_type: String,
    pub source_tablet_id: i64,
    pub destination_tablet_id: i64,
    pub placement_scope_id: String,
    pub start_shard: i64,
    pub end_shard: i64,
    pub required_applied_index: i64,
    pub status: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub completed_at_ms: Option<i64>,
}

#[allow(clippy::too_many_arguments)]
pub async fn upsert_pending(
    pool: &AnyPool,
    movement_id: &str,
    movement_type: &str,
    source_tablet_id: i64,
    destination_tablet_id: i64,
    placement_scope_id: &str,
    start_shard: i64,
    end_shard: i64,
    required_applied_index: i64,
    now_ms: i64,
) -> MetaResult<()> {
    validate(
        movement_id,
        movement_type,
        source_tablet_id,
        destination_tablet_id,
        start_shard,
        end_shard,
        required_applied_index,
    )?;
    sqlx::query(
        "INSERT INTO tablet_movements
            (movement_id, movement_type, source_tablet_id, destination_tablet_id,
             placement_scope_id, start_shard, end_shard, required_applied_index,
             status, created_at_ms, updated_at_ms, completed_at_ms)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', $9, $10, NULL)
         ON CONFLICT (movement_id) DO UPDATE SET
             movement_type = excluded.movement_type,
             source_tablet_id = excluded.source_tablet_id,
             destination_tablet_id = excluded.destination_tablet_id,
             placement_scope_id = excluded.placement_scope_id,
             start_shard = excluded.start_shard,
             end_shard = excluded.end_shard,
             required_applied_index = excluded.required_applied_index,
             status = 'pending',
             updated_at_ms = excluded.updated_at_ms,
             completed_at_ms = NULL",
    )
    .bind(movement_id)
    .bind(movement_type)
    .bind(source_tablet_id)
    .bind(destination_tablet_id)
    .bind(placement_scope_id)
    .bind(start_shard)
    .bind(end_shard)
    .bind(required_applied_index)
    .bind(now_ms)
    .bind(now_ms)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_completed(pool: &AnyPool, movement_id: &str, now_ms: i64) -> MetaResult<bool> {
    let rows = sqlx::query(
        "UPDATE tablet_movements
            SET status = 'completed', updated_at_ms = $1, completed_at_ms = $2
          WHERE movement_id = $3 AND status = 'pending'",
    )
    .bind(now_ms)
    .bind(now_ms)
    .bind(movement_id)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

pub async fn list_pending(pool: &AnyPool, limit: i64) -> MetaResult<Vec<TabletMovement>> {
    list_recent(pool, Some("pending"), limit).await
}

pub async fn list_open(pool: &AnyPool, limit: i64) -> MetaResult<Vec<TabletMovement>> {
    let rows: Vec<TabletMovementRow> = sqlx::query_as(
        "SELECT movement_id, movement_type, source_tablet_id, destination_tablet_id,
                placement_scope_id, start_shard, end_shard, required_applied_index,
                status, created_at_ms, updated_at_ms, completed_at_ms
           FROM tablet_movements
          WHERE status IN ('pending', 'completed')
          ORDER BY updated_at_ms DESC, movement_id
          LIMIT $1",
    )
    .bind(limit.clamp(1, 1_000))
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_movement).collect())
}

pub async fn list_open_split_group(
    pool: &AnyPool,
    placement_scope_id: &str,
    source_tablet_id: i64,
) -> MetaResult<Vec<TabletMovement>> {
    let rows: Vec<TabletMovementRow> = sqlx::query_as(
        "SELECT movement_id, movement_type, source_tablet_id, destination_tablet_id,
                placement_scope_id, start_shard, end_shard, required_applied_index,
                status, created_at_ms, updated_at_ms, completed_at_ms
           FROM tablet_movements
          WHERE status IN ('pending', 'completed')
            AND movement_type = 'split'
            AND placement_scope_id = $1
            AND source_tablet_id = $2
          ORDER BY start_shard, end_shard, destination_tablet_id, movement_id",
    )
    .bind(placement_scope_id)
    .bind(source_tablet_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_movement).collect())
}

pub async fn list_open_merge_group(
    pool: &AnyPool,
    placement_scope_id: &str,
    destination_tablet_id: i64,
) -> MetaResult<Vec<TabletMovement>> {
    let rows: Vec<TabletMovementRow> = sqlx::query_as(
        "SELECT movement_id, movement_type, source_tablet_id, destination_tablet_id,
                placement_scope_id, start_shard, end_shard, required_applied_index,
                status, created_at_ms, updated_at_ms, completed_at_ms
           FROM tablet_movements
          WHERE status IN ('pending', 'completed')
            AND movement_type = 'merge'
            AND placement_scope_id = $1
            AND destination_tablet_id = $2
          ORDER BY start_shard, end_shard, source_tablet_id, movement_id",
    )
    .bind(placement_scope_id)
    .bind(destination_tablet_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_movement).collect())
}

pub async fn get(pool: &AnyPool, movement_id: &str) -> MetaResult<Option<TabletMovement>> {
    let row: Option<TabletMovementRow> = sqlx::query_as(
        "SELECT movement_id, movement_type, source_tablet_id, destination_tablet_id,
                placement_scope_id, start_shard, end_shard, required_applied_index,
                status, created_at_ms, updated_at_ms, completed_at_ms
           FROM tablet_movements
          WHERE movement_id = $1",
    )
    .bind(movement_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(into_movement))
}

pub async fn list_recent(
    pool: &AnyPool,
    status: Option<&str>,
    limit: i64,
) -> MetaResult<Vec<TabletMovement>> {
    if let Some(status) = status {
        validate_status(status)?;
    }
    let limit = limit.clamp(1, 1_000);
    let rows: Vec<TabletMovementRow> = if let Some(status) = status {
        sqlx::query_as(
            "SELECT movement_id, movement_type, source_tablet_id, destination_tablet_id,
                    placement_scope_id, start_shard, end_shard, required_applied_index,
                    status, created_at_ms, updated_at_ms, completed_at_ms
               FROM tablet_movements
              WHERE status = $1
              ORDER BY updated_at_ms DESC, movement_id
              LIMIT $2",
        )
        .bind(status)
        .bind(limit)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT movement_id, movement_type, source_tablet_id, destination_tablet_id,
                    placement_scope_id, start_shard, end_shard, required_applied_index,
                    status, created_at_ms, updated_at_ms, completed_at_ms
               FROM tablet_movements
              ORDER BY updated_at_ms DESC, movement_id
              LIMIT $1",
        )
        .bind(limit)
        .fetch_all(pool)
        .await?
    };
    Ok(rows.into_iter().map(into_movement).collect())
}

fn validate(
    movement_id: &str,
    movement_type: &str,
    source_tablet_id: i64,
    destination_tablet_id: i64,
    start_shard: i64,
    end_shard: i64,
    required_applied_index: i64,
) -> MetaResult<()> {
    if movement_id.trim().is_empty() {
        return Err(MetaError::InvalidInput(
            "movement_id must not be empty".into(),
        ));
    }
    validate_type(movement_type)?;
    if source_tablet_id < 0 || destination_tablet_id < 0 {
        return Err(MetaError::InvalidInput(
            "movement tablet ids must be non-negative".into(),
        ));
    }
    if start_shard < 0 || end_shard < start_shard {
        return Err(MetaError::InvalidInput(format!(
            "invalid movement range {start_shard}..{end_shard}"
        )));
    }
    if required_applied_index < 0 {
        return Err(MetaError::InvalidInput(
            "required_applied_index must be non-negative".into(),
        ));
    }
    Ok(())
}

fn validate_type(movement_type: &str) -> MetaResult<()> {
    match movement_type {
        "split" | "merge" => Ok(()),
        other => Err(MetaError::InvalidInput(format!(
            "invalid tablet movement type `{other}`"
        ))),
    }
}

fn validate_status(status: &str) -> MetaResult<()> {
    match status {
        "pending" | "completed" | "cancelled" => Ok(()),
        other => Err(MetaError::InvalidInput(format!(
            "invalid tablet movement status `{other}`"
        ))),
    }
}

fn into_movement(
    (
        movement_id,
        movement_type,
        source_tablet_id,
        destination_tablet_id,
        placement_scope_id,
        start_shard,
        end_shard,
        required_applied_index,
        status,
        created_at_ms,
        updated_at_ms,
        completed_at_ms,
    ): TabletMovementRow,
) -> TabletMovement {
    TabletMovement {
        movement_id,
        movement_type,
        source_tablet_id,
        destination_tablet_id,
        placement_scope_id,
        start_shard,
        end_shard,
        required_applied_index,
        status,
        created_at_ms,
        updated_at_ms,
        completed_at_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> AnyPool {
        crate::test_pool_sqlite_memory().await
    }

    #[tokio::test]
    async fn upsert_list_and_complete_movement() {
        let p = pool().await;
        upsert_pending(&p, "split:7:8", "split", 7, 8, "default", 0, 49, 12, 100)
            .await
            .unwrap();
        upsert_pending(&p, "split:7:8", "split", 7, 8, "default", 0, 49, 13, 200)
            .await
            .unwrap();

        let rows = list_recent(&p, Some("pending"), 10).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].required_applied_index, 13);
        assert_eq!(rows[0].status, "pending");
        assert_eq!(list_pending(&p, 10).await.unwrap(), rows);

        assert!(mark_completed(&p, "split:7:8", 300).await.unwrap());
        let rows = list_recent(&p, Some("completed"), 10).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].completed_at_ms, Some(300));

        let rows = list_open(&p, 10).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].movement_id, "split:7:8");
        let rows = list_open_split_group(&p, "default", 7).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].movement_id, "split:7:8");

        upsert_pending(&p, "merge:7:9", "merge", 7, 9, "default", 0, 49, 13, 400)
            .await
            .unwrap();
        let rows = list_open_merge_group(&p, "default", 9).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].movement_id, "merge:7:9");
    }

    #[tokio::test]
    async fn validation_rejects_bad_status() {
        let p = pool().await;
        let err = list_recent(&p, Some("running"), 10).await.unwrap_err();
        assert!(matches!(err, MetaError::InvalidInput(_)));
    }
}
