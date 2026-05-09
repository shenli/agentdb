//! Retained tablet snapshot metadata.
//!
//! Snapshot bytes can be staged locally, retained in object storage, or only
//! sent point-to-point for catch-up. This table records what was produced so
//! operators can see snapshot age, size, target, and retention state.

use sqlx::AnyPool;

use crate::error::{MetaError, MetaResult};

type TabletSnapshotRow = (
    String,
    i64,
    String,
    i64,
    String,
    i64,
    i64,
    i64,
    i64,
    String,
    Option<String>,
    String,
    i64,
    i64,
    Option<i64>,
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletSnapshot {
    pub snapshot_id: String,
    pub tablet_id: i64,
    pub source_node_id: String,
    pub source_raft_node_id: i64,
    pub target_node_id: String,
    pub target_raft_node_id: i64,
    pub snapshot_index: i64,
    pub snapshot_term: i64,
    pub size_bytes: i64,
    pub sha256_hex: String,
    pub storage_uri: Option<String>,
    pub status: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub expires_at_ms: Option<i64>,
}

#[allow(clippy::too_many_arguments)]
pub async fn record(
    pool: &AnyPool,
    snapshot_id: &str,
    tablet_id: i64,
    source_node_id: &str,
    source_raft_node_id: i64,
    target_node_id: &str,
    target_raft_node_id: i64,
    snapshot_index: i64,
    snapshot_term: i64,
    size_bytes: i64,
    sha256_hex: &str,
    storage_uri: Option<&str>,
    status: &str,
    now_ms: i64,
    expires_at_ms: Option<i64>,
) -> MetaResult<()> {
    validate(
        snapshot_id,
        tablet_id,
        source_raft_node_id,
        target_raft_node_id,
    )?;
    validate_index(snapshot_index, "snapshot_index")?;
    validate_index(snapshot_term, "snapshot_term")?;
    validate_index(size_bytes, "size_bytes")?;
    validate_status(status)?;
    sqlx::query(
        "INSERT INTO tablet_snapshots
            (snapshot_id, tablet_id, source_node_id, source_raft_node_id,
             target_node_id, target_raft_node_id, snapshot_index, snapshot_term,
             size_bytes, sha256_hex, storage_uri, status, created_at_ms,
             updated_at_ms, expires_at_ms)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
         ON CONFLICT (snapshot_id) DO UPDATE SET
            tablet_id = excluded.tablet_id,
            source_node_id = excluded.source_node_id,
            source_raft_node_id = excluded.source_raft_node_id,
            target_node_id = excluded.target_node_id,
            target_raft_node_id = excluded.target_raft_node_id,
            snapshot_index = excluded.snapshot_index,
            snapshot_term = excluded.snapshot_term,
            size_bytes = excluded.size_bytes,
            sha256_hex = excluded.sha256_hex,
            storage_uri = excluded.storage_uri,
            status = excluded.status,
            updated_at_ms = excluded.updated_at_ms,
            expires_at_ms = excluded.expires_at_ms",
    )
    .bind(snapshot_id)
    .bind(tablet_id)
    .bind(source_node_id)
    .bind(source_raft_node_id)
    .bind(target_node_id)
    .bind(target_raft_node_id)
    .bind(snapshot_index)
    .bind(snapshot_term)
    .bind(size_bytes)
    .bind(sha256_hex)
    .bind(storage_uri)
    .bind(status)
    .bind(now_ms)
    .bind(now_ms)
    .bind(expires_at_ms)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list_recent(
    pool: &AnyPool,
    tablet_id: Option<i64>,
    include_deleted: bool,
    limit: i64,
) -> MetaResult<Vec<TabletSnapshot>> {
    let limit = limit.clamp(1, 1_000);
    let rows: Vec<TabletSnapshotRow> = match (tablet_id, include_deleted) {
        (Some(tablet_id), true) => {
            sqlx::query_as(
                "SELECT snapshot_id, tablet_id, source_node_id, source_raft_node_id,
                    target_node_id, target_raft_node_id, snapshot_index, snapshot_term,
                    size_bytes, sha256_hex, storage_uri, status, created_at_ms,
                    updated_at_ms, expires_at_ms
               FROM tablet_snapshots
              WHERE tablet_id = $1
              ORDER BY created_at_ms DESC, snapshot_index DESC
              LIMIT $2",
            )
            .bind(tablet_id)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
        (Some(tablet_id), false) => {
            sqlx::query_as(
                "SELECT snapshot_id, tablet_id, source_node_id, source_raft_node_id,
                    target_node_id, target_raft_node_id, snapshot_index, snapshot_term,
                    size_bytes, sha256_hex, storage_uri, status, created_at_ms,
                    updated_at_ms, expires_at_ms
               FROM tablet_snapshots
              WHERE tablet_id = $1 AND status != 'deleted'
              ORDER BY created_at_ms DESC, snapshot_index DESC
              LIMIT $2",
            )
            .bind(tablet_id)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
        (None, true) => {
            sqlx::query_as(
                "SELECT snapshot_id, tablet_id, source_node_id, source_raft_node_id,
                    target_node_id, target_raft_node_id, snapshot_index, snapshot_term,
                    size_bytes, sha256_hex, storage_uri, status, created_at_ms,
                    updated_at_ms, expires_at_ms
               FROM tablet_snapshots
              ORDER BY created_at_ms DESC, snapshot_index DESC
              LIMIT $1",
            )
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
        (None, false) => {
            sqlx::query_as(
                "SELECT snapshot_id, tablet_id, source_node_id, source_raft_node_id,
                    target_node_id, target_raft_node_id, snapshot_index, snapshot_term,
                    size_bytes, sha256_hex, storage_uri, status, created_at_ms,
                    updated_at_ms, expires_at_ms
               FROM tablet_snapshots
              WHERE status != 'deleted'
              ORDER BY created_at_ms DESC, snapshot_index DESC
              LIMIT $1",
            )
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
    };
    Ok(rows.into_iter().map(into_snapshot).collect())
}

pub async fn latest_available_by_tablet(pool: &AnyPool) -> MetaResult<Vec<TabletSnapshot>> {
    let rows: Vec<TabletSnapshotRow> = sqlx::query_as(
        "SELECT snapshot_id, tablet_id, source_node_id, source_raft_node_id,
                target_node_id, target_raft_node_id, snapshot_index, snapshot_term,
                size_bytes, sha256_hex, storage_uri, status, created_at_ms,
                updated_at_ms, expires_at_ms
           FROM tablet_snapshots
          WHERE status = 'available'
          ORDER BY tablet_id, target_node_id, snapshot_index DESC",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_snapshot).collect())
}

pub async fn latest_available_for_target(
    pool: &AnyPool,
    target_node_id: &str,
) -> MetaResult<Vec<TabletSnapshot>> {
    let rows: Vec<TabletSnapshotRow> = sqlx::query_as(
        "SELECT snapshot_id, tablet_id, source_node_id, source_raft_node_id,
                target_node_id, target_raft_node_id, snapshot_index, snapshot_term,
                size_bytes, sha256_hex, storage_uri, status, created_at_ms,
                updated_at_ms, expires_at_ms
           FROM tablet_snapshots
          WHERE status = 'available' AND target_node_id = $1
          ORDER BY tablet_id, snapshot_index DESC",
    )
    .bind(target_node_id)
    .fetch_all(pool)
    .await?;
    let mut latest = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for snapshot in rows.into_iter().map(into_snapshot) {
        if seen.insert(snapshot.tablet_id) {
            latest.push(snapshot);
        }
    }
    Ok(latest)
}

pub async fn list_expired(
    pool: &AnyPool,
    now_ms: i64,
    limit: i64,
) -> MetaResult<Vec<TabletSnapshot>> {
    let limit = limit.clamp(1, 1_000);
    let rows: Vec<TabletSnapshotRow> = sqlx::query_as(
        "SELECT snapshot_id, tablet_id, source_node_id, source_raft_node_id,
                target_node_id, target_raft_node_id, snapshot_index, snapshot_term,
                size_bytes, sha256_hex, storage_uri, status, created_at_ms,
                updated_at_ms, expires_at_ms
           FROM tablet_snapshots
          WHERE status != 'deleted'
            AND expires_at_ms IS NOT NULL
            AND expires_at_ms <= $1
          ORDER BY expires_at_ms ASC, created_at_ms ASC
          LIMIT $2",
    )
    .bind(now_ms)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_snapshot).collect())
}

pub async fn count_expired(pool: &AnyPool, now_ms: i64) -> MetaResult<i64> {
    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)
           FROM tablet_snapshots
          WHERE status != 'deleted'
            AND expires_at_ms IS NOT NULL
            AND expires_at_ms <= $1",
    )
    .bind(now_ms)
    .fetch_one(pool)
    .await?;
    Ok(count.0)
}

pub async fn mark_expired_deleted(pool: &AnyPool, now_ms: i64, dry_run: bool) -> MetaResult<i64> {
    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)
           FROM tablet_snapshots
          WHERE status != 'deleted'
            AND storage_uri IS NULL
            AND expires_at_ms IS NOT NULL
            AND expires_at_ms <= $1",
    )
    .bind(now_ms)
    .fetch_one(pool)
    .await?;
    if dry_run || count.0 == 0 {
        return Ok(count.0);
    }
    sqlx::query(
        "UPDATE tablet_snapshots
            SET status = 'deleted', updated_at_ms = $1
          WHERE status != 'deleted'
            AND storage_uri IS NULL
            AND expires_at_ms IS NOT NULL
            AND expires_at_ms <= $2",
    )
    .bind(now_ms)
    .bind(now_ms)
    .execute(pool)
    .await?;
    Ok(count.0)
}

pub async fn mark_deleted(pool: &AnyPool, snapshot_id: &str, now_ms: i64) -> MetaResult<bool> {
    if snapshot_id.is_empty() {
        return Err(MetaError::InvalidInput(
            "snapshot_id must not be empty".into(),
        ));
    }
    let rows = sqlx::query(
        "UPDATE tablet_snapshots
            SET status = 'deleted', updated_at_ms = $1
          WHERE snapshot_id = $2",
    )
    .bind(now_ms)
    .bind(snapshot_id)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

fn validate(
    snapshot_id: &str,
    tablet_id: i64,
    source_raft_node_id: i64,
    target_raft_node_id: i64,
) -> MetaResult<()> {
    if snapshot_id.is_empty() {
        return Err(MetaError::InvalidInput(
            "snapshot_id must not be empty".into(),
        ));
    }
    validate_index(tablet_id, "tablet_id")?;
    validate_index(source_raft_node_id, "source_raft_node_id")?;
    validate_index(target_raft_node_id, "target_raft_node_id")?;
    Ok(())
}

fn validate_status(status: &str) -> MetaResult<()> {
    match status {
        "available" | "failed" | "deleted" => Ok(()),
        other => Err(MetaError::InvalidInput(format!(
            "invalid tablet snapshot status `{other}`"
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

fn into_snapshot(row: TabletSnapshotRow) -> TabletSnapshot {
    let (
        snapshot_id,
        tablet_id,
        source_node_id,
        source_raft_node_id,
        target_node_id,
        target_raft_node_id,
        snapshot_index,
        snapshot_term,
        size_bytes,
        sha256_hex,
        storage_uri,
        status,
        created_at_ms,
        updated_at_ms,
        expires_at_ms,
    ) = row;
    TabletSnapshot {
        snapshot_id,
        tablet_id,
        source_node_id,
        source_raft_node_id,
        target_node_id,
        target_raft_node_id,
        snapshot_index,
        snapshot_term,
        size_bytes,
        sha256_hex,
        storage_uri,
        status,
        created_at_ms,
        updated_at_ms,
        expires_at_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn record_list_and_prune_expired_snapshots() {
        let pool = crate::test_pool_sqlite_memory().await;
        record(
            &pool,
            "snap-1",
            7,
            "node-a",
            1,
            "node-b",
            2,
            42,
            3,
            128,
            "abcd",
            None,
            "available",
            1_000,
            Some(1_500),
        )
        .await
        .unwrap();

        let snapshots = list_recent(&pool, Some(7), false, 10).await.unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].target_node_id, "node-b");
        assert_eq!(snapshots[0].snapshot_index, 42);

        assert_eq!(mark_expired_deleted(&pool, 1_400, false).await.unwrap(), 0);
        assert_eq!(mark_expired_deleted(&pool, 1_600, true).await.unwrap(), 1);
        assert_eq!(list_recent(&pool, None, false, 10).await.unwrap().len(), 1);
        assert_eq!(mark_expired_deleted(&pool, 1_600, false).await.unwrap(), 1);
        assert!(list_recent(&pool, None, false, 10)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            list_recent(&pool, None, true, 10).await.unwrap()[0].status,
            "deleted"
        );
    }

    #[tokio::test]
    async fn object_backed_snapshots_require_explicit_delete() {
        let pool = crate::test_pool_sqlite_memory().await;
        record(
            &pool,
            "snap-object",
            7,
            "node-a",
            1,
            "node-b",
            2,
            42,
            3,
            128,
            "abcd",
            Some("object://snap-object"),
            "available",
            1_000,
            Some(1_500),
        )
        .await
        .unwrap();

        let expired = list_expired(&pool, 1_600, 10).await.unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(mark_expired_deleted(&pool, 1_600, false).await.unwrap(), 0);
        assert_eq!(list_recent(&pool, None, false, 10).await.unwrap().len(), 1);
        assert!(mark_deleted(&pool, "snap-object", 1_700).await.unwrap());
        assert!(list_recent(&pool, None, false, 10)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn latest_available_for_target_keeps_highest_index_per_tablet() {
        let pool = crate::test_pool_sqlite_memory().await;
        for (snapshot_id, index) in [("snap-old", 10), ("snap-new", 20)] {
            record(
                &pool,
                snapshot_id,
                7,
                "node-a",
                1,
                "node-b",
                2,
                index,
                3,
                128,
                "abcd",
                Some("object://snap"),
                "available",
                index,
                None,
            )
            .await
            .unwrap();
        }
        record(
            &pool,
            "snap-other-target",
            7,
            "node-a",
            1,
            "node-c",
            3,
            30,
            3,
            128,
            "abcd",
            Some("object://snap"),
            "available",
            30,
            None,
        )
        .await
        .unwrap();

        let snapshots = latest_available_for_target(&pool, "node-b").await.unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].snapshot_id, "snap-new");
        assert_eq!(snapshots[0].snapshot_index, 20);
    }
}
