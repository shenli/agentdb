//! Compressed virtual-shard range to tablet mapping.
//!
//! This is the target representation for tablet maps. The older `shards`
//! table remains as a compatibility fallback.

use sqlx::AnyPool;

use crate::error::{MetaError, MetaResult};
use crate::placement_scopes::DEFAULT_PLACEMENT_SCOPE_ID;

type TabletRangeRow = (String, i64, i64, i64, i64, i64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletRange {
    pub placement_scope_id: String,
    pub start_shard: i64,
    pub end_shard: i64,
    pub tablet_id: i64,
    pub map_epoch: i64,
    pub updated_at_ms: i64,
}

impl TabletRange {
    pub fn contains(&self, placement_scope_id: &str, shard_id: i64) -> bool {
        self.placement_scope_id == placement_scope_id
            && self.start_shard <= shard_id
            && shard_id <= self.end_shard
    }
}

pub async fn upsert(
    pool: &AnyPool,
    placement_scope_id: &str,
    start_shard: i64,
    end_shard: i64,
    tablet_id: i64,
    map_epoch: i64,
    now_ms: i64,
) -> MetaResult<()> {
    validate_range(start_shard, end_shard)?;
    let mut tx = pool.begin().await?;
    let overlap: Option<TabletRangeRow> = sqlx::query_as(
        "SELECT placement_scope_id, start_shard, end_shard, tablet_id, map_epoch, updated_at_ms
           FROM tablet_ranges
          WHERE placement_scope_id = ?
            AND start_shard <= ?
            AND end_shard >= ?
            AND NOT (start_shard = ? AND end_shard = ?)
          LIMIT 1",
    )
    .bind(placement_scope_id)
    .bind(end_shard)
    .bind(start_shard)
    .bind(start_shard)
    .bind(end_shard)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some(row) = overlap {
        let range = into_range(row);
        tx.rollback().await?;
        return Err(MetaError::Conflict(format!(
            "tablet range {placement_scope_id}:{start_shard}..{end_shard} overlaps existing range {}..{} for tablet {}",
            range.start_shard, range.end_shard, range.tablet_id
        )));
    }
    sqlx::query(
        "DELETE FROM tablet_ranges
          WHERE placement_scope_id = ? AND start_shard = ? AND end_shard = ?",
    )
    .bind(placement_scope_id)
    .bind(start_shard)
    .bind(end_shard)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO tablet_ranges
            (placement_scope_id, start_shard, end_shard, tablet_id, map_epoch, updated_at_ms)
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(placement_scope_id)
    .bind(start_shard)
    .bind(end_shard)
    .bind(tablet_id)
    .bind(map_epoch)
    .bind(now_ms)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub async fn find(
    pool: &AnyPool,
    placement_scope_id: &str,
    shard_id: i64,
) -> MetaResult<Option<TabletRange>> {
    let row: Option<TabletRangeRow> = sqlx::query_as(
        "SELECT placement_scope_id, start_shard, end_shard, tablet_id, map_epoch, updated_at_ms
           FROM tablet_ranges
          WHERE placement_scope_id = ? AND start_shard <= ? AND end_shard >= ?
          ORDER BY map_epoch DESC, start_shard DESC
          LIMIT 1",
    )
    .bind(placement_scope_id)
    .bind(shard_id)
    .bind(shard_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(into_range))
}

pub async fn list_all(pool: &AnyPool) -> MetaResult<Vec<TabletRange>> {
    let rows: Vec<TabletRangeRow> = sqlx::query_as(
        "SELECT placement_scope_id, start_shard, end_shard, tablet_id, map_epoch, updated_at_ms
           FROM tablet_ranges
          ORDER BY placement_scope_id, start_shard, end_shard",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_range).collect())
}

pub async fn list_by_tablet(pool: &AnyPool, tablet_id: i64) -> MetaResult<Vec<TabletRange>> {
    let rows: Vec<TabletRangeRow> = sqlx::query_as(
        "SELECT placement_scope_id, start_shard, end_shard, tablet_id, map_epoch, updated_at_ms
           FROM tablet_ranges
          WHERE tablet_id = ?
          ORDER BY placement_scope_id, start_shard, end_shard",
    )
    .bind(tablet_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_range).collect())
}

pub async fn delete(
    pool: &AnyPool,
    placement_scope_id: &str,
    start_shard: i64,
    end_shard: i64,
) -> MetaResult<bool> {
    let rows = sqlx::query(
        "DELETE FROM tablet_ranges
          WHERE placement_scope_id = ? AND start_shard = ? AND end_shard = ?",
    )
    .bind(placement_scope_id)
    .bind(start_shard)
    .bind(end_shard)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

pub fn range_override(
    ranges: &[TabletRange],
    placement_scope_id: Option<&str>,
    shard_id: i64,
) -> Option<i64> {
    let scope = placement_scope_id.unwrap_or(DEFAULT_PLACEMENT_SCOPE_ID);
    ranges
        .iter()
        .filter(|range| range.contains(scope, shard_id))
        .max_by_key(|range| range.map_epoch)
        .map(|range| range.tablet_id)
}

fn validate_range(start_shard: i64, end_shard: i64) -> MetaResult<()> {
    if start_shard < 0 || end_shard < start_shard {
        return Err(MetaError::InvalidInput(format!(
            "invalid tablet range {start_shard}..{end_shard}"
        )));
    }
    Ok(())
}

fn into_range(
    (placement_scope_id, start_shard, end_shard, tablet_id, map_epoch, updated_at_ms): TabletRangeRow,
) -> TabletRange {
    TabletRange {
        placement_scope_id,
        start_shard,
        end_shard,
        tablet_id,
        map_epoch,
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
    async fn upsert_and_find_range() {
        let p = pool().await;
        upsert(&p, DEFAULT_PLACEMENT_SCOPE_ID, 0, 99, 7, 1, 100)
            .await
            .unwrap();
        upsert(&p, DEFAULT_PLACEMENT_SCOPE_ID, 100, 199, 8, 1, 100)
            .await
            .unwrap();

        assert_eq!(
            find(&p, DEFAULT_PLACEMENT_SCOPE_ID, 42)
                .await
                .unwrap()
                .unwrap()
                .tablet_id,
            7
        );
        assert_eq!(
            find(&p, DEFAULT_PLACEMENT_SCOPE_ID, 100)
                .await
                .unwrap()
                .unwrap()
                .tablet_id,
            8
        );
        assert!(find(&p, DEFAULT_PLACEMENT_SCOPE_ID, 200)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn range_override_prefers_latest_epoch() {
        let ranges = vec![
            TabletRange {
                placement_scope_id: DEFAULT_PLACEMENT_SCOPE_ID.into(),
                start_shard: 0,
                end_shard: 99,
                tablet_id: 1,
                map_epoch: 1,
                updated_at_ms: 0,
            },
            TabletRange {
                placement_scope_id: DEFAULT_PLACEMENT_SCOPE_ID.into(),
                start_shard: 0,
                end_shard: 99,
                tablet_id: 2,
                map_epoch: 2,
                updated_at_ms: 0,
            },
        ];
        assert_eq!(
            range_override(&ranges, Some(DEFAULT_PLACEMENT_SCOPE_ID), 50),
            Some(2)
        );
    }

    #[tokio::test]
    async fn upsert_rejects_overlapping_range() {
        let p = pool().await;
        upsert(&p, DEFAULT_PLACEMENT_SCOPE_ID, 0, 99, 7, 1, 100)
            .await
            .unwrap();

        let err = upsert(&p, DEFAULT_PLACEMENT_SCOPE_ID, 50, 149, 8, 1, 200)
            .await
            .unwrap_err();
        assert!(matches!(err, MetaError::Conflict(_)));

        upsert(&p, DEFAULT_PLACEMENT_SCOPE_ID, 0, 99, 8, 2, 300)
            .await
            .unwrap();
        assert_eq!(
            find(&p, DEFAULT_PLACEMENT_SCOPE_ID, 42)
                .await
                .unwrap()
                .unwrap()
                .tablet_id,
            8
        );
    }
}
