//! Compressed virtual-shard range to tablet mapping.
//!
//! This is the target representation for tablet maps. The older `shards`
//! table remains as a compatibility fallback.

use sqlx::AnyPool;

use crate::error::{MetaError, MetaResult};
use crate::placement_scopes::DEFAULT_PLACEMENT_SCOPE_ID;

type TabletRangeRow = (String, i64, i64, i64, i64, i64);

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TabletRange {
    pub placement_scope_id: String,
    pub start_shard: i64,
    pub end_shard: i64,
    pub tablet_id: i64,
    pub map_epoch: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RangeBounds {
    pub start_shard: i64,
    pub end_shard: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplacementRange {
    pub start_shard: i64,
    pub end_shard: i64,
    pub tablet_id: i64,
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
          WHERE placement_scope_id = $1
            AND start_shard <= $2
            AND end_shard >= $3
            AND NOT (start_shard = $4 AND end_shard = $5)
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
          WHERE placement_scope_id = $1 AND start_shard = $2 AND end_shard = $3",
    )
    .bind(placement_scope_id)
    .bind(start_shard)
    .bind(end_shard)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO tablet_ranges
            (placement_scope_id, start_shard, end_shard, tablet_id, map_epoch, updated_at_ms)
         VALUES ($1, $2, $3, $4, $5, $6)",
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
          WHERE placement_scope_id = $1 AND start_shard <= $2 AND end_shard >= $3
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
          WHERE tablet_id = $1
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
          WHERE placement_scope_id = $1 AND start_shard = $2 AND end_shard = $3",
    )
    .bind(placement_scope_id)
    .bind(start_shard)
    .bind(end_shard)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

pub async fn replace_ranges(
    pool: &AnyPool,
    placement_scope_id: &str,
    old_ranges: &[RangeBounds],
    new_ranges: &[ReplacementRange],
    map_epoch: i64,
    now_ms: i64,
) -> MetaResult<()> {
    validate_replacement(old_ranges, new_ranges)?;
    let replace_start = old_ranges
        .iter()
        .map(|range| range.start_shard)
        .min()
        .expect("old_ranges validated non-empty");
    let replace_end = old_ranges
        .iter()
        .map(|range| range.end_shard)
        .max()
        .expect("old_ranges validated non-empty");

    let mut tx = pool.begin().await?;
    for old in old_ranges {
        let rows = sqlx::query(
            "DELETE FROM tablet_ranges
              WHERE placement_scope_id = $1 AND start_shard = $2 AND end_shard = $3",
        )
        .bind(placement_scope_id)
        .bind(old.start_shard)
        .bind(old.end_shard)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if rows == 0 {
            tx.rollback().await?;
            return Err(MetaError::NotFound(format!(
                "tablet range {placement_scope_id}:{}..{}",
                old.start_shard, old.end_shard
            )));
        }
    }

    let overlap: Option<TabletRangeRow> = sqlx::query_as(
        "SELECT placement_scope_id, start_shard, end_shard, tablet_id, map_epoch, updated_at_ms
           FROM tablet_ranges
          WHERE placement_scope_id = $1
            AND start_shard <= $2
            AND end_shard >= $3
          LIMIT 1",
    )
    .bind(placement_scope_id)
    .bind(replace_end)
    .bind(replace_start)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some(row) = overlap {
        let range = into_range(row);
        tx.rollback().await?;
        return Err(MetaError::Conflict(format!(
            "replacement range {placement_scope_id}:{replace_start}..{replace_end} overlaps existing range {}..{} for tablet {}",
            range.start_shard, range.end_shard, range.tablet_id
        )));
    }

    for new in new_ranges {
        sqlx::query(
            "INSERT INTO tablet_ranges
                (placement_scope_id, start_shard, end_shard, tablet_id, map_epoch, updated_at_ms)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(placement_scope_id)
        .bind(new.start_shard)
        .bind(new.end_shard)
        .bind(new.tablet_id)
        .bind(map_epoch)
        .bind(now_ms)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub fn validate_replacement_plan(
    old_ranges: &[RangeBounds],
    new_ranges: &[ReplacementRange],
) -> MetaResult<()> {
    validate_replacement(old_ranges, new_ranges)
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

fn validate_replacement(
    old_ranges: &[RangeBounds],
    new_ranges: &[ReplacementRange],
) -> MetaResult<()> {
    if old_ranges.is_empty() || new_ranges.is_empty() {
        return Err(MetaError::InvalidInput(
            "range replacement requires old and new ranges".into(),
        ));
    }
    let old = normalize_bounds(old_ranges)?;
    let new_bounds: Vec<RangeBounds> = new_ranges
        .iter()
        .map(|range| RangeBounds {
            start_shard: range.start_shard,
            end_shard: range.end_shard,
        })
        .collect();
    let new = normalize_bounds(&new_bounds)?;
    if old != new {
        return Err(MetaError::InvalidInput(format!(
            "replacement must cover exactly {}..{}, got {}..{}",
            old.start_shard, old.end_shard, new.start_shard, new.end_shard
        )));
    }
    for range in new_ranges {
        if range.tablet_id < 0 {
            return Err(MetaError::InvalidInput(
                "replacement tablet_id must be non-negative".into(),
            ));
        }
    }
    Ok(())
}

fn normalize_bounds(ranges: &[RangeBounds]) -> MetaResult<RangeBounds> {
    let mut sorted = ranges.to_vec();
    sorted.sort_by_key(|range| (range.start_shard, range.end_shard));
    for range in &sorted {
        validate_range(range.start_shard, range.end_shard)?;
    }
    for pair in sorted.windows(2) {
        if pair[0].end_shard + 1 != pair[1].start_shard {
            return Err(MetaError::InvalidInput(format!(
                "ranges must be contiguous, found {}..{} then {}..{}",
                pair[0].start_shard, pair[0].end_shard, pair[1].start_shard, pair[1].end_shard
            )));
        }
    }
    Ok(RangeBounds {
        start_shard: sorted[0].start_shard,
        end_shard: sorted[sorted.len() - 1].end_shard,
    })
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

    #[tokio::test]
    async fn replace_ranges_splits_one_range_atomically() {
        let p = pool().await;
        upsert(&p, DEFAULT_PLACEMENT_SCOPE_ID, 0, 99, 7, 1, 100)
            .await
            .unwrap();

        replace_ranges(
            &p,
            DEFAULT_PLACEMENT_SCOPE_ID,
            &[RangeBounds {
                start_shard: 0,
                end_shard: 99,
            }],
            &[
                ReplacementRange {
                    start_shard: 0,
                    end_shard: 49,
                    tablet_id: 8,
                },
                ReplacementRange {
                    start_shard: 50,
                    end_shard: 99,
                    tablet_id: 9,
                },
            ],
            2,
            200,
        )
        .await
        .unwrap();

        assert_eq!(
            find(&p, DEFAULT_PLACEMENT_SCOPE_ID, 25)
                .await
                .unwrap()
                .unwrap()
                .tablet_id,
            8
        );
        assert_eq!(
            find(&p, DEFAULT_PLACEMENT_SCOPE_ID, 75)
                .await
                .unwrap()
                .unwrap()
                .tablet_id,
            9
        );
    }

    #[tokio::test]
    async fn replace_ranges_merges_adjacent_ranges() {
        let p = pool().await;
        upsert(&p, DEFAULT_PLACEMENT_SCOPE_ID, 0, 49, 8, 1, 100)
            .await
            .unwrap();
        upsert(&p, DEFAULT_PLACEMENT_SCOPE_ID, 50, 99, 9, 1, 100)
            .await
            .unwrap();

        replace_ranges(
            &p,
            DEFAULT_PLACEMENT_SCOPE_ID,
            &[
                RangeBounds {
                    start_shard: 0,
                    end_shard: 49,
                },
                RangeBounds {
                    start_shard: 50,
                    end_shard: 99,
                },
            ],
            &[ReplacementRange {
                start_shard: 0,
                end_shard: 99,
                tablet_id: 10,
            }],
            2,
            200,
        )
        .await
        .unwrap();

        let ranges = list_all(&p).await.unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].tablet_id, 10);
        assert_eq!(ranges[0].start_shard, 0);
        assert_eq!(ranges[0].end_shard, 99);
    }

    #[tokio::test]
    async fn replace_ranges_rejects_partial_coverage() {
        let p = pool().await;
        upsert(&p, DEFAULT_PLACEMENT_SCOPE_ID, 0, 99, 7, 1, 100)
            .await
            .unwrap();

        let err = replace_ranges(
            &p,
            DEFAULT_PLACEMENT_SCOPE_ID,
            &[RangeBounds {
                start_shard: 0,
                end_shard: 99,
            }],
            &[ReplacementRange {
                start_shard: 0,
                end_shard: 98,
                tablet_id: 8,
            }],
            2,
            200,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, MetaError::InvalidInput(_)));

        assert_eq!(
            find(&p, DEFAULT_PLACEMENT_SCOPE_ID, 50)
                .await
                .unwrap()
                .unwrap()
                .tablet_id,
            7
        );
    }
}
