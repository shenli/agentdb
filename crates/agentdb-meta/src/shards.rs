//! Shard→tablet assignment. 16384 rows, seeded on cluster init.
//! `tablet_id = NULL` means "use hash-based default routing"
//! — the common case until a manual override or tablet split.

use sqlx::sqlite::SqlitePool;

use crate::error::MetaResult;

/// Raw row from the `shards` table. `tablet_id == None` means "no
/// override — default routing applies". `updated_at_ms` tracks the
/// last time an operator or the scheduler rewrote the row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRow {
    pub shard_id: i64,
    pub tablet_id: Option<i64>,
    pub updated_at_ms: i64,
}

/// Default shard count `S`. Fixed at cluster-init, never changes for a
/// given cluster. See design §2.4.
pub const DEFAULT_SHARD_COUNT: i64 = 16384;

pub async fn seed_shards(pool: &SqlitePool, s: i64, now_ms: i64) -> MetaResult<()> {
    // Idempotent: skip seeding if the table already has rows.
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM shards")
        .fetch_one(pool)
        .await?;
    if count >= s {
        return Ok(());
    }
    // Bulk insert. A simple loop with INSERT OR IGNORE is fine for
    // a one-time init path; performance is not critical.
    let mut tx = pool.begin().await?;
    for shard_id in 0..s {
        sqlx::query(
            "INSERT OR IGNORE INTO shards (shard_id, tablet_id, updated_at_ms) VALUES (?, NULL, ?)",
        )
        .bind(shard_id)
        .bind(now_ms)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Override the assignment for one shard. Pass `None` to revert to
/// default (hash) routing.
pub async fn set_assignment(
    pool: &SqlitePool,
    shard_id: i64,
    tablet_id: Option<i64>,
    now_ms: i64,
) -> MetaResult<()> {
    sqlx::query("UPDATE shards SET tablet_id = ?, updated_at_ms = ? WHERE shard_id = ?")
        .bind(tablet_id)
        .bind(now_ms)
        .bind(shard_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn get_assignment(pool: &SqlitePool, shard_id: i64) -> MetaResult<Option<i64>> {
    let row: Option<(Option<i64>,)> =
        sqlx::query_as("SELECT tablet_id FROM shards WHERE shard_id = ?")
            .bind(shard_id)
            .fetch_optional(pool)
            .await?;
    Ok(row.and_then(|(t,)| t))
}

/// Fetch every shard row, ordered by `shard_id`. Used by
/// [`crate::LocationProxy`]'s periodic refresh to rebuild its local
/// cache in one round-trip. At prototype scale (16384 rows) this is
/// cheap; production-scale sync will need incremental LISTEN/NOTIFY
/// (Postgres-only, follow-up PR).
pub async fn list_all(pool: &SqlitePool) -> MetaResult<Vec<ShardRow>> {
    let rows: Vec<(i64, Option<i64>, i64)> =
        sqlx::query_as("SELECT shard_id, tablet_id, updated_at_ms FROM shards ORDER BY shard_id")
            .fetch_all(pool)
            .await?;
    Ok(rows
        .into_iter()
        .map(|(shard_id, tablet_id, updated_at_ms)| ShardRow {
            shard_id,
            tablet_id,
            updated_at_ms,
        })
        .collect())
}

/// Every shard that carries an explicit tablet override, ordered
/// by shard_id. Used by `GET /api/admin/shards/list` — the full
/// 16384-row listing is too noisy for humans; operators almost
/// always want "show me what's actually overridden."
pub async fn list_overrides(pool: &SqlitePool) -> MetaResult<Vec<ShardRow>> {
    let rows: Vec<(i64, Option<i64>, i64)> = sqlx::query_as(
        "SELECT shard_id, tablet_id, updated_at_ms FROM shards \
         WHERE tablet_id IS NOT NULL ORDER BY shard_id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(shard_id, tablet_id, updated_at_ms)| ShardRow {
            shard_id,
            tablet_id,
            updated_at_ms,
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
    async fn seed_creates_s_rows() {
        let p = pool().await;
        seed_shards(&p, 16, 100).await.unwrap();
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM shards")
            .fetch_one(&p)
            .await
            .unwrap();
        assert_eq!(count, 16);
    }

    #[tokio::test]
    async fn seed_is_idempotent() {
        let p = pool().await;
        seed_shards(&p, 16, 100).await.unwrap();
        seed_shards(&p, 16, 200).await.unwrap();
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM shards")
            .fetch_one(&p)
            .await
            .unwrap();
        assert_eq!(count, 16);
    }

    #[tokio::test]
    async fn set_and_get_assignment() {
        let p = pool().await;
        seed_shards(&p, 16, 100).await.unwrap();
        assert_eq!(get_assignment(&p, 3).await.unwrap(), None);
        set_assignment(&p, 3, Some(42), 200).await.unwrap();
        assert_eq!(get_assignment(&p, 3).await.unwrap(), Some(42));
        // Revert.
        set_assignment(&p, 3, None, 300).await.unwrap();
        assert_eq!(get_assignment(&p, 3).await.unwrap(), None);
    }

    #[tokio::test]
    async fn list_all_returns_every_row_ordered_by_shard_id() {
        let p = pool().await;
        seed_shards(&p, 16, 100).await.unwrap();
        set_assignment(&p, 5, Some(55), 200).await.unwrap();
        set_assignment(&p, 3, Some(33), 200).await.unwrap();
        let rows = list_all(&p).await.unwrap();
        assert_eq!(rows.len(), 16);
        // Sorted by shard_id.
        for i in 0..15 {
            assert!(rows[i].shard_id < rows[i + 1].shard_id);
        }
        // Overrides visible; unassigned shards are None.
        assert_eq!(rows[3].tablet_id, Some(33));
        assert_eq!(rows[5].tablet_id, Some(55));
        assert_eq!(rows[0].tablet_id, None);
    }

    #[tokio::test]
    async fn list_overrides_returns_only_set_rows() {
        let p = pool().await;
        seed_shards(&p, 16, 100).await.unwrap();
        set_assignment(&p, 5, Some(55), 200).await.unwrap();
        set_assignment(&p, 3, Some(33), 200).await.unwrap();
        set_assignment(&p, 11, Some(11), 200).await.unwrap();
        let rows = list_overrides(&p).await.unwrap();
        let pairs: Vec<_> = rows.iter().map(|r| (r.shard_id, r.tablet_id)).collect();
        assert_eq!(pairs, vec![(3, Some(33)), (5, Some(55)), (11, Some(11))]);
    }

    #[tokio::test]
    async fn list_overrides_is_empty_on_fresh_table() {
        let p = pool().await;
        seed_shards(&p, 16, 100).await.unwrap();
        assert!(list_overrides(&p).await.unwrap().is_empty());
    }
}
