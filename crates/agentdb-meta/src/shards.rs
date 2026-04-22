//! Shard→tablet assignment. 16384 rows, seeded on cluster init.
//! `tablet_id = NULL` means "use hash-based default routing"
//! — the common case until a manual override or tablet split.

use sqlx::sqlite::SqlitePool;

use crate::error::MetaResult;

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
    let row: Option<(Option<i64>,)> = sqlx::query_as("SELECT tablet_id FROM shards WHERE shard_id = ?")
        .bind(shard_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.and_then(|(t,)| t))
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
}
