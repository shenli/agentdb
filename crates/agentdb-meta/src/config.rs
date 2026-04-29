//! Cluster-wide K-V config. Small — expected O(10) keys at most.

use sqlx::AnyPool;

use crate::error::MetaResult;

pub async fn get(pool: &AnyPool, key: &str) -> MetaResult<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as("SELECT value FROM config WHERE key = ?")
        .bind(key)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|(v,)| v))
}

pub async fn set(pool: &AnyPool, key: &str, value: &str, now_ms: i64) -> MetaResult<()> {
    sqlx::query(
        "INSERT INTO config (key, value, updated_at_ms) VALUES (?, ?, ?)
         ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at_ms = excluded.updated_at_ms",
    )
    .bind(key)
    .bind(value)
    .bind(now_ms)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete(pool: &AnyPool, key: &str) -> MetaResult<()> {
    sqlx::query("DELETE FROM config WHERE key = ?")
        .bind(key)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> AnyPool {
        crate::test_pool_sqlite_memory().await
    }

    #[tokio::test]
    async fn set_get_roundtrip() {
        let p = pool().await;
        set(&p, "shard_count", "16384", 100).await.unwrap();
        assert_eq!(
            get(&p, "shard_count").await.unwrap().as_deref(),
            Some("16384")
        );
    }

    #[tokio::test]
    async fn set_overwrites() {
        let p = pool().await;
        set(&p, "k", "v1", 100).await.unwrap();
        set(&p, "k", "v2", 200).await.unwrap();
        assert_eq!(get(&p, "k").await.unwrap().as_deref(), Some("v2"));
    }

    #[tokio::test]
    async fn unknown_returns_none() {
        let p = pool().await;
        assert_eq!(get(&p, "nothing").await.unwrap(), None);
    }

    #[tokio::test]
    async fn delete_removes_row() {
        let p = pool().await;
        set(&p, "k", "v", 100).await.unwrap();
        delete(&p, "k").await.unwrap();
        assert_eq!(get(&p, "k").await.unwrap(), None);
    }
}
