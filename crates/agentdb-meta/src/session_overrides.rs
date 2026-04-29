//! Session-level routing overrides (rare). Empty in the common case;
//! entries added only when a specific session is migrated away from
//! its hash-computed tablet (hot-session escape hatch per §2.5).

use sqlx::AnyPool;

use crate::error::MetaResult;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOverride {
    pub session_id: String,
    pub tablet_id: i64,
    pub created_at_ms: i64,
}

pub async fn get(pool: &AnyPool, session_id: &str) -> MetaResult<Option<SessionOverride>> {
    let row: Option<(String, i64, i64)> = sqlx::query_as(
        "SELECT session_id, tablet_id, created_at_ms FROM session_overrides WHERE session_id = ?",
    )
    .bind(session_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(s, t, c)| SessionOverride {
        session_id: s,
        tablet_id: t,
        created_at_ms: c,
    }))
}

pub async fn set(pool: &AnyPool, session_id: &str, tablet_id: i64, now_ms: i64) -> MetaResult<()> {
    sqlx::query(
        "INSERT INTO session_overrides (session_id, tablet_id, created_at_ms) VALUES (?, ?, ?)
         ON CONFLICT (session_id) DO UPDATE SET tablet_id = excluded.tablet_id",
    )
    .bind(session_id)
    .bind(tablet_id)
    .bind(now_ms)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete(pool: &AnyPool, session_id: &str) -> MetaResult<()> {
    sqlx::query("DELETE FROM session_overrides WHERE session_id = ?")
        .bind(session_id)
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
    async fn set_get_delete_roundtrip() {
        let p = pool().await;
        assert_eq!(get(&p, "sess-1").await.unwrap(), None);
        set(&p, "sess-1", 42, 100).await.unwrap();
        let o = get(&p, "sess-1").await.unwrap().unwrap();
        assert_eq!(o.tablet_id, 42);
        set(&p, "sess-1", 43, 200).await.unwrap();
        assert_eq!(get(&p, "sess-1").await.unwrap().unwrap().tablet_id, 43);
        delete(&p, "sess-1").await.unwrap();
        assert_eq!(get(&p, "sess-1").await.unwrap(), None);
    }
}
