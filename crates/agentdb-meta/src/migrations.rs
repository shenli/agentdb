//! Embedded migrations. Hand-rolled because `refinery` pulls another
//! dep tree; for the ~handful of migrations we'll have, the rolled
//! version is simpler and the test surface is tiny.
//!
//! A migration is (version, description, SQL). Applied in version
//! order; each successfully-applied migration records a row in
//! `schema_migrations`. Subsequent opens are no-ops because the
//! version check skips anything already recorded.

use sqlx::sqlite::SqlitePool;

use crate::error::{MetaError, MetaResult};

/// Every migration in order. Append new ones; NEVER renumber or
/// reorder existing entries.
const MIGRATIONS: &[(i64, &str, &str)] = &[
    (
        1,
        "initial control-plane schema",
        include_str!("../migrations/001_initial.sql"),
    ),
    (
        2,
        "scheduler leader singleton lease",
        include_str!("../migrations/002_scheduler_leader.sql"),
    ),
    (
        3,
        "SCRAM-SHA-256 users",
        include_str!("../migrations/003_users.sql"),
    ),
];

/// Apply all pending migrations to the given pool.
pub async fn apply(pool: &SqlitePool) -> MetaResult<()> {
    // Bootstrap: create the tracking table first so we can check what's
    // already applied. It's also created by migration 001 (for fresh
    // DBs that skip this bootstrap), so `IF NOT EXISTS` guards both
    // paths.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version       INTEGER PRIMARY KEY,
            applied_at_ms INTEGER NOT NULL,
            description   TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await?;

    for &(version, desc, sql) in MIGRATIONS {
        let already_applied: Option<(i64,)> =
            sqlx::query_as("SELECT version FROM schema_migrations WHERE version = ?")
                .bind(version)
                .fetch_optional(pool)
                .await?;
        if already_applied.is_some() {
            continue;
        }
        // Execute each statement in the migration SQL separately — SQLite's
        // sqlx doesn't run multi-statement strings by default.
        for stmt in split_sql(sql) {
            if stmt.trim().is_empty() {
                continue;
            }
            sqlx::query(stmt).execute(pool).await.map_err(|e| {
                MetaError::Migration(format!(
                    "version {version} `{desc}` statement failed: {e}; sql={stmt}"
                ))
            })?;
        }
        let now_ms = now_ms();
        sqlx::query(
            "INSERT INTO schema_migrations (version, applied_at_ms, description) VALUES (?, ?, ?)",
        )
        .bind(version)
        .bind(now_ms)
        .bind(desc)
        .execute(pool)
        .await?;
        tracing::info!(%version, %desc, "applied control-plane migration");
    }
    Ok(())
}

/// Split a SQL file into statements on the `;` boundary, respecting
/// line comments (-- to end-of-line). Crude but sufficient for our
/// hand-maintained migration files.
fn split_sql(sql: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut in_comment = false;
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if in_comment {
            if b == b'\n' {
                in_comment = false;
            }
            i += 1;
            continue;
        }
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            in_comment = true;
            i += 2;
            continue;
        }
        if b == b';' {
            out.push(&sql[start..i]);
            start = i + 1;
        }
        i += 1;
    }
    if start < sql.len() {
        out.push(&sql[start..]);
    }
    out
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fresh_pool() -> SqlitePool {
        SqlitePool::connect("sqlite::memory:").await.unwrap()
    }

    #[tokio::test]
    async fn apply_creates_tables_on_fresh_db() {
        let pool = fresh_pool().await;
        apply(&pool).await.unwrap();
        let tables: Vec<(String,)> =
            sqlx::query_as("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                .fetch_all(&pool)
                .await
                .unwrap();
        let names: Vec<&str> = tables.iter().map(|(n,)| n.as_str()).collect();
        for expected in [
            "config",
            "nodes",
            "schema_migrations",
            "scheduler_leader",
            "session_overrides",
            "shards",
            "tablets",
            "tenants",
            "users",
        ] {
            assert!(
                names.contains(&expected),
                "expected table {expected} after apply; got {names:?}"
            );
        }
    }

    #[tokio::test]
    async fn apply_is_idempotent() {
        let pool = fresh_pool().await;
        apply(&pool).await.unwrap();
        apply(&pool).await.unwrap();
        apply(&pool).await.unwrap();
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM schema_migrations")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, MIGRATIONS.len() as i64);
    }

    #[test]
    fn split_sql_respects_comments_and_semicolons() {
        let sql = "-- a comment with ; inside\nCREATE TABLE x (a INT);\n-- another\nCREATE INDEX idx ON x(a);";
        let stmts = split_sql(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("CREATE TABLE"));
        assert!(stmts[1].contains("CREATE INDEX"));
    }
}
