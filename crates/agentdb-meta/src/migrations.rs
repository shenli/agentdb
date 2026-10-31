//! Embedded migrations — dual-dialect.
//!
//! Each migration ships TWO SQL variants side-by-side: a
//! SQLite file (`NNN_name.sql`) and a Postgres file
//! (`NNN_name.pg.sql`). The runner picks the file that
//! matches [`crate::MetaDialect`] at `apply` time.
//!
//! A migration is (version, description, sqlite_sql,
//! postgres_sql). Applied in version order; each successfully-
//! applied migration records a row in `schema_migrations`.
//! Subsequent opens are no-ops because the version check
//! skips anything already recorded.

use sqlx::AnyPool;

use crate::error::{MetaError, MetaResult};
use crate::MetaDialect;

/// The control-plane schema as a single consolidated migration.
///
/// There are no deployed clusters to migrate, so the historical 001-031
/// incremental migrations were collapsed into one `001_initial` schema
/// (derived by applying them to a fresh DB and dumping the result). If the
/// schema needs to change before the first real deployment, edit
/// `001_initial.{sql,pg.sql}` in place rather than adding a new numbered
/// migration. Once there ARE clusters to preserve, resume appending new
/// entries here (NEVER renumbering the consolidated `1`).
const MIGRATIONS: &[(i64, &str, &str, &str)] = &[(
    1,
    "initial control-plane schema",
    include_str!("../migrations/001_initial.sql"),
    include_str!("../migrations/001_initial.pg.sql"),
)];

/// Apply all pending migrations to `pool`. `dialect` tells the
/// runner which SQL variant to embed.
pub async fn apply(pool: &AnyPool, dialect: MetaDialect) -> MetaResult<()> {
    // Bootstrap: create the tracking table first so we can
    // check what's already applied. Both dialects recreate it
    // in migration 001 too; the `IF NOT EXISTS` guards that.
    let bootstrap_sql = match dialect {
        MetaDialect::Sqlite => {
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version       INTEGER PRIMARY KEY,
                applied_at_ms INTEGER NOT NULL,
                description   TEXT NOT NULL
            )"
        }
        MetaDialect::Postgres => {
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version       BIGINT PRIMARY KEY,
                applied_at_ms BIGINT NOT NULL,
                description   TEXT NOT NULL
            )"
        }
    };
    sqlx::query(bootstrap_sql).execute(pool).await?;

    for &(version, desc, sqlite_sql, pg_sql) in MIGRATIONS {
        let version_sql = match dialect {
            MetaDialect::Sqlite => "SELECT version FROM schema_migrations WHERE version = ?",
            MetaDialect::Postgres => "SELECT version FROM schema_migrations WHERE version = $1",
        };
        let already_applied: Option<(i64,)> = sqlx::query_as(version_sql)
            .bind(version)
            .fetch_optional(pool)
            .await?;
        if already_applied.is_some() {
            continue;
        }
        let sql = match dialect {
            MetaDialect::Sqlite => sqlite_sql,
            MetaDialect::Postgres => pg_sql,
        };
        // Postgres' JDBC-style driver can handle
        // multi-statement text directly, but sqlx-any routes
        // through per-driver prepared statements that only
        // accept one statement at a time. Split here on
        // top-level `;` (respecting -- comments + $$-delimited
        // plpgsql bodies so the trigger-function block in
        // migration 009 stays intact).
        for stmt in split_sql(sql) {
            let trimmed = stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            sqlx::query(stmt).execute(pool).await.map_err(|e| {
                MetaError::Migration(format!(
                    "version {version} `{desc}` statement failed: {e}; sql={stmt}"
                ))
            })?;
        }
        let now_ms = now_ms();
        let insert_sql = match dialect {
            MetaDialect::Sqlite => {
                "INSERT INTO schema_migrations (version, applied_at_ms, description) VALUES (?, ?, ?)"
            }
            MetaDialect::Postgres => {
                "INSERT INTO schema_migrations (version, applied_at_ms, description) VALUES ($1, $2, $3)"
            }
        };
        sqlx::query(insert_sql)
            .bind(version)
            .bind(now_ms)
            .bind(desc)
            .execute(pool)
            .await?;
        tracing::info!(%version, %desc, dialect = dialect.as_str(), "applied control-plane migration");
    }
    Ok(())
}

/// Split a SQL file on top-level `;`, respecting `--`-to-EOL
/// comments AND `$$...$$` dollar-quoted string literals (the
/// plpgsql bodies in migration 009). Crude but sufficient for
/// our hand-written migration files — we don't parse SQL.
fn split_sql(sql: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut in_line_comment = false;
    let mut in_dollar = false;
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if in_line_comment {
            if b == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }
        if in_dollar {
            // Close on the next `$$`.
            if b == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'$' {
                in_dollar = false;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            in_line_comment = true;
            i += 2;
            continue;
        }
        if b == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'$' {
            in_dollar = true;
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
    use sqlx::any::AnyPoolOptions;

    async fn fresh_pool() -> AnyPool {
        // Cap at 1 conn so apply() + subsequent queries share
        // the same in-memory DB (each SQLite `:memory:` conn
        // gets its own private database otherwise).
        crate::install_any_drivers_for_tests();
        AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn apply_creates_tables_on_fresh_db() {
        let pool = fresh_pool().await;
        apply(&pool, MetaDialect::Sqlite).await.unwrap();
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
            "shards",
            "store_incarnations",
            "tablet_movements",
            "tablets",
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
        apply(&pool, MetaDialect::Sqlite).await.unwrap();
        apply(&pool, MetaDialect::Sqlite).await.unwrap();
        apply(&pool, MetaDialect::Sqlite).await.unwrap();
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

    #[test]
    fn split_sql_respects_dollar_quoted_blocks() {
        // plpgsql body carries `;`s inside `$$...$$`. The
        // splitter must keep those intact.
        let sql = "CREATE FUNCTION f() RETURNS TRIGGER AS $$\nBEGIN\n  PERFORM pg_notify('c', 'x');\n  RETURN NULL;\nEND;\n$$ LANGUAGE plpgsql;\nSELECT 1;";
        let stmts = split_sql(sql);
        assert_eq!(stmts.len(), 2, "got: {stmts:?}");
        assert!(stmts[0].contains("CREATE FUNCTION"));
        assert!(stmts[1].trim().starts_with("SELECT 1"));
    }
}
