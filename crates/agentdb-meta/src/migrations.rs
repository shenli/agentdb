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

/// Every migration in order. Append new ones; NEVER renumber
/// or reorder existing entries.
const MIGRATIONS: &[(i64, &str, &str, &str)] = &[
    (
        1,
        "initial control-plane schema",
        include_str!("../migrations/001_initial.sql"),
        include_str!("../migrations/001_initial.pg.sql"),
    ),
    (
        2,
        "scheduler leader singleton lease",
        include_str!("../migrations/002_scheduler_leader.sql"),
        include_str!("../migrations/002_scheduler_leader.pg.sql"),
    ),
    (
        3,
        "SCRAM-SHA-256 users",
        include_str!("../migrations/003_users.sql"),
        include_str!("../migrations/003_users.pg.sql"),
    ),
    (
        4,
        "admin bearer tokens",
        include_str!("../migrations/004_admin_tokens.sql"),
        include_str!("../migrations/004_admin_tokens.pg.sql"),
    ),
    (
        5,
        "admin audit log",
        include_str!("../migrations/005_admin_audit.sql"),
        include_str!("../migrations/005_admin_audit.pg.sql"),
    ),
    (
        6,
        "global per-tenant rate limits",
        include_str!("../migrations/006_tenant_quotas.sql"),
        include_str!("../migrations/006_tenant_quotas.pg.sql"),
    ),
    (
        7,
        "admin token scope (RBAC)",
        include_str!("../migrations/007_admin_token_scope.sql"),
        include_str!("../migrations/007_admin_token_scope.pg.sql"),
    ),
    (
        8,
        "legacy node peer address column",
        include_str!("../migrations/008_node_mongo_address.sql"),
        include_str!("../migrations/008_node_mongo_address.pg.sql"),
    ),
    (
        9,
        "Postgres LISTEN/NOTIFY triggers (SQLite no-op)",
        include_str!("../migrations/009_pg_notify_triggers.sql"),
        include_str!("../migrations/009_pg_notify_triggers.pg.sql"),
    ),
    (
        10,
        "tablets.handoff_target_node_id for cooperative handoff",
        include_str!("../migrations/010_tablet_handoff.sql"),
        include_str!("../migrations/010_tablet_handoff.pg.sql"),
    ),
    (
        11,
        "session_locators (M26 Phase 1: session-centric identity)",
        include_str!("../migrations/011_session_locators.sql"),
        include_str!("../migrations/011_session_locators.pg.sql"),
    ),
    (
        12,
        "session_locators.tenant_external_id (M26 Phase 1b: skip the internal-id round-trip)",
        include_str!("../migrations/012_session_locator_external_tenant.sql"),
        include_str!("../migrations/012_session_locator_external_tenant.pg.sql"),
    ),
    (
        13,
        "nodes.availability_zone (cross-AZ failure-domain awareness)",
        include_str!("../migrations/013_nodes_availability_zone.sql"),
        include_str!("../migrations/013_nodes_availability_zone.pg.sql"),
    ),
    (
        14,
        "nodes.region (durable residency routing metadata)",
        include_str!("../migrations/014_nodes_region.sql"),
        include_str!("../migrations/014_nodes_region.pg.sql"),
    ),
    (
        15,
        "session_locators LISTEN/NOTIFY trigger",
        include_str!("../migrations/015_session_locator_notify.sql"),
        include_str!("../migrations/015_session_locator_notify.pg.sql"),
    ),
];

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
        let already_applied: Option<(i64,)> =
            sqlx::query_as("SELECT version FROM schema_migrations WHERE version = ?")
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
        sqlx::query(
            "INSERT INTO schema_migrations (version, applied_at_ms, description) VALUES (?, ?, ?)",
        )
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
            "session_locators",
            "scheduler_leader",
            "session_overrides",
            "shards",
            "tablets",
            "tenants",
            "users",
            "admin_tokens",
            "admin_audit",
            "tenant_quotas",
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
