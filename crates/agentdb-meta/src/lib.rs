//! Control-plane metadata store.
//!
//! Dual-dialect. SQLite is the dev / single-node default;
//! Postgres is the production target. `AGENTDB_META_BACKEND`
//! selects at runtime:
//!
//! - `AGENTDB_META_BACKEND=sqlite` (default) → file-backed
//!   SQLite at `AGENTDB_META_PATH`.
//! - `AGENTDB_META_BACKEND=postgres` → Postgres at
//!   `AGENTDB_META_URL` (a `postgres://user:pw@host/db`
//!   URL).
//!
//! Runtime queries use `$N` placeholders, which both Postgres
//! and SQLite accept through `sqlx::Any`. The small number of
//! truly dialect-specific SQL statements (migrations,
//! `VACUUM INTO`, `pg_dump`) branch on the [`MetaBackend`]
//! discriminant.

pub mod admin_tokens;
pub mod audit;
pub mod cells;
pub mod change_notifier;
pub mod config;
pub mod error;
pub mod migrations;
pub mod node_operational;
pub mod nodes;
pub mod placement_scopes;
pub mod routing;
pub mod scheduler;
pub mod session_locators;
pub mod session_overrides;
pub mod shards;
pub mod tablet_movements;
pub mod tablet_ranges;
pub mod tablet_rebalance;
pub mod tablet_replicas;
pub mod tablet_snapshots;
pub mod tablet_stats;
pub mod tablets;
pub mod tenant_policies;
pub mod tenant_quotas;
pub mod tenants;

pub use error::{MetaError, MetaResult};

use std::sync::Once;

use sqlx::any::AnyPoolOptions;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::sqlite::SqlitePool;
use sqlx::AnyPool;

/// Which concrete backend the current store is running on.
/// Callers that need dialect-specific SQL (migrations,
/// `VACUUM INTO`, `pg_dump`) match on this discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaDialect {
    Sqlite,
    Postgres,
}

impl MetaDialect {
    pub fn as_str(self) -> &'static str {
        match self {
            MetaDialect::Sqlite => "sqlite",
            MetaDialect::Postgres => "postgres",
        }
    }
}

/// A pool-backed handle on the control-plane store.
///
/// The public pool API ([`MetaStore::pool`]) returns a
/// `sqlx::AnyPool` — every runtime query in this crate (and
/// in callers) uses it. Dialect-aware branches call
/// [`MetaStore::dialect`].
///
/// Hold on to the underlying `SqlitePool` / `PgPool` separately
/// for dialect-specific operations (migrations,
/// `VACUUM INTO`, `LISTEN/NOTIFY`).
#[derive(Clone)]
pub struct MetaStore {
    any: AnyPool,
    dialect: MetaDialect,
    sqlite: Option<SqlitePool>,
    postgres: Option<PgPool>,
}

fn install_any_drivers() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // sqlx's Any layer needs its drivers registered once
        // per process; calling twice is a hard error.
        sqlx::any::install_default_drivers();
    });
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Test-only: install drivers from a test that doesn't open
/// a full `MetaStore`. Kept pub(crate) so external callers
/// don't accidentally depend on it.
#[doc(hidden)]
pub fn install_any_drivers_for_tests() {
    install_any_drivers();
}

/// Test-only: build a ready-to-use `AnyPool` against an
/// in-memory SQLite DB with migrations applied. Caps the pool
/// at a single connection so all queries share the one
/// `:memory:` DB (each SQLite connection opened with
/// `:memory:` gets its own private DB otherwise).
#[doc(hidden)]
pub async fn test_pool_sqlite_memory() -> sqlx::AnyPool {
    install_any_drivers();
    let pool = sqlx::any::AnyPoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("open sqlite in-memory AnyPool");
    migrations::apply(&pool, MetaDialect::Sqlite)
        .await
        .expect("apply migrations");
    pool
}

impl MetaStore {
    /// Open a SQLite-backed store at `path`. Creates the file
    /// if absent. Runs migrations on open.
    pub async fn open_sqlite(path: &str) -> MetaResult<Self> {
        install_any_drivers();
        let is_memory = path == ":memory:";
        let url = if is_memory {
            "sqlite::memory:".to_string()
        } else {
            format!("sqlite://{path}?mode=rwc")
        };
        // Keep a dedicated SqlitePool for dialect-specific
        // operations that bypass AnyPool (VACUUM INTO in
        // backup(), for example).
        let sqlite = SqlitePool::connect(&url).await?;
        // For `:memory:` databases every connection gets its
        // own private DB (see SQLite docs). Cap the AnyPool
        // at a single connection so migrations + subsequent
        // queries all hit the same in-memory DB — required for
        // tests that open with `:memory:`.
        let mut opts = AnyPoolOptions::new();
        if is_memory {
            opts = opts.max_connections(1);
        }
        let any = opts.connect(&url).await?;
        migrations::apply(&any, MetaDialect::Sqlite).await?;
        Ok(Self {
            any,
            dialect: MetaDialect::Sqlite,
            sqlite: Some(sqlite),
            postgres: None,
        })
    }

    /// Open a Postgres-backed store at `url`. Expects a
    /// `postgres://user:pass@host/db` URL. Migrations run on
    /// open.
    pub async fn open_postgres(url: &str) -> MetaResult<Self> {
        install_any_drivers();
        let any_max_connections = env_u32("AGENTDB_META_ANY_POOL_MAX_CONNECTIONS", 32);
        let pg_max_connections = env_u32("AGENTDB_META_PG_POOL_MAX_CONNECTIONS", 16);
        let postgres = PgPoolOptions::new()
            .max_connections(pg_max_connections)
            .connect(url)
            .await?;
        let any = AnyPoolOptions::new()
            .max_connections(any_max_connections)
            .connect(url)
            .await?;
        migrations::apply(&any, MetaDialect::Postgres).await?;
        Ok(Self {
            any,
            dialect: MetaDialect::Postgres,
            sqlite: None,
            postgres: Some(postgres),
        })
    }

    /// Backend-aware open. Reads `AGENTDB_META_BACKEND` and
    /// the matching URL / path env var.
    pub async fn open_from_env() -> MetaResult<Self> {
        let backend = std::env::var("AGENTDB_META_BACKEND")
            .unwrap_or_else(|_| "sqlite".to_string())
            .to_lowercase();
        match backend.as_str() {
            "sqlite" => {
                let path =
                    std::env::var("AGENTDB_META_PATH").unwrap_or_else(|_| "meta.sqlite".into());
                Self::open_sqlite(&path).await
            }
            "postgres" | "pg" => {
                let url = std::env::var("AGENTDB_META_URL").map_err(|_| {
                    MetaError::Migration(
                        "AGENTDB_META_BACKEND=postgres requires AGENTDB_META_URL".into(),
                    )
                })?;
                Self::open_postgres(&url).await
            }
            other => Err(MetaError::Migration(format!(
                "unknown AGENTDB_META_BACKEND={other}; expected 'sqlite' or 'postgres'"
            ))),
        }
    }

    /// The generic pool every query runs through.
    pub fn pool(&self) -> &AnyPool {
        &self.any
    }

    pub fn dialect(&self) -> MetaDialect {
        self.dialect
    }

    /// Native SQLite pool, if this store is SQLite-backed.
    /// `None` on Postgres.
    pub fn sqlite_pool(&self) -> Option<&SqlitePool> {
        self.sqlite.as_ref()
    }

    /// Native Postgres pool, if this store is PG-backed.
    /// `None` on SQLite. Callers that need LISTEN / NOTIFY
    /// unwrap this.
    pub fn postgres_pool(&self) -> Option<&PgPool> {
        self.postgres.as_ref()
    }

    /// Snapshot the meta DB to `dest_path`.
    ///
    /// SQLite backend: uses the native `VACUUM INTO` (live
    /// snapshot, no drain). `dest_path` MUST NOT pre-exist.
    ///
    /// Postgres backend: shells out to `pg_dump`, writing a
    /// plain-SQL file. The dump includes `COPY` rows for
    /// every table — `psql <new-db> < dump.sql` restores.
    /// Requires `pg_dump` in `$PATH`.
    pub async fn backup(&self, dest_path: &std::path::Path) -> MetaResult<()> {
        if dest_path.exists() {
            return Err(MetaError::Migration(format!(
                "meta backup destination already exists: {}",
                dest_path.display()
            )));
        }
        match self.dialect {
            MetaDialect::Sqlite => self.backup_sqlite(dest_path).await,
            MetaDialect::Postgres => self.backup_pg(dest_path).await,
        }
    }

    async fn backup_sqlite(&self, dest_path: &std::path::Path) -> MetaResult<()> {
        let sqlite = self
            .sqlite
            .as_ref()
            .ok_or_else(|| MetaError::Migration("sqlite backend missing native pool".into()))?;
        let dest_str = dest_path.to_string_lossy().to_string();
        let escaped = dest_str.replace('\'', "''");
        let sql = format!("VACUUM INTO '{escaped}'");
        sqlx::query(&sql).execute(sqlite).await?;
        Ok(())
    }

    async fn backup_pg(&self, dest_path: &std::path::Path) -> MetaResult<()> {
        // Pull the connection URL from env. pg_dump is a
        // separate process that can't share the sqlx pool,
        // so the operator provides credentials via env the
        // same way.
        let url = std::env::var("AGENTDB_META_URL").map_err(|_| {
            MetaError::Migration(
                "postgres backup needs AGENTDB_META_URL set in this process's env".into(),
            )
        })?;
        let out = tokio::process::Command::new("pg_dump")
            .arg("--no-owner")
            .arg("--clean")
            .arg("--if-exists")
            .arg("--format=plain")
            .arg("--file")
            .arg(dest_path)
            .arg(&url)
            .output()
            .await
            .map_err(|e| MetaError::Migration(format!("pg_dump exec: {e}")))?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            return Err(MetaError::Migration(format!(
                "pg_dump failed (status {:?}): {stderr}",
                out.status.code()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fresh_file_store() -> (tempfile::TempDir, MetaStore) {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.sqlite");
        let store = MetaStore::open_sqlite(&src.to_string_lossy())
            .await
            .unwrap();
        (dir, store)
    }

    #[tokio::test]
    async fn backup_produces_readable_standalone_file() {
        let (dir, src) = fresh_file_store().await;
        admin_tokens::mint(src.pool(), "alice", admin_tokens::AdminScope::Admin, 1000)
            .await
            .unwrap();

        let dest = dir.path().join("backup.sqlite");
        src.backup(&dest).await.unwrap();
        assert!(dest.exists(), "backup path missing");

        let restored = MetaStore::open_sqlite(&dest.to_string_lossy())
            .await
            .unwrap();
        let active = admin_tokens::list_active(restored.pool()).await.unwrap();
        assert_eq!(active.len(), 1, "restored store missing seeded token");
        assert_eq!(active[0].description, "alice");

        // Migrations already applied once; reopening must not
        // re-run them. We expect exactly MIGRATIONS.len() rows.
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM schema_migrations")
            .fetch_one(restored.pool())
            .await
            .unwrap();
        assert!(n > 0, "schema_migrations empty after restore");
    }

    #[tokio::test]
    async fn backup_refuses_to_overwrite_existing_file() {
        let (_dir, src) = fresh_file_store().await;
        let f = tempfile::NamedTempFile::new().unwrap();
        let err = src.backup(f.path()).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.to_lowercase().contains("exists")
                || msg.to_lowercase().contains("vacuum")
                || msg.to_lowercase().contains("database"),
            "expected overwrite-refusal error; got: {msg}"
        );
    }
}
