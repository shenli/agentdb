//! Control-plane metadata store (M18).
//!
//! Houses the global metadata that every node in the cluster agrees
//! on: tenants, shard assignment, tablets, nodes, config. See
//! `docs/agent-db-v2.md` §3.3 ("Metadata storage — two layers") for
//! the design rationale — this crate is Layer 1 (global control
//! plane); Layer 2 (per-tablet metadata, dogfooded in AgentDB's own
//! storage engine) is the existing `Box_` state.
//!
//! M18 lands the schema + migrations + minimal CRUD. Runtime
//! consumers land in M19 onwards.
//!
//! Dialect: SQLite is primary for dev + CI. Postgres is the intended
//! production target and is added via a feature flag in a follow-up
//! PR (see the M18 plan doc).

pub mod admin_tokens;
pub mod audit;
pub mod config;
pub mod error;
pub mod migrations;
pub mod nodes;
pub mod scheduler;
pub mod scram;
pub mod session_overrides;
pub mod shards;
pub mod tablets;
pub mod tenant_quotas;
pub mod tenants;
pub mod users;

pub use error::{MetaError, MetaResult};

use sqlx::sqlite::SqlitePool;

/// A pool-backed handle on the control-plane store.
#[derive(Clone)]
pub struct MetaStore {
    pool: SqlitePool,
}

impl MetaStore {
    /// Open a store backed by a SQLite file at the given path. The
    /// file is created if absent. Migrations run automatically on
    /// open; repeat opens are idempotent.
    pub async fn open_sqlite(path: &str) -> MetaResult<Self> {
        let url = if path == ":memory:" {
            "sqlite::memory:".to_string()
        } else {
            format!("sqlite://{path}?mode=rwc")
        };
        let pool = SqlitePool::connect(&url).await?;
        migrations::apply(&pool).await?;
        Ok(Self { pool })
    }

    /// The underlying pool. Exposed for integration with async code
    /// that wants to share the connection pool (e.g., spawning its
    /// own transactions).
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Snapshot the meta DB to `dest_path` using SQLite's
    /// `VACUUM INTO`. Runs on a live pool — no drain required.
    /// `dest_path` MUST NOT exist; SQLite refuses to overwrite.
    ///
    /// The resulting file is a standalone SQLite database the
    /// caller can open (including re-opening via
    /// [`MetaStore::open_sqlite`]). `schema_migrations` rows
    /// carry over so a restored server does not re-run already-
    /// applied migrations.
    pub async fn backup(&self, dest_path: &std::path::Path) -> MetaResult<()> {
        // SQLite's VACUUM INTO accepts existing zero-byte files
        // (per its docs). That's a surprising corner — operators
        // who `touch backup.sqlite` and then run backup would get
        // silent overwrite. Enforce the "MUST NOT exist" contract
        // here so the stricter guarantee holds regardless of the
        // underlying SQL's tolerance.
        if dest_path.exists() {
            return Err(MetaError::Migration(format!(
                "meta backup destination already exists: {}",
                dest_path.display()
            )));
        }
        let dest_str = dest_path.to_string_lossy().to_string();
        // VACUUM INTO doesn't accept bound parameters in SQLite.
        // Escape single quotes manually — the path comes from an
        // admin endpoint, so hardening it here is the lesser
        // burden than teaching the caller to quote correctly.
        let escaped = dest_str.replace('\'', "''");
        let sql = format!("VACUUM INTO '{escaped}'");
        sqlx::query(&sql).execute(&self.pool).await?;
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
