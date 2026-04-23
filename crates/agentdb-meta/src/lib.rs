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
pub mod config;
pub mod error;
pub mod migrations;
pub mod nodes;
pub mod scheduler;
pub mod scram;
pub mod session_overrides;
pub mod shards;
pub mod tablets;
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
}
