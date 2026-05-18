use thiserror::Error;

pub type MetaResult<T> = Result<T, MetaError>;

#[derive(Debug, Error)]
pub enum MetaError {
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("migration: {0}")]
    Migration(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// Data on disk failed an integrity check — e.g. a base64
    /// column didn't decode, or a stored hash was the wrong
    /// length. Should never happen absent DB tampering.
    #[error("invariant violation: {0}")]
    Invariant(String),

    /// A meta-layer query exceeded its caller-supplied timeout.
    /// Distinguishes "the meta layer is contended and we should
    /// retry on the next tick" from "the query returned an actual
    /// error". Used by the scheduler's renewal path to bound the
    /// worst-case lease-refresh wall-clock so a sustained Postgres
    /// stall does not expire tablet ownership leases under load.
    #[error("meta query timed out: {0}")]
    Timeout(String),
}
