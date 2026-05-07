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
}
