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
}
