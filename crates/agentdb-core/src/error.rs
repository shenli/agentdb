use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(#[from] agentdb_storage::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("{0}")]
    Invalid(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_error_converts_via_from() {
        let storage_err = agentdb_storage::Error::Other("boom".into());
        let err: Error = storage_err.into();
        assert!(matches!(err, Error::Storage(_)));
    }

    #[test]
    fn not_found_formats_message() {
        let err = Error::NotFound("session s-9".into());
        assert_eq!(err.to_string(), "not found: session s-9");
    }
}
