use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("rocksdb error: {0}")]
    RocksDb(#[from] rocksdb::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_from_io_error() {
        let io = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
        let err: Error = io.into();
        assert!(matches!(err, Error::Io(_)));
        assert!(err.to_string().contains("io error"));
    }

    #[test]
    fn other_variant_carries_message() {
        let err = Error::Other("nope".into());
        assert_eq!(err.to_string(), "nope");
    }
}
