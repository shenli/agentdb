use thiserror::Error;

#[derive(Debug, Error)]
pub enum WireError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("bson error: {0}")]
    Bson(#[from] bson::de::Error),
    #[error("bson ser error: {0}")]
    BsonSer(#[from] bson::ser::Error),
    #[error("core error: {0}")]
    Core(#[from] agentdb_core::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
}

pub type WireResult<T> = std::result::Result<T, WireError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_error_converts() {
        let io = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof");
        let err: WireError = io.into();
        assert!(matches!(err, WireError::Io(_)));
    }

    #[test]
    fn protocol_error_formats() {
        let err = WireError::Protocol("bad opcode".into());
        assert_eq!(err.to_string(), "protocol error: bad opcode");
    }
}
