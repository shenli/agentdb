use thiserror::Error;

#[derive(Debug, Error)]
pub enum WireError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("bson decode error: {0}")]
    BsonDecode(#[from] bson::de::Error),
    #[error("bson encode error: {0}")]
    BsonEncode(#[from] bson::ser::Error),
    #[error("core error: {0}")]
    Core(String),
    #[error("protocol error: {0}")]
    Protocol(String),
}

pub type WireResult<T> = Result<T, WireError>;

impl From<agentdb_core::Error> for WireError {
    fn from(value: agentdb_core::Error) -> Self {
        Self::Core(format!("{value:?}"))
    }
}
