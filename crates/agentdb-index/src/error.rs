use thiserror::Error;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("{0}")]
    Invalid(String),
}

pub type IndexResult<T> = std::result::Result<T, IndexError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_formats() {
        let e = IndexError::Invalid("bad path".into());
        assert_eq!(e.to_string(), "bad path");
    }
}
