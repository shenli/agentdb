use serde::Serialize;

#[derive(Debug, Copy, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    TenantNotFound,
    AgentNotFound,
    SessionNotFound,
    ArtifactNotFound,
    RecordNotFound,
    SkillNotFound,
    PolicyNotFound,
    EventTooLarge,
    ArtifactTooLarge,
    HashMismatch,
    IdempotencyConflict,
    PolicyDenied,
    InvalidCursor,
    RateLimited,
    Internal,
}

impl ErrorCode {
    pub fn retryable(&self) -> bool {
        matches!(self, ErrorCode::RateLimited | ErrorCode::Internal)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct V2Error {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
}

impl V2Error {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: code.retryable(),
            suggestion: None,
        }
    }

    pub fn with_suggestion(mut self, s: impl Into<String>) -> Self {
        self.suggestion = Some(s.into());
        self
    }
}
