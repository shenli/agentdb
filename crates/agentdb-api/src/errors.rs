//! HTTP error mapping for the runtime's `Error`.
//!
//! Routes return `Result<T, ApiError>`; `ApiError` wraps an `agentdb_core::Error`
//! and implements `IntoResponse` so the framework can serialize the wire body
//! and pick the right status code without each handler repeating the mapping.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use agentdb_core::{Error, ErrorCode};

/// Wrapper that lets axum convert an `agentdb_core::Error` into an HTTP response.
///
/// The body is the Error JSON serialization (already includes `code`,
/// `message`, `retryable`, optional `suggestion`). The status code is chosen
/// from the `code` per the V2.0 contract.
pub struct ApiError(pub Error);

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = status_for(self.0.code);
        (status, Json(self.0)).into_response()
    }
}

fn status_for(code: ErrorCode) -> StatusCode {
    use ErrorCode::*;
    match code {
        TenantNotFound | AgentNotFound | SessionNotFound | ArtifactNotFound | RecordNotFound
        | SkillNotFound | PolicyNotFound => StatusCode::NOT_FOUND,
        EventTooLarge | ArtifactTooLarge | HashMismatch | PolicyDenied | InvalidCursor => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        IdempotencyConflict | AliasConflict | VersionConflict => StatusCode::CONFLICT,
        RateLimited => StatusCode::TOO_MANY_REQUESTS,
        NodeDraining | KeyUnavailable | RaftNotLeader => StatusCode::SERVICE_UNAVAILABLE,
        Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Helper: parse a path-param string into an `Id` of the expected kind, or
/// return the right NotFound code as an `ApiError`. This is the only spot in
/// the HTTP layer that turns parse failures into structured 404s; downstream
/// runtime calls then rely on the parsed `Id` having the right kind.
pub fn parse_id_or(
    s: &str,
    kind: agentdb_core::IdKind,
    not_found_code: ErrorCode,
) -> Result<agentdb_core::Id, ApiError> {
    agentdb_core::Id::parse_kind(s, kind)
        .map_err(|e| ApiError(Error::new(not_found_code, e.to_string())))
}
