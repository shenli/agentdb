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
        // Machine-readable backoff: agents' stock retry loops key off
        // Retry-After, so every shed/unavailable response carries one.
        // 429 = quota pressure drains quickly; 503 = drain/election-scale
        // conditions deserve a longer pause.
        let retry_after_secs = match status {
            StatusCode::TOO_MANY_REQUESTS => Some("1"),
            StatusCode::SERVICE_UNAVAILABLE => Some("5"),
            _ => None,
        };
        let mut response = (status, Json(self.0)).into_response();
        if let Some(secs) = retry_after_secs {
            response.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static(secs),
            );
        }
        response
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

/// Response-mapping middleware: any client-error response under `/api/` that
/// is not already JSON (axum extractor rejections — malformed bodies, wrong
/// content type, unknown routes, oversized payloads — are plain text) gets
/// wrapped in the standard error envelope. Agents parse errors as JSON; a
/// text/plain 4xx breaks every SDK error path. Pinned by the
/// agent-experience eval suite against the LLMS.md contract.
pub async fn envelope_plain_client_errors(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let is_api = request.uri().path().starts_with("/api/");
    let response = next.run(request).await;
    if !is_api || !response.status().is_client_error() {
        return response;
    }
    let already_json = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|ct| ct.starts_with("application/json"));
    if already_json {
        return response;
    }
    let (parts, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, 64 * 1024)
        .await
        .unwrap_or_default();
    let mut message = String::from_utf8_lossy(&bytes).trim().to_string();
    if message.is_empty() {
        message = parts
            .status
            .canonical_reason()
            .unwrap_or("bad request")
            .to_string();
    }
    let code = match parts.status {
        StatusCode::NOT_FOUND => "NOT_FOUND",
        StatusCode::METHOD_NOT_ALLOWED => "METHOD_NOT_ALLOWED",
        StatusCode::PAYLOAD_TOO_LARGE => "PAYLOAD_TOO_LARGE",
        StatusCode::UNSUPPORTED_MEDIA_TYPE => "UNSUPPORTED_MEDIA_TYPE",
        StatusCode::UNPROCESSABLE_ENTITY => "UNPROCESSABLE_ENTITY",
        _ => "BAD_REQUEST",
    };
    let mut wrapped = (
        parts.status,
        Json(serde_json::json!({
            "code": code,
            "message": message,
            "retryable": false
        })),
    )
        .into_response();
    // Preserve auth challenges and similar contract headers if present.
    for header in [
        axum::http::header::WWW_AUTHENTICATE,
        axum::http::header::RETRY_AFTER,
    ] {
        if let Some(value) = parts.headers.get(&header) {
            wrapped.headers_mut().insert(header.clone(), value.clone());
        }
    }
    wrapped
}
