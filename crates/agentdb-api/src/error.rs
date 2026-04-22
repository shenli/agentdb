use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error(transparent)]
    Core(#[from] agentdb_core::Error),
}

/// The JSON body we emit for every REST error. Always carries
/// `code`, `type`, `message`; typed extras per code are flat fields.
#[derive(Serialize, ToSchema)]
pub struct ErrorResponse {
    pub code: ErrorCode,
    #[serde(rename = "type")]
    pub kind: ErrorKind,
    pub message: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub box_id: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fault_kind: Option<String>,
    /// Absolute URL to the runbook for this error code. Present when
    /// the code has a published runbook; omitted otherwise. Operators
    /// should be able to paste this URL into a browser, Slack, or an
    /// incident ticket and land on concrete diagnose/remediate steps.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc_url: Option<String>,
}

/// Stable machine-readable error codes.
#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    NotFound,
    InvalidRequest,
    QuotaExceeded,
    FaultInjected,
    StorageError,
    SerializationError,
    /// M21: the box's lease has expired. Callers should retry (the
    /// scheduler may have reassigned the tablet to a new owner) or
    /// fail over. Returned as HTTP 503 — same shape as FaultInjected
    /// because from the client's perspective it's "this node can't
    /// serve this tablet right now."
    LeaseLost,
}

#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    InvalidRequestError,
    RateLimitError,
    ApiError,
}

impl ApiError {
    fn to_response(&self) -> (StatusCode, HeaderMap, ErrorResponse) {
        let ApiError::Core(err) = self;
        let message = err.to_string();
        let (status, headers, mut body) = match err {
            agentdb_core::Error::NotFound { resource, id } => (
                StatusCode::NOT_FOUND,
                HeaderMap::new(),
                ErrorResponse {
                    code: ErrorCode::NotFound,
                    kind: ErrorKind::InvalidRequestError,
                    message,
                    resource: Some(resource.as_str().into()),
                    id: Some(id.clone()),
                    field: None,
                    scope: None,
                    retry_after_ms: None,
                    box_id: None,
                    fault_kind: None,
                    doc_url: None,
                },
            ),
            agentdb_core::Error::Invalid { field, .. } => (
                StatusCode::BAD_REQUEST,
                HeaderMap::new(),
                ErrorResponse {
                    code: ErrorCode::InvalidRequest,
                    kind: ErrorKind::InvalidRequestError,
                    message,
                    resource: None,
                    id: None,
                    field: field.clone(),
                    scope: None,
                    retry_after_ms: None,
                    box_id: None,
                    fault_kind: None,
                    doc_url: None,
                },
            ),
            agentdb_core::Error::TooManyRequests {
                scope,
                retry_after_ms,
            } => {
                let mut h = HeaderMap::new();
                h.insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    h,
                    ErrorResponse {
                        code: ErrorCode::QuotaExceeded,
                        kind: ErrorKind::RateLimitError,
                        message,
                        resource: None,
                        id: None,
                        field: None,
                        scope: Some(scope.as_str().into()),
                        retry_after_ms: Some(*retry_after_ms),
                        box_id: None,
                        fault_kind: None,
                        doc_url: None,
                    },
                )
            }
            agentdb_core::Error::FaultInjected { box_id, fault_kind } => (
                StatusCode::SERVICE_UNAVAILABLE,
                HeaderMap::new(),
                ErrorResponse {
                    code: ErrorCode::FaultInjected,
                    kind: ErrorKind::ApiError,
                    message,
                    resource: None,
                    id: None,
                    field: None,
                    scope: None,
                    retry_after_ms: None,
                    box_id: Some(*box_id),
                    fault_kind: Some(fault_kind_name(fault_kind).into()),
                    doc_url: None,
                },
            ),
            agentdb_core::Error::LeaseLost { box_id, epoch: _ } => (
                StatusCode::SERVICE_UNAVAILABLE,
                HeaderMap::new(),
                ErrorResponse {
                    code: ErrorCode::LeaseLost,
                    kind: ErrorKind::ApiError,
                    message,
                    resource: None,
                    id: None,
                    field: None,
                    scope: None,
                    retry_after_ms: None,
                    box_id: Some(*box_id),
                    fault_kind: None,
                    doc_url: None,
                },
            ),
            agentdb_core::Error::Storage(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
                ErrorResponse {
                    code: ErrorCode::StorageError,
                    kind: ErrorKind::ApiError,
                    message,
                    resource: None,
                    id: None,
                    field: None,
                    scope: None,
                    retry_after_ms: None,
                    box_id: None,
                    fault_kind: None,
                    doc_url: None,
                },
            ),
            agentdb_core::Error::Serde(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
                ErrorResponse {
                    code: ErrorCode::SerializationError,
                    kind: ErrorKind::ApiError,
                    message,
                    resource: None,
                    id: None,
                    field: None,
                    scope: None,
                    retry_after_ms: None,
                    box_id: None,
                    fault_kind: None,
                    doc_url: None,
                },
            ),
        };
        body.doc_url = doc_url_for(body.code).map(String::from);
        (status, headers, body)
    }
}

fn fault_kind_name(k: &agentdb_core::FaultKind) -> &'static str {
    match k {
        agentdb_core::FaultKind::Latency { .. } => "latency",
        agentdb_core::FaultKind::Error => "error",
    }
}

/// Runbook URL for each error code, or `None` when no runbook is
/// published yet. The URL points to the repo's `docs/runbooks/`
/// directory on GitHub so operators can open it from logs, Slack, or
/// an incident ticket without pulling a local checkout.
///
/// Adding a new runbook: commit the markdown under `docs/runbooks/`
/// and return `Some(...)` here for the corresponding code.
pub fn doc_url_for(code: ErrorCode) -> Option<&'static str> {
    match code {
        ErrorCode::LeaseLost => {
            Some("https://github.com/shenli/agentdb/blob/main/docs/runbooks/lease_lost.md")
        }
        ErrorCode::FaultInjected => {
            Some("https://github.com/shenli/agentdb/blob/main/docs/runbooks/fault_injected.md")
        }
        ErrorCode::QuotaExceeded => {
            Some("https://github.com/shenli/agentdb/blob/main/docs/runbooks/quota_exceeded.md")
        }
        ErrorCode::NotFound
        | ErrorCode::InvalidRequest
        | ErrorCode::StorageError
        | ErrorCode::SerializationError => None,
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, mut headers, body) = self.to_response();
        let json = axum::Json(body);
        let mut resp = (status, json).into_response();
        for (k, v) in headers.drain() {
            if let Some(k) = k {
                resp.headers_mut().insert(k, v);
            }
        }
        resp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doc_url_covers_published_runbooks() {
        // Every code that claims a runbook must point at the right
        // filename. Opening these URLs in a browser is part of the
        // operator's diagnose step — we lock them down here.
        assert_eq!(
            doc_url_for(ErrorCode::LeaseLost),
            Some("https://github.com/shenli/agentdb/blob/main/docs/runbooks/lease_lost.md")
        );
        assert_eq!(
            doc_url_for(ErrorCode::FaultInjected),
            Some("https://github.com/shenli/agentdb/blob/main/docs/runbooks/fault_injected.md")
        );
        assert_eq!(
            doc_url_for(ErrorCode::QuotaExceeded),
            Some("https://github.com/shenli/agentdb/blob/main/docs/runbooks/quota_exceeded.md")
        );
    }

    #[test]
    fn doc_url_is_none_for_codes_without_a_runbook_yet() {
        // Per-code runbooks for these are follow-ups; explicit None
        // here prevents silently returning stale/wrong URLs.
        assert!(doc_url_for(ErrorCode::NotFound).is_none());
        assert!(doc_url_for(ErrorCode::InvalidRequest).is_none());
        assert!(doc_url_for(ErrorCode::StorageError).is_none());
        assert!(doc_url_for(ErrorCode::SerializationError).is_none());
    }

    #[test]
    fn lease_lost_response_populates_doc_url() {
        let err = ApiError::Core(agentdb_core::Error::LeaseLost {
            box_id: 3,
            epoch: 7,
        });
        let (_, _, body) = err.to_response();
        assert_eq!(
            body.doc_url.as_deref(),
            Some("https://github.com/shenli/agentdb/blob/main/docs/runbooks/lease_lost.md")
        );
        assert!(matches!(body.code, ErrorCode::LeaseLost));
    }

    #[test]
    fn storage_error_response_has_no_doc_url_yet() {
        let err = ApiError::Core(agentdb_core::Error::Storage(agentdb_storage::Error::Other(
            "boom".into(),
        )));
        let (_, _, body) = err.to_response();
        assert!(body.doc_url.is_none());
    }
}
