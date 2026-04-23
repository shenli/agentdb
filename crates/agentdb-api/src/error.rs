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
    /// Node identifier carried by process-level errors
    /// (`NodeDraining`) so clients can log which specific process
    /// emitted the error when they round-robin across a fleet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
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
    /// M22: this node has been drained via `POST /api/admin/drain`.
    /// Unlike `LeaseLost`, this is permanent for the process's
    /// lifetime — clients must retry at a different node.
    NodeDraining,
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
                    node_id: None,
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
                    node_id: None,
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
                        node_id: None,
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
                    node_id: None,
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
                    node_id: None,
                    doc_url: None,
                },
            ),
            agentdb_core::Error::NodeDraining { node_id } => (
                StatusCode::SERVICE_UNAVAILABLE,
                HeaderMap::new(),
                ErrorResponse {
                    code: ErrorCode::NodeDraining,
                    kind: ErrorKind::ApiError,
                    message,
                    resource: None,
                    id: None,
                    field: None,
                    scope: None,
                    retry_after_ms: None,
                    box_id: None,
                    fault_kind: None,
                    node_id: Some(node_id.clone()),
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
                    node_id: None,
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
                    node_id: None,
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

/// Runbook URL for each error code. The URL points to the repo's
/// `docs/runbooks/` directory on GitHub so operators can open it
/// from logs, Slack, or an incident ticket without pulling a local
/// checkout. Returns `None` only for codes that don't yet have a
/// runbook — currently every variant is covered.
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
        ErrorCode::NotFound => {
            Some("https://github.com/shenli/agentdb/blob/main/docs/runbooks/not_found.md")
        }
        ErrorCode::InvalidRequest => {
            Some("https://github.com/shenli/agentdb/blob/main/docs/runbooks/invalid_request.md")
        }
        ErrorCode::StorageError => {
            Some("https://github.com/shenli/agentdb/blob/main/docs/runbooks/storage_error.md")
        }
        ErrorCode::SerializationError => {
            Some("https://github.com/shenli/agentdb/blob/main/docs/runbooks/serialization_error.md")
        }
        ErrorCode::NodeDraining => {
            Some("https://github.com/shenli/agentdb/blob/main/docs/runbooks/node_draining.md")
        }
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
    fn doc_url_covers_every_error_code() {
        // Every variant must have a published runbook. Opening these
        // URLs in a browser is part of the operator's diagnose step
        // — we lock them down here against accidental renames.
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
        assert_eq!(
            doc_url_for(ErrorCode::NotFound),
            Some("https://github.com/shenli/agentdb/blob/main/docs/runbooks/not_found.md")
        );
        assert_eq!(
            doc_url_for(ErrorCode::InvalidRequest),
            Some("https://github.com/shenli/agentdb/blob/main/docs/runbooks/invalid_request.md")
        );
        assert_eq!(
            doc_url_for(ErrorCode::StorageError),
            Some("https://github.com/shenli/agentdb/blob/main/docs/runbooks/storage_error.md")
        );
        assert_eq!(
            doc_url_for(ErrorCode::SerializationError),
            Some(
                "https://github.com/shenli/agentdb/blob/main/docs/runbooks/serialization_error.md"
            )
        );
        assert_eq!(
            doc_url_for(ErrorCode::NodeDraining),
            Some("https://github.com/shenli/agentdb/blob/main/docs/runbooks/node_draining.md")
        );
    }

    #[test]
    fn node_draining_response_carries_node_id_and_doc_url() {
        let err = ApiError::Core(agentdb_core::Error::NodeDraining {
            node_id: "node-smoke".into(),
        });
        let (status, _, body) = err.to_response();
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(matches!(body.code, ErrorCode::NodeDraining));
        assert_eq!(body.node_id.as_deref(), Some("node-smoke"));
        assert_eq!(
            body.doc_url.as_deref(),
            Some("https://github.com/shenli/agentdb/blob/main/docs/runbooks/node_draining.md")
        );
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
    fn storage_error_response_populates_doc_url() {
        let err = ApiError::Core(agentdb_core::Error::Storage(agentdb_storage::Error::Other(
            "boom".into(),
        )));
        let (_, _, body) = err.to_response();
        assert_eq!(
            body.doc_url.as_deref(),
            Some("https://github.com/shenli/agentdb/blob/main/docs/runbooks/storage_error.md")
        );
    }
}
