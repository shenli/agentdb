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
        match err {
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
                },
            ),
        }
    }
}

fn fault_kind_name(k: &agentdb_core::FaultKind) -> &'static str {
    match k {
        agentdb_core::FaultKind::Latency { .. } => "latency",
        agentdb_core::FaultKind::Error => "error",
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
