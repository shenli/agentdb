use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error(transparent)]
    Core(#[from] agentdb_core::Error),
    #[error("bad request: {0}")]
    BadRequest(String),
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let is_rate_limited = matches!(
            &self,
            ApiError::Core(agentdb_core::Error::TooManyRequests { .. })
        );
        let status = match &self {
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::Core(agentdb_core::Error::NotFound(_)) => StatusCode::NOT_FOUND,
            ApiError::Core(agentdb_core::Error::Invalid(_)) => StatusCode::BAD_REQUEST,
            ApiError::Core(agentdb_core::Error::TooManyRequests { .. }) => {
                StatusCode::TOO_MANY_REQUESTS
            }
            ApiError::Core(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = self.to_string();
        let mut resp = (status, axum::Json(ErrorBody { error: &body })).into_response();
        if is_rate_limited {
            resp.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        resp
    }
}
