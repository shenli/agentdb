use axum::http::StatusCode;
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
        let status = match &self {
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::Core(agentdb_core::Error::NotFound(_)) => StatusCode::NOT_FOUND,
            ApiError::Core(agentdb_core::Error::Invalid(_)) => StatusCode::BAD_REQUEST,
            ApiError::Core(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = self.to_string();
        (status, axum::Json(ErrorBody { error: &body })).into_response()
    }
}
