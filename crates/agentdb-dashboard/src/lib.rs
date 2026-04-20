use std::path::PathBuf;

use axum::Router;
use tower_http::services::ServeDir;

/// Sub-router that serves the dashboard's static assets under the caller's
/// chosen mount prefix.
pub fn router() -> Router {
    let static_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("static");
    Router::new().fallback_service(ServeDir::new(static_dir))
}
