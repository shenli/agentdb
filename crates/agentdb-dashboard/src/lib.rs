use axum::{response::Html, routing::get, Router};
use tower_http::services::ServeDir;

/// Sub-router that serves the dashboard's static assets under the caller's
/// chosen mount prefix.
pub fn router() -> Router {
    let static_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("static");
    Router::new()
        .route("/", get(index))
        .fallback_service(ServeDir::new(static_dir))
}

pub async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}
