use axum::{
    http::{header, HeaderValue},
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};

/// Sub-router that serves the dashboard's static assets under the caller's
/// chosen mount prefix.
pub fn router() -> Router {
    Router::new()
        .route("/", get(index))
        .route("/sessions/:tenant_id/:session_id", get(index))
        .route("/app.js", get(app_js))
        .route("/styles.css", get(styles_css))
}

pub async fn index() -> Response {
    let mut response = Html(include_str!("../static/index.html")).into_response();
    response.headers_mut().insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; connect-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
        ),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn app_js() -> Response {
    embedded_asset(
        "application/javascript; charset=utf-8",
        include_str!("../static/app.js"),
    )
}

async fn styles_css() -> Response {
    embedded_asset(
        "text/css; charset=utf-8",
        include_str!("../static/styles.css"),
    )
}

fn embedded_asset(content_type: &'static str, body: &'static str) -> Response {
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "no-cache"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        body,
    )
        .into_response()
}
