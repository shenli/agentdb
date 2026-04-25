//! End-to-end test for W3C trace-context propagation across
//! HTTP boundaries.
//!
//! Stands up a minimal axum router carrying the
//! `trace_parent_middleware` and one test endpoint that echoes
//! the trace id of the current span. We send a request with a
//! synthetic `traceparent` header and assert the server's
//! handler observed the same trace id.
//!
//! Without the middleware (or with a broken propagator), the
//! server's local span gets a fresh trace id and the assertion
//! fails. With the middleware in place, the per-request span
//! inherits the inbound trace id and any descendant spans
//! emitted inside the handler share it — which is the whole
//! point of cross-node trace propagation.

use std::sync::Once;

use agentdb_api::trace_context;
use axum::{routing::get, Router};
use opentelemetry::trace::{TraceContextExt, TracerProvider};
use opentelemetry_sdk::trace::TracerProvider as SdkProvider;
use serial_test::serial;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

static INIT: Once = Once::new();

/// Install an OTel layer onto the global tracing subscriber,
/// once per process. The provider has no exporter — we don't
/// care where spans go, only that they get OTel context
/// attached so trace propagation can be observed via
/// `Span::current().context()`.
fn install_otel_layer_once() {
    INIT.call_once(|| {
        let provider = SdkProvider::builder().build();
        let tracer = provider.tracer("agentdb-test");
        let layer = tracing_opentelemetry::layer().with_tracer(tracer);
        // try_init: if some prior test already installed a
        // subscriber, don't fight over it.
        let _ = tracing_subscriber::registry().with(layer).try_init();
    });
}

async fn echo_trace_id() -> String {
    let ctx = tracing::Span::current().context();
    let sc = ctx.span().span_context().clone();
    format!(
        "{:032x}",
        u128::from_be_bytes(sc.trace_id().to_bytes())
    )
}

async fn spawn_server() -> String {
    let app = Router::new()
        .route("/echo-trace", get(echo_trace_id))
        .layer(axum::middleware::from_fn(
            trace_context::trace_parent_middleware,
        ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
#[serial]
async fn middleware_makes_handler_span_inherit_inbound_trace_id() {
    install_otel_layer_once();
    let base = spawn_server().await;
    // Synthetic but well-formed W3C traceparent. Trace flags
    // 01 = sampled. Picked the example trace id from the W3C
    // spec so it's recognizable.
    let trace_id = "0af7651916cd43dd8448eb211c80319c";
    let traceparent = format!("00-{trace_id}-b7ad6b7169203331-01");
    let resp = reqwest::Client::new()
        .get(format!("{base}/echo-trace"))
        .header("traceparent", &traceparent)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(
        resp, trace_id,
        "handler span should inherit the inbound trace id; got {resp}"
    );
}

#[tokio::test]
#[serial]
async fn missing_traceparent_yields_a_fresh_local_trace_id() {
    install_otel_layer_once();
    let base = spawn_server().await;
    // No traceparent on the outgoing request. The server should
    // start a fresh local trace; its trace id must be a non-zero
    // 32-hex string different from any prior test's trace id.
    let resp = reqwest::Client::new()
        .get(format!("{base}/echo-trace"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(resp.len(), 32, "trace id must be 32 hex chars: {resp}");
    assert_ne!(
        resp, "0".repeat(32),
        "trace id must be non-zero (OTel layer is installed)"
    );
}

#[tokio::test]
#[serial]
async fn malformed_traceparent_falls_through_to_a_fresh_trace() {
    install_otel_layer_once();
    let base = spawn_server().await;
    // Malformed but syntactically header-shaped. Per W3C and
    // our parser, this is silently ignored — the request gets
    // a fresh local trace, not an error.
    let resp = reqwest::Client::new()
        .get(format!("{base}/echo-trace"))
        .header("traceparent", "garbage-input")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(resp.len(), 32);
    assert_ne!(resp, "0".repeat(32));
    // Crucially, NOT the "garbage-input" string-as-trace-id.
    assert_ne!(resp, "garbage-input000000000000000000");
}
