//! Tracing + OpenTelemetry initialization for the server binary.
//!
//! Two modes, selected by `AGENTDB_OTLP_ENDPOINT`:
//!
//! - **Empty/unset** → dev mode. Human-readable spans + events go to
//!   stdout via `tracing_subscriber::fmt`.
//! - **Set** (e.g. `http://localhost:4317`) → prod mode. OTLP/gRPC
//!   exporter batches spans to the named collector; stdout still gets
//!   pretty-printed events so `docker compose logs` remains useful.
//!
//! Per design doc §8.1 and the M15 implementation plan, Part A.
//!
//! Returns a `TracerGuard` RAII handle that flushes pending spans on
//! drop — the caller (main) must keep it alive until shutdown.

use anyhow::Context;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::trace::{Sampler, TracerProvider};
use opentelemetry_sdk::{runtime, Resource};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Registry};

/// RAII guard that shuts down the tracer provider on drop, flushing
/// any pending batched spans. Must be held alive for the lifetime of
/// the process; dropping it mid-run detaches the exporter.
pub struct TracerGuard {
    provider: Option<TracerProvider>,
}

impl Drop for TracerGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take() {
            // Best-effort flush + shutdown; we don't propagate errors on
            // drop per RAII convention.
            for result in provider.force_flush() {
                if let Err(e) = result {
                    eprintln!("tracer flush error on shutdown: {e:?}");
                }
            }
        }
    }
}

/// Initialize the global tracing subscriber. Reads env:
///
/// - `AGENTDB_OTLP_ENDPOINT` — e.g. `http://localhost:4317`. If unset,
///   OTLP export is disabled and only stdout `fmt` is active.
/// - `AGENTDB_TRACE_SERVICE_NAME` — OTel resource attribute
///   `service.name`. Default: `"agentdb"`.
/// - `RUST_LOG` / `EnvFilter` standard filter syntax, e.g. `info` or
///   `agentdb_core=debug,info`.
pub fn init() -> anyhow::Result<TracerGuard> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = fmt::layer().with_target(true);

    let otlp_endpoint = std::env::var("AGENTDB_OTLP_ENDPOINT").ok().filter(|s| !s.is_empty());
    let service_name =
        std::env::var("AGENTDB_TRACE_SERVICE_NAME").unwrap_or_else(|_| "agentdb".into());

    // Branch on OTLP availability. Both paths use the same fmt layer so
    // stdout logging is consistent.
    let guard = if let Some(endpoint) = otlp_endpoint {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&endpoint)
            .with_protocol(Protocol::Grpc)
            .build()
            .context("build OTLP exporter")?;

        let provider = TracerProvider::builder()
            .with_batch_exporter(exporter, runtime::Tokio)
            .with_resource(Resource::new(vec![KeyValue::new(
                "service.name",
                service_name.clone(),
            )]))
            .with_sampler(Sampler::AlwaysOn)
            .build();

        // Clone a tracer for the layer before handing the provider to
        // the guard — `tracing-opentelemetry` wants a `Tracer`, not a
        // provider.
        let tracer = provider.tracer(service_name);
        let otel_layer = OpenTelemetryLayer::new(tracer);

        // Install as global so `opentelemetry::global::tracer()` etc.
        // work from anywhere.
        global::set_tracer_provider(provider.clone());

        Registry::default()
            .with(env_filter)
            .with(fmt_layer)
            .with(otel_layer)
            .try_init()
            .context("install tracing subscriber")?;

        TracerGuard {
            provider: Some(provider),
        }
    } else {
        // Dev mode: fmt only, no OTel provider to flush.
        Registry::default()
            .with(env_filter)
            .with(fmt_layer)
            .try_init()
            .context("install tracing subscriber")?;
        TracerGuard { provider: None }
    };

    Ok(guard)
}
