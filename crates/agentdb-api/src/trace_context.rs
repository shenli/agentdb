//! W3C Trace Context propagation across cross-node forwarding.
//!
//! When node A forwards a request to node B over HTTP, we
//! attach a `traceparent` header so node B's REST handler can
//! make its span a child of A's. Without this, every forwarded
//! write looks like a brand-new trace tree on the receiving
//! side and the multi-node lineage is lost — which defeats the
//! whole point of having tracing in a multi-node setup.
//!
//! Format follows the W3C standard:
//!     traceparent: 00-<32-hex-trace-id>-<16-hex-span-id>-<2-hex-flags>
//!
//! We hand-roll the format rather than pulling in a propagator
//! crate. The format is simple, stable, and the round-trip is a
//! handful of lines per direction.
//!
//! Usage:
//!   * Outbound: `inject_into_request` reads the current span's
//!     context and adds a `traceparent` header to the
//!     `reqwest::RequestBuilder`.
//!   * Inbound: `parent_from_headers` reads `traceparent` from
//!     the inbound `HeaderMap`, parses it into an
//!     `opentelemetry::Context`, and the caller passes that to
//!     `tracing::Span::set_parent`.

use axum::http::HeaderMap;
use opentelemetry::trace::{
    SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
};
use opentelemetry::Context;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Standard W3C header name. Lower-cased per HTTP/2 norm.
pub const TRACEPARENT_HEADER: &str = "traceparent";

/// Format the W3C traceparent string for the current `tracing`
/// span's OTel context. Returns `None` when no OTel layer is
/// installed (the tests-only `fmt`-only setup) or when the
/// context's span is invalid.
pub fn current_traceparent() -> Option<String> {
    let span = tracing::Span::current();
    let ctx = span.context();
    let sc = ctx.span().span_context().clone();
    if !sc.is_valid() {
        return None;
    }
    Some(format_traceparent(&sc))
}

fn format_traceparent(sc: &SpanContext) -> String {
    // Format: 00-<32-hex>-<16-hex>-<2-hex>. Each component is
    // big-endian hex, lower-case, no separators inside fields.
    format!(
        "00-{:032x}-{:016x}-{:02x}",
        u128::from_be_bytes(sc.trace_id().to_bytes()),
        u64::from_be_bytes(sc.span_id().to_bytes()),
        sc.trace_flags().to_u8(),
    )
}

/// Inject the current span's traceparent into a
/// `reqwest::RequestBuilder`. No-op when there's no current
/// trace (e.g., a unit test with no tracing layer).
///
/// Takes the builder by value because `reqwest::RequestBuilder`
/// is move-only; returns it after the optional chain.
pub fn inject_into_request(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    if let Some(tp) = current_traceparent() {
        builder.header(TRACEPARENT_HEADER, tp)
    } else {
        builder
    }
}

/// Build an `opentelemetry::Context` carrying the parent span
/// from the inbound `traceparent` header. Returns `None` when
/// the header is absent or malformed; callers fall through to
/// "this request starts a new trace."
///
/// Used by middleware: the inbound REST handler reads this and
/// calls `tracing::Span::set_parent(ctx)` on its top-level
/// span before any work happens.
pub fn parent_from_headers(headers: &HeaderMap) -> Option<Context> {
    let raw = headers.get(TRACEPARENT_HEADER)?.to_str().ok()?;
    parse_traceparent(raw)
}

fn parse_traceparent(raw: &str) -> Option<Context> {
    // 4 dash-separated fields. We only support version 00 (the
    // only published W3C version today). Future versions are
    // backwards-compatible per spec — peers that emit 01-...
    // will be ignored rather than mis-parsed.
    let parts: Vec<&str> = raw.split('-').collect();
    if parts.len() != 4 {
        return None;
    }
    if parts[0] != "00" {
        return None;
    }
    // Strict-validate field lengths up front. The OTel hex
    // parsers accept short input by zero-padding, which would
    // turn a malformed peer header into a silent partial trace
    // id — strictly worse than rejecting. The W3C spec mandates
    // exact lengths for every field.
    if parts[1].len() != 32 || parts[2].len() != 16 || parts[3].len() != 2 {
        return None;
    }
    let trace_id = TraceId::from_hex(parts[1]).ok()?;
    let span_id = SpanId::from_hex(parts[2]).ok()?;
    let flags = u8::from_str_radix(parts[3], 16).ok()?;
    let sc = SpanContext::new(
        trace_id,
        span_id,
        TraceFlags::new(flags),
        // remote=true: the parent lives on a different node.
        true,
        TraceState::default(),
    );
    Some(Context::new().with_remote_span_context(sc))
}

/// Wire the inbound traceparent (if any) into the current
/// `tracing` span as its parent. Combines `parent_from_headers`
/// + `Span::set_parent` for the common middleware case.
pub fn attach_parent_from_headers(headers: &HeaderMap, span: &tracing::Span) {
    if let Some(ctx) = parent_from_headers(headers) {
        span.set_parent(ctx);
    }
}

/// Axum middleware: attach the inbound `traceparent` (if any)
/// to a fresh `rest.request` span and run the rest of the
/// request inside it. Every per-route `#[tracing::instrument]`
/// span thereby becomes a descendant of the originating
/// caller's span, making cross-node forwarding observable as
/// one trace tree rather than two disconnected ones.
///
/// No-op for plain (non-forwarded) requests — the `rest.request`
/// span is just a local root in that case.
pub async fn trace_parent_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use tracing::Instrument;
    let span = tracing::info_span!(
        "rest.request",
        method = %req.method(),
        uri = %req.uri(),
    );
    attach_parent_from_headers(req.headers(), &span);
    next.run(req).instrument(span).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_traceparent_accepts_canonical_format() {
        let raw = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        let ctx = parse_traceparent(raw).expect("valid traceparent parses");
        let sc = ctx.span().span_context().clone();
        assert!(sc.is_valid());
        assert!(sc.is_remote());
        assert!(sc.is_sampled());
        // Round-trip: re-format and compare.
        assert_eq!(format_traceparent(&sc), raw);
    }

    #[test]
    fn parse_traceparent_rejects_wrong_version() {
        // Version 01 might be a real future version — but until
        // we know what changed, ignore unknown versions rather
        // than risk misinterpreting the trailing fields.
        assert!(
            parse_traceparent("01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
                .is_none()
        );
    }

    #[test]
    fn parse_traceparent_rejects_malformed_input() {
        assert!(parse_traceparent("").is_none());
        assert!(parse_traceparent("not-even-close").is_none());
        assert!(parse_traceparent("00-too-short").is_none());
        // Wrong-length trace id (31 chars instead of 32).
        assert!(parse_traceparent("00-0af7651916cd43dd8448eb211c80319-b7ad6b7169203331-01")
            .is_none());
    }

    #[test]
    fn current_traceparent_is_none_without_otel_layer() {
        // No layer installed in this test → no-op span context.
        assert!(current_traceparent().is_none());
    }

    #[test]
    fn parent_from_headers_returns_none_without_header() {
        let h = HeaderMap::new();
        assert!(parent_from_headers(&h).is_none());
    }

    #[test]
    fn parent_from_headers_skips_malformed_value() {
        let mut h = HeaderMap::new();
        h.insert(TRACEPARENT_HEADER, "garbage".parse().unwrap());
        assert!(parent_from_headers(&h).is_none());
    }
}
