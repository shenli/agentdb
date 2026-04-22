//! REST `X-AgentDB-HLC` header propagation.
//!
//! Every HTTP request may carry an `X-AgentDB-HLC: physical_ns.logical`
//! header; the middleware calls `HlcClock::update` with it before the
//! handler runs, so the local clock absorbs causality from the caller.
//! Every response includes the same header on the outbound side so the
//! caller can track the server's clock.
//!
//! Parsing is lenient: a missing or malformed header is ignored (the
//! request proceeds with the current local HLC). A header that
//! violates the max-offset guard returns HTTP 400 so a rogue or
//! misconfigured peer cannot poison our clock.
//!
//! Mirror of the Mongo `$clusterTime` bridge in `agentdb-wire::hlc`.

use std::sync::Arc;

use agentdb_core::{BoxRegistry, HlcTimestamp};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::Response;

use crate::AppState;

pub const HLC_HEADER: &str = "x-agentdb-hlc";

/// Parse `physical_ns.logical` into an `HlcTimestamp`. Returns `None` on
/// any parse failure — caller treats that as "no HLC present" rather
/// than erroring.
pub fn parse_header(raw: &str) -> Option<HlcTimestamp> {
    let (phys, log) = raw.split_once('.')?;
    Some(HlcTimestamp {
        physical_ns: phys.trim().parse().ok()?,
        logical: log.trim().parse().ok()?,
    })
}

/// Format an `HlcTimestamp` for the outbound header.
pub fn format_header(hlc: HlcTimestamp) -> String {
    format!("{}.{}", hlc.physical_ns, hlc.logical)
}

/// Axum middleware: absorb inbound HLC, inject outbound HLC.
pub async fn hlc_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    let hlc_clock = state.registry.hlc();

    // Absorb inbound header, if any.
    if let Some(raw) = req.headers().get(HLC_HEADER) {
        if let Ok(s) = raw.to_str() {
            if let Some(remote) = parse_header(s) {
                if remote != HlcTimestamp::ZERO {
                    if let Err(e) = hlc_clock.update(remote) {
                        return Err((
                            StatusCode::BAD_REQUEST,
                            format!("invalid X-AgentDB-HLC: {e}"),
                        ));
                    }
                }
            }
        }
    }

    let mut response = next.run(req).await;

    // Advance the clock on the outbound path so every response carries a
    // fresh HLC tick — matches MongoDB `$clusterTime` per-message
    // semantics and guarantees the outbound header is strictly > any
    // inbound we just absorbed.
    let out = format_header(hlc_clock.now());
    if let Ok(v) = HeaderValue::from_str(&out) {
        response.headers_mut().insert(HLC_HEADER, v);
    }

    Ok(response)
}

/// Extract `X-AgentDB-HLC` from a response for test assertions.
pub fn extract_hlc_from_headers(headers: &HeaderMap) -> Option<HlcTimestamp> {
    let raw = headers.get(HLC_HEADER)?.to_str().ok()?;
    parse_header(raw)
}

/// Construct a registry + state pair for tests. Re-exported convenience.
pub fn hlc_from_registry(reg: &Arc<BoxRegistry>) -> HlcTimestamp {
    reg.hlc().current()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_roundtrips_format() {
        let ts = HlcTimestamp {
            physical_ns: 1_700_000_123_456_789_000,
            logical: 9,
        };
        let s = format_header(ts);
        let back = parse_header(&s).unwrap();
        assert_eq!(back, ts);
    }

    #[test]
    fn parse_tolerates_whitespace() {
        let ts = parse_header("  1700000000000000000 . 42  ").unwrap();
        assert_eq!(ts.physical_ns, 1_700_000_000_000_000_000);
        assert_eq!(ts.logical, 42);
    }

    #[test]
    fn parse_rejects_missing_dot() {
        assert!(parse_header("12345").is_none());
    }

    #[test]
    fn parse_rejects_non_numeric() {
        assert!(parse_header("abc.def").is_none());
    }

    #[test]
    fn parse_rejects_negative() {
        // physical_ns is u64; negatives don't parse.
        assert!(parse_header("-1.0").is_none());
    }
}
