//! Cross-node REST forwarding for write requests (M20 multi-node).
//!
//! When `BoxRegistry::route_session_for_write` returns
//! [`agentdb_core::RouteDecision::Remote`], the REST handler
//! calls into this module to re-issue the same request against
//! the owning node and stream the response back.
//!
//! Design:
//!
//! 1. **Preserve the caller's authentication.** The
//!    `Authorization` header is forwarded verbatim so the remote
//!    node's Basic Auth middleware verifies independently. No
//!    "trust me" header — the peer validates the credential.
//!
//! 2. **Preserve the HLC bridge.** `X-AgentDB-HLC` is forwarded
//!    verbatim and the remote response's `X-AgentDB-HLC` is
//!    returned to the client unchanged. The local HLC middleware
//!    still fires around this call — it absorbs the inbound
//!    header once and the outbound header reflects the local
//!    clock after forwarding. Safe because the remote node's HLC
//!    is merged via the request it received.
//!
//! 3. **Loop guard.** Every forwarded request carries
//!    `X-AgentDB-Forwarded: 1`. The handler refuses to forward
//!    twice: if that header is present on an inbound request
//!    *and* routing still resolves to Remote, return 421
//!    Misdirected Request. Prevents two nodes with drifted
//!    control-plane state from ping-ponging a write.
//!
//! 4. **No internal retry.** A failed forward surfaces a 503 to
//!    the caller. Retrying internally would mask slow failover
//!    that operators need to see.

use axum::body::Bytes;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use reqwest::Client;
use serde_json::Value;

use agentdb_core::NodeTarget;

/// HTTP header signaling that this request has already been
/// forwarded once. Every forward adds it; every handler checks
/// it before forwarding again.
pub const FORWARDED_HEADER: &str = "x-agentdb-forwarded";
/// HLC bridge header. Re-declared here (matches
/// `hlc_header::HLC_HEADER`) to keep this module self-contained
/// for the forward path. Kept in sync with the source via a
/// compile-time assertion in unit tests.
pub const HLC_HEADER: &str = "x-agentdb-hlc";

/// Forward a POST `/api/tenants/:tid/sessions/:sid/append_and_fetch`
/// to `target` and return the remote response verbatim. The
/// caller passes the original body + the headers of interest
/// (Authorization, HLC); everything else is reconstructed from
/// the path.
///
/// Returns either a successful `Response` (copied from the peer)
/// or an `Err(ForwardError)` that the caller maps to 503.
pub async fn forward_append_and_fetch(
    http: &Client,
    target: &NodeTarget,
    tenant_id: &str,
    session_id: &str,
    body: Bytes,
    passthrough: PassthroughHeaders,
) -> Result<Response, ForwardError> {
    let base = normalize_base(&target.address);
    let url = format!(
        "{base}/api/tenants/{}/sessions/{}/append_and_fetch",
        urlencode(tenant_id),
        urlencode(session_id),
    );

    let mut req = http
        .post(&url)
        .header(header::CONTENT_TYPE, "application/json")
        .header(FORWARDED_HEADER, "1")
        .body(body);
    if let Some(v) = passthrough.authorization {
        req = req.header(header::AUTHORIZATION, v);
    }
    if let Some(v) = passthrough.hlc {
        req = req.header(HLC_HEADER, v);
    }
    let remote = req.send().await.map_err(|e| ForwardError::Transport {
        node_id: target.node_id.clone(),
        address: target.address.clone(),
        reason: e.to_string(),
    })?;

    let status = remote.status();
    let mut out_headers = HeaderMap::new();
    if let Some(v) = remote.headers().get(HLC_HEADER).cloned() {
        out_headers.insert(HLC_HEADER, v);
    }
    let bytes = remote.bytes().await.map_err(|e| ForwardError::Transport {
        node_id: target.node_id.clone(),
        address: target.address.clone(),
        reason: e.to_string(),
    })?;

    // Build the axum Response. We deliberately don't re-parse
    // the body into a typed struct — the server is the wire
    // contract and we pass its answer through unchanged.
    let mut resp = Response::builder()
        .status(
            StatusCode::from_u16(status.as_u16())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        )
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(bytes))
        .map_err(|e| ForwardError::Synthesize {
            reason: e.to_string(),
        })?;
    for (k, v) in out_headers.iter() {
        resp.headers_mut().insert(k, v.clone());
    }
    Ok(resp)
}

/// 421 Misdirected Request. Used when a forwarded request
/// arrives and the receiving node's own routing table still
/// says it should be forwarded (cross-node state drift).
/// Emitting a distinct status code lets operators distinguish
/// "routing disagreement" from a normal remote error.
pub fn misdirected_reply(local_node: &str, tablet_id: i64) -> Response {
    let body = serde_json::to_vec(&serde_json::json!({
        "code": "misdirected",
        "type": "api_error",
        "message": format!(
            "routing drift: forwarded request targets tablet {tablet_id} but this node ({local_node}) \
             also considers it remote. Check shard/tablet assignments via /api/admin/shards and \
             /api/admin/tablets."
        ),
    }))
    .unwrap_or_else(|_| b"{}".to_vec());
    Response::builder()
        .status(StatusCode::MISDIRECTED_REQUEST)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(body))
        .expect("static misdirected reply builds")
}

/// Headers the forwarder lifts from the inbound request. Kept
/// as a typed bundle so the call site at `handlers.rs` can
/// extract them once before consuming the body.
#[derive(Debug, Default, Clone)]
pub struct PassthroughHeaders {
    pub authorization: Option<String>,
    pub hlc: Option<String>,
}

impl PassthroughHeaders {
    pub fn from_headers(h: &HeaderMap) -> Self {
        Self {
            authorization: h
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .map(String::from),
            hlc: h.get(HLC_HEADER).and_then(|v| v.to_str().ok()).map(String::from),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ForwardError {
    #[error("forwarding to {node_id}@{address} failed: {reason}")]
    Transport {
        node_id: String,
        address: String,
        reason: String,
    },
    #[error("failed to synthesize response: {reason}")]
    Synthesize { reason: String },
}

impl ForwardError {
    /// Compact JSON body for the forwarded-failure 503 response.
    pub fn to_body(&self) -> Value {
        match self {
            ForwardError::Transport {
                node_id,
                address,
                reason,
            } => serde_json::json!({
                "code": "forward_failed",
                "type": "api_error",
                "message": format!("forwarding to peer failed: {reason}"),
                "node_id": node_id,
                "address": address,
            }),
            ForwardError::Synthesize { reason } => serde_json::json!({
                "code": "internal",
                "type": "api_error",
                "message": format!("response synthesis failed: {reason}"),
            }),
        }
    }
}

/// Turn an entry from `nodes.address` into a usable HTTP base.
/// Accepts:
///   - `http://host:port`       (used as-is, minus trailing `/`)
///   - `https://host:port`      (used as-is, minus trailing `/`)
///   - `host:port` / `ip:port`  (prepend `http://`)
fn normalize_base(address: &str) -> String {
    let trimmed = address.trim_end_matches('/');
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    }
}

/// Minimal percent-encoding for path components. Matches
/// `bin/agentdb-server/src/admin.rs::urlencode` — kept local
/// so this crate doesn't pull in a URL encoding dep.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => out.push(c),
            _ => {
                for b in c.to_string().as_bytes() {
                    out.push_str(&format!("%{:02X}", b));
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hlc_header_matches_source() {
        assert_eq!(HLC_HEADER, crate::hlc_header::HLC_HEADER);
    }

    #[test]
    fn normalize_base_prepends_scheme_when_missing() {
        assert_eq!(normalize_base("127.0.0.1:7878"), "http://127.0.0.1:7878");
        assert_eq!(normalize_base("example.com:80"), "http://example.com:80");
    }

    #[test]
    fn normalize_base_preserves_explicit_scheme() {
        assert_eq!(
            normalize_base("https://edge.example.com"),
            "https://edge.example.com"
        );
        assert_eq!(
            normalize_base("http://127.0.0.1:7878/"),
            "http://127.0.0.1:7878"
        );
    }

    #[test]
    fn urlencode_passes_safe_chars_through() {
        assert_eq!(urlencode("t-1"), "t-1");
        assert_eq!(urlencode("ABC_xyz.9~"), "ABC_xyz.9~");
    }

    #[test]
    fn urlencode_percent_encodes_unsafe_chars() {
        assert_eq!(urlencode("a/b"), "a%2Fb");
        assert_eq!(urlencode("a b"), "a%20b");
    }

    #[test]
    fn passthrough_headers_lifts_auth_and_hlc() {
        let mut h = HeaderMap::new();
        h.insert(header::AUTHORIZATION, "Basic abc".parse().unwrap());
        h.insert(HLC_HEADER, "100.0".parse().unwrap());
        let p = PassthroughHeaders::from_headers(&h);
        assert_eq!(p.authorization.as_deref(), Some("Basic abc"));
        assert_eq!(p.hlc.as_deref(), Some("100.0"));
    }

    #[test]
    fn misdirected_reply_carries_421() {
        let r = misdirected_reply("node-a", 7);
        assert_eq!(r.status().as_u16(), 421);
    }
}
