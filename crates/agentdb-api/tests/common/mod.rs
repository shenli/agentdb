//! Shared harness for pub/sub-flavored integration tests.
//!
//! Spins up an in-process axum server backed by RocksDB tempdirs.
//! Exposes the live `AppState` so tests can subscribe to the
//! `EventBus` directly while issuing HTTP requests against the
//! same server. Pattern mirrors the per-file `spawn()` helpers
//! used elsewhere in this crate (e.g. `http.rs`); this version
//! is centralized so the pub/sub tests don't have to reinvent it.
//!
//! Not exported from the crate — `tests/common/mod.rs` is only
//! visible to integration test binaries that declare `mod common;`.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use agentdb_api::cors::CorsConfig;
use agentdb_api::jwt::{AgentDbClaims, JwtConfig, SessionsClaim, TenantJwtConfig};
use agentdb_api::{router, AppState, BroadcastBus, EventBus, StreamBus};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tempfile::TempDir;

/// Live in-process server for an integration test. The `state`
/// field is the same `AppState` the router was built with — tests
/// can call `state.event_bus.subscribe()` and observe envelopes
/// emitted by handler invocations on this server.
///
/// `Clone` so race-style tests can move a handle into a spawned
/// task that hits the same server in parallel. Tempdirs are
/// `Arc`-wrapped so all clones share lifetime — the dirs only
/// drop after the last clone goes away.
#[derive(Clone)]
pub struct TestServer {
    pub state: AppState,
    pub base: String,
    /// Held until drop so RocksDB + object-store dirs survive
    /// for the test's lifetime. `_` prefixed because tests don't
    /// touch them directly. `Arc` so clones share lifetime.
    _kv_dir: Arc<TempDir>,
    _obj_dir: Arc<TempDir>,
}

impl TestServer {
    pub async fn start() -> Self {
        Self::start_with_event_bus(EventBus::default()).await
    }

    /// Like `start()` but constructs the underlying `EventBus` with
    /// the given capacity. Used by the lag-too-far test to force
    /// overflow under a small capacity instead of waiting for the
    /// production default (16384) to fill.
    pub async fn start_with_event_bus_capacity(cap: usize) -> Self {
        Self::start_with_event_bus(EventBus::new(cap)).await
    }

    async fn start_with_event_bus(event_bus: EventBus) -> Self {
        Self::start_inner(
            event_bus,
            JwtConfig::default(),
            CorsConfig::default(),
            BroadcastBus::default(),
        )
        .await
    }

    /// Boot a TestServer where tenant `t-1` requires HMAC-signed JWTs
    /// using the given secret. Issuer is `https://auth.test`, no
    /// audience. Other tenants have no JWT config and the subscribe
    /// middleware falls through to whatever auth the route already
    /// applies (here: none, since `auth` is `None`). Used by the
    /// Task 9c-3 integration tests.
    pub async fn start_with_jwt_hmac(secret: &[u8]) -> Self {
        let mut per_tenant = HashMap::new();
        per_tenant.insert(
            "t-1".to_string(),
            TenantJwtConfig::Hmac {
                secret: secret.to_vec(),
                issuer: "https://auth.test".into(),
                audience: None,
            },
        );
        Self::start_inner(
            EventBus::default(),
            JwtConfig { per_tenant },
            CorsConfig::default(),
            BroadcastBus::default(),
        )
        .await
    }

    /// Boot a TestServer where the given tenant has CORS configured
    /// with the given allowlisted origins. Other tenants have no
    /// CORS config and the middleware passes their responses through
    /// without `Access-Control-Allow-Origin`. Used by the Task 10-2
    /// integration tests.
    pub async fn start_with_cors(tenant: &str, origins: &[&str]) -> Self {
        let mut per_tenant = HashMap::new();
        per_tenant.insert(
            tenant.to_string(),
            origins.iter().map(|s| (*s).to_string()).collect(),
        );
        Self::start_inner(
            EventBus::default(),
            JwtConfig::default(),
            CorsConfig { per_tenant },
            BroadcastBus::default(),
        )
        .await
    }

    async fn start_inner(
        event_bus: EventBus,
        jwt_config: JwtConfig,
        cors_config: CorsConfig,
        broadcast_bus: BroadcastBus,
    ) -> Self {
        let kv_dir = TempDir::new().expect("kv tempdir");
        let obj_dir = TempDir::new().expect("obj tempdir");
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).expect("rocks open"));
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).expect("local fs"));
        let state = AppState {
            registry: Arc::new(BoxRegistry::new(kv.clone(), store, 4).expect("registry")),
            tenants: Arc::new(TenantRegistry::new(kv.clone())),
            sessions: Arc::new(SessionRegistry::new(kv)),
            stream: StreamBus::default(),
            event_bus,
            http: agentdb_api::webhook::new_client(),
            auth: None,
            admin_auth: None,
            jwt_config,
            cors_config,
            broadcast_bus,
        };
        let app = router(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        Self {
            state,
            base: format!("http://{addr}"),
            _kv_dir: Arc::new(kv_dir),
            _obj_dir: Arc::new(obj_dir),
        }
    }

    /// POST a JSON body to a path on the running server. Panics
    /// on transport errors so tests' assertion lines stay focused
    /// on the response, not the plumbing.
    pub async fn post_json(&self, path: &str, body: serde_json::Value) -> reqwest::Response {
        reqwest::Client::new()
            .post(format!("{}{}", self.base, path))
            .json(&body)
            .send()
            .await
            .expect("send")
    }

    /// Open an SSE stream against `path`. Asserts a 200 status so
    /// callers can dive into frame parsing without re-checking.
    pub async fn open_sse(&self, path: &str) -> SseStream {
        let resp = reqwest::Client::new()
            .get(format!("{}{}", self.base, path))
            .send()
            .await
            .expect("send sse open");
        assert_eq!(resp.status(), 200, "SSE open failed: {}", resp.status());
        SseStream {
            body: resp,
            buf: String::new(),
        }
    }
}

/// Buffered Server-Sent Events stream. Holds the raw `reqwest`
/// response and a UTF-8 buffer; calls to [`SseStream::next_event`]
/// pull more chunks as needed. axum emits keep-alive comment
/// frames (`":\n\n"`) — those are silently skipped here so tests
/// only see real events.
pub struct SseStream {
    body: reqwest::Response,
    buf: String,
}

/// One parsed SSE frame. `event_name` is `Some` only when the
/// frame includes an `event:` line (axum sets this for the
/// `subscribed` control frame); regular envelope frames have no
/// `event:` line and so come through as `None`.
pub struct SseFrame {
    pub event_name: Option<String>,
    pub data: String,
}

impl SseStream {
    /// Read the next event frame, blocking until one is available.
    /// Skips comment-only keep-alive frames so callers see real
    /// payloads only. Panics if the underlying stream ends — the
    /// test should observe an event before the server closes.
    pub async fn next_event(&mut self) -> SseFrame {
        loop {
            if let Some(idx) = self.buf.find("\n\n") {
                let block = self.buf[..idx].to_string();
                self.buf.drain(..idx + 2);
                let frame = parse_sse_block(&block);
                if frame.event_name.is_none() && frame.data.is_empty() {
                    continue;
                }
                return frame;
            }
            let chunk = self
                .body
                .chunk()
                .await
                .expect("sse chunk")
                .expect("sse stream ended");
            self.buf
                .push_str(std::str::from_utf8(&chunk).expect("utf8"));
        }
    }

    /// Convenience for tests that don't care about the initial
    /// `subscribed` control event.
    pub async fn skip_subscribed_event(&mut self) {
        let _ = self.next_event().await;
    }

    /// Like [`SseStream::next_event`] but returns `None` on EOF or
    /// transport error instead of panicking. Tests asserting that
    /// the server *closes* the stream (e.g. after a `lag_too_far`
    /// error frame) need to distinguish "got another frame" from
    /// "server hung up", which `next_event`'s panic-on-end behavior
    /// hides.
    pub async fn try_next_event(&mut self) -> Option<SseFrame> {
        loop {
            if let Some(idx) = self.buf.find("\n\n") {
                let block = self.buf[..idx].to_string();
                self.buf.drain(..idx + 2);
                let frame = parse_sse_block(&block);
                if frame.event_name.is_none() && frame.data.is_empty() {
                    continue;
                }
                return Some(frame);
            }
            match self.body.chunk().await {
                Ok(Some(chunk)) => {
                    // EOF and transport error legitimately resolve
                    // to `None` (the server may close after an
                    // error frame); a non-UTF-8 chunk is *not* a
                    // clean close — masking it as one would hide a
                    // real serialization bug. Mirror `next_event`
                    // and panic loudly.
                    let s = std::str::from_utf8(&chunk)
                        .expect("sse chunk must be valid utf-8");
                    self.buf.push_str(s);
                }
                _ => return None,
            }
        }
    }
}

/// Wall-clock seconds since the UNIX epoch. Used by the JWT helpers
/// to compute `exp`. Panics on a clock before the epoch — fine for
/// tests; not a path that should ever fire on real hardware.
pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs()
}

/// Mint an HS256 JWT shaped like an AgentDB subscribe token.
///
/// `sessions == "*"` produces a `Wildcard` claim; any other string
/// produces a `List` with that single session id. Issuer is fixed
/// to `https://auth.test` to match `start_with_jwt_hmac`. `ttl_secs`
/// is added to `now_secs()` to set `exp`.
pub fn mint_hmac_token(secret: &[u8], tenant: &str, sessions: &str, ttl_secs: u64) -> String {
    let claims = AgentDbClaims {
        iss: "https://auth.test".into(),
        aud: None,
        sub: "test-user".into(),
        tenant: tenant.into(),
        sessions: if sessions == "*" {
            SessionsClaim::Wildcard("*".into())
        } else {
            SessionsClaim::List(vec![sessions.into()])
        },
        channels: None,
        exp: now_secs() + ttl_secs,
    };
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret),
    )
    .expect("encode hmac jwt for test")
}

/// Same shape as [`mint_hmac_token`] but takes an explicit list of
/// allowed session ids. Used by tests that need to verify
/// "session not in claim → 403" without ambiguity.
pub fn mint_hmac_token_for_sessions(
    secret: &[u8],
    tenant: &str,
    sessions: &[&str],
    ttl_secs: u64,
) -> String {
    let claims = AgentDbClaims {
        iss: "https://auth.test".into(),
        aud: None,
        sub: "test-user".into(),
        tenant: tenant.into(),
        sessions: SessionsClaim::List(sessions.iter().map(|s| (*s).to_string()).collect()),
        channels: None,
        exp: now_secs() + ttl_secs,
    };
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret),
    )
    .expect("encode hmac jwt for test")
}

fn parse_sse_block(block: &str) -> SseFrame {
    let mut event_name = None;
    let mut data = String::new();
    for line in block.lines() {
        if line.starts_with(':') {
            // Comment line (axum keep-alive uses `:`).
            continue;
        }
        if let Some(rest) = line.strip_prefix("event:") {
            event_name = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
        }
    }
    SseFrame { event_name, data }
}
