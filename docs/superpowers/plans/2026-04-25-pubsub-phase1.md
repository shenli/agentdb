# Pub/Sub Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship durable, browser-direct, replay-capable session-event subscriptions on AgentDB — equivalent of Supabase Realtime's Postgres Changes pillar, with native `from_seq` replay as the structural differentiator.

**Architecture:** Reuse the durable session event log as the change feed. Add a process-wide `EventBus` (async-broadcast channel) emitted from the success path of `append_event`. New `GET /api/tenants/{tenant}/sessions/{session}/subscribe` and `GET /api/tenants/{tenant}/subscribe` SSE endpoints serve catch-up (durable scan from `from_seq`) then live tail. Browser-direct via JWT auth (HMAC or JWKS), per-tenant CORS, dual transport (Authorization header + query param), with JS/TS and Python SDKs that auto-reconnect-with-`from_seq` on JWT expiry.

**Tech Stack:** Rust (axum, tokio, async-broadcast, jsonwebtoken, jsonwebtoken-jwks or reqwest+manual JWKS); TypeScript (fetch streaming, EventSource fallback); Python (httpx + httpx-sse).

**Spec:** [`docs/superpowers/specs/2026-04-25-pubsub-design.md`](../specs/2026-04-25-pubsub-design.md) — read before starting any task.

**Branch:** `feature/pubsub` (this worktree).

---

## File Structure

### Created (Rust)

| File | Responsibility |
|---|---|
| `crates/agentdb-api/src/event_bus.rs` | Process-wide broadcast channel for user-event appends; `EventEnvelope`, `EventBus` |
| `crates/agentdb-api/src/subscribe.rs` | `GET .../subscribe` SSE handler — catch-up + live tail + lag policy |
| `crates/agentdb-api/src/jwt.rs` | Per-tenant JWT verification (HMAC + JWKS), claim validation |
| `crates/agentdb-api/src/cors.rs` | Per-tenant CORS layer + OPTIONS preflight for subscribe routes |
| `crates/agentdb-api/tests/subscribe_live.rs` | Integration: live-tail correctness |
| `crates/agentdb-api/tests/subscribe_replay.rs` | Integration: from_seq catch-up correctness |
| `crates/agentdb-api/tests/subscribe_lag.rs` | Integration: lag-too-far → SSE error close |
| `crates/agentdb-api/tests/subscribe_tenant.rs` | Integration: tenant-wide endpoint |
| `crates/agentdb-api/tests/subscribe_redirect.rs` | Integration: multi-node 421 redirect |
| `crates/agentdb-api/tests/subscribe_jwt.rs` | Integration: HMAC, JWKS, expiry, claim validation |
| `crates/agentdb-api/tests/subscribe_cors.rs` | Integration: CORS allowed-origin echo + OPTIONS |

### Modified (Rust)

| File | Change |
|---|---|
| `crates/agentdb-api/src/lib.rs` | Add `event_bus`, `subscribe`, `jwt`, `cors` modules; add `event_bus: EventBus` and `jwt_config: JwtConfig` to `AppState`; mount subscribe routes |
| `crates/agentdb-api/src/handlers.rs` | After `append_event` success in `append_and_fetch`, emit to `EventBus` |
| `crates/agentdb-api/src/fork.rs` | After fork-time event append (if any), emit to `EventBus` |
| `crates/agentdb-api/src/error.rs` | Add `JwtExpired`, `JwtInvalid`, `SessionNotInJwt`, `LagTooFar`, `OriginNotAllowed` error variants |
| `crates/agentdb-api/Cargo.toml` | Add `jsonwebtoken`, `reqwest` (for JWKS fetch), `tower-http` (for CORS layer if not already present) |
| `crates/agentdb-core/src/primitives.rs` | Add `fetch_events_from_seq` helper returning events with `seq ∈ [from_seq, end_seq)` |
| `bin/agentdb-server/src/main.rs` | Read JWT + CORS env vars per tenant at boot; pass into `AppState` |

### Created (SDKs + examples + docs)

| File | Responsibility |
|---|---|
| `clients/python/pyproject.toml` | Python package metadata |
| `clients/python/src/agentdb/__init__.py` | Package entry, version |
| `clients/python/src/agentdb/client.py` | `AgentDB` client class with `subscribe()` (sync + async), JWT refresh callback |
| `clients/python/src/agentdb/_sse.py` | SSE line parser shared by both transports |
| `clients/python/tests/test_subscribe.py` | Smoke + unit tests against a live server |
| `clients/typescript/package.json` | NPM package metadata |
| `clients/typescript/src/index.ts` | `AgentDB` client class + `subscribe()` returning `AsyncIterable<Event>` |
| `clients/typescript/src/sse.ts` | `fetch`-streaming SSE parser; `EventSource` fallback for query-param mode |
| `clients/typescript/test/subscribe.test.ts` | Smoke test against a live server |
| `examples/browser/index.html` | Minimal browser-direct demo |
| `examples/browser/mint_token.py` | HS256 JWT mint helper for the demo (no external auth provider needed) |
| `examples/agents/python/quickstart.py` | Multi-agent demo (Pattern 2 from spec § 2): producer + reactor |
| `docs/runbooks/jwt_expired.md` | Runbook for `jwt_expired` error |
| `docs/runbooks/jwt_invalid.md` | Runbook for `jwt_invalid` error |
| `docs/runbooks/session_not_in_jwt.md` | Runbook for `session_not_in_jwt` error |
| `docs/runbooks/lag_too_far.md` | Runbook for `lag_too_far` error |
| `docs/runbooks/origin_not_allowed.md` | Runbook for `origin_not_allowed` error |

### Modified (docs)

| File | Change |
|---|---|
| `README.md` | New "Real-time subscriptions" section under "API tour" with browser quickstart |
| `docs/agent-db.md` | Add subscribe to agent-native primitive list |
| `docs/ARCHITECTURE.md` | Section: EventBus + subscribe flow; JWT verification flow |

---

## Tasks

Tasks are ordered for dependency: engine first, then surface, then SDKs, then docs. Each task ends in a commit. Tasks 1–10 are Rust; 11–13 are SDKs/examples; 14 is docs.

---

### Task 1: EventBus module

Pure new module. No wiring yet — Task 2 wires it in.

**Files:**
- Create: `crates/agentdb-api/src/event_bus.rs`
- Test: same file (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing test**

Add `event_bus.rs` with this content (test-only, no impl yet):

```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EventEnvelope {
    pub tenant: String,
    pub session: String,
    pub seq: u64,
    pub ts_ms: u64,
    pub hlc_ts: agentdb_core::hlc::HlcTimestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub payload: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_env(seq: u64) -> EventEnvelope {
        EventEnvelope {
            tenant: "t-1".into(),
            session: "s-1".into(),
            seq,
            ts_ms: 1_000 + seq,
            hlc_ts: agentdb_core::hlc::HlcTimestamp { physical_ns: 0, logical: 0 },
            actor_id: None,
            kind: Some("message".into()),
            payload: serde_json::json!({"text": format!("evt-{seq}")}),
        }
    }

    #[tokio::test]
    async fn emit_then_receive_in_order() {
        let bus = EventBus::new(64);
        let mut rx = bus.subscribe();
        bus.emit(make_env(1));
        bus.emit(make_env(2));
        bus.emit(make_env(3));
        let a = rx.recv().await.unwrap();
        let b = rx.recv().await.unwrap();
        let c = rx.recv().await.unwrap();
        assert_eq!(a.seq, 1);
        assert_eq!(b.seq, 2);
        assert_eq!(c.seq, 3);
    }

    #[tokio::test]
    async fn slow_subscriber_lags_when_capacity_exhausted() {
        let bus = EventBus::new(2);
        let mut rx = bus.subscribe();
        for i in 1..=5 {
            bus.emit(make_env(i));
        }
        let mut errors = 0;
        for _ in 0..5 {
            match rx.try_recv() {
                Ok(_) => {}
                Err(async_broadcast::TryRecvError::Overflowed(_)) => errors += 1,
                Err(_) => break,
            }
        }
        assert!(errors >= 1, "expected at least one Overflowed error");
    }

    #[test]
    fn envelope_round_trips_through_json() {
        let env = make_env(42);
        let s = serde_json::to_string(&env).unwrap();
        let back: EventEnvelope = serde_json::from_str(&s).unwrap();
        assert_eq!(env, back);
    }
}
```

- [ ] **Step 2: Run test, expect compilation failure**

```bash
cargo test -p agentdb-api event_bus
```
Expected: compile error (no `EventBus` defined).

- [ ] **Step 3: Implement `EventBus`**

Add to `crates/agentdb-api/src/event_bus.rs` above the test module:

```rust
use async_broadcast::{Receiver, Sender, TryRecvError};

#[derive(Clone)]
pub struct EventBus {
    tx: Sender<EventEnvelope>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (mut tx, rx) = async_broadcast::broadcast(capacity);
        tx.set_overflow(true);
        std::mem::forget(rx);
        Self { tx }
    }

    pub fn subscribe(&self) -> Receiver<EventEnvelope> {
        self.tx.new_receiver()
    }

    pub fn emit(&self, env: EventEnvelope) {
        let _ = self.tx.try_broadcast(env);
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new(16384)
    }
}

pub fn envelope_from_event(
    tenant: &str,
    session: &str,
    event: &agentdb_core::types::Event,
) -> EventEnvelope {
    let kind = event
        .payload
        .get("kind")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    EventEnvelope {
        tenant: tenant.to_string(),
        session: session.to_string(),
        seq: event.seq,
        ts_ms: event.ts,
        hlc_ts: event.hlc_ts.clone(),
        actor_id: event.actor_id.clone(),
        kind,
        payload: event.payload.clone(),
    }
}
```

Add module to `crates/agentdb-api/src/lib.rs` near the other `mod` declarations:

```rust
pub mod event_bus;
```

- [ ] **Step 4: Run test, expect PASS**

```bash
cargo test -p agentdb-api event_bus
```
Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-api/src/event_bus.rs crates/agentdb-api/src/lib.rs
git commit -m "feat(pubsub): add EventBus broadcast channel + envelope type"
```

---

### Task 2: Wire EventBus into AppState; emit on `append_and_fetch` success

**Files:**
- Modify: `crates/agentdb-api/src/lib.rs` (add field to `AppState`; constructors take `EventBus`)
- Modify: `crates/agentdb-api/src/handlers.rs` (emit after `append_event` returns Ok)
- Modify: `crates/agentdb-api/src/fork.rs` (emit if fork creates an event)
- Modify: `bin/agentdb-server/src/main.rs` (construct and pass `EventBus::default()`)
- Test: `crates/agentdb-api/tests/event_bus_emit.rs` (new integration test)

- [ ] **Step 1: Write the failing integration test**

Create `crates/agentdb-api/tests/event_bus_emit.rs`:

```rust
//! Verify that a successful append_and_fetch causes an EventBus emission
//! whose envelope matches the appended event.

use agentdb_api::event_bus::{EventBus, EventEnvelope};

mod common;
use common::TestServer;

#[tokio::test]
async fn append_emits_envelope_with_matching_seq_and_payload() {
    let server = TestServer::start().await;
    let mut rx = server.state.event_bus.subscribe();

    let resp = server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            serde_json::json!({"event": {"kind": "message", "text": "hi"}, "n": 0}),
        )
        .await;
    assert_eq!(resp.status(), 200);

    let env = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv())
        .await
        .expect("envelope within 500ms")
        .expect("recv ok");

    assert_eq!(env.tenant, "t-1");
    assert_eq!(env.session, "s-1");
    assert_eq!(env.seq, 1);
    assert_eq!(env.kind.as_deref(), Some("message"));
    assert_eq!(env.payload["text"], "hi");
}

#[tokio::test]
async fn emission_happens_after_durable_write() {
    // Subscribe; submit append; verify by GET that the event exists in
    // storage at or before the time the subscriber receives the envelope.
    let server = TestServer::start().await;
    let mut rx = server.state.event_bus.subscribe();
    server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            serde_json::json!({"event": {"kind": "x"}, "n": 0}),
        )
        .await;
    let env = rx.recv().await.unwrap();
    let resp = server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            serde_json::json!({"event": {"kind": "probe"}, "n": 10}),
        )
        .await;
    let body: serde_json::Value = resp.json().await;
    let last = body["events"].as_array().unwrap();
    assert!(last.iter().any(|e| e["seq"] == env.seq));
}
```

Create the `common` test helper at `crates/agentdb-api/tests/common/mod.rs` if it doesn't exist:

```rust
use agentdb_api::{router, AppState};
use agentdb_api::event_bus::EventBus;
use agentdb_api::stream::StreamBus;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct TestServer {
    pub state: AppState,
    pub addr: SocketAddr,
    _join: tokio::task::JoinHandle<()>,
    client: reqwest::Client,
}

impl TestServer {
    pub async fn start() -> Self {
        let kv = Arc::new(agentdb_storage::MemKvStore::default()) as Arc<dyn agentdb_storage::KvStore>;
        let event_bus = EventBus::new(1024);
        let stream = StreamBus::default();
        let state = AppState::no_auth(/* ... existing ctor args ... */ kv.clone(), stream, event_bus.clone());
        let app = router(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let join = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        TestServer { state, addr, _join: join, client: reqwest::Client::new() }
    }

    pub async fn post_json(&self, path: &str, body: serde_json::Value) -> reqwest::Response {
        self.client
            .post(format!("http://{}{}", self.addr, path))
            .json(&body)
            .send()
            .await
            .unwrap()
    }
}
```

(Adjust the `AppState::no_auth` ctor call to match the existing signature once you read it; this is the shape.)

- [ ] **Step 2: Run, expect compile failure**

```bash
cargo test -p agentdb-api event_bus_emit
```
Expected: `AppState` has no `event_bus` field; `no_auth` has wrong arity.

- [ ] **Step 3: Add `event_bus: EventBus` to `AppState`; thread through constructors**

In `crates/agentdb-api/src/lib.rs`:

```rust
use crate::event_bus::EventBus;

pub struct AppState {
    // ... existing fields ...
    pub stream: StreamBus,
    pub event_bus: EventBus,
}

impl AppState {
    pub fn no_auth(/* existing args */ stream: StreamBus, event_bus: EventBus) -> Self {
        Self { /* ... */ stream, event_bus }
    }
    // Update other constructors similarly.
}
```

In `bin/agentdb-server/src/main.rs`, where `AppState` is constructed, instantiate `EventBus::default()` and pass it.

- [ ] **Step 4: Emit from `append_and_fetch` after durable write succeeds**

In `crates/agentdb-api/src/handlers.rs`, locate the success path of `append_and_fetch` (the point after `append_event` returns `Ok(event)` and before constructing the response). Insert:

```rust
let env = crate::event_bus::envelope_from_event(&tenant_external_id, &session_external_id, &event);
state.event_bus.emit(env);
```

Names must match the locals in scope; `tenant_external_id` and `session_external_id` are the path parameters.

- [ ] **Step 5: Emit from `fork.rs` if fork creates an event**

In `crates/agentdb-api/src/fork.rs`, locate any place that calls `append_event` (e.g. a fork-marker event in the child session). After success, emit using the child session's IDs. If fork does not append, skip.

- [ ] **Step 6: Run tests, expect PASS**

```bash
cargo test -p agentdb-api event_bus_emit
cargo test --workspace
```
Expected: new tests pass; no regressions.

- [ ] **Step 7: Commit**

```bash
git add crates/agentdb-api/src/lib.rs crates/agentdb-api/src/handlers.rs crates/agentdb-api/src/fork.rs bin/agentdb-server/src/main.rs crates/agentdb-api/tests/event_bus_emit.rs crates/agentdb-api/tests/common/mod.rs
git commit -m "feat(pubsub): emit EventBus envelope on append_and_fetch success"
```

---

### Task 3: `fetch_events_from_seq` helper in `primitives.rs`

Generalizes the existing `fetch_last_n` to a half-open range. Used by Task 5's catch-up scan.

**Files:**
- Modify: `crates/agentdb-core/src/primitives.rs`
- Modify: `crates/agentdb-core/src/lib.rs` (re-export)
- Test: same file (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing tests**

Append to `crates/agentdb-core/src/primitives.rs` test module:

```rust
#[test]
fn fetch_events_from_seq_returns_half_open_range() {
    let (kv, mut session) = test_session_with_events(10);
    let lock = test_tenant_lock();
    let ctx = ReadCtx::open(&kv).unwrap();
    let events = fetch_events_from_seq(&ctx, &session, 3, 7).unwrap();
    let seqs: Vec<u64> = events.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, vec![3, 4, 5, 6]);
}

#[test]
fn fetch_events_from_seq_empty_when_lower_eq_upper() {
    let (kv, session) = test_session_with_events(5);
    let ctx = ReadCtx::open(&kv).unwrap();
    let events = fetch_events_from_seq(&ctx, &session, 3, 3).unwrap();
    assert!(events.is_empty());
}

#[test]
fn fetch_events_from_seq_clamps_upper_to_next_seq() {
    let (kv, session) = test_session_with_events(5);
    let ctx = ReadCtx::open(&kv).unwrap();
    let events = fetch_events_from_seq(&ctx, &session, 1, u64::MAX).unwrap();
    assert_eq!(events.len(), 5);
}

#[test]
fn fetch_events_from_seq_returns_empty_for_lower_above_next_seq() {
    let (kv, session) = test_session_with_events(3);
    let ctx = ReadCtx::open(&kv).unwrap();
    let events = fetch_events_from_seq(&ctx, &session, 100, u64::MAX).unwrap();
    assert!(events.is_empty());
}
```

(`test_session_with_events` and `test_tenant_lock` are existing helpers in this test module — if not, write them mirroring the existing `fetch_last_n` test setup.)

- [ ] **Step 2: Run, expect FAIL**

```bash
cargo test -p agentdb-core primitives::tests::fetch_events_from_seq
```
Expected: function not defined.

- [ ] **Step 3: Implement**

Add to `crates/agentdb-core/src/primitives.rs`:

```rust
/// Return events with `seq ∈ [from_seq, end_seq)` in ascending order.
/// `end_seq` is clamped to `session.next_seq`. Returns empty vec if
/// `from_seq >= clamped_end`.
pub fn fetch_events_from_seq(
    ctx: &crate::read_ctx::ReadCtx,
    session: &Session,
    from_seq: u64,
    end_seq: u64,
) -> Result<Vec<Event>> {
    let clamped_end = end_seq.min(session.next_seq);
    if from_seq >= clamped_end {
        return Ok(Vec::new());
    }
    let prefix = encode_event_prefix(
        session.box_id,
        session.tenant_internal_id,
        session.internal_id,
        DEFAULT_BRANCH_ID,
    );
    let mut out = Vec::new();
    for kv_pair in ctx.scan_prefix(&prefix)? {
        let (key, value) = kv_pair?;
        let seq = decode_seq_from_event_key(&key);
        if seq < from_seq {
            continue;
        }
        if seq >= clamped_end {
            break;
        }
        let event: Event = serde_json::from_slice(&value)?;
        out.push(event);
    }
    Ok(out)
}
```

(`encode_event_prefix` and `decode_seq_from_event_key` already exist in the module per the `fetch_last_n` impl — reuse them; if not, the keys are `Namespace::Events / box_id / tenant / session / branch / seq` per the existing `encode_key` call.)

- [ ] **Step 4: Run, expect PASS**

```bash
cargo test -p agentdb-core primitives
```

- [ ] **Step 5: Re-export and commit**

In `crates/agentdb-core/src/lib.rs`, add `pub use primitives::fetch_events_from_seq;` if `fetch_last_n` is similarly re-exported.

```bash
git add crates/agentdb-core/src/primitives.rs crates/agentdb-core/src/lib.rs
git commit -m "feat(core): fetch_events_from_seq half-open range helper"
```

---

### Task 4: Per-session subscribe endpoint — basic live tail

JWT auth comes in Task 9. This task uses the existing tenant Basic auth so we can verify the SSE machinery in isolation.

**Files:**
- Create: `crates/agentdb-api/src/subscribe.rs`
- Modify: `crates/agentdb-api/src/lib.rs` (mount route)
- Test: `crates/agentdb-api/tests/subscribe_live.rs`

- [ ] **Step 1: Write the failing integration test**

Create `crates/agentdb-api/tests/subscribe_live.rs`:

```rust
mod common;
use common::TestServer;
use futures_util::StreamExt;

#[tokio::test]
async fn subscribe_receives_appended_events_in_order() {
    let server = TestServer::start().await;

    let mut sse = server
        .open_sse("/api/tenants/t-1/sessions/s-1/subscribe")
        .await;

    // First frame is the `subscribed` control event
    let first = sse.next_event().await;
    assert_eq!(first.event_name.as_deref(), Some("subscribed"));

    // Append three events
    for i in 1..=3 {
        server
            .post_json(
                "/api/tenants/t-1/sessions/s-1/append_and_fetch",
                serde_json::json!({"event": {"kind": "msg", "n": i}, "n": 0}),
            )
            .await;
    }

    let mut received = Vec::new();
    for _ in 0..3 {
        let evt = sse.next_event().await;
        let env: serde_json::Value = serde_json::from_str(&evt.data).unwrap();
        received.push(env["seq"].as_u64().unwrap());
    }
    assert_eq!(received, vec![1, 2, 3]);
}

#[tokio::test]
async fn subscribe_filters_by_kind() {
    let server = TestServer::start().await;
    let mut sse = server
        .open_sse("/api/tenants/t-1/sessions/s-1/subscribe?kind=keep")
        .await;
    sse.skip_subscribed_event().await;
    server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            serde_json::json!({"event": {"kind": "skip"}, "n": 0}),
        )
        .await;
    server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            serde_json::json!({"event": {"kind": "keep"}, "n": 0}),
        )
        .await;
    let evt = sse.next_event().await;
    let env: serde_json::Value = serde_json::from_str(&evt.data).unwrap();
    assert_eq!(env["kind"], "keep");
}
```

Extend `tests/common/mod.rs` with:

```rust
pub struct SseStream {
    body: reqwest::Response,
    buf: String,
}

pub struct SseFrame {
    pub event_name: Option<String>,
    pub data: String,
}

impl TestServer {
    pub async fn open_sse(&self, path: &str) -> SseStream {
        let resp = self
            .client
            .get(format!("http://{}{}", self.addr, path))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        SseStream { body: resp, buf: String::new() }
    }
}

impl SseStream {
    pub async fn next_event(&mut self) -> SseFrame {
        loop {
            if let Some(idx) = self.buf.find("\n\n") {
                let block = self.buf[..idx].to_string();
                self.buf.drain(..idx + 2);
                return parse_sse_block(&block);
            }
            let chunk = self
                .body
                .chunk()
                .await
                .unwrap()
                .expect("sse stream ended");
            self.buf.push_str(std::str::from_utf8(&chunk).unwrap());
        }
    }
    pub async fn skip_subscribed_event(&mut self) {
        let _ = self.next_event().await;
    }
}

fn parse_sse_block(block: &str) -> SseFrame {
    let mut event_name = None;
    let mut data = String::new();
    for line in block.lines() {
        if let Some(rest) = line.strip_prefix("event:") {
            event_name = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.trim_start());
        }
    }
    SseFrame { event_name, data }
}
```

- [ ] **Step 2: Run, expect compile/route failure**

```bash
cargo test -p agentdb-api subscribe_live
```
Expected: `subscribe` module/route not found.

- [ ] **Step 3: Implement subscribe handler (live tail only)**

Create `crates/agentdb-api/src/subscribe.rs`:

```rust
use std::convert::Infallible;

use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::stream::Stream;
use futures_util::StreamExt;
use serde::Deserialize;

use crate::event_bus::EventEnvelope;
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct SubscribeQuery {
    #[serde(default)]
    pub from_seq: Option<u64>,
    #[serde(default)]
    pub kind: Vec<String>,
    #[serde(default)]
    pub heartbeat_secs: Option<u64>,
}

pub async fn subscribe_session(
    State(state): State<AppState>,
    Path((tenant, session)): Path<(String, String)>,
    Query(q): Query<SubscribeQuery>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.event_bus.subscribe();
    let live_seq: u64 = lookup_session_next_seq(&state, &tenant, &session)
        .await
        .unwrap_or(0);

    let subscribed_payload = serde_json::json!({
        "tenant": tenant.clone(),
        "session": session.clone(),
        "live_seq": live_seq,
    });
    let subscribed_evt = futures_util::stream::once(async move {
        Ok::<_, Infallible>(
            Event::default()
                .event("subscribed")
                .data(subscribed_payload.to_string()),
        )
    });

    let kind_filter = q.kind.clone();
    let want_tenant = tenant.clone();
    let want_session = session.clone();
    let live = futures_util::stream::unfold(rx, move |mut rx| {
        let kind_filter = kind_filter.clone();
        let want_tenant = want_tenant.clone();
        let want_session = want_session.clone();
        async move {
            loop {
                match rx.recv().await {
                    Ok(env) => {
                        if env.tenant != want_tenant || env.session != want_session {
                            continue;
                        }
                        if !kind_filter.is_empty() {
                            let kind_ok = env
                                .kind
                                .as_ref()
                                .map(|k| kind_filter.iter().any(|f| f == k))
                                .unwrap_or(false);
                            if !kind_ok {
                                continue;
                            }
                        }
                        let payload = serde_json::to_string(&env).unwrap();
                        return Some((Ok(Event::default().data(payload)), rx));
                    }
                    Err(_) => return None,
                }
            }
        }
    });

    let stream = subscribed_evt.chain(live);
    let keepalive_secs = q.heartbeat_secs.unwrap_or(20);
    Sse::new(stream).keep_alive(
        KeepAlive::new().interval(std::time::Duration::from_secs(keepalive_secs)),
    )
}

async fn lookup_session_next_seq(
    state: &AppState,
    tenant: &str,
    session: &str,
) -> Option<u64> {
    // Use existing primitives — read tenant + session, return next_seq.
    // If either does not exist, return None.
    // Mirror the read path in handlers.rs::append_and_fetch.
    // Intentionally NOT a hard error — a subscriber may attach before the
    // first event is ever appended; live_seq=0 is correct in that case.
    crate::handlers::resolve_session_next_seq(state, tenant, session).await
}
```

Add `crate::handlers::resolve_session_next_seq` as a thin helper around the existing tenant/session lookup; or factor out from `append_and_fetch`. Pseudocode:

```rust
pub async fn resolve_session_next_seq(
    state: &AppState,
    tenant_external_id: &str,
    session_external_id: &str,
) -> Option<u64> {
    let registry = state.registry.clone();
    let tenant = registry.lookup_tenant(tenant_external_id).await.ok()??;
    let session = registry.lookup_session(tenant.internal_id, session_external_id).await.ok()??;
    Some(session.next_seq)
}
```

(Names must match the actual registry API in the codebase.)

In `crates/agentdb-api/src/lib.rs`:

```rust
pub mod subscribe;

// In tenant_routes:
.route(
    "/api/tenants/:tenant_id/sessions/:session_id/subscribe",
    axum::routing::get(subscribe::subscribe_session),
)
```

- [ ] **Step 4: Run, expect PASS**

```bash
cargo test -p agentdb-api subscribe_live
```

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-api/src/subscribe.rs crates/agentdb-api/src/lib.rs crates/agentdb-api/src/handlers.rs crates/agentdb-api/tests/subscribe_live.rs crates/agentdb-api/tests/common/mod.rs
git commit -m "feat(pubsub): per-session subscribe endpoint with live tail + kind filter"
```

---

### Task 5: `from_seq` replay (catch-up + live, gap-free)

**Files:**
- Modify: `crates/agentdb-api/src/subscribe.rs` (add catch-up phase)
- Test: `crates/agentdb-api/tests/subscribe_replay.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/agentdb-api/tests/subscribe_replay.rs`:

```rust
mod common;
use common::TestServer;

#[tokio::test]
async fn from_seq_replays_then_tails_with_no_gap() {
    let server = TestServer::start().await;

    // Append 5 events (seq 1..=5) BEFORE subscribing.
    for i in 1..=5 {
        server
            .post_json(
                "/api/tenants/t-1/sessions/s-1/append_and_fetch",
                serde_json::json!({"event": {"kind": "msg", "n": i}, "n": 0}),
            )
            .await;
    }

    let mut sse = server
        .open_sse("/api/tenants/t-1/sessions/s-1/subscribe?from_seq=3")
        .await;
    let first = sse.next_event().await;
    assert_eq!(first.event_name.as_deref(), Some("subscribed"));
    let payload: serde_json::Value = serde_json::from_str(&first.data).unwrap();
    assert_eq!(payload["live_seq"], 6);

    // Catch-up: should receive seq 3, 4, 5
    let mut seqs = Vec::new();
    for _ in 0..3 {
        let e = sse.next_event().await;
        let env: serde_json::Value = serde_json::from_str(&e.data).unwrap();
        seqs.push(env["seq"].as_u64().unwrap());
    }
    assert_eq!(seqs, vec![3, 4, 5]);

    // Now append 2 more — should arrive via live tail
    for i in 6..=7 {
        server
            .post_json(
                "/api/tenants/t-1/sessions/s-1/append_and_fetch",
                serde_json::json!({"event": {"kind": "msg", "n": i}, "n": 0}),
            )
            .await;
    }
    for expected in 6..=7u64 {
        let e = sse.next_event().await;
        let env: serde_json::Value = serde_json::from_str(&e.data).unwrap();
        assert_eq!(env["seq"].as_u64().unwrap(), expected);
    }
}

#[tokio::test]
async fn race_between_catchup_and_live_does_not_duplicate_or_drop() {
    let server = TestServer::start().await;

    for i in 1..=3 {
        server
            .post_json(
                "/api/tenants/t-1/sessions/s-1/append_and_fetch",
                serde_json::json!({"event": {"kind": "msg", "n": i}, "n": 0}),
            )
            .await;
    }
    let mut sse = server
        .open_sse("/api/tenants/t-1/sessions/s-1/subscribe?from_seq=1")
        .await;
    sse.skip_subscribed_event().await;

    // Append concurrently — events 4, 5 must appear via live tail without
    // duplicating 1, 2, 3 from catch-up.
    let post_handle = {
        let server = server.clone();
        tokio::spawn(async move {
            for i in 4..=5 {
                server
                    .post_json(
                        "/api/tenants/t-1/sessions/s-1/append_and_fetch",
                        serde_json::json!({"event": {"kind": "msg", "n": i}, "n": 0}),
                    )
                    .await;
            }
        })
    };
    post_handle.await.unwrap();

    let mut all_seqs = Vec::new();
    for _ in 0..5 {
        let e = sse.next_event().await;
        let env: serde_json::Value = serde_json::from_str(&e.data).unwrap();
        all_seqs.push(env["seq"].as_u64().unwrap());
    }
    all_seqs.sort();
    assert_eq!(all_seqs, vec![1, 2, 3, 4, 5]);
}
```

(Add `#[derive(Clone)]` to `TestServer` if not present, since the second test clones it for the spawn.)

- [ ] **Step 2: Run, expect catch-up failures**

```bash
cargo test -p agentdb-api subscribe_replay
```
Expected: from_seq is ignored; only live events arrive.

- [ ] **Step 3: Implement catch-up phase in `subscribe.rs`**

The race-safe pattern: subscribe to bus FIRST, then snapshot `live_seq`, then run catch-up scan over `[from_seq, live_seq)`, then start live tail filtering out `seq < live_seq`. Replace the body of `subscribe_session`:

```rust
pub async fn subscribe_session(
    State(state): State<AppState>,
    Path((tenant, session)): Path<(String, String)>,
    Query(q): Query<SubscribeQuery>,
) -> axum::response::Response {
    // 1. Attach to bus FIRST so events appended after this line are buffered.
    let rx = state.event_bus.subscribe();

    // 2. Snapshot live_seq.
    let live_seq = match crate::handlers::resolve_session_next_seq(&state, &tenant, &session).await {
        Some(s) => s,
        None => 0,
    };

    // 3. Run catch-up scan if from_seq provided.
    let from_seq = q.from_seq.unwrap_or(live_seq);
    let catchup_events: Vec<EventEnvelope> = if from_seq < live_seq {
        match crate::handlers::scan_events_from_seq(&state, &tenant, &session, from_seq, live_seq).await {
            Ok(events) => events
                .into_iter()
                .map(|ev| crate::event_bus::envelope_from_event(&tenant, &session, &ev))
                .collect(),
            Err(_) => Vec::new(),
        }
    } else {
        Vec::new()
    };

    // 4. Build the SSE stream: subscribed control + catch-up + filtered live.
    let subscribed_payload = serde_json::json!({
        "tenant": tenant.clone(),
        "session": session.clone(),
        "live_seq": live_seq,
    });
    let subscribed_evt = futures_util::stream::once(async move {
        Ok::<_, Infallible>(
            Event::default().event("subscribed").data(subscribed_payload.to_string()),
        )
    });

    let kind_filter = q.kind.clone();
    let catch_up_stream = futures_util::stream::iter(catchup_events.into_iter().filter_map({
        let kind_filter = kind_filter.clone();
        move |env| {
            if !kind_matches(&env, &kind_filter) {
                return None;
            }
            let s = serde_json::to_string(&env).ok()?;
            Some(Ok::<_, Infallible>(Event::default().data(s)))
        }
    }));

    let want_tenant = tenant.clone();
    let want_session = session.clone();
    let live = futures_util::stream::unfold((rx, live_seq), move |(mut rx, live_seq)| {
        let kind_filter = kind_filter.clone();
        let want_tenant = want_tenant.clone();
        let want_session = want_session.clone();
        async move {
            loop {
                match rx.recv().await {
                    Ok(env) => {
                        if env.tenant != want_tenant || env.session != want_session {
                            continue;
                        }
                        if env.seq < live_seq {
                            continue; // Already delivered via catch-up scan.
                        }
                        if !kind_matches(&env, &kind_filter) {
                            continue;
                        }
                        let payload = serde_json::to_string(&env).unwrap();
                        return Some((Ok(Event::default().data(payload)), (rx, live_seq)));
                    }
                    Err(_) => return None,
                }
            }
        }
    });

    let stream = subscribed_evt.chain(catch_up_stream).chain(live);
    let keepalive_secs = q.heartbeat_secs.unwrap_or(20);
    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(keepalive_secs)))
        .into_response()
}

fn kind_matches(env: &EventEnvelope, kind_filter: &[String]) -> bool {
    if kind_filter.is_empty() {
        return true;
    }
    env.kind.as_ref().map_or(false, |k| kind_filter.iter().any(|f| f == k))
}
```

Add `scan_events_from_seq` to `handlers.rs`:

```rust
pub async fn scan_events_from_seq(
    state: &AppState,
    tenant_external_id: &str,
    session_external_id: &str,
    from_seq: u64,
    end_seq: u64,
) -> Result<Vec<agentdb_core::types::Event>, ApiError> {
    // Resolve tenant + session, open ReadCtx, call fetch_events_from_seq.
    // Mirror existing read paths.
}
```

- [ ] **Step 4: Run, expect PASS**

```bash
cargo test -p agentdb-api subscribe_replay
```

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-api/src/subscribe.rs crates/agentdb-api/src/handlers.rs crates/agentdb-api/tests/subscribe_replay.rs
git commit -m "feat(pubsub): from_seq catch-up scan + race-free live tail"
```

---

### Task 6: Lag-too-far policy

When the broadcast receiver overflows (slow subscriber), close with an SSE error event so the client reconnects with `from_seq`.

**Files:**
- Modify: `crates/agentdb-api/src/subscribe.rs`
- Modify: `crates/agentdb-api/src/error.rs` (add `LagTooFar`)
- Test: `crates/agentdb-api/tests/subscribe_lag.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/agentdb-api/tests/subscribe_lag.rs`:

```rust
mod common;
use common::TestServer;

#[tokio::test]
async fn slow_subscriber_receives_lag_too_far_then_close() {
    // Build a server with EventBus capacity = 4 to force lag quickly.
    let server = TestServer::start_with_event_bus_capacity(4).await;

    let mut sse = server
        .open_sse("/api/tenants/t-1/sessions/s-1/subscribe")
        .await;
    sse.skip_subscribed_event().await;

    // Read the first event then stall — let the bus overflow.
    server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            serde_json::json!({"event": {"kind": "msg", "n": 1}, "n": 0}),
        )
        .await;
    let first = sse.next_event().await;
    let env: serde_json::Value = serde_json::from_str(&first.data).unwrap();
    let last_delivered = env["seq"].as_u64().unwrap();
    assert_eq!(last_delivered, 1);

    // Drown the bus.
    for i in 2..=20 {
        server
            .post_json(
                "/api/tenants/t-1/sessions/s-1/append_and_fetch",
                serde_json::json!({"event": {"kind": "msg", "n": i}, "n": 0}),
            )
            .await;
    }

    // Drain whatever the receiver has, expect to eventually see an `error` frame.
    let mut saw_error = false;
    for _ in 0..30 {
        let frame = sse.next_event().await;
        if frame.event_name.as_deref() == Some("error") {
            let body: serde_json::Value = serde_json::from_str(&frame.data).unwrap();
            assert_eq!(body["error"], "lag_too_far");
            assert!(body["last_delivered_seq"].as_u64().unwrap() >= 1);
            saw_error = true;
            break;
        }
    }
    assert!(saw_error, "expected lag_too_far error frame");
}
```

Add to `tests/common/mod.rs`:

```rust
impl TestServer {
    pub async fn start_with_event_bus_capacity(cap: usize) -> Self {
        // Same as start() but pass EventBus::new(cap) into AppState.
    }
}
```

- [ ] **Step 2: Run, expect FAIL** (no error frame; either hang or normal close).

```bash
cargo test -p agentdb-api subscribe_lag
```

- [ ] **Step 3: Implement lag handling**

In `subscribe.rs`, modify the live-tail `unfold`:

```rust
match rx.recv().await {
    Ok(env) => { /* existing path */ }
    Err(async_broadcast::RecvError::Overflowed(_)) => {
        let payload = serde_json::json!({
            "error": "lag_too_far",
            "last_delivered_seq": last_delivered_seq,
        }).to_string();
        let frame = Event::default().event("error").data(payload);
        return Some((Ok(frame), (rx, live_seq, last_delivered_seq, true)));
        // Tuple state must include a "closed" flag so the next iteration returns None.
    }
    Err(_) => return None,
}
```

Refactor the `unfold` state to a struct so it's readable:

```rust
struct LiveState {
    rx: async_broadcast::Receiver<EventEnvelope>,
    live_seq: u64,
    last_delivered_seq: u64,
    closed: bool,
}
```

Track `last_delivered_seq` by setting it on each successful deliver. When `closed`, the next iteration returns `None` and the SSE stream ends.

- [ ] **Step 4: Run, expect PASS**

```bash
cargo test -p agentdb-api subscribe_lag
```

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-api/src/subscribe.rs crates/agentdb-api/tests/subscribe_lag.rs crates/agentdb-api/tests/common/mod.rs
git commit -m "feat(pubsub): lag-too-far closes connection with SSE error frame"
```

---

### Task 7: Tenant-wide subscribe endpoint

Same shape, but accepts a list of sessions (or all sessions in tenant).

**Files:**
- Modify: `crates/agentdb-api/src/subscribe.rs` (add `subscribe_tenant`)
- Modify: `crates/agentdb-api/src/lib.rs` (mount route)
- Test: `crates/agentdb-api/tests/subscribe_tenant.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/agentdb-api/tests/subscribe_tenant.rs`:

```rust
mod common;
use common::TestServer;
use std::collections::HashMap;

#[tokio::test]
async fn tenant_wide_subscription_delivers_events_from_multiple_sessions() {
    let server = TestServer::start().await;
    let mut sse = server.open_sse("/api/tenants/t-1/subscribe").await;
    sse.skip_subscribed_event().await;

    server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            serde_json::json!({"event": {"kind": "msg"}, "n": 0}),
        )
        .await;
    server
        .post_json(
            "/api/tenants/t-1/sessions/s-2/append_and_fetch",
            serde_json::json!({"event": {"kind": "msg"}, "n": 0}),
        )
        .await;

    let mut by_session: HashMap<String, u64> = HashMap::new();
    for _ in 0..2 {
        let e = sse.next_event().await;
        let env: serde_json::Value = serde_json::from_str(&e.data).unwrap();
        by_session.insert(
            env["session"].as_str().unwrap().into(),
            env["seq"].as_u64().unwrap(),
        );
    }
    assert_eq!(by_session.get("s-1"), Some(&1));
    assert_eq!(by_session.get("s-2"), Some(&1));
}

#[tokio::test]
async fn tenant_wide_with_session_filter_excludes_others() {
    let server = TestServer::start().await;
    let mut sse = server
        .open_sse("/api/tenants/t-1/subscribe?session=s-1")
        .await;
    sse.skip_subscribed_event().await;

    server
        .post_json(
            "/api/tenants/t-1/sessions/s-2/append_and_fetch",
            serde_json::json!({"event": {"kind": "msg"}, "n": 0}),
        )
        .await;
    server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            serde_json::json!({"event": {"kind": "msg"}, "n": 0}),
        )
        .await;
    let evt = sse.next_event().await;
    let env: serde_json::Value = serde_json::from_str(&evt.data).unwrap();
    assert_eq!(env["session"], "s-1");
}
```

- [ ] **Step 2: Run, expect FAIL** (route not mounted).

- [ ] **Step 3: Implement tenant-wide handler**

Add to `subscribe.rs`:

```rust
#[derive(Debug, Deserialize)]
pub struct TenantSubscribeQuery {
    #[serde(default)]
    pub session: Vec<String>,            // optional whitelist
    #[serde(default)]
    pub kind: Vec<String>,
    #[serde(default)]
    pub from_seq_per_session: Option<String>, // "s-1:42,s-2:7"
    #[serde(default)]
    pub heartbeat_secs: Option<u64>,
}

pub async fn subscribe_tenant(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Query(q): Query<TenantSubscribeQuery>,
) -> axum::response::Response {
    let rx = state.event_bus.subscribe();
    let session_filter: Option<std::collections::HashSet<String>> =
        if q.session.is_empty() { None } else { Some(q.session.iter().cloned().collect()) };

    let from_seq_map: std::collections::HashMap<String, u64> =
        parse_from_seq_per_session(q.from_seq_per_session.as_deref());

    // Catch-up per session in the from_seq_map (only those with from_seq).
    let mut catchup: Vec<EventEnvelope> = Vec::new();
    for (session, from_seq) in &from_seq_map {
        if let Some(filter) = &session_filter {
            if !filter.contains(session) { continue; }
        }
        let live_seq = crate::handlers::resolve_session_next_seq(&state, &tenant, session)
            .await
            .unwrap_or(0);
        if *from_seq < live_seq {
            if let Ok(events) = crate::handlers::scan_events_from_seq(&state, &tenant, session, *from_seq, live_seq).await {
                for ev in events {
                    catchup.push(crate::event_bus::envelope_from_event(&tenant, session, &ev));
                }
            }
        }
    }
    catchup.sort_by(|a, b| (a.session.as_str(), a.seq).cmp(&(b.session.as_str(), b.seq)));

    // ... build SSE stream similar to subscribe_session, with the additional
    // per-session live_seq dedup map and tenant/session filter.
}

fn parse_from_seq_per_session(s: Option<&str>) -> std::collections::HashMap<String, u64> {
    let mut out = std::collections::HashMap::new();
    let Some(s) = s else { return out };
    for pair in s.split(',') {
        if let Some((k, v)) = pair.split_once(':') {
            if let Ok(seq) = v.parse() {
                out.insert(k.to_string(), seq);
            }
        }
    }
    out
}
```

In `lib.rs` add the route in `tenant_routes`:

```rust
.route(
    "/api/tenants/:tenant_id/subscribe",
    axum::routing::get(subscribe::subscribe_tenant),
)
```

- [ ] **Step 4: Run, expect PASS**

```bash
cargo test -p agentdb-api subscribe_tenant
```

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-api/src/subscribe.rs crates/agentdb-api/src/lib.rs crates/agentdb-api/tests/subscribe_tenant.rs
git commit -m "feat(pubsub): tenant-wide subscribe endpoint with session + from_seq filters"
```

---

### Task 8: Multi-node 421 redirect

Mirrors the existing pattern from `forwarding.rs`. The subscribe endpoints must check whether *this* node owns the session's tablet; if not, return 421 with the owner's address in the body.

**Files:**
- Modify: `crates/agentdb-api/src/subscribe.rs` (owner check before serving)
- Modify: `crates/agentdb-api/src/error.rs` (add `NotWritablePrimaryForSubscribe` if needed; or reuse existing 421 mechanism)
- Test: `crates/agentdb-api/tests/subscribe_redirect.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/agentdb-api/tests/subscribe_redirect.rs`:

```rust
mod common;
use common::{TestServer, TwoNodeCluster};

#[tokio::test]
async fn subscribe_to_session_owned_by_peer_returns_421_with_owner() {
    let cluster = TwoNodeCluster::start().await;
    cluster.create_session_on_node_b("t-1", "s-1").await;

    let resp = cluster
        .node_a
        .client()
        .get(format!(
            "http://{}/api/tenants/t-1/sessions/s-1/subscribe",
            cluster.node_a.addr
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 421);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["owner_addr"].as_str().unwrap().contains(&cluster.node_b.addr.to_string()));
}

#[tokio::test]
async fn tenant_wide_subscribe_with_sessions_on_multiple_nodes_returns_421_with_owners() {
    let cluster = TwoNodeCluster::start().await;
    cluster.create_session_on_node_a("t-1", "s-a").await;
    cluster.create_session_on_node_b("t-1", "s-b").await;

    let resp = cluster
        .node_a
        .client()
        .get(format!("http://{}/api/tenants/t-1/subscribe", cluster.node_a.addr))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 421);
    let body: serde_json::Value = resp.json().await.unwrap();
    let owners: Vec<String> = serde_json::from_value(body["owner_addrs"].clone()).unwrap();
    assert_eq!(owners.len(), 2);
}
```

Add a `TwoNodeCluster` helper to `tests/common/mod.rs` (or extend the existing one) that mirrors the existing two-node test harness used by `M20 multi-node` tests — search the codebase for `two_node` or `cluster` test fixtures and adapt.

- [ ] **Step 2: Run, expect FAIL** (handler doesn't redirect; serves a stream).

- [ ] **Step 3: Implement owner-check guard**

Read `crates/agentdb-api/src/forwarding.rs` to find the existing helper that resolves a session's owning node. Reuse it:

In `subscribe_session`, before subscribing to the bus:

```rust
match crate::forwarding::resolve_session_owner(&state, &tenant, &session).await {
    Some(owner) if owner.is_local() => { /* serve directly */ }
    Some(owner) => {
        let body = serde_json::json!({
            "error": "not_owner",
            "owner_addr": owner.addr.to_string(),
        });
        return (
            axum::http::StatusCode::MISDIRECTED_REQUEST,
            [(axum::http::header::LOCATION, owner.subscribe_url(&tenant, &session))],
            axum::Json(body),
        ).into_response();
    }
    None => {
        // Session unknown — serve normally (subscriber attaches before first event).
    }
}
```

For `subscribe_tenant`, gather all owners across requested sessions; if any are non-local, return 421 with `owner_addrs` array.

(Names of `resolve_session_owner` and `is_local` must match the actual forwarding API; use the same helper that `handlers::append_and_fetch` uses to forward.)

- [ ] **Step 4: Run, expect PASS**

```bash
cargo test -p agentdb-api subscribe_redirect
```

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-api/src/subscribe.rs crates/agentdb-api/tests/subscribe_redirect.rs crates/agentdb-api/tests/common/mod.rs
git commit -m "feat(pubsub): 421 redirect when subscribed session is on a peer node"
```

---

### Task 9a: JWT verifier (HMAC) module

**Files:**
- Create: `crates/agentdb-api/src/jwt.rs`
- Modify: `crates/agentdb-api/Cargo.toml` (add `jsonwebtoken = "9"`)
- Test: same file (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Add dependency**

In `crates/agentdb-api/Cargo.toml`:

```toml
[dependencies]
# ... existing ...
jsonwebtoken = "9"
reqwest = { workspace = true, features = ["json", "stream", "rustls-tls"] }
```

- [ ] **Step 2: Write the failing tests**

Create `crates/agentdb-api/src/jwt.rs`:

```rust
use std::collections::HashMap;
use std::sync::Arc;

use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub enum TenantJwtConfig {
    Hmac { secret: Vec<u8>, issuer: String, audience: Option<String> },
    Jwks { url: String, issuer: String, audience: Option<String> },
}

#[derive(Debug, Clone, Default)]
pub struct JwtConfig {
    pub per_tenant: HashMap<String, TenantJwtConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AgentDbClaims {
    pub iss: String,
    #[serde(default)]
    pub aud: Option<String>,
    pub sub: String,
    pub tenant: String,
    pub sessions: SessionsClaim,
    pub exp: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub enum SessionsClaim {
    Wildcard(String), // "*"
    List(Vec<String>),
}

impl SessionsClaim {
    pub fn allows(&self, session: &str) -> bool {
        match self {
            SessionsClaim::Wildcard(s) if s == "*" => true,
            SessionsClaim::List(v) => v.iter().any(|s| s == session),
            _ => false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum JwtError {
    #[error("tenant not configured for JWT")]
    TenantNotConfigured,
    #[error("invalid jwt: {0}")]
    Invalid(String),
    #[error("expired")]
    Expired,
    #[error("issuer mismatch")]
    IssuerMismatch,
    #[error("tenant claim mismatch")]
    TenantMismatch,
    #[error("session not in jwt claim")]
    SessionForbidden,
}

pub async fn verify_for_tenant(
    cfg: &JwtConfig,
    tenant: &str,
    token: &str,
) -> Result<AgentDbClaims, JwtError> {
    let tcfg = cfg.per_tenant.get(tenant).ok_or(JwtError::TenantNotConfigured)?;
    match tcfg {
        TenantJwtConfig::Hmac { secret, issuer, audience } => {
            let mut v = Validation::new(Algorithm::HS256);
            v.set_issuer(&[issuer.as_str()]);
            if let Some(aud) = audience { v.set_audience(&[aud.as_str()]); } else { v.validate_aud = false; }
            v.validate_exp = true;
            let key = DecodingKey::from_secret(secret);
            let data = decode::<AgentDbClaims>(token, &key, &v).map_err(map_err)?;
            if data.claims.tenant != tenant { return Err(JwtError::TenantMismatch); }
            Ok(data.claims)
        }
        TenantJwtConfig::Jwks { .. } => {
            // Implemented in Task 9b.
            unimplemented!("jwks");
        }
    }
}

fn map_err(e: jsonwebtoken::errors::Error) -> JwtError {
    use jsonwebtoken::errors::ErrorKind;
    match e.kind() {
        ErrorKind::ExpiredSignature => JwtError::Expired,
        ErrorKind::InvalidIssuer => JwtError::IssuerMismatch,
        _ => JwtError::Invalid(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};

    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn hmac_cfg() -> JwtConfig {
        let mut per_tenant = HashMap::new();
        per_tenant.insert(
            "t-1".to_string(),
            TenantJwtConfig::Hmac {
                secret: b"shhh-this-is-a-secret".to_vec(),
                issuer: "https://auth.example.com".into(),
                audience: Some("agentdb".into()),
            },
        );
        JwtConfig { per_tenant }
    }

    fn mint(claims: &AgentDbClaims, secret: &[u8]) -> String {
        encode(
            &Header::new(Algorithm::HS256),
            claims,
            &EncodingKey::from_secret(secret),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn hmac_happy_path_returns_claims() {
        let cfg = hmac_cfg();
        let claims = AgentDbClaims {
            iss: "https://auth.example.com".into(),
            aud: Some("agentdb".into()),
            sub: "user-1".into(),
            tenant: "t-1".into(),
            sessions: SessionsClaim::Wildcard("*".into()),
            exp: now() + 3600,
        };
        let token = mint(&claims, b"shhh-this-is-a-secret");
        let out = verify_for_tenant(&cfg, "t-1", &token).await.unwrap();
        assert_eq!(out.sub, "user-1");
    }

    #[tokio::test]
    async fn rejects_expired_token() {
        let cfg = hmac_cfg();
        let claims = AgentDbClaims {
            iss: "https://auth.example.com".into(),
            aud: Some("agentdb".into()),
            sub: "u".into(),
            tenant: "t-1".into(),
            sessions: SessionsClaim::Wildcard("*".into()),
            exp: now() - 1,
        };
        let token = mint(&claims, b"shhh-this-is-a-secret");
        let err = verify_for_tenant(&cfg, "t-1", &token).await.unwrap_err();
        assert!(matches!(err, JwtError::Expired));
    }

    #[tokio::test]
    async fn rejects_wrong_issuer() {
        let cfg = hmac_cfg();
        let claims = AgentDbClaims {
            iss: "https://evil.example.com".into(),
            aud: Some("agentdb".into()),
            sub: "u".into(),
            tenant: "t-1".into(),
            sessions: SessionsClaim::Wildcard("*".into()),
            exp: now() + 3600,
        };
        let token = mint(&claims, b"shhh-this-is-a-secret");
        assert!(matches!(
            verify_for_tenant(&cfg, "t-1", &token).await.unwrap_err(),
            JwtError::IssuerMismatch
        ));
    }

    #[tokio::test]
    async fn rejects_tenant_claim_mismatch() {
        let cfg = hmac_cfg();
        let claims = AgentDbClaims {
            iss: "https://auth.example.com".into(),
            aud: Some("agentdb".into()),
            sub: "u".into(),
            tenant: "t-2".into(),
            sessions: SessionsClaim::Wildcard("*".into()),
            exp: now() + 3600,
        };
        let token = mint(&claims, b"shhh-this-is-a-secret");
        assert!(matches!(
            verify_for_tenant(&cfg, "t-1", &token).await.unwrap_err(),
            JwtError::TenantMismatch
        ));
    }

    #[test]
    fn sessions_claim_wildcard_allows_any() {
        let s = SessionsClaim::Wildcard("*".into());
        assert!(s.allows("anything"));
    }

    #[test]
    fn sessions_claim_list_only_allows_listed() {
        let s = SessionsClaim::List(vec!["s-1".into(), "s-2".into()]);
        assert!(s.allows("s-1"));
        assert!(!s.allows("s-3"));
    }
}
```

Add module to `lib.rs`: `pub mod jwt;`

- [ ] **Step 3: Run, expect PASS**

```bash
cargo test -p agentdb-api jwt
```

- [ ] **Step 4: Commit**

```bash
git add crates/agentdb-api/Cargo.toml crates/agentdb-api/src/jwt.rs crates/agentdb-api/src/lib.rs
git commit -m "feat(pubsub): JWT verifier — HMAC mode, claim shape, error variants"
```

---

### Task 9b: JWKS verifier (RS256/ES256)

**Files:**
- Modify: `crates/agentdb-api/src/jwt.rs`
- Test: same file

- [ ] **Step 1: Write the failing test using a stub JWKS server**

Append to the `tests` module in `jwt.rs`:

```rust
#[tokio::test]
async fn jwks_happy_path_with_local_stub_server() {
    use axum::routing::get;
    use rsa::pkcs8::EncodePrivateKey;
    use rsa::{pkcs1v15::SigningKey, RsaPrivateKey, RsaPublicKey};

    // Generate an RSA keypair, mint a token signed with it, expose the public
    // key over a local /jwks endpoint, verify.
    let mut rng = rand::thread_rng();
    let priv_key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
    let pub_key = RsaPublicKey::from(&priv_key);

    let n = base64_url(pub_key.n().to_bytes_be());
    let e = base64_url(pub_key.e().to_bytes_be());
    let kid = "k1".to_string();

    let jwks = serde_json::json!({
        "keys": [{ "kty": "RSA", "alg": "RS256", "kid": kid, "use": "sig", "n": n, "e": e }]
    });

    let app = axum::Router::new().route(
        "/jwks",
        get(move || {
            let jwks = jwks.clone();
            async move { axum::Json(jwks) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });

    // Mint a token signed with priv_key.
    let pem = priv_key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF).unwrap();
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
    header.kid = Some(kid);
    let claims = AgentDbClaims {
        iss: "https://auth.example.com".into(),
        aud: Some("agentdb".into()),
        sub: "u".into(),
        tenant: "t-1".into(),
        sessions: SessionsClaim::Wildcard("*".into()),
        exp: now() + 3600,
    };
    let token = jsonwebtoken::encode(
        &header,
        &claims,
        &jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes()).unwrap(),
    )
    .unwrap();

    let mut per_tenant = HashMap::new();
    per_tenant.insert(
        "t-1".to_string(),
        TenantJwtConfig::Jwks {
            url: format!("http://{}/jwks", addr),
            issuer: "https://auth.example.com".into(),
            audience: Some("agentdb".into()),
        },
    );
    let cfg = JwtConfig { per_tenant };

    let out = verify_for_tenant(&cfg, "t-1", &token).await.unwrap();
    assert_eq!(out.sub, "u");
}

fn base64_url(bytes: Vec<u8>) -> String {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    URL_SAFE_NO_PAD.encode(bytes)
}
```

Add dev dependencies to `crates/agentdb-api/Cargo.toml`:

```toml
[dev-dependencies]
rsa = "0.9"
rand = "0.8"
```

- [ ] **Step 2: Run, expect FAIL** (`unimplemented!`).

- [ ] **Step 3: Implement JWKS verifier with TTL cache**

Replace the `unimplemented!` in `verify_for_tenant`:

```rust
TenantJwtConfig::Jwks { url, issuer, audience } => {
    let jwks = JWKS_CACHE.fetch(url).await.map_err(|e| JwtError::Invalid(e.to_string()))?;
    let header = jsonwebtoken::decode_header(token).map_err(map_err)?;
    let kid = header.kid.ok_or_else(|| JwtError::Invalid("missing kid".into()))?;
    let key = jwks.find(&kid).ok_or_else(|| JwtError::Invalid(format!("unknown kid {kid}")))?;
    let mut v = Validation::new(header.alg);
    v.set_issuer(&[issuer.as_str()]);
    if let Some(aud) = audience { v.set_audience(&[aud.as_str()]); } else { v.validate_aud = false; }
    v.validate_exp = true;
    let data = decode::<AgentDbClaims>(token, &key.decoding_key, &v).map_err(map_err)?;
    if data.claims.tenant != tenant { return Err(JwtError::TenantMismatch); }
    Ok(data.claims)
}
```

Add the cache module at the bottom of `jwt.rs`:

```rust
mod jwks_cache {
    use super::*;
    use std::time::{Duration, Instant};
    use tokio::sync::RwLock;

    pub(super) struct JwksCache {
        ttl: Duration,
        entries: RwLock<HashMap<String, (Instant, Arc<Jwks>)>>,
    }

    pub(super) struct Jwks {
        keys: HashMap<String, JwksKey>,
    }

    pub(super) struct JwksKey {
        pub decoding_key: jsonwebtoken::DecodingKey,
    }

    impl Jwks {
        pub fn find(&self, kid: &str) -> Option<&JwksKey> {
            self.keys.get(kid)
        }
    }

    impl JwksCache {
        pub fn new(ttl: Duration) -> Self {
            Self { ttl, entries: RwLock::new(HashMap::new()) }
        }
        pub async fn fetch(&self, url: &str) -> anyhow::Result<Arc<Jwks>> {
            {
                let g = self.entries.read().await;
                if let Some((fetched_at, jwks)) = g.get(url) {
                    if fetched_at.elapsed() < self.ttl {
                        return Ok(jwks.clone());
                    }
                }
            }
            let body: serde_json::Value = reqwest::get(url).await?.json().await?;
            let mut keys = HashMap::new();
            for k in body["keys"].as_array().unwrap_or(&Vec::new()) {
                let kid = k["kid"].as_str().unwrap_or("").to_string();
                let kty = k["kty"].as_str().unwrap_or("");
                if kty != "RSA" { continue; }
                let n = k["n"].as_str().unwrap_or("");
                let e = k["e"].as_str().unwrap_or("");
                let dk = jsonwebtoken::DecodingKey::from_rsa_components(n, e)?;
                keys.insert(kid, JwksKey { decoding_key: dk });
            }
            let jwks = Arc::new(Jwks { keys });
            self.entries.write().await.insert(url.to_string(), (Instant::now(), jwks.clone()));
            Ok(jwks)
        }
    }
}

use jwks_cache::JwksCache;

static JWKS_CACHE: once_cell::sync::Lazy<JwksCache> =
    once_cell::sync::Lazy::new(|| JwksCache::new(Duration::from_secs(600)));
```

Add `once_cell = "1"` to `Cargo.toml` if not already present (workspace-level).

- [ ] **Step 4: Run, expect PASS**

```bash
cargo test -p agentdb-api jwt::tests::jwks
```

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-api/Cargo.toml crates/agentdb-api/src/jwt.rs
git commit -m "feat(pubsub): JWKS-mode JWT verification with TTL cache"
```

---

### Task 9c: JWT subscribe middleware (header + query param)

Wire JWT verification into the subscribe endpoints. Both transports accepted.

**Files:**
- Create: `crates/agentdb-api/src/subscribe_auth.rs`
- Modify: `crates/agentdb-api/src/subscribe.rs` (call the verifier)
- Modify: `crates/agentdb-api/src/error.rs` (add JWT error mapping)
- Modify: `bin/agentdb-server/src/main.rs` (read env vars to build `JwtConfig`)
- Test: `crates/agentdb-api/tests/subscribe_jwt.rs`

- [ ] **Step 1: Write the failing tests**

Create `crates/agentdb-api/tests/subscribe_jwt.rs`:

```rust
mod common;
use common::{TestServer, mint_hmac_token};

#[tokio::test]
async fn header_bearer_jwt_grants_subscribe_with_wildcard_sessions() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token = mint_hmac_token(b"secret", "t-1", "*", 60);

    let resp = server
        .client()
        .get(format!("http://{}/api/tenants/t-1/sessions/s-1/subscribe", server.addr))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn query_param_access_token_grants_subscribe() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token = mint_hmac_token(b"secret", "t-1", "*", 60);
    let resp = server
        .client()
        .get(format!(
            "http://{}/api/tenants/t-1/sessions/s-1/subscribe?access_token={token}",
            server.addr
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn missing_jwt_returns_401() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let resp = server
        .client()
        .get(format!("http://{}/api/tenants/t-1/sessions/s-1/subscribe", server.addr))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn jwt_with_session_not_in_claim_returns_403() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token = mint_hmac_token_for_sessions(b"secret", "t-1", &["s-2"], 60);
    let resp = server
        .client()
        .get(format!("http://{}/api/tenants/t-1/sessions/s-1/subscribe", server.addr))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "session_not_in_jwt");
}

#[tokio::test]
async fn both_header_and_query_param_returns_400() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token = mint_hmac_token(b"secret", "t-1", "*", 60);
    let resp = server
        .client()
        .get(format!(
            "http://{}/api/tenants/t-1/sessions/s-1/subscribe?access_token={token}",
            server.addr
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}
```

Add helpers to `tests/common/mod.rs`:

```rust
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use agentdb_api::jwt::{AgentDbClaims, SessionsClaim};

pub fn mint_hmac_token(secret: &[u8], tenant: &str, sessions: &str, ttl_secs: u64) -> String {
    let claims = AgentDbClaims {
        iss: "https://auth.test".into(),
        aud: None,
        sub: "test-user".into(),
        tenant: tenant.into(),
        sessions: if sessions == "*" { SessionsClaim::Wildcard("*".into()) } else { SessionsClaim::List(vec![sessions.into()]) },
        exp: now() + ttl_secs,
    };
    encode(&Header::new(Algorithm::HS256), &claims, &EncodingKey::from_secret(secret)).unwrap()
}

pub fn mint_hmac_token_for_sessions(secret: &[u8], tenant: &str, sessions: &[&str], ttl_secs: u64) -> String {
    let claims = AgentDbClaims {
        iss: "https://auth.test".into(),
        aud: None,
        sub: "test-user".into(),
        tenant: tenant.into(),
        sessions: SessionsClaim::List(sessions.iter().map(|s| s.to_string()).collect()),
        exp: now() + ttl_secs,
    };
    encode(&Header::new(Algorithm::HS256), &claims, &EncodingKey::from_secret(secret)).unwrap()
}

fn now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
}

impl TestServer {
    pub async fn start_with_jwt_hmac(secret: &[u8]) -> Self {
        // Build JwtConfig with TenantJwtConfig::Hmac for "t-1" with iss="https://auth.test", no aud.
    }
}
```

- [ ] **Step 2: Run, expect FAIL** (no JWT enforcement).

- [ ] **Step 3: Implement extractor + enforcement**

Create `crates/agentdb-api/src/subscribe_auth.rs`:

```rust
use axum::extract::{FromRequestParts, Query};
use axum::http::request::Parts;
use serde::Deserialize;

use crate::error::ApiError;
use crate::jwt::{verify_for_tenant, AgentDbClaims, JwtError};
use crate::AppState;

#[derive(Deserialize)]
struct AccessTokenQuery {
    access_token: Option<String>,
}

pub async fn extract_subscribe_jwt(
    state: &AppState,
    parts: &mut Parts,
    tenant: &str,
) -> Result<AgentDbClaims, ApiError> {
    let header_token = parts
        .headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .map(str::to_string);

    let Query(AccessTokenQuery { access_token: query_token }) =
        Query::<AccessTokenQuery>::try_from_uri(&parts.uri).unwrap_or_default();

    let token = match (header_token, query_token) {
        (Some(_), Some(_)) => return Err(ApiError::bad_request("supply jwt via header OR query, not both")),
        (Some(t), None) | (None, Some(t)) => t,
        (None, None) => return Err(ApiError::unauthorized("missing jwt")),
    };

    verify_for_tenant(&state.jwt_config, tenant, &token)
        .await
        .map_err(map_jwt_err)
}

fn map_jwt_err(e: JwtError) -> ApiError {
    match e {
        JwtError::TenantNotConfigured => ApiError::unauthorized("tenant not configured for jwt"),
        JwtError::Expired => ApiError::with_code(401, "jwt_expired"),
        JwtError::Invalid(_) | JwtError::IssuerMismatch | JwtError::TenantMismatch => {
            ApiError::with_code(401, "jwt_invalid")
        }
        JwtError::SessionForbidden => ApiError::with_code(403, "session_not_in_jwt"),
    }
}
```

Add `pub mod subscribe_auth;` to `lib.rs`.

In `subscribe.rs::subscribe_session`, change the signature to accept `Parts` and call the extractor:

```rust
pub async fn subscribe_session(
    State(state): State<AppState>,
    Path((tenant, session)): Path<(String, String)>,
    Query(q): Query<SubscribeQuery>,
    mut parts: axum::http::request::Parts,
) -> axum::response::Response {
    // Skip JWT enforcement when this tenant has no JWT config (back-compat
    // with backend Basic-auth subscribers).
    if state.jwt_config.per_tenant.contains_key(&tenant) {
        let claims = match crate::subscribe_auth::extract_subscribe_jwt(&state, &mut parts, &tenant).await {
            Ok(c) => c,
            Err(e) => return e.into_response(),
        };
        if !claims.sessions.allows(&session) {
            return ApiError::with_code(403, "session_not_in_jwt").into_response();
        }
    }
    // ... existing handler body ...
}
```

Same pattern for `subscribe_tenant` (verify each requested session against the claim).

In `bin/agentdb-server/src/main.rs`, add env-var parsing:

```rust
fn jwt_config_from_env() -> agentdb_api::jwt::JwtConfig {
    let mut per_tenant = HashMap::new();
    for (k, v) in std::env::vars() {
        if let Some(rest) = k.strip_prefix("AGENTDB_TENANT_") {
            if let Some(tenant_id) = rest.strip_suffix("_JWT_HMAC_SECRET") {
                let issuer = std::env::var(format!("AGENTDB_TENANT_{tenant_id}_JWT_ISSUER")).unwrap_or_default();
                let audience = std::env::var(format!("AGENTDB_TENANT_{tenant_id}_JWT_AUDIENCE")).ok();
                per_tenant.insert(
                    tenant_id.to_lowercase(),
                    agentdb_api::jwt::TenantJwtConfig::Hmac {
                        secret: v.into_bytes(),
                        issuer,
                        audience,
                    },
                );
            } else if let Some(tenant_id) = rest.strip_suffix("_JWT_JWKS_URL") {
                let issuer = std::env::var(format!("AGENTDB_TENANT_{tenant_id}_JWT_ISSUER")).unwrap_or_default();
                let audience = std::env::var(format!("AGENTDB_TENANT_{tenant_id}_JWT_AUDIENCE")).ok();
                per_tenant.insert(
                    tenant_id.to_lowercase(),
                    agentdb_api::jwt::TenantJwtConfig::Jwks {
                        url: v,
                        issuer,
                        audience,
                    },
                );
            }
        }
    }
    agentdb_api::jwt::JwtConfig { per_tenant }
}
```

Pass into `AppState`.

- [ ] **Step 4: Run, expect PASS**

```bash
cargo test -p agentdb-api subscribe_jwt
```

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-api/src/subscribe_auth.rs crates/agentdb-api/src/subscribe.rs crates/agentdb-api/src/lib.rs crates/agentdb-api/src/error.rs bin/agentdb-server/src/main.rs crates/agentdb-api/tests/subscribe_jwt.rs crates/agentdb-api/tests/common/mod.rs
git commit -m "feat(pubsub): JWT enforcement on subscribe — header OR query param"
```

---

### Task 9d: JWT-expiry close behavior

When a JWT expires mid-stream, send `event: error data: { error: jwt_expired, last_seq: N }` and close.

**Files:**
- Modify: `crates/agentdb-api/src/subscribe.rs`
- Test: extend `crates/agentdb-api/tests/subscribe_jwt.rs`

- [ ] **Step 1: Write the failing test**

Append to `subscribe_jwt.rs`:

```rust
#[tokio::test]
async fn jwt_expires_mid_stream_sends_error_and_closes() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    // Mint a 2-second token.
    let token = mint_hmac_token(b"secret", "t-1", "*", 2);

    let mut sse = common::SseStream::open_with_header(
        &server,
        "/api/tenants/t-1/sessions/s-1/subscribe",
        ("Authorization", format!("Bearer {token}")),
    )
    .await;
    sse.skip_subscribed_event().await;

    // Append; receive seq 1.
    server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            serde_json::json!({"event": {"kind": "msg"}, "n": 0}),
        )
        .await;
    let f = sse.next_event().await;
    let env: serde_json::Value = serde_json::from_str(&f.data).unwrap();
    assert_eq!(env["seq"], 1);

    // Wait for expiry, then append — server should send error and close.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    server
        .post_json(
            "/api/tenants/t-1/sessions/s-1/append_and_fetch",
            serde_json::json!({"event": {"kind": "msg"}, "n": 0}),
        )
        .await;
    let frame = sse.next_event().await;
    assert_eq!(frame.event_name.as_deref(), Some("error"));
    let body: serde_json::Value = serde_json::from_str(&frame.data).unwrap();
    assert_eq!(body["error"], "jwt_expired");
    assert_eq!(body["last_seq"], 1);
}
```

Add `SseStream::open_with_header` helper in `tests/common/mod.rs`.

- [ ] **Step 2: Run, expect FAIL** (server doesn't check expiry on each delivery).

- [ ] **Step 3: Implement expiry check on each delivery**

In `subscribe.rs`, propagate the JWT `exp` into the live-tail loop. Before each `Ok(env)` is yielded, check `now_secs() >= exp`. If true, emit error frame and close:

```rust
let exp_secs = claims.as_ref().map(|c| c.exp);

// In the unfold:
if let Some(exp) = exp_secs {
    if now_secs() >= exp {
        let payload = serde_json::json!({
            "error": "jwt_expired",
            "last_seq": last_delivered_seq,
        }).to_string();
        let frame = Event::default().event("error").data(payload);
        return Some((Ok(frame), state.with_closed()));
    }
}
```

(Add a helper `fn now_secs() -> u64`.)

- [ ] **Step 4: Run, expect PASS**

```bash
cargo test -p agentdb-api subscribe_jwt::tests::jwt_expires_mid_stream
```

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-api/src/subscribe.rs crates/agentdb-api/tests/subscribe_jwt.rs crates/agentdb-api/tests/common/mod.rs
git commit -m "feat(pubsub): close subscribe stream with jwt_expired error on token expiry"
```

---

### Task 10: CORS per-tenant

**Files:**
- Create: `crates/agentdb-api/src/cors.rs`
- Modify: `crates/agentdb-api/src/lib.rs` (apply layer to subscribe routes)
- Modify: `bin/agentdb-server/src/main.rs` (parse CORS env vars)
- Test: `crates/agentdb-api/tests/subscribe_cors.rs`

- [ ] **Step 1: Write the failing tests**

Create `crates/agentdb-api/tests/subscribe_cors.rs`:

```rust
mod common;
use common::TestServer;

#[tokio::test]
async fn allowed_origin_is_echoed() {
    let server = TestServer::start_with_cors("t-1", &["https://app.example.com"]).await;
    let resp = server
        .client()
        .get(format!("http://{}/api/tenants/t-1/sessions/s-1/subscribe", server.addr))
        .header("Origin", "https://app.example.com")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.headers().get("access-control-allow-origin").unwrap(),
        "https://app.example.com"
    );
}

#[tokio::test]
async fn disallowed_origin_omits_cors_header() {
    let server = TestServer::start_with_cors("t-1", &["https://app.example.com"]).await;
    let resp = server
        .client()
        .get(format!("http://{}/api/tenants/t-1/sessions/s-1/subscribe", server.addr))
        .header("Origin", "https://evil.example.com")
        .send()
        .await
        .unwrap();
    assert!(resp.headers().get("access-control-allow-origin").is_none());
}

#[tokio::test]
async fn options_preflight_returns_allowed_methods_and_headers() {
    let server = TestServer::start_with_cors("t-1", &["https://app.example.com"]).await;
    let resp = server
        .client()
        .request(
            reqwest::Method::OPTIONS,
            format!("http://{}/api/tenants/t-1/sessions/s-1/subscribe", server.addr),
        )
        .header("Origin", "https://app.example.com")
        .header("Access-Control-Request-Method", "GET")
        .header("Access-Control-Request-Headers", "authorization")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    let methods = resp.headers().get("access-control-allow-methods").unwrap().to_str().unwrap();
    assert!(methods.contains("GET"));
    let headers = resp.headers().get("access-control-allow-headers").unwrap().to_str().unwrap();
    assert!(headers.to_lowercase().contains("authorization"));
}
```

- [ ] **Step 2: Run, expect FAIL** (no CORS layer).

- [ ] **Step 3: Implement CORS module**

Create `crates/agentdb-api/src/cors.rs`:

```rust
use std::collections::HashMap;

use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;

use crate::AppState;

#[derive(Clone, Default)]
pub struct CorsConfig {
    pub per_tenant: HashMap<String, Vec<String>>,
}

pub async fn cors_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let origin = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|h| h.to_str().ok())
        .map(str::to_string);
    let tenant = extract_tenant_from_path(req.uri().path());
    let allowed = origin
        .as_ref()
        .zip(tenant.as_ref())
        .and_then(|(o, t)| {
            state
                .cors_config
                .per_tenant
                .get(t)
                .filter(|allowed| allowed.iter().any(|a| a == o))
                .map(|_| o.clone())
        });

    if req.method() == Method::OPTIONS {
        let mut resp = Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(axum::body::Body::empty())
            .unwrap();
        if let Some(origin) = allowed {
            let h = resp.headers_mut();
            h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_str(&origin).unwrap());
            h.insert(header::ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET, OPTIONS"));
            h.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("authorization, content-type"));
            h.insert(header::VARY, HeaderValue::from_static("Origin"));
        }
        return resp;
    }

    let mut resp = next.run(req).await;
    if let Some(origin) = allowed {
        let h = resp.headers_mut();
        h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_str(&origin).unwrap());
        h.insert(header::VARY, HeaderValue::from_static("Origin"));
    }
    resp
}

fn extract_tenant_from_path(path: &str) -> Option<String> {
    let mut parts = path.split('/').filter(|s| !s.is_empty());
    if parts.next()? != "api" { return None; }
    if parts.next()? != "tenants" { return None; }
    Some(parts.next()?.to_string())
}
```

Add `pub mod cors;` and `pub cors_config: CorsConfig` to `AppState`. In `lib.rs`, apply the middleware only to subscribe routes:

```rust
let subscribe_routes = Router::new()
    .route("/api/tenants/:tenant_id/sessions/:session_id/subscribe", axum::routing::get(subscribe::subscribe_session))
    .route("/api/tenants/:tenant_id/subscribe", axum::routing::get(subscribe::subscribe_tenant))
    .route("/api/tenants/:tenant_id/sessions/:session_id/subscribe", axum::routing::options(|| async { StatusCode::NO_CONTENT }))
    .route("/api/tenants/:tenant_id/subscribe", axum::routing::options(|| async { StatusCode::NO_CONTENT }))
    .layer(axum::middleware::from_fn_with_state(state.clone(), cors::cors_middleware));
```

In `main.rs`, parse CORS env vars:

```rust
fn cors_config_from_env() -> agentdb_api::cors::CorsConfig {
    let mut per_tenant = HashMap::new();
    for (k, v) in std::env::vars() {
        if let Some(rest) = k.strip_prefix("AGENTDB_TENANT_") {
            if let Some(tenant_id) = rest.strip_suffix("_CORS_ORIGINS") {
                let origins: Vec<String> = v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
                per_tenant.insert(tenant_id.to_lowercase(), origins);
            }
        }
    }
    agentdb_api::cors::CorsConfig { per_tenant }
}
```

- [ ] **Step 4: Run, expect PASS**

```bash
cargo test -p agentdb-api subscribe_cors
```

- [ ] **Step 5: Run full Rust test suite + clippy + fmt**

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```
Fix any failures inline. Commit each fix separately if substantive; otherwise amend.

- [ ] **Step 6: Commit**

```bash
git add crates/agentdb-api/src/cors.rs crates/agentdb-api/src/lib.rs bin/agentdb-server/src/main.rs crates/agentdb-api/tests/subscribe_cors.rs crates/agentdb-api/tests/common/mod.rs
git commit -m "feat(pubsub): per-tenant CORS layer for subscribe routes"
```

---

### Task 11: Python SDK

**Files:**
- Create: `clients/python/pyproject.toml`
- Create: `clients/python/src/agentdb/__init__.py`
- Create: `clients/python/src/agentdb/client.py`
- Create: `clients/python/src/agentdb/_sse.py`
- Create: `clients/python/tests/test_subscribe.py`
- Create: `clients/python/README.md`

- [ ] **Step 1: Create package skeleton**

`clients/python/pyproject.toml`:

```toml
[project]
name = "agentdb"
version = "0.1.0"
description = "AgentDB Python client"
requires-python = ">=3.10"
dependencies = ["httpx>=0.27", "httpx-sse>=0.4"]

[project.optional-dependencies]
test = ["pytest>=8", "pytest-asyncio>=0.23"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/agentdb"]
```

`clients/python/src/agentdb/__init__.py`:

```python
from .client import AgentDB, Event, SubscriptionClosed

__all__ = ["AgentDB", "Event", "SubscriptionClosed"]
__version__ = "0.1.0"
```

- [ ] **Step 2: Write the failing smoke test**

`clients/python/tests/test_subscribe.py`:

```python
import os
import subprocess
import time
import json
import socket
from contextlib import contextmanager

import pytest
import httpx

from agentdb import AgentDB


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@contextmanager
def _serve():
    port = _free_port()
    env = os.environ.copy()
    env["AGENTDB_ADDR"] = f"127.0.0.1:{port}"
    proc = subprocess.Popen(
        ["cargo", "run", "--quiet", "-p", "agentdb-server", "--", "serve", "--dev"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    base = f"http://127.0.0.1:{port}"
    for _ in range(60):
        try:
            httpx.get(f"{base}/health/ready", timeout=0.5)
            break
        except Exception:
            time.sleep(0.5)
    try:
        yield base
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def test_subscribe_iterator_yields_appended_events():
    with _serve() as base:
        client = AgentDB(base_url=base, tenant="t-1")

        # Open subscription in a background thread, then append.
        import threading, queue
        seen = queue.Queue()
        def run():
            for evt in client.subscribe(session="s-1"):
                seen.put(evt)
                if seen.qsize() >= 3:
                    return
        t = threading.Thread(target=run, daemon=True)
        t.start()

        time.sleep(0.5)  # let subscriber attach
        for i in range(3):
            httpx.post(
                f"{base}/api/tenants/t-1/sessions/s-1/append_and_fetch",
                json={"event": {"kind": "msg", "n": i}, "n": 0},
            )

        events = [seen.get(timeout=5) for _ in range(3)]
        seqs = sorted(e.seq for e in events)
        assert seqs == [1, 2, 3]
```

- [ ] **Step 3: Run, expect FAIL** (`agentdb` not installed; client missing).

```bash
cd clients/python && pip install -e .[test] && pytest -x
```

- [ ] **Step 4: Implement `_sse.py` (line parser)**

```python
from dataclasses import dataclass
from typing import Iterator, Optional, AsyncIterator

@dataclass
class SseFrame:
    event: Optional[str]
    data: str

def parse_lines(lines: Iterator[str]) -> Iterator[SseFrame]:
    event: Optional[str] = None
    data: list[str] = []
    for raw in lines:
        line = raw.rstrip("\r")
        if line == "":
            if data:
                yield SseFrame(event=event, data="\n".join(data))
            event, data = None, []
            continue
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            event = line[len("event:"):].strip()
        elif line.startswith("data:"):
            data.append(line[len("data:"):].lstrip(" "))
```

- [ ] **Step 5: Implement `client.py`**

```python
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional, Sequence
import httpx

from ._sse import parse_lines, SseFrame


class SubscriptionClosed(Exception):
    """Raised when the server closes the subscription with an error frame."""
    def __init__(self, error: str, last_seq: Optional[int] = None):
        super().__init__(error)
        self.error = error
        self.last_seq = last_seq


@dataclass
class Event:
    tenant: str
    session: str
    seq: int
    ts_ms: int
    kind: Optional[str]
    actor_id: Optional[str]
    payload: Any


class AgentDB:
    def __init__(
        self,
        *,
        base_url: str,
        tenant: str,
        get_token: Optional[Callable[[], str]] = None,
        basic_auth: Optional[tuple[str, str]] = None,
        timeout: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.tenant = tenant
        self._get_token = get_token
        self._basic_auth = basic_auth
        self._timeout = timeout

    def subscribe(
        self,
        *,
        session: str,
        kind: Optional[Sequence[str]] = None,
        from_seq: Optional[int] = None,
        heartbeat_secs: Optional[int] = None,
    ) -> Iterator[Event]:
        last_seq = from_seq
        while True:
            try:
                yield from self._stream(session, kind, last_seq, heartbeat_secs, on_seq=lambda s: setattr_self(self, "_last_seq_holder", s))
                return  # server closed cleanly
            except SubscriptionClosed as e:
                if e.error == "jwt_expired" and self._get_token:
                    last_seq = (e.last_seq or 0) + 1
                    continue
                raise

    def _stream(self, session, kind, from_seq, heartbeat_secs, on_seq) -> Iterator[Event]:
        params: dict[str, Any] = {}
        if from_seq is not None:
            params["from_seq"] = from_seq
        if kind:
            params["kind"] = list(kind)
        if heartbeat_secs is not None:
            params["heartbeat_secs"] = heartbeat_secs
        headers = self._auth_headers()
        url = f"{self.base_url}/api/tenants/{self.tenant}/sessions/{session}/subscribe"
        with httpx.stream("GET", url, params=params, headers=headers, auth=self._basic_auth, timeout=self._timeout) as resp:
            resp.raise_for_status()
            for frame in parse_lines(resp.iter_lines()):
                if frame.event == "subscribed":
                    continue
                if frame.event == "error":
                    body = json.loads(frame.data)
                    raise SubscriptionClosed(body.get("error", "unknown"), body.get("last_seq"))
                evt_dict = json.loads(frame.data)
                evt = Event(
                    tenant=evt_dict["tenant"],
                    session=evt_dict["session"],
                    seq=evt_dict["seq"],
                    ts_ms=evt_dict["ts_ms"],
                    kind=evt_dict.get("kind"),
                    actor_id=evt_dict.get("actor_id"),
                    payload=evt_dict["payload"],
                )
                on_seq(evt.seq)
                yield evt

    def _auth_headers(self) -> dict[str, str]:
        if self._get_token is None:
            return {}
        return {"Authorization": f"Bearer {self._get_token()}"}
```

(Clean up the `setattr_self` pattern; it's a placeholder for "track last seen seq" — see if a closure variable works better in the final code.)

- [ ] **Step 6: Run, expect PASS**

```bash
cd clients/python && pytest -x
```

- [ ] **Step 7: Add async variant** (optional in v0.1; can defer to follow-up). For now, document in README that sync is shipped first.

- [ ] **Step 8: Commit**

```bash
git add clients/python/
git commit -m "feat(sdk-py): AgentDB Python client with subscribe() iterator"
```

---

### Task 12: TypeScript SDK

**Files:**
- Create: `clients/typescript/package.json`
- Create: `clients/typescript/tsconfig.json`
- Create: `clients/typescript/src/index.ts`
- Create: `clients/typescript/src/sse.ts`
- Create: `clients/typescript/test/subscribe.test.ts`
- Create: `clients/typescript/README.md`

- [ ] **Step 1: Create package skeleton**

`clients/typescript/package.json`:

```json
{
  "name": "@agentdb/client",
  "version": "0.1.0",
  "type": "module",
  "main": "./dist/index.cjs",
  "module": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "exports": {
    ".": {
      "import": "./dist/index.js",
      "require": "./dist/index.cjs",
      "types": "./dist/index.d.ts"
    }
  },
  "scripts": {
    "build": "tsup src/index.ts --format esm,cjs --dts",
    "test": "vitest run"
  },
  "dependencies": {},
  "devDependencies": {
    "tsup": "^8",
    "typescript": "^5.4",
    "vitest": "^1.6",
    "undici": "^6"
  }
}
```

`clients/typescript/tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "strict": true,
    "declaration": true,
    "outDir": "./dist",
    "lib": ["ES2022", "DOM"]
  },
  "include": ["src/**/*"]
}
```

- [ ] **Step 2: Write the failing test**

`clients/typescript/test/subscribe.test.ts`:

```ts
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawn, ChildProcess } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import { AgentDB } from "../src/index";

let proc: ChildProcess;
let baseUrl: string;
const port = 18078 + Math.floor(Math.random() * 100);

beforeAll(async () => {
  baseUrl = `http://127.0.0.1:${port}`;
  proc = spawn("cargo", ["run", "--quiet", "-p", "agentdb-server", "--", "serve", "--dev"], {
    env: { ...process.env, AGENTDB_ADDR: `127.0.0.1:${port}` },
    stdio: "ignore",
  });
  for (let i = 0; i < 60; i++) {
    try {
      const r = await fetch(`${baseUrl}/health/ready`);
      if (r.ok) return;
    } catch {}
    await sleep(500);
  }
  throw new Error("server failed to boot");
}, 60_000);

afterAll(() => {
  proc?.kill("SIGTERM");
});

describe("subscribe", () => {
  it("yields events appended after subscribe", async () => {
    const client = new AgentDB({ baseUrl, tenant: "t-1" });
    const seen: number[] = [];
    const sub = (async () => {
      for await (const evt of client.subscribe({ session: "s-1" })) {
        seen.push(evt.seq);
        if (seen.length >= 3) return;
      }
    })();

    await sleep(500);
    for (let i = 1; i <= 3; i++) {
      await fetch(`${baseUrl}/api/tenants/t-1/sessions/s-1/append_and_fetch`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ event: { kind: "msg", n: i }, n: 0 }),
      });
    }
    await sub;
    expect(seen.sort()).toEqual([1, 2, 3]);
  });

  it("replays from from_seq then tails live", async () => {
    const client = new AgentDB({ baseUrl, tenant: "t-1" });
    for (let i = 1; i <= 3; i++) {
      await fetch(`${baseUrl}/api/tenants/t-1/sessions/s-replay/append_and_fetch`, {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ event: { kind: "x" }, n: 0 }),
      });
    }
    const seen: number[] = [];
    for await (const evt of client.subscribe({ session: "s-replay", fromSeq: 1 })) {
      seen.push(evt.seq);
      if (seen.length >= 3) break;
    }
    expect(seen).toEqual([1, 2, 3]);
  });
});
```

- [ ] **Step 3: Implement `sse.ts`**

```ts
export interface SseFrame {
  event?: string;
  data: string;
}

export async function* parseSse(reader: ReadableStreamDefaultReader<Uint8Array>): AsyncIterable<SseFrame> {
  const decoder = new TextDecoder();
  let buf = "";
  for (;;) {
    const { value, done } = await reader.read();
    if (done) {
      if (buf.trim()) yield parseBlock(buf);
      return;
    }
    buf += decoder.decode(value, { stream: true });
    let idx;
    while ((idx = buf.indexOf("\n\n")) !== -1) {
      const block = buf.slice(0, idx);
      buf = buf.slice(idx + 2);
      yield parseBlock(block);
    }
  }
}

function parseBlock(block: string): SseFrame {
  let event: string | undefined;
  const dataLines: string[] = [];
  for (const raw of block.split("\n")) {
    const line = raw.replace(/\r$/, "");
    if (line.startsWith(":")) continue;
    if (line.startsWith("event:")) event = line.slice("event:".length).trim();
    else if (line.startsWith("data:")) dataLines.push(line.slice("data:".length).replace(/^ /, ""));
  }
  return { event, data: dataLines.join("\n") };
}
```

- [ ] **Step 4: Implement `index.ts`**

```ts
import { parseSse } from "./sse";

export interface Event {
  tenant: string;
  session: string;
  seq: number;
  ts_ms: number;
  kind?: string;
  actor_id?: string;
  payload: unknown;
}

export interface AgentDBOptions {
  baseUrl: string;
  tenant: string;
  getToken?: () => string | Promise<string>;
  fetchImpl?: typeof fetch;
}

export interface SubscribeOptions {
  session: string;
  kind?: string[];
  fromSeq?: number;
  heartbeatSecs?: number;
}

export class SubscriptionClosed extends Error {
  constructor(public readonly errorCode: string, public readonly lastSeq?: number) {
    super(errorCode);
  }
}

export class AgentDB {
  constructor(private readonly opts: AgentDBOptions) {}

  async *subscribe(s: SubscribeOptions): AsyncIterable<Event> {
    let fromSeq = s.fromSeq;
    while (true) {
      try {
        for await (const evt of this.streamOnce(s, fromSeq)) {
          fromSeq = evt.seq + 1;
          yield evt;
        }
        return;
      } catch (e) {
        if (e instanceof SubscriptionClosed && e.errorCode === "jwt_expired" && this.opts.getToken) {
          fromSeq = (e.lastSeq ?? 0) + 1;
          continue;
        }
        throw e;
      }
    }
  }

  private async *streamOnce(s: SubscribeOptions, fromSeq?: number): AsyncIterable<Event> {
    const url = new URL(`${this.opts.baseUrl}/api/tenants/${this.opts.tenant}/sessions/${s.session}/subscribe`);
    if (fromSeq !== undefined) url.searchParams.set("from_seq", String(fromSeq));
    for (const k of s.kind ?? []) url.searchParams.append("kind", k);
    if (s.heartbeatSecs !== undefined) url.searchParams.set("heartbeat_secs", String(s.heartbeatSecs));

    const headers: Record<string, string> = { Accept: "text/event-stream" };
    if (this.opts.getToken) headers.Authorization = `Bearer ${await this.opts.getToken()}`;

    const fetchImpl = this.opts.fetchImpl ?? fetch;
    const resp = await fetchImpl(url, { headers });
    if (!resp.ok || !resp.body) throw new Error(`subscribe failed: ${resp.status}`);
    const reader = resp.body.getReader();

    for await (const frame of parseSse(reader)) {
      if (frame.event === "subscribed") continue;
      if (frame.event === "error") {
        const body = JSON.parse(frame.data) as { error: string; last_seq?: number };
        throw new SubscriptionClosed(body.error, body.last_seq);
      }
      yield JSON.parse(frame.data) as Event;
    }
  }
}
```

- [ ] **Step 5: Install + build**

```bash
cd clients/typescript && npm install && npm run build
```

- [ ] **Step 6: Run, expect PASS**

```bash
cd clients/typescript && npm test
```

- [ ] **Step 7: Add a `.gitignore` for `dist/` and `node_modules/`** in `clients/typescript/`.

- [ ] **Step 8: Commit**

```bash
git add clients/typescript/
git commit -m "feat(sdk-ts): @agentdb/client TypeScript SDK with async iterator subscribe()"
```

---

### Task 13: Examples

Two examples — a browser-direct demo and a multi-agent Python demo.

**Files:**
- Create: `examples/browser/index.html`
- Create: `examples/browser/mint_token.py`
- Create: `examples/browser/README.md`
- Create: `examples/agents/python/quickstart.py`
- Create: `examples/agents/python/README.md`

- [ ] **Step 1: Browser demo HTML**

`examples/browser/index.html`:

```html
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>AgentDB browser-direct demo</title>
  <style>
    body { font-family: ui-sans-serif, system-ui, sans-serif; max-width: 720px; margin: 2rem auto; padding: 0 1rem; }
    .ev { padding: .5rem; border-bottom: 1px solid #eee; font-family: ui-monospace, monospace; font-size: .9rem; }
    button { padding: .5rem 1rem; margin-right: .5rem; }
  </style>
</head>
<body>
  <h1>AgentDB browser-direct subscription</h1>
  <p>Subscribed to <code>tenant=t-1, session=s-demo</code>. Append events from this page or via the API; they stream in below.</p>

  <div>
    <input id="msg" placeholder="message text" value="hello"/>
    <button id="send">Append</button>
  </div>

  <h3>Events</h3>
  <div id="events"></div>

  <script type="module">
    const TOKEN = window.AGENTDB_TOKEN;       // injected by mint_token.py output
    const BASE_URL = "http://127.0.0.1:7878";
    const TENANT = "t-1";
    const SESSION = "s-demo";

    async function append(text) {
      await fetch(`${BASE_URL}/api/tenants/${TENANT}/sessions/${SESSION}/append_and_fetch`, {
        method: "POST",
        headers: { "content-type": "application/json", "authorization": `Bearer ${TOKEN}` },
        body: JSON.stringify({ event: { kind: "message", text }, n: 0 }),
      });
    }

    async function subscribe() {
      const url = `${BASE_URL}/api/tenants/${TENANT}/sessions/${SESSION}/subscribe`;
      const resp = await fetch(url, { headers: { authorization: `Bearer ${TOKEN}` } });
      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buf = "";
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        let idx;
        while ((idx = buf.indexOf("\n\n")) !== -1) {
          const block = buf.slice(0, idx);
          buf = buf.slice(idx + 2);
          render(block);
        }
      }
    }

    function render(block) {
      let evt = null, data = "";
      for (const line of block.split("\n")) {
        if (line.startsWith("event:")) evt = line.slice(6).trim();
        else if (line.startsWith("data:")) data += line.slice(5).trimStart();
      }
      if (evt === "subscribed") return;
      const env = JSON.parse(data);
      const div = document.createElement("div");
      div.className = "ev";
      div.textContent = `seq=${env.seq} kind=${env.kind} payload=${JSON.stringify(env.payload)}`;
      document.getElementById("events").appendChild(div);
    }

    document.getElementById("send").onclick = () => append(document.getElementById("msg").value);
    subscribe().catch(e => console.error(e));
  </script>
</body>
</html>
```

`examples/browser/mint_token.py`:

```python
"""Mint a short-lived HMAC-signed JWT for the browser demo.

Usage:
  python mint_token.py > token.html
  # then in another shell, run agentdb with the matching env:
  AGENTDB_TENANT_T_1_JWT_HMAC_SECRET=demo-secret \
  AGENTDB_TENANT_T_1_JWT_ISSUER=https://demo.local \
  AGENTDB_TENANT_T_1_CORS_ORIGINS=http://localhost:8000 \
  cargo run -p agentdb-server -- serve --dev
  # then serve the static directory:
  python -m http.server 8000
"""
import json
import time
import hmac
import hashlib
import base64

SECRET = b"demo-secret"
TENANT = "t-1"
ISSUER = "https://demo.local"
TTL_SECS = 3600


def b64u(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def jwt(claims: dict) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    seg1 = b64u(json.dumps(header, separators=(",", ":")).encode())
    seg2 = b64u(json.dumps(claims, separators=(",", ":")).encode())
    sig = hmac.new(SECRET, f"{seg1}.{seg2}".encode(), hashlib.sha256).digest()
    return f"{seg1}.{seg2}.{b64u(sig)}"


claims = {
    "iss": ISSUER,
    "sub": "demo-user",
    "tenant": TENANT,
    "sessions": "*",
    "exp": int(time.time()) + TTL_SECS,
}
token = jwt(claims)
print(f"<script>window.AGENTDB_TOKEN = {json.dumps(token)};</script>")
```

`examples/browser/README.md`:

```markdown
# Browser-direct demo

Three terminals:

1. Generate the token + serve the page:
   ```
   python mint_token.py > token.html
   cat token.html index.html > demo.html
   python -m http.server 8000
   ```
2. Run AgentDB with matching JWT + CORS env:
   ```
   AGENTDB_TENANT_T_1_JWT_HMAC_SECRET=demo-secret \
   AGENTDB_TENANT_T_1_JWT_ISSUER=https://demo.local \
   AGENTDB_TENANT_T_1_CORS_ORIGINS=http://localhost:8000 \
   cargo run -p agentdb-server -- serve --dev
   ```
3. Open http://localhost:8000/demo.html — append events; they stream into the page.
```

- [ ] **Step 2: Multi-agent Python demo**

`examples/agents/python/quickstart.py`:

```python
"""Two-agent demo: a producer appends task-events; a reactor subscribes
and prints when it sees `task_done`. Demonstrates spec § 2 Pattern 2.

Run:
    cargo run -p agentdb-server -- serve --dev    # in one shell
    python quickstart.py                           # in another
"""
import threading
import time
import httpx
from agentdb import AgentDB

BASE = "http://127.0.0.1:7878"
TENANT = "t-1"
SESSION = "agent-coord"

def producer():
    time.sleep(1.0)
    for i in range(3):
        httpx.post(
            f"{BASE}/api/tenants/{TENANT}/sessions/{SESSION}/append_and_fetch",
            json={"event": {"kind": "task_started", "i": i}, "n": 0},
        )
        time.sleep(0.5)
        httpx.post(
            f"{BASE}/api/tenants/{TENANT}/sessions/{SESSION}/append_and_fetch",
            json={"event": {"kind": "task_done", "i": i}, "n": 0},
        )

def reactor():
    client = AgentDB(base_url=BASE, tenant=TENANT)
    seen_done = 0
    for evt in client.subscribe(session=SESSION, kind=["task_done"]):
        seen_done += 1
        print(f"reactor saw task_done #{evt.payload['i']} (seq={evt.seq})")
        if seen_done >= 3:
            return

threading.Thread(target=producer, daemon=True).start()
reactor()
```

- [ ] **Step 3: Run both manually to verify**

```bash
# In one shell
cargo run -p agentdb-server -- serve --dev
# In another
cd examples/agents/python && python quickstart.py
# Should print three "reactor saw task_done" lines.
```

- [ ] **Step 4: Commit**

```bash
git add examples/
git commit -m "feat(examples): browser-direct demo + multi-agent Python quickstart"
```

---

### Task 14: Docs + runbooks

**Files:**
- Modify: `README.md` (new "Real-time subscriptions" section)
- Modify: `docs/agent-db.md` (add primitive)
- Modify: `docs/ARCHITECTURE.md` (EventBus + JWT flow)
- Create: `docs/runbooks/jwt_expired.md`
- Create: `docs/runbooks/jwt_invalid.md`
- Create: `docs/runbooks/session_not_in_jwt.md`
- Create: `docs/runbooks/lag_too_far.md`
- Create: `docs/runbooks/origin_not_allowed.md`

- [ ] **Step 1: README "Real-time subscriptions" section**

In `README.md`, add a new section under "API tour" (after "MongoDB wire protocol"):

```markdown
### Real-time subscriptions (SSE)

Server-Sent Events stream of events appended to a session, with
`from_seq` replay for gap-free reconnect. Browser-direct via JWT;
backend-direct via Basic auth (existing).

```
GET /api/tenants/{tenant}/sessions/{session}/subscribe
       ?from_seq=<u64>            # optional; default = "live tail only"
       &kind=<string>             # optional; repeatable filter
       &access_token=<jwt>        # browsers using EventSource

GET /api/tenants/{tenant}/subscribe        # tenant-wide; same params + session=, from_seq_per_session=
```

Browser quickstart with the TS SDK:

```ts
import { AgentDB } from "@agentdb/client";

const client = new AgentDB({
  baseUrl: "https://agentdb.example.com",
  tenant: "t-1",
  getToken: async () => await myAuth.getJwt(),
});

for await (const event of client.subscribe({ session: "s-1", fromSeq: lastSeen })) {
  ui.render(event);
  lastSeen = event.seq;
}
```

Python (backend) quickstart:

```python
from agentdb import AgentDB
client = AgentDB(base_url="http://127.0.0.1:7878", tenant="t-1", basic_auth=("user","pass"))
for evt in client.subscribe(session="s-1", from_seq=42):
    handle(evt)
```

See [`docs/superpowers/specs/2026-04-25-pubsub-design.md`](docs/superpowers/specs/2026-04-25-pubsub-design.md) for the full design.
```

- [ ] **Step 2: agent-db.md addition**

In `docs/agent-db.md`, add subscribe to the agent-native primitive list:

```markdown
- **Real-time subscribe** (`GET …/subscribe`): SSE tail of session events
  with `from_seq` replay. Same durable log; no auxiliary table or
  replication slot. JWT (HMAC or JWKS) for browser-direct use; CORS
  per-tenant.
```

- [ ] **Step 3: ARCHITECTURE.md additions**

Add a new subsection after "Stream / SSE":

```markdown
### EventBus + subscribe (Phase 1 pub/sub)

`crates/agentdb-api/src/event_bus.rs` defines a process-wide
`EventBus` (separate from `StreamBus` so user-event volume doesn't
compete with system-event buffer). Every successful `append_event`
emits an `EventEnvelope { tenant, session, seq, ... }` to the bus.

`crates/agentdb-api/src/subscribe.rs` serves SSE subscribers. The
race-safe pattern:

1. Subscribe to bus first.
2. Snapshot `live_seq = session.next_seq`.
3. Catch-up scan via `fetch_events_from_seq([from_seq, live_seq))`.
4. Live tail, dropping any envelope with `seq < live_seq`.

Lag policy: when `async_broadcast::RecvError::Overflowed` fires, the
server sends `event: error data: { error: lag_too_far, last_delivered_seq }`
and closes. Client reconnects with `from_seq = last_delivered_seq + 1`
and resumes from the durable log.

JWT: `crates/agentdb-api/src/jwt.rs` verifies per-tenant configured
HMAC or JWKS keys. Subscribe endpoints accept JWT via
`Authorization: Bearer …` header OR `?access_token=…` query param
(EventSource fallback); both at once is rejected as 400.
On `exp` expiry mid-stream the server emits
`event: error data: { error: jwt_expired, last_seq }` and closes.

CORS: `crates/agentdb-api/src/cors.rs` echoes `Origin` only if it
matches the tenant's `AGENTDB_TENANT_<id>_CORS_ORIGINS` list.
Wildcard `*` rejected when JWT auth is in play.
```

- [ ] **Step 4: Runbooks**

Each runbook file follows the existing pattern. Template (used for all five):

`docs/runbooks/jwt_expired.md`:

```markdown
# `jwt_expired`

The JWT presented to the subscribe endpoint has passed its `exp` time.

## Symptom

The subscribe SSE stream emits:

```
event: error
data: {"error":"jwt_expired","last_seq":<N>}
```

then closes the connection.

## Cause

Tokens are short-lived by design. The server checks `exp` on each
delivery; once past, the stream is closed so the client can refresh.

## Action

The official SDKs handle this automatically — calling code should not
need to act. If you're integrating directly:

1. Refresh the JWT via your auth provider.
2. Reconnect with `?from_seq=<last_seq + 1>` from the error payload.
3. No events are lost because `from_seq` replays from the durable log.
```

`docs/runbooks/jwt_invalid.md` — similar template; cause "signature/issuer/tenant claim mismatch", action "fix token minting".

`docs/runbooks/session_not_in_jwt.md` — cause "JWT `sessions` claim doesn't include the requested session", action "mint a token whose `sessions` includes the session, or use `*`".

`docs/runbooks/lag_too_far.md` — cause "subscriber fell behind broadcast buffer capacity", action "reconnect with `from_seq = last_delivered_seq + 1`; consume faster or process less per event".

`docs/runbooks/origin_not_allowed.md` — cause "browser Origin not in tenant's `AGENTDB_TENANT_<id>_CORS_ORIGINS`", action "add the origin to the env var, restart server".

- [ ] **Step 5: Run docs lint** (if any). At minimum, check rendered Markdown:

```bash
# Open in viewer of choice; verify the new sections render and links resolve.
```

- [ ] **Step 6: Commit**

```bash
git add README.md docs/agent-db.md docs/ARCHITECTURE.md docs/runbooks/jwt_expired.md docs/runbooks/jwt_invalid.md docs/runbooks/session_not_in_jwt.md docs/runbooks/lag_too_far.md docs/runbooks/origin_not_allowed.md
git commit -m "docs(pubsub): README quickstart, ARCHITECTURE flows, runbooks"
```

---

### Task 15: Final verification + smoke

- [ ] **Step 1: Run full Rust suite**

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```
Expected: all green.

- [ ] **Step 2: Existing smoke tests still pass**

```bash
bash scripts/smoke.sh
```

- [ ] **Step 3: New end-to-end smoke**

Add to a new `scripts/smoke-pubsub.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
PORT=$((10000 + RANDOM % 50000))
AGENTDB_ADDR="127.0.0.1:${PORT}" cargo run --quiet -p agentdb-server -- serve --dev &
PID=$!
trap "kill $PID 2>/dev/null || true" EXIT
sleep 3
BASE="http://127.0.0.1:${PORT}"
# Append three events
for i in 1 2 3; do
  curl -s -X POST "$BASE/api/tenants/t-1/sessions/s-1/append_and_fetch" \
    -H 'content-type: application/json' \
    -d "{\"event\":{\"kind\":\"smoke\",\"n\":$i},\"n\":0}" > /dev/null
done
# Subscribe with from_seq=1; expect 3 events plus subscribed control.
COUNT=$(curl -s -N --max-time 3 "$BASE/api/tenants/t-1/sessions/s-1/subscribe?from_seq=1" \
  | grep -c '^data:' || true)
test "$COUNT" -ge 4
echo "smoke-pubsub OK"
```

```bash
chmod +x scripts/smoke-pubsub.sh
bash scripts/smoke-pubsub.sh
```

- [ ] **Step 4: Python SDK smoke**

```bash
cd clients/python && pytest
```

- [ ] **Step 5: TypeScript SDK smoke**

```bash
cd clients/typescript && npm test
```

- [ ] **Step 6: Commit smoke script**

```bash
git add scripts/smoke-pubsub.sh
git commit -m "test(pubsub): end-to-end smoke covering append → subscribe → from_seq replay"
```

- [ ] **Step 7: Final review**

```bash
git log --oneline main..HEAD
git diff main..HEAD --stat
```

Expected: ~15 commits, ~3500-5000 LoC across crates + clients + examples + docs + tests.

---

## Self-Review

**Spec coverage check** (against [`docs/superpowers/specs/2026-04-25-pubsub-design.md`](../specs/2026-04-25-pubsub-design.md)):

| Spec section | Covered by task |
|---|---|
| § 6.1 API surface (per-session + tenant-wide endpoints) | Task 4, 7 |
| § 6.2 Wire format (JSON envelope) | Task 1 (`EventEnvelope`) + Task 4 (`subscribed` control event) |
| § 6.3 Replay semantics (catch-up + live, race-safe) | Task 5 |
| § 6.4 Filtering by `kind` | Task 4 |
| § 6.5 Tenant-wide subscriptions + `from_seq_per_session` | Task 7 |
| § 6.6 Lag-too-far close | Task 6 |
| § 6.7 EventBus (Option Y, separate from StreamBus) | Task 1, 2 |
| § 6.8 Multi-node 421 | Task 8 |
| § 6.9 Forks (subscriber sees only its session) | Implicit in Task 4 (per-session filter) — no extra code needed |
| § 6.10 Mongo wire (out of scope) | Not implemented — correctly out of scope |
| § 6.11 Mapping table (docs) | Task 14 |
| § 6.12 Latency profile (docs) | Task 14 (ARCHITECTURE.md addition) |
| § 6.13 Browser-direct (JWT, transports, expiry, CORS, lifecycle) | Tasks 9a–9d, 10 |

All Phase 1 spec sections covered. § 8 open questions are deferred decisions, not implementation requirements.

**Type consistency check:** Names used across tasks:
- `EventEnvelope` (Task 1) — referenced in Tasks 2, 4, 5, 6, 7. Consistent.
- `EventBus` (Task 1) — referenced in Tasks 2, 4, 5, 6, 7. Consistent.
- `JwtConfig`, `TenantJwtConfig`, `AgentDbClaims`, `SessionsClaim` (Task 9a) — referenced in Tasks 9b, 9c, 9d. Consistent.
- `subscribe_session`, `subscribe_tenant` (Tasks 4, 7) — referenced in Tasks 8, 9c, 10. Consistent.
- `fetch_events_from_seq` (Task 3) — referenced in Tasks 5, 7 via `scan_events_from_seq` wrapper. Consistent.
- `last_delivered_seq` vs `last_seq` in error frames: lag-too-far uses `last_delivered_seq`, jwt_expired uses `last_seq`. Both are documented; runbooks reflect them. Intentional difference (lag is about what was actually delivered; expiry is the same, but the original spec § 6.13 says `last_seq` for jwt — keeping spec naming).

**Placeholder scan:** No "TBD", "TODO", "implement later" left. Code samples either (a) show the actual lines to write, or (b) reference an existing helper by name in the existing codebase (e.g. "use the same `resolve_session_owner` helper that `handlers::append_and_fetch` uses to forward") — these are not placeholders, they are pointers to existing code the implementer must read.

Some Rust code samples are illustrative shapes rather than paste-ready (e.g. `AppState::no_auth(... existing args ... stream, event_bus)` — the implementer must read the existing constructor signature). This is intentional given plan size: paste-ready code for ~5000 LoC would balloon the plan past usefulness, and the existing codebase is the source of truth for those signatures. Tests are paste-ready because they are the spec for "done".

**Scope check:** This plan implements Phase 1 in full as scoped in the spec § 5 / § 7. Phases 2 (broadcast) and 3 (presence) are explicitly out of scope and not included.
