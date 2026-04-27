# Pub/Sub Phase 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship ephemeral broadcast channels on AgentDB — `POST .../publish` + `GET .../subscribe` over SSE — reusing Phase 1's JWT, CORS, and SDK plumbing.

**Architecture:** New process-wide `BroadcastBus` keyed by `(tenant, channel)` with per-channel `async_broadcast` senders, lazy-allocated, GC'd on idle. JWT gains a `channels` claim alongside Phase 1's `sessions`. New routes layered with the same CORS middleware as Phase 1 subscribe routes. Per-node local fan-out only (no cross-node forwarding in v1). All three SDKs gain `broadcast()` + `subscribe_channel()`.

**Tech Stack:** Rust (axum, tokio, async-broadcast, jsonwebtoken — all already in workspace from Phase 1). Python (httpx — already in main's SDK). TypeScript (fetch streaming — already in main's SDK). Go (net/http — already in main's SDK).

**Spec:** [`docs/superpowers/specs/2026-04-26-pubsub-phase2-design.md`](../specs/2026-04-26-pubsub-phase2-design.md) — read before starting.

**Branch:** `feature/pubsub-phase2` (will be created in a worktree before Task 1 starts).

---

## File Structure

### Created (Rust)

| File | Responsibility |
|---|---|
| `crates/agentdb-api/src/broadcast_bus.rs` | `BroadcastBus`, `BroadcastEnvelope`, `ChannelKey`, channel-name validation, GC. |
| `crates/agentdb-api/src/broadcast.rs` | `publish` and `subscribe_channel` HTTP handlers, query/body types. |
| `crates/agentdb-api/tests/broadcast_publish.rs` | Integration: publish → echo back to subscriber, body validation, 413, 400. |
| `crates/agentdb-api/tests/broadcast_subscribe.rs` | Integration: multi-subscriber fan-out, channel + tenant isolation. |
| `crates/agentdb-api/tests/broadcast_lag.rs` | Integration: capacity overflow does NOT close the stream. |
| `crates/agentdb-api/tests/broadcast_jwt.rs` | Integration: `channels` claim wildcard + list + missing. |
| `crates/agentdb-api/tests/broadcast_cors.rs` | Integration: CORS Origin echo + OPTIONS preflight on channel routes. |

### Modified (Rust)

| File | Change |
|---|---|
| `crates/agentdb-api/src/lib.rs` | Add `pub mod broadcast_bus;`, `pub mod broadcast;`. Add `pub broadcast_bus: BroadcastBus` to `AppState`. Mount channel routes in a `channel_routes` subtree alongside `subscribe_routes` with the same CORS layer. |
| `crates/agentdb-api/src/jwt.rs` | Add `channels: Option<SessionsClaim>` to `AgentDbClaims`. |
| `crates/agentdb-api/src/error.rs` | Add `ChannelNotInJwt`, `ChannelNameInvalid` to `ErrorCode` (and `doc_url_for` mapping). |
| `bin/agentdb-server/src/serve.rs` | Wire `BroadcastBus::default()` into `AppState`. Spawn the GC sweep task. |
| `crates/agentdb-api/tests/common/mod.rs` | Extend `start_inner` to accept `BroadcastBus` (or default it). Add `mint_hmac_token_with_channels` helper. |

### Created (SDKs)

| File | Responsibility |
|---|---|
| `clients/python/agentdb/_broadcast.py` | `BroadcastMessage` dataclass + sync/async iterator helpers. |
| `clients/typescript/src/broadcast.ts` | `BroadcastMessage` interface + parsing helpers. |
| `clients/go/agentdb/broadcast.go` | `BroadcastMessage` struct + `Broadcast` / `SubscribeChannel` methods. |

### Modified (SDKs)

| File | Change |
|---|---|
| `clients/python/agentdb/client.py` | Add `Client.broadcast(...)` and `Client.subscribe_channel(...)`. |
| `clients/python/agentdb/_async_client.py` | Add async equivalents. |
| `clients/python/agentdb/__init__.py` | Re-export `BroadcastMessage`. |
| `clients/typescript/src/client.ts` | Add `Client.broadcast(...)` and `Client.subscribeChannel(...)`. |
| `clients/typescript/src/index.ts` | Re-export `BroadcastMessage`. |
| `clients/go/agentdb/client.go` | (no changes — methods live in `broadcast.go`). |
| Each SDK's tests directory | Add unit tests for parsing + gated smoke tests. |

### Created (examples + docs)

| File | Responsibility |
|---|---|
| `examples/agents/typescript/broadcast_typing.ts` | Typing-indicator demo: agent A publishes "typing" pings; agent B subscribes and prints them. |
| `examples/agents/typescript/README.md` | Append a section for the new demo. |
| `docs/runbooks/channel_not_in_jwt.md` | Runbook for the new error code. |
| `scripts/smoke-broadcast.sh` | End-to-end shell smoke. |

### Modified (docs)

| File | Change |
|---|---|
| `README.md` | Add "Ephemeral broadcast" subsection under "Real-time subscriptions". |
| `docs/agent-db.md` | Add broadcast to the agent-native primitive list. |
| `docs/ARCHITECTURE.md` | Add "Phase 2: ephemeral broadcast" subsection under existing Pub/Sub. |
| `docs/runbooks/README.md` | List the new runbook. |

---

## Tasks

Ordered for dependency: bus first, then handlers, then routes, then SDKs, then docs. Each task ends in a commit.

---

### Task 1: BroadcastBus module

Pure new module with inline tests. No wiring (Task 2 wires it into `AppState`).

**Files:**
- Create: `crates/agentdb-api/src/broadcast_bus.rs`
- Modify: `crates/agentdb-api/src/lib.rs` (add `pub mod broadcast_bus;`)

- [ ] **Step 1: Write the failing tests**

Create `crates/agentdb-api/src/broadcast_bus.rs`:

```rust
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_broadcast::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// Channel-keyed broadcast envelope. Tenant-scoped, no seq, no replay.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BroadcastEnvelope {
    pub tenant: String,
    pub channel: String,
    pub ts_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<String>,
    pub payload: serde_json::Value,
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct ChannelKey {
    pub tenant: String,
    pub channel: String,
}

#[derive(Clone, Default)]
pub struct BroadcastBus {
    inner: Arc<RwLock<HashMap<ChannelKey, ChannelEntry>>>,
}

#[derive(Clone)]
struct ChannelEntry {
    tx: Sender<BroadcastEnvelope>,
    subscriber_count: Arc<AtomicUsize>,
    last_publish_ts: Arc<AtomicU64>,
}

const PER_CHANNEL_CAPACITY: usize = 256;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl BroadcastBus {
    /// Allocate (if needed) and return a sender for the channel.
    /// Internal helper used by both publish and subscribe.
    async fn ensure_entry(&self, key: &ChannelKey) -> ChannelEntry {
        {
            let g = self.inner.read().await;
            if let Some(e) = g.get(key) {
                return e.clone();
            }
        }
        let mut g = self.inner.write().await;
        if let Some(e) = g.get(key) {
            return e.clone();
        }
        let (mut tx, rx) = async_broadcast::broadcast(PER_CHANNEL_CAPACITY);
        tx.set_overflow(true);
        std::mem::forget(rx);
        let entry = ChannelEntry {
            tx,
            subscriber_count: Arc::new(AtomicUsize::new(0)),
            last_publish_ts: Arc::new(AtomicU64::new(now_ms())),
        };
        g.insert(key.clone(), entry.clone());
        entry
    }

    pub async fn publish(&self, key: &ChannelKey, env: BroadcastEnvelope) {
        let entry = self.ensure_entry(key).await;
        entry.last_publish_ts.store(now_ms(), Ordering::Relaxed);
        let _ = entry.tx.try_broadcast(env);
    }

    pub async fn subscribe(&self, key: &ChannelKey) -> BroadcastSubscription {
        let entry = self.ensure_entry(key).await;
        entry.subscriber_count.fetch_add(1, Ordering::Relaxed);
        BroadcastSubscription {
            rx: entry.tx.new_receiver(),
            count: entry.subscriber_count.clone(),
        }
    }

    /// Drop entries with zero subscribers and last publish older than `idle_ms`.
    /// Returns the number of entries reclaimed.
    pub async fn gc(&self, idle_ms: u64) -> usize {
        let cutoff = now_ms().saturating_sub(idle_ms);
        let mut g = self.inner.write().await;
        let before = g.len();
        g.retain(|_k, entry| {
            let subs = entry.subscriber_count.load(Ordering::Relaxed);
            let last_pub = entry.last_publish_ts.load(Ordering::Relaxed);
            subs > 0 || last_pub > cutoff
        });
        before - g.len()
    }
}

pub struct BroadcastSubscription {
    rx: Receiver<BroadcastEnvelope>,
    count: Arc<AtomicUsize>,
}

impl BroadcastSubscription {
    pub async fn recv(&mut self) -> Result<BroadcastEnvelope, async_broadcast::RecvError> {
        self.rx.recv().await
    }
}

impl Drop for BroadcastSubscription {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Validate a channel name. Spec § 5.7: `^[a-zA-Z0-9._:-]{1,256}$`.
pub fn validate_channel_name(name: &str) -> bool {
    let len = name.len();
    if len == 0 || len > 256 {
        return false;
    }
    name.bytes().all(|b| {
        b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b':' || b == b'-'
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(tenant: &str, channel: &str) -> ChannelKey {
        ChannelKey { tenant: tenant.into(), channel: channel.into() }
    }

    fn env(channel: &str, payload: serde_json::Value) -> BroadcastEnvelope {
        BroadcastEnvelope {
            tenant: "t-1".into(),
            channel: channel.into(),
            ts_ms: 1_000,
            sender: None,
            payload,
        }
    }

    #[tokio::test]
    async fn publish_then_receive_in_order() {
        let bus = BroadcastBus::default();
        let k = key("t-1", "c-1");
        let mut sub = bus.subscribe(&k).await;
        bus.publish(&k, env("c-1", serde_json::json!({"n": 1}))).await;
        bus.publish(&k, env("c-1", serde_json::json!({"n": 2}))).await;
        let a = sub.recv().await.unwrap();
        let b = sub.recv().await.unwrap();
        assert_eq!(a.payload["n"], 1);
        assert_eq!(b.payload["n"], 2);
    }

    #[tokio::test]
    async fn channels_are_isolated() {
        let bus = BroadcastBus::default();
        let k1 = key("t-1", "c-1");
        let k2 = key("t-1", "c-2");
        let mut sub1 = bus.subscribe(&k1).await;
        let mut sub2 = bus.subscribe(&k2).await;
        bus.publish(&k1, env("c-1", serde_json::json!({}))).await;
        // sub1 sees it, sub2 does not (within a short timeout).
        assert!(tokio::time::timeout(std::time::Duration::from_millis(50), sub1.recv())
            .await
            .is_ok());
        assert!(tokio::time::timeout(std::time::Duration::from_millis(50), sub2.recv())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn tenants_are_isolated() {
        let bus = BroadcastBus::default();
        let ka = key("t-a", "c-1");
        let kb = key("t-b", "c-1");
        let mut suba = bus.subscribe(&ka).await;
        let mut subb = bus.subscribe(&kb).await;
        bus.publish(&ka, env("c-1", serde_json::json!({}))).await;
        assert!(tokio::time::timeout(std::time::Duration::from_millis(50), suba.recv())
            .await
            .is_ok());
        assert!(tokio::time::timeout(std::time::Duration::from_millis(50), subb.recv())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn slow_subscriber_overflows_silently() {
        let bus = BroadcastBus::default();
        let k = key("t-1", "c-1");
        let mut sub = bus.subscribe(&k).await;
        for i in 0..(PER_CHANNEL_CAPACITY * 4) {
            bus.publish(&k, env("c-1", serde_json::json!({"n": i}))).await;
        }
        // Drain whatever's available — overflow returns Err but recv() will
        // also resync. The point is that publish never panicked.
        let mut got = 0;
        for _ in 0..PER_CHANNEL_CAPACITY {
            match tokio::time::timeout(std::time::Duration::from_millis(20), sub.recv()).await {
                Ok(Ok(_)) => got += 1,
                _ => break,
            }
        }
        assert!(got > 0, "expected at least one delivered envelope");
    }

    #[tokio::test]
    async fn gc_drops_idle_entries_with_zero_subscribers() {
        let bus = BroadcastBus::default();
        let k = key("t-1", "c-idle");
        bus.publish(&k, env("c-idle", serde_json::json!({}))).await;
        // No subscriber held. Wait > idle threshold and GC.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let reclaimed = bus.gc(10).await;
        assert_eq!(reclaimed, 1);
    }

    #[tokio::test]
    async fn gc_keeps_entries_with_active_subscribers() {
        let bus = BroadcastBus::default();
        let k = key("t-1", "c-live");
        let _sub = bus.subscribe(&k).await;
        bus.publish(&k, env("c-live", serde_json::json!({}))).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let reclaimed = bus.gc(10).await;
        assert_eq!(reclaimed, 0);
    }

    #[test]
    fn channel_name_validation() {
        assert!(validate_channel_name("room-42"));
        assert!(validate_channel_name("room:42"));
        assert!(validate_channel_name("team.alpha.standup"));
        assert!(validate_channel_name("a"));
        assert!(validate_channel_name(&"x".repeat(256)));
        assert!(!validate_channel_name(""));
        assert!(!validate_channel_name(&"x".repeat(257)));
        assert!(!validate_channel_name("with/slash"));
        assert!(!validate_channel_name("with space"));
        assert!(!validate_channel_name("emoji😀"));
    }

    #[test]
    fn envelope_round_trips_through_json() {
        let e = BroadcastEnvelope {
            tenant: "t-1".into(),
            channel: "c-1".into(),
            ts_ms: 42,
            sender: Some("sub".into()),
            payload: serde_json::json!({"k": "v"}),
        };
        let s = serde_json::to_string(&e).unwrap();
        let back: BroadcastEnvelope = serde_json::from_str(&s).unwrap();
        assert_eq!(e, back);
    }
}
```

Add `pub mod broadcast_bus;` to `crates/agentdb-api/src/lib.rs` next to `pub mod broadcast;` (which Task 5 will add — for now just `broadcast_bus`).

- [ ] **Step 2: Run tests, expect pass**

```bash
cargo test -p agentdb-api --lib broadcast_bus
```
Expected: 8 tests pass.

- [ ] **Step 3: clippy**

```bash
cargo clippy -p agentdb-api --lib -- -D warnings
```

- [ ] **Step 4: Commit**

```bash
git add crates/agentdb-api/src/broadcast_bus.rs crates/agentdb-api/src/lib.rs
git commit -m "feat(pubsub): BroadcastBus with per-channel capacity + GC"
```

---

### Task 2: Wire BroadcastBus into AppState

**Files:**
- Modify: `crates/agentdb-api/src/lib.rs` (add field to `AppState`, threading through `no_auth`)
- Modify: `bin/agentdb-server/src/serve.rs` (construct + pass `BroadcastBus::default()`)
- Modify: every `AppState { ... }` struct-literal site
- Modify: `crates/agentdb-api/tests/common/mod.rs` (extend `start_inner` to accept the bus)

- [ ] **Step 1: Add field to `AppState`**

In `crates/agentdb-api/src/lib.rs`, locate the `AppState` struct (it has `event_bus`, `jwt_config`, `cors_config` from Phase 1). Add:

```rust
use crate::broadcast_bus::BroadcastBus;

pub struct AppState {
    // ... existing fields (registry, kv, event_bus, jwt_config, cors_config, etc.) ...
    pub broadcast_bus: BroadcastBus,
}
```

In `AppState::no_auth`:

```rust
impl AppState {
    pub fn no_auth(/* existing args */) -> Self {
        Self {
            // ... existing initializations ...
            broadcast_bus: BroadcastBus::default(),
        }
    }
}
```

Re-export at the top of `lib.rs`:

```rust
pub use broadcast_bus::{BroadcastBus, BroadcastEnvelope};
```

- [ ] **Step 2: Update `bin/agentdb-server/src/serve.rs`**

Find the `AppState { ... }` struct literal that builds the production state. Add:

```rust
broadcast_bus: agentdb_api::BroadcastBus::default(),
```

- [ ] **Step 3: Update every other struct-literal site**

```bash
grep -rn 'AppState {' --include='*.rs' /Users/lishen/work/agentdb
```

For each site, add `broadcast_bus: agentdb_api::BroadcastBus::default(),` (or `broadcast_bus: BroadcastBus::default(),` depending on imports).

Phase 1 found ~24 sites; verify the count hasn't drifted on main. Don't miss `crates/agentdb-wire/tests/driver.rs`.

- [ ] **Step 4: Update `tests/common/mod.rs`**

Extend the private `start_inner` constructor to accept a `BroadcastBus`:

```rust
async fn start_inner(
    event_bus: EventBus,
    jwt_config: JwtConfig,
    cors_config: CorsConfig,
    broadcast_bus: BroadcastBus,
) -> Self {
    // ... existing body, threading broadcast_bus into AppState struct literal ...
}
```

Update existing public constructors (`start`, `start_with_event_bus_capacity`, `start_with_jwt_hmac`, `start_with_cors`) to pass `BroadcastBus::default()`.

- [ ] **Step 5: Build + test**

```bash
cargo build --workspace
cargo test --workspace -p agentdb-api -p agentdb-wire
```

All existing tests must still pass.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(pubsub): wire BroadcastBus into AppState across workspace"
```

---

### Task 3: Add `channels` claim to `AgentDbClaims` + new `ErrorCode` variants

**Files:**
- Modify: `crates/agentdb-api/src/jwt.rs`
- Modify: `crates/agentdb-api/src/error.rs`
- Modify: `crates/agentdb-api/tests/snapshots/openapi.json` (regenerate with the new ErrorCode variants)

- [ ] **Step 1: Extend `AgentDbClaims`**

In `crates/agentdb-api/src/jwt.rs`:

```rust
pub struct AgentDbClaims {
    pub iss: String,
    #[serde(default)]
    pub aud: Option<String>,
    pub sub: String,
    pub tenant: String,
    pub sessions: SessionsClaim,
    /// Phase 2: channel access for ephemeral broadcast.
    /// `None` = no broadcast access (token can still subscribe to sessions).
    /// `Some(Wildcard("*"))` = any channel in the tenant.
    /// `Some(List([...]))` = whitelist of channel names.
    #[serde(default)]
    pub channels: Option<SessionsClaim>,
    pub exp: u64,
}
```

- [ ] **Step 2: Add unit tests in `jwt.rs`**

Append to the `#[cfg(test)] mod tests` block:

```rust
#[test]
fn channels_claim_defaults_to_none() {
    let raw = r#"{
        "iss": "https://auth.test",
        "sub": "u",
        "tenant": "t-1",
        "sessions": "*",
        "exp": 9999999999
    }"#;
    let claims: AgentDbClaims = serde_json::from_str(raw).unwrap();
    assert!(claims.channels.is_none());
}

#[test]
fn channels_claim_wildcard_parses() {
    let raw = r#"{
        "iss": "i", "sub": "s", "tenant": "t-1",
        "sessions": "*", "channels": "*", "exp": 9999999999
    }"#;
    let claims: AgentDbClaims = serde_json::from_str(raw).unwrap();
    let ch = claims.channels.unwrap();
    assert!(matches!(ch, SessionsClaim::Wildcard(ref s) if s == "*"));
    assert!(ch.allows("anything"));
}

#[test]
fn channels_claim_list_parses_and_filters() {
    let raw = r#"{
        "iss": "i", "sub": "s", "tenant": "t-1",
        "sessions": "*", "channels": ["a", "b"], "exp": 9999999999
    }"#;
    let claims: AgentDbClaims = serde_json::from_str(raw).unwrap();
    let ch = claims.channels.unwrap();
    assert!(ch.allows("a"));
    assert!(ch.allows("b"));
    assert!(!ch.allows("c"));
}
```

- [ ] **Step 3: Add `ErrorCode` variants**

In `crates/agentdb-api/src/error.rs`, locate the `ErrorCode` enum (Phase 1 added `JwtInvalid`, `JwtExpired`, etc.). Add:

```rust
/// Token's `channels` claim doesn't allow the requested channel.
ChannelNotInJwt,
/// Channel name fails validation (spec § 5.7 regex).
ChannelNameInvalid,
```

Add their `doc_url_for` mapping (return `Some("/docs/runbooks/channel_not_in_jwt.md")` for `ChannelNotInJwt`, `None` for `ChannelNameInvalid`).

- [ ] **Step 4: Regenerate the OpenAPI snapshot**

```bash
# Find how Phase 1 regenerated this snapshot — check existing tests/scripts.
# Likely either an env var on the test or a separate script.
grep -rn 'regenerate.*openapi\|update.*snapshot' /Users/lishen/work/agentdb
```

Then regenerate, OR if there's no automated way, manually edit `crates/agentdb-api/tests/snapshots/openapi.json` to add the two new enum strings (`channel_not_in_jwt`, `channel_name_invalid`) in the same place Phase 1's variants live.

- [ ] **Step 5: Build + test**

```bash
cargo test -p agentdb-api --lib jwt
cargo test -p agentdb-api openapi    # snapshot test
```

- [ ] **Step 6: Commit**

```bash
git add crates/agentdb-api/src/jwt.rs crates/agentdb-api/src/error.rs crates/agentdb-api/tests/snapshots/openapi.json
git commit -m "feat(pubsub): channels JWT claim + ChannelNotInJwt/ChannelNameInvalid codes"
```

---

### Task 4: `POST /publish` handler

JWT enforcement, body validation, channel-name validation, 16 KiB body cap.

**Files:**
- Create: `crates/agentdb-api/src/broadcast.rs` (will hold both `publish` and `subscribe_channel`)
- Modify: `crates/agentdb-api/src/lib.rs` (add `pub mod broadcast;`, mount route in next task)
- Create: `crates/agentdb-api/tests/broadcast_publish.rs`

- [ ] **Step 1: Write the failing tests**

Create `crates/agentdb-api/tests/broadcast_publish.rs`:

```rust
mod common;
use common::TestServer;

#[tokio::test]
async fn publish_returns_204_and_emits_to_bus() {
    let server = TestServer::start().await;
    let mut sub = server
        .state
        .broadcast_bus
        .subscribe(&agentdb_api::broadcast_bus::ChannelKey {
            tenant: "t-1".into(),
            channel: "c-1".into(),
        })
        .await;

    let resp = reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-1/publish", server.base))
        .json(&serde_json::json!({"kind": "typing"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    let env = tokio::time::timeout(std::time::Duration::from_millis(500), sub.recv())
        .await
        .expect("envelope within 500ms")
        .expect("recv ok");
    assert_eq!(env.tenant, "t-1");
    assert_eq!(env.channel, "c-1");
    assert_eq!(env.payload["kind"], "typing");
    assert!(env.sender.is_none()); // no JWT, no sub
}

#[tokio::test]
async fn publish_with_invalid_channel_name_returns_400() {
    let server = TestServer::start().await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/with%2Fslash/publish", server.base))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    // Note: percent-encoded slash in URL — but since slashes are excluded by
    // both the regex and the route pattern, axum may 404 on the path itself.
    // The handler will see "with/slash" once decoded; it MUST 400.
    // Adjust the assertion based on what axum does for percent-encoded /.
    assert!(resp.status() == 400 || resp.status() == 404);
}

#[tokio::test]
async fn publish_too_large_returns_413() {
    let server = TestServer::start().await;
    let big = "x".repeat(20 * 1024); // 20 KiB > 16 KiB cap
    let resp = reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-1/publish", server.base))
        .json(&serde_json::json!({"data": big}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413);
}

#[tokio::test]
async fn publish_with_basic_auth_has_no_sender() {
    // Tenant has no JWT config → falls through; sender stays None.
    let server = TestServer::start().await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-1/publish", server.base))
        .json(&serde_json::json!({"hello": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
}
```

- [ ] **Step 2: Run, expect failure**

```bash
cargo test -p agentdb-api --test broadcast_publish
```
Expected: route not found (404 on every test).

- [ ] **Step 3: Implement the publish handler**

Create `crates/agentdb-api/src/broadcast.rs`:

```rust
//! Phase 2 ephemeral broadcast handlers (spec § 5.1, § 5.5, § 5.7).

use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::Value;

use crate::broadcast_bus::{validate_channel_name, BroadcastEnvelope, ChannelKey};
use crate::error::{ApiError, ErrorCode};
use crate::AppState;

/// 16 KiB cap on broadcast bodies (spec § 5.5).
pub const MAX_BODY_BYTES: usize = 16 * 1024;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub async fn publish(
    State(state): State<AppState>,
    Path((tenant, channel)): Path<(String, String)>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if !validate_channel_name(&channel) {
        return ApiError::Coded(
            StatusCode::BAD_REQUEST,
            ErrorCode::ChannelNameInvalid,
            "channel name fails validation".into(),
        ).into_response();
    }
    if body.len() > MAX_BODY_BYTES {
        return ApiError::Coded(
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorCode::Internal, // or a dedicated code if the codebase has one
            format!("body exceeds {} bytes", MAX_BODY_BYTES),
        ).into_response();
    }

    // JWT enforcement (only if tenant has JWT config).
    let claims = match crate::subscribe_auth::extract_subscribe_jwt(
        &state, &headers, None, &tenant,
    ).await {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    if let Some(c) = &claims {
        let allowed = c.channels
            .as_ref()
            .map(|ch| ch.allows(&channel))
            .unwrap_or(false);
        if !allowed {
            return ApiError::Coded(
                StatusCode::FORBIDDEN,
                ErrorCode::ChannelNotInJwt,
                format!("jwt channels claim does not allow channel {channel}"),
            ).into_response();
        }
    }

    // Parse body as JSON (broadcast payloads are arbitrary JSON values).
    let payload: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::Coded(
                StatusCode::BAD_REQUEST,
                ErrorCode::Internal,
                format!("malformed json body: {e}"),
            ).into_response();
        }
    };

    let env = BroadcastEnvelope {
        tenant: tenant.clone(),
        channel: channel.clone(),
        ts_ms: now_ms(),
        sender: claims.as_ref().map(|c| c.sub.clone()),
        payload,
    };
    let key = ChannelKey { tenant, channel };
    state.broadcast_bus.publish(&key, env).await;

    StatusCode::NO_CONTENT.into_response()
}
```

If `ErrorCode` doesn't have a clean variant for `BAD_REQUEST` malformed JSON, add `MalformedBody` (or reuse the closest existing variant — `Internal` is the placeholder above; replace as appropriate).

Note: the 413 path uses `ErrorCode::Internal` as a placeholder above — find or add a more appropriate code (`PayloadTooLarge` or similar). Read the existing `ErrorCode` enum for the closest match.

In `crates/agentdb-api/src/lib.rs`:

```rust
pub mod broadcast;
```

(But don't mount the route yet — Task 6 mounts publish + subscribe together.)

- [ ] **Step 4: Add a temporary route mount for testing**

Per the plan structure, the official mount happens in Task 6. To make Task 4's tests pass NOW without conflating with Task 6, add a one-route mount inside `tenant_routes` (or an analogous existing place) just for `publish`:

```rust
.route(
    "/api/tenants/:tenant_id/channels/:channel/publish",
    axum::routing::post(broadcast::publish),
)
```

Task 6 will move this into the dedicated `channel_routes` subtree with CORS. For now this gets us a passing test.

- [ ] **Step 5: Run, expect pass**

```bash
cargo test -p agentdb-api --test broadcast_publish
```

- [ ] **Step 6: Commit**

```bash
git add crates/agentdb-api/src/broadcast.rs crates/agentdb-api/src/lib.rs crates/agentdb-api/src/error.rs crates/agentdb-api/tests/broadcast_publish.rs
git commit -m "feat(pubsub): POST /channels/:c/publish with JWT + body validation"
```

---

### Task 5: `GET /subscribe_channel` handler

JWT enforcement, optional `?no_echo=true`, SSE stream, no error frame on lag.

**Files:**
- Modify: `crates/agentdb-api/src/broadcast.rs` (add `subscribe_channel` handler)
- Modify: `crates/agentdb-api/src/lib.rs` (mount route — temporary, Task 6 reorganizes)
- Create: `crates/agentdb-api/tests/broadcast_subscribe.rs`

- [ ] **Step 1: Write the failing tests**

Create `crates/agentdb-api/tests/broadcast_subscribe.rs`:

```rust
mod common;
use common::TestServer;

#[tokio::test]
async fn multiple_subscribers_each_receive_every_publish() {
    let server = TestServer::start().await;
    let mut s1 = server.open_sse("/api/tenants/t-1/channels/c-1/subscribe").await;
    let mut s2 = server.open_sse("/api/tenants/t-1/channels/c-1/subscribe").await;

    reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-1/publish", server.base))
        .json(&serde_json::json!({"i": 1}))
        .send()
        .await
        .unwrap();
    reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-1/publish", server.base))
        .json(&serde_json::json!({"i": 2}))
        .send()
        .await
        .unwrap();

    for sub in [&mut s1, &mut s2] {
        let mut got = vec![];
        for _ in 0..2 {
            let frame = sub.next_event().await;
            let env: serde_json::Value = serde_json::from_str(&frame.data).unwrap();
            got.push(env["payload"]["i"].as_u64().unwrap());
        }
        got.sort();
        assert_eq!(got, vec![1, 2]);
    }
}

#[tokio::test]
async fn subscribers_on_different_channels_are_isolated() {
    let server = TestServer::start().await;
    let mut s = server.open_sse("/api/tenants/t-1/channels/c-1/subscribe").await;

    // Publish to c-2 — s (subscribed to c-1) must NOT see it.
    reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-2/publish", server.base))
        .json(&serde_json::json!({"x": 1}))
        .send()
        .await
        .unwrap();

    // Then publish to c-1 — s sees it.
    reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-1/publish", server.base))
        .json(&serde_json::json!({"x": 2}))
        .send()
        .await
        .unwrap();

    let frame = s.next_event().await;
    let env: serde_json::Value = serde_json::from_str(&frame.data).unwrap();
    assert_eq!(env["payload"]["x"], 2);
}

#[tokio::test]
async fn cross_tenant_isolation_holds() {
    let server = TestServer::start().await;
    let mut s = server.open_sse("/api/tenants/t-1/channels/c-1/subscribe").await;

    reqwest::Client::new()
        .post(format!("{}/api/tenants/t-other/channels/c-1/publish", server.base))
        .json(&serde_json::json!({"leak": true}))
        .send()
        .await
        .unwrap();
    reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-1/publish", server.base))
        .json(&serde_json::json!({"leak": false}))
        .send()
        .await
        .unwrap();

    let frame = s.next_event().await;
    let env: serde_json::Value = serde_json::from_str(&frame.data).unwrap();
    assert_eq!(env["payload"]["leak"], false);
    assert_eq!(env["tenant"], "t-1");
}
```

- [ ] **Step 2: Run, expect failure**

```bash
cargo test -p agentdb-api --test broadcast_subscribe
```

- [ ] **Step 3: Implement `subscribe_channel`**

Append to `crates/agentdb-api/src/broadcast.rs`:

```rust
use axum::extract::Query;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::stream::{self, Stream};
use serde::Deserialize;
use std::convert::Infallible;

#[derive(Debug, Deserialize)]
pub struct SubscribeChannelQuery {
    #[serde(default)]
    pub heartbeat_secs: Option<u64>,
    #[serde(default)]
    pub access_token: Option<String>,
    #[serde(default)]
    pub no_echo: Option<bool>,
}

pub async fn subscribe_channel(
    State(state): State<AppState>,
    Path((tenant, channel)): Path<(String, String)>,
    axum_extra::extract::Query(q): axum_extra::extract::Query<SubscribeChannelQuery>,
    headers: HeaderMap,
) -> Response {
    if !validate_channel_name(&channel) {
        return ApiError::Coded(
            StatusCode::BAD_REQUEST,
            ErrorCode::ChannelNameInvalid,
            "channel name fails validation".into(),
        ).into_response();
    }

    let claims = match crate::subscribe_auth::extract_subscribe_jwt(
        &state, &headers, q.access_token.as_deref(), &tenant,
    ).await {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    if let Some(c) = &claims {
        let allowed = c.channels
            .as_ref()
            .map(|ch| ch.allows(&channel))
            .unwrap_or(false);
        if !allowed {
            return ApiError::Coded(
                StatusCode::FORBIDDEN,
                ErrorCode::ChannelNotInJwt,
                format!("jwt channels claim does not allow channel {channel}"),
            ).into_response();
        }
    }

    let no_echo = q.no_echo.unwrap_or(false);
    let echo_filter_sub = no_echo.then(|| claims.as_ref().map(|c| c.sub.clone())).flatten();

    let key = ChannelKey { tenant, channel };
    let mut sub = state.broadcast_bus.subscribe(&key).await;

    let live = stream::unfold((sub, echo_filter_sub), move |(mut sub, drop_sender)| {
        async move {
            loop {
                match sub.recv().await {
                    Ok(env) => {
                        if let Some(d) = &drop_sender {
                            if env.sender.as_deref() == Some(d) {
                                continue;
                            }
                        }
                        let payload = match serde_json::to_string(&env) {
                            Ok(s) => s,
                            Err(_) => continue,
                        };
                        return Some((Ok::<_, Infallible>(Event::default().data(payload)), (sub, drop_sender)));
                    }
                    Err(async_broadcast::RecvError::Overflowed(_)) => continue, // silent drop
                    Err(async_broadcast::RecvError::Closed) => return None,
                }
            }
        }
    });

    let keepalive_secs = q.heartbeat_secs.unwrap_or(20);
    Sse::new(live)
        .keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(keepalive_secs)))
        .into_response()
}
```

In `lib.rs`, add a temporary route mount alongside the publish one:

```rust
.route(
    "/api/tenants/:tenant_id/channels/:channel/subscribe",
    axum::routing::get(broadcast::subscribe_channel),
)
```

(Task 6 reorganizes both routes under a dedicated subtree with CORS.)

- [ ] **Step 4: Run, expect pass**

```bash
cargo test -p agentdb-api --test broadcast_subscribe
cargo test -p agentdb-api --test broadcast_publish
```

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-api/src/broadcast.rs crates/agentdb-api/src/lib.rs crates/agentdb-api/tests/broadcast_subscribe.rs
git commit -m "feat(pubsub): GET /channels/:c/subscribe SSE stream + no_echo filter"
```

---

### Task 6: Mount channel routes with CORS layer

Reorganize the temporary mounts from Tasks 4 and 5 into a dedicated `channel_routes` subtree under the same CORS middleware as Phase 1's `subscribe_routes`.

**Files:**
- Modify: `crates/agentdb-api/src/lib.rs`

- [ ] **Step 1: Move mounts into `channel_routes` subtree**

In `crates/agentdb-api/src/lib.rs`, locate where `subscribe_routes` is built (Phase 1's separate Router subtree with CORS). Build a sibling:

```rust
let channel_routes: Router<AppState> = Router::new()
    .route(
        "/api/tenants/:tenant_id/channels/:channel/publish",
        axum::routing::post(broadcast::publish),
    )
    .route(
        "/api/tenants/:tenant_id/channels/:channel/subscribe",
        axum::routing::get(broadcast::subscribe_channel),
    )
    .layer(axum::middleware::from_fn_with_state(state.clone(), cors::cors_middleware));
```

Then merge it into the top-level router alongside `subscribe_routes`. Remove the temporary mounts added in Tasks 4 and 5.

- [ ] **Step 2: Verify Phase 1 routes still work**

```bash
cargo test -p agentdb-api --test subscribe_live --test subscribe_replay --test subscribe_lag --test subscribe_tenant --test subscribe_redirect --test subscribe_jwt --test subscribe_cors
```

All Phase 1 tests must still pass after the router restructure.

- [ ] **Step 3: Add the CORS test for channels**

Create `crates/agentdb-api/tests/broadcast_cors.rs`:

```rust
mod common;
use common::TestServer;

#[tokio::test]
async fn allowed_origin_echoed_on_publish() {
    let server = TestServer::start_with_cors("t-1", &["https://app.example.com"]).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-1/publish", server.base))
        .header("Origin", "https://app.example.com")
        .json(&serde_json::json!({"k": "v"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.headers().get("access-control-allow-origin").unwrap(),
        "https://app.example.com"
    );
}

#[tokio::test]
async fn allowed_origin_echoed_on_subscribe() {
    let server = TestServer::start_with_cors("t-1", &["https://app.example.com"]).await;
    let resp = reqwest::Client::new()
        .get(format!("{}/api/tenants/t-1/channels/c-1/subscribe", server.base))
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
async fn options_preflight_204_with_methods_and_headers() {
    let server = TestServer::start_with_cors("t-1", &["https://app.example.com"]).await;
    let resp = reqwest::Client::new()
        .request(
            reqwest::Method::OPTIONS,
            format!("{}/api/tenants/t-1/channels/c-1/publish", server.base),
        )
        .header("Origin", "https://app.example.com")
        .header("Access-Control-Request-Method", "POST")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    let methods = resp.headers().get("access-control-allow-methods").unwrap().to_str().unwrap();
    assert!(methods.contains("POST") || methods.contains("GET"));
}
```

- [ ] **Step 4: Run + commit**

```bash
cargo test -p agentdb-api --test broadcast_cors
cargo test -p agentdb-api    # full crate

git add crates/agentdb-api/src/lib.rs crates/agentdb-api/tests/broadcast_cors.rs
git commit -m "feat(pubsub): mount channel routes under CORS subtree"
```

---

### Task 7: GC sweep task

A background tokio task that calls `BroadcastBus::gc(idle_ms)` on a fixed interval.

**Files:**
- Modify: `bin/agentdb-server/src/serve.rs` — spawn the GC task at startup
- Add a simple test in `broadcast_bus.rs::tests` (already in Task 1)

- [ ] **Step 1: Spawn GC in `serve.rs`**

Find where the server's other periodic tasks are spawned (e.g., the auto-shed scheduler from Phase 1). Add:

```rust
// Phase 2 broadcast GC: every 30s, drop channels with 0 subs and no
// publish in the last 60s. Spec § 9 question #3.
{
    let bus = state.broadcast_bus.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            tick.tick().await;
            let _ = bus.gc(60_000).await;
        }
    });
}
```

(Place after `state` is fully constructed.)

- [ ] **Step 2: Build + test**

```bash
cargo build --workspace
cargo test --workspace -p agentdb-api
```

The GC task is hard to integration-test (requires running the server for >30s). The unit tests in Task 1 cover the `gc()` function itself; the spawn is verified by code inspection.

- [ ] **Step 3: Commit**

```bash
git add bin/agentdb-server/src/serve.rs
git commit -m "feat(pubsub): spawn periodic BroadcastBus GC task at startup"
```

---

### Task 8: JWT integration test for `channels` claim

**Files:**
- Create: `crates/agentdb-api/tests/broadcast_jwt.rs`
- Modify: `crates/agentdb-api/tests/common/mod.rs` (add `mint_hmac_token_with_channels`)

- [ ] **Step 1: Add the helper**

Append to `crates/agentdb-api/tests/common/mod.rs`:

```rust
pub fn mint_hmac_token_with_channels(
    secret: &[u8],
    tenant: &str,
    sessions: &str,
    channels: Option<&[&str]>,
    ttl_secs: u64,
) -> String {
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
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
        channels: channels.map(|chs| {
            if chs == ["*"] {
                SessionsClaim::Wildcard("*".into())
            } else {
                SessionsClaim::List(chs.iter().map(|s| s.to_string()).collect())
            }
        }),
        exp: now_secs() + ttl_secs,
    };
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret),
    )
    .unwrap()
}
```

- [ ] **Step 2: Write the integration test**

Create `crates/agentdb-api/tests/broadcast_jwt.rs`:

```rust
mod common;
use common::{TestServer, mint_hmac_token_with_channels};

#[tokio::test]
async fn channels_wildcard_allows_publish_and_subscribe() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token = mint_hmac_token_with_channels(b"secret", "t-1", "*", Some(&["*"]), 60);

    let pub_resp = reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-1/publish", server.base))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"ok": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(pub_resp.status(), 204);

    let sub_resp = reqwest::Client::new()
        .get(format!("{}/api/tenants/t-1/channels/c-1/subscribe", server.base))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(sub_resp.status(), 200);
}

#[tokio::test]
async fn channels_list_excludes_other_channels() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token = mint_hmac_token_with_channels(b"secret", "t-1", "*", Some(&["c-1"]), 60);

    let resp_allowed = reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-1/publish", server.base))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp_allowed.status(), 204);

    let resp_denied = reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-other/publish", server.base))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp_denied.status(), 403);
    let body: serde_json::Value = resp_denied.json().await.unwrap();
    assert_eq!(body["code"], "channel_not_in_jwt");
}

#[tokio::test]
async fn missing_channels_claim_returns_403() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    // sessions=* but channels claim is None.
    let token = mint_hmac_token_with_channels(b"secret", "t-1", "*", None, 60);

    let resp = reqwest::Client::new()
        .post(format!("{}/api/tenants/t-1/channels/c-1/publish", server.base))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
}
```

- [ ] **Step 3: Run + commit**

```bash
cargo test -p agentdb-api --test broadcast_jwt

git add crates/agentdb-api/tests/broadcast_jwt.rs crates/agentdb-api/tests/common/mod.rs
git commit -m "test(pubsub): channels claim wildcard, list, and missing"
```

---

### Task 9: Lag-policy integration test

**Files:**
- Create: `crates/agentdb-api/tests/broadcast_lag.rs`

- [ ] **Step 1: Test that overflow does NOT close the SSE stream**

```rust
mod common;
use common::TestServer;

#[tokio::test]
async fn slow_subscriber_loses_messages_but_stream_stays_open() {
    let server = TestServer::start().await;
    let mut sse = server.open_sse("/api/tenants/t-1/channels/c-1/subscribe").await;

    // Pump well over capacity (256). Bypass HTTP for speed: use the bus directly.
    let key = agentdb_api::broadcast_bus::ChannelKey {
        tenant: "t-1".into(),
        channel: "c-1".into(),
    };
    for i in 0..2000 {
        server
            .state
            .broadcast_bus
            .publish(
                &key,
                agentdb_api::broadcast_bus::BroadcastEnvelope {
                    tenant: "t-1".into(),
                    channel: "c-1".into(),
                    ts_ms: i,
                    sender: None,
                    payload: serde_json::json!({"i": i}),
                },
            )
            .await;
    }

    // Read until quiescent. Stream must NOT have closed (no error frame).
    let mut received = 0;
    for _ in 0..300 {
        match tokio::time::timeout(std::time::Duration::from_millis(50), sse.try_next_event()).await {
            Ok(Some(_)) => received += 1,
            _ => break,
        }
    }
    assert!(received > 0, "expected some messages delivered");
    assert!(received < 2000, "expected to lose some messages to overflow");

    // The stream is still alive — publish one more and read it.
    server.state.broadcast_bus.publish(
        &key,
        agentdb_api::broadcast_bus::BroadcastEnvelope {
            tenant: "t-1".into(),
            channel: "c-1".into(),
            ts_ms: 9999,
            sender: None,
            payload: serde_json::json!({"final": true}),
        },
    ).await;
    let frame = tokio::time::timeout(std::time::Duration::from_secs(2), sse.next_event())
        .await
        .expect("stream still alive");
    let env: serde_json::Value = serde_json::from_str(&frame.data).unwrap();
    assert_eq!(env["payload"]["final"], true);
}
```

- [ ] **Step 2: Run + commit**

```bash
cargo test -p agentdb-api --test broadcast_lag

git add crates/agentdb-api/tests/broadcast_lag.rs
git commit -m "test(pubsub): broadcast overflow does not close SSE stream"
```

---

### Task 10: Python SDK — `broadcast()` + `subscribe_channel()`

**Files:**
- Create: `clients/python/agentdb/_broadcast.py`
- Modify: `clients/python/agentdb/client.py` (sync)
- Modify: `clients/python/agentdb/_async_client.py` (async)
- Modify: `clients/python/agentdb/__init__.py` (re-exports)
- Create: `clients/python/tests/test_broadcast.py`

- [ ] **Step 1: Read the existing client structure**

```bash
cat clients/python/agentdb/client.py | head -80
cat clients/python/agentdb/_async_client.py | head -80
cat clients/python/agentdb/__init__.py
```

Note the `Client` constructor signature, the auth handling pattern, and how `subscribe()` was added in Phase 1. Mirror that exactly.

- [ ] **Step 2: Create `_broadcast.py`**

```python
"""Phase 2 broadcast message type + parsing helpers."""

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class BroadcastMessage:
    tenant: str
    channel: str
    ts_ms: int
    sender: Optional[str]
    payload: Any

    @classmethod
    def from_envelope(cls, env: dict) -> "BroadcastMessage":
        return cls(
            tenant=env["tenant"],
            channel=env["channel"],
            ts_ms=env["ts_ms"],
            sender=env.get("sender"),
            payload=env["payload"],
        )
```

- [ ] **Step 3: Add `broadcast()` and `subscribe_channel()` to `Client`**

Sync version in `clients/python/agentdb/client.py`:

```python
from ._broadcast import BroadcastMessage

class Client:
    # ... existing methods ...

    def broadcast(self, *, channel: str, payload: Any) -> None:
        """Publish an ephemeral broadcast message to a channel."""
        url = f"{self._base_url}/api/tenants/{self._tenant}/channels/{channel}/publish"
        resp = self._http.post(url, json=payload, headers=self._auth_headers())
        resp.raise_for_status()

    def subscribe_channel(self, *, channel: str) -> Iterator[BroadcastMessage]:
        """Subscribe to a channel. Yields BroadcastMessage objects.

        Ephemeral: no replay, no JWT auto-reconnect on expiry (the broadcast
        stream is best-effort — users who need durability use subscribe()
        on a session instead).
        """
        url = f"{self._base_url}/api/tenants/{self._tenant}/channels/{channel}/subscribe"
        with httpx.stream("GET", url, headers=self._auth_headers(), timeout=None) as resp:
            resp.raise_for_status()
            for frame in parse_lines(resp.iter_lines()):
                if frame.event in (None, "message"):
                    if not frame.data:
                        continue
                    env = json.loads(frame.data)
                    yield BroadcastMessage.from_envelope(env)
```

(Adapt `parse_lines` import + `_auth_headers()` based on what main's SDK actually exposes — Phase 1 added these.)

Async version in `clients/python/agentdb/_async_client.py`:

```python
class AsyncClient:
    async def broadcast(self, *, channel: str, payload: Any) -> None:
        url = f"{self._base_url}/api/tenants/{self._tenant}/channels/{channel}/publish"
        resp = await self._http.post(url, json=payload, headers=await self._auth_headers())
        resp.raise_for_status()

    async def subscribe_channel(self, *, channel: str) -> AsyncIterator[BroadcastMessage]:
        url = f"{self._base_url}/api/tenants/{self._tenant}/channels/{channel}/subscribe"
        async with self._http.stream("GET", url, headers=await self._auth_headers(), timeout=None) as resp:
            resp.raise_for_status()
            async for frame in parse_lines_async(resp.aiter_lines()):
                if frame.event in (None, "message"):
                    if not frame.data:
                        continue
                    env = json.loads(frame.data)
                    yield BroadcastMessage.from_envelope(env)
```

- [ ] **Step 4: Re-export in `__init__.py`**

```python
from ._broadcast import BroadcastMessage

__all__ = [..., "BroadcastMessage"]
```

- [ ] **Step 5: Tests**

Create `clients/python/tests/test_broadcast.py`:

```python
"""Tests for broadcast / subscribe_channel."""

import json
import os
import socket
import subprocess
import threading
import time
from contextlib import contextmanager
from typing import Iterator

import httpx
import pytest

from agentdb import Client, BroadcastMessage
from agentdb._broadcast import BroadcastMessage as BM


def test_broadcast_message_parses_full_envelope():
    msg = BM.from_envelope({
        "tenant": "t-1",
        "channel": "c-1",
        "ts_ms": 100,
        "sender": "u-1",
        "payload": {"k": "v"},
    })
    assert msg.tenant == "t-1"
    assert msg.sender == "u-1"
    assert msg.payload == {"k": "v"}


def test_broadcast_message_handles_missing_sender():
    msg = BM.from_envelope({
        "tenant": "t-1",
        "channel": "c-1",
        "ts_ms": 100,
        "payload": {},
    })
    assert msg.sender is None


@pytest.mark.skipif(
    os.environ.get("AGENTDB_PYTHON_SMOKE") != "1",
    reason="set AGENTDB_PYTHON_SMOKE=1 to run live-server smoke",
)
def test_broadcast_round_trip_against_live_server():
    # Boot agentdb-server --dev, publish, subscribe, assert delivery.
    # Use the same _serve fixture as test_subscribe.py from Phase 1.
    from .test_subscribe import _serve  # reuse Phase 1's helper
    with _serve() as base:
        client = Client(base_url=base, tenant="t-1")
        seen: list = []
        def consume():
            for msg in client.subscribe_channel(channel="c-1"):
                seen.append(msg)
                if len(seen) >= 2:
                    return
        t = threading.Thread(target=consume, daemon=True)
        t.start()
        time.sleep(0.5)
        client.broadcast(channel="c-1", payload={"i": 1})
        client.broadcast(channel="c-1", payload={"i": 2})
        t.join(timeout=10)
        assert len(seen) == 2
        assert {m.payload["i"] for m in seen} == {1, 2}
```

- [ ] **Step 6: Run + commit**

```bash
cd clients/python
pytest tests/test_broadcast.py -k "not skipif"
cd ../..

git add clients/python/
git commit -m "feat(sdk-py): add broadcast() + subscribe_channel() with BroadcastMessage"
```

---

### Task 11: TypeScript SDK — `broadcast()` + `subscribeChannel()`

**Files:**
- Create: `clients/typescript/src/broadcast.ts`
- Modify: `clients/typescript/src/client.ts`
- Modify: `clients/typescript/src/index.ts`
- Create: `clients/typescript/test/broadcast.test.ts`

- [ ] **Step 1: Read existing client**

```bash
cat clients/typescript/src/client.ts | head -100
cat clients/typescript/src/index.ts
```

- [ ] **Step 2: Create `broadcast.ts`**

```ts
export interface BroadcastMessage {
  tenant: string;
  channel: string;
  ts_ms: number;
  sender?: string;
  payload: unknown;
}

export function parseBroadcastEnvelope(json: string): BroadcastMessage {
  const env = JSON.parse(json) as BroadcastMessage;
  return {
    tenant: env.tenant,
    channel: env.channel,
    ts_ms: env.ts_ms,
    sender: env.sender,
    payload: env.payload,
  };
}
```

- [ ] **Step 3: Add methods to `Client` in `client.ts`**

```ts
import { BroadcastMessage, parseBroadcastEnvelope } from "./broadcast";
import { parseSse } from "./sse";  // from Phase 1

export class Client {
  // ... existing fields + methods ...

  async broadcast(opts: { channel: string; payload: unknown }): Promise<void> {
    const url = `${this.baseUrl}/api/tenants/${this.tenant}/channels/${opts.channel}/publish`;
    const headers = await this.authHeaders();
    headers["content-type"] = "application/json";
    const resp = await (this.fetchImpl ?? fetch)(url, {
      method: "POST",
      headers,
      body: JSON.stringify(opts.payload),
    });
    if (!resp.ok) throw new Error(`broadcast failed: ${resp.status}`);
  }

  async *subscribeChannel(opts: { channel: string }): AsyncIterable<BroadcastMessage> {
    const url = `${this.baseUrl}/api/tenants/${this.tenant}/channels/${opts.channel}/subscribe`;
    const headers = await this.authHeaders();
    headers["accept"] = "text/event-stream";
    const resp = await (this.fetchImpl ?? fetch)(url, { headers });
    if (!resp.ok || !resp.body) throw new Error(`subscribe failed: ${resp.status}`);
    const reader = resp.body.getReader();
    for await (const frame of parseSse(reader)) {
      if (frame.event && frame.event !== "message") continue;
      if (!frame.data) continue;
      yield parseBroadcastEnvelope(frame.data);
    }
  }
}
```

- [ ] **Step 4: Re-export from `index.ts`**

```ts
export { BroadcastMessage } from "./broadcast";
```

- [ ] **Step 5: Tests**

Create `clients/typescript/test/broadcast.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import { parseBroadcastEnvelope } from "../src/broadcast";

describe("parseBroadcastEnvelope", () => {
  it("parses full envelope", () => {
    const msg = parseBroadcastEnvelope(JSON.stringify({
      tenant: "t-1", channel: "c-1", ts_ms: 100,
      sender: "u-1", payload: { k: "v" },
    }));
    expect(msg.tenant).toBe("t-1");
    expect(msg.sender).toBe("u-1");
    expect(msg.payload).toEqual({ k: "v" });
  });

  it("handles missing sender", () => {
    const msg = parseBroadcastEnvelope(JSON.stringify({
      tenant: "t-1", channel: "c-1", ts_ms: 100, payload: {},
    }));
    expect(msg.sender).toBeUndefined();
  });
});
```

(Smoke test gated behind `AGENTDB_TS_SMOKE=1` — extend the existing `subscribe.test.ts` pattern.)

- [ ] **Step 6: Build + run + commit**

```bash
cd clients/typescript
npm run build
npm test
cd ../..

git add clients/typescript/
git commit -m "feat(sdk-ts): add broadcast() + subscribeChannel() with BroadcastMessage"
```

---

### Task 12: Go SDK — `Broadcast` + `SubscribeChannel`

**Files:**
- Create: `clients/go/agentdb/broadcast.go`
- Create: `clients/go/agentdb/broadcast_test.go`

- [ ] **Step 1: Read existing client**

```bash
cat clients/go/agentdb/client.go | head -80
cat clients/go/agentdb/subscribe.go | head -50
```

- [ ] **Step 2: Implement `broadcast.go`**

```go
package agentdb

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
)

type BroadcastMessage struct {
    Tenant  string          `json:"tenant"`
    Channel string          `json:"channel"`
    TsMs    int64           `json:"ts_ms"`
    Sender  string          `json:"sender,omitempty"`
    Payload json.RawMessage `json:"payload"`
}

// Broadcast publishes an ephemeral message to a channel.
func (c *Client) Broadcast(ctx context.Context, channel string, payload any) error {
    body, err := json.Marshal(payload)
    if err != nil {
        return fmt.Errorf("marshal payload: %w", err)
    }
    url := fmt.Sprintf("%s/api/tenants/%s/channels/%s/publish",
        c.baseURL, c.defaultTenant, channel)
    req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
    if err != nil {
        return err
    }
    req.Header.Set("Content-Type", "application/json")
    if err := c.applyAuth(req); err != nil {
        return err
    }
    resp, err := c.http.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusNoContent {
        b, _ := io.ReadAll(resp.Body)
        return fmt.Errorf("broadcast: %d: %s", resp.StatusCode, string(b))
    }
    return nil
}

// SubscribeChannel returns channels for messages and errors. The caller
// reads from msgs until it's closed; errs receives at most one terminal error.
func (c *Client) SubscribeChannel(ctx context.Context, channel string) (<-chan BroadcastMessage, <-chan error) {
    msgs := make(chan BroadcastMessage)
    errs := make(chan error, 1)
    go func() {
        defer close(msgs)
        defer close(errs)
        url := fmt.Sprintf("%s/api/tenants/%s/channels/%s/subscribe",
            c.baseURL, c.defaultTenant, channel)
        req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
        if err != nil {
            errs <- err
            return
        }
        req.Header.Set("Accept", "text/event-stream")
        if err := c.applyAuth(req); err != nil {
            errs <- err
            return
        }
        resp, err := c.http.Do(req)
        if err != nil {
            errs <- err
            return
        }
        defer resp.Body.Close()
        if resp.StatusCode != http.StatusOK {
            b, _ := io.ReadAll(resp.Body)
            errs <- fmt.Errorf("subscribe channel: %d: %s", resp.StatusCode, string(b))
            return
        }
        for frame := range scanSseFrames(resp.Body) {
            if frame.Event != "" && frame.Event != "message" {
                continue
            }
            if frame.Data == "" {
                continue
            }
            var msg BroadcastMessage
            if err := json.Unmarshal([]byte(frame.Data), &msg); err != nil {
                continue
            }
            select {
            case msgs <- msg:
            case <-ctx.Done():
                return
            }
        }
    }()
    return msgs, errs
}
```

(`scanSseFrames` and `applyAuth` are reused from Phase 1's `subscribe.go` / `client.go` — verify exact names by reading those files.)

- [ ] **Step 3: Test**

```go
// clients/go/agentdb/broadcast_test.go
package agentdb

import (
    "encoding/json"
    "testing"
)

func TestBroadcastMessageJSONRoundTrip(t *testing.T) {
    in := BroadcastMessage{
        Tenant:  "t-1",
        Channel: "c-1",
        TsMs:    100,
        Sender:  "u-1",
        Payload: json.RawMessage(`{"k":"v"}`),
    }
    b, _ := json.Marshal(in)
    var out BroadcastMessage
    if err := json.Unmarshal(b, &out); err != nil {
        t.Fatal(err)
    }
    if out.Tenant != "t-1" || out.Sender != "u-1" {
        t.Fatalf("round trip failed: %+v", out)
    }
}
```

- [ ] **Step 4: Run + commit**

```bash
cd clients/go
go test ./...
cd ../..

git add clients/go/
git commit -m "feat(sdk-go): add Broadcast + SubscribeChannel channels"
```

---

### Task 13: Browser/multi-agent example

A typing-indicator demo, smaller than Phase 1's. Pick TypeScript since the friend's deployment shape is browser-direct.

**Files:**
- Create: `examples/agents/typescript/broadcast_typing.ts`
- Modify: `examples/agents/typescript/README.md` (append a section)

- [ ] **Step 1: Create the demo**

```ts
// examples/agents/typescript/broadcast_typing.ts
//
// Two-agent typing-indicator demo using Phase 2 broadcast.
//
// Run:
//   cargo run -p agentdb-server -- serve --dev
//   npx ts-node examples/agents/typescript/broadcast_typing.ts

import { Client } from "@agentdb/client";

const BASE = "http://127.0.0.1:7878";
const TENANT = "t-1";
const CHANNEL = "room:42";

async function typingPublisher() {
  const client = new Client({ baseUrl: BASE, tenant: TENANT });
  for (let i = 0; i < 5; i++) {
    await client.broadcast({
      channel: CHANNEL,
      payload: { kind: "typing", agent: "alice", at: Date.now() },
    });
    console.log("publisher: sent typing ping", i);
    await new Promise((r) => setTimeout(r, 500));
  }
  await client.broadcast({
    channel: CHANNEL,
    payload: { kind: "stopped_typing", agent: "alice" },
  });
}

async function typingReactor() {
  const client = new Client({ baseUrl: BASE, tenant: TENANT });
  console.log("reactor: subscribing to", CHANNEL);
  for await (const msg of client.subscribeChannel({ channel: CHANNEL })) {
    const p = msg.payload as { kind: string; agent: string };
    console.log(`reactor: ${p.agent} → ${p.kind}`);
    if (p.kind === "stopped_typing") return;
  }
}

async function main() {
  // Run both in parallel; reactor stops when it sees the final stopped_typing.
  await new Promise((r) => setTimeout(r, 200));
  const consumer = typingReactor();
  await new Promise((r) => setTimeout(r, 200));
  await typingPublisher();
  await consumer;
}

main().catch((e) => { console.error(e); process.exit(1); });
```

- [ ] **Step 2: Update README**

Append to `examples/agents/typescript/README.md`:

```markdown
## broadcast_typing.ts — typing indicator demo (Phase 2)

Demonstrates ephemeral pub/sub: a publisher emits typing pings every 500ms;
a reactor subscribes to the channel and prints them. No durability — if
the reactor blinks, it loses pings, by design.

```bash
cargo run -p agentdb-server -- serve --dev    # in one shell
npx ts-node examples/agents/typescript/broadcast_typing.ts
```
```

- [ ] **Step 3: Sanity check + commit**

```bash
# Type-check the example
cd examples/agents/typescript
# (Likely no separate build setup; if there is, run it.)
cd ../../..

git add examples/agents/typescript/
git commit -m "feat(examples): typescript typing-indicator broadcast demo"
```

---

### Task 14: Docs + runbook

**Files:**
- Modify: `README.md` (append "Ephemeral broadcast" subsection)
- Modify: `docs/agent-db.md`
- Modify: `docs/ARCHITECTURE.md` (append Phase 2 subsection)
- Create: `docs/runbooks/channel_not_in_jwt.md`
- Modify: `docs/runbooks/README.md` (list the new runbook)

- [ ] **Step 1: README "Ephemeral broadcast" subsection**

In `README.md`, find the "Real-time subscriptions (SSE)" section added in Phase 1. Append:

```markdown
#### Ephemeral broadcast (Phase 2)

For transient signals (typing indicators, cursor positions, agent-status
pings) that should NOT be persisted in the session log, use the broadcast
endpoints:

```
POST /api/tenants/{tenant}/channels/{channel}/publish
GET  /api/tenants/{tenant}/channels/{channel}/subscribe
```

Tenant-scoped channels, in-memory fan-out. No durability, no replay — slow
subscribers silently drop messages. Per-channel JWT auth via the `channels`
claim (alongside `sessions`):

```json
{ "tenant": "t-1", "sessions": "*", "channels": ["room:42", "room:43"] }
```

**SDKs:**

```ts
await client.broadcast({ channel: "room:42", payload: { kind: "typing" } });
for await (const msg of client.subscribeChannel({ channel: "room:42" })) { ... }
```

```python
client.broadcast(channel="room:42", payload={"kind": "typing"})
for msg in client.subscribe_channel(channel="room:42"):
    print(msg.payload)
```

```go
client.Broadcast(ctx, "room:42", payload)
msgs, errs := client.SubscribeChannel(ctx, "room:42")
```

If you need durability or replay, use `append_and_fetch` + Phase 1 subscribe
instead — the two paths intentionally don't blur.
```

- [ ] **Step 2: agent-db.md**

Add to the agent-native primitive list:

```markdown
- **Ephemeral broadcast** (`POST .../publish`, `GET .../subscribe`): tenant-
  scoped channels for typing indicators, cursors, and other transient signals.
  In-memory only — no durability, no replay. JWT `channels` claim controls
  per-channel access. See README "Ephemeral broadcast" and spec
  `docs/superpowers/specs/2026-04-26-pubsub-phase2-design.md`.
```

- [ ] **Step 3: ARCHITECTURE.md addendum**

Append to the existing "Pub/Sub (Phase 1)" section:

```markdown
### Phase 2: ephemeral broadcast

`crates/agentdb-api/src/broadcast_bus.rs` defines a process-wide
`BroadcastBus` keyed by `(tenant, channel)`. Per-channel buffer is
256 (small — broadcast is for high-frequency small payloads, not bulk
data). Lazy-allocated on first publish or subscribe; GC'd by a 30s
sweep task that drops entries with zero subscribers and no publish in
60s.

`crates/agentdb-api/src/broadcast.rs` serves `POST /publish` and
`GET /subscribe` on `/api/tenants/{tenant}/channels/{channel}/...`.
Both go through the same JWT extractor as Phase 1's subscribe routes;
authorization checks `claims.channels.allows(channel)` (a new
`channels` claim alongside `sessions`).

Lag policy: when `async_broadcast::RecvError::Overflowed` fires, the
SSE handler silently skips the lost messages and continues — no error
frame, no close. Phase 2 is ephemeral by design; there's no durable log
to replay from.

Multi-node: per-node local. Publishers and subscribers on different
nodes for the same channel won't see each other's messages. Documented
gotcha. Cross-node forwarding is deferred (would need hash-shard +
421 redirect, mirroring Phase 1's session routing).

Body cap: 16 KiB per publish (spec § 5.5). Larger payloads belong in
sessions or object storage, not broadcast.

Channel-name validation: `^[a-zA-Z0-9._:-]{1,256}$`. Slashes excluded
to keep URL parsing unambiguous; colons allowed for namespacing
conventions (`room:42`, `presence:doc-7`).
```

- [ ] **Step 4: Runbook for `channel_not_in_jwt`**

Create `docs/runbooks/channel_not_in_jwt.md`:

```markdown
# `channel_not_in_jwt`

The JWT was valid, but its `channels` claim does not authorize the requested channel.

## Symptom

HTTP 403 with body:

```json
{"code":"channel_not_in_jwt","type":"...","message":"jwt channels claim does not allow channel <c>"}
```

Returned on `POST .../publish` and `GET .../subscribe` for channels.

## Cause

The `channels` claim must be either `"*"` (any channel in the tenant) or a
list `["room:42", "room:43", ...]` containing the channel being accessed.
A token with no `channels` claim at all (the field absent) gets 403 on every
channel — Phase 2 broadcast access is opt-in per token.

## Action

Re-mint the JWT with a `channels` claim that includes the target channel.
Wildcard for trusted server-to-server tokens; explicit list for end-user
browser tokens to limit blast radius.

## Related

- `jwt_invalid` / `jwt_expired` / `session_not_in_jwt` — parallel codes for Phase 1 subscribe.
```

- [ ] **Step 5: Update runbooks index**

Add to `docs/runbooks/README.md` next to the Phase 1 SSE-error runbooks:

```markdown
- [`channel_not_in_jwt`](channel_not_in_jwt.md) — JWT `channels` claim does not include the requested channel (Phase 2 broadcast).
```

- [ ] **Step 6: Commit**

```bash
git add README.md docs/agent-db.md docs/ARCHITECTURE.md docs/runbooks/channel_not_in_jwt.md docs/runbooks/README.md
git commit -m "docs(pubsub): Phase 2 broadcast — README, ARCHITECTURE, runbook"
```

---

### Task 15: End-to-end smoke + final verification

**Files:**
- Create: `scripts/smoke-broadcast.sh`

- [ ] **Step 1: Create the smoke**

```bash
#!/usr/bin/env bash
# End-to-end smoke for Phase 2 ephemeral broadcast.
# Boots agentdb-server in --dev mode, publishes to a channel, subscribes,
# asserts at least one message arrives.

set -euo pipefail

PORT=$((10000 + RANDOM % 50000))
MONGO_PORT=$((PORT + 1))
BASE="http://127.0.0.1:${PORT}"

echo "==> Booting agentdb-server on ${BASE}"
AGENTDB_ADDR="127.0.0.1:${PORT}" \
AGENTDB_MONGO_ADDR="127.0.0.1:${MONGO_PORT}" \
cargo run --quiet -p agentdb-server -- serve --dev &
SERVER_PID=$!
trap "kill $SERVER_PID 2>/dev/null || true" EXIT

for i in $(seq 1 60); do
    if curl -fsS "${BASE}/health/ready" > /dev/null 2>&1; then break; fi
    sleep 0.5
done
curl -fsS "${BASE}/health/ready" > /dev/null

echo "==> Subscribing in background"
TMP=$(mktemp)
curl -sN --max-time 5 "${BASE}/api/tenants/t-1/channels/c-smoke/subscribe" > "$TMP" &
CURL_PID=$!

# Give the subscriber 500ms to attach.
sleep 0.5

echo "==> Publishing 3 messages"
for i in 1 2 3; do
    curl -fsS -X POST "${BASE}/api/tenants/t-1/channels/c-smoke/publish" \
        -H 'content-type: application/json' \
        -d "{\"i\":${i}}" > /dev/null
done

# Wait for the curl subscriber to time out.
wait $CURL_PID || true

LINES=$(grep -c '^data:' "$TMP" || true)
if [ "${LINES}" -lt 3 ]; then
    echo "FAIL: expected >=3 data lines, got ${LINES}"
    cat "$TMP"
    exit 1
fi
rm -f "$TMP"

echo "==> smoke-broadcast OK"
```

- [ ] **Step 2: Make executable + run**

```bash
chmod +x scripts/smoke-broadcast.sh
bash scripts/smoke-broadcast.sh
```

Must print `smoke-broadcast OK`.

- [ ] **Step 3: Full verification**

```bash
cargo build --workspace
cargo test --workspace
cargo clippy -p agentdb-api --lib -- -D warnings
bash scripts/smoke-pubsub.sh                # Phase 1 still works
bash scripts/smoke-broadcast.sh             # Phase 2 works

cd clients/python && AGENTDB_PYTHON_SMOKE=1 pytest -k broadcast && cd ../..
cd clients/typescript && AGENTDB_TS_SMOKE=1 npm test && cd ../..
cd clients/go && go test ./... && cd ../..
```

All must pass.

- [ ] **Step 4: Commit**

```bash
git add scripts/smoke-broadcast.sh
git commit -m "test(pubsub): end-to-end smoke for Phase 2 broadcast"
```

- [ ] **Step 5: Final review**

```bash
git log --oneline origin/main..HEAD
git diff --stat origin/main..HEAD | tail -3
```

Expected: ~13-15 commits, ~1500-2500 LoC across crates + clients + examples + docs.

---

## Self-Review

**Spec coverage check** (against `docs/superpowers/specs/2026-04-26-pubsub-phase2-design.md`):

| Spec section | Covered by task |
|---|---|
| § 5.1 API surface (publish + subscribe endpoints) | Tasks 4, 5, 6 |
| § 5.2 Wire format (BroadcastEnvelope JSON) | Task 1 (struct + round-trip test); Task 4 (publish populates) |
| § 5.3 Auth — `channels` claim | Task 3 (claim definition); Task 4, 5 (handler enforcement); Task 8 (integration test) |
| § 5.4 BroadcastBus | Task 1 |
| § 5.5 Lag policy + 16 KiB body cap | Task 1 (overflow test); Task 4 (413); Task 9 (integration overflow test) |
| § 5.6 Multi-node = per-node local | Implicit (no cross-node code added); documented in Task 14 |
| § 5.7 Channel naming regex | Task 1 (`validate_channel_name` + tests); Tasks 4, 5 (handler enforcement) |
| § 5.8 Self-echo (default ON, `?no_echo` opt-out) | Task 5 (handler implements `no_echo`) |
| § 5.9 SDK shape (Python, TS, Go) | Tasks 10, 11, 12 |
| § 6 Tests (broadcast_publish, broadcast_subscribe, broadcast_lag, broadcast_jwt, broadcast_cors) | Tasks 4, 5, 6, 8, 9 |
| § 7 GC | Task 7 |
| § 8 Phase 3 appendix | Spec only — no code in this plan |

All Phase 2 spec requirements have a task. § 8 (Phase 3) is intentionally not implemented per the user's choice.

**Type consistency check:**
- `BroadcastEnvelope` (Task 1) — referenced in Tasks 4, 5, 9. Consistent.
- `ChannelKey` (Task 1) — referenced in Tasks 4, 5, 9. Consistent.
- `BroadcastBus` (Task 1) — referenced in Tasks 2, 4, 5, 7. Consistent.
- `validate_channel_name` (Task 1) — referenced in Tasks 4, 5. Consistent.
- `MAX_BODY_BYTES` (Task 4) — referenced in Tasks 4, 9. Consistent.
- `SessionsClaim` (already exists from Phase 1) — reused for `channels` field in Task 3.
- `BroadcastMessage` SDK type (Tasks 10, 11, 12) — same field names across all three SDKs.
- `extract_subscribe_jwt` (Phase 1) — reused in Tasks 4, 5.

**Placeholder scan:** No "TBD", "TODO", "implement later" left. Two locations use `ErrorCode::Internal` as a placeholder for codes that may need adding (`PayloadTooLarge`, `MalformedBody`); the surrounding text directs the implementer to add or pick the closest existing variant. This is a pointer to existing code the implementer must read, not a placeholder.

**Scope check:** Single-PR scope. Phase 3 sketched in spec, deferred. No multi-subsystem decomposition needed.
