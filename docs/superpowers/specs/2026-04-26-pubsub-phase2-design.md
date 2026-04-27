# Pub/Sub Phase 2 — Ephemeral Broadcast — Design

**Date:** 2026-04-26
**Status:** Spec, not yet implemented
**Builds on:** Phase 1 — `docs/superpowers/specs/2026-04-25-pubsub-design.md` (durable session-event subscriptions, shipped in PR #92)

---

## 1. Summary

Add ephemeral broadcast channels to AgentDB. New endpoints:

```
POST /api/tenants/{tenant_id}/channels/{channel}/publish
GET  /api/tenants/{tenant_id}/channels/{channel}/subscribe
```

Tenant-scoped, arbitrary string channel names. In-memory `BroadcastBus`,
no durability, no replay. Use cases: typing indicators, cursor
positions, transient agent-status pings — anything that should NOT be
persisted in the session event log.

Reuses Phase 1's plumbing wherever possible: same JWT verifier (with a
new `channels` claim added alongside `sessions`), same per-tenant CORS
middleware, same SSE transport, same SDK extension pattern across
Python / TypeScript / Go.

Phase 3 (presence) is sketched in § 8 as future work, not implemented.

---

## 2. Use cases (and what we're explicitly NOT solving)

### What Phase 2 enables

- **Typing indicators.** Agent A is composing a message; the UI shows
  "agent A is typing…" to all subscribers of the chat channel.
- **Cursor / selection positions.** Multi-cursor collaboration in a
  document or canvas; positions move 30+ times per second and don't
  belong in the durable log.
- **Transient agent-status pings.** "Agent X is reasoning about task
  Y" — useful for live UIs, but losing one if a subscriber blinks is
  fine, the state will be re-emitted shortly.
- **Client-to-client signaling** that doesn't need durability — e.g.,
  WebRTC SDP exchange, ephemeral coordination locks, presence-lite.

### What Phase 2 does NOT solve

- **The friend's stated pain.** The friend's WeChat reply (cited in
  Phase 1's design discussion) is about *durable* SSE subscriptions to
  database changes, not Redis-style ephemeral pub/sub. Phase 1 already
  shipped that. Phase 2 is feature breadth, not depth on the friend's
  actual ask.
- **Durable broadcast.** If a message must survive a subscriber's
  disconnect, callers should publish via `append_and_fetch` to a
  session and consume via Phase 1's `subscribe`. The two paths
  intentionally don't blur.
- **Cross-node broadcast.** Phase 2 is per-node local. Multi-node
  fan-out is deferred (see § 5.6).
- **Replay / catch-up / resume.** No `from_seq`, no error frames on
  lag, no SDK auto-reconnect-with-resume. Slow subscribers silently
  drop messages — by design.

---

## 3. What's already in place from Phase 1

Phase 2 is additive. The following infrastructure is reused unmodified:

| Phase 1 surface | Phase 2 reuse |
|---|---|
| `crates/agentdb-api/src/jwt.rs` (`AgentDbClaims`, `SessionsClaim`, HMAC + JWKS verifier) | Add `channels: Option<SessionsClaim>` to `AgentDbClaims`. Reuse `SessionsClaim::allows()`. |
| `crates/agentdb-api/src/subscribe_auth.rs` (header-or-query JWT extractor) | Reuse for both publish and subscribe endpoints. |
| `crates/agentdb-api/src/cors.rs` (per-tenant origin echo + OPTIONS preflight) | Apply to channel routes via the same `subscribe_routes`-style separate router subtree. |
| `crates/agentdb-api/src/error.rs::ApiError::Coded` + `ErrorCode` (Phase 1 added `JwtInvalid` etc.) | Add `ChannelNotInJwt`, `ChannelNameInvalid`. |
| `bin/agentdb-server/src/serve.rs` env-var loader pattern | No new env vars in Phase 2 (config is per-tenant JWT, already wired). |
| Phase 1 SDK `subscribe()` shape across Python / TS / Go | Mirror with `broadcast()` + `subscribe_channel()` methods. |

---

## 4. Scope & non-goals

### In scope

- `POST .../publish` and `GET .../subscribe` endpoints.
- `BroadcastBus` keyed by `(tenant_id, channel)`, per-channel
  bounded buffer, lazy creation, GC on idle.
- `channels` JWT claim with `Wildcard("*")` and `List([...])` semantics.
- Per-tenant CORS reused on the new routes.
- Multi-node behavior: per-node local; subscribers see only events
  published on the same node (§ 5.6).
- SDK methods in Python, TypeScript, Go.
- One example: a multi-cursor "agent activity" demo in Python or TS
  (TBD which during planning — depends on which is most readable).
- Docs: README section, ARCHITECTURE addendum, runbook for
  `channel_not_in_jwt`.

### Out of scope (deferred)

- **Phase 3 presence.** Sketched in § 8.
- **Cross-node broadcast forwarding.** § 5.6 documents the deferral.
- **Durable broadcast / replay.** Not part of Phase 2's design intent.
- **Per-channel rate limiting / quotas.** If abuse shows up, add later
  via the existing admission control surface.
- **Channel introspection / management endpoints.** No "list channels
  for a tenant" admin API. Channels are ephemeral; the bus is
  authoritative.
- **WebSocket transport.** SSE-only, per the friend's explicit
  preference for SSE in multi-tenant deployments.

---

## 5. Design

### 5.1 API surface

```
POST /api/tenants/{tenant_id}/channels/{channel}/publish
       Headers:
         Authorization: Bearer <jwt>     # optional if tenant has no JWT config
       Body (JSON, content-type: application/json):
         <arbitrary JSON value>
       Responses:
         204 No Content                  # published
         400                             # malformed body, channel name invalid
         401 jwt_invalid|jwt_expired
         403 channel_not_in_jwt
         404 tenant_unknown              # only when JWT config exists for "*" tenant
         413                             # body too large (16 KiB cap, see § 5.5)

GET /api/tenants/{tenant_id}/channels/{channel}/subscribe
       Headers:
         Authorization: Bearer <jwt>     # optional
       Query:
         ?heartbeat_secs=<u64>           # optional; SSE keepalive interval, default 20
         ?access_token=<jwt>             # EventSource-friendly auth
         ?no_echo=true                   # drop messages from same JWT `sub`
       Responses:
         200 OK (text/event-stream)      # streams BroadcastEnvelope JSON
         401 jwt_invalid|jwt_expired
         403 channel_not_in_jwt
```

Both endpoints mounted on a separate router subtree alongside Phase 1's
`subscribe_routes`, sharing the CORS middleware. Both go through the
same `extract_subscribe_jwt` helper from Phase 1 (it already returns
`Ok(None)` when the tenant has no JWT config — Basic auth still works
for backend publishers / subscribers).

### 5.2 Wire format

```json
{
  "tenant": "t-1",
  "channel": "room-42",
  "ts_ms": 1714063200123,
  "sender": "user-7",
  "payload": { "kind": "typing", "user": "alice" }
}
```

- `tenant`, `channel`: from URL path.
- `ts_ms`: server-stamped at publish time.
- `sender`: the JWT `sub` claim if JWT auth was used; absent for Basic
  auth publishers (no per-user identity available).
- `payload`: the JSON body verbatim. No schema, no kind extraction
  (unlike Phase 1 envelopes which extract `kind` from `payload.kind`).

No `seq`, no `hlc_ts`. Broadcast has no order guarantee beyond
"causally after publish-call returns" — different subscribers may
observe slightly different orderings under concurrent load.

### 5.3 Auth — `channels` JWT claim

Extend `crates/agentdb-api/src/jwt.rs::AgentDbClaims`:

```rust
pub struct AgentDbClaims {
    pub iss: String,
    pub aud: Option<String>,
    pub sub: String,
    pub tenant: String,
    pub sessions: SessionsClaim,
    #[serde(default)]
    pub channels: Option<SessionsClaim>,   // NEW
    pub exp: u64,
}
```

`channels` is `Option<SessionsClaim>` so existing tokens minted for
Phase 1 don't break — they implicitly have no broadcast access.

Semantics:

- `None` (claim absent) — token cannot publish or subscribe to any
  channel. Server returns 403 `channel_not_in_jwt`.
- `Some(Wildcard("*"))` — any channel in the tenant.
- `Some(List(["room-42", "room-43"]))` — only the listed channels.

Reuses `SessionsClaim::allows(name: &str) -> bool` — same semantics,
just applied to channel names instead of session IDs.

When the tenant has **no** JWT config, the channel routes fall through
to existing tenant-scoped Basic auth (same as Phase 1 — backend
publishers don't need JWTs). Basic-auth credentials are tenant-wide,
so no per-channel ACL applies in that mode.

### 5.4 `BroadcastBus`

```rust
// crates/agentdb-api/src/broadcast_bus.rs

pub struct BroadcastBus {
    inner: tokio::sync::RwLock<HashMap<ChannelKey, ChannelEntry>>,
    // Could be parking_lot or DashMap; pick whatever the rest of the
    // crate uses for similar maps.
}

#[derive(Hash, Eq, PartialEq, Clone)]
struct ChannelKey {
    tenant: String,
    channel: String,
}

struct ChannelEntry {
    sender: async_broadcast::Sender<BroadcastEnvelope>,
    subscriber_count: AtomicUsize,
    last_publish_ts: AtomicU64,
}

impl BroadcastBus {
    pub fn publish(&self, key: ChannelKey, env: BroadcastEnvelope);
    pub async fn subscribe(&self, key: ChannelKey) -> async_broadcast::Receiver<BroadcastEnvelope>;
}
```

Per-channel capacity: **256**. Smaller than Phase 1's `EventBus`
(16384) because:
- Ephemeral semantics — buffering doesn't help slow subscribers since
  there's no replay.
- Many channels (potentially thousands per tenant); larger buffers
  multiply.
- Typical throughput is "humans typing, cursors moving" — sub-1KB
  payloads at <100/sec per channel — 256 is generous.

Lazy creation: first `publish` or `subscribe` to a `(tenant, channel)`
pair allocates the entry. Subscriber count is tracked so the GC can
reclaim idle entries.

### 5.5 Lag policy

`async_broadcast` channel created with `set_overflow(true)`. When a
subscriber's `recv()` returns `RecvError::Overflowed(_)`, the SSE
handler:

- Increments a counter (for metrics).
- **Continues silently.** No error frame, no close.

This is the explicit Phase 2 trade-off vs Phase 1: broadcast has no
durable backing log, so there's nothing to replay. Subscribers that
fall behind miss messages, period. That's the point of "ephemeral."

If a subscriber wants no-message-loss semantics, they should be using
Phase 1's session subscribe.

**Body size cap:** publish requests cap at 16 KiB. Larger payloads
return 413. Rationale: broadcast is for small high-frequency signals;
multi-megabyte payloads belong in sessions or object storage.

### 5.6 Multi-node behavior

Phase 2 is **per-node local**. Each node has its own `BroadcastBus`.
A publish on node A is fanned out only to subscribers connected to
node A.

This is acceptable for the target use cases because:

- **Co-located subscribers.** Most broadcast use cases pair with a
  Phase 1 session subscription on the same channel name (e.g.,
  `room:42` for both durable messages and typing). Phase 1 already
  routes session subscribers to the session's owning node via 421
  redirect; if the broadcast channel name matches the session name
  convention, broadcast publishers and subscribers naturally end up
  on the same node.
- **No infrastructure cost.** Cross-node broadcast forwarding requires
  a meta-plane channel ownership map, rebalancing, and inter-node
  fan-out — all infra Phase 2 doesn't justify.

**Documented gotcha:** in a multi-node deployment, a publisher and
subscriber on different nodes for the same channel name will not see
each other's messages. The runbook + README will state this clearly.

If a future user reports cross-node broadcast as a real need, the
upgrade path is hash-shard channels to nodes (mirror Phase 1's
session routing) and add 421 redirects on publish/subscribe. Tracked
in § 8.

### 5.7 Channel naming

Validation regex: `^[a-zA-Z0-9._:-]{1,256}$`

- Excludes `/` so URL parsing stays unambiguous.
- Allows `:` for namespacing conventions (`room:42`,
  `presence:doc-7`, `cursor:doc-7`).
- Allows `.` for hierarchical names (`team.alpha.standup`).
- 256 char cap matches typical message-broker channel limits.

Invalid channel names return 400 `channel_name_invalid`.

### 5.8 Self-echo

Default: **the publisher receives their own broadcast** (echoed
back). Simpler default; matches in-process broadcast semantics.

Opt-out: `?no_echo=true` on subscribe. The handler compares the
incoming envelope's `sender` against the subscriber's JWT `sub`; if
they match, the envelope is dropped from that subscriber's stream.
Only effective when both publisher and subscriber use JWT auth (Basic
auth has no `sender`).

If a future user wants the inverse default (no_echo by default,
opt-in echo), it's a one-line flip and a docs change.

### 5.9 SDK shape

#### Python

```python
from agentdb import Client

client = Client(base_url="https://agentdb.example.com", tenant="t-1")

# Publish (sync)
client.broadcast(channel="room-42", payload={"kind": "typing", "user": "alice"})

# Subscribe (sync)
for msg in client.subscribe_channel(channel="room-42"):
    print(msg.payload)
```

Async client gets `async def broadcast(...)` and `async for msg in
client.subscribe_channel(...)` mirrors. Same JWT auth setup as Phase 1.

#### TypeScript

```ts
import { Client } from "@agentdb/client";

const client = new Client({ baseUrl: "...", tenant: "t-1" });

await client.broadcast({ channel: "room-42", payload: { kind: "typing" } });

for await (const msg of client.subscribeChannel({ channel: "room-42" })) {
  ui.update(msg.payload);
}
```

#### Go

```go
client.Broadcast(ctx, "room-42", payload)
msgs, errs := client.SubscribeChannel(ctx, "room-42")
for msg := range msgs { ... }
```

All three SDKs reuse the existing JWT/auth setup, the existing SSE
parser (no protocol differences), and the existing error types
(`SubscriptionClosed` from Phase 1 isn't relevant here — broadcast
doesn't close on lag).

---

## 6. Tests

### Rust integration tests

- `crates/agentdb-api/tests/broadcast_publish.rs`
  - publish + receive via in-process subscribe; verify envelope shape.
  - 413 on body > 16 KiB.
  - 400 on invalid channel name.
  - JWT-claim path: 403 when channel not in `channels` claim.
  - Echo: default echo, `?no_echo=true` drops self-published messages.
- `crates/agentdb-api/tests/broadcast_subscribe.rs`
  - Multiple subscribers on the same channel each receive every publish.
  - Subscribers on different channels are isolated.
  - Subscribers on different tenants are isolated (no cross-tenant leak).
- `crates/agentdb-api/tests/broadcast_lag.rs`
  - Force capacity exhaustion; assert subscriber CONTINUES (no close).
  - Assert metrics counter incremented.
- `crates/agentdb-api/tests/broadcast_jwt.rs`
  - Reuse mint helpers from Phase 1 + a new `mint_hmac_token_with_channels`.
  - Wildcard `channels: "*"` allows any channel.
  - List `channels: ["a", "b"]` allows a/b but rejects c.
  - Missing `channels` claim → 403 on both publish and subscribe.
- `crates/agentdb-api/tests/broadcast_cors.rs`
  - Allowed origin echoed; OPTIONS preflight returns 204.

### Inline unit tests

- `BroadcastBus::publish + subscribe` ordering.
- `BroadcastBus` lazy creation + reference counting + GC on idle.
- `ChannelName` regex validation.
- `BroadcastEnvelope` JSON round-trip.
- `SessionsClaim::allows` already tested in Phase 1; reuse.

### SDK tests

- Python / TS / Go each add 2-3 unit tests for `broadcast` /
  `subscribe_channel` payload handling and 1-2 gated smoke tests
  against a live `cargo run -- serve --dev`.

### Smoke

- Extend `scripts/smoke-pubsub.sh` (or add `scripts/smoke-broadcast.sh`)
  with a publish + subscribe round-trip.

---

## 7. Plan (high-level milestones)

A detailed step-by-step plan lands in `docs/superpowers/plans/` after
this spec is approved. High-level milestones for a single PR:

1. **`BroadcastBus` module + envelope type** + inline unit tests.
2. **Wire bus into `AppState`** (single-line addition; no struct-literal
   sweep needed unless we rebase across major main churn).
3. **`channels` JWT claim** + `SessionsClaim::allows` reuse.
4. **`POST .../publish` handler** + JWT enforcement + body validation.
5. **`GET .../subscribe` handler** + JWT enforcement + `no_echo` filter.
6. **Channel routes layered with CORS** (separate router subtree like
   Phase 1's subscribe routes).
7. **GC task** for idle channel entries.
8. **Python SDK** — `Client.broadcast()` + `Client.subscribe_channel()`.
9. **TypeScript SDK** — same shape.
10. **Go SDK** — same shape.
11. **Example** — typing / cursor demo (one language; pick during plan).
12. **Docs** — README "Ephemeral broadcast" section under "Real-time
    subscriptions", ARCHITECTURE addendum, `channel_not_in_jwt` runbook.
13. **Smoke** — publish + subscribe round-trip in
    `scripts/smoke-broadcast.sh` (or extend `smoke-pubsub.sh`).

Estimated diff: ~1500–2500 LoC across Rust + 3 SDKs + docs + tests.
Comparable to Phase 1 minus the durable-replay and multi-node-redirect
machinery.

---

## 8. Phase 3 (presence) — future-work appendix

Not implemented. Sketched here for continuity.

### Why deferred

- **Friend's actual ask is durable subs (Phase 1), not presence.** The
  WeChat conversation cited in Phase 1's design discussion shows the
  friend's pain is "Redis is troublesome and adds cost" — solved by
  durable SSE subs, not by presence.
- **WebSocket conflicts with the friend's explicit SSE preference.**
  The friend's reply emphasized SSE-multi-tenant cost efficiency.
  Adding WebSocket diverges from that property and adds protocol
  complexity for a use case no current user has named.
- **Presence is well-served by Phase 2 broadcast for many cases.**
  Typing indicators, cursor positions, "agent X is thinking" — these
  fit the broadcast semantics without needing membership tracking.

### When Phase 3 becomes worth doing

- A user explicitly asks for "who's online in this room right now"
  semantics that broadcast can't fake.
- A use case requires server-tracked membership (e.g., capacity
  enforcement, presence-state queryable via REST).

### Sketch (when we pick it up)

**Endpoint:**

```
GET /api/tenants/{tenant_id}/channels/{channel}/presence
       SSE stream of presence events (join, leave, update)
       Query: ?access_token=<jwt>, ?heartbeat_secs=<u64>

POST /api/tenants/{tenant_id}/channels/{channel}/presence/heartbeat
       Body: { state: <opaque JSON> }
       Server records last_heartbeat_ts + state for the JWT sub.
       Idempotent.
```

Heartbeats are POSTs from clients (every 5-10s). The server tracks a
`HashMap<(tenant, channel, sub), { state, last_heartbeat_ts }>`.
Subscribers to `/presence` see:

- `event: join` when a sub heartbeats for the first time on this channel.
- `event: update` on each subsequent heartbeat (with state diff).
- `event: leave` when a sub misses N consecutive heartbeats (default 3).

**Why SSE + POST instead of WebSocket:** preserves AgentDB's
SSE-only multi-tenant cost story. WebSocket would force a bidirectional
upgrade per presence subscriber — diverging from Phase 1+2's
HTTP-only model.

**Auth:** reuse the `channels` JWT claim from Phase 2. No separate
`presence` claim — if a user can broadcast on a channel, they can
participate in its presence.

**Open question for Phase 3:** whether presence shares the same
channel namespace as broadcast (so `room:42` broadcast and
`room:42` presence are the same channel) or uses a separate
namespace (e.g., `presence:room:42`). Supabase combines them; we
likely should too for simplicity.

**State conflict resolution:** last-write-wins per `sub`. CRDT-style
merging is overkill for presence-as-typically-used (one user, one
state at a time).

**Multi-node:** same per-node-local model as Phase 2 unless we hash-
shard channels by then.

---

## 9. Open questions

1. **Channel name `:` vs `.` namespacing convention.** Both allowed by
   the regex. README should pick one as the recommended style; any
   preference?
2. **Per-channel buffer capacity (256).** Easy to tune later; revisit
   if a real workload needs more. Not a v1 blocker.
3. **GC interval.** Current proposal: drop entry when subscriber count
   = 0 AND last publish > 60s ago. Sweep interval: every 30s. Numbers
   are reasonable defaults; revisit under load.
4. **Self-echo default = ON vs OFF.** Picked ON for simplicity. Easy
   to flip if real users prefer Supabase's OFF default.
5. **Body size cap (16 KiB).** Generous for typing / cursor / small
   payloads. Larger payloads belong in sessions; revisit if
   compelling use case appears.
6. **Whether to add a global `BroadcastBus` capacity ceiling**
   (e.g., max 10K channels per tenant). Not in v1; add later if
   abuse shows up.

---

## 10. Non-goals (re-emphasized)

- **No durable broadcast.** That's `append_and_fetch` + Phase 1 subscribe.
- **No cross-node fan-out.** Per-node local; deferred.
- **No WebSocket transport.** SSE only.
- **No Phase 3 presence in this PR.** Sketched only.
- **No replay / `from_seq`.** Ephemeral by design.
- **No `lag_too_far` close.** Slow subscribers silently lose messages.
