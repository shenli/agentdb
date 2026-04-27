# Pub/Sub on AgentDB — Design

**Date:** 2026-04-25
**Status:** Spec, not yet implemented
**Origin:** External feedback — a friend running on Supabase Realtime in
production says it works but is too expensive (in-memory fanout cluster
billed per connection × messages); they currently use Redis pub/sub and
asked for a native pub/sub primitive in AgentDB.

---

## 1. Summary

Add a native pub/sub interface to AgentDB. Phase 1 ships **durable
session-event subscriptions over SSE** — the equivalent of Supabase
Realtime's "Postgres Changes" pillar — by reusing AgentDB's existing
event log as the change feed and the existing broadcast bus as the
fanout mechanism. No separate cluster, no replication slot, no extra
in-memory store of "what's in the table". Subscribers get
durable-by-default, replay-from-seq-by-default streams of events
appended to a session (or to a whole tenant).

Phase 2 (ephemeral broadcast) and phase 3 (presence) are sketched at
the end and not committed to.

---

## 2. Use cases

The two concrete use cases the friend named, both of which Phase 1
solves directly:

- **A. Real-time UI updates.** An agent appends a message to a session;
  the user-facing UI must show "1 unread" without polling. The UI may
  be temporarily disconnected (tab backgrounded, network blip) and
  must catch up on missed messages — a "1 unread" that should have
  been "5 unread" is a bug.
- **B. Multi-agent coordination.** Agent A finishes a sub-task and
  signals "done"; agent B is waiting for that signal. Agent B may
  briefly restart and must not silently miss the signal.

Both are durable use cases — losing a message because the subscriber
blinked is wrong in both. Both reduce to the same primitive: **"tail
the events appended to session S, optionally starting at seq N to
catch up after a disconnect."**

Two patterns AgentDB users can compose to do multi-agent collaboration
on top of this primitive:

- **Pattern 1 — shared session.** Agent A and agent B both append to
  *and* subscribe to a shared "coordination" session. Like a chat
  room. Order is total per session.
- **Pattern 2 — subscribe-to-peer's-session.** Agent A appends only
  to its own session. Agent B subscribes to A's session and reacts to
  specific events (e.g. `kind = "task_done"`). No shared write surface.

Both work with the same Phase 1 API; the doc just calls out the
patterns so users don't have to figure them out from primitives.

**Deployment shape — frontend-direct.** The friend's confirmed usage
is **browser → AgentDB directly**, with no application-server backend
in the middle proxying events. That is the convenience advantage
Supabase Realtime sells, and it is what makes "rolling your own Redis"
painful — Redis is not browser-friendly without a backend WebSocket
service in between. Phase 1 must support this shape end-to-end (auth,
CORS, JS SDK) — see § 6.13.

---

## 3. What Supabase Realtime gives, and what's expensive

Supabase Realtime has three pillars:

| Pillar | What it is | What's expensive |
|---|---|---|
| **Postgres Changes** | Subscribe to INSERT/UPDATE/DELETE on a table with row-level filters. Implemented via Postgres logical replication slot → Realtime cluster → WebSocket fanout to clients. | Two systems to run: Postgres + the Realtime cluster (Erlang/Elixir). The Realtime cluster holds replication state and per-connection state; billing is per concurrent connection × message volume × payload size. |
| **Broadcast** | Ephemeral message to all subscribers of a channel name; not persisted. | Pure in-memory, but still served by the same separately-billed cluster. |
| **Presence** | Per-channel "who's here" state; CRDT-ish join/leave/state sync. | Same cluster, same billing. |

The friend's complaint targets the first pillar (Postgres Changes) —
which is also the most valuable and the most expensive. The structural
reason: Realtime is a *bolt-on* on top of Postgres, with its own
connection state, its own replication consumer, its own fanout
process. Cost is intrinsic to that architecture, not a Supabase pricing
choice.

---

## 4. Why AgentDB has a structural advantage

In AgentDB, **the change feed is the database's primary write path**.

- Every `append_and_fetch` already writes a durable event with a
  monotonic per-session `seq` ([`primitives.rs:27-72`](../../crates/agentdb-core/src/primitives.rs)).
- The runtime already has a process-wide broadcast bus
  ([`StreamBus` at `stream.rs:73`](../../crates/agentdb-api/src/stream.rs))
  used today for system events (tier changes, forks, fault injections,
  metrics ticks), already exposed over SSE at `GET /api/stream`.
- Sessions already track `next_seq`, so "give me events for session S
  since seq N" is a bounded prefix scan.

A subscriber is just an SSE connection tailing the same log the writer
just appended to. There is no second system, no replication slot, no
separate fanout cluster. The marginal cost of a subscriber is one
in-memory broadcast receiver plus one open HTTP connection.

**Pitch one-liner:** *Real-time isn't a bolt-on at 5× the cost — it's
the same primitive.*

### 4.1 The replay differentiator

Per Supabase's own docs
([guides/realtime/subscribing-to-database-changes](https://supabase.com/docs/guides/realtime/subscribing-to-database-changes)),
the Postgres-Changes API delivers events via a callback registered on a
channel — and **does not expose a replay or from-sequence API**.
Practically: if a subscriber disconnects (tab backgrounded, mobile
network blip, server-side WebSocket reset, deploy), changes published
during the gap are lost from the subscriber's perspective. Recovering
them requires a separate query against the underlying table — which
the application has to write itself, including some way to remember
"what's the latest row I'd already processed."

For the friend's stated use cases this is the actual pain:

- **UI unread badge.** A 2-second disconnect during a burst of
  messages means the badge silently undercounts. The UI then has to
  also fall back to polling `/messages?since=<last_id>` to be correct
  — which defeats the point of using realtime.
- **Multi-agent done-signals.** A briefly-restarted agent B will miss
  whatever agent A published during the restart window. Recovery
  requires application-level bookkeeping.

AgentDB's Phase 1 sub API ships `from_seq=N` as a first-class
parameter, served from the durable log. A subscriber that reconnects
with `from_seq = last_seen_seq + 1` is guaranteed to see every event
in seq order, with no gaps and no application-side bookkeeping beyond
remembering one integer. **This is the headline.**

### 4.2 Supabase's two-pipeline architecture, vs AgentDB's one hop

A closer look at Supabase Realtime
([guides/realtime/concepts#schema-and-tables](https://supabase.com/docs/guides/realtime/concepts#schema-and-tables))
reveals **two table-backed pipelines**, both of which converge on the
same separately-billed Realtime cluster:

| Pipeline | Trigger | Path |
|---|---|---|
| **Postgres Changes** | INSERT/UPDATE/DELETE on a user table | user table → WAL → logical replication slot → Realtime cluster (Erlang) → WebSocket fanout |
| **Broadcast from Database** | Server-side `realtime.send()` SQL function call | `realtime.messages` table → WAL → logical replication slot → Realtime cluster → WebSocket fanout |

The existence of the second pipeline is itself the tell: **Supabase
users wanted durable broadcast badly enough that Supabase added a SQL
workaround for it.** `realtime.send()` → `realtime.messages` →
replication slot → Realtime → client is a 4-hop path whose only purpose
is to do, with a workaround, what AgentDB does natively in **one
hop**:

```
append_and_fetch  →  EventBus (in-process)  →  SSE  →  client
```

The `realtime.messages` table is partitioned by day and cleaned up
after **3 days** — it's a delivery buffer, not a replayable log. Even
the "durable" path is not designed for replay. AgentDB's event log,
in contrast, **is** the replay surface: subscribers reconnect with
`from_seq` and read the same data the application reads.

**Cost driver in plain terms:** the Realtime cluster is the line item.
Both Supabase pipelines pay for WAL flush + logical decoding +
Erlang-cluster fanout + per-connection state. AgentDB's single in-
process hop doesn't have those line items to bill, because there is
no second system to run.

---

## 5. Scope & phasing

| Phase | What | Status in this doc |
|---|---|---|
| **1. Durable session-event subscriptions, browser-direct** | SSE stream of events appended to a tenant + session, with kind filter and `from_seq` catch-up. Tenant-wide variant. Multi-node redirect. **JWT auth (HMAC or JWKS) for browser subscribers**; existing tenant Basic auth still valid for backend subscribers. CORS. JS/TS + Python SDKs with auto-reconnect-on-expiry. | **Designed below. To be implemented as one PR.** |
| **2. Ephemeral broadcast** | New `PUBLISH(channel, payload)` / `SUBSCRIBE(channel)` API for non-durable signals (typing, cursor position). In-memory only. | Sketched; deferred. |
| **3. Presence** | "Who's in this session/channel" — heartbeat-driven membership with state sync. | Sketched; deferred. |

Phase 1 is the headline feature and the structural differentiator. It
fully solves the two use cases above. Phases 2 and 3 are additive and
should ship only if a real user asks.

---

## 6. Phase 1 design

### 6.1 API surface

Two new endpoints, both SSE:

```
GET /api/tenants/{tenant}/sessions/{session}/subscribe
       ?from_seq=<u64>            # optional; default = "live tail only"
       &kind=<string>             # optional; repeatable; payload-kind filter
       &heartbeat_secs=<u64>      # optional; SSE keepalive interval

GET /api/tenants/{tenant}/subscribe
       ?session=<string>          # optional; repeatable; subset of tenant's sessions
       &from_seq_per_session=...  # see § 6.5; tenant-wide replay is more nuanced
       &kind=<string>             # optional; repeatable
       &heartbeat_secs=<u64>      # optional
```

Authentication: dual model.
- **Backend subscribers** (Python SDK, server-to-server): existing
  tenant-scoped Basic auth on `/api/tenants/*` — unchanged.
- **Browser subscribers** (JS/TS SDK, end-user devices): per-tenant
  JWT verified via HMAC secret or JWKS URL. Full design in § 6.13.

(Mongo wire SCRAM is out of scope for SSE in Phase 1.)

No new POST. **Publishing is `append_and_fetch`.** That is the central
design choice — there is no second write path. Anything that's
appended is automatically published; a subscriber sees exactly what
the durable log holds, in seq order.

### 6.2 Wire format

SSE event per appended event. JSON body:

```json
{
  "tenant": "t-1",
  "session": "s-1",
  "seq": 42,
  "ts_ms": 1714063200123,
  "hlc_ts": "...",
  "actor_id": "agent-a",
  "kind": "message",
  "payload": { ... }
}
```

`kind` is extracted from the appended `Document` payload's top-level
`kind` field if present (matching the existing convention for client
filters). If absent, kind is `null` and the event is delivered to
subscribers without a `kind` filter.

A control event is sent on connect:

```
event: subscribed
data: { "tenant": "t-1", "session": "s-1", "live_seq": 42 }
```

`live_seq` is the session's `next_seq` at subscription time — the
first seq that will arrive via live fanout (events with `seq <
live_seq` were either replayed from `from_seq` or are not delivered).

### 6.3 Replay semantics

When `from_seq=N` is provided, the server delivers events in this
order:

1. **Catch-up phase.** Open a read context. Scan events with
   `seq ∈ [N, live_seq)` via the existing key-prefix scan
   ([`primitives.rs::fetch_last_n`](../../crates/agentdb-core/src/primitives.rs)
   — generalize the helper or add a sibling that takes a
   `from_seq` lower bound).
2. **Live phase.** Stream events as they're appended, where
   `seq >= live_seq`.

To prevent the classic catch-up/tail race (event appended between
catch-up scan and bus attachment), the implementation **subscribes to
the bus first**, then snapshots `live_seq`, then runs the catch-up
scan up to `live_seq`. Live events arriving during catch-up are
buffered in the bus receiver. After catch-up, the live stream skips
any seq already delivered (defense in depth — under correct
implementation no skipping should be needed).

Without `from_seq`, the server skips catch-up entirely and starts at
the current `live_seq` — the "tail only" mode.

### 6.4 Filtering

Phase 1 supports server-side filtering on `kind` only. Multiple
`kind=foo&kind=bar` parameters are OR'd. No payload-field filtering
yet — that would require a server-side query language; defer until a
user asks. Clients that need richer filters can do client-side
filtering on the JSON.

### 6.5 Tenant-wide subscriptions

`GET /api/tenants/{tenant}/subscribe` delivers events from any session
in the tenant. This is what the UI-unread-badge use case wants when
a user has many sessions visible in their UI.

`?session=s-1&session=s-2&...` narrows to a subset.

`from_seq` is per-session, so a tenant-wide replay needs a per-session
map of starting sequences. The simplest API:

```
?from_seq_per_session=s-1:42,s-2:7
```

Sessions not in the map start at "tail only". This is a string-encoded
map for a GET request; if it gets unwieldy, switch to a JSON body via
POST + SSE response (`POST /api/tenants/{tenant}/subscribe`) — note as
an open question (§ 8).

### 6.6 Backpressure and lag policy

The bus channel has bounded capacity. If a subscriber falls too far
behind:

- **Phase 1 policy: server closes the connection** with an SSE error
  event indicating `lag_too_far` and the last successfully delivered
  seq. The client reconnects with `from_seq = last_delivered_seq + 1`
  and catches up from the durable log.

This works because the durable log *is* the source of truth — the
broadcast bus is a fast path, not a buffer of record. Slow consumers
fall back to durable read; fast consumers stay on the bus.

Concretely: use a per-subscriber bounded `async_broadcast::Receiver`
(the existing pattern). On `RecvError::Lagged(_)`, send the SSE error
event and close.

### 6.7 The bus extension

The current `StreamBus` carries only system events. We extend it (or
add a parallel `EventBus`) to carry user-event appends.

Two design options:

- **Option X — single bus, new variant.** Add `StreamEvent::EventAppended {
  tenant, session, event }`. Pro: minimal code; existing `/api/stream`
  could even gain user events. Con: system-event subscribers (the
  dashboard) compete for buffer with high-volume user events; the 1024
  capacity is wrong for both.
- **Option Y — separate `EventBus`** dedicated to user events, larger
  capacity (e.g. 16384), independent overflow policy. Pro: isolates
  the volume profiles. Con: two buses.

**Decision: Option Y.** Phase 1 ships a dedicated `EventBus`. The
emission point is inside `append_event` (or its sole caller in
`fork.rs` / `handlers.rs`), immediately after `kv.write_batch` returns
success. Emission MUST happen after the durable write succeeds —
otherwise subscribers can see events that were never persisted.

For per-session fanout efficiency at scale, a keyed map `HashMap<(tenant,
session), Sender>` is the obvious upgrade. Phase 1 keeps a single
`EventBus` and lets subscribers filter; document the upgrade path but
don't ship it now.

### 6.8 Multi-node behavior

AgentDB is now multi-node (M20+). Sessions are owned by a specific
node's tablet. The subscribe endpoint follows the existing pattern:

- If the session is owned by **this** node → serve the SSE stream
  directly.
- Otherwise → return `421 NotWritablePrimary`-equivalent over REST,
  with the owning node's address in the body and a `Location` header.
  The client reconnects to the owner.

This matches the existing Mongo-wire behavior for session-scoped reads
and the REST cross-node forwarding pattern.

**Cross-node SSE forwarding** (server-side proxy of the SSE stream,
transparent to the client) is **out of scope for Phase 1**, in line
with cross-node Mongo wire forwarding being out of scope. Add later
if needed.

The tenant-wide subscription endpoint is more nuanced: a tenant's
sessions can live on multiple nodes. Phase 1 returns 421 with the list
of owning nodes, and the client opens one SSE connection per owner.
A future "fan-in proxy" can multiplex.

### 6.9 Forks

A forked child session has its own seq space starting at 1; the
parent's events live in the parent session and are visible to the
child via fork-aware reads ([`primitives.rs:265`](../../crates/agentdb-core/src/primitives.rs)).

Phase 1: **subscribers see only the session they subscribed to.** A
subscriber on the child session sees only events appended after the
fork; they do not see parent events, even though those events are
logically visible via fork-aware reads.

A "fork-aware subscription" mode (parent events up to fork point,
then child events) is potentially useful for branching agent-flow
visualizations — defer to a follow-up. Note in § 8.

### 6.10 Mongo wire integration

Out of scope for Phase 1. Mongo's `tailable cursor` on a capped
collection is the closest equivalent and could be wired up to the
same `EventBus` later. REST/SSE first.

### 6.11 Mapping to Supabase Realtime concepts

Reader's onramp for anyone coming from Supabase. This is also source
material for the README / SDK docs once Phase 1 lands.

| Supabase Realtime | AgentDB Phase 1 | Notes |
|---|---|---|
| `supabase.channel('name')` | *no equivalent* | Supabase channels are a WebSocket multiplexing convention; AgentDB SSE uses one connection per subscription. Will revisit when WebSocket lands in Phase 2/3. |
| `.on('postgres_changes', {...}, cb)` | `GET /api/tenants/{tenant}/sessions/{session}/subscribe` | One AgentDB subscription = one SSE endpoint. |
| `event: 'INSERT' \| 'UPDATE' \| 'DELETE' \| '*'` | *implicit — only "appended" exists* | AgentDB's event log is append-only; there is no UPDATE/DELETE concept. |
| `schema: 'public'` | *implicit — no schema concept* | |
| `table: 'messages'` | *no direct equivalent — a session is the unit* | A session is a stream of typed events; closer to a Kafka topic than a SQL table. |
| `filter: 'user_id=eq.123'` (eq, neq, gt, gte, lt, lte, in) | `?kind=foo` (single dimension, OR over repeats) | Phase 1 deliberately keeps filter expressiveness minimal. Richer payload filtering deferred — see § 8. |
| `.subscribe()` | *implicit — open the SSE connection* | |
| Anon key in WebSocket URL (`?apikey=...`) | Tenant ID in path (`/api/tenants/{tenant}/...`) | No separate "anon key"; tenant identifies the project. |
| JWT (`access_token` in `phx_join` payload) | JWT in `Authorization: Bearer ...` header **or** `?access_token=<jwt>` query param (EventSource fallback) | See § 6.13. |
| Supabase Auth mints JWTs | Customer's auth provider mints JWTs (Supabase Auth, Auth0, Clerk, Cognito, custom); AgentDB only **verifies** them | AgentDB does not ship a user-auth service. |
| Postgres RLS policies on tables | JWT claims declare `tenant` + `sessions: [...]` or `"*"`; verified per-tenant via HMAC or JWKS | Backend subscribers can still use Basic auth (existing). |
| `realtime.setAuth(newJwt)` over WebSocket — in-place refresh | SSE close on JWT expiry → SDK refreshes → reconnect with `from_seq=last+1` | No missed events because `from_seq` is gap-free (§ 4.1). |
| **(no replay)** — missed changes are lost | **`?from_seq=N`** — replay from any seq | The headline differentiator (§ 4.1). |
| `realtime.send()` SQL → `realtime.messages` table | `append_and_fetch` | Supabase needs an auxiliary table + replication slot to do durable broadcast; AgentDB does it in one operation (§ 4.2). |
| 3-day retention on `realtime.messages` | Full event-log retention via warm → cold → archive lifecycle | AgentDB's events are first-class data, not a delivery buffer. |
| Payload: `{schema, table, commit_timestamp, eventType, new, old, errors}` | Payload: `{tenant, session, seq, ts_ms, hlc_ts, actor_id, kind, payload}` (§ 6.2) | No `old` because append-only; `seq` instead of `commit_timestamp` for the replay key. |

**SDK shape sketches** — what the `clients/typescript/` and
`clients/python/` SDKs should expose so the migration story is "this
is the same thing, plus replay, with no in-between backend":

```js
// Supabase (JS) — for reference
supabase.channel('chat')
  .on('postgres_changes',
      { event: 'INSERT', schema: 'public', table: 'messages',
        filter: 'session_id=eq.s-1' },
      callback)
  .subscribe()

// AgentDB (TypeScript — Phase 1 SDK target, browser-direct)
const client = new AgentDB({
  baseUrl: 'https://agentdb.example.com',
  tenant: 't-1',
  getToken: async () => await myAuth.getJwt(),  // refresh callback
});

for await (const event of client.subscribe({
  session: 's-1',
  kind: ['message'],   // optional
  fromSeq: 42,         // optional — the differentiator
})) {
  handle(event);  // server JWT-expires? SDK transparently reconnects.
}
```

```python
# AgentDB (Python — backend subscribers)
for event in client.subscribe(
    tenant="t-1",
    session="s-1",
    kind=["message"],   # optional
    from_seq=42,        # optional — the differentiator
):
    handle(event)
```

Iterator shape (not callback) is idiomatic in both languages and
composes naturally with `async for`. The Supabase-style callback
shape can be a thin wrapper if a user wants it.

### 6.12 Latency profile (is it real-time?)

Yes — the live-tail path is pure push, no polling, no tick interval.
The subscriber task is parked in `async_broadcast::Receiver::recv().await`;
the tokio runtime wakes it the moment an event lands in the receiver's
buffer. SSE keepalives (~15-30s) only exist to keep TCP connections
alive through middleboxes; they are not a delivery mechanism.

**End-to-end latency breakdown:**

| Stage | Cost |
|---|---|
| `kv.write_batch` (RocksDB local) | ~1ms (memtable + WAL buffer; no per-write fsync) |
| `kv.write_batch` (SlateDB / S3) | tens to hundreds of ms (S3-dominated) |
| `bus.try_broadcast` | sub-microsecond (in-process) |
| Tokio task wake → JSON serialize → socket write | <1ms |
| Network (localhost / LAN / WAN) | <1 / 1-5 / 10-100+ ms |

Realistic totals:

- **Localhost + RocksDB:** ~1-5ms end-to-end. Indistinguishable from
  Redis pub/sub for a UI use case; faster than Supabase Realtime
  (which adds WAL flush + logical decoding + Erlang-cluster fanout).
- **LAN + RocksDB:** ~5-10ms.
- **WAN + RocksDB:** 10-100+ms, network-dominated. Same as any
  network-bound pub/sub.
- **Anywhere + SlateDB/S3:** S3-write-dominated. This is the cost of
  using object storage as primary; not specific to pub/sub.

**Honest caveats:**

1. **Catch-up phase is not real-time.** Reconnect with `from_seq=N`
   triggers a durable scan running at storage-iteration speed
   (thousands of events/sec from RocksDB; slower from SlateDB). Large
   backlogs take measurable time. Once caught up, live tail is back
   to the push profile above. Same trade-off Kafka makes: durability
   for replay isn't free.
2. **Per-tenant write lock serializes publishes within a tenant** (the
   existing admission control). A subscriber on a different tenant
   is unaffected; same-tenant writes queue. This caps per-tenant
   publish throughput, not latency in the no-contention case.
3. **Multi-node Phase 1 has no cross-node SSE forwarding.** Subscribers
   must connect to the owning node; we redirect with 421. A future
   fan-in proxy adds one network hop.

### 6.13 Browser-direct subscription

The deployment shape the friend confirmed (§ 2) is **the browser
subscribes directly to AgentDB** — no application backend in the
middle. Phase 1 ships the equivalent of what
`supabase.channel().on().subscribe()` gives JS developers: open the
SDK, get an auth-aware live event stream.

#### Auth model

AgentDB does **not** ship its own user-auth service (signup, password
reset, OAuth flows) — that is a separate product surface and one
people already own (Supabase Auth, Auth0, Clerk, Cognito, custom IdP).
Phase 1 instead **accepts JWTs minted by the customer's auth
provider** and verifies them per-tenant via either:

- **HMAC secret** — simplest. The customer signs JWTs themselves
  using a shared secret (e.g. `HS256`).
- **JWKS URL** — for enterprise IdPs and Supabase-Auth-style
  providers. AgentDB fetches the JWKS, caches it with TTL, and
  verifies signatures (`RS256`, `ES256`).

Per-tenant config (env-var-driven for Phase 1; admin endpoint can
come later):

```
AGENTDB_TENANT_<id>_JWT_HMAC_SECRET="..."         # HMAC mode
AGENTDB_TENANT_<id>_JWT_JWKS_URL="https://..."    # JWKS mode
AGENTDB_TENANT_<id>_JWT_ISSUER="https://..."      # required iss claim, both modes
AGENTDB_TENANT_<id>_JWT_AUDIENCE="agentdb"        # optional aud claim, both modes
```

A tenant configures one signing model, not both.

#### JWT claim schema

| Claim | Required | Meaning |
|---|---|---|
| `iss` | yes | Issuer; must match tenant's `JWT_ISSUER` config |
| `aud` | optional | Audience; if tenant has `JWT_AUDIENCE` configured, must match |
| `sub` | yes | End-user identifier; logged + carried in subscriber audit |
| `exp` | yes | Expiry; standard JWT claim |
| `tenant` | yes | Must match the tenant in the URL path |
| `sessions` | yes | Either `"*"` (any session in the tenant) or `["s-1", "s-2"]` (whitelist of session IDs) |

The server rejects subscribe with `403` when:
- `tenant` claim ≠ URL tenant
- requested session not in `sessions` (and `sessions != "*"`)

Other claims (custom roles, scopes) are accepted but not enforced
by AgentDB; they are propagated to subscriber audit logs.

#### Transport: header AND query parameter

Browser `EventSource` does **not** support custom request headers,
so `Authorization: Bearer ...` works only with `fetch`-based SSE
consumers. To handle both:

- **`Authorization: Bearer <jwt>` header** — preferred. Used by the
  shipped JS SDK (which uses `fetch` streaming, not native
  `EventSource`), the Python SDK, Go, etc.
- **`?access_token=<jwt>` query parameter** — fallback for vanilla
  `EventSource`. Token lands in URL — discouraged for production
  (URL logs, browser history, referrer leaks) but supported for
  compatibility. JS SDK uses query-param mode only when explicitly
  opted into.

Both paths validate identically. Sending both = `400`.

#### JWT expiry on a live connection

When a JWT expires mid-stream (server checks on each delivery, or on
a periodic timer), the server sends an SSE error event and closes:

```
event: error
data: { "error": "jwt_expired", "last_seq": 1234 }
```

The client SDK refreshes the JWT (via an app-provided callback — the
SDK does not know how the customer's auth provider mints new
tokens) and reconnects with `from_seq = last_seq + 1`. Because
`from_seq` is gap-free (§ 4.1), the user-visible UX is "uninterrupted
stream" even though the wire connection blipped.

This is the AgentDB answer to the requirement that drove Supabase to
WebSocket. Supabase needs in-place token refresh because their stream
is *not* gap-free on reconnect — losing the connection means losing
events. AgentDB has gap-free reconnect, so dropping and reopening on
expiry is fine. WebSocket support comes in Phase 2 anyway (Broadcast
/ Presence need bidirectional); Phase 1 stays SSE.

#### CORS

Per-tenant allowed-origins config:

```
AGENTDB_TENANT_<id>_CORS_ORIGINS="https://app.example.com,https://staging.example.com"
```

The subscribe endpoints set `Access-Control-Allow-Origin` from this
list per-request (echo the request's `Origin` if it matches; omit
otherwise). `*` is rejected when JWT auth is in play — the browser's
own CORS rules forbid wildcard origin with credentials, so accepting
it would be a footgun.

`OPTIONS` preflight is supported with appropriate
`Access-Control-Allow-Headers` (`Authorization`, `Content-Type`) and
`Access-Control-Allow-Methods` (`GET`).

#### Connection lifecycle (browser → AgentDB, end to end)

1. App authenticates the end-user via its auth provider → receives JWT.
2. JS SDK opens SSE with `fetch`:
   `fetch('/api/tenants/t-1/sessions/s-1/subscribe?from_seq=42', { headers: { Authorization: 'Bearer ...' } })`.
3. Server validates JWT (signature, `exp`, `iss`, `tenant`, `sessions`).
4. Server sends `event: subscribed` with `live_seq`, then catch-up
   events (seq ∈ [42, live_seq)), then live tail.
5. On JWT expiry (server-detected): `event: error` with
   `{ jwt_expired, last_seq }`, close.
6. SDK calls the app's refresh callback, gets a new JWT, reconnects
   with `from_seq = last_seq + 1`.
7. Stream resumes; loop.

Step 6 is where the friend's "frontend-direct" promise is kept: the
end-user sees an unbroken UI, even across token refreshes, with no
backend service in the loop.

---

## 7. Plan

One PR for Phase 1. Milestones inside the PR (each landable
independently in review, but all in the same diff per the
project's "bundle milestones" preference):

**Engine — server-side event delivery:**

1. **EventBus plumbing.** Add `crates/agentdb-api/src/event_bus.rs`
   with a tenant+session-tagged broadcast channel. Wire emission into
   the success path of `append_event`'s callers (`handlers.rs` for
   `append_and_fetch`, `fork.rs` for fork-time events if any). Unit
   tests on emission ordering relative to durable write.
2. **Catch-up scan helper.** Generalize `fetch_last_n` (or add a
   sibling) that returns events with `seq ∈ [from_seq, end_seq)` in
   ascending order. Unit tests.
3. **Per-session subscribe endpoint.** `GET /api/tenants/{tenant}/sessions/{session}/subscribe`.
   Catch-up + live-tail. `from_seq`, `kind`, `heartbeat_secs`.
   Backend auth via existing Basic middleware (browser auth comes in
   milestone 7). Lag policy. Integration test: append events,
   subscribe with `from_seq`, verify ordering and replay correctness.
4. **Tenant-wide subscribe endpoint.** `GET /api/tenants/{tenant}/subscribe`
   with `session=` and `from_seq_per_session=` filters. Integration
   test.
5. **Multi-node 421 redirect.** Both endpoints return 421 with the
   owner's address when the session is on a peer node. Integration
   test using the existing two-node test harness.
6. **Lag-too-far behavior.** Integration test that fills the bus
   beyond capacity and asserts the slow subscriber gets a
   `lag_too_far` SSE error event with `last_delivered_seq`.

**Browser-direct surface — auth, CORS, transport:**

7. **JWT verification middleware.** Per-tenant config for HMAC
   secret or JWKS URL (env-var-driven; admin endpoint deferred).
   `Authorization: Bearer ...` and `?access_token=...` both
   accepted. Verify `iss`, `aud` (if configured), `tenant`,
   `sessions`, `exp`. Cache JWKS with TTL. Reject mixed
   header+query as `400`. Integration tests: HMAC happy path,
   JWKS happy path, expired token, wrong tenant, session not in
   `sessions` claim, missing required claims.
8. **JWT-expiry close behavior.** Server checks `exp` on each
   delivery; on expiry, sends `event: error` with
   `{ "error": "jwt_expired", "last_seq": N }` and closes the
   connection. Integration test: short-lived token, observe
   close + payload.
9. **CORS per-tenant.** Read `AGENTDB_TENANT_<id>_CORS_ORIGINS`,
   echo origin if matched, set `Access-Control-Allow-Headers`
   (`Authorization`, `Content-Type`) and `Access-Control-Allow-Methods`
   (`GET`, `OPTIONS`). Reject `*` with credentials. `OPTIONS`
   preflight handler. Integration test from a browser-shape `fetch`
   stub.

**SDKs — what developers actually call:**

10. **JS/TS SDK** (`clients/typescript/` — new). Iterator-shaped
    `subscribe()` returning an `AsyncIterable<Event>`. Uses `fetch`
    streaming with `Authorization: Bearer ...`; falls back to
    `EventSource` with `?access_token=` if explicitly opted into.
    Auto-reconnect on disconnect with `from_seq=last_seen+1`.
    JWT refresh via app-provided callback (`onTokenRefresh: () =>
    Promise<string>`). Bundled as ESM + CJS; types included.
    Smoke test against a live server in CI.
11. **Python SDK** (`clients/python/` — extend). Same
    iterator-shaped `subscribe()` (sync + async variants).
    JWT auth path mirroring JS SDK. Auto-reconnect with `from_seq`.
    Smoke test.

**Docs + examples:**

12. **Docs and examples:**
    - README: new "Real-time subscriptions" section under "API tour"
      with browser-direct quickstart (HTML + a few lines of JS).
    - `docs/agent-db.md`: add the primitive to the agent-native list.
    - `docs/ARCHITECTURE.md`: EventBus + endpoint flow + JWT
      verification flow.
    - `docs/runbooks/`: new pages for `jwt_expired`, `lag_too_far`,
      `subscribe_session_forbidden`, `subscribe_tenant_mismatch`
      (matching the existing per-error-code runbook pattern).
    - Quickstart agent example: extend
      `examples/agents/python/quickstart.py` to subscribe and react,
      showing a multi-agent toy.
    - `examples/browser/` (new): minimal HTML page that subscribes
      from the browser using the JS SDK + a tiny JWT-mint script
      (HMAC) so demo runs end-to-end without an external auth
      provider.

Estimated diff size: ~3500–5000 LoC across Rust (engine + JWT +
CORS) + TypeScript SDK + Python SDK extension + docs + tests.
Larger than the original ~2000–3000 LoC estimate because Phase 1
now ships the full browser-direct path, not just the engine.
Still in line with stated PR-size preference.

---

## 8. Open questions / deferred decisions

1. **GET vs POST for tenant-wide replay with many sessions.** If
   `from_seq_per_session` query strings get long, switch to a POST
   with JSON body returning SSE response. Pick the threshold during
   implementation.
2. **Fork-aware subscriptions** — see § 6.9. Useful for visualizing
   branching agent flows; defer until a user asks.
3. **WebSocket transport.** SSE is one-way and sufficient for
   Phase 1. Phases 2 (broadcast) and 3 (presence) want bidirectional;
   they'll need WebSocket. Decide WebSocket vs duplexed-HTTP at that
   point.
4. **Per-session keyed buses.** Phase 1 uses a single `EventBus` with
   client-side filtering. Switch to keyed when fanout volume justifies
   it; document the upgrade path but don't ship now.
5. **Cross-node SSE forwarding.** Phase 1 returns 421 redirects.
   Transparent server-side forwarding is a follow-up, like cross-node
   Mongo wire forwarding.
6. **Subscription quotas.** Per-tenant or per-box max-concurrent-subs
   limit. Not in Phase 1; add as part of admission control if abuse
   shows up.
7. **Mongo wire surface** — see § 6.10.
8. **Authorization granularity.** Phase 1 ships per-session
   authorization for browser subscribers via the JWT `sessions`
   claim (§ 6.13). Backend Basic auth remains tenant-wide. Tighter
   per-session controls on Basic-auth credentials (e.g. session
   whitelist on a Basic credential) are deferred — JWT covers the
   needed granularity for the browser-direct use case.
9. **Token signing key rotation.** When an HMAC secret or the
   keys behind a JWKS rotate, in-flight connections continue with
   their old (still-unexpired) tokens; new connections use the
   rotated keys immediately. Acceptable because tokens are
   short-lived. Document the rotation procedure in runbooks.
10. **CORS / JWT config surface.** Phase 1 uses env vars
    (`AGENTDB_TENANT_<id>_*`). For dynamic per-tenant config without
    a process restart, an admin endpoint would help — defer until a
    user asks.
11. **Mobile SDKs.** The JS SDK works in React Native via an
    `EventSource` polyfill or `fetch` streaming. Native iOS (Swift)
    and Android (Kotlin) SDKs are deferred until there's demand;
    REST + JWT is straightforward to consume from any HTTP client
    in the meantime.
12. **In-place JWT refresh on a live SSE connection.** Phase 1
    closes-and-reconnects on expiry. An HTTP/2 trailers or
    server-sent control-event-asking-client-to-push-token mechanism
    is theoretically possible but not worth the wire complexity
    given gap-free reconnect is the AgentDB norm. Revisit if
    customers complain about the reconnect latency.

---

## 9. Phase 2 (broadcast) and Phase 3 (presence) — sketch only

Not designed in detail. Brief shape so the Phase 1 API doesn't paint
us into a corner.

**Phase 2 — ephemeral broadcast.**

```
POST /api/tenants/{tenant}/channels/{channel}/publish
GET  /api/tenants/{tenant}/channels/{channel}/subscribe
```

Channels are arbitrary strings, scoped to a tenant, not persisted.
In-memory `BroadcastBus` keyed by `(tenant, channel)`. Use cases:
typing indicators, cursor positions, transient status pings. Bounded
buffer; lag policy = drop slow subscribers (no durable fallback by
design).

**Phase 3 — presence.**

```
WS /api/tenants/{tenant}/channels/{channel}/presence
```

Bidirectional. Clients send heartbeats with their presence state;
server fans out join/leave/update events. WebSocket because heartbeats
are client→server. Last-write-wins per client_id; expire after
N missed heartbeats.

Both can coexist with Phase 1 — they touch different endpoints,
different buses. No conflicts foreseen.

---

## 10. Non-goals

- Replacing Kafka / NATS / SQS. AgentDB is not a general-purpose
  message broker; it's an agent-native event-log database that happens
  to expose the log as a real-time feed.
- Cross-tenant subscriptions. Hard isolation by tenant remains the
  rule.
- Exactly-once delivery. Subscribers may see duplicates after a
  reconnect-with-`from_seq` if they hadn't acked the seq they were
  about to send; clients should be idempotent on `seq`.
- Server-side filtering by arbitrary payload field. Phase 1 supports
  `kind` only.
- Persistence guarantees beyond what `append_and_fetch` already
  provides. The change feed inherits the storage's durability profile;
  pub/sub adds no new guarantees.
