# M12: Archive Tier + Lifecycle Webhooks — Design

## Goal

1. Add `Tier::Archive` as the third tier below cold. In the prototype
   this is a metadata-only flip on the `Session` record; in a real
   deployment it reserves the hook for a storage-class transition
   (S3 Standard → Glacier, etc.).
2. Emit an HTTP webhook POST on every tier transition — auto or
   admin-forced — so external services can build derived-data
   pipelines, retention workers, or compliance trails against a
   single event stream.

## Motivation

§1.3 of the design doc calls for a three-state lifecycle
(warm/cold/archive), and §2.5 makes "derived data as first-class"
dependent on a trigger mechanism the DB provides so the application
can respond. Today the prototype has warm and cold; there is no tier
below cold, and no way for an external service to learn that a tier
transition happened other than by polling `/api/admin/boxes` or
keeping the SSE stream open.

A tier transition is the **one** event in the DB that a derived-data
service needs to hear about: it's the point where a session is
"done enough" to summarize, embed, or archive. The webhook gives that
service a stable, at-most-once delivery contract.

Intentionally NOT in scope: actually generating derivatives,
LLM calls, or prescribing a payload schema for derivative-writeback.
Those are the integrator's concern per §2.9.

## Scope

**In:**
- New `Tier::Archive` variant in `agentdb-core/src/types.rs`, plumbed
  through every `match` on `Tier`.
- Automatic cold→archive transition via the existing
  `LifecycleEngine` sweep: if `session.tier == Cold` and
  `now - last_active_at > T_ARCHIVE_SECS`, flip to archive.
  `T_ARCHIVE_SECS` is controlled by `AGENTDB_T_ARCHIVE_SECS` env var
  (default: 10× `AGENTDB_T_COLD_SECS`).
- `POST /api/admin/tier/force` accepts `target: "archive"` and
  transitions from cold (or warm, via a cold stop-off that happens
  inline). Archive → cold / warm also supported via the same endpoint.
- Archived sessions remain queryable: hybrid retrieval and
  `fetch_last_n` work unchanged because the segment file isn't moved.
  Only `Session.tier` changes.
- New per-box webhook URL: `POST /api/admin/boxes/:id/webhook`
  accepts `{"url": "https://..." | null}`. Stored in
  `Box_::webhook: RwLock<Option<String>>`. No persistence — cleared on
  restart, same as `Quota` and `InjectedFault`.
- Every tier transition (auto or admin, and for all 4 direction pairs
  {warm↔cold, cold↔archive, plus warm→archive as warm→cold→archive
  inline}) fires exactly one webhook POST.
- Fire-and-forget delivery via `tokio::spawn`. 5s connect+read
  timeout via `reqwest::Client`. No retries.
- New metrics on `BoxMetrics`: `webhook_fires: AtomicU64` and
  `webhook_failures: AtomicU64`. Both show up in the existing
  `MetricsSnapshot` and SSE `MetricsTick`.
- Dashboard shows the webhook URL (masked after `://host`) plus
  fires/failures counts on each box tile.
- New gauges on `GaugeSnapshot`: `archive_sessions` already exists
  from M7 — ensure it's incremented/decremented everywhere a tier
  transition happens.

**Out:**
- No actual storage movement on archive (no S3 storage class, no
  Glacier). Documented as future work in ARCHITECTURE.md.
- No retries, no durable outbox, no retry metrics beyond the single
  `webhook_failures` counter.
- No per-tenant webhook routing. One URL per box.
- No webhook auth beyond what the integrator's TLS endpoint provides.
  Follow-up: optional `X-AgentDB-Bearer` token from env config.
- No SSE event for "webhook fired" or "webhook failed" — those are
  counter-only.
- No webhook on non-tier-transition events (fork, fault, quota hit,
  etc.). The existing SSE surface covers those for in-process
  observers.
- Archive tier does NOT drop raw events, derived artifacts, or
  anything else. It's a relabel. (The spec discussion explicitly
  rejected option #1 "drop raw, keep derived" for the prototype.)

## Design

### Tier plumbing

1. Add `Archive` to `Tier` enum. Current variants: `Warm`, `Cold`,
   `Archive` (the third one already exists in the type system from M7
   but isn't reached by any transition). Verify every `match` on
   `Tier` handles all three arms without a trailing `_ => ...` that
   would silently swallow Archive.

2. Add `cold_to_archive(session: &mut Session)` and
   `archive_to_cold(session: &mut Session)` functions in
   `agentdb-core/src/tiering.rs`. Both just mutate `session.tier` and
   persist via `SessionRegistry`. No KV event deletes, no object-store
   reads.

3. Add `AGENTDB_T_ARCHIVE_SECS` env var. `LifecycleConfig` grows a
   `t_archive: Duration` field. Server main parses it.

4. `LifecycleEngine::sweep` grows a second pass: after the existing
   warm→cold walk, iterate sessions where `tier == Cold` and check
   the archive threshold. Structurally identical to the warm→cold
   pass, different threshold + different target tier.

5. `POST /api/admin/tier/force` accepts `target: "archive"`. Admit
   the transitions:
   - warm → archive: warm → cold → archive (two inline steps, both
     fire webhooks)
   - cold → archive: direct
   - archive → cold: direct
   - archive → warm: cold_reactivate from archive's segment, then
     warm (two inline steps, both fire webhooks)

### Webhook config + delivery

- `Box_::webhook: RwLock<Option<String>>` initialized to `None`.
- Admin endpoint `POST /api/admin/boxes/:id/webhook`:
  ```rust
  #[derive(Deserialize)]
  struct WebhookRequest { url: Option<String> }
  ```
  Validates that `url`, if present, starts with `http://` or
  `https://`. Otherwise returns 400.
- Delivery: a new free function in `agentdb-api/src/webhook.rs`:
  ```rust
  pub fn fire_tier_change(
      state: &AppState,
      box_: &Arc<Box_>,
      tenant_external: &str,
      session_external: &str,
      from: Tier,
      to: Tier,
      session: &Session,
  );
  ```
  This reads `box_.webhook` under the RwLock read guard, and if
  `Some(url)`, spawns a `tokio::spawn` that POSTs the JSON payload
  with a 5s timeout. On any failure (network, 4xx, 5xx, timeout) it
  increments `box_.metrics.webhook_failures` and logs a `warn!`. On
  success it increments `webhook_fires`.
- The `AppState` owns a `reqwest::Client` pre-built with the timeout
  configured, so we don't build one per fire.

### Call sites that fire webhooks

Every tier transition inside the API and core layers now calls
`fire_tier_change`. There are five sites:

1. `crates/agentdb-api/src/admin.rs::force_tier` — after the existing
   `cold_transition` / `reactivate` / new `cold_to_archive` /
   `archive_to_cold` calls succeeds. This is already the place where
   the `TierChange` SSE event fires; webhook fires alongside it.

2. `crates/agentdb-core/src/lifecycle.rs::sweep` — the existing
   warm→cold branch already records metrics after `cold_transition`;
   we extend it to also call `fire_tier_change`. BUT: core does not
   know about `AppState` or webhooks (layer boundary). Solution: the
   `LifecycleEngine` takes a callback:
   ```rust
   pub type TierChangeHook = Arc<
       dyn Fn(&Arc<Box_>, Session, Tier /*from*/, Tier /*to*/)
           + Send + Sync
   >;
   ```
   Stored on `LifecycleEngine` at construction. The server binary
   builds the hook as a closure that owns an `AppState` clone and
   calls `webhook::fire_tier_change`. If no hook is registered
   (tests), it's a no-op.

3. `crates/agentdb-core/src/lifecycle.rs::sweep` — cold→archive
   branch, same hook call.

4. `crates/agentdb-api/src/admin.rs::force_tier` for the
   warm→archive path needs two hook calls (one per inline step).

5. `crates/agentdb-api/src/admin.rs::force_tier` for the
   archive→warm path likewise needs two hook calls.

### Payload

```json
{
  "event": "TierChange",
  "ts_ms": 1729800000000,
  "box_id": 2,
  "tenant_id": "t-demo-a",
  "session_id": "ticket-42",
  "from": "cold",
  "to": "archive",
  "session": {
    "internal_id": 17,
    "next_seq": 143,
    "last_active_at": 1729790000000,
    "segment_path": "segments/2/1/17/0.seg"
  }
}
```

`from` / `to` are lowercase string names (`"warm"`, `"cold"`,
`"archive"`). The `"event"` key is a fixed `"TierChange"` so future
webhook variants can discriminate. `tenant_id` and `session_id` at
the top level are the external string ids (what the caller knows);
`session.internal_id` is the numeric id scoped to the box.

### Metrics

```rust
pub struct BoxMetrics {
    // ...existing...
    pub webhook_fires: AtomicU64,
    pub webhook_failures: AtomicU64,
}
```

Both exposed in `MetricsSnapshot`. SSE `MetricsTick.boxes[i]` doesn't
need new fields for this — dashboard can read them through the
`/api/admin/boxes` snapshot.

### Dashboard

- The per-box tile gets two extra small text lines:
  ```
  webhook: https://…/xyz (or "none")
  fires: 42 / fails: 0
  ```
- No new SSE event variant.

### Tests

**agentdb-core unit tests:**
- `LifecycleConfig::t_archive` defaults from env var
- `cold_to_archive` flips tier, persists, leaves segment unchanged
- `LifecycleEngine::sweep` transitions cold sessions past
  `t_archive` to Archive
- `LifecycleEngine::sweep` ignores cold sessions within
  `t_archive` window
- Hook callback invoked with correct from/to tiers (using a
  `Mutex<Vec<(Tier, Tier)>>` capture)

**agentdb-api unit tests:**
- `POST /api/admin/boxes/:id/webhook` with `{url: "http://..."}`
  persists on `Box_`
- `POST .../webhook` with `{url: null}` clears
- `POST .../webhook` with `{url: "ftp://bad"}` returns 400
- `fire_tier_change` increments fires counter on 2xx response from
  a local mock server
- `fire_tier_change` increments failures counter on 500 / timeout
- `fire_tier_change` with no URL configured is a no-op

**agentdb-api integration test (new tests/archive_webhook.rs):**
- Start a local `hyper` mock server that echoes to a channel
- Configure a box's webhook URL to point at it
- `append_and_fetch` + force `cold` → assert one webhook received
  with `from=warm, to=cold`
- Force `archive` → assert one more webhook received with
  `from=cold, to=archive`
- Force `warm` from archive → assert two webhooks: first
  `archive→cold`, second `cold→warm`

## Risks / open questions

1. **Hook callback pattern introduces a layer-crossing closure.**
   Core shouldn't depend on `AppState` or `reqwest`. The callback
   type (`Fn(&Arc<Box_>, Session, Tier, Tier)`) keeps core ignorant
   of the concrete implementation; the server binary wires the
   closure. Cleaner alternatives (trait objects, channels) are
   overkill for one callsite pair. Documented.
2. **Fire-and-forget means a restart loses in-flight webhooks.**
   If the server crashes between the tier flip and the POST, the
   integrator misses the event. Acceptable for a prototype; a
   durable outbox is future work.
3. **Double-fire on admin forced warm→archive.** The spec fires two
   webhooks (warm→cold then cold→archive) to be uniform with what
   the auto path would do if it transitioned over time. An
   integrator doing stateful accounting needs to handle this. The
   payload's `from`/`to` fields make it unambiguous.
4. **`reqwest::Client` reuse.** One `reqwest::Client` on `AppState`,
   shared by all webhook fires. Already thread-safe and connection-
   pooled. We already use `reqwest` as a dev-dep in integration
   tests; adding it as a regular dep of `agentdb-api` is fine but
   documented.
5. **Test flakiness from mock-server timing.** The integration test
   uses a tokio channel with a generous timeout (1s) to receive.
   Fire-and-forget means the event arrives some time after the
   transition call returns; we wait, we don't sleep.

## Success criteria

- `cargo test --workspace` clean.
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
- Archived sessions queryable via REST `fetch_last_n` and Mongo
  `find` / `aggregate` (prove via an integration test).
- Webhook payload matches the spec schema exactly (field names,
  casing).
- Dashboard tile shows webhook URL + fires/failures when a URL is
  configured.
- `POST /api/admin/boxes/:id/webhook` with a bad URL returns 400
  without changing state.
- `bash scripts/smoke.sh` prints `smoke ok`.
