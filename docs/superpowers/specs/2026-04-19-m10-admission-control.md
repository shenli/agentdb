# M10: Admission Control — Design

## Goal

Turn the existing advisory `Quota.max_qps` field into real token-bucket
backpressure. Rate-limit in two dimensions — per-box and per-tenant —
so one runaway tenant inside a box can't starve its neighbors, and a
single box can't monopolize the server.

## Motivation

`Quota.max_qps: Option<u64>` has existed on `Box_` since M7 but is
never consulted. Only `max_warm_sessions` is enforced, and even then
only as a single-threshold check with no smoothing.

The design doc's §1.8 third-tier multi-tenant story explicitly calls
for "per-box and global admission control" to cope with the bursty,
many-small-tenants workload. The current prototype has the surface
area (the `Quota` struct, the `record_quota_hit()` metric, the admin
endpoint) but not the mechanism.

This milestone lands the mechanism. It's scoped narrowly: one
algorithm (token bucket), one cost model (1 token per op), two
dimensions (per-box and per-tenant-in-box). Global-process and
per-tenant-override come later.

## Scope

**In:**
- Token bucket per `Box_` for aggregate per-box QPS
- Token bucket per tenant within a box (`HashMap<u64, TokenBucket>`
  keyed by `tenant_internal_id`), all sharing the same limit
- Both buckets must grant. An operation is admitted only if the
  per-box AND per-tenant buckets have tokens.
- Enforcement at four call sites:
  - `agentdb_core::primitives::append_and_fetch`
  - `agentdb_core::fork::fork_session`
  - `agentdb_wire::handlers::find`
  - `agentdb_wire::handlers::aggregate`
- Admin endpoints (`/api/admin/*`), SSE stream (`/api/stream`), and
  dashboard routes (`/dashboard/*`) are explicitly exempt.
- Config via the existing `Quota` struct, extended with two new
  optional fields. Admin `POST /api/admin/boxes/:id/quota` grows
  to accept them.
- REST: HTTP 429 with `Retry-After: 1` header + JSON error body.
- Mongo wire: command error response with code 82
  (ExceededTimeLimit); Mongo drivers surface this as `MongoError`.
- Dashboard: new SSE event `Throttled { ts_ms, box_id, tenant,
  scope }` broadcast on each denied request.

**Out:**
- Weighted costs (fork = 5, vector search = 2 tokens, etc.)
- Per-tenant `max_qps` overrides on the `Tenant` struct (v1 has one
  `max_tenant_qps` value per box, shared by all its tenants)
- Global process-wide QPS ceiling
- Background GC of idle tenant buckets (memory grows with tenant
  count; prototype-acceptable since the box cap is logical)
- Distributed rate limiting across multiple server processes

## Design

### Token bucket

Standard token-bucket semantics. `TokenBucket` and `AdmissionDenied`
both live in a new file `crates/agentdb-core/src/admission.rs`, which
the crate's `lib.rs` will `pub mod admission`. The types are re-
exported from the crate root so the wire and API layers can match on
`AdmissionDenied::scope` without pulling in the module path.

```rust
pub struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
    qps: f64,        // refill rate, tokens/second
    burst: f64,      // bucket capacity
}

impl TokenBucket {
    pub fn new(qps: u64, burst: u64) -> Self { ... }

    /// Try to consume one token. Refills based on elapsed wall
    /// time since last call. Returns true on success.
    pub fn admit(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.qps).min(self.burst);
        self.last_refill = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}
```

A `qps = 0` bucket (or `max_qps = None` from config) is treated as
"unlimited" — we never instantiate a bucket at all and `admit()`
trivially returns `true` via the `Option<TokenBucket>` wrapper.

### Bucket state on `Box_`

```rust
pub struct Box_ {
    // ...existing fields...
    box_bucket: Mutex<Option<TokenBucket>>,
    tenant_buckets: Mutex<HashMap<u64, TokenBucket>>,
}
```

Both behind `parking_lot::Mutex` (matches the existing convention on
`Box_` — `fault` and `quota` are already `parking_lot::RwLock`). The
admit path is:

```rust
impl Box_ {
    /// Returns Ok(()) if the request is admitted, or
    /// Err(AdmissionDenied { scope }) if either bucket refused.
    pub fn admit(&self, tenant_internal_id: u64) -> Result<(), AdmissionDenied> {
        // Per-box gate first (cheaper lookup).
        {
            let mut b = self.box_bucket.lock();
            if let Some(bucket) = b.as_mut() {
                if !bucket.admit() {
                    self.metrics.record_quota_hit();
                    return Err(AdmissionDenied { scope: "box" });
                }
            }
        }
        // Per-tenant gate.
        let per_tenant_qps = self.quota.read().max_tenant_qps;
        if let Some(qps) = per_tenant_qps {
            let burst = self.quota.read().burst.unwrap_or(qps);
            let mut map = self.tenant_buckets.lock();
            let entry = map.entry(tenant_internal_id)
                .or_insert_with(|| TokenBucket::new(qps, burst));
            if !entry.admit() {
                self.metrics.record_quota_hit();
                return Err(AdmissionDenied { scope: "tenant" });
            }
        }
        Ok(())
    }
}
```

`AdmissionDenied` is a new error type that each call site converts to
its protocol-appropriate response.

### `Quota` extension

```rust
pub struct Quota {
    pub max_warm_sessions: Option<u64>,
    pub max_qps: Option<u64>,       // now enforced, applies per-box
    pub max_tenant_qps: Option<u64>,// NEW: applies to each tenant within box
    pub burst: Option<u64>,         // NEW: bucket capacity; applies to both
                                    //      buckets. If None, each bucket's
                                    //      capacity defaults to its own qps
                                    //      (so ~1 second of buffer).
}
```

When `set_quota` is called via `POST /api/admin/boxes/:id/quota`, the
admin handler rebuilds both buckets from scratch — we do NOT try to
preserve in-flight token counts across a quota change. This is the
simplest correct behavior: operators tuning quotas accept a momentary
burst reset.

### Enforcement wiring

Each call site adds one line. Example in `append_and_fetch`, right
after the existing `max_warm_sessions` check:

```rust
let box_ = registry.route_tenant(tenant_external);
if box_.apply_fault().await { /* existing */ }
// NEW:
let tenant_internal = ...; // obtained via lookup_or_create below, but we need it first
box_.admit(tenant_internal).map_err(|e| Error::TooManyRequests(e.scope.into()))?;
```

There's a small chicken-and-egg issue: we need `tenant_internal_id`
to check per-tenant, but that id comes from `lookup_or_create`. Two
options:

1. Check per-box first (doesn't need tenant id), then resolve tenant,
   then check per-tenant. Two lock acquisitions per call.
2. Resolve tenant first, then check both in one `admit(tenant_id)`
   call. One lock acquisition but the tenant lookup runs even for
   requests that will be denied by the per-box limit.

Plan uses option 2 — simpler code, and tenant lookup is cheap (KV
point read) relative to any real operation. The order "box-first"
only matters if tenant lookup is itself rate-limited, which it
isn't.

### Protocol-specific responses

**REST (`agentdb_api::error::ApiError`):**
```rust
pub enum ApiError {
    // ...existing variants...
    TooManyRequests { scope: String },
}

impl IntoResponse for ApiError {
    // ...
    ApiError::TooManyRequests { scope } => (
        StatusCode::TOO_MANY_REQUESTS,
        [(header::RETRY_AFTER, "1")],
        Json(json!({"error": format!("rate limit exceeded ({scope})")}))
    ).into_response(),
}
```

**Mongo wire (`agentdb_wire::handlers`):** Return a command error via
the existing error path. Error code 82 maps to Mongo's
`ExceededTimeLimit`; Mongo drivers already know to surface it cleanly
without retrying forever.

### Metrics + SSE

- `record_quota_hit()` is called in both `admit()` failure branches.
  This is the existing per-box counter from M7; it already shows up
  in `/api/admin/boxes` responses and the SSE `MetricsTick`.
- New SSE event variant (`agentdb_api::stream::StreamEvent`):
  ```rust
  Throttled {
      ts_ms: u64,
      box_id: u16,
      tenant: String,    // external id, for dashboard display
      scope: String,     // "box" | "tenant"
  }
  ```
  Emitted from each call site on admission failure, before returning
  the error.

### Dashboard

- Add a per-tile "throttled" indicator when a `Throttled` event
  arrives for that box in the last N seconds. Simple animated dot
  pulse, no sparkline (§2 of the M7 perf sizing — dashboard code is
  intentionally small).
- The `Throttled` event also joins the event feed on the right.

### Tests

- Unit tests in `agentdb-core/src/admission.rs` (new file):
  - `token_bucket_starts_full_and_refills_over_time`
  - `token_bucket_respects_burst_ceiling`
  - `token_bucket_zero_qps_is_unlimited_via_none_wrapper`
  - `box_admit_denies_when_per_box_empty_even_if_tenant_empty`
  - `box_admit_denies_when_per_tenant_empty_even_if_box_has_room`
- Integration test in `agentdb-api/tests/admission.rs`:
  - Configure box with `max_qps=10, max_tenant_qps=5, burst=10`
  - Fire 20 rapid `append_and_fetch` calls from one tenant → first 5
    succeed, rest 429 (per-tenant gate trips first)
  - Sleep 1s → next request succeeds
  - Fire 20 rapid calls from two different tenants (10 each) → first
    10 total succeed (per-box cap), rest 429
- Integration test in `agentdb-wire/tests/driver.rs`:
  - Same scenario over the Mongo driver, confirm client sees a
    `MongoError` with code 82

## Risks / open questions

1. **Mutex contention under load.** Every admitted op takes two
   mutexes (box_bucket, tenant_buckets HashMap). Per-tenant bucket
   lookup is inside a second `Mutex<HashMap>`, which serializes all
   concurrent tenants in a box. For prototype scale (thousands of
   QPS) this is fine; production would want sharded maps or
   `DashMap`. Documented as a known limitation.
2. **Clock source.** `Instant::now()` is monotonic on every
   supported platform, so refill math can't go backwards. No
   fallback needed.
3. **Test flakiness.** Wall-clock dependent tests ("sleep 1s then
   request succeeds") are usually unreliable in CI. Mitigation: use
   generous margins (sleep 1200 ms, expect refill ≥ 1) and don't
   assert exact counts — only that "some throttled, some admitted"
   happened.
4. **Interaction with fault injection.** An injected latency fault
   also consumes time inside the box — requests under latency fault
   will hit the token bucket slower than normal, which accidentally
   gives the appearance of admission control working. Tests must run
   without injected faults to isolate admission-control behavior.

## Success criteria

- All existing tests still pass.
- `Quota.max_qps` and `Quota.max_tenant_qps` behave as described:
  configured limit = observed sustained admission rate (± 10 %), and
  excess requests get 429.
- Mongo driver test receives a code-82 command error on throttled
  query.
- Dashboard shows a `Throttled` event in the feed when the workload
  exceeds a configured limit.
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
