# Plan: M23 continuation — per-box admission semaphore

Date: 2026-04-22
Scope: the *bounded-concurrency* half of M23. PR #31 landed per-tenant
write locking (ordering coordination). This PR adds a per-box semaphore
capping in-flight writes so a burst from many tenants on one box
can't drive unbounded memory / IO pressure. Rounds out M23 except for
LRU eviction of the lock map and the `lock_contention_wait_ns`
histogram, which remain for follow-ups.

## Why

With per-tenant locking now allowing cross-tenant parallelism on one
box, a burst of writes across many tenants no longer serializes at
the lock but *does* serialize at IO — each write triggers KV puts,
index mutation, and segment work. Without a cap, 1000 simultaneous
writes would spawn 1000 simultaneous KV operations, holding 1000
index write-lock waiters, etc. Tail latency goes bad and memory
pressure grows.

The v2 design §5.2 and the M23 plan entry both call out a bounded
tablet-admission semaphore as the companion to per-session / per-
tenant locking. This PR is that semaphore.

## Scope

**Code (`crates/agentdb-core/src/box_runtime.rs`):**

- Add `Box_::admission: Arc<tokio::sync::Semaphore>`. Capacity is set
  at Box_ construction time; default 64 permits. The semaphore is
  independent of `Box_::admit`'s token-bucket QPS gating — QPS
  controls sustained rate, admission controls concurrency. Both fire.
- Add `Box_::admission_permits_total()` accessor returning the
  configured capacity (for metrics exposure).
- Constructor surface:
  - `Box_::new(id, hlc)` → capacity 64.
  - `Box_::new_with_capacity(id, hlc, capacity)` → configurable.
  - `BoxRegistry::new_with_capacity(kv, store, num, capacity)` →
    passes capacity down to every box.

**Server binary (`bin/agentdb-server/src/main.rs`):**

- New env `AGENTDB_ADMISSION_CAPACITY` (default 64). Non-zero `u32`.
- Passed through `BoxRegistry::new_with_capacity` so every box in
  this process shares the same capacity (heterogeneous per-box caps
  aren't useful at prototype scale).

**Write path (`crates/agentdb-core/src/primitives.rs`):**

- `append_and_fetch_with_actor`: acquire a permit from
  `box_.admission` immediately after the fault check, before QPS
  admission. Order matters:
  1. Fault check — short-circuit on injected error.
  2. Admission permit — bounded-concurrency gate (this PR).
  3. QPS admission — sustained-rate gate.
  4. Tenant write lock — ordering.
  Then do the write. The permit lives in a local binding and drops
  at function return, naturally releasing.
- Rationale for order: the fault check is work-free and should
  short-circuit before queueing; the admission permit bounds queue
  depth; QPS admission is cheap and refuses rather than waiting.

**Metrics (`crates/agentdb-core/src/metrics.rs`):**

- New counter `admission_waits_total` — bumped every time a caller
  has to wait on the semaphore (i.e., the permit wasn't available
  immediately). Tells operators when the cap is biting.
- New gauge `admission_in_flight` — permits currently held
  (`capacity - available_permits`). Present in `MetricsSnapshot` so
  it shows up in the `/api/admin/boxes/:id` endpoint.

## Out of scope

- **Per-tenant admission capacity.** One hot tenant can still hold
  all permits and starve others. Fairness is a follow-up (weighted
  semaphores or per-tenant sub-caps).
- **LRU eviction** of `Box_::tenant_locks`. Map is bounded by
  distinct-tenants-per-box at prototype scale; follow-up.
- **`lock_contention_wait_ns` histogram.** Histograms need a
  dependency choice (hdrhistogram / quanta / etc.) that deserves its
  own decision; follow-up.
- **Mongo-wire write path gating.** Wire handlers do go through the
  same `append_and_fetch_with_actor`, so the semaphore applies there
  too — no extra plumbing.

## Acceptance

- Integration test (new): spawn N concurrent writes on a box built
  with capacity K, where N > K. Observe `admission_waits_total` >= 1
  (some requests had to wait) and `admission_in_flight` reaches K
  briefly during the burst. Every write succeeds.
- `cargo test --workspace` green.
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
- OpenAPI snapshot regenerated (new `MetricsSnapshot` fields).
- Server binary accepts `AGENTDB_ADMISSION_CAPACITY` and the
  running-server `/api/admin/boxes/0` response carries the new
  metric fields.

## Shape of the commit

Single PR, single commit:
`feat(core): per-box admission semaphore (M23 continuation)`.
