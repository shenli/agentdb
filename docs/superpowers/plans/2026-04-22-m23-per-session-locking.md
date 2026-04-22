# Plan: M23 — per-tenant write locking (scoped)

Date: 2026-04-22
Scope: the *ordering-coordination* half of M23. Replace the single
`Box_::write_lock: Mutex<()>` with a per-tenant lock map so two
tenants on the same box no longer serialize through one mutex. Full
M23 (tablet-admission semaphore, per-session LRU-evicted map,
lock-contention histogram) remains for follow-up PRs.

## Why

Every write today acquires `box_.write_lock.lock().await` — one
mutex per box. Two tenants that happen to route to the same box
serialize at the write path regardless of available cores. The plan
documents this as a hard throughput ceiling per box (see design
§5.2 and `notes/2026-04-21-architecture-review-decision-matrix.md`
B3). Per-tenant locking unblocks cross-tenant parallelism with a
bounded complexity budget.

## Why per-tenant, not per-session

The M23 plan entry calls for per-*session* locking. Two realities
argue for tenant-level in this PR:

1. **Mint races.** `TenantRegistry::get_or_mint` and
   `SessionRegistry::get_or_mint` both use a lookup-then-write
   pattern that's racy without external synchronization. Today the
   box-wide write lock serializes mints. Moving to per-session locks
   wouldn't help — two concurrent first-touches of the same tenant
   from two different sessions would still race on tenant mint.
   Per-tenant locking preserves the existing mint guarantee with no
   additional machinery.
2. **Incremental scope.** Per-tenant is a one-file refactor of the
   lock acquisition + a type signature change on five helper
   functions. Per-session adds a tenant-mint serialization layer on
   top, which doubles the PR's surface area. Full per-session
   locking lands after M20 when *sessions* (not tenants) are the
   routing unit anyway.

The design matrix specifically calls this out: "only makes sense
after placements exist as a distinct concept — otherwise you're just
renaming the box lock." We're not renaming; we're actually unlocking
cross-tenant parallelism within a box.

## Scope

**Code (`crates/agentdb-core/src/box_runtime.rs`):**

- Add `Box_::tenant_locks: DashMap<String, Arc<tokio::sync::Mutex<()>>>`.
- Add `Box_::tenant_write_lock(tenant_external: &str) -> Arc<Mutex<()>>`
  that lazily creates the entry on first access. No LRU eviction in
  this PR — the map grows with unique tenant count, which is bounded
  per box by `max_warm_sessions` * tenant-to-session ratio.
- Remove `Box_::write_lock` after all callsites migrate.

**Helper signatures:**

- `cold_transition`, `reactivate`, `cold_to_archive`, `archive_to_cold`,
  `fork_session`, `append_event`: currently take
  `_guard: &tokio::sync::MutexGuard<'_, ()>` as proof. Change to
  `&tokio::sync::OwnedMutexGuard<()>`. Single type swap per helper.

**Callsites (~39, 11 files):**

- `primitives.rs`, `admin.rs`, `fork.rs` (api + core), `lifecycle.rs`,
  `segment_gc.rs`, `tiering.rs` tests, `mvcc_isolation.rs` test,
  `planner.rs` tests, `read_ctx.rs` tests, `wire/handlers/find.rs`.
- Each callsite already has the tenant external string (via request
  body, loop variable, or test literal). Swap
  `box_.write_lock.lock().await` → `box_.tenant_write_lock(ext).lock_owned().await`.

**Fork's two-session case:** `POST /api/sessions/:id/fork` grabs the
lock for the parent session's tenant. Since fork creates a child
*within the same tenant*, one lock covers both. No ordered
acquisition needed (the parent and child share the key).

## Out of scope

- **Per-session locking** — see above; deferred to post-M20.
- **LRU eviction** of the tenant lock map. Tenants are long-lived
  at prototype scale; the map is bounded by the number of distinct
  tenants that ever wrote to this box. Follow-up PR.
- **Tablet-level admission semaphore** — the other M23 component.
  Separate PR, same theme.
- **Lock-contention histogram** (`lock_contention_wait_ns`).
  Follow-up observability addition.
- **Cross-tenant ordered acquisition** (for rare cross-tenant admin
  ops). None exist today; add when the first one lands.

## Acceptance

- Integration test (new): spawn N concurrent `append_and_fetch`
  calls for N distinct tenants on the same box. Wall time is
  materially less than N × single-call time (at least 2× speedup
  for N=4). This proves the parallelism is real.
- Existing tests: `cargo test --workspace` passes; mvcc-isolation
  tests in particular continue to hold (they rely on the lock
  semantics for correctness).
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
- No regressions in single-tenant hot path (same-tenant writes
  still serialize correctly).

## Risk

- **Correctness of mint races.** `get_or_mint` is TOCTOU-vulnerable
  without serialization. Per-tenant lock preserves this because
  both racing requests for the same *new* tenant share the same
  key and thus the same lock. Verified by re-running the existing
  mvcc tests and the tenant-mint test.
- **Lock map growth.** Bounded by distinct tenants per box — on the
  order of hundreds at prototype scale. Follow-up PR adds LRU.
- **Guard-type substitution.** Five helper signatures change. The
  change is mechanical; callers pass `&guard` identically whether
  `guard` is `MutexGuard` or `OwnedMutexGuard`.

## Shape of the commit

Single PR, single commit:
`feat(core): per-tenant write locking (M23, scoped)`.
