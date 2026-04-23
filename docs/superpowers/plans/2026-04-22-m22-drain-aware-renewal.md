# Plan: M22 — drain-aware lease renewal

Date: 2026-04-22
Scope: tie the drain switch from PR #35 into the lease-renewal
loop. When an operator POSTs `/api/admin/drain`, the renewal loop
immediately stops renewing; after `lease_duration` elapses, this
node's tablet leases expire naturally, the stale-lease reaper (PR
#34) on another node clears them, and the tablet rows become
available for re-acquire. No SIGTERM required for the handoff.

## Why

Today the drain switch refuses new writes but leaves the lease
renewal loop untouched. The leases keep getting renewed forever
until an operator SIGTERMs. That's fine for correctness (no
split-brain writes — drain refuses them) but wasteful for
failover: the tablet rows stay pinned to the drained node until
the process exits, so any observer (admin tooling, a future
scheduler-driven reassignment step) sees a stale ownership picture.

Making renewal drain-aware closes the loop:

- **Drain → stop renewing → leases expire → reaper clears →
  tablet rows show `owner = NULL`.**
- Time-to-clean-state = `lease_duration + safety_margin + one
  reaper tick`. With defaults (30s + 5s + 10s) that's ~45s.

## Scope

**Core (`crates/agentdb-core/src/box_runtime.rs`):**

- Expose the existing `draining: Arc<AtomicBool>` via a public
  accessor `draining_flag() -> Arc<AtomicBool>`. Clone-on-read is
  free (it's already an Arc); admin and renewal-job code need this
  same flag without threading a `BoxRegistry` handle around.

**Core (`crates/agentdb-core/src/lease.rs`):**

- `LeaseRenewalJob::start_with_drain(lease, drain_flag)`: same as
  `start` except the renewal loop checks the flag at the top of
  every tick and skips `lease.renew()` when set. Logs at INFO the
  first time it observes the drain so operators see the transition
  in the log; subsequent ticks skip silently.
- `LeaseRenewalJob::start(lease)` stays as a thin wrapper that
  calls `start_with_drain` with a freshly-allocated always-false
  flag — preserves every existing caller (tests + legacy paths)
  with no signature churn.

**Server (`bin/agentdb-server/src/main.rs`):**

- Pass `registry.draining_flag()` to `LeaseRenewalJob::start_with_drain`.

**Runbook (`docs/runbooks/node_draining.md`):**

- Add a short paragraph to the "Remediate" section documenting the
  new timing: operators can wait `lease_duration + safety_margin
  + reaper_tick` after drain to see the tablet rows clear, without
  needing to SIGTERM first. Makes the rolling-upgrade runbook (a
  future PR) simpler.

## Out of scope

- **Active lease release.** Not doing a "surrender" call that
  actively clears the row. Passive expiry is simpler and has the
  same end state. Active release buys back the `lease_duration`
  seconds but complicates failure modes (what if the release call
  errors? what if meta is unreachable during drain?). Keep it
  simple for now.
- **Drain-aware write-path check.** Already done in PR #35 — write
  path returns `NodeDraining` immediately, no change here.
- **Drain-aware election.** The scheduler leader election (PR #33)
  is intentionally separate from tablet ownership; a draining
  node can stay leader of scheduler until SIGTERM without hurting
  anything. Making election drain-aware is a follow-up if we
  decide it's worth the extra plumbing.

## Acceptance

- Unit test: `LeaseRenewalJob::start_with_drain` with a flag set
  to `true` before the first tick; spawn the job, wait past
  several ticks, observe no calls to `lease.renew()` (verified
  via metric counter).
- Unit test: same as above but flag starts false, flips true after
  one successful renewal — second renewal is skipped.
- Smoke test: boot a server, verify lease counter advances; POST
  `/api/admin/drain`; confirm `lease_renewals` counter stops
  advancing on subsequent ticks; verify the tablets row for this
  process eventually (within `lease_duration + safety_margin +
  reaper_tick`) has `owner_node_id = NULL`.
- `cargo test --workspace` green; `cargo clippy ... -D warnings`
  clean.

## Shape of the commit

Single PR, single commit:
`feat(core): drain-aware lease renewal (M22 continuation)`.
