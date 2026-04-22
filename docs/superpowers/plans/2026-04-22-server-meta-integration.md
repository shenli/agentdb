# Plan: wire M18 control plane + M21 leases into the server binary

Date: 2026-04-22
Scope: *narrow follow-up* to M18 (PR #24) and M21 (PR #25). No new milestone;
this is the step that takes those merged pieces from "present in the crate
graph" to "actually exercised at runtime."

## Why

Today, `bin/agentdb-server/src/main.rs` still builds the box registry via
`BoxRegistry::new(kv, object_store, num_boxes)` — the pre-M21 constructor.
The `agentdb-meta` crate is a dependency of the integration tests only;
the running binary never opens a `MetaStore`, never assigns a `node_id`,
and never attaches a lease to any box. M21's `LeaseManager::is_live()` check
therefore short-circuits for every production write because `Box_::lease`
is always `None`.

That's fine for landing the pieces independently but it leaves the whole
control plane dormant. A single small PR ties the loop: server boots →
opens meta store → acquires leases → writes enforce them.

## Scope

In: `bin/agentdb-server/src/main.rs` (and a minimal touch to
`crates/agentdb-api/src/lib.rs` for health routes).

1. **Open a `MetaStore`.** New env vars:
   - `AGENTDB_META_BACKEND` — `sqlite` (default) or `postgres`.
   - `AGENTDB_META_URL` — dsn/path. Default for sqlite: `sqlite:./data/meta.sqlite?mode=rwc`.
     For postgres the user supplies the full `postgres://` URL.
   - Delegate to the existing `MetaStore::open_sqlite` / `open_postgres`
     helpers; migrations run as part of `open_*`.

2. **Node id.** New env var `AGENTDB_NODE_ID`. Default: `"<hostname>-<pid>"`
   if the hostname is available, else `"node-<pid>"`. Logged on startup.

3. **Swap constructor.** `BoxRegistry::new(...)` →
   `BoxRegistry::new_with_meta(kv, object_store, num_boxes, hlc, meta, node_id)`.
   On failure (meta unreachable, epoch stolen), the server exits — this is
   the behavior `new_with_meta` already mandates.

4. **Spawn renewal jobs.** Per box, call
   `LeaseRenewalJob::start(box_.lease().unwrap().clone())`. `std::mem::forget`
   the job handles — same pattern as the existing lifecycle engines and
   segment-gc jobs — since shutdown is process exit in the prototype.

5. **Health endpoints** (minimal — full M27 work is a separate milestone):
   - `GET /health/live` → 200 if the process is up. No deeper probe; this
     is the Kubernetes livenessProbe target.
   - `GET /health/ready` → 200 only if (a) every box reports
     `lease.is_some()` AND `lease.is_live()`, and (b) `MetaStore::health_check()`
     returns Ok. Returns 503 with a minimal JSON body listing which box
     (if any) lost its lease. This is the Kubernetes readinessProbe target
     and is what peers should hit before routing traffic.

## Out of scope

- Any change to lease config surface beyond what's already in `LeaseConfig::from_env`.
- Full M26 `--dev` mode CLI surface (clap, `dump-config`, `examples/compose/`).
  That's a bigger PR with UX decisions to make.
- Full M27 error-code-doc URLs and admin CLI.
- Lease epoch propagation to WAL/segment paths. Still a follow-up.
- Runbooks. A separate documentation PR.

## Acceptance

- Server boots against default config on a fresh machine: `./data/meta.sqlite`
  is created, migrations apply, each box row lands in the `tablets` table
  with a live lease.
- `GET /health/live` returns 200 immediately.
- `GET /health/ready` returns 200 once all boxes' leases are acquired.
- Kill the server, restart it → the previous leases are re-acquired
  (epoch bumps by one per box), readiness comes back.
- All pre-existing tests stay green (`cargo test --workspace`).
- `cargo clippy --workspace --all-targets -- -D warnings` clean.

## Test plan

- Unit: none added; the acquisition + renewal code already has unit and
  integration coverage.
- Smoke: start the binary with `AGENTDB_DATA=/tmp/agentdb-smoke` (fresh dir),
  `curl localhost:7878/health/ready` until 200. Kill the binary. Restart.
  Confirm via `sqlite3 meta.sqlite 'SELECT tablet_id, epoch FROM tablets'`
  that epochs incremented — proves the meta store persisted across restart.
- Existing integration tests keep using `BoxRegistry::new` directly and
  remain unchanged.

## Shape of the commit

Single PR, single commit, title
`feat(server): wire MetaStore + lease renewal + health endpoints into agentdb-server`.
