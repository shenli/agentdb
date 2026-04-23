# Plan: M22 — node drain admin endpoint (scoped)

Date: 2026-04-22
Scope: graceful write-refusal for a single process, toggled via
`POST /api/admin/drain`. The natural continuation of PRs #33
(election) and #34 (stale-lease reaper). The full `agentdb drain`
CLI, the scheduler-driven tablet handoff, and the rolling-upgrade
runbook remain for follow-up PRs.

## Why

Today, stopping an AgentDB node means SIGTERM → writes already
in-flight may error → leases expire → the reaper on another node
eventually clears them → operators shrug and move on.

That's fine for development but rough for production:

- Clients see write errors that, from their perspective, look
  indistinguishable from a genuine outage.
- Operators have no way to verify a node has stopped accepting new
  traffic before killing it.
- Restart-on-config-change is strictly rougher than a rolling
  upgrade would allow.

A drain endpoint fixes the first two problems: the node refuses
new writes with a **specific, documented** error the moment the
operator flips the switch, and the operator can confirm the drain
with a GET before proceeding to SIGTERM.

## Scope

**Core (`crates/agentdb-core/src/box_runtime.rs`):**

- `BoxRegistry::draining: Arc<AtomicBool>` (new field, initialized
  false). `drain()` flips it to true; `is_draining()` reads it.
  Arc-wrapped so admin handlers can clone cheap references. Once
  true, the state does not flip back — a drained node stays
  drained until the process exits and re-launches (operators who
  want to "undrain" restart the process).

**Core errors (`crates/agentdb-core/src/error.rs`):**

- New `Error::NodeDraining { node_id: String }` variant. Carries
  the node id so clients can log which specific process is
  draining (useful when they round-robin across a fleet).

**Write-path gate (`crates/agentdb-core/src/primitives.rs`):**

- `append_and_fetch_with_actor` short-circuits with
  `NodeDraining` *before* the fault check and admission permit
  acquisition. No reason to queue behind a permit or sleep on a
  fault when we already know we won't accept the write.

**API error mapping (`crates/agentdb-api/src/error.rs`):**

- `ErrorCode::NodeDraining` → HTTP 503, shape mirrors
  `LeaseLost` / `FaultInjected`.
- `doc_url_for(NodeDraining)` returns the new runbook URL.

**Runbook (`docs/runbooks/node_draining.md`):**

- Standard 5-section template. Distinguishes intentional drain
  from accidental / leftover drain, points at `POST /api/admin/drain`
  and `/api/admin/drain` GET for verification.

**Admin (`crates/agentdb-api/src/admin_drain.rs`, new):**

- `POST /api/admin/drain`: flips the registry bit. If `meta`
  extension is layered, also updates `nodes.state` on this
  process's row to `"draining"` (so `/api/admin/nodes` shows it
  to operators). Returns `{ "node_id": "...", "drained_at_ms": N }`.
  Second POST is idempotent — returns the same body (the
  `drained_at_ms` from the first call is captured in a server-local
  `OnceLock<i64>`).
- `GET /api/admin/drain`: `{ "node_id": "...", "is_draining": bool,
  "drained_at_ms": Option<i64> }`. Safe to poll.

## Out of scope

- **`agentdb drain` CLI.** M26 packaging work. This PR's endpoint
  is the underlying machinery; the CLI is a thin wrapper that
  curls it.
- **Lease-renewal stop on drain.** Leases expire naturally when
  the operator eventually SIGTERMs; the reaper on another node
  handles the cleanup. Making the renewal loop drain-aware is a
  follow-up that lets the drained node gracefully vacate *before*
  SIGTERM, but doesn't change correctness — just latency of the
  handoff.
- **Un-draining.** Operators restart the process to undo. Adding a
  POST-to-undrain invites mistakes (e.g. load-balancer state
  drifting from node state) and doesn't have a clear use case.
- **Redirection of refused writes** to another node. Clients
  already retry on 503 from `LeaseLost` and `FaultInjected`; the
  same retry path handles `NodeDraining` without extra plumbing.

## Acceptance

- Integration test: fresh registry, `draining = false` → write
  path succeeds. Call `registry.drain()` → next write returns
  `Error::NodeDraining { node_id }`. Idempotent: second `drain()`
  doesn't error; state stays true.
- Unit test on the API error path: `ApiError::Core(NodeDraining)`
  → HTTP 503, body includes `code: "node_draining"`, populated
  `node_id`, and the runbook `doc_url`.
- Smoke test: boot a server, POST `/api/admin/drain`, confirm
  GET returns `is_draining: true`, attempt an `append_and_fetch`,
  observe the 503 with `doc_url` pointing at the new runbook.
- OpenAPI snapshot regenerated.
- `cargo test --workspace` green; `cargo clippy ... -D warnings`
  clean.

## Shape of the commit

Single PR, single commit:
`feat(server): POST /api/admin/drain + NodeDraining error (M22 continuation)`.
