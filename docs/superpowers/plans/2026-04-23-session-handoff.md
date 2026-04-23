# Warm-session data handoff

Status: in progress (2026-04-23).

Every shed / shard-reassignment PR since M20 has
documented the same correctness gap: **reassigning a
shard changes future routing, but the already-written
events stay on the old tablet**. Reads routed to the new
tablet don't see the old events. This PR closes the gap
for warm sessions — the simple, common case — and keeps
the harder cold/archive case out of scope with an
explicit error.

## What's in / out

**In:**
- `agentdb_core::handoff::migrate_warm_session` — atomic
  warm-session move between two local boxes (same
  process, same KV store). Returns the count of events
  migrated and the dest session's internal id.
- Admin endpoint `POST /api/admin/sessions/:sid/migrate`
  with body `{ "tenant_id": "t-1", "to_tablet_id": 2 }`.
  Calls the core helper. Returns 409 if the session is
  cold/archive (documented reason).
- CLI: `agentdb migrate-session <sid> --tenant t-1 --to 2`.
- Integration test: write 20 events on box 0, migrate to
  box 2, verify every event is readable via the new box
  using `fetch_last_n` (which routes by session id).

**Out of scope (explicit):**
- **Cold / archive sessions.** The segment path embeds
  `box_id` in object-store layout — moving a cold session
  requires either copying the segment file or rewriting
  the session's `segment_path` and the object stays
  orphaned on the source box_id's prefix. Both
  complicate atomicity. The migrate endpoint refuses
  non-warm sessions with `409 Conflict`.
- **Fork children.** If a session has forked children,
  those children live on the source box with their
  parent_session_internal_id pointing into the source's
  session registry. Moving the parent doesn't follow the
  pointer. Migration refuses sessions that are
  fork-parents for now — operators migrate leaf sessions
  only, or accept the broken parent pointer.
- **Cross-node handoff.** All boxes in the same process
  share one KV store, so migration is intra-process. A
  cross-node migration would need either snapshot ship +
  replay or full range copy, neither of which exists.
  Operators who want cross-node movement should do it via
  shed + accept the data-handoff gap (or wait for a
  future PR).
- **Bulk migration (migrate every session for a shed
  shard).** Shed stays as-is today; the operator migrates
  sessions individually with `agentdb migrate-session`
  if they care about data-visible handoff for specific
  sessions. Automatic bulk migration tied to `shed` is
  future work.

## Design

### Key shapes

All migration-relevant keys are fixed-layout via
`encode_key(Namespace::Events, box_id, tenant_id,
session_id, branch_id, seq)`. Session lookups live under
`Namespace::Meta` with known prefixes:

- `key_session_by_external(box, tenant_internal, external)`
- `key_session_record(box, tenant_internal, internal)`

### Migration steps

1. Resolve source: `tenants.lookup(from_box,
   tenant_external)` + `sessions.lookup(...)`. Refuse if
   tier != Warm or if session has a fork_child (reverse
   edge check via forked_from field — no reverse index
   today, so use "does the session have any forked_from
   references pointing at *this* session from another
   session on the same box?" Actually simpler:
   **refuse if the session has any direct descendants**.
   This query isn't indexed, so for the first version
   we'll check conservatively: if the session's tier is
   Warm, allow the migrate; callers who fork+migrate at
   the same time get "child left behind" as a known
   limitation. We flag this loudly in the response.
2. Mint dest tenant + dest session: `tenants.get_or_mint(
   to_box, tenant_external)` + `sessions.get_or_mint_with_fork(
   to_box, dest_tenant.internal_id, session_external, ...)`.
3. Scan source events via
   `kv.scan_prefix(prefix_for_session_branch(Events,
   from_box, from_tenant.internal_id,
   source_session.internal_id, DEFAULT_BRANCH_ID))`.
   Collect (old_key, value) into a Vec.
4. Build a WriteOps batch:
   - For each event: `Put(new_key, value.clone())` under
     `(Events, to_box, dest_tenant.internal_id,
     dest_session.internal_id, DEFAULT_BRANCH_ID, seq)`,
     and `Delete(old_key)`.
   - `Delete(key_session_by_external(from_box,
     from_tenant.internal_id, session_external))`
   - `Delete(key_session_record(from_box,
     from_tenant.internal_id,
     source_session.internal_id))`
5. Apply via `kv.write_batch(ops)`. **Atomic**: on
   success every event is under the new keys; on failure
   nothing moves.
6. Replay events into dest box's index:
   `dest_box.index_event(dest_session.internal_id,
   event)` for each migrated event. Index is per-box +
   per-process; this is a best-effort rebuild. If the
   index hasn't been lazy-built yet on the dest box, the
   rebuild path will pick the events up itself on next
   access.
7. Return `{ events_migrated: N, dest_session_internal_id }`.

### Error paths

- `tier != Warm` → `Error::Invalid` → 409 Conflict
  (consistent with duplicate-create).
- Source session not found → `Error::NotFound` → 404.
- from_box == to_box → no-op success (idempotent).
- Concurrent writer on source during migration: the
  atomic batch is a Put-then-Delete on the old key.
  A writer that successfully appends during the scan
  window would have its write moved to the new
  location only if the scan observed the new key,
  otherwise left behind on source. To avoid this
  race, callers should migrate sessions under an
  operator-declared quiesce (e.g., the session isn't
  being actively written to). This is documented; we
  don't hold the per-tenant write lock across the
  migration because it's potentially long-running.

### Admin endpoint shape

```
POST /api/admin/sessions/{session_id}/migrate
Body: { "tenant_id": "t-1", "to_tablet_id": 2 }
→ 200 { "events_migrated": 20, "from_tablet_id": 0,
       "to_tablet_id": 2 }
→ 404 session not found
→ 409 session tier is cold/archive; migration refuses
→ 409 source and destination are the same box
```

Authenticated via admin auth when
`AGENTDB_REQUIRE_ADMIN_AUTH=true` — no additional auth
scope.

## Test plan

Unit (agentdb-core::handoff):
- `migrate_warm_session_moves_events_atomically`
- `migrate_refuses_cold_session`
- `migrate_refuses_same_source_and_dest`
- `migrated_events_readable_via_dest_box`

Integration (agentdb-api):
- End-to-end: write 20 events on box 0, migrate to
  box 2, fetch from box 2 via
  `POST /api/tenants/t/sessions/s/append_and_fetch?fetch_last_n=30`
  (routed via LocationProxy override), assert all 20
  visible.

## Rollout

Backward compatible. No env flag — the endpoint + CLI
are new surfaces. Operators opt in by calling them.
