# Tiered Storage Redesign Implementation Plan

**Date:** 2026-05-02

**Goal:** Implement the tiered-storage redesign described in
`docs/architecture/TIERED_STORAGE_REDESIGN.md` so AgentDB's lifecycle states
map to real storage behavior instead of metadata-only transitions.

## Why This Plan Exists

The current runtime has a lifecycle enum and lifecycle policy thresholds, but
session history is not yet physically separated into hot and cold forms. The
implementation effort should close that gap in a staged way without breaking
the native API or the runtime object model.

## Scope

In scope:

- real cold storage for session history
- unified session-history read abstraction
- append-to-cold behavior through a new warm head
- explicit storage metadata and admin visibility
- lifecycle worker redesign
- dashboard and metrics updates

Out of scope for the first implementation wave:

- distributed archival orchestration
- multi-cloud storage backends beyond the existing abstractions
- complex archive compaction heuristics
- async restore orchestration across nodes

## Delivery Strategy

The work should be split into five phases. Each phase should leave the product
in a coherent, testable state.

## Phase 1: Make Cold Real

### Objective

Introduce sealed session segments and make `Cold` correspond to real storage
movement for session event history.

### Code Changes

- Modify `crates/agentdb-core/src/session.rs`
  - add session storage metadata
- Add `crates/agentdb-core/src/segments.rs`
  - segment metadata type
  - segment encoding/decoding
  - durable object/file storage helpers
- Add `crates/agentdb-core/src/session_history.rs`
  - unified history reader
- Modify `crates/agentdb-core/src/runtime.rs`
  - replace lifecycle-only cold transitions with real segment sealing
  - replace direct event scans for session history paths with the new reader
- Modify persistence layers as needed to store segment metadata

### Behavior

- warm session history remains appendable in the hot store
- lifecycle transition to cold seals the warm head into immutable segments
- hot rows for sealed ranges are removed or detached
- reads continue to work through the history reader

### Tests

- cold transition preserves full event history
- `fetch_from_seq` returns identical results before and after cold transition
- replay and fork-parent reads remain correct when parent is cold
- append-and-fetch still works for cold-backed sessions

## Phase 2: Append To Cold Via New Warm Head

### Objective

Avoid full session rehydration for the common resume path.

### Code Changes

- Modify `crates/agentdb-core/src/runtime.rs`
  - when appending to a cold session, create a new warm head
- Extend `session_history.rs`
  - merge sealed segments with the warm head transparently

### Behavior

- appending to a cold session changes lifecycle back to warm
- existing cold segments remain immutable
- only the new active portion becomes hot

### Tests

- append to cold session creates new warm head
- history reader returns `cold segments + warm head` in order
- repeated cold/warm/cold cycles preserve history correctness

## Phase 3: Operator Control And Visibility

### Objective

Expose lifecycle/storage state through explicit admin APIs and metrics.

### Code Changes

- Modify `crates/agentdb-api/src/routes.rs`
  - add admin lifecycle transition routes
  - add session storage inspection routes
- Add or modify API DTOs for storage state responses
- Modify dashboard assets
  - overview metrics
  - tenant storage summaries
  - session storage detail

### Recommended APIs

```text
POST /api/admin/sessions/{session_id}/lifecycle/transition
GET  /api/admin/sessions/{session_id}/storage
POST /api/admin/sessions/{session_id}/restore
GET  /api/admin/lifecycle/queue
GET  /api/admin/tenants/{tenant_id}/storage-summary
GET  /api/admin/storage/segments
```

### Metrics

- sessions by lifecycle
- bytes by tier
- transition counts
- transition failures
- restore counts and latency

### Tests

- admin forced transition changes real storage state
- storage inspection reflects real segment metadata
- dashboard API surfaces match runtime state

## Phase 4: Make Archive Real

### Objective

Replace metadata-only archive with an explicit archive representation and
product contract.

### Code Changes

- Add archive manifest types
- Extend session storage metadata
- Implement cold-to-archive transition
- Implement archive read mode handling in `session_history.rs`

### Behavior

- archived sessions use archive manifests or archive storage class metadata
- archive reads are explicit about latency/feature level
- restore state can be surfaced if archive reads require staging

### Tests

- archive transition preserves the declared read contract
- archive reads return correct results in supported modes
- archive restore path updates session storage state correctly

## Phase 5: Finalize DerivativeOnly

### Objective

Make raw purge behavior explicit and consistent for both session history and
artifact content.

### Code Changes

- extend lifecycle/retention policies
- add purge tracking for raw session-history bytes
- preserve searchable/derived state intentionally

### Behavior

- derivative-only sessions lose raw replay bytes
- metadata, summaries, and selected derivatives remain
- APIs report that full replay is no longer available

### Tests

- derivative-only state blocks raw replay as designed
- search and metadata inspection continue to work
- purge is idempotent and observable

## Supporting Refactors

### Replace Full Sweep With Scheduled Eligibility

Current lifecycle scanning should be replaced with a scheduled eligibility
mechanism.

Expected work:

- compute `next_cold_at_ms`, `next_archive_at_ms`, and
  `next_derivative_only_at_ms`
- maintain a queue or indexed schedule
- process due transitions with bounded concurrency

### Keep Artifact Separation

The existing artifact metadata/content split should stay. It is already the
cleanest part of the current lifecycle design and should become the model for
session-history separation.

## Migration Notes

### Existing Sessions

Sessions without storage metadata should be treated as:

- lifecycle value is advisory
- history is still in the hot store
- first cold transition writes explicit storage metadata

### Backward Compatibility

Because the project is pre-adoption, implementation should favor clarity over
long-lived compatibility shims. Short migration adapters are acceptable during
the rollout, but the final model should be simple.

## Suggested File Map

Likely new files:

- `crates/agentdb-core/src/segments.rs`
- `crates/agentdb-core/src/session_history.rs`
- `crates/agentdb-core/tests/session_history.rs`
- `crates/agentdb-api/tests/admin_lifecycle.rs`

Likely modified files:

- `crates/agentdb-core/src/session.rs`
- `crates/agentdb-core/src/runtime.rs`
- `crates/agentdb-core/src/policy.rs`
- `crates/agentdb-api/src/routes.rs`
- `crates/agentdb-dashboard/static/index.html`
- `crates/agentdb-dashboard/static/app.js`
- `crates/agentdb-dashboard/static/styles.css`
- `docs/architecture/ARCHITECTURE.md`
- `docs/operations/PERFORMANCE.md`

## Acceptance Criteria

The redesign is complete when all of the following are true:

- `Cold` means session history is physically separated from the hot store
- the main session-history reads behave the same for warm and cold sessions
- appending to a cold session does not require rewriting full history
- archive state is represented explicitly, not just by an enum label
- lifecycle storage state is visible to operators and the dashboard
- product docs, performance notes, and implementation all describe the same
  thing

## Final Note

The first priority is honesty of the product model. AgentDB does not need a
complex archival subsystem before it has a real cold-storage boundary for
session history.
