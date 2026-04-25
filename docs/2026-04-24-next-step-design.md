# AgentDB Next-Step Design

Date: 2026-04-24

This document proposes the next architectural step after the current review.

The goal is not "finish v2" in one milestone. The goal is to resolve the highest-value structural contradictions first, so later work lands on a stable contract.

## Objective

Implement a coherent session-centric routing model with auth-bound tenant identity.

At the end of this step, the system should satisfy the following contract:

1. Tenant identity is resolved by auth, not supplied as a client routing input.
2. Session id is the primary routing key for writes, fork, and session-scoped reads.
3. A moved session remains readable and writable through the same client-visible identity.
4. Cross-tablet reads are semantically correct, even if initially slower or narrower.
5. Archive tier does not change query correctness.

## Why This Step First

This is the highest-leverage step because it aligns four currently conflicting subsystems:

- API shape
- auth model
- storage identity
- routing/read semantics

Without this step, additional features will keep deepening the split between the prototype contract and the v2 contract.

## Proposed Milestone

### M26: Session-Centric Identity And Correctness

This milestone should be treated as a coherence milestone, not a feature milestone.

## Design

### 1. Introduce a canonical session locator

Add a canonical metadata record that answers:

- which tenant owns this session
- which tablet currently owns this session
- optional current shard id

Suggested shape:

```text
session_locator/{session_id} -> {
  tenant_id,
  tablet_id,
  shard_id,
  created_at_ms
}
```

This record should become the primary lookup for:

- append
- fork
- session-scoped reads
- migration bookkeeping

For the transition, this can initially live in the control plane metadata store if that is the lowest-risk implementation path. Long term it can be dogfooded into AgentDB storage if desired, but the next step should optimize for correctness and clarity first.

### 2. Move to globally unique session ids at the API contract level

The next-step API contract should assume session ids are globally unique.

That means:

- `fork` should take `parent_session_id` and `new_session_id`, without `tenant_id` in the body
- any session-scoped read or write path should route from `session_id`
- tenant remains available internally for quota, auth, and billing

A compatibility layer can temporarily preserve the current REST path, but the internal architecture should stop depending on `(tenant_id, session_id)` as a composite identity.

### 3. Make tenant implicit in authenticated REST

For the REST surface:

- auth should resolve the tenant once
- handlers should use authenticated tenant identity, not path-provided tenant identity
- state-mutating primitives must all live behind the same auth boundary

There are two reasonable transition options:

Option A:

- keep `/api/tenants/{tenant_id}/...` temporarily
- verify it matches auth
- ignore it for internal routing

Option B:

- add new auth-bound routes such as `/api/sessions/{session_id}/append`
- keep old routes as compatibility aliases

Option B is cleaner long term, but Option A is a smaller migration.

### 4. Replace per-tenant locking with per-session locking

Introduce a session lock map keyed by globally unique session id.

Required properties:

- two writes to the same session serialize
- writes to different sessions can proceed concurrently
- lock lifecycle should avoid unbounded growth, ideally with simple idle cleanup later

This is the most important runtime change in the milestone because it aligns execution behavior with the session-centric design.

### 5. Make read semantics correct before making them fast

Cross-tablet reads must stop assuming the tenant-hash box is authoritative.

The next step should implement a two-tier approach:

Tier 1:

- session-scoped reads use `session_locator` and go directly to the owning tablet

Tier 2:

- tenant-wide reads explicitly fan out across the relevant tablets and merge results

For an initial implementation, "relevant tablets" can be conservative:

- all tablets currently hosting shards for that tenant, if available
- otherwise all tablets in the cluster

This may be slower, but it is semantically correct. Performance refinement can follow once the contract is correct.

### 6. Make indexed retrieval archive-complete

Any index build, restore, or replay path must treat archive as queryable data.

Two acceptable implementations:

Option A:

- include archive segments in the tablet index

Option B:

- keep archive out of the hot index, but explicitly fan in archive-backed candidates during query planning

Option A is simpler semantically. Option B may be cheaper operationally. The key requirement is that the query contract no longer depends on whether data is archived.

### 7. Narrow the role of tenant-hash routing

After `session_locator` exists, tenant hash should only remain as:

- legacy fallback during transition
- coarse placement heuristic for new-session creation if needed

It should no longer be the load-bearing routing rule for existing-session traffic.

## Implementation Plan

### Phase 1. Identity and auth cleanup

- Add `session_locator` metadata.
- Ensure new sessions populate it.
- Route `fork` by parent session id through the locator.
- Move `fork` under the same auth model as other tenant-scoped mutations.
- Make handlers trust auth-derived tenant identity over request-supplied tenant identity.

### Phase 2. Per-session runtime semantics

- Introduce session lock map.
- Switch append and fork serialization from tenant lock to session lock.
- Keep quota/admission per tenant and per tablet as they are.

### Phase 3. Correct read routing

- Route session-scoped reads through `session_locator`.
- Change tenant-wide Mongo `find` and `aggregate` to use explicit multi-tablet fanout when the request is not session-pinned.
- Keep a simple merge strategy first.

### Phase 4. Archive-complete query semantics

- Remove archive skips from index build/replay or compensate in the planner.
- Add tests that prove archived data is visible through both locality-bound and unhinted retrieval paths.

## Acceptance Criteria

The milestone is complete when all of the following are true:

1. `fork` no longer depends on client-supplied tenant identity for correctness.
2. Authenticated REST mutation paths all sit behind one consistent tenant-resolution model.
3. Writes to different sessions of the same tenant can proceed concurrently.
4. Session moves do not cause silent partial reads.
5. Archived data remains query-correct for indexed retrieval, not only direct session scans.

## Explicit Non-Goals

The following are out of scope for this step:

- fully dogfooded per-tablet metadata in AgentDB storage
- final tablet split/merge algorithms
- sophisticated fanout planning
- performance optimization of multi-tablet retrieval
- multi-region or AZ-aware placement work

## Notes On Migration Strategy

This step does not need a flag day.

A pragmatic transition is:

1. Add `session_locator`.
2. Teach new code paths to prefer it.
3. Keep tenant-hash fallback only for pre-existing sessions without locator rows.
4. Backfill locators lazily on first touch or via an offline scan.
5. Remove tenant-hash dependence after coverage is complete.

That path minimizes disruption while still converging on the v2 model.
