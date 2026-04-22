# M17 Archive-Read Semantic Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Write tests first for each non-trivial step (superpowers:test-driven-development).

**Goal:** Finish the job M12 started. Archive tier should affect *latency*, not *semantics* — today it affects both. Three API surfaces currently return empty results against archived sessions purely because the read path has `Tier::Archive => Vec::new()`. Replace those branches with tier-aware segment reads.

**Three gaps:**

- `crates/agentdb-core/src/primitives.rs` — `fetch_last_n_merged` on a forked child whose parent is archived returns child-only (parent events dropped).
- `crates/agentdb-core/src/planner.rs` — `plan_locality_bound` on an archived session returns zero hits.
- `crates/agentdb-wire/src/handlers/find.rs` — Mongo `find` over `events` uses `scan_session_events` which is warm-only; archived sessions yield no documents.

**Design call.** Archive tier in this prototype is metadata-only: the `segment_path` still points at the cold-tier segment on object storage; only the `Session::tier` flag flips from `Cold` to `Archive` (see ARCHITECTURE.md "Lifecycle: warm → cold → archive"). So "reading an archived session" is the same object-store fetch as "reading a cold session" — just with a different `Tier` attribute. A shared helper handles both tiers uniformly.

**Source design:** `docs/agent-db-v2.md` §6 (performance), "archive reads must be semantically uniform" — also called out explicitly in `docs/ARCHITECTURE.md` M12 notes.
**Plan ref:** `docs/superpowers/plans/2026-04-21-v2-design-implementation.md` §4 M17.
**Acceptance criteria:** Test matrix — each of {REST `fetch_last_n_merged`, Mongo `find`, hybrid retrieval with session-hint} × {Warm, Cold, Archive} — returns semantically correct results. No path returns empty purely because of the archive tier.

---

## Phase A — Unified tier-aware scan helper

### Task A1: `scan_session_all_tiers` in `primitives.rs`

**Files:**
- Modify: `crates/agentdb-core/src/primitives.rs`

- [ ] **Step 1: Signature**

```rust
pub async fn scan_session_all_tiers(
    ctx: &ReadCtx,
    store: &Arc<dyn ObjectStore>,
    session: &Session,
) -> Result<Vec<Event>>
```

Returns every event visible through this `ReadCtx` for the given session, regardless of tier:

- `Tier::Warm` → existing `scan_session_events` (KV prefix scan via ctx snapshot).
- `Tier::Cold` → `read_segment_hedged` with `Tier::Cold`, filter `seq < session.next_seq`, filter `hlc_ts <= ctx.hlc_cutoff`.
- `Tier::Archive` → same as Cold but hedge at the longer archive delay (`read_segment_hedged` already does this per-tier).

If the session is Cold/Archive but has no `segment_path` (shouldn't happen but defense in depth), return empty.

- [ ] **Step 2: Unit test covering all three tiers**

Seed a warm session, tier-transition to Cold, then directly flip to Archive, and call the helper at each state. Same event set must be returned every time.

---

## Phase B — Rewire `fetch_last_n_merged`

### Task B1: Archive branch reads through segment

**Files:**
- Modify: `crates/agentdb-core/src/primitives.rs`

- [ ] **Step 1: Replace the Archive match arm**

Before:
```rust
Tier::Archive => Vec::new(),
```

After: use `scan_session_all_tiers` (or the per-tier segment read pattern) so archived fork-parents' events are visible.

- [ ] **Step 2: Regression test**

Fork a parent, append events to parent, transition parent Cold then Archive, call `fetch_last_n_merged` on the child. Should return `child_events + parent_events[seq < parent_seq_at_fork]` — identical to the warm / cold case.

---

## Phase C — Rewire `plan_locality_bound`

### Task C1: Archive branch scans segment

**Files:**
- Modify: `crates/agentdb-core/src/planner.rs`

- [ ] **Step 1: Replace the Archive match arm**

Mirror Cold's behavior — use `read_segment_hedged` with `Tier::Archive`.

- [ ] **Step 2: Regression test**

Text-search an archived session with a session hint → must return the hit. Today returns empty.

---

## Phase D — Rewire Mongo `find`

### Task D1: Use the tier-aware helper

**Files:**
- Modify: `crates/agentdb-wire/src/handlers/find.rs`

- [ ] **Step 1: Replace `scan_session_events(&ctx, &session)?` with `scan_session_all_tiers(&ctx, store, &session).await?`** (the `find` handler is already async).

- [ ] **Step 2: Thread `store` parameter** — `find_response` already has access to `registry.object_store()`; just pass it through.

- [ ] **Step 3: Regression test**

`insert` a document → transition session Cold then Archive → Mongo `find` returns the document.

---

## Phase E — Driver-level integration test

**Files:**
- Modify: `crates/agentdb-wire/tests/driver.rs`

- [ ] **Step 1: Test covering all tiers end-to-end via the real `mongodb` driver.**

Append via REST, transition via admin endpoint, query via Mongo driver. Assert results are identical across Warm/Cold/Archive.

---

## Phase F — Documentation

### Task F1: Update ARCHITECTURE.md

- [ ] Update the M12 "archive tier" section: the "partial" qualifier no longer applies; archive is semantically uniform with cold (only latency differs, per design).
- [ ] New short section "Archive read semantics (M17)" or integrate into existing Hybrid retrieval / Lifecycle sections.

### Task F2: Honest limits

- [ ] Call out explicitly what remains deferred: actual archive tiering to a cheaper S3 class (Glacier Instant / IA) is still a future deployment choice; we didn't change that. Only the read semantics are unified.

---

## Risks & non-goals

- **Risk: recall regression from locality-bound path over large archive segments.** Mitigation: benchmark is out of scope for M17; the goal is correctness, not perf. Follow-up if measurements surface a problem.
- **Non-goal: moving archive data to a cheaper object-storage class.** That's deployment-side and deliberately out of scope. See ARCHITECTURE.md "Lifecycle" section.
- **Non-goal: cross-tablet archive fanout.** M23+.

## Scope commitment

S–M scope, linear phases (A → B → C → D → E → F), no branching complexity. Expected: one short PR.
