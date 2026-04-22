# Architecture Review Response — Placement Unit & Identity Model

**Date:** 2026-04-20
**Context:** Response to an external expert review of AgentDB's identity, placement, and distributed-locality design. The review's TL;DR: `tenant` is logical (hidden), `session` is the core data unit, introduce a `placement unit` as the internal locality primitive, and treat `agent` as a first-class product concept but not a physical shard key.

This note has two parts:

1. **Agreed improvements** — items where the expert's position and the existing design (`docs/agent-db.md`, `docs/ARCHITECTURE.md`) converge or where the review usefully sharpens something the design already acknowledges.
2. **Open questions** — items where I initially pushed back; expanded research and thinking on each.

A recommended next-step sequence is at the end.

---

## Part 1 — Agreed Improvements

These can be folded into `docs/agent-db.md` and sequenced into an M13-scale milestone without further debate.

### 1.1 Tenant is implicit, resolved from connection/auth context

**Current state.** Every REST and Mongo wire request carries an explicit `tenant_external_id`. Routing is `box_id = FNV1a(tenant_external_id) % N` (ARCHITECTURE.md:45–49). Tenant is load-bearing in both the API surface and the key layout.

**Target state.**

- Tenant is resolved at connection time from auth credentials (SCRAM for Mongo wire; bearer token for REST). The request payload no longer carries tenant.
- Tenant continues to exist internally as the logical boundary for:
  - Quota attribution (§4.5)
  - Security isolation (tenant A cannot see tenant B's sessions)
  - Billing accounting
- Session IDs become globally unique (UUID v7 or a KSUID-shaped identifier). Tenant namespace is no longer load-bearing for session identity.

**Why this change is uncontroversial.** The design doc already argues tenants are logical (§3.6) and the prototype already maintains a separate `tenant` registry. The only shift is *dropping tenant from the wire surface* — the internal concept stays intact.

### 1.2 `placement` is a distinct internal concept, separate from `box`

**Problem with the current model.** `box = hash(tenant)` forces an entire tenant into one box. A head tenant is a guaranteed hotspot and a concentrated blast-radius — exactly the failure mode §2.8 calls out and the prototype's box model does not solve.

**Target model.**

- **Placement** is the unit of locality and routing. A placement owns: a write-lock pool (see 1.3), placement-local indexes (see 1.4), a KV-key prefix, and lifecycle/tiering state.
- **Session → Placement** is the primary binding. A session is pinned to one placement for its entire lifetime.
- **Tenant → Placements** is 1:many. A tenant's sessions are distributed across placements by the default routing function `hash(session_id)`, so a hot tenant naturally spreads across many placements (and therefore hardware).
- **Box** becomes a co-hosting unit on a node — one process can host many placements. It is no longer the routing-visible concept.

**Default routing.** `placement_id = hash(session_id) % N`. Tenant is not in the routing path.

**Consequence for KV key layout.**

```
# Current
session/{box_id}/{tenant_internal_id}/{external}                   -> Session
event/{box_id}/{tenant_internal}/{session_internal}/{branch}/{seq} -> Event

# Proposed
session/{placement_id}/{session_id}                                -> Session   (tenant lives in the record, not the key)
event/{placement_id}/{session_id}/{branch}/{seq}                   -> Event
```

A small `session_index/{session_id} -> placement_id` lookup is needed for operations that arrive with a session id but no placement. The hot path (append) is already routed via `hash(session_id)`, so it skips the lookup. Fork, admin tooling, and forked-child parent walks pay one lookup per request.

### 1.3 Per-session write lock, not per-box

**Current state.** One `tokio::sync::Mutex` per box (`Box_::write_lock`, ARCHITECTURE.md:48). All writes in a box serialize, including writes to unrelated sessions.

**Target state.**

- **Per-session write lock** inside a placement. Mutations on session A and session B in the same placement can proceed in parallel.
- **Placement-level admission control** via semaphore (bounds in-flight writes per placement — replaces what the box-wide mutex implicitly enforced).
- **Sharded lock map** (e.g., `DashMap<SessionId, Arc<Mutex<()>>>`) with LRU eviction of cold entries to bound memory.

**Justification.** §2.7 establishes that single-session atomic writes are the universal baseline. A per-session lock matches the natural atomicity granularity exactly. Box-wide serialization is strictly tighter than required and leaves perf on the floor.

**Edge case.** Cross-session operations inside a placement (rare; e.g., multi-session atomic checkpoint) acquire session locks in deterministic order (by session id, ascending) to avoid deadlocks.

### 1.4 Placement-local indexes with fanout-merge for tenant-wide queries

**Current state.** `PerBoxIndexes` — per-box, lazy-built, in-memory, rebuilt on restart (ARCHITECTURE.md:93–97).

**Target state.**

- Indexes are placement-scoped (placement replaces box; semantics carry).
- Queries that pin `session_id` → route to the session's placement → single-placement search.
- Queries that pin `agent_id` or another tenant-scope filter but not `session_id` → fanout across the tenant's placements → rank/merge at the coordinator.
- Unfiltered tenant-wide search → fanout across all of a tenant's placements. Discouraged in the API surface but possible.

**This is a direct read of §4.4 + §4.5.** The only substantive change is substituting "placement" for "box" and making fanout+merge a first-class coordinator capability instead of an afterthought.

### 1.5 Indexes must be persistent and incremental (elevate to P0)

**Current state.** ARCHITECTURE.md:94–97 already flags this: lazy build, no incremental updates, lost on restart. M8 shipped HNSW via `instant-distance` but did not address persistence or incrementality.

**Target state.**

- **Persistence.** Index state survives a process restart. For HNSW this means snapshotting the graph; for BM25 the inverted-index tables. Probably serialized into the object store under a placement-scoped prefix.
- **Incrementality.** New events flow into the index on append, not through a rebuild. HNSW supports incremental insert natively; `instant-distance` does not, so a backend switch (hnswlib, `usearch`) may be required.

**Why elevate.** An agent-native database with a lazy, non-persistent index is not actually usable for retrieval at scale — every restart pays a full rebuild proportional to total warm data. The expert flags this as P0; the design doc already knows it; M8 deferred it. Time to stop deferring.

### 1.6 Archive reads must be semantically uniform

**Current state.** ARCHITECTURE.md:162–169 explicitly acknowledges M12 left partial archive-read coverage: `fetch_last_n` on a forked child whose parent is archived returns partial results; locality-bound planner calls return empty on archive; Mongo `find` on `events` returns empty for archived sessions.

**Target state.** Archive tier affects latency only, never semantics. Every read path reads through segments for archived sessions the same way it does for cold ones.

**Why this is agreed.** It is not a design decision — it is a known incompleteness the review usefully affirms as P0. The work is execution, not debate.

---

## Part 2 — Open Questions: Deeper Research

### 2.1 "Agent as first-class" — reframed as "agent as metadata dimension, not API resource"

**Expert's position.** `agent` is a first-class product API resource, separate from session, with its own endpoints. Sessions record `primary_agent_id`, events record `actor_id`, and multi-agent sessions are natively supported.

**Design doc's position (§3.8, §3.9).** Fork is included. Agent-as-concept is conditional on multi-agent becoming a mainstream pattern. §3.9 sets a high bar for inclusion: performance requires proximity to data AND atomicity requires transactional guarantees AND usage is broad enough that nearly every agent would use it.

**My initial pushback.** Scope creep; §3.9 fails.

**Deeper look — decompose the expert's proposal into four sub-claims:**

a) Events should carry an `actor_id` distinguishing which agent wrote them.
b) Sessions should record a `primary_agent_id`.
c) `agent` should be a queryable dimension (cross-session filtering).
d) `agent` should have its own lifecycle, endpoints, and quotas (a full first-class resource).

**Evaluation:**

- **(a) Yes.** `actor_id` on events is cheap and aligns with §2.5 (prefix-scan by agent is already a core query). This is just making an existing query dimension explicit in the schema.
- **(b) Yes — as a session-metadata field.** A session can attribute itself to an agent; this is a field on the session record, not a new entity.
- **(c) Yes.** An index over `agent_id` serves the §2.5 prefix-scan and §3.7 "hybrid retrieval with agent filter" queries. This is a retrieval feature, not a new API resource.
- **(d) No — for now.** A first-class agent resource forces decisions AgentDB should not make: what is an agent? (LangGraph node? CrewAI worker? tool-calling loop?) Who registers it? What is its lifecycle, separate from the session that embodies it? What is its quota model, separate from tenant quotas? None of these have database-native answers — they bind AgentDB to a particular agent framework. §3.9's three-way test fails: proximity (no — agent metadata is not in the hot path), atomicity (no — agent lifecycle doesn't need transactional guarantees with event writes), broad universality (unclear — depends on an agent framework's shape).

**Synthesis.** Adopt the *data-model* pieces of the expert's proposal (a, b, c) without adopting the *API-resource* piece (d). This gives §2.5's cross-session prefix-scan query a clean implementation and leaves room for full agent semantics later without pre-committing to a framework-bound shape. A later v2 can promote `agent` to a resource if a clear product direction emerges.

**Sub-question: validation.** Does `agent_id` need to be validated against a registered agent table (implies the resource concept) or is it free-form (implies it is just a tag)? I lean **free-form**, consistent with the tenant-on-first-use model the prototype already uses. First appearance of an `agent_id` auto-registers it if needed for billing; no registration call in the API.

### 2.2 Placement declaration surface

**Expert proposed two options:** SQL-ish `PLACEMENT BY` DDL, and Mongo `db.createCollection({placementKey: ...})`.

**My initial pushback.** DDL violates §3.1 property 4 ("DDL is not required") and §4.3 (document interface primary).

**Deeper look.** Three realizable options, increasing in user-facing complexity:

**Option A — No user-facing placement declaration.** Fixed default: `placement_id = hash(session_id) % N`. Users don't know placement exists; it is a pure implementation detail.
- Pros: zero API surface area; consistent with "tenant implicit"; no declaration path to version.
- Cons: workloads with genuine locality needs (N sessions per workflow that frequently cross-read each other) cannot express the hint.

**Option B — createCollection metadata.** Mongo-native declaration, e.g. `db.createCollection("events", {placementKey: {agent_id: 1, session_id: 1}})`. Native REST equivalent via an admin endpoint.
- Pros: fits existing Mongo wire naturally (Mongo has shard keys); mental model is familiar; backwards compatible (absent declaration → Option A default).
- Cons: still a declarative act at collection-creation time; rigid once set; introduces a config surface that must be versioned.

**Option C — Adaptive placement.** System observes access patterns and migrates sessions to co-locate "commonly fanned-out together" groups.
- Pros: no user burden; self-tuning.
- Cons: large engineering investment; live migration cost (segments on object storage are cheap to move but not free); debugging and predictability nightmares.

**§3.1 compatibility re-check.** §3.1's "no DDL" is about *schema evolution* (add fields without migrations). Collection-level administrative metadata at deployment time is a different thing — Mongo itself does this with `createCollection` and indexes. Option B does not violate §3.1.

**Recommendation.** Ship **A only** as the declared interface. Design the data layout so that placement_id is a computed value (not a hardcoded `hash(session_id)`) — the function can be swapped later. When a real workload demonstrates a need, add **B** as opt-in via Mongo createCollection metadata. **C** is out of scope for v1 and probably v2.

### 2.3 Affinity key (user-supplied placement hint)

**Expert suggestion.** Let callers supply an `affinity_key` at session creation; placement routing hashes on affinity_key instead of session_id when present.

**Research.**

**Use case.** A workflow orchestrator creates N sessions (one per agent in a crew) that coordinate. Co-locating them on one placement makes cross-session reads local and enables single-placement multi-session transactions.

**Without affinity key.** All sessions hash to random placements. Coordination = fanout.

**With affinity key.** `placement_id = hash(affinity_key) % N`. All sessions sharing `affinity_key` co-locate.

**Risks.**

1. **Hotspot via abuse.** If every user picks `"default"` as affinity_key, traffic converges. Mitigation: salt the hash with tenant_id so same-key traffic from different tenants spreads.
2. **Lock-in.** Once a session is bound, it cannot move (segments are placement-prefixed on the object store; moving = rewriting the prefix). If affinity becomes wrong later, remediation is an expensive migration.
3. **API surface bloat.** Another optional parameter on session creation that must be documented, validated, carried through every session-creation path, and explained to users.

**Recommendation.** **P2.** Not part of the initial placement-unit work. Revisit when a concrete benchmark or sample-app shows cross-session coordination as a real bottleneck (the `sample-app-benchmark` work-in-progress may surface this). The API-design cost of adding it later is low (optional field); the cost of adding it early and discovering it is wrong is high.

### 2.4 Tenant-implicit under Mongo wire — does this actually work?

**Concern.** Mongo wire protocol doesn't have first-class multi-tenant primitives. How does "tenant from connection context" integrate with standard Mongo drivers?

**Research.**

Two candidate carriers:

**(i) Mongo's `$db` field in OP_MSG.** Every OP_MSG names a logical database. Mongo's own multi-tenant patterns map tenant → database. A driver does `client.getDatabase("tenant_alpha")` and every subsequent command is namespaced. AgentDB reads `$db` at the wire-layer handler, resolves it to internal tenant_id, and passes tenant to downstream code.

- Pros: zero driver-side changes; natural model; compatible with existing Mongo tooling (mongo shell, Compass).
- Cons: `$db` is client-picked — spoofable unless the auth layer enforces per-command authorization. And any command that names a database the authenticated user doesn't own must be rejected at the wire layer.

**(ii) SCRAM-authenticated user identity.** Each connection authenticates as a specific tenant-scoped user. `connection.tenant = auth.tenant`, set once at connection-accept, immutable for the connection's life.

- Pros: harder to spoof (tenant is baked into auth); one connection = one tenant is a clear mental model.
- Cons: multi-tenant internal tooling needs connection pools per tenant; doesn't use `$db` at all, so databases within a tenant need a different sub-namespacing mechanism.

**Recommendation.** **Both, composed.** Tenant comes from SCRAM (authoritative). `$db` is a sub-namespace inside the tenant — useful for organizing collections within a tenant (`dev`, `staging`, `prod` as logical sub-namespaces). Command routing: `collection_ref = (scram.tenant, op_msg.$db, collection_name)`. This matches how managed Mongo services actually do multi-tenant Mongo in practice.

### 2.5 Migration sequencing (how to land the placement-unit work safely)

Not a design question — a scheduling one. Worth noting because the scope is broad enough to blow up if sequenced wrong.

The placement-unit change touches: KV key layout, routing code (`box_registry` → `placement_registry`), API signatures (drop `tenant_external_id` from requests), index storage (placement-local persistence), Mongo wire (tenant from auth). That is M13- or M14-scale work. A sensible order:

1. **Index persistence + incrementality (1.5)** — first, because it establishes persistent per-placement-scoped storage that the rest depends on.
2. **Placement unit introduction (1.1–1.4)** — key-layout change, routing rewrite, per-session lock, fanout-merge coordinator. Done in one coordinated milestone because the key layout cannot be half-migrated.
3. **Archive read completion (1.6)** — pulls the remaining `fetch_last_n` / Mongo `find` / locality-bound planner paths onto segment-aware reads. Share a milestone with (2) since many of the same files are touched.

Attempting to land these as three independent PRs on different timelines is a recipe for long-lived divergent branches.

---

## Part 3 — Recommended Next Steps

1. **Fold Part 1 into `docs/agent-db.md`.** Most of the doc is already compatible; targeted edits in §3.6 (tenant-implicit), §4.4 (placement-local indexes with fanout), §4.5 (placement vs box distinction). Keep §3.8's "agent conditional on multi-agent adoption" stance; do not promote agent to a resource.

2. **Draft an M13 spec** under `docs/superpowers/specs/` covering items 1.1–1.4 + 1.6, with item 1.5 as a prerequisite milestone (M12.5 or similar).

3. **Defer Part 2 items** pending either external workload signal (the sample-app benchmark may generate some) or specific product direction on multi-agent patterns.

4. **No behavior changes until a plan lands.** This note captures the decision posture; the actual code refactor begins after the spec is written and reviewed.
