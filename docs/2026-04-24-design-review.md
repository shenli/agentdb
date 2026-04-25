# AgentDB Design Review

Date: 2026-04-24

This document records a design review of the current repository state across:

- `README.md`
- `docs/ARCHITECTURE.md`
- `docs/agent-db.md`
- `docs/agent-db-v2.md`
- the current Rust implementation

The main conclusion is that the repository is directionally strong, but it is currently carrying three partially-overlapping designs at once:

1. The original tenant-hash, box-centric prototype.
2. An intermediate implementation with shard overrides and multi-node forwarding.
3. The revised v2 design centered on implicit tenant identity, globally-routed sessions, and tablet-local execution.

That overlap is now the main design risk. The problem is not lack of features; it is that several core contracts are inconsistent between docs and code.

## Highest-Priority Findings

### 1. Auth and identity model are inconsistent

The v2 design says:

- tenant is implicit and auth-bound
- tenant should not appear on the client-visible surface
- session should be the main routing identity

The implementation still exposes tenant explicitly on the main write path:

- `POST /api/tenants/{tenant_id}/sessions/{session_id}/append_and_fetch`
- `ForkRequest` still carries `tenant_id`

Worse, the `fork` route is mounted outside the tenant-auth subtree, so the current routing structure does not fully enforce the intended auth boundary.

This is the most important design issue because it is a contract problem, not just an API style issue. If tenant identity is supposed to be auth-bound, the API surface and middleware layout need to make that true everywhere.

### 2. Cross-tablet reads are not yet semantically correct

The v2 design explicitly moves from "tenant hashes to one box" to "tenant fanout across many shards/tablets". That means tenant-wide reads must either:

- fan out across tablets and merge results, or
- clearly declare a narrower scope

The current implementation still anchors general Mongo `find` and `aggregate` paths on one tenant-routed box unless the request is pinned to a single session. Once shard overrides or tablet migration are used, unpinned reads become partial.

This is a correctness gap. The system can now move writes away from the tenant-hash box, but the corresponding read paths do not yet follow that same topology.

### 3. Archive semantics are still inconsistent for indexed retrieval

`docs/ARCHITECTURE.md` states that archive now affects latency, not semantics. That is true for:

- `fetch_last_n_merged`
- locality-bound session reads
- Mongo `find` on archived sessions

It is not yet true for general indexed retrieval. The index build and replay code skips `Tier::Archive`, which means unhinted text/vector retrieval can still lose archived data.

This is an important design mismatch because the docs currently promise a stronger semantic contract than the full implementation provides.

### 4. Write locking is still per-tenant, not per-session

The v2 design correctly identifies per-session locking as the needed concurrency boundary. The current runtime still serializes writes by tenant within a box/tablet via `tenant_write_lock`.

For agent workloads this is a structural bottleneck:

- one tenant can legitimately drive many concurrent sessions
- per-tenant locking destroys concurrency even when session-level ordering would be sufficient

This means one of the most important v2 scaling claims is not yet reflected in the implementation.

### 5. Session identity is still tenant-scoped in storage

The v2 design says sessions should be globally unique and self-contained. The implementation still stores and resolves sessions by:

- `box_id`
- `tenant_id`
- external session id

That keeps tenant as a load-bearing part of session identity and routing. As long as this remains true, the system has not really moved to the v2 session-centric model; it is still an extended tenant-centric model.

This is not just naming. It affects routing, migration, auth, and how much later refactoring will be required.

## Secondary Findings

### 6. Tablet abstraction is only partially real

The code now has:

- shard assignment
- tablet rows in the control plane
- leases
- cross-node forwarding
- dynamic tablet creation

But the runtime still largely treats local boxes as the operative data structure, with `tablet_id == box_id` as a frequent simplifying assumption. That is acceptable for a prototype, but it means the design should describe this as an approximation, not as a completed tablet refactor.

### 7. Metadata story is split between strong direction and transitional storage

The v2 design says per-tablet metadata should be dogfooded in AgentDB storage, while the current code still keeps core session and tenant metadata in the local KV layout keyed by box and tenant. This is reasonable for the prototype, but it reinforces that the next step should reduce architectural overlap rather than add more features on top of it.

### 8. The design docs are ahead of the implementation in a few key places

That is acceptable, but the repo has now crossed the point where the gaps are load-bearing. Several sections in `docs/agent-db-v2.md` read like architectural commitment, while the code remains in transitional mode. The next step should either:

- close those specific gaps, or
- explicitly label them as planned rather than current design commitments

## Suggestions

### Suggestion 1. Choose one identity contract and make the whole surface obey it

Do not keep both of these models alive:

- tenant-explicit REST and tenant-keyed storage
- tenant-implicit auth-bound routing and globally unique sessions

The cleaner path is to move forward with the v2 contract, but only if the next milestone includes the supporting storage and read-path work.

### Suggestion 2. Prioritize semantic correctness over additional features

The next milestone should not primarily add new admin endpoints or more scheduler behavior. It should make sure that once a session is moved, every relevant write and read path still behaves correctly.

That means:

- one canonical session locator
- one canonical auth-bound tenant identity flow
- one canonical read fanout story

### Suggestion 3. Treat session lookup as the central primitive

The v2 design already points toward a `session_index/{session_id} -> tablet_id` style model. That should become the architectural center of the next step. Once that exists, several current inconsistencies simplify naturally:

- `fork` no longer needs tenant in the request
- routing no longer starts from tenant hash
- moved sessions remain reachable without scanning tenant-local structures

### Suggestion 4. Keep the prototype approximation explicit

Where the code still uses "box as local tablet approximation", document that directly. The repo is easier to reason about when it is honest about transitional structures.

## Overall Assessment

I think this is a useful project for AI agents, but only if it stays disciplined about what it is trying to be.

The core thesis is strong. Agent workloads do differ from classic OLTP in ways that matter operationally:

- session-shaped state
- append-heavy writes
- fork and snapshot semantics
- hybrid retrieval over operational data
- aggressive warm/cold/archive lifecycle management

Those are real problems, not invented ones. If the target is infrastructure for long-lived, tool-using, memory-heavy agents, there is a genuine gap between "just use Postgres" and "build a full distributed database from scratch". This project is aiming at that gap, and that is a worthwhile place to work.

What is especially valuable is that the project is not just trying to be:

- a vector database
- a log store
- a Mongo clone

Instead, it is trying to define an operational model for agents with a coherent set of primitives:

- `append_and_fetch`
- session fork
- lifecycle-aware storage
- retrieval integrated with operational state
- multi-tenant isolation as a first-class concern

That is the right ambition level.

The main risk is not that the idea is wrong. The main risk is that the repo is currently trying to be several things at once:

- a prototype
- a new database architecture
- a Mongo-compatible system
- a distributed control plane
- an agent memory backend

Those can fit together, but only if the central contract is very clear. Right now that contract is split between the original prototype design, the transitional implementation, and the v2 design target.

My current judgment is:

1. It is already useful as an agent-infrastructure experiment.
2. It could become useful as a real backend for agent runtimes if the identity, auth, routing, and read-semantics model is cleaned up.
3. It is not yet persuasive as a general-purpose distributed database, and it should not be positioned that way.

If I had to summarize the project in one sentence:

This is a credible and worthwhile attempt to make the session, rather than the row or document, the core operational unit for AI agents.

That is the right direction. The next step should focus on making that contract fully true in the implementation.

## Recommended Scope Boundary

The next step should focus on the following contract:

"A session is globally addressable, tenant is auth-bound, writes route by session, and reads are semantically correct after shard/tablet movement."

If that contract is implemented cleanly, the rest of the v2 design becomes much easier to land incrementally.

## Non-Goals For The Next Step

The following should not be the main goal of the next step:

- broader product surface
- more admin APIs
- more scheduler heuristics
- deeper dashboard work
- performance tuning before semantic cleanup

Those are useful later, but they should come after the identity/routing/read model is coherent.
