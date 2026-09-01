# AgentDB Product Simplification Proposal

**Status:** Proposal  
**Scope:** Product boundary, public API, packaging, and sequencing  
**Does not change:** Current runtime behavior, compatibility commitments, or
implementation status

## Decision Summary

AgentDB should focus its first supported product on one promise:

> AgentDB is a durable, replayable session log for AI agents.

The product should be evaluated against one recovery loop:

```text
append -> crash -> resume -> replay or fork
```

The first release should make this loop durable, understandable, observable,
and easy to operate on a single node. Features that do not materially improve
this loop should move out of the core product surface until customer evidence
justifies their maintenance and operational cost.

This proposal narrows the public model to four concepts, consolidates the
application API around six intents, and separates core, experimental, and
internal functionality.

## Why Simplify Now

The external positioning is focused: AgentDB is the durable session log for an
agent run. The implemented surface is substantially broader:

- nine native runtime object families: tenants, agents, sessions, events,
  artifacts, records, skills, policies, and record graph edges;
- 101 registered HTTP paths, including 45 administrative paths and 46
  tenant-runtime paths;
- separate subscribe, watch, durable export, backfill, and managed-consumer
  concepts;
- records, BM25/vector retrieval, graph edges, context assembly, skills,
  lifecycle tiers, sandbox snapshot references, and artifact management;
- a dashboard, multiple adapter proofs, Helm examples, encryption/KMS seams,
  and a distributed tablet control plane; and
- a core runtime and API route implementation of approximately 14,000 and
  8,600 lines respectively as of September 2026.

These numbers are not inherently defects. They indicate that each new user,
SDK, release, and operator workflow must understand and maintain several
product layers before the primary value proposition has been established.
AgentDB is still pre-release, so this is the least expensive point at which to
reduce public commitments.

## Proposed Product Boundary

### Core

The supported core should contain only the capabilities required to record,
recover, inspect, and branch agent execution:

- durable single-node storage;
- workspace isolation and authentication;
- sessions and ordered, append-only events;
- append-and-fetch with durable idempotency;
- recent reads and full replay;
- pointer-based forks;
- artifacts and event-to-artifact references;
- one cursor-based changefeed;
- bounded retention and cleanup with safe defaults;
- health, metrics, backup, restore, and crash-recovery tooling; and
- maintained Python and TypeScript SDKs.

### Labs Or Integrations

The following capabilities may remain available for evaluation, but should not
be required concepts in the quickstart, stable SDK, or core compatibility
contract:

- records and scoped search;
- text/vector indexes and record graph edges;
- versioned skill registry and bindings;
- server-side context assembly;
- advanced lifecycle transitions and replay-prefix controls;
- sandbox snapshot metadata;
- framework-specific adapters; and
- rich dashboard views.

Where practical, these should consume or project the session log rather than
expand the synchronous append path.

### Deferred

The following work should wait for a named design partner, a measured capacity
limit, or a supported hosted-product requirement:

- production distributed tablets and their public control-plane API;
- additional SDK languages or a binary application protocol;
- managed export consumers and multiple sink-specific products;
- customer-managed key catalogs, rotation, and revocation workflows; and
- automated multi-tier storage orchestration beyond bounded retention.

Basic encryption, authentication, backup, and retention remain core reliability
and security requirements. The proposal defers broad enterprise management
surfaces, not safe defaults.

## Simplified Public Model

The primary documentation and SDKs should teach four nouns.

| Concept | Meaning |
|---|---|
| Workspace | Isolation, authentication, quota, and retention boundary. |
| Session | One durable agent execution or branch. |
| Event | An ordered fact about what happened during the execution. |
| Artifact | Large or binary data referenced by an event. |

The following concepts become attributes, references, projections, or internal
mechanisms:

- Agent identity is session and event metadata unless independent lifecycle
  management is proven necessary.
- A fork is a relationship between sessions, not another top-level object.
- A skill is an artifact or external reference recorded by an event.
- A record is a projection derived from events, not required runtime truth.
- A context pack is an SDK or harness decision whose provenance can be written
  as an event.
- Lifecycle and policy are workspace configuration with automatic defaults,
  not required per-session application workflows.

This does not require deleting every existing representation immediately. It
changes which representations are part of the supported product vocabulary.

## Simplified Application API

The stable application API should be organized around six intents.

1. **Open a session.** Idempotently create or resolve the workspace, agent
   identity, and session in one call.
2. **Append an event.** Durably append one event and optionally return the
   recent tail in the same call.
3. **Read history.** Fetch a recent slice or replay the complete fork-aware
   event view.
4. **Fork a session.** Create a child session at an explicit sequence.
5. **Store or fetch an artifact.** Write bytes once and reference them from
   events.
6. **Read changes.** Consume committed changes from an opaque cursor.

A target public surface would be approximately 8 to 12 application routes and
no more than about 12 primary SDK methods. Administrative routes may still
exist internally, but cluster and maintenance implementation details should not
be presented as application APIs.

### Changefeed Consolidation

Session subscribe, tenant watch, durable export watch, backfill, and managed
consumer state currently express overlapping change-consumption needs. The
public primitive should be one cursor-based changefeed with:

- explicit workspace and optional session scope;
- commit-ordered items;
- opaque, durable cursors;
- bounded page size and long-poll behavior;
- cursor-expiration and recovery semantics; and
- enough metadata for downstream indexing, analytics, and dashboards.

Sink-specific checkpointing, claims, retries, and schema conversion belong in
consumer libraries or operators unless AgentDB is sold as a managed pipeline
service.

### One-Call Session Opening

First use should not require a tenant-create, agent-create, session-create
choreography. The SDK should expose an idempotent operation similar to:

```python
session = db.open_session(
    workspace="acme",
    agent="sales-agent",
    session="ticket-42",
)
```

The returned handle should be immediately usable for append, read, fork, and
artifact operations. Lower-level resource administration can remain available
to operators without defining the beginner experience.

## Product And Demo Strategy

AgentDB and CodeMate currently represent two plausible products:

- AgentDB is infrastructure for agent platforms.
- CodeMate is a local engineering-review product for developers.

The recommended strategy is to keep AgentDB as the product and use CodeMate as
the canonical dogfood application. CodeMate should demonstrate the core loop
using real coding-agent sessions: import events and artifacts, inspect a durable
timeline, recover after interruption, and replay or fork a review window.

CodeMate should not create an independent platform roadmap until customer
discovery shows that developers want the review product but do not want a
runtime database. If that evidence emerges, the relationship should reverse:
CodeMate becomes the product and AgentDB becomes its internal engine. Marketing
and maintaining both as independent products would dilute the initial wedge.

## Supported Deployment Shape

The first supported release should have one easy-to-explain deployment:

- one AgentDB binary;
- RocksDB metadata and local durable artifact storage;
- durable writes enabled by default;
- file-based configuration;
- Docker as the primary packaged example;
- single-node production with published limits;
- health, metrics, backup, restore, and upgrade workflows; and
- no required background service beyond AgentDB itself.

Distributed routing, tablets, movement, and Raft administration should be
feature-gated and labeled evaluation-only until the single-node runtime no
longer meets a measured tenant, session, event-volume, artifact-size, or
availability requirement.

Helm and cluster documentation may remain for evaluation, but should not imply
the same support level as the single-node product.

## Stability Labels

Every endpoint, SDK method, runtime object, configuration field, and operator
workflow should have one of three labels:

- **Core:** documented in the main path, covered by maintained SDKs, tested for
  compatibility, and supported operationally.
- **Labs:** callable and testable, but explicitly unstable and absent from the
  beginner path and compatibility promise.
- **Internal:** implementation or operator machinery with no application-level
  compatibility promise.

The label should be machine-readable where possible and enforced in the docs,
generated API specification, SDK generation or review, release checklist, and
deprecation policy. Labeling without reducing SDK, documentation, testing, and
support obligations does not simplify the product.

## Release Gates

A simplified first release should not be called ready until it meets these
measurable gates.

### Developer Experience

- A new user reaches the first durable append in under 5 minutes from a clean
  checkout or container environment.
- The first append requires no more than three concepts and one resource-opening
  call.
- The stable SDK presents no more than about 12 primary methods.
- The stable application API presents no more than about 12 routes.
- One example demonstrates append, process crash, resume, artifact recovery,
  replay, and fork without framework-specific code.

### Reliability

- A SIGKILL recovery test proves that every acknowledged append survives.
- Idempotent retry is tested across a lost response and server restart.
- Artifact metadata and bytes have a tested failure-cleanup and recovery path.
- Backup restoration is tested into a clean installation.
- Retention and cleanup have byte and item bounds, observable progress, and
  documented failure behavior.

### Capacity And Operations

- Startup time, steady-state memory, append throughput, and recent-tail latency
  are published for at least 1 million and 10 million events.
- Tests state event-size, artifact-size, active-session, total-session, and
  concurrency assumptions.
- Single-node limits are explicit rather than hidden behind a distributed
  roadmap.
- The deployment requires no mandatory service beyond the AgentDB process and
  its durable volumes.

## Proposed Sequencing

### Phase 1: Freeze And Classify

1. Freeze new distributed, search, lifecycle, compatibility, and adapter
   surface work except correctness and security fixes.
2. Inventory every API path, SDK method, runtime object, config field, and
   runbook as Core, Labs, or Internal.
3. Publish the four-concept model and six-intent API as the proposed stable
   contract.
4. Identify existing customers or examples that would break under the cut.

### Phase 2: Create The Narrow Path

1. Add one-call session opening to both maintained SDKs.
2. Define one durable changefeed and map existing watchers/exporters onto it.
3. Remove Labs concepts from the quickstart and default documentation path.
4. Hide internal cluster administration from the application API reference.
5. Make the core durability and recovery example the release smoke test.

### Phase 3: Reduce Maintenance Surface

1. Deprecate duplicate APIs and SDK methods with a dated removal plan.
2. Move optional projections and adapters into separately versioned packages
   where that materially reduces core coupling.
3. Delete unused implementations after one deprecation window; because the
   product is pre-release, keep that window short.
4. Re-measure route count, SDK method count, binary size, compile time, test
   time, startup memory, and operator documentation volume.

### Phase 4: Validate The Wedge

1. Use CodeMate or one similarly demanding application to exercise the core
   contract end to end.
2. Run at least five integration studies with agent-platform developers.
3. Record time to first append, concepts that required explanation, recovery
   failures, and requested missing capabilities.
4. Promote a Labs capability only when multiple users require it and the core
   event/artifact model cannot serve the need through an integration.

## Non-Goals Of This Proposal

This proposal does not argue that search, skills, context, lifecycle, cluster
storage, or encryption management are intrinsically unimportant. It argues
that supporting them all as first-class product concepts before the durable
session-log wedge is validated increases cognitive and operational cost.

It also does not authorize immediate deletion. Removal requires an inventory,
dependency analysis, compatibility decision, migration or deprecation plan,
and documentation update.

## Decision Questions

Before adopting this proposal, the team should answer:

1. Is AgentDB the product, with CodeMate as dogfood, or is CodeMate the product
   with AgentDB as an internal engine?
2. Are records/search required by an existing design partner, or can they be a
   changefeed-driven projection?
3. Are skills independently managed runtime objects, or versioned artifacts
   referenced by events?
4. Which availability or capacity requirement currently justifies exposing the
   distributed tablet control plane?
5. Which enterprise customer currently requires AgentDB-owned BYOK rotation
   rather than ordinary encrypted storage?
6. Can subscribe, watch, and export be expressed by one cursor contract without
   losing a demonstrated use case?

If the answers do not identify a concrete user, workload, limit, or reliability
requirement, the capability should remain outside the first stable product.

