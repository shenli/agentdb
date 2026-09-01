# AgentDB Design

AgentDB is a runtime database for AI agent platforms. It is designed around
agent-native runtime objects rather than a compatibility facade for an existing
database protocol.

## Product Thesis

Long-running agent platforms need a stable boundary between:

- **brains**: models, harnesses, planners, context builders, and agent loops
- **hands**: sandboxes, tools, MCP servers, browsers, filesystems, and external
  systems
- **sessions**: durable execution state that lets brains recover, inspect,
  resume, replay, fork, and audit work

AgentDB owns the session layer. It is not the model runtime, harness, sandbox,
or workflow engine. A harness should be able to crash and restart with a
`session_id`, fetch durable state, append the next event, and continue. A tool or
sandbox should be replaceable because its outputs and errors are recorded as
session events and artifacts rather than hidden inside a process.

Agent platforms repeatedly need the same primitives:

- ordered session history
- append-and-fetch for the agent turn loop
- artifact offload for large or binary payloads
- searchable scoped records
- skill registry and exact-version replay
- fork and replay for branchable runs
- subscriptions and watch cursors for observers and dashboards
- durable observability export for ClickHouse, ClickStack, JSONL, and archive
  pipelines
- policy, quota, retention, lifecycle, and garbage collection per tenant

AgentDB makes those primitives first-class so framework adapters are mostly
mapping code.

The session is not the model context window. Context windows are transient,
lossy, model-specific, and often transformed by compaction or trimming. The
session log is durable, ordered, queryable, and recoverable. Context builders and
framework adapters choose which slices, summaries, records, artifacts, and
skills to pass to a model, while AgentDB preserves the underlying execution
truth.

## Runtime Boundaries

AgentDB fits into a three-part agent platform stack:

- **Brains**: models, harnesses, planners, context builders, and agent loops.
- **Hands**: sandboxes, tools, MCP servers, browsers, filesystems, and external
  systems.
- **Sessions**: durable execution state that lets brains recover, inspect,
  resume, replay, fork, and audit work.

The matching storage boundaries are intentionally separate:

| Storage tier | Holds | Workload shape |
|---|---|---|
| Agent metadata / vault | agent definitions, identity, config, credentials | low-volume, read-mostly, trusted |
| AgentDB session store | ordered execution log, fork graph, artifact refs, records, lifecycle | append-heavy, per-session tail reads, replay/fork |
| Sandbox filesystem | working files, scratch state, tool output bytes | POSIX read/write, large blobs, snapshot-on-demand |

AgentDB is always below the credential trust line. Credentials live in the
control plane / vault and are injected at egress by a broker. AgentDB stores
opaque `credential_refs`, event history, artifacts refs, and runtime metadata;
it must not store secret values, mounted filesystem paths, host credentials, or
bearer-token object-store URLs.

Sandbox state follows the same separation. AgentDB stores layered
`SnapshotRef` metadata at committed session sequence numbers, while an external
sandbox service owns byte capture, copy-on-write fork, restore, mount, and byte
retention. Resume restores the referenced filesystem layers, then replays the
AgentDB session tail from the snapshot point. Fork is a pointer in AgentDB and a
copy-on-write relationship in the sandbox store.

Memory, observability, analytics, and lakehouse data are derived tiers. They
consume the durable session log through `watch` / `export_watch`; they do not
own runtime truth. If a derived projection is lost or built incorrectly, it
should be rebuildable from AgentDB history.

## Current Commitments

The current product shape is intentionally narrow:

- single-node operation must be excellent before distributed operation expands
- configuration is file-first through `agentdb.toml`
- Python and TypeScript are the maintained SDKs
- the native HTTP API is the public protocol and SDK substrate
- framework adapters should prove the runtime primitives, not reimplement them
- compatibility layers and extra SDKs are secondary until the core experience is
  stable
- object-store-backed runtime metadata is an optional evaluation path; the
  production write architecture should not depend on synchronous object-store
  writes for every append

## Implementation Status

This document is the current-state design contract. It intentionally separates
runtime features that are implemented from integration work that belongs outside
AgentDB or still needs product hardening.

| Area | Status | Notes |
|---|---|---|
| Runtime objects | Implemented | Tenant, agent, session, event, artifact, record, skill, policy, and tenant metadata exist in the Rust runtime and native API. |
| Native API and SDKs | Implemented | Tenant-scoped HTTP API plus maintained Python and TypeScript SDKs. Non-loopback binds require rotating installation bearer auth; scoped admin tokens protect admin routes. Loopback development may remain open, and tenant-scoped end-user authorization remains a hosted/control-plane concern. |
| Append loop | Implemented | Per-session serialized append, contiguous `seq`, append-and-fetch, idempotency, backpressure signals, and raw-secret write-path guards. |
| Fork and replay | Implemented | Pointer forks, fork-aware replay, bounded ancestor reads, replay scan stats, and explicit replay-prefix materialization over core/HTTP/SDK surfaces. |
| Changefeeds | Implemented | Session subscribe, cursor-based `watch`, fork-edge control items, commit-ordered durable `export_watch`, and managed export consumers with retained-cursor validation and claim/ack/rewind. |
| Artifacts | Implemented | Metadata and content bytes are stored separately with lifecycle-aware purge paths, a 128 MiB absolute object limit, local-filesystem fsync, failure cleanup, and standalone startup orphan reconciliation. The local sweep fully scans and materializes the blob listing; object-store listing/reconciliation is not yet implemented. |
| Records and retrieval | Implemented for scoped runtime retrieval | Durable base records, local/global indexes, commit-sequence visibility repair, record edges, and source refs exist. Memory extraction, ranking, summarization, and embedding spend are application/platform responsibilities. |
| Skills | Implemented | Versioned skill manifests and agent bindings are runtime objects; exact-version replay is supported through event refs and bindings. |
| Policy, lifecycle, and GC | Implemented | Quota/backpressure, warm/cold/archive lifecycle, archive restore, retention, legal hold, derivative-only artifact purge, and policy workers exist. |
| Credential boundary | Implemented defense in depth | Events carry opaque `credential_refs`; SDKs and runtime sanitize/reject common raw-secret payload shapes. Real secret resolution remains in the control plane/vault. |
| Sandbox snapshots | AgentDB refs implemented; sandbox service external | AgentDB stores `SnapshotRef` / `SnapshotLayer` metadata and resolves resume refs across fork chains. Snapshot bytes, copy-on-write fork, restore, and byte retention belong to the sandbox service. |
| Dashboard | Implemented baseline | Session-first dashboard assets are embedded in the binary; bearer input is page-memory-only and URL token parameters are scrubbed. Richer replay/fork/context/artifact inspection remains product work. |
| Packaging | Implemented baseline | Docker images and tagged release archives contain both `agentdb` and `agentdbctl`; Compose/Helm provide the required API bearer secret for their non-loopback listener. |
| Single-node operations | Partially implemented | In-memory and RocksDB modes, config-first server flow, authenticated Docker/Compose/Helm examples, meaningful live/ready probes, startup hydration guarded by a 512 MiB serialized-KV budget plus per-prefix row caps, backup manifest/checkpoint endpoint, and runbooks exist. Whole-state hydration, decoded-memory overhead, and some restore/upgrade automation remain limits. |
| Distributed/tablet operation | Evaluation/hardening | Metadata-backed routing, tablets, shard ranges, forwarding, tablet movement, snapshots, and Raft-backed write paths exist. This is not yet the default production deployment shape. |
| Encryption at rest / BYOK | Adapter seams only | KMS availability and key-ID seams plus keyed envelope framing exist. Default adapters are identity transforms, so production cryptography, full durable-path coverage, rotation, revocation, restore validation, and performance qualification remain. |
| Observability exporters | Partially implemented | Durable export primitives and managed consumers exist. First-party ClickHouse/ClickStack/JSONL/Parquet tooling is still maturing around those primitives. |

## Component Design Map

Use this map to avoid overloading one design doc with every subsystem detail.

| Question | Read |
|---|---|
| Why AgentDB exists, what it is not, and which workload it targets | [`product-thesis.md`](product-thesis.md) |
| Current design contract and implementation status | this document |
| Crate boundaries, request flow, API routes, and runtime implementation map | [`architecture.md`](architecture.md) |
| Hot storage engine, tablet-local LSM state, Raft commit-before-apply, split/merge, placement classes, and index placement | [`tablet-storage-engine.md`](tablet-storage-engine.md) |
| Cluster storage topology, cells, AZ placement, tenant blast radius, failover, scheduler behavior, and cluster validation | [`cluster-storage-topology.md`](cluster-storage-topology.md) |
| Encryption at rest, BYOK, key hierarchy, rotation, revocation, storage coverage, and implementation plan | [`encryption-at-rest.md`](encryption-at-rest.md) |
| Warm/cold/archive/derivative-only lifecycle, sealed segments, archive manifests, artifact byte lifecycle, and restore semantics | [`lifecycle-storage.md`](lifecycle-storage.md) |
| Observability export ledger, managed consumers, ClickHouse/ClickStack path, sink contracts, and backfill | [`observability-export.md`](observability-export.md) |

## Active Interfaces

The primary developer interface is the SDK surface. The supported public
protocol underneath it is the native HTTP API mounted under `/api/...`. Routes
are tenant-scoped and use typed opaque IDs because tenant is the isolation,
policy, quota, lifecycle, storage, routing, and admin boundary. SDKs should
treat tenant selection as an authentication and authorization concern, then let
normal application code use workspace, agent, and session aliases rather than
threading `tenant_id` through the agent loop.

HTTP is the right canonical API for the current product shape: agent
frameworks, SDKs, dashboards, examples, and local operators can use it without
custom database drivers, it handles JSON/event payloads naturally, and SSE gives
a simple subscription path.

AgentDB should not pretend HTTP is a universal database wire protocol. If the
data plane later needs lower overhead, the expected evolution is to keep HTTP as
the public and operational surface while adding a binary SDK transport or
internal node protocol for hot paths.

The current workspace intentionally does not maintain:

- a Go SDK
- tenantless routes from earlier prototypes
- a version-gating server flag
- a MongoDB wire-protocol compatibility listener
- a SlateDB `KvStore` backend

## Identity

IDs are public opaque strings:

| Kind | Prefix |
|---|---|
| Tenant | `ten_` |
| Agent | `agt_` |
| Session | `ses_` |
| Event | `evt_` |
| Artifact | `art_` |
| Record | `rec_` |
| Skill | `skl_` |
| Policy | `pol_` |

Clients must not parse meaning from the suffix. The prefix is for validation,
debugging, and route-shape clarity.

SDKs may let developers provide domain-friendly external names where that helps
ergonomics, but the server-assigned opaque IDs remain the durable identifiers.
Tenant IDs are the exception developers should usually not see: they are
infrastructure identifiers for routing, policy, quota, storage, and admin
surfaces. Hosted deployments should bind the current tenant/workspace from
authentication and only expose tenant IDs as low-level escape hatches.

## Data Model

- **Tenant**: isolation, policy, retention, quota, and lifecycle boundary.
- **Agent**: actor configuration inside one tenant.
- **Session**: ordered execution log with lifecycle state and optional fork
  parent.
- **Event**: session-local sequence, typed kind, JSON payload, artifact refs,
  source refs, schema version, idempotency key, and policy version.
- **Artifact**: metadata plus content-backend bytes.
- **Record**: searchable memory or observation with namespace, scope, metadata,
  tags, source refs, and indexing policy.
- **Skill**: manifest/version plus optional artifact refs; bindings attach
  skills to agents.
- **Policy**: tenant and agent behavior for quota, retention, lifecycle,
  context budgets, legal hold, and garbage collection.

## Runtime Semantics

Append is serialized per session and produces contiguous session-local `seq`
values. `append-and-fetch` writes one event and returns a recent-event slice in
the same runtime operation. Idempotency is part of mutating runtime calls so
retrying clients do not duplicate events or registrations.

Fork creates a child session with fresh child-local sequence numbers and replay
visibility over parent events through the fork point. Replay is explicit and
fork-aware. Deep fork chains can be collapsed by materializing the non-leaf
replay prefix into durable content and replaying that prefix plus the leaf tail.

Subscribe currently emits replay-style SSE frames from a requested sequence.
Tenant, session, and agent watch returns changefeed items with cursors for
higher-level consumers. Fork creation is emitted as a first-class fork-edge
control item rather than by re-emitting ancestor events.

Fork-aware feeds are feeds over a DAG of per-session sub-streams. Each physical
event is emitted once on the session where it was written. A fork emits a
`fork_edge` control item carrying `(parent_id, fork_point_seq, child_id)` so
consumers can discover and follow child streams without duplicating parent
history. Opaque frontier cursors track per-session next-sequence positions.

Observability export is the durable sink boundary. Export endpoints serve
tenant-scoped envelopes for events and runtime metadata, support backfill plus
tail, and let named managed consumers claim leases and ack only after an
external sink accepts a batch. The first supported sink is ClickHouse over HTTP,
with native AgentDB tables and optional OTel-shaped ClickStack views. JSONL and
Parquet paths provide archive/lakehouse handoff without adding sink-specific
semantics to the runtime. This boundary currently requires a single-tablet
runtime; snapshot-routed clusters reject export until a cross-tablet merge
frontier exists.

Record indexes follow local/global semantics. A search with `sharding_key` uses
the owner-local index and is the strong read-after-write boundary. A search
without `sharding_key` uses the global index, which is GSI-like and eventually
consistent. Record writes return a tenant-scoped commit sequence; callers can
pass `min_visible_record_commit_seq`, and SDKs propagate the latest sequence
automatically, so same-client reads repair from committed base records when an
async index is behind.

Placement uses virtual shards and physical tablets. Shards are lightweight
routing slots, not independently managed storage regions. A tablet owns one or
more shard ranges and is the unit that should eventually carry ownership,
replication, local LSM state, snapshots, compaction, and movement. Default shard
selection must include a tenant-internal routing key, such as `session_id` for
session-scoped requests or `sharding_key` for record/search requests, rather
than hashing only `tenant_id`. This lets one tenant spread across multiple
tablets without exposing placement to SDK callers.

This is deliberately similar to DynamoDB's local/global secondary-index
behavior: locality gives stronger read-after-write semantics, while global
discovery is an asynchronous derived view. The base record write is the durable
source of truth; index lag is repaired or observed rather than treated as a
failed write.

The canonical harness flow is:

```text
wake(session_id)
get events, records, artifacts, skills, and policy hints
build model context in the harness
call the model
execute requested tools or sandboxes as external hands
append tool calls, tool results, model outputs, approvals, and artifacts
repeat or sleep
```

Sandbox resume uses a related two-store flow:

```text
resolve newest non-expired SnapshotRef for the session/fork point
sandbox service restores the referenced filesystem layers
AgentDB replays the session tail after the snapshot point
append recovered agent status and continue
```

AgentDB stores the snapshot reference and replay truth. The sandbox service owns
filesystem bytes, copy-on-write fork, mount, restore, and snapshot garbage
collection.

The canonical observability flow is:

```text
runtime mutation commits
append export envelope to durable ledger
connector claims a managed consumer lease
connector backfills or tails export envelopes
sink accepts batch: ClickHouse, ClickStack view tables, JSONL, or Parquet
connector acks the durable export cursor
dashboards and alerting query the external sink
```

## Configuration And Operation

AgentDB is config-file first:

```bash
agentdb init-config
agentdb doctor --config agentdb.toml
agentdb serve --config agentdb.toml
```

`agentdb.toml` is the durable operational contract. CLI flags are explicit
overrides for local development, scripts, and tests. Server runtime behavior
should not depend on a large set of environment variables.

Docker, Compose, and Helm deployments should mount or generate a config file
instead of encoding normal runtime settings as environment variables.

## Policy, Lifecycle, And GC

Policy is resolved per tenant with agent overrides. It covers quota admission,
backpressure headers, lifecycle transitions, context budgets, retention, legal
hold, and GC scheduling. Deletes are soft first; GC later performs physical
purge in a policy-controlled order.

## Storage

Runtime metadata is stored through the existing KV abstraction. Local persistent
mode uses RocksDB. Artifact content uses a separate content backend so metadata
and blob bytes can be placed, retained, and purged independently.

Observability export deliberately uses committed runtime truth, but external
sink failure must not block ordinary appends. Connectors replay from durable
export cursors and apply sink-specific schema, privacy, retry, and dashboard
logic outside the hot runtime path.

An object-store-backed `KvStore` (SlateDB) was evaluated to validate the
abstraction and then retired. Object storage is not the answer for hot write
durability or multi-node safety. A clustered AgentDB needs a replicated local
log/tablet commit path, such as Raft-style quorum replication, before
acknowledging writes; object storage belongs behind asynchronous sealing,
archival, and artifact flows.

## SDKs And Adapters

Python and TypeScript are the maintained SDKs. They should make the common
agent path feel natural: create or open a workspace, create or open an agent,
create or open a session, append events, fetch context, store artifacts, search
records, and subscribe to progress.

The SDKs should keep tenant scope out of normal app code. Authenticated
deployments should derive it from user-info metadata; local development can use
explicit workspace aliases. Tenant-scoped `Client` methods remain available for
tests, admin tooling, and low-level integrations.

Framework adapters validate that the native primitives cover real persistence
needs without recreating ordering, forking, idempotency, artifact, search, and
replay behavior in adapter code.
