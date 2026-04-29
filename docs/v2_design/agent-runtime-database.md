# AgentDB V2: Agent Runtime Database

Status: draft

Date: 2026-04-28

## 1. Positioning

AgentDB V2 is not a general-purpose document store and not a replacement for an
application's OLTP database.

AgentDB V2 is:

> The runtime database for AI Agents Platforms.

The target user is not a solo agent developer. For a single agent, files,
SQLite, Postgres, MongoDB, Redis, or a vector store are usually good enough.
AgentDB becomes valuable when an AI Agents Platform operates many
customer-scoped agents and needs runtime semantics that generic storage systems
do not provide as a single coherent product.

Examples:

- A CRM-oriented AI Agents Platform running sales, marketing, enrichment, and
  follow-up agents for every customer.
- A support AI Agents Platform running customer-service agents for every
  workspace.
- A coding AI Agents Platform running development agents per repository and
  user.
- A research AI Agents Platform running analyst agents for teams and projects.
- A vertical AI Agents Platform embedding autonomous agents into each customer's
  workflow.

These platforms still need a product database for business entities:

```text
CRM DB:
  contacts, accounts, opportunities, campaigns, billing, permissions
```

AgentDB stores agent execution truth:

```text
AgentDB:
  what the agent did,
  what context it saw,
  which tools and skills it used,
  what artifacts it produced,
  what it learned,
  why it made a decision,
  how to replay, resume, fork, archive, and audit that execution.
```

Short version:

```text
Your product database stores customer truth.
AgentDB stores agent execution truth.
```

## 2. Why Existing Storage Is Not Enough

Existing tools can store the bytes:

- Postgres, MongoDB, DynamoDB store structured records.
- S3, R2, MinIO store large artifacts.
- Qdrant, Pinecone, pgvector, Elasticsearch store retrieval indexes.
- Redis, NATS, Kafka provide pub/sub and queues.

The problem is not insertion of JSON.

The problem is that AI Agents Platforms need runtime semantics:

- tenant-aware admission and quotas,
- runaway-agent containment,
- ordered execution history,
- replay and resume,
- fork lineage,
- large artifact management,
- scoped search for memory use cases,
- skill version pinning,
- lifecycle and archive policies,
- durable live subscriptions,
- cost/metering hooks,
- retention and compliance controls.

If these are only application conventions, every AI Agents Platform rebuilds them,
and every integration can bypass or corrupt them.

AgentDB's job is to make these semantics first-class and centrally enforced.

## 3. Target Use Cases

AgentDB V2 is designed for AI Agents Platforms where execution history is part
of the live runtime, not only a log humans inspect later.

### 3.1 Long-running agent execution

Examples: coding agents, research agents, incident-response agents, browser
agents, workflow agents.

Runtime requirements:

- runs last minutes, hours, or days,
- runs may pause for humans or external systems,
- process/model/tool failures must resume cleanly,
- every action must be replayable for debugging and audit,
- large tool outputs and generated files must be attached without bloating the
  hot event log.

Required primitives:

- session/run log,
- ordered events,
- append-and-fetch,
- replay,
- resume,
- artifact refs,
- lifecycle state,
- durable subscribe.

### 3.2 Multi-tenant agent fleets

Examples: CRM-oriented platforms, support platforms, coding platforms, research
platforms, vertical workflow platforms.

Runtime requirements:

- many customer/workspace-scoped agents,
- most agents idle most of the time,
- some agents run on schedules or wake from events,
- some agents become runaway loops,
- tenants need isolation, quota, retention, and billing attribution,
- old runs must become cheap without losing replay/audit semantics.

Required primitives:

- tenant boundary,
- agent registry,
- quota/admission policy,
- lifecycle/archive policy,
- metering hooks,
- hot-session detection,
- tenant-scoped indexes,
- export/delete controls.

### 3.3 Human-in-the-loop agent workflows

Examples: approval before sending customer email, escalation to human support,
code review before patch application, finance/legal review before action.

Runtime requirements:

- pause and resume,
- durable approval events,
- exact context shown to the human,
- policy decisions recorded,
- live UI updates,
- audit trail for later compliance review.

Required primitives:

- approval/policy events,
- subscription from seq,
- session state,
- replayable context,
- skill/tool version pinning,
- artifact integrity.

### 3.4 Debugging, evaluation, and compliance

Examples: "why did this agent email this prospect?", "which tool version made
this change?", "what context was visible before this action?", "which searchable
records reference this run?"

Runtime requirements:

- exact execution timeline,
- event/artifact/source references,
- skill/model/policy version history,
- branch comparison,
- replay bundles,
- tenant-scoped audit export.

Required primitives:

- event IDs and per-session seq,
- artifact IDs and hashes,
- searchable record source refs,
- skill version refs,
- policy version refs,
- fork lineage,
- session export.

### 3.5 Self-hosted and privacy-sensitive platforms

Examples: platforms handling source code, customer support records, financial
documents, healthcare/legal data, or proprietary internal operations.

Runtime requirements:

- deployable without sending prompts/tool outputs to a third party,
- data residency,
- retention/delete policies,
- raw artifact redaction or deletion,
- audit logs,
- backup/restore.

Required primitives:

- local/object artifact store,
- retention policy,
- derivative-only mode,
- tenant-scoped export/delete,
- audit log,
- encryption and key-management hooks.

### 3.6 Open-source adoption path

The open-source entry point should not assume individual developers will build
complex agents directly on AgentDB. Most will adopt popular agents or frameworks
first, then consider AgentDB if it improves durability, replay, debugging, or
deployment.

The single-machine distribution should therefore be useful as a local/runtime
backend for existing tools:

- framework session/checkpoint storage,
- agent run history and replay,
- artifact capture for tool outputs, browser screenshots, generated files, and
  code patches,
- scoped searchable records for framework memory use cases,
- skill/version usage records,
- local dashboard for inspecting runs,
- import/export for framework traces and replay bundles.

Priority integrations should follow where agent developers already are:

- LangGraph/LangChain checkpoint and store adapters,
- OpenAI Agents SDK session adapter,
- Vercel AI SDK persistence template,
- MCP server exposing AgentDB sessions, artifacts, search, and replay,
- adapters for popular open-source agents such as OpenClaw/Hermes when their
  persistence and trace boundaries are stable,
- adapters for emerging multi-agent frameworks such as pi-mono if they become a
  common platform layer.

The OSS message should be:

```text
Keep using your agent framework. Use AgentDB as the durable runtime backend for
runs, replay, artifacts, scoped search, and local inspection.
```

This gives individual developers a reason to try AgentDB without requiring them
to design a complex agent architecture themselves. It also creates a migration
path from local single-machine use to production AI Agents Platforms.

### 3.7 Non-target use cases

AgentDB V2 is not optimized for:

- single-agent hobby apps,
- simple chatbots,
- small RAG apps,
- generic document storage,
- application OLTP records,
- observability-only trace storage,
- memory-intelligence products,
- workflow orchestration engines.

AgentDB can integrate with these systems, but it should not position itself as a
replacement for them.

## 4. Architectural Boundary

AgentDB V2 separates the agent runtime semantics from the lower-level storage
engine capabilities:

```text
User / SDK / API
  Agent, Session, Searchable Record, Artifact, Skill, Policy

AgentDB Runtime Core
  agent-native semantics and invariants

AgentDB Storage Engine
  durable ordered storage, metadata records, artifacts, indexes,
  snapshots, lifecycle primitives, changefeeds
```

Users should consume agent primitives. They should not build them in their
application.

The storage engine should not need to understand every high-level agent concept.
It must instead provide the low-level capabilities the runtime core needs to
enforce agent semantics correctly and efficiently.

## 5. First-Class Runtime Objects

AgentDB V2 exposes these first-class objects in the public API and runtime core.

| Object | Purpose | Core Operations |
|---|---|---|
| Tenant | Customer/workspace boundary for isolation, quota, retention, billing | create, update policy, quota, list agents/sessions |
| Agent | Runtime actor with metadata, skills, search namespaces, active sessions | register, configure, bind skills, write/search records |
| Session / Run | Unit of execution history, replay, resume, fork, lifecycle | append, fetch, scan, replay, fork, subscribe, archive |
| Event | Ordered execution record inside a session | append, fetch, scan, index, subscribe |
| Artifact | Large raw payload attached to sessions/events/search records/skills | put, get, verify, tier, delete/archive |
| Searchable Record | Framework/application-provided record for memory, knowledge, or recall use cases | write, search, update, expire, attach source refs |
| Skill | Versioned reusable capability/instruction/tool package | register, version, bind, resolve, audit use |
| Policy | Quota, lifecycle, retention, indexing, approval, access rules | define, attach, evaluate, audit |
| Subscription | Durable or ephemeral stream for runtime progress/wakeups | subscribe, replay from seq, channel publish |

These objects are runtime concepts, not necessarily separate physical storage
engines.

## 6. Stable Core vs Extensible Semantics

Some agent concepts will change. AgentDB should distinguish durable primitives
from evolving ones.

Stable core:

- tenant,
- actor/agent attribution,
- session/run boundary,
- ordered event history,
- artifact references,
- metadata records,
- policies,
- subscriptions,
- indexes.

Extensible semantics:

- memory models,
- skill formats,
- planning formats,
- agent hierarchy,
- handoff models,
- evaluation records,
- context construction strategies.

Design rule:

> Keep ordered history, artifacts, metadata, policy, and retrieval stable. Keep
> memory, skills, plans, and workflow semantics versioned and extensible.

Every semantic object should carry a schema version.

```json
{
  "kind": "skill_used",
  "schema_version": "2026-04-28",
  "payload": {
    "skill_type": "mcp_tool",
    "name": "gmail.send",
    "version": "1.2.0"
  }
}
```

## 7. Data Model

Logical hierarchy:

```text
Tenant
  Agent
    Metadata
    Policies
    Skills
    Searchable Records / Indexes
    Sessions / Runs
      Events
      Artifacts
      Derived context
```

The session log is the timeline. It references the other runtime objects.

Examples:

```json
{
  "kind": "artifact_created",
  "artifact_id": "art_01H...",
  "summary": "Full log bundle for EU checkout failures"
}
```

```json
{
  "kind": "record_written",
  "record_id": "rec_01H...",
  "source": { "session_id": "ses_01H...", "seq": 42 }
}
```

```json
{
  "kind": "skill_used",
  "skill_id": "skl_01H...",
  "version": "1.2.0"
}
```

Events are the durable execution timeline. Artifacts are large evidence.
Searchable records support framework memory and knowledge use cases. Skills are
reusable capabilities. Metadata is current operational state. Policies govern the
runtime.

## 8. Event, Artifact, Searchable Record, Skill Split

AgentDB V2 should not inline all action data into events.

```text
Event:
  small/medium structured execution record, usually KB-scale

Artifact:
  large raw payload, KB-MB+, stored behind an artifact content abstraction

Derived context:
  summary, extracted facts, embeddings, indexed text

Searchable record:
  framework/application-provided memory, knowledge, or recall record

Skill:
  versioned capability, instructions, tool manifest, or package
```

Recommended event shape for large tool output:

```json
{
  "kind": "tool_result",
  "tool": "search_logs",
  "summary": "Found EU checkout failures after feature flag rollout",
  "artifact": {
    "artifact_id": "art_01H...",
    "content_type": "application/jsonl",
    "bytes": 3481230,
    "sha256": "..."
  }
}
```

Recommended API rule:

AgentDB should expose a simple artifact API and artifact refs. The runtime should
not expose whether artifact content is stored inline in the local engine, in a
filesystem/object-store-like content layout, or in S3-compatible object storage.
That placement decision belongs to the storage engine.

Artifact metadata is runtime metadata. It should be stored in the same metadata
store as events, sessions, agents, policies, and skills. Artifact content can be
stored separately by the artifact content backend.

Recommended default placement policy for storage engines:

| Artifact content size | Storage engine behavior |
|---:|---|
| 0-64 KB | May inline content in local metadata/blob store if efficient |
| 64-256 KB | May inline or spill to content store; indexing capped by policy |
| 256 KB-1 MB | Prefer content store placement |
| >1 MB | Content store placement by default |
| >16 MB | Native artifact API only; generic document adapters may reject |

## 9. Runtime Core Responsibilities

The runtime core owns agent-native invariants.

### 9.1 Tenant isolation and platform control

- Resolve authenticated tenant.
- Enforce tenant, agent, and session scopes.
- Enforce quota and admission policies.
- Detect runaway agents and hot sessions.
- Emit metering events.
- Apply retention and archive policies.

### 9.2 Session execution history

- Append ordered events.
- Assign per-session sequence numbers.
- Maintain session status and lifecycle state.
- Fetch recent events with byte/token budgets.
- Scan/replay event ranges.
- Subscribe from a sequence.
- Fork sessions with lineage.

### 9.3 Artifact management

- Store artifact metadata in the runtime metadata store.
- Store artifact content through the storage engine's artifact content backend.
- Verify hashes.
- Attach artifacts to events, searchable records, and skills.
- Move artifacts across tiers.
- Enforce retention policies.

### 9.4 Search for memory use cases

- Store framework/application-provided records for memory, knowledge, and recall
  use cases.
- Preserve optional source references to sessions, events, artifacts, or external
  objects when the caller provides them.
- Index selected fields under tenant, namespace, and policy constraints.
- Support search by scope, filters, text, vector, time, tags, and source refs.
- Leave extraction, summarization, forgetting policy, importance scoring, and
  memory semantics to the application, framework, or pluggable hooks.

AgentDB should not claim to solve agent memory intelligence. Its responsibility
is to provide durable, policy-controlled, searchable storage that memory systems
can use.

### 9.5 Skill management

- Store skill manifests and versions.
- Bind skills to agents or tenants.
- Track skill use in session events.
- Pin exact skill version for replay/audit.
- Store larger skill packages as artifacts.

Skill formats should be extensible: MCP tools, Claude skills, OpenAI tools,
custom manifests, prompts, scripts, or documents.

### 9.6 Context builder

The runtime core should provide a budgeted context API that can combine:

- recent session events,
- selected artifacts or summaries,
- searched records for memory use cases,
- active skill instructions,
- current metadata/policy hints.

Example:

```ts
const context = await session.context({
  tokenBudget: 64_000,
  byteBudget: 1_000_000,
  include: ["recent_events", "search_records", "skill_instructions"],
  largeEvents: "summary_ref"
});
```

Context construction strategies will evolve, so this API must be extensible.

## 10. Storage Engine Capabilities

The storage engine should expose generic capabilities, not hardcoded agent
business concepts.

### 10.1 Ordered streams

Required for sessions, replay, audit, and durable subscriptions.

```text
append(stream_id, event) -> seq
scan(stream_id, from_seq, limit, budget)
tail(stream_id, from_seq)
fork_stream(parent_stream_id, child_stream_id, at_seq)
```

### 10.2 Transactional metadata records

Required for tenants, agents, session state, policy records, skills, searchable
record metadata, and artifact metadata.

```text
get(key)
put(key, value, condition?)
compare_and_swap(key, expected, value)
txn([ops])
```

### 10.3 Artifact store

Required for MB-scale context snapshots, tool results, files, logs, screenshots,
skill packages, and generated outputs.

The storage engine owns physical placement. Local engines may use a filesystem or
object-store-like directory for content. Distributed engines may use
S3-compatible object storage. Both should keep artifact metadata in the same
metadata engine as events and agent/session metadata.

```text
put_blob(bytes, metadata) -> artifact_id
get_blob(artifact_id)
head_blob(artifact_id)
delete_or_archive_blob(artifact_id)
verify(artifact_id, sha256)
```

### 10.4 Indexing hooks

Runtime decides what to index. Storage/index layer executes.

```text
index_text(scope, doc_id, fields)
index_vector(scope, doc_id, vector)
delete_index(scope, doc_id)
search(scope, query, filters)
```

### 10.5 Snapshot reads / MVCC

Required for fork, replay, consistent context building, and cross-object reads.

```text
read_at(timestamp_or_seq)
scan_at(...)
```

### 10.6 Lifecycle primitives

Runtime owns policy. Storage performs movement.

```text
seal_segment(stream_id)
move_to_cold(segment_id)
archive(segment_id)
hydrate(stream_id)
```

### 10.7 Changefeed

Required for durable subscribe, indexing hooks, dashboards,
and downstream integrations.

```text
watch(stream_id | tenant_id | agent_id, from_cursor)
```

## 11. Lessons From The V1 Prototype

V1 is a prototype. V2 is allowed to change the API, storage layout, wire
protocol, and SDK model. The purpose of reviewing V1 is to identify which ideas
validated and which should be redesigned cleanly.

### 11.1 Ideas to keep

The following V1 ideas remain important in V2:

- **Session as the core execution boundary.** The platform-facing object may be
  called session, run, thread, or task, but AgentDB needs one ordered execution
  history boundary.
- **Ordered per-session sequence.** `(session_id, seq)` is still the right
  replay and subscription ordering primitive, even if V2 also adds global
  `event_id`.
- **Append-and-fetch.** Writing an event and fetching recent context in one
  operation matches the agent loop.
- **Copy-free fork.** Fork lineage remains valuable for hypothesis exploration,
  debugging, coding agents, and replay.
- **Durable subscribe plus ephemeral channels.** Durable execution events and
  transient coordination signals should stay separate.
- **Warm/cold/archive lifecycle.** Agent sessions become mostly cold; lifecycle
  management is a core differentiator.
- **Object storage for cold data and large payloads.** This remains the right
  cost structure for old sessions and artifacts.
- **Tenant-aware admission.** Runaway agents and noisy-neighbor containment are
  platform requirements, not optional operations tooling.

### 11.2 Ideas to change

The following V1 choices should be reconsidered for V2:

- **Agent as only metadata.** V2 should make Agent a first-class resource with
  metadata, policies, skills, search namespaces, and sessions.
- **Events as the only main data model.** V2 needs events, artifacts, searchable
  records, skills, metadata, and policies as separate runtime objects connected
  by source refs where useful.
- **Large payloads inline in events.** V2 should use artifact refs for large
  context snapshots, tool outputs, files, logs, and generated assets.
- **Last-N context only.** V2 should add budgeted context building by bytes,
  tokens, event kinds, search scopes, and artifact summaries.
- **Recursive indexing of all payload content.** V2 should make indexing policy
  explicit and default to selected fields or summaries.
- **Operator-first dashboard.** V2 should make session/run observability the
  primary dashboard: timeline, artifacts, searchable records, skills, policies, fork tree,
  and replay.
- **Document-store adapters as primary API.** V2 should prioritize native
  agent-runtime APIs. Compatibility adapters can exist later, but should not
  define the product model.

### 11.3 Clean-slate implementation stance

V2 should be free to change:

- public API shape,
- ID formats,
- storage layout,
- route structure,
- SDK abstractions,
- dashboard model,
- indexing defaults,
- artifact layout,
- managed-service identity model.

The V2 prototype should optimize for validating the new runtime model, not for
maintaining prototype behavior.

## 12. Managed Service Data-Plane Considerations

If AgentDB is run as a managed service, the data plane must keep the runtime
model intact. This section does not design the control plane. It describes what
hot runtime services and storage paths must support after policy, identity, and
configuration have been resolved into data-plane state.

The public runtime surface should stay agent-native:

```text
tenants
agents
sessions
events
artifacts
search_records
skills
policies
subscriptions
```

Managed-service concerns should not turn the runtime API into provider
administration concepts. Tenant remains the runtime isolation, quota, retention,
indexing, and metering boundary visible to the agent platform.

### 12.1 Tenant isolation in all runtime paths

Every data-plane key, index scope, artifact path, subscription cursor, lifecycle
job, and metering event must carry the runtime tenant boundary explicitly.
Globally unique object IDs are useful, but they must not replace tenant-scoped
routing and authorization checks.

Required invariant:

```text
All reads, writes, scans, indexes, artifacts, subscriptions, lifecycle actions,
and delete/export operations are scoped by tenant_id before object_id.
```

Examples:

```text
event/{tenant_id}/{session_id}/{seq}
artifact_content/{tenant_id}/{artifact_id}/...
search_scope/{tenant_id}/{scope_hash}/{record_id}
index/vector/{tenant_id}/...
subscription/{tenant_id}/{session_id}/{consumer_id}
```

Cross-tenant scans should not exist on hot runtime paths. Operational tooling may
aggregate signals out of band, but data-plane serving paths should preserve
tenant-local authorization, quota, lifecycle, and residency decisions.

### 12.2 Policy enforcement during control-plane degradation

The data plane must continue to enforce the last known good runtime policy even
when external control systems are slow, unavailable, or partitioned. Runtime
authorization, admission, quota, indexing, retention, artifact, and lifecycle
decisions should fail closed where accepting a write would violate tenant policy.

Data-plane policy state should be:

- versioned,
- locally cacheable,
- auditable on each decision,
- attached to the affected tenant, agent, session, artifact, searchable record,
  skill, or policy object,
- refreshed asynchronously without blocking the hot path when the cached version
  is still valid.

Policy decisions that affect replay or audit should be recorded in the runtime
timeline or metadata, including the policy version that was evaluated.

### 12.3 Admission, quotas, and runaway containment

Admission control must be enforced before expensive work begins. Append,
artifact upload, searchable record write, skill package write, subscription
fanout, replay, indexing, and lifecycle hydration should all check tenant and
agent limits before they allocate storage, CPU, network, or index capacity.

The data plane should support quotas for:

- append rate and event bytes,
- active sessions and parallel appends,
- hot-session write rate,
- artifact size, upload rate, egress, and storage by tier,
- searchable record count and search/recall rate,
- subscription count, fanout, and delivery backlog,
- replay, scan, and hydration budgets,
- indexing operations, index storage, and index freshness backlog,
- lifecycle work issued per tenant.

Runaway containment should identify sessions or agents that loop, append too
quickly, create excessive artifacts, fan out subscriptions, or repeatedly trigger
replay/index work. Containment actions should include throttling, rejecting new
writes, requiring explicit resume, suppressing derived work, or moving bulk work
to lower-priority queues.

### 12.4 Metering events emitted by the data plane

The data plane should emit durable metering events as side effects of accepted
runtime operations. Metering does not need to aggregate invoices in the hot path,
but it must produce enough tenant-scoped facts for cost attribution and customer
debugging.

Metering events should include:

- tenant_id, agent_id, session_id, and object IDs where applicable,
- operation class,
- accepted/rejected/throttled outcome,
- bytes read, written, stored, indexed, and egressed,
- artifact tier and lifecycle transition,
- index type and indexed fields or summaries,
- subscription messages and backlog bytes,
- replay, scan, and hydration work units,
- policy version and quota bucket involved,
- idempotency key when present.

Metering event emission must be best-effort durable for accepted writes. If
metering cannot be emitted synchronously, the operation should leave a replayable
record that allows metering reconstruction from the data-plane log.

### 12.5 Residency, region routing, and storage constraints

Data residency is a data-plane routing and storage invariant, not only an
administrative label. A tenant's runtime objects, artifacts, indexes,
subscriptions, lifecycle queues, and derived work must stay inside allowed
regions unless an explicit policy permits movement.

The data plane should enforce residency for:

- append and fetch routing,
- artifact upload, download, tiering, and deletion,
- text/vector indexing and embedding-related processing,
- searchable record lookup, search, and source-ref reads,
- subscription delivery and changefeed fanout,
- replay, hydration, export, redaction, and lifecycle jobs,
- backups or retained segments used by runtime restore.

Region isolation should prefer deterministic tenant routing over request-time
guessing. If a request arrives in the wrong region, the data plane should reject
or redirect according to policy without reading tenant data in the wrong place.

### 12.6 Encryption and KMS hooks at storage boundaries

Encryption must be enforced where data crosses durable storage boundaries:
ordered event storage, metadata records, artifact blobs, indexes, snapshots,
lifecycle tiers, and replay/export bundles.

The data plane should support KMS hooks for:

- resolving the tenant's active encryption key or key version,
- envelope-encrypting artifact and segment data,
- encrypting index payloads or stored fields when supported,
- rotating keys without breaking replay or artifact integrity checks,
- refusing writes when required key material is unavailable,
- recording key version metadata needed for future reads and audit.

Artifact hashes should verify plaintext or canonical payload identity according
to policy, while storage encryption details remain an implementation boundary.

### 12.7 Indexing policy enforcement

Indexing is compliance-sensitive and must be enforced by the data plane, not left
to background worker convention. The runtime must decide whether each event,
artifact, searchable record, skill, or derived summary can be indexed before
content leaves the tenant's allowed processing path.

Supported indexing modes should include:

- no indexing,
- metadata-only indexing,
- selected-field indexing,
- summary-only indexing,
- vector indexing allowed or denied,
- artifact-derived indexing allowed or denied,
- region-bound indexing.

Index records must preserve tenant scope and source provenance. Deleting or
redacting a source object must delete or invalidate derived index entries unless
policy explicitly allows derivative-only retention.

### 12.8 Artifact lifecycle, redaction, and derivative-only mode

Artifacts are often the highest-risk and highest-cost objects in AgentDB. The
data plane must enforce artifact retention policy at put, get, tier, redact,
delete, and lifecycle-transition time.

Required capabilities:

- raw artifact retention windows by tenant, agent, session, artifact type, or
  policy attachment,
- redaction that updates artifact metadata and prevents future raw reads,
- delete workflows that remove raw bytes, metadata access, indexes, and
  subscription visibility according to policy,
- derivative-only mode, where summaries, extracted facts, memories, hashes, and
  audit metadata may remain after raw payload deletion,
- provenance from derivatives back to deleted or redacted sources without
  re-exposing raw content,
- lifecycle actions that are idempotent and resumable after worker failure.

Artifact reads must check current policy, not only object existence. A retained
artifact may still be unreadable if policy, residency, redaction, or key state no
longer permits access.

### 12.9 Hot-path isolation from bulk work

Session append, recent fetch, artifact head/get for active sessions, scoped
search/recall, and subscription delivery are runtime hot paths. They must be
isolated from bulk replay, indexing, lifecycle sweeps, artifact tiering,
export/delete jobs, and hydration.

Isolation mechanisms should include:

- separate queues or priority classes for hot and bulk work,
- tenant-aware scheduling and backpressure,
- bounded fanout from append into indexers and subscribers,
- replay and lifecycle budgets that cannot consume append capacity,
- circuit breakers for lagging indexers or subscribers,
- per-tenant bulk-work throttles independent of hot-path quotas.

Freshness can degrade for derived work, but append ordering, accepted-write
durability, and recent fetch correctness should not depend on bulk workers
keeping up.

### 12.10 Data-plane SLO classes

Managed AgentDB should define SLOs by runtime operation class rather than a
single generic database target.

Recommended data-plane SLO classes:

- append latency and accepted-write durability,
- recent event fetch latency,
- artifact upload, head, and download latency,
- searchable record write and search/recall latency,
- subscription delivery lag and reconnect replay latency,
- index freshness by tenant and index policy,
- lifecycle transition lag for warm/cold/archive/delete/redact,
- replay and hydration progress,
- policy propagation delay to data-plane enforcement,
- metering event durability lag.

Each class should define whether it is a hot-path correctness SLO, a freshness
SLO, or a background-completion SLO. This prevents index freshness or lifecycle
lag from being confused with append availability.

### 12.11 Customer-visible operational signals

The data plane should expose signals that help agent platforms understand runtime
behavior without exposing provider internals.

Useful tenant-scoped signals include:

- current throttling and quota bucket state,
- rejected admission decisions and policy versions,
- hot sessions and suspected runaway agents,
- append latency and error rates,
- subscription lag and dropped ephemeral signals,
- index freshness and indexing suppression reasons,
- artifact tier, retention, redaction, and deletion state,
- lifecycle backlog and hydration progress,
- residency or region-routing rejection reasons,
- KMS/key availability errors affecting reads or writes,
- metering lag and reconstructable metering gaps.

Signals should be tied back to runtime objects such as tenants, agents, sessions,
events, artifacts, searchable records, skills, policies, and subscriptions so
that platform operators can debug agent behavior directly.

### 12.12 Idempotency for runtime mutations

Retries are normal in agent runtimes. Data-plane mutation APIs should accept
idempotency keys for operations where duplicate writes would corrupt replay,
double-store artifacts, or create conflicting runtime metadata.

Idempotency keys should be supported for:

- event append and append-and-fetch,
- artifact put, redact, delete, and lifecycle transition,
- searchable record write, update, and delete,
- skill registration, version creation, and binding mutation,
- policy creation, version update, and attachment mutation.

Idempotency scope should include tenant_id, operation class, target object, and
caller-provided key. Replays of the same key should return the original result or
a structured conflict if the payload differs.

## 13. ID Allocation And Scope

ID scope must balance global uniqueness, developer ergonomics, routing, and
multi-tenant isolation.

### 13.1 Principles

- Internal IDs should be globally unique unless there is a strong reason not to.
- External aliases may be scoped for developer ergonomics.
- All persisted references should use stable internal IDs.
- All user-facing APIs may accept aliases, but should resolve them to IDs.
- IDs should be sortable where useful for debugging and storage locality.
- Do not derive security from ID opacity. Auth and tenant checks are mandatory.

Recommended ID format: UUIDv7, ULID, or KSUID with type prefixes.

Examples:

```text
ten_01J...
agt_01J...
ses_01J...
evt_01J...
art_01J...
rec_01J...
skl_01J...
pol_01J...
```

Type prefixes improve debugging and prevent accidental cross-type use.

### 13.2 Tenant ID

Tenant IDs should be globally unique.

```text
tenant_id = ten_<uuidv7>
tenant_external_key = customer/workspace slug from caller, unique by deployment
```

Rationale:

- Tenant is the isolation, quota, billing, retention, and compliance boundary.
- Tenant IDs appear in almost every metadata record and index scope.
- Global uniqueness simplifies audit, export, support, and multi-cluster
  movement.

Recommended uniqueness:

```text
tenant_id: globally unique
tenant_external_key: unique within an AgentDB cluster/deployment
```

### 13.3 Agent ID

Agent IDs should be globally unique internally, with optional tenant-scoped
aliases.

```text
agent_id = agt_<uuidv7>
agent_alias = "sales-rep", unique within tenant unless overridden
```

Rationale:

- Agents may be moved, cloned, exported, or referenced across sessions.
- Human-friendly aliases matter for DX.
- Policies, search namespaces, and skill bindings should reference stable IDs.

Recommended uniqueness:

```text
agent_id: globally unique
agent_alias: unique within tenant
```

### 13.4 Session / Run ID

Session IDs should be globally unique internally. Developer-provided semantic
session names should be aliases.

```text
session_id = ses_<uuidv7>
session_alias = "incident-123" or "lead-followup-2026-04-28"
```

Rationale:

- Global session IDs simplify routing, fork references, artifact provenance,
  audit, and export.
- Developers want semantic names.
- A session may outlive an agent version or move across tenants in rare admin
  workflows.

Recommended uniqueness:

```text
session_id: globally unique
session_alias: unique within tenant, or within (tenant_id, agent_id) if explicitly scoped
```

API rule:

- Public APIs may accept `session_alias`.
- Persisted refs should store `session_id`.
- Ambiguous aliases must be rejected.

### 13.5 Event ID

Events need both a globally unique event ID and a per-session sequence number.

```text
event_id = evt_<uuidv7>
session_seq = monotonically increasing u64 within session
```

Rationale:

- `session_seq` is the primary replay/subscription ordering primitive.
- `event_id` is useful for global audit, idempotency, indexing, deduplication,
  exports, and cross-session references.
- Subscriptions should deduplicate by `(session_id, session_seq)`.

Recommended uniqueness:

```text
event_id: globally unique
(session_id, session_seq): unique and ordered
```

Do not rely only on globally sorted event IDs for session ordering. Per-session
`seq` remains the stable runtime primitive.

### 13.6 Artifact ID

Artifact IDs should be globally unique. Artifacts may also have content hashes.

```text
artifact_id = art_<uuidv7>
sha256 = content hash, optional but recommended
```

Rationale:

- Artifacts are referenced by events, searchable records, skills, and exports.
- Global IDs simplify object-store paths and deduplication.
- Content hash supports integrity verification and optional dedup.

Recommended uniqueness:

```text
artifact_id: globally unique
sha256: content identity, not the primary mutable metadata key
```

Artifact path example:

```text
artifacts/{tenant_id}/{artifact_id}/blob
artifacts/{tenant_id}/{artifact_id}/meta
```

Tenant remains in the path for isolation and lifecycle listing even if
`artifact_id` is globally unique.

### 13.7 Searchable Record ID

Searchable record IDs should be globally unique internally, with scope fields for
search and recall.

```text
record_id = rec_<uuidv7>
scope = [tenant_id, agent_id?, customer_id?, project_id?]
```

Rationale:

- Searchable records may represent memory, knowledge chunks, summaries, or
  framework store items.
- Recall depends on scope, namespace, filters, and indexes, not ID.
- Optional source refs may point to source session/event/artifact when available.

Recommended uniqueness:

```text
record_id: globally unique
scope: explicit field, indexed
```

### 13.8 Skill ID

Skill identity needs a stable skill ID plus a version.

```text
skill_id = skl_<uuidv7>
skill_name = "draft-outreach"
version = "1.2.0" or content-derived version
```

Rationale:

- Skills must be replayable by exact version.
- Names are human-facing and may collide across tenants.
- Skill packages may be stored as artifacts.

Recommended uniqueness:

```text
skill_id: globally unique
(tenant_id, skill_name, version): unique for tenant-owned skills
(global_namespace, skill_name, version): unique for shared/global skills
```

Session events should record the exact skill version used.

### 13.9 Policy ID

Policy IDs should be globally unique, with attachment records.

```text
policy_id = pol_<uuidv7>
attached_to = tenant | agent | session | skill
```

Rationale:

- Policies should be versioned and auditable.
- A policy change can affect future sessions while old sessions need replay with
  historical policy context.

Recommended uniqueness:

```text
policy_id: globally unique
policy_version: monotonically increasing per policy
```

## 14. ID Summary

| Object | Internal ID | Scope | Alias / Secondary Key |
|---|---|---|---|
| Tenant | globally unique `ten_*` | global | external key unique per deployment |
| Agent | globally unique `agt_*` | tenant-owned | alias unique within tenant |
| Session / Run | globally unique `ses_*` | tenant/agent-associated | alias unique within tenant or tenant+agent |
| Event | globally unique `evt_*` + session seq | session ordered | `(session_id, seq)` |
| Artifact | globally unique `art_*` | tenant-associated | sha256 for integrity/dedup |
| Searchable Record | globally unique `rec_*` | explicit search scope | source refs, tags |
| Skill | globally unique `skl_*` | tenant or global namespace | `(namespace, name, version)` |
| Policy | globally unique `pol_*` | attachment scope | version |

## 15. Storage Layout Sketch

Logical keys can be implemented on top of RocksDB, SlateDB, object storage,
Postgres, or a future custom engine. The runtime core should not expose these
keys as public API.

```text
record/tenant/{tenant_id}
record/agent/{tenant_id}/{agent_id}
record/session/{tenant_id}/{session_id}
record/session_alias/{tenant_id}/{alias} -> session_id
record/skill/{tenant_id}/{skill_id}/{version}
record/policy/{tenant_id}/{policy_id}/{version}

event/{tenant_id}/{session_id}/{seq} -> EventEnvelope
event_by_id/{event_id} -> {tenant_id, session_id, seq}

artifact_meta/{tenant_id}/{artifact_id}
artifact_content/{tenant_id}/{artifact_id}/...

search_record/{tenant_id}/{record_id}
search_scope/{tenant_id}/{scope_hash}/{record_id}

index/text/{tenant_id}/...
index/vector/{tenant_id}/...
```

For cluster routing, internal globally unique IDs can still hash to shards.
Tenant must remain an explicit field for authorization, quota, lifecycle, and
billing.

## 16. API Sketch

TypeScript-style API:

```ts
const db = new AgentDB(url, { tenant: "acme" });

const agent = db.agent("sales-rep");
await agent.configure({ model: "claude-sonnet-4.5" });

await agent.skills.register({
  name: "draft-outreach",
  version: "1.0.0",
  instructions: "Draft concise prospect outreach.",
});

const session = await agent.session("lead-followup-123");

const artifact = await session.artifacts.put({
  contentType: "application/json",
  body: largeToolResult,
  compress: "zstd",
});

const { seq, recent } = await session.appendAndFetch(
  {
    kind: "tool_result",
    tool: "crm.lookup",
    summary: "Found recent funding event for target account",
    artifact,
  },
  { last: 20, maxBytes: 1_000_000 },
);

await agent.records.write({
  text: "ACME prefers ROI-focused email copy.",
  source: { session: session.id, seq },
});

const branch = await session.fork("try-shorter-copy");

for await (const event of session.subscribe({ fromSeq: seq + 1 })) {
  render(event);
}
```

### 16.1 Adapter-facing API Requirements

The native AgentDB API should remain session/event/artifact/search oriented, but
open-source adoption depends on adapters that map cleanly to existing framework
interfaces. The runtime core should therefore expose a small set of adapter-facing
surfaces on top of the canonical event log.

Frameworks commonly need these shapes:

| Framework family | Required shape | AgentDB surface |
|---|---|---|
| LangGraph/LangChain | thread checkpoints, pending writes, checkpoint history, namespaced store, vector search | `threads`, `checkpoints`, `checkpoint_writes`, `store`, `vector_search` |
| OpenAI Agents SDK | session items, tool calls/results, interruptions, traces/spans | `session_items`, `runtime_events`, `traces` |
| Vercel AI SDK | chat messages, tool parts, stream status, resumable streams | `messages`, `message_parts`, `stream_state`, `stream_events` |
| CrewAI-like frameworks | scoped memory records, knowledge chunks, task/agent/tool traces | `store`, `collections`, `search`, `traces`, `artifacts` |
| AutoGen/AG2-like frameworks | serializable team/agent state snapshots | `runtime_state` snapshots with optimistic concurrency |
| LlamaIndex-like frameworks | chat store, doc store, index metadata, vector store, workflow context | modular `store` namespaces plus vector/text indexes |
| MCP | tools/resources/prompts, tool-call audit, resource subscriptions | MCP server backed by AgentDB sessions, artifacts, search, and replay |

These surfaces should be projections or typed records under the runtime model, not
separate databases. Each adapter write should still be attributable to tenant,
agent, session/thread, framework, and idempotency key where appropriate.

Minimum adapter-facing primitives:

- `messages.append/list/replace_tail` for chat/message-history frameworks.
- `checkpoints.put/get/list/put_writes` for graph checkpointing frameworks.
- `store.put/get/search/delete` with namespace tuples and metadata filters.
- `runtime_state.put/get` for opaque framework/team/agent snapshots with compare
  and set revision control.
- `traces.ingest` for spans linked to sessions, events, tool calls, checkpoints,
  and artifacts.
- `streams.resume` for frameworks that need reconnectable token/message streams.
- `imports/exports` for framework-native JSONL or bundle formats.

The adapter contract should preserve the raw framework payload for round-trip
compatibility while indexing common fields such as role, text, tool call ID,
model, usage, timestamp, trace ID, checkpoint ID, and metadata filters.

### 16.2 Framework Adapters As Proof Workloads

Framework adapters are not only distribution. They are also proof workloads for
AgentDB's low-level primitives.

Popular agent frameworks expose real agent workload patterns: message history,
checkpointing, resumable sessions, tool calls and results, traces, stream resume,
search/recall, runtime state snapshots, artifacts, and multi-agent coordination.
If AgentDB can implement these framework persistence interfaces more naturally
than Postgres or MongoDB, that is evidence that its primitives match the AI agent
runtime workload.

The validation criterion should be adapter simplicity:

```text
A production-quality framework adapter should be mostly mapping code, not
infrastructure code.
```

For example, a LangGraph checkpointer and store adapter should not have to invent
ordering, idempotency, pending-write tables, checkpoint lineage, artifact offload,
live subscriptions, search indexes, lifecycle, or index policy. Those should be
AgentDB primitives. The adapter should map LangGraph's interface to AgentDB's
checkpoint, session, store, artifact, and search APIs.

This is a useful benchmark against generic databases:

| Framework need | Generic database work | AgentDB expectation |
|---|---|---|
| Append message/tool event | custom schema and sequence logic | native ordered append |
| Resume from last event | custom cursor convention | built-in seq/cursor |
| Save latest checkpoint | custom table plus transaction/CAS | checkpoint API plus latest pointer |
| Pending writes | custom side table | checkpoint writes primitive |
| Replay run | custom query convention | native replay/scan |
| Live UI updates | add queue/pubsub infrastructure | durable subscribe from cursor |
| Large tool output | add object store conventions | artifact refs and lifecycle |
| Search/recall | add vector/text index stack | scoped search with policy controls |
| Retry safety | custom idempotency table | native idempotency key |
| Fork run/checkpoint | custom copy/link model | fork lineage primitive |

The goal is not to prove AgentDB is a better general database. The goal is to
prove that agent framework SDKs become simpler and safer because AgentDB already
provides the runtime primitives the workload needs.

### 16.3 What AgentDB Should Do Better Than Postgres Or MongoDB

Postgres and MongoDB can store framework records, but they do not provide
agent-runtime semantics as one enforced product. AgentDB should compete by making
the adapter path operationally safer and easier:

- ordered session/thread timelines with replay and durable subscribe built in,
- atomic append plus context fetch for agent loops,
- checkpoint lineage and copy-free fork as first-class behavior,
- artifact offload, hashing, tiering, redaction, and derivative-only retention,
- tenant-scoped quota/admission/runaway containment on the write path,
- idempotency for retries across append, checkpoint, artifact, searchable record,
  skill, and policy mutations,
- tenant-scoped text/vector search for memory and knowledge records,
- policy-controlled indexing so sensitive payloads are not accidentally embedded
  or searched,
- unified trace/session/artifact/checkpoint linkage for debugging,
- resumable subscriptions and streams without a separate queue stack,
- lifecycle transitions from hot to cold to archive while preserving replay,
- framework import/export so developers are not locked into one adapter.

The product claim should not be "AgentDB stores JSON better." It should be:

```text
AgentDB gives existing agent frameworks a durable runtime layer with replay,
resume, artifacts, checkpoints, scoped search, policies, and live streams as
built-in semantics instead of application conventions.
```

## 17. Policy Model

Policies are first-class because AI Agents Platforms need operational control.

Policy categories:

- quota/admission policy,
- lifecycle/archive policy,
- retention/delete policy,
- indexing policy,
- artifact size policy,
- approval/human-in-the-loop policy,
- tool/skill access policy,
- tenant data access policy.

Example:

```json
{
  "policy_id": "pol_01J...",
  "version": 3,
  "attached_to": { "type": "agent", "agent_id": "agt_01J..." },
  "quota": {
    "max_actions_per_hour": 100,
    "max_parallel_sessions": 20
  },
  "lifecycle": {
    "cold_after_minutes": 30,
    "archive_after_days": 14,
    "delete_raw_artifacts_after_days": 90
  },
  "artifacts": {
    "inline_event_max_bytes": 262144,
    "artifact_required_after_bytes": 1048576
  },
  "indexing": {
    "default": "summary_only"
  }
}
```

Policies are evaluated by the runtime core before writes, during lifecycle
sweeps, and when building context.

## 18. Lifecycle Model

Session lifecycle is one of AgentDB's main differentiators.

```text
active -> warm -> cold -> archive -> derivative-only/delete-raw (optional)
```

Meaning:

- **Active**: currently running or recently appended; low-latency event access.
- **Warm**: recent and likely to resume; full event envelope access in fast KV.
- **Cold**: inactive but replayable; sealed segments and artifacts in object
  storage; indexes retained or compacted.
- **Archive**: rarely read; cheap storage; hydration required for full replay.
- **Derivative-only**: optional compliance/cost mode; raw artifacts deleted after
  memory/summaries/extracted facts are retained.

The runtime core owns lifecycle policy. The storage engine provides tiering
primitives.

## 19. Subscription Semantics

Durable session subscriptions are sequence-based.

Recommended wording:

```text
Each session has a monotonic seq. Subscribe can replay from seq N and then
live-tail. SDKs track last delivered seq and reconnect with from_seq=N+1.
Consumers should handle at-least-once delivery across transport retries and
deduplicate by (session_id, seq) if handlers are not idempotent.
```

Do not claim strict exactly-once network delivery unless the protocol adds
consumer acknowledgements and durable consumer offsets.

Ephemeral channels remain separate for transient signals.

## 20. What Belongs In AgentDB vs Application

AgentDB should own invariants that, if broken, compromise replay, audit,
security, lifecycle, or platform control.

AgentDB owns:

- event ordering,
- session replay and scan,
- fork lineage,
- artifact integrity and refs,
- tenant quota/admission,
- lifecycle/archive enforcement,
- scoped searchable records and indexes,
- skill version records,
- subscription replay,
- policy enforcement,
- index inclusion/exclusion policy.

Application owns or plugs in:

- LLM calls,
- memory extraction/summarization intelligence,
- memory semantics, scoring, and forgetting policy,
- planning algorithm,
- business object mutations,
- tool execution side effects,
- prompt style,
- task-specific evaluation.

## 21. Prototype Priorities

V2 prototype should validate semantics before distributed scale.

The implementation handoff for AI coding agents is
[`minimal-single-machine-implementation.md`](minimal-single-machine-implementation.md).
That document is the source of truth for local V2 API contracts, event envelope,
storage keys, concurrency, idempotency, fork semantics, and tests.

P0:

1. Tenant, agent, session, event, artifact, searchable record, skill, policy
   records.
2. Clean session APIs: append, append-and-fetch, fetch, scan, replay, fork,
   subscribe.
3. Artifact API and event refs.
4. Scoped searchable records with text/vector search for memory use cases.
5. Skill registry with version pinning and session `skill_used` events.
6. Lifecycle policy for warm/cold/archive in single-node mode.
7. Session-first dashboard: timeline, artifacts, searchable records, skills,
   fork tree.
8. Single-machine `serve --dev` mode suitable for framework adapters.
9. Python SDK/client and TypeScript SDK/client for the native `/api` surface.
10. At least one high-quality framework adapter that proves AgentDB can sit under
   an existing agent runtime without forcing a rewrite.
11. Structured errors with `suggestion`, `retryable`, and `docs`.

P1:

1. OpenAI Agents SDK session adapter.
2. MCP server exposing AgentDB tools/resources/prompts.
3. Vercel AI SDK template.
4. Adapters for popular open-source agents/frameworks once their persistence
   boundaries are stable.
5. Multi-node scale-out path.
6. Advanced tenant quotas and runaway-agent detection.
7. Artifact dedup and derivative-only retention mode.

## 22. Non-Goals For V2 Prototype

- Full OLTP replacement.
- General-purpose document database surface.
- Full MongoDB-compatible API as the primary surface.
- Built-in LLM memory extraction intelligence.
- Built-in tool execution side effects.
- Go/Golang SDK or client in the V2 prototype.
- Cross-region active-active writes.
- General analytics warehouse.

## 23. Bottom Line

AgentDB V2 should be an agent-native runtime database, not a thin application
library and not a generic distributed document store.

The core architectural split is:

```text
Runtime Core:
  tenant, agent, session, event, artifact, search, skill, policy semantics

Storage Engine:
  ordered streams, metadata records, blobs, indexes, snapshots, lifecycle,
  changefeeds
```

Users should get first-class agent primitives. The storage engine should provide
enough capability for the runtime core to enforce those primitives correctly.
