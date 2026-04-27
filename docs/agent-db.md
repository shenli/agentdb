# Agent-Native Operational Database — Design

Li Shen · [shenli3514@gmail.com](mailto:shenli3514@gmail.com)

## Abstract

The access patterns of AI agents diverge from human-driven OLTP along
several axes at once: action volume, payload size, read/write ratio,
session locality, lifecycle dynamics, and tenant cardinality. Taken
individually, each divergence admits a local patch on top of an
existing database. Taken together, they motivate a different
operational database design — one whose storage is object-backed,
whose primitives are session-shaped, and whose multi-tenant model is
built around hundreds of millions of mostly-sleeping tenants rather
than thousands of busy ones.

This document describes that design in four layers — **requirements**,
**exposed abstractions**, **architecture**, **implementation
trade-offs** — followed by a discussion of how existing tool
combinations fall short, a product positioning statement, and a list
of open questions.

It is a design document, not a product plan. The corresponding
prototype's implementation is described separately in
[ARCHITECTURE.md](ARCHITECTURE.md).

---

## 1. Motivation: Why Now

The case for an agent-native database rests on a single empirical
claim: **agents drive a 1000×–10000× increase in action volume per
end user**, with no corresponding increase in the database's budget
or latency tolerance.

### 1.1 The structural shift: user-triggered → agent-triggered actions

A SaaS user generates tens to hundreds of actions per day — clicks,
form submissions, messages — each one explicitly human-initiated. A
product with one million users produces on the order of 10⁸ actions
per day.

An agent receives a task and autonomously iterates: reading data,
calling APIs, mutating state, invoking LLMs. Three amplifiers compound
the per-user action count:

- **Parallelism.** One user task may spawn multiple subagents working
  concurrently.
- **Continuous operation.** Agents run scheduled tasks, monitoring,
  and long-horizon research while the user is offline.
- **Cross-tool composition.** A single agent task may invoke dozens
  of tools, each one a separate database interaction.

At the same million-user scale, action volume plausibly jumps to
10¹⁰ or 10¹². This is not "somewhat more traffic"; it is a
1000×–10000× regime change. The shift is structural: it follows from
agents automating what was previously human-rate-limited, and it
persists regardless of which agent platform ultimately dominates.

### 1.2 The second amplifier: per-action payload growth

Action-count growth is only half the story. Individual action size
grows concurrently:

- **Traditional OLTP**: hundreds of bytes per action (an order row, a
  click event, a state transition).
- **Agent workload**: kilobytes to megabytes per action (full context
  + LLM response + tool-call results + intermediate state).

Multiplied together, write volume scales by roughly 1000×–10000× from
action count and another 10×–100× from payload growth: up to 10⁶×
total. This is why the transition from "Postgres + jsonb works" to
"Postgres + jsonb collapses" is abrupt. There is no intermediate
regime where incremental tuning absorbs the increase.

### 1.3 Implication for database design

If the action-volume claim holds, three consequences follow:

- **Existing operational databases are designed for the human-rate
  regime.** Linearly scaling them does not cover the agent regime.
- **Incremental optimization is insufficient.** Every architectural
  assumption — transaction model, replication, indexing, storage
  layout — was chosen against a workload that no longer obtains.
- **The work is tractable now.** The trend is underway; waiting
  several years means catching up rather than building ahead.

If the claim fails — if agents remain a minority of production
workloads and most traffic stays human-scale — the design in this
document is overbuilt. Postgres with jsonb remains adequate for
hundreds to thousands of agents, and the box architecture, object
storage foundation, and tunable transaction model become unjustified
complexity.

This document proceeds on the premise that the action-volume shift
is real.

---

## 2. Requirements

This section describes the workload without prescribing a solution.

### 2.1 Access patterns differ from OLTP

| Dimension | Traditional OLTP | Agent workload |
| :--- | :--- | :--- |
| Latency | p99 < 10 ms is a hard requirement | p99 < 100 ms is acceptable; LLM inference dominates end-to-end latency |
| Read/write ratio | Read-heavy (~10:1) | Write-heavy (hundreds of writes, handful of reads per agent run) |
| Value size | Sub-kilobyte | 1 KB – MB, occasionally larger |
| Access pattern | Random point lookups | Sequential in-session append + prefix scan |
| Concurrency | High-QPS small transactions | Moderate-QPS, many operations per transaction |

The single most load-bearing consequence is that **tail-latency
tolerance is looser than traditional OLTP** by an order of magnitude.
Every subsequent architectural choice is downstream of that budget.

### 2.2 State is semi-structured

Agent state is conversation history, tool-call traces, scratchpads,
and intermediate artifacts. These are inherently semi-structured:
relational schemas force frequent migrations; `jsonb` in a relational
database runs but loses native indexing, path-expression semantics,
and schema evolution.

The MongoDB document model was designed for web applications; the
agent workload fits that model better than it fits jsonb.

### 2.3 Lifecycle: activity drops off a cliff, not a curve

This is the largest single deviation from OLTP assumptions.

Traditional OLTP access is Zipf-distributed: hot keys are read and
written repeatedly, cold keys are touched occasionally, and the
distribution is continuous. There is no clean boundary.

Agent sessions have a **discontinuous** activity profile:

- During an active session (minutes to hours): high-frequency writes,
  append-dominant.
- After idle: activity drops abruptly, but not to zero. A user may
  resume days later, delayed feedback arrives, replay is used for
  debugging.
- Cold data is frequently **refined** into memory — summaries,
  embeddings, extracted entities — which later agents retrieve in a
  different shape than the raw events.

The consequences for storage:

- **In-place updates are rare.** Historical events are not modified;
  mutation breaks auditability and replay.
- **Appends continue at decaying frequency.** Active → idle → cold
  is a continuum, not a binary.
- **A two-state (active/archived) model is insufficient.** At least
  three states are required:
  - **Warm** — recently active or frequently accessed.
  - **Cold** — archived but still subject to append or reactivation.
  - **Archive** — effectively write-never, read-only via derivatives
    such as embeddings and summaries.

### 2.4 Working set is small; total state is large

A platform hosting 100K agents at 100 runs per day at 10 MB per run
adds 100 TB daily. The concurrently active working set is on the
order of a few GB.

This ratio — active set as a fraction of total — is in the permille
range, and it is structural. The cost of storing cold data cheaply
therefore drives the product's pricing competitiveness.

### 2.5 Query patterns are concentrated

Unlike OLTP's open-ended SQL, agent queries cluster into five
categories that jointly cover roughly 90% of traffic:

- **Session load** — all events for one session (highest frequency).
- **Prefix scan** — by agent id plus time range, for replay and audit.
- **Hybrid retrieval** — vector similarity plus structured filter,
  the core memory-layer query.
- **Full-text search** — primarily for debugging.
- **Aggregation** — monitoring and usage statistics.

The concentration is a design opportunity. A query surface of this
shape does not need a general-purpose optimizer.

### 2.6 Operations have a natural aggregation unit: the session

OLTP operations are discrete. Agent operations aggregate naturally by
session:

- All writes in one agent iteration belong to the same session.
- Session load is a whole-session scan.
- Hybrid retrieval almost always filters by `agent_id` or `session_id`.
- Replay and debugging operate on a session or a set of sessions.

If data layout aligns with this unit, much of the coordination cost of
distributed systems can be avoided entirely.

### 2.7 Transaction requirements are narrower than OLTP

Surveying the transaction needs of typical agent operations:

- **Single event append** — no transaction needed; one KV put suffices.
- **Append plus state update** — a single-document atomic write.
- **Context fork** — snapshot-read consistency, not a write transaction.
- **Multi-agent shared state** — usually modeled as append-only log
  plus last-write-wins; genuine serializability is rare.
- **Derived-data writeback** — pure insert with idempotent keys.
- **Checkpoint** — single-document atomic write.

The agent workload requires:

- **Single-document atomic writes** — universal baseline.
- **Snapshot reads** — for replay, debugging, context fork.
- **Cross-key atomic writes** — occasional, usually avoidable via
  document denormalization.

It does not require:

- Serializable isolation.
- Cross-row read-modify-write concurrency control.
- Strong transactional semantics in the style of a banking system.

OLTP databases amortize the cost of supporting unrestricted
concurrent read-modify-write — MVCC, locking, conflict detection —
across every write. Most of that cost is not recoverable in the agent
workload.

### 2.8 Multi-tenancy: from tens of tenants to millions

This is the requirement that most sharply invalidates existing
designs. Multi-tenant scenarios form a spectrum of complexity, and
the agent regime sits past the end of that spectrum.

**Type I — Internal multi-service infrastructure.** Canonical
examples: shared Kafka clusters, shared Redis fleets, internal data
platforms (Uber, Airbnb, Databricks).

- Tenant count: 10¹–10³.
- Each tenant is an internal service, maintained by a team.
- Core requirements: resource isolation, priority control.
- Data privacy is not a primary concern; schemas are
  service-independent.
- Standard solution: namespaces, quotas, pooling, QoS.

**Type II — SaaS multi-tenancy.** Canonical examples: Salesforce,
Shopify, Notion.

- Tenant count: 10²–10⁵.
- Adds **data privacy** (tenants must not see each other's data)
  and **schema evolution** (per-tenant feature flags, versioning,
  staged rollouts).
- Noisy-neighbor problems become SLA-relevant.
- Standard solution: row-level security, per-tenant schema
  versioning, independent backup/restore.

The Type I→II transition is additive: new dimensions, each linearly
scalable.

**Type III — Agent-platform multi-tenancy.** Tenant count: 10⁵–10⁷
(every agent is effectively a tenant), with extreme-sparsity traffic:

- At any instant, 99%+ of tenants are at 0 QPS.
- Most active tenants run at single-digit QPS.
- A small head of popular agents runs at high QPS.
- A subset occasionally "runs away" (bug-induced infinite loops).

The Type II→III transition is not additive. The fine-grained
per-tenant mechanisms that work at Type II scale break at Type III:

- **Per-tenant metering** — metering cost can exceed tenant revenue.
- **Per-tenant schemas** — schema metadata itself reaches PB scale.
- **Per-tenant quotas** — the quota check becomes the bottleneck.
- **Per-tenant backups** — backup infrastructure needs its own
  database.

Type III requires a different paradigm: **batching, shared
infrastructure, sampled metering, and coarse isolation.**

Under that paradigm, several new noisy-neighbor patterns emerge:

- **Cold-start storms.** 99% of tenants live in cold storage. When a
  few thousand cold tenants reactivate simultaneously, they
  collectively saturate object-storage bandwidth, local cache, and
  metadata services.
- **Long-tail resource fragmentation.** Each tenant consumes trivial
  state — kilobytes of metadata, a few connections, a handful of
  index entries. Summed across millions of tenants, this becomes the
  dominant operational cost.
- **Bursty write amplification.** A single agent spikes to hundreds
  of QPS. Hundreds of agents doing so simultaneously — a normal
  occurrence — is tens of times the baseline write pressure.
  Per-tenant rate limiting cannot detect the aggregate violation;
  per-box and global admission control is required.
- **Metering granularity problem.** Per-use billing is the natural
  model for agent platforms, but precise per-use metering at 10⁷
  tenants is prohibitive. Tiered metering — precise for the head,
  sampled for the tail — is necessary.
- **Blast-radius expansion.** At Type II, one bad tenant affects
  tens to hundreds of co-located tenants. At Type III, one bad
  tenant can affect tens of thousands. Finer-grained fault isolation
  (the "box" construct introduced in §4.5) contains this.

All of these trace back to a 1000×–100000× shift in tenant cardinality.

### 2.9 Self-hosted deployment is a first-class concern

Agent inputs and outputs routinely include PII, trade secrets, and
proprietary code. A large fraction of potential customers will not
allow this data to leave their network.

This constrains the deployment model:

- No complex Kubernetes operator that requires dedicated operators
  to run.
- No requirement that customers understand distributed database
  operations.
- Preferred shape: stateless containers plus managed cloud
  primitives (S3, Postgres, Redis).
- Monitorability and diagnosability must be designed in from the
  start, not retrofitted.

---

## 3. Design: What Abstractions to Expose

This layer specifies the data model, lifecycle concepts, query
primitives, and API surface. It does not specify storage-engine
choices.

### 3.1 Document as a first-class data model

"First-class document" is often marketing. A useful definition has
four concrete properties:

- **Storage**: documents are not shredded into per-field columns.
  The entire document is one stored value, avoiding the
  serialization/deserialization cost of reconstructing it on read.
- **Indexing**: nested fields can be indexed directly, including
  array expansion. The `tool_calls.*.error_type` pattern needs
  multikey-index semantics.
- **Query**: path expressions are native syntax, not string
  manipulation over a JSON column. The planner understands paths
  and pushes predicates accordingly.
- **Schema evolution**: DDL is not required. New fields are written
  directly; missing fields have well-defined read semantics, which
  requires an `exists`-style primitive in the query language.

Postgres `jsonb` satisfies the first two properties; the last two
are degraded. MongoDB's BSON model satisfies all four. An
agent-native database requires the MongoDB-style abstraction.

### 3.2 Locality as a first-order design principle

Given the aggregation unit identified in §2.6, locality is not an
optimization — it is the property whose violation collapses the
design's distinguishing characteristics.

Locality applies at five layers:

- **Data locality.** All events, state, and metadata for one session
  are physically co-located — same shard, same segment, adjacent
  storage blocks. Session load is a single sequential read, not a
  fan-out.
- **Index locality.** Each shard maintains its own secondary
  indexes. Queries carry `session_id` or `agent_id` filters that
  localize to a shard subset; a local index scan after shard
  selection is sufficient. Global indexes are not needed.
- **Transaction locality.** Most transactions are single-shard by
  construction (same-session operations). These use a lightweight
  single-shard path with no 2PC and no global timestamp. Cross-shard
  transactions remain available as an opt-in capability, but
  partition strategy makes them rare.
- **Compute locality.** Pushable computation — filtering,
  projection, simple aggregation, vector similarity, full-text
  matching — happens next to the data. Agent values are large
  (MB-scale), so avoiding data movement compounds.
- **Cache locality.** Active sessions reside entirely in local SSD
  cache. Operations on an active session hit local cache; only WAL
  writes cross the network.

The inverse of locality is not "slightly slower." It is "the
architecture loses its differentiation." A design that falls back to
cross-shard coordination, global indexes, and distributed
transactions by default has no meaningful advantage over a
conventional distributed OLTP system.

### 3.3 Transaction model: weak writes, strong reads, explicit strong consistency

Per the analysis in §2.7:

- **Write path: read-committed by default, single-document atomic
  write.** Most writes are single-document append or upsert;
  anything stronger than read-committed is unnecessary for these
  cases. Serializable is not provided by default because almost no
  workload needs it, and offering it adds coordination cost to
  every write.
- **Read path: snapshot reads by default.** A consistent
  point-in-time view matters for replay, debugging, and context
  fork. If the underlying KV layer supports MVCC, snapshot reads are
  free at the storage layer; the API layer needs to expose them.
- **Optional strong isolation, opt-in.** A minority of operations —
  shared counters across agents, quota tracking — need cross-key
  transactional semantics. These use an explicit API, not a default
  execution mode. Complexity is paid for only by callers who
  actually need it.

The overall shape — weak writes, strong reads, explicit strong
consistency — is analogous to DynamoDB's transactional write API
(opt-in, separate from normal puts) and is well-suited to agent
workloads.

### 3.4 Session lifecycle as a native concept

Lifecycle management is a database responsibility, not an application
convention:

- **Sessions are first-class objects.** They are not merely
  `session_id`-prefixed groups of events; they have metadata
  (`created_at`, `last_active_at`, `status`, `owner`).
- **Activity signals drive transitions.** Idle-timeout or explicit
  archive calls move a session between tiers; no hard
  `session.close()` contract is required.
- **Declarative lifecycle hooks.** Rules like "archive on 7-day
  idle" or "generate derivatives on archive" are schema
  declarations, not application cron jobs.
- **Reactivation.** A cold session is inexpensive to reactivate by
  appending or loading; no data migration is required.

### 3.5 Derived data as first-class, with a clean boundary

Cold agent data exists in multiple shapes: raw traces, summaries,
embeddings, extracted entities. Treating derivatives as an
application concern loses opportunities for shared transaction
visibility and unified querying.

The agent-native database owns storage, indexing, and query of
derivatives. It does not own their *generation*, because generation
requires LLM calls and would bind the database to a model vendor and
version.

The concrete split:

- **Derivative computation** (LLM calls) lives in the application
  layer or a companion service.
- **Derivative storage, indexing, query** are database
  responsibilities, sharing the transaction view with raw data and
  queryable alongside it.
- **Triggers** — the signal that a tier transition has happened —
  are provided by the database via lifecycle hooks plus webhooks.
  The application responds to the hook, generates the derivative,
  and writes it back.

The database schedules and stores; the application computes.

### 3.6 Tenants as logical units

Given the 10⁵–10⁷ tenant cardinality from §2.8, per-tenant physical
resources are infeasible — dedicated connection pools, per-tenant
index structures, per-tenant metadata rows do not scale.

Tenants must be logical:

- Partitioned for billing and isolation.
- Physically sharing larger resource units (e.g., 1000 tenants per
  shard, with prefix-based isolation within a shared index file).
- Adding a tenant is a metadata operation, not physical resource
  allocation.

This is consistent with "each tenant is a logical partition." A
partition is the unit of billing; the unit of physical resource
allocation is larger and is introduced in §4.5 as the box.

### 3.7 Query primitives

Per the query-shape analysis in §2.5, the minimal primitive set:

- **Session scan** — all events for a given `session_id`.
- **Prefix range** — primary-key prefix plus time range.
- **Hybrid retrieval** —
  `vector_similar(embedding, top_k=N) WHERE agent_id = X AND ts > Y`.
  Vector search and structured filter share a single query plan.
- **Full-text search** — composable with structured filters.
- **Append-and-fetch-last-N** — the highest-frequency atomic
  operation. Writes one event and returns the last N in a single
  round trip.
- **Real-time subscribe** (`GET .../subscribe`) — SSE tail of
  session events with optional `from_seq` replay. The change
  feed is the durable event log itself: no replication slot, no
  auxiliary table, the same primitive that serves the write
  serves the live read. JWT auth (HMAC or JWKS) for
  browser-direct subscribers; Basic auth for backend.
  Lag-too-far closes with `last_delivered_seq` so clients
  reconnect gap-free via `from_seq`. See README "Real-time
  subscriptions".
- **Ephemeral broadcast** (`POST .../publish`, `GET .../subscribe`):
  tenant-scoped channels for typing indicators, cursors, and other
  transient signals. In-memory only — no durability, no replay. JWT
  `channels` claim controls per-channel access. See README
  "Ephemeral broadcast" and spec
  `docs/superpowers/specs/2026-04-26-pubsub-phase2-design.md`.

Hybrid retrieval is the most differentiated of these. The difficulty
lies in the interaction between vector search and structured filter.
Implementing it inside the database — with filter and vector sharing
a data source and a transaction view — offers consistency guarantees
that external vector-search services cannot provide.

### 3.8 Context fork as a primitive

Whether fork belongs inside the database depends on whether
multi-agent architecture becomes a mainstream pattern.

The motivating use case is a parent agent spawning subagents that
need a snapshot of the parent's context. An application-layer deep
copy is expensive in both space and time. A database-layer
implementation is copy-on-write: `fork(session_id) -> new_session_id`
shares the underlying data until the child writes.

Arguments for inclusion:

- Underlying MVCC supports it naturally.
- Application-layer implementations cannot achieve the same
  performance.
- Multi-agent scenarios require it.

Arguments against:

- If multi-agent architecture loses to "one LLM switching roles,"
  the primitive has no users.

Current position: include, flagged as dependent on multi-agent
adoption.

### 3.9 What the database should not do

Scope discipline prevents product bloat:

- **LLM calls** (including automatic embedding generation) — the
  application's responsibility. Automating this binds the database
  to model choices and versioning.
- **Inter-agent messaging** — message queue or application
  coordination, not a database concern.
- **Tool execution** — self-evidently out of scope.
- **Workflow orchestration** — Temporal and LangGraph already own
  this; cross-disciplinary expansion loses focus.
- **Semantic caching** — middleware responsibility; mixing it into
  database concepts clouds the surface.

The default: capabilities live in the application layer unless they
pass all three of the following tests — performance requires
proximity to data, atomicity requires transactional guarantees, and
usage is broad enough that nearly every agent would use them.

---

## 4. Architecture

This layer describes concrete implementation choices: storage
engine, read/write paths, index structure, physical isolation.

### 4.1 Object storage as primary storage

Object storage should be the primary storage layer for a new
operational database aimed at agent workloads. The requirements from
§2 map onto object-storage properties almost one-for-one:

- **§2.1 Loose latency budget.** The 30–100 ms first-access latency
  of object storage is fatal for traditional OLTP and tolerable for
  agent workloads: active data lives in local cache, cold access
  absorbs into LLM-inference latency.
- **§2.4 Working-set / total-set ratio.** An object storage plus
  local cache model is the natural fit for a small active set on top
  of a large inactive one.
- **Data-volume growth rate.** Local SSD forces sharding at the
  terabyte scale, driving operational overhead. Object storage
  capacity is effectively unbounded.
- **§2.3 Lifecycle.** Cold data can transition to S3 Infrequent
  Access or Glacier classes, aligning with the warm/cold/archive
  tiering.
- **§2.8 Multi-tenant sparsity.** "Near-zero cost for an idle
  tenant" is only achievable on object storage. On physical disks,
  even megabyte-granularity per-tenant reservations waste petabytes
  at million-tenant scale.
- **§2.9 Self-hosting.** S3-compatible options (MinIO, Ceph) are
  mature. A stateless-container + S3 + Postgres/Redis shape is the
  lowest-operational-complexity self-hosting target.
- **§3.8 Context fork.** The immutability of objects plus metadata
  pointers is a native copy-on-write substrate. Fork is a metadata
  operation that points a new session at an existing set of
  objects.

There is also a cost argument. AWS us-east-1 pricing at the time of
writing:

| Storage class | Cost per GB-month |
|---|---|
| EBS gp3 | $0.080 |
| S3 Standard | $0.023 |
| S3 Infrequent Access | $0.0125 |
| S3 Glacier Instant | $0.004 |

For 1 PB of cold data, the difference is $80K/month (EBS),
$12.5K/month (S3 IA), or $4K/month (Glacier). At 95% cold
data — realistic for agent platforms — this ratio directly sets
pricing competitiveness.

Partial object-storage designs — where metadata, statistics, or
consensus state still live on local disk — inherit the operational
complexity of both tiers. The cleaner commitment is: metadata in
Postgres, coordination in Redis, data in object storage.

### 4.2 A distributed KV middle layer with tunable transactions

Object storage is too coarse-grained for per-event reads and writes.
A distributed KV layer sits between the two, responsible for:

- WAL intake for high-frequency writes (one-request-one-object into
  object storage).
- Transaction coordination, on demand.
- Local caching of active data.
- Range and prefix scans.

TiKV is a reasonable reference implementation for this layer —
production-mature, native range-scan support, MVCC snapshot reads,
established object-storage integration. Alternatives (FoundationDB,
a lighter etcd derivative, or a purpose-built implementation) satisfy
the same requirements. The choice is not central; what matters is
the capability set and the avoided complexity.

**Tunable transactions.** TiKV implements full distributed ACID
transactions (Percolator, 2PC, globally synchronized timestamps via
PD). This is essential for financial workloads and unnecessary
overhead for most agent operations, which pay coordination cost for
capabilities they do not use.

Per §3.3 and §3.2, on a TiKV-like base:

- **Default: single-shard path.** Local MVCC only, no 2PC, no global
  timestamp wait.
- **Cross-shard transactions: opt-in.** Full Percolator path, only
  when explicitly requested.
- **Snapshot reads: always available.** Already provided by TiKV;
  surface it in the API.

Partition strategy carries the rest of the weight: same-session data
co-located on the same shard, same-tenant data grouped into a small
set of shards. Business operations that are naturally single-tenant
or single-session become single-shard transactions by construction.
**Transaction boundary equals business boundary equals physical
boundary.** Distributed-transaction cost is designed out of the data
layout, not handled at runtime.

A tangential observation: TiKV's potential is underrated by the TiDB
positioning. Wrapping TiKV in a MySQL-compatible relational database
is the wrong abstraction for agent workloads — not because the
relational model is wrong (though it is a poor match for semi-
structured state), but because SQL-compatible protocols are not
what document-shaped agent data needs.

### 4.3 Document interface, with Postgres compatibility as secondary

If resource constraints force one interface, the document interface
is the stronger choice, for three reasons:

- **Better competitive positioning.** The Postgres-compatible
  hosted-database market is saturated (Supabase, Neon, PlanetScale,
  Aurora, AlloyDB). Document scale-out is crowded only at the
  endpoints (Mongo Atlas, DynamoDB); the middle is empty.
  SurrealDB is niche; FerretDB is protocol translation without a
  scaling story.
- **Stronger technical fit.** Native document plus object-storage
  scale is genuine differentiation for agent workloads. `jsonb` is
  a workable but compromised substitute.
- **Lower engineering complexity.** Postgres compatibility requires
  a query optimizer, a type system, and an extension ecosystem. A
  document interface requires query translation plus secondary
  indexes; the heavy lifting is not at the storage layer.

A pragmatic middle path:

- **Storage layer**: distributed KV plus object storage.
- **Abstraction layer**: document model with native
  put/get/query/index APIs.
- **Interface layer**: native document API primary, MongoDB
  wire-protocol compatibility as a secondary surface (for
  ecosystem access), and optionally a limited Postgres-compatibility
  layer covering jsonb-relevant syntax (without committing to full
  Postgres semantics).

The central bet is on the document abstraction.

### 4.4 Read and write paths

**Write path.** Request → WAL (one-object-per-request into object
storage) → background asynchronous compaction into segments
(clustered by session_id) → per-segment local indexes (row store,
inverted index, column store, vector index, Bloom filter) → metadata
to Postgres.

**Read path.** Query → parse → merge three sources: unprocessed WAL
entries, compacted but unindexed data, fully indexed data. Active
sessions are entirely in local cache; cold sessions are pulled from
object storage on demand.

**Tier-to-storage mapping.**

- **Warm** — segments cached on local SSD.
- **Cold** — segments in S3 Standard; metadata remains lightweight.
- **Archive** — raw data in S3 IA or Glacier; derivatives
  (embeddings, summaries) retained in the cold tier for query.

### 4.5 Physical isolation: the cell / box

Per §2.8, the physical isolation unit cannot be "one tenant" —
metadata explodes. It also cannot be "global pool with per-tenant
quotas" — blast radius is too large.

The appropriate granularity is intermediate: a group of tenants
sharing one set of physical resources, with logical isolation
between tenants and physical isolation between groups. This unit is
called a **box** (or a cell, matching AWS's cell-based architecture
terminology):

- Each box hosts a fixed-size tenant population (e.g. 1 000 – 10 000).
- Within a box, tenants share compute, cache, and index files
  (prefix-isolated).
- Between boxes, resources, failure domains, and rate limits are
  independent.
- A misbehaving tenant's impact is bounded to its box.
- Boxes are the unit of scale-out, migration, and upgrade.

Beyond fault isolation, boxes also serve as the **physical
aggregation unit for long-tail tenants**. Tenants in the same box
share an object-storage prefix, a local cache budget, and batched
WAL writes. These amortization effects drive the marginal cost of
an idle tenant toward zero.

Supporting mechanisms:

- **Per-box quotas** — bound total resource use by a single box,
  preventing cold-start storms.
- **Per-tenant quotas** — bound single-tenant burstiness,
  constraining runaway agents.
- **Global admission control** — protect system-wide resources
  (object-storage bandwidth, metadata service).
- **Tiered metering** — precise for head tenants, sampled for the
  long tail, keeping metering overhead bounded.

---

## 5. Implementation Trade-offs

Each item here is an engineering decision with real cost on both
sides. None have a free answer.

### 5.1 Session-clustered compaction

LSM architectures do not guarantee that spans of one session end up
in the same segment. If spans scatter across memtables and SSTs,
session load merges results from multiple files.

Clustering requires either session-aware compaction policy or
routing at write time. The payoff is direct: O(1) seeks on a single
SST versus O(log N) seeks plus merge on several. The cost is
compaction complexity and potentially higher write amplification.

### 5.2 Write amplification from local secondary indexes

Efficient nested-field queries via the document interface require
secondary indexes. Local indexes update atomically with the primary
write (same-shard lightweight transaction); the write amplification
is lower than global-index designs but nonzero.

Since agent workloads are write-heavy to begin with, the acceptable
amplification needs measurement against a real workload.
Mitigations:

- **Asynchronous indexing** — acknowledge the write immediately,
  build the index in background, and read-merge WAL for freshness.
- **Selective indexing** — index only frequently queried fields.
- **Configurable granularity** — session-level versus event-level
  indexes.

### 5.3 Nested-field indexing: shred or don't shred

Indexing paths like `tool_calls.3.result.error_type` has two
approaches:

- **Shred.** Decompose nested fields into separate KV pairs for
  indexing. Efficient for lookup, costly when schemas change
  (reintroducing the warehouse shred-JSON problem).
- **Don't shred.** Keep documents intact and index via path
  pointers. Flexible under schema change, less efficient at lookup
  time.

MongoDB's multikey-index approach is a tuned form of shredding.
Reimplementing it on a TiKV substrate requires a redesign; the
tradeoff is not captured by simply lifting MongoDB's structures.

### 5.4 Hybrid retrieval execution plans

Combining vector search with structured filters admits three
execution orders:

- **Pre-filter.** Apply structured filter first to narrow the
  candidate set, then vector search. Explodes when the filtered set
  is large.
- **Post-filter.** Vector top-K first, then filter. Produces zero
  recall if top-K is heavily filtered.
- **Joint.** Pre-filter index plus vector index coordinated at
  scan time. Complex to implement, best quality.

Milvus, Weaviate, and Qdrant all contest this space. Production-grade
quality requires significant engineering investment — and this is
the core differentiator of the hybrid-retrieval primitive.

### 5.5 Lifecycle hook implementation

An `on_idle_timeout` mechanism requires a scheduler. Two options:

- **In-database scheduler.** Simple integration, tight coupling.
- **External orchestrator** (Temporal, Kubernetes CronJobs). Adds a
  dependency but decouples lifecycle rules from database internals.

The external option is preferred: agent platforms require a workflow
engine anyway, and reusing it avoids a scheduler inside the
database. The database's responsibility is to emit events; external
systems decide how to react.

### 5.6 One-object-per-request WAL cost

The one-object-per-request WAL design maximizes write throughput (no
coordination) and guarantees immediate durability. The cost is
object-storage API calls: S3 PUT at roughly $0.005 per 1000 requests.
At high throughput, this becomes material.

Options for mitigation:

- **Batching** — trade a small amount of latency for amortized API
  cost.
- **Local WAL buffering** — batch in memory briefly before the
  object-storage PUT.

The right choice depends on the concrete throughput target and
latency budget of the deployment.

### 5.7 Box sizing

Tenants per box is a primary design parameter:

- **Smaller box.** Better isolation, smaller blast radius, worse
  resource utilization (each box pays fixed baseline overhead).
- **Larger box.** Better utilization, stronger amortization effects,
  larger blast radius and bigger noisy-neighbor exposure.

The heuristic range of 1 000 – 10 000 tenants per box is a starting
point. Optimal sizing depends on inter-tenant burst correlation — a
property that cannot be measured without live traffic from the
target workload.

### 5.8 Cold-start concurrency control

Thousands of cold tenants reactivating simultaneously will saturate
object-storage bandwidth. The design needs:

- **Per-box reactivation queuing** to cap concurrent reactivations.
- **Priority ordering** — user-initiated requests ahead of
  background tasks.
- **Always-hot metadata.** Only data should be cold; metadata
  lookups should never touch object storage directly, or cold-start
  becomes a double penalty.

---

## 6. Alternatives: Building on Existing Tools

The agent-native database described here is one shape. Teams can
assemble comparable functionality from existing tools. This section
covers three common assemblies, their appropriate scales, and where
they break.

### 6.1 Alternative A — Postgres-plus-S3

**Components.** Postgres (jsonb) for agent state, session events,
event logs; pgvector for embedding search. Redis for caches and
rate limits. S3 for cold archival, managed by the application.
Application layer does all coordination.

**Strengths.** Single source of truth, unified transactions, simple
operations, fast iteration. Managed Postgres services (Supabase,
Neon) reduce ops cost further.

**Breakdown points.**

- Autovacuum fails to keep up with agent-level write rates; dead
  tuples degrade query performance.
- Deeply nested jsonb queries perform poorly.
- GIN index size grows sharply with session count, slowing updates.
- Multi-tenant isolation is application-only; complexity escalates
  past ~10K tenants.
- Hot/cold tiering requires application-driven data migration with
  visible impact on live traffic.

**Appropriate scale.** Fewer than 1 000 tenants, small per-agent
data volumes, fewer than 1 000 QPS. Most early-stage agent startups
operate in this regime.

### 6.2 Alternative B — Multi-tool decomposition

**Components.** Postgres for relational metadata (users, billing,
config). MongoDB or DynamoDB for session events and tool-call
traces. Pinecone, Weaviate, or Qdrant for vector search.
Elasticsearch (optional) for full-text. S3 for raw-trace archival.
Application layer handles cross-system coordination and consistency.

**Strengths.** Each workload runs on a specialized tool, with good
single-system performance. Each layer scales independently.

**Breakdown points.**

- Three to five databases to operate; operations cost scales
  linearly.
- No cross-database transactions; the application handles partial
  failures.
- Hybrid retrieval (vector plus filter) splits into two queries,
  losing the single-plan optimization.
- Consistency semantics are application-enforced; failure-recovery
  logic is complex.
- Per-tool tenant management; no unified multi-tenant view.
- Bill aggregation across three to five managed services is
  substantial.

**Appropriate scale.** 1 000 – 10 000 tenants, a dedicated
infrastructure team, and business requirements that reward
per-workload tuning. MongoDB Atlas plus Pinecone is a common
instance.

### 6.3 Alternative C — Custom build (Brainstore-shape)

**Components.** TiKV or FoundationDB as distributed KV. S3 for
cold data and WAL. Tantivy or Lucene for inverted indexes.
Postgres for metadata. Redis for coordination. Proprietary
indexing, query, and lifecycle layers.

**Strengths.** Fit-for-purpose performance, controllable cost
structure. Brainstore is the archetype of this path.

**Breakdown points.**

- Engineering effort is substantial — Braintrust spent roughly two
  years.
- High team requirements: experience in distributed systems and
  storage engines.
- Maintenance cost is continuous; each component needs on-call
  coverage.
- Not a product; new engineers need extensive documentation to
  onboard.

**Appropriate scale.** Database is core to the business, existing
solutions have hit hard limits, and the company has the funding and
talent to sustain a long build. Typical context: a Series B+
agent-platform company with a specific ceiling problem.

### 6.4 What no assembly solves

Five problems are not addressed by any of the three assemblies above.
They define the space for a purpose-built agent-native database:

1. **No native session lifecycle.** "Session idles → cools →
   generates memory → can reactivate" is always application-written
   under these assemblies.
2. **No context fork.** Subagent context snapshots are
   application-layer deep copies in every alternative.
3. **Multi-tenancy ceiling at tens of thousands.** All three tool
   combinations collapse at 10⁶+ tenant cardinality.
4. **Non-atomic hybrid retrieval.** Vector plus filter is always
   multi-step, giving up the single-plan optimization space.
5. **Cold data is structurally expensive.** None of the assemblies
   achieves "95% of data stored near-free."

---

## 7. Product Positioning

If the premise from §1 holds — agent action volume rises 1 000×
or more — the operational database requires redesign. Combining the
decisions above, the distinguishing claims are:

- Document as the primary data model, with native semi-structured
  support.
- Object storage as primary storage; scale is effectively unbounded
  and cost is low.
- Locality as a design principle across data layout, indexing,
  transactions, and compute.
- Tunable transactions — lightweight single-shard by default,
  opt-in cross-shard.
- Native session lifecycle management driven by business semantics,
  not operator cron.
- Hybrid retrieval (vector + structured + full-text) under a single
  transaction view.
- Box architecture supporting million-scale tenant populations
  with near-zero marginal cost per idle tenant.
- Logical tenant isolation, physical resource sharing, usage-based
  metering.

Competitive positioning:

- **Versus DynamoDB.** Lower cost (object-storage structural
  advantage), document semantics better suited to agent data,
  native hybrid retrieval.
- **Versus MongoDB Atlas.** Better scale, lower cost, first-class
  agent primitives.
- **Versus Postgres (Supabase, Neon).** Document-native data model,
  agent-workload-tuned execution.
- **Versus Pinecone / Weaviate.** Full operational database rather
  than a memory sidecar, with integrated transactional semantics.

---

## 8. Open Questions

The positions below are load-bearing for the design but not fully
resolved. Each one, reversed, changes what the database should be.

### 8.1 Is context fork a real requirement?

The primitive is differentiating: MVCC supports it naturally, the
application layer cannot match it, and multi-agent architecture
requires it. The dependency is on multi-agent architecture becoming
mainstream. If "one LLM switching roles fast" proves more effective
than "parent spawns subagents with independent context," the
primitive's user base shrinks.

### 8.2 Who defines the session boundary?

Agent frameworks disagree: LangGraph threads, CrewAI crew runs,
in-house agent tasks. No unified session concept exists. Requiring
the application to declare session boundaries may be too high a
bar; falling back to idle-timeout-based inference may be too coarse.
The choice affects every API that operates on a session.

### 8.3 Who pays for derivative-generation LLM calls?

The database cannot cover embedding or summarization costs on
behalf of users. The derivative pipeline therefore must be
application-scheduled, database-stored. This narrows the database's
role to "supports hybrid retrieval" plus "schedules lifecycle
hooks" — whether that's enough differentiation to build a product
around is the open question.

### 8.4 Should cold raw data be preserved at all?

If 95% of cold data is never accessed in raw form and only
derivatives are queried, users should plausibly be offered a
"derivatives-only, delete raw" mode. This choice has large effects
on cost structure and product positioning.

### 8.5 Will MCP absorb agent database needs?

Many agents currently use filesystems plus markdown for state,
bypassing databases entirely. If MCP standardizes structured-data
access, it may either obviate dedicated agent databases or create
conditions under which more agents become database-dependent. The
outcome is not predictable from first principles.

### 8.6 Is observability a distinct concern?

Agents frequently need replay, time-travel, and run-to-run
comparison. This implies specific versioning semantics. Whether
this deserves to be an independent design axis or can be derived
from the combination of session lifecycle plus append-only plus
snapshot reads is unresolved.

### 8.7 Should the transaction model be tiered as a product?

§3.3 proposes "weak writes by default, strong consistency on opt-in."
Whether this should be packaged as pricing tiers (basic / standard /
enterprise, differentiated by transactional guarantees) is a
product-strategy question. Tiering enables segmented pricing but
adds complexity and risks mis-selection by customers.

### 8.8 What's the right box size?

The 1 000 – 10 000 heuristic range from §4.5 is directional. The
optimal number depends on inter-tenant burst correlation, which
cannot be measured before live traffic exists at the target scale.
The validation strategy is itself an open question.

---

## References

- Ankur Goyal. [AI observability is a database problem: how
  Brainstore works](https://x.com/ankrgyl/status/2041206959848735107).
- PingCAP. [How Manus 1.5 uses TiDB X to let agents ship full-stack
  apps at scale](https://www.pingcap.com/blog/how-manus-1-5-uses-tidb-x-to-let-agents-ship-full-stack-apps-at-scale/).
- [SurrealDB](https://surrealdb.com/) — multi-model interface on
  top of TiKV.
