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

It is a design document, not a product plan.

**Three docs, three jobs.** This file is the **WHY** —
motivation, workload requirements, design principles. The
durable parts of the answer to "why does this database exist."
For the **WHAT** (current architectural commitments — identity
model, layering, real-time, deployment, operability), see
[`DESIGN.md`](DESIGN.md). For the **HOW**, today (current
implementation state), see [`ARCHITECTURE.md`](ARCHITECTURE.md).

---

## What AgentDB is, by elimination

To position AgentDB precisely, it helps to distinguish three
kinds of stores an AI agent actually needs. They have
different shapes, different access patterns, and different
vendors. Most production agent stacks end up running all
three.

```
       ┌────────────────────────────────────────────┐
       │              An AI agent                   │
       └─────────────┬──────────┬─────────┬─────────┘
                     │          │         │
                     ▼          ▼         ▼
              ┌──────────┐  ┌────────┐  ┌──────────┐
              │   1.     │  │  2.    │  │   3.     │
              │ Memory / │  │Runtime │  │ App OLTP │
              │knowledge │  │/ state │  │          │
              │  store   │  │/context│  │ Postgres │
              │          │  │        │  │ MySQL    │
              │ MemGPT   │  │AgentDB │  │ MongoDB  │
              │ Mem0     │  │        │  │ DynamoDB │
              │ Letta    │  │        │  │          │
              │ LangChain│  │        │  │          │
              │  memory  │  │        │  │          │
              └──────────┘  └────────┘  └──────────┘
              "what should  "what just  "the user's
               I remember"   happened"   business data"
```

**1. Memory / knowledge store.** Not a pure database — a
memory and knowledge management layer. Cumulative facts the
agent has learned, distilled summaries of past sessions,
semantic indices over a corpus, retrieval-augmented context.
Often involves LLM-driven summarization, episodic-to-semantic
distillation, importance scoring, and compression. Examples:
LangChain memory, MemGPT, Mem0, Letta. The hard problem here
is *what to remember and how to compress it* — the schema is
light; the intelligence is heavy.

**2. Runtime / state / context store.** The agent's working
memory during execution: every event of every session — user
turns, model outputs, tool calls, tool results, observations.
Append-heavy, retrievable by recency + similarity, scoped per
session, branchable via fork, with a lifecycle from active to
archive. **This is AgentDB's scope.** The hard problem here
is *throughput, multi-tenant isolation, and cheap storage at
scale* — the schema is mostly fixed; the volume and operational
shape are what matter.

**3. The application's own database.** The product an agent
operates inside still has rows: users, accounts, orders,
calendar events, documents, CRM records. Apps built on
Lovable, Replit, Vercel, etc. need a general-purpose OLTP
database for that — Postgres, MySQL, MongoDB, DynamoDB. The
hard problem here is *transactions, joins, schema evolution* —
exactly what general-purpose databases solve.

These three overlap at the edges (a memory system may be
backed by a vector DB; a runtime store can persist
LLM-generated summaries; an OLTP database can serve session
state for a small app), but the workload shapes are distinct
enough that one system rarely serves all three well at scale.

AgentDB lives in slot #2. It does not try to:

- **Replace memory frameworks (#1).** AgentDB stores raw
  events; it does not decide what's worth remembering or how
  to summarize. A memory system layered above AgentDB writes
  its distilled artifacts back as events, with retrieval
  scoped to the session.
- **Replace your application's OLTP database (#3).** AgentDB
  is event-log-shaped, not row-mutation-shaped. CRM updates,
  payment records, multi-row ACID transactions belong on
  Postgres / MySQL / MongoDB. AgentDB sits alongside,
  capturing what the agent did and why.

The motivation that follows in §1 is specifically about the
runtime / state / context workload — why agent-driven traffic
is structurally different from human-driven OLTP at *that*
layer.

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

---

## 3. Alternatives: Building on Existing Tools

The agent-native database described here is one shape. Teams can
assemble comparable functionality from existing tools. This section
covers three common assemblies, their appropriate scales, and where
they break.

### 3.1 Alternative A — Postgres-plus-S3

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

### 3.2 Alternative B — Multi-tool decomposition

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

### 3.3 Alternative C — Custom build (Brainstore-shape)

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

### 3.4 What no assembly solves

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

## 4. Product Positioning

If the premise from §1 holds — agent action volume rises 1 000×
or more — the operational database requires redesign. The
distinguishing claims of the design that follows are:

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
- A `box` / `tablet` architecture supporting million-scale tenant
  populations with near-zero marginal cost per idle tenant.
- Logical tenant isolation, physical resource sharing, usage-based
  metering.
- Real-time event subscription as a first-class primitive,
  durable + replayable for the agent log and ephemeral for
  transient signals.

### 4.1 Where this fits, and where it doesn't

Honest positioning attracts the right adopters by repelling
the wrong ones. AgentDB is **not** a general-purpose database.
For most early-stage AI startups, the right answer is still
Postgres + pgvector + Redis. AgentDB starts paying off when
two or more of these are true:

- A **multi-tenant agent platform** (many distinct customers'
  agents on one stack) where per-tenant isolation, quotas, and
  admission control would otherwise have to be built from scratch.
- **Long-running sessions** at scale — hundreds to thousands
  of events each, where automatic warm → cold → archive
  tiering avoids hand-rolled `pg_partman`-and-friends work.
- **Fork as a first-class operation** in the agent design —
  branching into "what if A vs B" timelines, comparing
  branches. Few teams use this today; the ones that do find
  Postgres impressively awkward at it.
- **Hybrid retrieval ceiling on pgvector + tsvector** has
  been hit and the operational burden of two indexers is
  showing.
- **The agent-loop hot path is bottlenecked on
  append-then-read-recent-N**, and the round-trip savings of
  `append_and_fetch` show up in the latency budget.

For a workload where zero or one of these is true, AgentDB is
overkill: friction with no payoff, and the right answer is to
keep building on what's already there.

### 4.2 What it isn't

Three explicit non-replacements:

- **Not an OLTP system of record.** Cross-entity ACID
  transactions (update CRM customer + linked deal + audit
  row atomically) belong on Postgres. AgentDB's session-log
  shape doesn't offer that and isn't intended to.
- **Not a SQL analytics engine.** No joins across sessions,
  no GROUP BY across the corpus, no columnar scan path. Use a
  warehouse or DuckDB for that.
- **Not a general-purpose search engine.** The BM25 indexer
  is intentionally vanilla (whitespace + lowercase, no
  stemming, no language models). For real search use
  Elasticsearch / Tantivy / Lucene.

The design line is: a specialized database for agent-runtime
workloads. The right comparisons are with the *shape* of agent
state — not with general-purpose databases on their home turf.

---

## 5. Open Questions

The positions below are load-bearing for the design but not fully
resolved. Each one, reversed, changes what the database should be.

### 5.1 How load-bearing does context fork turn out to be?

Fork is implemented (MVCC-backed copy-free) and demoed in
every quickstart. The remaining open question is *how much
agent design actually uses it*. The original concern — would
multi-agent platforms branch into independent contexts? — has
not been settled by the market yet. If "one LLM switching
roles fast" continues to dominate over "parent spawns subagents
with independent context," the fork primitive sits there
correct but lightly used. If multi-agent goes mainstream, fork
turns out to be the most-differentiating primitive in the
design. The bet was made; it's unlikely to be wrong, but the
magnitude of the payoff is still measured in years.

### 5.2 Who defines the session boundary?

Agent frameworks disagree: LangGraph threads, CrewAI crew runs,
in-house agent tasks. No unified session concept exists. Requiring
the application to declare session boundaries may be too high a
bar; falling back to idle-timeout-based inference may be too coarse.
The choice affects every API that operates on a session.

### 5.3 Who pays for derivative-generation LLM calls?

The database cannot cover embedding or summarization costs on
behalf of users. The derivative pipeline therefore must be
application-scheduled, database-stored. This narrows the database's
role to "supports hybrid retrieval" plus "schedules lifecycle
hooks" — whether that's enough differentiation to build a product
around is the open question.

### 5.4 Should cold raw data be preserved at all?

If 95% of cold data is never accessed in raw form and only
derivatives are queried, users should plausibly be offered a
"derivatives-only, delete raw" mode. This choice has large effects
on cost structure and product positioning.

### 5.5 Will MCP absorb agent database needs?

Many agents currently use filesystems plus markdown for state,
bypassing databases entirely. If MCP standardizes structured-data
access, it may either obviate dedicated agent databases or create
conditions under which more agents become database-dependent. The
outcome is not predictable from first principles.

### 5.6 Is observability a distinct concern?

Agents frequently need replay, time-travel, and run-to-run
comparison. This implies specific versioning semantics. Whether
this deserves to be an independent design axis or can be derived
from the combination of session lifecycle plus append-only plus
snapshot reads is unresolved.

### 5.7 Should the transaction model be tiered as a product?

The "weak writes by default, strong consistency on opt-in"
posture (DESIGN.md §3.4) raises a product-strategy question:
should this be packaged as pricing tiers (basic / standard /
enterprise, differentiated by transactional guarantees)?
Tiering enables segmented pricing but adds complexity and
risks mis-selection by customers.

### 5.8 What's the right box / tablet size?

The 1 000 – 10 000 tenants-per-tablet heuristic range
(DESIGN.md §1.5) is directional. The optimal number depends
on inter-tenant burst correlation, which cannot be measured
before live traffic exists at the target scale. The
validation strategy is itself an open question.

---

## Recommended pattern: config-as-events

AgentDB doesn't ship a configuration store. Instead, it provides the
primitives to put configuration *in AgentDB sessions* and reduce-over-events
at read time. This pattern works well for agent platforms, multi-tenant
SaaS overrides, and any case where "configuration changes are themselves
historical data worth keeping."

### Convention

- Each tenant has a session named `_config:<scope>` (e.g., `_config:agents`).
- Each event in the session has `kind: "<scope>_override"` plus the
  override target and a patch.
- Code holds the *defaults*. The session events hold the *overrides*.
- At read time, fold the events in seq order onto the code defaults.

### Example

```ts
async function loadAgentConfig(client, tenant, role) {
  const defaults = CODE_DEFAULTS[role];
  const session = client.session("_config:agents", { tenant });
  const resp = await session.append({ kind: "_noop" }, { fetchLastN: 1024 });
  const events = resp.recent.filter((e) => e.payload?.kind !== "_noop");
  const patch = events
    .filter((e) => e.payload?.kind === "agent_override" && e.payload?.role === role)
    .reduce((acc, e) => ({ ...acc, ...e.payload.patch }), {});
  return { ...defaults, ...patch };
}

// Append an override:
await session.append(
  { kind: "agent_override", role: "engineer", patch: { systemPromptSuffix: "be terse" } },
  { fetchLastN: 0 },
);
```

### Why this is good

- **Event-sourced.** Free history of every config change. Roll back by reading older events.
- **No new database.** Same primitives as everything else.
- **Multi-tenant clean.** Each tenant's config lives inside that tenant — no cross-tenant access.
- **Hot-reloadable.** Reload config at the start of each agent iteration; new overrides take effect immediately.
- **Inspectable.** Just events; can be queried, replayed, exported.

### Trade-offs

- "Current value" is reduce-over-events, slower than a key-value `GET`.
  At AgentDB-typical config session sizes (tens to low-hundreds of events),
  this is negligible. For huge config sessions, periodic snapshotting can
  be added.
- The `_noop` append-and-fetch trick used to read the session log writes a
  small placeholder event each time. Fine at human-update frequency; for
  high-frequency reloads, consider a dedicated `fetch` helper at the SDK
  layer.
- No schema enforcement. Up to the application to validate patches.

## Recommended pattern: plan-as-events

Plans are mutable. A real agent revises its plan as it learns what
works and what doesn't. Storing a plan as a row you `UPDATE` loses the
*reason* a step changed, the *order* steps completed, the *observations*
the agent made along the way. Storing it as an event log keeps all of
that.

The pattern: dedicate one session per run (e.g., `{runId}:_plan`) and
append four kinds of events.

| Event kind        | Producer            | Payload                                                              |
|-------------------|---------------------|----------------------------------------------------------------------|
| `plan_revised`    | The planner agent   | `{ steps, reason }` — the entire step list, replaced wholesale.      |
| `step_dispatched` | The planner agent   | `{ step_id, assigned, text }` — handing the step off.                |
| `step_completed`  | The executing agent | `{ step_id, status, summary, from }` — outcome.                      |
| `observation`     | Auto-emitted        | `{ from, toolName, payload, relates_to_step? }` — any tool-call ran. |

Read time: reduce events into a `{ steps, observations }` snapshot. The
latest `plan_revised` defines the step list; later `step_dispatched` and
`step_completed` events flip individual step statuses. `observation`
events accumulate but don't change steps — they're the planner's
context for re-planning.

Why event-sourced plans:

  - **Replay**: every run produces a self-contained log. Step through it.
  - **Resume**: a process restart loads `_plan`, reduces, and the planner
    continues exactly where it left off.
  - **Fork**: `forkSession("{runId}:_plan", "alt-strategy")` creates an
    O(1) copy-on-write branch — explore an alternate plan without
    disturbing the main run.
  - **Observability**: tool calls auto-emit observation events into the
    same log. The plan session reads like an executive briefing.

---

## References

- Ankur Goyal. [AI observability is a database problem: how
  Brainstore works](https://x.com/ankrgyl/status/2041206959848735107).
- PingCAP. [How Manus 1.5 uses TiDB X to let agents ship full-stack
  apps at scale](https://www.pingcap.com/blog/how-manus-1-5-uses-tidb-x-to-let-agents-ship-full-stack-apps-at-scale/).
- [SurrealDB](https://surrealdb.com/) — multi-model interface on
  top of TiKV.
