# AgentDB Product Thesis

AgentDB is a runtime database for AI agent platforms. It stores the durable
execution state that lets agent systems recover, inspect, resume, replay, fork,
and audit work.

This document is the **why**: motivation, workload requirements, design
principles, positioning, and product-shaping questions. It avoids tracking
feature-by-feature implementation status; for the current design contract and
status table, see [`design.md`](design.md). For the live gap tracker, see
[`../product-roadmap.md`](../product-roadmap.md). For the implementation shape, see
[`architecture.md`](architecture.md).

## What AgentDB Is, By Elimination

An AI agent platform usually needs three different persistence layers:

```text
AI agent platform
    |
    +-- memory / knowledge: what should the agent remember?
    +-- runtime / session: what just happened, and how can it continue?
    +-- application OLTP: what is the user's business data?
```

**Memory / knowledge store.** This layer manages cumulative facts, summaries,
semantic search, importance scoring, and compression. Examples include
framework memory systems, vector databases, and long-term knowledge stores. The
hard problem is deciding what to remember and how to retrieve it.

**Runtime / session store.** This layer records every execution event: user
messages, model outputs, tool calls, tool results, observations, artifacts,
records, policy decisions, forks, and lifecycle state. It is append
heavy, session scoped, replayable, and recoverable. This is AgentDB's scope.

**Application database.** Product data such as users, orders, billing records,
calendars, CRM objects, and transactions belongs in an application database.
AgentDB does not replace Postgres, MySQL, MongoDB, DynamoDB, or a warehouse.

These layers overlap at the edges. A memory system may store summaries derived
from AgentDB events. An application database may store lightweight session state
for a small product. A vector database may index AgentDB records. The point is
that the workload shapes are different enough that one system rarely serves all
three well at scale.

AgentDB owns the runtime/session layer. It does not run models, execute tools,
schedule workflows, rank memories, or replace the system of record for the
application.

## Why Now

The case for an agent runtime database rests on a workload shift: agents can
increase action volume and payload size per end user by orders of magnitude.

A human-driven SaaS user produces tens to hundreds of explicit actions per day:
clicks, form submissions, messages, edits. An agent receives a task and
iterates autonomously: reading data, calling tools, writing observations,
invoking models, spawning subagents, and monitoring state while the user is
offline.

Three amplifiers compound the action count:

- **Parallelism.** One user task can fan out into multiple subagents or tool
  workers.
- **Continuous operation.** Agents run scheduled work, monitors, and long-running
  research outside the user's active session.
- **Cross-tool composition.** A single task can call dozens of tools, each with
  inputs, outputs, errors, retries, and observations worth preserving.

Payloads also grow. Traditional OLTP actions are often hundreds of bytes. Agent
events are commonly kilobytes and can reach megabytes when they include traces,
tool outputs, model responses, logs, or artifact references.

The important implication is not that existing databases cannot store this data.
They can. The issue is that agent runtime semantics become conventions scattered
through application code: which sequence is authoritative, how retries are
deduplicated, how a fork sees parent history, how artifacts are retained, how a
dashboard follows progress, and how an agent wakes after a crash.

AgentDB makes those semantics explicit infrastructure.

## Workload Requirements

### Access Patterns Differ From OLTP

| Dimension | Human-driven OLTP | Agent runtime |
|---|---|---|
| Latency | Often p99 under 10 ms | Usually dominated by model/tool latency |
| Read/write ratio | Often read-heavy | Append-heavy during runs |
| Value size | Small structured rows | JSON events, traces, artifacts, model/tool output |
| Access pattern | Point lookups, joins, transactions | Session append, recent replay, scoped search |
| Lifecycle | Continuously active hot data | Active sessions, idle sessions, archived runs |

AgentDB should optimize for the common runtime path: append the current event,
fetch the recent session context, preserve large artifacts separately, and make
the result visible to observers.

### State Is Semi-Structured

Agent state includes conversation turns, tool-call traces, observations,
scratchpads, plans, structured records, and artifacts. The schema is not
arbitrary, but it changes faster than ordinary product tables. The database
should enforce the runtime envelope while allowing event payloads and record
metadata to evolve.

This is why AgentDB uses typed runtime objects with JSON payloads rather than a
relational application schema or a compatibility facade for another database
protocol.

### Lifecycle Drops Off Sharply

Agent sessions have a discontinuous activity profile:

- During an active run, writes are frequent and append-dominant.
- After idle, activity drops sharply but does not become impossible. A user may
  resume days later, delayed feedback may arrive, or replay may be needed for
  debugging.
- Cold data may be refined into summaries, embeddings, records, or other
  derivatives that are queried more often than the raw trace.

The consequences are important:

- Historical events should be append-only; mutation breaks auditability and
  replay.
- Lifecycle needs more nuance than active versus deleted.
- Retention and garbage collection are product semantics, not just storage
  maintenance.
- Artifact bytes and metadata should be managed separately so large payloads can
  be retained, moved, or purged independently.

### Working Set Is Small; Total State Can Be Large

At platform scale, most sessions are idle most of the time. The concurrently
active working set is a small fraction of total retained state. This pushes the
design toward cheap idle storage, explicit lifecycle, and predictable
reactivation behavior.

Single-node AgentDB focuses first on making that simple locally: RocksDB
metadata, filesystem content storage, explicit config, and operational commands.
Future distributed storage should preserve the same runtime semantics rather
than exposing a different product model.

Agent platforms will want far more state to remain cheaply searchable and
recoverable than memory-first architectures can afford. turbopuffer is a useful
proof point outside AgentDB's domain: by building search around object storage as
the durable source of truth with cache/index layers above it, it shows how a
focused architecture can change product economics rather than merely improve an
implementation detail.

The lesson for AgentDB is not that AgentDB should become a vector database. It
is that architecture has to match the workload. Cold and derived state should be
designed around cheap durable storage, while the product API stays focused on
runtime semantics.

For AgentDB, this suggests a split:

- The live runtime path needs strict session semantics: ordered append,
  idempotency, replay, fork, watch, and policy.
- Large artifacts, cold event history, and derived searchable records should be
  designed so they can eventually sit on cheap durable storage with a cache and
  index layer above them.
- Search should be scoped by the runtime model: tenant, agent, session,
  namespace, source refs, policy, and lifecycle. Search alone is not enough.

This split also means object storage should not be the synchronous hot-write
contract. AgentDB evaluated an object-store-backed metadata store (SlateDB) to
harden the storage abstraction and retired it: object storage does not provide
multi-node commit consensus. Production hot writes should commit to local
durable state replicated across nodes; object storage should receive sealed
segments, archive bundles, and large artifacts asynchronously after the write
is already safe.

### Query Patterns Are Concentrated

Agent runtime queries cluster into a small set:

- **Session load**: fetch events for one session.
- **Append and recent fetch**: write the current event and return context.
- **Replay**: reconstruct a fork-aware event view.
- **Scoped search**: find records or observations by tenant, namespace, scope,
  tags, metadata, and text/vector signals.
- **Watch and subscribe**: follow changes for dashboards, supervisors, or test
  harnesses.
- **Lifecycle and policy scans**: move sessions through retention and cleanup.

This concentration is a design opportunity. AgentDB does not need to be a
general-purpose query optimizer to be useful. It needs to make the agent runtime
paths reliable and ergonomic.

This does not mean AgentDB automatically beats mature databases on every
storage benchmark. In a safe single-node `fsync` profile, Postgres is a strong
baseline because its WAL group commit is highly optimized. AgentDB's narrower
runtime semantics create an opportunity to win, but only if the implementation
uses them to reduce the acknowledged write path: append a compact durable log,
batch many sessions into each flush, serve read-after-write from committed
runtime state, and materialize secondary/session/index rows outside the critical
ack path where possible.

Record search follows the same distinction agent applications usually make in
practice. If the caller knows the owner or placement key, a local index can
serve fast, read-after-write filtering for that scope. If the caller is doing
tenant-wide or cross-owner discovery, a global index can update asynchronously
and provide eventual consistency. AgentDB exposes this as explicit local/global
record-index behavior rather than hiding the consistency tradeoff in adapter
code.

### The Session Is The Natural Aggregation Unit

Agent operations aggregate naturally by session:

- writes in one agent iteration belong to the same session
- context loading is usually a recent session scan plus scoped records
- debugging and replay operate on sessions and forks
- watch streams are useful at tenant, agent, and session scope
- artifacts and records often point back to source events

Aligning APIs and storage semantics with sessions reduces coordination cost and
makes the system easier to reason about.

### Transaction Requirements Are Narrower Than OLTP

Most agent runtime operations need one of these guarantees:

- append one event atomically
- append one event and fetch recent context
- deduplicate a retried mutation by idempotency key
- store artifact metadata and content references consistently
- read a replayable session view
- fork a session at a defined point

They usually do not need unrestricted cross-row serializable transactions.
Cross-entity product mutations remain the responsibility of the application
database. AgentDB's job is to make runtime operations durable, ordered,
idempotent, and replayable.

### Multi-Tenancy Is A First-Class Runtime Concern

Agent platforms can have many tenants, agents, and sessions with sparse traffic:
most are idle, some are active, and a small head can be very hot. A few agents
may also run away because of bad prompts, tool loops, or scheduler bugs.

AgentDB therefore needs:

- tenant-scoped identity and routes
- quota and backpressure hooks
- lifecycle and retention policy
- coarse isolation that is cheap for idle tenants
- observability for runaway sessions and noisy neighbors
- operational paths that do not require per-tenant manual work

The product is single-node first, but the tenant model is deliberate. Tenant
boundaries are part of the architecture: routes, policies, quotas, lifecycle
work, watch scopes, storage accounting, and placement decisions all preserve
tenant scope. Distributed and managed deployments should extend those concepts
instead of inventing a second product surface.

Tenant identity is infrastructure state. Authentication and user-info metadata
should establish the tenant/workspace boundary, while normal application code
talks in terms of workspaces, agents, and sessions. The server and low-level
routes keep tenant IDs because they are the isolation, routing, policy, quota,
storage, billing, and admin boundary.

### Self-Hosted Deployment Matters

Agent inputs and outputs often contain proprietary code, credentials, customer
data, operational logs, and private reasoning traces. Many users will want to run
the runtime database in their own environment.

That makes developer and operator experience part of the design:

- one binary should be useful
- configuration should be visible in `agentdb.toml`
- Docker, Compose, and Helm should mount config rather than requiring many
  memorized environment variables
- diagnosis should start with simple health and doctor commands
- backup, restore, upgrade, and maintenance should be understandable for a
  single-node deployment before distributed operation expands

## Core Primitives

AgentDB's native objects are intentionally close to the runtime domain:

- **Tenant**: isolation, policy, retention, quota, and lifecycle boundary.
- **Agent**: actor configuration inside one tenant.
- **Session**: ordered execution log with lifecycle state and optional fork
  parent.
- **Event**: typed append-only entry with JSON payload, sequence, idempotency,
  source refs, artifact refs, and policy metadata.
- **Artifact**: metadata plus content bytes for large or binary payloads.
- **Record**: searchable memory, note, observation, or retrieved context entry.
- **Policy**: quota, lifecycle, retention, context budget, legal hold, and GC
  behavior.

The hot path is:

```text
wake(session_id)
load recent events, records, artifacts, and policy hints
build model context in the harness
call the model
execute tools or sandboxes as external hands
append tool calls, tool results, model outputs, approvals, and artifacts
repeat or sleep
```

The session log is not the model context window. The context window is a lossy,
model-specific input. The session log is durable execution truth.

## Alternatives: Building On Existing Tools

AgentDB is not the only way to build agent persistence. The common alternatives
are valid at the right scale.

### Postgres Plus Blob Storage

Postgres with JSON columns, pgvector, Redis, and object storage is often the
right starting point. It gives teams one familiar source of truth and excellent
tooling.

It starts to hurt when session event volume grows, JSON indexes become large,
hot/cold movement becomes application code, and retry, replay, fork, and
artifact semantics are duplicated across services.

Appropriate scale: early products, low to moderate event volume, and teams that
benefit more from familiarity than specialized runtime semantics.

### Why Common Databases Are Not Ideal As The Runtime Layer

Several existing database families look close to AgentDB at first glance. The
right comparison is not "can this database store JSON events?" Almost all of
them can. The better question is: **what runtime guarantees does the application
still have to build above the database?**

AgentDB's claim is that agent-runtime state needs a product-level abstraction:
tenant, agent, session, event, artifact, record, policy, replay, fork,
watch, and lifecycle. Existing databases can implement parts of this, but they
do not expose that abstraction directly.

**Wide-column stores, such as HBase.** HBase is strong when the application can
design row keys around known access patterns. Rows are stored in row-key order,
which makes colocated range scans efficient. That is attractive for session
logs: `(tenant, session, seq)` is a natural row-key shape.

The tradeoff is that row-key design becomes the runtime design. Keeping all
events for a session adjacent helps replay, but hot sessions or time-ordered
keys can hotspot a region. Salting or hashing spreads writes, but makes ordered
session reads and lifecycle scans more complex. HBase gives powerful storage
building blocks; it does not give native idempotent append-and-fetch, fork-aware
replay, artifact retention, scoped records, policy, or SDK ergonomics.

HBase is a plausible storage engine component for a large future AgentDB data
plane. It is not, by itself, the agent runtime product.

**Key-value and serverless NoSQL stores, such as DynamoDB.** DynamoDB is strong
for keyed access at high operational scale. A session log can be modeled with a
partition key and sort key, and sort-key range queries can retrieve related
session events efficiently. For many products, this is a reasonable first
implementation.

The mismatch shows up when the agent runtime grows beyond a single access
pattern:

- Partition-key design must balance locality against hot-key risk. A hot
  session wants locality for replay but distribution for throughput.
- Item-size limits push large model/tool outputs into separate blob storage,
  requiring explicit artifact semantics.
- Secondary indexes are separate data paths with their own capacity and
  consistency concerns, so records, lifecycle scans, and dashboard queries need
  careful extra modeling.
- Transactions exist but are bounded; broad runtime invariants still become
  application-managed protocols.
- Streams can notify consumers of table changes, but they are not the same as a
  session-aware replay, fork, and watch model.

DynamoDB is excellent infrastructure for some keyed runtime data. AgentDB's
value is making the agent-session semantics the API rather than asking every
platform to rediscover the same single-table design and operational conventions.

**Document databases, such as MongoDB.** MongoDB is appealing because agent
events and metadata are semi-structured. It supports flexible documents,
indexes, transactions, and change streams. For moderate event volume, it can be
a practical session store.

The core issue is that a document is not the right top-level abstraction for an
agent runtime. Large event histories force an early modeling decision: one large
session document, one document per event, or buckets of events. Each option
creates different failure modes around document size, append contention, index
growth, and replay cost.

MongoDB change streams are useful for reacting to database changes, but they are
not a substitute for durable session replay and scoped runtime subscriptions.
They observe collection changes; AgentDB needs to expose domain events with
session sequence, fork visibility, idempotency, artifact refs, source refs, and
policy metadata.

MongoDB can back an implementation, especially early. But exposing a
MongoDB-shaped product would optimize for document CRUD while the user is
actually asking for session runtime semantics.

AgentDB once shipped a narrow MongoDB wire-compatibility listener as a driver
adapter and has since removed it: maintaining even a small command subset pulled
the product toward document CRUD. The native API and SDKs are the runtime
surface.

**Columnar analytics stores, such as ClickHouse.** ClickHouse is excellent for
high-volume analytical ingestion, aggregation, dashboards, and log-style
queries. Agent platforms may reasonably export AgentDB events into ClickHouse
for observability, product analytics, cost analysis, or run comparison.

The mismatch is that ClickHouse is optimized for analytical reads over columns,
not the live agent loop:

- The hot path is append one event, deduplicate retries, fetch recent context,
  and continue the run.
- Runtime state needs ordered per-session semantics, fork-aware replay,
  artifact coordination, and policy decisions.
- Lifecycle operations are operational semantics, not just analytical filters.
- Updates and deletes are possible in ClickHouse, but they are not its natural
  row-level mutation path.

ClickHouse is a strong downstream analytical sink for AgentDB events. It is not
the right primary runtime database for waking an agent, appending the next event,
and continuing work.

The pattern is consistent: these systems are valuable, and some may be useful
inside or beside AgentDB. The product gap is the missing runtime layer. AgentDB
should make agent-session behavior native instead of requiring every team to
compose it from generic database primitives.

turbopuffer is a good counterexample to "just use the existing database shape."
It demonstrates that when a workload becomes important enough, a purpose-built
architecture can make previously uneconomic product experiences practical. For
search, the winning abstraction is not "Postgres table with embeddings"; it is a
search-native system with different storage economics. For agent runtime state,
the analogous abstraction is not "documents plus streams plus blob storage"; it
is a runtime-native system with sessions, events, artifacts, records, replay,
fork, watch, policy, and lifecycle as first-class behavior.

### Multi-Tool Decomposition

A common production stack uses Postgres for metadata, a document or key-value
store for events, a vector database for retrieval, a search engine for logs, and
object storage for raw traces.

This scales individual workloads well, but cross-system semantics become
application code: partial failure handling, consistency, per-tenant policy,
artifact references, billing attribution, and observability all span multiple
systems.

Appropriate scale: larger teams with infrastructure capacity and strong reasons
to tune each subsystem independently.

### Custom Internal Runtime Store

Large agent platforms may eventually build a specialized internal store from KV,
blob storage, indexing, queueing, and metadata components. This can achieve an
excellent fit for one company's workload.

The cost is engineering time, on-call load, onboarding complexity, and a
persistence surface that every framework integration must learn.

Appropriate scale: the runtime database is core to the business, existing tools
have hit hard limits, and the team can sustain a storage project.

### What These Assemblies Usually Miss

The missing pieces are less about bytes and more about semantics:

1. Native session lifecycle.
2. Idempotent append-and-fetch for agent loops.
3. Fork-aware replay.
4. Unified tenant policy and cleanup.
5. Durable artifact references tied to events.
6. Watch and subscribe behavior for supervisors and dashboards.
7. SDK ergonomics that make the session path natural.

AgentDB should earn its place by solving these cleanly.

## Product Positioning

AgentDB is useful when two or more of these are true:

- You run a multi-tenant agent platform and need isolation, quotas, lifecycle,
  and admission control.
- Sessions are long-running or high-volume enough that hand-rolled event tables
  are becoming a product risk.
- Recovery, replay, and auditability matter.
- Forking or branch comparison is part of the agent design.
- The append-then-read-recent-context path is on the critical loop.
- Artifacts, observations, and records need a common runtime model.
- You want framework adapters to map concepts, not reinvent persistence.

AgentDB is overkill when the workload is a small number of agents, session
history is short, replay does not matter, and existing application tables are
still easy to reason about.

## What AgentDB Is Not

AgentDB is not:

- a general-purpose OLTP database
- a SQL analytics engine
- a vector database
- a memory-ranking or summarization engine
- an agent framework
- a workflow scheduler
- a sandbox, browser, or tool runtime
- a compatibility facade for an existing database wire protocol

Those systems can integrate with AgentDB. They should not be hidden inside it.

## Workload Fit And Tradeoffs

AgentDB should earn its place by being meaningfully better than general-purpose
databases on one narrow workload: durable runtime state for AI agents.

The target hot path is:

```text
append event -> assign session seq -> persist -> fetch recent/context state
```

Generic databases can implement this with multiple calls and application-side
coordination. AgentDB makes it one runtime operation with built-in ordering,
idempotency, policy, and visibility. The advantage should come from removing
round trips and generic database machinery from the agent turn loop, not from
trying to be a faster general-purpose database.

Target direction:

- **10x lower latency** on the agent-loop hot path where one AgentDB operation
  replaces multiple generic database calls.
- **10% retained-history cost** for long-lived session/event history by moving
  inactive data out of hot database storage and into cheaper content/object
  storage.

These are product targets, not claims about the current implementation.

AgentDB is intended to be a strong fit for:

| Workload | Why AgentDB Should Fit |
|---|---|
| Agent turn loops | One append-and-fetch operation instead of several database calls |
| Long-running sessions | Ordered durable history with replay and lifecycle |
| Agent debugging | Session replay, fork, watch, and event envelopes are first-class |
| Retry-heavy execution | Idempotency keys and ordered append are runtime semantics |
| Large retained traces | Cold/archive storage can reduce hot database footprint |
| Framework adapters | Adapters map to primitives instead of rebuilding them |
| Tenant-scoped agent platforms | Runtime identity, policy, quota, and lifecycle are tenant-aware |

AgentDB is probably not the right primary database for:

| Workload | Better Fit |
|---|---|
| Product OLTP data | Postgres, MySQL, MongoDB, DynamoDB |
| Arbitrary document apps | MongoDB |
| Key-value/table apps with fixed access patterns | DynamoDB |
| Complex relational transactions | Postgres or another relational database |
| Analytical workloads | Warehouses, lakehouses, OLAP engines |
| Standalone vector search | Vector/search databases |
| General log analytics | Log/search platforms |
| Full MongoDB/DynamoDB compatibility | MongoDB or DynamoDB |

The benchmark scope should stay explicit:

```text
append event + fetch last N events
append event + build context under budget
replay 1k / 10k / 100k event sessions
fork + continue
watch active sessions
retain 1M / 100M / 1B events for 90 or 365 days
restore or replay archived history
```

If AgentDB cannot produce order-of-magnitude wins on these paths, the product
needs to be rethought.

## Recommended Pattern: Config As Events

AgentDB does not need a separate configuration store for many agent-platform
settings. Configuration changes can be represented as events in ordinary
sessions, then reduced at read time.

Convention:

- Each tenant has one or more config sessions, such as `_config:agents`.
- Defaults live in code or deployment config.
- Override events record the target, patch, author, and reason.
- At read time, the SDK or application loads the session and folds events over
  defaults.

Example event payload:

```json
{
  "kind": "agent_config_override",
  "role": "support",
  "patch": {
    "max_tool_calls": 12,
    "system_prompt_suffix": "Prefer concise answers."
  },
  "reason": "reduce support-ticket latency"
}
```

Why this works:

- history is free because config changes are events
- rollback can be modeled by appending a compensating event
- tenant isolation follows the normal tenant/session boundary
- dashboards and audits can inspect configuration changes
- hot reload can happen at the start of an agent iteration

Tradeoffs:

- current-value reads require reducing events or reading a snapshot
- applications still own schema validation for patches
- very large config streams should periodically snapshot derived state

## Recommended Pattern: Plans As Events

Plans are mutable. A real agent revises its plan as it learns what works and
what does not. Storing the current plan as a row loses the reason a step changed,
the order steps completed, and the observations that caused replanning.

Instead, dedicate a session per run or plan and append events:

| Event kind | Producer | Payload |
|---|---|---|
| `plan_revised` | planner | full step list plus reason |
| `step_dispatched` | planner | step id, assignee, text |
| `step_completed` | worker | step id, status, summary |
| `observation` | tool or harness | tool name, payload, related step |

Read time reduces the event stream into `{ steps, observations }`. The latest
`plan_revised` defines the step list; later dispatch and completion events
update status; observations accumulate as planning context.

Why event-sourced plans fit AgentDB:

- replay shows how the plan evolved
- resume loads the plan session and continues after process restart
- fork explores alternate strategies without mutating the main run
- observability can show the plan, tool results, and model decisions together

## Open Questions

These are product-shaping questions, not the implementation gap tracker. See
[`../product-roadmap.md`](../product-roadmap.md) for current feature progress and remaining
engineering work.

### How Load-Bearing Is Fork?

Fork is valuable for alternate strategies, subagent branches, debugging, and
replay. The open question is how common branch-heavy agent design becomes in
mainstream products. If most systems remain linear, fork is correct but less
differentiating. If multi-agent branching becomes common, fork becomes central.

### Who Defines The Session Boundary?

Frameworks use different concepts: threads, runs, tasks, conversations, crews,
graphs. AgentDB can provide durable sessions, but applications and adapters must
map their framework boundary onto them. SDK ergonomics should make this explicit
without forcing every user to think like a storage engineer.

### Who Pays For Derivative Generation?

Summaries, embeddings, extracted entities, and memory compaction require model
or embedding calls. AgentDB should store source events, derived records, source
refs, and lifecycle hints, but the application or platform usually schedules and
pays for derivative generation.

### Should Cold Raw Data Always Be Preserved?

Some users will need raw traces for auditability. Others may prefer a cheaper
mode that preserves summaries, records, and artifacts while deleting raw event
or artifact payloads after a retention window. AgentDB already supports
lifecycle and derivative-only artifact purge semantics; the open product
question is how opinionated the default retention modes should be for different
markets and compliance profiles.

### How Much Search Belongs In The Runtime Database?

AgentDB needs scoped search over records and runtime state. It should not become
a full search product. The right line between built-in retrieval and integration
with dedicated search/vector systems will depend on real workloads.

Making more historical state searchable is valuable for agents, but AgentDB's
differentiator is not raw search quality. It is the combination of search with
runtime truth: source events, artifact references, fork points, retention policy,
tenant scope, and replay. The likely product shape is built-in scoped retrieval
for common agent context paths, plus clean export or integration with dedicated
search systems when the search workload becomes a product in its own right.

### What Is The Right Distributed Shape?

The product priority remains excellent single-node operation, while the codebase
also contains evaluation machinery for tablets, routing, movement, snapshots,
and Raft-backed write paths. Future distributed work should preserve the same
native objects and runtime semantics. The open question is where to draw
boundaries between open-source server behavior, managed control plane, indexing,
and artifact storage without making self-hosting too complex.

## References

- Anthropic. [How we built our multi-agent research system](https://www.anthropic.com/engineering/managed-agents).
- Ankur Goyal. [AI observability is a database problem: how Brainstore works](https://x.com/ankrgyl/status/2041206959848735107).
- Apache HBase. [Reference Guide: rowkey design and hotspotting](https://hbase.apache.org/book.html#rowkey.design).
- AWS. [DynamoDB partition key design](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-design.html).
- AWS. [DynamoDB constraints](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html).
- MongoDB. [Change streams](https://www.mongodb.com/docs/manual/changestreams/).
- ClickHouse. [Updating and deleting data](https://clickhouse.com/docs/en/guides/developer/mutations).
- turbopuffer. [Serverless vector and full-text search built on object storage](https://turbopuffer.com/).
- AWS Startups. [How turbopuffer is refactoring the economics of search](https://aws.amazon.com/startups/learn/how-turbopuffer-is-refactoring-the-economics-of-search).
