# AgentDB: Agent Runtime Database

AgentDB is a runtime database for AI agent platforms. It stores the durable
execution state that lets agent systems recover, inspect, resume, replay, fork,
and audit work.

This document is the **why**: motivation, workload requirements, design
principles, positioning, and open questions. For the current design
commitments, see [`DESIGN.md`](DESIGN.md). For the implementation shape, see
[`ARCHITECTURE.md`](ARCHITECTURE.md).

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
records, skills, policy decisions, forks, and lifecycle state. It is append
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

The current implementation is single-node first, but the tenant model is
deliberate. A future distributed implementation should extend the same concepts
instead of inventing a second product surface.

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
- **Skill**: registered package/tool manifest with versioned bindings.
- **Policy**: quota, lifecycle, retention, context budget, legal hold, and GC
  behavior.

The hot path is:

```text
wake(session_id)
load recent events, records, artifacts, skills, and policy hints
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
- Artifacts, observations, records, and skills need a common runtime model.
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

These questions remain product-shaping rather than fully settled:

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
payloads after a retention window. This choice affects product positioning,
compliance, and storage architecture.

### How Much Search Belongs In The Runtime Database?

AgentDB needs scoped search over records and runtime state. It should not become
a full search product. The right line between built-in retrieval and integration
with dedicated search/vector systems will depend on real workloads.

### What Is The Right Distributed Shape?

The current priority is excellent single-node operation. Future distributed
work should preserve the same native objects and runtime semantics. The open
question is where to draw boundaries between control plane, data plane, indexing,
and artifact storage without making self-hosting too complex.

## References

- Anthropic. [How we built our multi-agent research system](https://www.anthropic.com/engineering/managed-agents).
- Ankur Goyal. [AI observability is a database problem: how Brainstore works](https://x.com/ankrgyl/status/2041206959848735107).
- PingCAP. [How Manus 1.5 uses TiDB X to let agents ship full-stack apps at scale](https://www.pingcap.com/blog/how-manus-1-5-uses-tidb-x-to-let-agents-ship-full-stack-apps-at-scale/).
