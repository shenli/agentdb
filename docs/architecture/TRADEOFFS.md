# AgentDB Tradeoffs

Every database earns its performance and cost profile by choosing what it will
optimize and what it will not try to be. AgentDB is designed for one narrow
workload: durable runtime state for AI agents.

This document makes that product boundary explicit.

## Goal

AgentDB should be the runtime database for AI agent platforms: the place where
agent systems store the execution state needed to recover, inspect, resume,
replay, fork, audit, and continue work.

The target workload is not general CRUD. It is agent runtime state:

- ordered session event logs
- append-heavy agent turns
- append-and-fetch context loading
- fork-aware replay
- session and tenant watch streams
- artifact metadata and content references
- searchable records derived from agent work
- lifecycle movement from warm to cold to archive
- idempotent writes and retry-safe execution

The performance and cost bar is intentionally high. AgentDB should only matter
if it can be meaningfully better than using MongoDB or DynamoDB directly for
this workload.

Target direction:

- **10x lower latency** on the agent-loop hot path where one AgentDB operation
  replaces multiple generic database calls.
- **10% retained-history cost** for long-lived session/event history by moving
  inactive data out of hot database storage and into cheaper content/object
  storage.

These are product targets, not claims about the current implementation.

## Non-Goal

AgentDB is not trying to become a general MongoDB, DynamoDB, Postgres, MySQL, or
warehouse replacement.

AgentDB should not optimize for:

- arbitrary document CRUD
- arbitrary table access patterns
- broad OLTP transactions
- joins or relational modeling
- complex general aggregation
- ad hoc analytical queries over all runtime data
- being the system of record for application business data
- exact compatibility with every MongoDB or DynamoDB driver behavior

Application data such as users, billing, orders, permissions, calendars,
inventory, and product-specific entities should stay in the application
database.

## Design Choices

### Optimize The Agent Loop

The central hot path is:

```text
append event -> assign session seq -> persist -> fetch recent/context state
```

Generic databases can implement this with multiple calls and application-side
coordination. AgentDB should make it one runtime operation with built-in
ordering, idempotency, policy, and visibility.

This is where AgentDB can win latency. The advantage comes from removing round
trips and generic database machinery from the agent turn loop, not from trying
to be a faster general-purpose database.

### Treat Session History As An Append Log

Agent execution history should be append-only by default. Append-only history
makes replay, audit, recovery, debugging, and fork behavior understandable.

Mutable metadata exists around the session, but the event log itself should not
be modeled as arbitrary updatable documents.

### Make Lifecycle A Product Primitive

Most sessions are active briefly and idle for a long time. AgentDB should
separate:

- warm session heads that are appendable and fast
- cold sealed segments that remain replayable
- archive bundles that are cheap to retain

This is the main path to lower cost than keeping all history in a hot
MongoDB/DynamoDB table or collection.

### Keep Search Scoped To Runtime Semantics

AgentDB needs searchable records, observations, and event-derived metadata, but
search should stay grounded in runtime boundaries:

- tenant
- agent
- session
- namespace
- tags
- source refs
- policy
- lifecycle

AgentDB should be good at finding useful runtime context. It does not need to be
a general search engine or analytical query engine.

### Keep Compatibility APIs Secondary

MongoDB compatibility can be valuable for developer adoption, inspection, and
simple document access. DynamoDB concepts are valuable for consistency and
indexing design.

Neither compatibility surface should define the core product.

The native AgentDB API remains canonical for:

- append-and-fetch
- replay
- fork
- context
- watch and subscribe
- artifact content
- lifecycle
- policy and quota

## AgentDB Excels At

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

## AgentDB May Not Be The Best Fit For

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

## Performance Thesis

AgentDB can plausibly outperform MongoDB/DynamoDB for AI agent runtime workloads
because the workload is narrower:

- appends are session-scoped
- ordering is session-local
- recent context reads are predictable
- replay follows session/fork structure
- old history can be sealed and archived
- artifacts can live outside metadata storage
- indexes can be derived and runtime-aware

AgentDB should not claim lower latency or cost for arbitrary database
workloads. The benchmark scope should stay explicit:

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

## Compatibility Position

The likely compatibility direction is:

- **MongoDB API** for inspection, migration comfort, document-shaped reads, and
  developer adoption.
- **DynamoDB-style ideas** for partitioning, conditional writes, GSI-like global
  indexes, LSI-like local indexes, and read-after-write visibility fences.
- **Native AgentDB API** for the performance-critical runtime path.

Compatibility should help users get started and inspect data. It should not
pull AgentDB toward becoming a general-purpose database clone.

## Open Questions

- What exact benchmark must pass before claiming `10x` hot-path latency?
- What exact storage model is required before claiming `10%` retained-history
  cost?
- Which compatibility APIs should be enabled by default, if any?
- How much MongoDB query behavior is enough for useful inspection?
- Which records should remain hot-indexed after a session moves to cold or
  archive?
- How should AgentDB price or expose the distinction between warm, cold, and
  archive state?
