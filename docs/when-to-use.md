# When to use AgentDB (and when Postgres is enough)

This document is deliberately honest. AgentDB is a specialized
database for agent-runtime workloads, not a general-purpose
replacement for the OLTP store you already run. The wrong
adoption — "let's put everything here because it's
agent-native" — produces friction the project doesn't deserve
and doesn't need.

If you read nothing else: **for an early-stage AI startup, a
well-set-up Postgres + pgvector is probably enough**. Come back
to AgentDB when your workload develops the specific shape
described below.

---

## TL;DR checklist

You'll feel AgentDB pay off when **two or more** of these are
true. Until then, save yourself a database migration.

- [ ] You run a **multi-tenant agent platform** — many distinct
      customers' agents on one stack — and need per-tenant
      isolation, quotas, and admission control without
      building it yourself.
- [ ] Your agents have **long-running sessions** with hundreds
      to thousands of events each, and you'd benefit from
      automatic warm → cold → archive tiering instead of a
      hand-rolled `pg_partman` strategy.
- [ ] Your agent design **uses fork** as a first-class operation
      — branching into "what if A vs B" timelines, comparing
      branches, possibly merging back. Few teams use this
      today; the ones that do find Postgres impressively
      awkward at it.
- [ ] You've already hit the **hybrid retrieval ceiling on
      pgvector + tsvector** — you're maintaining two indexers,
      writing custom RRF code, and your team's complaining
      about the operational burden.
- [ ] Your **agent-loop hot path** is bottlenecked on the
      append-then-read-recent-N pattern, and the round-trip
      savings of `append_and_fetch` actually show up in your
      latency budget.

If zero or one of these is true, you don't need AgentDB.
Honest answer.

---

## What AgentDB is good at

These are the strong claims, and we'll defend them:

1. **The append-and-fetch primitive.** One round trip writes a
   new event and reads back the last N for context. In Postgres
   that's a transaction with an INSERT, a `MAX(seq) + 1`, and a
   `SELECT ... ORDER BY seq DESC LIMIT N`. Agents do this
   millions of times. The savings compound.

2. **Fork as a copy-free MVCC operation.** A child session
   sees a frozen snapshot of the parent's events at fork time,
   and from there both sessions evolve independently. No data
   copy. The read merge is the server's job, not yours.
   Building this correctly in Postgres is non-trivial — you
   need a recursive CTE, a `parent_seq` cap, and you have to
   re-derive the right answer for every read.

3. **Hybrid retrieval auto-indexed by payload shape.** Strings
   become BM25-tokenized; numeric arrays become HNSW vector
   entries. No `CREATE INDEX`. Trade-off documented in
   [`docs/follow-ups.md`](follow-ups.md) §2.

4. **Three-tier lifecycle out of the box.** Warm sessions
   live in KV. Cold sessions get serialized to a single
   immutable segment in object storage. Archive is a
   metadata-only relabel. Reads are tier-transparent. The
   nearest Postgres equivalent is `pg_partman` plus a custom
   archival job to S3 plus a custom read path that knows
   where to look — months of operational work.

5. **Multi-tenant by default.** Tenant isolation, per-tenant
   token-bucket admission control, per-tenant quotas, and
   audit logs are baked in, not bolted on. SCRAM-SHA-256 on
   the Mongo wire and HTTP Basic on REST when auth is
   enabled. The session log auto-routes to the tenant.

---

## What AgentDB is *not* good at

These are the explicit non-goals. If your workload needs any
of them, AgentDB is the wrong tool.

1. **Cross-entity ACID transactions.** Updating a customer
   record AND a linked deal record AND an audit row atomically
   is not what AgentDB does. The session-log model is append-
   only with correction events; it's not a multi-row OLTP
   transaction system. Use a real OLTP store for that.

2. **Relational queries / SQL joins across entities.** No SQL
   surface, no `JOIN`. You filter within a session, you filter
   across sessions of one tenant, but you don't join customers
   to orders.

3. **General-purpose full-text search.** The BM25 indexer is
   intentionally vanilla — whitespace + lowercase tokenization,
   no stemming, no language models. If you need a real search
   engine (multi-language, custom analyzers, faceting,
   re-ranking), use Elasticsearch / Tantivy / Lucene.

4. **High-cardinality analytical workloads.** Counting all
   events by hour across tens of millions of sessions is not
   what the storage shape optimizes for. Dump events into a
   columnar store (DuckDB, ClickHouse, BigQuery) for that.

5. **Production deployment of the prototype.** This is a
   prototype. There are no SLAs, no production customers, no
   24/7 on-call rotation behind it. Run it where you're
   willing to accept "the database might be wrong about
   something we haven't tested yet."

---

## "Postgres is enough" — when, exactly?

The honest threshold for an early-stage AI startup:

- **< ~1k sessions/day total.** Postgres + pgvector handles
  this without breaking a sweat. You won't measure
  AgentDB's primitives.
- **Single-tenant or two-tenant.** The multi-tenant primitives
  don't pay off until you have many distinct customers.
- **No fork in your agent design.** If your agent doesn't
  branch sessions, you don't pay the cost of getting fork
  right and you don't get the benefit either.
- **Sessions that fit in memory / Postgres TOAST.** If your
  longest session is a few hundred events, tier transitions
  are unnecessary.
- **Vector recall good enough at < 100k vectors per tenant.**
  pgvector with HNSW handles this comfortably.
- **One language, one team.** A second database means another
  driver, another monitoring story, another backup runbook.
  Worth it only when the workload demands it.

If all of those are true, **build on Postgres**. You'll move
faster, your team will know the operational story, and you
can migrate to AgentDB later when the workload changes —
the agent code is what migrates, not the data semantics.

---

## Migration path (Postgres → AgentDB, when the time comes)

The good news: the agent's *code* maps cleanly between the
two. You can write your agent loop against an interface and
swap implementations. Concretely:

```python
class SessionStore(Protocol):
    def append(self, session_id: str, event: dict, fetch_last_n: int = 0) -> ...: ...
    def fork(self, session_id: str, child_id: str) -> ...: ...
    def search_text(self, query: str, limit: int) -> ...: ...
    def search_vector(self, query_vec: list[float], limit: int) -> ...: ...
```

A Postgres implementation backs both `append` and `search_*`
with hand-rolled SQL. An AgentDB implementation calls the SDK.
Switching is a config change.

When you migrate the data: AgentDB's append model means you
can replay a Postgres history into AgentDB by appending
events in seq order per session, using the original
`ts_ms`. The locator-driven routing handles the placement.

---

## Decision flowchart

```
Are you running an agent platform?
├── No → use Postgres. Stop reading.
└── Yes
    │
    ├── Single-tenant, < 1k sessions/day, no fork?
    │     → Postgres + pgvector is enough. Stop reading.
    │
    ├── Hybrid retrieval painful in Postgres,
    │   sessions getting long, fork actually used?
    │     → AgentDB starts paying off. Try it.
    │
    └── Need cross-entity ACID, SQL joins,
        production-grade SLAs?
          → Postgres is the system of record;
            AgentDB is the session log alongside it.
            Read [`docs/follow-ups.md`](follow-ups.md) §3
            for the integration pattern.
```

---

## Open questions we want input on

If you've evaluated AgentDB and decided not to use it, we'd
love to know which of these tipped the balance:

- The prototype framing (no production users, no SLA).
- The lack of a managed-cloud option.
- A specific feature gap (which one?).
- The integration story with your operational store.
- Performance vs Postgres + pgvector at your scale.

File an issue with `[adoption]` in the title; we'd rather hear
"Postgres won and here's why" than guess.
