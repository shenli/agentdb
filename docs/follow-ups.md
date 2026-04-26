# Follow-ups

Product-shape gaps that we know about but haven't shipped yet.
Each section captures the gap, the existing workaround, and a
sketch of what the proper fix looks like. Keep entries
narrow — when something becomes a milestone, give it its own
design doc.

---

## 1. Hybrid search rank fusion (RRF)

**Status**: not implemented.

**The gap.** AgentDB exposes BM25 (`$search`) and vector cosine
(`$vectorSearch`) as independent aggregation stages. To do
hybrid retrieval — the widely-validated pattern of running both
and combining ranks — the caller has to:

1. Run `$search` to get a list ranked by BM25 score.
2. Run `$vectorSearch` to get a list ranked by cosine distance.
3. Merge the two lists in app code (typically Reciprocal Rank
   Fusion: `score = sum(1 / (k + rank_i))`).

This is a measurable usability cost — every consumer rewrites
the same merge logic, often badly (mishandling duplicates,
score normalization, or RRF's `k` constant).

**What the fix looks like.** Add a server-side `$rankFusion`
(or `$hybrid`) aggregation stage that takes both pipelines as
sub-inputs and emits a fused list:

```js
db.events.aggregate([
  { $rankFusion: {
      input: {
        pipelines: {
          text:   [ { $search: { text: { query: "...", path: "payload.text" } } } ],
          vector: [ { $vectorSearch: { queryVector: [...], path: "payload.embedding" } } ]
        }
      },
      combination: { method: "rrf", k: 60 },
      limit: 10
  }}
])
```

Mongo 8.0's `$rankFusion` and Vespa / Weaviate / Qdrant
equivalents are all reasonable shapes to mirror. The actual
combiner is ~30 lines of code; the wire-protocol plumbing is
the bulk of the work.

**Workaround until it ships.**

```python
# Run both pipelines, fuse client-side.
def rrf_merge(*ranked_lists, k=60, limit=10):
    scores = {}
    for lst in ranked_lists:
        for rank, doc in enumerate(lst):
            key = (doc["session_id"], doc["seq"])
            scores.setdefault(key, [doc, 0.0])
            scores[key][1] += 1.0 / (k + rank)
    out = sorted(scores.values(), key=lambda x: -x[1])
    return [doc for doc, _ in out[:limit]]
```

---

## 2. Selective indexing

**Status**: works as auto-everything; selective opt-in/opt-out
not yet exposed.

**The gap.** At ingest time the indexer walks every event's
`payload` recursively and indexes:

- Every JSON **string** → BM25 token stream at `payload.<path>`.
- Every JSON **array of floats** of length 2–2048 → HNSW
  vector entry at `payload.<path>`.

This is great for first-touch DX (no `CREATE INDEX`, no Atlas
configuration, your data is searchable as soon as it's
appended). But it has a real cost:

| Field shape | Indexer does | Useful? |
|---|---|---|
| `payload.text = "What's 2+2?"` | full BM25 tokenization | yes |
| `payload.role = "user"` | full BM25 tokenization | rarely |
| `payload.session_id = "agent-1"` | full BM25 tokenization | no — it's the session id, you already filter on it |
| `payload.actor_id = 7` | nothing (not a string) | n/a |
| `payload.embedding = [0.13, ...]` | HNSW per-path entry | yes |
| `payload.metrics.latency_p99 = [...]` | HNSW per-path entry | almost never — non-semantic numeric series get treated as a vector |

The CPU + RAM + disk waste is bounded for typical agent
payloads (small JSON, mostly real text), but it's measurable
under load and gets worse if your payloads carry structured
metadata you never search.

**What the fix looks like.** Two flavors, complementary:

1. **Per-tenant indexer config** in the meta store:

   ```sql
   CREATE TABLE indexer_paths (
     tenant_external_id TEXT NOT NULL,
     path               TEXT NOT NULL,        -- e.g. "payload.text"
     mode               TEXT NOT NULL,        -- 'text' | 'vector' | 'skip'
     PRIMARY KEY (tenant_external_id, path)
   );
   ```

   The indexer consults this when walking each path. Default
   stays "auto-detect by shape" so existing users don't change.
   Operators who care about overhead set explicit rules.

2. **Per-event opt-out** via a reserved field:

   ```json
   {"_no_index": true, "session_id": "x", "_internal": {...}}
   ```

   Skips indexing of this single event. Useful for bookkeeping
   events the agent never wants to retrieve.

**Trade-off vs Postgres.** Postgres makes you opt *in* per
column / index. AgentDB makes you opt *out*. Both are
reasonable — the right choice depends on whether
"discoverable by default" or "explicit and minimal" is the
team value. Path-level config gives operators the choice.

**Workaround until it ships.** Be deliberate about payload
shape. Store searchable text under a single `payload.text`
field; put non-search-relevant metadata under a
`payload.meta.*` subtree (still gets indexed, but the cost is
predictable). Don't put long ID strings in payload — use
`actor_id` (typed integer, not indexed for BM25).

---

## 3. Operational data alongside session logs

**Status**: explicitly out of scope; documented integration
patterns only.

**The gap.** AgentDB is event-log shaped: append-only at the
core, with `update` and `delete` modeled as correction +
tombstone events that `find` folds by default. That's the
right shape for the agent's *session log*.

What it isn't: a relational / multi-document-transactional
system of record. There's no:

- Cross-record ACID transaction (update CRM customer + linked
  deal + audit row in one commit).
- SQL joins across entities ("show me customers whose
  most-recent session contained the word 'refund'").
- Mutable single-row-per-customer model where readers see only
  the latest committed value (events fold to the latest, but
  the storage shape is still append-only).

For an agent that updates a CRM after analyzing a customer
call, AgentDB is *one half* of the stack. The CRM remains the
system of record.

**What the recommended pattern is.**

```
                ┌───────────────────────────────────┐
                │  Sales agent                      │
                └──────────┬────────────────────────┘
       ┌───────────────────┼────────────────────────┐
       ▼                   ▼                        ▼
  ┌──────────┐      ┌─────────────┐         ┌──────────────┐
  │ AgentDB  │      │  Postgres / │         │  External    │
  │          │      │  Salesforce │         │  APIs        │
  │ session  │      │  (CRM SoR)  │         │  (call audio,│
  │ log,     │      │             │         │  email, ...)  │
  │ working  │      │ customers,  │         │              │
  │ memory,  │      │ deals,      │         │              │
  │ retrieval│      │ stages, ... │         │              │
  └──────────┘      └─────────────┘         └──────────────┘
```

A typical agent turn:

1. **Read CRM** → fetch customer state.
2. **Reason in an AgentDB session** → append events (call
   transcript chunks, retrieved past notes, candidate
   decisions). Use `fork` for "what if A vs B" branches.
3. **Decide**, then **write to the CRM via its normal API**.
   Multi-row ACID happens there.
4. **Log the action back to AgentDB**:

   ```json
   {"role": "tool", "name": "crm.update", "customer_id": 123,
    "before": {...}, "after": {...}, "reason": "..."}
   ```

The session log becomes the **agent's audit trail**: for every
CRM mutation, retrieve the events that led up to it via
`$search` / `$vectorSearch` and you have an explanation.

**Open issues with this pattern.**

- **No two-phase commit** between CRM mutation and AgentDB
  append. If the CRM write succeeds and the AgentDB log fails
  (or vice versa), you get drift. Workaround: log first,
  mutate second, accept retries are eventually consistent.
- **No federated query.** "Customers whose session log
  contains 'refund'" requires two queries (AgentDB →
  session ids → Postgres → customer rows) glued in app code.

**What a longer-term fix could look like.** A federated
read endpoint that joins session-log results with operational
records via a configured join key:

```python
# Speculative API
hybrid.find(
  events_filter={"$search": {"text": {"query": "refund"}}},
  join_to="postgres://.../customers",
  join_on=("payload.customer_id", "id"),
)
```

Not on the immediate roadmap; documenting the shape so the
question doesn't keep coming up unrooted.

---

## How to add to this list

Tiny rule: only file a follow-up here if it's a real,
reproducible gap, not a "wouldn't it be nice." Each entry has
three sections (gap / fix sketch / workaround) so a reader
can decide whether to wait or work around.

When something here gets a real design doc, link it from the
section header and stop maintaining the inline sketch.
