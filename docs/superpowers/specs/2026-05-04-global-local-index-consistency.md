# Global And Local Index Consistency Design

## Goal

Define the indexing consistency model for distributed search so AgentDB
can have:

- a GSI-like global index used for cross-shard discovery
- an LSI-like local shard/placement index used for fast filtering on
  the current shard
- guaranteed read-after-write visibility for the same logical
  session/connection

The design must not require strong consistency for index maintenance,
but it must preserve correctness for the writer that just committed.

## Behavioral model

AgentDB should follow DynamoDB's behavioral split for secondary
indexes:

- **Global index behaves like a DynamoDB GSI**
  It is asynchronously propagated from committed base writes and is
  eventually consistent.
- **Local index behaves like a DynamoDB LSI**
  It is scoped to the same ownership boundary as the base data and must
  satisfy the read-after-write contract for the writer.

This is a behavioral analogy, not a requirement to copy DynamoDB's
physical storage design exactly.

## Requirement

### R1. Global index is asynchronous

The global index is derived state, built out of band from committed
writes. It is not in the write-ack path and is allowed to lag.

### R2. Local index is LSI-like at the API contract level

Each placement keeps a local index for the data it owns. The local
index exists to accelerate filtering, candidate generation, and local
ranking on the current placement, but unlike the global index it must
preserve same-session read-after-write semantics.

### R3. Same-session read-after-write is mandatory

After a write commits, subsequent reads issued by the same logical
session must observe that write, even if the global index and the local
index have not caught up yet.

This guarantee applies to the logical AgentDB session, not merely to one
TCP connection. Retries, reconnects, and node handoff must not lose the
visibility guarantee.

### R4. Eventual consistency is acceptable for other readers

Readers outside the writer's logical session may observe index lag.
Their results converge as the local and global indexing pipelines catch
up.

## Non-goals

- Strongly consistent global secondary indexes in the write path
- Cross-placement consensus just to maintain search indexes
- Using the global index as the final source of truth for read
  visibility
- Requiring every read to bypass indexes and scan base storage

## Design Summary

Use three layers with different responsibilities:

1. **Primary store / commit log**
   The authoritative source of truth. Writes commit here first and
   receive a monotonic commit token.
2. **Local placement index**
   Placement-scoped, optimized for fast local filtering and ranking,
   and required to satisfy LSI-like read semantics for the writer.
3. **Global index**
   Asynchronously maintained, cross-placement, used for routing and
   coarse candidate discovery.

The serving rule is:

- if the query includes the sharding key, route to the owner placement
  and use the local index
- if the query does not include the sharding key, use the global index
  for cross-placement discovery
- after a placement is selected, the local index decides **what probably
  matches on this placement**
- the primary store closes any visibility gap required for
  read-after-write correctness

## Consistency Contract

### Commit token

Every committed write produces a monotonic visibility token. The exact
representation can be `seq`, HLC timestamp, WAL offset, or another
per-placement monotonic commit marker.

Properties required:

- later committed writes have greater-or-equal tokens
- the token is durably tied to the committed write
- the read path can compare a required token against index progress

### Session visibility fence

The server tracks, per logical session, the highest commit token
observed by that session.

Subsequent reads from that logical session carry an implicit or explicit
requirement:

`visible_at_least >= session_last_seen_commit_token`

A read is not allowed to return a state older than that fence.

## Write Path

1. The request is routed to the owning placement.
2. The write commits to the authoritative store.
3. The system records or emits an index mutation event derived from that
   committed write.
4. The write response returns the committed object plus its
   `commit_token`.
5. The session's visibility fence advances to at least that token.

Important constraints:

- The write ack does not wait for global-index apply.
- Index maintenance is idempotent and replayable from durable committed
  state.

Local-index visibility is implementation-defined:

- if local-index mutation is in the owner-placement write path, the next
  read can rely directly on the local index
- if local-index mutation is asynchronous, the next read must use the
  visibility fence and repair path so the observed behavior remains
  LSI-like

## Index Pipelines

### Local placement index

Each placement consumes committed mutations for the data it owns and
updates its local text/vector/filter indexes incrementally.

Responsibilities:

- text and vector candidate generation
- tag/namespace/source filtering acceleration
- local ranking and over-fetch
- persisted snapshots plus catch-up replay on restart

The local index is expected to be close to real time, but it is still a
derived structure and may lag briefly behind commits. At the API
contract level it is LSI-like: the writer's next read must observe its
committed write. The implementation may achieve that either by
synchronously visible local-index mutation or by asynchronous
maintenance plus the visibility-repair path described below.

### Global index

The global index consumes the same committed mutation stream
asynchronously and maintains cross-placement discovery data.

Recommended scope:

- term or vector partition to candidate placement ids
- coarse shard-level summaries
- optional lightweight posting lists or sketches for fanout reduction
- per-placement freshness watermarks

The global index should be treated as a routing and recall structure,
not as the final authority for same-session visibility.

## Read Path

### Query routing rule

Query planning starts with the sharding key:

- **Sharding key present**
  Route directly to the owner placement and use the local index. This is
  the LSI-like path.
- **Sharding key absent**
  Query the global index to discover candidate placements, then fan out
  to those placements. This is the GSI-like path and is eventually
  consistent unless the caller supplies a visibility token that forces
  fallback/repair.

The initial sharding key is expected to be `session_id` for
session-owned data. Tenant/namespace filters alone are not sufficient to
choose one placement when a tenant spans many placements.

### Session-local or placement-local reads

For a read on placement `P` with required visibility token `T`:

1. Check placement `P`'s local-index applied watermark.
2. If local index watermark `>= T`, serve from the normal local indexed
   path.
3. If local index watermark `< T`, enter the visibility-repair path.

### Visibility-repair path

When the local index is behind the required token, the server must
repair visibility before replying. The preferred mechanism is:

1. run the normal indexed query against the local index
2. read the committed tail from base storage for writes after the local
   index watermark and up to token `T`
3. merge the missing committed records/events into the candidate set
4. apply final filtering and ranking over the repaired result set

This preserves LSI-like read-after-write semantics without converting
index lag into a hard stall on every fresh read.

### Optional short wait

A brief wait for local-index catch-up is allowed as an optimization, but
it must not be the only correctness mechanism. If the wait expires, the
server still has to satisfy the read through visibility repair.

### Cross-placement reads

For tenant-wide or cross-placement search where the sharding key is not
provided:

1. use the global index to choose candidate placements
2. fan out to those placements
3. each placement enforces its own local session visibility fence
4. merge placement results at the coordinator

Because the global index is GSI-like, a no-sharding-key search can miss
fresh writes while the global index lags. If the caller requires
read-after-write for a fresh token, the coordinator must widen fanout or
route to the known owner placement so the owner can use the local repair
path. Correctness does not come from assuming the global index is
current.

## Watermarks

The design needs at least two progress markers.

### Local-index applied watermark

Each placement exposes the highest commit token fully applied to its
local index.

Used for:

- deciding whether a read can trust the local index alone
- measuring local indexing lag
- determining the missing tail for visibility repair

### Global-index applied watermark

The global indexing service exposes per-placement or per-partition
progress markers.

Used for:

- observability
- stale-routing detection
- optional coordinator heuristics about when to widen fanout

The global watermark is not sufficient to guarantee same-session
read-after-write, exactly like a GSI cannot be treated as the strong
consistency boundary.

## Failure Model

### Node restart

The primary store remains authoritative. Local indexes recover from
snapshot plus replay. The session visibility contract still holds
because reads can always repair from committed base storage.

### Index worker crash or lag

Search quality and latency may degrade, but same-session correctness is
preserved by the visibility fence and repair path.

### Reconnect or handoff

Because the guarantee is attached to the logical session rather than a
single socket, the visibility fence must be carried in protocol state,
server session state, or another durable/readable session-scoped
mechanism.

## API / protocol implications

At minimum, the runtime needs:

- a returned `commit_token` on writes
- a way to remember or propagate the latest token for a logical session
- per-placement local-index watermarks
- a read path that can merge committed tail data when the index lags

Possible surface shapes:

- explicit token returned to the SDK and echoed on later reads
- server-maintained session fence keyed by `session_id`
- both, with explicit token support for retries and diagnostics

If only connection affinity is used, the guarantee is incomplete.

## DynamoDB analogy

The intended mapping is:

- **AgentDB global index -> DynamoDB GSI**
  Cross-placement, separately maintained, eventually consistent,
  suitable for broad query routing and discovery when the sharding key
  is absent.
- **AgentDB local index -> DynamoDB LSI**
  Same ownership boundary as the base data, used for local query
  acceleration when the sharding key is present, and required to
  preserve read-after-write visibility for the writer.

One important implementation difference is allowed: AgentDB may emulate
LSI-like visibility with a repair path even if local-index updates are
not fully synchronous. That is acceptable as long as the externally
visible semantics remain LSI-like.

## Open questions

1. What exact commit token shape should be standard in the runtime:
   session `seq`, placement-local HLC, or WAL offset?
2. Should the repair path merge only the writer's own missing writes, or
   all committed writes between local watermark and required token?
   The safer default is all committed writes in that range.
3. How much payload should the global index carry: placement summaries
   only, or lightweight candidate references too?
4. Which query shapes are allowed to use a short wait before repair, and
   what timeout is acceptable?

## Acceptance criteria

- A write acknowledged to session `S` is visible to the next read from
  `S`, even if both local and global indexes are behind.
- Cross-placement queries do not require a strongly consistent global
  index in the write path.
- Local placement indexes remain the default fast path for filtering and
  ranking.
- Restart or index-worker lag does not break same-session
  read-after-write semantics.

## References

- DynamoDB secondary index overview:
  <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html>
- DynamoDB global secondary index behavior:
  <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html>
