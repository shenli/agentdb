# Clustered Read Semantics

## Status

This document classifies every clustered read surface by its consistency
guarantee and states which mechanism enforces it. It is the contract
required by `distributed-safety-hardening.md` §1. It describes the
replicated (clustered) runtime; a single-node runtime trivially serves
every read from the one authoritative copy.

## The four labels

- **Linearizable** — confirmed against a current quorum (Raft ReadIndex
  barrier) before reading. A former leader that cannot confirm leadership
  fails the read; it never falls back to a local copy.
- **Read-your-writes** — observes at least a caller-provided commit or
  sequence watermark. Weaker than linearizable but never regresses behind
  the caller's own last write.
- **Snapshot-consistent** — reads from a named, stable boundary.
- **Eventually consistent** — may return stale data; the staleness signal
  or repair mechanism is documented alongside.

No endpoint earns a stronger label merely because it routed to the node
currently recorded as the tablet owner. Owner routing is a control-plane
lease fact, not proof of current Raft authority.

## Mechanism: the Raft read barrier

Linearizable reads use ReadIndex
(`RaftRsSharedGroup::read_barrier`, exposed through
`RaftRsGroupRegistry::read_barrier`):

```text
request reaches the node routed to as owner
        |
        v
submit ReadIndex on the local raft replica   (must be leader)
        |
        v
raft confirms leadership with a quorum        (Safe ReadOnlyOption)
        |
        v
obtain read_index; wait local applied_index >= read_index
        |
        v
read one stable local snapshot
```

The barrier has a bounded timeout (`AGENTDB_READ_BARRIER_TIMEOUT_MS`,
default 5000) and returns a typed retryable error on failure. It never
falls back to a local read when quorum confirmation fails — that is the
property that stops a partitioned or resumed former leader from serving
stale data.

### Why check_quorum is required

The raft group runs with `check_quorum` **on** (paired with `pre_vote`),
and this is load-bearing for linearizable reads, not incidental. raft-rs
tracks a per-peer `recent_active` flag; `check_quorum` is the machinery
that clears it each election timeout and steps a leader down when it can
no longer re-confirm a quorum. With `check_quorum` off, `recent_active`
is never reset, so an isolated former leader believes its peers are still
reachable forever and its `Safe` ReadIndex keeps self-confirming against a
quorum that no longer exists — i.e. it serves stale reads. (This was
observed directly: an isolated leader's progress tracker still showed both
partitioned peers as `recent_active=true`.)

With `check_quorum` on, the isolated leader clears `recent_active`, fails
to re-confirm a quorum, and steps down; `pre_vote` then prevents it from
winning another election while it cannot reach a quorum. So it never
resumes leadership and never self-confirms a read. Reads that reach the
former leader afterward either refuse locally (while it is still the
routed owner) or forward to the healthy leader once ownership moves —
both correct, neither a stale local serve. S22 verifies the isolated node
leaves the leader role within a settle window and stays out of it. It is disabled only by the explicit kill switch
`AGENTDB_READ_BARRIER=off`, which downgrades linearizable reads to
eventually-consistent (documented, for incident mitigation only).

`append-and-fetch` has a narrower requirement: the returned tail must
include the append that response acknowledged. It uses that mutation's
own committed/applied index rather than paying for a second quorum round
trip.

## Endpoint classification

| Surface | Label | Enforced by |
|---|---|---|
| `GET /api/tenants/{t}/sessions/{s}/events` | Linearizable | read barrier before local serve |
| `GET /api/tenants/{t}/sessions/{s}` | Linearizable | read barrier before local serve |
| append-and-fetch (`POST …/events` returning the tail) | Read-your-writes | the append's own applied index |
| `GET …/sessions/{s}/watch` (changefeed) | Eventually consistent | cursor advances as entries apply; caller polls |
| `GET /api/tenants/{t}/records/search` | Eventually consistent | **unrouted** — any node serves from local index state; see note |
| `GET /api/admin/*` (raft status, cluster health) | Eventually consistent | observational; per-node local view by design |

### Note on `records/search`

Record search is not routed through the tablet owner at all today: any
node answers from whatever local index state it holds. It is therefore
eventually consistent, and this table says so rather than implying a
guarantee the code does not provide. Promoting record search to a routed,
barrier-gated read is tracked as follow-up work; it requires routing the
query to the owning tablet first, which record search does not do yet.

## Acceptance

The barrier's behavior is pinned by:

- A storage unit test (`read_barrier_confirms_on_healthy_leader_and_times_out_when_isolated`):
  a healthy leader confirms; a non-leader is rejected; a leader isolated
  from its followers times out instead of serving locally.
- The S22 real-cluster scenario: an isolated live leader cannot commit,
  and (check_quorum) must leave the leader role within a settle window and
  stay out of it while partitioned, so it can never self-confirm a stale
  local read.
