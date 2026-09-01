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

### Known limitation: isolated re-elected leader

The barrier reliably refuses a read whenever the serving node is a
follower, or is a leader that cannot confirm quorum within the timeout —
this is what the storage unit test pins
(`read_barrier_confirms_on_healthy_leader_and_times_out_when_isolated`).

There is one real-cluster gap it does **not** yet close. The raft group
runs with `check_quorum` disabled, and tablet ownership is a control-plane
SQL lease that is independent of raft authority. An isolated former leader
that is still the recorded SQL owner keeps heartbeating the metadata
store, so the owner-campaign worker keeps re-electing it as raft leader.
raft-rs `Safe` ReadIndex on that repeatedly-re-elected leader can
intermittently self-confirm, so such a node can still serve a stale read.

Closing this gap is not a `check_quorum` flip: enabling `check_quorum`
alone just makes the isolated node flap between leader and follower as the
owner-campaign fights the step-down, without closing the window. The
proper fix is to **decouple raft authority from the SQL owner lease** so an
isolated node stops campaigning once it cannot reach a quorum — after
which `check_quorum` + `pre_vote` deliver the strict guarantee. This is
tracked as the next hardening step (it belongs with the same
routing-vs-authority work called out in
`distributed-safety-hardening.md`). Until then these reads are
**leader-confirmed (best-effort linearizable)**: strictly linearizable
except for the isolated-re-elected-leader window described here. It is disabled only by the explicit kill switch
`AGENTDB_READ_BARRIER=off`, which downgrades linearizable reads to
eventually-consistent (documented, for incident mitigation only).

`append-and-fetch` has a narrower requirement: the returned tail must
include the append that response acknowledged. It uses that mutation's
own committed/applied index rather than paying for a second quorum round
trip.

## Endpoint classification

| Surface | Label | Enforced by |
|---|---|---|
| `GET /api/tenants/{t}/sessions/{s}/events` | Leader-confirmed (best-effort linearizable) | read barrier before local serve; see limitation below |
| `GET /api/tenants/{t}/sessions/{s}` | Leader-confirmed (best-effort linearizable) | read barrier before local serve; see limitation below |
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
- The S22 real-cluster scenario, extended once the barrier is wired into
  the read path: an isolated live leader must fail a linearizable read,
  not just a write.
