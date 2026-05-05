# `lag_too_far`

The subscriber fell behind the EventBus broadcast buffer; the
server closed the stream rather than silently drop events.

## Symptom

The subscribe SSE stream emits a terminal frame and closes:

```
event: error
data: {"error":"lag_too_far","last_delivered_seq":<N>}
```

For tenant-wide subscriptions the body is keyed per session:

```
event: error
data: {"error":"lag_too_far","last_delivered_seq_by_session":{"s-1":42,"s-2":7}}
```

The subscriber typically reports its `recv` returned
`Overflowed` immediately before the close, in the rare case
where the SDK exposes that lower-level signal.

## Diagnose

1. Confirm the body code is `lag_too_far` (not `catchup_failed`,
   not `jwt_expired`). The three close codes have visually
   similar shapes.
2. Inspect the publisher rate on the affected session(s):

   ```bash
   agentdbctl --url http://<node>:7878 -o json raw get '/api/admin/sessions/<sid>?tenant_id=<tid>' \
     | jq '.next_seq'
   ```

   Watch how fast `next_seq` grows. If the gap between
   `last_delivered_seq` and the current `next_seq` is many
   thousands per second, the producer is faster than the
   consumer's drain rate.
3. Inspect the consumer:
   - Per-event work that calls a slow downstream (LLM,
     network) within the subscribe loop is the most common
     cause.
   - Single-threaded JS / Python consumers blocked on a
     synchronous render also hit this.
4. Check whether multiple subscribers are sharing a single
   process — if one slow subscriber is dragging the channel
   into overflow for a co-located fast one, the close hits
   only the slow one (broadcast channels are per-receiver).

## Verify

- Server logs (with `RUST_LOG=agentdb_api=debug`) record a
  `tracing::warn!` per close, including the receiver's
  `last_delivered_seq` and the bus capacity at the moment of
  overflow.
- Reconnect with `?from_seq=<last_delivered_seq + 1>` and
  observe whether the catch-up phase drains. If catch-up
  itself can't keep up before the live tail re-overflows, the
  consumer is genuinely too slow for the workload — not a
  transient burst.

## Remediate

1. Reconnect with `from_seq = last_delivered_seq + 1`
   (per-session) or the per-session map (tenant-wide). The
   catch-up phase reads the durable log so no events are
   lost. The official SDKs surface this via
   `SubscriptionClosed` and let the caller drive the
   reconnect.
2. If `lag_too_far` recurs:
   - Move per-event work off the hot path. Hand events to a
     queue / channel and let a downstream worker pool do the
     slow work. The subscribe loop itself should be a
     dispatch loop, not a processing loop.
   - Batch downstream operations. If each event triggers an
     external call, batch them per N events or per K
     milliseconds.
   - Shard the tenant-wide subscription across multiple
     consumers using `?session=` whitelists so each consumer
     drains a subset.
3. If the producer is genuinely bursty (an agent loop went
   wild), apply per-tenant rate limits (`set-tenant-quota`)
   to bound the cause rather than scaling out the consumer.

## Prevent

- Treat the subscribe loop as a dispatch path, not a
  processing path. The capacity is 16384 envelopes per
  receiver — at 1 KB envelopes that's 16 MB of headroom,
  which a synchronous `await fetch(...)` per event will burn
  through in seconds.
- Monitor consumer lag end-to-end (publisher seq vs.
  subscriber's last-rendered seq). A growing gap predicts
  `lag_too_far` minutes before it fires.
- The broadcast buffer is currently fixed at 16384. If a
  workload is structurally over that ceiling and the consumer
  truly can't be sped up, raising it requires a code change
  today (a future release may make it configurable). Sizing
  it past a bursty producer is wasted memory if the consumer
  stays slow — it just delays the close.

## Related

- [`catchup_failed`](./catchup_failed.md) — different
  scenario; the durable scan itself failed during catch-up,
  not the live broadcast buffer.
- [`jwt_expired`](./jwt_expired.md) — different mid-stream
  close; the token's `exp` ran out.
