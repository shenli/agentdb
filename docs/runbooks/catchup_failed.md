# `catchup_failed`

The catch-up scan over the durable event log failed, before the
live tail could begin (per-session) or for one specific session
(tenant-wide).

## Symptom

The subscribe SSE stream emits a terminal frame:

```
event: error
data: {"error":"catchup_failed","from_seq":<N>,"live_seq":<M>}
```

Per-session endpoint closes the stream immediately. Tenant-wide
endpoint logs the failure, marks that one session as degraded
(skips its catch-up), and continues live tailing for the
remaining sessions.

## Diagnose

1. Inspect the `from_seq` and `live_seq` in the body. If they
   straddle a known compaction / GC boundary, the event log
   for that range may have been GCed before the subscriber
   reached it.
2. Pull the server log for the failing scan:

   ```
   journalctl -u agentdb | grep 'catchup_failed' | tail -20
   ```

   Each close emits a `tracing::warn!` carrying the underlying
   error string from `agentdb_core::fetch_events_from_seq`. That
   string is the authoritative cause.
3. Categorize the underlying error:
   - **Storage transient.** S3 throttling, RocksDB stall,
     network blip. The error string typically names
     the backend.
   - **Session deleted.** The session was removed between the
     subscribe call and the scan. The error refers to a
     missing session in the meta store.
   - **Range out of bounds.** `from_seq` is past the session's
     `next_seq`, or refers to a compacted-away prefix.
4. Check `agentdbctl --url https://<node> status` on the same node. A
   `storage_error` or `lease_lost` readiness reason points the same way.

## Verify

- Re-issue the same subscribe request after a short delay. If
  the scan now succeeds, the underlying error was transient
  (cache warm, throttling subsided, etc.).
- For tenant-wide subscribers, observe whether other sessions
  in the same fan-out continue tailing successfully. If they
  do, the failure is scoped to the one named session — that
  matches the "degrade one session, continue the rest" path
  in `subscribe.rs`.

## Remediate

1. **Transient storage failure.** Reconnect with the same
   `from_seq` after a short backoff. The official SDKs surface
   this as `SubscriptionClosed` with `error_code:
   "catchup_failed"` and let the caller drive the retry — they
   do NOT auto-reconnect on this code (in contrast to
   `jwt_expired`) because looping a failing scan is rarely
   the right behaviour.
2. **Session deleted.** Stop reconnecting; the session is
   gone. The consumer should branch on the close reason and
   surface a "session no longer exists" error to its caller.
3. **Range out of bounds.** Reconnect without `from_seq` (live
   tail only). The events the consumer asked for are no
   longer in the durable log; replaying from the gap isn't
   recoverable.
4. **Persistent across retries.** Escalate. Inspect:
   - `agentdbctl raw get /api/admin/boxes` for `is_stale` / `lease_lost`
     conditions on the box that owns the session until box state has a modeled
     CLI command.
   - The KV-backend metrics (RocksDB compaction, S3 5xx rate).
   - The audit log for recent admin operations on the
     affected session id.

## Prevent

- Catch-up failures are mostly storage failures in disguise.
  The same monitors that catch `storage_error` REST
  responses (KV-backend SLO, S3 5xx rate, RocksDB compaction
  lag) catch this in advance.
- For long-lived browser-direct subscribers, prefer reading
  in cached batches (`from_seq` resumes from a known cursor
  on every reconnect) rather than buffering enormous gaps in
  the client. Smaller catch-up windows fail less often and
  recover faster when they do.
- The server does not retry the scan internally. If reliability
  matters more than fail-fast for a particular consumer,
  wrap the SDK's subscribe call in an exponential-backoff
  retry on `catchup_failed` — but with a ceiling, since a
  permanent error (deleted session, missing range) will
  never succeed.

## Related

- [`lag_too_far`](./lag_too_far.md) — different code; the
  live broadcast buffer overflowed, not the durable scan.
- [`storage_error`](./storage_error.md) — the same underlying
  classes of failure manifest as REST responses on
  non-streaming endpoints.
