# `jwt_expired`

The JWT presented to a subscribe endpoint has passed its `exp`
time mid-stream.

## Symptom

The subscribe SSE stream emits a terminal frame and closes:

```
event: error
data: {"error":"jwt_expired","last_seq":<N>}
```

For tenant-wide subscriptions the body is keyed per session:

```
event: error
data: {"error":"jwt_expired","last_delivered_seq_by_session":{"s-1":42,"s-2":7}}
```

This is returned mid-stream (not at subscribe time) when a
long-lived connection's token expires while the server is still
delivering events. At subscribe time an expired token is rejected
with the `jwt_invalid` HTTP 401 path instead.

## Diagnose

1. Confirm the body carries `error: "jwt_expired"` and an
   `exp`-style cause — distinguish from `jwt_invalid` (signature
   / claim mismatch) and `lag_too_far` (slow subscriber).
2. Inspect the token's `exp` claim:

   ```
   jq -R 'split(".")[1] | @base64d | fromjson' <<< "$JWT"
   ```

   Confirm `exp` is in the past relative to the server's clock.
3. Check the gap between the token's `iat` and `exp` — if it's
   shorter than the agent run, the SDK's reconnect-on-refresh
   loop will be exercised constantly.

## Verify

- Server logs (with `RUST_LOG=agentdb_api=debug`) include a
  `tracing::warn!` for the closed stream noting the expired
  token, the tenant, and `last_seq`. The error frame body
  matches.
- The same token re-presented to a fresh subscribe request
  returns HTTP 401 with `code: "jwt_invalid"` (post-`exp`
  tokens fail validation at subscribe time too).

## Remediate

1. **From an SDK with token refresh configured.** No operator
   action. The Python and TypeScript SDKs auto-reconnect when
   constructed with `get_token` / `getToken`: on the close frame they call the
   provider for a fresh token and reconnect with
   `from_seq = last_seq + 1` (or per-session map for the
   tenant-wide endpoint). Consumers see no gap.
2. **Direct integration (raw EventSource, custom HTTP).**
   Refresh the JWT via your auth provider, then reconnect with
   `?from_seq=<last_seq + 1>` (or a per-session
   `?from_seq_per_session=` map) carried from the error
   payload. The catch-up phase reads the durable log, so no
   events are lost.
3. **If the token *can't* be refreshed** (auth provider
   degraded), the consumer must back off and retry. The
   AgentDB server has nothing to fix.

## Prevent

- Construct the SDK with a refreshable token provider in every
  long-running subscriber. Static `jwt=...` is fine for short
  scripts; long-lived processes that outlive a token want the
  callback form.
- Pick token TTLs that comfortably exceed expected reconnect
  intervals. Sub-minute TTLs on a tenant-wide subscriber make
  the refresh path the hot path.
- Monitor for repeated `jwt_expired` close frames per
  subscriber — sustained churn there is usually a clock-skew
  issue between the auth provider and the AgentDB node.

## Related

- [`jwt_invalid`](./jwt_invalid.md) — token rejected at
  subscribe time (signature, issuer, audience, format).
- [`lag_too_far`](./lag_too_far.md) — a different mid-stream
  close; subscriber fell behind the broadcast buffer.
