# `channel_not_in_jwt`

The JWT was valid, but its `channels` claim does not authorize
the requested channel.

## Symptom

HTTP 403 with a Stripe-style JSON body:

```json
{
  "code": "channel_not_in_jwt",
  "type": "invalid_request_error",
  "message": "jwt channels claim does not allow channel <c>"
}
```

Returned at request time on `POST .../publish` and
`GET .../subscribe` for channels, after `jwt_invalid` checks
pass. The SSE stream is never opened; the publish never lands
on the bus.

## Diagnose

1. Decode the token's `channels` claim:

   ```
   jq -R 'split(".")[1] | @base64d | fromjson | .channels' <<< "$JWT"
   ```

   Expect either the literal string `"*"` (any channel in the
   tenant), or a JSON array of channel names. `null` means the
   claim is absent — every channel request from this token will
   be rejected.
2. Compare with the request: the channel segment in
   `/api/tenants/{tenant}/channels/{channel}/...` must be in
   the array (or the claim must be `"*"`).
3. If the claim is missing entirely, this is **not** rejected as
   `jwt_invalid` — Phase 2 broadcast access is opt-in per token,
   so an absent `channels` claim still parses cleanly and just
   denies every channel.

## Verify

- Mint a fresh token with `channels: "*"` (or a list including
  the target channel) and confirm the same request now succeeds.
  If it does, the issue was authorization scope; if not,
  re-diagnose against `jwt_invalid` / `jwt_expired`.
- Server logs for the rejected request name the offending
  channel name so multi-channel deployments can debug without
  decoding the token.

## Remediate

1. Re-mint the JWT with a `channels` claim that includes the
   target channel. Either:
   - Wildcard: `channels: "*"` for trusted server-to-server
     tokens that may publish or subscribe to any channel in the
     tenant.
   - Explicit list: `channels: ["room:42", "room:43"]` for
     end-user browser tokens to limit blast radius.
2. The `channels` claim is **separate** from `sessions`. A token
   can have `sessions: "*"` (every session in the tenant) but
   no `channels` claim — that token can subscribe to durable
   session events but will be rejected on every broadcast
   channel. Add `channels` explicitly when minting tokens that
   need broadcast access.
3. Reload the page / reconnect the SDK with the new token.
   Tokens already in flight aren't affected (they didn't fail
   on `channels` — they passed earlier with a different scope).

```json
{
  "iss": "...",
  "sub": "...",
  "tenant": "t-1",
  "sessions": "*",
  "channels": ["room:42"],
  "exp": ...
}
```

## Prevent

- Treat the `channels` claim as the audit-able authz boundary
  for browser-direct broadcast clients. Every channel a user
  can publish to or subscribe to should be the result of an
  explicit decision in the token-minting code, not a fallback.
- Avoid wildcard tokens to untrusted clients — a leaked
  wildcard token is a tenant-wide channel leak. Per-channel
  lists are the safer default.
- Mirror the `sessions` claim policy: if your auth layer issues
  `sessions: ["s-1", "s-2"]` per request, issue
  `channels: ["room:s-1", "room:s-2"]` (or whatever your naming
  convention is) at the same time, so the two scopes stay in
  sync.

## Related

- [`jwt_invalid`](./jwt_invalid.md) — token failed signature
  / claim verification before this check ran.
- [`jwt_expired`](./jwt_expired.md) — mid-stream auth close
  on a token whose `exp` ran out.
- [`session_not_in_jwt`](./session_not_in_jwt.md) — parallel
  error for Phase 1 session subscribe.
