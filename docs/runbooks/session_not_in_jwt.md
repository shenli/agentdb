# `session_not_in_jwt`

The JWT was valid, but its `sessions` claim does not authorize
the requested session.

## Symptom

HTTP 403 with a Stripe-style JSON body:

```json
{
  "code": "session_not_in_jwt",
  "type": "invalid_request_error",
  "message": "jwt sessions claim does not allow session <s>"
}
```

Returned at subscribe time after `jwt_invalid` checks pass. The
SSE stream is never opened.

## Diagnose

1. Decode the token's `sessions` claim:

   ```
   jq -R 'split(".")[1] | @base64d | fromjson | .sessions' <<< "$JWT"
   ```

   Expect either the literal string `"*"` (any session in the
   tenant), or a JSON array of session ids.
2. Compare with the request:
   - Per-session endpoint: the session id in
     `/api/tenants/{tenant}/sessions/{session}/subscribe`
     must be in the array (or the claim must be `"*"`).
   - Tenant-wide endpoint: every session named in
     `?session=...` and every key in
     `?from_seq_per_session=...` must be in the array (or the
     claim must be `"*"`); a tenant-wide request with no
     session filter requires `"*"`.
3. If the claim is missing entirely, the verifier rejects
   earlier with `jwt_invalid` (claim shape error), not
   `session_not_in_jwt`.

## Verify

- Mint a fresh token with `sessions: "*"` (or a list including
  the target session) and confirm the same request now succeeds.
  If it does, the issue was authorization scope; if not,
  re-diagnose against `jwt_invalid` / `jwt_expired`.
- Server logs for the rejected request name the offending
  session id so multi-session requests can be debugged without
  decoding the token.

## Remediate

1. Re-mint the JWT with a `sessions` claim that includes the
   target session. Either:
   - Wildcard: `sessions: "*"` if the user is authorized for
     every session in the tenant.
   - Explicit list: `sessions: ["s-1", "s-2", "s-3"]` for
     per-session authorization.
2. For tenant-wide subscribers that legitimately need to fan
   in across many sessions, prefer issuing a per-user explicit
   list keyed by what your authz layer already knows, rather
   than `"*"` — the principal-of-least-privilege default is
   safer and easier to revoke.
3. Reload the page / reconnect the SDK with the new token.
   Tokens already in flight aren't affected (they didn't fail
   on `sessions` — they passed earlier with a different scope).

## Prevent

- Treat the `sessions` claim as the audit-able authz boundary
  for browser-direct subscribers. Every session a user can
  read should be the result of an explicit decision in the
  token-minting code, not a fallback.
- Avoid wildcard tokens to untrusted clients — a leaked
  wildcard token is a tenant-wide read leak. Per-session
  lists are the safer default.
- For multi-tenant frontend apps, mint a separate token per
  visible-tenant context rather than a single token spanning
  every tenant the user has touched; the `tenant` claim is
  scoped to one URL path.

## Related

- [`jwt_invalid`](./jwt_invalid.md) — token failed signature
  / claim verification before this check ran.
- [`jwt_expired`](./jwt_expired.md) — mid-stream auth close
  on a token whose `exp` ran out.
