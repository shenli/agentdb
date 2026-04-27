# `jwt_invalid`

The JWT presented to a subscribe endpoint failed verification at
subscribe time.

## Symptom

HTTP 401 with a Stripe-style JSON body:

```json
{
  "code": "jwt_invalid",
  "type": "invalid_request_error",
  "message": "invalid jwt: <reason>"
}
```

The connection never makes it to the SSE stream — the verifier
runs in the request extractor and rejects before the stream is
opened. Browser-direct callers will see the EventSource error
event without ever receiving the `subscribed` frame.

## Diagnose

1. Read the `<reason>` substring in the error body. Common
   shapes:
   - `signature verification failed` — HMAC secret mismatch or
     wrong JWKS key.
   - `issuer mismatch` — token's `iss` doesn't match the
     tenant's `JWT_ISSUER`.
   - `audience mismatch` — `aud` missing or doesn't include
     the configured audience.
   - `tenant claim does not match path` — token authorizes a
     different tenant than the URL's `:tenant_id`.
   - `algorithm not allowed` — token is signed with an
     algorithm not in the verifier's allowlist (e.g. `none`,
     unrecognized HMAC variant).
   - `malformed` / `decode error` — token isn't a valid
     three-segment JWT.
2. Decode the JWT to inspect claims without the signature:

   ```
   jq -R 'split(".")[1] | @base64d | fromjson' <<< "$JWT"
   ```
3. Inspect the tenant's server-side config:

   ```
   env | grep AGENTDB_TENANT_<id>_JWT_
   env | grep AGENTDB_TENANT_<id>_CORS_ORIGINS
   ```

## Verify

- Server logs (with `RUST_LOG=agentdb_api=debug`) include the
  underlying JWT-library error string for each rejection. That
  string is authoritative — the wire `<reason>` is its summary.
- Re-mint a fresh token with the same minting code path and
  confirm it succeeds against the same tenant. If the new
  token also fails, the issue is configuration (server-side
  env var, JWKS endpoint, signing key); if only old tokens
  fail, focus on TTL / `exp` (see `jwt_expired`).

## Remediate

1. Verify the tenant config matches the issuer:
   - `AGENTDB_TENANT_<id>_JWT_HMAC_SECRET` matches the
     signing secret, OR `AGENTDB_TENANT_<id>_JWT_JWKS_URL`
     resolves to a JWKS document with the kid the token was
     signed with.
   - `AGENTDB_TENANT_<id>_JWT_ISSUER` matches the token's
     `iss` exactly (string equality, including scheme).
   - `AGENTDB_TENANT_<id>_JWT_AUDIENCE` matches the token's
     `aud` exactly when set; unset means no audience check.
2. Re-mint the token if the issuer side has the wrong claims:
   - `tenant` claim equals the URL's `:tenant_id`.
   - Algorithm is `HS256` (HMAC) or `RS256`/`RS384`/`RS512`/
     `ES256` (JWKS).
3. For JWKS, confirm the AgentDB node can reach the JWKS URL.
   The verifier caches keys for 10 minutes — a recently rotated
   key may not yet be visible. Either wait out the cache or
   restart the affected node to drop it.

## Prevent

- Wire JWT minting and AgentDB tenant config from a shared
  source of truth (a config file, a secrets manager). Drift
  between the two is the most common cause.
- For HMAC, rotate secrets via the AgentDB env-var path and
  the auth provider in lockstep — there is no overlap window
  built into the verifier.
- For JWKS, prefer kid'd keys with overlapping rotation so
  the 10-minute cache TTL is never the critical path.
- Surface the rejection reason in client-side telemetry. The
  body's `message` is intentionally readable so dashboards can
  highlight "issuer mismatch" vs "signature verification
  failed" without server-log access.

## Related

- [`jwt_expired`](./jwt_expired.md) — different code; token
  was once valid and ran out mid-stream.
- [`session_not_in_jwt`](./session_not_in_jwt.md) — token
  passed verification but its `sessions` claim doesn't
  authorize the requested session.
