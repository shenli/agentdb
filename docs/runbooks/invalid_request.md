# `invalid_request`

## Symptom

Client receives a JSON body with `code: "invalid_request"` and HTTP
400. Typically carries a `field` name when the validation failure is
per-field, and a human-readable `message`:

```json
{
  "code": "invalid_request",
  "type": "invalid_request_error",
  "message": "num_boxes must be > 0",
  "field": "num_boxes",
  "doc_url": "https://github.com/shenli/agentdb/blob/main/docs/runbooks/invalid_request.md"
}
```

`field` is absent for request-level validation failures (e.g. a
payload that's shaped right but semantically wrong).

## Diagnose

1. Look at `field` first. If present, it names the specific request
   field the server rejected. `num_boxes`, `max_qps`, `burst`, etc.
2. Cross-reference the server's OpenAPI spec — the authoritative
   schema for every REST payload lives at:

   ```
   GET /openapi.json
   ```

   A quick way to validate a request body on the client side before
   sending is to run it against the schema referenced there for the
   matching path/method.

3. If the 400 has no `field`, the message usually contains enough
   context. Common request-level rejections:
   - Payload decoded but is missing a required nested object.
   - A tier-force request passed an unknown `target`.
   - A fault-injection request with neither a latency nor error
     `kind`.

## Verify

- Compare the request body against the OpenAPI schema component for
  the endpoint. `/openapi.json` is the single source of truth; if
  your client uses a generated SDK, regenerate from the latest
  spec.
- For shape errors, a `curl -v` with the exact payload reproduces
  deterministically. If the same payload passes on one request and
  fails on another, something else is going on — this code should
  be deterministic.

## Remediate

1. **Fix the client.** These are by-contract errors: the client sent
   a payload the server can't accept, regardless of cluster state.
   Fix the request and retry.
2. **Type-stripping by a proxy.** If an intermediary (load balancer,
   API gateway) is rewriting the body, double-check it's passing
   JSON through untouched. Boolean → string and integer → string
   coercion are common proxy bugs.
3. **SDK / API drift.** If the client was auto-generated against an
   older OpenAPI snapshot, regenerate. Field renames between AgentDB
   versions do happen (though we try to hold the line); the
   snapshot-drift test in CI catches them at server-side release
   time, not client-side.

## Prevent

- Pin the client SDK to the AgentDB server's version and regenerate
  it on every server version bump. `/openapi.json` is stable per
  tag.
- Validate payloads client-side before sending when latency matters.
- If you're an API consumer without a generated SDK, at minimum
  write a smoke-test suite that exercises each endpoint's happy
  path — this catches most 400 regressions during upgrade.
