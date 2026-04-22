# `quota_exceeded`

## Symptom

Client receives a JSON error body with `code: "quota_exceeded"`, HTTP
429, and a `Retry-After` response header:

```json
{
  "code": "quota_exceeded",
  "type": "rate_limit_error",
  "message": "rate limit exceeded (Tenant)",
  "scope": "tenant",
  "retry_after_ms": 1200,
  "doc_url": "https://github.com/shenli/agentdb/blob/main/docs/runbooks/quota_exceeded.md"
}
```

`scope` distinguishes the gate that refused: `box` means the
per-box QPS ceiling kicked in; `tenant` means the per-tenant ceiling
did. `retry_after_ms` is the advisory wait — a well-behaved client
honors it before retrying.

## Diagnose

1. Identify which box the request routed to — the error doesn't carry
   `box_id` for quota errors (per design: tenant-scope quota is not
   box-specific). The server access log or tracing span will show
   it.

2. Check the box's current quota config:

   ```
   curl https://<node>/api/admin/boxes/<box_id>
   ```

   Compare `quota.max_qps`, `quota.max_tenant_qps`, and `quota.burst`
   against expected headroom.

3. Pull the box metrics:

   ```
   curl https://<node>/api/admin/boxes/<box_id> | jq '.metrics.quota_hits'
   ```

   A growing `quota_hits` counter confirms the gate is the bottleneck.

## Verify

- The `scope` in the error body tells you which token bucket ran out.
  Cross-check the config:
  - `scope: "box"` → `max_qps` is too low for the aggregate traffic
    hitting this box.
  - `scope: "tenant"` → `max_tenant_qps` is too low for this
    particular tenant's traffic, even though the box still has
    headroom.
- If metrics say quota_hits is flat but clients still see 429,
  something else is issuing 429s upstream (a reverse proxy, a
  client-side rate limiter). The body's `doc_url` only covers
  AgentDB's own quota gate.

## Remediate

1. **Legitimate traffic growth.** Raise the relevant quota:

   ```
   curl -XPOST -H 'Content-Type: application/json' \
     -d '{"max_qps":500,"max_tenant_qps":200,"burst":1000}' \
     https://<node>/api/admin/boxes/<box_id>/quota
   ```

   Quota changes take effect at the next `admit` call for each
   tenant; no restart needed.

2. **Runaway tenant.** If one tenant is saturating a box that's
   hosting others, consider *lowering* `max_tenant_qps` below the
   current traffic rate for that tenant while you get them in sync
   on expected usage. The per-box bucket will still serve the other
   tenants' traffic.

3. **Retry storm.** If clients ignore `retry_after_ms` and retry
   immediately, the gate stays saturated even after traffic drops.
   Fix the client to honor the hint (or add jitter).

## Prevent

- Set `max_qps` and `max_tenant_qps` defaults based on observed p99
  load from the benchmark workload (see `benchmarks/`), not
  guesses. A too-low default turns normal spikes into incidents.
- Monitor `quota_hits` as a rate metric. Sustained non-zero values
  mean either the quotas need raising or traffic patterns need
  investigating — the right answer is rarely "never change quotas."
- Document expected quota headroom per tenant tier. When a tenant
  upgrades their plan, the quota change should be part of the
  onboarding, not a reactive response to 429s.
