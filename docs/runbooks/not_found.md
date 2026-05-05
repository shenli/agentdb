# `not_found`

## Symptom

Client receives a JSON body with `code: "not_found"` and HTTP 404:

```json
{
  "code": "not_found",
  "type": "invalid_request_error",
  "message": "Session s-42 not found",
  "resource": "session",
  "id": "s-42",
  "doc_url": "https://github.com/shenli/agentdb/blob/main/docs/runbooks/not_found.md"
}
```

`resource` is one of `tenant`, `session`, `box` and `id` is the
specific identifier the server couldn't resolve. The body fields are
the authoritative signal — the message string is for humans.

## Diagnose

Behavior depends on which `resource` is missing.

**`session` or `tenant`:**

1. Decide whether the id should already exist. Sessions are created under
   an agent before events are appended. A 404 here usually means the client is
   reading before session creation, using the wrong tenant, or using a session
   that used to exist.
2. Check the tenant view for that id:

   ```bash
   agentdbctl --url https://<node> raw get /api/admin/sessions/<id>
   ```

   If 404 there too, the session genuinely doesn't exist on the box
   serving this tenant. Possible causes: session was GC'd after
   archive (rare), or the client is hitting the wrong tenant.

**`box`:**

1. Much rarer. The server only emits `Resource::Box_` from admin
   endpoints that take a `box_id` path param. 404 here means the
   id is out of range for this cluster's `AGENTDB_BOXES`.
2. Compare the path id against:

   ```bash
   agentdbctl --url https://<node> -o json raw get /api/admin/boxes \
     | jq '.[].id'
   ```

## Verify

- For sessions: the session's full archive chain is visible via
  `/api/admin/sessions/<id>` even when the session has been moved
  to cold or archive tier. If it's genuinely gone, that endpoint
  also returns 404 with this same code.
- For tenants: the client is authenticated as one tenant per design
  §3.2 (once M19 lands); pre-M19, the tenant id is passed in the
  request path. Typos in that path explain most 404s.

## Remediate

1. **Session not created yet.** Create the session first with
   `POST /api/tenants/:tenant_id/agents/:agent_id/sessions`, then append via
   `POST /api/tenants/:tenant_id/sessions/:session_id/append-and-fetch`.
2. **Session was archived and is now missing.** If the session
   should still exist, this is a data-loss bug — escalate with the
   session id and the last known tier. Check segment-gc logs for
   the session's segments under the relevant box.
3. **Wrong tenant or session id in the request path.** Fix on the
   client side. The path format is
   `/api/tenants/{tenant_id}/sessions/{session_id}/...` — if the
   client concatenates ids incorrectly the 404 is the symptom.

## Prevent

- Document the expected id shape per tenant in the client
  onboarding materials. "Tenant id is a UUID" vs "tenant id is the
  customer's slug" — pick one and communicate it.
- Consider mid-term adding a `401 tenant_not_authenticated` error
  code so clients that point at the wrong tenant get a more
  actionable signal than a generic 404 (separate PR; depends on M19
  SCRAM).
- Keep `agentdbctl raw get /api/admin/sessions/{id}` in your
  incident-response playbook so operators don't have to grep RocksDB dumps to
  answer "does this session still exist?"
