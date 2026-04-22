# Plan: error `doc_url` + first runbooks (M27 partial)

Date: 2026-04-22
Scope: narrow slice of M27's "operational surface" milestone. Wires
every error response with a stable URL that points to a runbook, and
lands the first three runbooks. The rest of M27 (admin CLI,
structured health JSON, the full runbook library) stays deferred.

## Why

Today an operator who hits `"code": "lease_lost"` has the message
string and nothing else. They don't know:

- What `lease_lost` means operationally.
- Whether this is expected transient noise or a symptom of a bigger
  problem.
- What checks to run or commands to issue.

Per the v2 design §15.5 every error response should carry a doc URL;
per M27's scope the first runbooks should cover the error codes
operators are most likely to see first.

## Scope

**Protocol change (additive, non-breaking):**

- `ErrorResponse` (REST native) gains
  `doc_url: Option<String>` — skipped when None, populated when the
  error code has a runbook. Clients that don't know about the field
  ignore it per JSON conventions.

**Code:**

- `crates/agentdb-api/src/error.rs`: new
  `doc_url_for(code: ErrorCode) -> Option<&'static str>` maps each
  known code to its runbook URL. The URL is the GitHub-pages-style
  absolute path — `https://github.com/shenli/agentdb/blob/main/
  docs/runbooks/<name>.md` — so it works in logs, Slack, etc., not
  just in the repo checkout.
- Every branch of `ApiError::to_response` populates `doc_url` via
  this helper. The mapping function is unit-tested.

**Docs:**

- `docs/runbooks/README.md` — index of the runbook library and the
  shared template.
- `docs/runbooks/lease_lost.md` — "what the client sees, what it
  means, what to check."
- `docs/runbooks/fault_injected.md` — same for the admin-injected
  fault path.
- `docs/runbooks/quota_exceeded.md` — same for `TooManyRequests`.

Each runbook follows the structure the v2 design calls out:

1. **Symptom** — what the client observes (HTTP status + code +
   message shape).
2. **Diagnose** — what to check first (admin endpoints, log lines,
   metrics).
3. **Verify** — how to confirm the diagnosis.
4. **Remediate** — concrete steps to unblock the request / fix the
   state.
5. **Prevent** — reasonable config/practice to reduce recurrence.

## Out of scope

- Mongo wire `ErrorKind` → runbook mapping. The Mongo driver
  swallows unknown fields aggressively; adding the URL there is a
  separate PR with its own testing.
- Full runbook library for every error code.
- Admin CLI (`agentdb diagnose`, etc). M27 proper.
- `/health/ready` returning a typed `Unhealthy` response with the
  same doc_url treatment. Incremental next step.

## Acceptance

- `curl` against an error path returns `doc_url` in the JSON body.
  Manually verified against an intentionally-expired lease
  (via admin fault-inject + lease expire-for-test is not exposed in
  HTTP; use `LeaseLost` observation path instead).
- `cargo test --workspace` green.
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
- OpenAPI snapshot regenerated (new field).

## Shape of the commit

Single PR, single commit:
`feat(api): doc_url on error responses + first runbooks (M27 partial)`.
