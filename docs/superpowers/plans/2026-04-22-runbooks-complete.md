# Plan: finish the first-pass runbook library

Date: 2026-04-22
Scope: closes the M27-partial arc started in PR #29. Writes runbooks
for the four `ErrorCode` variants that still returned `None` from
`doc_url_for` (`not_found`, `invalid_request`, `storage_error`,
`serialization_error`) and wires their URLs. After this PR every
error code the REST API can emit has a published runbook.

## Why

PR #29 established the `doc_url` protocol on `ErrorResponse` and
landed three runbooks for the codes operators hit first (`lease_lost`,
`fault_injected`, `quota_exceeded`). The other four codes still
return `None` from `doc_url_for`, so operators seeing them in prod
logs get no link — exactly the state the arc was trying to fix.

## Scope

- `docs/runbooks/not_found.md`: HTTP 404 semantic. Distinguishes
  "tenant hasn't been minted yet" (transient, fixes itself on first
  write) from "this session genuinely doesn't exist on this tablet"
  (real error, probably a routing bug).
- `docs/runbooks/invalid_request.md`: HTTP 400 for client-side
  shape/validation bugs. Lists the common fields we validate
  (`num_boxes > 0`, JSON decode errors on admin payloads) and
  points at the OpenAPI snapshot as the authoritative schema.
- `docs/runbooks/storage_error.md`: HTTP 500 from the
  KV / object-store backend. Splits RocksDB errors (usually disk)
  from SlateDB / object-store errors (usually S3 creds, region,
  or temporary outage). Points at the `AGENTDB_OBJECT_STORE` /
  `AGENTDB_S3_*` env vars.
- `docs/runbooks/serialization_error.md`: HTTP 500 from serde. Rare
  in practice — typically indicates corruption (disk bitrot) or a
  cross-version schema mismatch if two processes at different
  versions share a KV. Documents how to tell them apart.

All four follow the same 5-section template the index documents.

**Code:**

- `crates/agentdb-api/src/error.rs`: `doc_url_for` returns `Some(...)`
  for all four codes. Unit tests updated to assert the URL shapes
  (no more `None` cases in the "unpublished" test).

**Docs:**

- `docs/runbooks/README.md`: index table adds the four new rows;
  removes the "without a published runbook yet" footnote.

## Out of scope

- Mongo-wire `ErrorKind` → runbook mapping (driver-side pass-through
  work; separate PR).
- Runbooks for error codes the REST API doesn't emit directly (e.g.
  config-validation errors at startup — the server just exits with
  an anyhow context line; no JSON body).
- Admin CLI wiring so operators can fetch runbooks from within the
  binary. M27 proper.

## Acceptance

- `doc_url_for` returns `Some(_)` for every `ErrorCode` variant.
- `cargo test --workspace` green.
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
- OpenAPI snapshot unchanged (no structural change — only string
  values in `doc_url_for`, which is not in the schema).
- Each new runbook passes a read-through sanity check: structure
  matches the template, cross-links resolve.

## Shape of the commit

Single PR, single commit:
`docs(runbooks): finish first-pass runbook library (4 more codes)`.
