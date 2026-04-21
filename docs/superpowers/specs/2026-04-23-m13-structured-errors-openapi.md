# M13: Structured Errors + OpenAPI — Design Spec

## Goal

1. Replace the current ad-hoc REST error shape (`{"error": "free text"}`)
   with a Stripe-style structured response carrying a machine-readable
   `code`, category `type`, and typed extras per code.
2. Publish an OpenAPI 3.x specification of the REST surface at
   `GET /openapi.json`, generated from the code via `utoipa`, so the
   spec and the code cannot drift.

This is the foundation milestone for M14 (MCP server) and M15 (typed
SDK). Both will consume the OpenAPI contract without re-discovering
the error shape or the route signatures.

## Motivation

AgentDB is moving from a "developer-focused prototype you read the
source to understand" posture to a "product with a stable API
contract" posture. The current surface is blocked on three things:

1. **Errors aren't structured.** Today every failure path returns
   HTTP status + `{"error": "<formatted string>"}`. Clients —
   especially agent tool-use runtimes — cannot branch deterministically
   on outcome. A rate-limit hit and a bad field both return opaque
   strings the caller must regex-parse.
2. **No machine-readable API description.** There is no OpenAPI /
   JSON Schema spec. SDKs cannot be auto-generated; agent harnesses
   cannot introspect the surface; reviewers must read axum handler
   source to know what exists.
3. **No stable contract for MCP / SDK layers.** Both future
   milestones need a single source of truth for paths, request /
   response schemas, and error shapes. Building MCP on top of the
   current implicit surface bakes the ad-hoc strings into tool
   schemas.

M13 fixes those three together. Specifically excluded from scope
(to keep the milestone focused):

- Mongo wire error format — stays as-is per the protocol
- Authentication / authorization — separate milestone
- MCP server — M14
- Typed SDK (TS / Python) — M15
- Idempotency keys, rate-limit headers beyond `Retry-After` — later
- Dashboard consuming OpenAPI for an API explorer — later

## Scope

**In:**

- Replace `agentdb_core::Error::{Invalid(String), NotFound(String)}`
  with typed variants carrying the taxonomy fields
- Replace `agentdb_api::error::ApiError::BadRequest(String)` with a
  typed variant set in the same taxonomy
- `impl IntoResponse for ApiError` emits the Stripe-style JSON body
  (always `{code, type, message}` + typed extras per code)
- Six error codes across three categories:

| code slug | type category | HTTP | Extra fields |
|---|---|---|---|
| `not_found` | `invalid_request_error` | 404 | `resource: "tenant" \| "session" \| "box"`, `id: string` |
| `invalid_request` | `invalid_request_error` | 400 | `field: string?` (optional) |
| `quota_exceeded` | `rate_limit_error` | 429 | `scope: "box" \| "tenant"`, `retry_after_ms: u32` |
| `fault_injected` | `api_error` | 503 | `box_id: u16`, `fault_kind: "latency" \| "error"` |
| `storage_error` | `api_error` | 500 | — |
| `serialization_error` | `api_error` | 500 | — |

- Add `utoipa` + `utoipa-axum` dependencies to `agentdb-api`
- Annotate every REST handler with `#[utoipa::path(...)]`
- Annotate every request / response type (including the new error
  response schema) with `#[derive(ToSchema)]`
- A top-level `ApiDoc` struct in `agentdb-api/src/lib.rs` aggregates
  all paths and schemas
- Serve the spec at `GET /openapi.json` (root, unauthenticated)
- Snapshot test: generated OpenAPI output is locked against a
  checked-in file, forcing intentional updates on schema changes
- Integration tests that exercise one failure case per code and
  assert the emitted JSON matches the schema
- Update `docs/ARCHITECTURE.md` with a short "Error taxonomy"
  section and a pointer to `/openapi.json`

**Out (deferred, not regressions):**

- Mongo wire error format — untouched; M10's `{ok: 0, errmsg, code: 82}`
  mapping for rate-limit stays. Future Mongo-code-to-error-variant
  mappings can be added incrementally without breaking M13's REST
  contract.
- Back-compat with the old `{"error": "..."}` shape. We break it
  cleanly; this is a prototype, not a production product with
  external integrators.
- Auth, idempotency keys, SDK generation, MCP consumption.

## Design

### Internal error type

Replace the current core `Error`:

```rust
// Before
pub enum Error {
    Storage(#[from] agentdb_storage::Error),
    Serde(#[from] serde_json::Error),
    NotFound(String),
    Invalid(String),
    TooManyRequests { scope: String },
}
```

with typed variants:

```rust
pub enum Error {
    Storage(#[from] agentdb_storage::Error),
    Serde(#[from] serde_json::Error),
    NotFound { resource: Resource, id: String },
    Invalid { field: Option<String>, message: String },
    TooManyRequests { scope: Scope, retry_after_ms: u32 },
    FaultInjected { box_id: BoxId, fault_kind: FaultKind },
}

pub enum Resource { Tenant, Session, Box_ }
```

`Scope` already exists from M10 (`agentdb_core::admission::Scope`);
reuse it. `FaultKind` is the existing `agentdb_core::FaultKind`.

Note: `TooManyRequests { scope: String }` → `{ scope: Scope, retry_after_ms: u32 }`.
Call sites that construct this error today use a string; they'll
build a `Scope` enum value and compute `retry_after_ms` from the
token bucket's refill rate (or pass a sensible default when the
bucket hasn't been sized yet).

### `ApiError` consolidation

`ApiError` today has two variants: `Core(agentdb_core::Error)` and
`BadRequest(String)`. With `Error::Invalid { field, message }` now
typed, the `BadRequest(String)` variant is redundant — every
previous `BadRequest("...")` call site becomes
`Error::Invalid { field: Some("..."), message: "..." }` (or `field:
None` for general bad-request errors). `ApiError` collapses to a
single `Core(Error)` wrapper; the `#[derive(From)]` stays. This
keeps one error enum as the source of truth.

### REST error serialization

`impl IntoResponse for ApiError` composes the response:

```json
HTTP/1.1 429 Too Many Requests
Content-Type: application/json
Retry-After: 1

{
  "code": "quota_exceeded",
  "type": "rate_limit_error",
  "message": "Rate limit exceeded on box 2 (tenant scope)",
  "scope": "tenant",
  "retry_after_ms": 1200
}
```

Always-present fields: `code`, `type`, `message`. Typed extras per
code per the table above, serialized flat (not under a `details`
wrapper).

`message` is human-readable and built from the error's `Display`
impl. It's allowed to change between releases — agents branch on
`code`, not `message`.

The `Retry-After` HTTP header continues to ride alongside the body
for `quota_exceeded` (already done in M10). The `retry_after_ms`
JSON field gives finer-grained guidance for non-HTTP clients.

### OpenAPI generation

`utoipa` drives the spec from the code:

```rust
// In agentdb-api/src/lib.rs

#[derive(OpenApi)]
#[openapi(
    paths(
        handlers::append_and_fetch,
        fork::fork_handler,
        admin::force_tier,
        admin_boxes::list_boxes,
        // ... every REST handler
    ),
    components(schemas(
        dto::AppendFetchRequest,
        dto::AppendFetchResponse,
        error::ErrorResponse,
        error::ErrorCode,
        // ... every request/response + error schema
    )),
    tags(
        (name = "primitives", description = "Agent-native write/read primitives"),
        (name = "admin", description = "Operator-only endpoints"),
        (name = "stream", description = "Server-Sent Events"),
    )
)]
pub struct ApiDoc;

pub fn router(state: AppState) -> Router {
    Router::new()
        // ... existing routes ...
        .route("/openapi.json", get(openapi_json))
        .with_state(state)
}

async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}
```

Each handler is annotated:

```rust
#[utoipa::path(
    post,
    path = "/api/tenants/{tenant_id}/sessions/{session_id}/append_and_fetch",
    tag = "primitives",
    params(
        ("tenant_id" = String, Path, description = "External tenant id"),
        ("session_id" = String, Path, description = "External session id"),
    ),
    request_body = AppendFetchRequest,
    responses(
        (status = 200, description = "Event appended, recent events returned", body = AppendFetchResponse),
        (status = 400, description = "Malformed request body", body = ErrorResponse),
        (status = 429, description = "Rate limit exceeded", body = ErrorResponse),
        (status = 503, description = "Fault injected", body = ErrorResponse),
    ),
)]
pub async fn append_and_fetch(...) -> Result<Json<AppendFetchResponse>, ApiError> {
    // unchanged handler body
}
```

### SSE endpoint caveat

`/api/stream` is documented as a path in OpenAPI, but its response
body is described as "see `StreamEvent` schema; emitted repeatedly
as an event stream over `text/event-stream`." OpenAPI does not model
event streams well and we don't pretend otherwise. The `StreamEvent`
schema itself is fully defined so SDK generators can type event
payloads.

### Snapshot testing

```rust
#[test]
fn openapi_snapshot_matches() {
    let spec = agentdb_api::ApiDoc::openapi();
    let json = serde_json::to_string_pretty(&spec).unwrap();
    let expected = include_str!("../snapshots/openapi.json");
    if json.trim() != expected.trim() {
        eprintln!("OpenAPI snapshot drift. Run `cargo test snapshot -- --bless` or update manually.");
        assert_eq!(json.trim(), expected.trim());
    }
}
```

`tests/snapshots/openapi.json` is checked into git. On intentional
changes, the committer updates it in the same commit. Reviewers see
the diff.

### Integration tests

One per error code (six total, or five if we skip `storage_error`
which is hard to trigger cleanly):

- `not_found` — `GET /api/admin/sessions/nonexistent?tenant_id=t-x`
  → 404 + `{"code": "not_found", "resource": "session", ...}`
- `invalid_request` — malformed append body → 400 + `{"code": "invalid_request", ...}`
- `quota_exceeded` — set tight quota, hammer past it → 429 + typed
  body + `Retry-After: 1` header
- `fault_injected` — inject error fault, append → 503 + `{"code": "fault_injected", "box_id": 0, "fault_kind": "error"}`
- `serialization_error` — send invalid JSON → 400 with
  `invalid_request` (the internal `serialization_error` code is for
  failures the server encounters on its own data; client-side decode
  failures are `invalid_request`)

### Docs

Update `docs/ARCHITECTURE.md` with:

- Short "Error taxonomy" section near "Request flow"
- Pointer to `/openapi.json` as the source of truth for the REST
  surface
- Note that Mongo wire errors follow the Mongo protocol and are
  documented separately

## Risks

1. **Call-site refactor is wide.** Every handler that currently
   constructs `Err(ApiError::BadRequest(format!(...)))` or
   `Err(Error::Invalid(format!(...)))` needs to be updated to a
   typed variant. Scope: ~15–20 call sites across `agentdb-api/src/*.rs`
   and `agentdb-core/src/*.rs`. Manageable but tedious.

2. **Utoipa annotation maintenance.** New handlers must be
   annotated; annotations must stay in sync with the function
   signatures. Mitigation: the snapshot test catches missing or
   out-of-sync handlers (they won't appear in the spec, so the
   snapshot diff surfaces it).

3. **Snapshot churn.** Every handler signature change updates the
   snapshot. Acceptable — that's the point of snapshot tests.

4. **Tool-description prose for future MCP.** Utoipa lets us write
   `description = "..."` inline, but long multi-paragraph agent-
   facing descriptions get ugly in handler source files. For now,
   we keep descriptions concise (one or two sentences each); the
   richer "when to use this vs that" prose can live in M14's
   separate skill doc. Not a blocker for M13.

5. **`TooManyRequests` needs `retry_after_ms`.** Today the error
   only carries `scope`. The token bucket admission path in M10
   doesn't currently compute a retry-after value. For M13 we pick a
   default (`1000` ms) when the bucket can't give a precise number;
   the admission path can be refined in a later milestone to surface
   the true refill estimate.

6. **`FaultInjected` is new.** Today the fault-inject path returns
   `Error::Invalid("box ... has an injected error fault")` — a plain
   string. M13 promotes it to a first-class variant so agents can
   distinguish "server is deliberately failing this request for
   chaos-testing reasons" from "your request was malformed." The
   call site in `primitives::append_and_fetch` needs to construct
   the typed variant.

## Success criteria

- All existing tests pass.
- `GET /openapi.json` serves a valid OpenAPI 3.x document.
- Every handler is discoverable in the spec (each has a `path` and
  a schema for request + response).
- Snapshot test passes against a checked-in `openapi.json`.
- Each of the six error codes has a test that exercises it and
  asserts the correct JSON shape + HTTP status + (for
  `quota_exceeded`) the `Retry-After` header.
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
- `cargo fmt --all --check` clean.
- `bash scripts/smoke.sh` prints `smoke ok`.
- ARCHITECTURE.md describes the taxonomy + points at `/openapi.json`.

## What this unlocks

- **M14 MCP server** can be generated from (or validated against)
  `ApiDoc::openapi()` in the same process. Tool schemas will
  reference the same `ErrorResponse` component.
- **M15 Typed SDK** (TS first, Python later) uses
  `openapi-generator` or similar against the checked-in snapshot
  or the live endpoint.
- **Dashboard** gains the option (not built in M13) to auto-render
  an API explorer from the spec, replacing hand-maintained curl
  examples in the README.
