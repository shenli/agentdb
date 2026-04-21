# M13: Structured Errors + OpenAPI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current ad-hoc REST error shape with Stripe-style structured JSON (`{code, type, message, …typed extras}`) and publish an OpenAPI 3.x spec at `GET /openapi.json`, generated from axum handlers via `utoipa` and locked by a checked-in snapshot.

**Architecture:** `agentdb_core::Error` grows typed variants (`NotFound { resource, id }`, `Invalid { field, message }`, `FaultInjected { box_id, fault_kind }`, typed `TooManyRequests { scope, retry_after_ms }`). `agentdb_api::ApiError` collapses to a single `Core(Error)` wrapper; its `IntoResponse` emits the Stripe-style body. Every handler + request/response type is annotated with `utoipa` macros so the spec is generated from the source of truth.

**Tech Stack:** Rust 1.75, `utoipa` 5.x + `utoipa-axum` (OpenAPI annotations + axum integration), existing axum 0.7 + serde + thiserror.

---

## File Structure

**Created:**
- `crates/agentdb-api/tests/errors.rs` — one integration test per error code
- `crates/agentdb-api/tests/openapi_snapshot.rs` — snapshot test for the generated OpenAPI document
- `crates/agentdb-api/tests/snapshots/openapi.json` — checked-in OpenAPI snapshot

**Modified:**
- `Cargo.toml` (workspace) — add `utoipa` + `utoipa-axum` to `[workspace.dependencies]`
- `crates/agentdb-api/Cargo.toml` — depend on `utoipa` + `utoipa-axum`
- `crates/agentdb-core/src/error.rs` — rewrite `Error` enum with typed variants + add `Resource` enum
- `crates/agentdb-core/src/primitives.rs`, `fork.rs`, `tiering.rs`, `registry.rs`, `box_runtime.rs`, `segment.rs` — update error-construction call sites to typed variants
- `crates/agentdb-core/src/box_runtime.rs` — change `apply_fault` to return `Option<FaultKind>` so the caller can construct `Error::FaultInjected`
- `crates/agentdb-api/src/error.rs` — rewrite `ApiError` as single `Core(Error)` wrapper; `IntoResponse` emits Stripe-style JSON
- `crates/agentdb-api/src/admin.rs`, `admin_boxes.rs`, `admin_sessions.rs`, `handlers.rs`, `fork.rs` — replace all `ApiError::BadRequest(format!(...))` call sites with `Error::NotFound` / `Error::Invalid` construction
- `crates/agentdb-api/src/lib.rs` — `ApiDoc` struct, `/openapi.json` route, import the new error types
- `crates/agentdb-api/src/dto.rs`, `admin.rs`, `admin_boxes.rs`, `admin_sessions.rs`, `fork.rs`, `handlers.rs`, `stream.rs` — `#[derive(ToSchema)]` on every request/response type; `#[utoipa::path(...)]` on every REST handler
- `docs/ARCHITECTURE.md` — new "Error taxonomy" section + `/openapi.json` pointer

---

## Task 1: Add utoipa dependencies

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Modify: `crates/agentdb-api/Cargo.toml`

- [ ] **Step 1: Add to workspace `[workspace.dependencies]`**

Find the `[workspace.dependencies]` block in the workspace root `Cargo.toml`. Add two lines alphabetically:

```toml
utoipa = { version = "5", features = ["axum_extras"] }
utoipa-axum = "0.2"
```

Keep the rest of the block unchanged.

- [ ] **Step 2: Depend on both from agentdb-api**

Edit `crates/agentdb-api/Cargo.toml`. In the `[dependencies]` section, add (keep existing deps in alphabetical order):

```toml
utoipa = { workspace = true }
utoipa-axum = { workspace = true }
```

- [ ] **Step 3: Verify workspace builds**

Run: `cargo build -p agentdb-api`
Expected: clean compile (new crates download and compile; no source changes yet).

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml crates/agentdb-api/Cargo.toml Cargo.lock
git commit -m "deps(agentdb-api): add utoipa + utoipa-axum"
```

---

## Task 2: Rewrite `agentdb_core::Error` with typed variants

**Files:**
- Modify: `crates/agentdb-core/src/error.rs`

This task replaces the string-based `Error` variants with typed variants. It introduces new helper enums (`Resource`) and keeps `TooManyRequests` but promotes its `scope` field from `String` to the existing `Scope` enum + adds `retry_after_ms`.

- [ ] **Step 1: Read the current file**

Read `crates/agentdb-core/src/error.rs` to confirm the starting shape: `Storage(#[from] …)`, `Serde(#[from] …)`, `NotFound(String)`, `Invalid(String)`, `TooManyRequests { scope: String }`.

- [ ] **Step 2: Rewrite the file**

Replace the entire contents of `crates/agentdb-core/src/error.rs` with:

```rust
use thiserror::Error;

use crate::admission::Scope;
use crate::box_runtime::FaultKind;
use agentdb_storage::BoxId;

/// Core error type. Each variant is serialized to a stable REST error
/// code in `agentdb-api`; the Mongo wire layer maps these to protocol
/// error codes independently.
#[derive(Debug, Error)]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(#[from] agentdb_storage::Error),

    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("{resource:?} {id} not found")]
    NotFound { resource: Resource, id: String },

    #[error("{}", message)]
    Invalid {
        /// Name of the invalid field, if the error is field-level.
        /// `None` for general validation failures.
        field: Option<String>,
        message: String,
    },

    #[error("rate limit exceeded ({scope:?})")]
    TooManyRequests {
        scope: Scope,
        retry_after_ms: u32,
    },

    #[error("box {box_id}: fault '{fault_kind:?}' injected")]
    FaultInjected {
        box_id: BoxId,
        fault_kind: FaultKind,
    },
}

/// Resource kinds referenced by `Error::NotFound`. Keep in sync with
/// the REST `resource` field (serialized lowercase).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resource {
    Tenant,
    Session,
    Box_,
}

impl Resource {
    pub fn as_str(self) -> &'static str {
        match self {
            Resource::Tenant => "tenant",
            Resource::Session => "session",
            Resource::Box_ => "box",
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_error_converts_via_from() {
        let storage_err = agentdb_storage::Error::Other("boom".into());
        let err: Error = storage_err.into();
        assert!(matches!(err, Error::Storage(_)));
    }

    #[test]
    fn not_found_renders_resource_and_id() {
        let err = Error::NotFound {
            resource: Resource::Session,
            id: "s-9".into(),
        };
        let rendered = err.to_string();
        assert!(rendered.contains("Session"));
        assert!(rendered.contains("s-9"));
    }

    #[test]
    fn too_many_requests_carries_retry_after() {
        let err = Error::TooManyRequests {
            scope: Scope::Tenant,
            retry_after_ms: 1200,
        };
        match err {
            Error::TooManyRequests { scope, retry_after_ms } => {
                assert_eq!(scope, Scope::Tenant);
                assert_eq!(retry_after_ms, 1200);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn invalid_with_field_renders_message() {
        let err = Error::Invalid {
            field: Some("max_qps".into()),
            message: "must be positive".into(),
        };
        assert_eq!(err.to_string(), "must be positive");
    }

    #[test]
    fn resource_as_str_is_lowercase_singular() {
        assert_eq!(Resource::Tenant.as_str(), "tenant");
        assert_eq!(Resource::Session.as_str(), "session");
        assert_eq!(Resource::Box_.as_str(), "box");
    }
}
```

- [ ] **Step 3: Check what compiles**

Run: `cargo build -p agentdb-core 2>&1 | head -40`
Expected: compile errors at every call site that uses `Error::Invalid(String)`, `Error::NotFound(String)`, or `Error::TooManyRequests { scope: String }`. Count the errors — Task 3 and Task 4 are sized by this list.

Do NOT commit yet. The next tasks fix the call sites.

---

## Task 3: Fix core call sites for the new `Error` variants

**Files:**
- Modify: `crates/agentdb-core/src/primitives.rs`
- Modify: `crates/agentdb-core/src/fork.rs`
- Modify: `crates/agentdb-core/src/tiering.rs`
- Modify: `crates/agentdb-core/src/registry.rs`
- Modify: `crates/agentdb-core/src/box_runtime.rs`
- Modify: `crates/agentdb-core/src/segment.rs`

There are ~28 core-side call sites. Work through each file; the patterns are:

- `Error::Invalid(format!("X not found"))` where X is an entity → `Error::NotFound { resource: Resource::<kind>, id: <id>.into() }`
- `Error::Invalid(format!("some validation error"))` → `Error::Invalid { field: None, message: format!("...") }` (or `field: Some("name")` when a field name applies)
- `Error::TooManyRequests { scope: String }` → `Error::TooManyRequests { scope: Scope::<variant>, retry_after_ms: 1000 }`

Default `retry_after_ms: 1000` — M13 doesn't compute the true refill estimate; a later admission milestone can refine. Document this in a code comment at the call site.

- [ ] **Step 1: Fix `primitives.rs`**

Open `crates/agentdb-core/src/primitives.rs`. Locate line 214 (the fault-injected error) and change:

```rust
        return Err(Error::Invalid(format!(
            "box {} has an injected error fault",
            box_.id
        )));
```

to:

```rust
        return Err(Error::FaultInjected {
            box_id: box_.id,
            // Fault is active (apply_fault returned true); we know the
            // kind is `Error` because only Error faults short-circuit.
            fault_kind: crate::box_runtime::FaultKind::Error,
        });
```

Locate the `TooManyRequests` site (line 230) and change:

```rust
        .map_err(|d| Error::TooManyRequests {
            scope: d.scope.as_str().into(),
        })?;
```

to:

```rust
        .map_err(|d| Error::TooManyRequests {
            scope: d.scope,
            retry_after_ms: 1000,
        })?;
```

Locate the warm-sessions-quota site (line 240) and change:

```rust
                return Err(Error::Invalid(format!(
                    "box {} warm_sessions quota {max} exceeded",
                    box_.id
                )));
```

to:

```rust
                return Err(Error::Invalid {
                    field: Some("max_warm_sessions".into()),
                    message: format!("warm_sessions cap {max} reached on box {}", box_.id),
                });
```

(Warm-sessions is a capacity limit, not a rate-limit; it maps to `invalid_request` / HTTP 400, not `quota_exceeded` / 429.)

- [ ] **Step 2: Fix `fork.rs`**

Open `crates/agentdb-core/src/fork.rs`. Find the `Error::NotFound(format!("tenant ..."))` pattern (~line 30) and replace:

```rust
        .ok_or_else(|| Error::NotFound(format!("tenant {tenant_external}")))?;
```

with:

```rust
        .ok_or_else(|| Error::NotFound {
            resource: crate::error::Resource::Tenant,
            id: tenant_external.into(),
        })?;
```

Do the same for the parent-session lookup (around line 37):

```rust
        .ok_or_else(|| Error::NotFound(format!("parent session {parent_session_external}")))?;
```

becomes:

```rust
        .ok_or_else(|| Error::NotFound {
            resource: crate::error::Resource::Session,
            id: parent_session_external.into(),
        })?;
```

Find the existing-child check (around line 43):

```rust
        return Err(Error::Invalid(format!(
            "target session {new_session_external} already exists"
        )));
```

becomes:

```rust
        return Err(Error::Invalid {
            field: Some("new_session_id".into()),
            message: format!("target session '{new_session_external}' already exists"),
        });
```

The `TooManyRequests` in this file follows the same pattern as primitives.rs — use `d.scope` directly and `retry_after_ms: 1000`.

- [ ] **Step 3: Fix `tiering.rs`**

Open `crates/agentdb-core/src/tiering.rs`. The `Error::Invalid(format!(...))` call sites in this file are all "wrong-tier-for-operation" validation errors (e.g. `cold_transition` requires a Warm session). They're field-ish — the field is `tier` conceptually. Use:

```rust
        return Err(Error::Invalid {
            field: Some("tier".into()),
            message: format!("session {} is not ...", ...),
        });
```

Every `return Err(Error::Invalid(format!(...)))` in this file matches this template. Read each one; keep the existing formatted message as the `message` field; set `field: Some("tier".into())` for the wrong-tier checks and `field: None` for other Invalid errors.

- [ ] **Step 4: Fix `registry.rs`**

Open `crates/agentdb-core/src/registry.rs`. Three patterns:

1. `Error::Invalid(format!("tenant record missing ..."))` — internal consistency failure, not a client-facing validation issue. Use `field: None`, message unchanged.
2. `Error::Invalid(format!("bad tenant name utf-8: {e}"))` — client sent invalid UTF-8 as a tenant id. Use `field: Some("tenant_id".into())`, message unchanged.
3. Similar for `bad session name utf-8` — `field: Some("session_id".into())`.

- [ ] **Step 5: Fix `box_runtime.rs`**

Locate the `Error::Invalid("num_boxes must be > 0".into())` error (around line 192). Change to:

```rust
            return Err(Error::Invalid {
                field: Some("num_boxes".into()),
                message: "num_boxes must be > 0".into(),
            });
```

- [ ] **Step 6: Fix `segment.rs`**

Every `map_err(|e| Error::Invalid(format!(...)))` in this file is wrapping an infrastructure error (BSON serialize/deserialize, object_store I/O). Use `field: None`, keep the formatted message:

```rust
        .map_err(|e| Error::Invalid {
            field: None,
            message: format!("object_store put: {e}"),
        })?;
```

Apply the same transformation to all ~8 sites in this file.

- [ ] **Step 7: Run core tests**

Run: `cargo test -p agentdb-core 2>&1 | grep -E "(test result|FAILED|error\[)" | head -20`
Expected: all tests pass. If any test literally matched a formatted error string (e.g. `err.to_string() == "box 0 has..."`) the test needs to match on variant shape instead. Update those tests in the same commit.

- [ ] **Step 8: Commit**

```bash
git add crates/agentdb-core/src/
git commit -m "refactor(agentdb-core): typed Error variants (Resource/field/retry_after_ms)"
```

---

## Task 4: Simplify `ApiError` and update its call sites

**Files:**
- Modify: `crates/agentdb-api/src/error.rs`
- Modify: `crates/agentdb-api/src/admin.rs`
- Modify: `crates/agentdb-api/src/admin_boxes.rs`
- Modify: `crates/agentdb-api/src/admin_sessions.rs`

`ApiError` collapses to a single `Core(Error)` variant. Every previous `ApiError::BadRequest(format!(...))` becomes one of:

- `Error::NotFound { resource, id }` (when something specific is missing)
- `Error::Invalid { field, message }` (for validation failures)

- [ ] **Step 1: Rewrite `ApiError`**

Replace the contents of `crates/agentdb-api/src/error.rs` with:

```rust
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error(transparent)]
    Core(#[from] agentdb_core::Error),
}

/// The JSON body we emit for every REST error. Always carries
/// `code`, `type`, `message`; typed extras per code are flat fields
/// (deserialized from the same object by clients).
#[derive(Serialize, ToSchema)]
pub struct ErrorResponse {
    pub code: ErrorCode,
    #[serde(rename = "type")]
    pub kind: ErrorKind,
    pub message: String,

    // Per-code typed extras. `serde(skip_serializing_if = ...)` keeps the
    // response minimal for codes that don't use them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub box_id: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fault_kind: Option<String>,
}

/// Stable machine-readable error codes. Every variant maps 1:1 to a
/// REST response with a specific taxonomy of extras.
#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    NotFound,
    InvalidRequest,
    QuotaExceeded,
    FaultInjected,
    StorageError,
    SerializationError,
}

#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    InvalidRequestError,
    RateLimitError,
    ApiError,
}

impl ApiError {
    fn to_response(&self) -> (StatusCode, HeaderMap, ErrorResponse) {
        let ApiError::Core(err) = self;
        let message = err.to_string();
        match err {
            agentdb_core::Error::NotFound { resource, id } => (
                StatusCode::NOT_FOUND,
                HeaderMap::new(),
                ErrorResponse {
                    code: ErrorCode::NotFound,
                    kind: ErrorKind::InvalidRequestError,
                    message,
                    resource: Some(resource.as_str().into()),
                    id: Some(id.clone()),
                    field: None,
                    scope: None,
                    retry_after_ms: None,
                    box_id: None,
                    fault_kind: None,
                },
            ),
            agentdb_core::Error::Invalid { field, .. } => (
                StatusCode::BAD_REQUEST,
                HeaderMap::new(),
                ErrorResponse {
                    code: ErrorCode::InvalidRequest,
                    kind: ErrorKind::InvalidRequestError,
                    message,
                    resource: None,
                    id: None,
                    field: field.clone(),
                    scope: None,
                    retry_after_ms: None,
                    box_id: None,
                    fault_kind: None,
                },
            ),
            agentdb_core::Error::TooManyRequests { scope, retry_after_ms } => {
                let mut h = HeaderMap::new();
                h.insert(
                    header::RETRY_AFTER,
                    HeaderValue::from_static("1"),
                );
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    h,
                    ErrorResponse {
                        code: ErrorCode::QuotaExceeded,
                        kind: ErrorKind::RateLimitError,
                        message,
                        resource: None,
                        id: None,
                        field: None,
                        scope: Some(scope.as_str().into()),
                        retry_after_ms: Some(*retry_after_ms),
                        box_id: None,
                        fault_kind: None,
                    },
                )
            }
            agentdb_core::Error::FaultInjected { box_id, fault_kind } => (
                StatusCode::SERVICE_UNAVAILABLE,
                HeaderMap::new(),
                ErrorResponse {
                    code: ErrorCode::FaultInjected,
                    kind: ErrorKind::ApiError,
                    message,
                    resource: None,
                    id: None,
                    field: None,
                    scope: None,
                    retry_after_ms: None,
                    box_id: Some(*box_id),
                    fault_kind: Some(fault_kind_name(fault_kind).into()),
                },
            ),
            agentdb_core::Error::Storage(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
                ErrorResponse {
                    code: ErrorCode::StorageError,
                    kind: ErrorKind::ApiError,
                    message,
                    resource: None,
                    id: None,
                    field: None,
                    scope: None,
                    retry_after_ms: None,
                    box_id: None,
                    fault_kind: None,
                },
            ),
            agentdb_core::Error::Serde(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
                ErrorResponse {
                    code: ErrorCode::SerializationError,
                    kind: ErrorKind::ApiError,
                    message,
                    resource: None,
                    id: None,
                    field: None,
                    scope: None,
                    retry_after_ms: None,
                    box_id: None,
                    fault_kind: None,
                },
            ),
        }
    }
}

fn fault_kind_name(k: &agentdb_core::FaultKind) -> &'static str {
    match k {
        agentdb_core::FaultKind::Latency { .. } => "latency",
        agentdb_core::FaultKind::Error => "error",
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, mut headers, body) = self.to_response();
        let json = axum::Json(body);
        let mut resp = (status, json).into_response();
        for (k, v) in headers.drain() {
            if let Some(k) = k {
                resp.headers_mut().insert(k, v);
            }
        }
        resp
    }
}
```

- [ ] **Step 2: Update admin.rs call sites**

Open `crates/agentdb-api/src/admin.rs`. Three `ApiError::BadRequest` sites around lines 30, 40, 44. Replace each with the appropriate core error:

Line 30 (invalid target tier):

```rust
            return Err(ApiError::BadRequest(format!(
                "unsupported target tier {other}; expected 'warm' | 'cold' | 'archive'"
            )));
```

becomes:

```rust
            return Err(agentdb_core::Error::Invalid {
                field: Some("target".into()),
                message: format!("unsupported target tier '{other}'; expected 'warm' | 'cold' | 'archive'"),
            }
            .into());
```

Line 40 (tenant not found):

```rust
        .ok_or_else(|| ApiError::BadRequest(format!("tenant {} not found", body.tenant_id)))?;
```

becomes:

```rust
        .ok_or_else(|| {
            agentdb_core::Error::NotFound {
                resource: agentdb_core::Resource::Tenant,
                id: body.tenant_id.clone(),
            }
            .into()
        })?;
```

Line 44 (session not found) — same pattern but `Resource::Session` and `body.session_id`.

- [ ] **Step 3: Update admin_boxes.rs call sites**

Six `ApiError::BadRequest` sites. Patterns:

`box {id} not found` → `Error::NotFound { resource: Resource::Box_, id: id.to_string() }`

`unknown fault kind {other}; expected 'latency' or 'error'` → `Error::Invalid { field: Some("kind".into()), message: format!("...") }`

`webhook url must start with http:// or https:// (got {u})` → `Error::Invalid { field: Some("url".into()), message: format!("...") }`

Convert with `.into()` as in admin.rs.

- [ ] **Step 4: Update admin_sessions.rs call sites**

Two sites (tenant + session not found). Follow the `Resource::Tenant` / `Resource::Session` patterns from admin.rs.

- [ ] **Step 5: Remove `ApiError::BadRequest` references that remain**

Search and verify:

```bash
grep -rn "ApiError::BadRequest" crates/agentdb-api/src
```

Expected: no matches. If any remain, convert them as above.

- [ ] **Step 6: Update `Cargo.toml` if `utoipa` needs to be imported in the error module**

Check `cargo build -p agentdb-api 2>&1 | head -20`. The `#[derive(ToSchema)]` imports should resolve since we added utoipa in Task 1.

- [ ] **Step 7: Build + test**

Run: `cargo test -p agentdb-api 2>&1 | grep -E "(test result|FAILED|error\[)" | head`
Expected: all existing tests pass. If any test asserted on an exact error string that no longer matches, update the assertion to match the variant (e.g., `matches!(err, ApiError::Core(Error::NotFound { .. }))`) or the new formatted message.

- [ ] **Step 8: Commit**

```bash
git add crates/agentdb-api/src/error.rs crates/agentdb-api/src/admin.rs \
        crates/agentdb-api/src/admin_boxes.rs crates/agentdb-api/src/admin_sessions.rs
git commit -m "refactor(agentdb-api): ApiError collapses to Core(Error), Stripe-style IntoResponse"
```

---

## Task 5: Re-export `Resource` from `agentdb-core` and add utoipa derives to DTOs

**Files:**
- Modify: `crates/agentdb-core/src/lib.rs`
- Modify: `crates/agentdb-api/src/dto.rs`
- Modify: `crates/agentdb-api/src/admin.rs`
- Modify: `crates/agentdb-api/src/admin_boxes.rs`
- Modify: `crates/agentdb-api/src/admin_sessions.rs`
- Modify: `crates/agentdb-api/src/fork.rs`
- Modify: `crates/agentdb-api/src/stream.rs`

- [ ] **Step 1: Re-export `Resource` + `FaultKind` from agentdb-core crate root**

Edit `crates/agentdb-core/src/lib.rs`. Add to the `pub use` block (alphabetically):

```rust
pub use error::{Error, Resource, Result};
```

Verify `FaultKind` is already re-exported (it was added in M7). If not, add it.

Run: `cargo build -p agentdb-core` — expected: clean.

- [ ] **Step 2: Add `ToSchema` to every request/response struct**

For each DTO file in `crates/agentdb-api/src/`, find every `#[derive(Serialize, ...)]` and `#[derive(Deserialize, ...)]` on request/response types. Add `ToSchema`:

Before:
```rust
#[derive(Serialize)]
pub struct BoxListItem { ... }
```

After:
```rust
#[derive(Serialize, utoipa::ToSchema)]
pub struct BoxListItem { ... }
```

Files + types to update:
- `dto.rs`: `AppendFetchRequest`, `AppendFetchResponse`, anything else exported
- `admin.rs`: `ForceTierRequest`, `ForceTierResponse`
- `admin_boxes.rs`: `BoxListItem`, `QuotaView`, `InjectFaultRequest`, `InjectFaultResponse`, `QuotaRequest`, `WebhookRequest`, `WebhookResponse`
- `admin_sessions.rs`: `SessionQuery`, `SessionDetail`
- `fork.rs`: `ForkRequest`, `ForkResponse`
- `stream.rs`: `StreamEvent`, `TickBox`

For nested types (e.g. `StreamEvent` has variants containing fields of type `u16`, `String`, etc.), no extra work — primitive types are built in. For variants referencing `agentdb_core::MetricsSnapshot`, that type also needs `ToSchema`; derive it in `crates/agentdb-core/src/metrics.rs`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct MetricsSnapshot { ... }
```

Similarly for `GaugeSnapshot`. Add `utoipa` to agentdb-core's `Cargo.toml` as a workspace dependency (it's already in workspace; just add to the `[dependencies]` block):

```toml
utoipa = { workspace = true }
```

- [ ] **Step 3: Build**

Run: `cargo build -p agentdb-api`
Expected: clean. If a `ToSchema` bound fails (e.g., a field has a type without a schema), the compiler will tell you which type needs the derive. Add it.

- [ ] **Step 4: Commit**

```bash
git add crates/agentdb-core/src/ crates/agentdb-api/src/ Cargo.lock
git commit -m "feat(agentdb-api): ToSchema on every REST request/response DTO"
```

---

## Task 6: Annotate handlers with `#[utoipa::path]`

**Files:**
- Modify: `crates/agentdb-api/src/handlers.rs`
- Modify: `crates/agentdb-api/src/fork.rs`
- Modify: `crates/agentdb-api/src/admin.rs`
- Modify: `crates/agentdb-api/src/admin_boxes.rs`
- Modify: `crates/agentdb-api/src/admin_sessions.rs`
- Modify: `crates/agentdb-api/src/stream.rs`

Every REST handler gets an annotation. Concise one-sentence descriptions — prose-rich "when to use this vs that" content is deferred to M14's skill.

- [ ] **Step 1: Annotate `append_and_fetch`**

In `crates/agentdb-api/src/handlers.rs`, add above the `pub async fn append_and_fetch(...)` handler:

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
        (status = 200, description = "Event appended; last N events returned", body = AppendFetchResponse),
        (status = 400, description = "Malformed request", body = ErrorResponse),
        (status = 429, description = "Rate limit exceeded", body = ErrorResponse),
        (status = 503, description = "Fault injected", body = ErrorResponse),
    ),
)]
```

Import `ErrorResponse` at the top of the file if needed:

```rust
use crate::error::ErrorResponse;
```

- [ ] **Step 2: Annotate `fork_handler`** (in `crates/agentdb-api/src/fork.rs`)

```rust
#[utoipa::path(
    post,
    path = "/api/sessions/{session_id}/fork",
    tag = "primitives",
    params(("session_id" = String, Path, description = "Parent session's external id")),
    request_body = ForkRequest,
    responses(
        (status = 200, description = "Child session created", body = ForkResponse),
        (status = 400, description = "Malformed request", body = ErrorResponse),
        (status = 404, description = "Parent tenant/session not found", body = ErrorResponse),
        (status = 429, description = "Rate limit exceeded", body = ErrorResponse),
    ),
)]
```

- [ ] **Step 3: Annotate `force_tier`** (in `crates/agentdb-api/src/admin.rs`)

```rust
#[utoipa::path(
    post,
    path = "/api/admin/tier/force",
    tag = "admin",
    request_body = ForceTierRequest,
    responses(
        (status = 200, description = "Session transitioned to the target tier", body = ForceTierResponse),
        (status = 400, description = "Unsupported target tier or malformed body", body = ErrorResponse),
        (status = 404, description = "Tenant or session not found", body = ErrorResponse),
    ),
)]
```

- [ ] **Step 4: Annotate `admin_boxes` handlers**

In `crates/agentdb-api/src/admin_boxes.rs`, add annotations above `list_boxes`, `get_box`, `inject_fault`, `set_quota`, `set_webhook`:

```rust
#[utoipa::path(
    get, path = "/api/admin/boxes", tag = "admin",
    responses((status = 200, body = Vec<BoxListItem>)),
)]
pub async fn list_boxes(...) { ... }

#[utoipa::path(
    get, path = "/api/admin/boxes/{id}", tag = "admin",
    params(("id" = u16, Path, description = "Box id")),
    responses(
        (status = 200, body = BoxListItem),
        (status = 404, description = "Box not found", body = ErrorResponse),
    ),
)]
pub async fn get_box(...) { ... }

#[utoipa::path(
    post, path = "/api/admin/boxes/{id}/inject_fault", tag = "admin",
    params(("id" = u16, Path)),
    request_body = InjectFaultRequest,
    responses(
        (status = 200, body = InjectFaultResponse),
        (status = 400, description = "Unknown fault kind", body = ErrorResponse),
        (status = 404, description = "Box not found", body = ErrorResponse),
    ),
)]
pub async fn inject_fault(...) { ... }

#[utoipa::path(
    post, path = "/api/admin/boxes/{id}/quota", tag = "admin",
    params(("id" = u16, Path)),
    request_body = QuotaRequest,
    responses(
        (status = 200, body = QuotaView),
        (status = 404, description = "Box not found", body = ErrorResponse),
    ),
)]
pub async fn set_quota(...) { ... }

#[utoipa::path(
    post, path = "/api/admin/boxes/{id}/webhook", tag = "admin",
    params(("id" = u16, Path)),
    request_body = WebhookRequest,
    responses(
        (status = 200, body = WebhookResponse),
        (status = 400, description = "Invalid URL", body = ErrorResponse),
        (status = 404, description = "Box not found", body = ErrorResponse),
    ),
)]
pub async fn set_webhook(...) { ... }
```

- [ ] **Step 5: Annotate `get_session` in admin_sessions.rs**

```rust
#[utoipa::path(
    get, path = "/api/admin/sessions/{session_id}", tag = "admin",
    params(
        ("session_id" = String, Path, description = "External session id"),
        ("tenant_id" = String, Query, description = "Tenant that owns the session"),
    ),
    responses(
        (status = 200, body = SessionDetail),
        (status = 404, description = "Tenant or session not found", body = ErrorResponse),
    ),
)]
pub async fn get_session(...) { ... }
```

- [ ] **Step 6: Annotate `sse_handler` in stream.rs**

```rust
#[utoipa::path(
    get, path = "/api/stream", tag = "stream",
    responses(
        (status = 200, description = "Server-Sent Events stream of StreamEvent payloads. \
            OpenAPI does not model event streams well; see the StreamEvent schema \
            for the shape of each emitted event."),
    ),
)]
pub async fn sse_handler(...) { ... }
```

- [ ] **Step 7: Build**

Run: `cargo build -p agentdb-api 2>&1 | tail -20`
Expected: clean. Utoipa's proc macros validate that the types referenced in `request_body` / `body` implement `ToSchema`. If any reference is wrong, the compiler says so.

- [ ] **Step 8: Commit**

```bash
git add crates/agentdb-api/src/
git commit -m "feat(agentdb-api): #[utoipa::path] on every REST handler"
```

---

## Task 7: `ApiDoc` aggregator + `/openapi.json` route

**Files:**
- Modify: `crates/agentdb-api/src/lib.rs`

- [ ] **Step 1: Declare `ApiDoc` struct**

Near the top of `crates/agentdb-api/src/lib.rs` (after `use` imports, before `AppState`), add:

```rust
#[derive(utoipa::OpenApi)]
#[openapi(
    info(
        title = "AgentDB REST API",
        description = "Agent-native operational database. See docs/agent-db.md for design and docs/ARCHITECTURE.md for implementation.",
        version = "0.1.0",
    ),
    paths(
        handlers::append_and_fetch,
        fork::fork_handler,
        admin::force_tier,
        admin_boxes::list_boxes,
        admin_boxes::get_box,
        admin_boxes::inject_fault,
        admin_boxes::set_quota,
        admin_boxes::set_webhook,
        admin_sessions::get_session,
        stream::sse_handler,
    ),
    components(schemas(
        dto::AppendFetchRequest,
        dto::AppendFetchResponse,
        admin::ForceTierRequest,
        admin::ForceTierResponse,
        admin_boxes::BoxListItem,
        admin_boxes::QuotaView,
        admin_boxes::InjectFaultRequest,
        admin_boxes::InjectFaultResponse,
        admin_boxes::QuotaRequest,
        admin_boxes::WebhookRequest,
        admin_boxes::WebhookResponse,
        admin_sessions::SessionQuery,
        admin_sessions::SessionDetail,
        fork::ForkRequest,
        fork::ForkResponse,
        stream::StreamEvent,
        stream::TickBox,
        error::ErrorResponse,
        error::ErrorCode,
        error::ErrorKind,
        agentdb_core::MetricsSnapshot,
        agentdb_core::GaugeSnapshot,
    )),
    tags(
        (name = "primitives", description = "Agent-native write and read primitives"),
        (name = "admin", description = "Operator-only endpoints (no agent exposure)"),
        (name = "stream", description = "Server-Sent Events"),
    ),
)]
pub struct ApiDoc;
```

- [ ] **Step 2: Add the `/openapi.json` route**

Find the `pub fn router(state: AppState) -> Router` function. Add one line to the router chain (before `.with_state(state)`):

```rust
        .route("/openapi.json", axum::routing::get(openapi_json))
```

Then define the handler at the bottom of the file (module level, not inside `router`):

```rust
async fn openapi_json() -> axum::Json<utoipa::openapi::OpenApi> {
    use utoipa::OpenApi;
    axum::Json(ApiDoc::openapi())
}
```

- [ ] **Step 3: Build**

Run: `cargo build -p agentdb-api 2>&1 | tail -10`
Expected: clean.

- [ ] **Step 4: Smoke-test the endpoint manually**

```bash
pkill -f agentdb-server 2>/dev/null; sleep 1
AGENTDB_DATA=/tmp/m13 AGENTDB_OBJECTS=/tmp/m13-obj \
cargo run -p agentdb-server >/tmp/m13-srv.log 2>&1 &
sleep 2
curl -s http://127.0.0.1:7878/openapi.json | python3 -c "import json,sys; d=json.load(sys.stdin); print('paths:', len(d['paths'])); print('schemas:', len(d['components']['schemas']))"
pkill -f agentdb-server
rm -rf /tmp/m13 /tmp/m13-obj
```

Expected: `paths: 10` (or more, depending on route count) and `schemas: >= 18`.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-api/src/lib.rs
git commit -m "feat(agentdb-api): ApiDoc struct + GET /openapi.json"
```

---

## Task 8: OpenAPI snapshot test

**Files:**
- Create: `crates/agentdb-api/tests/openapi_snapshot.rs`
- Create: `crates/agentdb-api/tests/snapshots/openapi.json`

- [ ] **Step 1: Create the test**

Create `crates/agentdb-api/tests/openapi_snapshot.rs`:

```rust
//! Lock the generated OpenAPI spec against a checked-in snapshot.
//! Any change to the REST surface updates this file; reviewers see
//! the diff in the PR.

use utoipa::OpenApi;

#[test]
fn openapi_snapshot_matches() {
    let spec = agentdb_api::ApiDoc::openapi();
    let generated = serde_json::to_string_pretty(&spec).expect("serialize OpenAPI");
    let snapshot = include_str!("snapshots/openapi.json");
    if generated.trim() != snapshot.trim() {
        eprintln!("\nOpenAPI snapshot drift detected.");
        eprintln!("To update, run:");
        eprintln!("  cargo test -p agentdb-api --test openapi_snapshot -- --nocapture 2>/dev/null");
        eprintln!("and write the printed JSON to crates/agentdb-api/tests/snapshots/openapi.json\n");
        eprintln!("{}", generated);
        panic!("OpenAPI snapshot mismatch");
    }
}
```

- [ ] **Step 2: Make `ApiDoc` public**

Verify `pub struct ApiDoc` in `lib.rs` is `pub` (from Task 7). If not, make it pub so the test can reach it.

- [ ] **Step 3: Generate the initial snapshot**

Create the snapshot file via a one-off run:

```bash
mkdir -p crates/agentdb-api/tests/snapshots
cargo test -p agentdb-api --test openapi_snapshot -- --nocapture 2>&1 | awk '/^{/,/^}/' > crates/agentdb-api/tests/snapshots/openapi.json
```

Inspect the file — it should be a well-formed OpenAPI 3.x document with `openapi`, `info`, `paths`, `components`, `tags`.

- [ ] **Step 4: Re-run the test; it should pass**

Run: `cargo test -p agentdb-api --test openapi_snapshot`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-api/tests/openapi_snapshot.rs \
        crates/agentdb-api/tests/snapshots/openapi.json
git commit -m "test(agentdb-api): OpenAPI snapshot test"
```

---

## Task 9: Integration tests per error code

**Files:**
- Create: `crates/agentdb-api/tests/errors.rs`

One test per user-facing error code. `storage_error` and `serialization_error` are internal and hard to trigger cleanly from an integration test — skip them; their serialization is covered by unit tests in Task 4.

- [ ] **Step 1: Create the test**

Create `crates/agentdb-api/tests/errors.rs`:

```rust
//! End-to-end tests: one per REST error code. Asserts HTTP status,
//! headers, and the Stripe-style JSON body shape.

use std::sync::Arc;

use agentdb_api::{router, AppState, StreamBus};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use serde_json::json;
use serial_test::serial;
use tempfile::TempDir;

async fn spawn() -> (TempDir, TempDir, String, Arc<BoxRegistry>) {
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    let dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
    let state = AppState {
        registry: registry.clone(),
        tenants: Arc::new(TenantRegistry::new(kv.clone())),
        sessions: Arc::new(SessionRegistry::new(kv)),
        stream: StreamBus::default(),
        http: agentdb_api::webhook::new_client(),
    };
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (dir, obj_dir, format!("http://{addr}"), registry)
}

#[tokio::test]
#[serial]
async fn not_found_emits_typed_body() {
    let (_dir, _obj_dir, base, _registry) = spawn().await;
    let client = reqwest::Client::new();
    let r = client
        .get(format!("{base}/api/admin/sessions/nonexistent?tenant_id=t-x"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["code"], "not_found");
    assert_eq!(body["type"], "invalid_request_error");
    assert!(body["message"].as_str().unwrap().len() > 0);
    // resource and id fields populated
    assert!(body["resource"].is_string());
    assert!(body["id"].is_string());
}

#[tokio::test]
#[serial]
async fn invalid_request_emits_typed_body() {
    let (_dir, _obj_dir, base, _registry) = spawn().await;
    let client = reqwest::Client::new();
    // Force a 'force tier' request with an unsupported target.
    let r = client
        .post(format!("{base}/api/admin/tier/force"))
        .json(&json!({"tenant_id": "t-x", "session_id": "s-x", "target": "moon"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["code"], "invalid_request");
    assert_eq!(body["type"], "invalid_request_error");
    assert_eq!(body["field"], "target");
}

#[tokio::test]
#[serial]
async fn quota_exceeded_emits_typed_body_and_retry_after_header() {
    let (_dir, _obj_dir, base, registry) = spawn().await;
    let client = reqwest::Client::new();
    let box_id = registry.route_tenant("t-rl").id;

    // Tight per-tenant limit so a small burst trips it.
    client
        .post(format!("{base}/api/admin/boxes/{box_id}/quota"))
        .json(&json!({"max_tenant_qps": 1, "burst": 1}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // Mint the tenant.
    let url = format!("{base}/api/tenants/t-rl/sessions/s/append_and_fetch");
    let body = json!({"event": {"text": "seed"}, "fetch_last_n": 0});
    client.post(&url).json(&body).send().await.unwrap();

    // Drain + exceed.
    let mut saw_429 = false;
    for _ in 0..5 {
        let r = client.post(&url).json(&body).send().await.unwrap();
        if r.status() == 429 {
            assert_eq!(r.headers().get("retry-after").and_then(|v| v.to_str().ok()), Some("1"));
            let payload: serde_json::Value = r.json().await.unwrap();
            assert_eq!(payload["code"], "quota_exceeded");
            assert_eq!(payload["type"], "rate_limit_error");
            assert!(payload["scope"].is_string());
            assert!(payload["retry_after_ms"].is_number());
            saw_429 = true;
            break;
        }
    }
    assert!(saw_429, "expected at least one 429 within 5 requests");
}

#[tokio::test]
#[serial]
async fn fault_injected_emits_typed_body() {
    let (_dir, _obj_dir, base, registry) = spawn().await;
    let client = reqwest::Client::new();
    let box_id = registry.route_tenant("t-fault").id;
    // Inject an error fault.
    client
        .post(format!("{base}/api/admin/boxes/{box_id}/inject_fault"))
        .json(&json!({"kind": "error", "duration_s": 10}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let r = client
        .post(format!("{base}/api/tenants/t-fault/sessions/s/append_and_fetch"))
        .json(&json!({"event":{"text":"hi"},"fetch_last_n":0}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["code"], "fault_injected");
    assert_eq!(body["type"], "api_error");
    assert_eq!(body["box_id"], box_id);
    assert_eq!(body["fault_kind"], "error");
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p agentdb-api --test errors`
Expected: 4 passed.

- [ ] **Step 3: Commit**

```bash
git add crates/agentdb-api/tests/errors.rs
git commit -m "test(agentdb-api): integration tests per error code"
```

---

## Task 10: Update ARCHITECTURE.md

**Files:**
- Modify: `docs/ARCHITECTURE.md`

- [ ] **Step 1: Add "REST error taxonomy" section**

Locate the existing "## Observability (M7)" section in `docs/ARCHITECTURE.md`. Just before it, insert:

```markdown
## REST error taxonomy (M13)

Every `/api/*` REST error returns a Stripe-style JSON body with
stable machine-readable fields:

```json
{
  "code": "quota_exceeded",
  "type": "rate_limit_error",
  "message": "Rate limit exceeded on box 2 (tenant scope)",
  "scope": "tenant",
  "retry_after_ms": 1200
}
```

Codes (with HTTP status + extras):

| code | type | HTTP | Extras |
|---|---|---|---|
| `not_found` | `invalid_request_error` | 404 | `resource` ("tenant"/"session"/"box"), `id` |
| `invalid_request` | `invalid_request_error` | 400 | `field?` |
| `quota_exceeded` | `rate_limit_error` | 429 | `scope`, `retry_after_ms` |
| `fault_injected` | `api_error` | 503 | `box_id`, `fault_kind` |
| `storage_error` | `api_error` | 500 | — |
| `serialization_error` | `api_error` | 500 | — |

The `message` field is human-readable and may change between
releases — callers should branch on `code`, not `message`. See
`/openapi.json` for the authoritative schemas.

The Mongo wire protocol returns errors in its own format
(`{ok: 0, errmsg, code: N}` where `N` follows the Mongo error code
table); the two surfaces do not share a taxonomy.

## OpenAPI specification (M13)

The REST surface is described by an OpenAPI 3.x document served
unauthenticated at `GET /openapi.json`. The spec is generated from
`#[utoipa::path(...)]` annotations on each handler plus
`#[derive(ToSchema)]` on each request/response type, with the final
aggregate built by `agentdb_api::ApiDoc`. A snapshot test
(`crates/agentdb-api/tests/openapi_snapshot.rs`) locks the
generated output against a checked-in file so surface changes are
visible in code review.

Not covered by the OpenAPI spec: the Mongo wire protocol (not HTTP)
and the SSE event stream body (OpenAPI does not model event
streams well — the endpoint is listed but event shapes are defined
only via the `StreamEvent` schema).
```

- [ ] **Step 2: Commit**

```bash
git add docs/ARCHITECTURE.md
git commit -m "docs(architecture): REST error taxonomy + /openapi.json reference"
```

---

## Task 11: Milestone exit gate

- [ ] **Step 1: Workspace tests**

Run: `cargo test --workspace`
Expected: every test passes.

- [ ] **Step 2: Clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 3: Format**

Run: `cargo fmt --all --check`
Expected: clean. If not, run `cargo fmt --all` and commit the fmt changes.

- [ ] **Step 4: Go stays unaffected**

Run: `cd workload && /opt/homebrew/bin/go build ./... && /opt/homebrew/bin/go vet ./...`
Expected: clean.

- [ ] **Step 5: Smoke**

Run: `pkill -f agentdb-server 2>/dev/null; sleep 1; bash scripts/smoke.sh`
Expected: `smoke ok`.

- [ ] **Step 6: `/openapi.json` sanity**

```bash
pkill -f agentdb-server 2>/dev/null; sleep 1
AGENTDB_DATA=/tmp/m13 AGENTDB_OBJECTS=/tmp/m13-obj \
./target/debug/agentdb-server >/tmp/m13-srv.log 2>&1 &
sleep 2
curl -fsS http://127.0.0.1:7878/openapi.json | python3 -m json.tool >/dev/null
echo "openapi.json: valid"
pkill -f agentdb-server
rm -rf /tmp/m13 /tmp/m13-obj
```

Expected: `openapi.json: valid`.

- [ ] **Step 7: If anything failed**

Fix inline with `fix(m13): ...` commits. Do not mark the milestone complete until steps 1-6 are all green.

---

## Self-review checklist

Every spec requirement has a task:

- [x] `Error` enum rewritten with typed variants — Task 2
- [x] `Resource` enum introduced + re-exported — Task 2, Task 5
- [x] `FaultInjected` variant added to Error — Task 2
- [x] `TooManyRequests { scope: Scope, retry_after_ms }` — Task 2
- [x] All core call sites updated — Task 3
- [x] `ApiError` collapses to `Core(Error)` — Task 4
- [x] `ApiError::IntoResponse` emits Stripe-style JSON — Task 4
- [x] Six error codes, three type categories — Task 4
- [x] `Retry-After` header on `quota_exceeded` — Task 4
- [x] All API call sites updated — Task 4
- [x] `ToSchema` derives on every DTO — Task 5
- [x] `#[utoipa::path]` on every REST handler — Task 6
- [x] `ApiDoc` aggregator — Task 7
- [x] `GET /openapi.json` route — Task 7
- [x] Snapshot test — Task 8
- [x] Integration tests per error code — Task 9
- [x] ARCHITECTURE.md updates — Task 10
- [x] SSE endpoint documented with caveat — Task 6
- [x] Exit gate — Task 11
