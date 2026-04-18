# AgentDB Prototype — Milestone 2: Core types + skeletal REST + `append_and_fetch`

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first layer above the storage foundation: document / event / session / tenant types, meta-keyspace lookups, logical boxes with tenant routing, and the hot-path `append_and_fetch` primitive exposed over a small REST surface that works end-to-end from an HTTP client through to RocksDB.

**Architecture:** New `agentdb-core` crate holding data model, metadata registries, and box runtime on top of the `agentdb-storage` `KvStore` trait. New `agentdb-api` crate providing an `axum` server with a single REST endpoint. New `bin/agentdb-server` binary wiring the crates together. External callers use opaque string IDs (e.g., `"t-42"`, `"s-9"`); the server mints internal u64s lazily and persists the mapping in the Meta namespace. Writes in a box are serialized behind a per-box `tokio::sync::Mutex` to keep the seq monotonic — coarse but correct, refine later.

**Tech Stack (M2 additions):** `serde` + `serde_json` (documents as JSON for now, BSON arrives in M3), `tokio` (rt-multi-thread), `axum` 0.7, `tower` (axum prereq), `tracing` + `tracing-subscriber`, `reqwest` (test-only), `serial_test` (test-only, for tests that bind ports).

**References:**
- Spec: `docs/superpowers/specs/2026-04-17-agentdb-prototype-design.md` §4 (data model), §4.3 (external vs internal IDs), §4.4 (DocId), §7.3 (native REST), §10 (box architecture)
- Previous milestone: `docs/superpowers/plans/2026-04-17-m1-storage-foundation.md`

---

## File Structure

All paths relative to repo root `/Users/lishen/work/agentdb`.

**Created by this plan:**
- `crates/agentdb-core/Cargo.toml`
- `crates/agentdb-core/src/lib.rs` — module declarations + public re-exports
- `crates/agentdb-core/src/error.rs` — error enum (wraps storage errors)
- `crates/agentdb-core/src/types.rs` — `Document`, `Event`, `Tenant`, `Session`, `Tier`
- `crates/agentdb-core/src/meta.rs` — meta-keyspace sub-key encoding, persisted record structs
- `crates/agentdb-core/src/registry.rs` — `TenantRegistry`, `SessionRegistry`
- `crates/agentdb-core/src/box_runtime.rs` — `Box`, `BoxRegistry`, tenant routing
- `crates/agentdb-core/src/primitives.rs` — `append_event`, `fetch_last_n`, `append_and_fetch`
- `crates/agentdb-core/tests/integration.rs` — in-process core tests against a real RocksDB
- `crates/agentdb-api/Cargo.toml`
- `crates/agentdb-api/src/lib.rs` — `router()`, `AppState`, public entry points
- `crates/agentdb-api/src/handlers.rs` — REST handlers
- `crates/agentdb-api/src/dto.rs` — request/response JSON types
- `crates/agentdb-api/src/error.rs` — API error → HTTP response mapping
- `crates/agentdb-api/tests/http.rs` — HTTP end-to-end tests via reqwest + spawned server
- `bin/agentdb-server/Cargo.toml`
- `bin/agentdb-server/src/main.rs` — binary entry point

**Modified:**
- `Cargo.toml` (workspace members list: add `agentdb-core`, `agentdb-api`, `agentdb-server`)

**Responsibilities:**
- `types.rs` — pure data, serde-friendly; no behavior
- `meta.rs` — key encoding and serialized-record shapes for the Meta namespace; pure, no runtime
- `registry.rs` — persisted string↔u64 mappings; depends on meta + storage
- `box_runtime.rs` — per-box mutex, tenant→box routing; depends on registry
- `primitives.rs` — the semantic hot-path (`append_and_fetch` etc.); depends on box_runtime
- `handlers.rs` — thin HTTP adapters; no business logic
- `dto.rs` — HTTP request/response DTOs; serde-only
- `bin/agentdb-server/src/main.rs` — wiring only; no logic

---

## Concurrency contract (M2)

- **Per-box mutex** serializes all writes within a box. Reads don't take the mutex.
- **Seq** is stored in the session record and is the source of truth. Rebuilding it on restart is unnecessary because the record is persisted on every append.
- **Session and tenant creation** are read-or-mint operations performed under the box mutex on the write path; read-only resolution uses a snapshot and no mutex.
- **Cross-box operations are not required in M2.** Every request targets a single box.

---

## Task 1: Scaffold `agentdb-core` crate

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Create: `crates/agentdb-core/Cargo.toml`
- Create: `crates/agentdb-core/src/lib.rs`
- Create: `crates/agentdb-core/src/error.rs`

- [ ] **Step 1: Register new crates in workspace root**

Edit `Cargo.toml` at repo root. Replace the `members` line and add new workspace dependencies. The end state of the workspace Cargo.toml must be:
```toml
[workspace]
resolver = "2"
members = [
    "crates/agentdb-storage",
    "crates/agentdb-core",
    "crates/agentdb-api",
    "bin/agentdb-server",
]

[workspace.package]
edition = "2021"
rust-version = "1.75"
license = "MIT OR Apache-2.0"

[workspace.dependencies]
anyhow = "1"
thiserror = "1"
rocksdb = { version = "0.22", default-features = false, features = ["lz4"] }
tempfile = "3"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "sync", "signal", "net"] }
axum = "0.7"
tower = "0.4"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
serial_test = "3"
```

- [ ] **Step 2: Write the agentdb-core Cargo.toml**

Save to `crates/agentdb-core/Cargo.toml`:
```toml
[package]
name = "agentdb-core"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
agentdb-storage = { path = "../agentdb-storage" }
thiserror = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
anyhow = { workspace = true }
tempfile = { workspace = true }
tokio = { workspace = true, features = ["test-util", "macros"] }
```

- [ ] **Step 3: Stub lib.rs with module declarations**

Save to `crates/agentdb-core/src/lib.rs`:
```rust
//! AgentDB core types, metadata registries, and box runtime.

pub mod error;
pub mod types;
pub mod meta;
pub mod registry;
pub mod box_runtime;
pub mod primitives;

pub use error::{Error, Result};
// Other re-exports added as types land.
```

- [ ] **Step 4: Write the Error type**

Save to `crates/agentdb-core/src/error.rs`:
```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(#[from] agentdb_storage::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("{0}")]
    Invalid(String),
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
    fn not_found_formats_message() {
        let err = Error::NotFound("session s-9".into());
        assert_eq!(err.to_string(), "not found: session s-9");
    }
}
```

Create empty stubs for the remaining modules so the crate compiles:
- `crates/agentdb-core/src/types.rs`: `// Filled in by Task 2-3.`
- `crates/agentdb-core/src/meta.rs`: `// Filled in by Task 4.`
- `crates/agentdb-core/src/registry.rs`: `// Filled in by Tasks 5-6.`
- `crates/agentdb-core/src/box_runtime.rs`: `// Filled in by Task 7-8.`
- `crates/agentdb-core/src/primitives.rs`: `// Filled in by Tasks 9-11.`

- [ ] **Step 5: Stub the API and server crates so workspace compiles**

Save `crates/agentdb-api/Cargo.toml`:
```toml
[package]
name = "agentdb-api"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
agentdb-core = { path = "../agentdb-core" }
agentdb-storage = { path = "../agentdb-storage" }
axum = { workspace = true }
tokio = { workspace = true }
tower = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
anyhow = { workspace = true }
tempfile = { workspace = true }
reqwest = { workspace = true }
serial_test = { workspace = true }
tokio = { workspace = true, features = ["test-util", "macros"] }
```

Save `crates/agentdb-api/src/lib.rs`:
```rust
//! AgentDB HTTP API.
// Filled in by Tasks 12-13.
```

Save `bin/agentdb-server/Cargo.toml`:
```toml
[package]
name = "agentdb-server"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[[bin]]
name = "agentdb-server"
path = "src/main.rs"

[dependencies]
agentdb-api = { path = "../../crates/agentdb-api" }
agentdb-core = { path = "../../crates/agentdb-core" }
agentdb-storage = { path = "../../crates/agentdb-storage" }
anyhow = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }
```

Save `bin/agentdb-server/src/main.rs`:
```rust
// Filled in by Task 14.
fn main() {
    println!("agentdb-server stub");
}
```

- [ ] **Step 6: Verify compile**

Run: `cargo check --workspace`
Expected: clean compile with at most a warning for the unused stub main.

- [ ] **Step 7: Format and commit**

Run: `cargo fmt --all && cargo fmt --all --check`

```bash
git add Cargo.toml crates/agentdb-core crates/agentdb-api bin
git commit -m "chore: scaffold agentdb-core, agentdb-api, agentdb-server crates"
```

---

## Task 2: `Document` and `Event` types

**Files:**
- Modify: `crates/agentdb-core/src/types.rs`
- Modify: `crates/agentdb-core/src/lib.rs`

- [ ] **Step 1: Write failing tests**

Replace `crates/agentdb-core/src/types.rs` with:
```rust
use serde::{Deserialize, Serialize};

/// A schema-free document. Backed by serde_json for M2 simplicity;
/// will move to BSON when the Mongo wire protocol lands in M3.
pub type Document = serde_json::Value;

/// An append-only record inside a session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub seq: u64,
    /// Unix epoch millis at write time.
    pub ts: u64,
    pub payload: Document,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn event_roundtrips_through_json() {
        let original = Event {
            seq: 42,
            ts: 1_700_000_000_000,
            payload: json!({ "kind": "tool_call", "name": "search" }),
        };
        let text = serde_json::to_string(&original).unwrap();
        let back: Event = serde_json::from_str(&text).unwrap();
        assert_eq!(back, original);
    }

    #[test]
    fn document_accepts_arbitrary_nesting() {
        let d: Document = json!({ "a": { "b": [1, 2, { "c": true }] } });
        let s = serde_json::to_string(&d).unwrap();
        assert!(s.contains("\"c\":true"));
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p agentdb-core types::tests -- --nocapture`
Expected: 2 passed.

- [ ] **Step 3: Re-export from lib.rs**

Edit `crates/agentdb-core/src/lib.rs`, add after the existing `pub use`:
```rust
pub use types::{Document, Event};
```

Run: `cargo check --workspace`
Expected: clean.

- [ ] **Step 4: Format and commit**

Run: `cargo fmt --all && cargo fmt --all --check`

```bash
git add crates/agentdb-core/src/types.rs crates/agentdb-core/src/lib.rs
git commit -m "feat(core): Document alias and Event type"
```

---

## Task 3: Metadata types (`Tenant`, `Session`, `Tier`)

**Files:**
- Modify: `crates/agentdb-core/src/types.rs`
- Modify: `crates/agentdb-core/src/lib.rs`

- [ ] **Step 1: Write failing tests**

Append to `crates/agentdb-core/src/types.rs` (inside the existing `mod tests`, add a test after the existing two):
```rust
    #[test]
    fn session_roundtrips_through_json() {
        let s = Session {
            internal_id: 17,
            external_id: "s-9".into(),
            tenant_internal_id: 3,
            box_id: 1,
            created_at: 100,
            last_active_at: 150,
            tier: Tier::Warm,
            next_seq: 5,
        };
        let text = serde_json::to_string(&s).unwrap();
        let back: Session = serde_json::from_str(&text).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn tier_serializes_as_lowercase_string() {
        let t = Tier::Archive;
        let s = serde_json::to_string(&t).unwrap();
        assert_eq!(s, "\"archive\"");
    }

    #[test]
    fn tenant_roundtrips_through_json() {
        let t = Tenant {
            internal_id: 100,
            external_id: "t-42".into(),
            box_id: 2,
            created_at: 1,
        };
        let text = serde_json::to_string(&t).unwrap();
        let back: Tenant = serde_json::from_str(&text).unwrap();
        assert_eq!(back, t);
    }
```

- [ ] **Step 2: Run (expect failure)**

Run: `cargo test -p agentdb-core types::tests -- --nocapture`
Expected: FAIL — `Tier`, `Session`, `Tenant` not defined.

- [ ] **Step 3: Implement the types**

Append to `crates/agentdb-core/src/types.rs`, before the `#[cfg(test)] mod tests` block:
```rust
use agentdb_storage::{BoxId, SessionId, TenantId};

/// Lifecycle tier of a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Warm,
    Cold,
    Archive,
}

/// Persisted tenant record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tenant {
    pub internal_id: TenantId,
    pub external_id: String,
    pub box_id: BoxId,
    pub created_at: u64,
}

/// Persisted session record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Session {
    pub internal_id: SessionId,
    pub external_id: String,
    pub tenant_internal_id: TenantId,
    pub box_id: BoxId,
    pub created_at: u64,
    pub last_active_at: u64,
    pub tier: Tier,
    pub next_seq: u64,
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p agentdb-core types::tests -- --nocapture`
Expected: 5 passed (2 from T2 + 3 new).

- [ ] **Step 5: Re-export from lib.rs**

Edit `crates/agentdb-core/src/lib.rs`, expand the types re-export line to:
```rust
pub use types::{Document, Event, Session, Tenant, Tier};
```

Run: `cargo check --workspace`
Expected: clean.

- [ ] **Step 6: Format and commit**

Run: `cargo fmt --all && cargo fmt --all --check`

```bash
git add crates/agentdb-core/src/types.rs crates/agentdb-core/src/lib.rs
git commit -m "feat(core): Tenant, Session, and Tier metadata types"
```

---

## Task 4: Meta keyspace helpers

**Files:**
- Modify: `crates/agentdb-core/src/meta.rs`
- Modify: `crates/agentdb-core/src/lib.rs`

**Context:** The storage layer encodes keys as `ns|box|tenant|session|branch|seq`. For meta entries we use the `Namespace::Meta` byte and then encode a sub-kind into the following bytes. We only use a handful of meta entry kinds in M2:

- `TenantByExternal(box_id, external_name)` → record {internal_id, created_at}
- `SessionByExternal(box_id, tenant_id, external_name)` → record {internal_id, created_at}
- `SessionRecord(box_id, tenant_id, session_id)` → full `Session` JSON
- `TenantRecord(box_id, tenant_id)` → full `Tenant` JSON
- `NextInternalSessionId(box_id, tenant_id)` → `u64` — counter for minting session ids
- `NextInternalTenantId(box_id)` → `u64` — counter for minting tenant ids

All of these ride in the `Meta` namespace. Because `encode_key` is fixed-width at 31 bytes and doesn't fit string external IDs, we use an out-of-band key encoding for meta that stays in the Meta namespace via the `ns` byte but otherwise composes sub-kinds + variable-length fields itself. We reserve the entire `Meta` namespace for this sub-encoding.

Meta key shape:
```
key = 0x04 (Meta ns) | sub_tag(1B) | <sub-kind-specific fields>

sub_tag:
  0x01 = TenantByExternal(box_id, name_bytes)
  0x02 = SessionByExternal(box_id, tenant_id, name_bytes)
  0x03 = TenantRecord(box_id, tenant_id)
  0x04 = SessionRecord(box_id, tenant_id, session_id)
  0x05 = NextInternalTenantId(box_id)
  0x06 = NextInternalSessionId(box_id, tenant_id)
```

Fields are big-endian fixed-width (matching storage conventions). Variable-length name bytes go at the end of the key.

- [ ] **Step 1: Write failing tests**

Replace `crates/agentdb-core/src/meta.rs` with:
```rust
use agentdb_storage::{BoxId, Namespace, SessionId, TenantId};

const NS_META: u8 = Namespace::Meta as u8;

const TAG_TENANT_BY_EXT: u8 = 0x01;
const TAG_SESSION_BY_EXT: u8 = 0x02;
const TAG_TENANT_RECORD: u8 = 0x03;
const TAG_SESSION_RECORD: u8 = 0x04;
const TAG_NEXT_TENANT_ID: u8 = 0x05;
const TAG_NEXT_SESSION_ID: u8 = 0x06;

pub fn key_tenant_by_external(box_id: BoxId, external: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + external.len());
    out.push(NS_META);
    out.push(TAG_TENANT_BY_EXT);
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(external.as_bytes());
    out
}

pub fn key_session_by_external(
    box_id: BoxId,
    tenant_id: TenantId,
    external: &str,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + external.len());
    out.push(NS_META);
    out.push(TAG_SESSION_BY_EXT);
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out.extend_from_slice(external.as_bytes());
    out
}

pub fn key_tenant_record(box_id: BoxId, tenant_id: TenantId) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    out.push(NS_META);
    out.push(TAG_TENANT_RECORD);
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out
}

pub fn key_session_record(
    box_id: BoxId,
    tenant_id: TenantId,
    session_id: SessionId,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(20);
    out.push(NS_META);
    out.push(TAG_SESSION_RECORD);
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out.extend_from_slice(&session_id.to_be_bytes());
    out
}

pub fn key_next_tenant_id(box_id: BoxId) -> Vec<u8> {
    let mut out = Vec::with_capacity(4);
    out.push(NS_META);
    out.push(TAG_NEXT_TENANT_ID);
    out.extend_from_slice(&box_id.to_be_bytes());
    out
}

pub fn key_next_session_id(box_id: BoxId, tenant_id: TenantId) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    out.push(NS_META);
    out.push(TAG_NEXT_SESSION_ID);
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_meta_keys_start_with_meta_namespace_byte() {
        assert_eq!(key_tenant_by_external(1, "t").first(), Some(&NS_META));
        assert_eq!(key_session_by_external(1, 2, "s").first(), Some(&NS_META));
        assert_eq!(key_tenant_record(1, 2).first(), Some(&NS_META));
        assert_eq!(key_session_record(1, 2, 3).first(), Some(&NS_META));
        assert_eq!(key_next_tenant_id(1).first(), Some(&NS_META));
        assert_eq!(key_next_session_id(1, 2).first(), Some(&NS_META));
    }

    #[test]
    fn tenant_by_external_encodes_box_and_name() {
        let k = key_tenant_by_external(0x0123, "t-42");
        assert_eq!(k[0], NS_META);
        assert_eq!(k[1], TAG_TENANT_BY_EXT);
        assert_eq!(&k[2..4], &[0x01, 0x23]);
        assert_eq!(&k[4..], b"t-42");
    }

    #[test]
    fn session_by_external_encodes_box_tenant_name() {
        let k = key_session_by_external(7, 0x0A0B0C0D0E0F1011, "s-1");
        assert_eq!(&k[0..2], &[NS_META, TAG_SESSION_BY_EXT]);
        assert_eq!(&k[2..4], &7u16.to_be_bytes());
        assert_eq!(&k[4..12], &0x0A0B0C0D0E0F1011u64.to_be_bytes());
        assert_eq!(&k[12..], b"s-1");
    }

    #[test]
    fn different_kinds_never_collide() {
        let a = key_tenant_record(1, 2);
        let b = key_next_session_id(1, 2);
        assert_ne!(a, b);
        assert_ne!(a[1], b[1]);
    }

    #[test]
    fn different_external_names_produce_different_keys() {
        let a = key_tenant_by_external(1, "foo");
        let b = key_tenant_by_external(1, "bar");
        assert_ne!(a, b);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p agentdb-core meta::tests -- --nocapture`
Expected: 5 passed.

- [ ] **Step 3: Re-export from lib.rs**

Edit `crates/agentdb-core/src/lib.rs`, add after the types re-export:
```rust
pub use meta::{
    key_next_session_id, key_next_tenant_id, key_session_by_external, key_session_record,
    key_tenant_by_external, key_tenant_record,
};
```

Run: `cargo check --workspace`.

- [ ] **Step 4: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core/src/meta.rs crates/agentdb-core/src/lib.rs
git commit -m "feat(core): meta keyspace sub-key encoders"
```

---

## Task 5: `TenantRegistry` — read-or-mint tenant IDs

**Files:**
- Modify: `crates/agentdb-core/src/registry.rs`
- Modify: `crates/agentdb-core/src/lib.rs`

**Context:** The registry turns an opaque string tenant ID like `"t-42"` into an internal `(box_id, internal_tenant_id)` pair, minting the internal id on first reference. The box_id is provided by the caller (tenant→box routing is done in T7). All operations use the storage's `KvStore` trait directly.

Mint flow (inside caller-supplied box mutex later, but the registry itself doesn't lock):
1. Read `key_tenant_by_external` → if present, deserialize `{internal_id, ...}`, done.
2. Otherwise: read `key_next_tenant_id(box_id)` (default 1 if missing), allocate this id.
3. Write the lookup record (`key_tenant_by_external`), the full tenant record (`key_tenant_record`), and the new counter (`key_next_tenant_id` = allocated+1) as a `write_batch`.
4. Return the `Tenant`.

- [ ] **Step 1: Write failing tests**

Replace `crates/agentdb-core/src/registry.rs` with:
```rust
use std::sync::Arc;

use agentdb_storage::{BoxId, KvStore, TenantId, WriteOp};

use crate::error::Result;
use crate::meta::{key_next_tenant_id, key_tenant_by_external, key_tenant_record};
use crate::types::Tenant;

pub struct TenantRegistry {
    kv: Arc<dyn KvStore>,
}

impl TenantRegistry {
    pub fn new(kv: Arc<dyn KvStore>) -> Self {
        Self { kv }
    }

    /// Look up an existing tenant by its external string id.
    pub fn lookup(&self, box_id: BoxId, external: &str) -> Result<Option<Tenant>> {
        let Some(bytes) = self
            .kv
            .get(&key_tenant_by_external(box_id, external))?
        else {
            return Ok(None);
        };
        let internal_id: TenantId = serde_json::from_slice(&bytes)?;
        let record_bytes = self
            .kv
            .get(&key_tenant_record(box_id, internal_id))?
            .ok_or_else(|| {
                crate::error::Error::Invalid(format!(
                    "tenant record missing for box={box_id} tenant={internal_id}"
                ))
            })?;
        let t: Tenant = serde_json::from_slice(&record_bytes)?;
        Ok(Some(t))
    }

    /// Get an existing tenant or mint a new internal id. Persists both the
    /// lookup entry and the full tenant record atomically.
    pub fn get_or_mint(
        &self,
        box_id: BoxId,
        external: &str,
        now_millis: u64,
    ) -> Result<Tenant> {
        if let Some(t) = self.lookup(box_id, external)? {
            return Ok(t);
        }
        let next_key = key_next_tenant_id(box_id);
        let current: TenantId = match self.kv.get(&next_key)? {
            Some(b) => serde_json::from_slice(&b)?,
            None => 1,
        };
        let tenant = Tenant {
            internal_id: current,
            external_id: external.to_string(),
            box_id,
            created_at: now_millis,
        };
        let tenant_bytes = serde_json::to_vec(&tenant)?;
        let lookup_bytes = serde_json::to_vec(&tenant.internal_id)?;
        let next_bytes = serde_json::to_vec(&(current + 1))?;
        self.kv.write_batch(vec![
            WriteOp::Put {
                key: key_tenant_by_external(box_id, external),
                value: lookup_bytes,
            },
            WriteOp::Put {
                key: key_tenant_record(box_id, current),
                value: tenant_bytes,
            },
            WriteOp::Put {
                key: next_key,
                value: next_bytes,
            },
        ])?;
        Ok(tenant)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_storage::RocksKvStore;
    use tempfile::TempDir;

    fn store() -> (TempDir, Arc<dyn KvStore>) {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        (dir, kv)
    }

    #[test]
    fn lookup_unknown_tenant_returns_none() {
        let (_dir, kv) = store();
        let reg = TenantRegistry::new(kv);
        assert!(reg.lookup(1, "nobody").unwrap().is_none());
    }

    #[test]
    fn first_get_mints_id_one_then_two() {
        let (_dir, kv) = store();
        let reg = TenantRegistry::new(kv);
        let a = reg.get_or_mint(1, "t-a", 100).unwrap();
        let b = reg.get_or_mint(1, "t-b", 101).unwrap();
        assert_eq!(a.internal_id, 1);
        assert_eq!(b.internal_id, 2);
        assert_eq!(a.box_id, 1);
        assert_eq!(a.external_id, "t-a");
        assert_eq!(a.created_at, 100);
    }

    #[test]
    fn get_or_mint_is_idempotent_for_same_external_id() {
        let (_dir, kv) = store();
        let reg = TenantRegistry::new(kv);
        let a1 = reg.get_or_mint(1, "t-a", 100).unwrap();
        let a2 = reg.get_or_mint(1, "t-a", 200).unwrap();
        assert_eq!(a1, a2); // created_at preserved, no re-mint
    }

    #[test]
    fn different_boxes_mint_independent_id_spaces() {
        let (_dir, kv) = store();
        let reg = TenantRegistry::new(kv);
        let a = reg.get_or_mint(1, "same", 0).unwrap();
        let b = reg.get_or_mint(2, "same", 0).unwrap();
        assert_eq!(a.internal_id, 1);
        assert_eq!(b.internal_id, 1); // different box -> fresh counter
        assert_ne!(a.box_id, b.box_id);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p agentdb-core registry::tests -- --nocapture`
Expected: 4 passed.

- [ ] **Step 3: Re-export from lib.rs**

Edit `crates/agentdb-core/src/lib.rs`, add:
```rust
pub use registry::TenantRegistry;
```

Run: `cargo check --workspace`.

- [ ] **Step 4: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core/src/registry.rs crates/agentdb-core/src/lib.rs
git commit -m "feat(core): TenantRegistry with read-or-mint semantics"
```

---

## Task 6: `SessionRegistry` — read-or-mint sessions

**Files:**
- Modify: `crates/agentdb-core/src/registry.rs`
- Modify: `crates/agentdb-core/src/lib.rs`

- [ ] **Step 1: Write failing tests**

Append inside `#[cfg(test)] mod tests` in `crates/agentdb-core/src/registry.rs`:
```rust
    #[test]
    fn session_lookup_unknown_returns_none() {
        let (_dir, kv) = store();
        let reg = SessionRegistry::new(kv);
        assert!(reg.lookup(1, 1, "none").unwrap().is_none());
    }

    #[test]
    fn session_get_or_mint_creates_default_session() {
        let (_dir, kv) = store();
        let reg = SessionRegistry::new(kv);
        let s = reg.get_or_mint(1, 10, "s-1", 500).unwrap();
        assert_eq!(s.external_id, "s-1");
        assert_eq!(s.tenant_internal_id, 10);
        assert_eq!(s.box_id, 1);
        assert_eq!(s.internal_id, 1);
        assert_eq!(s.created_at, 500);
        assert_eq!(s.last_active_at, 500);
        assert_eq!(s.tier, crate::types::Tier::Warm);
        assert_eq!(s.next_seq, 1);
    }

    #[test]
    fn session_mint_is_idempotent() {
        let (_dir, kv) = store();
        let reg = SessionRegistry::new(kv);
        let a = reg.get_or_mint(1, 10, "s-1", 500).unwrap();
        let b = reg.get_or_mint(1, 10, "s-1", 900).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn session_ids_increment_within_same_tenant() {
        let (_dir, kv) = store();
        let reg = SessionRegistry::new(kv);
        let s1 = reg.get_or_mint(1, 10, "s-a", 0).unwrap();
        let s2 = reg.get_or_mint(1, 10, "s-b", 0).unwrap();
        assert_eq!(s1.internal_id, 1);
        assert_eq!(s2.internal_id, 2);
    }

    #[test]
    fn save_persists_updates_to_session_record() {
        let (_dir, kv) = store();
        let reg = SessionRegistry::new(kv);
        let mut s = reg.get_or_mint(1, 10, "s-1", 500).unwrap();
        s.next_seq = 99;
        s.last_active_at = 999;
        reg.save(&s).unwrap();
        let reloaded = reg.lookup(1, 10, "s-1").unwrap().unwrap();
        assert_eq!(reloaded.next_seq, 99);
        assert_eq!(reloaded.last_active_at, 999);
    }
```

- [ ] **Step 2: Run (expect failure)**

Run: `cargo test -p agentdb-core registry::tests -- --nocapture`
Expected: FAIL — `SessionRegistry` not defined.

- [ ] **Step 3: Implement `SessionRegistry`**

Append to `crates/agentdb-core/src/registry.rs` (after the closing `}` of `impl TenantRegistry`, before `#[cfg(test)]`):
```rust
use crate::meta::{key_next_session_id, key_session_by_external, key_session_record};
use crate::types::{Session, Tier};
use agentdb_storage::SessionId;

pub struct SessionRegistry {
    kv: Arc<dyn KvStore>,
}

impl SessionRegistry {
    pub fn new(kv: Arc<dyn KvStore>) -> Self {
        Self { kv }
    }

    pub fn lookup(
        &self,
        box_id: BoxId,
        tenant_id: TenantId,
        external: &str,
    ) -> Result<Option<Session>> {
        let Some(bytes) = self
            .kv
            .get(&key_session_by_external(box_id, tenant_id, external))?
        else {
            return Ok(None);
        };
        let internal_id: SessionId = serde_json::from_slice(&bytes)?;
        let record_bytes = self
            .kv
            .get(&key_session_record(box_id, tenant_id, internal_id))?
            .ok_or_else(|| {
                crate::error::Error::Invalid(format!(
                    "session record missing for box={box_id} tenant={tenant_id} session={internal_id}"
                ))
            })?;
        let s: Session = serde_json::from_slice(&record_bytes)?;
        Ok(Some(s))
    }

    pub fn get_or_mint(
        &self,
        box_id: BoxId,
        tenant_id: TenantId,
        external: &str,
        now_millis: u64,
    ) -> Result<Session> {
        if let Some(s) = self.lookup(box_id, tenant_id, external)? {
            return Ok(s);
        }
        let next_key = key_next_session_id(box_id, tenant_id);
        let current: SessionId = match self.kv.get(&next_key)? {
            Some(b) => serde_json::from_slice(&b)?,
            None => 1,
        };
        let session = Session {
            internal_id: current,
            external_id: external.to_string(),
            tenant_internal_id: tenant_id,
            box_id,
            created_at: now_millis,
            last_active_at: now_millis,
            tier: Tier::Warm,
            next_seq: 1,
        };
        let session_bytes = serde_json::to_vec(&session)?;
        let lookup_bytes = serde_json::to_vec(&session.internal_id)?;
        let next_bytes = serde_json::to_vec(&(current + 1))?;
        self.kv.write_batch(vec![
            WriteOp::Put {
                key: key_session_by_external(box_id, tenant_id, external),
                value: lookup_bytes,
            },
            WriteOp::Put {
                key: key_session_record(box_id, tenant_id, current),
                value: session_bytes,
            },
            WriteOp::Put {
                key: next_key,
                value: next_bytes,
            },
        ])?;
        Ok(session)
    }

    /// Overwrite the persisted session record. Does NOT rewrite the lookup
    /// entry — that mapping is immutable once created.
    pub fn save(&self, session: &Session) -> Result<()> {
        let bytes = serde_json::to_vec(session)?;
        self.kv.put(
            &key_session_record(session.box_id, session.tenant_internal_id, session.internal_id),
            &bytes,
        )?;
        Ok(())
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p agentdb-core registry::tests -- --nocapture`
Expected: 9 passed (4 from T5 + 5 new).

- [ ] **Step 5: Re-export from lib.rs**

Edit `crates/agentdb-core/src/lib.rs`, update the registry re-export to include `SessionRegistry`:
```rust
pub use registry::{SessionRegistry, TenantRegistry};
```

Run: `cargo check --workspace`.

- [ ] **Step 6: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core/src/registry.rs crates/agentdb-core/src/lib.rs
git commit -m "feat(core): SessionRegistry with read-or-mint and save"
```

---

## Task 7: `Box` runtime and `BoxRegistry`

**Files:**
- Modify: `crates/agentdb-core/src/box_runtime.rs`
- Modify: `crates/agentdb-core/src/lib.rs`

**Context:** Tenant→box routing for M2 is deterministic hash-mod. The `BoxRegistry` owns a fixed `Vec<Arc<Box>>` sized at startup (configurable; default 4 in M2). Each `Box` holds a `BoxId` and a `tokio::sync::Mutex<()>` used as the write lock for the hot path. The KV store is shared — boxes just carve up the keyspace by their `box_id` prefix.

- [ ] **Step 1: Write failing tests**

Replace `crates/agentdb-core/src/box_runtime.rs` with:
```rust
use std::sync::Arc;

use agentdb_storage::{BoxId, KvStore};
use tokio::sync::Mutex;

use crate::error::{Error, Result};

/// One logical box. Owns a mutex that serializes the hot-path writes
/// (append, metadata updates) for tenants it hosts.
pub struct Box_ {
    pub id: BoxId,
    pub write_lock: Mutex<()>,
}

impl Box_ {
    fn new(id: BoxId) -> Self {
        Self {
            id,
            write_lock: Mutex::new(()),
        }
    }
}

/// Owns the pool of boxes and the shared KV store, plus the tenant→box
/// routing function.
pub struct BoxRegistry {
    boxes: Vec<Arc<Box_>>,
    kv: Arc<dyn KvStore>,
}

impl BoxRegistry {
    pub fn new(kv: Arc<dyn KvStore>, num_boxes: u16) -> Result<Self> {
        if num_boxes == 0 {
            return Err(Error::Invalid("num_boxes must be > 0".into()));
        }
        let boxes = (0..num_boxes).map(|i| Arc::new(Box_::new(i))).collect();
        Ok(Self { boxes, kv })
    }

    pub fn kv(&self) -> &Arc<dyn KvStore> {
        &self.kv
    }

    pub fn num_boxes(&self) -> u16 {
        self.boxes.len() as u16
    }

    /// Route a tenant external id to a box using FNV-1a hash mod num_boxes.
    pub fn route_tenant(&self, external: &str) -> Arc<Box_> {
        let mut hash: u64 = 0xcbf29ce484222325;
        for b in external.as_bytes() {
            hash ^= u64::from(*b);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        let idx = (hash as usize) % self.boxes.len();
        self.boxes[idx].clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_storage::RocksKvStore;
    use std::collections::HashSet;
    use tempfile::TempDir;

    fn registry(num: u16) -> (TempDir, BoxRegistry) {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        (dir, BoxRegistry::new(kv, num).unwrap())
    }

    #[test]
    fn zero_boxes_is_rejected() {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        assert!(BoxRegistry::new(kv, 0).is_err());
    }

    #[test]
    fn same_tenant_always_routes_to_same_box() {
        let (_dir, reg) = registry(8);
        let a = reg.route_tenant("t-42").id;
        let b = reg.route_tenant("t-42").id;
        assert_eq!(a, b);
    }

    #[test]
    fn different_tenants_eventually_hit_multiple_boxes() {
        let (_dir, reg) = registry(4);
        let mut hit = HashSet::new();
        for i in 0..200 {
            hit.insert(reg.route_tenant(&format!("t-{i}")).id);
        }
        assert!(hit.len() > 1, "expected distribution across boxes, got {hit:?}");
    }

    #[test]
    fn num_boxes_reports_correct_count() {
        let (_dir, reg) = registry(5);
        assert_eq!(reg.num_boxes(), 5);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p agentdb-core box_runtime::tests -- --nocapture`
Expected: 4 passed.

- [ ] **Step 3: Re-export from lib.rs**

Edit `crates/agentdb-core/src/lib.rs`, add:
```rust
pub use box_runtime::{Box_, BoxRegistry};
```

Run: `cargo check --workspace`.

- [ ] **Step 4: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core/src/box_runtime.rs crates/agentdb-core/src/lib.rs
git commit -m "feat(core): Box runtime with tenant hash routing"
```

---

## Task 8: `append_event` primitive

**Files:**
- Modify: `crates/agentdb-core/src/primitives.rs`
- Modify: `crates/agentdb-core/src/lib.rs`

**Context:** `append_event` writes a new event to the session's event stream at its current `next_seq`, then increments `next_seq` and updates `last_active_at` on the session record — all in one `write_batch`. Must be called under the box's write mutex; the function takes `&tokio::sync::MutexGuard<()>` to make this explicit and unforgeable.

- [ ] **Step 1: Write failing tests**

Replace `crates/agentdb-core/src/primitives.rs` with:
```rust
use std::sync::Arc;

use agentdb_storage::{encode_key, KvStore, Namespace, WriteOp};
use tokio::sync::MutexGuard;

use crate::error::Result;
use crate::meta::key_session_record;
use crate::registry::SessionRegistry;
use crate::types::{Event, Session};

/// Default branch id for sessions that have never been forked. Fork arrives in M6.
pub const DEFAULT_BRANCH_ID: u32 = 0;

/// Append one event to a session, atomically bumping its `next_seq` and
/// `last_active_at`. Must be called while holding the box's write lock
/// (demonstrated by the MutexGuard reference).
pub fn append_event(
    kv: &Arc<dyn KvStore>,
    _lock: &MutexGuard<'_, ()>,
    session: &mut Session,
    payload: crate::types::Document,
    now_millis: u64,
) -> Result<Event> {
    let seq = session.next_seq;
    let event = Event {
        seq,
        ts: now_millis,
        payload,
    };
    let event_key = encode_key(
        Namespace::Events,
        session.box_id,
        session.tenant_internal_id,
        session.internal_id,
        DEFAULT_BRANCH_ID,
        seq,
    );
    let event_bytes = serde_json::to_vec(&event)?;
    session.next_seq = seq + 1;
    session.last_active_at = now_millis;
    let session_bytes = serde_json::to_vec(session)?;
    let session_key = key_session_record(
        session.box_id,
        session.tenant_internal_id,
        session.internal_id,
    );
    kv.write_batch(vec![
        WriteOp::Put {
            key: event_key,
            value: event_bytes,
        },
        WriteOp::Put {
            key: session_key,
            value: session_bytes,
        },
    ])?;
    Ok(event)
}

/// Convenience: open the session (creating if absent), hold the box's write
/// lock, append. Only used from tests in T8; production call sites sequence
/// these themselves via T11's `append_and_fetch`.
pub async fn open_and_append(
    kv: Arc<dyn KvStore>,
    sessions: &SessionRegistry,
    box_lock: &tokio::sync::Mutex<()>,
    box_id: agentdb_storage::BoxId,
    tenant_id: agentdb_storage::TenantId,
    session_external: &str,
    payload: crate::types::Document,
    now_millis: u64,
) -> Result<(Event, Session)> {
    let guard = box_lock.lock().await;
    let mut session = sessions.get_or_mint(box_id, tenant_id, session_external, now_millis)?;
    let event = append_event(&kv, &guard, &mut session, payload, now_millis)?;
    Ok((event, session))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{SessionRegistry, TenantRegistry};
    use agentdb_storage::RocksKvStore;
    use serde_json::json;
    use tempfile::TempDir;
    use tokio::sync::Mutex;

    fn setup() -> (TempDir, Arc<dyn KvStore>, TenantRegistry, SessionRegistry) {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let tenants = TenantRegistry::new(kv.clone());
        let sessions = SessionRegistry::new(kv.clone());
        (dir, kv, tenants, sessions)
    }

    #[tokio::test]
    async fn append_increments_seq_and_persists_event() {
        let (_dir, kv, tenants, sessions) = setup();
        let lock = Mutex::new(());
        let t = tenants.get_or_mint(0, "t", 10).unwrap();

        let (e1, s1) = open_and_append(
            kv.clone(),
            &sessions,
            &lock,
            0,
            t.internal_id,
            "s-1",
            json!({"k": 1}),
            100,
        )
        .await
        .unwrap();
        assert_eq!(e1.seq, 1);
        assert_eq!(s1.next_seq, 2);
        assert_eq!(s1.last_active_at, 100);

        let (e2, s2) = open_and_append(
            kv.clone(),
            &sessions,
            &lock,
            0,
            t.internal_id,
            "s-1",
            json!({"k": 2}),
            200,
        )
        .await
        .unwrap();
        assert_eq!(e2.seq, 2);
        assert_eq!(s2.next_seq, 3);
        assert_eq!(s2.last_active_at, 200);
    }

    #[tokio::test]
    async fn event_is_readable_at_expected_key() {
        let (_dir, kv, tenants, sessions) = setup();
        let lock = Mutex::new(());
        let t = tenants.get_or_mint(0, "t", 1).unwrap();
        let (_e, s) = open_and_append(
            kv.clone(),
            &sessions,
            &lock,
            0,
            t.internal_id,
            "s-1",
            json!({"hello": "world"}),
            50,
        )
        .await
        .unwrap();

        let k = encode_key(
            Namespace::Events,
            s.box_id,
            s.tenant_internal_id,
            s.internal_id,
            DEFAULT_BRANCH_ID,
            1,
        );
        let bytes = kv.get(&k).unwrap().unwrap();
        let ev: Event = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(ev.seq, 1);
        assert_eq!(ev.payload, json!({"hello": "world"}));
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p agentdb-core primitives::tests -- --nocapture`
Expected: 2 passed.

- [ ] **Step 3: Re-export from lib.rs**

Edit `crates/agentdb-core/src/lib.rs`, add:
```rust
pub use primitives::{append_event, DEFAULT_BRANCH_ID};
```

Run: `cargo check --workspace`.

- [ ] **Step 4: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core/src/primitives.rs crates/agentdb-core/src/lib.rs
git commit -m "feat(core): append_event primitive with per-session seq bump"
```

---

## Task 9: `fetch_last_n`

**Files:**
- Modify: `crates/agentdb-core/src/primitives.rs`
- Modify: `crates/agentdb-core/src/lib.rs`

**Context:** Fetches the last N events of a session's default branch. Uses a prefix scan over the session-branch prefix, collects everything, returns the last N. At prototype scale session sizes are small; we can optimize with reverse iteration later.

- [ ] **Step 1: Write failing tests**

Append inside the existing `#[cfg(test)] mod tests` in `crates/agentdb-core/src/primitives.rs`:
```rust
    #[tokio::test]
    async fn fetch_last_n_returns_tail_in_order() {
        let (_dir, kv, tenants, sessions) = setup();
        let lock = Mutex::new(());
        let t = tenants.get_or_mint(0, "t", 0).unwrap();
        for i in 1..=5u64 {
            open_and_append(
                kv.clone(),
                &sessions,
                &lock,
                0,
                t.internal_id,
                "s-1",
                json!({"i": i}),
                i,
            )
            .await
            .unwrap();
        }
        let session = sessions.lookup(0, t.internal_id, "s-1").unwrap().unwrap();

        let last_3 = fetch_last_n(&kv, &session, 3).unwrap();
        assert_eq!(last_3.len(), 3);
        assert_eq!(last_3[0].seq, 3);
        assert_eq!(last_3[1].seq, 4);
        assert_eq!(last_3[2].seq, 5);
    }

    #[tokio::test]
    async fn fetch_last_n_larger_than_size_returns_all_in_order() {
        let (_dir, kv, tenants, sessions) = setup();
        let lock = Mutex::new(());
        let t = tenants.get_or_mint(0, "t", 0).unwrap();
        for i in 1..=2u64 {
            open_and_append(
                kv.clone(),
                &sessions,
                &lock,
                0,
                t.internal_id,
                "s-1",
                json!({"i": i}),
                i,
            )
            .await
            .unwrap();
        }
        let session = sessions.lookup(0, t.internal_id, "s-1").unwrap().unwrap();
        let all = fetch_last_n(&kv, &session, 100).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].seq, 1);
        assert_eq!(all[1].seq, 2);
    }

    #[test]
    fn fetch_last_n_zero_returns_empty() {
        let (_dir, kv, _tenants, _sessions) = setup();
        let session = Session {
            internal_id: 1,
            external_id: "s".into(),
            tenant_internal_id: 1,
            box_id: 0,
            created_at: 0,
            last_active_at: 0,
            tier: crate::types::Tier::Warm,
            next_seq: 1,
        };
        let out = fetch_last_n(&kv, &session, 0).unwrap();
        assert!(out.is_empty());
    }
```

- [ ] **Step 2: Run (expect failure)**

Run: `cargo test -p agentdb-core primitives::tests -- --nocapture`
Expected: FAIL — `fetch_last_n` not defined.

- [ ] **Step 3: Implement `fetch_last_n`**

In `crates/agentdb-core/src/primitives.rs`, add the following function after `open_and_append`:
```rust
/// Fetch the last `n` events of `session`'s default branch in seq order
/// (oldest-to-newest within the returned slice).
pub fn fetch_last_n(kv: &Arc<dyn KvStore>, session: &Session, n: usize) -> Result<Vec<Event>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let prefix = agentdb_storage::prefix_for_session_branch(
        Namespace::Events,
        session.box_id,
        session.tenant_internal_id,
        session.internal_id,
        DEFAULT_BRANCH_ID,
    );
    let mut all: Vec<Event> = Vec::new();
    for res in kv.scan_prefix(&prefix)? {
        let (_k, v) = res?;
        let ev: Event = serde_json::from_slice(&v)?;
        all.push(ev);
    }
    let start = all.len().saturating_sub(n);
    Ok(all.split_off(start))
}
```

Note `prefix_for_session_branch` needs to be in scope — add `prefix_for_session_branch,` to the top-level `use agentdb_storage::{...}` if not already there.

- [ ] **Step 4: Run tests**

Run: `cargo test -p agentdb-core primitives::tests -- --nocapture`
Expected: 5 passed (2 from T8 + 3 new).

- [ ] **Step 5: Re-export from lib.rs**

Edit `crates/agentdb-core/src/lib.rs`, expand primitives re-export:
```rust
pub use primitives::{append_event, fetch_last_n, DEFAULT_BRANCH_ID};
```

Run: `cargo check --workspace`.

- [ ] **Step 6: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core/src/primitives.rs crates/agentdb-core/src/lib.rs
git commit -m "feat(core): fetch_last_n via session-branch prefix scan"
```

---

## Task 10: `append_and_fetch` composed primitive

**Files:**
- Modify: `crates/agentdb-core/src/primitives.rs`
- Modify: `crates/agentdb-core/src/lib.rs`

**Context:** `append_and_fetch` is the headline hot-path primitive. It accepts external tenant and session ids, the payload, and `fetch_last_n` count, then: looks up box, mints/reads tenant + session, appends under the box lock, and returns the new seq plus the last-N events. Returns `Arc<Box_>` only as an artifact of routing — callers don't need it.

- [ ] **Step 1: Write failing tests**

Append to `crates/agentdb-core/src/primitives.rs` inside `mod tests`:
```rust
    use crate::box_runtime::BoxRegistry;

    fn context() -> (
        TempDir,
        BoxRegistry,
        TenantRegistry,
        SessionRegistry,
    ) {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let registry = BoxRegistry::new(kv.clone(), 4).unwrap();
        let tenants = TenantRegistry::new(kv.clone());
        let sessions = SessionRegistry::new(kv);
        (dir, registry, tenants, sessions)
    }

    #[tokio::test]
    async fn append_and_fetch_returns_new_seq_and_recent_events() {
        let (_dir, registry, tenants, sessions) = context();
        let out = append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"a": 1}),
            5,
            10,
        )
        .await
        .unwrap();
        assert_eq!(out.new_seq, 1);
        assert_eq!(out.recent.len(), 1);
        assert_eq!(out.recent[0].payload, json!({"a": 1}));

        let out2 = append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"a": 2}),
            5,
            20,
        )
        .await
        .unwrap();
        assert_eq!(out2.new_seq, 2);
        assert_eq!(out2.recent.len(), 2);
        assert_eq!(out2.recent[1].payload, json!({"a": 2}));
    }

    #[tokio::test]
    async fn append_and_fetch_limits_recent_to_n() {
        let (_dir, registry, tenants, sessions) = context();
        for i in 1..=5u64 {
            append_and_fetch(
                &registry,
                &tenants,
                &sessions,
                "t-1",
                "s-1",
                json!({"i": i}),
                2,
                i,
            )
            .await
            .unwrap();
        }
        let out = append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"final": true}),
            2,
            100,
        )
        .await
        .unwrap();
        assert_eq!(out.new_seq, 6);
        assert_eq!(out.recent.len(), 2);
        assert_eq!(out.recent[0].seq, 5);
        assert_eq!(out.recent[1].seq, 6);
    }

    #[tokio::test]
    async fn different_tenants_do_not_interfere() {
        let (_dir, registry, tenants, sessions) = context();
        let a = append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-A",
            "s-1",
            json!({"owner": "A"}),
            5,
            10,
        )
        .await
        .unwrap();
        let b = append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-B",
            "s-1",
            json!({"owner": "B"}),
            5,
            20,
        )
        .await
        .unwrap();
        assert_eq!(a.new_seq, 1);
        assert_eq!(b.new_seq, 1);
        assert_ne!(a.recent, b.recent);
    }
```

- [ ] **Step 2: Run (expect failure)**

Run: `cargo test -p agentdb-core primitives::tests -- --nocapture`
Expected: FAIL — `append_and_fetch` and `AppendFetchOutput` not defined.

- [ ] **Step 3: Implement `append_and_fetch`**

Append to `crates/agentdb-core/src/primitives.rs`, after `fetch_last_n`:
```rust
use crate::box_runtime::BoxRegistry;
use crate::registry::TenantRegistry;

#[derive(Debug)]
pub struct AppendFetchOutput {
    pub new_seq: u64,
    pub recent: Vec<Event>,
    pub session: Session,
}

pub async fn append_and_fetch(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    tenant_external: &str,
    session_external: &str,
    payload: crate::types::Document,
    fetch_last_n_: usize,
    now_millis: u64,
) -> Result<AppendFetchOutput> {
    let box_ = registry.route_tenant(tenant_external);
    let guard = box_.write_lock.lock().await;
    let kv = registry.kv().clone();
    let tenant = tenants.get_or_mint(box_.id, tenant_external, now_millis)?;
    let mut session =
        sessions.get_or_mint(box_.id, tenant.internal_id, session_external, now_millis)?;
    let event = append_event(&kv, &guard, &mut session, payload, now_millis)?;
    drop(guard);
    let recent = fetch_last_n(&kv, &session, fetch_last_n_)?;
    Ok(AppendFetchOutput {
        new_seq: event.seq,
        recent,
        session,
    })
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p agentdb-core primitives::tests -- --nocapture`
Expected: 8 passed (5 from prior + 3 new).

- [ ] **Step 5: Re-export from lib.rs**

Edit `crates/agentdb-core/src/lib.rs`, update primitives re-export:
```rust
pub use primitives::{append_and_fetch, append_event, fetch_last_n, AppendFetchOutput, DEFAULT_BRANCH_ID};
```

Run: `cargo check --workspace`.

- [ ] **Step 6: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core/src/primitives.rs crates/agentdb-core/src/lib.rs
git commit -m "feat(core): append_and_fetch composed primitive with box routing"
```

---

## Task 11: `agentdb-api` crate skeleton — `AppState`, `router()`, error mapping

**Files:**
- Modify: `crates/agentdb-api/src/lib.rs`
- Create: `crates/agentdb-api/src/error.rs`
- Create: `crates/agentdb-api/src/dto.rs`
- Create: `crates/agentdb-api/src/handlers.rs`

- [ ] **Step 1: Error mapping**

Save to `crates/agentdb-api/src/error.rs`:
```rust
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error(transparent)]
    Core(#[from] agentdb_core::Error),
    #[error("bad request: {0}")]
    BadRequest(String),
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::Core(agentdb_core::Error::NotFound(_)) => StatusCode::NOT_FOUND,
            ApiError::Core(agentdb_core::Error::Invalid(_)) => StatusCode::BAD_REQUEST,
            ApiError::Core(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = self.to_string();
        (status, axum::Json(ErrorBody { error: &body })).into_response()
    }
}
```

- [ ] **Step 2: DTOs**

Save to `crates/agentdb-api/src/dto.rs`:
```rust
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct AppendAndFetchRequest {
    pub event: serde_json::Value,
    #[serde(default)]
    pub fetch_last_n: usize,
}

#[derive(Serialize)]
pub struct AppendAndFetchResponse {
    pub seq: u64,
    pub recent: Vec<EventDto>,
}

#[derive(Serialize)]
pub struct EventDto {
    pub seq: u64,
    pub ts: u64,
    pub payload: serde_json::Value,
}

impl From<agentdb_core::Event> for EventDto {
    fn from(e: agentdb_core::Event) -> Self {
        Self {
            seq: e.seq,
            ts: e.ts,
            payload: e.payload,
        }
    }
}
```

- [ ] **Step 3: Handler stub (filled in Task 12)**

Save to `crates/agentdb-api/src/handlers.rs`:
```rust
// Filled in by Task 12.
```

- [ ] **Step 4: lib.rs with AppState and router builder**

Replace `crates/agentdb-api/src/lib.rs` with:
```rust
//! AgentDB HTTP API (native REST).

use std::sync::Arc;

use axum::Router;

pub mod dto;
pub mod error;
pub mod handlers;

pub use error::ApiError;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<agentdb_core::BoxRegistry>,
    pub tenants: Arc<agentdb_core::TenantRegistry>,
    pub sessions: Arc<agentdb_core::SessionRegistry>,
}

pub fn router(state: AppState) -> Router {
    // Routes added in Task 12.
    Router::new().with_state(state)
}
```

- [ ] **Step 5: Compile check**

Run: `cargo check --workspace`
Expected: clean compile.

- [ ] **Step 6: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-api
git commit -m "feat(api): AppState, router skeleton, error mapping, DTOs"
```

---

## Task 12: `POST /api/tenants/{t}/sessions/{s}/append_and_fetch` handler

**Files:**
- Modify: `crates/agentdb-api/src/handlers.rs`
- Modify: `crates/agentdb-api/src/lib.rs`

**Context:** A single endpoint that calls `agentdb_core::append_and_fetch`, converts the output to DTO, returns JSON.

- [ ] **Step 1: Write handler**

Replace `crates/agentdb-api/src/handlers.rs` with:
```rust
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::Json;

use crate::dto::{AppendAndFetchRequest, AppendAndFetchResponse, EventDto};
use crate::error::ApiError;
use crate::AppState;

pub async fn append_and_fetch(
    State(state): State<AppState>,
    Path((tenant, session)): Path<(String, String)>,
    Json(body): Json<AppendAndFetchRequest>,
) -> Result<Json<AppendAndFetchResponse>, ApiError> {
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let out = agentdb_core::append_and_fetch(
        &state.registry,
        &state.tenants,
        &state.sessions,
        &tenant,
        &session,
        body.event,
        body.fetch_last_n,
        now_millis,
    )
    .await?;
    let recent = out.recent.into_iter().map(EventDto::from).collect();
    Ok(Json(AppendAndFetchResponse {
        seq: out.new_seq,
        recent,
    }))
}
```

- [ ] **Step 2: Wire the route**

Edit `crates/agentdb-api/src/lib.rs`, replace the `router` function:
```rust
pub fn router(state: AppState) -> Router {
    Router::new()
        .route(
            "/api/tenants/:tenant_id/sessions/:session_id/append_and_fetch",
            axum::routing::post(handlers::append_and_fetch),
        )
        .with_state(state)
}
```

- [ ] **Step 3: Compile check**

Run: `cargo check --workspace`
Expected: clean.

- [ ] **Step 4: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-api
git commit -m "feat(api): append_and_fetch REST endpoint"
```

---

## Task 13: `agentdb-server` binary wiring

**Files:**
- Modify: `bin/agentdb-server/src/main.rs`

- [ ] **Step 1: Write the server main**

Replace `bin/agentdb-server/src/main.rs` with:
```rust
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use agentdb_api::{router, AppState};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use anyhow::Context;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let data_dir: PathBuf = std::env::var_os("AGENTDB_DATA")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./data/rocksdb"));
    let num_boxes: u16 = std::env::var("AGENTDB_BOXES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let addr: SocketAddr = std::env::var("AGENTDB_ADDR")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| "127.0.0.1:7878".parse().unwrap());

    std::fs::create_dir_all(&data_dir).context("create data dir")?;
    let kv: Arc<dyn KvStore> =
        Arc::new(RocksKvStore::open(&data_dir).context("open rocksdb")?);
    let registry = Arc::new(BoxRegistry::new(kv.clone(), num_boxes)?);
    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv));
    let state = AppState {
        registry,
        tenants,
        sessions,
    };

    let app = router(state);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("bind listener")?;
    tracing::info!(%addr, "agentdb-server listening");
    axum::serve(listener, app).await.context("serve")?;
    Ok(())
}
```

- [ ] **Step 2: Build check**

Run: `cargo build -p agentdb-server`
Expected: clean build.

- [ ] **Step 3: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add bin/agentdb-server/src/main.rs
git commit -m "feat(server): wire storage + core + api in tokio main"
```

---

## Task 14: HTTP end-to-end integration test

**Files:**
- Create: `crates/agentdb-api/tests/http.rs`

**Context:** Spawns the server on an ephemeral port against a temp RocksDB, uses `reqwest` to POST to the endpoint twice, and verifies the response body and that events are actually persisted.

- [ ] **Step 1: Write the integration test**

Save to `crates/agentdb-api/tests/http.rs`:
```rust
use std::sync::Arc;

use agentdb_api::{router, AppState};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use serde_json::json;
use serial_test::serial;
use tempfile::TempDir;

async fn spawn() -> (TempDir, String) {
    let dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let state = AppState {
        registry: Arc::new(BoxRegistry::new(kv.clone(), 4).unwrap()),
        tenants: Arc::new(TenantRegistry::new(kv.clone())),
        sessions: Arc::new(SessionRegistry::new(kv)),
    };
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (dir, format!("http://{addr}"))
}

#[tokio::test]
#[serial]
async fn append_and_fetch_endpoint_round_trip() {
    let (_dir, base) = spawn().await;
    let client = reqwest::Client::new();

    let resp1: serde_json::Value = client
        .post(format!("{base}/api/tenants/t-1/sessions/s-1/append_and_fetch"))
        .json(&json!({"event": {"kind": "hello"}, "fetch_last_n": 5}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp1["seq"], 1);
    assert_eq!(resp1["recent"].as_array().unwrap().len(), 1);
    assert_eq!(resp1["recent"][0]["payload"]["kind"], "hello");

    let resp2: serde_json::Value = client
        .post(format!("{base}/api/tenants/t-1/sessions/s-1/append_and_fetch"))
        .json(&json!({"event": {"kind": "followup"}, "fetch_last_n": 5}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp2["seq"], 2);
    let recent = resp2["recent"].as_array().unwrap();
    assert_eq!(recent.len(), 2);
    assert_eq!(recent[0]["payload"]["kind"], "hello");
    assert_eq!(recent[1]["payload"]["kind"], "followup");
}

#[tokio::test]
#[serial]
async fn fetch_last_n_respects_bound() {
    let (_dir, base) = spawn().await;
    let client = reqwest::Client::new();

    for i in 1..=4 {
        client
            .post(format!(
                "{base}/api/tenants/t-1/sessions/s-bound/append_and_fetch"
            ))
            .json(&json!({"event": {"i": i}, "fetch_last_n": 0}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    let resp: serde_json::Value = client
        .post(format!(
            "{base}/api/tenants/t-1/sessions/s-bound/append_and_fetch"
        ))
        .json(&json!({"event": {"i": 5}, "fetch_last_n": 2}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["seq"], 5);
    let recent = resp["recent"].as_array().unwrap();
    assert_eq!(recent.len(), 2);
    assert_eq!(recent[0]["payload"]["i"], 4);
    assert_eq!(recent[1]["payload"]["i"], 5);
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p agentdb-api --test http -- --nocapture`
Expected: 2 passed.

- [ ] **Step 3: Run the whole workspace to catch regressions**

Run: `cargo test --workspace`
Expected: all tests pass (28 from M1 + the new ones from M2).

- [ ] **Step 4: Clippy and fmt gates**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings.

Run: `cargo fmt --all --check`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-api/tests/http.rs
git commit -m "test(api): end-to-end HTTP round trip of append_and_fetch"
```

---

## Milestone exit criteria

When every task's tests pass and the following single command is green, M2 is done:

```bash
cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check
```

At that point we have:
- Document / event / session / tenant types with serde round-tripping.
- Meta keyspace that doesn't collide with the events/state/secondary namespaces.
- Read-or-mint registries for external ↔ internal id mapping, idempotent and persisted.
- Logical boxes with deterministic tenant routing.
- Hot-path `append_and_fetch` primitive exercising all of the above atomically per box.
- A working HTTP server that callers can POST JSON to end-to-end.

## Notes for M3 (not in this plan)

When writing the plan for M3 (MongoDB wire protocol subset) we will know:
- Whether `Document = serde_json::Value` is adequate, or whether we need BSON upgrade now.
- Exact shape of the `AppendFetchOutput` a Mongo driver would expect if we re-exposed this primitive through wire protocol.
- Whether the per-box write mutex is the right granularity; Mongo protocol will drive more concurrency.
