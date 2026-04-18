# AgentDB Prototype — Milestone 1: Storage Foundation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a pluggable KV storage foundation: a `KvStore` trait with a `RocksKvStore` implementation and a well-tested key encoder that enables session-locality through lexicographic ordering.

**Architecture:** Cargo workspace with a single `agentdb-storage` crate for now. Trait-based abstraction (`KvStore`) hides backend specifics behind get / put / delete / scan_prefix / write_batch / snapshot primitives. Keys carry a 1-byte namespace tag followed by fixed-width fields (`box_id`, `tenant_id`, `session_id`, `branch_id`, `seq`) totalling 31 bytes, so lexicographic order automatically gives us event-within-session, session-within-tenant, tenant-within-box locality.

**Tech Stack:** Rust 2021 edition, `rust-rocksdb` 0.22, `thiserror` 1, `tempfile` 3 (test-only), `anyhow` 1 (test-only).

**References:**
- Spec: `docs/superpowers/specs/2026-04-17-agentdb-prototype-design.md` §4 (Data model), §5 (Storage layer)
- Source design: `docs/agent-db.md` §3.1 (Object storage as main storage), §3.2 (Distributed KV as middle layer)

---

## File Structure

All paths relative to repo root `/Users/lishen/work/agentdb`.

**Created by this plan:**
- `Cargo.toml` — workspace root
- `rust-toolchain.toml` — pin Rust edition
- `crates/agentdb-storage/Cargo.toml`
- `crates/agentdb-storage/src/lib.rs` — module declarations + public re-exports
- `crates/agentdb-storage/src/error.rs` — error types
- `crates/agentdb-storage/src/kv.rs` — `KvStore` and `KvSnapshot` traits, `WriteOp`, `KvIter`
- `crates/agentdb-storage/src/key.rs` — `Namespace`, ID type aliases, `encode_key`, `decode_key`, `DecodedKey`, prefix helpers
- `crates/agentdb-storage/src/rocks.rs` — `RocksKvStore` and its snapshot handle
- `crates/agentdb-storage/tests/rocks_integration.rs` — integration tests against a real RocksDB instance

**Responsibilities:**
- `lib.rs` — surface only public types; no logic
- `error.rs` — one enum, `From` conversions only
- `kv.rs` — trait definitions, small value types (`WriteOp`, `KvIter` alias). No implementations.
- `key.rs` — pure functions; no I/O
- `rocks.rs` — single implementation of the traits backed by RocksDB

---

## Task 1: Bootstrap workspace and `agentdb-storage` crate

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `crates/agentdb-storage/Cargo.toml`
- Create: `crates/agentdb-storage/src/lib.rs`

- [ ] **Step 1: Write the workspace root `Cargo.toml`**

```toml
[workspace]
resolver = "2"
members = ["crates/agentdb-storage"]

[workspace.package]
edition = "2021"
rust-version = "1.75"
license = "MIT OR Apache-2.0"

[workspace.dependencies]
anyhow = "1"
thiserror = "1"
rocksdb = { version = "0.22", default-features = false, features = ["lz4"] }
tempfile = "3"
```

Save to `Cargo.toml`.

- [ ] **Step 2: Pin the Rust edition**

```toml
[toolchain]
channel = "stable"
components = ["rustfmt", "clippy"]
```

Save to `rust-toolchain.toml`.

- [ ] **Step 3: Write the crate's `Cargo.toml`**

```toml
[package]
name = "agentdb-storage"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
thiserror = { workspace = true }
rocksdb = { workspace = true }

[dev-dependencies]
anyhow = { workspace = true }
tempfile = { workspace = true }
```

Save to `crates/agentdb-storage/Cargo.toml`.

- [ ] **Step 4: Stub `lib.rs` with empty modules**

```rust
//! AgentDB storage layer.
//!
//! Provides the `KvStore` trait and a RocksDB-backed implementation.
//! Keys are produced via the helpers in [`key`] to guarantee
//! session-locality through lexicographic ordering.

pub mod error;
pub mod key;
pub mod kv;
pub mod rocks;

pub use error::{Error, Result};
pub use key::{
    decode_key, encode_key, prefix_for_box, prefix_for_session, prefix_for_session_branch,
    prefix_for_tenant, BoxId, BranchId, DecodedKey, Namespace, Seq, SessionId, TenantId,
};
pub use kv::{KvIter, KvSnapshot, KvStore, WriteOp};
pub use rocks::RocksKvStore;
```

Save to `crates/agentdb-storage/src/lib.rs`.

Empty stubs so the crate compiles before all modules exist:

Save `crates/agentdb-storage/src/error.rs`:
```rust
// Filled in by Task 2.
```
Save `crates/agentdb-storage/src/key.rs`:
```rust
// Filled in by Tasks 4-6.
```
Save `crates/agentdb-storage/src/kv.rs`:
```rust
// Filled in by Tasks 3-4.
```
Save `crates/agentdb-storage/src/rocks.rs`:
```rust
// Filled in by Tasks 8-12.
```

- [ ] **Step 5: Verify the workspace compiles**

Run: `cargo check --workspace`

Expected: Compile errors — `lib.rs` references `pub use` items that don't exist yet. That's expected. The test is merely that `cargo check` reaches the compilation stage (i.e., `Cargo.toml` is valid and the workspace resolves). If you see "error[E0432]: unresolved import" that's fine; if you see "error: failed to parse manifest" fix the toml.

- [ ] **Step 6: Temporarily comment out re-exports to verify clean compile**

Edit `crates/agentdb-storage/src/lib.rs` — wrap the `pub use` blocks in `/* ... */` comments so the crate compiles with empty stubs.

Run: `cargo check --workspace`
Expected: clean compile with warnings only.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml rust-toolchain.toml crates/agentdb-storage/
git commit -m "chore: scaffold workspace and agentdb-storage crate"
```

---

## Task 2: Error types

**Files:**
- Modify: `crates/agentdb-storage/src/error.rs`

- [ ] **Step 1: Write the failing test for `From<rocksdb::Error>` conversion**

Replace the contents of `crates/agentdb-storage/src/error.rs` with:
```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("rocksdb error: {0}")]
    RocksDb(#[from] rocksdb::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_from_io_error() {
        let io = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
        let err: Error = io.into();
        assert!(matches!(err, Error::Io(_)));
        assert!(err.to_string().contains("io error"));
    }

    #[test]
    fn other_variant_carries_message() {
        let err = Error::Other("nope".into());
        assert_eq!(err.to_string(), "nope");
    }
}
```

- [ ] **Step 2: Run test to verify they compile and pass**

Run: `cargo test -p agentdb-storage error::tests -- --nocapture`
Expected: 2 passed.

- [ ] **Step 3: Re-enable the `error` re-exports in `lib.rs`**

In `crates/agentdb-storage/src/lib.rs`, uncomment just the error block:
```rust
pub use error::{Error, Result};
```
Leave `key`, `kv`, `rocks` re-exports commented.

Run: `cargo check --workspace`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/agentdb-storage/src/error.rs crates/agentdb-storage/src/lib.rs
git commit -m "feat(storage): add Error and Result types"
```

---

## Task 3: `WriteOp`, `KvIter`, `KvSnapshot` and `KvStore` traits

**Files:**
- Modify: `crates/agentdb-storage/src/kv.rs`
- Modify: `crates/agentdb-storage/src/lib.rs`

- [ ] **Step 1: Write the trait module**

Replace the contents of `crates/agentdb-storage/src/kv.rs` with:
```rust
use crate::error::Result;

/// A single operation in a write batch.
#[derive(Debug, Clone)]
pub enum WriteOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Iterator yielded by `scan_prefix`.
///
/// Items are owned `(key, value)` pairs so callers do not have to
/// reason about borrow lifetimes against the backing store.
pub type KvIter = Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Send>;

/// A point-in-time read view over a `KvStore`.
pub trait KvSnapshot: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter>;
}

/// Minimum surface the rest of AgentDB depends on.
///
/// Additional backends (e.g. SlateDB) can implement this trait to be
/// drop-in swappable. The key-shape contract (see `key.rs`) is assumed.
pub trait KvStore: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    fn delete(&self, key: &[u8]) -> Result<()>;
    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter>;
    fn write_batch(&self, ops: Vec<WriteOp>) -> Result<()>;
    /// Snapshot borrows from the store, so callers keep the store alive
    /// (e.g. `Arc<dyn KvStore>`) for the snapshot's lifetime.
    fn snapshot<'a>(&'a self) -> Result<Box<dyn KvSnapshot + 'a>>;
}
```

- [ ] **Step 2: Re-enable `kv` re-exports in `lib.rs`**

In `crates/agentdb-storage/src/lib.rs`, uncomment:
```rust
pub use kv::{KvIter, KvSnapshot, KvStore, WriteOp};
```

- [ ] **Step 3: Verify compile**

Run: `cargo check --workspace`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/agentdb-storage/src/kv.rs crates/agentdb-storage/src/lib.rs
git commit -m "feat(storage): define KvStore, KvSnapshot, WriteOp, KvIter"
```

---

## Task 4: `Namespace` enum and ID type aliases

**Files:**
- Modify: `crates/agentdb-storage/src/key.rs`

- [ ] **Step 1: Write the failing test**

Replace `crates/agentdb-storage/src/key.rs` with:
```rust
use crate::error::{Error, Result};

pub type BoxId = u16;
pub type TenantId = u64;
pub type SessionId = u64;
pub type BranchId = u32;
pub type Seq = u64;

/// Namespace tag — the first byte of every key.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Namespace {
    Events = 0x01,
    State = 0x02,
    Secondary = 0x03,
    Meta = 0x04,
}

impl Namespace {
    pub fn from_byte(b: u8) -> Result<Self> {
        match b {
            0x01 => Ok(Self::Events),
            0x02 => Ok(Self::State),
            0x03 => Ok(Self::Secondary),
            0x04 => Ok(Self::Meta),
            other => Err(Error::Other(format!("unknown namespace tag 0x{:02x}", other))),
        }
    }

    pub fn as_byte(self) -> u8 {
        self as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_roundtrip_all_variants() {
        for ns in [
            Namespace::Events,
            Namespace::State,
            Namespace::Secondary,
            Namespace::Meta,
        ] {
            let byte = ns.as_byte();
            assert_eq!(Namespace::from_byte(byte).unwrap(), ns);
        }
    }

    #[test]
    fn from_byte_rejects_unknown_tag() {
        let err = Namespace::from_byte(0xFF).unwrap_err();
        assert!(err.to_string().contains("0xff"));
    }
}
```

- [ ] **Step 2: Run tests to verify they compile and pass**

Run: `cargo test -p agentdb-storage key::tests -- --nocapture`
Expected: 2 passed.

- [ ] **Step 3: Re-enable partial `key` re-exports in `lib.rs`**

In `crates/agentdb-storage/src/lib.rs`, update the `key` re-export line to export only what exists now:
```rust
pub use key::{BoxId, BranchId, Namespace, Seq, SessionId, TenantId};
```
Keep `encode_key`, `decode_key`, `DecodedKey`, `prefix_for_*` commented out until Task 5 and Task 6.

Run: `cargo check --workspace`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/agentdb-storage/src/key.rs crates/agentdb-storage/src/lib.rs
git commit -m "feat(storage): add Namespace enum and ID aliases"
```

---

## Task 5: Key encode and decode

**Files:**
- Modify: `crates/agentdb-storage/src/key.rs`

- [ ] **Step 1: Write the failing round-trip test**

Append the following to `crates/agentdb-storage/src/key.rs`, AFTER the existing `#[cfg(test)] mod tests { ... }` closing brace (i.e., extend that module). Merge into the existing `mod tests`:
```rust
// --- inside mod tests, add these tests ---

    #[test]
    fn encode_decode_roundtrip() {
        let encoded = encode_key(
            Namespace::Events,
            42,               // box
            1_000_000,        // tenant
            7,                // session
            3,                // branch
            99,               // seq
        );
        assert_eq!(encoded.len(), KEY_LEN);

        let decoded = decode_key(&encoded).unwrap();
        assert_eq!(decoded.ns, Namespace::Events);
        assert_eq!(decoded.box_id, 42);
        assert_eq!(decoded.tenant_id, 1_000_000);
        assert_eq!(decoded.session_id, 7);
        assert_eq!(decoded.branch_id, 3);
        assert_eq!(decoded.seq, 99);
    }

    #[test]
    fn decode_rejects_wrong_length() {
        let err = decode_key(&[0x01, 0x02, 0x03]).unwrap_err();
        assert!(err.to_string().contains("length"));
    }

    #[test]
    fn decode_rejects_unknown_namespace() {
        let mut key = vec![0u8; KEY_LEN];
        key[0] = 0x77;
        let err = decode_key(&key).unwrap_err();
        assert!(err.to_string().contains("namespace"));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p agentdb-storage key::tests::encode_decode_roundtrip -- --nocapture`
Expected: FAIL — `encode_key`, `KEY_LEN`, `decode_key`, `DecodedKey` are not defined.

- [ ] **Step 3: Implement `encode_key`, `decode_key`, `DecodedKey`, `KEY_LEN`**

Add between the `Namespace` impl and the `#[cfg(test)]` block in `crates/agentdb-storage/src/key.rs`:
```rust
/// Fixed length of every key: 1 + 2 + 8 + 8 + 4 + 8 = 31 bytes.
pub const KEY_LEN: usize = 1 + 2 + 8 + 8 + 4 + 8;

/// Fully-decoded view of a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedKey {
    pub ns: Namespace,
    pub box_id: BoxId,
    pub tenant_id: TenantId,
    pub session_id: SessionId,
    pub branch_id: BranchId,
    pub seq: Seq,
}

/// Encode a fully-qualified key.
pub fn encode_key(
    ns: Namespace,
    box_id: BoxId,
    tenant_id: TenantId,
    session_id: SessionId,
    branch_id: BranchId,
    seq: Seq,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(KEY_LEN);
    out.push(ns.as_byte());
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out.extend_from_slice(&session_id.to_be_bytes());
    out.extend_from_slice(&branch_id.to_be_bytes());
    out.extend_from_slice(&seq.to_be_bytes());
    debug_assert_eq!(out.len(), KEY_LEN);
    out
}

/// Decode a fully-qualified key.
pub fn decode_key(key: &[u8]) -> Result<DecodedKey> {
    if key.len() != KEY_LEN {
        return Err(Error::Other(format!(
            "key has wrong length: got {}, expected {}",
            key.len(),
            KEY_LEN
        )));
    }
    let ns = Namespace::from_byte(key[0])?;
    let box_id = u16::from_be_bytes(key[1..3].try_into().unwrap());
    let tenant_id = u64::from_be_bytes(key[3..11].try_into().unwrap());
    let session_id = u64::from_be_bytes(key[11..19].try_into().unwrap());
    let branch_id = u32::from_be_bytes(key[19..23].try_into().unwrap());
    let seq = u64::from_be_bytes(key[23..31].try_into().unwrap());
    Ok(DecodedKey {
        ns,
        box_id,
        tenant_id,
        session_id,
        branch_id,
        seq,
    })
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p agentdb-storage key::tests -- --nocapture`
Expected: all 5 tests pass.

- [ ] **Step 5: Re-enable the encode/decode re-exports**

In `crates/agentdb-storage/src/lib.rs` expand the key re-export to:
```rust
pub use key::{
    decode_key, encode_key, BoxId, BranchId, DecodedKey, Namespace, Seq, SessionId, TenantId,
};
```
`prefix_for_*` still commented.

Run: `cargo check --workspace`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/agentdb-storage/src/key.rs crates/agentdb-storage/src/lib.rs
git commit -m "feat(storage): add encode_key, decode_key with round-trip test"
```

---

## Task 6: Prefix helpers

**Files:**
- Modify: `crates/agentdb-storage/src/key.rs`

- [ ] **Step 1: Write failing tests for each prefix helper**

Append inside the existing `#[cfg(test)] mod tests` block in `crates/agentdb-storage/src/key.rs`:
```rust
    #[test]
    fn prefix_for_box_is_prefix_of_full_key() {
        let full = encode_key(Namespace::Events, 42, 1, 2, 3, 4);
        let prefix = prefix_for_box(Namespace::Events, 42);
        assert_eq!(prefix.len(), 3);
        assert!(full.starts_with(&prefix));
    }

    #[test]
    fn prefix_for_tenant_is_prefix_of_full_key() {
        let full = encode_key(Namespace::State, 17, 99, 2, 3, 4);
        let prefix = prefix_for_tenant(Namespace::State, 17, 99);
        assert_eq!(prefix.len(), 11);
        assert!(full.starts_with(&prefix));
    }

    #[test]
    fn prefix_for_session_is_prefix_of_full_key() {
        let full = encode_key(Namespace::Meta, 1, 2, 333, 4, 5);
        let prefix = prefix_for_session(Namespace::Meta, 1, 2, 333);
        assert_eq!(prefix.len(), 19);
        assert!(full.starts_with(&prefix));
    }

    #[test]
    fn prefix_for_session_branch_is_prefix_of_full_key() {
        let full = encode_key(Namespace::Events, 1, 2, 3, 77, 5);
        let prefix = prefix_for_session_branch(Namespace::Events, 1, 2, 3, 77);
        assert_eq!(prefix.len(), 23);
        assert!(full.starts_with(&prefix));
    }

    #[test]
    fn prefix_for_box_does_not_match_different_box() {
        let other = encode_key(Namespace::Events, 43, 1, 2, 3, 4);
        let prefix = prefix_for_box(Namespace::Events, 42);
        assert!(!other.starts_with(&prefix));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p agentdb-storage key::tests::prefix -- --nocapture`
Expected: FAIL — helpers don't exist yet.

- [ ] **Step 3: Implement the four prefix helpers**

Append in `crates/agentdb-storage/src/key.rs` after the `decode_key` function (before the `#[cfg(test)]` block):
```rust
pub fn prefix_for_box(ns: Namespace, box_id: BoxId) -> Vec<u8> {
    let mut out = Vec::with_capacity(3);
    out.push(ns.as_byte());
    out.extend_from_slice(&box_id.to_be_bytes());
    out
}

pub fn prefix_for_tenant(ns: Namespace, box_id: BoxId, tenant_id: TenantId) -> Vec<u8> {
    let mut out = Vec::with_capacity(11);
    out.push(ns.as_byte());
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out
}

pub fn prefix_for_session(
    ns: Namespace,
    box_id: BoxId,
    tenant_id: TenantId,
    session_id: SessionId,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(19);
    out.push(ns.as_byte());
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out.extend_from_slice(&session_id.to_be_bytes());
    out
}

pub fn prefix_for_session_branch(
    ns: Namespace,
    box_id: BoxId,
    tenant_id: TenantId,
    session_id: SessionId,
    branch_id: BranchId,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(23);
    out.push(ns.as_byte());
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out.extend_from_slice(&session_id.to_be_bytes());
    out.extend_from_slice(&branch_id.to_be_bytes());
    out
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p agentdb-storage key::tests -- --nocapture`
Expected: all 10 tests pass.

- [ ] **Step 5: Re-enable the prefix re-exports**

In `crates/agentdb-storage/src/lib.rs` expand:
```rust
pub use key::{
    decode_key, encode_key, prefix_for_box, prefix_for_session, prefix_for_session_branch,
    prefix_for_tenant, BoxId, BranchId, DecodedKey, Namespace, Seq, SessionId, TenantId,
};
```

Run: `cargo check --workspace`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/agentdb-storage/src/key.rs crates/agentdb-storage/src/lib.rs
git commit -m "feat(storage): add prefix helpers for box/tenant/session/branch"
```

---

## Task 7: Key ordering property tests

**Files:**
- Modify: `crates/agentdb-storage/src/key.rs`

**Context:** These tests prove the locality property that the whole architecture depends on. If keys don't sort the way we expect, every later milestone breaks.

- [ ] **Step 1: Write failing ordering tests**

Append inside `#[cfg(test)] mod tests` in `crates/agentdb-storage/src/key.rs`:
```rust
    #[test]
    fn events_in_same_session_sort_by_seq() {
        let a = encode_key(Namespace::Events, 1, 10, 100, 1, 1);
        let b = encode_key(Namespace::Events, 1, 10, 100, 1, 2);
        let c = encode_key(Namespace::Events, 1, 10, 100, 1, 100);
        assert!(a < b && b < c);
    }

    #[test]
    fn sessions_in_same_tenant_are_contiguous_and_ordered() {
        let s1_last = encode_key(Namespace::Events, 1, 10, 1, 0, u64::MAX);
        let s2_first = encode_key(Namespace::Events, 1, 10, 2, 0, 0);
        let s3_first = encode_key(Namespace::Events, 1, 10, 3, 0, 0);
        assert!(s1_last < s2_first);
        assert!(s2_first < s3_first);
    }

    #[test]
    fn tenants_in_same_box_are_contiguous_and_ordered() {
        let t1_last = encode_key(Namespace::Events, 1, 10, u64::MAX, u32::MAX, u64::MAX);
        let t2_first = encode_key(Namespace::Events, 1, 11, 0, 0, 0);
        let t3_first = encode_key(Namespace::Events, 1, 12, 0, 0, 0);
        assert!(t1_last < t2_first);
        assert!(t2_first < t3_first);
    }

    #[test]
    fn namespaces_do_not_interleave() {
        let events_max = encode_key(Namespace::Events, u16::MAX, u64::MAX, u64::MAX, u32::MAX, u64::MAX);
        let state_min = encode_key(Namespace::State, 0, 0, 0, 0, 0);
        assert!(events_max < state_min);
    }

    #[test]
    fn boxes_do_not_interleave_within_namespace() {
        let box1_max = encode_key(Namespace::Events, 1, u64::MAX, u64::MAX, u32::MAX, u64::MAX);
        let box2_min = encode_key(Namespace::Events, 2, 0, 0, 0, 0);
        assert!(box1_max < box2_min);
    }
```

- [ ] **Step 2: Run to verify they pass**

Run: `cargo test -p agentdb-storage key::tests -- --nocapture`
Expected: all 15 tests pass (previous 10 + 5 new ordering tests).

If any fail, the bug is in `encode_key`. Most likely cause: little-endian instead of big-endian — fix and re-run.

- [ ] **Step 3: Commit**

```bash
git add crates/agentdb-storage/src/key.rs
git commit -m "test(storage): prove lexicographic locality properties for keys"
```

---

## Task 8: `RocksKvStore::open` with persistence test

**Files:**
- Modify: `crates/agentdb-storage/src/rocks.rs`
- Create: `crates/agentdb-storage/tests/rocks_integration.rs`

- [ ] **Step 1: Write the failing integration test**

Replace the contents of `crates/agentdb-storage/tests/rocks_integration.rs` with:
```rust
use agentdb_storage::{
    encode_key, BoxId, KvStore, Namespace, RocksKvStore, TenantId,
};
use tempfile::TempDir;

fn tmp() -> TempDir {
    TempDir::new().expect("tempdir")
}

#[test]
fn reopens_and_reads_previously_written_data() {
    let dir = tmp();
    let path = dir.path().to_path_buf();

    {
        let store = RocksKvStore::open(&path).expect("open 1");
        let key = encode_key(Namespace::Events, 1 as BoxId, 2 as TenantId, 3, 0, 1);
        store.put(&key, b"hello").unwrap();
    } // drop

    let store = RocksKvStore::open(&path).expect("reopen");
    let key = encode_key(Namespace::Events, 1, 2, 3, 0, 1);
    let got = store.get(&key).unwrap();
    assert_eq!(got.as_deref(), Some(b"hello".as_ref()));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p agentdb-storage --test rocks_integration reopens -- --nocapture`
Expected: FAIL — `RocksKvStore` not defined.

- [ ] **Step 3: Implement `RocksKvStore::open` with only `put` and `get`**

Replace the contents of `crates/agentdb-storage/src/rocks.rs` with:
```rust
use std::path::Path;
use std::sync::Arc;

use rocksdb::{Options, DB};

use crate::error::Result;
use crate::kv::{KvIter, KvSnapshot, KvStore, WriteOp};

/// RocksDB-backed `KvStore`.
pub struct RocksKvStore {
    db: Arc<DB>,
}

impl RocksKvStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path)?;
        Ok(Self { db: Arc::new(db) })
    }
}

impl KvStore for RocksKvStore {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get(key)?)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put(key, value)?;
        Ok(())
    }

    fn delete(&self, _key: &[u8]) -> Result<()> {
        unimplemented!("delete — Task 9")
    }

    fn scan_prefix(&self, _prefix: &[u8]) -> Result<KvIter> {
        unimplemented!("scan_prefix — Task 10")
    }

    fn write_batch(&self, _ops: Vec<WriteOp>) -> Result<()> {
        unimplemented!("write_batch — Task 11")
    }

    fn snapshot<'a>(&'a self) -> Result<Box<dyn KvSnapshot + 'a>> {
        unimplemented!("snapshot — Task 12")
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p agentdb-storage --test rocks_integration reopens -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-storage/src/rocks.rs crates/agentdb-storage/tests/rocks_integration.rs
git commit -m "feat(storage): RocksKvStore::open, put, get"
```

---

## Task 9: `RocksKvStore::delete`

**Files:**
- Modify: `crates/agentdb-storage/src/rocks.rs`
- Modify: `crates/agentdb-storage/tests/rocks_integration.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/agentdb-storage/tests/rocks_integration.rs`:
```rust
#[test]
fn delete_removes_a_key() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();
    let k = encode_key(Namespace::Events, 1, 2, 3, 0, 1);
    store.put(&k, b"v").unwrap();
    assert_eq!(store.get(&k).unwrap().as_deref(), Some(b"v".as_ref()));
    store.delete(&k).unwrap();
    assert_eq!(store.get(&k).unwrap(), None);
}

#[test]
fn delete_of_missing_key_is_ok() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();
    let k = encode_key(Namespace::Events, 1, 2, 3, 0, 1);
    store.delete(&k).unwrap(); // must not error
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p agentdb-storage --test rocks_integration delete -- --nocapture`
Expected: FAIL — `unimplemented!()` panics.

- [ ] **Step 3: Implement `delete`**

In `crates/agentdb-storage/src/rocks.rs`, replace the `delete` body:
```rust
    fn delete(&self, key: &[u8]) -> Result<()> {
        self.db.delete(key)?;
        Ok(())
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p agentdb-storage --test rocks_integration delete -- --nocapture`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-storage/src/rocks.rs crates/agentdb-storage/tests/rocks_integration.rs
git commit -m "feat(storage): RocksKvStore::delete"
```

---

## Task 10: `RocksKvStore::scan_prefix`

**Files:**
- Modify: `crates/agentdb-storage/src/rocks.rs`
- Modify: `crates/agentdb-storage/tests/rocks_integration.rs`

- [ ] **Step 1: Write failing tests for prefix scanning with locality semantics**

Append to `crates/agentdb-storage/tests/rocks_integration.rs`:
```rust
use agentdb_storage::{prefix_for_session, prefix_for_tenant};

#[test]
fn scan_prefix_returns_only_session_keys_in_order() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();

    // Three sessions for the same tenant; session 2 is the one we query.
    for seq in 1..=3 {
        let k = encode_key(Namespace::Events, 1, 10, 1, 0, seq);
        store.put(&k, format!("s1-{seq}").as_bytes()).unwrap();
    }
    for seq in 1..=3 {
        let k = encode_key(Namespace::Events, 1, 10, 2, 0, seq);
        store.put(&k, format!("s2-{seq}").as_bytes()).unwrap();
    }
    for seq in 1..=3 {
        let k = encode_key(Namespace::Events, 1, 10, 3, 0, seq);
        store.put(&k, format!("s3-{seq}").as_bytes()).unwrap();
    }

    let prefix = prefix_for_session(Namespace::Events, 1, 10, 2);
    let mut iter = store.scan_prefix(&prefix).unwrap();
    let got: Vec<_> = iter.by_ref().map(|r| r.unwrap().1).collect();
    assert_eq!(
        got,
        vec![b"s2-1".to_vec(), b"s2-2".to_vec(), b"s2-3".to_vec()],
    );
}

#[test]
fn scan_prefix_does_not_leak_across_namespaces() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();

    let k_event = encode_key(Namespace::Events, 1, 10, 1, 0, 1);
    let k_state = encode_key(Namespace::State, 1, 10, 1, 0, 1);
    store.put(&k_event, b"event").unwrap();
    store.put(&k_state, b"state").unwrap();

    let prefix = prefix_for_tenant(Namespace::Events, 1, 10);
    let got: Vec<_> = store
        .scan_prefix(&prefix)
        .unwrap()
        .map(|r| r.unwrap().1)
        .collect();
    assert_eq!(got, vec![b"event".to_vec()]);
}

#[test]
fn scan_prefix_on_empty_store_returns_no_rows() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();
    let prefix = prefix_for_tenant(Namespace::Events, 1, 10);
    assert_eq!(store.scan_prefix(&prefix).unwrap().count(), 0);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p agentdb-storage --test rocks_integration scan_prefix -- --nocapture`
Expected: FAIL — `unimplemented!()`.

- [ ] **Step 3: Implement `scan_prefix`**

In `crates/agentdb-storage/src/rocks.rs`, add the import and replace the `scan_prefix` body.

Add/modify imports at the top:
```rust
use rocksdb::{Direction, IteratorMode, Options, DB};
```

Replace `scan_prefix`:
```rust
    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter> {
        let prefix_vec = prefix.to_vec();
        let iter = self
            .db
            .iterator(IteratorMode::From(&prefix_vec, Direction::Forward));
        let check = prefix_vec;
        let mapped = iter
            .take_while(move |res| match res {
                Ok((k, _)) => k.starts_with(&check),
                Err(_) => true, // surface errors through the iterator
            })
            .map(|res| {
                res.map(|(k, v)| (k.to_vec(), v.to_vec()))
                    .map_err(crate::error::Error::from)
            });
        Ok(Box::new(mapped))
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p agentdb-storage --test rocks_integration scan_prefix -- --nocapture`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-storage/src/rocks.rs crates/agentdb-storage/tests/rocks_integration.rs
git commit -m "feat(storage): RocksKvStore::scan_prefix with prefix-bounded iterator"
```

---

## Task 11: `RocksKvStore::write_batch`

**Files:**
- Modify: `crates/agentdb-storage/src/rocks.rs`
- Modify: `crates/agentdb-storage/tests/rocks_integration.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/agentdb-storage/tests/rocks_integration.rs`:
```rust
use agentdb_storage::WriteOp;

#[test]
fn write_batch_applies_mixed_put_and_delete_atomically() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();

    let k_keep = encode_key(Namespace::Events, 1, 10, 1, 0, 1);
    let k_drop = encode_key(Namespace::Events, 1, 10, 1, 0, 2);
    store.put(&k_keep, b"pre-keep").unwrap();
    store.put(&k_drop, b"pre-drop").unwrap();

    let k_new = encode_key(Namespace::Events, 1, 10, 1, 0, 3);
    store
        .write_batch(vec![
            WriteOp::Put {
                key: k_keep.clone(),
                value: b"post-keep".to_vec(),
            },
            WriteOp::Delete {
                key: k_drop.clone(),
            },
            WriteOp::Put {
                key: k_new.clone(),
                value: b"new".to_vec(),
            },
        ])
        .unwrap();

    assert_eq!(store.get(&k_keep).unwrap().as_deref(), Some(b"post-keep".as_ref()));
    assert_eq!(store.get(&k_drop).unwrap(), None);
    assert_eq!(store.get(&k_new).unwrap().as_deref(), Some(b"new".as_ref()));
}

#[test]
fn empty_write_batch_is_a_noop() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();
    store.write_batch(Vec::new()).unwrap();
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p agentdb-storage --test rocks_integration write_batch -- --nocapture`
Expected: FAIL — `unimplemented!()`.

- [ ] **Step 3: Implement `write_batch`**

Add to imports in `crates/agentdb-storage/src/rocks.rs`:
```rust
use rocksdb::WriteBatch;
```

Replace the `write_batch` body:
```rust
    fn write_batch(&self, ops: Vec<WriteOp>) -> Result<()> {
        let mut batch = WriteBatch::default();
        for op in ops {
            match op {
                WriteOp::Put { key, value } => batch.put(&key, &value),
                WriteOp::Delete { key } => batch.delete(&key),
            }
        }
        self.db.write(batch)?;
        Ok(())
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p agentdb-storage --test rocks_integration write_batch -- --nocapture`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-storage/src/rocks.rs crates/agentdb-storage/tests/rocks_integration.rs
git commit -m "feat(storage): RocksKvStore::write_batch"
```

---

## Task 12: `RocksKvStore::snapshot` with isolation test

**Files:**
- Modify: `crates/agentdb-storage/src/rocks.rs`
- Modify: `crates/agentdb-storage/tests/rocks_integration.rs`

**Context:** RocksDB's `Snapshot` borrows from `&DB`. We express the lifetime on the trait method (`snapshot<'a>(&'a self)`). The returned `KvSnapshot` object borrows from the store — callers hold the store via `Arc<dyn KvStore>` so this is fine. For iteration we collect eagerly inside `scan_prefix` to avoid lifetime gymnastics on the iterator type.

- [ ] **Step 1: Write failing isolation tests**

Append to `crates/agentdb-storage/tests/rocks_integration.rs`:
```rust
#[test]
fn snapshot_does_not_observe_writes_that_happened_after_it() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();
    let k = encode_key(Namespace::Events, 1, 10, 1, 0, 1);
    store.put(&k, b"v1").unwrap();

    let snap = store.snapshot().unwrap();
    store.put(&k, b"v2").unwrap();

    assert_eq!(snap.get(&k).unwrap().as_deref(), Some(b"v1".as_ref()));
    assert_eq!(store.get(&k).unwrap().as_deref(), Some(b"v2".as_ref()));
}

#[test]
fn snapshot_scan_prefix_is_consistent() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();
    for seq in 1..=3 {
        let k = encode_key(Namespace::Events, 1, 10, 1, 0, seq);
        store.put(&k, format!("v{seq}").as_bytes()).unwrap();
    }

    let snap = store.snapshot().unwrap();
    // write a new one after snapshot
    let k4 = encode_key(Namespace::Events, 1, 10, 1, 0, 4);
    store.put(&k4, b"v4").unwrap();

    let prefix = prefix_for_session(Namespace::Events, 1, 10, 1);
    let snap_values: Vec<_> = snap
        .scan_prefix(&prefix)
        .unwrap()
        .map(|r| r.unwrap().1)
        .collect();
    assert_eq!(
        snap_values,
        vec![b"v1".to_vec(), b"v2".to_vec(), b"v3".to_vec()],
        "snapshot should not see the key written after it"
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p agentdb-storage --test rocks_integration snapshot -- --nocapture`
Expected: FAIL — `unimplemented!()`.

- [ ] **Step 3: Implement the snapshot handle**

In `crates/agentdb-storage/src/rocks.rs`, add the snapshot handle below the `impl KvStore` block:
```rust
/// Borrows from a `RocksKvStore`; caller keeps the store alive via its own reference.
struct RocksSnapshotHandle<'a> {
    snap: rocksdb::Snapshot<'a>,
}

impl<'a> KvSnapshot for RocksSnapshotHandle<'a> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.snap.get(key)?)
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter> {
        let prefix_vec = prefix.to_vec();
        // Eagerly collect so the returned iterator is 'static w.r.t. lifetimes.
        let mut out: Vec<std::result::Result<(Vec<u8>, Vec<u8>), crate::error::Error>> = Vec::new();
        let iter = self
            .snap
            .iterator(IteratorMode::From(&prefix_vec, Direction::Forward));
        for res in iter {
            match res {
                Ok((k, v)) => {
                    if !k.starts_with(&prefix_vec) {
                        break;
                    }
                    out.push(Ok((k.to_vec(), v.to_vec())));
                }
                Err(e) => {
                    out.push(Err(crate::error::Error::from(e)));
                    break;
                }
            }
        }
        Ok(Box::new(out.into_iter()))
    }
}
```

Replace the `snapshot` body in `impl KvStore for RocksKvStore`:
```rust
    fn snapshot<'a>(&'a self) -> Result<Box<dyn KvSnapshot + 'a>> {
        Ok(Box::new(RocksSnapshotHandle {
            snap: self.db.snapshot(),
        }))
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p agentdb-storage --test rocks_integration snapshot -- --nocapture`
Expected: 2 passed.

- [ ] **Step 5: Run the whole crate's tests to catch regressions**

Run: `cargo test -p agentdb-storage -- --nocapture`
Expected: all tests pass (unit + integration combined).

- [ ] **Step 6: Commit**

```bash
git add crates/agentdb-storage/src/rocks.rs crates/agentdb-storage/tests/rocks_integration.rs
git commit -m "feat(storage): RocksKvStore::snapshot with read isolation"
```

---

## Task 13: End-to-end locality integration test

**Files:**
- Modify: `crates/agentdb-storage/tests/rocks_integration.rs`

**Context:** This is the story-test. It writes data shaped like the real workload will produce and verifies that prefix scans retrieve exactly the locality slices we expect. If this passes, M1 is done.

- [ ] **Step 1: Write the failing end-to-end locality test**

Append to `crates/agentdb-storage/tests/rocks_integration.rs`:
```rust
use agentdb_storage::{prefix_for_box};

/// Simulates three sessions across two tenants within one box, writes
/// 10 events per session plus a state doc per session, and exercises
/// prefix scans at every locality level.
#[test]
fn end_to_end_locality_slices_line_up() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();

    let box_id: BoxId = 7;
    let tenant_a: TenantId = 100;
    let tenant_b: TenantId = 200;

    // Write events for (tenant_a, session 1), (tenant_a, session 2), (tenant_b, session 1)
    let plan = [
        (tenant_a, 1u64),
        (tenant_a, 2u64),
        (tenant_b, 1u64),
    ];
    for (tenant, session) in plan {
        for seq in 1..=10u64 {
            let k = encode_key(Namespace::Events, box_id, tenant, session, 0, seq);
            let v = format!("t{tenant}-s{session}-e{seq}");
            store.put(&k, v.as_bytes()).unwrap();
        }
        // also one state doc per session
        let sk = encode_key(Namespace::State, box_id, tenant, session, 0, 0);
        store.put(&sk, format!("state-t{tenant}-s{session}").as_bytes()).unwrap();
    }

    // Session-level event scan returns exactly those 10 events in order.
    let p = prefix_for_session(Namespace::Events, box_id, tenant_a, 1);
    let rows: Vec<_> = store
        .scan_prefix(&p)
        .unwrap()
        .map(|r| r.unwrap().1)
        .collect();
    assert_eq!(rows.len(), 10);
    assert_eq!(rows[0], b"t100-s1-e1".to_vec());
    assert_eq!(rows[9], b"t100-s1-e10".to_vec());

    // Tenant-level event scan returns 20 events: all of t_a's two sessions.
    let p = prefix_for_tenant(Namespace::Events, box_id, tenant_a);
    let rows: Vec<_> = store.scan_prefix(&p).unwrap().map(|r| r.unwrap().1).collect();
    assert_eq!(rows.len(), 20);

    // Box-level event scan returns 30 events across all tenants.
    let p = prefix_for_box(Namespace::Events, box_id);
    let rows: Vec<_> = store.scan_prefix(&p).unwrap().map(|r| r.unwrap().1).collect();
    assert_eq!(rows.len(), 30);

    // Box-level STATE scan returns exactly 3 state docs, does not leak events.
    let p = prefix_for_box(Namespace::State, box_id);
    let rows: Vec<_> = store.scan_prefix(&p).unwrap().map(|r| r.unwrap().1).collect();
    assert_eq!(rows.len(), 3);
    for row in &rows {
        let s = std::str::from_utf8(row).unwrap();
        assert!(s.starts_with("state-"));
    }
}
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `cargo test -p agentdb-storage --test rocks_integration end_to_end_locality -- --nocapture`
Expected: PASS on the first run — no new production code needed, this is purely a validation that the foundations compose.

If it fails, the failure will be in one of the already-implemented layers (encode_key, scan_prefix). Fix there, not in the test.

- [ ] **Step 3: Run the whole crate test suite once more**

Run: `cargo test -p agentdb-storage -- --nocapture`
Expected: all pass.

- [ ] **Step 4: Run clippy and format checks**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings. Fix any that appear in code you wrote.

Run: `cargo fmt --all --check`
Expected: no diffs. Run `cargo fmt --all` if there are.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-storage/tests/rocks_integration.rs
git commit -m "test(storage): end-to-end locality slices line up across namespaces"
```

---

## Milestone exit criteria

When every task's tests pass and the following single command is green, M1 is done:

```bash
cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check
```

At that point we have:
- A pluggable `KvStore` trait suitable for a second backend (SlateDB) later.
- A production-faithful RocksDB implementation used throughout later milestones.
- A key encoder whose locality properties are proven by unit tests.
- An integration suite that future milestones extend, not rewrite.

## Notes for M2 (not in this plan)

When writing the plan for M2 we will know:
- Exactly how the RocksDB error shapes propagate — inform how `agentdb-core` wraps them.
- Which key helpers are missing for fanout operations (e.g., iterating branches within a session).

Carry any surprises from M1 into the M2 plan as explicit task notes.
