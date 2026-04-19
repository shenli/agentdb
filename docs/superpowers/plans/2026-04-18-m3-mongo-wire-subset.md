# AgentDB Prototype — Milestone 3: MongoDB wire protocol subset

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose AgentDB's existing core primitives over a subset of the MongoDB wire protocol so that the real Rust `mongodb` driver and `mongosh` can connect, list databases/collections, `find`, and `insert` against the server.

**Architecture:** New `agentdb-wire` crate holding OP_MSG framing, BSON↔JSON conversion, a small command-dispatch trait, and concrete handlers for the target commands. The `agentdb-server` binary adds a second TCP listener (default port 7879) that runs the wire protocol loop per connection. Database-to-tenant mapping: the Mongo `db` field on each OP_MSG is treated as the external tenant id. Fixed synthetic collections `events` and `sessions` expose the existing event and session records. Writes go through the existing `append_and_fetch` primitive.

**Tech Stack (M3 additions):** `bson` 2 (BSON encoder/decoder — maintained by MongoDB Inc), `bytes` 1 (byte buffer ergonomics), `byteorder` 1 (LE integer parsing). Integration-test dep: `mongodb` 2 (the official Rust driver).

**References:**
- Spec: `docs/superpowers/specs/2026-04-17-agentdb-prototype-design.md` §7.1 (MongoDB wire protocol), §7.2 (custom aggregation stages — reserved for M4)
- Previous milestones: `docs/superpowers/plans/2026-04-17-m1-storage-foundation.md`, `docs/superpowers/plans/2026-04-18-m2-core-types-and-append-and-fetch.md`

---

## Scope (tight)

**In scope for M3:**
- OP_MSG wire frame (opcode 2013), single-body sections only (no document-sequence payloads in request bodies for M3)
- BSON ↔ `serde_json::Value` conversion for payloads
- Commands: `hello` / `isMaster`, `ping`, `buildInfo`, `listDatabases`, `listCollections`, `find`, `insert`
- Tenant = Mongo database name; fixed collections `events` and `sessions`
- Integration test using the official `mongodb` Rust driver

**Out of scope (explicitly deferred):**
- `getMore` / real cursor management — M3 returns all results in the first batch (no cursor continuation)
- `update`, `delete` — events are append-only; updating sessions is an M5 lifecycle concern
- Auth flows (SCRAM) — our `hello` advertises no mechanisms, driver skips auth
- `aggregate` (including `$vectorSearch` / `$search`) — M4 adds these
- Document-sequence payload sections in requests
- Replica-set / sharding metadata, change streams
- `mongosh` shell-specific convenience commands beyond the subset above (if `mongosh` sends an unsupported command, we return `{ok: 0, errmsg: "<cmd> not implemented in M3"}` so it surfaces cleanly)

**Concurrency contract (unchanged from M2):** writes go through `append_and_fetch` which serializes per-box via the existing `tokio::sync::Mutex`. Reads do not take the mutex. Each TCP connection is handled by its own tokio task; connections share the server's `AppState`.

---

## File Structure

All paths relative to repo root `/Users/lishen/work/agentdb`.

**Created:**
- `crates/agentdb-wire/Cargo.toml`
- `crates/agentdb-wire/src/lib.rs`
- `crates/agentdb-wire/src/error.rs`
- `crates/agentdb-wire/src/opmsg.rs` — OP_MSG header + body codec
- `crates/agentdb-wire/src/bsonx.rs` — BSON ↔ JSON conversion
- `crates/agentdb-wire/src/command.rs` — Command enum + dispatch
- `crates/agentdb-wire/src/handlers/mod.rs`
- `crates/agentdb-wire/src/handlers/hello.rs`
- `crates/agentdb-wire/src/handlers/browse.rs` — listDatabases, listCollections, buildInfo, ping
- `crates/agentdb-wire/src/handlers/find.rs`
- `crates/agentdb-wire/src/handlers/insert.rs`
- `crates/agentdb-wire/src/server.rs` — `serve(listener, state)` accept loop + per-connection driver
- `crates/agentdb-wire/tests/driver.rs` — integration test via `mongodb` crate

**Modified:**
- `Cargo.toml` (add `agentdb-wire` workspace member; new workspace deps: `bson`, `bytes`, `byteorder`, `mongodb`)
- `bin/agentdb-server/Cargo.toml` (add `agentdb-wire` dep)
- `bin/agentdb-server/src/main.rs` (bind + spawn second listener)

**Responsibilities:**
- `opmsg.rs` — pure binary I/O; no command knowledge
- `bsonx.rs` — pure conversion; no wire or storage coupling
- `command.rs` — name + dispatch plumbing; no per-command logic
- `handlers/*.rs` — one command per file; takes `&Connection` + request doc, returns response doc
- `server.rs` — connection lifecycle: accept TCP, read one OP_MSG, dispatch, write reply, loop
- bin changes — wiring only

---

## Task 1: Scaffold `agentdb-wire` crate

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Create: `crates/agentdb-wire/Cargo.toml`
- Create: `crates/agentdb-wire/src/lib.rs`
- Create: `crates/agentdb-wire/src/error.rs`
- Create stubs: `crates/agentdb-wire/src/{opmsg,bsonx,command,server}.rs`
- Create: `crates/agentdb-wire/src/handlers/mod.rs`

- [ ] **Step 1: Register new crate + deps in workspace root**

Edit `Cargo.toml`. Add `crates/agentdb-wire` to `members`. Add to `workspace.dependencies`:
```toml
bson = "2"
bytes = "1"
byteorder = "1"
mongodb = { version = "2", default-features = false, features = ["tokio-runtime"] }
```
Final `members` list:
```toml
members = [
    "crates/agentdb-storage",
    "crates/agentdb-core",
    "crates/agentdb-api",
    "crates/agentdb-wire",
    "bin/agentdb-server",
]
```

- [ ] **Step 2: Write the crate Cargo.toml**

Save to `crates/agentdb-wire/Cargo.toml`:
```toml
[package]
name = "agentdb-wire"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
agentdb-core = { path = "../agentdb-core" }
bson = { workspace = true }
byteorder = { workspace = true }
bytes = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
agentdb-storage = { path = "../agentdb-storage" }
anyhow = { workspace = true }
mongodb = { workspace = true }
tempfile = { workspace = true }
tokio = { workspace = true, features = ["test-util", "macros"] }
```

- [ ] **Step 3: Write error.rs with tests**

Save to `crates/agentdb-wire/src/error.rs`:
```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WireError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("bson error: {0}")]
    Bson(#[from] bson::de::Error),
    #[error("bson ser error: {0}")]
    BsonSer(#[from] bson::ser::Error),
    #[error("core error: {0}")]
    Core(#[from] agentdb_core::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
}

pub type WireResult<T> = std::result::Result<T, WireError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_error_converts() {
        let io = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof");
        let err: WireError = io.into();
        assert!(matches!(err, WireError::Io(_)));
    }

    #[test]
    fn protocol_error_formats() {
        let err = WireError::Protocol("bad opcode".into());
        assert_eq!(err.to_string(), "protocol error: bad opcode");
    }
}
```

- [ ] **Step 4: lib.rs with module declarations**

Save to `crates/agentdb-wire/src/lib.rs`:
```rust
//! AgentDB MongoDB wire protocol subset.

pub mod bsonx;
pub mod command;
pub mod error;
pub mod handlers;
pub mod opmsg;
pub mod server;

pub use error::{WireError, WireResult};
```

Stub files:
- `crates/agentdb-wire/src/opmsg.rs`: `// Filled in by Task 2.`
- `crates/agentdb-wire/src/bsonx.rs`: `// Filled in by Task 3.`
- `crates/agentdb-wire/src/command.rs`: `// Filled in by Task 4.`
- `crates/agentdb-wire/src/server.rs`: `// Filled in by Task 11.`
- `crates/agentdb-wire/src/handlers/mod.rs`:
  ```rust
  // Filled in by Tasks 5-10.
  ```

- [ ] **Step 5: Verify compile + tests**

Run: `cargo check --workspace`
Expected: clean (warnings OK for empty stubs).

Run: `cargo test -p agentdb-wire error::tests`
Expected: 2 passed.

- [ ] **Step 6: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add Cargo.toml crates/agentdb-wire
git commit -m "chore: scaffold agentdb-wire crate"
```

---

## Task 2: OP_MSG wire codec

**Files:**
- Modify: `crates/agentdb-wire/src/opmsg.rs`

**Context:** OP_MSG (opcode 2013) is the only message type we implement. Frame layout:
```
Header (16 bytes, little-endian):
  messageLength: i32  // total message size including this header
  requestID:     i32
  responseTo:    i32
  opCode:        i32  // 2013 = OP_MSG

OP_MSG body:
  flagBits: u32
  sections: repeated Section
  // optional 4-byte checksum if flagBits bit 0

Section kind byte:
  0 = Body: followed by one BSON document
  1 = Document Sequence: i32 size + cstring identifier + BSON docs (we only DECODE not encode these in M3)
```

For M3 we only write responses as single-body-section OP_MSG. Requests may contain both kinds; we accept kind=0 and kind=1.

- [ ] **Step 1: Write the codec with unit tests**

Replace `crates/agentdb-wire/src/opmsg.rs` with:
```rust
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Read, Write};

use crate::error::{WireError, WireResult};

pub const OP_MSG: i32 = 2013;
pub const HEADER_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub message_length: i32,
    pub request_id: i32,
    pub response_to: i32,
    pub op_code: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Section {
    Body(bson::Document),
    DocumentSequence {
        identifier: String,
        documents: Vec<bson::Document>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpMsg {
    pub header: Header,
    pub flag_bits: u32,
    pub sections: Vec<Section>,
}

impl OpMsg {
    /// Pull the single body section if present (most commands use exactly one).
    pub fn body(&self) -> Option<&bson::Document> {
        self.sections.iter().find_map(|s| match s {
            Section::Body(d) => Some(d),
            _ => None,
        })
    }
}

pub fn decode(buf: &[u8]) -> WireResult<OpMsg> {
    if buf.len() < HEADER_LEN {
        return Err(WireError::Protocol(format!(
            "message too short: {} < {}",
            buf.len(),
            HEADER_LEN
        )));
    }
    let mut rdr = Cursor::new(buf);
    let message_length = rdr.read_i32::<LittleEndian>()?;
    let request_id = rdr.read_i32::<LittleEndian>()?;
    let response_to = rdr.read_i32::<LittleEndian>()?;
    let op_code = rdr.read_i32::<LittleEndian>()?;
    if op_code != OP_MSG {
        return Err(WireError::Protocol(format!("unsupported opcode {op_code}")));
    }
    if (message_length as usize) != buf.len() {
        return Err(WireError::Protocol(format!(
            "messageLength {} does not match buffer {}",
            message_length,
            buf.len()
        )));
    }
    let flag_bits = rdr.read_u32::<LittleEndian>()?;
    let has_checksum = (flag_bits & 0x1) != 0;
    let payload_end = buf.len() - if has_checksum { 4 } else { 0 };

    let mut sections = Vec::new();
    while (rdr.position() as usize) < payload_end {
        let kind = rdr.read_u8()?;
        match kind {
            0 => {
                let pos = rdr.position() as usize;
                let doc_len = (&buf[pos..]).read_i32::<LittleEndian>()?;
                let end = pos + doc_len as usize;
                let doc: bson::Document = bson::from_slice(&buf[pos..end])?;
                rdr.set_position(end as u64);
                sections.push(Section::Body(doc));
            }
            1 => {
                let seq_size = rdr.read_i32::<LittleEndian>()?;
                let seq_start = rdr.position() as usize;
                let seq_end = seq_start + (seq_size as usize) - 4; // size is inclusive of its own 4 bytes
                // cstring identifier
                let mut ident = Vec::new();
                loop {
                    let b = rdr.read_u8()?;
                    if b == 0 {
                        break;
                    }
                    ident.push(b);
                }
                let identifier = String::from_utf8(ident)
                    .map_err(|e| WireError::Protocol(format!("bad identifier: {e}")))?;
                let mut docs = Vec::new();
                while (rdr.position() as usize) < seq_end {
                    let pos = rdr.position() as usize;
                    let doc_len = (&buf[pos..]).read_i32::<LittleEndian>()?;
                    let end = pos + doc_len as usize;
                    let doc: bson::Document = bson::from_slice(&buf[pos..end])?;
                    rdr.set_position(end as u64);
                    docs.push(doc);
                }
                sections.push(Section::DocumentSequence {
                    identifier,
                    documents: docs,
                });
            }
            other => {
                return Err(WireError::Protocol(format!(
                    "unknown section kind 0x{other:02x}"
                )));
            }
        }
    }
    Ok(OpMsg {
        header: Header {
            message_length,
            request_id,
            response_to,
            op_code,
        },
        flag_bits,
        sections,
    })
}

/// Encode a single-body-section OP_MSG response to `to`.
pub fn encode_body_response(
    to: &mut Vec<u8>,
    request_id: i32,
    response_to: i32,
    body: &bson::Document,
) -> WireResult<()> {
    let body_bytes = bson::to_vec(body)?;
    let payload_len = 4 /* flag_bits */ + 1 /* kind */ + body_bytes.len();
    let total_len: i32 = (HEADER_LEN + payload_len) as i32;
    to.reserve(total_len as usize);
    let mut h = Vec::with_capacity(HEADER_LEN);
    h.write_i32::<LittleEndian>(total_len)?;
    h.write_i32::<LittleEndian>(request_id)?;
    h.write_i32::<LittleEndian>(response_to)?;
    h.write_i32::<LittleEndian>(OP_MSG)?;
    to.extend_from_slice(&h);
    to.write_u32::<LittleEndian>(0)?; // flag_bits
    to.write_u8(0)?; // section kind = Body
    to.extend_from_slice(&body_bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn single_body_roundtrip() {
        let request = doc! { "hello": 1, "$db": "admin" };
        let mut buf = Vec::new();
        encode_body_response(&mut buf, 42, 0, &request).unwrap();
        let decoded = decode(&buf).unwrap();
        assert_eq!(decoded.header.op_code, OP_MSG);
        assert_eq!(decoded.header.request_id, 42);
        assert_eq!(decoded.header.response_to, 0);
        assert_eq!(decoded.sections.len(), 1);
        match &decoded.sections[0] {
            Section::Body(d) => assert_eq!(d.get_i32("hello").unwrap(), 1),
            other => panic!("expected Body, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_non_opmsg() {
        let mut buf = Vec::new();
        buf.write_i32::<LittleEndian>(16).unwrap();
        buf.write_i32::<LittleEndian>(0).unwrap();
        buf.write_i32::<LittleEndian>(0).unwrap();
        buf.write_i32::<LittleEndian>(9999).unwrap(); // bad opcode
        let err = decode(&buf).unwrap_err();
        assert!(err.to_string().contains("unsupported opcode"));
    }

    #[test]
    fn decode_rejects_length_mismatch() {
        let mut buf = Vec::new();
        buf.write_i32::<LittleEndian>(999).unwrap();
        buf.write_i32::<LittleEndian>(0).unwrap();
        buf.write_i32::<LittleEndian>(0).unwrap();
        buf.write_i32::<LittleEndian>(OP_MSG).unwrap();
        let err = decode(&buf).unwrap_err();
        assert!(err.to_string().contains("messageLength"));
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p agentdb-wire opmsg::tests -- --nocapture`
Expected: 3 passed.

- [ ] **Step 3: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-wire/src/opmsg.rs
git commit -m "feat(wire): OP_MSG frame codec with single-body + doc-sequence sections"
```

---

## Task 3: BSON ↔ JSON conversion

**Files:**
- Modify: `crates/agentdb-wire/src/bsonx.rs`

**Context:** `agentdb-core` stores documents as `serde_json::Value`. The wire protocol speaks BSON. We convert at the boundary. BSON-specific types (ObjectId, Decimal128, Binary, Timestamp) are stringified on conversion to JSON; inverse direction treats all strings as plain strings. Known lossy; acceptable for prototype.

- [ ] **Step 1: Write conversion module with tests**

Replace `crates/agentdb-wire/src/bsonx.rs` with:
```rust
use bson::{Bson, Document};
use serde_json::{Map, Number, Value};

/// Convert a BSON value to a JSON value. Types without a JSON equivalent
/// (ObjectId, Timestamp, Decimal128, Binary, etc.) are stringified.
pub fn bson_to_json(b: Bson) -> Value {
    match b {
        Bson::Double(n) => Number::from_f64(n).map(Value::Number).unwrap_or(Value::Null),
        Bson::String(s) => Value::String(s),
        Bson::Array(a) => Value::Array(a.into_iter().map(bson_to_json).collect()),
        Bson::Document(d) => Value::Object(doc_to_map(d)),
        Bson::Boolean(b) => Value::Bool(b),
        Bson::Null => Value::Null,
        Bson::Int32(i) => Value::Number(Number::from(i)),
        Bson::Int64(i) => Value::Number(Number::from(i)),
        Bson::ObjectId(oid) => Value::String(oid.to_hex()),
        Bson::DateTime(dt) => Value::String(dt.to_string()),
        Bson::Timestamp(ts) => Value::String(format!("{}:{}", ts.time, ts.increment)),
        Bson::Decimal128(d) => Value::String(d.to_string()),
        Bson::Binary(bin) => Value::String(format!("{:?}", bin.bytes)),
        Bson::Symbol(s) => Value::String(s),
        Bson::RegularExpression(r) => Value::String(format!("/{}/{}", r.pattern, r.options)),
        Bson::JavaScriptCode(s) | Bson::JavaScriptCodeWithScope(bson::JavaScriptCodeWithScope { code: s, .. }) => {
            Value::String(s)
        }
        Bson::DbPointer(_) | Bson::Undefined | Bson::MaxKey | Bson::MinKey => Value::Null,
    }
}

pub fn doc_to_map(d: Document) -> Map<String, Value> {
    let mut out = Map::with_capacity(d.len());
    for (k, v) in d {
        out.insert(k, bson_to_json(v));
    }
    out
}

pub fn doc_to_json(d: Document) -> Value {
    Value::Object(doc_to_map(d))
}

/// Convert a JSON value to BSON. Strings remain strings (no ObjectId inference).
pub fn json_to_bson(v: Value) -> Bson {
    match v {
        Value::Null => Bson::Null,
        Value::Bool(b) => Bson::Boolean(b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Bson::Int64(i)
            } else {
                n.as_f64().map(Bson::Double).unwrap_or(Bson::Null)
            }
        }
        Value::String(s) => Bson::String(s),
        Value::Array(a) => Bson::Array(a.into_iter().map(json_to_bson).collect()),
        Value::Object(o) => {
            let mut d = Document::new();
            for (k, v) in o {
                d.insert(k, json_to_bson(v));
            }
            Bson::Document(d)
        }
    }
}

pub fn json_to_doc(v: Value) -> Document {
    match json_to_bson(v) {
        Bson::Document(d) => d,
        other => {
            let mut d = Document::new();
            d.insert("value", other);
            d
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use serde_json::json;

    #[test]
    fn json_primitive_roundtrip() {
        let v = json!({"s": "hi", "n": 42, "b": true, "f": 1.5, "nil": null});
        let d = json_to_doc(v.clone());
        let back = doc_to_json(d);
        assert_eq!(back, v);
    }

    #[test]
    fn nested_array_and_object_roundtrip() {
        let v = json!({"a": [1, 2, {"x": [true, null]}]});
        let d = json_to_doc(v.clone());
        let back = doc_to_json(d);
        assert_eq!(back, v);
    }

    #[test]
    fn object_id_becomes_hex_string() {
        let oid = bson::oid::ObjectId::new();
        let d = doc! { "_id": oid };
        let v = doc_to_json(d);
        assert_eq!(v["_id"].as_str().unwrap(), oid.to_hex());
    }

    #[test]
    fn int32_preserved_as_integer_in_json() {
        let d = doc! { "n": 7i32 };
        let v = doc_to_json(d);
        assert_eq!(v["n"].as_i64(), Some(7));
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p agentdb-wire bsonx::tests -- --nocapture`
Expected: 4 passed.

- [ ] **Step 3: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-wire/src/bsonx.rs
git commit -m "feat(wire): BSON <-> JSON conversion at the wire boundary"
```

---

## Task 4: Command dispatch infrastructure

**Files:**
- Modify: `crates/agentdb-wire/src/command.rs`

**Context:** Every OP_MSG body document's first field is the command name (e.g., `hello`, `find`, `insert`). The `$db` field names the database (maps to tenant). Command handlers return a response `bson::Document`. A handler produces either a successful response (with `ok: 1`) or an error response (with `ok: 0` + `errmsg`).

- [ ] **Step 1: Write command module with tests**

Replace `crates/agentdb-wire/src/command.rs` with:
```rust
use bson::{doc, Document};

use crate::error::{WireError, WireResult};

/// Extract the command name (first field of the body) and the target database (`$db`).
pub struct CommandHead<'a> {
    pub name: &'a str,
    pub db: Option<&'a str>,
}

pub fn inspect(body: &Document) -> WireResult<CommandHead<'_>> {
    let (name, _value) = body
        .iter()
        .next()
        .ok_or_else(|| WireError::Protocol("empty command body".into()))?;
    let db = body.get_str("$db").ok();
    Ok(CommandHead {
        name: name.as_str(),
        db,
    })
}

/// Turn any internal error into an `ok: 0` response document.
pub fn error_reply(err: &WireError) -> Document {
    doc! {
        "ok": 0.0,
        "errmsg": err.to_string(),
        "code": 1,
    }
}

pub fn not_implemented(cmd: &str) -> Document {
    doc! {
        "ok": 0.0,
        "errmsg": format!("{cmd} not implemented in M3"),
        "code": 59, // CommandNotFound
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inspect_returns_command_name() {
        let body = doc! { "find": "events", "$db": "t-1", "filter": {} };
        let head = inspect(&body).unwrap();
        assert_eq!(head.name, "find");
        assert_eq!(head.db, Some("t-1"));
    }

    #[test]
    fn inspect_missing_db_is_ok() {
        let body = doc! { "hello": 1 };
        let head = inspect(&body).unwrap();
        assert_eq!(head.name, "hello");
        assert!(head.db.is_none());
    }

    #[test]
    fn error_reply_has_ok_zero_and_errmsg() {
        let err = WireError::Protocol("bad".into());
        let d = error_reply(&err);
        assert_eq!(d.get_f64("ok").unwrap(), 0.0);
        assert!(d.get_str("errmsg").unwrap().contains("bad"));
    }

    #[test]
    fn not_implemented_reply_carries_command_name() {
        let d = not_implemented("killCursors");
        assert!(d.get_str("errmsg").unwrap().contains("killCursors"));
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p agentdb-wire command::tests -- --nocapture`
Expected: 4 passed.

- [ ] **Step 3: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-wire/src/command.rs
git commit -m "feat(wire): command inspection and error/not-implemented helpers"
```

---

## Task 5: `hello` / `isMaster` / `ping` / `buildInfo` handlers

**Files:**
- Create: `crates/agentdb-wire/src/handlers/hello.rs`
- Create: `crates/agentdb-wire/src/handlers/browse.rs`
- Modify: `crates/agentdb-wire/src/handlers/mod.rs`

**Context:** These four commands return fixed responses. `hello` / `isMaster` must make the driver treat us as a usable single-node server. We advertise `maxWireVersion = 17` (Mongo 6.0+ OP_MSG era) and no auth mechanisms. `buildInfo` returns a plausible version string. `ping` returns `{ok: 1}`.

- [ ] **Step 1: Write hello.rs**

Save to `crates/agentdb-wire/src/handlers/hello.rs`:
```rust
use bson::doc;

/// Build the hello/isMaster response used by both command names.
pub fn hello_response(connection_id: i64) -> bson::Document {
    doc! {
        "isWritablePrimary": true,
        "ismaster": true,
        "helloOk": true,
        "maxBsonObjectSize": 16_777_216_i32,
        "maxMessageSizeBytes": 48_000_000_i32,
        "maxWriteBatchSize": 100_000_i32,
        "localTime": bson::DateTime::now(),
        "logicalSessionTimeoutMinutes": 30_i32,
        "connectionId": connection_id,
        "minWireVersion": 0_i32,
        "maxWireVersion": 17_i32,
        "readOnly": false,
        "saslSupportedMechs": bson::Bson::Array(vec![]),
        "ok": 1.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_advertises_writable_primary_and_ok() {
        let r = hello_response(1);
        assert!(r.get_bool("isWritablePrimary").unwrap());
        assert_eq!(r.get_f64("ok").unwrap(), 1.0);
        assert_eq!(r.get_i64("connectionId").unwrap(), 1);
    }

    #[test]
    fn hello_disables_auth_mechanisms() {
        let r = hello_response(1);
        let mechs = r.get_array("saslSupportedMechs").unwrap();
        assert!(mechs.is_empty());
    }
}
```

Note: `connectionId` may be either i32 or i64 depending on the driver; `bson::doc!` with an `i64` value produces Int64. The test reads back as i32 which works only when the value fits; but to be safe we'll read `connection_id` as i64 via the driver. Leave as i64 in the response.

- [ ] **Step 2: Write browse.rs with buildInfo, ping, listDatabases, listCollections**

Save to `crates/agentdb-wire/src/handlers/browse.rs`:
```rust
use agentdb_core::{BoxRegistry, TenantRegistry};
use bson::{doc, Bson, Document};

use crate::error::WireResult;

pub fn build_info_response() -> Document {
    doc! {
        "version": "7.0.0-agentdb",
        "versionArray": [7_i32, 0_i32, 0_i32, 0_i32],
        "gitVersion": "agentdb-prototype",
        "modules": Bson::Array(vec![]),
        "sysInfo": "deprecated",
        "allocator": "system",
        "javascriptEngine": "none",
        "bits": 64_i32,
        "debug": false,
        "maxBsonObjectSize": 16_777_216_i32,
        "storageEngines": Bson::Array(vec![Bson::String("rocksdb".into())]),
        "ok": 1.0,
    }
}

pub fn ping_response() -> Document {
    doc! { "ok": 1.0 }
}

/// Enumerate tenants as databases. Scans the Meta namespace for tenant records
/// across all boxes (delegated to the registry).
pub fn list_databases_response(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
) -> WireResult<Document> {
    let mut names: Vec<String> = Vec::new();
    for box_id in 0..registry.num_boxes() {
        let mut per_box = tenants.list_in_box(box_id)?;
        names.append(&mut per_box);
    }
    names.sort();
    names.dedup();
    let dbs: Vec<Bson> = names
        .into_iter()
        .map(|name| {
            Bson::Document(doc! {
                "name": name,
                "sizeOnDisk": 0_i64,
                "empty": false,
            })
        })
        .collect();
    Ok(doc! {
        "databases": Bson::Array(dbs),
        "totalSize": 0_i64,
        "ok": 1.0,
    })
}

/// Collections within a database are a fixed set in M3.
pub fn list_collections_response(_db: &str) -> Document {
    let events = doc! { "name": "events", "type": "collection" };
    let sessions = doc! { "name": "sessions", "type": "collection" };
    let batch = vec![Bson::Document(events), Bson::Document(sessions)];
    doc! {
        "cursor": {
            "id": 0_i64,
            "ns": format!("{}.$cmd.listCollections", _db),
            "firstBatch": Bson::Array(batch),
        },
        "ok": 1.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_info_has_version() {
        let r = build_info_response();
        assert!(r.get_str("version").unwrap().contains("agentdb"));
    }

    #[test]
    fn ping_ok() {
        assert_eq!(ping_response().get_f64("ok").unwrap(), 1.0);
    }

    #[test]
    fn list_collections_returns_events_and_sessions() {
        let r = list_collections_response("t-1");
        let cursor = r.get_document("cursor").unwrap();
        let batch = cursor.get_array("firstBatch").unwrap();
        let names: Vec<&str> = batch
            .iter()
            .map(|b| {
                b.as_document()
                    .unwrap()
                    .get_str("name")
                    .unwrap()
            })
            .collect();
        assert_eq!(names, vec!["events", "sessions"]);
    }
}
```

Note `list_databases_response` calls `tenants.list_in_box(box_id)` — that method does not yet exist in `TenantRegistry`. Add it in the next task (T6).

- [ ] **Step 3: Update handlers/mod.rs**

Replace `crates/agentdb-wire/src/handlers/mod.rs` with:
```rust
pub mod browse;
pub mod hello;
// find, insert filled in by Tasks 8-10.
```

- [ ] **Step 4: Compile check — expect failure on `list_in_box`**

Run: `cargo check --workspace`
Expected: ERROR — `list_in_box` not defined on `TenantRegistry`. This is expected; T6 adds it.

- [ ] **Step 5: Comment out the failing call temporarily**

To let the rest compile, replace the body of `list_databases_response` in `browse.rs` with a stub that returns an empty list:
```rust
pub fn list_databases_response(
    _registry: &BoxRegistry,
    _tenants: &TenantRegistry,
) -> WireResult<Document> {
    // Real enumeration lands in Task 6.
    Ok(doc! {
        "databases": Bson::Array(vec![]),
        "totalSize": 0_i64,
        "ok": 1.0,
    })
}
```

Run: `cargo check --workspace`
Expected: clean.

- [ ] **Step 6: Run tests**

Run: `cargo test -p agentdb-wire handlers -- --nocapture`
Expected: 5 passed (2 hello + 3 browse).

- [ ] **Step 7: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-wire/src/handlers
git commit -m "feat(wire): hello, ping, buildInfo, listCollections handlers"
```

---

## Task 6: `TenantRegistry::list_in_box` and wire it into listDatabases

**Files:**
- Modify: `crates/agentdb-core/src/registry.rs`
- Modify: `crates/agentdb-core/src/meta.rs`
- Modify: `crates/agentdb-wire/src/handlers/browse.rs`

**Context:** `listDatabases` requires enumerating tenants per box. We add a prefix for "all tenant lookup entries in a given box" using the existing `TAG_TENANT_BY_EXT = 0x01`. The key has shape `Meta|0x01|box(2B)|<external bytes>`. A 4-byte prefix `Meta|0x01|box` selects all tenant lookups for the box. The registry iterates, pulls the external name out of the key suffix.

- [ ] **Step 1: Add `prefix_tenant_by_external_for_box` helper in meta.rs**

In `crates/agentdb-core/src/meta.rs`, after the existing functions (and before `#[cfg(test)]`), add:
```rust
pub(crate) fn prefix_tenant_by_external_for_box(box_id: BoxId) -> Vec<u8> {
    let mut out = Vec::with_capacity(4);
    out.push(NS_META);
    out.push(TAG_TENANT_BY_EXT);
    out.extend_from_slice(&box_id.to_be_bytes());
    out
}
```

Inside `#[cfg(test)] mod tests`, add:
```rust
    #[test]
    fn prefix_tenant_by_external_matches_concrete_tenant_keys() {
        let prefix = prefix_tenant_by_external_for_box(5);
        let concrete = key_tenant_by_external(5, "t-a");
        assert!(concrete.starts_with(&prefix));
        let other = key_tenant_by_external(6, "t-a");
        assert!(!other.starts_with(&prefix));
    }
```

- [ ] **Step 2: Write failing test for `list_in_box` in registry tests**

In `crates/agentdb-core/src/registry.rs`, append inside `#[cfg(test)] mod tests`:
```rust
    #[test]
    fn list_in_box_returns_all_minted_externals_sorted() {
        let (_dir, kv) = store();
        let reg = TenantRegistry::new(kv);
        reg.get_or_mint(1, "t-c", 0).unwrap();
        reg.get_or_mint(1, "t-a", 0).unwrap();
        reg.get_or_mint(1, "t-b", 0).unwrap();
        reg.get_or_mint(2, "t-elsewhere", 0).unwrap();

        let mut got = reg.list_in_box(1).unwrap();
        got.sort();
        assert_eq!(got, vec!["t-a".to_string(), "t-b".to_string(), "t-c".to_string()]);
    }
```

- [ ] **Step 3: Implement `list_in_box` on TenantRegistry**

In `crates/agentdb-core/src/registry.rs`, add a method inside `impl TenantRegistry`:
```rust
    /// Enumerate all external tenant ids minted under this box.
    pub fn list_in_box(&self, box_id: BoxId) -> Result<Vec<String>> {
        let prefix = crate::meta::prefix_tenant_by_external_for_box(box_id);
        let mut out = Vec::new();
        for res in self.kv.scan_prefix(&prefix)? {
            let (k, _v) = res?;
            // key = Meta(1) | TAG(1) | box(2) | external-bytes
            if k.len() <= 4 {
                continue;
            }
            let name = std::str::from_utf8(&k[4..])
                .map_err(|e| crate::error::Error::Invalid(format!("bad tenant name utf-8: {e}")))?
                .to_string();
            out.push(name);
        }
        Ok(out)
    }
```

- [ ] **Step 4: Restore the real `list_databases_response` body in browse.rs**

Replace the stubbed body with:
```rust
pub fn list_databases_response(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
) -> WireResult<Document> {
    let mut names: Vec<String> = Vec::new();
    for box_id in 0..registry.num_boxes() {
        let mut per_box = tenants.list_in_box(box_id)?;
        names.append(&mut per_box);
    }
    names.sort();
    names.dedup();
    let dbs: Vec<Bson> = names
        .into_iter()
        .map(|name| {
            Bson::Document(doc! {
                "name": name,
                "sizeOnDisk": 0_i64,
                "empty": false,
            })
        })
        .collect();
    Ok(doc! {
        "databases": Bson::Array(dbs),
        "totalSize": 0_i64,
        "ok": 1.0,
    })
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p agentdb-core -- --nocapture`
Expected: all core tests pass (34 now: 33 previous + 1 new for list_in_box; plus the meta prefix test = 35).

Run: `cargo test -p agentdb-wire -- --nocapture`
Expected: all wire tests pass (2 error + 3 opmsg + 4 bsonx + 4 command + 5 handlers = 18).

- [ ] **Step 6: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core/src/meta.rs crates/agentdb-core/src/registry.rs crates/agentdb-wire/src/handlers/browse.rs
git commit -m "feat(core,wire): TenantRegistry::list_in_box wired into listDatabases"
```

---

## Task 7: BSON filter → event predicate (minimal)

**Files:**
- Create: `crates/agentdb-wire/src/handlers/filter.rs`
- Modify: `crates/agentdb-wire/src/handlers/mod.rs`

**Context:** For M3 `find` we support a minimal filter dialect:
- Equality: `{ field: value }` matches documents where `doc[field] == value`
- `$eq`, `$gt`, `$gte`, `$lt`, `$lte`, `$ne` operators
- Nested field paths via dotted keys (`"payload.kind"`)
- Implicit AND across top-level fields
- No `$and` / `$or` / `$in` / `$exists` in M3 — return an "unsupported filter" error

The filter operates on `serde_json::Value` (since events are stored as JSON with seq/ts/payload fields).

- [ ] **Step 1: Write filter module with tests**

Save to `crates/agentdb-wire/src/handlers/filter.rs`:
```rust
use bson::Document as BsonDoc;
use serde_json::Value;

use crate::bsonx::bson_to_json;
use crate::error::{WireError, WireResult};

#[derive(Debug, Clone)]
enum Condition {
    Eq(Value),
    Ne(Value),
    Gt(Value),
    Gte(Value),
    Lt(Value),
    Lte(Value),
}

#[derive(Debug, Clone)]
struct FieldMatch {
    path: Vec<String>,
    cond: Condition,
}

#[derive(Debug, Clone)]
pub struct Filter {
    conjuncts: Vec<FieldMatch>,
}

impl Filter {
    pub fn parse(doc: &BsonDoc) -> WireResult<Self> {
        let mut conjuncts = Vec::new();
        for (k, v) in doc.iter() {
            if k.starts_with('$') {
                return Err(WireError::Protocol(format!(
                    "top-level operator {k} not supported in M3 filter"
                )));
            }
            let path: Vec<String> = k.split('.').map(|s| s.to_string()).collect();
            match v {
                bson::Bson::Document(inner) => {
                    // operator doc: { $op: value, ... }
                    for (op, opval) in inner.iter() {
                        let cond = match op.as_str() {
                            "$eq" => Condition::Eq(bson_to_json(opval.clone())),
                            "$ne" => Condition::Ne(bson_to_json(opval.clone())),
                            "$gt" => Condition::Gt(bson_to_json(opval.clone())),
                            "$gte" => Condition::Gte(bson_to_json(opval.clone())),
                            "$lt" => Condition::Lt(bson_to_json(opval.clone())),
                            "$lte" => Condition::Lte(bson_to_json(opval.clone())),
                            other => {
                                return Err(WireError::Protocol(format!(
                                    "filter operator {other} not supported in M3"
                                )))
                            }
                        };
                        conjuncts.push(FieldMatch {
                            path: path.clone(),
                            cond,
                        });
                    }
                }
                other => conjuncts.push(FieldMatch {
                    path,
                    cond: Condition::Eq(bson_to_json(other.clone())),
                }),
            }
        }
        Ok(Filter { conjuncts })
    }

    pub fn matches(&self, value: &Value) -> bool {
        self.conjuncts.iter().all(|fm| match walk(value, &fm.path) {
            Some(got) => eval(&fm.cond, got),
            None => matches!(fm.cond, Condition::Ne(_)), // missing field only matches $ne
        })
    }

    /// Returns the top-level equality value for a known field, if present.
    /// Used by the planner to detect session-scoped queries.
    pub fn top_level_eq(&self, field: &str) -> Option<&Value> {
        self.conjuncts.iter().find_map(|fm| {
            if fm.path.len() == 1 && fm.path[0] == field {
                match &fm.cond {
                    Condition::Eq(v) => Some(v),
                    _ => None,
                }
            } else {
                None
            }
        })
    }
}

fn walk<'a>(root: &'a Value, path: &[String]) -> Option<&'a Value> {
    let mut cur = root;
    for segment in path {
        cur = cur.get(segment)?;
    }
    Some(cur)
}

fn eval(cond: &Condition, got: &Value) -> bool {
    match cond {
        Condition::Eq(v) => got == v,
        Condition::Ne(v) => got != v,
        Condition::Gt(v) => cmp(got, v).map(|o| o == std::cmp::Ordering::Greater).unwrap_or(false),
        Condition::Gte(v) => cmp(got, v).map(|o| o != std::cmp::Ordering::Less).unwrap_or(false),
        Condition::Lt(v) => cmp(got, v).map(|o| o == std::cmp::Ordering::Less).unwrap_or(false),
        Condition::Lte(v) => cmp(got, v).map(|o| o != std::cmp::Ordering::Greater).unwrap_or(false),
    }
}

fn cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            let xf = x.as_f64()?;
            let yf = y.as_f64()?;
            xf.partial_cmp(&yf)
        }
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use serde_json::json;

    #[test]
    fn equality_matches_top_level() {
        let f = Filter::parse(&doc! { "session_id": "s-1" }).unwrap();
        assert!(f.matches(&json!({"session_id": "s-1", "seq": 1})));
        assert!(!f.matches(&json!({"session_id": "s-2"})));
    }

    #[test]
    fn nested_path_matches() {
        let f = Filter::parse(&doc! { "payload.kind": "message" }).unwrap();
        assert!(f.matches(&json!({"payload": {"kind": "message"}})));
        assert!(!f.matches(&json!({"payload": {"kind": "tool_call"}})));
    }

    #[test]
    fn gt_operator_matches_numbers() {
        let f = Filter::parse(&doc! { "seq": { "$gt": 3 } }).unwrap();
        assert!(f.matches(&json!({"seq": 4})));
        assert!(!f.matches(&json!({"seq": 3})));
        assert!(!f.matches(&json!({"seq": 2})));
    }

    #[test]
    fn conjunction_across_fields() {
        let f = Filter::parse(&doc! { "session_id": "s-1", "seq": { "$gte": 2 } }).unwrap();
        assert!(f.matches(&json!({"session_id": "s-1", "seq": 2})));
        assert!(f.matches(&json!({"session_id": "s-1", "seq": 9})));
        assert!(!f.matches(&json!({"session_id": "s-1", "seq": 1})));
        assert!(!f.matches(&json!({"session_id": "s-2", "seq": 9})));
    }

    #[test]
    fn unsupported_operator_errors_out() {
        let err = Filter::parse(&doc! { "field": { "$regex": ".*" } }).unwrap_err();
        assert!(err.to_string().contains("$regex"));
    }

    #[test]
    fn top_level_eq_returns_scoped_value() {
        let f = Filter::parse(&doc! { "session_id": "s-9", "seq": { "$gt": 1 } }).unwrap();
        assert_eq!(f.top_level_eq("session_id"), Some(&json!("s-9")));
        assert!(f.top_level_eq("seq").is_none()); // seq has operator, not plain eq
    }
}
```

- [ ] **Step 2: Update handlers/mod.rs**

Edit `crates/agentdb-wire/src/handlers/mod.rs` — add `pub mod filter;` line.

- [ ] **Step 3: Run tests**

Run: `cargo test -p agentdb-wire handlers::filter -- --nocapture`
Expected: 6 passed.

- [ ] **Step 4: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-wire/src/handlers
git commit -m "feat(wire): minimal BSON filter parser with dotted paths and comparison ops"
```

---

## Task 8: `find` handler

**Files:**
- Create: `crates/agentdb-wire/src/handlers/find.rs`
- Modify: `crates/agentdb-wire/src/handlers/mod.rs`
- Modify: `crates/agentdb-core/src/registry.rs` (new helper)

**Context:** The `find` handler scans events for a given tenant (the Mongo `db` name) and applies the filter. For M3:
- Collection `events` → scan all sessions' events under the tenant
- Collection `sessions` → enumerate session records
- Returns all matches in the first batch, `cursor.id = 0` (no continuation)
- Honors `limit` if provided
- `top_level_eq("session_id")` enables a session-scoped scan (prefix lookup); otherwise tenant-wide scan

To keep the scope tight, add a helper `TenantRegistry::list_sessions_in_tenant(box, tenant)` that returns the list of internal session ids (needed for tenant-wide scans across sessions).

Actually — a simpler approach: add a helper on `SessionRegistry` that returns the list of sessions for a given external tenant name, and a helper on `agentdb_core` that scans events for a given session. Let's implement these as thin registry additions and a scan helper in primitives.

- [ ] **Step 1: Add `SessionRegistry::list_in_tenant` with test**

In `crates/agentdb-core/src/registry.rs`, append to the existing `mod tests`:
```rust
    #[test]
    fn session_list_in_tenant_returns_all_externals() {
        let (_dir, kv) = store();
        let reg = SessionRegistry::new(kv);
        reg.get_or_mint(0, 10, "s-a", 0).unwrap();
        reg.get_or_mint(0, 10, "s-b", 0).unwrap();
        reg.get_or_mint(0, 11, "elsewhere", 0).unwrap();
        let mut got = reg.list_in_tenant(0, 10).unwrap();
        got.sort();
        assert_eq!(got, vec!["s-a".to_string(), "s-b".to_string()]);
    }
```

Also add to `meta.rs` alongside the existing prefix helper:
```rust
pub(crate) fn prefix_session_by_external_for_tenant(
    box_id: BoxId,
    tenant_id: TenantId,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    out.push(NS_META);
    out.push(TAG_SESSION_BY_EXT);
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out
}
```

Implement `list_in_tenant` inside `impl SessionRegistry`:
```rust
    pub fn list_in_tenant(
        &self,
        box_id: BoxId,
        tenant_id: TenantId,
    ) -> Result<Vec<String>> {
        let prefix = crate::meta::prefix_session_by_external_for_tenant(box_id, tenant_id);
        let mut out = Vec::new();
        for res in self.kv.scan_prefix(&prefix)? {
            let (k, _v) = res?;
            if k.len() <= 12 {
                continue;
            }
            let name = std::str::from_utf8(&k[12..])
                .map_err(|e| crate::error::Error::Invalid(format!("bad session name utf-8: {e}")))?
                .to_string();
            out.push(name);
        }
        Ok(out)
    }
```

Run: `cargo test -p agentdb-core registry::tests::session_list_in_tenant -- --nocapture`
Expected: 1 passed.

- [ ] **Step 2: Add `scan_session_events` helper in primitives.rs**

After `fetch_last_n` in `crates/agentdb-core/src/primitives.rs`:
```rust
/// Scan all events of a session in seq order (no bound). Used by wire-protocol
/// `find`. Streams through RocksDB's prefix iterator; eager-collect because the
/// caller typically applies a filter and limit.
pub fn scan_session_events(
    kv: &Arc<dyn KvStore>,
    session: &Session,
) -> Result<Vec<Event>> {
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
    Ok(all)
}
```

Add `pub use primitives::scan_session_events;` to `crates/agentdb-core/src/lib.rs` primitives re-export, producing:
```rust
pub use primitives::{
    append_and_fetch, append_event, fetch_last_n, scan_session_events, AppendFetchOutput,
    DEFAULT_BRANCH_ID,
};
```

Run: `cargo check --workspace` — expect clean.

- [ ] **Step 3: Write handlers/find.rs with tests**

Save to `crates/agentdb-wire/src/handlers/find.rs`:
```rust
use std::sync::Arc;

use agentdb_core::{scan_session_events, BoxRegistry, Event, SessionRegistry, TenantRegistry};
use bson::{doc, Bson, Document};
use serde_json::Value;

use crate::bsonx::json_to_bson;
use crate::error::{WireError, WireResult};
use crate::handlers::filter::Filter;

pub fn find_response(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    db: &str,
    collection: &str,
    filter_doc: &Document,
    limit: Option<i64>,
) -> WireResult<Document> {
    let filter = Filter::parse(filter_doc)?;
    let tenant = match tenants.lookup(routing_box(registry, db), db)? {
        Some(t) => t,
        None => return Ok(empty_batch(db, collection)),
    };
    let mut batch: Vec<Value> = Vec::new();
    let cap = limit.unwrap_or(i64::MAX).max(0) as usize;

    match collection {
        "events" => {
            let session_externals = sessions.list_in_tenant(tenant.box_id, tenant.internal_id)?;
            // If the filter targets a specific session, only scan that one.
            let scoped: Vec<String> = match filter.top_level_eq("session_id") {
                Some(Value::String(s)) => vec![s.clone()],
                _ => session_externals,
            };
            for s_ext in scoped {
                let session = match sessions.lookup(tenant.box_id, tenant.internal_id, &s_ext)? {
                    Some(s) => s,
                    None => continue,
                };
                for ev in scan_session_events(registry.kv(), &session)? {
                    let row = project_event(&session.external_id, ev);
                    if filter.matches(&row) {
                        batch.push(row);
                        if batch.len() >= cap {
                            break;
                        }
                    }
                }
                if batch.len() >= cap {
                    break;
                }
            }
        }
        "sessions" => {
            let session_externals = sessions.list_in_tenant(tenant.box_id, tenant.internal_id)?;
            for s_ext in session_externals {
                let session = match sessions.lookup(tenant.box_id, tenant.internal_id, &s_ext)? {
                    Some(s) => s,
                    None => continue,
                };
                let row = serde_json::to_value(&session)?;
                if filter.matches(&row) {
                    batch.push(row);
                    if batch.len() >= cap {
                        break;
                    }
                }
            }
        }
        other => {
            return Err(WireError::Protocol(format!(
                "unknown collection {other}; only 'events' and 'sessions' exist in M3"
            )));
        }
    }

    let first_batch: Vec<Bson> = batch.into_iter().map(json_to_bson).collect();
    Ok(doc! {
        "cursor": {
            "id": 0_i64,
            "ns": format!("{}.{}", db, collection),
            "firstBatch": Bson::Array(first_batch),
        },
        "ok": 1.0,
    })
}

fn empty_batch(db: &str, collection: &str) -> Document {
    doc! {
        "cursor": {
            "id": 0_i64,
            "ns": format!("{}.{}", db, collection),
            "firstBatch": Bson::Array(vec![]),
        },
        "ok": 1.0,
    }
}

fn project_event(session_id: &str, ev: Event) -> Value {
    serde_json::json!({
        "session_id": session_id,
        "seq": ev.seq,
        "ts": ev.ts,
        "payload": ev.payload,
    })
}

fn routing_box(registry: &BoxRegistry, tenant_external: &str) -> agentdb_storage::BoxId {
    registry.route_tenant(tenant_external).id
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_storage::{KvStore, RocksKvStore};
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn setup_with_events() -> (TempDir, Arc<BoxRegistry>, Arc<TenantRegistry>, Arc<SessionRegistry>) {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let registry = Arc::new(BoxRegistry::new(kv.clone(), 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        for seq in 1..=3u64 {
            agentdb_core::append_and_fetch(
                &registry,
                &tenants,
                &sessions,
                "t-1",
                "s-1",
                json!({"kind": "msg", "i": seq}),
                0,
                seq,
            )
            .await
            .unwrap();
        }
        (dir, registry, tenants, sessions)
    }

    #[tokio::test]
    async fn find_events_returns_all_for_tenant() {
        let (_dir, registry, tenants, sessions) = setup_with_events().await;
        let resp = find_response(&registry, &tenants, &sessions, "t-1", "events", &doc! {}, None).unwrap();
        let batch = resp.get_document("cursor").unwrap().get_array("firstBatch").unwrap();
        assert_eq!(batch.len(), 3);
    }

    #[tokio::test]
    async fn find_events_with_session_id_filter_scans_only_that_session() {
        let (_dir, registry, tenants, sessions) = setup_with_events().await;
        // create a second session
        for _ in 0..2 {
            agentdb_core::append_and_fetch(
                &registry,
                &tenants,
                &sessions,
                "t-1",
                "s-2",
                json!({"kind": "other"}),
                0,
                0,
            )
            .await
            .unwrap();
        }
        let resp = find_response(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "events",
            &doc! { "session_id": "s-1" },
            None,
        )
        .unwrap();
        let batch = resp.get_document("cursor").unwrap().get_array("firstBatch").unwrap();
        assert_eq!(batch.len(), 3);
    }

    #[tokio::test]
    async fn find_respects_limit() {
        let (_dir, registry, tenants, sessions) = setup_with_events().await;
        let resp = find_response(&registry, &tenants, &sessions, "t-1", "events", &doc! {}, Some(2)).unwrap();
        let batch = resp.get_document("cursor").unwrap().get_array("firstBatch").unwrap();
        assert_eq!(batch.len(), 2);
    }

    #[tokio::test]
    async fn find_unknown_tenant_returns_empty() {
        let (_dir, registry, tenants, sessions) = setup_with_events().await;
        let resp = find_response(&registry, &tenants, &sessions, "nope", "events", &doc! {}, None).unwrap();
        let batch = resp.get_document("cursor").unwrap().get_array("firstBatch").unwrap();
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn find_on_sessions_collection_returns_session_records() {
        let (_dir, registry, tenants, sessions) = setup_with_events().await;
        let resp = find_response(&registry, &tenants, &sessions, "t-1", "sessions", &doc! {}, None).unwrap();
        let batch = resp.get_document("cursor").unwrap().get_array("firstBatch").unwrap();
        assert_eq!(batch.len(), 1);
        let rec = batch[0].as_document().unwrap();
        assert_eq!(rec.get_str("external_id").unwrap(), "s-1");
    }
}
```

Add `pub mod find;` to `crates/agentdb-wire/src/handlers/mod.rs`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p agentdb-wire handlers::find -- --nocapture`
Expected: 5 passed.

- [ ] **Step 5: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core/src/meta.rs crates/agentdb-core/src/primitives.rs crates/agentdb-core/src/registry.rs crates/agentdb-core/src/lib.rs crates/agentdb-wire/src/handlers
git commit -m "feat(wire): find handler with session-scoped and tenant-wide scans"
```

---

## Task 9: `insert` handler

**Files:**
- Create: `crates/agentdb-wire/src/handlers/insert.rs`
- Modify: `crates/agentdb-wire/src/handlers/mod.rs`

**Context:** `insert` on collection `events` maps each document to an `append_and_fetch` call. The document must include `session_id`; the rest becomes the event payload. `insert` on collection `sessions` is not supported in M3 (sessions are auto-created). Response includes `n` (count inserted) and potentially `writeErrors`.

- [ ] **Step 1: Write handlers/insert.rs with tests**

Save to `crates/agentdb-wire/src/handlers/insert.rs`:
```rust
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use bson::{doc, Bson, Document};

use crate::bsonx::bson_to_json;
use crate::error::{WireError, WireResult};

pub async fn insert_response(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    db: &str,
    collection: &str,
    docs: &[Document],
) -> WireResult<Document> {
    if collection != "events" {
        return Err(WireError::Protocol(format!(
            "insert into {collection} not supported in M3; only 'events' accepts writes"
        )));
    }

    let mut inserted = 0i32;
    let mut write_errors: Vec<Bson> = Vec::new();
    for (idx, d) in docs.iter().enumerate() {
        let Some(session_id) = d.get_str("session_id").ok() else {
            write_errors.push(bson::Bson::Document(doc! {
                "index": idx as i32,
                "code": 2_i32, // BadValue
                "errmsg": "document is missing required field 'session_id'",
            }));
            continue;
        };

        let mut payload_doc = d.clone();
        payload_doc.remove("session_id");
        let payload = bson_to_json(Bson::Document(payload_doc));

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        match agentdb_core::append_and_fetch(
            registry, tenants, sessions, db, session_id, payload, 0, now,
        )
        .await
        {
            Ok(_) => inserted += 1,
            Err(e) => write_errors.push(bson::Bson::Document(doc! {
                "index": idx as i32,
                "code": 11_i32, // UserNotFound / generic
                "errmsg": e.to_string(),
            })),
        }
    }

    let mut resp = doc! {
        "n": inserted,
        "ok": 1.0,
    };
    if !write_errors.is_empty() {
        resp.insert("writeErrors", Bson::Array(write_errors));
    }
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_storage::{KvStore, RocksKvStore};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn context() -> (
        TempDir,
        Arc<BoxRegistry>,
        Arc<TenantRegistry>,
        Arc<SessionRegistry>,
    ) {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let registry = Arc::new(BoxRegistry::new(kv.clone(), 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        (dir, registry, tenants, sessions)
    }

    #[tokio::test]
    async fn insert_one_event_succeeds_and_n_is_one() {
        let (_d, r, t, s) = context();
        let resp = insert_response(
            &r,
            &t,
            &s,
            "t-1",
            "events",
            &[doc! { "session_id": "s-1", "kind": "hello" }],
        )
        .await
        .unwrap();
        assert_eq!(resp.get_i32("n").unwrap(), 1);
        assert_eq!(resp.get_f64("ok").unwrap(), 1.0);
    }

    #[tokio::test]
    async fn insert_document_missing_session_id_reports_write_error() {
        let (_d, r, t, s) = context();
        let resp = insert_response(
            &r,
            &t,
            &s,
            "t-1",
            "events",
            &[doc! { "kind": "no-session-id-here" }],
        )
        .await
        .unwrap();
        assert_eq!(resp.get_i32("n").unwrap(), 0);
        let errs = resp.get_array("writeErrors").unwrap();
        assert_eq!(errs.len(), 1);
        assert!(errs[0]
            .as_document()
            .unwrap()
            .get_str("errmsg")
            .unwrap()
            .contains("session_id"));
    }

    #[tokio::test]
    async fn insert_into_unknown_collection_errors_out() {
        let (_d, r, t, s) = context();
        let err = insert_response(&r, &t, &s, "t-1", "widgets", &[doc! {}])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("widgets"));
    }
}
```

Add `pub mod insert;` to `crates/agentdb-wire/src/handlers/mod.rs`.

- [ ] **Step 2: Run tests**

Run: `cargo test -p agentdb-wire handlers::insert -- --nocapture`
Expected: 3 passed.

- [ ] **Step 3: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-wire/src/handlers
git commit -m "feat(wire): insert handler delegates to append_and_fetch"
```

---

## Task 10: Command dispatch function

**Files:**
- Modify: `crates/agentdb-wire/src/command.rs`
- Modify: `crates/agentdb-wire/src/lib.rs`

**Context:** Compose the handlers into a single `dispatch(body, state)` async function that pattern-matches on command name and routes to the right handler. Unknown commands return `not_implemented`.

- [ ] **Step 1: Add `WireState` type alias and dispatch**

Create a new module `state.rs`:
Save to `crates/agentdb-wire/src/state.rs`:
```rust
use std::sync::Arc;

use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};

#[derive(Clone)]
pub struct WireState {
    pub registry: Arc<BoxRegistry>,
    pub tenants: Arc<TenantRegistry>,
    pub sessions: Arc<SessionRegistry>,
}
```

In `crates/agentdb-wire/src/lib.rs`, add `pub mod state;` and `pub use state::WireState;`.

- [ ] **Step 2: Extend command.rs with `dispatch`**

Append to `crates/agentdb-wire/src/command.rs`:
```rust
use bson::Bson;

use crate::handlers::{browse, find as find_mod, hello as hello_mod, insert as insert_mod};
use crate::state::WireState;

pub async fn dispatch(body: &Document, state: &WireState, connection_id: i64) -> Document {
    match inspect(body) {
        Err(e) => error_reply(&e),
        Ok(head) => match head.name {
            "hello" | "isMaster" | "ismaster" => hello_mod::hello_response(connection_id),
            "ping" => browse::ping_response(),
            "buildInfo" | "buildinfo" => browse::build_info_response(),
            "listDatabases" => match browse::list_databases_response(&state.registry, &state.tenants) {
                Ok(d) => d,
                Err(e) => error_reply(&e),
            },
            "listCollections" => browse::list_collections_response(head.db.unwrap_or("")),
            "find" => {
                let collection = match body.get_str("find") {
                    Ok(s) => s.to_string(),
                    Err(_) => return error_reply(&crate::error::WireError::Protocol(
                        "find command missing collection name".into(),
                    )),
                };
                let filter = body
                    .get_document("filter")
                    .cloned()
                    .unwrap_or_default();
                let limit = body.get_i64("limit").ok();
                match find_mod::find_response(
                    &state.registry,
                    &state.tenants,
                    &state.sessions,
                    head.db.unwrap_or(""),
                    &collection,
                    &filter,
                    limit,
                ) {
                    Ok(d) => d,
                    Err(e) => error_reply(&e),
                }
            }
            "insert" => {
                let collection = match body.get_str("insert") {
                    Ok(s) => s.to_string(),
                    Err(_) => return error_reply(&crate::error::WireError::Protocol(
                        "insert command missing collection name".into(),
                    )),
                };
                let docs_bson = body
                    .get_array("documents")
                    .cloned()
                    .unwrap_or_default();
                let docs: Vec<Document> = docs_bson
                    .into_iter()
                    .filter_map(|b| match b {
                        Bson::Document(d) => Some(d),
                        _ => None,
                    })
                    .collect();
                match insert_mod::insert_response(
                    &state.registry,
                    &state.tenants,
                    &state.sessions,
                    head.db.unwrap_or(""),
                    &collection,
                    &docs,
                )
                .await
                {
                    Ok(d) => d,
                    Err(e) => error_reply(&e),
                }
            }
            other => not_implemented(other),
        },
    }
}
```

- [ ] **Step 2b: Extend dispatch test coverage (optional but recommended)**

Append inside `#[cfg(test)] mod tests` in `command.rs`:
```rust
    use crate::state::WireState;
    use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
    use agentdb_storage::{KvStore, RocksKvStore};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn state() -> (TempDir, WireState) {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let registry = Arc::new(BoxRegistry::new(kv.clone(), 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        (
            dir,
            WireState {
                registry,
                tenants,
                sessions,
            },
        )
    }

    #[tokio::test]
    async fn dispatch_hello_returns_ok_one() {
        let (_d, st) = state();
        let r = dispatch(&doc! { "hello": 1 }, &st, 7).await;
        assert_eq!(r.get_f64("ok").unwrap(), 1.0);
        assert_eq!(r.get_i64("connectionId").unwrap(), 7);
    }

    #[tokio::test]
    async fn dispatch_unknown_command_returns_not_implemented() {
        let (_d, st) = state();
        let r = dispatch(&doc! { "killCursors": 1 }, &st, 1).await;
        assert_eq!(r.get_f64("ok").unwrap(), 0.0);
        assert!(r.get_str("errmsg").unwrap().contains("killCursors"));
    }
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p agentdb-wire -- --nocapture`
Expected: all wire tests pass (previous total + 2 new dispatch tests).

- [ ] **Step 4: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-wire/src
git commit -m "feat(wire): command dispatch with WireState context"
```

---

## Task 11: TCP server (`serve`) — per-connection loop

**Files:**
- Modify: `crates/agentdb-wire/src/server.rs`

**Context:** Reads OP_MSG frames off a TCP connection, dispatches, writes replies. Uses `tokio::net::TcpStream` + `tokio::io::AsyncReadExt` / `AsyncWriteExt`. Frame reading:
1. Read 4 bytes → parse `messageLength` as i32 LE
2. Read remaining `messageLength - 4` bytes
3. Parse the full buffer via `opmsg::decode`
4. Extract body section; dispatch
5. Encode response via `opmsg::encode_body_response`
6. Write response bytes

Connection ID: a monotonic `AtomicI64` per `serve` call.

- [ ] **Step 1: Write server.rs**

Replace `crates/agentdb-wire/src/server.rs` with:
```rust
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use byteorder::{LittleEndian, ReadBytesExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::command::dispatch;
use crate::error::{WireError, WireResult};
use crate::opmsg;
use crate::state::WireState;

/// Accept connections and spawn per-connection tasks until the listener is dropped.
pub async fn serve(listener: TcpListener, state: WireState) -> WireResult<()> {
    let next_id = Arc::new(AtomicI64::new(1));
    loop {
        let (socket, peer) = listener.accept().await?;
        let st = state.clone();
        let id = next_id.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            if let Err(err) = run_connection(socket, st, id).await {
                tracing::debug!(%peer, ?err, "wire connection closed with error");
            }
        });
    }
}

async fn run_connection(
    mut socket: TcpStream,
    state: WireState,
    connection_id: i64,
) -> WireResult<()> {
    loop {
        let mut len_buf = [0u8; 4];
        if let Err(err) = socket.read_exact(&mut len_buf).await {
            if err.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(()); // peer closed cleanly
            }
            return Err(err.into());
        }
        let msg_len = (&len_buf[..]).read_i32::<LittleEndian>()? as usize;
        if msg_len < opmsg::HEADER_LEN {
            return Err(WireError::Protocol(format!("short frame: {msg_len}")));
        }
        let mut buf = vec![0u8; msg_len];
        buf[..4].copy_from_slice(&len_buf);
        socket.read_exact(&mut buf[4..]).await?;

        let msg = opmsg::decode(&buf)?;
        let body = msg
            .body()
            .ok_or_else(|| WireError::Protocol("request has no body section".into()))?;
        let response_doc = dispatch(body, &state, connection_id).await;
        let mut reply = Vec::with_capacity(64 + response_doc.len() * 16);
        opmsg::encode_body_response(
            &mut reply,
            msg.header.request_id,
            msg.header.request_id,
            &response_doc,
        )?;
        socket.write_all(&reply).await?;
        socket.flush().await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn spawn_server() -> (String, WireState) {
        use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
        use agentdb_storage::{KvStore, RocksKvStore};
        use std::sync::Arc;
        let dir = tempfile::TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let registry = Arc::new(BoxRegistry::new(kv.clone(), 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        let state = WireState { registry, tenants, sessions };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let st = state.clone();
        tokio::spawn(async move {
            let _ = serve(listener, st).await;
        });
        // Leak the tempdir so it lives past this fn — real integration tests
        // use a separate fixture.
        std::mem::forget(dir);
        (format!("127.0.0.1:{}", addr.port()), state)
    }

    #[tokio::test]
    async fn raw_hello_round_trip() {
        let (addr, _state) = spawn_server().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let mut req_bytes = Vec::new();
        opmsg::encode_body_response(&mut req_bytes, 1, 0, &doc! { "hello": 1, "$db": "admin" })
            .unwrap();
        stream.write_all(&req_bytes).await.unwrap();

        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let total = (&len_buf[..]).read_i32::<LittleEndian>().unwrap() as usize;
        let mut rest = vec![0u8; total - 4];
        stream.read_exact(&mut rest).await.unwrap();
        let mut whole = Vec::with_capacity(total);
        whole.extend_from_slice(&len_buf);
        whole.extend_from_slice(&rest);
        let msg = opmsg::decode(&whole).unwrap();
        let body = msg.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
        assert!(body.get_bool("isWritablePrimary").unwrap());
    }
}
```

- [ ] **Step 2: Run test**

Run: `cargo test -p agentdb-wire server -- --nocapture`
Expected: 1 passed.

- [ ] **Step 3: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-wire/src/server.rs crates/agentdb-wire/src/lib.rs crates/agentdb-wire/src/state.rs
git commit -m "feat(wire): TCP serve loop with per-connection dispatch"
```

---

## Task 12: Wire the Mongo listener into `agentdb-server`

**Files:**
- Modify: `bin/agentdb-server/Cargo.toml`
- Modify: `bin/agentdb-server/src/main.rs`

**Context:** The binary now binds two listeners: the existing HTTP listener and a new Mongo listener. Default Mongo port 7879 (configurable via `AGENTDB_MONGO_ADDR`). Uses `tokio::select!` (or spawns both) so either listener error terminates the process.

- [ ] **Step 1: Add dep**

Edit `bin/agentdb-server/Cargo.toml` — add `agentdb-wire = { path = "../../crates/agentdb-wire" }` in `[dependencies]`.

- [ ] **Step 2: Rewire main**

Replace `bin/agentdb-server/src/main.rs` with:
```rust
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use agentdb_api::{router, AppState};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use agentdb_wire::{serve as serve_mongo, WireState};
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
    let num_boxes: u16 = match std::env::var("AGENTDB_BOXES") {
        Ok(v) => v.parse().context("AGENTDB_BOXES must be a valid u16")?,
        Err(_) => 4,
    };
    let http_addr: SocketAddr = match std::env::var("AGENTDB_ADDR") {
        Ok(v) => v
            .parse()
            .context("AGENTDB_ADDR must be a valid socket address")?,
        Err(_) => "127.0.0.1:7878".parse().unwrap(),
    };
    let mongo_addr: SocketAddr = match std::env::var("AGENTDB_MONGO_ADDR") {
        Ok(v) => v
            .parse()
            .context("AGENTDB_MONGO_ADDR must be a valid socket address")?,
        Err(_) => "127.0.0.1:7879".parse().unwrap(),
    };

    std::fs::create_dir_all(&data_dir).context("create data dir")?;
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(&data_dir).context("open rocksdb")?);
    let registry = Arc::new(BoxRegistry::new(kv.clone(), num_boxes)?);
    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv));

    let http_state = AppState {
        registry: registry.clone(),
        tenants: tenants.clone(),
        sessions: sessions.clone(),
    };
    let wire_state = WireState {
        registry,
        tenants,
        sessions,
    };

    let app = router(http_state);
    let http_listener = tokio::net::TcpListener::bind(http_addr)
        .await
        .context("bind http listener")?;
    let mongo_listener = tokio::net::TcpListener::bind(mongo_addr)
        .await
        .context("bind mongo listener")?;
    tracing::info!(%http_addr, %mongo_addr, "agentdb-server listening");

    let http_task = tokio::spawn(async move {
        axum::serve(http_listener, app).await.context("http serve")
    });
    let mongo_task = tokio::spawn(async move {
        serve_mongo(mongo_listener, wire_state)
            .await
            .map_err(|e| anyhow::anyhow!("mongo serve: {e}"))
    });

    tokio::select! {
        r = http_task => r.context("http task join")??,
        r = mongo_task => r.context("mongo task join")??,
    };
    Ok(())
}
```

- [ ] **Step 3: Build check**

Run: `cargo build -p agentdb-server`
Expected: clean build.

- [ ] **Step 4: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add bin/agentdb-server
git commit -m "feat(server): bind both HTTP and MongoDB wire listeners"
```

---

## Task 13: End-to-end integration test via the `mongodb` Rust driver

**Files:**
- Create: `crates/agentdb-wire/tests/driver.rs`

**Context:** Spawns the wire server on an ephemeral port with a temp RocksDB, connects with the real `mongodb::Client`, and exercises `listDatabases`, `find`, `insert`, and `hello` round-trips. The driver needs a `directConnection=true` URI option so it doesn't try to discover a replica set.

- [ ] **Step 1: Write the integration test**

Save to `crates/agentdb-wire/tests/driver.rs`:
```rust
use std::sync::Arc;

use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use agentdb_wire::{serve, WireState};
use bson::doc;
use mongodb::options::ClientOptions;
use mongodb::Client;
use tempfile::TempDir;

async fn spawn() -> (TempDir, u16) {
    let dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let registry = Arc::new(BoxRegistry::new(kv.clone(), 4).unwrap());
    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv));
    let state = WireState {
        registry,
        tenants,
        sessions,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = serve(listener, state).await;
    });
    (dir, port)
}

async fn client(port: u16) -> Client {
    let uri = format!("mongodb://127.0.0.1:{port}/?directConnection=true");
    let mut options = ClientOptions::parse(&uri).await.unwrap();
    // Avoid auth handshake
    options.credential = None;
    Client::with_options(options).unwrap()
}

#[tokio::test]
async fn hello_over_real_driver() {
    let (_dir, port) = spawn().await;
    let cli = client(port).await;
    let admin = cli.database("admin");
    let reply = admin.run_command(doc! { "ping": 1 }, None).await.unwrap();
    assert_eq!(reply.get_f64("ok").unwrap(), 1.0);
}

#[tokio::test]
async fn insert_then_find_round_trip() {
    let (_dir, port) = spawn().await;
    let cli = client(port).await;
    let db = cli.database("t-42");
    let events = db.collection::<bson::Document>("events");

    events
        .insert_one(doc! { "session_id": "s-1", "kind": "hello" }, None)
        .await
        .unwrap();
    events
        .insert_one(doc! { "session_id": "s-1", "kind": "world" }, None)
        .await
        .unwrap();

    let cursor = events
        .find(doc! { "session_id": "s-1" }, None)
        .await
        .unwrap();
    use futures_util::StreamExt;
    let docs: Vec<bson::Document> = cursor.filter_map(|r| async move { r.ok() }).collect().await;
    assert_eq!(docs.len(), 2);
    let kinds: Vec<&str> = docs.iter().map(|d| {
        d.get_document("payload")
            .unwrap()
            .get_str("kind")
            .unwrap()
    }).collect();
    assert!(kinds.contains(&"hello"));
    assert!(kinds.contains(&"world"));
}

#[tokio::test]
async fn list_databases_sees_tenant_after_write() {
    let (_dir, port) = spawn().await;
    let cli = client(port).await;
    let events = cli.database("t-abc").collection::<bson::Document>("events");
    events
        .insert_one(doc! { "session_id": "s-1", "x": 1 }, None)
        .await
        .unwrap();

    let dbs = cli.list_database_names(None, None).await.unwrap();
    assert!(dbs.contains(&"t-abc".to_string()));
}
```

Note: this test pulls in `futures-util` transitively via `mongodb`. Add it as a direct dev-dep in `crates/agentdb-wire/Cargo.toml` under `[dev-dependencies]`:
```toml
futures-util = "0.3"
```
And `mongodb` itself to workspace deps (already done in T1) with `tokio-runtime` feature.

- [ ] **Step 2: Run the integration tests**

Run: `cargo test -p agentdb-wire --test driver -- --nocapture`
Expected: 3 passed.

If the `mongodb` driver fails at handshake (e.g., "no servers available" / "authentication required"), check the `hello` response shape. Common issues:
- `maxWireVersion` too low → raise to 17 or higher (we set 17)
- Driver tries SASL despite `credential: None` → ensure `saslSupportedMechs: []` is in hello response
- Driver times out waiting for server selection → ensure `isWritablePrimary: true` and `ok: 1.0` are in the response

- [ ] **Step 3: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-wire/Cargo.toml crates/agentdb-wire/tests/driver.rs
git commit -m "test(wire): end-to-end mongodb driver round trip for hello/insert/find/listDbs"
```

---

## Task 14: Milestone exit gate

**Files:** none beyond re-running checks.

- [ ] **Step 1: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: all tests pass.

Counts to confirm:
- agentdb-storage: 17 unit + 11 integration
- agentdb-core: ≥ 35 (M2 had 33, T6+T8 add ~2-3)
- agentdb-api: 2 integration
- agentdb-wire unit: ~20-25 (error 2, opmsg 3, bsonx 4, command 4 + 2 dispatch, handlers/hello 2 + browse 3 + filter 6 + find 5 + insert 3, server 1)
- agentdb-wire integration: 3

- [ ] **Step 2: Clippy gate**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings. Fix any in code you wrote during M3.

- [ ] **Step 3: Fmt gate**

Run: `cargo fmt --all --check`
Expected: clean.

- [ ] **Step 4: Commit if anything changed from fmt**

If fmt made changes, commit them as:
```bash
git add -A
git commit -m "chore: cargo fmt"
```

Otherwise nothing to commit.

## Milestone exit criteria

`cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` returns exit 0.

Manual demo check (optional):
```bash
cargo run -p agentdb-server &
mongosh "mongodb://127.0.0.1:7879/?directConnection=true"
> use t-demo
> db.events.insertOne({session_id: "s-1", text: "hi"})
> db.events.find({})
> show dbs
```

## Known limitations to carry into M4

- No cursors: `find` returns all matching docs in the first batch. Large result sets eat memory.
- No `update` / `delete` — event stream is append-only, sessions are managed via REST.
- No `aggregate`, no `$vectorSearch`, no `$search` — these are the M4 headliners.
- `_id` fields in inserted events are ignored; seq is the canonical identifier.
- BSON extended types (ObjectId, Dates, Decimal128) are stringified on the JSON boundary.
- Auth is effectively disabled. Any client can connect.
