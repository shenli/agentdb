# M11 SlateDB Spike — API Decision Record

**Date:** 2026-04-19
**Status:** DONE
**Crate pin:** `slatedb = "0.12.1"`
**object_store pin:** `object_store = "0.12"` (must bump from workspace's current `"0.11"` — see "Notable surprises" below)

## Confirmed API shape

All core methods are native on `Db` and fully async. Key/value arguments take `impl AsRef<[u8]>` on reads/writes, and `Db::get` returns `Bytes` (not owned `Vec<u8>`).

| operation      | slatedb call                                                                                            | sync/async |
|----------------|---------------------------------------------------------------------------------------------------------|------------|
| open           | `Db::open(path: impl Into<Path>, object_store: Arc<dyn ObjectStore>) -> Result<Self, Error>`            | async      |
| open (custom)  | `Db::builder(path, object_store) -> DbBuilder` then `.build().await`                                    | async      |
| get(key)       | `db.get<K: AsRef<[u8]> + Send>(key) -> Result<Option<Bytes>, Error>`                                    | async      |
| put(key, val)  | `db.put<K, V: AsRef<[u8]>>(key, value) -> Result<WriteHandle, Error>`                                   | async      |
| delete(key)    | `db.delete<K: AsRef<[u8]>>(key) -> Result<WriteHandle, Error>`                                          | async      |
| scan(range)    | `db.scan<K, T: RangeBounds<K>>(range) -> Result<DbIterator, Error>`; then `iter.next().await -> Result<Option<KeyValue>, Error>` (async iteration, not a `Stream`) | async |
| scan_prefix    | `db.scan_prefix<P: AsRef<[u8]> + Send>(prefix) -> Result<DbIterator, Error>` (native — no next-prefix math needed) | async |
| write_batch    | `let mut b = WriteBatch::new(); b.put(k, v); b.delete(k2); db.write(b).await -> Result<WriteHandle, Error>` — atomic | async |
| snapshot       | `db.snapshot() -> Result<Arc<DbSnapshot>, Error>`; `DbSnapshot::{get, scan, scan_prefix}` mirror `Db`; `snapshot.seq() -> u64` exposes the pinned sequence | async |
| close          | `db.close() -> Result<(), Error>`                                                                       | async      |
| flush          | `db.flush() -> Result<(), Error>`                                                                       | async      |

## Snapshot tier chosen

**Chosen tier for M11: Tier 1 (native).**

**Rationale:** SlateDB 0.12.1 exposes a first-class `Db::snapshot() -> Arc<DbSnapshot>` that returns a point-in-time view pinned to a sequence number (`DbSnapshot::seq() -> u64`). `DbSnapshot` supports the full read surface we need (`get`, `scan`, `scan_prefix`, plus `_with_options` variants), and the docs state the snapshot "determines data visibility for reads in this snapshot" — i.e. reads are stable against concurrent writes to the parent `Db`. No checkpoint or sequence-pinning gymnastics needed. T7's `KvSnapshot` can be a thin wrapper over `Arc<DbSnapshot>`, and `snapshot_owned` is just a clone of the `Arc`.

## Atomic write_batch

**Confirmed.** `slatedb::WriteBatch` is a first-class public type with `new()`, `put(k, v)`, `delete(k)`, `merge(k, v)` and their `_with_options` variants; the batch is submitted via `db.write(batch).await` (or `db.write_with_options(batch, &opts).await`). The doc string states: "All operations in the batch are applied atomically to the database." No BLOCKER. T6 is straightforward.

## Cargo resolution check

`cargo add --dry-run slatedb@0.12.1 -p agentdb-storage` resolves cleanly. In an isolated Cargo.toml that requests both `slatedb = "0.12.1"` and `object_store = "0.11"`, cargo happily locks `object_store v0.11.2` AND `object_store v0.12.5` side-by-side. **That dual-resolution is a landmine**, not a green light — the `Arc<dyn ObjectStore>` we build from 0.11 is a different trait object than the one `Db::open` expects (from 0.12). The workspace must move `object_store` forward to `0.12` so everyone shares one trait.

## Notable surprises

- **object_store version bump required.** slatedb 0.12.1's workspace pins `object_store = "0.12.3"`. Our workspace currently pins `"0.11"` (see `Cargo.toml:40` and consumers in `bin/agentdb-server/src/main.rs`, `crates/agentdb-api/src/admin.rs`, `crates/agentdb-wire/src/handlers/find.rs`). T2 must bump the workspace dep to `"0.12"` and skim these call sites for any breaking API changes between 0.11 → 0.12 (the `ObjectStore` trait, `PutResult`, `GetResult`, and `LocalFileSystem` constructor are the usual suspects).
- **Values come back as `bytes::Bytes`.** `Db::get` returns `Option<Bytes>`, not `Option<Vec<u8>>`. Current `KvStore` trait returns `Option<Vec<u8>>`; we'll pay a `.to_vec()` on the hot read path unless T3 widens the trait to return `Bytes`. Recommendation: keep the trait as-is for now (rocksdb backend parity) and document the copy cost; re-evaluate after benches.
- **`DbIterator` is async-next, not `Stream`.** `iter.next().await -> Result<Option<KeyValue>, Error>`. No `futures::Stream` impl, no `for await` ergonomics. T5's `scan_prefix` loop is a `while let Some(kv) = iter.next().await?` driven from inside the blocking `KvStore::scan_prefix` via `tokio::runtime::Handle::block_on` (or whichever async bridge the scaffold chooses). This matches the plan's expectation.
- **`scan_prefix` is native.** We do not need the `next_prefix` helper — SlateDB provides `Db::scan_prefix` and `DbSnapshot::scan_prefix` directly. T5's "next_prefix helper" becomes a no-op; we can close it out by documenting that the backend delegates to `scan_prefix` directly. (The `next_prefix` helper may still be useful for the **rocksdb** backend's own `scan_prefix`, so do not delete it unconditionally.)
- **Writes return a `WriteHandle`, not `()`.** `Db::put`/`delete`/`write` return `Result<WriteHandle, Error>`. The handle lets callers await durability (it's how SlateDB exposes "awaiting flush to object-store"). Our `KvStore` trait returns `()`, so T4/T6 will drop the handle. If M11 later needs durability-on-commit, we have a seam.
- **`Db::open` takes `Arc<dyn ObjectStore>` directly.** No `DbOptions` needed for the minimal path; `builder(path, object_store).build().await` is the configurable path (WAL settings, cache, compactor scheduling, etc.). T3 can start with `Db::open` and only reach for the builder when we need `wal_disable` or the foyer cache for benches.
- **Default features pull `aws` + `foyer`.** `cargo add slatedb@0.12.1` with defaults drags in `object_store/aws` and the `foyer` cache. For the local-filesystem dev path we don't strictly need them, but they're cheap to compile and give us S3 out of the box — recommend keeping defaults. If CI build time becomes an issue, switch to `default-features = false` + explicit `aws` for prod, no features for unit tests.
- **No `Db::new` — only `Db::open` / `Db::builder`.** Matches the docs; just noting so T3 doesn't chase a phantom constructor.

## Links consulted

- https://docs.rs/slatedb/0.12.1/slatedb/ (crate root)
- https://docs.rs/slatedb/0.12.1/slatedb/struct.Db.html
- https://docs.rs/slatedb/0.12.1/slatedb/struct.WriteBatch.html
- https://docs.rs/slatedb/0.12.1/slatedb/struct.DbSnapshot.html
- https://docs.rs/slatedb/0.12.1/slatedb/struct.DbIterator.html
- https://crates.io/crates/slatedb (latest = 0.12.1 as of 2026-04-19)
- https://raw.githubusercontent.com/slatedb/slatedb/v0.12.1/Cargo.toml (workspace dep versions)
- https://raw.githubusercontent.com/slatedb/slatedb/v0.12.1/slatedb/Cargo.toml (per-crate dep inheritance)
