# HNSW backend spike — outcome

**Date:** 2026-04-22
**Gate:** M16 Phase A Task A1 Step 2
**Outcome: PASS. Proceed with `hnsw_rs`. No need to evaluate `usearch`.**

## What was tested

`crates/agentdb-index/tests/hnsw_backend_spike.rs` exercised the three axes the production swap depends on:

| Axis | Test | Result |
|---|---|---|
| Incremental insert | Build 2000-vector index one insert at a time, query top-10 | **Pass** — recall@10 ≥ 0.9 vs exact brute force |
| Serialize ↔ deserialize | Build, `file_dump`, drop, `HnswIo::load_hnsw`, requery | **Pass** — identical top-K ids before and after reload |
| Post-load incremental | Load dumped index, insert a new signature vector, query | **Pass** — new vector visible in results |

## Library: `hnsw_rs` 0.3.4

- Pure Rust with `anndists` as the distance backbone
- `hnsw_rs::prelude::Hnsw::<T, DistCosine>::new(max_conn, max_elems, max_layers, ef_construction, dist)`
- `Hnsw::insert((&[T], usize))` — online, no rebuild
- `Hnsw::search(query, k, ef_search) -> Vec<Neighbour>`
- `Hnsw::file_dump(&Path, basename)` via `hnsw_rs::api::AnnT` trait — writes `{basename}.hnsw.data` + `{basename}.hnsw.graph`
- `hnsw_rs::prelude::HnswIo::new(&Path, basename).load_hnsw()` — reload

## Assumptions confirmed

- **Incremental insert without full rebuild.** Solves the core problem that made `instant-distance` unusable at scale (~33 min cold start per BoTE).
- **File-based snapshot format.** Works with our object-storage-as-truth model — write the file locally, stream to object store; reverse on restore.
- **Exact reload fidelity.** Top-K identical before and after dump/load, so the snapshot+replay recovery strategy in Phase E is sound.

## Notes for Phase B (swap)

- The API shape differs from `instant-distance`: no `HnswMap` wrapping; the `id: usize` is stored directly inside the `Hnsw`. Our existing `DocId` needs a mapping to/from `usize`. Since `DocId = (session_internal_id: u64, seq: u64)`, we will keep a side table `Vec<DocId>` with the HNSW id as the index.
- `DistCosine` expects unit-normalized vectors for cosine similarity to equal the dot product; existing `normalise()` stays.
- `AnnT` trait provides `file_dump`; must be brought into scope at the call site (`use hnsw_rs::api::AnnT`).
- `ef_construction` and `ef_search` map cleanly to our existing `AGENTDB_HNSW_EF` env var; keep the single knob.

## Deferred spike items

- **Serialize to `Vec<u8>` directly** (no filesystem intermediate) — `hnsw_rs` 0.3.4 only exposes `file_dump` to a directory. For object-storage snapshots we'll dump to a tempdir, read the files back into bytes, and upload. Acceptable for v1; a small tax but not a blocker.
- **Throughput benchmark at 10⁶ vectors** — deferred to Phase B Task B1 Step 5 where it gates the swap against `instant-distance` baseline.

## Conclusion

Proceed to Phase B (swap `instant-distance` for `hnsw_rs`). No library re-evaluation needed.
