# M8: HNSW Vector Index — Design

## Goal

Replace the O(n) brute-force cosine search in `agentdb-index` with an
HNSW-backed approximate nearest-neighbor index. Same public interface,
same query semantics, sub-linear search.

## Motivation

The prototype today implements vector search as brute-force cosine over
all vectors under a path (see `InMemoryFlatVec` in
`crates/agentdb-index/src/vector.rs`). This is correct and obvious but
scales linearly with the number of indexed vectors — fine for hundreds,
painful for tens of thousands.

Agent hybrid retrieval is one of the three differentiating query shapes
in the design doc (§1.5 / §2.7 / §4.4). Making vector search actually
approximate gives the prototype a credible answer to "what algorithm do
you use" when someone asks.

## Scope

**In:**
- Swap `InMemoryFlatVec` for a new `HnswVectorIndex` inside
  `crates/agentdb-index/src/vector.rs`. Same `VectorIndex` trait
  surface.
- Use [`instant-distance`](https://github.com/InstantDomain/instant-distance)
  as the HNSW implementation — pure Rust, maintained, builder API.
- Keep per-path isolation (one HNSW per `path` string within a box).
  Each path's dimension is fixed by its first inserted vector; later
  inserts with a mismatched dim return `IndexError::Invalid` (same as
  today).
- Expose `AGENTDB_HNSW_M` (default 16) and `AGENTDB_HNSW_EF`
  (default 100) env vars. Not wired into the public API.
- Fix `docs/ARCHITECTURE.md` key-layout section, which currently
  describes a non-existent `index_vector/...` KV layout.
- Update `docs/PERFORMANCE.md` with before/after vector-knn numbers on
  one realistic dataset size (e.g. 10k vectors × 384 dims).

**Out:**
- Incremental index updates. The existing
  `Box_::indexes_built` architecture builds lazily on first query and
  caches; subsequent appends do not flow into the cached index. This
  limitation is carried forward from the current prototype and
  documented as a known issue, not fixed in M8.
- Cold-tier-specific ANN. Cold sessions are already indexed by the
  current lazy-build path (it reads segment files). M8 preserves that
  behavior; per-segment HNSW is a future milestone.
- Persisted HNSW snapshots. We rebuild on first query after restart.
- ANN parameters per tenant, per path, or per session. Single pair of
  M / ef values for the whole server.
- Replacing `VectorIndex` with a richer trait that supports delete /
  tombstone. HNSW doesn't need delete support because the index is
  thrown away on box restart.

## Design

### New struct

```rust
// crates/agentdb-index/src/vector.rs

pub struct HnswVectorIndex {
    per_path: HashMap<String, PathHnsw>,
    m: usize,        // HNSW M parameter (default 16)
    ef: usize,       // HNSW ef_construction (default 100)
}

struct PathHnsw {
    dim: usize,
    // instant-distance wants Vec<Vec<f32>> at build time. We build on
    // the first `knn` call, not incrementally, to match the existing
    // lazy architecture.
    pending: Vec<(DocId, Vec<f32>)>,
    built: Option<BuiltIndex>,
}

struct BuiltIndex {
    hnsw: instant_distance::Hnsw<NormPoint>,
    // instant-distance returns opaque usize IDs; we map back to our
    // DocId via this parallel array.
    doc_ids: Vec<DocId>,
}

struct NormPoint(Vec<f32>);  // impls instant_distance::Point for cosine
```

`insert` just appends to `pending`. On `knn`, we build `BuiltIndex` the
first time (staying within the lazy-build spirit of the caller), then
reuse it. The build is ~`O(n log n)` via `instant_distance::Builder`.

### Point implementation

`instant_distance` takes points that implement its `Point` trait
(`distance(&self, other: &Self) -> f32`). We normalize on insert and
use `1.0 - dot(a, b)` so smaller distance = more similar, and the
returned HNSW rank order maps to descending cosine similarity after
negation.

### Trait changes

None. `VectorIndex::insert` and `VectorIndex::knn` keep their current
signatures. The one mechanical change: `knn` takes `&self` today and
can't mutate — but building on first `knn` requires mutation. We wrap
each `PathHnsw` in a `parking_lot::Mutex` (the trait is already
`Send + Sync`, which rules out `RefCell`) so `knn` stays `&self`.
Reasoning: the alternative is changing the trait to `&mut self`, which
would ripple into every caller; the lock is uncontended in practice
(a box has one index and queries serialize through the planner anyway).

### Plumbing

`crates/agentdb-core/src/indexes.rs` switches:

```rust
pub vector: HnswVectorIndex,
// ...
vector: HnswVectorIndex::new(),
```

…and that's it for the core side. The `VectorIndex` trait abstraction
did its job.

### Tests

Keep every existing test in `crates/agentdb-index/src/vector.rs`. They
exercise the public API shape and MUST still pass against the HNSW
implementation (ANN recall on a handful of 2-3 dim points is trivially
100%).

Add:
- `knn_recall_on_100_vectors_is_reasonable` — insert 100 random
  128-dim points, verify that a query for one of them returns the
  same point as hit #1.
- `dim_mismatch_across_paths_is_isolated` — two paths with different
  dims coexist, neither interferes.

### Performance doc update

Extend `docs/PERFORMANCE.md` with a short "Vector search" section
comparing:
- **Before**: brute force on 10k × 384 dims
- **After**: HNSW on the same data

Capture `knn` latency and recall@10 (against brute-force as the oracle,
on a small held-out query set). Include this in the PR.

### ARCHITECTURE.md fix

The "Storage layout (RocksDB keys)" section currently lists:
```
index_vector/{box_id}/{session_internal}/{seq} -> embedding bytes
index_posting/{box_id}/{token}/...             -> posting lists
```

Neither key exists. Indexes live in RAM, built from event payloads on
demand. Replace those two lines with a note:

> Indexes are NOT stored in RocksDB. `PerBoxIndexes` is built lazily
> per box by walking all events (warm KV + cold segment files) on
> first query and cached until server restart.

## Risks / open questions

1. **`instant-distance` build time at realistic N.** If building a
   10k-vector HNSW takes > 1 s, the first query after server restart
   feels slow. Mitigation: lazy build is already the behavior today;
   the user-visible latency is the same order of magnitude, just with
   better asymptotic search time afterward. If it's a problem, we can
   move the build off the query hot path in a follow-up.
2. **HNSW recall vs brute force.** Default `M=16, ef=100` typically
   gives ≥ 95% recall@10 on embedding distributions. If our tests
   (dense, low-dim, adversarial) trip up the heuristics, we bump `ef`
   or fall back to brute force for small N. Plan: if `pending.len() <
   64`, bypass HNSW and do brute force — same correctness, no
   algorithmic surprise on tiny datasets.
3. **Trait `knn(&self)` via interior mutability.** The `Mutex` inside
   the "immutable" knn path is a small wart. Acceptable because the
   index is per-box and queries serialize through the planner anyway.

## Success criteria

- All existing `agentdb-index` and `agentdb-core` tests pass
  unchanged.
- New recall test passes (hit #1 for a query = the inserted point).
- `docs/PERFORMANCE.md` Vector search section shows measurable
  speedup at 10k × 384 dims vs brute force, recall@10 ≥ 0.9.
- `docs/ARCHITECTURE.md` key layout section is accurate.
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
