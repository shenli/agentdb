//! M16 Phase A — HNSW backend spike.
//!
//! Probes `hnsw_rs` API on the three axes the production swap depends on:
//!
//! 1. **Incremental insert** — insert vectors one-at-a-time and query as we go
//! 2. **Query recall vs brute force** — confirm HNSW approximation is good
//!    enough compared to the exact-cosine baseline
//! 3. **Serialize → drop → deserialize → still-works** — required for snapshot
//!    persistence in Phase D
//!
//! This file is **intentionally test-only**: it does not touch production
//! code. If any assertion here fails or the library's API doesn't cover
//! the need, the swap aborts and we evaluate `usearch` instead
//! (see plan Phase A Task A1).

use hnsw_rs::api::AnnT;
use hnsw_rs::prelude::{DistCosine, Hnsw};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const DIM: usize = 128;
const N: usize = 2_000;
const K: usize = 10;

fn random_vecs(count: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count)
        .map(|_| (0..dim).map(|_| rng.gen_range(-1.0..1.0f32)).collect())
        .collect()
}

fn normalise(v: &[f32]) -> Vec<f32> {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm <= 1e-12 {
        return v.to_vec();
    }
    v.iter().map(|x| x / norm).collect()
}

fn brute_force_topk(vectors: &[Vec<f32>], query: &[f32], k: usize) -> Vec<usize> {
    let mut scored: Vec<(usize, f32)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let dot: f32 = v.iter().zip(query.iter()).map(|(a, b)| a * b).sum();
            (i, dot)
        })
        .collect();
    // Higher dot = more similar (vectors are normalized).
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    scored.into_iter().take(k).map(|(i, _)| i).collect()
}

fn recall_at_k(approx: &[usize], exact: &[usize]) -> f32 {
    let exact_set: std::collections::HashSet<_> = exact.iter().copied().collect();
    let hits = approx.iter().filter(|i| exact_set.contains(i)).count();
    hits as f32 / exact.len() as f32
}

#[test]
fn incremental_insert_then_query_beats_baseline_recall() {
    let vecs: Vec<Vec<f32>> = random_vecs(N, DIM, 42).into_iter().map(|v| normalise(&v)).collect();

    // Build HNSW incrementally. hnsw_rs signature:
    //   Hnsw::new(max_nb_connections, max_elements, max_layers, ef_construction, distance)
    let hnsw: Hnsw<'_, f32, DistCosine> =
        Hnsw::new(16, N + 1000, 16, 200, DistCosine {});

    // Insert one-at-a-time to verify the library supports online insert.
    for (i, v) in vecs.iter().enumerate() {
        hnsw.insert((v.as_slice(), i));
    }

    // Pick a random existing vector as the query and test recall.
    let query = &vecs[7];
    let ef_search = 50;
    let neighbours = hnsw.search(query, K, ef_search);
    let approx_ids: Vec<usize> = neighbours.iter().map(|n| n.d_id).collect();
    let exact_ids = brute_force_topk(&vecs, query, K);

    let recall = recall_at_k(&approx_ids, &exact_ids);
    println!("incremental: recall@{K} = {recall:.3}");
    assert!(
        recall >= 0.9,
        "recall@{K} below 0.9 for N={N}; got {recall:.3}. \
         hnsw_rs API may need different ef/M tuning."
    );

    // Identity property: the query is itself in the index and must be in
    // the top-1 hit.
    assert!(
        approx_ids.contains(&7),
        "query vector not in its own neighbours: {approx_ids:?}"
    );
}

#[test]
fn serialize_dump_and_reload_preserves_query_results() {
    use hnsw_rs::prelude::HnswIo;
    use std::path::PathBuf;

    let tmp = tempfile::tempdir().unwrap();
    let prefix = "spike";
    let dir: PathBuf = tmp.path().to_path_buf();

    let vecs: Vec<Vec<f32>> = random_vecs(1000, DIM, 99)
        .into_iter()
        .map(|v| normalise(&v))
        .collect();

    // Build + dump.
    let query_id_a: Vec<usize>;
    {
        let hnsw: Hnsw<'_, f32, DistCosine> = Hnsw::new(16, 2000, 16, 200, DistCosine {});
        for (i, v) in vecs.iter().enumerate() {
            hnsw.insert((v.as_slice(), i));
        }
        let neighbours = hnsw.search(&vecs[11], K, 50);
        query_id_a = neighbours.iter().map(|n| n.d_id).collect();

        // file_dump writes `{prefix}.hnsw.data` + `{prefix}.hnsw.graph`
        hnsw.file_dump(&dir, prefix)
            .expect("file_dump must succeed");
        // Drop happens at scope end.
    }

    // Reload.
    let mut io = HnswIo::new(&dir, prefix);
    let loaded: Hnsw<'_, f32, DistCosine> =
        io.load_hnsw().expect("load_hnsw must succeed");
    let neighbours_b = loaded.search(&vecs[11], K, 50);
    let query_id_b: Vec<usize> = neighbours_b.iter().map(|n| n.d_id).collect();

    assert_eq!(
        query_id_a, query_id_b,
        "reloaded HNSW produced different top-K ids: before={query_id_a:?}, after={query_id_b:?}"
    );
}

#[test]
fn post_load_incremental_insert_visible_in_query() {
    // Pre-load snapshot, then insert new points, verify they appear.
    let vecs: Vec<Vec<f32>> = random_vecs(500, DIM, 7)
        .into_iter()
        .map(|v| normalise(&v))
        .collect();
    let hnsw: Hnsw<'_, f32, DistCosine> = Hnsw::new(16, 2000, 16, 200, DistCosine {});
    for (i, v) in vecs.iter().enumerate() {
        hnsw.insert((v.as_slice(), i));
    }

    // Insert a "signature" vector with a deliberately-unique id after
    // the initial batch.
    let signature_id = 99_999;
    let signature_vec = normalise(&vec![1.0_f32; DIM]); // all-ones vector
    hnsw.insert((signature_vec.as_slice(), signature_id));

    let neighbours = hnsw.search(&signature_vec, K, 50);
    let approx_ids: Vec<usize> = neighbours.iter().map(|n| n.d_id).collect();
    assert!(
        approx_ids.contains(&signature_id),
        "post-insert signature id {signature_id} not found in top-K: {approx_ids:?}"
    );
}
