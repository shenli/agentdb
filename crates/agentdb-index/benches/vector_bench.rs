use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::hint::black_box;

use agentdb_index::{
    vector::{HnswVectorIndex, VectorIndex},
    DocId,
};
use criterion::{criterion_group, criterion_main, Criterion};

const N: usize = 10_000;
const DIM: usize = 384;
const QUERIES: usize = 100;
const TOP_K: usize = 10;

fn pseudo_rand(seed: u64, i: u64) -> f32 {
    let mut h = DefaultHasher::new();
    (seed, i).hash(&mut h);
    ((h.finish() as f64 / u64::MAX as f64) * 2.0 - 1.0) as f32
}

fn gen_vecs(n: usize, dim: usize, seed_base: u64) -> Vec<Vec<f32>> {
    (0..n as u64)
        .map(|i| (0..dim as u64).map(|j| pseudo_rand(seed_base + i, j)).collect())
        .collect()
}

fn brute_force_knn(vecs: &[Vec<f32>], query: &[f32], k: usize) -> Vec<(usize, f32)> {
    let norm_q: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-12);
    let mut ranked: Vec<(usize, f32)> = vecs
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let norm_v: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-12);
            let dot: f32 = v.iter().zip(query.iter()).map(|(a, b)| a * b).sum();
            (i, dot / (norm_v * norm_q))
        })
        .collect();
    ranked.sort_by(|(_, a), (_, b)| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    ranked.truncate(k);
    ranked
}

fn bench(c: &mut Criterion) {
    let vecs = gen_vecs(N, DIM, 1);
    let queries = gen_vecs(QUERIES, DIM, 1_000_000);

    let mut idx = HnswVectorIndex::from_env();
    for (i, v) in vecs.iter().enumerate() {
        idx.insert("bench", DocId::new(0, i as u64), v).unwrap();
    }
    // Warm the cache (trigger lazy build).
    let _ = idx.knn("bench", &queries[0], TOP_K);

    c.bench_function("hnsw_knn_10k_x_384", |b| {
        let mut q = 0usize;
        b.iter(|| {
            let res = idx.knn("bench", black_box(&queries[q % QUERIES]), TOP_K);
            q += 1;
            res
        })
    });

    c.bench_function("brute_knn_10k_x_384", |b| {
        let mut q = 0usize;
        b.iter(|| {
            let res = brute_force_knn(&vecs, black_box(&queries[q % QUERIES]), TOP_K);
            q += 1;
            res
        })
    });

    // Recall@10 printed once; not a criterion measurement.
    let mut total_recall = 0.0;
    for q in &queries {
        let truth = brute_force_knn(&vecs, q, TOP_K);
        let hits = idx.knn("bench", q, TOP_K);
        let truth_ids: std::collections::HashSet<u64> =
            truth.iter().map(|(i, _)| *i as u64).collect();
        let hnsw_ids: std::collections::HashSet<u64> = hits.iter().map(|(id, _)| id.seq).collect();
        let overlap = truth_ids.intersection(&hnsw_ids).count();
        total_recall += overlap as f32 / TOP_K as f32;
    }
    let avg_recall = total_recall / queries.len() as f32;
    eprintln!(
        "recall@10 (averaged over {} queries) = {:.3}",
        queries.len(),
        avg_recall
    );
}

criterion_group!(benches, bench);
criterion_main!(benches);
