//! Prototype benchmark for RocksDB checkpoint/SST-based tablet movement
//! (the eager first rung of hardening §7). It compares the two ways to
//! install a moved tablet's key range into a fresh target store:
//!
//!   A. logical replay — scan the source range, ship the KV pairs, and
//!      `write_batch` them into the target (what movement does today, after
//!      the compact-encoding change). Every key goes through the target's
//!      memtable → flush → compaction.
//!   B. SST ingest — write the source range to a self-contained SST
//!      (`write_range_to_sst`) and `ingest_ssts` it into the target, landing
//!      it at the LSM bottom with no per-key work.
//!
//! It measures the transfer size and, crucially, the **target-side install
//! time** — where SST ingest should win big for large tablets. Deliberate
//! benchmark; `#[ignore]`d. Run:
//!   cargo test -p agentdb-storage --test sst_movement_bench -- \
//!     --ignored --nocapture

use std::time::Instant;

use agentdb_storage::{KvStore, RocksKvStore, WriteOp};
use tempfile::TempDir;

const TABLET_PREFIX: &[u8] = b"tablet/1/";

fn seed_source(dir: &std::path::Path, events: usize) -> RocksKvStore {
    let kv = RocksKvStore::open(dir).unwrap();
    let value = vec![0xABu8; 200]; // ~200-byte event, like a chat message
    let mut batch = Vec::with_capacity(1024);
    for i in 0..events {
        let key = [TABLET_PREFIX, format!("event/{i:012}").as_bytes()].concat();
        batch.push(WriteOp::Put {
            key,
            value: value.clone(),
        });
        if batch.len() >= 1024 {
            kv.write_batch(std::mem::take(&mut batch)).unwrap();
        }
    }
    if !batch.is_empty() {
        kv.write_batch(batch).unwrap();
    }
    kv
}

#[tokio::test]
#[ignore = "deliberate benchmark; run with --ignored --nocapture"]
async fn sst_ingest_vs_logical_replay_install() {
    let checkpoints = [1_000usize, 5_000, 20_000, 80_000];

    println!();
    println!("§7 prototype — target-side install: logical write_batch vs SST ingest");
    println!(
        "{:>8}  {:>14}  {:>14}  {:>14}  {:>12}  {:>9}",
        "events", "range_bytes", "sst_bytes", "replay_ms", "ingest_ms", "speedup"
    );

    for &events in &checkpoints {
        let src_dir = TempDir::new().unwrap();
        let src = seed_source(src_dir.path(), events);

        // Read the tablet range once (both paths transfer this).
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = src
            .scan_prefix(TABLET_PREFIX)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        let range_bytes: usize = pairs.iter().map(|(k, v)| k.len() + v.len()).sum();

        // Path A: logical replay via write_batch into a fresh target.
        let replay_dir = TempDir::new().unwrap();
        let replay_target = RocksKvStore::open(replay_dir.path()).unwrap();
        let started = Instant::now();
        let ops: Vec<WriteOp> = pairs
            .iter()
            .map(|(k, v)| WriteOp::Put {
                key: k.clone(),
                value: v.clone(),
            })
            .collect();
        replay_target.write_batch(ops).unwrap();
        let replay_ms = started.elapsed().as_secs_f64() * 1000.0;

        // Path B: export the range to an SST, ingest into a fresh target.
        let sst_dir = TempDir::new().unwrap();
        let sst_path = sst_dir.path().join("tablet-1.sst");
        let sst_bytes = src.write_range_to_sst(TABLET_PREFIX, &sst_path).unwrap();
        let ingest_dir = TempDir::new().unwrap();
        let ingest_target = RocksKvStore::open(ingest_dir.path()).unwrap();
        let started = Instant::now();
        ingest_target
            .ingest_ssts(std::slice::from_ref(&sst_path))
            .unwrap();
        let ingest_ms = started.elapsed().as_secs_f64() * 1000.0;

        // Correctness: both targets hold the same range.
        let replay_count = replay_target.scan_prefix(TABLET_PREFIX).unwrap().count();
        let ingest_count = ingest_target.scan_prefix(TABLET_PREFIX).unwrap().count();
        assert_eq!(replay_count, events);
        assert_eq!(ingest_count, events);
        let sample = ingest_target
            .get(&[TABLET_PREFIX, b"event/000000000000"].concat())
            .unwrap();
        assert_eq!(sample.as_deref(), Some(&vec![0xABu8; 200][..]));

        println!(
            "{:>8}  {:>14}  {:>14}  {:>14.1}  {:>12.1}  {:>8.1}x",
            events,
            range_bytes,
            sst_bytes,
            replay_ms,
            ingest_ms,
            replay_ms / ingest_ms.max(0.001)
        );
    }
    println!();
    println!("SST ingest lands the whole range at the LSM bottom (no memtable/compaction),");
    println!("so target-side install time is ~flat and the transfer is compressed. See");
    println!("docs/design/tablet-attachment-evaluation.md.");
}
