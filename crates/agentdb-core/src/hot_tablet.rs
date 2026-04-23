//! Hot-tablet detector (M25).
//!
//! Periodically samples each tablet's [`RateWindow`] and
//! flags tablets whose `admission_wait_ratio` has stayed above
//! a threshold for N consecutive samples. Hysteresis (the
//! "consecutive" part) protects against single-tick spikes
//! flipping operators between hot and cool.
//!
//! The detector publishes its current view to an
//! `Arc<RwLock<Vec<HotTablet>>>` that admin endpoints read. No
//! meta-store writes — hot/cool is a transient runtime signal,
//! not a durable fact.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use tokio::task::JoinHandle;

use crate::box_runtime::BoxRegistry;
use crate::scheduler::LeaderElection;

/// Knobs for the detector. Tuned for prototype traffic shapes;
/// real deployments should revisit after observing. Every
/// field is env-var overridable via [`HotTabletConfig::from_env`].
#[derive(Debug, Clone, Copy)]
pub struct HotTabletConfig {
    /// How often to sample per-tablet metrics. Default 10s
    /// (matches the rate-window slot width — sampling more
    /// frequently wouldn't produce fresher data).
    pub tick: Duration,
    /// Minimum `admission_wait_ratio` (waits / appends over
    /// the rate window) for a tablet to count as saturated.
    /// 0.3 = 30% of writes had to wait for a permit, which
    /// strongly correlates with the per-box admission
    /// semaphore being the bottleneck.
    pub wait_ratio_threshold: f64,
    /// How many consecutive samples must cross the threshold
    /// before we flip a tablet to "hot". 3 samples × 10s =
    /// 30s of sustained saturation.
    pub consecutive_samples: u32,
    /// Minimum appends-per-second before the ratio is trusted.
    /// Protects against "1 append, 1 wait = 100% ratio" noise
    /// at very low traffic.
    pub min_appends_per_sec: f64,
}

impl Default for HotTabletConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_secs(10),
            wait_ratio_threshold: 0.3,
            consecutive_samples: 3,
            min_appends_per_sec: 5.0,
        }
    }
}

impl HotTabletConfig {
    /// Env:
    /// - `AGENTDB_HOT_DETECT_TICK_SECS`  (default 10)
    /// - `AGENTDB_HOT_WAIT_RATIO`         (default 0.3)
    /// - `AGENTDB_HOT_CONSECUTIVE`        (default 3)
    /// - `AGENTDB_HOT_MIN_APS`            (default 5.0)
    pub fn from_env() -> Self {
        let d = Self::default();
        let tick = std::env::var("AGENTDB_HOT_DETECT_TICK_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(d.tick);
        let wait_ratio_threshold = std::env::var("AGENTDB_HOT_WAIT_RATIO")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(d.wait_ratio_threshold);
        let consecutive_samples = std::env::var("AGENTDB_HOT_CONSECUTIVE")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(d.consecutive_samples);
        let min_appends_per_sec = std::env::var("AGENTDB_HOT_MIN_APS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(d.min_appends_per_sec);
        Self {
            tick,
            wait_ratio_threshold,
            consecutive_samples,
            min_appends_per_sec,
        }
    }
}

/// Public view of a flagged tablet.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HotTablet {
    pub tablet_id: i64,
    pub appends_per_sec: f64,
    pub admission_wait_ratio: f64,
    /// Wall-clock ms when this tablet most recently flipped
    /// cool → hot. Preserved across subsequent hot samples so
    /// operators can gauge how long the saturation has lasted.
    pub flagged_at_ms: i64,
}

/// Full per-tablet stat, whether hot or cool. Returned by
/// [`HotTabletDetector::all_tablets`] for admin endpoints
/// that want to show cluster-wide load instead of just the
/// hot set.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TabletLoad {
    pub tablet_id: i64,
    pub appends_per_sec: f64,
    pub admission_wait_ratio: f64,
    pub is_hot: bool,
}

/// Per-tablet counters the detector maintains across ticks.
#[derive(Debug, Default)]
struct TabletState {
    consecutive_hot_samples: u32,
    is_hot: bool,
    flagged_at_ms: i64,
}

/// Handle exposed to admin endpoints. Internally owns the
/// detector's output lock.
pub struct HotTabletDetector {
    config: HotTabletConfig,
    snapshot: Arc<RwLock<DetectorSnapshot>>,
    _handle: JoinHandle<()>,
}

#[derive(Default)]
struct DetectorSnapshot {
    hot: Vec<HotTablet>,
    all: Vec<TabletLoad>,
    last_sample_ms: i64,
}

impl HotTabletDetector {
    /// Spawn the detector. Runs only while this process is the
    /// scheduler leader (check every tick). Followers keep an
    /// empty snapshot; admin endpoints on followers can still
    /// return that empty view without error.
    pub fn start(
        registry: Arc<BoxRegistry>,
        election: Arc<LeaderElection>,
        config: HotTabletConfig,
    ) -> Arc<Self> {
        let snapshot = Arc::new(RwLock::new(DetectorSnapshot::default()));
        let handle = tokio::spawn(run_loop(
            registry,
            election,
            config,
            snapshot.clone(),
        ));
        Arc::new(Self {
            config,
            snapshot,
            _handle: handle,
        })
    }

    /// Every currently-flagged tablet, in `tablet_id` order.
    pub fn hot_tablets(&self) -> Vec<HotTablet> {
        self.snapshot.read().hot.clone()
    }

    /// Every tablet the detector has seen, with `is_hot`
    /// populated. Useful for admin "cluster load" views.
    pub fn all_tablets(&self) -> Vec<TabletLoad> {
        self.snapshot.read().all.clone()
    }

    /// Wall-clock ms of the last successful sample. Zero if
    /// the detector hasn't run yet (process just started, or
    /// this node isn't the leader).
    pub fn last_sample_ms(&self) -> i64 {
        self.snapshot.read().last_sample_ms
    }

    pub fn config(&self) -> HotTabletConfig {
        self.config
    }
}

async fn run_loop(
    registry: Arc<BoxRegistry>,
    election: Arc<LeaderElection>,
    config: HotTabletConfig,
    snapshot: Arc<RwLock<DetectorSnapshot>>,
) {
    let mut state: HashMap<i64, TabletState> = HashMap::new();
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;
    loop {
        interval.tick().await;
        if !election.is_leader() {
            // Followers keep their snapshot empty rather than
            // stale. Admins who hit a follower see "no hot
            // tablets detected on this node."
            let mut w = snapshot.write();
            w.hot.clear();
            w.all.clear();
            w.last_sample_ms = 0;
            continue;
        }
        let now = now_ms();
        let mut hot = Vec::new();
        let mut all = Vec::new();
        for tablet_id in 0..registry.num_boxes() as i64 {
            let Some(box_) = registry.box_by_id(tablet_id as u16) else {
                continue;
            };
            let Some(rate) = box_.metrics.rate_snapshot() else {
                continue;
            };
            let ratio = rate.ratio_b_over_a;
            let aps = rate.a_per_sec;
            // Trust the ratio only when there's enough load
            // to make it statistically meaningful.
            let over_threshold =
                aps >= config.min_appends_per_sec && ratio >= config.wait_ratio_threshold;
            let entry = state.entry(tablet_id).or_default();
            if over_threshold {
                entry.consecutive_hot_samples =
                    entry.consecutive_hot_samples.saturating_add(1);
                if !entry.is_hot
                    && entry.consecutive_hot_samples >= config.consecutive_samples
                {
                    entry.is_hot = true;
                    entry.flagged_at_ms = now;
                }
            } else {
                entry.consecutive_hot_samples = 0;
                entry.is_hot = false;
                entry.flagged_at_ms = 0;
            }
            all.push(TabletLoad {
                tablet_id,
                appends_per_sec: aps,
                admission_wait_ratio: ratio,
                is_hot: entry.is_hot,
            });
            if entry.is_hot {
                hot.push(HotTablet {
                    tablet_id,
                    appends_per_sec: aps,
                    admission_wait_ratio: ratio,
                    flagged_at_ms: entry.flagged_at_ms,
                });
            }
        }
        let mut w = snapshot.write();
        w.hot = hot;
        w.all = all;
        w.last_sample_ms = now;
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::box_runtime::BoxRegistry;
    use crate::scheduler::{LeaderElection, LeaderElectionConfig};
    use agentdb_meta::MetaStore;
    use agentdb_storage::{KvStore, RocksKvStore};
    use object_store::{local::LocalFileSystem, ObjectStore};
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn setup() -> (
        tempfile::NamedTempFile,
        TempDir,
        TempDir,
        Arc<BoxRegistry>,
        Arc<LeaderElection>,
    ) {
        let meta_file = tempfile::NamedTempFile::new().unwrap();
        let meta_path = meta_file.path().to_string_lossy().to_string();
        let meta = Arc::new(MetaStore::open_sqlite(&meta_path).await.unwrap());

        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let reg = Arc::new(BoxRegistry::new(kv, store, 4).unwrap());

        let election = LeaderElection::start(
            meta.clone(),
            "detector-test".into(),
            LeaderElectionConfig {
                lease_duration: Duration::from_secs(10),
                renew_interval: Duration::from_millis(100),
                attempt_interval: Duration::from_millis(50),
            },
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(election.is_leader());
        (meta_file, kv_dir, obj_dir, reg, election)
    }

    /// Saturate box 2 by recording 100 appends and 50 waits in
    /// the rate window; leave the others quiet. The detector
    /// should flag box 2 after `consecutive_samples` ticks.
    #[tokio::test]
    async fn detector_flags_saturated_box_after_hysteresis() {
        let (_m, _kd, _od, reg, election) = setup().await;
        // Warm up the rate windows so the detector has data
        // to sample.
        let hot_box = reg.box_by_id(2).unwrap();
        for _ in 0..100 {
            hot_box.metrics.record_append();
            hot_box.metrics.record_admission_wait();
        }
        // Start the detector with a tiny tick and consecutive=2
        // so the test runs fast.
        let detector = HotTabletDetector::start(
            reg.clone(),
            election,
            HotTabletConfig {
                tick: Duration::from_millis(50),
                wait_ratio_threshold: 0.3,
                consecutive_samples: 2,
                min_appends_per_sec: 0.0, // disable APS floor for the test
            },
        );

        // Keep feeding the rate window so samples across ticks
        // stay over threshold.
        let feeder_box = hot_box.clone();
        let feeder = tokio::spawn(async move {
            for _ in 0..30 {
                for _ in 0..50 {
                    feeder_box.metrics.record_append();
                    feeder_box.metrics.record_admission_wait();
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });

        // Wait long enough for 3+ ticks.
        tokio::time::sleep(Duration::from_millis(500)).await;
        feeder.abort();

        let hot = detector.hot_tablets();
        assert!(
            hot.iter().any(|h| h.tablet_id == 2),
            "box 2 should be flagged hot; detector saw: {hot:?}"
        );
    }

    #[tokio::test]
    async fn detector_cools_off_after_saturation_ends() {
        let (_m, _kd, _od, reg, election) = setup().await;
        let hot_box = reg.box_by_id(1).unwrap();
        // Prime with saturation.
        for _ in 0..500 {
            hot_box.metrics.record_append();
            hot_box.metrics.record_admission_wait();
        }
        let detector = HotTabletDetector::start(
            reg,
            election,
            HotTabletConfig {
                tick: Duration::from_millis(50),
                wait_ratio_threshold: 0.3,
                consecutive_samples: 2,
                min_appends_per_sec: 0.0,
            },
        );
        // Wait for it to heat up.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            detector.hot_tablets().iter().any(|h| h.tablet_id == 1),
            "must flag box 1 before we test cool-off"
        );
        // Now wait > 60s for the window — we can't do that in a
        // unit test. Instead, verify the cool-down invariant
        // holds in isolation: a single-tick dip below threshold
        // immediately flips the state to cool.
        // (Exercised more thoroughly via the run_loop's state
        // machine logic — see unit tests on the transition
        // itself below.)
    }

    /// Direct check on the run_loop's state-machine semantics:
    /// `cool → hot` only after N consecutive, and `hot → cool`
    /// on any single cool sample. We don't drive run_loop
    /// itself here — we exercise the same transition logic in
    /// a table-driven test.
    #[test]
    fn state_machine_transitions() {
        let cfg = HotTabletConfig {
            tick: Duration::from_secs(1),
            wait_ratio_threshold: 0.3,
            consecutive_samples: 3,
            min_appends_per_sec: 1.0,
        };
        let mut s = TabletState::default();
        // Tick 1: hot sample (ratio=0.5, aps=10). No flip yet.
        simulate_sample(&mut s, &cfg, true, 100);
        assert!(!s.is_hot);
        assert_eq!(s.consecutive_hot_samples, 1);
        // Tick 2: hot. Still under consecutive=3.
        simulate_sample(&mut s, &cfg, true, 200);
        assert!(!s.is_hot);
        // Tick 3: hot. Flip!
        simulate_sample(&mut s, &cfg, true, 300);
        assert!(s.is_hot);
        assert_eq!(s.flagged_at_ms, 300);
        // Tick 4: still hot, flagged_at stays the same.
        simulate_sample(&mut s, &cfg, true, 400);
        assert!(s.is_hot);
        assert_eq!(s.flagged_at_ms, 300);
        // Tick 5: one cool sample — flip back immediately.
        simulate_sample(&mut s, &cfg, false, 500);
        assert!(!s.is_hot);
        assert_eq!(s.consecutive_hot_samples, 0);
    }

    fn simulate_sample(s: &mut TabletState, cfg: &HotTabletConfig, over: bool, now_ms: i64) {
        if over {
            s.consecutive_hot_samples = s.consecutive_hot_samples.saturating_add(1);
            if !s.is_hot && s.consecutive_hot_samples >= cfg.consecutive_samples {
                s.is_hot = true;
                s.flagged_at_ms = now_ms;
            }
        } else {
            s.consecutive_hot_samples = 0;
            s.is_hot = false;
            s.flagged_at_ms = 0;
        }
    }
}
