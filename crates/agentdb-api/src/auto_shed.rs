//! Auto-shed scheduler (post-M25 operator-ergonomics).
//!
//! Watches the hot-tablet detector's snapshot on a timer.
//! When a tablet has been flagged "hot" past a delay
//! threshold AND the per-tablet cooldown has elapsed, fires
//! an internal shed-with-data-migrate. Opt-in via
//! `AGENTDB_AUTO_SHED_ENABLED=true`.
//!
//! The detector itself stays pure signal (M25); this module
//! owns the action side. Keeps the layering clean — detector
//! in `agentdb-core`, action that depends on `AppState` and
//! shed machinery stays in `agentdb-api`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agentdb_core::HotTabletDetector;
use agentdb_meta::MetaStore;
use tokio::task::JoinHandle;

use crate::admin_hot_tablets::migrate_sessions_for_shards;
use crate::AppState;

/// Tunables for the auto-shed loop.
#[derive(Debug, Clone, Copy)]
pub struct AutoShedConfig {
    /// How often to scan the detector snapshot. Defaults to
    /// 10s — matches the detector's own tick so we don't
    /// poll more often than the data changes.
    pub tick: Duration,
    /// Minimum interval between consecutive auto-sheds of
    /// the *same* source tablet. Prevents thrash when a
    /// tablet stays hot right after we sheltered it.
    pub cooldown: Duration,
    /// Fraction of shards to move each time. Matches the
    /// shed endpoint's default when `fraction: null`.
    pub fraction: f64,
}

impl Default for AutoShedConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_secs(10),
            cooldown: Duration::from_secs(300),
            fraction: 0.5,
        }
    }
}

impl AutoShedConfig {
    /// Env:
    /// - `AGENTDB_AUTO_SHED_TICK_SECS` (default 10)
    /// - `AGENTDB_AUTO_SHED_COOLDOWN_SECS` (default 300)
    /// - `AGENTDB_AUTO_SHED_FRACTION` (default 0.5)
    pub fn from_env() -> Self {
        let d = Self::default();
        let tick = std::env::var("AGENTDB_AUTO_SHED_TICK_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(d.tick);
        let cooldown = std::env::var("AGENTDB_AUTO_SHED_COOLDOWN_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(d.cooldown);
        let fraction = std::env::var("AGENTDB_AUTO_SHED_FRACTION")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(d.fraction);
        Self {
            tick,
            cooldown,
            fraction,
        }
    }
}

pub struct AutoShedJob {
    total: Arc<AtomicU64>,
    _handle: JoinHandle<()>,
}

impl AutoShedJob {
    pub fn start(
        state: AppState,
        detector: Arc<HotTabletDetector>,
        meta: Arc<MetaStore>,
        config: AutoShedConfig,
    ) -> Arc<Self> {
        let total = Arc::new(AtomicU64::new(0));
        let handle = tokio::spawn(run_loop(
            state,
            detector,
            meta,
            config,
            total.clone(),
        ));
        Arc::new(Self {
            total,
            _handle: handle,
        })
    }

    /// Total auto-sheds fired across this process's
    /// lifetime. Exposed so the admin `scheduler` view can
    /// surface it alongside reaper counters.
    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Acquire)
    }
}

async fn run_loop(
    state: AppState,
    detector: Arc<HotTabletDetector>,
    meta: Arc<MetaStore>,
    config: AutoShedConfig,
    total: Arc<AtomicU64>,
) {
    let last_fired: Arc<Mutex<HashMap<i64, std::time::Instant>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;
    loop {
        interval.tick().await;
        let hot = detector.hot_tablets();
        if hot.is_empty() {
            continue;
        }
        let now = std::time::Instant::now();
        for ht in hot {
            {
                let seen = last_fired.lock().unwrap();
                if let Some(t) = seen.get(&ht.tablet_id) {
                    if now.duration_since(*t) < config.cooldown {
                        continue;
                    }
                }
            }
            // Pick a destination: the least-loaded tablet
            // from the detector's current snapshot, excluding
            // the source. Matches the shed endpoint's auto-
            // pick logic.
            let Some(dest) = detector
                .all_tablets()
                .into_iter()
                .filter(|t| t.tablet_id != ht.tablet_id)
                .min_by(|a, b| {
                    a.appends_per_sec
                        .partial_cmp(&b.appends_per_sec)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|t| t.tablet_id)
            else {
                tracing::warn!(
                    tablet_id = ht.tablet_id,
                    "auto-shed: no viable destination tablet"
                );
                continue;
            };

            match fire_one(&state, &meta, ht.tablet_id, dest, config.fraction).await {
                Ok(summary) => {
                    total.fetch_add(1, Ordering::Relaxed);
                    last_fired.lock().unwrap().insert(ht.tablet_id, now);
                    tracing::info!(
                        from = ht.tablet_id,
                        to = dest,
                        moved_shards = summary.moved_shards,
                        migrated_sessions = summary.migrated_sessions,
                        skipped_sessions = summary.skipped_sessions,
                        "auto-shed fired"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        %err,
                        from = ht.tablet_id,
                        to = dest,
                        "auto-shed fire failed; will retry next tick past cooldown"
                    );
                }
            }
        }
    }
}

#[derive(Debug)]
struct ShedOutcome {
    moved_shards: usize,
    migrated_sessions: usize,
    skipped_sessions: usize,
}

/// Internal equivalent of `POST /api/admin/tablets/:id/shed
/// { migrate_data: true, fraction }`. Duplicates a modest
/// amount of logic from the REST handler so the auto-shed
/// loop doesn't have to go through HTTP — in exchange we
/// get structured results that log better and a caller-side
/// error type.
async fn fire_one(
    state: &AppState,
    meta: &Arc<MetaStore>,
    tablet_id: i64,
    dest: i64,
    fraction: f64,
) -> Result<ShedOutcome, String> {
    let shards = agentdb_meta::shards::list_by_tablet(meta.pool(), tablet_id)
        .await
        .map_err(|e| format!("list shards: {e}"))?;
    if shards.is_empty() {
        return Ok(ShedOutcome {
            moved_shards: 0,
            migrated_sessions: 0,
            skipped_sessions: 0,
        });
    }
    let n = ((shards.len() as f64) * fraction.clamp(0.0, 1.0)).ceil() as usize;
    let n = n.min(shards.len()).max(1);
    let moved: Vec<i64> = shards[..n].to_vec();
    let now = crate::stream::now_ms() as i64;
    for sid in &moved {
        agentdb_meta::shards::set_assignment(meta.pool(), *sid, Some(dest), now)
            .await
            .map_err(|e| format!("set_assignment {sid}: {e}"))?;
    }
    if let Some(loc) = state.registry.location() {
        if let Err(e) = loc.refresh().await {
            tracing::warn!(%e, "auto-shed: post-move LocationProxy refresh failed");
        }
    }
    let (migrated, skipped) =
        migrate_sessions_for_shards(state, tablet_id as u16, dest as u16, &moved).await;
    Ok(ShedOutcome {
        moved_shards: moved.len(),
        migrated_sessions: migrated.len(),
        skipped_sessions: skipped.len(),
    })
}
