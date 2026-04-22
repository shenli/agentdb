//! Hedged-request primitive.
//!
//! Fire a primary request. If it hasn't completed after `delay`, fire a
//! secondary request. Return whichever finishes first; the loser is
//! cancelled by Rust's cooperative future-drop. Implements the pattern
//! described in *The Tail at Scale* (Dean & Barroso, 2013) and committed
//! to in `docs/agent-db-v2.md` §6.3.
//!
//! Only **latency** is hedged: if the primary fails fast (before
//! `delay`), the error is returned directly — no retry. That matches
//! the design intent ("bound p99.9 on object-storage tails") without
//! introducing implicit retry semantics for errors, which would be a
//! separate feature.

use std::future::Future;
use std::time::Duration;

use tokio::select;

/// Which request produced the returned value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Winner {
    /// The primary request (the one started at t=0) completed first.
    Primary,
    /// The secondary hedge (started at t=delay) completed first.
    Secondary,
}

/// Outcome of a hedged request.
#[derive(Debug)]
pub struct HedgedResult<T> {
    pub value: T,
    pub winner: Winner,
    /// `true` iff the secondary was fired (i.e. primary did not finish
    /// within `delay`). Equivalent to `winner == Secondary`-OR-primary-
    /// was-slow-but-finished-first. Useful for the `hedged_triggers`
    /// metric distinct from `hedged_secondary_wins`.
    pub triggered: bool,
}

/// Race a primary request against a delayed secondary. Both futures
/// share a return type `Result<T, E>`. The primary is polled at t=0; if
/// it has not completed after `delay`, `secondary_factory` is invoked
/// to build the secondary and both are polled concurrently. The first
/// to resolve wins.
///
/// Errors from the primary before `delay` are returned immediately; the
/// secondary is never spawned.
pub async fn hedged<F1, F2, T, E, Fac>(
    primary: F1,
    delay: Duration,
    secondary_factory: Fac,
) -> Result<HedgedResult<T>, E>
where
    F1: Future<Output = Result<T, E>>,
    F2: Future<Output = Result<T, E>>,
    Fac: FnOnce() -> F2,
{
    tokio::pin!(primary);
    // Phase 1: primary-only, until delay elapses or primary resolves.
    select! {
        biased;
        result = &mut primary => {
            return result.map(|value| HedgedResult {
                value,
                winner: Winner::Primary,
                triggered: false,
            });
        }
        _ = tokio::time::sleep(delay) => {}
    }

    // Phase 2: delay elapsed with primary still in flight; fire secondary.
    let secondary = secondary_factory();
    tokio::pin!(secondary);
    select! {
        result = &mut primary => result.map(|value| HedgedResult {
            value,
            winner: Winner::Primary,
            triggered: true,
        }),
        result = &mut secondary => result.map(|value| HedgedResult {
            value,
            winner: Winner::Secondary,
            triggered: true,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Instant;

    #[tokio::test]
    async fn primary_wins_when_fast() {
        let r = hedged(
            async { Ok::<u32, ()>(7) },
            Duration::from_millis(100),
            || async { Ok::<u32, ()>(42) },
        )
        .await
        .unwrap();
        assert_eq!(r.value, 7);
        assert_eq!(r.winner, Winner::Primary);
        assert!(!r.triggered);
    }

    #[tokio::test]
    async fn secondary_fires_and_wins_when_primary_slow() {
        let started = Instant::now();
        let r = hedged(
            async {
                tokio::time::sleep(Duration::from_millis(500)).await;
                Ok::<u32, ()>(1)
            },
            Duration::from_millis(20),
            || async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok::<u32, ()>(2)
            },
        )
        .await
        .unwrap();
        let elapsed = started.elapsed();
        assert_eq!(r.winner, Winner::Secondary);
        assert_eq!(r.value, 2);
        assert!(r.triggered);
        // ~40ms (20ms delay + 20ms secondary), certainly < 500ms primary.
        assert!(
            elapsed < Duration::from_millis(400),
            "hedged completion took {elapsed:?}; expected ≈40ms"
        );
    }

    #[tokio::test]
    async fn primary_error_is_returned_directly_without_secondary() {
        // If primary fails before the delay, we DO NOT fire the secondary.
        // This is the "latency hedging only, not retry" contract.
        let fired = Arc::new(AtomicU32::new(0));
        let fired_clone = fired.clone();
        let r = hedged(
            async { Err::<u32, &str>("nope") },
            Duration::from_millis(100),
            move || {
                let f = fired_clone.clone();
                async move {
                    f.fetch_add(1, Ordering::SeqCst);
                    Ok::<u32, &str>(0)
                }
            },
        )
        .await;
        assert!(matches!(r, Err("nope")));
        assert_eq!(fired.load(Ordering::SeqCst), 0, "secondary must not fire");
    }

    #[tokio::test]
    async fn both_fail_after_delay_returns_the_first_failure() {
        // Primary times out in Phase 2, secondary also fails — whichever
        // finishes first is what the caller sees. The contract is
        // "first-to-complete wins"; no retry on errors either.
        let r = hedged(
            async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Err::<u32, &str>("primary")
            },
            Duration::from_millis(10),
            || async {
                tokio::time::sleep(Duration::from_millis(30)).await;
                Err::<u32, &str>("secondary")
            },
        )
        .await;
        assert!(matches!(r, Err("secondary")), "secondary finished first");
    }

    #[tokio::test]
    async fn losing_future_is_cancelled_via_drop_with_no_side_effects() {
        // The point of the hedge primitive is that the loser's side
        // effects do not happen. Model "side effect" as an atomic counter
        // the loser's tail would bump; verify it stays at zero.
        let side_effects = Arc::new(AtomicU32::new(0));
        let side_effects_l = side_effects.clone();

        let r = hedged(
            async { Ok::<u32, ()>(1) }, // primary wins immediately
            Duration::from_millis(50),
            move || {
                let c = side_effects_l.clone();
                async move {
                    // This future is never awaited because primary wins
                    // before the delay; the factory isn't even invoked.
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok::<u32, ()>(2)
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(r.winner, Winner::Primary);
        assert_eq!(
            side_effects.load(Ordering::SeqCst),
            0,
            "secondary factory invoked or body ran when primary won fast"
        );
    }
}
