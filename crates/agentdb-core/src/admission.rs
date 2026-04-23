//! Token-bucket admission control.
//!
//! The bucket refills based on elapsed wall-clock time since the last
//! call — no background thread, no ticker. A `qps=0` or "unlimited"
//! bucket is represented by `None` at the call site; this module never
//! instantiates a zero-QPS bucket.

use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AdmissionDenied {
    pub scope: Scope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// Per-box QPS cap (via `Quota::max_qps`).
    Box,
    /// Per-box-per-tenant QPS cap (via `Quota::max_tenant_qps`).
    /// One bucket per tenant, each box maintains its own map.
    Tenant,
    /// Global per-tenant QPS cap (via the meta store's
    /// `tenant_quotas` table). One bucket per tenant,
    /// aggregated across every box on this node.
    TenantGlobal,
}

impl Scope {
    pub fn as_str(self) -> &'static str {
        match self {
            Scope::Box => "box",
            Scope::Tenant => "tenant",
            Scope::TenantGlobal => "tenant_global",
        }
    }
}

#[derive(Debug)]
pub struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
    qps: f64,
    burst: f64,
}

impl TokenBucket {
    /// Construct a full bucket. `qps` is the steady-state refill rate
    /// (tokens/second), `burst` is the capacity.
    pub fn new(qps: u64, burst: u64) -> Self {
        let b = burst.max(1) as f64;
        Self {
            tokens: b,
            last_refill: Instant::now(),
            qps: qps as f64,
            burst: b,
        }
    }

    /// Attempt to consume one token. Refills first based on elapsed
    /// time. Returns `true` if admitted, `false` if the caller should
    /// be denied.
    pub fn admit(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.qps).min(self.burst);
        self.last_refill = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn starts_full_and_admits_up_to_burst() {
        let mut b = TokenBucket::new(10, 3);
        assert!(b.admit());
        assert!(b.admit());
        assert!(b.admit());
        assert!(!b.admit());
    }

    #[test]
    fn refills_over_time() {
        let mut b = TokenBucket::new(100, 1);
        assert!(b.admit());
        assert!(!b.admit());
        thread::sleep(Duration::from_millis(50));
        // 100 qps * 0.05s = 5 tokens added, capped at burst=1.
        assert!(b.admit());
    }

    #[test]
    fn cap_cannot_exceed_burst() {
        let mut b = TokenBucket::new(1000, 2);
        // Drain.
        assert!(b.admit());
        assert!(b.admit());
        // Wait long enough for 1000+ tokens if uncapped.
        thread::sleep(Duration::from_millis(50));
        // Only `burst` = 2 admits before denial.
        assert!(b.admit());
        assert!(b.admit());
        assert!(!b.admit());
    }

    #[test]
    fn scope_as_str_is_stable() {
        assert_eq!(Scope::Box.as_str(), "box");
        assert_eq!(Scope::Tenant.as_str(), "tenant");
        assert_eq!(Scope::TenantGlobal.as_str(), "tenant_global");
    }
}
