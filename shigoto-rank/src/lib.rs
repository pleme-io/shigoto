//! shigoto-rank — generic anti-starvation work ordering.
//!
//! The companion to [`shigoto-budget`]: budget answers *"how much may run at
//! once"* (the no-crash bound); rank answers *"of everything waiting, which
//! should run next, and in what order"* (the most-value-first bound). Together
//! they let a scheduler hold 100 000 pending units without crashing and still
//! converge each toward its goal in the most valuable order — and never starve
//! a quiet tenant behind a loud one.
//!
//! This crate is **generic over a [`Schedulable`] trait** so any consumer — the
//! shigoto scheduler's ready-wave, a Kubernetes reconcile loop, a fleet
//! orchestrator, a CI fan-out — gets the same ordering by implementing eight
//! cheap accessors. It is pure `std` (no async, no allocator tricks) so the
//! lightest consumer pays nothing for tokio.
//!
//! ## The ordering, most-significant first
//!
//! 1. **Priority class** ([`PriorityClass`]) — a hard tier. A `Critical` unit
//!    always precedes any `High` unit, regardless of how urgent the `High` one
//!    looks. Tiers are the operator's coarse intent; urgency is the fine sort
//!    *within* a tier.
//! 2. **Urgency** — a weighted score combining how stale the unit is, whether
//!    it is behind its target revision, how much it has drifted, and its
//!    **fairness deficit** (how many rounds it has been passed over). The
//!    fairness term is the anti-starvation guarantee: a unit that keeps losing
//!    accrues deficit until it wins, so no unit waits forever.
//! 3. **Stable id** — a deterministic tiebreak so equal-urgency units order
//!    reproducibly (no `HashMap`-iteration nondeterminism; replayable).
//!
//! Eligibility (dependencies satisfied, not in backoff) is a *gate*, not a sort
//! key: an ineligible unit is excluded entirely, never merely deprioritized.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

/// Hard priority tier. Lower discriminant = scheduled first. A higher tier
/// always precedes a lower one regardless of urgency — tiers express coarse
/// operator intent; [`Schedulable::urgency`] is the fine sort within a tier.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum PriorityClass {
    /// Drop-everything: safety, security, customer-down.
    Critical = 0,
    /// Important: behind target, actionable drift.
    High = 1,
    /// Steady-state reconcile.
    #[default]
    Normal = 2,
    /// Best-effort: cleanup, opportunistic refresh.
    Low = 3,
}

/// Weights for the urgency score. Each term is multiplied and summed; tune to
/// shift the balance between "most stale", "furthest behind", "most drifted",
/// and "longest starved". Defaults bias toward closing real drift and honoring
/// fairness, with staleness as a gentle background pressure.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct UrgencyWeights {
    pub staleness: u64,
    pub behind_target: u64,
    pub drift: u64,
    pub fairness_deficit: u64,
}

impl Default for UrgencyWeights {
    fn default() -> Self {
        UrgencyWeights { staleness: 1, behind_target: 3600, drift: 600, fairness_deficit: 1800 }
    }
}

/// A unit of pending work that can be ordered. Implement the eight accessors;
/// the default [`urgency`](Schedulable::urgency) / [`rank_key`](Schedulable::rank_key)
/// give every consumer the same anti-starvation ordering for free.
pub trait Schedulable {
    /// Stable, unique id — the deterministic final tiebreak. Equal-urgency
    /// units order by this, so a rank is reproducible across runs.
    fn sched_id(&self) -> &str;

    /// Hard priority tier.
    fn priority_class(&self) -> PriorityClass;

    /// Seconds since this unit last reached its goal (most-stale-first).
    fn staleness_secs(&self) -> u64;

    /// The source/desired state has moved past what this unit last realized.
    fn behind_target(&self) -> bool;

    /// Size of the gap to close (drifted resources, pending diffs, …). Close
    /// the biggest gap first.
    fn drift_magnitude(&self) -> u32;

    /// Anti-starvation term: how many scheduling rounds this unit (or its
    /// fairness bucket) has been passed over. Grows while it loses; the urgency
    /// it adds guarantees the unit eventually wins.
    fn fairness_deficit(&self) -> u64;

    /// Whether this unit may run *now*: dependencies satisfied and not in
    /// backoff. An ineligible unit is excluded from the ranking entirely.
    fn eligible(&self, now_secs: u64) -> bool;

    /// Weighted urgency score (higher = run sooner). Saturating arithmetic so a
    /// pathological input can never panic or wrap.
    fn urgency(&self, w: &UrgencyWeights) -> u64 {
        let mut s = self.staleness_secs().saturating_mul(w.staleness);
        if self.behind_target() {
            s = s.saturating_add(w.behind_target);
        }
        s = s.saturating_add(u64::from(self.drift_magnitude()).saturating_mul(w.drift));
        s = s.saturating_add(self.fairness_deficit().saturating_mul(w.fairness_deficit));
        s
    }

    /// Total ordering key: `(class, -urgency, id)`. `Ord` on the tuple sorts
    /// class ascending (Critical first), urgency descending (most urgent
    /// first, via `Reverse`), id ascending (stable tiebreak).
    fn rank_key(&self, w: &UrgencyWeights) -> (PriorityClass, std::cmp::Reverse<u64>, String) {
        (self.priority_class(), std::cmp::Reverse(self.urgency(w)), self.sched_id().to_string())
    }
}

/// Order **every eligible** unit in `pending`, most-valuable-first. Ineligible
/// units are dropped. Deterministic: equal-urgency units fall back to stable
/// id, so the result is replayable.
#[must_use]
pub fn rank<'a, T: Schedulable>(pending: &'a [T], now_secs: u64, w: &UrgencyWeights) -> Vec<&'a T> {
    let mut eligible: Vec<&T> = pending.iter().filter(|d| d.eligible(now_secs)).collect();
    eligible.sort_by(|a, b| a.rank_key(w).cmp(&b.rank_key(w)));
    eligible
}

/// Like [`rank`], but return only the top `slots` — the next wave to dispatch.
/// Pair with [`shigoto-budget`] for the no-crash admission bound: rank picks the
/// *order*, budget enforces the *count*.
#[must_use]
pub fn pick<'a, T: Schedulable>(
    pending: &'a [T],
    now_secs: u64,
    w: &UrgencyWeights,
    slots: usize,
) -> Vec<&'a T> {
    let mut out = rank(pending, now_secs, w);
    out.truncate(slots);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal test unit exercising every accessor.
    struct Mock {
        id: String,
        class: PriorityClass,
        staleness: u64,
        behind: bool,
        drift: u32,
        deficit: u64,
        backoff_until: u64,
        deps_ready: bool,
    }

    impl Mock {
        fn new(id: &str, class: PriorityClass) -> Self {
            Mock {
                id: id.into(),
                class,
                staleness: 0,
                behind: false,
                drift: 0,
                deficit: 0,
                backoff_until: 0,
                deps_ready: true,
            }
        }
    }

    impl Schedulable for Mock {
        fn sched_id(&self) -> &str { &self.id }
        fn priority_class(&self) -> PriorityClass { self.class }
        fn staleness_secs(&self) -> u64 { self.staleness }
        fn behind_target(&self) -> bool { self.behind }
        fn drift_magnitude(&self) -> u32 { self.drift }
        fn fairness_deficit(&self) -> u64 { self.deficit }
        fn eligible(&self, now: u64) -> bool { self.deps_ready && now >= self.backoff_until }
    }

    fn w() -> UrgencyWeights { UrgencyWeights::default() }

    #[test]
    fn priority_class_dominates_urgency() {
        let mut loud = Mock::new("loud", PriorityClass::Normal);
        loud.staleness = 1_000_000;
        loud.drift = 1000;
        let crit = Mock::new("crit", PriorityClass::Critical);
        let pending = vec![loud, crit];
        let order: Vec<&str> = rank(&pending, 0, &w()).iter().map(|m| m.sched_id()).collect();
        assert_eq!(order, vec!["crit", "loud"], "Critical precedes a much louder Normal");
    }

    #[test]
    fn within_a_class_more_urgent_wins() {
        let mut a = Mock::new("a", PriorityClass::Normal);
        a.drift = 1;
        let mut b = Mock::new("b", PriorityClass::Normal);
        b.drift = 50;
        let pending = vec![a, b];
        assert_eq!(rank(&pending, 0, &w())[0].sched_id(), "b", "more drift = more urgent");
    }

    #[test]
    fn fairness_deficit_rescues_the_starved() {
        // A unit with no drift but huge accrued deficit beats a busy peer —
        // the anti-starvation guarantee.
        let mut busy = Mock::new("busy", PriorityClass::Normal);
        busy.drift = 5;
        let mut starved = Mock::new("starved", PriorityClass::Normal);
        starved.deficit = 1000;
        let pending = vec![busy, starved];
        assert_eq!(rank(&pending, 0, &w())[0].sched_id(), "starved");
    }

    #[test]
    fn ineligible_units_are_excluded_not_deprioritized() {
        let mut blocked = Mock::new("blocked", PriorityClass::Critical);
        blocked.deps_ready = false;
        let mut backoff = Mock::new("backoff", PriorityClass::Critical);
        backoff.backoff_until = 100;
        let ready = Mock::new("ready", PriorityClass::Low);
        let pending = vec![blocked, backoff, ready];
        let order: Vec<&str> = rank(&pending, 50, &w()).iter().map(|m| m.sched_id()).collect();
        assert_eq!(order, vec!["ready"], "deps-blocked + in-backoff excluded; only ready Low survives");
    }

    #[test]
    fn ordering_is_deterministic_on_ties() {
        // Identical urgency → stable id tiebreak, both directions of input.
        let pending = vec![
            Mock::new("zeta", PriorityClass::Normal),
            Mock::new("alpha", PriorityClass::Normal),
            Mock::new("mu", PriorityClass::Normal),
        ];
        let fwd: Vec<&str> = rank(&pending, 0, &w()).iter().map(|m| m.sched_id()).collect();
        assert_eq!(fwd, vec!["alpha", "mu", "zeta"]);
    }

    #[test]
    fn pick_bounds_the_wave() {
        let pending: Vec<Mock> = (0..100_000)
            .map(|i| {
                // vary id deterministically without Math.random
                let id = format!("u{i:06}");
                Mock::new(&id, PriorityClass::Normal)
            })
            .collect();
        assert_eq!(pick(&pending, 0, &w(), 8).len(), 8, "100k pending → bounded wave");
    }

    #[test]
    fn urgency_saturates_never_panics() {
        let mut m = Mock::new("x", PriorityClass::Normal);
        m.staleness = u64::MAX;
        m.drift = u32::MAX;
        m.deficit = u64::MAX;
        m.behind = true;
        // Must not panic / overflow even with maxed inputs.
        let _ = m.urgency(&w());
    }
}
