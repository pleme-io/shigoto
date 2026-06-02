//! Typed pure-decision function — the canonical `Decision` trait every
//! fleet-wide deterministic state-transition rule consumes.
//! Spec: `theory/CONVERGENCE-ADOPTION.md` §II.3.
//!
//! A **lightweight, general** convergence primitive — pure, no I/O, no
//! domain coupling. Homed in `shigoto-types` next to its siblings
//! ([`CascadePolicy`](crate::policy::CascadePolicy),
//! [`Sink`](crate::sink::Sink), [`Classifier`](crate::classify::Classifier),
//! [`TimeoutWatcher`](crate::watch::TimeoutWatcher)) so lightweight
//! controllers adopt it without the magma executor closure. (Re-homed
//! 2026-06-02 — was `magma-converge::decision`.)
//!
//! Subsumes the hand-rolled `decide_*` free functions in
//! `tatara::decide_pool_reconcile` / `decide_allocation_reconcile`,
//! `pangea-operator::reactive::evaluate`, and `tend::derive_from_receipt`.
//!
//! # The shape — `(State, Policy, Observed) -> Output`
//!
//! Every "given (observed state, declared policy, ambient observations),
//! decide what to do" rule has the same typed-input → typed-output shape.
//! No `self` receiver — implementors are zero-sized markers
//! (`pub struct MyDecision;`); the `decide` associated function is the
//! entire pure rule, which forbids `&mut self` and accidental stateful
//! caching at the trait boundary.
//!
//! ## Three inputs, two of them frequently `()`
//!
//! A 2026-06-02 fleet survey of the five real `decide_*` functions this
//! trait subsumes found the original fourth input, `Event`, was **dead
//! weight** — 0 of 5 consumers took a distinct triggering event (they all
//! read it out of `State`), and the reference impl ignored it. `Event`
//! was removed. The remaining two non-`State` inputs are **often absent**,
//! so the convention is to set them to `()`:
//!
//! - `Policy` — only 1 of 5 surveyed functions took a distinct operator
//!   policy (pangea's `EffectiveReactivePolicy`); the rest encode policy
//!   in `State`. Set `type Policy = ();` when there is none.
//! - `Observed` — 3 of 5 use a clock; set `type Observed = ();` when the
//!   decision needs no ambient observation, or
//!   `type Observed = chrono::DateTime<Utc>` / a richer struct when it
//!   does. (Stable Rust has no defaulted associated types, so `()` is
//!   spelled explicitly per impl.)
//!
//! The deeper friction the survey surfaced — real `decide_*` functions
//! take *borrowed* multi-inputs, which an owned-associated-type `State`
//! doesn't accommodate without a bundle/lifetime dance — is recorded in
//! `theory/CONVERGENCE-ADOPTION.md` §II.3 as the thing the first real
//! consumer (tatara-pool-reconciler) must resolve.
//!
//! # The trait law
//!
//! `D::decide(&s,&p,&o) == D::decide(&s,&p,&o)` (determinism) — no I/O,
//! no randomness, no hidden state. The point is to make every
//! reconciler's decision logic proptest-able without mocks; the
//! signature forbids side effects.

use serde::{Deserialize, Serialize};

/// Pure decision function — typed inputs, typed output, NO I/O. The
/// substrate's canonical "given (state, policy, observed), decide what
/// to do" abstraction. Implementations are unit structs
/// (`pub struct MyDecision;`); the `decide` associated function is the
/// entire rule.
///
/// Set `type Policy = ();` / `type Observed = ();` for decisions that
/// don't take a distinct operator policy / ambient observation — most
/// real consumers encode policy in `State` and need at most a clock.
/// See the module docs for the survey that drove this shape.
pub trait Decision: Send + Sync {
    /// Current observed state (workspace status, pod phase, CR
    /// condition). Read-only — Decision never mutates state.
    type State;
    /// Operator-declared policy (retry budget, threshold, allowlist).
    /// `()` when the decision encodes its rules in `State` (the common
    /// case across the surveyed fleet).
    type Policy;
    /// Ambient observations that aren't state/policy — most commonly a
    /// clock (`chrono::DateTime<Utc>`); `()` when the decision needs
    /// none.
    type Observed;
    /// Typed decision the controller acts on.
    type Output;

    /// The decision rule. Pure function of its inputs.
    fn decide(
        state: &Self::State,
        policy: &Self::Policy,
        observed: &Self::Observed,
    ) -> Self::Output;
}

// ── Reference impl: a tiny pool-decision (generic demo) ───────────
// The canonical first impl + law suite. Synthetic (not domain-specific):
// it demonstrates the shape every consumer follows — zero-sized marker
// struct, three typed associated types, one associated function. Unlike
// the pre-2026-06-02 demo (which ignored its Event + Observed inputs),
// this one HONESTLY uses every slot it declares: State (member count +
// last-scaled time), Policy (sizing bounds + cooldown), Observed (the
// clock, for the cooldown gate). No dead inputs — a faithful template.

/// Demo state: how many members the pool has now + when it last scaled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolStateDemo {
    pub current_members: u32,
    /// Epoch seconds of the last spawn/reap — drives the cooldown gate.
    pub last_scaled_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolPolicyDemo {
    pub min_size: u32,
    pub max_size: u32,
    pub desired_size: u32,
    /// Don't scale again within this many seconds of the last scale.
    pub cooldown_secs: i64,
}

/// The ambient clock — epoch seconds. The canonical `Observed` when the
/// only ambient input is the current time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClockNow(pub i64);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum PoolDecisionDemo {
    NoOp,
    Spawn { count: u32 },
    ReapExcess { count: u32 },
}

/// The reference Decision impl. Zero-sized marker; `decide` is the
/// entire rule — the canonical migration-target shape for every existing
/// `fn decide_*` free function in the fleet.
#[derive(Debug, Default, Copy, Clone)]
pub struct PoolDecisionDemoImpl;

impl Decision for PoolDecisionDemoImpl {
    type State = PoolStateDemo;
    type Policy = PoolPolicyDemo;
    type Observed = ClockNow;
    type Output = PoolDecisionDemo;

    fn decide(
        state: &Self::State,
        policy: &Self::Policy,
        observed: &Self::Observed,
    ) -> Self::Output {
        // Cooldown gate (uses Observed + State): don't thrash-scale if we
        // scaled too recently. Clamped at zero against clock skew.
        let elapsed = observed.0.saturating_sub(state.last_scaled_at);
        if elapsed < policy.cooldown_secs {
            return PoolDecisionDemo::NoOp;
        }
        let current = state.current_members;
        let desired = policy.desired_size.clamp(policy.min_size, policy.max_size);
        if current < desired {
            PoolDecisionDemo::Spawn {
                count: desired - current,
            }
        } else if current > desired {
            PoolDecisionDemo::ReapExcess {
                count: current - desired,
            }
        } else {
            PoolDecisionDemo::NoOp
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // State last scaled at t=0; the default clock is far past it so the
    // cooldown is satisfied and the sizing logic is what's exercised
    // (unless a test specifically probes the cooldown gate).
    fn s(n: u32) -> PoolStateDemo {
        PoolStateDemo {
            current_members: n,
            last_scaled_at: 0,
        }
    }
    fn p(min: u32, max: u32, desired: u32) -> PoolPolicyDemo {
        PoolPolicyDemo {
            min_size: min,
            max_size: max,
            desired_size: desired,
            cooldown_secs: 60,
        }
    }
    fn now() -> ClockNow {
        ClockNow(10_000) // far past last_scaled_at=0 + cooldown=60
    }

    #[test]
    fn at_desired_size_is_noop() {
        assert_eq!(
            PoolDecisionDemoImpl::decide(&s(3), &p(1, 10, 3), &now()),
            PoolDecisionDemo::NoOp
        );
    }

    #[test]
    fn below_desired_spawns_delta() {
        assert_eq!(
            PoolDecisionDemoImpl::decide(&s(2), &p(1, 10, 5), &now()),
            PoolDecisionDemo::Spawn { count: 3 }
        );
    }

    #[test]
    fn above_desired_reaps_delta() {
        assert_eq!(
            PoolDecisionDemoImpl::decide(&s(8), &p(1, 10, 5), &now()),
            PoolDecisionDemo::ReapExcess { count: 3 }
        );
    }

    #[test]
    fn desired_clamped_by_policy_bounds() {
        assert_eq!(
            PoolDecisionDemoImpl::decide(&s(5), &p(1, 10, 20), &now()),
            PoolDecisionDemo::Spawn { count: 5 },
            "desired clamped to max"
        );
        assert_eq!(
            PoolDecisionDemoImpl::decide(&s(5), &p(2, 10, 0), &now()),
            PoolDecisionDemo::ReapExcess { count: 3 },
            "desired clamped to min"
        );
    }

    #[test]
    fn within_cooldown_is_noop_even_when_undersized() {
        // Scaled at t=9_990, clock at t=10_000 → 10s elapsed, cooldown=60
        // → gate active → NoOp despite being undersized. Proves the
        // Observed (clock) slot is load-bearing, not dead weight.
        let recently_scaled = PoolStateDemo {
            current_members: 1,
            last_scaled_at: 9_990,
        };
        assert_eq!(
            PoolDecisionDemoImpl::decide(&recently_scaled, &p(1, 10, 5), &ClockNow(10_000)),
            PoolDecisionDemo::NoOp,
            "cooldown gate (Observed) suppresses scaling"
        );
    }

    /// The trait law: same inputs → same output, every time. Dogfoods
    /// the shared `testing::assert_deterministic` helper rather than
    /// hand-spelling the let-a-let-b shape.
    #[test]
    fn determinism_law() {
        for (state, policy) in [(s(0), p(1, 5, 3)), (s(3), p(1, 5, 3)), (s(7), p(1, 5, 3))].iter() {
            crate::testing::assert_deterministic(|| {
                PoolDecisionDemoImpl::decide(state, policy, &now())
            });
        }
    }

    /// The common consumer pattern: generic over `Decision` impls (each
    /// impl is a unit struct, so monomorphization is cheap).
    #[test]
    fn generic_consumer_pattern() {
        fn run_one<D: Decision>(s: &D::State, p: &D::Policy, o: &D::Observed) -> D::Output {
            D::decide(s, p, o)
        }
        assert_eq!(
            run_one::<PoolDecisionDemoImpl>(&s(2), &p(1, 10, 5), &now()),
            PoolDecisionDemo::Spawn { count: 3 }
        );
    }

    #[test]
    fn decision_output_serde_roundtrip() {
        let dec = PoolDecisionDemo::Spawn { count: 7 };
        let json = serde_json::to_string(&dec).unwrap();
        let back: PoolDecisionDemo = serde_json::from_str(&json).unwrap();
        assert_eq!(dec, back);
    }
}
