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
//! `pangea-operator::reactive::evaluate`, `lava-operator::drift_scan`,
//! and `tend::derive_from_receipt`.
//!
//! # The shape
//!
//! Every "given (observed state, triggering event, declared policy,
//! ambient observations), decide what to do" function has the same
//! four-typed-input, one-typed-output shape. No `self` receiver —
//! implementors are zero-sized markers (`pub struct MyDecision;`); the
//! `decide` associated function is the entire pure rule, which forbids
//! `&mut self` and accidental stateful caching at the trait boundary.
//!
//! # The trait law
//!
//! `D::decide(&s,&e,&p,&o) == D::decide(&s,&e,&p,&o)` (determinism) — no
//! I/O, no randomness, no hidden state. The point is to make every
//! reconciler's decision logic proptest-able without mocks; the
//! signature forbids side effects. `chrono::DateTime<Utc>` is the
//! canonical `Observed` when the only ambient observation is the clock.

use serde::{Deserialize, Serialize};

/// Pure decision function — typed inputs, typed output, NO I/O. The
/// substrate's canonical "given (state, event, policy, observed),
/// decide what to do" abstraction. Implementations are unit structs
/// (`pub struct MyDecision;`); the `decide` associated function is the
/// entire rule.
pub trait Decision: Send + Sync {
    /// Current observed state (workspace status, pod phase, CR
    /// condition). Read-only — Decision never mutates state.
    type State;
    /// Triggering event (pull-failed, drift detected, timer expired).
    type Event;
    /// Operator-declared policy (retry budget, threshold, allowlist).
    type Policy;
    /// Ambient observations that aren't state/event/policy — most
    /// commonly a clock; richer for decisions needing fleet metrics.
    type Observed;
    /// Typed decision the controller acts on.
    type Output;

    /// The decision rule. Pure function of its inputs.
    fn decide(
        state: &Self::State,
        event: &Self::Event,
        policy: &Self::Policy,
        observed: &Self::Observed,
    ) -> Self::Output;
}

// ── Reference impl: a tiny pool-decision (generic demo) ───────────
// The canonical first impl + law suite. Synthetic (not domain-specific):
// it demonstrates the shape every consumer follows — zero-sized marker
// struct, four typed associated types, one associated function.

/// Demo: the "should the pool spawn more members?" decision shape.
/// Mirrors the structure of `tatara::decide_pool_reconcile`; the real
/// tatara function migrates onto `Decision` in Phase 0.5 consumer work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolStateDemo {
    pub current_members: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PoolEventDemo {
    HeartbeatTick,
    MemberFailed,
    MemberJoined,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolPolicyDemo {
    pub min_size: u32,
    pub max_size: u32,
    pub desired_size: u32,
}

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
    type Event = PoolEventDemo;
    type Policy = PoolPolicyDemo;
    type Observed = ClockNow;
    type Output = PoolDecisionDemo;

    fn decide(
        state: &Self::State,
        _event: &Self::Event,
        policy: &Self::Policy,
        _observed: &Self::Observed,
    ) -> Self::Output {
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

    fn s(n: u32) -> PoolStateDemo {
        PoolStateDemo { current_members: n }
    }
    fn p(min: u32, max: u32, desired: u32) -> PoolPolicyDemo {
        PoolPolicyDemo {
            min_size: min,
            max_size: max,
            desired_size: desired,
        }
    }
    fn ev() -> PoolEventDemo {
        PoolEventDemo::HeartbeatTick
    }
    fn now() -> ClockNow {
        ClockNow(0)
    }

    #[test]
    fn at_desired_size_is_noop() {
        assert_eq!(
            PoolDecisionDemoImpl::decide(&s(3), &ev(), &p(1, 10, 3), &now()),
            PoolDecisionDemo::NoOp
        );
    }

    #[test]
    fn below_desired_spawns_delta() {
        assert_eq!(
            PoolDecisionDemoImpl::decide(&s(2), &ev(), &p(1, 10, 5), &now()),
            PoolDecisionDemo::Spawn { count: 3 }
        );
    }

    #[test]
    fn above_desired_reaps_delta() {
        assert_eq!(
            PoolDecisionDemoImpl::decide(&s(8), &ev(), &p(1, 10, 5), &now()),
            PoolDecisionDemo::ReapExcess { count: 3 }
        );
    }

    #[test]
    fn desired_clamped_by_policy_bounds() {
        assert_eq!(
            PoolDecisionDemoImpl::decide(&s(5), &ev(), &p(1, 10, 20), &now()),
            PoolDecisionDemo::Spawn { count: 5 },
            "desired clamped to max"
        );
        assert_eq!(
            PoolDecisionDemoImpl::decide(&s(5), &ev(), &p(2, 10, 0), &now()),
            PoolDecisionDemo::ReapExcess { count: 3 },
            "desired clamped to min"
        );
    }

    /// The trait law: same inputs → same output, every time.
    #[test]
    fn determinism_law() {
        for (state, policy) in [(s(0), p(1, 5, 3)), (s(3), p(1, 5, 3)), (s(7), p(1, 5, 3))].iter() {
            let a = PoolDecisionDemoImpl::decide(state, &ev(), policy, &now());
            let b = PoolDecisionDemoImpl::decide(state, &ev(), policy, &now());
            assert_eq!(a, b, "non-deterministic for state={state:?} policy={policy:?}");
        }
    }

    /// The common consumer pattern: generic over `Decision` impls
    /// (each impl is a unit struct, so monomorphization is cheap).
    #[test]
    fn generic_consumer_pattern() {
        fn run_one<D: Decision>(
            s: &D::State,
            e: &D::Event,
            p: &D::Policy,
            o: &D::Observed,
        ) -> D::Output {
            D::decide(s, e, p, o)
        }
        assert_eq!(
            run_one::<PoolDecisionDemoImpl>(&s(2), &ev(), &p(1, 10, 5), &now()),
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
