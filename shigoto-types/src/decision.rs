//! Typed pure action-decision — the canonical `Decision` trait every
//! fleet-wide "decide a controller action from a structured context"
//! rule consumes.
//! Spec: `theory/CONVERGENCE-ADOPTION.md` §II.3.
//!
//! A **lightweight, general** convergence primitive — pure, no I/O, no
//! domain coupling. Homed in `shigoto-types` next to its siblings
//! ([`CascadePolicy`](crate::policy::CascadePolicy),
//! [`Sink`](crate::sink::Sink), [`Classifier`](crate::classify::Classifier),
//! [`TimeoutWatcher`](crate::watch::TimeoutWatcher)) so lightweight
//! controllers adopt it without the magma executor closure.
//!
//! # What this is FOR — and how it differs from `Classifier`
//!
//! `Decision` answers **"what should I DO?"** — given a structured,
//! typed decision *context*, pick a typed *action*. It is the
//! action-decision surface that **code generation + `(defdecision …)`
//! Lisp authoring** target: every controller's "what to do this tick"
//! becomes a uniform `Ctx -> Action` rule the substrate can wire and
//! author declaratively.
//!
//! This is a DIFFERENT job from
//! [`Classifier`](crate::classify::Classifier), which answers
//! **"what IS this?"** — categorize an *opaque / unstructured* input (a
//! stderr string, an HTTP status) into a typed variant, with
//! *runtime-composable rules* held in `&self` (the `ChainedClassifier`
//! rule chain). Pick the right tool:
//!
//! - `classify_pull_failure(stderr) -> FailureKind` — categorization → `Classifier`.
//! - `decide_pool_reconcile(pool, members, now) -> PoolDecision` — action → `Decision`.
//!
//! # The shape — one structured `Ctx`, one typed `Action`
//!
//! ```ignore
//! pub trait Decision {
//!     type Ctx;     // the structured decision context (owned; usually Serialize)
//!     type Action;  // the typed decision the controller acts on
//!     fn decide(ctx: &Self::Ctx) -> Self::Action;
//! }
//! ```
//!
//! No `self` receiver — implementors are zero-sized markers
//! (`pub struct MyDecision;`); the `decide` associated function IS the
//! whole rule, which forbids `&mut self` and accidental stateful
//! caching at the trait boundary.
//!
//! The context is a **single owned struct** that bundles *whatever the
//! decision needs* — observed state, operator policy, the clock — into
//! one typed value. Multi-part raw inputs (e.g. tatara's pool spec + a
//! member slice + the clock) fold into one `Ctx`; the controller does
//! the cheap **observe → decide** split (extract `Ctx` from raw kube
//! objects, then `decide`). This is deliberate, not a limitation: a
//! decision that reads two borrowed things is *still one decision over
//! one context* — the owned `Ctx` makes that context first-class.
//! Because `Ctx` is owned and usually `Serialize`, the decision is
//! table-testable from a literal, and the same `Ctx` shape is what a
//! future `(defdecision …)` form authors + what a generator emits.
//!
//! # The trait law
//!
//! `D::decide(&ctx) == D::decide(&ctx)` (determinism) — no I/O, no
//! randomness, no hidden state. Proven per impl via
//! [`crate::testing::assert_deterministic`].

use serde::{Deserialize, Serialize};

/// Pure action-decision — a structured typed context in, a typed action
/// out, NO I/O. The substrate's canonical "decide what to do" surface.
/// Implementations are unit structs (`pub struct MyDecision;`); the
/// `decide` associated function is the entire rule.
///
/// See the module docs for how this differs from
/// [`Classifier`](crate::classify::Classifier) (categorize an opaque
/// input) and when to reach for each.
pub trait Decision: Send + Sync {
    /// The structured decision context — a single owned value bundling
    /// observed state, operator policy, and ambient observations (clock,
    /// metrics) into one typed struct. Usually `Serialize` so decisions
    /// are table-testable + `(defdecision)`-authorable.
    type Ctx;
    /// The typed decision the controller acts on.
    type Action;

    /// The decision rule. Pure function of its context.
    fn decide(ctx: &Self::Ctx) -> Self::Action;
}

// ── Reference impl: a tiny pool-decision (generic demo) ───────────
// The canonical first impl. Synthetic (not domain-specific): it shows
// the shape every consumer follows — a zero-sized marker, one owned
// `Serialize` `Ctx` that bundles the observed state + sizing policy +
// clock, one associated function. Models the real multi-part case
// (tatara's pool spec + member observations + clock) collapsed into a
// single owned context.

/// The structured pool-reconcile context. Bundles the observed pool
/// state, the sizing policy, and the clock into one owned, serializable
/// value — the shape a `(defdecision)` form would author.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolDecisionCtx {
    // ── observed ──
    /// Members currently in the pool.
    pub current_members: u32,
    /// Epoch seconds of the last spawn/reap — drives the cooldown gate.
    pub last_scaled_at: i64,
    /// Current time, epoch seconds.
    pub now: i64,
    // ── policy ──
    pub min_size: u32,
    pub max_size: u32,
    pub desired_size: u32,
    /// Don't scale again within this many seconds of the last scale.
    pub cooldown_secs: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum PoolDecisionDemo {
    NoOp,
    Spawn { count: u32 },
    ReapExcess { count: u32 },
}

/// The reference Decision impl. Zero-sized marker; `decide` is the
/// entire rule — the canonical migration-target shape for every
/// controller's "what to do this tick" function.
#[derive(Debug, Default, Copy, Clone)]
pub struct PoolDecisionDemoImpl;

impl Decision for PoolDecisionDemoImpl {
    type Ctx = PoolDecisionCtx;
    type Action = PoolDecisionDemo;

    fn decide(ctx: &Self::Ctx) -> Self::Action {
        // Cooldown gate (observed + clock): don't thrash-scale if we
        // scaled too recently. Clamped at zero against clock skew.
        let elapsed = ctx.now.saturating_sub(ctx.last_scaled_at);
        if elapsed < ctx.cooldown_secs {
            return PoolDecisionDemo::NoOp;
        }
        let desired = ctx.desired_size.clamp(ctx.min_size, ctx.max_size);
        if ctx.current_members < desired {
            PoolDecisionDemo::Spawn {
                count: desired - ctx.current_members,
            }
        } else if ctx.current_members > desired {
            PoolDecisionDemo::ReapExcess {
                count: ctx.current_members - desired,
            }
        } else {
            PoolDecisionDemo::NoOp
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A context scaled long ago (cooldown satisfied) with given member
    // count + desired; helpers vary only what each test probes.
    fn ctx(current: u32, desired: u32) -> PoolDecisionCtx {
        PoolDecisionCtx {
            current_members: current,
            last_scaled_at: 0,
            now: 10_000, // far past last_scaled_at + cooldown
            min_size: 1,
            max_size: 10,
            desired_size: desired,
            cooldown_secs: 60,
        }
    }

    #[test]
    fn at_desired_size_is_noop() {
        assert_eq!(
            PoolDecisionDemoImpl::decide(&ctx(3, 3)),
            PoolDecisionDemo::NoOp
        );
    }

    #[test]
    fn below_desired_spawns_delta() {
        assert_eq!(
            PoolDecisionDemoImpl::decide(&ctx(2, 5)),
            PoolDecisionDemo::Spawn { count: 3 }
        );
    }

    #[test]
    fn above_desired_reaps_delta() {
        assert_eq!(
            PoolDecisionDemoImpl::decide(&ctx(8, 5)),
            PoolDecisionDemo::ReapExcess { count: 3 }
        );
    }

    #[test]
    fn desired_clamped_by_policy_bounds() {
        // desired 20 clamped to max 10: current 5 < 10 → spawn 5.
        let to_max = PoolDecisionCtx {
            current_members: 5,
            desired_size: 20,
            ..ctx(5, 20)
        };
        assert_eq!(
            PoolDecisionDemoImpl::decide(&to_max),
            PoolDecisionDemo::Spawn { count: 5 },
            "clamped to max"
        );
        // desired 0 clamped to min 2: current 5 > 2 → reap 3.
        let to_min = PoolDecisionCtx {
            current_members: 5,
            min_size: 2,
            desired_size: 0,
            ..ctx(5, 0)
        };
        assert_eq!(
            PoolDecisionDemoImpl::decide(&to_min),
            PoolDecisionDemo::ReapExcess { count: 3 },
            "clamped to min"
        );
    }

    #[test]
    fn within_cooldown_is_noop_even_when_undersized() {
        // Scaled at t=9_990, now t=10_000 → 10s elapsed, cooldown 60 →
        // gate active → NoOp despite being undersized. Proves the clock
        // half of the context is load-bearing.
        let c = PoolDecisionCtx {
            current_members: 1,
            last_scaled_at: 9_990,
            now: 10_000,
            ..ctx(1, 5)
        };
        assert_eq!(
            PoolDecisionDemoImpl::decide(&c),
            PoolDecisionDemo::NoOp,
            "cooldown gate suppresses scaling"
        );
    }

    /// The trait law: same context → same action, every time. Dogfoods
    /// the shared `testing::assert_deterministic` helper.
    #[test]
    fn determinism_law() {
        for (cur, des) in [(0u32, 3u32), (3, 3), (7, 3)] {
            let c = ctx(cur, des);
            crate::testing::assert_deterministic(|| PoolDecisionDemoImpl::decide(&c));
        }
    }

    /// The common consumer pattern: generic over `Decision` impls (each
    /// impl is a unit struct, so monomorphization is cheap).
    #[test]
    fn generic_consumer_pattern() {
        fn run_one<D: Decision>(c: &D::Ctx) -> D::Action {
            D::decide(c)
        }
        assert_eq!(
            run_one::<PoolDecisionDemoImpl>(&ctx(2, 5)),
            PoolDecisionDemo::Spawn { count: 3 }
        );
    }

    #[test]
    fn ctx_and_action_serde_roundtrip() {
        // Ctx is Serialize so decisions are table-testable from a literal
        // + the shape is `(defdecision)`-authorable.
        let c = ctx(2, 5);
        let back: PoolDecisionCtx =
            serde_json::from_str(&serde_json::to_string(&c).unwrap()).unwrap();
        assert_eq!(c, back);
        let a = PoolDecisionDemo::Spawn { count: 7 };
        let back_a: PoolDecisionDemo =
            serde_json::from_str(&serde_json::to_string(&a).unwrap()).unwrap();
        assert_eq!(a, back_a);
    }
}
