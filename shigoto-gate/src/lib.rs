//! shigoto-gate — typed precondition Gate trait + standard gates.
//!
//! Spec: `theory/SHIGOTO.md` §III.9. Gates are PURE — they evaluate
//! against the scheduler snapshot without IO. IO-dependent gating is
//! itself a Job that produces a typed fact a downstream gate checks.

#![forbid(unsafe_code)]

use std::num::NonZeroUsize;

use shigoto_dag::Dag;
use shigoto_types::{GateAggregate, JobId, JobPhase, SkipReason, Snapshot};

/// One typed precondition. Pure — no IO. Gate impls that "need"
/// IO are antipatterns; the right shape is a Job that emits a typed
/// fact and a downstream gate that checks the fact.
pub trait Gate: Send + Sync + 'static {
    /// Human-readable identifier. Surfaces in transition reasons +
    /// audit events.
    fn name(&self) -> &'static str;

    /// Evaluate this gate against the current scheduler state for the
    /// given job. Returns `Pass | Wait | Skip(reason)`.
    fn evaluate(&self, ctx: &GateContext) -> GateOutcome;
}

/// Everything a Gate needs to make its decision: the job in question,
/// the FSM snapshot (every other job's phase), the DAG (so gates
/// like `AllUpstreamsTerminal` can ask about predecessors).
pub struct GateContext<'a> {
    pub job_id: &'a JobId,
    pub snapshot: &'a Snapshot,
    pub dag: &'a Dag,
}

/// One gate's verdict.
///
/// **The derived-verdict law** (`theory/UNREPRESENTABILITY.md` §II.4):
/// a gate that examined nothing must not wear the badge of a gate that
/// examined many and found them all good. `Vacuous` is therefore a
/// distinct arm rather than a silent branch of `Pass`.
///
/// `#[non_exhaustive]` is deliberate and load-bearing: it forces every
/// downstream `match` to carry a wildcard, so the *next* arm this enum
/// grows — notably promoting `Pass` to carry its own subject-set
/// witness — is a non-breaking change instead of a fleet migration.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum GateOutcome {
    /// This gate examined a **non-empty** subject set and every
    /// subject satisfied it; the job may advance past this gate.
    Pass,
    /// This gate ran and its subject set was **empty**, so it makes no
    /// claim about anything. Non-blocking — a DAG root with no
    /// predecessors must still run — but never counted as a pass.
    Vacuous,
    /// This gate is not yet satisfied but expects to be later. The
    /// scheduler re-evaluates on each tick.
    Wait,
    /// This gate refuses the job permanently. The job advances to
    /// `Skipped(reason)` — terminal.
    Skip(SkipReason),
}

/// Reduce a cohort of Gate outcomes to one `GateAggregate` that the
/// FSM `advance()` consumes. Worst-outcome wins: any Skip → Skipped;
/// no Skip but any Wait → SomeWaiting. The first Skip's reason is
/// preserved.
///
/// **This is the single classifier** (§II.4 clause 1): the one place a
/// `GateAggregate` is chosen. No other function constructs one, so a
/// verdict and the subject set it claims about share one origin and
/// cannot drift apart.
///
/// The `AllPassed` / `Vacuous` split is derived, never asserted
/// (§II.4 clause 2): the reduction counts the gates that actually
/// returned `Pass`, and only a `NonZeroUsize` can name `AllPassed`.
/// A cohort of size 0 — or one in which every gate was itself
/// `Vacuous` — therefore has **no way to report a pass**. It reports
/// `Vacuous`, which drives the identical FSM transition (see
/// `shigoto_types::advance`) while saying something different and
/// true.
///
/// The `match` below is exhaustive over `GateOutcome` with no `_` arm.
/// `GateOutcome` is `#[non_exhaustive]` only for *downstream* crates;
/// inside this crate the arm set stays closed, so a future arm cannot
/// be silently rounded up into the pass branch (§II.3 subclass D).
#[must_use]
pub fn reduce(outcomes: &[GateOutcome]) -> GateAggregate {
    let mut any_wait = false;
    let mut passed: usize = 0;
    for o in outcomes {
        match o {
            GateOutcome::Skip(reason) => return GateAggregate::Skipped(reason.clone()),
            GateOutcome::Wait => any_wait = true,
            GateOutcome::Pass => passed += 1,
            // Examined nothing; contributes no evidence either way.
            GateOutcome::Vacuous => {}
        }
    }
    if any_wait {
        return GateAggregate::SomeWaiting;
    }
    match NonZeroUsize::new(passed) {
        Some(gates) => GateAggregate::AllPassed { gates },
        // Zero gates made a claim. There is no `AllPassed` to return
        // here — the type refuses to express one.
        None => GateAggregate::Vacuous,
    }
}

// ── Standard gates ───────────────────────────────────────────────────

/// `AllUpstreamsTerminal` — every direct DAG predecessor has reached
/// a terminal phase ({Succeeded, Skipped, Deadlettered}). The
/// scheduler implicitly applies this gate to enforce DAG edge
/// semantics; consumers don't normally register it explicitly.
///
/// Pass iff a **non-empty** predecessor set is entirely terminal.
/// Wait if any predecessor is non-terminal. `Vacuous` if there are no
/// predecessors at all. (We never Skip from this gate — a Deadlettered
/// ancestor releases its descendants per §VII.4's cascading-skip
/// rule, but the descendant's own gate cohort decides whether to
/// run.)
///
/// This gate is a **universally quantified claim** — "every upstream
/// is terminal" — so it is exactly the shape §II.4 governs. A DAG
/// *root* has zero predecessors; before the `Vacuous` arm existed it
/// returned the same `Pass` as a leaf that had genuinely verified two
/// terminal upstreams, and every root job in every DAG in the fleet
/// reported "all upstreams terminal: PASSED" having examined none.
/// Roots still run (`Vacuous` drives the identical transition) — they
/// just no longer claim a verification they never performed.
pub struct AllUpstreamsTerminal;

impl Gate for AllUpstreamsTerminal {
    fn name(&self) -> &'static str {
        "all-upstreams-terminal"
    }

    fn evaluate(&self, ctx: &GateContext) -> GateOutcome {
        let mut examined: usize = 0;
        for pred in ctx.dag.predecessors(ctx.job_id) {
            let phase = ctx
                .snapshot
                .phases
                .get(&pred)
                .cloned()
                .unwrap_or(JobPhase::Pending);
            if !is_terminal(&phase) {
                return GateOutcome::Wait;
            }
            examined += 1;
        }
        // Derived from the subject set, never asserted: the verdict is
        // a function of how many predecessors were actually inspected.
        if examined == 0 {
            GateOutcome::Vacuous
        } else {
            GateOutcome::Pass
        }
    }
}

/// `OperatorApproved` — pass iff an external operator has flipped a
/// pre-arranged flag. The flag itself lives in the consumer's state
/// store; this gate is a thin wrapper that holds an `Arc<AtomicBool>`
/// or similar. v0.1 ships with a `Closure` variant that takes a
/// `Fn() -> bool` for tests + ad-hoc cases.
pub struct OperatorApproved {
    name: &'static str,
    check: Box<dyn Fn() -> bool + Send + Sync>,
}

impl OperatorApproved {
    pub fn new(name: &'static str, check: impl Fn() -> bool + Send + Sync + 'static) -> Self {
        Self {
            name,
            check: Box::new(check),
        }
    }
}

impl Gate for OperatorApproved {
    fn name(&self) -> &'static str {
        self.name
    }

    fn evaluate(&self, _ctx: &GateContext) -> GateOutcome {
        if (self.check)() {
            GateOutcome::Pass
        } else {
            GateOutcome::Wait
        }
    }
}

// ── Internals ────────────────────────────────────────────────────────

fn is_terminal(phase: &JobPhase) -> bool {
    matches!(
        phase,
        JobPhase::Succeeded | JobPhase::Skipped(_) | JobPhase::Deadlettered
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use shigoto_types::{JobKindId, JobScope, JobSubject};
    use std::collections::HashMap;

    fn n(name: &str) -> JobId {
        JobId {
            scope: JobScope::Global,
            kind: JobKindId::new("test"),
            subject: JobSubject::Pinned(name.into()),
        }
    }

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("test witness must be non-zero")
    }

    // ── REGRESSIONS for the derived-verdict law (§II.4) ───────────
    //
    // These three are the INVERTED before-fix receipt. Each one PASSED
    // against the unfixed code at c75c2af, asserting the defective
    // behaviour (`reduce(&[]) == AllPassed`, `AllUpstreamsTerminal`
    // returning `Pass` over zero predecessors, and the two being
    // indistinguishable). They now assert the opposite and FAIL if
    // anyone reintroduces the old behaviour.

    #[test]
    fn reduce_empty_cohort_is_vacuous_never_all_passed() {
        // Was: `assert_eq!(reduce(&[]), GateAggregate::AllPassed)`.
        assert_eq!(reduce(&[]), GateAggregate::Vacuous);
        assert!(
            !matches!(reduce(&[]), GateAggregate::AllPassed { .. }),
            "a cohort that examined zero gates must never claim a pass"
        );
    }

    #[test]
    fn all_upstreams_terminal_is_vacuous_over_zero_predecessors() {
        // Was: `assert_eq!(..., GateOutcome::Pass)`.
        let dag = Dag::new();
        let snapshot = Snapshot {
            phases: HashMap::new(),
        };
        let root = n("root");
        let ctx = GateContext {
            job_id: &root,
            snapshot: &snapshot,
            dag: &dag,
        };
        assert_eq!(AllUpstreamsTerminal.evaluate(&ctx), GateOutcome::Vacuous);
    }

    #[test]
    fn zero_subject_verdict_is_distinguishable_from_a_real_pass() {
        // A root job: ZERO predecessors examined.
        let empty_dag = Dag::new();
        let empty_snapshot = Snapshot {
            phases: HashMap::new(),
        };
        let root = n("root");
        let root_outcome = AllUpstreamsTerminal.evaluate(&GateContext {
            job_id: &root,
            snapshot: &empty_snapshot,
            dag: &empty_dag,
        });

        // A leaf job: TWO predecessors examined, both terminal-good.
        let mut dag = Dag::new();
        dag.add_edge(n("a"), n("leaf"));
        dag.add_edge(n("b"), n("leaf"));
        let mut phases = HashMap::new();
        phases.insert(n("a"), JobPhase::Succeeded);
        phases.insert(n("b"), JobPhase::Succeeded);
        let snapshot = Snapshot { phases };
        let leaf = n("leaf");
        let leaf_outcome = AllUpstreamsTerminal.evaluate(&GateContext {
            job_id: &leaf,
            snapshot: &snapshot,
            dag: &dag,
        });

        // THE FIX: examined-nothing and examined-two-and-verified-both
        // are now different values, and reduce to different aggregates.
        assert_ne!(root_outcome, leaf_outcome);
        assert_eq!(root_outcome, GateOutcome::Vacuous);
        assert_eq!(leaf_outcome, GateOutcome::Pass);
        assert_ne!(reduce(&[root_outcome]), reduce(&[leaf_outcome]));
    }

    // ── reduce ────────────────────────────────────────────────

    #[test]
    fn reduce_all_pass_carries_the_subject_set_witness() {
        assert_eq!(
            reduce(&[GateOutcome::Pass, GateOutcome::Pass]),
            GateAggregate::AllPassed { gates: nz(2) }
        );
        // The witness is derived from the cohort, not asserted: a
        // three-gate cohort and a two-gate cohort are different values.
        assert_ne!(
            reduce(&[GateOutcome::Pass, GateOutcome::Pass, GateOutcome::Pass]),
            reduce(&[GateOutcome::Pass, GateOutcome::Pass])
        );
    }

    #[test]
    fn reduce_all_vacuous_is_vacuous_not_all_passed() {
        // Every gate examined nothing ⇒ the cohort claims nothing.
        assert_eq!(
            reduce(&[GateOutcome::Vacuous, GateOutcome::Vacuous]),
            GateAggregate::Vacuous
        );
    }

    #[test]
    fn reduce_counts_only_gates_that_actually_claimed() {
        // One real Pass beside two Vacuous gates ⇒ witness of 1, not 3.
        assert_eq!(
            reduce(&[
                GateOutcome::Vacuous,
                GateOutcome::Pass,
                GateOutcome::Vacuous
            ]),
            GateAggregate::AllPassed { gates: nz(1) }
        );
    }

    #[test]
    fn reduce_vacuous_never_masks_a_wait_or_a_skip() {
        assert_eq!(
            reduce(&[GateOutcome::Vacuous, GateOutcome::Wait]),
            GateAggregate::SomeWaiting
        );
        assert_eq!(
            reduce(&[
                GateOutcome::Vacuous,
                GateOutcome::Skip(SkipReason::GateRejected)
            ]),
            GateAggregate::Skipped(SkipReason::GateRejected)
        );
    }

    #[test]
    fn reduce_one_wait_is_some_waiting() {
        assert_eq!(
            reduce(&[GateOutcome::Pass, GateOutcome::Wait, GateOutcome::Pass]),
            GateAggregate::SomeWaiting
        );
    }

    #[test]
    fn reduce_skip_short_circuits_to_skipped() {
        assert_eq!(
            reduce(&[
                GateOutcome::Wait,
                GateOutcome::Skip(SkipReason::GateRejected),
                GateOutcome::Pass,
            ]),
            GateAggregate::Skipped(SkipReason::GateRejected)
        );
    }

    #[test]
    fn reduce_skip_takes_priority_over_wait() {
        // First Skip wins; subsequent Wait/Pass don't matter.
        assert_eq!(
            reduce(&[
                GateOutcome::Skip(SkipReason::Other("first".into())),
                GateOutcome::Wait,
            ]),
            GateAggregate::Skipped(SkipReason::Other("first".into()))
        );
    }

    // ── AllUpstreamsTerminal ──────────────────────────────────

    /// SCHEDULING BEHAVIOUR IS PRESERVED — the load-bearing test.
    ///
    /// §II.4 does not say "empty fails", and for a scheduler it must
    /// not: a DAG node with no predecessors has to run. The fix
    /// changes what the verdict SAYS, never what the scheduler DOES.
    /// This asserts both halves at once — a root reduces to `Vacuous`
    /// (a different value) and still advances `Pending → Ready` (the
    /// same transition a real pass produces).
    #[test]
    fn a_root_with_no_predecessors_still_becomes_ready() {
        use shigoto_types::{Signal, advance};

        let dag = Dag::new();
        let snapshot = Snapshot {
            phases: HashMap::new(),
        };
        let root = n("root");
        let ctx = GateContext {
            job_id: &root,
            snapshot: &snapshot,
            dag: &dag,
        };

        let aggregate = reduce(&[AllUpstreamsTerminal.evaluate(&ctx)]);
        assert_eq!(aggregate, GateAggregate::Vacuous);

        // The scheduler proceeds, exactly as it did before the fix.
        assert_eq!(
            advance(JobPhase::Pending, Signal::EvaluateGates(aggregate)),
            Ok(JobPhase::Ready),
            "a DAG root must still run — Vacuous is non-blocking"
        );

        // And it proceeds from every other gate-evaluating phase, so
        // no cell fell through to `IllegalTransition`.
        for from in [JobPhase::Gated, JobPhase::Ready] {
            assert_eq!(
                advance(from.clone(), Signal::EvaluateGates(GateAggregate::Vacuous)),
                Ok(JobPhase::Ready),
                "Vacuous must not wedge {from:?}"
            );
        }
        assert_eq!(
            advance(
                JobPhase::Retrying { until_ms: 0 },
                Signal::EvaluateGates(GateAggregate::Vacuous)
            ),
            Ok(JobPhase::Ready)
        );
    }

    #[test]
    fn all_upstreams_terminal_waits_when_upstream_is_pending() {
        let mut dag = Dag::new();
        dag.add_edge(n("root"), n("leaf"));
        let mut phases = HashMap::new();
        phases.insert(n("root"), JobPhase::Pending);
        let snapshot = Snapshot { phases };
        let leaf = n("leaf");
        let ctx = GateContext {
            job_id: &leaf,
            snapshot: &snapshot,
            dag: &dag,
        };
        assert_eq!(AllUpstreamsTerminal.evaluate(&ctx), GateOutcome::Wait);
    }

    #[test]
    fn all_upstreams_terminal_passes_when_upstream_succeeded() {
        let mut dag = Dag::new();
        dag.add_edge(n("root"), n("leaf"));
        let mut phases = HashMap::new();
        phases.insert(n("root"), JobPhase::Succeeded);
        let snapshot = Snapshot { phases };
        let leaf = n("leaf");
        let ctx = GateContext {
            job_id: &leaf,
            snapshot: &snapshot,
            dag: &dag,
        };
        assert_eq!(AllUpstreamsTerminal.evaluate(&ctx), GateOutcome::Pass);
    }

    #[test]
    fn all_upstreams_terminal_passes_when_upstream_deadlettered() {
        // Per §VII.4: a deadlettered ancestor releases its descendants.
        let mut dag = Dag::new();
        dag.add_edge(n("root"), n("leaf"));
        let mut phases = HashMap::new();
        phases.insert(n("root"), JobPhase::Deadlettered);
        let snapshot = Snapshot { phases };
        let leaf = n("leaf");
        let ctx = GateContext {
            job_id: &leaf,
            snapshot: &snapshot,
            dag: &dag,
        };
        assert_eq!(AllUpstreamsTerminal.evaluate(&ctx), GateOutcome::Pass);
    }

    #[test]
    fn all_upstreams_terminal_waits_when_any_upstream_pending() {
        // Two upstreams; one succeeded, one pending → wait.
        let mut dag = Dag::new();
        dag.add_edge(n("a"), n("leaf"));
        dag.add_edge(n("b"), n("leaf"));
        let mut phases = HashMap::new();
        phases.insert(n("a"), JobPhase::Succeeded);
        phases.insert(n("b"), JobPhase::Running);
        let snapshot = Snapshot { phases };
        let leaf = n("leaf");
        let ctx = GateContext {
            job_id: &leaf,
            snapshot: &snapshot,
            dag: &dag,
        };
        assert_eq!(AllUpstreamsTerminal.evaluate(&ctx), GateOutcome::Wait);
    }

    // ── OperatorApproved ──────────────────────────────────────

    #[test]
    fn operator_approved_waits_when_check_false() {
        let dag = Dag::new();
        let snapshot = Snapshot {
            phases: HashMap::new(),
        };
        let job = n("x");
        let ctx = GateContext {
            job_id: &job,
            snapshot: &snapshot,
            dag: &dag,
        };
        let gate = OperatorApproved::new("manual-approve", || false);
        assert_eq!(gate.evaluate(&ctx), GateOutcome::Wait);
        assert_eq!(gate.name(), "manual-approve");
    }

    #[test]
    fn operator_approved_passes_when_check_true() {
        let dag = Dag::new();
        let snapshot = Snapshot {
            phases: HashMap::new(),
        };
        let job = n("x");
        let ctx = GateContext {
            job_id: &job,
            snapshot: &snapshot,
            dag: &dag,
        };
        let gate = OperatorApproved::new("manual-approve", || true);
        assert_eq!(gate.evaluate(&ctx), GateOutcome::Pass);
    }
}
