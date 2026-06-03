//! shigoto-gate — typed precondition Gate trait + standard gates.
//!
//! Spec: `theory/SHIGOTO.md` §III.9. Gates are PURE — they evaluate
//! against the scheduler snapshot without IO. IO-dependent gating is
//! itself a Job that produces a typed fact a downstream gate checks.

#![forbid(unsafe_code)]

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateOutcome {
    /// This gate is satisfied; the job may advance past it.
    Pass,
    /// This gate is not yet satisfied but expects to be later. The
    /// scheduler re-evaluates on each tick.
    Wait,
    /// This gate refuses the job permanently. The job advances to
    /// `Skipped(reason)` — terminal.
    Skip(SkipReason),
}

/// Reduce a cohort of Gate outcomes to one `GateAggregate` that the
/// FSM `advance()` consumes. Worst-outcome wins: any Skip → Skipped;
/// no Skip but any Wait → SomeWaiting; otherwise → AllPassed. The
/// first Skip's reason is preserved.
#[must_use]
pub fn reduce(outcomes: &[GateOutcome]) -> GateAggregate {
    // Cohorts of size 0 trivially pass — a job with no gates can run
    // immediately. The scheduler's default behavior matches.
    let mut any_wait = false;
    for o in outcomes {
        match o {
            GateOutcome::Skip(reason) => return GateAggregate::Skipped(reason.clone()),
            GateOutcome::Wait => any_wait = true,
            GateOutcome::Pass => {}
        }
    }
    if any_wait {
        GateAggregate::SomeWaiting
    } else {
        GateAggregate::AllPassed
    }
}

// ── Standard gates ───────────────────────────────────────────────────

/// `AllUpstreamsTerminal` — every direct DAG predecessor has reached
/// a terminal phase ({Succeeded, Skipped, Deadlettered}). The
/// scheduler implicitly applies this gate to enforce DAG edge
/// semantics; consumers don't normally register it explicitly.
///
/// Pass iff every predecessor is terminal. Wait if any predecessor
/// is non-terminal. (We never Skip from this gate — a Deadlettered
/// ancestor releases its descendants per §VII.4's cascading-skip
/// rule, but the descendant's own gate cohort decides whether to
/// run.)
pub struct AllUpstreamsTerminal;

impl Gate for AllUpstreamsTerminal {
    fn name(&self) -> &'static str {
        "all-upstreams-terminal"
    }

    fn evaluate(&self, ctx: &GateContext) -> GateOutcome {
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
        }
        GateOutcome::Pass
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

    // ── reduce ────────────────────────────────────────────────

    #[test]
    fn reduce_empty_is_all_passed() {
        assert_eq!(reduce(&[]), GateAggregate::AllPassed);
    }

    #[test]
    fn reduce_all_pass_is_all_passed() {
        assert_eq!(
            reduce(&[GateOutcome::Pass, GateOutcome::Pass]),
            GateAggregate::AllPassed
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

    #[test]
    fn all_upstreams_terminal_passes_for_root_with_no_predecessors() {
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
        assert_eq!(AllUpstreamsTerminal.evaluate(&ctx), GateOutcome::Pass);
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
