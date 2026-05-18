//! shigoto-types — typed primitives every other shigoto crate consumes.
//!
//! Spec: `theory/SHIGOTO.md` §III.1–III.4 + §III.11–III.12.
//!
//! v0.1.0 is the scaffold: trait + enum + struct surfaces declared,
//! implementations land as the bootstrap consumer (`tend`) migrates
//! per `theory/SHIGOTO.md` §IV.3 (M0.9 in the broader plan).

#![forbid(unsafe_code)]

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Typed identity for a Job. Stable across cycles + scheduler restarts.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct JobId {
    pub scope: JobScope,
    pub kind: JobKindId,
    pub subject: JobSubject,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum JobScope {
    Global,
    Workspace(String),
    Repo { workspace: String, repo: String },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum JobSubject {
    None,
    Repo(String),
    Org(String),
    Path(PathBuf),
    Pinned(String),
}

/// Typed work-class identifier. Stored as `String` (not `&'static str`)
/// so it serializes through serde without lifetime constraints. Cheap
/// `Clone` is fine for the volume we expect (≤ ~100 kinds across the
/// whole scheduler).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct JobKindId(pub String);

impl JobKindId {
    #[must_use]
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

/// FSM phase a Job inhabits. See `theory/SHIGOTO.md` §III.3 for the
/// transition table.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum JobPhase {
    Pending,
    Gated,
    Ready,
    Running,
    Succeeded,
    Failed { attempts: u32 },
    Retrying { until_ms: i64 },
    Skipped(SkipReason),
    Deadlettered,
    WaitingForOperator,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum SkipReason {
    GateRejected,
    BlockedByDeadletteredAncestor,
    OperatorDecision,
    Other(String),
}

/// FSM driver — every legal way a Job's phase can change. Exhaustive
/// over the `(JobPhase, Signal)` cross-product per `theory/SHIGOTO.md`
/// §IV.1; the `advance` table below enumerates every cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Signal {
    /// Re-evaluate gates for a Pending or Gated or Retrying job. The
    /// outcome (Gated / Ready / Skipped) is carried in the signal
    /// payload so the FSM stays pure — gate evaluation itself is in
    /// shigoto-gate.
    EvaluateGates(GateAggregate),
    /// Scheduler chose to start this Ready job (budget allocated).
    AllocateBudget,
    /// `execute()` returned Ok(output).
    ExecutionSucceeded,
    /// `execute()` returned Err.
    ExecutionFailed,
    /// Retry decision after Failed. Carries whether to retry (then
    /// Retrying with backoff) or deadletter.
    RetryDecide(RetryOutcome),
    /// Cooperative cancellation signal — Running job was told to
    /// stop. Maps to Failed (with cancellation error).
    Cancel,
    /// Per-job timeout elapsed while Running. Maps to Failed (with
    /// timeout error).
    Timeout,
    /// Retry backoff window elapsed; ready to re-evaluate gates.
    BackoffElapsed,
    /// Externally-driven transition from operator action. Constrained
    /// to (WaitingForOperator → Ready|Skipped) and
    /// (Deadlettered → Pending) per §VII.3.
    OperatorTransition(JobPhase),
}

/// Aggregate gate outcome — what the cohort of gates collectively said.
/// Per §III.9 individual gates return Pass / Wait / Skip; the
/// aggregate is the worst outcome (Skip > Wait > Pass) per a typed
/// reducer in shigoto-gate. We carry the rolled-up result here so the
/// FSM stays language-agnostic about how the rollup is computed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateAggregate {
    /// Every gate returned Pass — job advances to Ready.
    AllPassed,
    /// At least one gate returned Wait — job stays Gated.
    SomeWaiting,
    /// At least one gate returned Skip — job advances to Skipped with
    /// that reason.
    Skipped(SkipReason),
}

/// Retry decision from a `RetryPolicy::decide()` call. Same shape that
/// shigoto-retry's `RetryDecision` exposes — duplicated as a typed
/// signal payload so the FSM stays in shigoto-types without a
/// dependency on shigoto-retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryOutcome {
    /// Retry after `until_ms` (absolute timestamp).
    Retry { until_ms: i64 },
    /// No more attempts — deadletter.
    Deadletter,
}

/// Rejected transition — `(from, signal)` is not a legal FSM cell.
/// Returning Result instead of panicking lets consumers report drift
/// (an operator action attempting an illegal transition) without
/// crashing the scheduler.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
#[error("illegal transition: {from:?} cannot consume {signal:?}")]
pub struct IllegalTransition {
    pub from: JobPhase,
    pub signal: Signal,
}

impl Signal {
    /// Whether this signal originated from an operator command.
    /// Operator-driven transitions get logged with extra metadata.
    #[must_use]
    pub fn is_operator_driven(&self) -> bool {
        matches!(self, Self::OperatorTransition(_))
    }
}

/// The canonical FSM driver. Pure: same `(from, signal)` always
/// produces the same result. Exhaustive over `JobPhase × Signal` —
/// adding a new phase or signal fails to compile until every cell
/// of the cross-product is decided.
///
/// Per `theory/SHIGOTO.md` §IV.1 + the diagram in §III.3.
pub fn advance(from: JobPhase, signal: Signal) -> Result<JobPhase, IllegalTransition> {
    use Signal::*;
    let new = match (&from, &signal) {
        // ── Pending dispatches via gate evaluation ─────────────
        (JobPhase::Pending, EvaluateGates(GateAggregate::AllPassed)) => JobPhase::Ready,
        (JobPhase::Pending, EvaluateGates(GateAggregate::SomeWaiting)) => JobPhase::Gated,
        (JobPhase::Pending, EvaluateGates(GateAggregate::Skipped(r))) => {
            JobPhase::Skipped(r.clone())
        }

        // ── Gated re-evaluates each tick ───────────────────────
        (JobPhase::Gated, EvaluateGates(GateAggregate::AllPassed)) => JobPhase::Ready,
        (JobPhase::Gated, EvaluateGates(GateAggregate::SomeWaiting)) => JobPhase::Gated,
        (JobPhase::Gated, EvaluateGates(GateAggregate::Skipped(r))) => {
            JobPhase::Skipped(r.clone())
        }

        // ── Ready awaits budget allocation ─────────────────────
        (JobPhase::Ready, AllocateBudget) => JobPhase::Running,
        // Re-evaluating gates from Ready is allowed (a config change
        // may have invalidated a previously-Pass gate); same dispatch
        // as Gated.
        (JobPhase::Ready, EvaluateGates(GateAggregate::AllPassed)) => JobPhase::Ready,
        (JobPhase::Ready, EvaluateGates(GateAggregate::SomeWaiting)) => JobPhase::Gated,
        (JobPhase::Ready, EvaluateGates(GateAggregate::Skipped(r))) => {
            JobPhase::Skipped(r.clone())
        }

        // ── Running terminates per execute() outcome ───────────
        (JobPhase::Running, ExecutionSucceeded) => JobPhase::Succeeded,
        (JobPhase::Running, ExecutionFailed) => JobPhase::Failed { attempts: 1 },
        (JobPhase::Running, Cancel) => JobPhase::Failed { attempts: 1 },
        (JobPhase::Running, Timeout) => JobPhase::Failed { attempts: 1 },

        // ── Failed waits for the retry policy's decision ───────
        (JobPhase::Failed { attempts }, RetryDecide(RetryOutcome::Retry { until_ms })) => {
            JobPhase::Retrying {
                until_ms: *until_ms,
            }
        }
        (JobPhase::Failed { .. }, RetryDecide(RetryOutcome::Deadletter)) => JobPhase::Deadlettered,

        // ── Retrying re-evaluates after the backoff window ─────
        (JobPhase::Retrying { .. }, BackoffElapsed) => JobPhase::Pending,
        // Gate re-eval from Retrying is allowed if a precondition
        // changes (rare; treated like Pending re-eval).
        (JobPhase::Retrying { .. }, EvaluateGates(GateAggregate::AllPassed)) => JobPhase::Ready,
        (JobPhase::Retrying { .. }, EvaluateGates(GateAggregate::SomeWaiting)) => JobPhase::Gated,
        (JobPhase::Retrying { .. }, EvaluateGates(GateAggregate::Skipped(r))) => {
            JobPhase::Skipped(r.clone())
        }

        // ── Operator-driven transitions (§VII.3) ───────────────
        // WaitingForOperator → Ready or Skipped.
        (JobPhase::WaitingForOperator, OperatorTransition(JobPhase::Ready)) => JobPhase::Ready,
        (JobPhase::WaitingForOperator, OperatorTransition(JobPhase::Skipped(r))) => {
            JobPhase::Skipped(r.clone())
        }
        // Deadlettered → Pending (operator retries from scratch).
        (JobPhase::Deadlettered, OperatorTransition(JobPhase::Pending)) => JobPhase::Pending,

        // ── Every other (phase, signal) pair is illegal ────────
        _ => return Err(IllegalTransition { from, signal }),
    };
    Ok(new)
}

/// Inputs / Outputs / Errors implement these marker traits so the
/// scheduler can serialize across boundaries when persistence lands.
pub trait JobInput: Send + Sync + 'static {}
pub trait JobOutput: Send + Sync + 'static {}
pub trait JobError: std::error::Error + Send + Sync + 'static {}

/// Derived per-tick rollup the scheduler emits on every `tick`.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TickReceipt {
    pub tick_at: chrono::DateTime<chrono::Utc>,
    pub phase_counts: std::collections::BTreeMap<String, u32>,
    pub transitions_this_tick: Vec<TransitionEvent>,
    pub unhealed: Vec<UnhealedDrift>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransitionEvent {
    pub at: chrono::DateTime<chrono::Utc>,
    pub job_id: JobId,
    pub from: JobPhase,
    pub to: JobPhase,
    pub reason: TransitionReason,
    /// Consumer-name tag (e.g. "tend", "forge-gen"). Stored as String
    /// for serde compatibility.
    pub tool: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UnhealedDrift {
    pub job_id: JobId,
    pub phase: JobPhase,
    pub age_seconds: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TransitionReason {
    GateEvaluation,
    BudgetAllocated,
    ExecutionSucceeded,
    ExecutionFailed(String),
    RetryScheduled,
    BackoffElapsed,
    TimedOut,
    Cancelled,
    OperatorAction(String),
}

/// Read-only snapshot of the scheduler's current FSM map.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub phases: std::collections::HashMap<JobId, JobPhase>,
}

#[cfg(test)]
mod fsm_tests {
    use super::*;

    fn pass() -> Signal {
        Signal::EvaluateGates(GateAggregate::AllPassed)
    }
    fn wait() -> Signal {
        Signal::EvaluateGates(GateAggregate::SomeWaiting)
    }
    fn skip() -> Signal {
        Signal::EvaluateGates(GateAggregate::Skipped(SkipReason::GateRejected))
    }

    // ── Pending dispatches ─────────────────────────────────────

    #[test]
    fn pending_with_all_pass_advances_to_ready() {
        assert_eq!(advance(JobPhase::Pending, pass()).unwrap(), JobPhase::Ready);
    }

    #[test]
    fn pending_with_some_wait_advances_to_gated() {
        assert_eq!(advance(JobPhase::Pending, wait()).unwrap(), JobPhase::Gated);
    }

    #[test]
    fn pending_with_skip_advances_to_skipped() {
        match advance(JobPhase::Pending, skip()).unwrap() {
            JobPhase::Skipped(SkipReason::GateRejected) => {}
            other => panic!("expected Skipped(GateRejected), got {other:?}"),
        }
    }

    // ── Gated re-evaluates each tick ───────────────────────────

    #[test]
    fn gated_to_ready_on_all_pass() {
        assert_eq!(advance(JobPhase::Gated, pass()).unwrap(), JobPhase::Ready);
    }

    #[test]
    fn gated_stays_gated_on_some_wait() {
        assert_eq!(advance(JobPhase::Gated, wait()).unwrap(), JobPhase::Gated);
    }

    #[test]
    fn gated_to_skipped_on_skip() {
        matches!(
            advance(JobPhase::Gated, skip()).unwrap(),
            JobPhase::Skipped(_)
        );
    }

    // ── Ready → Running on budget allocation ───────────────────

    #[test]
    fn ready_to_running_on_allocate_budget() {
        assert_eq!(
            advance(JobPhase::Ready, Signal::AllocateBudget).unwrap(),
            JobPhase::Running
        );
    }

    // ── Running terminates four ways ──────────────────────────

    #[test]
    fn running_to_succeeded_on_ok() {
        assert_eq!(
            advance(JobPhase::Running, Signal::ExecutionSucceeded).unwrap(),
            JobPhase::Succeeded
        );
    }

    #[test]
    fn running_to_failed_on_err() {
        assert_eq!(
            advance(JobPhase::Running, Signal::ExecutionFailed).unwrap(),
            JobPhase::Failed { attempts: 1 }
        );
    }

    #[test]
    fn running_to_failed_on_cancel() {
        assert_eq!(
            advance(JobPhase::Running, Signal::Cancel).unwrap(),
            JobPhase::Failed { attempts: 1 }
        );
    }

    #[test]
    fn running_to_failed_on_timeout() {
        assert_eq!(
            advance(JobPhase::Running, Signal::Timeout).unwrap(),
            JobPhase::Failed { attempts: 1 }
        );
    }

    // ── Failed waits for retry decision ───────────────────────

    #[test]
    fn failed_to_retrying_when_retry_decided() {
        assert_eq!(
            advance(
                JobPhase::Failed { attempts: 1 },
                Signal::RetryDecide(RetryOutcome::Retry { until_ms: 12345 })
            )
            .unwrap(),
            JobPhase::Retrying { until_ms: 12345 }
        );
    }

    #[test]
    fn failed_to_deadlettered_when_retries_exhausted() {
        assert_eq!(
            advance(
                JobPhase::Failed { attempts: 3 },
                Signal::RetryDecide(RetryOutcome::Deadletter)
            )
            .unwrap(),
            JobPhase::Deadlettered
        );
    }

    // ── Retrying re-enters Pending after backoff ───────────────

    #[test]
    fn retrying_to_pending_after_backoff() {
        assert_eq!(
            advance(JobPhase::Retrying { until_ms: 100 }, Signal::BackoffElapsed).unwrap(),
            JobPhase::Pending
        );
    }

    // ── Operator-driven transitions ───────────────────────────

    #[test]
    fn waiting_for_operator_to_ready_via_operator() {
        assert_eq!(
            advance(
                JobPhase::WaitingForOperator,
                Signal::OperatorTransition(JobPhase::Ready)
            )
            .unwrap(),
            JobPhase::Ready
        );
    }

    #[test]
    fn waiting_for_operator_to_skipped_via_operator() {
        let result = advance(
            JobPhase::WaitingForOperator,
            Signal::OperatorTransition(JobPhase::Skipped(SkipReason::OperatorDecision)),
        )
        .unwrap();
        assert!(matches!(
            result,
            JobPhase::Skipped(SkipReason::OperatorDecision)
        ));
    }

    #[test]
    fn deadlettered_to_pending_via_operator() {
        assert_eq!(
            advance(
                JobPhase::Deadlettered,
                Signal::OperatorTransition(JobPhase::Pending)
            )
            .unwrap(),
            JobPhase::Pending
        );
    }

    // ── Illegal transitions ───────────────────────────────────

    #[test]
    fn pending_with_allocate_budget_is_illegal() {
        let err = advance(JobPhase::Pending, Signal::AllocateBudget).unwrap_err();
        assert_eq!(err.from, JobPhase::Pending);
    }

    #[test]
    fn succeeded_with_anything_is_illegal_except_no_outbound() {
        // Succeeded is terminal-for-cycle. No transition signals
        // advance from it (operator re-runs land on Deadlettered →
        // Pending, not Succeeded → anything).
        let err = advance(JobPhase::Succeeded, Signal::AllocateBudget).unwrap_err();
        assert_eq!(err.from, JobPhase::Succeeded);
    }

    #[test]
    fn deadlettered_with_random_operator_transition_is_illegal() {
        // Only Deadlettered → Pending via OperatorTransition.
        let err = advance(
            JobPhase::Deadlettered,
            Signal::OperatorTransition(JobPhase::Ready),
        )
        .unwrap_err();
        assert!(matches!(err.from, JobPhase::Deadlettered));
    }

    #[test]
    fn running_with_evaluate_gates_is_illegal() {
        // A Running job is past the gate phase; re-evaluating gates
        // doesn't apply.
        let err = advance(JobPhase::Running, pass()).unwrap_err();
        assert_eq!(err.from, JobPhase::Running);
    }

    #[test]
    fn waiting_for_operator_with_evaluate_gates_is_illegal() {
        // WaitingForOperator requires explicit operator action.
        let err = advance(JobPhase::WaitingForOperator, pass()).unwrap_err();
        assert_eq!(err.from, JobPhase::WaitingForOperator);
    }

    #[test]
    fn signal_is_operator_driven_classifier() {
        assert!(Signal::OperatorTransition(JobPhase::Pending).is_operator_driven());
        assert!(!Signal::AllocateBudget.is_operator_driven());
        assert!(!pass().is_operator_driven());
    }
}
