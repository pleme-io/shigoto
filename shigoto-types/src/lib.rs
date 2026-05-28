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

pub mod failure;
pub use failure::{classify, signature, Failure, FailureKind};

pub mod sink;
pub use sink::{AuditFileSink, InMemorySink, MultiSink, NullSink, Sink};

pub mod chain;
pub use chain::Chain;

pub mod classify;
pub use classify::{ChainedClassifier, Classifier, FailureClassifier, FnClassifier};

pub mod watch;
pub use watch::{EscalationRouting, ScheduleWindow, TimeoutWatcher, WatchAction, WatchRule};

pub mod testing;

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
        (JobPhase::Failed { attempts: _ }, RetryDecide(RetryOutcome::Retry { until_ms })) => {
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

/// Typed receiver for `Job::Output` values. Jobs call `record` on a
/// successful `execute` so consumers (reconcile receipts, audit
/// trails, dashboards) can read the typed outcomes the scheduler's
/// phase-tracking discards.
///
/// Per `theory/SHIGOTO.md` §VIII (output capture) — the scheduler
/// itself doesn't hold sinks; Jobs carry them. This keeps the typed
/// `O` parameter from leaking into the scheduler's heterogeneous
/// `Vec<Box<dyn ErasedJob>>` storage (which would require type
/// erasure on the sink too). The cost: each Job impl decides which
/// sink (if any) to wire in, but the gain is full type safety from
/// producer to consumer.
///
/// Implementations:
/// - `NullSink<O>` — discards everything; default for Jobs that
///   don't care about output capture.
/// - `InMemorySink<O>` — stores into `Arc<Mutex<HashMap<JobId, O>>>`
///   so the consumer can drain after ticks complete.
/// - Both live in `shigoto-emit` alongside the `TransitionEmitter`
///   sinks, since the two surfaces are conceptually paired.
///
/// `record` takes `&O` so non-`Clone` Outputs are allowed at the
/// trait level. Concrete sinks that need owned values (`InMemorySink`)
/// add `O: Clone` at their `impl` boundary, not on the trait.
///
/// `record` is async because audit-style sinks may want to fsync or
/// push to a queue; in-memory sinks return immediately.
#[async_trait::async_trait]
pub trait OutputSink<O>: Send + Sync + 'static
where
    O: Send + Sync + 'static,
{
    /// Called by a Job from its `execute` method after computing a
    /// successful `Output`. The Job retains ownership; sinks that
    /// need storage should clone internally.
    ///
    /// `O: Sync` is required because the async-trait desugar captures
    /// `&O` across an `await` boundary — the resulting future is only
    /// `Send` when the borrowed reference is. Outputs that aren't
    /// `Sync` (rare for plain data; common for things like `Cell`)
    /// can't use the typed sink and must capture via a side channel.
    async fn record(&self, job_id: &JobId, output: &O);
}

/// Convenience trait that captures the most common Job authoring
/// shape across pleme-io consumers: a Job whose typed Output flows
/// through an [`OutputSink`] for consumer-side capture, and whose
/// identity decomposes into (scope, kind, subject).
///
/// Implementations write only the per-Job logic:
/// - `KIND` — the typed work-class id constant.
/// - `scope()` / `subject()` — the two non-kind coordinates of `JobId`.
/// - `output_sink()` — optional wired sink for output capture.
/// - `execute_body()` — the actual side-effecting work.
///
/// The blanket `impl<T: RecordingJob> Job for T` below derives:
/// - `Job::id()` — assembled from scope() + subject() + KIND.
/// - `Job::kind()` — `JobKindId::new(T::KIND)`.
/// - `Job::execute()` — calls `execute_body`, then records to the
///   sink (when present) before returning the typed Output.
///
/// Consumers either implement `Job` directly (full control) or
/// `RecordingJob` (the common case). Not both — the orphan rule +
/// the blanket impl mean implementing one excludes the other for
/// the same type.
#[async_trait::async_trait]
pub trait RecordingJob: Send + Sync + 'static {
    /// Typed work output. `Send + Sync + Clone` are needed so the
    /// blanket `Job::execute` can call `sink.record(&id, &output)`
    /// across an await boundary, and so `InMemorySink<O>` can hold
    /// owned copies.
    type Output: Send + Sync + Clone + 'static;

    /// Typed error. Same bounds as `Job::Error`.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Canonical kind id. Compile-time constant so two Jobs of the
    /// same impl always share the same `JobKindId` without an
    /// allocation per call.
    const KIND: &'static str;

    /// First coordinate of `JobId`. Implementations typically clone
    /// from `self.workspace` or similar.
    fn scope(&self) -> JobScope;

    /// Third coordinate of `JobId`. Implementations typically clone
    /// from `self.repo_name` or similar.
    fn subject(&self) -> JobSubject;

    /// Optional typed sink. `None` means outputs are dropped after
    /// execute returns; `Some` records every successful outcome.
    fn output_sink(&self) -> Option<&std::sync::Arc<dyn OutputSink<Self::Output>>>;

    /// The actual work. Run only on Ready→Running. MUST be idempotent.
    /// The blanket `Job::execute` wraps this with sink recording so
    /// callers never write the `if let Some(sink) = ... { sink.record... }`
    /// dance themselves.
    async fn execute_body(&self) -> Result<Self::Output, Self::Error>;
}

#[async_trait::async_trait]
impl<T: RecordingJob> Job for T {
    type Output = T::Output;
    type Error = T::Error;

    fn id(&self) -> JobId {
        JobId {
            scope: self.scope(),
            kind: JobKindId::new(T::KIND),
            subject: self.subject(),
        }
    }

    fn kind(&self) -> JobKindId {
        JobKindId::new(T::KIND)
    }

    async fn execute(&self) -> Result<T::Output, T::Error> {
        let outcome = self.execute_body().await?;
        if let Some(sink) = self.output_sink() {
            // Compute the JobId twice (once here, once via id() above).
            // Cheap — id() is pure data clones.
            let id = JobId {
                scope: self.scope(),
                kind: JobKindId::new(T::KIND),
                subject: self.subject(),
            };
            sink.record(&id, &outcome).await;
        }
        Ok(outcome)
    }
}

/// The typed Job trait — what every consumer's domain-specific job
/// implements. Per `theory/SHIGOTO.md` §III.1.
///
/// Constraints baked in:
/// - `'static + Send + Sync` — jobs may move between scheduler threads.
/// - Typed `Output` / `Error` — no untyped `Box<dyn Error>` in the
///   business-logic surface. Erased dispatch is `ErasedJob`.
/// - `execute` is async on tokio. Sync work uses `spawn_blocking`.
/// - `id()` and `kind()` are pure: scheduler reads them many times
///   per cycle; no IO.
/// - `execute` MUST be idempotent (§IV.2). A scheduler that crashes
///   between completing a side effect and emitting `Succeeded` will
///   re-invoke execute on the next cycle.
///
/// v0.1 omits Input + JobContext — they land when a real consumer
/// proves a need. For now: jobs hold their input in their own state
/// (`self`) and the scheduler manages cancellation/clock externally
/// (out-of-band via `tokio::time::timeout` + `CancellationToken`).
#[async_trait::async_trait]
pub trait Job: Send + Sync + 'static {
    type Output: Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Typed identity. Stable across cycles + scheduler restarts.
    fn id(&self) -> JobId;

    /// Typed work class.
    fn kind(&self) -> JobKindId;

    /// Side-effecting work. Called only on the `Ready → Running`
    /// transition (`Signal::AllocateBudget` → `advance` → Running).
    /// MUST be idempotent.
    async fn execute(&self) -> Result<Self::Output, Self::Error>;
}

/// Trait-object dispatch surface. The scheduler holds
/// `Box<dyn ErasedJob>` (`Job` itself isn't object-safe because of
/// the associated types); `ErasedJob` collapses the typed Output +
/// Error to `()` + boxed error so the scheduler can store
/// heterogeneous jobs in one DAG.
///
/// Blanket impl below gives every `T: Job` an `ErasedJob` view for
/// free — consumers write `impl Job for MyJob` and the scheduler
/// consumes it via `Box<dyn ErasedJob>` automatically.
#[async_trait::async_trait]
pub trait ErasedJob: Send + Sync + 'static {
    fn id(&self) -> JobId;
    fn kind(&self) -> JobKindId;
    async fn execute_erased(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

#[async_trait::async_trait]
impl<T: Job> ErasedJob for T {
    fn id(&self) -> JobId {
        <T as Job>::id(self)
    }

    fn kind(&self) -> JobKindId {
        <T as Job>::kind(self)
    }

    async fn execute_erased(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match <T as Job>::execute(self).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Box::new(e)),
        }
    }
}

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

impl Snapshot {
    /// Derived view: every job currently in {Failed, Retrying,
    /// Deadlettered}. Per `theory/SHIGOTO.md` §VII.2 — broader than
    /// `TickReceipt.unhealed` (which is just Deadlettered +
    /// WaitingForOperator) because it includes the transient retry
    /// states. Useful for debugging "what's currently failing across
    /// the fleet?" without waiting for jobs to deadletter.
    ///
    /// Returns owned tuples (cloned JobId + phase) so consumers can
    /// hold the result across snapshot drops.
    #[must_use]
    pub fn failure_set(&self) -> Vec<(JobId, JobPhase)> {
        self.phases
            .iter()
            .filter(|(_, p)| {
                matches!(
                    p,
                    JobPhase::Failed { .. }
                        | JobPhase::Retrying { .. }
                        | JobPhase::Deadlettered
                )
            })
            .map(|(id, p)| (id.clone(), p.clone()))
            .collect()
    }

    /// Count of jobs in each named phase. Stable ordering. Useful for
    /// receipts + dashboards.
    #[must_use]
    pub fn phase_counts(&self) -> std::collections::BTreeMap<&'static str, u32> {
        let mut counts: std::collections::BTreeMap<&'static str, u32> =
            std::collections::BTreeMap::new();
        for phase in self.phases.values() {
            let key = match phase {
                JobPhase::Pending => "pending",
                JobPhase::Gated => "gated",
                JobPhase::Ready => "ready",
                JobPhase::Running => "running",
                JobPhase::Succeeded => "succeeded",
                JobPhase::Failed { .. } => "failed",
                JobPhase::Retrying { .. } => "retrying",
                JobPhase::Skipped(_) => "skipped",
                JobPhase::Deadlettered => "deadlettered",
                JobPhase::WaitingForOperator => "waiting-for-operator",
            };
            *counts.entry(key).or_insert(0) += 1;
        }
        counts
    }
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

#[cfg(test)]
mod job_tests {
    use super::*;

    /// Sample Job impl — a no-op that succeeds. Verifies the trait
    /// shape compiles + the ErasedJob blanket impl gives us
    /// trait-object dispatch.
    struct NoopJob;

    #[derive(thiserror::Error, Debug)]
    #[error("noop")]
    struct NoopError;

    #[async_trait::async_trait]
    impl Job for NoopJob {
        type Output = ();
        type Error = NoopError;

        fn id(&self) -> JobId {
            JobId {
                scope: JobScope::Global,
                kind: JobKindId::new("noop"),
                subject: JobSubject::None,
            }
        }

        fn kind(&self) -> JobKindId {
            JobKindId::new("noop")
        }

        async fn execute(&self) -> Result<(), NoopError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn job_trait_compiles_and_executes() {
        let j = NoopJob;
        assert_eq!(<NoopJob as Job>::id(&j).kind.0, "noop");
        assert!(j.execute().await.is_ok());
    }

    #[tokio::test]
    async fn erased_job_blanket_impl_gives_trait_object() {
        let j: Box<dyn ErasedJob> = Box::new(NoopJob);
        assert_eq!(j.id().kind.0, "noop");
        assert!(j.execute_erased().await.is_ok());
    }

    // ── RecordingJob tests ──────────────────────────────────────────

    use std::sync::Arc;
    use std::sync::Mutex;

    /// Simple in-memory sink used to verify the blanket `Job::execute`
    /// records outputs after `execute_body` succeeds.
    #[derive(Default)]
    struct CaptureSink<O: Clone + Send + Sync + 'static> {
        records: Mutex<Vec<(JobId, O)>>,
    }

    #[async_trait::async_trait]
    impl<O: Clone + Send + Sync + 'static> OutputSink<O> for CaptureSink<O> {
        async fn record(&self, job_id: &JobId, output: &O) {
            self.records
                .lock()
                .expect("CaptureSink mutex poisoned")
                .push((job_id.clone(), output.clone()));
        }
    }

    /// Reference impl exercising every `RecordingJob` callback.
    struct RecJob {
        scope: JobScope,
        subject: JobSubject,
        sink: Option<Arc<dyn OutputSink<u32>>>,
        answer: u32,
    }

    #[async_trait::async_trait]
    impl RecordingJob for RecJob {
        type Output = u32;
        type Error = NoopError;
        const KIND: &'static str = "test-recording";

        fn scope(&self) -> JobScope {
            self.scope.clone()
        }
        fn subject(&self) -> JobSubject {
            self.subject.clone()
        }
        fn output_sink(&self) -> Option<&Arc<dyn OutputSink<Self::Output>>> {
            self.sink.as_ref()
        }
        async fn execute_body(&self) -> Result<u32, NoopError> {
            Ok(self.answer)
        }
    }

    #[tokio::test]
    async fn recording_job_blanket_provides_job_id_and_kind() {
        let job = RecJob {
            scope: JobScope::Workspace("ws".into()),
            subject: JobSubject::Repo("r".into()),
            sink: None,
            answer: 1,
        };
        let id = <RecJob as Job>::id(&job);
        assert_eq!(id.kind.0, "test-recording");
        match id.scope {
            JobScope::Workspace(w) => assert_eq!(w, "ws"),
            _ => panic!("wrong scope"),
        }
        match id.subject {
            JobSubject::Repo(r) => assert_eq!(r, "r"),
            _ => panic!("wrong subject"),
        }
        let kind = <RecJob as Job>::kind(&job);
        assert_eq!(kind.0, "test-recording");
    }

    #[tokio::test]
    async fn recording_job_blanket_execute_records_to_sink_on_success() {
        let sink: Arc<CaptureSink<u32>> = Arc::new(CaptureSink::default());
        let sink_dyn: Arc<dyn OutputSink<u32>> = sink.clone();
        let job = RecJob {
            scope: JobScope::Global,
            subject: JobSubject::None,
            sink: Some(sink_dyn),
            answer: 42,
        };
        let result = <RecJob as Job>::execute(&job).await.unwrap();
        assert_eq!(result, 42);

        let recs = sink.records.lock().unwrap();
        assert_eq!(recs.len(), 1, "sink should have captured one record");
        assert_eq!(recs[0].1, 42);
    }

    #[tokio::test]
    async fn recording_job_without_sink_skips_recording() {
        let job = RecJob {
            scope: JobScope::Global,
            subject: JobSubject::None,
            sink: None,
            answer: 7,
        };
        // execute returns Ok with the typed Output; no sink to verify
        // against — just confirm the absent-sink path doesn't panic.
        let result = <RecJob as Job>::execute(&job).await.unwrap();
        assert_eq!(result, 7);
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use std::collections::HashMap;

    fn id(name: &str) -> JobId {
        JobId {
            scope: JobScope::Global,
            kind: JobKindId::new("k"),
            subject: JobSubject::Pinned(name.into()),
        }
    }

    fn snapshot_with(entries: Vec<(&str, JobPhase)>) -> Snapshot {
        let mut phases: HashMap<JobId, JobPhase> = HashMap::new();
        for (name, phase) in entries {
            phases.insert(id(name), phase);
        }
        Snapshot { phases }
    }

    #[test]
    fn failure_set_includes_failed_retrying_deadlettered() {
        let s = snapshot_with(vec![
            ("ok", JobPhase::Succeeded),
            ("dead", JobPhase::Deadlettered),
            ("flap", JobPhase::Failed { attempts: 2 }),
            ("waiting", JobPhase::WaitingForOperator),
            ("retry", JobPhase::Retrying { until_ms: 0 }),
            ("ready", JobPhase::Ready),
        ]);
        let fs = s.failure_set();
        let names: std::collections::HashSet<String> = fs
            .iter()
            .filter_map(|(id, _)| match &id.subject {
                JobSubject::Pinned(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(names.len(), 3);
        assert!(names.contains("dead"));
        assert!(names.contains("flap"));
        assert!(names.contains("retry"));
        // WaitingForOperator does NOT appear (it's unhealed, not
        // a failure per §VII.2).
        assert!(!names.contains("waiting"));
        // Ok / Ready don't appear either.
        assert!(!names.contains("ok"));
        assert!(!names.contains("ready"));
    }

    #[test]
    fn phase_counts_summarizes_every_phase() {
        let s = snapshot_with(vec![
            ("a", JobPhase::Pending),
            ("b", JobPhase::Pending),
            ("c", JobPhase::Succeeded),
            ("d", JobPhase::Deadlettered),
        ]);
        let counts = s.phase_counts();
        assert_eq!(counts.get("pending"), Some(&2));
        assert_eq!(counts.get("succeeded"), Some(&1));
        assert_eq!(counts.get("deadlettered"), Some(&1));
        // Absent phases don't appear (no 0-counts).
        assert!(counts.get("ready").is_none());
    }
}
