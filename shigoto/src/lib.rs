//! shigoto (仕事) — the typed job-system primitive.
//!
//! Umbrella crate. Re-exports the public surface of every sibling so
//! consumers depend on `shigoto = "0.1"` and reach the full algebra
//! via `use shigoto::{Job, JobId, Scheduler, ...};` without naming
//! every sub-crate.
//!
//! Canonical spec: `theory/SHIGOTO.md`.
//! Theory frame: `theory/THEORY.md` §IV (Motion).

#![forbid(unsafe_code)]

// ── Budget (HOW MUCH runs at once) ──────────────────────────────────────
pub use shigoto_budget::{BudgetError, BudgetSpec, BudgetTree};

// ── Rank (WHICH runs next; anti-starvation ordering) ────────────────────
pub use shigoto_rank::{PriorityClass, Schedulable, UrgencyWeights, pick, rank};

// ── FSM (typed-lifecycle convergence proofs) ────────────────────────────
pub use shigoto_fsm::{ConvergentFsm, DefectKind, FsmDefect, assert_convergent_fsm};

// ── DAG ───────────────────────────────────────────────────────────────
pub use shigoto_dag::{Dag, DagError};

// ── Emit ──────────────────────────────────────────────────────────────
pub use shigoto_emit::{
    AuditFileEmitter, InMemorySink, MultiEmitter, NullEmitter, NullSink, TransitionEmitter,
};

// ── Gate ──────────────────────────────────────────────────────────────
pub use shigoto_gate::{
    self, AllUpstreamsTerminal, Gate, GateContext, GateOutcome, OperatorApproved,
};

// ── Retry ─────────────────────────────────────────────────────────────
pub use shigoto_retry::{FailureRecord, RetryDecider, RetryDecision, RetryPolicy};

// ── Scheduler ─────────────────────────────────────────────────────────
pub use shigoto_scheduler::{
    Clock, FixedClock, InProcessScheduler, JsonFileSchedulerStore, Scheduler, SchedulerError,
    SchedulerSnapshot, SchedulerStore, SchedulerStoreError, StoredJobState, SystemClock,
};

// ── Types (the foundation) ────────────────────────────────────────────
pub use shigoto_types::{
    ErasedJob, GateAggregate, IllegalTransition, Job, JobError, JobId, JobInput, JobKindId,
    JobOutput, JobPhase, JobScope, JobSubject, OutputSink, RecordingJob, RetryOutcome, Signal,
    SkipReason, Snapshot, TickReceipt, TransitionEvent, TransitionReason, UnhealedDrift, advance,
};

#[cfg(test)]
mod tests {
    //! Smoke tests that prove the canonical consumer import works:
    //! `use shigoto::{...}` reaches every concept needed to build a
    //! Job + register it + tick the scheduler.

    use super::*;
    use std::sync::Arc;

    #[derive(thiserror::Error, Debug)]
    #[error("smoke")]
    struct SmokeErr;

    struct SmokeJob;

    #[async_trait::async_trait]
    impl Job for SmokeJob {
        type Output = ();
        type Error = SmokeErr;
        fn id(&self) -> JobId {
            JobId {
                scope: JobScope::Global,
                kind: JobKindId::new("smoke"),
                subject: JobSubject::None,
            }
        }
        fn kind(&self) -> JobKindId {
            JobKindId::new("smoke")
        }
        async fn execute(&self) -> Result<(), SmokeErr> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn umbrella_consumer_reaches_full_surface() {
        // Every type used here is re-exported by `shigoto::*` so a
        // consumer outside this workspace can write the exact same
        // imports.
        let scheduler =
            InProcessScheduler::new("umbrella-smoke").with_emitter(Arc::new(NullEmitter::new()));
        let job = Arc::new(SmokeJob);
        let id = <SmokeJob as Job>::id(&job);
        scheduler.register_job(job).await;

        let mut dag = Dag::new();
        dag.ensure_node(id.clone());
        let receipt: TickReceipt = scheduler.tick(&mut dag).await.unwrap();
        // Snapshot via the trait method — Scheduler::snapshot lands
        // on the umbrella re-export too.
        let snap: Snapshot = scheduler.snapshot(&dag).await;
        assert_eq!(snap.phases.get(&id), Some(&JobPhase::Succeeded));
        assert!(receipt.phase_counts.contains_key("succeeded"));
    }
}
