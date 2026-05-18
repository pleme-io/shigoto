//! shigoto (仕事) — the typed job-system primitive.
//!
//! Umbrella crate. Re-exports the public surface of every sibling so
//! consumers depend on `shigoto = "0.1"` and reach the full algebra
//! via `use shigoto::{Job, JobId, JobPhase, Dag, Scheduler, ...};`.
//!
//! Canonical spec: `theory/SHIGOTO.md`.
//! Theory frame: `theory/THEORY.md` §IV (Motion).

#![forbid(unsafe_code)]

pub use shigoto_budget::{BudgetError, BudgetSpec, BudgetTree};
pub use shigoto_dag::{Dag, DagError};
pub use shigoto_emit::TransitionEmitter;
pub use shigoto_gate::{Gate, GateOutcome};
pub use shigoto_retry::{FailureRecord, RetryDecider, RetryDecision, RetryPolicy};
pub use shigoto_scheduler::{InProcessScheduler, Scheduler, SchedulerError};
pub use shigoto_types::{
    JobError, JobId, JobInput, JobKindId, JobOutput, JobPhase, JobScope, JobSubject, SkipReason,
    Snapshot, TickReceipt, TransitionEvent, TransitionReason, UnhealedDrift,
};
