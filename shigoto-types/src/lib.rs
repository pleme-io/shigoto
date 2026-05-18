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
