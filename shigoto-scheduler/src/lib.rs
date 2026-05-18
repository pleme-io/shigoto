//! shigoto-scheduler — the typed runtime.
//!
//! Spec: `theory/SHIGOTO.md` §III.6. A `Scheduler` is NOT a daemon —
//! one `tick` per call. Daemons loop `tick → wait_for_change → tick`;
//! K8s reconcilers map each CR event to a single `tick`. v0.1.0 ships
//! `InProcessScheduler`; future impls (persistent, distributed,
//! replayable) plug behind the trait.

#![forbid(unsafe_code)]

use async_trait::async_trait;

use shigoto_dag::Dag;
use shigoto_types::{Snapshot, TickReceipt};

#[derive(thiserror::Error, Debug)]
pub enum SchedulerError {
    #[error("dag mutation rejected: {0}")]
    DagMutationRejected(String),
    #[error("illegal transition: {0}")]
    IllegalTransition(String),
}

#[async_trait]
pub trait Scheduler: Send + Sync {
    /// Drive the Dag one tick forward.
    async fn tick(&self, dag: &mut Dag) -> Result<TickReceipt, SchedulerError>;

    /// Read-only snapshot of the current FSM map.
    fn snapshot(&self, dag: &Dag) -> Snapshot;
}

/// Default in-process scheduler. v0.1.0 scaffold — implementation
/// lifts from `tend/src/operator/{planner, apply, failure_set}.rs`
/// at M0.8 of the broader plan.
#[derive(Debug, Default)]
pub struct InProcessScheduler {
    _placeholder: (),
}

impl InProcessScheduler {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}
