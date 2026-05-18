//! shigoto-dag — typed dependency graph over Jobs.
//!
//! Spec: `theory/SHIGOTO.md` §III.5 + §V.
//!
//! v0.1.0 scaffold — full implementation lifts from
//! `tend/src/operator/dag.rs` during M0.7 of the broader plan.

#![forbid(unsafe_code)]

use shigoto_types::JobId;

#[derive(thiserror::Error, Debug)]
pub enum DagError {
    #[error("cycle in DAG at {0:?}")]
    Cycle(JobId),
    #[error("duplicate job: {0:?}")]
    DuplicateJob(JobId),
    #[error("edge to non-existent job: {0:?}")]
    DanglingEdge(JobId),
}

/// Typed DAG of JobIds. Edges declare "to may not start until from
/// reaches a terminal phase" (Succeeded | Skipped | Deadlettered).
#[derive(Debug, Default)]
pub struct Dag {
    // Implementation lifts from tend/src/operator/dag.rs at M0.7.
    // Placeholder shape preserves the public surface.
    _placeholder: (),
}

impl Dag {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}
