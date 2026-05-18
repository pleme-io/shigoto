//! shigoto-gate — typed precondition Gate trait + standard gates.
//!
//! Spec: `theory/SHIGOTO.md` §III.9. Gates are PURE — they evaluate
//! against the scheduler snapshot without IO. IO-dependent gating is
//! itself a Job that produces a typed fact a downstream gate checks.

#![forbid(unsafe_code)]

use shigoto_types::{JobId, SkipReason, Snapshot};

pub trait Gate: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn evaluate(&self, job: &JobId, snapshot: &Snapshot) -> GateOutcome;
}

#[derive(Debug, Clone)]
pub enum GateOutcome {
    Pass,
    Wait,
    Skip(SkipReason),
}
