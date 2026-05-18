//! shigoto-budget — typed three-dimension parallelism envelope.
//!
//! Spec: `theory/SHIGOTO.md` §III.7 + §VI. Composition is
//! min-intersection: a job runs iff every applicable budget (global,
//! by-kind, by-scope) has slack.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

use shigoto_types::{JobKindId, JobScope};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetSpec {
    pub max_concurrent: u32,
    pub max_failures_per_minute: u32,
    pub queue_depth: u32,
}

#[derive(Debug, Default)]
pub struct BudgetTree {
    pub global: Option<BudgetSpec>,
    pub by_kind: std::collections::HashMap<JobKindId, BudgetSpec>,
    pub by_scope: std::collections::HashMap<JobScope, BudgetSpec>,
}

#[derive(thiserror::Error, Debug)]
pub enum BudgetError {
    #[error("budget exhausted (global)")]
    GlobalExhausted,
    #[error("budget exhausted (kind={0})")]
    KindExhausted(String),
    #[error("budget exhausted (scope)")]
    ScopeExhausted,
}
