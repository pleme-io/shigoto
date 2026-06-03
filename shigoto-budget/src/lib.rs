//! shigoto-budget — typed three-dimension parallelism envelope.
//!
//! Spec: `theory/SHIGOTO.md` §III.7 + §VI. Composition is
//! min-intersection: a job runs iff every applicable budget (global,
//! by-kind, by-scope) has slack. v0.1 ships the pure allocate /
//! release primitive; scheduler integration lands in M0.9j.

#![forbid(unsafe_code)]

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use shigoto_types::{JobId, JobKindId, JobScope};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetSpec {
    pub max_concurrent: u32,
    pub max_failures_per_minute: u32,
    pub queue_depth: u32,
}

impl BudgetSpec {
    /// Convenience for tests + simple cases — max_concurrent set;
    /// other dimensions unbounded (u32::MAX).
    #[must_use]
    pub fn max_concurrent(n: u32) -> Self {
        Self {
            max_concurrent: n,
            max_failures_per_minute: u32::MAX,
            queue_depth: u32::MAX,
        }
    }
}

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum BudgetError {
    #[error("global budget exhausted")]
    GlobalExhausted,
    #[error("kind budget exhausted ({0})")]
    KindExhausted(String),
    #[error("scope budget exhausted")]
    ScopeExhausted,
}

/// Three-dimension budget envelope. Allocation checks every applicable
/// limit (min-intersection): a job runs iff all three have slack.
#[derive(Debug, Default)]
pub struct BudgetTree {
    /// Whole-scheduler cap. `None` = unbounded.
    pub global: Option<BudgetSpec>,
    /// Per-kind caps. Absent kind = unbounded.
    pub by_kind: HashMap<JobKindId, BudgetSpec>,
    /// Per-scope caps. Absent scope = unbounded.
    pub by_scope: HashMap<JobScope, BudgetSpec>,

    // ── Live counters (mutated by try_allocate / release) ──────
    running_global: u32,
    running_by_kind: HashMap<JobKindId, u32>,
    running_by_scope: HashMap<JobScope, u32>,
}

impl BudgetTree {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Reserve a slot for `id` across all three dimensions. On
    /// failure, no counter is modified — the call is atomic. On
    /// success, every applicable counter is incremented.
    pub fn try_allocate(&mut self, id: &JobId) -> Result<(), BudgetError> {
        // ── Check every dimension before mutating any counter ──
        if let Some(spec) = &self.global {
            if self.running_global >= spec.max_concurrent {
                return Err(BudgetError::GlobalExhausted);
            }
        }
        if let Some(spec) = self.by_kind.get(&id.kind) {
            let running = self.running_by_kind.get(&id.kind).copied().unwrap_or(0);
            if running >= spec.max_concurrent {
                return Err(BudgetError::KindExhausted(id.kind.0.clone()));
            }
        }
        if let Some(spec) = self.by_scope.get(&id.scope) {
            let running = self.running_by_scope.get(&id.scope).copied().unwrap_or(0);
            if running >= spec.max_concurrent {
                return Err(BudgetError::ScopeExhausted);
            }
        }

        // ── All checks passed — increment counters ─────────────
        self.running_global = self.running_global.saturating_add(1);
        *self.running_by_kind.entry(id.kind.clone()).or_insert(0) += 1;
        *self.running_by_scope.entry(id.scope.clone()).or_insert(0) += 1;
        Ok(())
    }

    /// Release a slot held by `id`. Symmetric to try_allocate.
    /// Saturating: double-release doesn't underflow.
    pub fn release(&mut self, id: &JobId) {
        self.running_global = self.running_global.saturating_sub(1);
        if let Some(c) = self.running_by_kind.get_mut(&id.kind) {
            *c = c.saturating_sub(1);
        }
        if let Some(c) = self.running_by_scope.get_mut(&id.scope) {
            *c = c.saturating_sub(1);
        }
    }

    /// Current global running count (for observability).
    #[must_use]
    pub fn running_global(&self) -> u32 {
        self.running_global
    }

    /// Current running count for a JobKind.
    #[must_use]
    pub fn running_kind(&self, kind: &JobKindId) -> u32 {
        self.running_by_kind.get(kind).copied().unwrap_or(0)
    }

    /// Current running count for a JobScope.
    #[must_use]
    pub fn running_scope(&self, scope: &JobScope) -> u32 {
        self.running_by_scope.get(scope).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shigoto_types::{JobKindId, JobScope, JobSubject};

    fn mk_id(kind: &str, scope: JobScope, subject: &str) -> JobId {
        JobId {
            scope,
            kind: JobKindId::new(kind),
            subject: JobSubject::Pinned(subject.into()),
        }
    }

    #[test]
    fn empty_budget_allocates_freely() {
        let mut b = BudgetTree::new();
        let id = mk_id("k", JobScope::Global, "x");
        assert!(b.try_allocate(&id).is_ok());
        assert_eq!(b.running_global(), 1);
    }

    #[test]
    fn global_limit_blocks_third_allocation() {
        let mut b = BudgetTree::new();
        b.global = Some(BudgetSpec::max_concurrent(2));
        let a = mk_id("k", JobScope::Global, "a");
        let bb = mk_id("k", JobScope::Global, "b");
        let c = mk_id("k", JobScope::Global, "c");
        assert!(b.try_allocate(&a).is_ok());
        assert!(b.try_allocate(&bb).is_ok());
        assert_eq!(b.try_allocate(&c), Err(BudgetError::GlobalExhausted));
        assert_eq!(b.running_global(), 2);
    }

    #[test]
    fn kind_limit_isolates_kinds() {
        let mut b = BudgetTree::new();
        b.by_kind
            .insert(JobKindId::new("hot"), BudgetSpec::max_concurrent(1));
        let h1 = mk_id("hot", JobScope::Global, "x");
        let h2 = mk_id("hot", JobScope::Global, "y");
        let cold = mk_id("cold", JobScope::Global, "z");
        assert!(b.try_allocate(&h1).is_ok());
        assert_eq!(
            b.try_allocate(&h2),
            Err(BudgetError::KindExhausted("hot".into()))
        );
        // Other kind unaffected.
        assert!(b.try_allocate(&cold).is_ok());
    }

    #[test]
    fn scope_limit_isolates_scopes() {
        let mut b = BudgetTree::new();
        let ws = JobScope::Workspace("pleme-io".into());
        b.by_scope.insert(ws.clone(), BudgetSpec::max_concurrent(1));
        let a = mk_id("k", ws.clone(), "a");
        let bb = mk_id("k", ws.clone(), "b");
        let other = mk_id("k", JobScope::Global, "c");
        assert!(b.try_allocate(&a).is_ok());
        assert_eq!(b.try_allocate(&bb), Err(BudgetError::ScopeExhausted));
        // Different scope unaffected.
        assert!(b.try_allocate(&other).is_ok());
    }

    #[test]
    fn min_intersection_narrowest_wins() {
        // Global=10, kind=1, scope=5. The narrowest (kind=1) binds.
        let mut b = BudgetTree::new();
        b.global = Some(BudgetSpec::max_concurrent(10));
        b.by_kind
            .insert(JobKindId::new("k"), BudgetSpec::max_concurrent(1));
        let ws = JobScope::Workspace("pleme-io".into());
        b.by_scope.insert(ws.clone(), BudgetSpec::max_concurrent(5));

        let a = mk_id("k", ws.clone(), "a");
        let bb = mk_id("k", ws.clone(), "b");
        assert!(b.try_allocate(&a).is_ok());
        // Second alloc fails — kind=1 binds even though global/scope have slack.
        assert_eq!(
            b.try_allocate(&bb),
            Err(BudgetError::KindExhausted("k".into()))
        );
    }

    #[test]
    fn release_decrements_all_three_counters() {
        let mut b = BudgetTree::new();
        b.global = Some(BudgetSpec::max_concurrent(1));
        let id = mk_id("k", JobScope::Global, "x");
        assert!(b.try_allocate(&id).is_ok());
        // Now exhausted.
        assert_eq!(b.try_allocate(&id), Err(BudgetError::GlobalExhausted));
        b.release(&id);
        // Slot freed.
        assert!(b.try_allocate(&id).is_ok());
    }

    #[test]
    fn release_is_saturating_no_underflow() {
        let mut b = BudgetTree::new();
        let id = mk_id("k", JobScope::Global, "x");
        // Release without allocate — should not panic / underflow.
        b.release(&id);
        b.release(&id);
        assert_eq!(b.running_global(), 0);
    }

    #[test]
    fn failed_allocation_does_not_mutate_counters() {
        // Verify atomicity: if any dimension fails, no counter changes.
        let mut b = BudgetTree::new();
        b.by_kind
            .insert(JobKindId::new("k"), BudgetSpec::max_concurrent(0));

        let id = mk_id("k", JobScope::Global, "x");
        assert!(b.try_allocate(&id).is_err());
        // Global counter still 0 even though we got past the global
        // check before failing the kind check.
        assert_eq!(b.running_global(), 0);
    }
}
