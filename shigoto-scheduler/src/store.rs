//! Durable persistence for [`crate::InProcessScheduler`] FSM state.
//!
//! `theory/SHIGOTO.md` names in-memory-only state as a standing v0.1 gap
//! (`InProcessScheduler`'s own doc comment: "a scheduler restart drops every
//! non-terminal job back to Pending"). This module closes it the same way
//! `formigueiro`'s `FilePlanStore` closes the analogous gap for its
//! `PlanStore` trait: one typed `SchedulerStore` trait scoped to shigoto's
//! own `Job`/`Dag` state shape, plus a `JsonFileSchedulerStore` reference
//! impl using the same atomic tmp-file + rename write technique.
//!
//! What gets persisted is deliberately narrow — **FSM progress, not the
//! executable graph.** A `Job` is a `Box<dyn ErasedJob>` (arbitrary
//! executable behavior), a `Gate` closes over arbitrary predicates, and
//! `RetryPolicy::Custom` can wrap an arbitrary `Arc<dyn RetryDecider>` — none
//! of that survives serialization, and none of it should: the *consumer*
//! reconstructs the DAG and re-registers Jobs/Gates/RetryPolicies on
//! startup (that's ordinary code, not state). What genuinely needs to
//! survive a restart is *where each job had gotten to* — its `JobPhase`,
//! attempt count, and failure history — so a resumed scheduler picks up
//! mid-flight instead of re-running every job from `Pending`.
//!
//! JSON object keys must be strings, so (mirroring `formigueiro::StoredEntry`,
//! which hits the exact same constraint for its `(kind, subject)` tuple key)
//! the snapshot is a flat `Vec<StoredJobState>` rather than a
//! `HashMap<JobId, _>` — `JobId` is a struct, not a string.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use shigoto_retry::FailureRecord;
use shigoto_types::{JobId, JobPhase};

/// One persisted job's FSM progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredJobState {
    pub id: JobId,
    pub phase: JobPhase,
    pub attempts: u32,
    pub failure_history: Vec<FailureRecord>,
}

/// The subset of `InProcessScheduler` state that survives a restart. See
/// the module doc for what's deliberately excluded (Jobs, Gates,
/// RetryPolicies — the executable graph, re-registered by the consumer).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchedulerSnapshot {
    pub jobs: Vec<StoredJobState>,
}

impl SchedulerSnapshot {
    /// Number of jobs carried in this snapshot.
    #[must_use]
    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    /// `true` iff the snapshot carries no jobs.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SchedulerStoreError {
    #[error("scheduler store io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("scheduler store serialize error: {0}")]
    Serde(#[from] serde_json::Error),
}

/// A store for scheduler FSM state across restarts. A trait so the
/// reference `JsonFileSchedulerStore` (workstation / single-process
/// daemon) and a future durable impl (operator CRD / Postgres, per
/// MAGMA-NATIVE's "in-memory + DB-persisted, never the pod filesystem"
/// destination) share one contract. Mirrors `formigueiro::PlanStore`'s
/// shape one repo over.
pub trait SchedulerStore: Send + Sync {
    /// Load the last-persisted snapshot. A store with nothing persisted
    /// yet (fresh install, first run) returns an empty snapshot, not an
    /// error — only a genuine I/O or parse failure the caller should
    /// know about returns `Err`.
    fn load(&self) -> Result<SchedulerSnapshot, SchedulerStoreError>;

    /// Persist `snapshot`, replacing whatever was stored before.
    fn save(&self, snapshot: &SchedulerSnapshot) -> Result<(), SchedulerStoreError>;
}

/// A [`SchedulerStore`] backed by a single JSON file, written atomically
/// (write to a `.tmp` sibling, then `rename` over the real path) so a
/// crash mid-write never leaves a half-written, unparseable snapshot
/// behind — the same technique `formigueiro::FilePlanStore` uses for its
/// per-target state file.
///
/// A missing file loads as an empty snapshot (first run); a *corrupt*
/// file is a genuine `Err` (unlike `FilePlanStore`, which degrades a
/// corrupt file to empty) — scheduler state loss is consequential enough
/// (silently re-running every in-flight job from `Pending`) that the
/// caller should decide how to handle it rather than have it happen
/// silently.
#[derive(Debug, Clone)]
pub struct JsonFileSchedulerStore {
    path: PathBuf,
}

impl JsonFileSchedulerStore {
    /// A store backed by `path`. Doesn't touch the filesystem until
    /// `load`/`save` is called.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl SchedulerStore for JsonFileSchedulerStore {
    fn load(&self) -> Result<SchedulerSnapshot, SchedulerStoreError> {
        match std::fs::read_to_string(&self.path) {
            Ok(json) => Ok(serde_json::from_str(&json)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(SchedulerSnapshot::default()),
            Err(e) => Err(e.into()),
        }
    }

    fn save(&self, snapshot: &SchedulerSnapshot) -> Result<(), SchedulerStoreError> {
        if let Some(dir) = self.path.parent() {
            if !dir.as_os_str().is_empty() {
                std::fs::create_dir_all(dir)?;
            }
        }
        let tmp = self.path.with_extension("json.tmp");
        {
            let mut file = std::fs::File::create(&tmp)?;
            serde_json::to_writer(&mut file, snapshot)?;
            file.flush()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

/// Project a scheduler's live FSM maps into a persistable snapshot. Kept
/// as a free function (rather than inlined at the one call site in
/// `InProcessScheduler::snapshot_state`) so the projection logic — which
/// fields matter, what the default is for a job with no attempts/history
/// yet — has exactly one place to read and one place to test.
pub(crate) fn project(
    phases: &HashMap<JobId, JobPhase>,
    attempts: &HashMap<JobId, u32>,
    failure_history: &HashMap<JobId, Vec<FailureRecord>>,
) -> SchedulerSnapshot {
    let jobs = phases
        .iter()
        .map(|(id, phase)| StoredJobState {
            id: id.clone(),
            phase: phase.clone(),
            attempts: attempts.get(id).copied().unwrap_or(0),
            failure_history: failure_history.get(id).cloned().unwrap_or_default(),
        })
        .collect();
    SchedulerSnapshot { jobs }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shigoto_types::{JobKindId, JobScope, JobSubject};

    fn id(subject: &str) -> JobId {
        JobId {
            scope: JobScope::Global,
            kind: JobKindId::new("test"),
            subject: JobSubject::Pinned(subject.into()),
        }
    }

    #[test]
    fn a_written_snapshot_survives_a_reload_across_a_simulated_restart() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("scheduler-state.json");

        let snapshot = SchedulerSnapshot {
            jobs: vec![StoredJobState {
                id: id("alpha"),
                phase: JobPhase::Retrying { until_ms: 12_345 },
                attempts: 2,
                failure_history: vec![FailureRecord::new(1, 1_000, "boom")],
            }],
        };

        {
            let store = JsonFileSchedulerStore::new(path.clone());
            store.save(&snapshot).unwrap();
        } // drop -- simulates the process exiting

        // A fresh store instance (a fresh process, in spirit) reloads the
        // exact same state.
        let reloaded = JsonFileSchedulerStore::new(path).load().unwrap();
        assert_eq!(reloaded.len(), 1);
        assert_eq!(reloaded.jobs[0].id, id("alpha"));
        assert_eq!(
            reloaded.jobs[0].phase,
            JobPhase::Retrying { until_ms: 12_345 }
        );
        assert_eq!(reloaded.jobs[0].attempts, 2);
        assert_eq!(reloaded.jobs[0].failure_history.len(), 1);
        assert_eq!(reloaded.jobs[0].failure_history[0].error, "boom");
    }

    #[test]
    fn a_missing_file_loads_empty_not_an_error() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("does-not-exist.json");
        let snapshot = JsonFileSchedulerStore::new(path).load().unwrap();
        assert!(snapshot.is_empty());
    }

    #[test]
    fn a_corrupt_file_is_a_genuine_error_not_a_silent_empty_default() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("corrupt.json");
        std::fs::write(&path, b"{ not json").unwrap();
        let result = JsonFileSchedulerStore::new(path).load();
        assert!(
            result.is_err(),
            "corrupt scheduler state must surface, never silently reset to empty"
        );
    }

    #[test]
    fn save_creates_missing_parent_directories() {
        let dir = tempfile::tempdir().expect("temp dir");
        let nested = dir.path().join("a").join("b").join("state.json");
        let store = JsonFileSchedulerStore::new(nested.clone());
        store.save(&SchedulerSnapshot::default()).unwrap();
        assert!(nested.exists());
    }

    #[test]
    fn project_fills_in_zero_and_empty_defaults_for_untracked_jobs() {
        let mut phases = HashMap::new();
        phases.insert(id("no-history-yet"), JobPhase::Pending);
        let attempts = HashMap::new();
        let failure_history = HashMap::new();

        let snapshot = project(&phases, &attempts, &failure_history);
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot.jobs[0].attempts, 0);
        assert!(snapshot.jobs[0].failure_history.is_empty());
    }
}
