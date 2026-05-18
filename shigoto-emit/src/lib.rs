//! shigoto-emit — typed transition emitter.
//!
//! Spec: `theory/SHIGOTO.md` §III.10 + §VIII. Every FSM transition
//! emits one event; sinks compose. The transition log is the
//! canonical history — `tend report`, K8s controller status,
//! observability dashboards, MCP operator surface all read the same
//! log. No second source of truth.

#![forbid(unsafe_code)]

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use shigoto_types::TransitionEvent;

pub trait TransitionEmitter: Send + Sync {
    fn emit(&self, event: TransitionEvent);
}

/// No-op emitter — the default for tests + consumers without
/// observability wired up. Sinks should compose via MultiEmitter
/// instead of stubbing this in production.
#[derive(Debug, Default)]
pub struct NullEmitter;

impl TransitionEmitter for NullEmitter {
    fn emit(&self, _event: TransitionEvent) {
        // intentional no-op
    }
}

/// Append-only JSONL audit file. One event per line. Same shape as
/// tend's existing `audit.rs` so operators can grep both with the
/// same tooling.
///
/// v0.1 is synchronous (write-on-emit with a Mutex). Async upgrade to
/// a background flush task lands when the synchronous path becomes a
/// bottleneck (per §VIII.1's non-blocking requirement).
pub struct AuditFileEmitter {
    path: PathBuf,
    /// Write serialized through a Mutex so concurrent emit() calls
    /// produce interleaved (not interleaved-mid-line) JSONL output.
    /// Holds the open File handle so we don't reopen per-emit.
    sink: Mutex<std::fs::File>,
}

impl AuditFileEmitter {
    /// Open (or create) `path` for append. The parent directory is
    /// created if missing.
    pub fn new(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            path,
            sink: Mutex::new(file),
        })
    }

    /// Path the emitter writes to.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl TransitionEmitter for AuditFileEmitter {
    fn emit(&self, event: TransitionEvent) {
        let line = match serde_json::to_string(&event) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "audit emit serialize failed; dropping event");
                return;
            }
        };
        if let Ok(mut file) = self.sink.lock() {
            if let Err(e) = writeln!(file, "{}", line) {
                tracing::warn!(error = %e, path = %self.path.display(), "audit emit write failed; dropping event");
            }
        }
    }
}

/// Fan-out emitter — every inner emitter receives every event.
pub struct MultiEmitter {
    inner: Vec<Box<dyn TransitionEmitter>>,
}

impl MultiEmitter {
    #[must_use]
    pub fn new() -> Self {
        Self { inner: Vec::new() }
    }

    /// Append a sink. Returns self so calls chain.
    #[must_use]
    pub fn with(mut self, emitter: Box<dyn TransitionEmitter>) -> Self {
        self.inner.push(emitter);
        self
    }

    pub fn push(&mut self, emitter: Box<dyn TransitionEmitter>) {
        self.inner.push(emitter);
    }
}

impl Default for MultiEmitter {
    fn default() -> Self {
        Self::new()
    }
}

impl TransitionEmitter for MultiEmitter {
    fn emit(&self, event: TransitionEvent) {
        for sink in &self.inner {
            sink.emit(event.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shigoto_types::{
        JobId, JobKindId, JobPhase, JobScope, JobSubject, TransitionEvent, TransitionReason,
    };
    use std::sync::Arc;

    fn sample_event() -> TransitionEvent {
        TransitionEvent {
            at: chrono::Utc::now(),
            job_id: JobId {
                scope: JobScope::Global,
                kind: JobKindId::new("test-kind"),
                subject: JobSubject::None,
            },
            from: JobPhase::Pending,
            to: JobPhase::Ready,
            reason: TransitionReason::GateEvaluation,
            tool: "test".into(),
        }
    }

    #[test]
    fn null_emitter_does_nothing() {
        let e = NullEmitter;
        // Must not panic and must return.
        e.emit(sample_event());
    }

    #[test]
    fn audit_file_emitter_appends_jsonl() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("audit.jsonl");

        let emitter = AuditFileEmitter::new(&path).unwrap();
        emitter.emit(sample_event());
        emitter.emit(sample_event());

        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        // Each line should be valid JSON.
        for line in lines {
            let _: serde_json::Value = serde_json::from_str(line).unwrap();
        }
    }

    #[test]
    fn audit_file_emitter_creates_parent_directory() {
        let dir = tempfile::tempdir().expect("temp dir");
        let nested = dir.path().join("a").join("b").join("audit.jsonl");
        // Parent doesn't exist yet.
        let emitter = AuditFileEmitter::new(&nested).unwrap();
        emitter.emit(sample_event());
        assert!(nested.exists());
    }

    #[test]
    fn multi_emitter_fans_out() {
        // Use an Arc<Mutex<Vec>> as a capture sink in two slots so we
        // can verify both received the event.
        struct CaptureEmitter {
            log: Arc<Mutex<Vec<TransitionEvent>>>,
        }
        impl TransitionEmitter for CaptureEmitter {
            fn emit(&self, event: TransitionEvent) {
                self.log.lock().unwrap().push(event);
            }
        }

        let a = Arc::new(Mutex::new(Vec::new()));
        let b = Arc::new(Mutex::new(Vec::new()));
        let multi = MultiEmitter::new()
            .with(Box::new(CaptureEmitter { log: a.clone() }))
            .with(Box::new(CaptureEmitter { log: b.clone() }));

        multi.emit(sample_event());
        assert_eq!(a.lock().unwrap().len(), 1);
        assert_eq!(b.lock().unwrap().len(), 1);
    }

    #[test]
    fn audit_file_emitter_persists_across_drops() {
        // A fresh emitter against the same path should see the
        // previous emitter's events — verifies we're appending, not
        // truncating.
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("audit.jsonl");

        {
            let e = AuditFileEmitter::new(&path).unwrap();
            e.emit(sample_event());
        }
        {
            let e = AuditFileEmitter::new(&path).unwrap();
            e.emit(sample_event());
        }

        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents.lines().count(), 2);
    }
}
