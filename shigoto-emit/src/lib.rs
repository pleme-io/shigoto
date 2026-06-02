//! shigoto-emit — typed transition emitter.
//!
//! Spec: `theory/SHIGOTO.md` §III.10 + §VIII. Every FSM transition
//! emits one event; sinks compose. The transition log is the
//! canonical history — `tend report`, K8s controller status,
//! observability dashboards, MCP operator surface all read the same
//! log. No second source of truth.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Mutex;

use shigoto_types::{JobId, OutputSink, TransitionEvent};

/// Receivers of `TransitionEvent`. Thin trait over the canonical
/// [`shigoto_types::sink::Sink<TransitionEvent>`] so every consumer
/// writing `Arc<dyn TransitionEmitter>` keeps working unchanged after
/// the theory/CONVERGENCE-ADOPTION.md Phase 0.1 extraction. The blanket
/// impl below means any `Sink<TransitionEvent>` impl auto-satisfies
/// `TransitionEmitter` — no per-impl wiring at the consumer side.
///
/// `emit` takes `&TransitionEvent` (matching `Sink::record`); the
/// scheduler passes a borrow and keeps ownership for its own
/// `transitions_this_tick` vec.
pub trait TransitionEmitter: Send + Sync {
    fn emit(&self, event: &TransitionEvent);
}

impl<T: shigoto_types::sink::Sink<TransitionEvent> + ?Sized> TransitionEmitter for T {
    fn emit(&self, event: &TransitionEvent) {
        shigoto_types::sink::Sink::record(self, event);
    }
}

/// No-op emitter — the default for tests + consumers without
/// observability wired up. Sinks should compose via `MultiEmitter`
/// instead of stubbing this in production. Alias of the canonical
/// `shigoto_types::sink::NullSink<TransitionEvent>`.
pub type NullEmitter = shigoto_types::sink::NullSink<TransitionEvent>;

/// Append-only JSONL audit file. One event per line. Same shape as
/// tend's existing `audit.rs` so operators can grep both with the
/// same tooling. Alias of the canonical
/// `shigoto_types::sink::AuditFileSink<TransitionEvent>` — JSONL
/// serialization is byte-identical (one `serde_json::to_string` per
/// line + `writeln!` append).
pub type AuditFileEmitter = shigoto_types::sink::AuditFileSink<TransitionEvent>;

/// Fan-out emitter — every inner sink receives every event. Alias of
/// the canonical `shigoto_types::sink::MultiSink<TransitionEvent>`;
/// inner sinks are `Arc<dyn Sink<TransitionEvent>>`.
pub type MultiEmitter = shigoto_types::sink::MultiSink<TransitionEvent>;

// ── Output sinks ─────────────────────────────────────────────────────
//
// Concrete `OutputSink<O>` impls. Sinks parallel `TransitionEmitter`
// in shape but carry typed `Output` values rather than phase
// transitions. See `shigoto_types::OutputSink` for the full design
// rationale (§VIII output capture).

/// No-op sink — discards every output. Default for Jobs that don't
/// need their typed Output surfaced.
#[derive(Debug, Default)]
pub struct NullSink<O>(std::marker::PhantomData<fn() -> O>);

impl<O> NullSink<O> {
    #[must_use]
    pub const fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

#[async_trait::async_trait]
impl<O> OutputSink<O> for NullSink<O>
where
    O: Send + Sync + 'static,
{
    async fn record(&self, _job_id: &JobId, _output: &O) {
        // intentional no-op
    }
}

/// In-memory `JobId → Output` map. The consumer reads via `drain`
/// (clears + returns) or `snapshot` (clones + keeps) after ticks
/// complete. `Arc<Mutex<...>>` interior so the same sink can be
/// shared between the Job (recording) and the consumer (reading).
///
/// Stores by cloning `Output` — implementations whose Output is
/// expensive to clone should use a custom sink (e.g. one that
/// extracts only the part they care about).
pub struct InMemorySink<O>
where
    O: Clone + Send + Sync + 'static,
{
    storage: std::sync::Arc<Mutex<HashMap<JobId, O>>>,
}

impl<O> InMemorySink<O>
where
    O: Clone + Send + Sync + 'static,
{
    #[must_use]
    pub fn new() -> Self {
        Self {
            storage: std::sync::Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Take all recorded outputs, clearing the sink. Returns a fresh
    /// map keyed by `JobId`.
    pub fn drain(&self) -> HashMap<JobId, O> {
        let mut guard = self.storage.lock().expect("InMemorySink mutex poisoned");
        std::mem::take(&mut *guard)
    }

    /// Clone every recorded output. Useful when you want to inspect
    /// without clearing.
    pub fn snapshot(&self) -> HashMap<JobId, O> {
        self.storage
            .lock()
            .expect("InMemorySink mutex poisoned")
            .clone()
    }

    /// Number of jobs whose outputs are currently held.
    pub fn len(&self) -> usize {
        self.storage
            .lock()
            .expect("InMemorySink mutex poisoned")
            .len()
    }

    /// True iff `len() == 0`.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<O> Default for InMemorySink<O>
where
    O: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl<O> OutputSink<O> for InMemorySink<O>
where
    O: Clone + Send + Sync + 'static,
{
    async fn record(&self, job_id: &JobId, output: &O) {
        let mut guard = self.storage.lock().expect("InMemorySink mutex poisoned");
        guard.insert(job_id.clone(), output.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shigoto_types::{
        JobId, JobKindId, JobPhase, JobScope, JobSubject, OutputSink, TransitionEvent,
        TransitionReason,
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
        let e = NullEmitter::new();
        // Must not panic and must return.
        e.emit(&sample_event());
    }

    #[test]
    fn audit_file_emitter_appends_jsonl() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("audit.jsonl");

        let emitter = AuditFileEmitter::new(&path).unwrap();
        emitter.emit(&sample_event());
        emitter.emit(&sample_event());
        // Drop so the underlying file flushes.
        drop(emitter);

        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        // Each line should be valid JSON — same JSONL shape the
        // deleted hand-rolled AuditFileEmitter produced.
        for line in lines {
            let _: TransitionEvent = serde_json::from_str(line).unwrap();
        }
    }

    #[test]
    fn audit_file_emitter_creates_parent_directory() {
        let dir = tempfile::tempdir().expect("temp dir");
        let nested = dir.path().join("a").join("b").join("audit.jsonl");
        // Parent doesn't exist yet.
        let emitter = AuditFileEmitter::new(&nested).unwrap();
        emitter.emit(&sample_event());
        assert!(nested.exists());
    }

    #[test]
    fn multi_emitter_fans_out() {
        use shigoto_types::sink::{InMemorySink, Sink};
        // Use InMemorySink in two slots so we can verify both received
        // the event.
        let a: Arc<InMemorySink<TransitionEvent>> = Arc::new(InMemorySink::new());
        let b: Arc<InMemorySink<TransitionEvent>> = Arc::new(InMemorySink::new());
        let multi = MultiEmitter::new()
            .with(a.clone() as Arc<dyn Sink<TransitionEvent>>)
            .with(b.clone() as Arc<dyn Sink<TransitionEvent>>);

        multi.emit(&sample_event());
        assert_eq!(a.len(), 1);
        assert_eq!(b.len(), 1);
    }

    // ── Output sink tests ────────────────────────────────────────────

    fn sample_job_id(name: &str) -> JobId {
        JobId {
            scope: JobScope::Global,
            kind: JobKindId::new("test-kind"),
            subject: JobSubject::Repo(name.into()),
        }
    }

    #[tokio::test]
    async fn null_sink_discards_outputs() {
        let sink: NullSink<String> = NullSink::new();
        sink.record(&sample_job_id("a"), &"hello".to_string()).await;
        // No assertion needed — the call must just not panic.
    }

    #[tokio::test]
    async fn in_memory_sink_records_per_job_id() {
        let sink: InMemorySink<u32> = InMemorySink::new();
        assert!(sink.is_empty());

        let id_a = sample_job_id("a");
        let id_b = sample_job_id("b");

        sink.record(&id_a, &42).await;
        sink.record(&id_b, &99).await;
        assert_eq!(sink.len(), 2);

        let snap = sink.snapshot();
        assert_eq!(snap.get(&id_a), Some(&42));
        assert_eq!(snap.get(&id_b), Some(&99));
        // snapshot didn't clear.
        assert_eq!(sink.len(), 2);

        let drained = sink.drain();
        assert_eq!(drained.len(), 2);
        assert!(sink.is_empty());
    }

    #[tokio::test]
    async fn in_memory_sink_overwrites_on_duplicate_job_id() {
        // A Job that gets re-executed (e.g. after a retry) should
        // overwrite its previous output, not accumulate.
        let sink: InMemorySink<String> = InMemorySink::new();
        let id = sample_job_id("retry");

        sink.record(&id, &"first".to_string()).await;
        sink.record(&id, &"second".to_string()).await;

        let snap = sink.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap.get(&id), Some(&"second".to_string()));
    }

    #[tokio::test]
    async fn in_memory_sink_shared_via_arc_sees_recorded_outputs() {
        // Realistic consumer pattern: hold an Arc<InMemorySink>, hand
        // clones to Jobs, drain after ticks. The Mutex inside the
        // sink coordinates concurrent recording.
        let sink: std::sync::Arc<InMemorySink<u8>> = std::sync::Arc::new(InMemorySink::new());
        let sink_for_job = std::sync::Arc::clone(&sink);

        tokio::spawn(async move {
            sink_for_job.record(&sample_job_id("bg"), &7).await;
        })
        .await
        .unwrap();

        assert_eq!(sink.snapshot().get(&sample_job_id("bg")), Some(&7));
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
            e.emit(&sample_event());
        }
        {
            let e = AuditFileEmitter::new(&path).unwrap();
            e.emit(&sample_event());
        }

        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents.lines().count(), 2);
    }
}
