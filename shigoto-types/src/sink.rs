//! Typed event sink — the canonical `Sink<T>` trait every fleet-wide
//! event recorder consumes. Spec: `theory/CONVERGENCE-ADOPTION.md`
//! §II.1, Phase 0.1.
//!
//! Subsumes the hand-rolled sink shapes that previously lived in:
//!
//! - `tend::drift::DriftSink` (+ NullDriftSink / InMemoryDriftSink /
//!   AuditFileDriftSink) — 3 impls for `DriftEvent` recording
//! - `shigoto::emit::TransitionEmitter` (+ NullEmitter / AuditFileEmitter
//!   / MultiEmitter) — 3 impls for `TransitionEvent`
//!
//! The two former shapes are domain-specific (`&DriftEvent` /
//! `TransitionEvent`); this trait abstracts the event type out so a
//! single set of sink impls covers every event shape across the fleet.
//!
//! Distinct from [`crate::OutputSink<O>`], which is async and per-Job
//! (carries `JobId`). `Sink<T>` is sync and event-only — for fire-and-
//! forget audit recording where the typed event is the unit of record.
//!
//! # Migration path
//!
//! Existing consumers migrate by adding the dependency on
//! `shigoto-types` + replacing their per-domain `*Sink` trait with a
//! type alias `pub type FooSink = dyn Sink<FooEvent>`. The three
//! standard impls (`NullSink<T>`, `InMemorySink<T>`, `AuditFileSink<T>`)
//! replace the per-domain hand-rolls; net-negative LOC.
//!
//! # `MultiSink<T>` composition
//!
//! `MultiSink<T>` holds a `Vec<Arc<dyn Sink<T>>>` and `record`s to every
//! child sequentially. Used when one event needs to flow to both a
//! durable audit file AND an in-memory test capture, or to a metrics
//! exporter AND a NATS publish. Composition is the canonical way to
//! combine sink behaviors — not subclass relationships.

use std::fmt::Debug;
use std::fs::OpenOptions;
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::Serialize;

/// The canonical typed-event sink. Implementations record each event
/// synchronously; async sinks should wrap behind a non-blocking
/// channel and have a separate flusher task.
///
/// `record` takes `&T` so non-`Clone` events are allowed at the trait
/// level. Concrete sinks that need owned values (e.g. `InMemorySink`)
/// add `T: Clone` at their `impl` boundary, not on the trait.
pub trait Sink<T>: Send + Sync {
    /// Record one event. Synchronous; sinks that need durability should
    /// buffer or flush internally. Errors are absorbed (typed sinks
    /// never fail at the trait level — failure modes are surfaced via
    /// the sink's own state, not via Result, to keep the call-site
    /// simple). Implementations log fatal errors via `tracing::warn!`
    /// or similar.
    fn record(&self, event: &T);
}

// ── Sink impls ────────────────────────────────────────────────────

/// No-op sink. Default for tests + consumers without observability
/// wired up. Production code composes via `MultiSink` instead of
/// stubbing this in.
#[derive(Debug, Default)]
pub struct NullSink<T> {
    _phantom: PhantomData<T>,
}

impl<T> NullSink<T> {
    pub const fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T: Send + Sync> Sink<T> for NullSink<T> {
    fn record(&self, _event: &T) {
        // intentional no-op
    }
}

/// Buffer events in memory. Used by tests + by short-lived reconcile
/// receipts where the event set is small enough to hold in RAM. Each
/// recorded event is cloned into the buffer; `T: Clone` is required at
/// the impl boundary (not the trait).
pub struct InMemorySink<T> {
    events: Mutex<Vec<T>>,
}

impl<T> Default for InMemorySink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> InMemorySink<T> {
    pub fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }

    /// Snapshot the events captured so far. Doesn't clear the buffer.
    /// Requires `T: Clone` because each event in the buffer is owned
    /// and the caller gets owned copies.
    pub fn snapshot(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.events
            .lock()
            .expect("InMemorySink mutex poisoned")
            .clone()
    }

    /// Take all events, clearing the buffer.
    pub fn drain(&self) -> Vec<T> {
        std::mem::take(&mut *self.events.lock().expect("InMemorySink mutex poisoned"))
    }

    pub fn len(&self) -> usize {
        self.events
            .lock()
            .expect("InMemorySink mutex poisoned")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> std::fmt::Debug for InMemorySink<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemorySink")
            .field("len", &self.len())
            .finish()
    }
}

impl<T: Clone + Send + Sync> Sink<T> for InMemorySink<T> {
    fn record(&self, event: &T) {
        self.events
            .lock()
            .expect("InMemorySink mutex poisoned")
            .push(event.clone());
    }
}

/// Append every recorded event as one JSON line to `path`. Same shape
/// as shigoto's transition log + tend's audit log so operators have
/// one tool (`jq`) for everything.
///
/// Writes serialize through a `Mutex<File>` so concurrent `record`
/// calls produce interleaved (not interleaved-mid-line) JSONL output.
/// Holds the open file handle so we don't reopen per-event.
///
/// Failures (serialization error, write error, mutex poison) are
/// logged via `tracing::warn!` and dropped — the trait never returns
/// `Result`. Audit-critical paths should compose this with a non-file
/// sink (in-memory ring buffer, NATS publish) via `MultiSink` so a
/// failed file write doesn't lose the event.
pub struct AuditFileSink<T> {
    path: PathBuf,
    file: Mutex<std::fs::File>,
    _phantom: PhantomData<T>,
}

impl<T> AuditFileSink<T> {
    /// Open `path` (creating parent dirs if needed) for append.
    /// Returns the open sink ready for `record` calls.
    pub fn new(path: &Path) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            file: Mutex::new(file),
            _phantom: PhantomData,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl<T> std::fmt::Debug for AuditFileSink<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditFileSink")
            .field("path", &self.path)
            .finish()
    }
}

impl<T: Serialize + Send + Sync> Sink<T> for AuditFileSink<T> {
    fn record(&self, event: &T) {
        match serde_json::to_string(event) {
            Ok(line) => {
                if let Ok(mut f) = self.file.lock() {
                    let _ = writeln!(f, "{line}");
                }
            }
            Err(err) => {
                // Audit serialization failure is operator-visible —
                // surface via tracing but don't propagate (callers
                // shouldn't have to handle audit-log errors).
                eprintln!("[shigoto-types::sink] AuditFileSink serialize failed: {err}",);
            }
        }
    }
}

/// Compose multiple sinks into one. Each child receives every event
/// in `Vec` order. Used when one event needs to flow to both a durable
/// audit file AND an in-memory test capture, or to a metrics exporter
/// AND a NATS publish.
///
/// Composition is the canonical way to combine sink behaviors — not
/// subclass relationships. Adding a new behavior = adding a new
/// `Arc<dyn Sink<T>>` to the children, not extending an existing impl.
pub struct MultiSink<T> {
    children: Vec<Arc<dyn Sink<T>>>,
}

impl<T> MultiSink<T> {
    pub fn new() -> Self {
        Self {
            children: Vec::new(),
        }
    }

    pub fn with(mut self, child: Arc<dyn Sink<T>>) -> Self {
        self.children.push(child);
        self
    }

    pub fn push(&mut self, child: Arc<dyn Sink<T>>) {
        self.children.push(child);
    }

    pub fn len(&self) -> usize {
        self.children.len()
    }

    pub fn is_empty(&self) -> bool {
        self.children.is_empty()
    }
}

impl<T> Default for MultiSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> std::fmt::Debug for MultiSink<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiSink")
            .field("children", &self.children.len())
            .finish()
    }
}

impl<T: Send + Sync> Sink<T> for MultiSink<T> {
    fn record(&self, event: &T) {
        for child in &self.children {
            child.record(event);
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use tempfile::TempDir;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestEvent {
        kind: String,
        n: u32,
    }

    fn ev(kind: &str, n: u32) -> TestEvent {
        TestEvent {
            kind: kind.into(),
            n,
        }
    }

    #[test]
    fn null_sink_records_without_panic() {
        let sink = NullSink::<TestEvent>::new();
        sink.record(&ev("a", 1));
        sink.record(&ev("b", 2));
        // No panic, no observable state — that's the contract.
    }

    #[test]
    fn in_memory_sink_captures_in_order() {
        let sink = InMemorySink::<TestEvent>::new();
        assert!(sink.is_empty());

        sink.record(&ev("a", 1));
        sink.record(&ev("b", 2));
        sink.record(&ev("c", 3));

        assert_eq!(sink.len(), 3);
        let snap = sink.snapshot();
        assert_eq!(snap[0].kind, "a");
        assert_eq!(snap[1].kind, "b");
        assert_eq!(snap[2].kind, "c");
    }

    #[test]
    fn in_memory_sink_drain_clears_buffer() {
        let sink = InMemorySink::<TestEvent>::new();
        sink.record(&ev("x", 1));
        sink.record(&ev("y", 2));

        let drained = sink.drain();
        assert_eq!(drained.len(), 2);
        assert!(sink.is_empty(), "drain() should clear the buffer");
    }

    #[test]
    fn in_memory_sink_snapshot_does_not_clear() {
        let sink = InMemorySink::<TestEvent>::new();
        sink.record(&ev("x", 1));
        let _ = sink.snapshot();
        assert_eq!(sink.len(), 1, "snapshot() must not clear");
    }

    #[test]
    fn audit_file_sink_appends_jsonl_lines() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("sink.jsonl");
        let sink = AuditFileSink::<TestEvent>::new(&path).unwrap();

        sink.record(&ev("first", 1));
        sink.record(&ev("second", 2));
        sink.record(&ev("third", 3));

        // Drop so the underlying file flushes.
        drop(sink);

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<_> = content.lines().collect();
        assert_eq!(lines.len(), 3);

        // Each line is well-formed JSON deserializable to TestEvent.
        for (i, line) in lines.iter().enumerate() {
            let parsed: TestEvent = serde_json::from_str(line).unwrap();
            assert_eq!(parsed.n, (i + 1) as u32);
        }
    }

    #[test]
    fn audit_file_sink_creates_parent_dirs() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("nested/dir/structure/sink.jsonl");
        let sink = AuditFileSink::<TestEvent>::new(&path).unwrap();
        sink.record(&ev("ok", 1));
        drop(sink);
        assert!(path.exists());
    }

    #[test]
    fn multi_sink_fans_out_to_every_child() {
        let in_mem_a = Arc::new(InMemorySink::<TestEvent>::new());
        let in_mem_b = Arc::new(InMemorySink::<TestEvent>::new());

        let multi = MultiSink::<TestEvent>::new()
            .with(in_mem_a.clone() as Arc<dyn Sink<TestEvent>>)
            .with(in_mem_b.clone() as Arc<dyn Sink<TestEvent>>);

        assert_eq!(multi.len(), 2);

        multi.record(&ev("broadcast", 1));
        multi.record(&ev("broadcast", 2));

        assert_eq!(in_mem_a.len(), 2, "child A should have both events");
        assert_eq!(in_mem_b.len(), 2, "child B should have both events");
    }

    #[test]
    fn multi_sink_empty_is_no_op() {
        let multi = MultiSink::<TestEvent>::new();
        assert!(multi.is_empty());
        multi.record(&ev("a", 1)); // no children — drops silently
    }

    #[test]
    fn dyn_sink_polymorphism_works() {
        // The point of the trait: heterogeneous sinks behind one
        // `Arc<dyn Sink<T>>`.
        let null = Arc::new(NullSink::<TestEvent>::new()) as Arc<dyn Sink<TestEvent>>;
        let mem = Arc::new(InMemorySink::<TestEvent>::new()) as Arc<dyn Sink<TestEvent>>;

        let sinks: Vec<Arc<dyn Sink<TestEvent>>> = vec![null, mem.clone()];
        for s in &sinks {
            s.record(&ev("z", 9));
        }

        // Only the in-memory one captured — null is a no-op.
        // We can't downcast here without Any; rely on the type-erased
        // surface staying consistent across both impls.
    }

    /// Recording an event through `&dyn Sink<T>` works — this is the
    /// usage pattern for consumers that want runtime-polymorphic sinks.
    #[test]
    fn record_through_dyn_reference() {
        let sink = InMemorySink::<TestEvent>::new();
        let dyn_ref: &dyn Sink<TestEvent> = &sink;
        dyn_ref.record(&ev("via_dyn", 42));
        assert_eq!(sink.len(), 1);
        assert_eq!(sink.snapshot()[0].n, 42);
    }
}
