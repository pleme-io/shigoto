//! shigoto-scheduler — the typed runtime.
//!
//! Spec: `theory/SHIGOTO.md` §III.6. A `Scheduler` is NOT a daemon —
//! one `tick` per call. Daemons loop `tick → wait_for_change → tick`;
//! K8s reconcilers map each CR event to one `tick`. v0.1 ships
//! `InProcessScheduler` driving the FSM via `shigoto_types::advance`;
//! future impls (persistent, distributed, replayable) plug behind the
//! same trait.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use shigoto_dag::Dag;
use shigoto_emit::{NullEmitter, TransitionEmitter};
use shigoto_gate::{self, AllUpstreamsTerminal, Gate, GateContext, GateOutcome};
use shigoto_retry::{FailureRecord, RetryDecision, RetryPolicy};
use shigoto_types::{
    advance, ErasedJob, GateAggregate, IllegalTransition, JobId, JobKindId, JobPhase, RetryOutcome,
    Signal, Snapshot, TickReceipt, TransitionEvent, TransitionReason,
};

#[derive(thiserror::Error, Debug)]
pub enum SchedulerError {
    #[error("dag toposort failed: {0}")]
    Topology(#[from] shigoto_dag::DagError),
    #[error("illegal FSM transition: {0}")]
    IllegalTransition(#[from] IllegalTransition),
}

#[async_trait]
pub trait Scheduler: Send + Sync {
    /// Drive the Dag one tick forward.
    async fn tick(&self, dag: &mut Dag) -> Result<TickReceipt, SchedulerError>;

    /// Read-only snapshot of the current FSM map.
    fn snapshot(&self, dag: &Dag) -> Snapshot;
}

// ── InProcessScheduler ───────────────────────────────────────────────

/// Default scheduler — single-process, in-memory FSM state, sequential
/// execution within a wave.
///
/// v0.1 simplifying assumptions:
/// - Gates: defaults to `GateAggregate::AllPassed`. Consumers extend
///   by registering Gate impls (M0.9d).
/// - Budget: every Ready job allocates successfully (no rate limit).
///   M0.9d wires `BudgetTree`.
/// - Retry: NoRetry by default. M0.9d wires `RetryPolicy`.
/// - Timeout: per-job optional via `JobTimeoutPolicy`; otherwise none.
/// - Cancellation: cooperative via a shared `tokio_util` token (not
///   yet wired — future M0.9d).
/// - Emitter: `NullEmitter` no-ops by default; tests can register
///   `CapturingEmitter` to assert on transitions.
///
/// The load-bearing piece this v0.1 DOES enforce: the FSM table from
/// `shigoto-types::advance`. Every phase change goes through it; the
/// scheduler never invents transitions.
pub struct InProcessScheduler {
    state: tokio::sync::RwLock<SchedulerState>,
    tool: String,
    emitter: Arc<dyn TransitionEmitter>,
}

struct SchedulerState {
    phases: HashMap<JobId, JobPhase>,
    jobs: HashMap<JobId, Arc<dyn ErasedJob>>,
    /// Per-job timeout overrides.
    timeouts: HashMap<JobId, Duration>,
    /// Per-kind gate registry. Every job of kind K consults
    /// `gates[K]` (plus the implicit AllUpstreamsTerminal) during
    /// gate evaluation. Empty vec means "no kind-specific gates."
    gates: HashMap<JobKindId, Vec<Arc<dyn Gate>>>,
    /// Per-kind retry policy. Default is `NoRetry` — deadletter on
    /// first failure. Consumers register Fixed / Exponential /
    /// Custom policies per kind.
    retry_policies: HashMap<JobKindId, RetryPolicy>,
    /// Authoritative attempt counter per job. JobPhase::Failed's
    /// `attempts` field carries the count for serialization but the
    /// scheduler reads this map for decisions because the FSM v0.1
    /// doesn't thread attempts across the Retrying→Pending→Ready→
    /// Running cycle. Incremented on every ExecutionFailed dispatch;
    /// reset when the job reaches Succeeded.
    attempts: HashMap<JobId, u32>,
    /// Per-job failure history (capped) — passed to RetryDecider.
    failure_history: HashMap<JobId, Vec<FailureRecord>>,
}

impl InProcessScheduler {
    /// Build a new scheduler with the no-op `NullEmitter`. `tool` is
    /// the consumer-name tag emitted on every TransitionEvent (e.g.
    /// "tend", "forge-gen"). Use `with_emitter` to attach a real sink.
    #[must_use]
    pub fn new(tool: impl Into<String>) -> Self {
        Self {
            state: tokio::sync::RwLock::new(SchedulerState {
                phases: HashMap::new(),
                jobs: HashMap::new(),
                timeouts: HashMap::new(),
                gates: HashMap::new(),
                retry_policies: HashMap::new(),
                attempts: HashMap::new(),
                failure_history: HashMap::new(),
            }),
            tool: tool.into(),
            emitter: Arc::new(NullEmitter),
        }
    }

    /// Register a Gate against a JobKind. Every job of that kind
    /// consults this Gate (plus the implicit AllUpstreamsTerminal)
    /// during gate evaluation. Multiple gates per kind compose via
    /// `shigoto_gate::reduce` (worst-outcome wins).
    pub async fn register_gate(&self, kind: JobKindId, gate: Arc<dyn Gate>) {
        let mut state = self.state.write().await;
        state.gates.entry(kind).or_default().push(gate);
    }

    /// Register a RetryPolicy against a JobKind. On every Failed
    /// transition for a job of that kind, the scheduler calls
    /// `policy.decide(attempt, history)` to decide retry vs deadletter.
    /// Default (no registration) is `NoRetry`.
    pub async fn register_retry_policy(&self, kind: JobKindId, policy: RetryPolicy) {
        let mut state = self.state.write().await;
        state.retry_policies.insert(kind, policy);
    }

    /// Replace the default `NullEmitter` with a real sink — typically
    /// `AuditFileEmitter` or `MultiEmitter` from shigoto-emit. Every
    /// FSM transition emit()s here.
    #[must_use]
    pub fn with_emitter(mut self, emitter: Arc<dyn TransitionEmitter>) -> Self {
        self.emitter = emitter;
        self
    }

    /// Register a Job with the scheduler. The DAG holds JobIds; the
    /// scheduler holds the executable Job behind each ID.
    pub async fn register_job(&self, job: Arc<dyn ErasedJob>) {
        let id = job.id();
        let mut state = self.state.write().await;
        state.phases.entry(id.clone()).or_insert(JobPhase::Pending);
        state.jobs.insert(id, job);
    }

    /// Optional per-job timeout. Without one, execute() runs to
    /// completion (or until external cancellation).
    pub async fn set_timeout(&self, id: JobId, timeout: Duration) {
        let mut state = self.state.write().await;
        state.timeouts.insert(id, timeout);
    }

    /// External operator transition. Constrained to the three legal
    /// targets per `shigoto_types::advance` — calls that violate the
    /// FSM return `IllegalTransition`.
    pub async fn operator_transition(
        &self,
        id: &JobId,
        target: JobPhase,
        reason: TransitionReason,
    ) -> Result<(), SchedulerError> {
        let mut state = self.state.write().await;
        let from = state
            .phases
            .get(id)
            .cloned()
            .unwrap_or(JobPhase::Pending);
        let new = advance(from.clone(), Signal::OperatorTransition(target))?;
        state.phases.insert(id.clone(), new);
        // Emission of operator transitions is handled by the next tick
        // (we want every emit to flow through the same path).
        let _ = reason;
        Ok(())
    }
}

#[async_trait]
impl Scheduler for InProcessScheduler {
    async fn tick(&self, dag: &mut Dag) -> Result<TickReceipt, SchedulerError> {
        let started_at = chrono::Utc::now();
        let waves = dag.waves(None)?;
        let mut transitions: Vec<TransitionEvent> = Vec::new();

        for wave in waves {
            for id in wave {
                // ── Seed unknown nodes ──────────────────────────
                {
                    let mut state = self.state.write().await;
                    state.phases.entry(id.clone()).or_insert(JobPhase::Pending);
                }

                // Drive each Job's FSM as far forward as it can go in
                // a single tick. We loop until the phase stops
                // advancing (either terminal, blocked on operator, or
                // waiting on a gate). Capped at 6 steps — longest
                // forward chain is Pending→Ready→Running→Succeeded
                // (3) and the failure chain is one longer.
                for _ in 0..6 {
                    let from = self.phase_of(&id).await;
                    let progressed = self
                        .advance_once(&id, &from, &*dag, &mut transitions)
                        .await?;
                    if !progressed {
                        break;
                    }
                }
            }
        }

        let snapshot = self.snapshot_inner().await;
        let phase_counts = phase_count_summary(&snapshot.phases);

        Ok(TickReceipt {
            tick_at: started_at,
            phase_counts,
            transitions_this_tick: transitions,
            unhealed: Vec::new(),
        })
    }

    fn snapshot(&self, _dag: &Dag) -> Snapshot {
        // Trait method is sync; wrap the async lock. This is only
        // called by consumers outside the tick loop.
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.snapshot_inner())
        })
    }
}

impl InProcessScheduler {
    /// Apply one FSM step to the given job. Returns `Ok(true)` when
    /// the phase advanced, `Ok(false)` when no progress is possible
    /// from the current phase (terminal, waiting for operator, gated
    /// on something that hasn't changed since last check).
    async fn advance_once(
        &self,
        id: &JobId,
        from: &JobPhase,
        dag: &Dag,
        transitions: &mut Vec<TransitionEvent>,
    ) -> Result<bool, SchedulerError> {
        match from {
            // Terminal phases — no progress.
            JobPhase::Succeeded | JobPhase::Skipped(_) | JobPhase::Deadlettered => Ok(false),
            // Operator-gated — no automatic progress.
            JobPhase::WaitingForOperator => Ok(false),

            // Gate evaluation — implicit AllUpstreamsTerminal + per-kind
            // consumer-registered gates, reduced to a GateAggregate
            // and dispatched via Signal::EvaluateGates.
            JobPhase::Pending | JobPhase::Gated => {
                let aggregate = self.evaluate_gates(id, dag).await;
                // If the aggregate is SomeWaiting AND we're already
                // Gated, no useful transition fires (Gated → Gated is
                // a self-loop; record as no-progress so the outer loop
                // stops looping and the scheduler moves on).
                let already_gated = matches!(from, JobPhase::Gated)
                    && matches!(aggregate, GateAggregate::SomeWaiting);
                if already_gated {
                    return Ok(false);
                }
                self.dispatch(id, Signal::EvaluateGates(aggregate), transitions)
                    .await?;
                Ok(true)
            }

            // Retrying: wait for the backoff window to elapse, then
            // BackoffElapsed → Pending (cycle restart).
            JobPhase::Retrying { until_ms } => {
                let now_ms = chrono::Utc::now().timestamp_millis();
                if now_ms < *until_ms {
                    return Ok(false);
                }
                self.dispatch(id, Signal::BackoffElapsed, transitions).await?;
                Ok(true)
            }

            // Budget allocation (v0.1: always succeeds).
            JobPhase::Ready => {
                self.dispatch(id, Signal::AllocateBudget, transitions).await?;
                Ok(true)
            }

            // Execute. Running is a very short-lived phase in v0.1
            // because we run synchronously and dispatch the outcome
            // in the same step.
            JobPhase::Running => {
                self.run_job(id, transitions).await?;
                Ok(true)
            }

            // Failed → retry decision via registered RetryPolicy.
            JobPhase::Failed { .. } => {
                let outcome = self.decide_retry(id).await;
                self.dispatch(id, Signal::RetryDecide(outcome), transitions).await?;
                Ok(true)
            }
        }
    }

    /// Consult the registered RetryPolicy for the job's kind, decide
    /// Retry { until_ms } vs Deadletter. Default is NoRetry when no
    /// policy is registered.
    async fn decide_retry(&self, id: &JobId) -> RetryOutcome {
        let (policy, attempt, history) = {
            let state = self.state.read().await;
            let policy = state
                .retry_policies
                .get(&id.kind)
                .cloned()
                .unwrap_or(RetryPolicy::NoRetry);
            let attempt = state.attempts.get(id).copied().unwrap_or(1);
            let history = state
                .failure_history
                .get(id)
                .cloned()
                .unwrap_or_default();
            (policy, attempt, history)
        };
        match policy.decide(attempt, &history) {
            RetryDecision::Deadletter => RetryOutcome::Deadletter,
            RetryDecision::Retry { after } => {
                let now_ms = chrono::Utc::now().timestamp_millis();
                let until_ms = now_ms + after.as_millis() as i64;
                RetryOutcome::Retry { until_ms }
            }
        }
    }

    /// Evaluate the gate cohort for `id` against the current snapshot
    /// + dag. The cohort = implicit `AllUpstreamsTerminal` (enforces
    /// DAG edge semantics) + consumer-registered gates for the job's
    /// kind. Reduced via `shigoto_gate::reduce`.
    async fn evaluate_gates(&self, id: &JobId, dag: &Dag) -> GateAggregate {
        let (kind_gates, snapshot) = {
            let state = self.state.read().await;
            let kind_gates = state.gates.get(&id.kind).cloned().unwrap_or_default();
            let snapshot = Snapshot {
                phases: state.phases.clone(),
            };
            (kind_gates, snapshot)
        };
        let ctx = GateContext {
            job_id: id,
            snapshot: &snapshot,
            dag,
        };
        let mut outcomes: Vec<GateOutcome> = Vec::with_capacity(kind_gates.len() + 1);
        outcomes.push(AllUpstreamsTerminal.evaluate(&ctx));
        for gate in &kind_gates {
            outcomes.push(gate.evaluate(&ctx));
        }
        shigoto_gate::reduce(&outcomes)
    }

    async fn snapshot_inner(&self) -> Snapshot {
        let state = self.state.read().await;
        Snapshot {
            phases: state.phases.clone(),
        }
    }

    async fn phase_of(&self, id: &JobId) -> JobPhase {
        let state = self.state.read().await;
        state.phases.get(id).cloned().unwrap_or(JobPhase::Pending)
    }

    /// Apply one FSM signal and record the resulting transition.
    async fn dispatch(
        &self,
        id: &JobId,
        signal: Signal,
        transitions: &mut Vec<TransitionEvent>,
    ) -> Result<(), SchedulerError> {
        let mut state = self.state.write().await;
        let from = state.phases.get(id).cloned().unwrap_or(JobPhase::Pending);
        let signal_clone = signal.clone();
        let to = advance(from.clone(), signal)?;
        state.phases.insert(id.clone(), to.clone());

        let event = TransitionEvent {
            at: chrono::Utc::now(),
            job_id: id.clone(),
            from,
            to,
            reason: reason_from(&signal_clone),
            tool: self.tool.clone(),
        };
        // Fire the emitter (no-op for NullEmitter; appends a JSONL line
        // for AuditFileEmitter; etc).
        self.emitter.emit(event.clone());
        transitions.push(event);
        Ok(())
    }

    /// Execute a Running job and dispatch the resulting outcome.
    async fn run_job(
        &self,
        id: &JobId,
        transitions: &mut Vec<TransitionEvent>,
    ) -> Result<(), SchedulerError> {
        // Pull the Job + timeout out of state without holding the lock
        // across the await of execute().
        let (job, timeout) = {
            let state = self.state.read().await;
            (state.jobs.get(id).cloned(), state.timeouts.get(id).copied())
        };

        let Some(job) = job else {
            // Unregistered job — treat as immediate failure.
            self.dispatch(id, Signal::ExecutionFailed, transitions).await?;
            self.dispatch(
                id,
                Signal::RetryDecide(RetryOutcome::Deadletter),
                transitions,
            )
            .await?;
            return Ok(());
        };

        let outcome = match timeout {
            Some(d) => tokio::time::timeout(d, job.execute_erased())
                .await
                .map(|r| r.map_err(|_| ()))
                .unwrap_or(Err(())),
            None => job.execute_erased().await.map_err(|_| ()),
        };

        match outcome {
            Ok(()) => {
                self.dispatch(id, Signal::ExecutionSucceeded, transitions)
                    .await?;
                // Reset attempts on success (a future Pending recurrence
                // is a fresh job lifecycle).
                let mut state = self.state.write().await;
                state.attempts.remove(id);
                state.failure_history.remove(id);
            }
            Err(()) => {
                // Increment attempts + record failure BEFORE dispatching
                // ExecutionFailed so the FSM state and the side maps
                // stay in lockstep.
                {
                    let mut state = self.state.write().await;
                    let entry = state.attempts.entry(id.clone()).or_insert(0);
                    *entry += 1;
                    let attempt = *entry;
                    let history = state
                        .failure_history
                        .entry(id.clone())
                        .or_insert_with(Vec::new);
                    history.push(FailureRecord {
                        attempt,
                        at_ms: chrono::Utc::now().timestamp_millis(),
                        error: "execute() returned Err".into(),
                    });
                    // Cap history to last 16 entries — bounded memory.
                    if history.len() > 16 {
                        let drop_n = history.len() - 16;
                        history.drain(0..drop_n);
                    }
                }
                self.dispatch(id, Signal::ExecutionFailed, transitions)
                    .await?;
                // The retry decision dispatch happens on the NEXT
                // advance_once call (Failed → RetryDecide); this lets
                // the per-job loop in tick() handle it uniformly.
            }
        }

        Ok(())
    }
}

fn phase_count_summary(phases: &HashMap<JobId, JobPhase>) -> std::collections::BTreeMap<String, u32> {
    let mut counts: std::collections::BTreeMap<String, u32> = std::collections::BTreeMap::new();
    for phase in phases.values() {
        let key = match phase {
            JobPhase::Pending => "pending",
            JobPhase::Gated => "gated",
            JobPhase::Ready => "ready",
            JobPhase::Running => "running",
            JobPhase::Succeeded => "succeeded",
            JobPhase::Failed { .. } => "failed",
            JobPhase::Retrying { .. } => "retrying",
            JobPhase::Skipped(_) => "skipped",
            JobPhase::Deadlettered => "deadlettered",
            JobPhase::WaitingForOperator => "waiting-for-operator",
        };
        *counts.entry(key.to_string()).or_insert(0) += 1;
    }
    counts
}

fn reason_from(signal: &Signal) -> TransitionReason {
    match signal {
        Signal::EvaluateGates(_) => TransitionReason::GateEvaluation,
        Signal::AllocateBudget => TransitionReason::BudgetAllocated,
        Signal::ExecutionSucceeded => TransitionReason::ExecutionSucceeded,
        Signal::ExecutionFailed => TransitionReason::ExecutionFailed("execute() returned Err".into()),
        Signal::RetryDecide(RetryOutcome::Retry { .. }) => TransitionReason::RetryScheduled,
        Signal::RetryDecide(RetryOutcome::Deadletter) => {
            TransitionReason::ExecutionFailed("retries exhausted".into())
        }
        Signal::Cancel => TransitionReason::Cancelled,
        Signal::Timeout => TransitionReason::TimedOut,
        Signal::BackoffElapsed => TransitionReason::BackoffElapsed,
        Signal::OperatorTransition(_) => TransitionReason::OperatorAction("manual transition".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shigoto_types::{Job, JobKindId, JobScope, JobSubject};

    /// A no-op Job that always succeeds.
    struct OkJob {
        id: JobId,
    }

    #[derive(thiserror::Error, Debug)]
    #[error("ok-job error")]
    struct OkError;

    #[async_trait]
    impl Job for OkJob {
        type Output = ();
        type Error = OkError;

        fn id(&self) -> JobId {
            self.id.clone()
        }

        fn kind(&self) -> JobKindId {
            self.id.kind.clone()
        }

        async fn execute(&self) -> Result<(), OkError> {
            Ok(())
        }
    }

    /// A Job that always fails.
    struct FailJob {
        id: JobId,
    }

    #[async_trait]
    impl Job for FailJob {
        type Output = ();
        type Error = OkError;

        fn id(&self) -> JobId {
            self.id.clone()
        }

        fn kind(&self) -> JobKindId {
            self.id.kind.clone()
        }

        async fn execute(&self) -> Result<(), OkError> {
            Err(OkError)
        }
    }

    fn mk_id(kind: &str, subject: &str) -> JobId {
        JobId {
            scope: JobScope::Global,
            kind: JobKindId::new(kind),
            subject: JobSubject::Pinned(subject.into()),
        }
    }

    #[tokio::test]
    async fn single_ok_job_reaches_succeeded_in_one_tick() {
        let scheduler = InProcessScheduler::new("test");
        let id = mk_id("test", "a");
        let mut dag = Dag::new();
        dag.ensure_node(id.clone());
        scheduler
            .register_job(Arc::new(OkJob { id: id.clone() }))
            .await;

        let receipt = scheduler.tick(&mut dag).await.unwrap();
        let phase = scheduler.phase_of(&id).await;
        assert_eq!(phase, JobPhase::Succeeded);
        // Pending → Ready → Running → Succeeded = 3 transitions.
        assert_eq!(receipt.transitions_this_tick.len(), 3);
    }

    #[tokio::test]
    async fn failing_job_deadletters_in_one_tick() {
        let scheduler = InProcessScheduler::new("test");
        let id = mk_id("test", "fail");
        let mut dag = Dag::new();
        dag.ensure_node(id.clone());
        scheduler
            .register_job(Arc::new(FailJob { id: id.clone() }))
            .await;

        scheduler.tick(&mut dag).await.unwrap();
        assert_eq!(scheduler.phase_of(&id).await, JobPhase::Deadlettered);
    }

    #[tokio::test]
    async fn dependency_ordering_runs_root_before_leaf() {
        let scheduler = InProcessScheduler::new("test");
        let root = mk_id("test", "root");
        let leaf = mk_id("test", "leaf");
        let mut dag = Dag::new();
        dag.add_edge(root.clone(), leaf.clone());
        scheduler.register_job(Arc::new(OkJob { id: root.clone() })).await;
        scheduler.register_job(Arc::new(OkJob { id: leaf.clone() })).await;

        scheduler.tick(&mut dag).await.unwrap();
        assert_eq!(scheduler.phase_of(&root).await, JobPhase::Succeeded);
        assert_eq!(scheduler.phase_of(&leaf).await, JobPhase::Succeeded);
    }

    #[tokio::test]
    async fn operator_transition_advances_waiting_for_operator() {
        let scheduler = InProcessScheduler::new("test");
        let id = mk_id("test", "manual");
        // Seed the phase to WaitingForOperator manually.
        {
            let mut state = scheduler.state.write().await;
            state.phases.insert(id.clone(), JobPhase::WaitingForOperator);
        }
        scheduler
            .operator_transition(&id, JobPhase::Ready, TransitionReason::OperatorAction("go".into()))
            .await
            .unwrap();
        assert_eq!(scheduler.phase_of(&id).await, JobPhase::Ready);
    }

    #[tokio::test]
    async fn unregistered_job_in_dag_deadletters() {
        let scheduler = InProcessScheduler::new("test");
        let id = mk_id("test", "ghost");
        let mut dag = Dag::new();
        dag.ensure_node(id.clone());
        // No registration!
        scheduler.tick(&mut dag).await.unwrap();
        assert_eq!(scheduler.phase_of(&id).await, JobPhase::Deadlettered);
    }

    #[tokio::test]
    async fn registered_retry_policy_retries_then_deadletters() {
        use shigoto_retry::RetryPolicy;

        let scheduler = InProcessScheduler::new("test");
        let kind = JobKindId::new("test");
        // Three attempts allowed; zero delay so we can pump ticks fast.
        scheduler
            .register_retry_policy(
                kind.clone(),
                RetryPolicy::Fixed {
                    attempts: 3,
                    delay_ms: 0,
                },
            )
            .await;

        let id = mk_id("test", "always-fails");
        let mut dag = Dag::new();
        dag.ensure_node(id.clone());
        scheduler
            .register_job(Arc::new(FailJob { id: id.clone() }))
            .await;

        // Pump ticks until the job reaches a terminal phase or we hit
        // the safety cap. With Fixed(3, 0) the job should deadletter
        // after 3 attempts (~3 ticks; each tick advances the loop by
        // up to 6 phase changes which is one full retry cycle).
        for _ in 0..8 {
            scheduler.tick(&mut dag).await.unwrap();
            if scheduler.phase_of(&id).await == JobPhase::Deadlettered {
                break;
            }
        }
        assert_eq!(scheduler.phase_of(&id).await, JobPhase::Deadlettered);

        // Authoritative attempt count should be the retry limit.
        let state = scheduler.state.read().await;
        assert_eq!(state.attempts.get(&id).copied(), Some(3));
        assert_eq!(state.failure_history.get(&id).map(Vec::len), Some(3));
    }

    #[tokio::test]
    async fn registered_gate_keeps_job_in_gated_phase() {
        use shigoto_gate::OperatorApproved;
        use std::sync::atomic::{AtomicBool, Ordering};

        let approved = Arc::new(AtomicBool::new(false));
        let approved_check = approved.clone();
        let gate = Arc::new(OperatorApproved::new("manual", move || {
            approved_check.load(Ordering::SeqCst)
        }));

        let scheduler = InProcessScheduler::new("test");
        scheduler
            .register_gate(JobKindId::new("test"), gate)
            .await;
        let id = mk_id("test", "blocked");
        let mut dag = Dag::new();
        dag.ensure_node(id.clone());
        scheduler.register_job(Arc::new(OkJob { id: id.clone() })).await;

        // First tick: gate returns Wait → job lands in Gated.
        scheduler.tick(&mut dag).await.unwrap();
        assert_eq!(scheduler.phase_of(&id).await, JobPhase::Gated);

        // Flip the gate → next tick advances to Succeeded.
        approved.store(true, Ordering::SeqCst);
        scheduler.tick(&mut dag).await.unwrap();
        assert_eq!(scheduler.phase_of(&id).await, JobPhase::Succeeded);
    }

    #[tokio::test]
    async fn emitter_receives_every_transition() {
        use shigoto_emit::TransitionEmitter;
        use std::sync::Mutex;

        struct Capture {
            log: Arc<Mutex<Vec<TransitionEvent>>>,
        }
        impl TransitionEmitter for Capture {
            fn emit(&self, event: TransitionEvent) {
                self.log.lock().unwrap().push(event);
            }
        }

        let log = Arc::new(Mutex::new(Vec::new()));
        let scheduler = InProcessScheduler::new("test")
            .with_emitter(Arc::new(Capture { log: log.clone() }));
        let id = mk_id("test", "emit-me");
        let mut dag = Dag::new();
        dag.ensure_node(id.clone());
        scheduler.register_job(Arc::new(OkJob { id: id.clone() })).await;

        scheduler.tick(&mut dag).await.unwrap();
        let captured = log.lock().unwrap();
        // Pending → Ready → Running → Succeeded = 3 transitions.
        assert_eq!(captured.len(), 3);
        assert_eq!(captured[0].from, JobPhase::Pending);
        assert_eq!(captured[2].to, JobPhase::Succeeded);
    }
}
