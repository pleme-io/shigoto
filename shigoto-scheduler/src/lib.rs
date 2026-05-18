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

use shigoto_budget::BudgetTree;
use shigoto_dag::Dag;
use shigoto_emit::{NullEmitter, TransitionEmitter};
use shigoto_gate::{self, AllUpstreamsTerminal, Gate, GateContext, GateOutcome};
use shigoto_retry::{FailureRecord, RetryDecision, RetryPolicy};
use shigoto_types::{
    advance, ErasedJob, GateAggregate, IllegalTransition, JobId, JobKindId, JobPhase, RetryOutcome,
    Signal, Snapshot, TickReceipt, TransitionEvent, TransitionReason, UnhealedDrift,
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

    /// Read-only snapshot of the current FSM map. Async because the
    /// state lives behind a tokio RwLock; bridging via block_in_place
    /// would panic outside a tokio runtime context.
    async fn snapshot(&self, dag: &Dag) -> Snapshot;
}

// ── InProcessScheduler ───────────────────────────────────────────────

/// Default scheduler — single-process, in-memory FSM state, sequential
/// execution within a wave.
///
/// What v0.1 wires:
/// - Gates: implicit `AllUpstreamsTerminal` (DAG-edge enforcement)
///   plus per-kind registry via `register_gate(kind, Arc<dyn Gate>)`.
///   Multiple gates per kind reduce via `shigoto_gate::reduce`
///   (worst-outcome wins). [M0.9g]
/// - Retry: per-kind `RetryPolicy` via
///   `register_retry_policy(kind, policy)`. Default is NoRetry —
///   first failure → deadletter. NoRetry / Fixed / Exponential /
///   Custom decider all supported. [M0.9h]
/// - Budget: three-dimension envelope via `install_budget(tree)` —
///   global × by-kind × by-scope, min-intersection. allocate on
///   Ready→Running, release on Running→terminal. [M0.9j]
/// - Emitter: `with_emitter(Arc<dyn TransitionEmitter>)` replaces
///   the default NullEmitter. Every FSM transition + every operator
///   action emits in real time. [M0.9d + M0.9l]
/// - Timeout: optional per-job via `set_timeout(id, duration)`;
///   wrapped around `execute()` with `tokio::time::timeout`.
///
/// What v0.1 doesn't yet wire:
/// - Concurrency within a wave: jobs in the same wave run
///   sequentially (each completes before the next starts). Real
///   parallelism + cancellation-token plumbing land when a consumer
///   needs them.
/// - Persistence: in-memory only. A scheduler restart drops every
///   non-terminal job back to Pending. Idempotent jobs tolerate
///   this; persistence is a future Scheduler impl behind the trait.
/// - Per-job age tracking: TickReceipt.unhealed entries have
///   `age_seconds = 0`. Adding timestamps is a future M0.9 step.
///
/// The load-bearing invariant this v0.1 DOES enforce: every phase
/// change goes through `shigoto-types::advance`. The scheduler
/// never invents transitions. Compiler-checked exhaustiveness over
/// JobPhase × Signal makes illegal transitions unrepresentable.
pub struct InProcessScheduler {
    state: tokio::sync::RwLock<SchedulerState>,
    /// Budget allocation lives in its own Mutex (not the main state's
    /// RwLock) because allocate/release happen frequently and we want
    /// fine-grained locking.
    budget: tokio::sync::Mutex<BudgetTree>,
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
            budget: tokio::sync::Mutex::new(BudgetTree::new()),
            tool: tool.into(),
            emitter: Arc::new(NullEmitter),
        }
    }

    /// Replace the default unbounded BudgetTree with a configured one.
    /// The scheduler's Ready→Running transition becomes a real
    /// allocation check; Running→terminal releases.
    pub async fn install_budget(&self, budget: BudgetTree) {
        let mut b = self.budget.lock().await;
        *b = budget;
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
    /// FSM return `IllegalTransition`. The transition emits through
    /// the registered TransitionEmitter immediately (operator actions
    /// are auditable in real time, not deferred to the next tick).
    pub async fn operator_transition(
        &self,
        id: &JobId,
        target: JobPhase,
        reason: TransitionReason,
    ) -> Result<(), SchedulerError> {
        let from = {
            let state = self.state.read().await;
            state.phases.get(id).cloned().unwrap_or(JobPhase::Pending)
        };
        let signal = Signal::OperatorTransition(target);
        let to = advance(from.clone(), signal.clone())?;

        // Mutate the phase map.
        {
            let mut state = self.state.write().await;
            state.phases.insert(id.clone(), to.clone());
        }

        // Emit the transition with the operator-supplied reason
        // (overrides the default reason_from(signal) which would just
        // produce "manual transition"). Audit log + observability
        // sinks see operator context immediately.
        let event = TransitionEvent {
            at: chrono::Utc::now(),
            job_id: id.clone(),
            from,
            to,
            reason,
            tool: self.tool.clone(),
        };
        self.emitter.emit(event);
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
        // Use Snapshot's typed projection (one canonical key set).
        // BTreeMap<&'static str, u32> → BTreeMap<String, u32> for
        // TickReceipt's owned-string surface.
        let phase_counts = snapshot
            .phase_counts()
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        let unhealed = collect_unhealed(&snapshot.phases);

        Ok(TickReceipt {
            tick_at: started_at,
            phase_counts,
            transitions_this_tick: transitions,
            unhealed,
        })
    }

    async fn snapshot(&self, _dag: &Dag) -> Snapshot {
        self.snapshot_inner().await
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

            // Budget allocation — try to reserve a slot across global
            // × by-kind × by-scope. On Err we stay Ready; the next
            // tick re-tries when another job has released a slot.
            JobPhase::Ready => {
                let allocated = {
                    let mut budget = self.budget.lock().await;
                    budget.try_allocate(id).is_ok()
                };
                if !allocated {
                    return Ok(false);
                }
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
                {
                    let mut state = self.state.write().await;
                    state.attempts.remove(id);
                    state.failure_history.remove(id);
                }
                // Release the budget slot acquired at Ready→Running.
                self.budget.lock().await.release(id);
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
                // Release the budget slot acquired at Ready→Running.
                // Even if the policy decides Retry, the slot returns
                // to the pool — the retry's later Ready→Running will
                // re-allocate.
                self.budget.lock().await.release(id);
                // The retry decision dispatch happens on the NEXT
                // advance_once call (Failed → RetryDecide); this lets
                // the per-job loop in tick() handle it uniformly.
            }
        }

        Ok(())
    }
}

/// Project the FSM snapshot to the subset of jobs requiring operator
/// attention — Deadlettered (terminal failure; needs operator to
/// retry-from-scratch or accept) and WaitingForOperator (paused
/// pending decision). Failed and Retrying are transient (the next
/// tick resolves them) so they don't surface here.
///
/// age_seconds is 0 in v0.1 — requires per-job last-advanced
/// timestamps which the scheduler doesn't track yet. A future
/// milestone wires that in (would also unblock §III.13's "dirty
/// repo > 24h" gate).
fn collect_unhealed(phases: &HashMap<JobId, JobPhase>) -> Vec<UnhealedDrift> {
    let mut out: Vec<UnhealedDrift> = Vec::new();
    for (id, phase) in phases {
        let stuck = matches!(
            phase,
            JobPhase::Deadlettered | JobPhase::WaitingForOperator
        );
        if stuck {
            out.push(UnhealedDrift {
                job_id: id.clone(),
                phase: phase.clone(),
                age_seconds: 0,
            });
        }
    }
    // Stable order by stringified subject so receipts diff cleanly
    // across ticks even though HashMap iteration is non-deterministic.
    out.sort_by(|a, b| stable_job_key(&a.job_id).cmp(&stable_job_key(&b.job_id)));
    out
}

fn stable_job_key(id: &JobId) -> String {
    use shigoto_types::JobSubject;
    let subject = match &id.subject {
        JobSubject::None => String::new(),
        JobSubject::Repo(r) => format!("repo:{r}"),
        JobSubject::Org(o) => format!("org:{o}"),
        JobSubject::Path(p) => format!("path:{}", p.display()),
        JobSubject::Pinned(s) => format!("pin:{s}"),
    };
    format!("{}|{}", id.kind.0, subject)
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
        let id = mk_id("test", "manual");
        // Seed the phase to WaitingForOperator manually.
        {
            let mut state = scheduler.state.write().await;
            state.phases.insert(id.clone(), JobPhase::WaitingForOperator);
        }
        scheduler
            .operator_transition(
                &id,
                JobPhase::Ready,
                TransitionReason::OperatorAction("go".into()),
            )
            .await
            .unwrap();
        assert_eq!(scheduler.phase_of(&id).await, JobPhase::Ready);

        // Operator action emits immediately (not deferred to next tick).
        let captured = log.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].from, JobPhase::WaitingForOperator);
        assert_eq!(captured[0].to, JobPhase::Ready);
        assert!(matches!(
            &captured[0].reason,
            TransitionReason::OperatorAction(s) if s == "go"
        ));
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

    /// End-to-end integration: gates + retry + budget + emitter +
    /// dependency-ordered DAG all in one tick sequence. Proves the
    /// pieces compose, not just work individually.
    ///
    /// Shape:
    ///     root  ─→  middle  ─→  leaf
    ///   (OK)   (gate: bool)  (fail twice → succeed)
    ///
    /// - root succeeds immediately (no gate).
    /// - middle has a gate that defaults to false; we flip it after
    ///   tick 1.
    /// - leaf fails the first time, succeeds the second (Fixed(2, 0)
    ///   retry policy).
    /// - All transitions captured by an emitter; assert the order +
    ///   final phases.
    #[tokio::test]
    async fn integration_diamond_with_gate_and_retry_walks_expected_path() {
        use shigoto_gate::OperatorApproved;
        use shigoto_retry::RetryPolicy;
        use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
        use std::sync::Mutex;

        // ── Test jobs ───────────────────────────────────────
        // Middle's gate flips when this flips.
        let middle_gate_open = Arc::new(AtomicBool::new(false));

        // Leaf's first call fails, second succeeds (just to exercise
        // the retry path).
        struct FlakyLeaf {
            id: JobId,
            attempts: Arc<AtomicU32>,
        }

        #[async_trait]
        impl shigoto_types::Job for FlakyLeaf {
            type Output = ();
            type Error = OkError;
            fn id(&self) -> JobId {
                self.id.clone()
            }
            fn kind(&self) -> JobKindId {
                self.id.kind.clone()
            }
            async fn execute(&self) -> Result<(), OkError> {
                let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    Err(OkError)
                } else {
                    Ok(())
                }
            }
        }

        // ── Capturing emitter ──────────────────────────────
        struct Capture {
            log: Arc<Mutex<Vec<TransitionEvent>>>,
        }
        impl shigoto_emit::TransitionEmitter for Capture {
            fn emit(&self, event: TransitionEvent) {
                self.log.lock().unwrap().push(event);
            }
        }
        let log = Arc::new(Mutex::new(Vec::new()));

        // ── Scheduler ──────────────────────────────────────
        let scheduler = InProcessScheduler::new("integration")
            .with_emitter(Arc::new(Capture { log: log.clone() }));
        // Three distinct kinds so policies / gates apply per-node
        // rather than collide via shared kind.
        let root_kind = JobKindId::new("root-kind");
        let middle_kind = JobKindId::new("middle-kind");
        let leaf_kind = JobKindId::new("leaf-kind");

        // Middle's gate: closure reads our shared atomic.
        let gate_check = middle_gate_open.clone();
        scheduler
            .register_gate(
                middle_kind.clone(),
                Arc::new(OperatorApproved::new("middle-gate", move || {
                    gate_check.load(Ordering::SeqCst)
                })),
            )
            .await;

        // Leaf retries up to 2 attempts with zero delay.
        scheduler
            .register_retry_policy(
                leaf_kind.clone(),
                RetryPolicy::Fixed {
                    attempts: 2,
                    delay_ms: 0,
                },
            )
            .await;

        // ── DAG ────────────────────────────────────────────
        let root = JobId {
            scope: JobScope::Global,
            kind: root_kind.clone(),
            subject: JobSubject::Pinned("root".into()),
        };
        let middle = JobId {
            scope: JobScope::Global,
            kind: middle_kind.clone(),
            subject: JobSubject::Pinned("middle".into()),
        };
        let leaf = JobId {
            scope: JobScope::Global,
            kind: leaf_kind.clone(),
            subject: JobSubject::Pinned("leaf".into()),
        };
        let mut dag = Dag::new();
        dag.add_edge(root.clone(), middle.clone());
        dag.add_edge(middle.clone(), leaf.clone());

        scheduler
            .register_job(Arc::new(OkJob { id: root.clone() }))
            .await;
        scheduler
            .register_job(Arc::new(OkJob { id: middle.clone() }))
            .await;
        let leaf_attempts = Arc::new(AtomicU32::new(0));
        scheduler
            .register_job(Arc::new(FlakyLeaf {
                id: leaf.clone(),
                attempts: leaf_attempts.clone(),
            }))
            .await;

        // ── Tick 1 ─────────────────────────────────────────
        // root → Succeeded. middle: AllUpstreamsTerminal passes (root
        // succeeded) but OperatorApproved gate is closed → Gated.
        // leaf: AllUpstreamsTerminal waits on middle → stays Pending.
        scheduler.tick(&mut dag).await.unwrap();
        assert_eq!(scheduler.phase_of(&root).await, JobPhase::Succeeded);
        assert_eq!(scheduler.phase_of(&middle).await, JobPhase::Gated);
        let leaf_phase = scheduler.phase_of(&leaf).await;
        assert!(
            matches!(leaf_phase, JobPhase::Pending | JobPhase::Gated),
            "expected leaf in Pending/Gated, got {leaf_phase:?}"
        );

        // ── Flip middle's gate, pump ticks until convergence ────
        middle_gate_open.store(true, Ordering::SeqCst);

        // Schedulers guarantee eventual progress, not single-tick
        // progress — gate state changes propagate across at most
        // one tick per affected job. Pump up to 16 ticks then
        // assert final state (way more than enough for 3 jobs).
        for _ in 0..16 {
            let m = scheduler.phase_of(&middle).await;
            let l = scheduler.phase_of(&leaf).await;
            let m_done = matches!(m, JobPhase::Succeeded | JobPhase::Deadlettered);
            let l_done = matches!(l, JobPhase::Succeeded | JobPhase::Deadlettered);
            if m_done && l_done {
                break;
            }
            scheduler.tick(&mut dag).await.unwrap();
        }
        assert_eq!(
            scheduler.phase_of(&middle).await,
            JobPhase::Succeeded,
            "middle should succeed once gate is open"
        );
        assert_eq!(
            scheduler.phase_of(&leaf).await,
            JobPhase::Succeeded,
            "leaf should succeed on its retry"
        );
        // Leaf executed exactly twice — first fail, second success.
        assert_eq!(leaf_attempts.load(Ordering::SeqCst), 2);

        // ── Verify emitter captured every transition ───────
        let captured = log.lock().unwrap();
        // Every captured event has a non-empty tool tag.
        assert!(captured.iter().all(|e| e.tool == "integration"));
        // We should have at least: root (3 transitions) + middle (4+
        // including Gated detour) + leaf (cycle through retry).
        // Just assert a reasonable lower bound rather than the exact
        // count (depends on tick-cap-induced re-evaluations).
        assert!(
            captured.len() >= 10,
            "expected ≥10 transitions across 3 jobs + retry, got {}",
            captured.len()
        );
        // Every job reached Succeeded as terminal — assert one
        // transition with to=Succeeded per job.
        let succeeded_jobs: std::collections::HashSet<JobId> = captured
            .iter()
            .filter(|e| e.to == JobPhase::Succeeded)
            .map(|e| e.job_id.clone())
            .collect();
        assert!(succeeded_jobs.contains(&root));
        assert!(succeeded_jobs.contains(&middle));
        assert!(succeeded_jobs.contains(&leaf));
    }

    #[tokio::test]
    async fn unhealed_in_receipt_lists_deadlettered_and_waiting() {
        let scheduler = InProcessScheduler::new("test");

        // One Deadlettered (FailJob with NoRetry default).
        let dead = mk_id("test", "dead");
        let mut dag = Dag::new();
        dag.ensure_node(dead.clone());
        scheduler.register_job(Arc::new(FailJob { id: dead.clone() })).await;

        // One WaitingForOperator (seeded directly).
        let waiting = mk_id("test", "waiting");
        {
            let mut state = scheduler.state.write().await;
            state
                .phases
                .insert(waiting.clone(), JobPhase::WaitingForOperator);
        }

        // One Succeeded (OkJob).
        let happy = mk_id("test", "happy");
        dag.ensure_node(happy.clone());
        scheduler.register_job(Arc::new(OkJob { id: happy.clone() })).await;

        let receipt = scheduler.tick(&mut dag).await.unwrap();

        // Deadlettered and WaitingForOperator surface; happy doesn't.
        let unhealed_ids: Vec<&JobId> = receipt.unhealed.iter().map(|u| &u.job_id).collect();
        assert_eq!(receipt.unhealed.len(), 2);
        assert!(unhealed_ids.contains(&&dead));
        assert!(unhealed_ids.contains(&&waiting));
        assert!(!unhealed_ids.contains(&&happy));

        // Deterministic order (sorted by stable job key).
        let mut sorted_keys: Vec<String> = receipt
            .unhealed
            .iter()
            .map(|u| stable_job_key(&u.job_id))
            .collect();
        let original_order = sorted_keys.clone();
        sorted_keys.sort();
        assert_eq!(sorted_keys, original_order);
    }

    #[tokio::test]
    async fn budget_exhaustion_keeps_job_in_ready_phase() {
        use shigoto_budget::{BudgetSpec, BudgetTree};

        let scheduler = InProcessScheduler::new("test");

        // Install a budget with global=1, pre-allocate that slot to a
        // dummy JobId so the budget is exhausted when our real job
        // tries to allocate.
        let mut budget = BudgetTree::new();
        budget.global = Some(BudgetSpec::max_concurrent(1));
        let dummy = mk_id("test", "dummy-holder");
        budget.try_allocate(&dummy).unwrap();
        scheduler.install_budget(budget).await;

        let id = mk_id("test", "want-budget");
        let mut dag = Dag::new();
        dag.ensure_node(id.clone());
        scheduler.register_job(Arc::new(OkJob { id: id.clone() })).await;

        // Tick: gate evaluates → Ready. Budget allocation fails → stay Ready.
        scheduler.tick(&mut dag).await.unwrap();
        assert_eq!(scheduler.phase_of(&id).await, JobPhase::Ready);

        // Release the dummy holder's slot.
        scheduler.budget.lock().await.release(&dummy);

        // Tick: budget free → Ready → Running → Succeeded.
        scheduler.tick(&mut dag).await.unwrap();
        assert_eq!(scheduler.phase_of(&id).await, JobPhase::Succeeded);
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
