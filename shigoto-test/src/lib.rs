//! shigoto-test — test helpers for consumers.
//!
//! Per `theory/SHIGOTO.md` §III.13: the canonical proof that a Job
//! is idempotent. v0.1 ships `idempotence_quickcheck` — invokes
//! `execute()` twice with a consumer-provided "observe domain state"
//! closure, asserts that:
//!   (a) both calls succeed (Job is even runnable twice)
//!   (b) state after second call equals state after first call (the
//!       second call had no net effect)
//!
//! Consumers parametrize on what "state" means for their job — a
//! checked-in file's contents, a row count in a table, a flag in
//! memory. The harness is generic over any `S: PartialEq + Debug`.

#![forbid(unsafe_code)]

use std::fmt::Debug;

use shigoto_types::Job;

#[derive(Debug, thiserror::Error)]
pub enum IdempotenceViolation {
    /// The first `execute()` call returned Err. Can't be idempotent
    /// if it fails at all.
    #[error("first execute failed: {0}")]
    FirstFailed(String),
    /// The second `execute()` call returned Err. Idempotent jobs
    /// should succeed on every invocation — a failure on the second
    /// implies side-effect coupling.
    #[error("second execute failed (idempotent jobs succeed every time): {0}")]
    SecondFailed(String),
    /// Domain state changed between the first and second execute.
    /// The job re-mutated something on the second call instead of
    /// no-op'ing.
    #[error("domain state changed between calls — first={first}, second={second}")]
    StateChanged { first: String, second: String },
}

/// Run `execute()` twice and verify idempotence.
///
/// `observe_state` is a closure the caller provides that captures
/// the relevant domain state. The harness compares its return value
/// after the first execute vs after the second; equality proves the
/// second call was a no-op at the state-mutation level.
///
/// `S` must be PartialEq + Debug. Common choices:
///   - `String` for "contents of a file"
///   - `u64` for "row count" or "version number"
///   - a typed struct for compound state
///
/// Returns Ok(()) when both executes succeed and state matches; Err
/// with the typed reason otherwise.
pub async fn idempotence_quickcheck<J, S, F>(
    job: &J,
    observe_state: F,
) -> Result<(), IdempotenceViolation>
where
    J: Job,
    S: PartialEq + Debug,
    F: Fn() -> S,
{
    job.execute()
        .await
        .map_err(|e| IdempotenceViolation::FirstFailed(format!("{e}")))?;
    let state_after_first = observe_state();

    job.execute()
        .await
        .map_err(|e| IdempotenceViolation::SecondFailed(format!("{e}")))?;
    let state_after_second = observe_state();

    if state_after_first != state_after_second {
        return Err(IdempotenceViolation::StateChanged {
            first: format!("{state_after_first:?}"),
            second: format!("{state_after_second:?}"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use shigoto_types::{JobId, JobKindId, JobScope, JobSubject};
    use std::sync::Mutex;

    fn dummy_id(name: &str) -> JobId {
        JobId {
            scope: JobScope::Global,
            kind: JobKindId::new("test"),
            subject: JobSubject::Pinned(name.into()),
        }
    }

    #[derive(thiserror::Error, Debug)]
    #[error("dummy error")]
    struct DummyError;

    /// Job that increments a counter — NOT idempotent.
    struct IncrementJob {
        counter: std::sync::Arc<Mutex<u32>>,
    }

    #[async_trait]
    impl Job for IncrementJob {
        type Output = ();
        type Error = DummyError;
        fn id(&self) -> JobId {
            dummy_id("increment")
        }
        fn kind(&self) -> JobKindId {
            JobKindId::new("test")
        }
        async fn execute(&self) -> Result<(), DummyError> {
            *self.counter.lock().unwrap() += 1;
            Ok(())
        }
    }

    /// Job that sets a flag once — IDEMPOTENT (second call observes
    /// flag already set; doesn't mutate further).
    struct SetFlagJob {
        flag: std::sync::Arc<Mutex<bool>>,
    }

    #[async_trait]
    impl Job for SetFlagJob {
        type Output = ();
        type Error = DummyError;
        fn id(&self) -> JobId {
            dummy_id("set-flag")
        }
        fn kind(&self) -> JobKindId {
            JobKindId::new("test")
        }
        async fn execute(&self) -> Result<(), DummyError> {
            *self.flag.lock().unwrap() = true;
            Ok(())
        }
    }

    /// Job that fails. Idempotence harness rejects it on the first
    /// execute.
    struct AlwaysFailJob;

    #[async_trait]
    impl Job for AlwaysFailJob {
        type Output = ();
        type Error = DummyError;
        fn id(&self) -> JobId {
            dummy_id("fail")
        }
        fn kind(&self) -> JobKindId {
            JobKindId::new("test")
        }
        async fn execute(&self) -> Result<(), DummyError> {
            Err(DummyError)
        }
    }

    #[tokio::test]
    async fn increment_job_is_not_idempotent_state_changed() {
        let counter = std::sync::Arc::new(Mutex::new(0u32));
        let job = IncrementJob {
            counter: counter.clone(),
        };
        let snapshot = || *counter.lock().unwrap();
        let result = idempotence_quickcheck(&job, snapshot).await;
        match result {
            Err(IdempotenceViolation::StateChanged { .. }) => {}
            other => panic!("expected StateChanged, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn set_flag_job_is_idempotent() {
        let flag = std::sync::Arc::new(Mutex::new(false));
        let job = SetFlagJob { flag: flag.clone() };
        let snapshot = || *flag.lock().unwrap();
        let result = idempotence_quickcheck(&job, snapshot).await;
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        assert!(*flag.lock().unwrap()); // flag actually set
    }

    #[tokio::test]
    async fn failing_job_rejected_at_first_execute() {
        let result = idempotence_quickcheck(&AlwaysFailJob, || 0u32).await;
        match result {
            Err(IdempotenceViolation::FirstFailed(_)) => {}
            other => panic!("expected FirstFailed, got {other:?}"),
        }
    }
}
