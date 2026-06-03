//! shigoto-retry — typed failure-recovery strategies.
//!
//! Spec: `theory/SHIGOTO.md` §III.8 + §VII.
//!
//! Per-kind `RetryPolicy` registered with the scheduler. On every
//! `Failed → Retrying|Deadletter` decision the scheduler calls
//! `policy.decide(attempt, history)`. Default is `NoRetry` —
//! deadletter on first failure.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shigoto_types::FailureKind;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RetryPolicy {
    NoRetry,
    Fixed {
        attempts: u32,
        delay_ms: u64,
    },
    Exponential {
        attempts: u32,
        base_ms: u64,
        max_ms: u64,
        jitter: f64,
    },
    #[serde(skip)]
    Custom(Arc<dyn RetryDecider>),
}

impl RetryPolicy {
    /// Decide whether to retry. `attempt` is the 1-based attempt number
    /// that just FAILED (so the first failure is attempt=1). `history`
    /// carries the failure records prior to this decision.
    #[must_use]
    pub fn decide(&self, attempt: u32, history: &[FailureRecord]) -> RetryDecision {
        // META: Declarative failures route to Deadletter regardless of
        // policy / attempt budget. The operator-supplied declaration is
        // broken; no amount of retrying will fix it without operator
        // action. See `shigoto-types::failure` for the classification.
        //
        // This is what makes `RetryPolicy::Exponential { attempts: 100 }`
        // behave correctly: transient failures get 100 retries; the
        // first declarative failure stops the loop and surfaces in
        // status. Same policy, different outcome based on failure kind.
        if let Some(latest) = history.last() {
            if latest.kind == FailureKind::Declarative {
                return RetryDecision::Deadletter;
            }
        }
        match self {
            Self::NoRetry => RetryDecision::Deadletter,
            Self::Fixed { attempts, delay_ms } => {
                if attempt >= *attempts {
                    RetryDecision::Deadletter
                } else {
                    RetryDecision::Retry {
                        after: Duration::from_millis(*delay_ms),
                    }
                }
            }
            Self::Exponential {
                attempts,
                base_ms,
                max_ms,
                jitter,
            } => {
                if attempt >= *attempts {
                    return RetryDecision::Deadletter;
                }
                // Backoff = min(base * 2^(attempt-1), max), then jittered.
                let exp = base_ms.saturating_mul(1u64 << (attempt - 1).min(30));
                let capped = exp.min(*max_ms);
                let jittered = jitter_ms(capped, *jitter, attempt);
                RetryDecision::Retry {
                    after: Duration::from_millis(jittered),
                }
            }
            Self::Custom(decider) => decider.decide(attempt, history),
        }
    }
}

pub trait RetryDecider: std::fmt::Debug + Send + Sync {
    fn decide(&self, attempt: u32, history: &[FailureRecord]) -> RetryDecision;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryDecision {
    Retry { after: Duration },
    Deadletter,
}

#[derive(Debug, Clone)]
pub struct FailureRecord {
    pub attempt: u32,
    pub at_ms: i64,
    pub error: String,
    /// Typed classification (defaults to `Transient` for callers that
    /// haven't migrated). `RetryPolicy::decide` short-circuits to
    /// `Deadletter` whenever the latest record's `kind == Declarative`,
    /// regardless of the policy's attempt budget. See
    /// `shigoto-types::failure` for the classifier + signature helpers.
    #[doc(alias = "classification")]
    pub kind: FailureKind,
}

impl FailureRecord {
    /// Construct a record with conservative default classification
    /// (`Transient`). Use this when the caller doesn't know how to
    /// classify yet — preserves prior behaviour (always retry per
    /// policy). Upgrade call sites to `with_kind` as they migrate.
    #[must_use]
    pub fn new(attempt: u32, at_ms: i64, error: impl Into<String>) -> Self {
        Self {
            attempt,
            at_ms,
            error: error.into(),
            kind: FailureKind::Transient,
        }
    }

    /// Construct a typed record — preferred for daemons that classify
    /// at the failure site.
    #[must_use]
    pub fn with_kind(
        attempt: u32,
        at_ms: i64,
        error: impl Into<String>,
        kind: FailureKind,
    ) -> Self {
        Self {
            attempt,
            at_ms,
            error: error.into(),
            kind,
        }
    }
}

/// Deterministic-pseudo-random jitter: `+/- (jitter * base)` derived
/// from `attempt` so tests are reproducible. No external RNG.
fn jitter_ms(base: u64, jitter: f64, attempt: u32) -> u64 {
    if jitter <= 0.0 {
        return base;
    }
    // Map attempt → [-1.0, 1.0] via a stable hash.
    let h = (attempt.wrapping_mul(2654435761)) as f32;
    let fraction = (h.to_bits() & 0xFFFF) as f32 / 65535.0; // [0.0, 1.0]
    let signed = (fraction * 2.0 - 1.0) as f64; // [-1.0, 1.0]
    let delta = (base as f64) * jitter * signed;
    ((base as f64) + delta).max(0.0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_retry_always_deadletters() {
        assert_eq!(
            RetryPolicy::NoRetry.decide(1, &[]),
            RetryDecision::Deadletter
        );
        assert_eq!(
            RetryPolicy::NoRetry.decide(5, &[]),
            RetryDecision::Deadletter
        );
    }

    /// META invariant — Declarative failures route to Deadletter
    /// regardless of policy / attempt budget. This is the typed
    /// substrate's distinguishing behaviour: same `RetryPolicy`,
    /// different outcome by failure kind. Verifiability gate per the
    /// Compounding Directive — explicit test of the META primitive.
    #[test]
    fn declarative_failure_deadletters_regardless_of_policy() {
        let declarative = FailureRecord::with_kind(
            1,
            0,
            "error: does not provide attribute 'foo'",
            FailureKind::Declarative,
        );

        // Even with generous Fixed/Exponential budgets, Declarative
        // short-circuits to Deadletter.
        let fixed = RetryPolicy::Fixed {
            attempts: 100,
            delay_ms: 50,
        };
        assert_eq!(
            fixed.decide(1, &[declarative.clone()]),
            RetryDecision::Deadletter
        );

        let exp = RetryPolicy::Exponential {
            attempts: 100,
            base_ms: 100,
            max_ms: 60_000,
            jitter: 0.1,
        };
        assert_eq!(
            exp.decide(1, &[declarative.clone()]),
            RetryDecision::Deadletter
        );
    }

    #[test]
    fn transient_failure_honors_policy_budget() {
        let transient = FailureRecord::with_kind(
            1,
            0,
            "ssh: connect: Connection refused",
            FailureKind::Transient,
        );

        // Transient gets the policy's full retry budget.
        let fixed = RetryPolicy::Fixed {
            attempts: 3,
            delay_ms: 100,
        };
        match fixed.decide(1, &[transient.clone()]) {
            RetryDecision::Retry { .. } => (),
            d => panic!("expected Retry on Transient, got {d:?}"),
        }
    }

    #[test]
    fn declarative_after_transient_history_still_deadletters() {
        // The LATEST failure's kind matters — a trailing Declarative
        // overrides any prior Transient history.
        let history = vec![
            FailureRecord::with_kind(1, 0, "blip", FailureKind::Transient),
            FailureRecord::with_kind(2, 0, "blip", FailureKind::Transient),
            FailureRecord::with_kind(3, 0, "error: does not exist", FailureKind::Declarative),
        ];
        let fixed = RetryPolicy::Fixed {
            attempts: 100,
            delay_ms: 50,
        };
        assert_eq!(fixed.decide(3, &history), RetryDecision::Deadletter);
    }

    #[test]
    fn empty_history_falls_through_to_existing_policy() {
        // Backwards compat — no failure history means existing
        // policy logic decides. This protects every existing
        // consumer that hasn't migrated to typed failure records.
        let fixed = RetryPolicy::Fixed {
            attempts: 3,
            delay_ms: 100,
        };
        match fixed.decide(1, &[]) {
            RetryDecision::Retry { .. } => (),
            d => panic!("expected Retry on empty history, got {d:?}"),
        }
    }

    #[test]
    fn fixed_retries_until_limit() {
        let p = RetryPolicy::Fixed {
            attempts: 3,
            delay_ms: 100,
        };
        match p.decide(1, &[]) {
            RetryDecision::Retry { after } => assert_eq!(after.as_millis(), 100),
            d => panic!("expected Retry, got {d:?}"),
        }
        match p.decide(2, &[]) {
            RetryDecision::Retry { after } => assert_eq!(after.as_millis(), 100),
            d => panic!("expected Retry, got {d:?}"),
        }
        assert_eq!(p.decide(3, &[]), RetryDecision::Deadletter);
        assert_eq!(p.decide(4, &[]), RetryDecision::Deadletter);
    }

    #[test]
    fn exponential_doubles_until_max() {
        let p = RetryPolicy::Exponential {
            attempts: 5,
            base_ms: 100,
            max_ms: 1000,
            jitter: 0.0,
        };
        // attempt=1: base * 2^0 = 100
        // attempt=2: base * 2^1 = 200
        // attempt=3: base * 2^2 = 400
        // attempt=4: base * 2^3 = 800, capped to 1000 → still 800 (under cap)
        // attempt=5: deadletter
        let ms = |attempt| match p.decide(attempt, &[]) {
            RetryDecision::Retry { after } => after.as_millis() as u64,
            other => panic!("expected Retry, got {other:?}"),
        };
        assert_eq!(ms(1), 100);
        assert_eq!(ms(2), 200);
        assert_eq!(ms(3), 400);
        assert_eq!(ms(4), 800);
        assert_eq!(p.decide(5, &[]), RetryDecision::Deadletter);
    }

    #[test]
    fn exponential_caps_at_max() {
        let p = RetryPolicy::Exponential {
            attempts: 10,
            base_ms: 100,
            max_ms: 500,
            jitter: 0.0,
        };
        // attempt=4 wants 800 but max=500 caps it.
        match p.decide(4, &[]) {
            RetryDecision::Retry { after } => {
                assert_eq!(after.as_millis(), 500);
            }
            d => panic!("expected Retry, got {d:?}"),
        }
        // attempt=5: 1600 capped to 500.
        match p.decide(5, &[]) {
            RetryDecision::Retry { after } => {
                assert_eq!(after.as_millis(), 500);
            }
            d => panic!("expected Retry, got {d:?}"),
        }
    }

    #[test]
    fn exponential_jitter_stays_within_bounds() {
        let p = RetryPolicy::Exponential {
            attempts: 10,
            base_ms: 1000,
            max_ms: 1_000_000,
            jitter: 0.2,
        };
        // attempt=1: base = 1000. With 20% jitter, output in [800, 1200].
        for attempt in 1..=5 {
            match p.decide(attempt, &[]) {
                RetryDecision::Retry { after } => {
                    let ms = after.as_millis() as u64;
                    let expected = 1000u64 << (attempt - 1);
                    let low = (expected as f64 * 0.8) as u64;
                    let high = (expected as f64 * 1.2) as u64;
                    assert!(
                        ms >= low && ms <= high,
                        "attempt {attempt}: {ms} not in [{low}, {high}]"
                    );
                }
                d => panic!("expected Retry, got {d:?}"),
            }
        }
    }

    #[test]
    fn custom_decider_short_circuits() {
        #[derive(Debug)]
        struct AlwaysDeadletter;
        impl RetryDecider for AlwaysDeadletter {
            fn decide(&self, _attempt: u32, _history: &[FailureRecord]) -> RetryDecision {
                RetryDecision::Deadletter
            }
        }
        let p = RetryPolicy::Custom(Arc::new(AlwaysDeadletter));
        // Even though Fixed/Exponential would retry on attempt=1, the
        // Custom decider's say is final.
        assert_eq!(p.decide(1, &[]), RetryDecision::Deadletter);
    }
}
