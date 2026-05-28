//! Typed stuck-state escalation — the canonical `TimeoutWatcher<S>`
//! primitive every fleet-wide FSM-timeout watchdog consumes.
//! Spec: `theory/CONVERGENCE-ADOPTION.md` §II.4, Phase 0.3.
//!
//! Subsumes the hand-rolled "stuck in state X for too long" shapes
//! that previously lived in:
//!
//! - `pangea-operator::reactive::check_phase_timeout` — per-phase
//!   thresholds on `Phase` FSM (Compiling 5m / Planning 10m /
//!   Applying 30m)
//! - `lava-operator::check_verified_blocked` — Verified=False for
//!   >10m → Escalate
//! - `tend::SAFE-CONVERGENCE M5 StuckByFingerprint` (planned) —
//!   same DriftEvent fingerprint repeating for >N cycles
//!
//! The watcher is **pure** — given current state + when state was
//! entered + current time, returns an optional `WatchAction` to take.
//! The caller decides whether to fire the action (emit metrics, send
//! notification, transition phase). No I/O in the trait.
//!
//! # Per-state rule chain
//!
//! Implementations carry a `Vec<(S, Duration, WatchAction)>` — for
//! each state-class the watcher cares about, a threshold + action.
//! On `evaluate`, the watcher finds the first matching state-class
//! and checks whether `now - entered_at >= threshold`. First match
//! wins, mirroring `ChainedClassifier`'s rule-order semantics.
//!
//! # The trait law
//!
//! For any watcher `w`, state `s`, entry time `e`, and current time `n`:
//!
//!   `w.evaluate(s, e, n) == w.evaluate(s, e, n)`   (determinism)
//!
//! Pure function of its inputs. No clock side effects, no random
//! tiebreaks. The watcher reads only what's passed in.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Typed action to take when an FSM state has been stuck longer
/// than its declared threshold. Mirrors pangea-operator's
/// `ReactiveAction` shape so the two reconciler layers compose.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum WatchAction {
    /// Log + emit a structured event + set a Healthy=False condition,
    /// but keep reconciling. Lowest-urgency escalation.
    Alert(EscalationRouting),

    /// Patch state so the reconciler short-circuits until an operator
    /// clears the auto-suspend flag. For "we are not making progress;
    /// stop burning resources until a human looks."
    Suspend,

    /// Highest-urgency notify (operator paging). Same shape as Alert
    /// but the routing layer escalates priority.
    Page(EscalationRouting),
}

/// Where to send an escalation notification. The watcher only carries
/// the typed routing intent; the notification delivery is a separate
/// concern (ntfy / Slack / GitHub issue / Datadog event).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EscalationRouting {
    /// ntfy topic (https://ntfy.sh) for low-friction alerts.
    pub ntfy_topic: Option<String>,
    /// Slack channel (with leading #) for team-channel alerts.
    pub slack_channel: Option<String>,
    /// GitHub issue template name for filing a structured issue.
    pub github_issue_template: Option<String>,
    /// Free-form additional routing key (e.g. PagerDuty service id).
    pub routing_key: Option<String>,
}

impl EscalationRouting {
    /// True when no routing is configured. Empty routing still
    /// produces a structured log line; explicit notifications need
    /// at least one route set.
    pub fn is_empty(&self) -> bool {
        self.ntfy_topic.is_none()
            && self.slack_channel.is_none()
            && self.github_issue_template.is_none()
            && self.routing_key.is_none()
    }
}

/// Per-state watch rule: when in `state` for at least `threshold`,
/// fire `action`. The watcher iterates rules in declared order and
/// returns the first matching action.
#[derive(Debug, Clone)]
pub struct WatchRule<S> {
    pub state: S,
    pub threshold: Duration,
    pub action: WatchAction,
}

/// Stuck-state escalation watchdog. Carries per-state thresholds +
/// actions. Pure evaluation: `(state, entered_at, now) → Option<WatchAction>`.
///
/// `S: PartialEq` is the only state-class bound — states compare by
/// equality, not hash. Allows states that don't implement `Hash`
/// (enums with non-hashable variants) to still be watched.
#[derive(Debug, Clone, Default)]
pub struct TimeoutWatcher<S> {
    rules: Vec<WatchRule<S>>,
}

impl<S> TimeoutWatcher<S> {
    /// Empty watcher — `evaluate` always returns `None`.
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Append a rule. Returns self for fluent chaining.
    #[must_use]
    pub fn with_rule(mut self, state: S, threshold: Duration, action: WatchAction) -> Self {
        self.rules.push(WatchRule {
            state,
            threshold,
            action,
        });
        self
    }

    /// Number of rules declared.
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }

    /// `true` when no rules declared (watcher is a no-op).
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }
}

impl<S: PartialEq> TimeoutWatcher<S> {
    /// Evaluate against the current state + when state was entered +
    /// current time. Returns the first matching action whose threshold
    /// has elapsed, or `None` when no rule fires.
    ///
    /// When `entered_at > now` (clock skew / replay), elapsed is
    /// treated as zero — never returns an action based on negative
    /// time math.
    pub fn evaluate(
        &self,
        state: &S,
        entered_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Option<&WatchAction> {
        let elapsed = elapsed_nonneg(entered_at, now);
        for rule in &self.rules {
            if rule.state == *state && elapsed >= rule.threshold {
                return Some(&rule.action);
            }
        }
        None
    }
}

/// Compute `now - entered_at` clamped at zero. Defensive against
/// clock skew or replayed timestamps that would otherwise produce
/// a negative duration.
fn elapsed_nonneg(entered_at: DateTime<Utc>, now: DateTime<Utc>) -> Duration {
    let delta = now.signed_duration_since(entered_at);
    delta.to_std().unwrap_or(Duration::ZERO)
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Phase {
        Compiling,
        Planning,
        Applying,
        Ready,
    }

    fn routing_to(topic: &str) -> EscalationRouting {
        EscalationRouting {
            ntfy_topic: Some(topic.into()),
            ..Default::default()
        }
    }

    fn t(secs: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(secs, 0).unwrap()
    }

    #[test]
    fn empty_watcher_is_noop() {
        let w = TimeoutWatcher::<Phase>::new();
        assert!(w.is_empty());
        assert_eq!(w.evaluate(&Phase::Compiling, t(0), t(99_999)), None);
    }

    #[test]
    fn threshold_not_yet_elapsed_returns_none() {
        let w = TimeoutWatcher::<Phase>::new().with_rule(
            Phase::Compiling,
            Duration::from_secs(300),
            WatchAction::Alert(routing_to("compile-stuck")),
        );
        assert_eq!(w.evaluate(&Phase::Compiling, t(0), t(100)), None);
        assert_eq!(w.evaluate(&Phase::Compiling, t(0), t(299)), None);
    }

    #[test]
    fn threshold_exactly_at_returns_action() {
        let w = TimeoutWatcher::<Phase>::new().with_rule(
            Phase::Compiling,
            Duration::from_secs(300),
            WatchAction::Alert(routing_to("compile-stuck")),
        );
        match w.evaluate(&Phase::Compiling, t(0), t(300)) {
            Some(WatchAction::Alert(r)) => assert_eq!(r.ntfy_topic.as_deref(), Some("compile-stuck")),
            other => panic!("expected Alert, got {other:?}"),
        }
    }

    #[test]
    fn threshold_exceeded_returns_action() {
        let w = TimeoutWatcher::<Phase>::new().with_rule(
            Phase::Compiling,
            Duration::from_secs(300),
            WatchAction::Alert(routing_to("c")),
        );
        assert!(w.evaluate(&Phase::Compiling, t(0), t(500)).is_some());
    }

    #[test]
    fn different_state_does_not_match() {
        let w = TimeoutWatcher::<Phase>::new().with_rule(
            Phase::Compiling,
            Duration::from_secs(60),
            WatchAction::Alert(routing_to("c")),
        );
        assert_eq!(w.evaluate(&Phase::Planning, t(0), t(99_999)), None);
        assert_eq!(w.evaluate(&Phase::Ready, t(0), t(99_999)), None);
    }

    #[test]
    fn multiple_rules_first_match_wins() {
        // Two rules on the same state: the first one wins, mirroring
        // ChainedClassifier semantics.
        let w = TimeoutWatcher::<Phase>::new()
            .with_rule(
                Phase::Compiling,
                Duration::from_secs(100),
                WatchAction::Alert(routing_to("first")),
            )
            .with_rule(
                Phase::Compiling,
                Duration::from_secs(50),
                WatchAction::Page(routing_to("second")),
            );

        match w.evaluate(&Phase::Compiling, t(0), t(200)) {
            Some(WatchAction::Alert(r)) => assert_eq!(r.ntfy_topic.as_deref(), Some("first")),
            other => panic!("expected first-match Alert, got {other:?}"),
        }
    }

    #[test]
    fn per_state_thresholds() {
        // pangea-operator's canonical pattern: per-phase thresholds.
        let w = TimeoutWatcher::<Phase>::new()
            .with_rule(
                Phase::Compiling,
                Duration::from_secs(5 * 60),
                WatchAction::Alert(routing_to("compile-stuck")),
            )
            .with_rule(
                Phase::Planning,
                Duration::from_secs(10 * 60),
                WatchAction::Alert(routing_to("plan-stuck")),
            )
            .with_rule(
                Phase::Applying,
                Duration::from_secs(30 * 60),
                WatchAction::Page(routing_to("apply-stuck")),
            );

        // Compile threshold reached, plan and apply have not.
        match w.evaluate(&Phase::Compiling, t(0), t(6 * 60)) {
            Some(WatchAction::Alert(r)) => {
                assert_eq!(r.ntfy_topic.as_deref(), Some("compile-stuck"))
            }
            other => panic!("expected Alert, got {other:?}"),
        }
        assert_eq!(w.evaluate(&Phase::Planning, t(0), t(6 * 60)), None);
        assert_eq!(w.evaluate(&Phase::Applying, t(0), t(6 * 60)), None);
    }

    #[test]
    fn clock_skew_returns_none() {
        // entered_at AFTER now (clock skew) — elapsed clamped to 0,
        // no rule fires regardless of threshold.
        let w = TimeoutWatcher::<Phase>::new().with_rule(
            Phase::Compiling,
            Duration::from_secs(1),
            WatchAction::Alert(routing_to("c")),
        );
        assert_eq!(w.evaluate(&Phase::Compiling, t(1000), t(500)), None);
    }

    #[test]
    fn watch_action_serializes_round_trip() {
        let a = WatchAction::Alert(routing_to("topic-a"));
        let json = serde_json::to_string(&a).unwrap();
        let back: WatchAction = serde_json::from_str(&json).unwrap();
        assert_eq!(a, back);
    }

    #[test]
    fn escalation_routing_is_empty_when_all_none() {
        let r = EscalationRouting::default();
        assert!(r.is_empty());
        let r2 = routing_to("anything");
        assert!(!r2.is_empty());
    }

    #[test]
    fn determinism_law() {
        let w = TimeoutWatcher::<Phase>::new().with_rule(
            Phase::Compiling,
            Duration::from_secs(60),
            WatchAction::Alert(routing_to("c")),
        );
        // Same inputs → same output, every time.
        for elapsed_s in [0, 30, 59, 60, 61, 1000] {
            let a = w.evaluate(&Phase::Compiling, t(0), t(elapsed_s));
            let b = w.evaluate(&Phase::Compiling, t(0), t(elapsed_s));
            assert_eq!(a.is_some(), b.is_some(), "non-deterministic at elapsed={elapsed_s}");
        }
    }
}
