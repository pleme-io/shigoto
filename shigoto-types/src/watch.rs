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

// ── ScheduleWindow — typed schedule-with-window primitive ─────────
//
// Spec: theory/FLUXCD-CONVERGENCE.md §III, P1.0.
//
// Subsumes FluxCD's ResourceSetInputProvider `schedule[].{cron, timeZone, window}`
// pattern: an operation should fire at each cron tick, but only run if
// the current time is within `window` of the scheduled fire-time.
// Useful for cleanup jobs, backups, scaled-down maintenance windows
// where missing the window means waiting for next scheduled fire.
//
// Stateless — callers do cron expression parsing (e.g. via a cron
// crate of their choice) and pass in the resolved `scheduled_at`.
// The primitive carries the typed cron/timezone/window declaration
// and the `is_in_window` evaluation; cron-expression dispatch lives
// at the consumer.

/// A schedule-with-window declaration. Cron expression + IANA
/// timezone + how long the firing window stays open.
///
/// Cron expression is left as `String` so consumers can pick their
/// cron-parser crate; the primitive enforces only the typed shape
/// + window evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleWindow {
    /// Cron expression (e.g. `"0 2 * * *"` for daily at 02:00).
    /// Format depends on the consumer's cron parser; this primitive
    /// stores the raw string.
    pub cron: String,

    /// IANA timezone (e.g. `"America/New_York"`, `"UTC"`). Consumers
    /// resolve cron firings against this zone.
    pub timezone: String,

    /// How long after a scheduled fire-time the operation may run.
    /// If `now > scheduled_at + window`, the firing is missed —
    /// skip until next cron tick.
    pub window: Duration,
}

impl ScheduleWindow {
    /// Construct with sensible defaults: UTC timezone, 5-minute window.
    pub fn new(cron: impl Into<String>) -> Self {
        Self {
            cron: cron.into(),
            timezone: "UTC".into(),
            window: Duration::from_secs(5 * 60),
        }
    }

    /// Set timezone. Fluent builder method.
    #[must_use]
    pub fn with_timezone(mut self, tz: impl Into<String>) -> Self {
        self.timezone = tz.into();
        self
    }

    /// Set window. Fluent builder method.
    #[must_use]
    pub fn with_window(mut self, window: Duration) -> Self {
        self.window = window;
        self
    }

    /// Is `now` within `window` of the scheduled fire-time? Used by
    /// the scheduler to decide "should we fire this tick?".
    ///
    /// Returns `true` exactly when `scheduled_at <= now <= scheduled_at + window`.
    ///
    /// When `now < scheduled_at`, returns `false` (haven't reached the
    /// fire-time yet). When `now > scheduled_at + window`, returns
    /// `false` (window has closed; wait for next fire).
    pub fn is_in_window(&self, scheduled_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        if now < scheduled_at {
            return false;
        }
        let window_end = scheduled_at
            + chrono::Duration::from_std(self.window).unwrap_or_else(|_| chrono::Duration::zero());
        now <= window_end
    }

    /// `true` when the window has closed and the next fire-time is in
    /// the future. Useful for deciding "skip this scheduled occurrence
    /// and wait for the next."
    pub fn is_window_closed(&self, scheduled_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        if now < scheduled_at {
            return false;
        }
        let window_end = scheduled_at
            + chrono::Duration::from_std(self.window).unwrap_or_else(|_| chrono::Duration::zero());
        now > window_end
    }
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

    // ── ScheduleWindow tests ──────────────────────────────────────

    #[test]
    fn schedule_window_defaults_utc_and_5min() {
        let s = ScheduleWindow::new("0 2 * * *");
        assert_eq!(s.cron, "0 2 * * *");
        assert_eq!(s.timezone, "UTC");
        assert_eq!(s.window, Duration::from_secs(5 * 60));
    }

    #[test]
    fn schedule_window_builder_overrides() {
        let s = ScheduleWindow::new("* * * * *")
            .with_timezone("America/New_York")
            .with_window(Duration::from_secs(120));
        assert_eq!(s.timezone, "America/New_York");
        assert_eq!(s.window, Duration::from_secs(120));
    }

    #[test]
    fn schedule_window_now_before_scheduled_is_not_in_window() {
        let s = ScheduleWindow::new("0 2 * * *").with_window(Duration::from_secs(300));
        // scheduled at t=1000; now=999 (1s before)
        assert!(!s.is_in_window(t(1000), t(999)));
        assert!(!s.is_window_closed(t(1000), t(999)));
    }

    #[test]
    fn schedule_window_now_exactly_at_scheduled_is_in_window() {
        let s = ScheduleWindow::new("0 2 * * *").with_window(Duration::from_secs(300));
        assert!(s.is_in_window(t(1000), t(1000)));
        assert!(!s.is_window_closed(t(1000), t(1000)));
    }

    #[test]
    fn schedule_window_within_window_is_in_window() {
        let s = ScheduleWindow::new("0 2 * * *").with_window(Duration::from_secs(300));
        // scheduled at t=1000, window=300s → window closes at t=1300
        assert!(s.is_in_window(t(1000), t(1200)));
        assert!(!s.is_window_closed(t(1000), t(1200)));
    }

    #[test]
    fn schedule_window_at_window_boundary_is_in_window() {
        let s = ScheduleWindow::new("0 2 * * *").with_window(Duration::from_secs(300));
        // exactly at boundary (now == scheduled + window) is IN window
        assert!(s.is_in_window(t(1000), t(1300)));
        assert!(!s.is_window_closed(t(1000), t(1300)));
    }

    #[test]
    fn schedule_window_past_window_is_closed() {
        let s = ScheduleWindow::new("0 2 * * *").with_window(Duration::from_secs(300));
        // 1s past the window boundary
        assert!(!s.is_in_window(t(1000), t(1301)));
        assert!(s.is_window_closed(t(1000), t(1301)));
        // way past
        assert!(!s.is_in_window(t(1000), t(99_999)));
        assert!(s.is_window_closed(t(1000), t(99_999)));
    }

    #[test]
    fn schedule_window_in_window_and_closed_are_complementary_after_scheduled() {
        // After the scheduled fire-time, exactly one of is_in_window
        // and is_window_closed is true (they partition the post-scheduled timeline).
        let s = ScheduleWindow::new("* * * * *").with_window(Duration::from_secs(60));
        for offset in [0, 1, 30, 59, 60, 61, 100, 1000] {
            let scheduled = t(1000);
            let now = t(1000 + offset);
            let in_w = s.is_in_window(scheduled, now);
            let closed = s.is_window_closed(scheduled, now);
            assert!(
                in_w != closed,
                "exactly one of in_window/closed must be true at offset {offset}; got in={in_w}, closed={closed}"
            );
        }
    }

    #[test]
    fn schedule_window_serde_round_trip() {
        let s = ScheduleWindow::new("0 2 * * *")
            .with_timezone("America/New_York")
            .with_window(Duration::from_secs(600));
        let json = serde_json::to_string(&s).unwrap();
        let back: ScheduleWindow = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn schedule_window_zero_window_only_fires_at_exact_moment() {
        let s = ScheduleWindow::new("* * * * *").with_window(Duration::ZERO);
        assert!(s.is_in_window(t(1000), t(1000)), "zero window is exactly the scheduled instant");
        assert!(!s.is_in_window(t(1000), t(1001)), "one second past is already closed");
    }
}
