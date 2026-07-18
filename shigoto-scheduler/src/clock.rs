//! Injectable time source for [`crate::InProcessScheduler`].
//!
//! Every FSM decision that reads wall-clock time (the `Retrying` backoff
//! check in `advance_once`, the `RetryOutcome::Retry { until_ms }` computed
//! in `decide_retry`, the `TransitionEvent.at` / `TickReceipt.tick_at`
//! timestamps) went through a bare `chrono::Utc::now()` call — fine in
//! production, but it makes "does this job wait out its backoff window and
//! then retry" untestable without a real `tokio::time::sleep`. This mirrors
//! `outorga::Clock` (`SystemClock` / `FixedClock`) one layer up: the FSM
//! transition table itself (`shigoto_types::advance`) stays pure and takes
//! no clock at all; only the *scheduler* (the driver) reads time, and it
//! reads it through this trait so tests can pin it.

use std::sync::Mutex;

use chrono::{DateTime, Utc};

/// A source of the current time for [`crate::InProcessScheduler`].
/// Production code uses [`SystemClock`] (the default); tests that need to
/// assert backoff/retry timing without a real sleep use [`FixedClock`].
pub trait Clock: Send + Sync {
    /// The current time.
    fn now(&self) -> DateTime<Utc>;
}

/// Wall-clock time via `chrono::Utc::now()`. Default clock for
/// `InProcessScheduler::new`.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// A settable clock for deterministic tests. `Clock::now` takes `&self` (the
/// scheduler holds clocks behind `Arc<dyn Clock>`), so the stored instant
/// lives behind a `Mutex` and moves only when the test calls [`FixedClock::set`]
/// or [`FixedClock::advance`] — never on its own. This is what lets a test
/// prove "job waits out its backoff, then retries" by moving the clock
/// forward programmatically instead of sleeping for real.
#[derive(Debug)]
pub struct FixedClock(Mutex<DateTime<Utc>>);

impl FixedClock {
    /// A clock pinned at `at`.
    #[must_use]
    pub fn new(at: DateTime<Utc>) -> Self {
        Self(Mutex::new(at))
    }

    /// The current pinned instant.
    #[must_use]
    pub fn get(&self) -> DateTime<Utc> {
        *self.0.lock().expect("FixedClock mutex poisoned")
    }

    /// Jump straight to `at`.
    pub fn set(&self, at: DateTime<Utc>) {
        *self.0.lock().expect("FixedClock mutex poisoned") = at;
    }

    /// Move the clock forward by `dur`.
    pub fn advance(&self, dur: std::time::Duration) {
        let delta = chrono::Duration::from_std(dur).unwrap_or(chrono::Duration::zero());
        let mut guard = self.0.lock().expect("FixedClock mutex poisoned");
        *guard += delta;
    }
}

impl Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_returns_a_recent_time() {
        let before = Utc::now();
        let got = SystemClock.now();
        let after = Utc::now();
        assert!(got >= before && got <= after);
    }

    #[test]
    fn fixed_clock_never_moves_on_its_own() {
        let at = Utc::now();
        let clock = FixedClock::new(at);
        assert_eq!(clock.now(), at);
        // A second read, with no set/advance in between, is identical --
        // proves the clock does not silently tick with wall-clock time.
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert_eq!(clock.now(), at);
    }

    #[test]
    fn fixed_clock_advance_moves_forward_by_exactly_the_given_duration() {
        let at = Utc::now();
        let clock = FixedClock::new(at);
        clock.advance(std::time::Duration::from_secs(30));
        assert_eq!(clock.now(), at + chrono::Duration::seconds(30));
    }

    #[test]
    fn fixed_clock_set_jumps_to_an_arbitrary_instant() {
        let clock = FixedClock::new(Utc::now());
        let target = Utc::now() + chrono::Duration::days(1);
        clock.set(target);
        assert_eq!(clock.now(), target);
    }
}
