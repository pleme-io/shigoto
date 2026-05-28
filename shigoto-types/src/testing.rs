//! Test-helpers for typed-primitive authors.
//!
//! Codifies the **Determinism law** pattern named in
//! `theory/PATTERN-EXTRACTION.md` Pattern 10 — every pure primitive
//! in the substrate ships a test asserting that the same input
//! produces the same output across repeated invocations.
//!
//! Authoring shape:
//!
//! ```rust,no_run
//! use shigoto_types::testing::assert_deterministic;
//!
//! fn add(a: u32, b: u32) -> u32 { a + b }
//!
//! #[test]
//! fn add_is_deterministic() {
//!     assert_deterministic(|| add(2, 3));
//! }
//! ```
//!
//! ## Why centralize this
//!
//! 11+ primitives in `magma-converge` + `shigoto-types` carry a
//! determinism-named test today (`Classifier::classifier_law_determinism`,
//! `TimeoutWatcher::determinism_law`, `Decision::determinism_law`,
//! `LabelSelector::matches_is_deterministic`,
//! `CascadePolicy::determinism_law`,
//! `OutcomeLattice::worst_determinism_across_impls`,
//! `RefSpec::display_round_trips_through_parse`, ...). Each spells
//! out the same `let a = f(x); let b = f(x); assert_eq!(a, b);` shape.
//! This helper collapses that into one line.
//!
//! ## What it doesn't cover
//!
//! The helper is for **pure functions** — functions whose only
//! input is their argument and whose only output is their return.
//! Side-effecting work (filesystem, network, time, randomness)
//! requires the Environment-trait pattern from
//! `pleme-io/CLAUDE.md` ★★ TYPED-SPEC + INTERPRETER TRIPLET §3.
//! Don't reach for `assert_deterministic` to test those — mock
//! the side effects and test the pure decision logic instead.

#![allow(missing_docs)]

/// Assert that the closure produces an identical result when invoked
/// **once vs twice vs three times** with no intervening state change.
///
/// Used in tests that codify the determinism law for a pure function.
/// The closure must:
///
///   - Have no captured mutable state
///   - Make no I/O calls (filesystem, network, time)
///   - Return a `T: PartialEq + std::fmt::Debug`
///
/// Failure is panic via `assert_eq!`, surfacing the first divergence
/// with both values printed.
///
/// # Examples
///
/// ```
/// use shigoto_types::testing::assert_deterministic;
///
/// assert_deterministic(|| 1 + 1);
/// assert_deterministic(|| String::from("hello"));
/// ```
///
/// # When to reach for the proptest variant instead
///
/// For functions over multiple typed inputs where you want the
/// determinism law to hold over a **distribution** of inputs (not
/// just one fixed input), use `assert_deterministic_over` and
/// drive it from a proptest strategy.
pub fn assert_deterministic<F, T>(f: F)
where
    F: Fn() -> T,
    T: PartialEq + std::fmt::Debug,
{
    let a = f();
    let b = f();
    let c = f();
    assert_eq!(a, b, "non-deterministic between invocation 1 and 2");
    assert_eq!(b, c, "non-deterministic between invocation 2 and 3");
}

/// Variant for closures taking one typed input — exercises
/// determinism *given a fixed input value*.
///
/// Use when the function signature carries one input you want to
/// pin while asserting the law. For multi-arg functions, capture
/// extra args in the closure.
///
/// # Examples
///
/// ```
/// use shigoto_types::testing::assert_deterministic_with;
///
/// assert_deterministic_with(42_u32, |x| x.wrapping_mul(7));
/// ```
pub fn assert_deterministic_with<I, F, T>(input: I, f: F)
where
    I: Clone,
    F: Fn(I) -> T,
    T: PartialEq + std::fmt::Debug,
{
    let a = f(input.clone());
    let b = f(input.clone());
    let c = f(input);
    assert_eq!(a, b, "non-deterministic between invocation 1 and 2");
    assert_eq!(b, c, "non-deterministic between invocation 2 and 3");
}

/// Variant exercising determinism **across a slice of fixed
/// inputs** — useful for table-driven tests where every input in
/// a list should individually witness the law.
///
/// # Examples
///
/// ```
/// use shigoto_types::testing::assert_deterministic_over;
///
/// assert_deterministic_over(&[0_u32, 1, 2, 100], |&x| x.wrapping_mul(7));
/// ```
pub fn assert_deterministic_over<I, F, T>(inputs: &[I], f: F)
where
    I: Clone,
    F: Fn(&I) -> T,
    T: PartialEq + std::fmt::Debug,
{
    for input in inputs {
        let a = f(input);
        let b = f(input);
        let c = f(input);
        assert_eq!(a, b, "non-deterministic for one input, 1 vs 2");
        assert_eq!(b, c, "non-deterministic for one input, 2 vs 3");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pure_closure_passes() {
        assert_deterministic(|| 1 + 1);
    }

    #[test]
    fn string_returning_closure_passes() {
        assert_deterministic(|| String::from("hello"));
    }

    #[test]
    fn with_input_passes() {
        assert_deterministic_with(42_u32, |x| x.wrapping_mul(7));
    }

    #[test]
    fn over_inputs_passes() {
        assert_deterministic_over(&[0_u32, 1, 2, 100, u32::MAX], |&x| {
            x.wrapping_mul(7)
        });
    }

    #[test]
    fn over_empty_slice_is_vacuous_truth() {
        let empty: &[u32] = &[];
        assert_deterministic_over(empty, |&x| x);
    }

    #[test]
    #[should_panic(expected = "non-deterministic")]
    fn non_deterministic_panics() {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNT: AtomicU32 = AtomicU32::new(0);
        // Reset for this test to be reproducible across runs.
        COUNT.store(0, Ordering::SeqCst);
        assert_deterministic(|| COUNT.fetch_add(1, Ordering::SeqCst));
    }
}
