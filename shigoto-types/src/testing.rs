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

use crate::policy::CascadePolicy;

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

// ── CascadePolicy law harness ─────────────────────────────────────
//
// Turns the `CascadePolicy` trait's documented contract (idempotence,
// per-field merge, innermost-wins fold, determinism) into a single
// machine-checked theorem any impl can invoke from its own tests — so
// no adopter re-spells the law boilerplate that `magma-converge`'s
// ReactivePolicy (14 hand-written law tests) and `shigoto-types`' own
// `TestPolicy` currently duplicate. Per `theory/CONVERGENCE-ADOPTION.md`
// §VI Pillar 10 (proptest + tameshi proof discipline) + the "promises
// become theorems" thesis: the trait's promise is *proven* per impl,
// not asserted.

/// Assert that a [`CascadePolicy`](crate::policy::CascadePolicy) impl
/// satisfies the full trait contract over a set of sample layers.
///
/// Call this once in a consumer's test module to prove the concrete
/// policy obeys the laws — no per-impl idempotence/innermost-wins/
/// determinism boilerplate. Laws checked:
///
/// 1. **resolve-identity** — `resolve(&[], default) == default`.
/// 2. **idempotence** — `x.merge(L); x.merge(L)` equals one `x.merge(L)`.
/// 3. **merge-self-identity** — `L.merge(&L) == L` (a layer absorbs
///    itself: every `Some` field overwrites with its own value, every
///    `None` preserves).
/// 4. **determinism** — the same layer slice resolves identically every
///    time.
/// 5. **fold-order = layer-order** — `resolve` folds layers left→right
///    (innermost / rightmost wins per field), matching an independent
///    re-fold; catches a consumer that wrongly overrides `resolve`.
///
/// `default` is the hard-default policy; `layers` are representative
/// sample layers — ideally each setting a different field, plus one or
/// two overlapping on a shared field, so the per-field + innermost-wins
/// laws actually witness.
///
/// Failure panics via `assert_eq!`, naming the violated law + layer
/// index.
///
/// # Examples
///
/// ```
/// use shigoto_types::policy::CascadePolicy;
/// use shigoto_types::testing::assert_cascade_laws;
///
/// #[derive(Clone, Default, PartialEq, Debug)]
/// struct Layer { a: Option<u32>, b: Option<bool> }
/// impl CascadePolicy for Layer {
///     fn merge(&mut self, layer: &Self) {
///         if let Some(v) = layer.a { self.a = Some(v); }
///         if let Some(v) = layer.b { self.b = Some(v); }
///     }
/// }
///
/// let default = Layer { a: Some(0), b: Some(false) };
/// let samples = [
///     Layer { a: Some(1), ..Default::default() },
///     Layer { b: Some(true), ..Default::default() },
///     Layer { a: Some(9), b: Some(true) },
/// ];
/// assert_cascade_laws(default, &samples);
/// ```
pub fn assert_cascade_laws<P>(default: P, layers: &[P])
where
    P: CascadePolicy + PartialEq + std::fmt::Debug,
{
    // 1. resolve-identity: no layers leaves the default untouched.
    assert_eq!(
        P::resolve(&[], default.clone()),
        default,
        "CascadePolicy law (resolve-identity): resolve(&[], default) must equal default"
    );

    for (i, l) in layers.iter().enumerate() {
        // 2. idempotence: merging the same layer twice == once.
        let mut once = default.clone();
        once.merge(l);
        let mut twice = default.clone();
        twice.merge(l);
        twice.merge(l);
        assert_eq!(
            once, twice,
            "CascadePolicy law (idempotence): merge applied twice must equal once (layer {i})"
        );

        // 3. merge-self-identity: a layer merged onto itself is unchanged.
        let mut self_merged = l.clone();
        self_merged.merge(l);
        assert_eq!(
            &self_merged, l,
            "CascadePolicy law (merge-self-identity): L.merge(&L) must equal L (layer {i})"
        );
    }

    // 4. determinism: the same layer slice resolves identically.
    let refs: Vec<Option<&P>> = layers.iter().map(Some).collect();
    assert_eq!(
        P::resolve(&refs, default.clone()),
        P::resolve(&refs, default.clone()),
        "CascadePolicy law (determinism): resolve must be deterministic for the same layers"
    );

    // 5. fold-order = layer-order: resolve folds left→right, so it equals
    //    merging each layer in order onto the default.
    let mut folded = default.clone();
    for l in layers {
        folded.merge(l);
    }
    assert_eq!(
        P::resolve(&refs, default.clone()),
        folded,
        "CascadePolicy law (fold-order): resolve must fold layers in declared order (innermost/rightmost wins)"
    );
}

/// [`assert_cascade_laws`] plus the **empty-layer** law (needs
/// `P: Default`): an all-`None` layer — the `Default` — is a no-op over
/// any accumulator, so `resolve(&[Some(&P::default())], default)` equals
/// `default`. This is the strongest harness; prefer it whenever the
/// policy's `Default` is the canonical all-`None` value (the usual case
/// for `#[derive(Default)]` over `Option<_>` fields).
///
/// # Examples
///
/// ```
/// use shigoto_types::policy::CascadePolicy;
/// use shigoto_types::testing::assert_cascade_laws_with_default;
///
/// #[derive(Clone, Default, PartialEq, Debug)]
/// struct Layer { a: Option<u32> }
/// impl CascadePolicy for Layer {
///     fn merge(&mut self, layer: &Self) {
///         if let Some(v) = layer.a { self.a = Some(v); }
///     }
/// }
///
/// assert_cascade_laws_with_default(
///     Layer { a: Some(0) },
///     &[Layer { a: Some(7) }],
/// );
/// ```
pub fn assert_cascade_laws_with_default<P>(default: P, layers: &[P])
where
    P: CascadePolicy + PartialEq + std::fmt::Debug + Default,
{
    assert_cascade_laws(default.clone(), layers);
    let empty = P::default();
    assert_eq!(
        P::resolve(&[Some(&empty)], default.clone()),
        default,
        "CascadePolicy law (empty-layer): an all-None (Default) layer must preserve the accumulator"
    );
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
        assert_deterministic_over(&[0_u32, 1, 2, 100, u32::MAX], |&x| x.wrapping_mul(7));
    }

    #[test]
    fn over_empty_slice_is_vacuous_truth() {
        let empty: &[u32] = &[];
        assert_deterministic_over(empty, |&x| x);
    }

    // ── CascadePolicy harness self-tests ──────────────────────────

    #[derive(Clone, Default, PartialEq, Debug)]
    struct GoodPolicy {
        a: Option<u32>,
        b: Option<String>,
    }
    impl CascadePolicy for GoodPolicy {
        fn merge(&mut self, layer: &Self) {
            if let Some(v) = &layer.a {
                self.a = Some(*v);
            }
            if let Some(v) = &layer.b {
                self.b = Some(v.clone());
            }
        }
    }

    #[test]
    fn cascade_harness_passes_a_correct_impl() {
        assert_cascade_laws_with_default(
            GoodPolicy {
                a: Some(0),
                b: Some("base".into()),
            },
            &[
                GoodPolicy {
                    a: Some(1),
                    ..Default::default()
                },
                GoodPolicy {
                    b: Some("x".into()),
                    ..Default::default()
                },
                GoodPolicy {
                    a: Some(9),
                    b: Some("y".into()),
                },
            ],
        );
    }

    // A deliberately BROKEN impl: `merge` ACCUMULATES into a Vec instead
    // of overwriting, so it is non-idempotent (merging twice doubles the
    // pushed values). The harness MUST catch this — a law harness that
    // can't fail on a broken impl proves nothing.
    #[derive(Clone, Default, PartialEq, Debug)]
    struct NonIdempotentPolicy {
        acc: Vec<u32>,
        a: Option<u32>,
    }
    impl CascadePolicy for NonIdempotentPolicy {
        fn merge(&mut self, layer: &Self) {
            if let Some(v) = layer.a {
                self.acc.push(v); // BUG: append, not overwrite.
                self.a = Some(v);
            }
        }
    }

    #[test]
    #[should_panic(expected = "idempotence")]
    fn cascade_harness_catches_a_non_idempotent_impl() {
        assert_cascade_laws(
            NonIdempotentPolicy::default(),
            &[NonIdempotentPolicy {
                a: Some(5),
                ..Default::default()
            }],
        );
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
