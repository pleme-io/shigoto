//! Typed policy cascade — the canonical `CascadePolicy` trait every
//! fleet-wide multi-layer-override system consumes.
//! Spec: `theory/CONVERGENCE-ADOPTION.md` §II.2.
//!
//! This is a **lightweight, general** convergence primitive — pure value
//! transformation, no IaC / domain / clock coupling. It is homed in
//! `shigoto-types` (not the heavy `magma-converge` IaC crate) alongside
//! its sibling primitives [`Sink`](crate::sink::Sink),
//! [`Classifier`](crate::classify::Classifier), and
//! [`TimeoutWatcher`](crate::watch::TimeoutWatcher), so that lightweight
//! controllers (pangea-operator's reactive policy, lava-operator's
//! `resolve_policy`, …) can adopt it without pulling the magma executor
//! closure. (Re-homed 2026-06-02 — was `magma-converge::policy`.)
//!
//! Subsumes the hand-rolled "innermost-wins per-field merge" shapes in
//! `pangea-operator::reactive::EffectiveReactivePolicy::resolve`,
//! `lava-operator::policy::resolve_policy`, and (future)
//! `tend::reaction::ReactionPolicy::resolve`.
//!
//! # The cascade rule
//!
//! Layers iterate in declared order; each non-`None` field in a layer
//! overrides the accumulator. Hard defaults fill any field no layer set.
//! The result is the merged value with **innermost-wins per field**.
//! Given layers `[A, B, C]` and a hard default `D`:
//!
//!   start: result = D
//!   for layer in [A, B, C]: for each field F: if layer.F is Some(v), result.F = v
//!   return result
//!
//! C's fields override B's override A's override D's. Composition is
//! associative + idempotent: `resolve([A, A]) == resolve([A])`.
//!
//! A derive macro (`#[derive(CascadePolicy)]`) auto-generating `merge`
//! from `Option<F>` fields is planned (0.4b); for now consumers impl
//! `merge` manually (one `if let Some(v) = &layer.field { … }` per field).

/// Innermost-wins per-field policy merge. Implementors define `merge`;
/// the blanket `resolve` walks a layer slice + hard default.
pub trait CascadePolicy: Sized + Clone {
    /// Merge `layer` over `self`, field by field. Fields present in
    /// `layer` (`Some(v)`) override `self`'s value; fields absent
    /// (`None`) preserve `self`.
    ///
    /// Implementors MUST satisfy:
    ///
    /// 1. **Idempotent.** `self.merge(layer); self.merge(layer)` yields
    ///    the same state as one `self.merge(layer)`.
    /// 2. **Per-field.** Each `Option` field in `layer` controls only
    ///    that field on `self`. Don't read other fields.
    /// 3. **No I/O.** Pure value transformation; no clock reads, no
    ///    randomness.
    fn merge(&mut self, layer: &Self);

    /// Cascade through layers in order (rightmost / innermost wins per
    /// field), starting from `default`. `None` slots are skipped —
    /// useful when a layer is conditionally present.
    fn resolve(layers: &[Option<&Self>], default: Self) -> Self {
        let mut result = default;
        for layer in layers.iter().flatten() {
            result.merge(layer);
        }
        result
    }
}

// ── Trait-law tests (generic minimal impl) ────────────────────────
// The canonical first impl + law suite live here with the trait. Domain
// reference impls (pangea-shaped ReactivePolicy, etc.) live in their own
// crates and mirror this shape.

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    struct TestPolicy {
        a: Option<u32>,
        b: Option<String>,
    }

    impl CascadePolicy for TestPolicy {
        fn merge(&mut self, layer: &Self) {
            if let Some(v) = &layer.a {
                self.a = Some(*v);
            }
            if let Some(v) = &layer.b {
                self.b = Some(v.clone());
            }
        }
    }

    fn d() -> TestPolicy {
        TestPolicy {
            a: Some(1),
            b: Some("base".into()),
        }
    }

    #[test]
    fn resolve_no_layers_returns_default() {
        assert_eq!(TestPolicy::resolve(&[], d()), d());
    }

    #[test]
    fn empty_layer_preserves_default() {
        let empty = TestPolicy::default();
        assert_eq!(TestPolicy::resolve(&[Some(&empty)], d()), d());
    }

    #[test]
    fn single_layer_overrides_only_its_set_field() {
        let layer = TestPolicy { a: Some(99), ..Default::default() };
        let r = TestPolicy::resolve(&[Some(&layer)], d());
        assert_eq!(r.a, Some(99));
        assert_eq!(r.b, d().b, "unset field preserved");
    }

    #[test]
    fn innermost_wins_per_field_and_for_same_field() {
        let outer = TestPolicy { a: Some(10), ..Default::default() };
        let inner = TestPolicy {
            a: Some(50),
            b: Some("inner".into()),
        };
        let r = TestPolicy::resolve(&[Some(&outer), Some(&inner)], TestPolicy::default());
        assert_eq!(r.a, Some(50), "innermost layer wins the shared field");
        assert_eq!(r.b, Some("inner".into()));
    }

    #[test]
    fn none_layer_is_skipped() {
        let inner = TestPolicy { a: Some(7), ..Default::default() };
        let r = TestPolicy::resolve(&[None, Some(&inner), None], TestPolicy::default());
        assert_eq!(r.a, Some(7));
    }

    #[test]
    fn merge_is_idempotent() {
        let layer = TestPolicy { a: Some(99), ..Default::default() };
        let mut once = d();
        let mut twice = d();
        once.merge(&layer);
        twice.merge(&layer);
        twice.merge(&layer);
        assert_eq!(once, twice, "re-applying the same layer is a no-op");
    }

    #[test]
    fn resolve_is_deterministic() {
        let l = TestPolicy { a: Some(3), ..Default::default() };
        let refs = [Some(&l)];
        assert_eq!(
            TestPolicy::resolve(&refs, d()),
            TestPolicy::resolve(&refs, d()),
        );
    }
}
