//! Typed input classification — the canonical `Classifier<I, O>`
//! trait every fleet-wide raw-input-to-typed-event pipeline consumes.
//! Spec: `theory/CONVERGENCE-ADOPTION.md` §II.5, Phase 0.2.
//!
//! Subsumes the hand-rolled classification shapes that previously
//! lived in:
//!
//! - `shigoto_types::failure::classify(raw: &str) -> FailureKind`
//!   (now wrapped via `FailureClassifier`, becomes the canonical
//!   first Classifier impl)
//! - `tend::drift::classify_pull_failure(workspace, repo, stderr)
//!   -> DriftEvent` (shipped 2026-05-28 M1; future migration to
//!   `Classifier<PullFailureContext, DriftEvent>` lands in 0.2b)
//!
//! # Why a trait
//!
//! Classification is a deterministic pure-function pattern that
//! recurs across the fleet (anywhere an opaque input — stderr string,
//! HTTP status, JSON shape — becomes a typed variant). The trait
//! gives consumers:
//!
//! - **Composability** — chain rules + fallback via `ChainedClassifier`
//! - **Testability** — proptest the trait law (determinism for same
//!   inputs) once, every impl inherits
//! - **Polymorphism** — pass `&dyn Classifier<I, O>` to a higher-level
//!   pipeline without locking in the implementation
//! - **Override** — per-workspace operator can swap in a custom
//!   `Classifier<&str, FailureKind>` that classifies their site's
//!   stderr shapes correctly
//!
//! # The trait law
//!
//! For any classifier `c` and any input `i`:
//!
//!   `c.classify(i) == c.classify(i)`   (determinism)
//!
//! No I/O, no randomness, no hidden state. Implementations that need
//! side effects classify a Context wrapper that records them
//! externally — the trait is for pure shape extraction only.
//!
//! # `ChainedClassifier<I, O>` composition
//!
//! Each rule is a closure `&I -> Option<O>` — `Some(out)` matches,
//! `None` falls through. The fallback closure `&I -> O` produces a
//! typed default when no rule matches. Order matters: rules fire in
//! `with_rule` order; the first match wins.

use std::marker::PhantomData;
use std::sync::Arc;

/// The canonical typed-input classifier. Implementations map an
/// opaque input `&I` to a typed output `O` deterministically.
pub trait Classifier<I: ?Sized, O>: Send + Sync {
    /// Classify one input. Pure — no side effects, deterministic for
    /// the same input.
    fn classify(&self, input: &I) -> O;
}

/// Adapter that wraps a free function `fn(&I) -> O` as a
/// `Classifier<I, O>`. Useful when a typed primitive already exists
/// as a free function (e.g. `shigoto_types::failure::classify`) and
/// you want to use it polymorphically without rewriting.
pub struct FnClassifier<I: ?Sized, O, F>
where
    F: Fn(&I) -> O + Send + Sync,
{
    f: F,
    _phantom: PhantomData<fn(&I) -> O>,
}

impl<I: ?Sized, O, F> FnClassifier<I, O, F>
where
    F: Fn(&I) -> O + Send + Sync,
{
    pub fn new(f: F) -> Self {
        Self {
            f,
            _phantom: PhantomData,
        }
    }
}

impl<I: ?Sized, O, F> Classifier<I, O> for FnClassifier<I, O, F>
where
    F: Fn(&I) -> O + Send + Sync,
{
    fn classify(&self, input: &I) -> O {
        (self.f)(input)
    }
}

/// Composes a chain of rules + a fallback into a single typed
/// classifier. Each rule is tried in declared order; the first
/// `Some(out)` wins. If no rule matches, the fallback fires.
///
/// Construction is fluent:
///
/// ```ignore
/// let c = ChainedClassifier::<str, FailureKind>::new(|_| FailureKind::Transient)
///     .with_rule(|s| s.contains("does not exist").then_some(FailureKind::Declarative))
///     .with_rule(|s| s.contains("infinite recursion").then_some(FailureKind::Declarative));
/// ```
pub struct ChainedClassifier<I: ?Sized, O> {
    rules: Vec<Arc<dyn Fn(&I) -> Option<O> + Send + Sync>>,
    fallback: Arc<dyn Fn(&I) -> O + Send + Sync>,
}

impl<I: ?Sized, O> ChainedClassifier<I, O> {
    /// Construct an empty chain with the given fallback. Adding rules
    /// via `with_rule` extends the chain.
    pub fn new<F>(fallback: F) -> Self
    where
        F: Fn(&I) -> O + Send + Sync + 'static,
    {
        Self {
            rules: Vec::new(),
            fallback: Arc::new(fallback),
        }
    }

    /// Append a rule. Returns self for fluent chaining.
    #[must_use]
    pub fn with_rule<R>(mut self, rule: R) -> Self
    where
        R: Fn(&I) -> Option<O> + Send + Sync + 'static,
    {
        self.rules.push(Arc::new(rule));
        self
    }

    /// Number of rules in the chain (excludes the fallback).
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }
}

impl<I: ?Sized, O> Classifier<I, O> for ChainedClassifier<I, O> {
    fn classify(&self, input: &I) -> O {
        for rule in &self.rules {
            if let Some(o) = rule(input) {
                return o;
            }
        }
        (self.fallback)(input)
    }
}

impl<I: ?Sized, O> std::fmt::Debug for ChainedClassifier<I, O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainedClassifier")
            .field("rules", &self.rules.len())
            .finish()
    }
}

// ── First canonical consumer: failure::classify ───────────────────
//
// Wraps the existing free-function `failure::classify` as a
// `Classifier<str, FailureKind>`. Demonstrates the migration shape
// every other consumer follows.

/// `Classifier<str, FailureKind>` impl wrapping
/// [`crate::failure::classify`]. The canonical first consumer of
/// the `Classifier` trait; future fleet classifiers follow this
/// shape (small newtype struct + impl Classifier).
#[derive(Debug, Default, Copy, Clone)]
pub struct FailureClassifier;

impl Classifier<str, crate::failure::FailureKind> for FailureClassifier {
    fn classify(&self, input: &str) -> crate::failure::FailureKind {
        crate::failure::classify(input)
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::failure::FailureKind;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Color {
        Red,
        Green,
        Blue,
        Unknown,
    }

    #[test]
    fn fn_classifier_wraps_free_function() {
        let c = FnClassifier::new(|s: &str| {
            if s == "red" { Color::Red } else { Color::Unknown }
        });
        assert_eq!(c.classify("red"), Color::Red);
        assert_eq!(c.classify("orange"), Color::Unknown);
    }

    #[test]
    fn chained_classifier_first_match_wins() {
        let c = ChainedClassifier::<str, Color>::new(|_| Color::Unknown)
            .with_rule(|s| s.contains("red").then_some(Color::Red))
            .with_rule(|s| s.contains("green").then_some(Color::Green))
            .with_rule(|s| s.contains("blue").then_some(Color::Blue));

        assert_eq!(c.classify("the red car"), Color::Red);
        assert_eq!(c.classify("green tree"), Color::Green);
        assert_eq!(c.classify("blue sky"), Color::Blue);
    }

    #[test]
    fn chained_classifier_fallback_fires_on_no_match() {
        let c = ChainedClassifier::<str, Color>::new(|_| Color::Unknown)
            .with_rule(|s| s.contains("red").then_some(Color::Red));

        assert_eq!(c.classify("totally unrelated"), Color::Unknown);
        assert_eq!(c.classify(""), Color::Unknown);
    }

    #[test]
    fn chained_classifier_rule_order_matters() {
        // First match wins — the same input matching multiple rules
        // returns the first rule's output, never the later one.
        let c = ChainedClassifier::<str, Color>::new(|_| Color::Unknown)
            .with_rule(|s| s.contains("multi").then_some(Color::Red))
            .with_rule(|s| s.contains("multi").then_some(Color::Green));

        assert_eq!(c.classify("multi"), Color::Red, "first rule wins");
    }

    #[test]
    fn chained_classifier_empty_chain_returns_fallback() {
        let c = ChainedClassifier::<str, Color>::new(|_| Color::Blue);
        assert_eq!(c.rule_count(), 0);
        assert_eq!(c.classify("anything"), Color::Blue);
    }

    #[test]
    fn classifier_dyn_polymorphism_works() {
        let c1: Box<dyn Classifier<str, Color>> = Box::new(
            ChainedClassifier::new(|_| Color::Unknown)
                .with_rule(|s: &str| s.contains("r").then_some(Color::Red)),
        );
        let c2: Box<dyn Classifier<str, Color>> =
            Box::new(FnClassifier::new(|_| Color::Green));

        assert_eq!(c1.classify("red"), Color::Red);
        assert_eq!(c2.classify("anything"), Color::Green);
    }

    #[test]
    fn classifier_law_determinism() {
        // The trait law: same input → same output, every time.
        let c = ChainedClassifier::<str, Color>::new(|_| Color::Unknown)
            .with_rule(|s| s.contains("r").then_some(Color::Red))
            .with_rule(|s| s.contains("g").then_some(Color::Green));

        for input in ["red", "green", "ranger", "neither", ""] {
            let a = c.classify(input);
            let b = c.classify(input);
            assert_eq!(a, b, "classifier must be deterministic for input {input:?}");
        }
    }

    #[test]
    fn failure_classifier_wraps_free_function() {
        // The canonical first consumer: shigoto_types::failure::classify
        // available polymorphically as a Classifier impl.
        let c = FailureClassifier;
        assert_eq!(
            c.classify("does not exist"),
            FailureKind::Declarative,
            "declarative pattern routes correctly"
        );
        assert_eq!(
            c.classify("network timeout"),
            FailureKind::Transient,
            "non-declarative stderr defaults to Transient"
        );
    }

    #[test]
    fn failure_classifier_via_dyn() {
        // The polymorphic surface: pass FailureClassifier as a
        // &dyn Classifier<str, FailureKind> to higher-level pipelines.
        let c: &dyn Classifier<str, FailureKind> = &FailureClassifier;
        assert_eq!(c.classify("infinite recursion encountered"), FailureKind::Declarative);
        assert_eq!(c.classify("connection refused"), FailureKind::Transient);
    }
}
