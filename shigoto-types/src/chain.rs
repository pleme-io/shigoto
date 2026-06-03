//! `Chain<I, O>` — first-match-wins composition over typed rules.
//!
//! Spec: `theory/PATTERN-EXTRACTION.md` Pattern 3 (Chain).
//!
//! Extracted after multiple primitives shipped the same shape:
//!
//!   - `ChainedClassifier<I, O>` — typed-classifier chain
//!   - `ChainedHealthCheck<R>` — typed-health-check chain (partial fit)
//!   - `TimeoutWatcher::rules` — first state+threshold match (partial fit)
//!
//! Each repeated: `Vec<Fn(&I) -> Option<O>>` rules + `Fn(&I) -> O`
//! fallback + first-match-wins evaluation.
//!
//! Chain factors out that shape; consumers reach for it whenever they
//! want a composable typed rule chain with a fallback.
//!
//! # The Chain law
//!
//! For any `Chain<I, O>` and any input `i`:
//!
//!   - **Determinism:** `c.evaluate(&i) == c.evaluate(&i)` (pure
//!     dispatch — rules and fallback are `Fn`)
//!   - **First-match-wins:** If multiple rules return `Some` for `i`,
//!     the rule added FIRST wins
//!   - **Fallback fires last:** If no rule returns `Some`, the
//!     fallback runs and supplies a value
//!   - **Total:** `evaluate` always returns a value (fallback is
//!     mandatory at construction)
//!
//! # Fluent construction
//!
//! ```
//! use shigoto_types::chain::Chain;
//!
//! enum Color { Red, Green, Other }
//!
//! let c = Chain::<str, Color>::new(|_| Color::Other)
//!     .with_rule(|s| s.contains("red").then_some(Color::Red))
//!     .with_rule(|s| s.contains("green").then_some(Color::Green));
//!
//! assert!(matches!(c.evaluate("rare red wine"), Color::Red));
//! assert!(matches!(c.evaluate("anything else"),  Color::Other));
//! ```

#![allow(missing_docs)]

use std::sync::Arc;

/// Typed first-match-wins rule chain with a mandatory fallback.
pub struct Chain<I: ?Sized, O> {
    rules: Vec<Arc<dyn Fn(&I) -> Option<O> + Send + Sync>>,
    fallback: Arc<dyn Fn(&I) -> O + Send + Sync>,
}

impl<I: ?Sized, O> Chain<I, O> {
    /// Construct an empty chain with the given fallback. Adding rules
    /// via `with_rule` extends the chain. The fallback is mandatory
    /// at construction — `Chain::evaluate` is total.
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

    /// Evaluate the chain on an input. Returns the first rule's
    /// `Some(...)` output, else the fallback's value. Total: always
    /// returns a value.
    pub fn evaluate(&self, input: &I) -> O {
        for rule in &self.rules {
            if let Some(o) = rule(input) {
                return o;
            }
        }
        (self.fallback)(input)
    }
}

impl<I: ?Sized, O> std::fmt::Debug for Chain<I, O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Chain")
            .field("rules", &self.rules.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::assert_deterministic_over;

    #[derive(Debug, PartialEq, Eq, Clone)]
    enum Color {
        Red,
        Green,
        Blue,
        Unknown,
    }

    #[test]
    fn empty_chain_returns_fallback() {
        let c = Chain::<str, Color>::new(|_| Color::Unknown);
        assert_eq!(c.evaluate("anything"), Color::Unknown);
        assert_eq!(c.rule_count(), 0);
    }

    #[test]
    fn first_matching_rule_wins() {
        let c = Chain::<str, Color>::new(|_| Color::Unknown)
            .with_rule(|s| s.contains("red").then_some(Color::Red))
            .with_rule(|s| s.contains("green").then_some(Color::Green))
            .with_rule(|s| s.contains("blue").then_some(Color::Blue));

        assert_eq!(c.evaluate("red and green"), Color::Red);
        assert_eq!(c.evaluate("blue sky"), Color::Blue);
        assert_eq!(c.evaluate("nothing"), Color::Unknown);
        assert_eq!(c.rule_count(), 3);
    }

    #[test]
    fn fallback_fires_on_no_match() {
        let c = Chain::<str, Color>::new(|_| Color::Unknown)
            .with_rule(|s| s.contains("red").then_some(Color::Red));
        assert_eq!(c.evaluate("none of that"), Color::Unknown);
    }

    #[test]
    fn determinism_law() {
        let c = Chain::<str, Color>::new(|_| Color::Unknown)
            .with_rule(|s| s.contains("r").then_some(Color::Red))
            .with_rule(|s| s.contains("g").then_some(Color::Green));

        assert_deterministic_over(&["red", "green", "ranger", "neither", ""], |&input| {
            c.evaluate(input)
        });
    }

    #[test]
    fn debug_impl_shows_rule_count() {
        let c = Chain::<str, Color>::new(|_| Color::Unknown)
            .with_rule(|s| s.contains("r").then_some(Color::Red));
        let debug_str = format!("{c:?}");
        assert!(debug_str.contains("rules"));
        assert!(debug_str.contains('1'));
    }

    #[test]
    fn composes_with_classifier_trait() {
        // Proof: `Chain<I, O>` IS a `Classifier<I, O>` via the
        // blanket impl in `classify.rs`. Same shape, same semantics —
        // the new ChainedClassifier typedef proves this is a
        // backwards-compat substitution.
        use crate::Classifier;
        let c = Chain::<str, Color>::new(|_| Color::Unknown)
            .with_rule(|s: &str| s.contains("red").then_some(Color::Red));
        assert_eq!(c.classify("red"), Color::Red);
        assert_eq!(c.classify("none"), Color::Unknown);
    }
}
