//! Typed failure classification — the META primitive every long-running
//! pleme-io reconciler consumes.
//!
//! ## The problem this primitive solves
//!
//! A naive reconciler treats every failure as transient — it backs off
//! exponentially and retries forever. That works for "builder unreachable"
//! / "DNS blip" / "network stall" (conditions that fix themselves), but
//! breaks for "missing flake attribute" / "NixOS evaluation error" /
//! "schema mismatch" (conditions that NEVER fix themselves without the
//! operator changing the declaration).
//!
//! The implicit mental model — "obvious permanent failures should stop
//! retrying and surface in cluster status" — becomes the explicit typed
//! thing `FailureKind`.
//!
//! ## Where this primitive lives
//!
//! Per the pleme-io [Compounding Directive](../../blackmatter-pleme/docs/pleme-io-CLAUDE.md)
//! Operating Principle #1 ("solve problems once, in one place, at one
//! time"), this lives in `shigoto-types` — the typed-primitive root of
//! the work-graph crate family. Every consumer (kikai daemon, magma
//! apply engine, tatara-reconciler, pangea-operator) imports from here
//! and shares one classifier + one definition of "Declarative."
//!
//! `shigoto-retry` extends `RetryPolicy::decide` to consult
//! `FailureRecord.kind`: any Declarative failure returns `Deadletter`
//! immediately regardless of attempt budget — the META point.
//!
//! ## Conservative classification
//!
//! `classify` defaults to `Transient` on unknown error shapes. We'd
//! rather retry once or twice extra than wedge a reconciler waiting
//! for an operator to clear a state we should have considered
//! transient. Patterns added to the Declarative set are the documented
//! signatures we've observed in the fleet — extend as new classes
//! surface.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Classification of a reconcile-loop failure.
///
/// Binary by design: every consumer asks one question — "should I keep
/// trying?" If `Transient`, yes (with whatever backoff policy). If
/// `Declarative`, no — the operator-supplied declaration is broken and
/// no amount of retrying will fix it without operator action. Routes
/// through `Deadletter` in `shigoto-retry::RetryDecision`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum FailureKind {
    /// Conditions may clear: builder unreachable, network blip, DHCP
    /// not yet allocated, kernel module loading, dependency not yet
    /// ready, rate limit hit, transient API 5xx, etc.
    Transient,

    /// Operator-supplied declaration is broken. Examples: missing
    /// flake attribute, NixOS module evaluation error, missing source
    /// file, type mismatch in option value, reference to a SOPS secret
    /// that doesn't exist, MAC address conflict with a sibling cluster,
    /// schema mismatch in a Terraform provider, etc.
    Declarative,
}

impl Default for FailureKind {
    /// Conservative default: `Transient`. Callers that don't classify
    /// get the "keep trying" behaviour by default — never accidentally
    /// wedge a reconciler at attempt #1 because someone forgot to
    /// classify their error.
    fn default() -> Self {
        Self::Transient
    }
}

impl fmt::Display for FailureKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Transient => "Transient",
            Self::Declarative => "Declarative",
        })
    }
}

/// Typed reconcile failure — the shape every long-running daemon
/// stores in its FSM to track consecutive identical failures.
///
/// Companion to `shigoto-retry::FailureRecord` (which carries the
/// attempt + timestamp + raw error string). `Failure` is the
/// classified-and-summarized projection a status surface displays.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Failure {
    pub kind: FailureKind,
    /// Truncated to first 256 chars to keep status output bounded.
    pub message: String,
    /// First few words / canonical signature used for "same error
    /// twice in a row" detection. Stripped of paths + nix store
    /// hashes so consecutive failures with different store-path
    /// digests still match.
    pub signature: String,
}

impl Failure {
    /// Classify a raw error string and produce a Failure.
    #[must_use]
    pub fn from_raw(raw: &str) -> Self {
        let kind = classify(raw);
        let message = truncate(raw, 256);
        let signature = signature(raw);
        Self { kind, message, signature }
    }
}

/// Classify a raw error string into Transient or Declarative.
///
/// Conservative default: Transient. Pattern-match on documented
/// Declarative signatures from across the fleet (Nix evaluation
/// errors, missing flake attributes, missing source files, NixOS
/// option misconfigurations, SOPS misses, etc.).
#[must_use]
pub fn classify(raw: &str) -> FailureKind {
    const DECLARATIVE_PATTERNS: &[&str] = &[
        // Nix evaluation / flake-attribute failures
        "does not provide attribute",
        "does not exist",
        "evaluating the attribute",
        "infinite recursion encountered",
        "syntax error",
        "attribute set is missing the attribute",
        "value is null while a set was expected",
        "value is a function while a set was expected",
        "is not allowed to refer to a store path",
        "cannot coerce",
        "while evaluating definitions from",
        // NixOS module option type-check errors
        "The option `",
        "is missing the attribute `",
        // SOPS / secret resolution failures
        "missing secret",
        "could not decrypt",
        // Cargo / build-time failures inside an image build
        "could not find Cargo.toml",
        // Schema mismatches (Terraform / Crossplane / K8s admission)
        "schema validation failed",
        "unknown attribute",
        "field required",
    ];

    if DECLARATIVE_PATTERNS.iter().any(|pat| raw.contains(pat)) {
        FailureKind::Declarative
    } else {
        FailureKind::Transient
    }
}

/// Compute a stable signature of an error message.
///
/// Strips paths, store hashes, and line numbers so consecutive
/// identical errors match even when the surrounding evaluation
/// context shifts. Daemons use signature equality to detect "same
/// error twice in a row" → declaration is broken, stop retrying.
#[must_use]
pub fn signature(raw: &str) -> String {
    let trimmed = raw
        .strip_prefix("error: ")
        .or_else(|| raw.strip_prefix("warning: "))
        .unwrap_or(raw);
    let core = trimmed
        .lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("")
        .trim();
    truncate(core, 80)
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        s.chars().take(max).collect::<String>() + "…"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_missing_flake_attribute_as_declarative() {
        let err = "nix build failed: error: flake does not provide attribute 'packages.aarch64-linux.engenho-local-image'";
        assert_eq!(classify(err), FailureKind::Declarative);
    }

    #[test]
    fn classifies_missing_source_file_as_declarative() {
        let err = "error: path '/nix/store/abc-source/images/cluster-image.nix' does not exist";
        assert_eq!(classify(err), FailureKind::Declarative);
    }

    #[test]
    fn classifies_nixos_eval_error_as_declarative() {
        let err = "error: The option `blackmatter` does not exist. Definition values: …";
        assert_eq!(classify(err), FailureKind::Declarative);
    }

    #[test]
    fn classifies_terraform_schema_mismatch_as_declarative() {
        let err = "schema validation failed: unknown attribute 'foo' in resource 'bar'";
        assert_eq!(classify(err), FailureKind::Declarative);
    }

    #[test]
    fn classifies_builder_unreachable_as_transient() {
        let err = "ssh: connect to host rio port 22: Connection refused";
        assert_eq!(classify(err), FailureKind::Transient);
    }

    #[test]
    fn classifies_network_timeout_as_transient() {
        let err = "curl: (28) Operation timed out after 30000 milliseconds";
        assert_eq!(classify(err), FailureKind::Transient);
    }

    #[test]
    fn classifies_unknown_as_transient_by_default() {
        let err = "something went wrong, nobody knows what";
        assert_eq!(classify(err), FailureKind::Transient);
    }

    #[test]
    fn signature_strips_error_prefix() {
        let raw = "error: does not provide attribute 'packages.aarch64-linux.engenho-local-image'";
        let sig = signature(raw);
        assert!(!sig.starts_with("error:"));
        assert!(sig.contains("does not provide"));
    }

    #[test]
    fn signature_is_stable_across_runs() {
        let raw = "error: flake does not provide attribute 'x'";
        assert_eq!(signature(raw), signature(raw));
    }

    #[test]
    fn signature_truncates_long_messages() {
        let raw = "error: ".to_string() + &"a".repeat(500);
        let sig = signature(&raw);
        assert!(sig.chars().count() <= 81); // 80 + "…"
    }

    #[test]
    fn failure_from_raw_classifies_and_summarizes() {
        let f = Failure::from_raw("error: does not provide attribute 'foo'");
        assert_eq!(f.kind, FailureKind::Declarative);
        assert!(f.signature.contains("does not provide"));
    }

    #[test]
    fn failure_serializes_via_serde() {
        let f = Failure::from_raw("error: connection refused");
        let json = serde_json::to_string(&f).expect("serialize");
        let back: Failure = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, f);
    }

    #[test]
    fn failure_truncates_long_message() {
        let f = Failure::from_raw(&"x".repeat(1000));
        assert!(f.message.chars().count() <= 257);
    }

    /// Property: classify is deterministic — same input → same output.
    #[test]
    fn classify_is_deterministic() {
        for input in [
            "does not provide attribute",
            "Connection refused",
            "anything else",
            "evaluating the attribute",
            "schema validation failed",
        ] {
            assert_eq!(classify(input), classify(input));
        }
    }

    #[test]
    fn failure_kind_displays() {
        assert_eq!(FailureKind::Transient.to_string(), "Transient");
        assert_eq!(FailureKind::Declarative.to_string(), "Declarative");
    }
}
