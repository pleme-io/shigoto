//! Verify shigoto-types registers `RetryOutcome` into the
//! fleet-wide DispatcherCatalog. shigoto is the FIFTH consumer
//! class adopting the typed-dispatcher catamorphism — joins
//! gen / caixa / wasm-platform / cofre.
//!
//! RetryOutcome is the typed decision a RetryPolicy emits when a
//! job fails: either retry at a future timestamp, or give up
//! (deadletter). The substrate's typed shadow now spans the
//! scheduler's failure-handling surface in addition to code
//! supply, OTP hot-upgrades, sandbox capabilities, and secret
//! materialization.

use gen_platform::{TypedDispatcherTrait, catalog};
use shigoto_types::RetryOutcome;

#[test]
fn retry_outcome_registers_into_fleet_catalog() {
    let entry = catalog::by_label("shigoto.retry-outcome")
        .expect("shigoto-types must register RetryOutcome");
    assert_eq!(entry.label, "shigoto.retry-outcome");
    assert_eq!((entry.variant_count)(), 2);
}

#[test]
fn variant_kinds_kebab() {
    let kinds = RetryOutcome::variant_kinds();
    assert_eq!(kinds, vec!["retry", "deadletter"]);
}

#[test]
fn variant_fields_surfaced() {
    let fields = RetryOutcome::variant_fields();
    assert_eq!(
        fields,
        vec![("retry", vec!["until_ms"]), ("deadletter", vec![]),]
    );
}

#[test]
fn variant_count_via_trait() {
    assert_eq!(RetryOutcome::variant_count(), 2);
}
