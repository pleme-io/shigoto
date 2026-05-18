//! shigoto-test — test helpers for consumers.
//!
//! Spec: `theory/SHIGOTO.md` §III.13. v0.1.0 scaffold — the idempotence
//! proptest harness lands once the scheduler is implemented.
//!
//! Planned harness signature:
//! ```ignore
//! pub async fn idempotence_quickcheck<J: Job>(
//!     job: &J,
//!     input: J::Input,
//! ) -> Result<(), IdempotenceViolation>;
//! ```

#![forbid(unsafe_code)]
