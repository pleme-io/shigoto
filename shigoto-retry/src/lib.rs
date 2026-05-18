//! shigoto-retry — typed failure-recovery strategies.
//!
//! Spec: `theory/SHIGOTO.md` §III.8 + §VII.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RetryPolicy {
    NoRetry,
    Fixed { attempts: u32, delay_ms: u64 },
    Exponential {
        attempts: u32,
        base_ms: u64,
        max_ms: u64,
        jitter: f64,
    },
    #[serde(skip)]
    Custom(Arc<dyn RetryDecider>),
}

pub trait RetryDecider: std::fmt::Debug + Send + Sync {
    fn decide(&self, attempt: u32, history: &[FailureRecord]) -> RetryDecision;
}

#[derive(Debug, Clone)]
pub enum RetryDecision {
    Retry { after: Duration },
    Deadletter,
}

#[derive(Debug, Clone)]
pub struct FailureRecord {
    pub attempt: u32,
    pub at_ms: i64,
    pub error: String,
}
