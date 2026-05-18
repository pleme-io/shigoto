//! shigoto-emit — typed transition emitter.
//!
//! Spec: `theory/SHIGOTO.md` §III.10 + §VIII. Every FSM transition
//! emits one event; sinks compose. Emission is non-blocking; queue
//! overflow drops with a typed `TransitionDropped` log line —
//! observability never back-pressures the scheduler.

#![forbid(unsafe_code)]

use shigoto_types::TransitionEvent;

pub trait TransitionEmitter: Send + Sync {
    fn emit(&self, event: TransitionEvent);
}
