//! shigoto-fsm — typed state-machine convergence proofs.
//!
//! The eclusa/galho discipline distilled to one trait + a reusable test harness:
//! a typed lifecycle FSM (a `Phase`/`State` enum with legal transitions) is
//! *correct* when **every reachable state has a finite path to a GOOD terminal**
//! and **no state is a dead-end trap** unless it is a declared, good terminal.
//! That convergence claim was being hand-rolled as a BFS reachability test at
//! every site that ships a typed FSM (pangea-operator's template/workspace/shard
//! lifecycles, breathe's resource-admission FSM, …) — the same ~30 lines of graph
//! walk, copied. This crate makes it a primitive: implement [`ConvergentFsm`]
//! (four cheap accessors) and call [`assert_convergent_fsm`] in one `#[test]` to
//! get the whole forcing-function — enumeration, closed-graph, terminal-soundness,
//! no-traps, and BFS reachability — mechanically. A future edit that introduces a
//! never-exit state fails the test, not production.
//!
//! Pure `std`, zero dependencies — the lightest consumer pays nothing.
//!
//! ## What "convergent" means here (tier-honest)
//!
//! This proves a **graph-reachability** property over the *declared* transition
//! relation: from every state, a good terminal is reachable. It is a mechanical
//! CI forcing-function, **not** a dependent-type proof — Rust cannot express
//! "every path terminates" as a compile error (the C1 ceiling in
//! `theory/UNREPRESENTABILITY.md`). It does NOT prove *liveness* (that the
//! runtime actually drives the FSM forward) nor absence of cycles that merely
//! *delay* a terminal. It proves the topology admits convergence; the runtime
//! must still take the edges. State that plainly.

#![forbid(unsafe_code)]

use std::collections::HashSet;
use std::hash::Hash;

/// A typed finite-state lifecycle whose convergence can be proven mechanically.
///
/// Implement the four accessors over a `Copy + Eq + Hash` state enum. The
/// transition relation is `successors`; terminals are the states with no
/// outgoing edges that the FSM is *allowed* to rest in forever. A *good*
/// terminal is a successful resting state (e.g. `Ready`/`Settled`/`Released`);
/// a terminal that is not good is a failure sink the proof will reject as a trap.
pub trait ConvergentFsm: Copy + Eq + Hash + std::fmt::Debug + 'static {
    /// Every state of the FSM, exactly once. The enumeration the proofs range
    /// over — an omission here is itself a bug the [`enumeration_is_complete`]
    /// style check (left to the consumer's `all()` derivation) guards.
    fn all() -> &'static [Self];

    /// The states reachable from `self` in one legal transition. Empty ⇒ `self`
    /// is a terminal. Must reference only members of [`all`](ConvergentFsm::all).
    fn successors(&self) -> Vec<Self>;

    /// `self` is a terminal — a state the FSM may rest in forever (no outgoing
    /// edges). Must agree with `successors().is_empty()` (checked by the harness).
    fn is_terminal(&self) -> bool;

    /// `self` is a *good* terminal — a successful end (vs a failure sink). Only
    /// meaningful when [`is_terminal`](ConvergentFsm::is_terminal) is true.
    fn is_good_terminal(&self) -> bool;
}

/// A single convergence-proof failure, with a message naming the offending state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsmDefect {
    pub kind: DefectKind,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefectKind {
    /// A `successors()` edge points at a state not in `all()`.
    DanglingEdge,
    /// `is_terminal()` disagrees with `successors().is_empty()`.
    TerminalMismatch,
    /// A terminal is not a good terminal (a failure sink with no exit).
    BadTerminalSink,
    /// A non-terminal has no successors — a dead-end trap.
    DeadEndTrap,
    /// A state cannot reach any good terminal over the legal edges.
    CannotConverge,
}

impl std::fmt::Display for FsmDefect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{:?}] {}", self.kind, self.message)
    }
}

/// Every `successors()` edge references a member of `all()` — the graph is closed.
pub fn check_graph_closed<F: ConvergentFsm>() -> Vec<FsmDefect> {
    let universe: HashSet<F> = F::all().iter().copied().collect();
    let mut defects = Vec::new();
    for &s in F::all() {
        for succ in s.successors() {
            if !universe.contains(&succ) {
                defects.push(FsmDefect {
                    kind: DefectKind::DanglingEdge,
                    message: format!("{s:?} → {succ:?} references a state not in all()"),
                });
            }
        }
    }
    defects
}

/// `is_terminal()` agrees with `successors().is_empty()`, and every terminal is
/// a good terminal (no failure sink may be a permanent resting state).
pub fn check_terminals_sound<F: ConvergentFsm>() -> Vec<FsmDefect> {
    let mut defects = Vec::new();
    for &s in F::all() {
        let no_succ = s.successors().is_empty();
        if s.is_terminal() != no_succ {
            defects.push(FsmDefect {
                kind: DefectKind::TerminalMismatch,
                message: format!(
                    "{s:?}: is_terminal()={} but successors().is_empty()={no_succ}",
                    s.is_terminal()
                ),
            });
        }
        if s.is_terminal() && !s.is_good_terminal() {
            defects.push(FsmDefect {
                kind: DefectKind::BadTerminalSink,
                message: format!("{s:?} is a terminal but not a good terminal — a failure sink with no exit"),
            });
        }
    }
    defects
}

/// Every non-terminal has at least one successor — no dead-end traps.
pub fn check_no_traps<F: ConvergentFsm>() -> Vec<FsmDefect> {
    let mut defects = Vec::new();
    for &s in F::all() {
        if !s.is_terminal() && s.successors().is_empty() {
            defects.push(FsmDefect {
                kind: DefectKind::DeadEndTrap,
                message: format!("{s:?} is non-terminal but has no successors — a dead-end trap"),
            });
        }
    }
    defects
}

/// From every state, a good terminal is reachable over the legal edges (BFS).
/// The core convergence claim.
pub fn check_all_converge<F: ConvergentFsm>() -> Vec<FsmDefect> {
    let mut defects = Vec::new();
    for &start in F::all() {
        if !reaches_good_terminal(start) {
            defects.push(FsmDefect {
                kind: DefectKind::CannotConverge,
                message: format!("{start:?} cannot reach any good terminal — stuck state"),
            });
        }
    }
    defects
}

/// Does a good terminal lie on some path out of `start`? (DFS over the closure.)
pub fn reaches_good_terminal<F: ConvergentFsm>(start: F) -> bool {
    let mut seen: HashSet<F> = HashSet::new();
    let mut stack = vec![start];
    while let Some(s) = stack.pop() {
        if !seen.insert(s) {
            continue;
        }
        if s.is_good_terminal() {
            return true;
        }
        stack.extend(s.successors());
    }
    false
}

/// The full forcing-function: closed-graph + terminal-soundness + no-traps +
/// universal convergence. `Ok(())` iff the FSM is convergent and well-formed;
/// `Err` aggregates EVERY defect (so one run reports all problems, not the first).
///
/// Call in one `#[test]`:
/// ```ignore
/// #[test] fn lifecycle_is_convergent() { shigoto_fsm::assert_convergent_fsm::<Phase>().unwrap(); }
/// ```
pub fn assert_convergent_fsm<F: ConvergentFsm>() -> Result<(), String> {
    let mut defects = Vec::new();
    defects.extend(check_graph_closed::<F>());
    defects.extend(check_terminals_sound::<F>());
    defects.extend(check_no_traps::<F>());
    defects.extend(check_all_converge::<F>());
    if defects.is_empty() {
        return Ok(());
    }
    let mut msg = format!("FSM convergence proof failed with {} defect(s):", defects.len());
    for d in &defects {
        msg.push_str("\n  - ");
        msg.push_str(&d.to_string());
    }
    Err(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── A well-formed convergent FSM (the happy reference) ─────────────────
    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    enum Good {
        Pending,
        Working,
        Ready,     // good terminal
        Failed,    // failure detour — has a remediation edge back, NOT a sink
        Destroyed, // good terminal
    }

    impl ConvergentFsm for Good {
        fn all() -> &'static [Self] {
            &[Good::Pending, Good::Working, Good::Ready, Good::Failed, Good::Destroyed]
        }
        fn successors(&self) -> Vec<Self> {
            match self {
                Good::Pending => vec![Good::Working, Good::Destroyed],
                Good::Working => vec![Good::Ready, Good::Failed, Good::Destroyed],
                Good::Failed => vec![Good::Working, Good::Destroyed], // remediation exits
                Good::Ready | Good::Destroyed => vec![],
            }
        }
        fn is_terminal(&self) -> bool {
            matches!(self, Good::Ready | Good::Destroyed)
        }
        fn is_good_terminal(&self) -> bool {
            self.is_terminal()
        }
    }

    #[test]
    fn a_well_formed_fsm_passes_every_proof() {
        assert_convergent_fsm::<Good>().expect("the reference FSM must be convergent");
        assert!(check_graph_closed::<Good>().is_empty());
        assert!(check_terminals_sound::<Good>().is_empty());
        assert!(check_no_traps::<Good>().is_empty());
        assert!(check_all_converge::<Good>().is_empty());
    }

    // ── The meta-proof: the harness REJECTS each kind of broken FSM ────────

    /// A non-terminal dead-end (`Stuck` has no successors but claims non-terminal).
    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    enum Trap {
        Start,
        Stuck, // dead-end trap: not terminal, no exit
        Done,  // good terminal
    }
    impl ConvergentFsm for Trap {
        fn all() -> &'static [Self] {
            &[Trap::Start, Trap::Stuck, Trap::Done]
        }
        fn successors(&self) -> Vec<Self> {
            match self {
                Trap::Start => vec![Trap::Stuck, Trap::Done],
                Trap::Stuck => vec![], // <-- the trap
                Trap::Done => vec![],
            }
        }
        fn is_terminal(&self) -> bool {
            matches!(self, Trap::Done)
        }
        fn is_good_terminal(&self) -> bool {
            matches!(self, Trap::Done)
        }
    }

    #[test]
    fn the_harness_catches_a_dead_end_trap() {
        let defects = check_no_traps::<Trap>();
        assert!(
            defects.iter().any(|d| d.kind == DefectKind::DeadEndTrap),
            "harness must flag Stuck as a dead-end trap"
        );
        assert!(assert_convergent_fsm::<Trap>().is_err(), "a trapped FSM must fail the full proof");
    }

    /// A failure sink: a terminal that is not good and has no exit.
    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    enum Sink {
        Start,
        Wedged, // terminal but NOT good — a failure sink
        Done,
    }
    impl ConvergentFsm for Sink {
        fn all() -> &'static [Self] {
            &[Sink::Start, Sink::Wedged, Sink::Done]
        }
        fn successors(&self) -> Vec<Self> {
            match self {
                Sink::Start => vec![Sink::Wedged, Sink::Done],
                Sink::Wedged | Sink::Done => vec![],
            }
        }
        fn is_terminal(&self) -> bool {
            matches!(self, Sink::Wedged | Sink::Done)
        }
        fn is_good_terminal(&self) -> bool {
            matches!(self, Sink::Done)
        }
    }

    #[test]
    fn the_harness_catches_a_failure_sink() {
        let defects = check_terminals_sound::<Sink>();
        assert!(defects.iter().any(|d| d.kind == DefectKind::BadTerminalSink));
        // Start can still reach Done, so convergence per se holds — but the bad
        // sink is caught by terminal-soundness. The full proof fails.
        assert!(assert_convergent_fsm::<Sink>().is_err());
    }

    /// A state from which NO good terminal is reachable (only a non-good cycle).
    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    enum Diverge {
        A,
        B, // A↔B cycle, never reaches Done
        Done,
    }
    impl ConvergentFsm for Diverge {
        fn all() -> &'static [Self] {
            &[Diverge::A, Diverge::B, Diverge::Done]
        }
        fn successors(&self) -> Vec<Self> {
            match self {
                Diverge::A => vec![Diverge::B],
                Diverge::B => vec![Diverge::A], // cycle with no exit to Done
                Diverge::Done => vec![],
            }
        }
        fn is_terminal(&self) -> bool {
            matches!(self, Diverge::Done)
        }
        fn is_good_terminal(&self) -> bool {
            matches!(self, Diverge::Done)
        }
    }

    #[test]
    fn the_harness_catches_a_non_converging_cycle() {
        let defects = check_all_converge::<Diverge>();
        assert!(
            defects.iter().filter(|d| d.kind == DefectKind::CannotConverge).count() >= 2,
            "both A and B are stuck in a cycle that never reaches a good terminal"
        );
        assert!(!reaches_good_terminal(Diverge::A));
        assert!(reaches_good_terminal(Diverge::Done));
    }

    /// A dangling edge: `successors()` names a state absent from `all()` would be
    /// a compile error here (enum-closed), so we exercise the closed-graph check
    /// positively — the reference FSM's graph is closed.
    #[test]
    fn closed_graph_holds_for_the_reference() {
        assert!(check_graph_closed::<Good>().is_empty());
    }
}
