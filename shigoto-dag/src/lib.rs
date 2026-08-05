//! shigoto-dag — typed dependency graph over JobIds.
//!
//! Spec: `theory/SHIGOTO.md` §III.5 + §V.
//!
//! Lifted from `tend/src/operator/dag.rs` (the prototype that exercised
//! this shape against pleme-io's flake-update workspace). Generalized
//! from the flake-specific `DagNodeId { repo, input }` to the typed
//! `JobId { scope, kind, subject }` from `shigoto-types`. Every consumer
//! (tend, forge-gen, pangea-operator, tameshi, convergence-flow) builds
//! its own JobIds and the Dag operates over them uniformly.
//!
//! v0.1 carries the IDs only; full Job-storage lands as M0.8 of the
//! migration plan (SHIGOTO.md §IV.3) when the Scheduler runtime needs
//! to hand each Job to its execute hook. Until then, `Dag` is the
//! typed topology surface — `add_edge`, `toposort`, `waves` — and
//! consumers maintain their own JobId → Job lookup.

#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};

use petgraph::algo::toposort;
use petgraph::graph::{DiGraph, NodeIndex};
use shigoto_types::JobId;

#[derive(thiserror::Error, Debug)]
pub enum DagError {
    #[error("cycle in DAG at {0:?}")]
    Cycle(JobId),
    #[error("dangling edge to job not in graph: {0:?}")]
    DanglingEdge(JobId),
}

/// Typed DAG of JobIds. Edges declare "to may not start until from
/// reaches a terminal phase" (Succeeded | Skipped | Deadlettered).
/// Acyclic by construction at edge-add time would require an O(V+E)
/// check on every insert; instead, cycles are detected at
/// `toposort()` / `waves()`, where consumers must run before
/// scheduling anyway.
///
/// `Debug` is derived deliberately, not incidentally. Without it a consumer
/// that wraps a `Dag` cannot derive `Debug` either, and the first thing that
/// silently breaks is `Result::unwrap_err()` / `expect_err()` — whose
/// `T: Debug` bound is what makes a *test* compile. Measured 2026-07-31:
/// `caixa-core` and `caixa-actions` had test targets that could not build for
/// exactly this reason, so no test in either crate ran at all, and nothing
/// reported it. A missing `Debug` on a widely-wrapped type does not read as a
/// missing trait; it reads as a green repo whose suite never executed.
#[derive(Debug)]
pub struct Dag {
    graph: DiGraph<JobId, ()>,
    index: HashMap<JobId, NodeIndex>,
    /// Jobs the caller actually DECLARED, as opposed to nodes that only
    /// exist because an edge mentioned them. See [`Dag::validate`].
    declared: HashSet<JobId>,
}

impl Dag {
    #[must_use]
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            index: HashMap::new(),
            declared: HashSet::new(),
        }
    }

    /// Add a node if absent; return its index either way.
    ///
    /// This DECLARES the job — it marks it as one the caller intends to
    /// run, which is what [`Dag::validate`] checks edges against.
    pub fn ensure_node(&mut self, id: JobId) -> NodeIndex {
        let idx = self.node_index(id.clone());
        self.declared.insert(id);
        idx
    }

    /// Intern a node without declaring it. Used by [`Dag::add_edge`] so
    /// that an edge naming a job nobody declared stays detectable.
    fn node_index(&mut self, id: JobId) -> NodeIndex {
        if let Some(idx) = self.index.get(&id) {
            return *idx;
        }
        let idx = self.graph.add_node(id.clone());
        self.index.insert(id, idx);
        idx
    }

    /// Add edge `from → to`. Idempotent: re-adding an existing edge is
    /// a no-op (we de-dup explicitly because petgraph would otherwise
    /// store parallel edges).
    /// An endpoint that was never declared is interned but NOT marked
    /// declared, so [`Dag::validate`] can report it. Previously both
    /// endpoints went through `ensure_node`, which silently materialised
    /// a node for a typo'd dependency — making `DagError::DanglingEdge`
    /// unconstructible and a mistyped edge look like a real job that
    /// simply never ran.
    pub fn add_edge(&mut self, from: JobId, to: JobId) {
        let f = self.node_index(from);
        let t = self.node_index(to);
        if !self.graph.contains_edge(f, t) {
            self.graph.add_edge(f, t, ());
        }
    }

    /// Check the graph before scheduling: every edge endpoint must be a
    /// declared job, and the graph must be acyclic.
    ///
    /// Call this once after construction. `toposort()` and `waves()`
    /// find cycles on their own, but neither can see a dangling edge —
    /// by then a typo'd dependency is indistinguishable from a real node
    /// with no work attached.
    ///
    /// # Errors
    ///
    /// [`DagError::DanglingEdge`] naming the first undeclared endpoint,
    /// or [`DagError::Cycle`] naming a node on the cycle.
    pub fn validate(&self) -> Result<(), DagError> {
        let mut dangling: Vec<&JobId> = self
            .index
            .keys()
            .filter(|id| !self.declared.contains(*id))
            .collect();
        // deterministic report: the same malformed graph names the same
        // job every run, so a failure is reproducible from its message.
        // JobId is not Ord, so sort by its Debug rendering — this is a
        // diagnostic ordering, not a semantic one.
        dangling.sort_by_key(|id| format!("{id:?}"));
        if let Some(id) = dangling.first() {
            return Err(DagError::DanglingEdge((*id).clone()));
        }
        self.toposort().map(|_| ())
    }

    /// Topologically sort the entire DAG. Returns nodes in order
    /// (parents before children). Errors if a cycle is found —
    /// consumers' edge graphs are acyclic by construction so this
    /// indicates a malformed input.
    pub fn toposort(&self) -> Result<Vec<JobId>, DagError> {
        toposort(&self.graph, None)
            .map_err(|cyc| DagError::Cycle(self.graph[cyc.node_id()].clone()))
            .map(|order| order.into_iter().map(|i| self.graph[i].clone()).collect())
    }

    /// Partition the DAG into topological waves. Wave 0 = nodes with
    /// zero remaining dependencies; wave N = nodes whose dependencies
    /// all live in waves [0..N).
    ///
    /// Restricted to `affected` if provided — useful when only a subset
    /// of nodes have actually advanced upstream and need verification.
    /// Unaffected nodes are skipped entirely.
    pub fn waves(&self, affected: Option<&HashSet<JobId>>) -> Result<Vec<Vec<JobId>>, DagError> {
        let order = self.toposort()?;
        let restrict_to: Option<HashSet<JobId>> = affected.cloned();

        let want = |id: &JobId| -> bool { restrict_to.as_ref().map_or(true, |s| s.contains(id)) };

        let mut wave_of: HashMap<JobId, usize> = HashMap::new();
        for id in &order {
            if !want(id) {
                continue;
            }
            let idx = self.index[id];
            let mut depth = 0;
            for parent in self
                .graph
                .neighbors_directed(idx, petgraph::Direction::Incoming)
            {
                let pid = &self.graph[parent];
                if want(pid) {
                    if let Some(pd) = wave_of.get(pid) {
                        depth = depth.max(*pd + 1);
                    }
                }
            }
            wave_of.insert(id.clone(), depth);
        }

        let max_wave = wave_of.values().copied().max().unwrap_or(0);
        let mut waves = vec![Vec::new(); max_wave + 1];
        for (id, depth) in wave_of {
            waves[depth].push(id);
        }
        // Sort within each wave for deterministic ordering. JobId is
        // not Ord by default (it contains Hash-only fields); sort by
        // a stable string projection.
        for w in &mut waves {
            w.sort_by_key(stable_job_key);
        }
        Ok(waves)
    }

    /// Total number of distinct nodes in the DAG.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    /// Total number of directed edges in the DAG.
    #[must_use]
    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    /// Direct predecessors of `id` — every node that has an edge
    /// pointing INTO `id`. Used by `AllUpstreamsTerminal` and similar
    /// gates to ask "have all my upstream dependencies finished?".
    ///
    /// Returns an empty vec when `id` is not in the DAG or has no
    /// incoming edges (root node). Order is petgraph-defined but
    /// stable for a given graph + insertion sequence; consumers that
    /// need deterministic order sort the result.
    #[must_use]
    pub fn predecessors(&self, id: &JobId) -> Vec<JobId> {
        let Some(idx) = self.index.get(id) else {
            return Vec::new();
        };
        self.graph
            .neighbors_directed(*idx, petgraph::Direction::Incoming)
            .map(|p| self.graph[p].clone())
            .collect()
    }
}

impl Default for Dag {
    fn default() -> Self {
        Self::new()
    }
}

/// Stable string projection of a JobId for within-wave deterministic
/// ordering. Composed from the typed fields so two JobIds that compare
/// equal yield the same key, and lexicographic comparison is total.
fn stable_job_key(id: &JobId) -> String {
    use shigoto_types::{JobScope, JobSubject};

    let scope = match &id.scope {
        JobScope::Global => "global".to_string(),
        JobScope::Workspace(ws) => format!("ws:{ws}"),
        JobScope::Repo { workspace, repo } => format!("repo:{workspace}/{repo}"),
    };
    let subject = match &id.subject {
        JobSubject::None => "".to_string(),
        JobSubject::Repo(r) => format!("repo:{r}"),
        JobSubject::Org(o) => format!("org:{o}"),
        JobSubject::Path(p) => format!("path:{}", p.display()),
        JobSubject::Pinned(s) => format!("pin:{s}"),
    };
    format!("{}|{}|{}", scope, id.kind.0, subject)
}

#[cfg(test)]
mod tests {
    use super::*;
    use shigoto_types::{JobKindId, JobScope, JobSubject};

    /// Test helper — build a flake-update-style JobId from `(repo, input)`.
    /// Mirrors the original tend `DagNodeId::new(repo, input)` shape so
    /// the lifted tests stay legible.
    fn n(repo: &str, input: &str) -> JobId {
        JobId {
            scope: JobScope::Repo {
                workspace: "test".into(),
                repo: repo.into(),
            },
            kind: JobKindId::new("flake-update"),
            subject: JobSubject::Pinned(input.into()),
        }
    }

    #[test]
    fn empty_dag_has_zero_nodes() {
        let d = Dag::new();
        assert_eq!(d.node_count(), 0);
        assert_eq!(d.toposort().unwrap(), Vec::<JobId>::new());
    }

    #[test]
    fn linear_chain_partitions_into_waves() {
        let mut d = Dag::new();
        d.add_edge(n("nix", "substrate"), n("nix", "blackmatter"));
        d.add_edge(n("nix", "blackmatter"), n("nix", "shell"));

        let waves = d.waves(None).unwrap();
        assert_eq!(waves.len(), 3);
        assert_eq!(waves[0], vec![n("nix", "substrate")]);
        assert_eq!(waves[1], vec![n("nix", "blackmatter")]);
        assert_eq!(waves[2], vec![n("nix", "shell")]);
    }

    #[test]
    fn diamond_collapses_to_three_waves() {
        // root → {a, b} → c
        let mut d = Dag::new();
        d.add_edge(n("r", "root"), n("r", "a"));
        d.add_edge(n("r", "root"), n("r", "b"));
        d.add_edge(n("r", "a"), n("r", "c"));
        d.add_edge(n("r", "b"), n("r", "c"));

        let waves = d.waves(None).unwrap();
        assert_eq!(waves.len(), 3);
        assert_eq!(waves[0].len(), 1);
        assert_eq!(waves[1].len(), 2);
        assert_eq!(waves[2].len(), 1);
        assert_eq!(waves[2][0], n("r", "c"));
    }

    #[test]
    fn affected_filter_prunes_unaffected_nodes() {
        let mut d = Dag::new();
        d.add_edge(n("r", "a"), n("r", "b"));
        d.add_edge(n("r", "b"), n("r", "c"));

        let affected: HashSet<JobId> = [n("r", "b"), n("r", "c")].into_iter().collect();
        let waves = d.waves(Some(&affected)).unwrap();
        let flat: Vec<JobId> = waves.into_iter().flatten().collect();
        assert!(flat.contains(&n("r", "b")));
        assert!(flat.contains(&n("r", "c")));
        assert!(!flat.contains(&n("r", "a")));
    }

    #[test]
    fn idempotent_edge_add() {
        let mut d = Dag::new();
        d.add_edge(n("r", "a"), n("r", "b"));
        d.add_edge(n("r", "a"), n("r", "b"));
        d.add_edge(n("r", "a"), n("r", "b"));
        assert_eq!(d.edge_count(), 1);
    }

    #[test]
    fn cycle_yields_typed_cycle_error() {
        let mut d = Dag::new();
        d.add_edge(n("r", "a"), n("r", "b"));
        d.add_edge(n("r", "b"), n("r", "a"));
        match d.toposort() {
            Err(DagError::Cycle(_)) => {}
            other => panic!("expected DagError::Cycle, got {other:?}"),
        }
    }

    #[test]
    fn predecessors_returns_direct_incoming_edges() {
        // root → a → c
        //       ↘ b → c
        let mut d = Dag::new();
        d.add_edge(n("r", "root"), n("r", "a"));
        d.add_edge(n("r", "root"), n("r", "b"));
        d.add_edge(n("r", "a"), n("r", "c"));
        d.add_edge(n("r", "b"), n("r", "c"));

        // c has two direct predecessors (a, b) — not root (root is
        // grandparent, not direct).
        let preds = d.predecessors(&n("r", "c"));
        assert_eq!(preds.len(), 2);
        assert!(preds.contains(&n("r", "a")));
        assert!(preds.contains(&n("r", "b")));
        // root has no incoming edges.
        assert!(d.predecessors(&n("r", "root")).is_empty());
        // Unknown node yields empty vec.
        assert!(d.predecessors(&n("r", "ghost")).is_empty());
    }

    /// A wrapping consumer must be able to derive `Debug`, and a `Result`
    /// carrying a `Dag` must support `unwrap_err`.
    ///
    /// This is a COMPILE-TIME seal, not a behavioural one: delete the
    /// `#[derive(Debug)]` on `Dag` and this test does not fail, it fails to
    /// BUILD — `Wrapper` stops deriving and `unwrap_err` loses its `T: Debug`
    /// bound. That is exactly the shape of the outage it guards, and it is
    /// why the assertion is a wrapper rather than a bare `Dag`: `Dag`'s own
    /// crate would notice a missing `Debug` immediately, while a CONSUMER
    /// three repos away notices only that its test target silently stopped
    /// compiling (measured in caixa, 2026-07-31).
    #[test]
    fn a_wrapping_consumer_can_derive_debug_and_unwrap_err() {
        #[derive(Debug)]
        struct Wrapper {
            #[allow(dead_code)]
            dag: Dag,
        }

        let w = Wrapper { dag: Dag::new() };
        assert!(format!("{w:?}").contains("Dag"));

        // The precise call whose `T: Debug` bound broke every caixa test
        // target. `Ok` here would panic, so the error path is the assertion.
        let r: Result<Dag, &str> = Err("boom");
        assert_eq!(r.unwrap_err(), "boom");
    }

    #[test]
    fn validate_rejects_an_edge_to_an_undeclared_job() {
        // The regression: add_edge used to ensure_node BOTH endpoints, so a
        // typo'd dependency silently became a real node with no work attached
        // and DagError::DanglingEdge was unconstructible dead code.
        let mut d = Dag::new();
        d.ensure_node(n("build","x"));
        d.add_edge(n("build","x"), n("tset","x")); // typo for "test"
        match d.validate() {
            Err(DagError::DanglingEdge(id)) => assert_eq!(id, n("tset","x")),
            other => panic!("expected DanglingEdge, got {other:?}"),
        }
    }

    #[test]
    fn validate_accepts_a_fully_declared_graph() {
        let mut d = Dag::new();
        d.ensure_node(n("build","x"));
        d.ensure_node(n("test","x"));
        d.add_edge(n("build","x"), n("test","x"));
        assert!(d.validate().is_ok());
    }

    #[test]
    fn validate_still_reports_cycles() {
        let mut d = Dag::new();
        d.ensure_node(n("a","x"));
        d.ensure_node(n("b","x"));
        d.add_edge(n("a","x"), n("b","x"));
        d.add_edge(n("b","x"), n("a","x"));
        assert!(matches!(d.validate(), Err(DagError::Cycle(_))));
    }

    #[test]
    fn add_edge_alone_does_not_declare_either_endpoint() {
        let mut d = Dag::new();
        d.add_edge(n("x","x"), n("y","x"));
        // both endpoints are dangling; the report is deterministic
        assert!(matches!(d.validate(), Err(DagError::DanglingEdge(_))));
    }
}
