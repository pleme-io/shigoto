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
pub struct Dag {
    graph: DiGraph<JobId, ()>,
    index: HashMap<JobId, NodeIndex>,
}

impl Dag {
    #[must_use]
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            index: HashMap::new(),
        }
    }

    /// Add a node if absent; return its index either way.
    pub fn ensure_node(&mut self, id: JobId) -> NodeIndex {
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
    pub fn add_edge(&mut self, from: JobId, to: JobId) {
        let f = self.ensure_node(from);
        let t = self.ensure_node(to);
        if !self.graph.contains_edge(f, t) {
            self.graph.add_edge(f, t, ());
        }
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
}
