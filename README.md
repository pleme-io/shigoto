# shigoto (仕事)

> The typed job-system primitive for pleme-io. **Spec:** [`theory/SHIGOTO.md`](https://github.com/pleme-io/theory/blob/main/SHIGOTO.md).

`shigoto` is a typed primitive every pleme-io tool whose internals form a
dependency-ordered, fallible, retryable, parallelism-bounded work graph
adopts. Bootstrap consumer is [`tend`](https://github.com/pleme-io/tend);
the daemon and the K8s operator share one Scheduler. Future consumers:
forge-gen, pangea-operator, tameshi, convergence-flow.

## Surface

| Concept | Type | What it owns |
|---|---|---|
| Job | `Job` trait | a unit of work — typed Input/Output/Error + idempotent `execute()` |
| JobId | `(scope, kind, subject)` | stable identity across cycles + restarts |
| JobPhase | 10-state FSM | Pending → Gated → Ready → Running → {Succeeded \| Failed → Retrying \| Deadlettered} \| Skipped \| WaitingForOperator |
| Dag | `Dag` | typed dependency graph; cycles rejected at `validate()` |
| Scheduler | `Scheduler` trait | one `tick` per call (daemons loop, k8s reconcilers map CR-events to ticks) |
| Budget | `BudgetTree` | three-dimension envelope (global × by-kind × by-scope), min-intersection |
| RetryPolicy | enum | NoRetry / Fixed / Exponential / Custom(RetryDecider) |
| Gate | `Gate` trait | typed precondition; **pure**, no IO |
| TransitionEmitter | trait | non-blocking audit sink — every FSM transition emits one event |
| TickReceipt | struct | derived per-tick rollup (phase counts + transitions + unhealed drift) |

## Crates

```
shigoto              — umbrella; re-exports the public surface
shigoto-types        — Job, JobId, JobPhase, JobKindId, TickReceipt, ...
shigoto-dag          — Dag + DagError (typed dependency graph)
shigoto-scheduler    — Scheduler trait + InProcessScheduler
shigoto-budget       — BudgetTree, BudgetSpec, BudgetError (HOW MUCH runs)
shigoto-rank         — Schedulable trait + rank/pick (WHICH runs next; anti-starvation)
shigoto-retry        — RetryPolicy, RetryDecider trait
shigoto-gate         — Gate trait + standard gates
shigoto-emit         — TransitionEmitter trait + sinks
shigoto-test         — idempotence proptest harness, golden tests
```

Consumers depend on `shigoto = "0.1"` and reach the algebra via
`use shigoto::{Job, JobId, JobPhase, Dag, Scheduler, …};`.

## Status

**v0.1.0 — scaffold.** Public surface declared per the [canonical spec](https://github.com/pleme-io/theory/blob/main/SHIGOTO.md);
implementations land as the bootstrap consumer (`tend`) migrates per
SHIGOTO.md §IV.3 (M0.7–M0.11).

Prime-directive promotion criteria (SHIGOTO.md §V.1):
- [ ] Two production consumers, ≥30 days operational use
- [ ] Typed surface stable ≥30 days (no breaking changes to Job/Dag/Scheduler)
- [ ] Audit log consumed by ≥1 sink nothing else reads
- [ ] One non-bootstrap consumer authored from scratch as shigoto-native

Until all four hold, shigoto is "documented; strongly recommended for new
work" but not enforced.

## Building

```
cargo build --workspace
cargo test --workspace
```

Or via Nix:

```
nix build
nix run .#check-all
```

The flake uses `substrate/lib/rust-library-workspace-flake.nix` —
per-member packages via `nix build .#shigoto-types`, etc.

## License

MIT.
