{
  description = "Typed job-system primitive (仕事 — work/job/task) — the seventh foundational pleme-io tier alongside tatara, shikumi, sekkei, takumi, forge, arch-synthesizer. Comprehensive typed surface: Job + JobId + JobPhase (10-state FSM) + JobKind + Dag + Scheduler + Budget tree + RetryPolicy + Gate + TransitionEmitter + TickReceipt. Bootstrap consumer is tend (daemon + operator share one Scheduler). Spec: theory/SHIGOTO.md.";

  # substrate.rust.library dispatches over Cargo.gen.lock (the slim gen delta,
  # reconstructed to the full BuildSpec in pure Nix) — no crate2nix, no Cargo.nix.
  inputs.substrate.url = "github:pleme-io/substrate";

  outputs = { substrate, ... }: substrate.rust.library {
    src = ./.;
    member = "shigoto";
  };
}
