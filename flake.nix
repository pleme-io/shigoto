{
  description = "Typed job-system primitive (仕事 — work/job/task) — the seventh foundational pleme-io tier alongside tatara, shikumi, sekkei, takumi, forge, arch-synthesizer. Comprehensive typed surface: Job + JobId + JobPhase (10-state FSM) + JobKind + Dag + Scheduler + Budget tree + RetryPolicy + Gate + TransitionEmitter + TickReceipt. Bootstrap consumer is tend (daemon + operator share one Scheduler). Spec: theory/SHIGOTO.md.";
  inputs = {
    nixpkgs = {
      url = "github:nixos/nixpkgs?ref=nixos-unstable";
    };
    flake-utils = {
      url = "github:numtide/flake-utils";
    };
    # crate2nix + fenix are consumed by the rust-library-workspace-flake
    # builder below (outputs destructures both). They MUST be declared as
    # inputs — without them nix resolves `flake:crate2nix` / `flake:fenix`
    # from the registry and `nix flake check` fails on a clean CI runner
    # ("cannot find flake 'flake:crate2nix' in the flake registries").
    # Both follow nixpkgs to avoid closure duplication (org convention).
    crate2nix = {
      url = "github:nix-community/crate2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    substrate = {
      url = "github:pleme-io/substrate";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };
  outputs = inputs @ { self, nixpkgs, crate2nix, fenix, substrate, ... }:
    (import "${substrate}/lib/rust-library-workspace-flake.nix" {
      inherit nixpkgs crate2nix fenix;
    }) {
      workspaceName = "shigoto";
      members = [ "shigoto" "shigoto-types" "shigoto-dag" "shigoto-scheduler" "shigoto-budget" "shigoto-retry" "shigoto-gate" "shigoto-emit" "shigoto-test" ];
      src = self;
    };
}
