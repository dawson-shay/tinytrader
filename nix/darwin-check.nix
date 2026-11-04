# checks.<system>.darwin-services: the nix-darwin module evaluated against a
# stand-in for nix-darwin's options (this flake doesn't depend on nix-darwin),
# with one agent and one daemon. Each must start through /bin/wait4path and
# then exec its own program with its arguments, keeping KeepAlive.
{ ... }:
{
  perSystem =
    { pkgs, lib, ... }:
    let
      anything = lib.mkOption {
        type = lib.types.anything;
        default = { };
      };
      # The nix-darwin options the module sets, as plain values.
      darwinStub = {
        options = {
          launchd.daemons = anything;
          launchd.user.agents = anything;
          system.primaryUser = lib.mkOption { type = lib.types.str; };
          system.activationScripts = anything;
          environment.etc = anything;
          assertions = lib.mkOption {
            type = lib.types.listOf lib.types.anything;
            default = [ ];
          };
        };
      };
      program = pkgs.writeShellScriptBin "svc" "exit 0";
      eval = lib.evalModules {
        modules = [
          darwinStub
          ../infra/modules/darwin-services.nix
          {
            system.primaryUser = "someone";
            tinytrader.services.one = {
              enable = true;
              package = program;
              args = [
                "--flag"
                "a value with spaces"
              ];
            };
            tinytrader.services.two = {
              enable = true;
              package = program;
              launchd = "daemon";
            };
          }
        ];
      };
      cfg = eval.config;
      jobs = [
        cfg.launchd.user.agents.tinytrader-one.serviceConfig
        cfg.launchd.daemons.tinytrader-two.serviceConfig
      ];
      exe = lib.getExe program;
      want = [
        [
          "/bin/sh"
          "-c"
          "/bin/wait4path /nix/store && exec \"$@\""
          "wait4path"
          exe
          "--flag"
          "a value with spaces"
        ]
        [
          "/bin/sh"
          "-c"
          "/bin/wait4path /nix/store && exec \"$@\""
          "wait4path"
          exe
        ]
      ];
      got = map (j: j.ProgramArguments) jobs;
      kept = lib.all (j: j.KeepAlive == true && j.RunAtLoad == true) jobs;
    in
    {
      checks.darwin-services =
        assert lib.assertMsg (
          got == want
        ) "darwin-services: ProgramArguments must start through wait4path: got ${builtins.toJSON got}";
        assert lib.assertMsg kept "darwin-services: KeepAlive and RunAtLoad must stay on";
        pkgs.runCommand "tinytrader-darwin-services-check" { } "touch $out";
    };
}
