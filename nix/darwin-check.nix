# checks.<system>.darwin-services: the nix-darwin module evaluated against a
# stand-in for nix-darwin's options (this flake doesn't depend on nix-darwin).
#   - Every service is a launchd daemon unless it asks otherwise, and none
#     is in launchd.user.agents: an agent starts only at login, so after a
#     reboot it stays down until someone logs in (a Mac's services were
#     down after a reboot that way).
#   - Each job starts through /bin/wait4path and then execs its own program
#     with its arguments, keeping KeepAlive and RunAtLoad, as the primary
#     user.
#   - A service that opts into launchd = "agent" still works, and warns.
{ ... }:
{
  perSystem =
    { pkgs, lib, ... }:
    let
      anything = lib.mkOption {
        type = lib.types.anything;
        default = { };
      };
      list = lib.mkOption {
        type = lib.types.listOf lib.types.anything;
        default = [ ];
      };
      # The nix-darwin options the module sets, as plain values.
      darwinStub = {
        options = {
          launchd.daemons = anything;
          launchd.user.agents = anything;
          system.primaryUser = lib.mkOption { type = lib.types.str; };
          system.activationScripts = anything;
          environment.etc = anything;
          assertions = list;
          warnings = list;
        };
      };
      program = pkgs.writeShellScriptBin "svc" "exit 0";
      evalWith =
        services:
        (lib.evalModules {
          modules = [
            darwinStub
            ../infra/modules/darwin-services.nix
            {
              system.primaryUser = "someone";
              tinytrader.services = services;
            }
          ];
        }).config;
      # As deployments write them: most leave `launchd` unset.
      cfg = evalWith {
        one = {
          enable = true;
          package = program;
          args = [
            "--flag"
            "a value with spaces"
          ];
        };
        two = {
          enable = true;
          package = program;
          launchd = "daemon";
        };
      };
      # The opt-in, still supported.
      optIn = evalWith {
        three = {
          enable = true;
          package = program;
          launchd = "agent";
        };
      };
      ours = jobs: lib.filter (lib.hasPrefix "tinytrader-") (lib.attrNames jobs);
      agents = ours cfg.launchd.user.agents;
      jobs = [
        cfg.launchd.daemons.tinytrader-one.serviceConfig
        cfg.launchd.daemons.tinytrader-two.serviceConfig
      ];
      exe = lib.getExe program;
      wait = [
        "/bin/sh"
        "-c"
        "/bin/wait4path /nix/store && exec \"$@\""
        "wait4path"
      ];
      want = [
        (
          wait
          ++ [
            exe
            "--flag"
            "a value with spaces"
          ]
        )
        (wait ++ [ exe ])
      ];
      got = map (j: j.ProgramArguments) jobs;
      kept = lib.all (j: j.KeepAlive == true && j.RunAtLoad == true) jobs;
      asUser = lib.all (j: j.UserName == "someone") jobs;
      agentJob = optIn.launchd.user.agents.tinytrader-three.serviceConfig or null;
    in
    {
      checks.darwin-services =
        assert lib.assertMsg (agents == [ ])
          "darwin-services: services must be launchd daemons, not agents (launchd.user.agents): ${toString agents}";
        assert lib.assertMsg (
          got == want
        ) "darwin-services: ProgramArguments must start through wait4path: got ${builtins.toJSON got}";
        assert lib.assertMsg kept "darwin-services: KeepAlive and RunAtLoad must stay on";
        assert lib.assertMsg asUser "darwin-services: a daemon must run as the primary user (UserName)";
        assert lib.assertMsg (
          cfg.warnings == [ ]
        ) "darwin-services: unexpected warnings: ${toString cfg.warnings}";
        assert lib.assertMsg (
          agentJob != null && agentJob.ProgramArguments == wait ++ [ exe ]
        ) "darwin-services: launchd = \"agent\" must still make a launchd agent that waits for /nix";
        assert lib.assertMsg (
          lib.length optIn.warnings == 1
        ) "darwin-services: an agent must warn that it doesn't come back after a reboot";
        pkgs.runCommand "tinytrader-darwin-services-check" { } "touch $out";
    };
}
