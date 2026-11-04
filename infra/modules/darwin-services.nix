# tinytrader.services.<name> on macOS (nix-darwin): the same definitions as
# on NixOS (service-options.nix), run as the host's primary user, so a service
# sees the drives that user attaches.
#
# Each service is com.tinytrader.<name>, started once the Nix store is
# mounted (/bin/wait4path: at boot it may not be yet, and launchd never
# retries a job that failed to exec), restarted when it exits (at most
# every 10 s), logging to ~/Library/Logs/tinytrader/<name>.log. The system's
# newsyslog rotates those logs past 10 MB, keeping 7. Its `launchd` option
# says how it starts:
#
#   "agent" (the default)  a launchd agent (launchd.user.agents, in
#                          ~/Library/LaunchAgents): started when the user
#                          logs in, in the gui/<uid> domain
#   "daemon"               a launchd daemon (launchd.daemons, in
#                          /Library/LaunchDaemons) with UserName set to the
#                          primary user: started at boot without anyone
#                          logging in, in the system domain. With FileVault
#                          on, boot still waits for the disk to be unlocked
#                          at the login window (no full login needed after).
#
# A daemon runs as the user but outside their login session: it gets HOME
# and USER (set here), not the user's keychain or GUI. Switching a service
# from agent to daemon, activation boots out the still-running agent first
# (nix-darwin removes the agent's plist but can't unload it from the user's
# session), so the two never run at once.
#
# launchd has no sandbox or memory limits like systemd's: `writablePaths`
# and `resources` other than `openFiles` apply on Linux only.
{ config, lib, ... }:
let
  enabled = lib.filterAttrs (_: s: s.enable) config.tinytrader.services;
  agents = lib.filterAttrs (_: s: s.launchd == "agent") enabled;
  daemons = lib.filterAttrs (_: s: s.launchd == "daemon") enabled;
  user = config.system.primaryUser;
  home = "/Users/${user}";
  logs = "${home}/Library/Logs/tinytrader";
  label = name: "com.tinytrader.${name}";
  inherit (import ../lib/launchd.nix) waitForNix;

  # What an agent and a daemon share.
  serviceConfig = name: svc: {
    Label = label name;
    # Once the Nix store is mounted: at boot it may not be yet (infra/lib/launchd.nix).
    ProgramArguments = waitForNix ([ (lib.getExe svc.package) ] ++ svc.args);
    EnvironmentVariables =
      lib.optionalAttrs (svc.observe.port != null) {
        TINYTRADER_LISTEN = "${svc.observe.bind}:${toString svc.observe.port}";
      }
      // svc.environment;
    RunAtLoad = true;
    KeepAlive = true;
    ThrottleInterval = 10;
    SoftResourceLimits.NumberOfFiles = svc.resources.openFiles;
    StandardOutPath = "${logs}/${name}.log";
    StandardErrorPath = "${logs}/${name}.log";
  };

  # Labels of launchd jobs defined elsewhere in the configuration.
  labelsOf =
    jobs: lib.filter (l: l != null) (lib.mapAttrsToList (_: j: j.serviceConfig.Label or null) jobs);
  q = lib.escapeShellArg;
in
{
  imports = [ ./service-options.nix ];

  config = lib.mkIf (enabled != { }) {
    launchd.user.agents = lib.mapAttrs' (
      name: svc: lib.nameValuePair "tinytrader-${name}" { serviceConfig = serviceConfig name svc; }
    ) agents;

    launchd.daemons = lib.mapAttrs' (
      name: svc:
      let
        base = serviceConfig name svc;
      in
      lib.nameValuePair "tinytrader-${name}" {
        serviceConfig = base // {
          UserName = user;
          GroupName = "staff";
          # launchd gives a daemon neither; the service's own settings win.
          EnvironmentVariables = {
            HOME = home;
            USER = user;
          }
          // base.EnvironmentVariables;
        };
      }
    ) daemons;

    # One label, one job: a service's label must not also be defined as the
    # other kind of job elsewhere in the configuration.
    assertions =
      lib.mapAttrsToList (name: _: {
        assertion = lib.count (l: l == label name) (labelsOf config.launchd.user.agents) == 0;
        message = "tinytrader.services.${name} runs as a launchd daemon, but a launchd agent (launchd.user.agents) is also labelled ${label name}.";
      }) daemons
      ++ lib.mapAttrsToList (name: _: {
        assertion = lib.count (l: l == label name) (labelsOf config.launchd.daemons) == 0;
        message = "tinytrader.services.${name} runs as a launchd agent, but a launchd daemon (launchd.daemons) is also labelled ${label name}; set its launchd = \"daemon\" instead.";
      }) agents;

    # Before the launchd services are loaded: launchd opens the log files but
    # doesn't create their directory (and a daemon's would be root's), and a
    # service now a daemon may still be running as the agent it was.
    system.activationScripts.preActivation.text = ''
      mkdir -p ${q logs}
      chown ${q user}:staff ${q logs}
      ${lib.concatMapStrings (name: ''
        touch ${q "${logs}/${name}.log"}
        chown ${q user}:staff ${q "${logs}/${name}.log"}
      '') (lib.attrNames enabled)}
      ${lib.concatMapStrings (name: ''
        if launchctl bootout "gui/$(id -u -- ${q user})/${label name}" 2>/dev/null; then
          echo "tinytrader: stopped the ${label name} agent; it runs as a daemon now" >&2
        fi
      '') (lib.attrNames daemons)}
    '';

    environment.etc."newsyslog.d/tinytrader.conf".text = ''
      # logfile                     owner:group  mode count size(KB) when flags
      ${logs}/*.log  ${user}:staff  644  7     10240    *    GJN
    '';
  };
}
