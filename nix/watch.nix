# packages.<system>.watch: a host's own monitor, which pushes what is wrong to
# the operator's phone (an ntfy-style URL) and pings a heartbeat URL
# (healthchecks.io) after every run, so a host that stops altogether is
# noticed by the heartbeat service instead. The script is nix/watch.sh.
#
#   watch           check every WATCH_INTERVAL seconds (default 60)
#   watch --once    check once; exit 1 if anything is wrong
#
# What it checks, each only when configured:
#   WATCH_DOCKER_SOCKET   the Docker engine (a ping, 5 s timeout) at this
#                         Unix socket, and then the containers below
#   WATCH_SERVICES        Compose services expected on this host (spaces,
#                         commas or lines), and/or WATCH_SERVICES_FROM, a file
#                         of them: each must have a running container whose
#                         restart count hasn't risen (a rise is a warning for
#                         WATCH_RESTART_HOLD seconds, default 600)
#   WATCH_COMPOSE_PROJECT only containers of this Compose project
#   WATCH_CONSOLE         the console (e.g. http://127.0.0.1:8080): its
#                         /api/state must answer; a service whose health is
#                         `failed` is a problem, one `degraded` or
#                         `recovering` for WATCH_DEGRADED_AFTER seconds
#                         (default 180) too; `idle` is fine
#   WATCH_STORE           the store's /health, judged the same way
#   WATCH_NATIVE          native tinytrader services (tinytrader.services
#                         names): launchd com.tinytrader.<name> must be
#                         running (macOS; found as a daemon, in the system
#                         domain, or as an agent, in gui/<uid>), systemd
#                         tinytrader-<name> active (elsewhere)
#   WATCH_DISKS           mount points: WATCH_DISK_WARN % used (default 90)
#                         is a warning, WATCH_DISK_CRIT % (95) or under
#                         WATCH_DISK_MIN_FREE_GIB free (10) high priority; a
#                         path that isn't its own mount (an unplugged drive)
#                         is high priority too. An entry
#                         PATH:WARN:CRIT:MIN_FREE_GIB (e.g.
#                         /Volumes/archive:90:95:10) has thresholds of its
#                         own; an empty field (/data::98:) takes the
#                         host-wide one. A malformed entry stops watch at start
#   WATCH_FRESH           files a scheduled job touches when it succeeds, as
#                         NAME:MAX_AGE_SECONDS:PATH (e.g.
#                         backup:108000:/var/lib/backup/last-backup): one
#                         missing or older than its max age is a warning
#                         (its age in minutes when the max is under an hour)
#
# Alerting: a problem must be seen two runs in a row before it is sent; it is
# sent again every WATCH_REPEAT_HIGH seconds (default 1800) while high
# priority, every WATCH_REPEAT_WARN (21600) as a warning, and a "resolved"
# notice follows when it clears. Notices that can't be delivered are retried
# next run, and that run's heartbeat goes to <url>/fail, so the heartbeat
# service's own integrations (email) carry it.
#   WATCH_NOTIFY(_FILE)     the ntfy-style URL (POST: body, Title, Priority,
#                           Tags); unset: notices are logged and skipped
#   WATCH_HEARTBEAT(_FILE)  the URL to ping after each run; unset: no ping
# Secrets follow the platform's convention: NAME, or the file NAME_FILE names
# (read at each run).
#
# Other settings: WATCH_HOST (the name in notices; default the hostname),
# WATCH_STATE (default $STATE_DIRECTORY, else
# $XDG_STATE_HOME/tinytrader-watch): a small state file.
#
# It connects only to the Docker socket, the console, the store and the two
# URLs above, never to an order gateway: gateways are judged by their
# containers' state and restarts alone. A console or store URL on one of
# WATCH_NEVER_PORTS (default 7710, the gateway's default port; list your
# gateways' ports) is refused at start.
#
# Run it as a service like any other: tinytrader.services.watch = { enable =
# true; package = tinytrader.packages.<system>.watch; environment = { ... };
# secrets.WATCH_NOTIFY = ...; } (secrets on NixOS; on macOS, put them in the
# environment from a wrapper, as darwin-services has no secrets; and give it
# launchd = "daemon" there so it starts at boot, without a login).
{ ... }:
{
  perSystem =
    { pkgs, config, ... }:
    {
      # Its alerting rules, against a console and store served from files.
      checks.watch = pkgs.runCommand "watch-test" { WATCH = pkgs.lib.getExe config.packages.watch; } ''
        bash ${./watch-test.sh}
        touch $out
      '';

      packages.watch = pkgs.writeShellApplication {
        name = "watch";
        runtimeInputs = with pkgs; [
          curl
          jq
          coreutils
          gnused
          gnugrep
        ];
        text = builtins.readFile ./watch.sh;
        meta.description = "A host's monitor: Docker, containers, health, native services, disks and job freshness, pushed to ntfy";
      };
    };
}
