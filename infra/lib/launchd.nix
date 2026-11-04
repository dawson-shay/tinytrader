# Starting a launchd job only once the Nix store is there.
#
# At boot launchd starts daemons, and agents at login, as soon as it can: on
# macOS the Nix store is a separate volume, mounted a little later. A job
# whose program is in /nix/store then fails to exec, launchd marks it
# EX_CONFIG (78), puts it in the penalty box and never retries it, KeepAlive
# or not. That is how a power cut on 2026-11-01 left a relay, databento-jobs
# and Nomad down until someone noticed.
#
# nix-darwin's own `command` and `script` options wrap the program the same
# way; a job that sets `serviceConfig.ProgramArguments` itself must use this.
{
  # The job's argv, run once /nix/store exists: `/bin/wait4path` waits for the
  # path, then the shell execs the program, so it is the job's process (its
  # signals, its exit code, KeepAlive's restarts). The program and its
  # arguments are passed to the shell as positional parameters ("$@"), never
  # spliced into the script, so nothing in them needs quoting.
  waitForNix =
    argv:
    [
      "/bin/sh"
      "-c"
      "/bin/wait4path /nix/store && exec \"$@\""
      "wait4path"
    ]
    ++ argv;
}
