# checks.<system>.watch: watch's alerting rules, against a console and a store
# served from files (curl reads file:// URLs), with no notify URL, so each
# notice is logged as "would notify". Run by `nix flake check`; WATCH is the
# watch program.
set -euo pipefail
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/console/api" "$work/store"
export WATCH_STATE="$work/state" WATCH_HOST=testhost WATCH_CONSOLE="file://$work/console"
unset WATCH_NOTIFY WATCH_NOTIFY_FILE WATCH_HEARTBEAT WATCH_HEARTBEAT_FILE

console() { # SERVICE STATE
  printf '{"services":{"%s":{"health":{"http_status":200,"body":{"state":"%s","detail":"d"}}}}}' "$1" "$2" > "$work/console/api/state"
}
run() { "$WATCH" --once 2> "$work/log" || true; cat "$work/log" >&2; }
says() { grep -qF -- "$1" "$work/log" || { echo "FAIL: expected: $1" >&2; exit 1; }; }
silent() { if grep -q "would notify" "$work/log"; then echo "FAIL: expected no notice ($1)" >&2; exit 1; fi; }

echo "== a failed service: told after two runs, once, then resolved"
console relay failed
run; silent "first sighting"
run; says "would notify: testhost: relay: failed (d)"
run; silent "not repeated before WATCH_REPEAT_HIGH"
WATCH_REPEAT_HIGH=0 run; says "(still)"
console relay ready
run; says "would notify: testhost: resolved (1)"
run; silent "resolved once"

echo "== a blip of one run says nothing"
console relay failed; run; console relay ready; run; silent "blip"

echo "== idle is fine; degraded only after a while"
console relay idle; run; run; silent "idle"
console relay degraded; run; run; silent "degraded under WATCH_DEGRADED_AFTER"
export WATCH_DEGRADED_AFTER=0
run; run; says "relay: degraded for 0 min"
unset WATCH_DEGRADED_AFTER
console relay ready; run; says "resolved"

echo "== the console not answering"
rm "$work/console/api/state"; run; run; says "console not answering"
console relay ready; run; says "resolved"

echo "== the store's /health"
printf '{"state":"failed","detail":"no volumes"}' > "$work/store/health"
WATCH_STORE="file://$work/store" run; WATCH_STORE="file://$work/store" run; says "store: failed (no volumes)"
run; says "resolved"

echo "== disks: a missing mount is high priority; a roomy one is fine"
WATCH_DISKS="/ $work/not-there" run; WATCH_DISKS="/ $work/not-there" run
says "not-there: missing"
WATCH_DISK_MIN_FREE_GIB=0 WATCH_DISKS="$work" run; WATCH_DISK_MIN_FREE_GIB=0 WATCH_DISKS="$work" run
says "not mounted (it is part of"

echo "== disk thresholds: a disk's own, else the host-wide ones"
# Host-wide: warn at $1 % used, never high priority.
disk() { WATCH_DISK_WARN="$1" WATCH_DISK_CRIT=100 WATCH_DISK_MIN_FREE_GIB=0 WATCH_DISKS="$2" run; }
disk 0 /; disk 0 /; says "would notify: testhost: /: "
disk 0 /:100:100:0; says "resolved"
disk 0 /:100:100:0; silent "its own thresholds over the host's"
disk 100 /:0::; disk 100 /:0::; says "would notify: testhost: /: "
disk 100 /::100:; says "resolved"
disk 100 /::100:; silent "an empty field takes the host-wide threshold"

echo "== a malformed WATCH_DISKS entry stops watch at start"
for bad in /data:90:95 /data:90:95:10:5 /data:9x:95:10 :90:95:10; do
  rc=0; WATCH_DISKS="/ $bad" "$WATCH" --once 2> "$work/log" || rc=$?
  [ "$rc" = 2 ] || { echo "FAIL: WATCH_DISKS=$bad: exit $rc, not 2" >&2; exit 1; }
  says "WATCH_DISKS entry not PATH or PATH:WARN:CRIT:MIN_FREE_GIB (whole numbers or empty): $bad"
done

echo "== a job's stamp: missing or stale is a warning; fresh is fine"
WATCH_FRESH="backup:3600:$work/stamp" run; WATCH_FRESH="backup:3600:$work/stamp" run
says "backup: never done"
touch -d "@$(( $(date +%s) - 7300 ))" "$work/stamp"
WATCH_FRESH="backup:3600:$work/stamp" run; says "backup: last done 2 h ago (limit 1 h)"
touch "$work/stamp"
WATCH_FRESH="backup:3600:$work/stamp" run; says "resolved"
touch -d "@$(( $(date +%s) - 600 ))" "$work/stamp"
WATCH_FRESH="link:180:$work/stamp" run; WATCH_FRESH="link:180:$work/stamp" run; says "link: last done 10 min ago (limit 3 min)"
touch "$work/stamp"
WATCH_FRESH="link:180:$work/stamp" run; says "resolved"
WATCH_FRESH="backup:3600" run; WATCH_FRESH="backup:3600" run; says "not NAME:SECONDS:PATH"
run

echo "== containers expected without a Docker socket is a problem"
WATCH_SERVICES="a,b" run; says "2 containers expected but no WATCH_DOCKER_SOCKET"

echo "== an order gateway's port is refused"
if WATCH_CONSOLE=http://127.0.0.1:7710 "$WATCH" --once 2> "$work/log"; then echo "FAIL: ran against 7710" >&2; exit 1; fi
says "refusing http://127.0.0.1:7710"
if WATCH_NEVER_PORTS="7700,7701" WATCH_STORE=http://127.0.0.1:7701 "$WATCH" --once 2> "$work/log"; then echo "FAIL: ran against 7701" >&2; exit 1; fi
says "port 7701"
echo "== native services: launchd daemons (system) and agents (gui/<uid>)"
# A fake launchctl: a job is a file $work/launchd/<domain>/<label> holding
# what `launchctl print` says of it.
uid="$(id -u)"
mkdir -p "$work/bin" "$work/launchd/system" "$work/launchd/gui/$uid"
# (#!$BASH: the build sandbox on Linux has no /usr/bin/env.)
cat > "$work/bin/launchctl" <<FAKE
#!$BASH
[ "\$1" = print ] && [ -f "$work/launchd/\$2" ] && exec cat "$work/launchd/\$2"
echo "Could not find service \$2" >&2; exit 113
FAKE
chmod +x "$work/bin/launchctl"
job() { printf '%s = {\n\tactive count = 1\n\tstate = %s\n\tpid = 1\n}\n' "${1##*/}" "$2" > "$work/launchd/$1"; }
job system/com.tinytrader.watchd running
job "gui/$uid/com.tinytrader.agent" running
native() { PATH="$work/bin:$PATH" WATCH_NATIVE="watchd agent $*" run; }
native; native; silent "a running daemon and a running agent"
native gone; native gone; says "gone: not loaded (launchd com.tinytrader.gone, neither a daemon nor an agent)"
native; says "resolved"
job system/com.tinytrader.watchd "not running"
native; native; says "watchd: not running (launchd system/com.tinytrader.watchd)"
job system/com.tinytrader.watchd running
job "gui/$uid/com.tinytrader.agent" exited
native; native; says "agent: exited (launchd gui/$uid/com.tinytrader.agent)"
job "gui/$uid/com.tinytrader.agent" running
native; says "resolved"

echo "== native services: systemd where there is no launchd"
if ! command -v launchctl >/dev/null 2>&1; then
  rm "$work/bin/launchctl"
  printf '#!%s\n[ "$2" = tinytrader-up.service ] && { echo active; exit 0; }\necho inactive; exit 3\n' "$BASH" > "$work/bin/systemctl"
  chmod +x "$work/bin/systemctl"
  PATH="$work/bin:$PATH" WATCH_NATIVE="up down" run; PATH="$work/bin:$PATH" WATCH_NATIVE="up down" run
  says "down: inactive (systemd tinytrader-down)"
  if grep -qF "up: " "$work/log"; then echo "FAIL: an active unit was reported" >&2; exit 1; fi
fi

echo "watch: all checks passed"
