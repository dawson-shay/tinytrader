# watch: a host's own monitor (packages.<system>.watch; configured by the
# environment, documented in nix/watch.nix). Every WATCH_INTERVAL seconds, or
# once with --once, it checks this host and pushes what is wrong to an
# ntfy-style URL, then pings a heartbeat URL (healthchecks.io), so a host that
# stops altogether is noticed too.
#
# It talks only to the Docker socket, the console, the store, the notify URL
# and the heartbeat URL. It never connects to an order gateway: gateways are
# checked through their containers' state and restart counts only.

usage() {
  echo "usage: watch [--once]   (configured by WATCH_* environment variables; see tinytrader's nix/watch.nix)" >&2
  exit 2
}

once=0
for a in "$@"; do
  case "$a" in
    --once) once=1 ;;
    *) usage ;;
  esac
done

log() { printf 'watch: %s\n' "$*" >&2; }

# Words of a list given as spaces, commas, semicolons or lines; # starts a comment.
words() { sed 's/#.*//' | tr ',;\t' '   ' | tr -s ' ' '\n' | sed '/^$/d'; }

# A secret: NAME, or the contents of the file NAME_FILE names (the platform's
# convention); empty when neither is set or the file is missing.
secret() {
  local v="${!1:-}" f="${1}_FILE"
  if [ -z "$v" ] && [ -n "${!f:-}" ]; then
    if [ -r "${!f}" ]; then v="$(tr -d '\n' < "${!f}")"; else log "$f names no readable file"; fi
  fi
  printf '%s' "$v"
}

interval="${WATCH_INTERVAL:-60}"
host="${WATCH_HOST:-$(uname -n | cut -d. -f1)}"
state_dir="${WATCH_STATE:-${STATE_DIRECTORY:-${XDG_STATE_HOME:-${HOME:-/tmp}/.local/state}/tinytrader-watch}}"
state_file="$state_dir/state.json"
sock="${WATCH_DOCKER_SOCKET:-}"
project="${WATCH_COMPOSE_PROJECT:-}"
console="${WATCH_CONSOLE:-}"
store="${WATCH_STORE:-}"
disk_warn="${WATCH_DISK_WARN:-90}"
disk_crit="${WATCH_DISK_CRIT:-95}"
disk_min_gib="${WATCH_DISK_MIN_FREE_GIB:-10}"
degraded_after="${WATCH_DEGRADED_AFTER:-180}"
restart_hold="${WATCH_RESTART_HOLD:-600}"
repeat_high="${WATCH_REPEAT_HIGH:-1800}"
repeat_warn="${WATCH_REPEAT_WARN:-21600}"
# Ports never to connect to (the order gateways'); a URL above that names one
# is refused at start.
never_ports="$(words <<<"${WATCH_NEVER_PORTS:-7710}")"

expected="$(words <<<"${WATCH_SERVICES:-}")"
if [ -n "${WATCH_SERVICES_FROM:-}" ]; then
  if [ -r "$WATCH_SERVICES_FROM" ]; then
    expected="$(printf '%s\n%s\n' "$expected" "$(words < "$WATCH_SERVICES_FROM")" | sed '/^$/d' | sort -u)"
  else
    log "WATCH_SERVICES_FROM names no readable file: $WATCH_SERVICES_FROM"
    exit 2
  fi
fi
native="$(words <<<"${WATCH_NATIVE:-}")"
# Files that must have changed recently: NAME:MAX_AGE_SECONDS:PATH each.
fresh="$(words <<<"${WATCH_FRESH:-}")"

for url in "$console" "$store"; do
  [ -n "$url" ] || continue
  port="$(sed -nE 's#^[a-z]+://[^/:]+:([0-9]+).*#\1#p' <<<"$url")"
  for p in $never_ports; do
    if [ "$port" = "$p" ]; then
      log "refusing $url: port $p is an order gateway's (WATCH_NEVER_PORTS)"
      exit 2
    fi
  done
done
for n in "$interval" "$disk_warn" "$disk_crit" "$disk_min_gib" "$degraded_after" "$restart_hold" "$repeat_high" "$repeat_warn"; do
  [[ "$n" =~ ^[0-9]+$ ]] || { log "not a whole number: $n"; exit 2; }
done
# Disks: PATH, or PATH:WARN:CRIT:MIN_FREE_GIB for thresholds of its own (an
# empty field takes the host-wide one); kept as PATH<TAB>WARN<TAB>CRIT<TAB>MIN.
disks=""
while read -r e; do
  [ -n "$e" ] || continue
  if ! [[ "$e" =~ ^([^:]+)(:([0-9]*):([0-9]*):([0-9]*))?$ ]]; then
    log "WATCH_DISKS entry not PATH or PATH:WARN:CRIT:MIN_FREE_GIB (whole numbers or empty): $e"
    exit 2
  fi
  disks+="${BASH_REMATCH[1]}"$'\t'"${BASH_REMATCH[3]:-$disk_warn}"$'\t'"${BASH_REMATCH[4]:-$disk_crit}"$'\t'"${BASH_REMATCH[5]:-$disk_min_gib}"$'\n'
done < <(words <<<"${WATCH_DISKS:-}")
mkdir -p "$state_dir"

# One run's findings: key<TAB>severity (warn|high)<TAB>message, one per line.
problems=""
problem() { problems+="$1"$'\t'"$2"$'\t'"$(printf '%s' "$3" | tr '\t\n' '  ')"$'\n'; }

dget() { curl -fsS -m 10 --unix-socket "$sock" "http://docker$1"; }

check_docker() {
  local ping list filter s c id st status inspect restarts health prev k sev msg now="$1"
  if [ -z "$sock" ]; then
    [ -z "$expected" ] || problem config high "$(grep -c . <<<"$expected") containers expected but no WATCH_DOCKER_SOCKET"
    return
  fi
  if ! ping="$(curl -fsS -m 5 --unix-socket "$sock" http://docker/_ping 2>/dev/null)" || [ "$ping" != OK ]; then
    problem docker high "Docker engine not answering on $sock${ping:+: ${ping:0:80}}"
    # Its containers can't be known: what was wrong with them stays so
    # (rather than reading as resolved) until the engine answers again.
    while IFS=$'\t' read -r k sev msg; do
      [ -n "$k" ] && problem "$k" "$sev" "$msg"
    done < <(jq -r '.problems // {} | to_entries[] | select(.key | test("^(svc|svc-health|restart):"))
                    | [.key, .value.sev, .value.msg] | @tsv' <<<"$state")
    return
  fi
  [ -n "$expected" ] || return 0
  filter='{}'
  [ -z "$project" ] || filter="$(jq -cn --arg p "com.docker.compose.project=$project" '{label: [$p]}')"
  if ! list="$(dget "/containers/json?all=1&filters=$(jq -rn --arg f "$filter" '$f | @uri')")"; then
    problem docker high "Docker engine answers but won't list containers"
    return
  fi
  while read -r s; do
    [ -n "$s" ] || continue
    c="$(jq -c --arg s "$s" '[.[] | select(.Labels["com.docker.compose.service"] == $s)] | sort_by(.State != "running") | .[0] // empty' <<<"$list")"
    if [ -z "$c" ]; then
      problem "svc:$s" high "$s: no container"
      continue
    fi
    id="$(jq -r '.Id' <<<"$c")"
    st="$(jq -r '.State' <<<"$c")"
    status="$(jq -r '.Status' <<<"$c")"
    [ "$st" = running ] || problem "svc:$s" high "$s: container $st ($status)"
    inspect="$(dget "/containers/$id/json" 2>/dev/null || echo '{}')"
    restarts="$(jq -r '.RestartCount // empty' <<<"$inspect")"
    health="$(jq -r '.State.Health.Status // empty' <<<"$inspect")"
    [ "$health" != unhealthy ] || problem "svc-health:$s" high "$s: its Docker healthcheck fails"
    [ -n "$restarts" ] || continue
    # Restarts since the last run, for the same container (a recreate starts over).
    prev="$(jq -c --arg s "$s" '.containers[$s] // {}' <<<"$state")"
    containers="$(jq -c --arg s "$s" --arg id "$id" --argjson n "$restarts" --argjson now "$now" --argjson p "$prev" '
      . + { ($s): { id: $id, restarts: $n,
                    rose_at: (if $p.id == $id and $n > ($p.restarts // $n) then $now else $p.rose_at end) } }' <<<"$containers")"
    if jq -e --arg s "$s" --argjson now "$now" --argjson hold "$restart_hold" \
      '.[$s].rose_at != null and ($now - .[$s].rose_at) < $hold' <<<"$containers" >/dev/null; then
      problem "restart:$s" warn "$s restarted (restart count $restarts)"
    fi
  done <<<"$expected"
}

# A service's health state (degraded and recovering count after a while).
judge_health() { # ID STATE DETAIL NOW
  local key="health:$1"
  case "$2" in
    failed) problem "$key" high "$1: failed${3:+ ($3)}" ;;
    degraded | recovering)
      degraded_keys+="$key"$'\n'
      degraded="$(jq -c --arg k "$key" --argjson now "$4" '.[$k] //= $now' <<<"$degraded")"
      if jq -e --arg k "$key" --argjson now "$4" --argjson after "$degraded_after" '($now - .[$k]) >= $after' <<<"$degraded" >/dev/null; then
        problem "$key" warn "$1: $2 for $(( ($4 - $(jq -r --arg k "$key" '.[$k]' <<<"$degraded")) / 60 )) min${3:+ ($3)}"
      fi
      ;;
  esac
}

check_console() {
  local body now="$1"
  [ -n "$console" ] || return 0
  if ! body="$(curl -fsS -m 10 "$console/api/state" 2>/dev/null)" || ! jq -e '.services | type == "object"' <<<"$body" >/dev/null 2>&1; then
    problem console high "console not answering at $console"
    return
  fi
  while IFS=$'\t' read -r id st detail; do
    [ -n "$id" ] && judge_health "$id" "$st" "$detail" "$now"
  done < <(jq -r '.services | to_entries[] | select(.value.health.body.state != null)
                  | [.key, .value.health.body.state, (.value.health.body.detail // "")] | @tsv' <<<"$body")
}

check_store() {
  local body st now="$1"
  [ -n "$store" ] || return 0
  # /health answers 503 while not ready, with the state in its body.
  if ! body="$(curl -sS -m 10 "$store/health" 2>/dev/null)" || ! st="$(jq -er '.state' <<<"$body" 2>/dev/null)"; then
    problem health:store high "store not answering at $store"
    return
  fi
  judge_health store "$st" "$(jq -r '.detail // ""' <<<"$body")" "$now"
}

# Native services: launchd jobs com.tinytrader.<name> where there is launchd
# (macOS), else systemd units tinytrader-<name>. A launchd service is a daemon
# (the system domain) or an agent of this user (gui/<uid>), so both are
# looked in; reading either needs no root.
check_native() {
  local n out st d where
  while read -r n; do
    [ -n "$n" ] || continue
    if command -v launchctl >/dev/null 2>&1; then
      where=""
      for d in system "gui/$(id -u)"; do
        if out="$(launchctl print "$d/com.tinytrader.$n" 2>/dev/null)"; then
          where="$d"
          break
        fi
      done
      if [ -z "$where" ]; then
        problem "native:$n" high "$n: not loaded (launchd com.tinytrader.$n, neither a daemon nor an agent)"
        continue
      fi
      st="$(sed -nE 's/^[[:space:]]*state = (.*)$/\1/p' <<<"$out" | head -n1)"
      [ "$st" = running ] || problem "native:$n" high "$n: ${st:-not running} (launchd $where/com.tinytrader.$n)"
    else
      st="$(systemctl is-active "tinytrader-$n.service" 2>/dev/null || true)"
      [ "$st" = active ] || problem "native:$n" high "$n: ${st:-unknown} (systemd tinytrader-$n)"
    fi
  done <<<"$native"
}

check_disks() {
  local m warn crit min_gib out size avail target pct free
  while IFS=$'\t' read -r m warn crit min_gib; do
    [ -n "$m" ] || continue
    if ! out="$(df -B1 --output=size,avail,target -- "$m" 2>/dev/null | tail -n1)"; then
      problem "disk:$m" high "$m: missing (not mounted?)"
      continue
    fi
    read -r size avail target <<<"$out"
    if [ "$target" != "$m" ]; then
      problem "disk:$m" high "$m: not mounted (it is part of $target)"
      continue
    fi
    [ "${size:-0}" -gt 0 ] || continue
    # Used against the whole size: on APFS, volumes share their container's space.
    pct=$(( (size - avail) * 100 / size ))
    free=$(( avail / 1073741824 ))
    if [ "$pct" -ge "$crit" ] || [ "$avail" -lt $(( min_gib * 1073741824 )) ]; then
      problem "disk:$m" high "$m: $pct% full, $free GiB free"
    elif [ "$pct" -ge "$warn" ]; then
      problem "disk:$m" warn "$m: $pct% full, $free GiB free"
    fi
  done <<<"$disks"
}

# Each WATCH_FRESH file: missing, or not modified for its max age, is a
# warning (a nightly job that didn't run, or failed before its stamp).
check_fresh() {
  local e name max path mtime age now="$1"
  while read -r e; do
    [ -n "$e" ] || continue
    name="${e%%:*}"; e="${e#*:}"; max="${e%%:*}"; path="${e#*:}"
    if [ -z "$name" ] || ! [[ "$max" =~ ^[0-9]+$ ]] || [ -z "$path" ] || [ "$path" = "$max" ]; then
      problem "fresh-config" high "WATCH_FRESH entry not NAME:SECONDS:PATH: $name:$e"
      continue
    fi
    if ! mtime="$(stat -c %Y -- "$path" 2>/dev/null)"; then
      problem "fresh:$name" warn "$name: never done ($path is missing)"
      continue
    fi
    age=$(( now - mtime ))
    # In minutes for a stamp kept every few minutes (a link check), else hours.
    if [ "$max" -lt 3600 ]; then
      [ "$age" -le "$max" ] || problem "fresh:$name" warn "$name: last done $(( age / 60 )) min ago (limit $(( max / 60 )) min)"
    else
      [ "$age" -le "$max" ] || problem "fresh:$name" warn "$name: last done $(( age / 3600 )) h ago (limit $(( max / 3600 )) h)"
    fi
  done <<<"$fresh"
}

# POST a notice: TITLE PRIORITY TAGS BODY. Fails when it isn't delivered.
notify() {
  local url
  url="$(secret WATCH_NOTIFY)"
  if [ -z "$url" ]; then
    log "no WATCH_NOTIFY(_FILE): would notify: $1: ${4//$'\n'/; }"
    return 0
  fi
  if ! curl -fsS -m 15 --retry 2 -o /dev/null -H "Title: $1" -H "Priority: $2" -H "Tags: $3" \
    --data-binary "$4" "$url" 2>/dev/null; then
    log "could not deliver a notice ($1); retrying next run"
    return 1
  fi
}

run_once() {
  local now plan alerts resolved n title prio body sent_alerts=true sent_resolved=true hb summary
  now="$(date +%s)"
  problems=""
  state="$(cat "$state_file" 2>/dev/null || true)"
  jq -e 'type == "object"' <<<"$state" >/dev/null 2>&1 || state='{}'
  containers="$(jq -c '.containers // {}' <<<"$state")"
  # When each service still degraded became so (judge_health).
  degraded="$(jq -c '.degraded // {}' <<<"$state")"
  degraded_keys=""

  check_docker "$now"
  check_console "$now"
  check_store "$now"
  check_native
  check_disks
  check_fresh "$now"

  # Forget when services no longer degraded became so.
  degraded="$(jq -c --arg keys "$degraded_keys" '($keys | split("\n")) as $k | with_entries(select(.key as $x | $k | index($x)))' <<<"$degraded")"

  plan="$(jq -c --arg cur "$problems" --argjson pending "$(jq -c '.pending_resolved // []' <<<"$state")" --argjson now "$now" --argjson rh "$repeat_high" --argjson rw "$repeat_warn" '
    def rep(s): if s == "high" then $rh else $rw end;
    (.problems // {}) as $p
    | ($cur | split("\n") | map(select(length > 0) | split("\t") | {key: .[0], sev: .[1], msg: .[2]})
       | map({key, value: .}) | from_entries) as $c
    | { problems: ($c | with_entries(.key as $k | .value as $n | ($p[$k] // {}) as $o | .value = {
          sev: $n.sev, msg: $n.msg, seen: (($o.seen // 0) + 1), since: ($o.since // $now),
          alerted_at: ($o.alerted_at // null), alerted_sev: ($o.alerted_sev // null) })) }
    | .alerts = [ .problems | to_entries[] | select(.value.seen >= 2)
        | select(.value.alerted_at == null
                 or (.value.alerted_sev == "warn" and .value.sev == "high")
                 or ($now - .value.alerted_at) >= rep(.value.sev))
        | {key, sev: .value.sev, msg: .value.msg, again: (.value.alerted_at != null)} ]
    # Cleared since the last run, having been told; and resolved notices that
    # could not be delivered before (unless the problem is back).
    | .resolved = ([ $p | to_entries[] | select($c[.key] == null and .value.alerted_at != null)
                     | {key, msg: .value.msg} ] + [ $pending[] | select($c[.key] == null) ]
                   | unique_by(.key)) ' <<<"$state")"

  alerts="$(jq -r '.alerts | length' <<<"$plan")"
  if [ "$alerts" -gt 0 ]; then
    prio="$(jq -r 'if any(.alerts[]; .sev == "high") then "high" else "default" end' <<<"$plan")"
    if [ "$alerts" -eq 1 ]; then
      title="$host: $(jq -r '.alerts[0].msg' <<<"$plan")"
    else
      title="$host: $alerts problems"
    fi
    body="$(jq -r '.alerts[] | "- \(.msg)\(if .again then " (still)" else "" end)"' <<<"$plan")"
    notify "$title" "$prio" "$([ "$prio" = high ] && echo rotating_light || echo warning)" "$body" || sent_alerts=false
  fi
  resolved="$(jq -r '.resolved | length' <<<"$plan")"
  if [ "$resolved" -gt 0 ]; then
    body="$(jq -r '.resolved[] | "- \(.msg)"' <<<"$plan")"
    notify "$host: resolved ($resolved)" low white_check_mark "$body" || sent_resolved=false
  fi

  # Record what was said, so it isn't said again before its repeat.
  jq -c --argjson plan "$plan" --argjson now "$now" --argjson sa "$sent_alerts" --argjson sr "$sent_resolved" \
    --argjson containers "$containers" --argjson degraded "$degraded" '
    { containers: $containers, degraded: $degraded,
      problems: ($plan.problems | with_entries(.key as $k
        | if $sa and any($plan.alerts[]; .key == $k) then .value.alerted_at = $now | .value.alerted_sev = .value.sev else . end)),
      pending_resolved: (if $sr then [] else $plan.resolved[-50:] end) }' <<<"{}" > "$state_file.tmp"
  mv "$state_file.tmp" "$state_file"

  n="$(printf '%s' "$problems" | grep -c . || true)"
  summary="$host: $n problem(s)$( [ "$n" -eq 0 ] || printf ': %s' "$(cut -f3 <<<"$problems" | sed '/^$/d' | paste -sd ';' -)")"
  log "$summary"

  # The heartbeat: this run happened. /fail when a notice couldn't be
  # delivered, so the heartbeat service's own alerts carry it.
  hb="$(secret WATCH_HEARTBEAT)"
  if [ -n "$hb" ]; then
    if [ "$sent_alerts" = false ] || [ "$sent_resolved" = false ]; then hb="${hb%/}/fail"; fi
    curl -fsS -m 10 --retry 2 -o /dev/null --data-raw "$summary" "$hb" 2>/dev/null || log "heartbeat ping failed"
  fi
  [ "$n" -eq 0 ]
}

while :; do
  if [ "$once" = 1 ]; then
    run_once
    exit $?
  fi
  run_once || true
  sleep "$interval"
done
