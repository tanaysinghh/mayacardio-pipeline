#!/bin/bash
set -uo pipefail
export MSYS_NO_PATHCONV=1

usage() {
  cat <<'EOF'
usage: scripts/ci-arm64-evidence.sh --out DIR [--minutes N] [--scenarios all|none|LIST] [--wake-after-idle-minutes N]
                                    [--image NAME] [--cleanup-only]

Produces the ARM64 evidence on a Linux host with Docker (run by .github/workflows/arm64-evidence.yml on the
GitHub Actions ubuntu-24.04-arm runner). Every test container is limited to 4 CPUs and 2 GB. The device reaches
the cloud named by CLOUD_URL directly, with the token from CLOUD_API_TOKEN; a network cut disconnects the test
container from its Docker network and a restore connects it again. Without CLOUD_URL a local cloud container
on the same network is used instead.

  --minutes N          length of the online resource run (default 60); the mock runs with the exact command
                       biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100
  --scenarios LIST     all (default), none, or a comma list of: core, backlog, diskfull, clock, or single
                       core scenarios; core = baseline, kill9, netcut, mockfaults, powercut (simulated by SIGKILL)
  --wake-after-idle-minutes N
                       at the end, leave the cloud idle for N minutes and time its wake-up (default 0 = skip)
  --image NAME         device image built from docker/arm64/Dockerfile (default pipeline-device:ci)
  --cleanup-only       only remove this script's containers and network and delete the recorded test sessions

Compact results go to DIR/arm64-ci/ and DIR/resources/; log tails go to DIR/logs/ (not for the repo).
The test sessions are deleted from the cloud at the end with DELETE /api/v1/sessions/{id}.
EOF
}

OUT=""
MINUTES=60
SCENARIOS=all
IDLE=0
IMAGE=pipeline-device:ci
CLEANUP_ONLY=0
LABEL="GitHub Actions ubuntu-24.04-arm native ARM64 runner, limited to 4 CPUs / 2 GB; not a Raspberry Pi 4; power cuts simulated by SIGKILL"
[ -n "${GITHUB_ACTIONS:-}" ] || LABEL="local run on $(uname -m), not the GitHub Actions runner, limited to 4 CPUs / 2 GB per container; not a Raspberry Pi 4; power cuts simulated by SIGKILL"
while [ $# -gt 0 ]; do
  case "$1" in
    --out) OUT="$2"; shift 2 ;;
    --minutes) MINUTES="$2"; shift 2 ;;
    --scenarios) SCENARIOS="$2"; shift 2 ;;
    --wake-after-idle-minutes) IDLE="$2"; shift 2 ;;
    --image) IMAGE="$2"; shift 2 ;;
    --cleanup-only) CLEANUP_ONLY=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option $1"; usage; exit 2 ;;
  esac
done
[ -n "$OUT" ] || { usage; exit 2; }
case "$MINUTES" in ''|*[!0-9]*) echo "--minutes must be a whole number"; exit 2 ;; esac
case "$IDLE" in ''|*[!0-9]*) echo "--wake-after-idle-minutes must be a whole number"; exit 2 ;; esac
[ "$MINUTES" -ge 1 ] || { echo "--minutes must be at least 1"; exit 2; }

mkdir -p "$OUT/arm64-ci" "$OUT/resources" "$OUT/logs"
OUT="$(cd "$OUT" && pwd)"
WORK="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/mc-ci-work"
CTL="$WORK/ctl"
mkdir -p "$CTL"
NET=mc-ci-net
PFX=mc-ci
SESSIONS="$OUT/logs/sessions-created.txt"
FAILED=""

host_path() {
  if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else echo "$1"; fi
}

uptime_s() {
  awk '{print $1}' /proc/uptime
}

LOCAL_CLOUD=0
if [ -z "${CLOUD_URL:-}" ]; then
  LOCAL_CLOUD=1
  CLOUD_URL="http://$PFX-cloud:8080"
  export CLOUD_API_TOKEN="${CLOUD_API_TOKEN:-ci-local-token}"
fi
CLOUD_URL="${CLOUD_URL%/}"
export CLOUD_API_TOKEN="${CLOUD_API_TOKEN:-}"
[ -n "$CLOUD_API_TOKEN" ] || { echo "CLOUD_API_TOKEN is empty"; exit 2; }
AUTH_HDR="$WORK/auth.hdr"
(umask 077 && printf 'Authorization: Bearer %s\n' "$CLOUD_API_TOKEN" > "$AUTH_HDR")
HOST_CLOUD_URL="$CLOUD_URL"
[ "$LOCAL_CLOUD" = 1 ] && HOST_CLOUD_URL="http://127.0.0.1:18089"

delete_sessions() {
  local deleted=0 failed=0 total=0 code
  if [ ! -s "$SESSIONS" ] && [ -f "$OUT/arm64-ci/cloud-cleanup.json" ]; then
    return 0
  fi
  if [ -s "$SESSIONS" ]; then
    for id in $(sort -u "$SESSIONS"); do
      total=$((total + 1))
      code=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE -H @"$AUTH_HDR" --max-time 120 "$HOST_CLOUD_URL/api/v1/sessions/$id" || true)
      if [ "$code" = 200 ]; then deleted=$((deleted + 1)); else failed=$((failed + 1)); echo "delete $id -> http $code"; fi
    done
  fi
  printf '{"sessions_created_by_this_run": %d, "deleted": %d, "failed": %d, "method": "DELETE /api/v1/sessions/{id} with the bearer token, one call per session id recorded by the harness", "at": "%s"}\n' \
    "$total" "$deleted" "$failed" "$(date -u +%FT%TZ)" > "$OUT/arm64-ci/cloud-cleanup.json"
  echo "cloud cleanup: $deleted of $total test sessions deleted, $failed failed"
  [ "$failed" = 0 ] && [ "$total" -gt 0 ] && : > "$SESSIONS"
}

remove_containers() {
  for c in $(docker ps -aq --filter "name=^$PFX-" 2>/dev/null); do
    docker rm -f "$c" >/dev/null 2>&1 || true
  done
  docker network rm "$NET" >/dev/null 2>&1 || true
}

fix_owner() {
  docker run --rm -v "$(host_path "$OUT"):/out" --entrypoint chown "$IMAGE" -R "$(id -u):$(id -g)" /out >/dev/null 2>&1 || true
}

cleanup() {
  [ -n "${WATCHER:-}" ] && kill "$WATCHER" >/dev/null 2>&1
  fix_owner
  if [ "$LOCAL_CLOUD" = 1 ]; then
    delete_sessions
    remove_containers
  else
    remove_containers
    delete_sessions
  fi
}

scrub_token() {
  if grep -rlF -e "$CLOUD_API_TOKEN" "$OUT" > "$WORK/leaks.txt" 2>/dev/null; then
    while read -r f; do
      rm -f "$f"
      echo "removed $f because it contained the token"
    done < "$WORK/leaks.txt"
  fi
  rm -f "$AUTH_HDR"
}

if [ "$CLEANUP_ONLY" = 1 ]; then
  remove_containers
  fix_owner
  [ "$LOCAL_CLOUD" = 1 ] || delete_sessions
  scrub_token
  exit 0
fi

wake_probe() {
  local file="$1" context="$2" start end attempts=0 first="" res code warm routing elapsed asleep
  start=$(uptime_s)
  while :; do
    attempts=$((attempts + 1))
    res=$(curl -s -o /dev/null -D "$WORK/wake.hdr" -w '%{http_code} %{time_total}' --max-time 120 "$HOST_CLOUD_URL/health" || true)
    [ -z "$first" ] && first="$res"
    code="${res%% *}"
    [ "$code" = 200 ] && break
    elapsed=$(awk -v s="$start" -v n="$(uptime_s)" 'BEGIN {print int(n - s)}')
    [ "$elapsed" -gt 300 ] && break
    sleep 1
  done
  end=$(uptime_s)
  routing=$(grep -i '^x-render-routing:' "$WORK/wake.hdr" 2>/dev/null | tr -d '\r' | cut -d' ' -f2- | head -1)
  warm=$(curl -s -o /dev/null -w '%{time_total}' --max-time 60 "$HOST_CLOUD_URL/health" || echo -1)
  elapsed=$(awk -v s="$start" -v e="$end" 'BEGIN {printf "%.2f", e - s}')
  asleep=$(awk -v e="$elapsed" 'BEGIN {print (e > 10 ? "true" : "false")}')
  printf '{"context": "%s", "measured_at": "%s", "url": "%s/health", "seconds_until_first_http_200": %s, "attempts": %d, "first_attempt": "%s", "final_http_code": "%s", "warm_request_seconds": %s, "x_render_routing": "%s", "inferred_asleep_before": %s, "note": "Render does not report whether the service was asleep; it is inferred from the time to the first 200 (more than 10 s means it had to start). Times include TLS setup from this runner."}\n' \
    "$context" "$(date -u +%FT%TZ)" "$CLOUD_URL" "$elapsed" "$attempts" "$first" "$code" "$warm" "$routing" "$asleep" > "$file"
  cat "$file"
}

capture_env() {
  local model vendor part
  model=$(lscpu 2>/dev/null | awk -F: '/^Model name/ {gsub(/^ +/, "", $2); print $2; exit}')
  vendor=$(lscpu 2>/dev/null | awk -F: '/^Vendor ID/ {gsub(/^ +/, "", $2); print $2; exit}')
  part=$(awk -F: '/^CPU part/ {gsub(/[ \t]/, "", $2); print $2; exit}' /proc/cpuinfo 2>/dev/null)
  {
    echo "label: $LABEL"
    echo "date_utc: $(date -u +%FT%TZ)"
    echo "github_run: ${GITHUB_RUN_ID:-local} attempt ${GITHUB_RUN_ATTEMPT:-} commit ${GITHUB_SHA:-$(git rev-parse HEAD 2>/dev/null)}"
    echo "runner: ${RUNNER_NAME:-} ${RUNNER_OS:-} ${RUNNER_ARCH:-} image ${ImageOS:-} ${ImageVersion:-}"
    echo "cloud: $CLOUD_URL"
    echo
    echo "## uname -a"
    uname -a
    echo
    echo "## lscpu"
    lscpu 2>/dev/null || echo "lscpu not available"
    echo
    echo "## /proc/cpuinfo"
    cat /proc/cpuinfo 2>/dev/null
    echo
    echo "## free -m"
    free -m 2>/dev/null || true
    echo
    echo "## docker"
    docker version --format 'client {{.Client.Version}}, server {{.Server.Version}} {{.Server.Os}}/{{.Server.Arch}}'
    docker info --format 'cgroup v{{.CgroupVersion}} ({{.CgroupDriver}}), kernel {{.KernelVersion}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes'
    echo
    echo "## inside a test container started with --cpus 4 --memory 2g --memory-swap 2g"
    docker run --rm --cpus 4 --memory 2g --memory-swap 2g --entrypoint sh "$IMAGE" -c \
      'echo "nproc: $(nproc)"; echo "cpu.max: $(cat /sys/fs/cgroup/cpu.max 2>/dev/null)"; echo "memory.max: $(cat /sys/fs/cgroup/memory.max 2>/dev/null)"; echo "uname -m: $(uname -m)"; java -version 2>&1 | head -2'
  } > "$OUT/arm64-ci/environment.txt" 2>&1
  printf '{"label": "%s", "machine": "%s", "kernel": "%s", "cpu_model": "%s", "cpu_vendor": "%s", "cpu_part": "%s", "host_cpus": %s, "host_mem_mb": %s, "container_limits": "--cpus 4 --memory 2g --memory-swap 2g", "github_run_id": "%s", "commit": "%s", "cloud": "%s", "measured_at": "%s"}\n' \
    "$LABEL" "$(uname -m)" "$(uname -r)" "${model:-unknown}" "${vendor:-unknown}" "${part:-unknown}" "$(nproc)" \
    "$(awk '/^MemTotal/ {print int($2 / 1024)}' /proc/meminfo)" "${GITHUB_RUN_ID:-local}" "${GITHUB_SHA:-$(git rev-parse HEAD 2>/dev/null)}" \
    "$CLOUD_URL" "$(date -u +%FT%TZ)" > "$OUT/arm64-ci/environment.json"
  cat "$OUT/arm64-ci/environment.json"
}

cat > "$CTL/net" <<'EOF'
#!/bin/sh
a="$1"
rm -f "/ctl/$a.done"
touch "/ctl/$a.req"
i=0
while [ ! -f "/ctl/$a.done" ]; do
  sleep 0.1
  i=$((i + 1))
  if [ "$i" -gt 600 ]; then
    echo "no answer from the host for $a"
    exit 1
  fi
done
r=$(cat "/ctl/$a.done")
rm -f "/ctl/$a.done"
echo "$r"
case "$r" in FAILED*) exit 1 ;; esac
exit 0
EOF

net_watcher() {
  local c="$1" n=0
  while :; do
    if [ -f "$CTL/cut.req" ]; then
      rm -f "$CTL/cut.req"
      if docker network disconnect "$NET" "$c" >/dev/null 2>"$WORK/net.err"; then
        echo "docker network disconnect $NET $c" > "$CTL/cut.done"
      else
        echo "FAILED $(cat "$WORK/net.err")" > "$CTL/cut.done"
      fi
    fi
    if [ -f "$CTL/restore.req" ]; then
      rm -f "$CTL/restore.req"
      if docker network connect "$NET" "$c" >/dev/null 2>"$WORK/net.err"; then
        echo "docker network connect $NET $c" > "$CTL/restore.done"
      else
        echo "FAILED $(cat "$WORK/net.err")" > "$CTL/restore.done"
      fi
    fi
    n=$((n + 1))
    if [ $((n % 10)) = 0 ] && [ "$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null)" != true ]; then
      return 0
    fi
    sleep 0.2
  done
}

run_harness() {
  local name="$1" rc
  shift
  local dargs=()
  while [ "$1" != "--" ]; do dargs+=("$1"); shift; done
  shift
  local c="$PFX-$name"
  docker rm -f "$c" >/dev/null 2>&1 || true
  rm -f "$CTL"/*.req "$CTL"/*.done
  echo "=== $name: failtest $* ($(date -u +%FT%TZ))"
  [ -n "${GITHUB_ACTIONS:-}" ] && echo "::group::$name"
  docker run -d --name "$c" --network "$NET" --cpus 4 --memory 2g --memory-swap 2g \
    -v "$(host_path "$OUT"):/out" -v "$(host_path "$CTL"):/ctl" -e CLOUD_API_TOKEN -e "FAILTEST_LOG_DIR=/out/logs/$name" \
    "${dargs[@]}" "$IMAGE" failtest-logged "$@" \
    --work /work --evidence /out/arm64-ci --session-log /out/logs/sessions-created.txt \
    --cloud-url "$CLOUD_URL" --net-cmd "sh /ctl/net {action}" --env-label "$LABEL" >/dev/null
  net_watcher "$c" &
  WATCHER=$!
  docker logs -f "$c" 2>&1 | tee "$OUT/logs/$name-harness.txt" | grep --line-buffered -E '^== |^\[[0-9]+ ms\] (device_online|device_offline|network_|cloud_|synced|measurement_|mock_session_ended|external_cloud)'
  rc=$(docker wait "$c")
  kill "$WATCHER" >/dev/null 2>&1
  wait "$WATCHER" 2>/dev/null
  WATCHER=""
  docker rm -f "$c" >/dev/null 2>&1
  [ -n "${GITHUB_ACTIONS:-}" ] && echo "::endgroup::"
  echo "=== $name: exit $rc"
  [ "$rc" = 0 ] || FAILED="$FAILED $name"
}

trap 'cleanup; scrub_token' EXIT

docker image inspect "$IMAGE" >/dev/null 2>&1 || { echo "image $IMAGE not found; build it from docker/arm64/Dockerfile"; exit 2; }
remove_containers
docker network create "$NET" >/dev/null

capture_env

if [ "$LOCAL_CLOUD" = 1 ]; then
  docker run -d --name "$PFX-cloud" --network "$NET" -p 127.0.0.1:18089:8080 -e CLOUD_API_TOKEN \
    -e CLOUD_SQLITE_PATH=/tmp/cloud/cloud.db -e PORT=8080 --entrypoint java "$IMAGE" -Xmx320m -jar /opt/pipeline/lib/cloud-1.0.0-all.jar >/dev/null
fi

wake_probe "$OUT/arm64-ci/cloud-wake-at-start.json" "first request of this run, before any test traffic"

run_harness soak --tmpfs /run/pipeline -- soak --minutes "$MINUTES" \
  --mock-cmd "biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100" --mock-port 7100 \
  --device-launcher /opt/pipeline/bin/pipeline --run-dir /run/pipeline \
  --resources-out "/out/resources/arm64-native-ci-online-${MINUTES}min.json"

want() {
  case ",$SCENARIOS," in *",all,"*|*",$1,"*) return 0 ;; *) return 1 ;; esac
}
CORE=""
for n in baseline kill9 netcut mockfaults powercut; do
  if want core || want "$n"; then CORE="$CORE,$n"; fi
done
if [ -n "$CORE" ]; then
  run_harness core -- "${CORE#,}"
fi
if want backlog; then
  run_harness backlog -- backlog --data-minutes 120 --fast-speed 20 --mock-buffer-seconds 200
fi
if want diskfull; then
  run_harness diskfull-within --tmpfs /small:size=64m -- diskfull --small-fs /small --hold 20 --tag within-buffer
  run_harness diskfull-beyond --tmpfs /small:size=64m -- diskfull --small-fs /small --hold 45 --memory-samples 8000 --tag beyond-buffer
fi
if want clock; then
  run_harness clock -- clock --clock-method faketime
fi

trap - EXIT
cleanup

if [ "$IDLE" -gt 0 ] && [ "$LOCAL_CLOUD" = 0 ]; then
  echo "leaving the cloud idle for $IDLE minutes before timing its wake-up"
  sleep $((IDLE * 60))
  wake_probe "$OUT/arm64-ci/cloud-wake-after-idle.json" "first request after $IDLE idle minutes (no requests from this run)"
fi

scrub_token

docker run --rm -v "$(host_path "$OUT"):/out" --entrypoint java "$IMAGE" -jar /opt/pipeline/lib/pipeline-tools.jar summarize /out >/dev/null \
  && echo "wrote $OUT/SUMMARY.md"
fix_owner

if [ -n "$FAILED" ]; then
  echo "failed:$FAILED"
  exit 1
fi
echo "all runs passed"
