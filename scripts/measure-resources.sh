#!/bin/bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: measure-resources.sh [--duration SECONDS] [--interval SECONDS] [--out FILE.json] [--pid PID] [--mock-pid PID] [--iface IFACE] [--status-file FILE]

Samples /proc while the pipeline daemon runs and reports, for the measured window:
  average CPU (percent of one core and of all cores), peak RSS, bytes written to storage by the
  process (/proc/PID/io write_bytes), and bytes transmitted on the network interface (/proc/net/dev).
Default duration is 3600 s (the 1-hour online run). Linux only.
EOF
}

DURATION=3600
INTERVAL=5
OUT="resources-$(date -u +%Y%m%dT%H%M%SZ).json"
PID=""
MOCK_PID=""
IFACE=""
STATUS_FILE="/run/pipeline/status.json"
while [ $# -gt 0 ]; do
  case "$1" in
    --duration) DURATION="$2"; shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --pid) PID="$2"; shift 2 ;;
    --mock-pid) MOCK_PID="$2"; shift 2 ;;
    --iface) IFACE="$2"; shift 2 ;;
    --status-file) STATUS_FILE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option $1"; usage; exit 2 ;;
  esac
done

find_pid() {
  local p=""
  if command -v systemctl >/dev/null 2>&1; then
    p=$(systemctl show -p MainPID --value pipeline.service 2>/dev/null || true)
    if [ -n "$p" ] && [ "$p" != "0" ] && [ -d "/proc/$p" ]; then
      local child
      child=$(pgrep -P "$p" -f 'device-1.0.0-all.jar' | head -1 || true)
      [ -n "$child" ] && p="$child"
      echo "$p"; return
    fi
  fi
  pgrep -f 'device-1.0.0-all.jar run' | head -1
}

[ -z "$PID" ] && PID=$(find_pid)
if [ -z "$PID" ] || [ ! -d "/proc/$PID" ]; then
  echo "pipeline daemon not found; pass --pid" >&2; exit 1
fi
[ -z "$MOCK_PID" ] && MOCK_PID=$(pgrep -f 'biosignal-mock' | head -1 || true)
[ -z "$IFACE" ] && IFACE=$(ip route show default 2>/dev/null | awk '/default/ {print $5; exit}')
[ -z "$IFACE" ] && IFACE=$(awk -F: 'NR>2 {gsub(/ /,"",$1); if ($1!="lo") {print $1; exit}}' /proc/net/dev)

TCK=$(getconf CLK_TCK)
NPROC=$(nproc)

cpu_ticks() { awk '{print $14 + $15}' "/proc/$1/stat"; }
rss_kb() { awk '/^VmRSS:/ {print $2}' "/proc/$1/status"; }
hwm_kb() { awk '/^VmHWM:/ {print $2}' "/proc/$1/status"; }
wbytes() { awk '/^write_bytes:/ {print $2}' "/proc/$1/io" 2>/dev/null || echo 0; }
tx_bytes() { awk -v i="$IFACE" -F'[: ]+' '{for (k=1;k<=NF;k++) if ($k==i) {print $(k+9); exit}}' /proc/net/dev; }
rx_bytes() { awk -v i="$IFACE" -F'[: ]+' '{for (k=1;k<=NF;k++) if ($k==i) {print $(k+1); exit}}' /proc/net/dev; }
app_bytes() { if [ -r "$STATUS_FILE" ]; then sed -n 's/.*"http_bytes_sent":\([0-9]*\).*/\1/p' "$STATUS_FILE"; else echo 0; fi; }

T0=$(date +%s.%N)
C0=$(cpu_ticks "$PID"); W0=$(wbytes "$PID"); TX0=$(tx_bytes); RX0=$(rx_bytes); A0=$(app_bytes)
MC0=0; MW0=0
if [ -n "$MOCK_PID" ]; then MC0=$(cpu_ticks "$MOCK_PID"); MW0=$(wbytes "$MOCK_PID"); fi
PEAK=$(rss_kb "$PID"); SUMRSS=0; N=0
RESTARTED=0
END=$(( $(date +%s) + DURATION ))
echo "measuring pid $PID (mock ${MOCK_PID:-none}) on iface ${IFACE:-?} for ${DURATION}s -> $OUT" >&2
while [ "$(date +%s)" -lt "$END" ]; do
  sleep "$INTERVAL"
  if [ ! -d "/proc/$PID" ]; then RESTARTED=1; break; fi
  R=$(rss_kb "$PID"); SUMRSS=$((SUMRSS + R)); N=$((N + 1))
  [ "$R" -gt "$PEAK" ] && PEAK=$R
done
T1=$(date +%s.%N)
C1=$(cpu_ticks "$PID" 2>/dev/null || echo "$C0"); W1=$(wbytes "$PID"); TX1=$(tx_bytes); RX1=$(rx_bytes); A1=$(app_bytes)
HWM=$(hwm_kb "$PID" 2>/dev/null || echo 0)
MC1=$MC0; MW1=$MW0
if [ -n "$MOCK_PID" ] && [ -d "/proc/$MOCK_PID" ]; then MC1=$(cpu_ticks "$MOCK_PID"); MW1=$(wbytes "$MOCK_PID"); fi

python3 - "$OUT" <<PY || awk 'BEGIN{print "python3 missing; raw values follow"}'
import json, sys, platform
el = $T1 - $T0
cpu = ($C1 - $C0) / $TCK
mcpu = ($MC1 - $MC0) / $TCK
avg_rss = ($SUMRSS / $N) if $N else 0
r = {
  "environment": {"machine": platform.machine(), "kernel": platform.release(), "cpus": $NPROC,
                  "model": open('/proc/device-tree/model').read().strip('\x00') if __import__('os').path.exists('/proc/device-tree/model') else platform.processor(),
                  "container": __import__('os').path.exists('/.dockerenv')},
  "pid": $PID, "measured_seconds": round(el, 1), "daemon_restarted_during_window": bool($RESTARTED),
  "pipeline_avg_cpu_percent_of_one_core": round(100 * cpu / el, 2),
  "pipeline_avg_cpu_percent_of_all_cores": round(100 * cpu / el / $NPROC, 3),
  "pipeline_peak_rss_mb_sampled": round($PEAK / 1024, 1),
  "pipeline_vm_hwm_mb_since_start": round($HWM / 1024, 1),
  "pipeline_avg_rss_mb": round(avg_rss / 1024, 1),
  "pipeline_mb_written_to_storage": round(($W1 - $W0) / 1e6, 2),
  "pipeline_mb_written_per_hour": round(($W1 - $W0) / 1e6 * 3600 / el, 2),
  "iface": "$IFACE",
  "iface_mb_sent": round(($TX1 - $TX0) / 1e6, 2),
  "iface_mb_received": round(($RX1 - $RX0) / 1e6, 2),
  "pipeline_http_mb_sent_app_counter": round(($A1 - $A0) / 1e6, 2),
  "mock_avg_cpu_percent_of_one_core": round(100 * mcpu / el, 2) if "$MOCK_PID" else None,
  "mock_mb_written": round(($MW1 - $MW0) / 1e6, 2) if "$MOCK_PID" else None,
  "notes": "write_bytes counts bytes this process caused to be sent to the block layer (SQLite DB + WAL, status file is on tmpfs). Interface bytes include all traffic on the interface, not only the pipeline."
}
json.dump(r, open(sys.argv[1], "w"), indent=2)
print(json.dumps(r, indent=2))
PY
