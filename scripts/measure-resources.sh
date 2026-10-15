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
      echo "$p"
      return
    fi
  fi
  pgrep -f 'device-1.0.0-all.jar run' | head -1 || true
}

[ -z "$PID" ] && PID=$(find_pid)
if [ -z "$PID" ] || [ ! -d "/proc/$PID" ]; then
  echo "pipeline daemon not found; pass --pid" >&2
  exit 1
fi
[ -z "$MOCK_PID" ] && MOCK_PID=$(pgrep -f 'biosignal-mock' | head -1 || true)
[ -z "$IFACE" ] && IFACE=$(ip route show default 2>/dev/null | awk '/default/ {print $5; exit}' || true)
[ -z "$IFACE" ] && IFACE=$(awk -F: 'NR>2 {gsub(/ /,"",$1); if ($1!="lo") {print $1; exit}}' /proc/net/dev)

TCK=$(getconf CLK_TCK)
NPROC=$(nproc)

cpu_ticks() { awk '{print $14 + $15}' "/proc/$1/stat"; }
rss_kb() { awk '/^VmRSS:/ {print $2}' "/proc/$1/status"; }
hwm_kb() { awk '/^VmHWM:/ {print $2}' "/proc/$1/status"; }
wbytes() { awk '/^write_bytes:/ {print $2}' "/proc/$1/io" 2>/dev/null || echo 0; }
dev_field() { awk -v i="$IFACE" -v f="$1" '{ sub(/^ +/, ""); split($0, a, /[: ]+/); if (a[1] == i) { print a[f]; exit } }' /proc/net/dev; }
app_bytes() {
  if [ -r "$STATUS_FILE" ]; then
    sed -n 's/.*"http_bytes_sent":\([0-9]*\).*/\1/p' "$STATUS_FILE"
  else
    echo 0
  fi
}

T0=$(awk '{print $1}' /proc/uptime)
C0=$(cpu_ticks "$PID"); W0=$(wbytes "$PID"); RX0=$(dev_field 2); TX0=$(dev_field 10); A0=$(app_bytes)
MC0=0; MW0=0
if [ -n "$MOCK_PID" ]; then MC0=$(cpu_ticks "$MOCK_PID"); MW0=$(wbytes "$MOCK_PID"); fi
PEAK=$(rss_kb "$PID"); SUMRSS=0; N=0; RESTARTED=0
END=$(awk -v d="$DURATION" '{print int($1) + d}' /proc/uptime)
echo "measuring pid $PID (mock ${MOCK_PID:-none}) on iface ${IFACE:-?} for ${DURATION}s -> $OUT" >&2
while [ "$(awk '{print int($1)}' /proc/uptime)" -lt "$END" ]; do
  sleep "$INTERVAL"
  if [ ! -d "/proc/$PID" ]; then RESTARTED=1; break; fi
  R=$(rss_kb "$PID"); SUMRSS=$((SUMRSS + R)); N=$((N + 1))
  if [ "$R" -gt "$PEAK" ]; then PEAK=$R; fi
done
T1=$(awk '{print $1}' /proc/uptime)
if [ -d "/proc/$PID" ]; then
  C1=$(cpu_ticks "$PID"); W1=$(wbytes "$PID"); HWM=$(hwm_kb "$PID")
else
  C1=$C0; W1=$W0; HWM=0
fi
RX1=$(dev_field 2); TX1=$(dev_field 10); A1=$(app_bytes)
MC1=$MC0; MW1=$MW0
if [ -n "$MOCK_PID" ] && [ -d "/proc/$MOCK_PID" ]; then MC1=$(cpu_ticks "$MOCK_PID"); MW1=$(wbytes "$MOCK_PID"); fi
MODEL=$(tr -d '\000' < /proc/device-tree/model 2>/dev/null || true)
[ -z "$MODEL" ] && MODEL=$(lscpu 2>/dev/null | awk -F: '/^Model name/ {gsub(/^ +/, "", $2); print $2; exit}' || true)
[ -z "$MODEL" ] && MODEL=$(uname -m)
CONTAINER=false
[ -f /.dockerenv ] && CONTAINER=true

awk -v t0="$T0" -v t1="$T1" -v c0="$C0" -v c1="$C1" -v tck="$TCK" -v np="$NPROC" -v peak="$PEAK" -v hwm="$HWM" \
    -v sum="$SUMRSS" -v n="$N" -v w0="$W0" -v w1="$W1" -v tx0="${TX0:-0}" -v tx1="${TX1:-0}" -v rx0="${RX0:-0}" -v rx1="${RX1:-0}" \
    -v a0="${A0:-0}" -v a1="${A1:-0}" -v mc0="$MC0" -v mc1="$MC1" -v mw0="$MW0" -v mw1="$MW1" -v mpid="${MOCK_PID:-}" \
    -v pid="$PID" -v iface="$IFACE" -v model="$MODEL" -v kernel="$(uname -r)" -v machine="$(uname -m)" \
    -v container="$CONTAINER" -v restarted="$RESTARTED" '
BEGIN {
  q = "\""
  el = t1 - t0
  cpu = (c1 - c0) / tck
  mcpu = (mc1 - mc0) / tck
  print "{"
  printf "  %senvironment%s: {%smachine%s: %s%s%s, %smodel%s: %s%s%s, %skernel%s: %s%s%s, %scpus%s: %d, %scontainer%s: %s},\n", q, q, q, q, q, machine, q, q, q, q, model, q, q, q, q, kernel, q, q, q, np, q, q, container
  printf "  %spid%s: %d,\n", q, q, pid
  printf "  %smeasured_seconds%s: %.1f,\n", q, q, el
  printf "  %sdaemon_restarted_during_window%s: %s,\n", q, q, (restarted == 1 ? "true" : "false")
  printf "  %spipeline_avg_cpu_percent_of_one_core%s: %.2f,\n", q, q, 100 * cpu / el
  printf "  %spipeline_avg_cpu_percent_of_all_cores%s: %.3f,\n", q, q, 100 * cpu / el / np
  printf "  %spipeline_peak_rss_mb_sampled%s: %.1f,\n", q, q, peak / 1024
  printf "  %spipeline_vm_hwm_mb_since_start%s: %.1f,\n", q, q, hwm / 1024
  printf "  %spipeline_avg_rss_mb%s: %.1f,\n", q, q, (n > 0 ? sum / n : 0) / 1024
  printf "  %spipeline_mb_written_to_storage%s: %.2f,\n", q, q, (w1 - w0) / 1e6
  printf "  %spipeline_mb_written_per_hour%s: %.2f,\n", q, q, (w1 - w0) / 1e6 * 3600 / el
  printf "  %siface%s: %s%s%s,\n", q, q, q, iface, q
  printf "  %siface_mb_sent%s: %.2f,\n", q, q, (tx1 - tx0) / 1e6
  printf "  %siface_mb_received%s: %.2f,\n", q, q, (rx1 - rx0) / 1e6
  printf "  %spipeline_http_mb_sent_app_counter%s: %.2f,\n", q, q, (a1 - a0) / 1e6
  if (mpid != "") {
    printf "  %smock_avg_cpu_percent_of_one_core%s: %.2f,\n", q, q, 100 * mcpu / el
    printf "  %smock_mb_written%s: %.2f,\n", q, q, (mw1 - mw0) / 1e6
  }
  printf "  %snotes%s: %swrite_bytes counts bytes this process caused to be sent to the block layer (SQLite database and WAL; the status file is on tmpfs). Interface bytes include all traffic on the interface. The app counter is request bodies plus an estimated 350 bytes of headers per request, before TLS.%s\n", q, q, q, q
  print "}"
}' | tee "$OUT"
