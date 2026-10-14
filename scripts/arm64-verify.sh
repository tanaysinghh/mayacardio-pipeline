#!/bin/bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: scripts/arm64-verify.sh [--seconds N] [--name NAME] [--cloud-url URL] [--token TOKEN]

Builds the device image for linux/arm64 (QEMU emulation through docker buildx when the host is not ARM),
runs the stand-in mock and the pipeline daemon inside it limited to 4 CPUs and 2 GB of memory, and records
resource figures with measure-resources.sh into evidence/resources/NAME.json.
Default --seconds is 3600 (the 1-hour online run). The cloud must be reachable from the container:
by default a local cloud jar on the host at http://host.docker.internal:18080.
EOF
}

SECONDS_TO_MEASURE=3600
NAME="arm64-online-$(date -u +%Y%m%dT%H%M%SZ)"
CLOUD_URL="http://host.docker.internal:18080"
TOKEN="${CLOUD_TOKEN:-}"
while [ $# -gt 0 ]; do
  case "$1" in
    --seconds) SECONDS_TO_MEASURE="$2"; shift 2 ;;
    --name) NAME="$2"; shift 2 ;;
    --cloud-url) CLOUD_URL="$2"; shift 2 ;;
    --token) TOKEN="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option $1"; usage; exit 2 ;;
  esac
done

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"
test -f dist/device-1.0.0-all.jar || { echo "build first: ./mvnw -DskipTests package and copy jars to dist/"; exit 1; }
docker buildx build --platform linux/arm64 -f docker/arm64/Dockerfile -t pipeline-device:arm64 --load .
mkdir -p evidence/resources
OUT="$REPO/evidence/resources/out-$NAME"
mkdir -p "$OUT"
docker run --rm --platform linux/arm64 --cpus 4 --memory 2g --memory-swap 2g \
  --add-host host.docker.internal:host-gateway \
  -e CLOUD_URL="$CLOUD_URL" -e CLOUD_TOKEN="$TOKEN" \
  -e MEASURE_SECONDS="$SECONDS_TO_MEASURE" -e MEASURE_NAME="$NAME" -e WARMUP_SECONDS=60 \
  -v "$OUT:/out" pipeline-device:arm64 device
cp "$OUT/$NAME.json" "evidence/resources/$NAME.json"
echo "wrote evidence/resources/$NAME.json"
