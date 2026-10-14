#!/bin/bash
set -euo pipefail

MODE="${1:-device}"
shift || true

case "$MODE" in
  device)
    mkdir -p /etc/pipeline /var/lib/pipeline /run/pipeline /out
    cat > /etc/pipeline/pipeline.conf <<EOF
mock.host=127.0.0.1
mock.port=7100
data.dir=/var/lib/pipeline
run.dir=/run/pipeline
cloud.url=${CLOUD_URL:-}
cloud.token=${CLOUD_TOKEN:-}
EOF
    mkdir -p /var/lib/mock-gt
    BIOSIGNAL_MOCK_GT_DIR=/var/lib/mock-gt biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100 > /out/mock.log 2>&1 &
    sleep 2
    /opt/pipeline/bin/pipeline run --config /etc/pipeline/pipeline.conf > /out/device.log 2>&1 &
    if [ -n "${MEASURE_SECONDS:-}" ]; then
      sleep "${WARMUP_SECONDS:-5}"
      /opt/pipeline/bin/measure-resources.sh --duration "$MEASURE_SECONDS" --out "/out/${MEASURE_NAME:-resources}.json" \
        --status-file /run/pipeline/status.json
      /opt/pipeline/bin/pipeline status > "/out/${MEASURE_NAME:-resources}-status.json"
    else
      wait
    fi
    ;;
  failtest)
    exec java -Xmx512m -jar /opt/pipeline/lib/pipeline-tools.jar failtest "$@" --jars /opt/pipeline/lib
    ;;
  *)
    exec "$MODE" "$@"
    ;;
esac
