#!/bin/sh
set -eu

usage() {
  cat <<'EOF'
usage: sudo ./scripts/install.sh [--cloud-url URL] [--token TOKEN] [--with-mock-standin] [--build]

Installs the pipeline on a Raspberry Pi OS / Debian (arm64 or amd64) board:
  - installs OpenJDK 17 if java is missing
  - creates the 'pipeline' system user and /opt/pipeline, /etc/pipeline, /var/lib/pipeline
  - installs the jars from ./dist (or builds them with --build, which needs a JDK)
  - installs /usr/local/bin/pipeline and the systemd unit, enables it at boot and starts it
  --with-mock-standin also installs and starts the stand-in mock (testing only):
      biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100
  An existing non-symlink /usr/local/bin/biosignal-mock (the official mock) is never overwritten.
EOF
}

REPO="$(cd "$(dirname "$0")/.." && pwd)"
CLOUD_URL=""
TOKEN=""
WITH_MOCK=0
BUILD=0
while [ $# -gt 0 ]; do
  case "$1" in
    --cloud-url) CLOUD_URL="$2"; shift 2 ;;
    --token) TOKEN="$2"; shift 2 ;;
    --with-mock-standin) WITH_MOCK=1; shift ;;
    --build) BUILD=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option $1"; usage; exit 2 ;;
  esac
done

if [ "$(id -u)" -ne 0 ]; then
  echo "run as root (sudo)"; exit 1
fi

if ! command -v java >/dev/null 2>&1; then
  apt-get update
  if [ "$BUILD" -eq 1 ]; then
    apt-get install -y openjdk-17-jdk-headless
  else
    apt-get install -y openjdk-17-jre-headless
  fi
fi

if [ "$BUILD" -eq 1 ] || [ ! -f "$REPO/dist/device-1.0.0-all.jar" ]; then
  echo "building jars (this takes a few minutes on a Pi)"
  (cd "$REPO" && sh ./mvnw -q -DskipTests package)
  mkdir -p "$REPO/dist"
  cp "$REPO/device/target/device-1.0.0-all.jar" "$REPO/mock-standin/target/biosignal-mock-standin.jar" \
     "$REPO/tools/target/pipeline-tools.jar" "$REPO/dist/"
  cp "$REPO/cloud/target/cloud-1.0.0-all.jar" "$REPO/dist/" 2>/dev/null || true
fi

id pipeline >/dev/null 2>&1 || useradd --system --home-dir /var/lib/pipeline --shell /usr/sbin/nologin pipeline
install -d -m 0755 /opt/pipeline/lib /opt/pipeline/bin /etc/pipeline
install -d -m 0770 -o pipeline -g pipeline /var/lib/pipeline
install -m 0644 "$REPO/dist/device-1.0.0-all.jar" /opt/pipeline/lib/
install -m 0644 "$REPO/dist/pipeline-tools.jar" /opt/pipeline/lib/ 2>/dev/null || true
install -m 0644 "$REPO/dist/biosignal-mock-standin.jar" /opt/pipeline/lib/ 2>/dev/null || true
install -m 0755 "$REPO/scripts/pipeline" /opt/pipeline/bin/pipeline
install -m 0755 "$REPO/scripts/measure-resources.sh" /opt/pipeline/bin/measure-resources.sh
install -m 0755 "$REPO/scripts/biosignal-mock" /opt/pipeline/bin/biosignal-mock
install -m 0755 "$REPO/scripts/check_ground_truth" /opt/pipeline/bin/check_ground_truth
ln -sf /opt/pipeline/bin/pipeline /usr/local/bin/pipeline
if [ ! -e /usr/local/bin/biosignal-mock ] || [ -L /usr/local/bin/biosignal-mock ]; then
  ln -sf /opt/pipeline/bin/biosignal-mock /usr/local/bin/biosignal-mock
fi
ln -sf /opt/pipeline/bin/check_ground_truth /usr/local/bin/check_ground_truth

if [ ! -f /etc/pipeline/pipeline.conf ]; then
  install -m 0640 -g pipeline "$REPO/scripts/pipeline.conf.example" /etc/pipeline/pipeline.conf
fi
if [ -n "$CLOUD_URL" ]; then
  sed -i "s#^cloud.url=.*#cloud.url=$CLOUD_URL#" /etc/pipeline/pipeline.conf
fi
if [ -n "$TOKEN" ]; then
  sed -i "s#^cloud.token=.*#cloud.token=$TOKEN#" /etc/pipeline/pipeline.conf
fi
chgrp pipeline /etc/pipeline/pipeline.conf
chmod 0640 /etc/pipeline/pipeline.conf

if [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != "root" ]; then
  usermod -a -G pipeline "$SUDO_USER" || true
  echo "$SUDO_USER ALL=(pipeline) NOPASSWD: /opt/pipeline/bin/pipeline" > /etc/sudoers.d/pipeline
  chmod 0440 /etc/sudoers.d/pipeline
fi

install -m 0644 "$REPO/scripts/systemd/pipeline.service" /etc/systemd/system/pipeline.service
if [ "$WITH_MOCK" -eq 1 ]; then
  install -m 0644 "$REPO/scripts/systemd/biosignal-mock-standin.service" /etc/systemd/system/biosignal-mock-standin.service
fi
systemctl daemon-reload
if [ "$WITH_MOCK" -eq 1 ]; then
  systemctl enable --now biosignal-mock-standin.service
fi
systemctl enable --now pipeline.service
sleep 3
/opt/pipeline/bin/pipeline status || true
echo "installed. logs: journalctl -u pipeline -f   status: pipeline status"
