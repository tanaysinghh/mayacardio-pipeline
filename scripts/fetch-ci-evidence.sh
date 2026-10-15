#!/bin/bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: scripts/fetch-ci-evidence.sh [RUN_ID]

Downloads the compact results of the latest completed run of .github/workflows/arm64-evidence.yml (or of RUN_ID)
with the GitHub CLI (run `gh auth login` first), puts them in evidence/arm64-ci/ and evidence/resources/,
and regenerates evidence/SUMMARY.md. Log tails (artifact arm64-evidence-logs) are not copied.
EOF
}

case "${1:-}" in -h|--help) usage; exit 0 ;; esac
command -v gh >/dev/null 2>&1 || { echo "the GitHub CLI (gh) is required"; exit 1; }
REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

RUN="${1:-}"
if [ -z "$RUN" ]; then
  RUN=$(gh run list --workflow arm64-evidence.yml --status completed --limit 1 --json databaseId --jq '.[0].databaseId')
  [ -n "$RUN" ] && [ "$RUN" != null ] || { echo "no completed run of arm64-evidence.yml found"; exit 1; }
fi
gh run view "$RUN" --json databaseId,conclusion,createdAt,headSha,url \
  --jq '"run \(.databaseId): \(.conclusion), started \(.createdAt), commit \(.headSha)\n\(.url)"'

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
gh run download "$RUN" --name arm64-evidence --dir "$TMP"
[ -d "$TMP/arm64-ci" ] || { echo "artifact arm64-evidence has no arm64-ci/ directory"; exit 1; }

big=$(find "$TMP" -type f -size +300k)
if [ -n "$big" ]; then
  echo "refusing to copy files larger than 300 KB into evidence/:"
  echo "$big"
  exit 1
fi

rm -rf evidence/arm64-ci
mkdir -p evidence/arm64-ci evidence/resources
cp "$TMP"/arm64-ci/* evidence/arm64-ci/
if [ -d "$TMP/resources" ]; then
  cp "$TMP"/resources/*.json evidence/resources/
fi
echo "copied:"
ls -l evidence/arm64-ci evidence/resources

JAR=""
for j in dist/pipeline-tools.jar tools/target/pipeline-tools.jar; do
  [ -f "$j" ] && { JAR="$j"; break; }
done
if [ -n "$JAR" ]; then
  java -jar "$JAR" summarize evidence
else
  echo "build first (./mvnw -DskipTests package), then run: java -jar tools/target/pipeline-tools.jar summarize evidence"
fi

for f in evidence/resources/arm64-native-ci-online-*.json; do
  [ -f "$f" ] || continue
  awk -v name="$(basename "$f" .json)" '
    /"model"/ { m = $0; sub(/.*"model": "/, "", m); sub(/".*/, "", m) }
    /"measured_seconds"/ { gsub(/[^0-9.]/, "", $2); sec = $2 }
    /"pipeline_avg_cpu_percent_of_one_core"/ { gsub(/[^0-9.]/, "", $2); cpu = $2 }
    /"pipeline_peak_rss_mb_sampled"/ { gsub(/[^0-9.]/, "", $2); rss = $2 }
    /"pipeline_mb_written_per_hour"/ { gsub(/[^0-9.]/, "", $2); wr = $2 }
    /"iface_mb_sent"/ { gsub(/[^0-9.]/, "", $2); tx = $2 }
    END {
      printf "\nDesign-note row (%s, %s):\n", name, m
      printf "| GitHub Actions native ARM64 runner (%s), 4 CPUs / 2 GB, %d min | %.1f %% (%.1f %% of one core) | %.0f MB | %.0f MB/h | %.1f MB/h |\n", m, sec / 60, cpu / 4, cpu, rss, wr, tx * 3600 / sec
    }' "$f"
done
