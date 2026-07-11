#!/usr/bin/env bash
set -euo pipefail

TEST="${1:-smoke}"
BASE_URL="${BASE_URL:-http://localhost:8080}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

case "$TEST" in
  smoke|load|reliability) ;;
  *)
    echo "Teste inválido: $TEST. Use smoke, load ou reliability." >&2
    exit 2
    ;;
esac

if [[ "${K6_DOCKER:-0}" == "1" ]]; then
  DOCKER_BASE_URL="${BASE_URL/http:\/\/localhost/http:\/\/host.docker.internal}"

  docker run --rm \
    -e "BASE_URL=$DOCKER_BASE_URL" \
    -e "RATE=${RATE:-}" \
    -e "DURATION=${DURATION:-}" \
    -e "PRE_ALLOCATED_VUS=${PRE_ALLOCATED_VUS:-}" \
    -e "MAX_VUS=${MAX_VUS:-}" \
    -e "SETTLE_SECONDS=${SETTLE_SECONDS:-}" \
    -e "VERIFY_ATTEMPTS=${VERIFY_ATTEMPTS:-}" \
    -e "VERIFY_INTERVAL_SECONDS=${VERIFY_INTERVAL_SECONDS:-}" \
    -v "$SCRIPT_DIR:/scripts:ro" \
    grafana/k6:2.1.0 run "/scripts/$TEST.js"
else
  BASE_URL="$BASE_URL" k6 run "$SCRIPT_DIR/$TEST.js"
fi
