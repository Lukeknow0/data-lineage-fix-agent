#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

GMS_URL="${DATAHUB_GMS_URL:-http://127.0.0.1:8979}"
LITE_FILE="$ROOT/artifacts/datahub-lite/datahub.duckdb"

if [[ "$GMS_URL" == "http://127.0.0.1:8979" ]]; then
  .venv/bin/python scripts/seed_datahub_lite.py --file "$LITE_FILE"
  .venv/bin/python -m data_lineage_fix_agent.lite_gms_bridge \
    --file "$LITE_FILE" --host 127.0.0.1 --port 8979 \
    >artifacts/lite_bridge.log 2>&1 &
  BRIDGE_PID=$!
  trap 'kill "$BRIDGE_PID" 2>/dev/null || true' EXIT
  for _ in {1..20}; do
    curl --fail --silent "$GMS_URL/config" >/dev/null && break
    sleep 0.25
  done
  export DATALINEAGE_DATAHUB_BACKEND="datahub-oss-lite-v1.6.0"
else
  .venv/bin/python scripts/seed_datahub.py
fi

if ! curl --fail --silent --show-error "$GMS_URL/config" >/dev/null; then
  echo "DataHub GMS is not ready at $GMS_URL" >&2
  exit 2
fi

.venv/bin/datalineage-fix run --mode mcp --datahub-url "$GMS_URL"
DATAHUB_GMS_URL="$GMS_URL" RUN_DATAHUB_INTEGRATION=1 \
  .venv/bin/python -m pytest -m integration

echo "LIVE GATE: PASS"
