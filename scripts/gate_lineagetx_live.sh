#!/usr/bin/env bash
set -euo pipefail

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
PYTHON="${PYTHON:-$ROOT/.venv/bin/python}"
GMS_INPUT="${DATAHUB_GMS_URL:-http://localhost:8080}"
WORK_ROOT="${LINEAGETX_LIVE_WORK_ROOT:-$ROOT/artifacts/runs/lineagetx-live}"
PHASE="${LINEAGETX_PHASE:-pause}"

case "$PHASE" in
  pause|resume|scripted-test)
    ;;
  *)
    echo "LINEAGETX_PHASE must be pause, resume, or scripted-test" >&2
    exit 2
    ;;
esac

if [[ ! -x "$PYTHON" ]]; then
  echo "LineageTX live gate requires the project virtual environment: $PYTHON" >&2
  exit 2
fi

GMS_URL="$("$PYTHON" - "$GMS_INPUT" <<'PY'
import sys

from data_lineage_fix_agent.lineagetx.datahub_context import normalize_datahub_gms_url

try:
    print(normalize_datahub_gms_url(sys.argv[1]))
except ValueError as error:
    print(f"LineageTX live gate rejected DATAHUB_GMS_URL: {error}", file=sys.stderr)
    raise SystemExit(2)
PY
)"

"$PYTHON" - "$GMS_URL" <<'PY'
import sys
import urllib.error
import urllib.request

request = urllib.request.Request(f"{sys.argv[1]}/health", method="GET")
try:
    with urllib.request.urlopen(request, timeout=10) as response:
        if response.status != 200:
            raise RuntimeError(f"unexpected HTTP status {response.status}")
except (OSError, RuntimeError, urllib.error.URLError) as error:
    print(f"full DataHub OSS GMS health check failed: {error}", file=sys.stderr)
    raise SystemExit(3)
PY

cd "$ROOT"

# Resume must observe DataHub drift since the durable pause; reseeding here
# would overwrite that evidence before the runner can fail closed.
if [[ "$PHASE" != "resume" ]]; then
  "$PYTHON" scripts/seed_lineagetx_datahub.py \
    --gms-url "$GMS_URL" \
    --verify-timeout-seconds "${LINEAGETX_SEED_TIMEOUT_SECONDS:-180}"
fi

RUN_LINEAGETX_DATAHUB_INTEGRATION=1 \
DATAHUB_GMS_URL="$GMS_URL" \
  "$PYTHON" -m pytest -q -m integration \
  tests/lineagetx/test_datahub_context.py

# The safe default stops at NEEDS_APPROVAL and persists signed resume state.
# COMMITTED requires either authenticated GitHub evidence in resume mode or
# the explicitly labeled scripted-test mode used only for deterministic tests.
RUN_ARGS=(
  --project-root "$ROOT"
  --work-root "$WORK_ROOT"
  --phase "$PHASE"
  --gms-url "$GMS_URL"
)
if [[ "$PHASE" != "resume" ]]; then
  RUN_ARGS+=(--reset)
fi
"$PYTHON" scripts/run_lineagetx_live.py "${RUN_ARGS[@]}" "$@"
