#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "$PYTHON_BIN" ]]; then
  for candidate in python3.11 python3; do
    if command -v "$candidate" >/dev/null 2>&1 && \
      "$candidate" -c 'import sys; raise SystemExit(sys.version_info[:2] != (3, 11))'; then
      PYTHON_BIN="$candidate"
      break
    fi
  done
fi
if [[ -z "$PYTHON_BIN" ]]; then
  echo "Python 3.11 is required. Set PYTHON_BIN to a Python 3.11 executable." >&2
  exit 2
fi

if [[ ! -x .venv/bin/python ]]; then
  "$PYTHON_BIN" -m venv .venv
elif ! .venv/bin/python -c 'import sys; raise SystemExit(sys.version_info[:2] != (3, 11))'; then
  echo "Existing .venv is not Python 3.11; recreate it with PYTHON_BIN=$PYTHON_BIN." >&2
  exit 2
fi

.venv/bin/python -m pip install -e '.[dev]'

echo "Ready. Run 'make start' for the offline replay, 'make verify' for tests, or 'make gate-live' for the DataHub MCP path."
