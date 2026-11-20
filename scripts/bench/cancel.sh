#!/usr/bin/env bash
set -euo pipefail
DIR=$(cd "$(dirname "$0")" && pwd)
: "${LAB:?private prepared directory}"
export KEY=sk-local-benchmark-fake-0123456789 PERMIT_PROBE=1
PY="$LAB/py/bin/python"
source "$DIR/case.sh"
trap '[[ -z ${GW_PID:-} ]] || kill "$GW_PID" 2>/dev/null || true' EXIT
for version in old next; do
  BIN="$LAB/$version-bin"
  start_case "${PREFIX:-}cancel-$version" A
  "$PY" "$DIR/cancel.py" "$CASE" >"$CASE/cancel.jsonl"
  end_case
done
