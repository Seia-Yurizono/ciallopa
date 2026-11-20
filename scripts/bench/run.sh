#!/usr/bin/env bash
set -euo pipefail
DIR=$(cd "$(dirname "$0")" && pwd)
: "${LAB:?private prepared directory}"
PY="$LAB/py/bin/python"
export KEY=sk-local-benchmark-fake-0123456789
source "$DIR/case.sh"
api() { curl --noproxy '*' -fsS -H "Authorization: Bearer $KEY" "http://127.0.0.1:18380/$1"; }
summary() { api 'usage/v1/summary?from=1&to=1'; }
observe() {
  while [[ -d /proc/$GW_PID ]]; do
    date +%s.%N
    stat -c 'wal %s' "$CASE/usage.db-wal" 2>/dev/null || true
    awk '/VmRSS|VmHWM|Threads/' "/proc/$GW_PID/status"
    cat "/proc/$GW_PID/io"
    if [[ $group == B ]]; then ss -tinm '( sport = :18380 )'; fi
    sleep 1
  done
}
cleanup() {
  for p in ${SAMPLER:-} ${IO:-} ${PS:-} ${SSE:-} ${PROBE:-} ${INJECT:-} ${GW_PID:-} ${ORIGIN:-}; do
    kill "$p" 2>/dev/null || true
  done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if ss -Hlt '( sport = :9443 or sport = :18380 )' | rg -q .; then echo 'benchmark ports occupied' >&2; exit 1; fi
"$PY" "$DIR/origin.py" >>"$LAB/origin.jsonl" 2>>"$LAB/origin.log" &
ORIGIN=$!
curl --noproxy '*' --retry 6 --retry-connrefused --http2 --cacert "$LAB/ca.pem" -fsS -o /dev/null -w '%{http_version} %{http_code}\n' -X POST https://127.0.0.1:9443/backend-api/codex/responses
for group in "${@:-A}"; do
  case "$group" in
    A) modes=(base quota mixed) ;;
    B) read -ra modes <<< "${B_MODES:-fast slow pause stop smallinput biginput long}" ;;
    *) echo "unknown group: $group" >&2; exit 1 ;;
  esac
  for ((r=1;r<=${ROUNDS:-3};r++)); do
    for mode in "${modes[@]}"; do
      versions=(old next)
      ((r%2)) || versions=(next old)
      for version in "${versions[@]}"; do
        BIN="$LAB/$version-bin"
        name="${PREFIX:-}$group-$r-$mode-$version"
        echo "$(date -Is) $name"
        start_case "$name" "$group"
        api admin/v1/runtime >"$CASE/before-runtime.json"
        if [[ $group == B ]]; then api admin/v1/memory >"$CASE/before-memory.json"; fi
        if [[ $group == A ]]; then
          for url in quota 'quota/history?days=7'; do api "usage/v1/$url" >/dev/null; done
        fi
        "$PY" "$DIR/load.py" base 2 >"$CASE/warm.jsonl"
        sleep 1
        summary >"$CASE/before.json"
        wc -l <"$LAB/origin.jsonl" >"$CASE/origin-start"
        observe >"$CASE/system.log" & SAMPLER=$!
        LC_ALL=C iostat -y -dx 1 >"$CASE/iostat" & IO=$!
        LC_ALL=C pidstat -h -dru -p "$GW_PID,$ORIGIN" 1 >"$CASE/pidstat" & PS=$!
        date +%s.%N >"$CASE/start-time"
        if [[ $group == A ]]; then
          "$PY" "$DIR/load.py" "$mode" "${A_SECONDS:-20}" >"$CASE/load.jsonl"
        else
          export PROBES=${B_SECONDS:-24}
          "$PY" "$DIR/load.py" base "$PROBES" >"$CASE/load.jsonl" & SSE=$!
          "$PY" "$DIR/wsload.py" probe 1 >"$CASE/probe.jsonl" & PROBE=$!
          sleep 3
          "$PY" "$DIR/wsload.py" "$mode" 3 >"$CASE/intervention.jsonl" &
          INJECT=$!
          wait "$SSE"; wait "$PROBE"; wait "$INJECT"
          unset SSE PROBE INJECT
        fi
        date +%s.%N >"$CASE/end-time"
        wc -l <"$LAB/origin.jsonl" >"$CASE/origin-end"
        sleep 1
        summary >"$CASE/after.json"
        api admin/v1/runtime >"$CASE/after-runtime.json"
        if [[ $group == B ]]; then api admin/v1/memory >"$CASE/after-memory.json"; fi
        kill "$SAMPLER" "$IO" "$PS"
        wait "$SAMPLER" "$IO" "$PS" || true
        unset SAMPLER IO PS
        if [[ $group == B && $r == 1 && ( $mode == smallinput || $mode == biginput ) ]]; then
          for tick in {0..6}; do
            date +%s.%N
            awk '/VmRSS|VmHWM/' "/proc/$GW_PID/status"
            ((tick == 6)) || sleep 5
          done >"$CASE/idle-memory.log"
          api admin/v1/runtime >"$CASE/idle-runtime.json"
        fi
        end_case
      done
    done
  done
done
