#!/usr/bin/env bash
# Fingerprint check against Codex CLI 0.163.0 recorded in fp-survey.
# Env: FP_SURVEY = checkout containing server/fpserver and ca.pem.
# Temporarily adds its own chatgpt.com hosts entry; leaves other entries and processes alone.
set -euo pipefail

hosts_entry() {
  sudo python3 - "$@" <<'PY'
import fcntl, sys
action, path, marker = sys.argv[1:]
entry = f"127.0.0.1 chatgpt.com # {marker}\n".encode()
with open(path, 'r+b') as f:
    fcntl.flock(f, fcntl.LOCK_EX)
    current = f.read()
    if action == 'add':
        for line in current.splitlines():
            if b'chatgpt.com' in line.split(b'#', 1)[0].split()[1:]:
                raise SystemExit('chatgpt.com already has a hosts entry; use an isolated environment.')
        updated = current + (b'\n' if current and not current.endswith(b'\n') else b'') + entry
    elif action == 'remove':
        # Read the current file, not an old backup: retain unrelated edits made during the check.
        updated = b''.join(line for line in current.splitlines(keepends=True) if line != entry)
    else:
        raise SystemExit('unknown hosts operation')
    if updated != current:
        f.seek(0)
        f.write(updated)
        f.truncate()
PY
}

start_server() {
  local server_dir=$1 work=$2
  SERVER_PID_FILE="$work/server.pid"
  : > "$SERVER_PID_FILE"
  # Record the process that execs fpserver, not the intermediate sudo process.
  sudo sh -ec 'printf "%s\n" "$$" > "$1"; cd "$2"; exec ./fpserver -addr 127.0.0.1:443 -ctl 127.0.0.1:9998 -out "$3"' \
    ciallopa-fp "$SERVER_PID_FILE" "$server_dir" "$work/captures.jsonl" >"$work/server.log" 2>&1 &
  SERVER_JOB=$!
}

stop_server() {
  [[ -n "${SERVER_JOB:-}" ]] || return 0
  local server_pid
  if sudo kill -0 "$SERVER_JOB" 2>/dev/null; then
    if [[ -s "$SERVER_PID_FILE" ]]; then
      read -r server_pid < "$SERVER_PID_FILE"
      [[ "$server_pid" =~ ^[1-9][0-9]*$ ]] || return 1
      sudo kill -TERM "$server_pid" 2>/dev/null || true
    else
      # An interruption before the launcher wrote its PID: sudo forwards TERM to its child.
      sudo kill -TERM "$SERVER_JOB" 2>/dev/null || true
    fi
  fi
  wait "$SERVER_JOB" || true
  SERVER_JOB=
}

check_captures() {
  python3 - "$1" <<'PY'
import json, sys
want_ja4 = "t13d1011h2_61a7ad8aa9b6_0d308c48d2a3"
want_res = "t13d1011h2_61a7ad8aa9b6_053248755fe4"
want_ak = "2:0;4:2097152;5:16384;6:16384|5177345|0|m,s,a,p"
# Responses WS capture: fp-survey/raw/codex-ws.jsonl, 2026-10-13.
want_ws = "t13d101000_61a7ad8aa9b6_0d308c48d2a3"
want_ws_hdr = "Host,Connection,Upgrade,Sec-WebSocket-Version,Sec-WebSocket-Key"
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
rows = [r for r in rows if r.get("label") == "ciallopa"]
ok = True
ws = [r for r in rows if r.get("method") == "GET" and "responses" in (r.get("path") or "")]
rows = [r for r in rows if r not in ws]
if not ws:
    print("FAIL ws: no websocket handshake captured"); ok = False
if not rows:
    print("FAIL http: no HTTP capture recorded"); ok = False
for r in ws:
    names = [h[0] for h in r.get("headers") or []]
    prefix = ",".join(names[:5])
    good = r["ja4"] == want_ws and prefix == want_ws_hdr and names[-1].lower() == "sec-websocket-extensions"
    ok &= good
    print(f"{'PASS' if good else 'FAIL'} ws path={r.get('path')} ja4={r['ja4']} headers={names}")
for r in rows:
    ak = r["h2"].get("akamai", "")
    j = r["ja4"]
    good = j == (want_res if r.get("resumed") else want_ja4) and ak == want_ak
    ok &= good
    print(f"{'PASS' if good else 'FAIL'} path={r.get('path')} ja4={j} akamai={ak} resumed={r.get('resumed')} alpn={r.get('negotiated_alpn')} grease={r['hello']['HasGREASE']}")
print(f"expected ja4={want_ja4} (resumed {want_res}) akamai={want_ak}")
sys.exit(0 if ok else 1)
PY
}

main() {
  local fp=${FP_SURVEY:?set to fp-survey checkout}
  local root
  root=$(cd "$(dirname "$0")/.." && pwd)
  fp=$(cd "$fp" && pwd)
  umask 077
  WORK=$(mktemp -d /tmp/ciallopa-fp.XXXXXX)
  HOSTS_MARKER="ciallopa-fp-${WORK##*/}"
  HOSTS_ADDED=false
  SERVER_JOB=
  cleanup() {
    local result=$?
    trap - EXIT
    stop_server || result=1
    if [[ "$HOSTS_ADDED" == true ]]; then
      hosts_entry remove /etc/hosts "$HOSTS_MARKER" || result=1
    fi
    echo "Fingerprint evidence: $WORK"
    exit "$result"
  }
  trap cleanup EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM

  (cd "$root" && cargo build --locked -q --bin fp-probe)
  sudo -v
  # Set before writing so interruption immediately after the write still removes our entry.
  HOSTS_ADDED=true
  hosts_entry add /etc/hosts "$HOSTS_MARKER"
  start_server "$fp/server" "$WORK"
  sleep 1
  sudo kill -0 "$SERVER_JOB"
  exec 3<>/dev/tcp/127.0.0.1/9998
  printf "ciallopa" >&3; cat <&3 >/dev/null; exec 3>&-
  SSL_CERT_FILE="$fp/ca.pem" "$root/target/debug/fp-probe" https://chatgpt.com/backend-api
  sleep 0.5
  stop_server
  # The directory is private; only the calling user needs to read the captures.
  sudo chown "$(id -u):$(id -g)" "$WORK/captures.jsonl"
  check_captures "$WORK/captures.jsonl"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then main "$@"; fi
