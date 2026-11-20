start_case() {
  CASE="$LAB/$1"
  mkdir "$CASE" || return 1
  if [ "$2" = A ]; then cp --reflink=never "$LAB/seed.db" "$CASE/usage.db" || return 1; fi
  cat >"$CASE/config.toml" <<EOF
listen = "127.0.0.1:18380"
api_keys = [{label = "bench", key = "$KEY", admin = true}]
upstream = "https://127.0.0.1:9443/backend-api"
auth_dir = "$LAB/auth"
proxy_enabled = false
max_inflight_requests = 64
ws_max_connections = 24
ws_max_message_mb = 16
ws_compression = true
[usage]
enabled = true
path = "$CASE/usage.db"
prices = "$CASE/prices.toml"
retention_days = 0
prices_source = ""
[ui]
reset_credits_refresh_mins = 0
[log]
level = "warn"
EOF
  if [[ $2 == A ]]; then
    SSL_CERT_FILE="$LAB/ca.pem" "$PY" "$DIR/trace.py" "$BIN" "$CASE/config.toml" "$CASE" &
    TRACE_PID=$!
    for ((n=0;n<200;n++)); do
      [[ -s "$CASE/pid" ]] && break
      kill -0 "$TRACE_PID" || return 1
      sleep 0.1
    done
    GW_PID=$(cat "$CASE/pid")
  else
    SSL_CERT_FILE="$LAB/ca.pem" "$BIN" -c "$CASE/config.toml" serve >"$CASE/gateway.log" 2>&1 &
    GW_PID=$!
  fi
  curl --noproxy "*" --retry 8 --retry-connrefused -fsS -H "Authorization: Bearer $KEY" \
    http://127.0.0.1:18380/admin/v1/config >"$CASE/start.json"
}
end_case() {
  kill -TERM "$GW_PID"
  wait "${TRACE_PID:-$GW_PID}"
  unset TRACE_PID GW_PID
}
