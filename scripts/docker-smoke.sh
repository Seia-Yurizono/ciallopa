#!/usr/bin/env bash
# Smoke test of a built ciallopa image (used as-is by .github/workflows/release.yml): the
# first-run flow (missing config, `init`, refusing to overwrite, the example key), then the
# running server: user, healthcheck, settings write through the config directory mount, and a
# stop within the grace period.
#   scripts/docker-smoke.sh <image>      (needs docker, sudo, curl, jq)
#   SMOKE_UID / SMOKE_GID: container user (default: the calling user, as in compose.yaml)
set -euo pipefail

IMAGE=${1:?usage: docker-smoke.sh <image>}
PORT=${SMOKE_PORT:-18327}
U=${SMOKE_UID:-$(id -u)}
G=${SMOKE_GID:-$(id -g)}
cd "$(dirname "$0")/.."
WORK=$(mktemp -d)
CID=
cleanup() {
    if [[ -n "$CID" ]]; then docker rm -f "$CID" >/dev/null 2>&1 || true; fi
    sudo rm -rf "$WORK"
}
trap cleanup EXIT

mkdir -p "$WORK/config" "$WORK/data" "$WORK/example"
sudo chown "$U:$G" "$WORK/config" "$WORK/data" "$WORK/example"
sudo chmod 700 "$WORK/config" "$WORK/data"

run() {
    docker run -d --user "$U:$G" --hostname ciallopa --read-only --cap-drop ALL \
        --security-opt no-new-privileges:true --health-interval 2s \
        -p "127.0.0.1:$PORT:8317" \
        -v "$WORK/config:/etc/ciallopa" -v "$WORK/data:/var/lib/ciallopa/.ciallopa" "$@"
}
expect_exit() {   # <pattern> <container id>: exits 1 and logs the pattern
    # Consume all producer output: grep -q can close the pipe early and turn
    # a successful match into SIGPIPE (141) under pipefail.
    docker wait "$2" | grep -x 1 >/dev/null
    docker logs "$2" 2>&1 | tee /dev/stderr | grep -F "$1" >/dev/null
    docker rm "$2" >/dev/null
}

# No config: exit 1 pointing at init.
CID=$(run "$IMAGE")
expect_exit 'no config file at /etc/ciallopa/config.toml (config/config.toml on the host, with the example compose file); run "ciallopa init"' "$CID"
CID=

# init: random key, listen from CIALLOPA_INIT_LISTEN, mode 0600, owned by the container user.
out=$(docker run --rm --user "$U:$G" --read-only -v "$WORK/config:/etc/ciallopa" "$IMAGE" init)
printf '%s\n' "$out" | sed -n '1,3p'
KEY=$(grep -oE 'sk-cpa-[0-9a-f]{48}' <<<"$out" | sed -n '1p')
test -n "$KEY"
grep -q "CPA_KEY=$KEY" <<<"$out"
grep -q 'docker compose exec ciallopa ciallopa login' <<<"$out"
grep -qF 'Wrote /etc/ciallopa/config.toml (mode 0600; config/config.toml on the host, with the example compose file)' <<<"$out"
cfg="$WORK/config/config.toml"
test "$(sudo stat -c '%u %a' "$cfg")" = "$U 600"
sudo grep -q '^listen = "0.0.0.0:8317"' "$cfg"
sudo grep -q "key = \"$KEY\", admin = true" "$cfg"
if docker run --rm --user "$U:$G" -v "$WORK/config:/etc/ciallopa" "$IMAGE" init 2>"$WORK/err"; then exit 1; fi
grep -q 'already exists; not overwritten' "$WORK/err"
# Enable settings writes in place (owner and mode kept).
sudo sh -c "sed 's/^allow_config_write = false/allow_config_write = true/' '$cfg' > '$WORK/c.tmp' && cat '$WORK/c.tmp' > '$cfg' && rm '$WORK/c.tmp'"

# The example key is refused.
sed 's/^listen = "127.0.0.1:8317"/listen = "0.0.0.0:8317"/' config.example.toml |
    sudo tee "$WORK/example/config.toml" >/dev/null
sudo chown "$U:$G" "$WORK/example/config.toml"
CID=$(docker run -d --user "$U:$G" -v "$WORK/example:/etc/ciallopa" "$IMAGE")
expect_exit 'api_keys contains the example key' "$CID"
CID=

CID=$(run "$IMAGE")
for _ in $(seq 30); do
    [[ "$(docker inspect -f '{{.State.Health.Status}}' "$CID")" == healthy ]] && break
    sleep 1
done
docker inspect -f '{{.State.Health.Status}}' "$CID" | grep -x healthy >/dev/null
curl -fsS "http://127.0.0.1:$PORT/healthz"; echo
test "$(ps -o uid= -p "$(docker inspect -f '{{.State.Pid}}' "$CID")" | tr -d ' ')" = "$U"
sudo test -d "$WORK/data/auths"
docker logs "$CID" 2>&1 | grep 'no accounts yet' >/dev/null

# Settings write: temp file + rename inside the mounted directory.
version=$(curl -fsS -H "X-Api-Key: $KEY" "http://127.0.0.1:$PORT/admin/v1/config" | jq -r .version)
curl -fsS -X PUT -H "X-Api-Key: $KEY" -H "If-Match: \"$version\"" -H 'Content-Type: application/json' \
    -d '{"changes":{"routing.strategy":"fill-first"}}' "http://127.0.0.1:$PORT/admin/v1/config"; echo
sudo grep -q '^strategy = "fill-first"' "$cfg"
sudo test -f "$cfg.bak"
sudo test -f "$cfg.audit.jsonl"
# Files the server created are private to its user, whatever the runtime's umask.
test -z "$(sudo find "$WORK/config" "$WORK/data" -mindepth 1 -perm /077)"
if sudo find "$WORK/config" -name '.config.toml.tmp-*' | grep . >/dev/null; then exit 1; fi

# Graceful stop: exit 0 well inside the grace period.
start=$(date +%s)
docker stop -t 75 "$CID" >/dev/null
test "$(docker inspect -f '{{.State.ExitCode}}' "$CID")" = 0
test $(( $(date +%s) - start )) -lt 15
docker logs "$CID" 2>&1 | tail -n 5
docker rm "$CID" >/dev/null
CID=
echo "docker smoke test passed"
