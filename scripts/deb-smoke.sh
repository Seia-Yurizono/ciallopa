#!/usr/bin/env bash
# Smoke test of the .deb on a systemd host (used as-is by .github/workflows/release.yml): the
# first-run flow (postinst hints, the unit waiting for a config, `init`, the root login guard),
# then the service: health, a settings write, systemd reload, remove and purge.
#   scripts/deb-smoke.sh dist/ciallopa_<version>-1_amd64.deb     (needs sudo, curl, jq)
set -euxo pipefail

DEB=$(realpath "${1:?usage: deb-smoke.sh <package.deb>}")
CFG=/etc/ciallopa/config.toml
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"

sudo apt-get install -y -qq "$DEB" 2>&1 | tee apt.log
grep -q 'sudo ciallopa -c /etc/ciallopa/config.toml init' apt.log
sudo stat -c '%U:%G %a %n' /etc/ciallopa /var/lib/ciallopa
# No config yet: the unit's condition keeps it inactive, the binary points at init.
sudo systemctl start ciallopa
test "$(systemctl is-active ciallopa || true)" = inactive
# Without -c on a machine with the package: points at the package's config, not ./config.toml.
if ciallopa 2>err; then exit 1; fi
grep -q "The ciallopa service uses /etc/ciallopa/config.toml" err
grep -q 'sudo systemctl start ciallopa' err
grep -q 'sudo ciallopa -c /etc/ciallopa/config.toml init' err
# init without sudo cannot write /etc/ciallopa and says to use sudo.
if ciallopa -c "$CFG" init 2>err; then exit 1; fi
grep -q 'permission denied' err
grep -q 'sudo ciallopa -c /etc/ciallopa/config.toml init' err

sudo ciallopa -c "$CFG" init | tee init.out >/dev/null
KEY=$(grep -oE 'sk-cpa-[0-9a-f]{48}' init.out | head -n 1)
test -n "$KEY"
grep -q 'sudo -u ciallopa HOME=/var/lib/ciallopa ciallopa -c /etc/ciallopa/config.toml login' init.out
test "$(sudo stat -c '%U:%G %a' "$CFG")" = "ciallopa:ciallopa 600"
if sudo ciallopa -c "$CFG" init 2>err; then exit 1; fi
grep -q 'already exists; not overwritten' err
# The config exists but this user cannot read it: says so and names the service-user command.
if ciallopa -c "$CFG" login 2>err; then exit 1; fi
grep -q 'cannot read /etc/ciallopa/config.toml: permission denied' err
grep -q 'sudo -u ciallopa HOME=/var/lib/ciallopa ciallopa -c /etc/ciallopa/config.toml login' err
# login as root is refused with the right command; as the service user it names the auth dir
# first (the unreachable issuer stops it before any device-code request).
if sudo ciallopa -c "$CFG" login 2>err; then exit 1; fi
grep -q 'sudo -u ciallopa HOME=/var/lib/ciallopa ciallopa -c /etc/ciallopa/config.toml login' err
if sudo -u ciallopa HOME=/var/lib/ciallopa CIALLOPA_OAUTH_ISSUER=http://127.0.0.1:9 \
    ciallopa -c "$CFG" login 2>&1 < /dev/null | tee login.out; then exit 1; fi
grep -q 'Accounts are saved to /var/lib/ciallopa/.ciallopa/auths' login.out

# Test port and settings writes, edited in place (owner and mode kept).
sudo sh -c "sed -e 's/^listen = \"127.0.0.1:8317\"/listen = \"127.0.0.1:18317\"/' \
    -e 's/^allow_config_write = false/allow_config_write = true/' $CFG > $CFG.new &&
    cat $CFG.new > $CFG && rm $CFG.new"
sudo systemctl start ciallopa
for _ in $(seq 20); do curl -fsS http://127.0.0.1:18317/healthz && break; sleep 0.5; done
curl -fsS http://127.0.0.1:18317/healthz
systemctl is-active ciallopa
# Settings write from the management API (temp file + rename in /etc/ciallopa).
version=$(curl -fsS -H "X-Api-Key: $KEY" http://127.0.0.1:18317/admin/v1/config | jq -r .version)
curl -fsS -X PUT -H "X-Api-Key: $KEY" -H "If-Match: \"$version\"" -H 'Content-Type: application/json' \
    -d '{"changes":{"routing.strategy":"fill-first"}}' http://127.0.0.1:18317/admin/v1/config
sudo grep -q '^strategy = "fill-first"' "$CFG"
test "$(sudo stat -c '%U %a' "$CFG")" = "ciallopa 600"
# The unit delivers SIGHUP; wait for the application to confirm the asynchronous reload.
service_pid=$(systemctl show ciallopa --property MainPID --value)
sudo systemctl reload ciallopa
for _ in $(seq 20); do
    curl -fsS -H "X-Api-Key: $KEY" http://127.0.0.1:18317/admin/v1/config > config.json
    if jq -e '.last_reload.source == "sighup" and .last_reload.ok' config.json; then break; fi
    sleep 0.5
done
jq -e '.last_reload.source == "sighup" and .last_reload.ok' config.json
test "$(systemctl show ciallopa --property MainPID --value)" = "$service_pid"
sudo systemctl stop ciallopa
sudo journalctl -u ciallopa --no-pager | tail -n 20
sudo journalctl -u ciallopa --no-pager | grep -q 'no accounts yet'
# Files the service created are private to it (UMask=0077).
test -z "$(sudo find /var/lib/ciallopa -perm /077 ! -path /var/lib/ciallopa)"

# remove keeps configuration and data; purge removes package data and keeps its owner.
sudo apt-get remove -y -qq ciallopa
sudo test -f "$CFG"
sudo test -d /var/lib/ciallopa
sudo apt-get purge -y -qq ciallopa
test ! -e /etc/ciallopa
test ! -e /var/lib/ciallopa
getent passwd ciallopa
getent group ciallopa
echo "deb smoke test passed"
