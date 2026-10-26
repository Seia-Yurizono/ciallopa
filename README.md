# ciallopa

A lightweight, self-hosted **AI gateway** in Rust. One endpoint for AI clients, with streaming
HTTP and WebSocket forwarding, account routing, request diagnostics and a built-in management UI.
Designed for predictable behavior and straightforward operation: one binary, local configuration
and SQLite usage records, behind a TLS reverse proxy.

- **Streaming first:** preserve incremental output, session affinity and upstream error semantics.
- **Operational clarity:** inspect requests, attempts, latency, token usage and account capacity.
- **Small deployment:** hot-reload settings, keep credentials on the server, and run as an
  unprivileged process or container.

The product direction is provider-independent. **Current protocol support** is the Responses
HTTP/SSE and WebSocket path, model listing and image endpoints described below; the implemented
upstream adapter is currently the Codex backend. Other providers and arbitrary API base URLs are
not yet supported. Client-specific setup documents today's compatibility, not a restriction on
the project's direction.

The current adapter originated from CLIProxyAPI's Codex path. Its network stack is pinned to
codex-rs; TLS, HTTP/2 and WebSocket compatibility is checked by `scripts/fp-check.sh`.

[Documentation](docs/README.md) · [Architecture](docs/ARCHITECTURE.md) · [UI](docs/UI.md) ·
[Metrics](docs/METRICS.md) · [Deployment](docs/DEPLOYMENT.md) · [Interfaces](docs/INTERFACES.md) ·
[Testing](docs/TESTING.md) · [Performance observations](docs/PERFORMANCE.md)

These documents follow `main`, which can contain unreleased changes. For an installed release,
use the documentation at its tag; pushing `main` does not update existing container images.

## Quick start (Linux x86_64 / arm64)
Each path ends with the server on `127.0.0.1:8317`, one account and a client key.
`ciallopa init` writes `config.toml` with a new random admin key and prints that key, the Codex
client config and the next commands; then [set up the client](#codex-cli-client).
The commands below use the x86_64 (`amd64`) files; on arm64 Linux use
`ciallopa_0.2.0-1_arm64.deb` or `ciallopa-0.2.0-aarch64-unknown-linux-musl.tar.gz`. The Docker
image is multi-arch, so Compose works unchanged on both, including Docker Desktop on Apple
Silicon Macs.

**.deb (Ubuntu / Debian)**
```
curl -fLO https://github.com/Seia-Yurizono/ciallopa/releases/download/v0.2.0/ciallopa_0.2.0-1_amd64.deb
sudo apt install ./ciallopa_0.2.0-1_amd64.deb
sudo ciallopa -c /etc/ciallopa/config.toml init
sudo -u ciallopa HOME=/var/lib/ciallopa ciallopa -c /etc/ciallopa/config.toml login
sudo systemctl start ciallopa
```

**Docker Compose**
```
mkdir ciallopa && cd ciallopa && mkdir -m 700 config data
curl -fsSLO https://raw.githubusercontent.com/Seia-Yurizono/ciallopa/v0.2.0/packaging/docker/compose.yaml
sed -i "s/^    user: .*/    user: \"$(id -u):$(id -g)\"/" compose.yaml
docker compose run --rm ciallopa init
docker compose up -d && docker compose exec ciallopa ciallopa login
```

**Tarball**
```
curl -fL https://github.com/Seia-Yurizono/ciallopa/releases/download/v0.2.0/ciallopa-0.2.0-x86_64-unknown-linux-musl.tar.gz | tar xz
cd ciallopa-0.2.0-x86_64-unknown-linux-musl
./ciallopa init
./ciallopa -c config.toml login
./ciallopa -c config.toml
```

`login` uses a device code by default: open the printed URL in any browser, sign in to the
ChatGPT account and enter the code. The management pages are at `http://127.0.0.1:8317/ui` and
ask for the admin key. On a remote server, reach them through an SSH tunnel from your own
machine and open the same URL there:
```
ssh -L 8317:127.0.0.1:8317 <server>
```
For clients on other machines put a TLS reverse proxy in front ([Caddy](#caddy)). Keep the
native `listen` address, or Docker's published host port, on loopback; the container itself
still listens on `0.0.0.0:8317`.

## Install (Linux x86_64 / arm64)
Releases ship a static binary for x86_64 and arm64 (aarch64) (musl, no runtime dependencies besides the system CA certificates)
as a tarball and as a Debian/Ubuntu package, with `SHA256SUMS`, at
`https://github.com/Seia-Yurizono/ciallopa/releases`. To check a download, fetch `SHA256SUMS`
from the same release and run `sha256sum -c --ignore-missing SHA256SUMS`.

### First run
- `ciallopa init` creates the config file that `-c` / `CIALLOPA_CONFIG` names (default
  `./config.toml`) from the bundled example, with its comments, and replaces only `api_keys` with
  one new random key labelled `admin` (`admin = true`, so it also opens the management pages).
  The file gets mode 0600; a missing parent directory is created with 0700. An existing file is
  never overwritten. Everything is printed to standard output once and the key is never logged.
  `CIALLOPA_INIT_LISTEN` sets `listen` in the new file (the container image sets
  `0.0.0.0:8317`).
- Every command that needs a config (the server, `login`, `healthcheck`, …) exits 1 when the
  file cannot be opened, whether or not `-c` was given, and says why: `no config file at <path>`
  with the `init` command to run, or `cannot read <path>: permission denied` with the uid:gid it
  ran as. On a machine with the `.deb`, the hints name the package's commands: `sudo ciallopa -c
  /etc/ciallopa/config.toml init`, then the same command as the `ciallopa` user (`sudo -u ciallopa
  HOME=/var/lib/ciallopa ciallopa -c /etc/ciallopa/config.toml …`) or `sudo systemctl start
  ciallopa` for the server; without `-c` they point there rather than at `./config.toml`. When
  `init` cannot write, it says to use `sudo` on the `.deb` layout and, in a container, which
  directories to `chown` or which `user:` to set. In a container, paths under the two mounts are
  shown with their host side as in the example compose file (`config/…`, `data/…`). A file that
  fails to parse or validate also exits 1 with the reason.
- Files and directories that ciallopa creates are readable by its own user only (umask 0077, as
  `UMask=0077` in the systemd unit): the config, its `.bak` and audit log, accounts, the usage
  database with its `-wal` / `-shm` files.
- The example's placeholder key `sk-cpa-change-me-0123456789` is rejected: the server does not
  start with it and a reload that introduces it is refused.
- With no accounts the server starts and warns `no accounts yet` in its log; requests then fail
  (see [Accounts](#accounts)) and the management pages show 没有可用账号.

### .deb
```
sudo apt install ./ciallopa_0.2.0-1_amd64.deb
sudo ciallopa -c /etc/ciallopa/config.toml init
sudo -u ciallopa HOME=/var/lib/ciallopa ciallopa -c /etc/ciallopa/config.toml login
sudo systemctl start ciallopa             # enabled at install; starts once config.toml exists
```
| Path | Content |
|---|---|
| `/usr/bin/ciallopa` | binary |
| `/etc/ciallopa/config.example.toml` | example config (dpkg conffile) |
| `/etc/ciallopa/config.toml` | the live config, created by `ciallopa init` |
| `/usr/lib/systemd/system/ciallopa.service` | systemd unit, runs as the `ciallopa` system user |
| `/var/lib/ciallopa` | `HOME` of the service: `~/.ciallopa/…` paths (accounts, usage DB, price table) resolve to `/var/lib/ciallopa/.ciallopa/…` |

The package creates no config and no key; it prints these steps after installation. The unit
has `ConditionPathExists=/etc/ciallopa/config.toml` and stays inactive until the file exists.
Run as root, `init` hands the new file to the `ciallopa` user (0600). Run `login` and other CLI
commands as the `ciallopa` user with `HOME=/var/lib/ciallopa`, as above, so the files land where
the service reads them. `sudo ciallopa login` refuses when the `ciallopa` user exists, because it
would save the account under `/root`, where the service cannot read it, and prints the command
above instead. `/etc/ciallopa` belongs to the `ciallopa` user (0750), so settings and account
writes from the management pages (`allow_config_write = true`) work as installed: they replace
`config.toml` through a temporary file and a rename in that directory, keep its mode and leave
the previous content in `config.toml.bak`.
`apt remove` stops the service and keeps the configuration and data. `apt purge` deletes
`/etc/ciallopa` (config, `.bak`, audit log) and `/var/lib/ciallopa` (accounts, usage log, price
table under the default paths). Data outside those two directories is left in place.
The `ciallopa` system user and group are always kept, so custom data paths and backups retain
their owner and their UID/GID cannot be reassigned to another account. Back up `/var/lib/ciallopa`
and any custom data paths first if they should be kept.

### Tarball
It contains `ciallopa`, `config.example.toml`, `README.md`, `LICENSE` and
`ciallopa.service` (the unit the package installs; it expects the binary in `/usr/bin`, the config
in `/etc/ciallopa/config.toml`, a `ciallopa` system user and `/var/lib/ciallopa`, see
[systemd](#systemd)). Accounts and the usage log go to `~/.ciallopa` of the user running it.

### Docker
The multi-arch image `ghcr.io/seia-yurizono/ciallopa` (`linux/amd64` and `linux/arm64`, so it
also runs natively under Docker Desktop on Apple Silicon) holds the same static binary on `gcr.io/distroless/static-debian13:nonroot`. It reads
`/etc/ciallopa/config.toml` (`CIALLOPA_CONFIG`), keeps data in `/var/lib/ciallopa/.ciallopa`
(`HOME=/var/lib/ciallopa`, so the example's `~/.ciallopa/…` paths need no change), writes
`listen = "0.0.0.0:8317"` on `init` and checks itself with `ciallopa healthcheck`. Using
[`packaging/docker/compose.yaml`](packaging/docker/compose.yaml) in an empty directory:

The current `v0.2.0` release uses image tag `0.2.0` (also `0.2`). The `latest` tag follows
successful tagged builds, including pre-releases. Only stable releases update
`<major>.<minor>`. Use an exact version or digest for controlled upgrades.

```
mkdir -m 700 config data
curl -fsSLO https://raw.githubusercontent.com/Seia-Yurizono/ciallopa/v0.2.0/packaging/docker/compose.yaml
sed -i "s/^    user: .*/    user: \"$(id -u):$(id -g)\"/" compose.yaml   # your uid:gid
docker compose run --rm ciallopa init           # writes config/config.toml, prints the key
docker compose up -d
docker compose exec ciallopa ciallopa login     # device code; --method paste works too
docker compose logs -f
```
The compose file runs the container as `user: "1000:1000"`, which the `sed` line replaces with
your own uid and gid. Running as the account that owns `config/` and `data/` means you edit the
config and back up the data without `sudo`, and the files the server writes stay yours; 1000
is the first regular user on most Linux installs. Create both
directories yourself before the first `docker compose` command: a directory Docker creates for a
missing mount belongs to root and the container cannot write to it. Without `user:` the image
runs as its own uid 65532; then create the directories with
`sudo install -d -o 65532 -g 65532 -m 700 config data`.

Accounts added with `login` (or on the management page) appear without a restart. `--method pkce`
does not work in a container, because its callback listener on `127.0.0.1:1455` is not reachable
from a browser. To import a Codex `auth.json`, use 导入文件 on the management page. When Compose
runs as your host uid:gid as above, you can also copy it to `data/auth.json`, run
`docker compose exec ciallopa ciallopa login --method import --from /var/lib/ciallopa/.ciallopa/auth.json`
and delete the copy. Upgrade with `docker compose pull && docker compose up -d`.

Codex account files from CLIProxyAPI (`codex-*.json`, `"type": "codex"`) work as they are. Upload
them with 导入文件 on the 账号 page, which checks each file and writes it itself. With the default
paths and Compose running as your host uid:gid, you can also copy them into `data/auths/`:
```
cp ~/.cli-proxy-api/codex-*.json data/auths/
sudo chown "$(id -u):$(id -g)" data/auths/*.json && chmod 600 data/auths/*.json
```
The ownership command also fixes files previously copied with `sudo cp`. If you use the image's
default uid 65532, use the page's 导入文件 instead: it writes both formats with the container's
ownership without opening up the private host directories. The account directory is watched,
so accounts appear without a restart (重新扫描目录 does the same on demand). Files of other
CLIProxyAPI providers (`claude`, `gemini`, …) are ignored.

Things the compose file sets on purpose:
- **The config directory is mounted, not the file.** Management-page writes replace `config.toml`
  by renaming a temporary file over it and keep `config.toml.bak` and `config.toml.audit.jsonl`
  beside it. A rename cannot replace a single-file bind mount, so with one the save fails with
  `Device or resource busy` (or `Read-only file system`) and the error says to mount the
  directory; an editor that saves by rename would also leave the container on the old file.
- **`stop_grace_period: 75s`.** On `SIGTERM`, HTTP/SSE requests and active WebSocket turns get
  up to 60 s to finish, followed by bounded close, flush and runtime shutdown. Docker's
  default 10 s can kill active work before that drain completes.
- **`hostname: ciallopa`.** `instance_id` in usage records defaults to the hostname, which in a
  container is otherwise a new random ID on every recreate.

Inside the container `listen` must be `0.0.0.0:8317`; `init` writes that, and the server warns at
startup when it listens on loopback inside an application container (Docker or Podman marker
files, or a Docker, containerd, Kubernetes or libpod cgroup). System containers such as LXC or
OpenVZ VPSes are treated as hosts. The port is published on `127.0.0.1`
only, for the reverse proxy on the host or an SSH tunnel. The root filesystem is read-only and
all capabilities are dropped; nothing outside the two mounts is written. See
[Deployment](docs/DEPLOYMENT.md) for image provenance, backups, upgrades and shutdown limits.

## Build / checks

```sh
cargo build --release --locked
```

See [Testing](docs/TESTING.md) for focused development commands and the shared PR checks.
Transport fingerprints, browser smoke, benchmarks and packaging checks run when relevant to
the change. The UI regression script uses Node.js and its standard library.

Pushing a `v*` tag runs `.github/workflows/release.yml`: fmt, clippy and tests, then
`scripts/package.sh`, a systemd smoke test of the `.deb`, the container image built from the
tarball (`packaging/docker/Dockerfile`) and smoke-tested with `scripts/docker-smoke.sh`, a push of
the image to GHCR, and a GitHub Release with the packages and `docs/RELEASE_NOTES.md` as its
text. Running the workflow by hand (`workflow_dispatch`) does the same without the push and the
release and keeps the packages as workflow artifacts. Maintainer steps: `docs/RELEASING.md`.

## Accounts
```
ciallopa -c config.toml login                     # device code (default; best for servers)
ciallopa -c config.toml login --method paste      # open URL, paste the redirected callback URL
ciallopa -c config.toml login --method pkce       # local browser, callback on 127.0.0.1:1455
ciallopa -c config.toml login --method import --from ~/.codex/auth.json
```
`login` first prints the directory the account is saved to: `auth_dir` (plain JSON, 0600,
CPA-compatible names; the directory is created with 0700 when missing). Optional per-account
keys in the file: `"priority": 5` (higher tier used first), `"weight": 3` (weighted-round-robin;
≤0 excluded), `"disabled": true`. Changes in `auth_dir` are picked up automatically; the
management page (`/ui/accounts`) can do the same edits, delete accounts, import account files
(several at once; an account already present is left unchanged) or run the device-code login
when writes are enabled (see below).

### How an account is chosen
A request that continues a bound session (same session and model, `routing.session_affinity`
on) stays on its account while that account is usable. Every other request picks an account
in three steps:

1. Drop disabled accounts and accounts cooling down for this model. With
   `weighted-round-robin`, accounts with weight ≤ 0 are dropped too.
2. With `routing.quota_avoid_threshold` above 0, skip accounts whose last observed usage in
   any unexpired quota window is at or over the threshold (unless that would skip all of them).
3. Keep only the highest `priority` tier and apply `routing.strategy`:
   - `round-robin` (default): take turns in account-id order, with a separate cursor per model;
     weights are ignored.
   - `weighted-round-robin`: share requests in proportion to each account's `weight`.
   - `fill-first`: always the first usable account in account-id order; the next one is used
     only while it is cooling down or disabled.

When the bound account cannot serve a request, `routing.session_affinity_rebind_on` decides
whether the binding moves: `quota` (default) moves it for quota, token, 402 or deactivation problems,
while short failures such as repeated 5xx only borrow another account until the original one
recovers; `any` moves it whenever the bound account is unusable.

Bindings survive a restart: they are saved to `session_affinity.json` next to the usage
database (mode 0600; at most every 30 s and only after a change, plus once at shutdown) and
loaded at startup. Expired bindings and bindings to accounts that are no longer configured are
dropped, and no binding outlives the current `routing.session_affinity_ttl`.

A `*.json` file in `auth_dir` that is not loaded is listed on the 账号 page with the reason,
until it is fixed or removed: the server user may not read it (with its owner, mode and a
`chown` command for the server's uid:gid), it is not valid JSON, it has no `access_token` (a Codex
CLI `auth.json` goes through import instead), or it belongs to another provider (ignored). The
log names each such file once, and again only when it changes.
Only a file that is gone removes its account: if an already loaded account's file becomes
unreadable or invalid (for example half-written by another tool), the account keeps its last good
content, cooldowns, session bindings and connections until the file is fixed or deleted.

While no account is usable (none added yet, or all disabled or waiting for a new login), proxied
requests are answered with HTTP 503 `server_is_overloaded` ("The server is overloaded or not
ready yet. Please retry your request."). That is a response the upstream itself gives; Codex's
recovery depends on the error and retry advice. An answer naming the proxy or its accounts would
tell the client that a proxy sits in between, so the reason goes to the operator instead: a `WARN` in the log at
startup (`no accounts yet`) and at most once a minute while requests are rejected, and a
没有可用账号 banner on the management pages.

While every account is cooling down after upstream errors, a request waits up to
`routing.cooldown_wait_secs` (default 20) for the first one to recover and is then routed
normally. When the first recovery is further away, the answer comes at once: 429
`usage_limit_reached` with that reset time when the account that recovers first is out of usage
quota (Codex shows "You've hit your usage limit. Try again at …"), otherwise 503
`server_is_overloaded` without `Retry-After` (Codex 0.163/0.164 can terminate without further
sampling retries). The log and usage records name the reason and the time to recovery.

Clients never receive a `Retry-After` of 0 or above 20 s on a 429 / 5xx (from the upstream or
from ciallopa). This bounds each forwarded delay; it does not bound an entire turn.
Codex has separate HTTP retries, stream recovery, transport fallback and connection recovery;
their combined cost depends on the error path and client version. See the versioned
[gateway and Agent network research](docs/RESEARCH.md) for the measured behavior and remaining checks.

## Config (`config.toml`)
The file is named by `-c` / `--config` or `CIALLOPA_CONFIG` (default `./config.toml`) and is
created by `ciallopa init` (see [First run](#first-run)). `ciallopa healthcheck` exits 0 when
the server on the configured `listen` address answers `GET /healthz` (an unspecified address is
probed on loopback) and 1 otherwise, within 3 s.
```toml
listen = "127.0.0.1:8317"              # keep loopback; Caddy terminates TLS
# Plain string keys can only forward. Management needs a table with admin = true.
# The placeholder below is rejected; `ciallopa init` writes a random key instead.
api_keys = [{ label = "laptop", key = "sk-cpa-change-me-0123456789", admin = true }]
instance_id = ""                        # default: hostname; stored per usage record
upstream = "https://chatgpt.com/backend-api"
auth_dir = "~/.ciallopa/auths"
proxy_url = ""                          # global egress: http:// | socks5h:// (https:// = HTTP only, not WS);
                                        # socks5:// is used as socks5h:// (the proxy resolves host names)
proxy_enabled = true                    # false = keep proxy_url but connect directly
ipv6 = false                            # IPv4 only (direct and to the proxy host); true = IPv6 too, Happy Eyeballs
max_body_mb = 64                        # incoming HTTP body limit in MiB
max_buffered_body_mb = 256              # buffered HTTP bodies + WS messages; full: wait 1 s, then 503
max_inflight_requests = 64              # HTTP response / running WS turn; 0 disables rejection
                                        # at capacity: 503 + Retry-After: 3, no queue; hot-reloadable
ws_compression = true                   # permessage-deflate on the client WebSocket when the client
                                        # offers it (Codex always does); applies to new connections
ws_max_connections = 24                 # client WebSockets open at once (idle too); over: handshake 503,
                                        # Codex then uses HTTP; 0 = unlimited; new connections
ws_max_message_mb = 16                  # largest client WS message after inflating; over: close 1009

[routing]
strategy = "round-robin"                # | weighted-round-robin | fill-first
session_affinity = true
session_affinity_ttl = "24h"
session_affinity_subagents = true
session_affinity_rebind_on = "quota"    # | any
quota_avoid_threshold = 0.0             # 0 = off (default): switch only on upstream quota errors; e.g. 0.95 = new sessions skip accounts at/over 95% used
cooldown_wait_secs = 20                 # every account cooling and one recovers within this: hold the
                                        # request until it does (WS handshakes up to 10 s); 0 = never hold

[upstream_opts]
stream_idle_timeout_secs = 300
response_header_timeout_secs = 300  # each HTTP attempt: upload + wait for response headers
preparation_timeout_secs = 600      # shared across HTTP retries/account switches: credentials,
                                    # headers, error bodies and retry waits; successful SSE
                                    # bootstrap/generation time is not counted
# Buffered errors are capped at 1 MiB / 10 s; models at 8 MiB / 10 s. Normal streams use only idle timeout.
max_attempts = 1    # HTTP failovers for transport/408/5xx before output: each budgeted switch moves
                    # to another usable account. With no alternative or no remaining budget, return
                    # the last response (status, body, Retry-After ≤ 20 s) for the client's backoff.
                    # This is not a total attempt limit: account errors (401/402/403/404/429) and
                    # recognized overload use separate rules. Overload may retry once on the last
                    # usable account, controlled by overload_retry_wait_secs below.
                    # Gateway WS handshakes have a separate budget of up to 4 retries
                    # (another account first, else the same one with backoff).
                    # Codex may also recover or fall back to HTTP; these budgets are independent.
                    # In-stream quota errors (usage_limit_reached etc.) before the first output
                    # event: account cooled + request replayed on the next account (SSE and WS).
                    # WS after output / incremental turn: frame withheld, close 1012 ⇒ Codex
                    # reconnects with full input on another account.
transient_cooldown_secs = 15   # upstream 5xx: fail over to another account if one is usable; cool an account only
                               # after 3 consecutive 5xx (Retry-After wins, max 60 s). The last usable
                               # account is never cooled. 0 = never cool on 5xx.
overload_streak = 2            # overloaded / "at capacity" before output: replayed on another usable account;
overload_cooldown_secs = 120   # after 2 in a row the account cools 120 s, doubling up to
overload_cooldown_max_secs = 600 # 600 s; output resets it. Last usable account never cooled.
overload_retry_wait_secs = 3   # no other usable account: wait ~2–5 s, retry once on it (0 = off)
deactivated_cooldown_secs = 86400    # 402 deactivated_workspace: switch account, cool it 24 h
payment_required_cooldown_secs = 1800 # other 402: switch account, cool it 30 min
not_found_cooldown_secs = 600  # 404 not naming the model as unsupported: cool that model 10 min
                               # (explicit "model not supported" 404: 12 h). 0 = switch, no cooldown.
connect_timeout_secs = 30
compress_requests = true   # zstd level 3 on plaintext HTTP POST /responses bodies (incl. compaction
                           # turns), like official Codex with ChatGPT auth. Already-encoded bodies,
                           # /responses/compact, /models, images, passthrough and WS are never touched.
                           # Client Accept-Encoding is always stripped on /responses so upstream
                           # SSE stays identity (usage/TTFT parsing). Upstream set-cookie is never
                           # returned to clients (HTTP and WS handshake).
cloudflare_cookies = false # true = per-account in-memory jar of ChatGPT infrastructure cookies
                           # (__cf_bm, __cflb, _cfuvid, cf_clearance, cf_chl_*, __oailb …, the
                           # official Codex allowlist), replayed on that account's HTTP, WS
                           # handshake and internal requests to https ChatGPT hosts only, expiry
                           # respected, never shared between accounts. Client Cookie headers are
                           # never forwarded upstream, enabled or not.

[images]
enabled = true
main_model = "gpt-6.1-sol"         # wrapper model for non-gpt-image-* Images requests
default_originator = "codex_exec"       # only for clients that don't send one
default_user_agent = "codex_exec/0.164.0"   # used when client UA doesn't start with "codex"
```

## Codex CLI client
`ciallopa init` prints this block with the right port. In `$CODEX_HOME/config.toml` (default
`~/.codex/config.toml`; `model_provider` belongs above the first `[table]`):
```toml
model_provider = "cpa"
[model_providers.cpa]
name = "cpa"
base_url = "https://cpa.example.com/v1"   # or http://127.0.0.1:8317/v1 locally / through ssh -L
env_key = "CPA_KEY"                       # the environment variable holding one of api_keys
wire_api = "responses"
supports_websockets = true                # needed for compression; false = HTTP SSE only

[model_providers.cpa.capabilities]
remote_compaction = "v2"                  # server-side compaction, same as the built-in openai provider
```
Codex reads `CPA_KEY` from its environment and from `$CODEX_HOME/.env` (default
`~/.codex/.env`). Every Codex process reads the `.env` file at start, including the
app-server daemon, which does not see your shell profile:
```
echo 'CPA_KEY=sk-cpa-…' >> ~/.codex/.env && chmod 600 ~/.codex/.env
codex app-server daemon restart           # a running daemon reads the key only at start
```
Exporting `CPA_KEY` in the shell profile works for `codex` started from that shell. A custom
provider with `env_key` needs no ChatGPT login on the client: `~/.codex/auth.json` is not used
for it, and `requires_openai_auth` must stay unset. Check the key and the address:
```
curl -fsS -H "Authorization: Bearer $CPA_KEY" http://127.0.0.1:8317/v1/models
```
It prints the model list once an account works; 401 means the key is wrong, and 503 that no
account is usable yet (see [Accounts](#accounts)).

Conversations started under Codex's own ChatGPT login cannot be continued through ciallopa, and
the other way round. Their history holds reasoning encrypted for that login's account, which the
proxy's accounts cannot decrypt; resuming one fails with `invalid_encrypted_content`. Start a
new session after switching.

Set `supports_websockets = true`. With a custom provider Codex never compresses HTTP request
bodies and never accepts compressed SSE, so over HTTP every turn sends the whole history
uncompressed. Over WebSocket Codex offers `permessage-deflate`, ciallopa accepts it
(`ws_compression`), and a connection that stays open sends only the new input each turn. On a
long conversation this saves about 30% of the upload (most of the history is encrypted reasoning,
which barely compresses) and about 80% of the streamed response. The client WebSocket is
served over HTTP/1.1 Upgrade; HTTP/2 extended CONNECT is not offered.

Without `remote_compaction = "v2"` Codex treats a custom provider as unsupported and compacts
locally (summarization prompt as a normal turn). With it (Codex >= 0.16x, `compact_remote_v2`),
compaction is an ordinary `/responses` turn (SSE or WS) whose input ends with
`{"type":"compaction_trigger"}`, header `x-codex-beta-features: remote_compaction_v2`; the reply
streams `response.compaction.compacting` and one `{"type":"compaction","encrypted_content":…}`
item. ciallopa forwards it unchanged. The `/responses/compact` route is also forwarded (upstream
answers 404 for it). Keep `session_affinity` on: compacted history is replayed to the bound account.

Encrypted reasoning and compaction items are bound to the account (organization) that produced
them. When a conversation's history reaches an account that cannot decrypt it, the upstream
rejects the request with `invalid_encrypted_content`. ciallopa, like Codex itself, does not strip
or rewrite history and does not retry or switch accounts for it; the client gets a clear
`invalid_encrypted_content` error asking to start a new session (see
[docs/USAGE.md](docs/USAGE.md#encrypted-reasoning-rejected-invalid_encrypted_content)).
Endpoints: `/backend-api/codex/{responses,responses/compact,models}` (+ WS on `responses`),
`/backend-api/*` passthrough (`wham/*` …), `/api/codex/*`→`/wham/*`, `/v1/{responses,responses/compact,models}`,
`/v1/images/{generations,edits}`, `/healthz` (no auth). Everything else 404.

## Usage log
Per-request records (account, model, effort, transport, status/error, retries/failover,
TTFT, duration, tokens) go to a local SQLite file without ever blocking forwarding. Read-only
API behind the client key: `GET /usage/v1/records`, `GET /usage/v1/summary?range=24h&group=key`,
`GET /usage/v1/quota` (latest upstream used %, reset times, capacity estimate, `needs_relogin`
flags; the % includes other clients/instances of the account). Records carry the client
key's label (never the key), `instance_id` and the quota snapshot.
```toml
[usage]
enabled = true
path = "~/.ciallopa/usage.db"
retention_days = 90   # 0 = keep forever
```
Notional cost: records and summaries carry `cost_usd` from the local price table
`~/.ciallopa/prices.toml` (`[usage] prices`): what the tokens would cost on the API, not what a
subscription bills. Unpriced models give `cost_usd: null` + `unknown_price`, never 0. A new
install has no table; the overview and settings pages then say which models are unpriced. Prices
are pulled on request only: 「检查更新」 on the settings page 「模型价格」 (`/ui/settings#prices`) downloads the
dataset in `[usage] prices_source` (default: LiteLLM's `model_prices_and_context_window.json`),
lists the added and changed models, and 「应用」 writes them. The CLI does the same with
`ciallopa prices sync` (dry run; `--write` to apply). Records also keep `service_tier`,
cache-write tokens and the Codex `thread_id` / `turn_id`.

Web pages: `GET /ui` overview (`/` redirects), `/ui/settings` and `/ui/accounts`
(`[ui] enabled = true` by default). Each is a single static HTML file embedded in the binary
(inline CSS/JS, no external resources). They share one login: the access key is kept in the
tab's `sessionStorage`, sent as `Authorization: Bearer` and never put in the URL. The overview
has a time-range / grouping bar (24h/7d/30d; by account/key/instance/model) that drives the KPI
strip, the summary table, the trend chart (requests, errors, tokens) and the recent requests.
Its quota table shows the 5-hour and weekly remaining % per account, the reset time, a warning
when the quota data is more than an hour old, and the reset-card count. The 账号 page shows each account's card count and 「已达额度上限」 when
the last 刷新额度 reported it. Quota data comes from
upstream response headers whenever an account serves a request; 刷新额度 (per account, in the
reset-card panel of the overview and in the account's actions) and 刷新全部额度 ask upstream
directly instead (`GET /wham/usage`, which the Codex backend client reads rate limits from: read-only, spends no
usage), at most once per account every 30 s and four accounts at a time. Optional background
刷新额度 (`[ui] quota_refresh_mins`, default off) runs the same path for accounts whose
「自动刷新额度」 switch is on. Strict CSP (`default-src 'none'`, script/style
allowed by SHA-256 hash only, `connect-src 'self'`); all data rendered via `textContent`.
Local demo with fake data: `cargo run --example ui_demo` → http://127.0.0.1:8399/ui
(key `sk-demo-0123456789abcdef`; the demo writes its own `config.toml` with
`allow_config_write = true`, so the settings and accounts pages are editable).

Reset credits (重置卡) — **manual, or automatic only where you turn it on per account**. Each account's cards are listed
from upstream `GET /wham/rate-limit-reset-credits` (same headers as Codex CLI) every
`[ui] reset_credits_refresh_mins` (default 0 = only on demand; 1–60 when on), including disabled accounts
(they only leave routing), and shown in the quota table (count; amber when one expires within
24 h; the popover lists every card). Read: `GET /usage/v1/reset-credits`; re-fetch:
`POST /usage/v1/reset-credits/refresh` (`{"account": "..."}` optional). Spending a card is
irreversible and is **off by default**; to enable the button / API:

```toml
[ui]
allow_reset_consume = true   # default false → POST below answers 403
```

`POST /admin/v1/accounts/{id}/reset-credits/consume` with an **admin** client key
(`Authorization: Bearer …`), `Content-Type: application/json` and `{"confirm": "<id>"}`
(optional `credit_id`, `redeem_request_id`). The page asks for confirmation in a dialog naming
the account, the card, and the current 5-hour / weekly remaining with their natural reset time. Each attempt is logged in the usage DB (`reset_consume`); a timeout / 5xx stays
`pending` and any further click on that account must retry the same attempt (HTTP 409 otherwise),
so a retry cannot spend twice. Needs `[usage] enabled`. A `reset` answer drops the account's
usage-limit cooldown at once, so it takes traffic again on the next request.

Per account, the 账号 page switch 「快到期时自动使用」 (default off, needs `allow_config_write`, not
`allow_reset_consume`) uses the earliest-expiring card `[ui] reset_credits_auto_lead_mins`
(default 10, 5–1440) minutes before it expires, through the same lock and consume path as the
button after re-listing the cards upstream; disabled / re-login accounts are skipped,
`nothing_to_reset` gives up on that card, other failures retry at most 5 times until expiry.

Internal upstream calls (token refresh, `/wham/*`, reset credits) reuse the `User-Agent` +
`originator` of the latest validated Codex client request (`GET /usage/v1/client-fingerprint`;
default `codex_cli_rs/0.164.0`). See docs/USAGE.md.

Details: docs/USAGE.md.

Token refresh is cross-process safe: under the file lock the auth file is re-read; a newer
token on disk is used instead of refreshing, and the write-back is skipped if another writer
changed the credentials meanwhile (CPA `refresh_patch` semantics). `refresh_token_reused` /
`invalid_grant` mark the account `needs_relogin` (excluded from routing, shown in
`/usage/v1/quota`, logged once, no retry loop) until you log in again. Like Codex 0.164, the
refresh request carries `refresh_reason`: `proactive_expiry` (access token expires within
5 minutes), `scheduled` (`last_refresh` older than 8 days), `unauthorized_recovery` (after an
upstream 401) or `forced` (the admin "refresh" action).

## Live settings

`config.toml` is the source of truth and is applied without a restart: edits to the file (and to
the price table and `auth_dir`) are picked up automatically, and `SIGHUP` /
`POST /admin/v1/config/reload` do the same on demand. A file that fails validation is rejected
and the running config stays as it was. Keys that only work at startup (`listen`, paths,
`instance_id`, `upstream`, usage writer sizing, `ui.enabled`) are reported as
pending until the next restart.

`/ui/settings` shows every setting with its running value and a 热更新 / 需重启 badge, the
pending-restart list, the client keys (label + last 4 characters) and the audit log.
`/ui/accounts` lists accounts with status, weight, priority and last-24h usage. Both pages are
read-only unless the file says:

```toml
[ui]
allow_config_write = true   # default false; file only, the page cannot turn it on
```

Then the settings page edits the file (preview diff → `PUT` with `If-Match`, comments kept,
`<file>.bak` written), generates or deletes client keys (a new key is shown once and is never
admin — set `admin = true` in the file for management), and the accounts page enables / disables,
sets weight and priority, deletes (with confirmation), imports and logs in accounts and refreshes
tokens. Every change is audited in `<config file>.audit.jsonl`. Details: docs/SETTINGS.md.

Management (`/admin/v1/*`: settings, accounts, keys, reset-credit consume and on-demand
reset-credit refresh) requires a client key with `admin = true`. Plain / labeled keys without
that flag may only forward and call `/usage/v1/*` reads. If no admin key is configured, every
management call answers 403 (there is no fallback to “any key”). Reload does **not** need
`allow_config_write`, but it does need an admin key.

Authentication headers: when both `X-Api-Key` and `Authorization: Bearer` are present, ciallopa
uses `X-Api-Key`. Send only one.

## Caddy
```
cpa.example.com {
    reverse_proxy 127.0.0.1:8317
}
```
Caddy [flushes SSE responses automatically](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy#streaming),
using their `Content-Type: text/event-stream`. This minimal configuration keeps its default
streaming and request-cancellation behavior; verify cancellation through your complete proxy
chain when adding custom transport or buffering settings.
WebSocket upgrades are proxied by Caddy automatically. Caddy forwards `Sec-WebSocket-Extensions`
both ways, so `permessage-deflate` is negotiated end to end between Codex and ciallopa.

## Cloudflare Tunnel / Access
Put `/ui*`, `/admin/v1/*` and `/usage/v1/*` behind Access; keep `/v1/*`, `/backend-api/*`,
`/api/codex/*` and `/healthz` off the interactive login (separate hostname or Bypass path, or a
service token sent by Codex through `env_http_headers`). The client key is still required
everywhere. Details: [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md#behind-cloudflare-tunnel-and-access).

## systemd
The `.deb` installs `packaging/ciallopa.service` (see Install): `User=ciallopa`,
`HOME=/var/lib/ciallopa` (`StateDirectory`), `ProtectSystem=strict` with `/etc/ciallopa` and
`/var/lib/ciallopa` writable, `ProtectHome`, `NoNewPrivileges`, an empty capability set, a
`@system-service` syscall filter and `TimeoutStopSec=75` for the bounded HTTP/SSE/WebSocket drain
and final flush on stop.
For a tarball install, copy the binary to `/usr/bin`, create the user and directories the package
would (`useradd --system --home-dir /var/lib/ciallopa --shell /usr/sbin/nologin ciallopa`,
`/etc/ciallopa` and `/var/lib/ciallopa` as `ciallopa:ciallopa` 0750), install the unit to
`/etc/systemd/system/` and continue as for the `.deb` with `init` and `login`.
For containers see Install → Docker.

## Disclaimer / 免责声明

This project is licensed under the [MIT License](LICENSE), which permits commercial use,
modification and distribution. Include the copyright and permission notices in all copies
or substantial portions of the software. It is provided "as is", without warranty; see
the license for the full terms.

Not affiliated with, endorsed by, or supported by any AI service provider. Access to connected
services remains subject to their own terms and applicable law; use accounts and credentials
you are authorized to use.

本项目采用 [MIT 许可证](LICENSE)，允许商业使用、修改和分发。软件的所有副本或实质性部分中
须保留版权声明和许可声明。软件按“原样”提供，不附带任何担保；完整条款见许可证。

本项目与任何 AI 服务厂商均无关联，亦未获其认可或支持。所接入服务的访问仍受其自身条款及
适用法律约束；请使用你获授权使用的账号和凭据。
