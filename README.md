# ciallopa

Minimal Rust rewrite of CLIProxyAPI's Codex path: several ChatGPT-subscription accounts behind one
endpoint for Codex clients (Codex CLI, plus OpenAI Images API clients), meant to sit behind Caddy.
Network stack pinned to codex-rs (reqwest/rustls/hyper/h2/tokio-tungstenite fork), so upstream TLS,
h2 and WebSocket fingerprints equal Codex CLI 0.163.0 (`scripts/fp-check.sh`).
Interfaces: `docs/INTERFACES.md` · acceptance: `docs/ACCEPTANCE.md`.

## Build / gates
```
cargo build --release && cargo clippy --all-targets -- -D warnings && cargo test
scripts/lock-check.sh   # every dep version ⊆ codex-rs Cargo.lock
scripts/fp-check.sh     # JA4 + Akamai h2 + WS handshake == Codex CLI 0.163.0 (sudo; restores /etc/hosts)
```

## Accounts
```
ciallopa -c config.toml login                     # device code (default; best for servers)
ciallopa -c config.toml login --method paste      # open URL, paste the redirected callback URL
ciallopa -c config.toml login --method pkce       # local browser, callback on 127.0.0.1:1455
ciallopa -c config.toml login --method import --from ~/.codex/auth.json
```
Files land in `auth_dir` (plain JSON, 0600, CPA-compatible names). Optional per-account keys in the
file: `"priority": 5` (higher tier used first), `"weight": 3` (weighted-round-robin; ≤0 excluded),
`"disabled": true`. Changes in `auth_dir` are picked up automatically; the management page
(`/ui/accounts`) can do the same edits, delete accounts, import an `auth.json` or run the
device-code login when writes are enabled (see below).

## Config (`config.toml`)
```toml
listen = "127.0.0.1:8317"              # keep loopback; Caddy terminates TLS
# Plain string keys can only forward. Management needs a table with admin = true:
api_keys = [{ label = "laptop", key = "sk-cpa-change-me-0123456789", admin = true }]
instance_id = ""                        # default: hostname; stored per usage record
upstream = "https://chatgpt.com/backend-api"
auth_dir = "~/.ciallopa/auths"
proxy_url = ""                          # global egress: http:// | socks5h:// (https:// = HTTP only, not WS)
max_body_mb = 16
ws_compression = true                   # permessage-deflate on the client WebSocket when the client
                                        # offers it (Codex always does); applies to new connections

[routing]
strategy = "round-robin"                # | weighted-round-robin | fill-first
session_affinity = true
session_affinity_ttl = "1h"
session_affinity_subagents = true
session_affinity_rebind_on = "quota"    # | any
quota_avoid_threshold = 0.0             # 0 = off (default): switch only on upstream quota errors; e.g. 0.95 = new sessions skip accounts at/over 95% used

[upstream_opts]
stream_idle_timeout_secs = 300
max_attempts = 1    # per-request failovers for transport/408/5xx before the first byte: each one moves
                    # to ANOTHER usable account (≤2 upstream requests per client attempt). The same
                    # account is never retried locally: with no other usable account the upstream
                    # response (status, body, Retry-After) goes straight back and the client retries
                    # it with its own backoff (Codex: 4 request retries + 5 stream retries).
                    # 401/403/404/429 fail over to another account and do not consume this budget.
                    # WS handshakes are not retried by Codex, so they get up to 4 retries
                    # (another account first, else the same one with backoff) regardless.
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
main_model = "gpt-5.5"             # wrapper model for non-gpt-image-* Images requests
default_originator = "codex_exec"       # only for clients that don't send one
default_user_agent = "codex_exec/0.163.0"   # used when client UA doesn't start with "codex"
```

## Codex CLI client (`~/.codex/config.toml`)
```toml
model_provider = "cpa"
[model_providers.cpa]
name = "cpa"
base_url = "https://cpa.example.com/v1"   # or http://127.0.0.1:8317/v1 locally
env_key = "CPA_KEY"                       # export CPA_KEY=<one of api_keys>
wire_api = "responses"
supports_websockets = true                # needed for compression; false = HTTP SSE only

[model_providers.cpa.capabilities]
remote_compaction = "v2"                  # server-side compaction, same as the built-in openai provider
```
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
Notional cost: copy `prices.example.toml` to `~/.ciallopa/prices.toml` (`[usage] prices`);
records and summaries then carry `cost_usd` (null + `unknown_price` for unpriced models, never
0). It is what the tokens would cost on the API, not what a subscription bills. Refresh with
`ciallopa prices sync --from models.dev` (dry run; `--write` to apply). Records also keep
`service_tier`, cache-write tokens and the Codex `thread_id` / `turn_id`.

Web pages: `GET /ui` overview (`/` redirects), `/ui/settings` and `/ui/accounts`
(`[ui] enabled = true` by default). Each is a single static HTML file embedded in the binary (inline CSS/JS, no external resources) that asks
for the access key (kept in `sessionStorage`, sent as `Authorization: Bearer`, never in the
URL) and shows account quota cards (remaining % with a bar colored by remaining), a summary table (24h/7d/30d, by account/key/instance/model),
a daily token/cost chart and recent requests. Strict CSP (`default-src 'none'`, script/style
allowed by SHA-256 hash only, `connect-src 'self'`); all data rendered via `textContent`.
Local demo with fake data: `cargo run --example ui_demo` → http://127.0.0.1:8399/ui
(key `sk-demo-0123456789abcdef`; the demo writes its own `config.toml` with
`allow_config_write = true`, so the settings and accounts pages are editable).

Reset credits (重置卡) — **manual only, never automatic**. Each account's cards are listed
from upstream `GET /wham/rate-limit-reset-credits` (same headers as Codex CLI) every
`[ui] reset_credits_refresh_mins` (default 60, `0` = only on demand) and shown on the quota card
(count + nearest expiry). Read: `GET /usage/v1/reset-credits`; re-fetch:
`POST /usage/v1/reset-credits/refresh` (`{"account": "..."}` optional). Spending a card is
irreversible and is **off by default**; to enable the button / API:

```toml
[ui]
allow_reset_consume = true   # default false → POST below answers 403
```

`POST /admin/v1/accounts/{id}/reset-credits/consume` with an **admin** client key
(`Authorization: Bearer …`), `Content-Type: application/json` and `{"confirm": "<id>"}`
(optional `credit_id`, `redeem_request_id`). The page asks for confirmation in a dialog naming
the account. Each attempt is logged in the usage DB (`reset_consume`); a timeout / 5xx stays
`pending` and any further click on that account must retry the same attempt (HTTP 409 otherwise),
so a retry cannot spend twice. Needs `[usage] enabled`.

Internal upstream calls (token refresh, `/wham/*`, reset credits) reuse the `User-Agent` +
`originator` of the latest validated Codex client request (`GET /usage/v1/client-fingerprint`;
default `codex_cli_rs/0.163.0`). See docs/USAGE.md.

Details: docs/USAGE.md.

Token refresh is cross-process safe: under the file lock the auth file is re-read; a newer
token on disk is used instead of refreshing, and the write-back is skipped if another writer
changed the credentials meanwhile (CPA `refresh_patch` semantics). `refresh_token_reused` /
`invalid_grant` mark the account `needs_relogin` (excluded from routing, shown in
`/usage/v1/quota`, logged once, no retry loop) until you log in again.

## Live settings

`config.toml` is the source of truth and is applied without a restart: edits to the file (and to
the price table and `auth_dir`) are picked up automatically, and `SIGHUP` /
`POST /admin/v1/config/reload` do the same on demand. A file that fails validation is rejected
and the running config stays as it was. Keys that only work at startup (`listen`, paths,
`instance_id`, `upstream`, `max_body_mb`, usage writer sizing, `ui.enabled`) are reported as
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
    encode off
    reverse_proxy 127.0.0.1:8317 {
        flush_interval -1          # stream SSE immediately
        transport http {
            read_timeout 0
            write_timeout 0
        }
    }
}
```
WebSocket upgrades are proxied by Caddy automatically. Caddy forwards `Sec-WebSocket-Extensions`
both ways, so `permessage-deflate` is negotiated end to end between Codex and ciallopa.

## systemd (optional)
```
[Unit]
Description=ciallopa
After=network-online.target
[Service]
ExecStart=/usr/local/bin/ciallopa -c /etc/ciallopa/config.toml
User=ciallopa
Restart=on-failure
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/var/lib/ciallopa
[Install]
WantedBy=multi-user.target
```
(set `auth_dir = "/var/lib/ciallopa/auths"`). Docker: `FROM debian:13-slim`, copy the release
binary, `ca-certificates`, mount the auth dir as a volume, run with `--network host` or publish 8317 on loopback.

## Disclaimer / 免责声明

> [!WARNING]
> **Personal, non-commercial use only. Not affiliated with, endorsed by, or supported by OpenAI.**
>
> - Use this project only in compliance with OpenAI's [Terms of Use](https://openai.com/policies/terms-of-use/), [Usage Policies](https://openai.com/policies/usage-policies/) and the applicable laws of your jurisdiction. You alone are responsible for how you use it.
> - Only use ChatGPT accounts that you own. The Terms of Use state that you "may not share your account credentials or make your account available to anyone else", and prohibit attempts to "lease, sell or distribute" the Services or to "circumvent any rate limits or restrictions or bypass any protective measures". Do not use this project to share, pool, resell or sublease access to accounts, or to get around usage limits.
> - Do not distribute this project at scale, run it as a public or paid relay/API service, or use it for any commercial purpose. The authors have never authorized commercial use of this project.
> - For learning and research. Provided "as is" without warranty of any kind; the authors accept no liability for account suspension, data loss or any other damage arising from its use.
>
> **仅供个人、非商业用途，与 OpenAI 无任何关联，亦未获其认可或支持。**
>
> - 请仅在遵守 OpenAI [使用条款（Terms of Use）](https://openai.com/policies/terms-of-use/)、[使用政策（Usage Policies）](https://openai.com/policies/usage-policies/)及所在地法律法规的前提下使用本项目，使用后果由使用者自行承担。
> - 仅使用你本人拥有的 ChatGPT 账号。OpenAI 使用条款规定不得共享账号凭据或将账号提供给他人使用，不得出租、出售或分发其服务，不得规避速率限制或绕过保护措施。请勿用本项目共享、合租、转售或转租账号访问权限，或规避用量限制。
> - 禁止大规模分发本项目，禁止将其作为公开或付费的中转/API 服务运营，禁止任何商业用途。作者从未授权任何基于本项目的商业行为。
> - 本项目仅供学习与研究，按"原样"提供，不附带任何担保；因使用本项目导致的账号封禁、数据丢失或其他任何损失，作者概不负责。
