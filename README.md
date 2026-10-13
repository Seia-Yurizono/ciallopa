# ciallopa

Minimal Rust rewrite of CLIProxyAPI's Codex path: several ChatGPT-subscription accounts behind one
endpoint for Codex clients (Codex CLI, plus OpenAI Images API clients), meant to sit behind Caddy.
Network stack pinned to codex-rs (reqwest/rustls/hyper/h2/tokio-tungstenite fork), so upstream TLS,
h2 and WebSocket fingerprints equal Codex CLI 0.162.1 (`scripts/fp-check.sh`).
Interfaces: `docs/INTERFACES.md` · acceptance: `docs/ACCEPTANCE.md`.

## Build / gates
```
cargo build --release && cargo clippy --all-targets -- -D warnings && cargo test
scripts/lock-check.sh   # every dep version ⊆ codex-rs Cargo.lock
scripts/fp-check.sh     # JA4 + Akamai h2 + WS handshake == Codex CLI 0.162.1 (sudo; restores /etc/hosts)
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
`"disabled": true`. Restart (or reload) after editing.

## Config (`config.toml`)
```toml
listen = "127.0.0.1:8317"              # keep loopback; Caddy terminates TLS
api_keys = ["sk-cpa-change-me-0123456789"]   # client keys (Bearer or x-api-key), ≥16 chars
# or labeled (label shows up in the usage log instead of the key):
# [[api_keys]]
# label = "laptop"
# key = "sk-cpa-change-me-0123456789"
instance_id = ""                        # default: hostname; stored per usage record
upstream = "https://chatgpt.com/backend-api"
auth_dir = "~/.ciallopa/auths"
proxy_url = ""                          # global egress: http:// | socks5h:// (https:// = HTTP only, not WS)
max_body_mb = 16

[routing]
strategy = "round-robin"                # | weighted-round-robin | fill-first
session_affinity = true
session_affinity_ttl = "1h"
session_affinity_subagents = true
session_affinity_rebind_on = "quota"    # | any
quota_avoid_threshold = 0.0             # 0 = off (default): switch only on upstream quota errors; e.g. 0.95 = new sessions skip accounts at/over 95% used

[upstream_opts]
stream_idle_timeout_secs = 300
max_attempts = 4    # RETRIES (Codex request_max_retries): transport/408/5xx before first byte ⇒ ≤5 requests.
                    # 401/403/404/429 fail over to another account and do not consume retries.
                    # In-stream quota errors (usage_limit_reached etc.) before the first output
                    # event: account cooled + request replayed on the next account (SSE and WS).
                    # WS after output / incremental turn: frame withheld, close 1012 ⇒ Codex
                    # reconnects with full input on another account.
connect_timeout_secs = 30

[images]
enabled = true
main_model = "gpt-5.5"             # wrapper model for non-gpt-image-* Images requests
default_originator = "codex_exec"       # only for clients that don't send one
default_user_agent = "codex_exec/0.162.1"   # used when client UA doesn't start with "codex"
```

## Codex CLI client (`~/.codex/config.toml`)
```toml
model_provider = "cpa"
[model_providers.cpa]
name = "cpa"
base_url = "https://cpa.example.com/v1"   # or http://127.0.0.1:8317/v1 locally
env_key = "CPA_KEY"                       # export CPA_KEY=<one of api_keys>
wire_api = "responses"
supports_websockets = true                # false = HTTP SSE only

[model_providers.cpa.capabilities]
remote_compaction = "v2"                  # server-side compaction, same as the built-in openai provider
```
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

Read-only web page: `GET /ui` (`/` redirects; `[ui] enabled = true` by default). It is a
single static HTML file embedded in the binary (inline CSS/JS, no external resources) that asks
for the access key (kept in `sessionStorage`, sent as `Authorization: Bearer`, never in the
URL) and shows account quota cards, a summary table (24h/7d/30d, by account/key/instance/model),
a daily token/cost chart and recent requests. Strict CSP (`default-src 'none'`, script/style
allowed by SHA-256 hash only, `connect-src 'self'`); all data rendered via `textContent`.
Local demo with fake data: `cargo run --example ui_demo` → http://127.0.0.1:8399/ui
(key `sk-demo-0123456789abcdef`).

Details: docs/USAGE.md.

Token refresh is cross-process safe: under the file lock the auth file is re-read; a newer
token on disk is used instead of refreshing, and the write-back is skipped if another writer
changed the credentials meanwhile (CPA `refresh_patch` semantics). `refresh_token_reused` /
`invalid_grant` mark the account `needs_relogin` (excluded from routing, shown in
`/usage/v1/quota`, logged once, no retry loop) until you log in again.

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
WebSocket upgrades are proxied by Caddy automatically.

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
