# Live settings

`config.toml` is the only source of truth. ciallopa never keeps settings anywhere else: the
management page and the admin API edit the same file, and every change — however it is made —
goes through one apply pipeline.

## Apply pipeline

Triggers:

| Trigger | Notes |
|---|---|
| File change | The config file, the price table (`[usage] prices`) and `auth_dir` are watched. Events are debounced (500 ms); a change whose content equals what is already applied (own write-back, `touch`) is skipped. |
| `SIGHUP` | `kill -HUP <pid>` / `systemctl reload ciallopa` with the packaged unit — reload config and re-scan accounts. |
| `POST /admin/v1/config/reload` | Same as SIGHUP; also the "重新加载配置文件" button. |
| `PUT /admin/v1/config`, key add / delete | Write-back (below), then the same reload. |

`systemctl reload` returns after delivering the signal. Configuration validation and apply
run asynchronously; check `last_reload` on the settings page or admin API, or the service log,
for the result. A successful command does not mean an invalid configuration was accepted.

Steps: read → parse + validate (types, ranges, durations, key rules, log filter) → semantic
checks (an HTTP client can be built with `proxy_url`; the price table parses) → diff against the
running config → build new components → publish component updates and the configuration
snapshot. Validation/build failure leaves the running process
untouched; the error is returned, logged and shown on the settings page (`last_reload`,
`file_error`).

Each `SharedConfig::load()` returns a complete configuration snapshot (`ArcSwap`). This is
not one transaction spanning every component or every stage of a request: routing, client
pools and error policy are consulted at the boundaries below. HTTP header/preparation/idle
timeouts are captured together before upstream work and kept through retries and response
streaming, including Images; a reload cannot shorten an already-running stream's idle limit.

## What applies when

**Hot** (applied by the reload at these boundaries):

| Keys | Effect |
|---|---|
| `api_keys` | New keys work and removed keys get 401 from the next request. Open WebSockets stay open. |
| `max_inflight_requests` | Default 64. HTTP holds one slot from before reading the body through response EOF/drop. A WS turn holds one slot from the client request to its terminal event, cancel or close; an idle WS holds none (connections are bounded by `ws_max_connections`), and the handshake itself is outside this limit. A WS turn at capacity is not forwarded: the client gets an error frame with status 503 `gateway_busy` and `retry-after: 1`, the connection stays open, and the refusal is recorded as `inflight` over `ws`. At capacity, new forwarding requests receive 503 `gateway_busy` with `Retry-After: 1`, without a queue or reading their bodies. Lowering the limit never interrupts existing work. `0` disables rejection but continues counting; re-enabling the limit includes that existing work. Health, admin and usage routes, and GET `/v1/models`, `/backend-api/codex/models`, `/backend-api/wham/usage`, `/api/codex/usage`, are outside the limit. Refusals are stored per minute (see USAGE.md *Refusals*). |
| `max_buffered_body_mb` | Default 256 MiB; `0` = unlimited, otherwise at least `max_body_mb` (max 65536). Total request bytes buffered at once across all forwarding routes: HTTP request bodies and client WebSocket messages. A body reserves its declared `Content-Length` before any byte is read (chunked bodies reserve as they arrive) and keeps it until every copy is freed (retries, in-stream replay, the zstd-compressed copy). Without room a request waits at most 1 s, then gets the same 503 `gateway_busy` with `Retry-After: 1`; the rest of its body is read and discarded first (at most 10 s) so the client sees the response, not a reset. The next body reads the new value. A client WebSocket message reserves its size (at most the whole budget) when it arrives and keeps it until its turn ends, because it is held for replay; without room it waits at most 1 s, then the client gets an error frame with status 503 `gateway_busy` and `retry-after: 1`, and the connection stays open. Process memory is about 1.4–2× this budget for HTTP (the parsed JSON and the compressed copy exist at the same time). For WebSocket the budget does not bound peak memory: per-connection receive and inflate buffers exist before a message can be counted, so `ws_max_connections` × `ws_max_message_mb` is the bound. Responses are not counted. `runtime.buffered_body_bytes` reports the current reservation. |
| `ws_max_connections` | Default 24; `0` = unlimited. Client WebSockets open at once, idle ones included. Over the cap the handshake gets 503 `gateway_busy` with `Retry-After: 1` / `retry-after-ms: 1000` and Codex falls back to HTTP for that turn. Lowering it never closes open connections; the next handshake reads the new value. |
| `ws_max_message_mb` | Default 16 MiB (1–4096). Largest client WebSocket message (and frame) after permessage-deflate inflation; a larger one closes the connection with 1009 *message too big*. Applies to new connections. Together with `ws_max_connections` this bounds WS receive memory: the defaults (24 × 16 MiB) peaked at 1026 MiB RSS in the worst-case test, fitting a 2 GB host. |
| `max_body_mb` | The next request reads the new limit, on forwarding and management routes. A request already reading its body keeps the limit it started with. |
| `proxy_url`, `proxy_enabled`, `ipv6` | Per-account clients (and token-refresh clients) are dropped and rebuilt on next use. An already-issued HTTP attempt keeps its client and uses that client's egress when classifying connection errors; the next attempt, including a retry of the same request, can use the new egress. When the effective egress changes (`proxy_enabled` and a non-empty `proxy_url`), open client WebSockets finish the running turn on the old connection and are then closed normally (1000) on both legs; Codex reconnects on its next request through the new egress. Not counted as an error, no cooldown. A change of `ipv6` counts as an egress change. |
| `upstream_opts.connect_timeout_secs` | Model-upstream HTTP clients are rebuilt on next use; the next WS handshake reads the new value. Already-issued attempts retain their clients. OAuth token refresh keeps its separate fixed 10-second connect limit and 30-second whole-request limit. |
| `ws_compression` | Next WebSocket handshake. Open connections keep what they negotiated. |
| `upstream_opts.cloudflare_cookies` | Off: every account's jar is dropped. On: empty jars. Connections are kept. |
| `upstream_opts.stream_idle_timeout_secs`, `response_header_timeout_secs`, `preparation_timeout_secs` | Captured together at the next HTTP request's upstream dispatch. The idle limit covers SSE bootstrap and body streaming; header/preparation budgets do not count successful stream reads. WS captures idle and preparation limits before its initial handshake and retains them for the entire connection: one preparation budget covers initial credentials/handshake retries, and each turn gets a fresh budget shared across its pre-output reconnects and retry waits. WS frame writes retain normal backpressure without this budget; successful replay restarts upstream idle. Reconnect to adopt changed WS limits. The response-header budget remains HTTP-only. |
| `upstream_opts.compress_requests` | When the next HTTP request body is prepared; its retries reuse the prepared bytes. |
| `upstream_opts.max_attempts`, `transient_cooldown_secs` | HTTP captures these at the start of each selection/retry sequence; a replay after a pre-output SSE error starts a new sequence and can see updated values. The retry count remains cumulative for that request. WS uses a fixed handshake retry limit of 4, independent of `max_attempts`, and reads transient cooldown when handling an error. |
| `upstream_opts.overload_streak`, `overload_cooldown_secs`, `overload_cooldown_max_secs`, `overload_retry_wait_secs`, `deactivated_cooldown_secs`, `payment_required_cooldown_secs`, `not_found_cooldown_secs` | Next error/cooldown decision, including errors from in-flight work. Already-running cooldowns keep their end time; an already-started retry wait is not rescheduled. |
| `routing.*` | Next selection. Cooldowns, quota snapshots, session bindings and round-robin cursors are kept; a new `session_affinity_ttl` applies to bindings written from then on. |
| `images.*` | Next request. |
| `log.level` | Immediately, unless `RUST_LOG` is set (it takes precedence; the page says so). |
| `usage.retention_days` | Picked up by the writer within a minute. |
| `usage.quota_weights.*` | Next quota estimate. |
| `usage.prices` (file content) | Reloaded; costs are computed at query time, so history is re-priced. |
| `usage.prices_source` | Used by the next price check (「检查更新」 / `ciallopa prices sync`). |
| `ui.allow_reset_consume`, `ui.allow_config_write` | Next request. **File only.** |
| `ui.reset_credits_refresh_mins` | The poller re-reads it every 30 s (`0` = on demand only). |
| `ui.reset_credits_auto_lead_mins` | Next automatic-use sweep (≤ 60 s); also re-checked right before a card is used. |

**Restart** (written to the file, reported as `pending_restart` with running and file value, and
shown as a banner until the process restarts): `listen`, `instance_id`, `upstream`, `auth_dir`,
`usage.enabled`, `usage.path`, `usage.queue_size`, `usage.batch_size`,
`usage.flush_ms`, `ui.enabled`.

`max_body_mb` is honored exactly (MiB, 1–4096; default 64). A larger declared `Content-Length`
gets a JSON 413 before the body is read. It applies to incoming HTTP bytes,
not decompressed JSON or WS frames (a zstd request body is inspected only up to the same decoded
size; a larger one is forwarded uninspected) (WebSocket messages and frames are limited by `ws_max_message_mb`, default 16 MiB).
The default was 16 until Codex 0.164 support. Real Codex requests can exceed 16 MiB: Codex keeps
images inline as base64 data URLs, resends the whole history on every HTTP request
(`store: false`), never compresses HTTP bodies for a custom provider, and keeps PNG/JPEG/WebP
bytes that already fit its size budget. One 2900×5367 PNG photo is about 4.2 MiB at high detail
and 18.5 MiB at original detail, so a session with a few pasted images would get stuck on 413
for every later turn. Bodies are read only after the API key check, but each in-flight request
buffers its body (plus the zstd copy), so lower this on memory-constrained hosts.
The in-flight limit bounds operation count; it is not a memory or throughput guarantee.

On SIGTERM/SIGINT the listener stops accepting connections and existing forwarding work
gets up to 60 seconds to finish. Idle WS connections close normally; active turns close
after their terminal event. Remaining HTTP work and WS relays are then cancelled, with
3 seconds for closing, 5 seconds for usage/fingerprint flush and 2 seconds for runtime
shutdown. A second signal skips the 60-second wait. Flush is best effort: the process does
not wait indefinitely for a blocked writer, and recent records can be lost on forced exit.
Interrupted unfinished streams use `shutdown` / `local` rather than `client_closed`.

Accounts: adding, editing or deleting files in `auth_dir` (also `ciallopa login` from another
shell) is picked up automatically. When an account disappears, its connection pool, cookie jar,
session bindings, cooldown state, turn-state ownership and reset-credit listing are dropped.

## Admin API

All routes sit behind the client-key middleware.

| Route | |
|---|---|
| `GET /admin/v1/config` | `version` (also the `ETag`), `path`, `write_enabled`, `values` (file, masked), `running` (masked), `schema` (key, kind, `hot`/`restart`, `editable`, options, description), `pending_restart`, `last_reload`, `file_error`, `overrides`, `egress` (effective proxy, masked, and `direct`), `runtime` (current `inflight`, `limit`, process-lifetime capacity `rejected` and per-reason `rejected_by` (`inflight`, `ws`, `body`, `ws_msg`), `ws_connections`, `draining`). Runtime values are a response-time snapshot; the version is still the configuration write token. |
| `POST /admin/v1/config/validate` | `{"changes": {"routing.strategy": "fill-first", …}}` → `{ok, errors, diff}`. Dry run; never writes. |
| `PUT /admin/v1/config` | Same body plus `If-Match: "<version>"`. Missing → 428, stale → 412. Returns `applied`, `pending_restart`, new `version`. `null` removes the key (built-in default). |
| `POST /admin/v1/config/reload` | Re-read the file and re-scan accounts. |
| `POST /admin/v1/config/keys` | `{"label": "phone"}` + `If-Match` → a server-generated key, returned **once**. |
| `DELETE /admin/v1/config/keys/{label}` | `If-Match`; the key used by the current session cannot delete itself. |
| `GET /admin/v1/config/audit?limit=50` | Recent audit entries. |
| `POST /admin/v1/proxy/test` | `{"proxy_url"?: "…"}` (absent or the masked saved value = the saved address, `""` = direct; unsaved candidates allowed) → `GET <upstream origin>/cdn-cgi/trace` with a fresh client: no account, no credentials, no cookies, not the account pools. Returns `ok`, `direct`, `proxy` (masked), `target`, `latency_ms`, and `ip` / `loc` / `colo` / `status` on a response, or `stage` (`connect` / `timeout` / `transport`) and `error` on failure. Never writes; needs the admin key and the JSON/CSRF rule, not `allow_config_write`. |
| `GET /admin/v1/accounts` | id, email, plan, status (`normal` / `cooling` / `disabled` / `token_invalid`), cooldown end and reason (`usage_limit`, `rate_limited`, `auth`, `workspace_deactivated`, `payment_required`, `transient`, `model_unsupported`, `not_found`), weight, priority, last-24h requests and tokens. No tokens. |
| `PATCH /admin/v1/accounts/{id}` | `{"disabled"?, "weight"?, "priority"?}`; takes effect on the next selection. |
| `DELETE /admin/v1/accounts/{id}` | `{"confirm": "<id>"}`; deletes the auth file and the runtime state above. |
| `POST /admin/v1/accounts/import` | `{"auth_json": {…}}` — a Codex CLI `auth.json` or CPA codex file. When the account already has a file, its tokens are **replaced**. `"dry_run": true` writes nothing and returns `{account, exists}`; the page uses it to ask before replacing. |
| `POST /admin/v1/accounts/login/device`, `GET /admin/v1/accounts/login/{id}` | Device-code login (URL + one-time code; the server polls and saves the account). |
| `POST /admin/v1/accounts/{id}/refresh` | `{"confirm": "<id>"}`; refresh the tokens now. Irreversible: the old refresh token stops working, so a Codex CLI sharing the same `auth.json` is logged out. ciallopa refreshes on its own before expiry. |
| `POST /admin/v1/accounts/reload` | Re-scan `auth_dir` → `{added, removed, accounts, auth_files}`. Only a file that is gone removes its account. A file that exists but cannot be read (`permission_denied`, `read_error`) or parsed (`invalid_json`, e.g. half-written) is listed in `auth_files.skipped` with its reason; if it belonged to a loaded account, the entry has `kept: true` and the account keeps its last good content and runtime state (cooldowns, session bindings, connection pool). The file watcher follows the same rule. |
| `POST /admin/v1/accounts/{id}/clear-cooldown` | `{}`; drops every cooldown and the consecutive-5xx count of the account (runtime only, audited; needs the admin key and the JSON/CSRF rule, not `allow_config_write`). |

## Write rules

- Every `/admin/v1/*` call (and `POST /usage/v1/reset-credits/refresh`) needs an **admin**
  client key (`admin = true` on its `[[api_keys]]` entry). Plain keys and labeled keys without
  the flag get 403. If no admin key is configured, management is unavailable for everyone (no
  fallback to “any key”). Page-generated keys are never admin — set the flag in the file.
  Auth header precedence: `X-Api-Key` over `Authorization: Bearer` when both are present.
- Writes (config PUT, keys, account actions, reset-credit consume) also need the matching
  `[ui] allow_*` switch, which can only be set in the file — the page cannot grant itself
  permissions. Without the switch the API answers 403 and the pages are read-only. Validate and
  reload stay available without `allow_config_write` (they only read the file the operator
  already controls), but they still need an admin key. Price updates (settings → 「价格」,
  `POST /admin/v1/prices/check` / `apply` / `models`) need an admin key only: they write the price table,
  not `config.toml`.
- Same CSRF rules as the reset-credit action: client key in a header (`Authorization: Bearer` or
  `x-api-key`; cookies are ignored, never used for auth), `Content-Type: application/json`
  (else 400), and a browser request must be same-origin: `Sec-Fetch-Site` `same-origin` / `none`
  when sent, otherwise an `Origin` matching `Host` or `X-Forwarded-Host` (else 403). Requests
  without either header (curl, scripts) pass on the key alone.
- Not editable from the page: `listen`, `auth_dir`, `usage.path`, `usage.prices`, `ui.enabled`,
  `ui.allow_*`, and key plaintext (keys can only be deleted or generated).
- Secrets are never returned: keys appear as label + `sk-…abcd`, a proxy password as `***`.
- Write-back uses `toml_edit`: only the changed keys are touched, comments and layout are kept.
  The file is replaced atomically (temp file + fsync + rename, original mode kept) and the
  previous content is saved as `<file>.bak`. If the reload rejects the new file, the previous
  content is restored.
- Auth files are changed under the same in-process lock and cross-process file lock as token
  refresh, so an edit never races a refresh.

## Audit log

Every applied change is appended to `<config file>.audit.jsonl` (mode 0600) and logged under the
`ciallopa::audit` target: time, actor (client key label; empty for file / signal), action,
source and the changed keys with masked old and new values. Account actions record the account
id and the changed fields, never tokens.

## `Retry-After` and `retry-after-ms`

Every response that carries `Retry-After` (admission 503 `gateway_busy`, buffered-body 503, the
WS handshake cap, and local cooldown / no-account 429/503 answers) also carries
`retry-after-ms` with the same delay in milliseconds, matching the OpenAI header pair that Codex
already prefers (`retry-after-ms` first, then `retry-after`). WS error frames carry the same pair
in their `headers`. Upstream 429/5xx values go through the same client filter: a delay of
1–20 s is kept and both headers then state it (an upstream that sent only `retry-after-ms` gets
the rounded-up `Retry-After`); zero, longer delays and HTTP dates drop both. Bodies and wording
stay the neutral upstream-style errors.

## `GET /admin/v1/memory`

Admin only. `{"now":{at_ms,rss_bytes,allocated_bytes,resident_bytes,buffered_body_bytes,
ws_connections,ws_bytes},"rss_peak_bytes","jemalloc","buffered_body_limit_bytes",
"ws_max_connections","ws_max_message_bytes","sample_every_secs","history":[samples]}`. RSS comes from `/proc/self/status`
(Linux, including Docker Desktop on macOS) and is `null` elsewhere; jemalloc figures appear only
in the release binary. A sample is kept every 10 s for 15 minutes, in memory only. The 概览
内存 panel reads it on each refresh.
