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
| `max_inflight_requests` | Default 64. HTTP holds one slot from before reading the body through response EOF/drop; a WS holds the same slot through its entire connection, including idle time. At capacity, new forwarding requests receive 503 `gateway_busy` with `Retry-After: 1`, without a queue or reading their bodies. Lowering the limit never interrupts existing work. `0` disables rejection but continues counting; re-enabling the limit includes that existing work. Health, admin and usage routes are outside the limit. |
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

**Restart** (written to the file, reported as `pending_restart` with running and file value, and
shown as a banner until the process restarts): `listen`, `instance_id`, `upstream`, `auth_dir`,
`max_body_mb`, `usage.enabled`, `usage.path`, `usage.queue_size`, `usage.batch_size`,
`usage.flush_ms`, `ui.enabled`.

`max_body_mb` is now honored exactly (MiB, 1–4096). Previously the implementation silently
used at least 20 MiB, including when the configured/default value was 16. The default now
actually rejects HTTP request bodies larger than 16 MiB; set 20 explicitly to retain the old
effective default. This limit applies to incoming bytes, not decompressed JSON or WS frames.
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
| `GET /admin/v1/config` | `version` (also the `ETag`), `path`, `write_enabled`, `values` (file, masked), `running` (masked), `schema` (key, kind, `hot`/`restart`, `editable`, options, description), `pending_restart`, `last_reload`, `file_error`, `overrides`, `egress` (effective proxy, masked, and `direct`), `runtime` (current `inflight`, `limit`, process-lifetime capacity `rejected`, `ws_connections`, `draining`). Runtime values are a response-time snapshot; the version is still the configuration write token. |
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
| `POST /admin/v1/accounts/import` | `{"auth_json": {…}}` — a Codex CLI `auth.json` or CPA codex file. |
| `POST /admin/v1/accounts/login/device`, `GET /admin/v1/accounts/login/{id}` | Device-code login (URL + one-time code; the server polls and saves the account). |
| `POST /admin/v1/accounts/{id}/refresh` | Refresh the access token now. |
| `POST /admin/v1/accounts/reload` | Re-scan `auth_dir`. |
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
  `POST /admin/v1/prices/check` / `apply`) need an admin key only: they write the price table,
  not `config.toml`.
- Same CSRF rules as the reset-credit action: client key in a header (`Authorization: Bearer` or
  `x-api-key`), `Content-Type: application/json`, no `Cookie` header.
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
