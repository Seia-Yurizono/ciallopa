# Live settings

`config.toml` is the only source of truth. ciallopa never keeps settings anywhere else: the
management page and the admin API edit the same file, and every change — however it is made —
goes through one apply pipeline.

## Apply pipeline

Triggers:

| Trigger | Notes |
|---|---|
| File change | The config file, the price table (`[usage] prices`) and `auth_dir` are watched. Events are debounced (500 ms); a change whose content equals what is already applied (own write-back, `touch`) is skipped. |
| `SIGHUP` | `kill -HUP <pid>` / `systemctl reload` — reload config and re-scan accounts. |
| `POST /admin/v1/config/reload` | Same as SIGHUP; also the "重新加载配置文件" button. |
| `PUT /admin/v1/config`, key add / delete | Write-back (below), then the same reload. |

Steps: read → parse + validate (types, ranges, durations, key rules, log filter) → semantic
checks (an HTTP client can be built with `proxy_url`; the price table parses) → diff against the
running config → build new components → atomic swap. Any failure leaves the running process
untouched; the error is returned, logged and shown on the settings page (`last_reload`,
`file_error`).

Each request takes one snapshot of the configuration (`ArcSwap`), so a request never sees half
of a change.

## What applies when

**Hot** (applied by the reload; requests and WebSockets in flight keep what they started with):

| Keys | Effect |
|---|---|
| `api_keys` | New keys work and removed keys get 401 from the next request. Open WebSockets stay open. |
| `proxy_url`, `upstream_opts.connect_timeout_secs` | Per-account clients (and token-refresh clients) are dropped and rebuilt on next use. |
| `upstream_opts.cloudflare_cookies` | Off: every account's jar is dropped. On: empty jars. Connections are kept. |
| `upstream_opts.stream_idle_timeout_secs`, `max_attempts`, `compress_requests` | Next request. |
| `routing.*` | Next selection. Cooldowns, quota snapshots, session bindings and round-robin cursors are kept; a new `session_affinity_ttl` applies to bindings written from then on. |
| `images.*` | Next request. |
| `log.level` | Immediately, unless `RUST_LOG` is set (it takes precedence; the page says so). |
| `usage.retention_days` | Picked up by the writer within a minute. |
| `usage.quota_weights.*` | Next quota estimate. |
| `usage.prices` (file content) | Reloaded for new records; stored history is not re-priced. |
| `ui.allow_reset_consume`, `ui.allow_config_write` | Next request. **File only.** |
| `ui.reset_credits_refresh_mins` | The poller re-reads it every 30 s (`0` = on demand only). |

**Restart** (written to the file, reported as `pending_restart` with running and file value, and
shown as a banner until the process restarts): `listen`, `instance_id`, `upstream`, `auth_dir`,
`max_body_mb`, `usage.enabled`, `usage.path`, `usage.queue_size`, `usage.batch_size`,
`usage.flush_ms`, `ui.enabled`.

Accounts: adding, editing or deleting files in `auth_dir` (also `ciallopa login` from another
shell) is picked up automatically. When an account disappears, its connection pool, cookie jar,
session bindings, cooldown state, turn-state ownership and reset-credit listing are dropped.

## Admin API

All routes sit behind the client-key middleware.

| Route | |
|---|---|
| `GET /admin/v1/config` | `version` (also the `ETag`), `path`, `write_enabled`, `values` (file, masked), `running` (masked), `schema` (key, kind, `hot`/`restart`, `editable`, options, description), `pending_restart`, `last_reload`, `file_error`, `overrides`. |
| `POST /admin/v1/config/validate` | `{"changes": {"routing.strategy": "fill-first", …}}` → `{ok, errors, diff}`. Dry run; never writes. |
| `PUT /admin/v1/config` | Same body plus `If-Match: "<version>"`. Missing → 428, stale → 412. Returns `applied`, `pending_restart`, new `version`. `null` removes the key (built-in default). |
| `POST /admin/v1/config/reload` | Re-read the file and re-scan accounts. |
| `POST /admin/v1/config/keys` | `{"label": "phone"}` + `If-Match` → a server-generated key, returned **once**. |
| `DELETE /admin/v1/config/keys/{label}` | `If-Match`; the key used by the current session cannot delete itself. |
| `GET /admin/v1/config/audit?limit=50` | Recent audit entries. |
| `GET /admin/v1/accounts` | id, email, plan, status (`normal` / `cooling` / `disabled` / `token_invalid`), cooldown end, weight, priority, last-24h requests and tokens. No tokens. |
| `PATCH /admin/v1/accounts/{id}` | `{"disabled"?, "weight"?, "priority"?}`; takes effect on the next selection. |
| `DELETE /admin/v1/accounts/{id}` | `{"confirm": "<id>"}`; deletes the auth file and the runtime state above. |
| `POST /admin/v1/accounts/import` | `{"auth_json": {…}}` — a Codex CLI `auth.json` or CPA codex file. |
| `POST /admin/v1/accounts/login/device`, `GET /admin/v1/accounts/login/{id}` | Device-code login (URL + one-time code; the server polls and saves the account). |
| `POST /admin/v1/accounts/{id}/refresh` | Refresh the access token now. |
| `POST /admin/v1/accounts/reload` | Re-scan `auth_dir`. |

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
  already controls), but they still need an admin key.
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
