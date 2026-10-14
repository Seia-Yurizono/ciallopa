# Usage log (per-request records)

Status: implemented on branch `usage-log` (crate `crates/usage`). Benchmarks with fake data:
`crates/usage/examples/usage_bench.rs`.

## Goals / non-goals
- One row per finished request (HTTP/SSE) or per turn (WS `response.create` → terminal event).
- **Never blocks or slows forwarding.** Losing records under overload is acceptable; losing
  requests is not.
- Personal multi-account scale (≤ ~1M rows / 30 days). No multi-tenant billing, no exact
  accounting guarantees.

## Write path
```
handler ──Tracker (per request/turn)──▶ Drop ──try_send──▶ bounded std::sync::mpsc (queue_size)
                                                      full ⇒ drop + counters.dropped++
writer OS thread "usage-writer":  recv first msg → collect until batch_size or flush_ms
                                  → one INSERT OR IGNORE transaction → hourly purge chunk
```
- `Tracker` lives in `crates/forward/src/usage.rs`. The record is built when the tracker is
  dropped (stream end, client disconnect, idle timeout, error), so every exit path records.
- Hot-path cost: one `Box<Record>` + `try_send` (~100 ns p50, ~1–2 µs p99) per request; SSE
  bodies are scanned with a byte search and only terminal events (`response.completed` /
  `failed` / `incomplete` / `error`) are JSON-parsed (usage tokens). WS turns reuse the frame.
- SQLite via `rusqlite 0.39` (bundled SQLite, `libsqlite3-sys 0.37.0` = same as codex-rs).
  Not on the network path; `scripts/lock-check.sh` allowlists only `rusqlite` and its pure-Rust
  helpers, the TLS/h2/WS stack is unchanged.
- PRAGMAs: `journal_mode=WAL`, `synchronous=NORMAL` (no fsync per commit; a power loss can
  drop the last ~second of rows, never corrupts), `busy_timeout=5s`. File mode 0600.
- Graceful shutdown (Ctrl-C) drains the queue (`Recorder::shutdown`).

## Fields
| column | meaning |
|---|---|
| `id` | request id = `start_ms << 20 \| seq` — unique, time-ordered, the rowid. API shows it as 16 hex chars. Duplicate ids are ignored (`INSERT OR IGNORE`). |
| `account` / `first_account` | final account; first tried account only when failover changed it |
| `model`, `effort` | from body / `response.create` (`reasoning.effort`) |
| `transport` | 1 sse, 2 ws, 3 http (images, non-stream) |
| `endpoint` | `responses`, `compact`, `images` |
| `status` | HTTP status to the client (WS turn: 200 or handshake status) |
| `error` | NULL = ok; else `usage_limit`, `idle_timeout`, `stream_error`, `client_closed`, `upstream_closed`, `response_failed`, `incomplete`, `upstream_error_event`, `rate_limited`, `auth`, `no_account`, `upstream_5xx`, … |
| `retries` | pre-first-byte retries (transport / 408 / 5xx) |
| `switches` | account switches (401/403/404/429 failover, quota-before-output replay) |
| `ttft_ms` | request start → first non-preamble event (SSE/WS); → response headers (non-stream) |
| `duration_ms` | request start → end of stream / turn |
| `in_tok`, `out_tok`, `cached_tok`, `reasoning_tok` | from `response.usage` |
| `cw_tok` | `input_tokens_details.cache_write_tokens` (part of `in_tok`) |
| `tier` | `service_tier` actually served: terminal event's `response.service_tier` (wins), else `response.created`'s, else the request body's; `auto` from a response is ignored (`priority` = Fast mode, `flex`, `default`/`auto` = standard). Pricing uses this. |
| `req_tier` | v5: request body's `service_tier` (`fast` stored as `priority`), kept even when downgraded |
| `resp_model`, `resp_effort` | v5: `response.model` / `response.reasoning.effort` (terminal event wins over `response.created`; SSE and WS) |
| `incomplete` | v7: `response.incomplete` reason (`incomplete_details.reason`, else `unknown`). Passed through untouched, tokens recorded, **not** an error and never a cooldown (Codex treats it as terminal since bc9cad3 / counts its tokens since #53670) |
| `thread`, `turn` | Codex thread / turn id from the `thread-id` header, `x-codex-turn-metadata` (JSON `thread_id`, `turn_id`) or body `client_metadata` (WS `response.create`). Stored for a future authoritative cost lookup; nothing queries upstream yet. Values > 128 chars are ignored. |
| `key` | **label** of the client key that made the request (`[[api_keys]] label`, or `key-<sha256[..8]>` for plain keys). The raw key is never stored. |
| `instance` | `instance_id` of the ciallopa that wrote the row (default: hostname) |
| `plan`, `p_pct`, `p_reset`, `s_pct`, `s_reset` | upstream quota snapshot seen on this response: `x-codex-plan-type`, primary/secondary `used-percent` and `reset-at` (unix s). SSE: response headers; WS: handshake headers, updated by `codex.rate_limits` frames. A window without `window-minutes` whose reset is "now" (free plan's empty secondary) is treated as absent. |
TPS is computed at query time: `out_tok / (duration_ms − ttft_ms)`.

Not recorded: `/models`, `/wham/*` and other passthrough, WS handshakes that succeed (only turns).

## Error details
Stored only for failed requests (status ≥ 400 or `error` set), in `error_detail`:
- `body`: upstream error body (HTTP error response, in-stream `error` / `response.failed` /
  `response.incomplete` event, WS error / quota frame, WS handshake failure, or the local
  JSON error such as `no_account`). Capture stops at 64 KiB; the stored text is redacted and
  then capped at **16 KiB** (`truncated = true`, `body_len` = original byte length).
- `message`: upstream `error.message` or a descriptive local message (`upstream stream idle
  for 300s (timeout)`, `no account available`, `upstream websocket closed mid-turn (code …)`, …).
- `headers`: only `x-codex-*`, `cf-ray`, `retry-after`, `content-type`, `x-request-id`,
  `x-oai-request-id`, `openai-*request-id`/`openai-processing-ms`; never cookies or auth.
- Redaction (body, message, headers): `Bearer`/`Basic` credentials, JWT-like `eyJ…` tokens,
  `sk-`/`rt_`/`sess-` style tokens, `cookie`/`set-cookie`/`authorization`/`*token*`/`api_key`/
  `password`/`secret`/`session` values (JSON keys and `k=v` / `k: v` text) and e-mail
  addresses become `[REDACTED…]`.
- Purged with the same `retention_days` cutoff as `requests`.
- `/ui`: click an error row to open the detail panel (pretty-printed JSON, copy button).

## Schema
```sql
CREATE TABLE labels(id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);  -- interned strings
CREATE TABLE requests(
  id INTEGER PRIMARY KEY, account INTEGER, first_account INTEGER, model INTEGER, effort INTEGER,
  transport INTEGER NOT NULL, endpoint INTEGER, status INTEGER NOT NULL, error INTEGER,
  retries INTEGER NOT NULL, switches INTEGER NOT NULL, ttft_ms INTEGER, duration_ms INTEGER NOT NULL,
  in_tok INTEGER, out_tok INTEGER, cached_tok INTEGER, reasoning_tok INTEGER,
  key INTEGER, instance INTEGER, plan INTEGER,                -- labels.id
  p_pct REAL, p_reset INTEGER, s_pct REAL, s_reset INTEGER,
  tier INTEGER, cw_tok INTEGER, thread INTEGER, turn TEXT);   -- v3
-- one row per *change* of an account's quota (id = observing request id)
CREATE TABLE quota_obs(id INTEGER PRIMARY KEY, account INTEGER NOT NULL, instance INTEGER,
  plan INTEGER, p_pct REAL, p_reset INTEGER, p_win INTEGER, s_pct REAL, s_reset INTEGER, s_win INTEGER);
CREATE INDEX quota_obs_account ON quota_obs(account, id);
-- v4: failed requests only (status >= 400 or error set); id = requests.id
CREATE TABLE error_detail(id INTEGER PRIMARY KEY, message TEXT, body TEXT,
  body_len INTEGER NOT NULL, truncated INTEGER NOT NULL, headers TEXT NOT NULL);
```
- v1 → v2 → v3 → v4 migrations are automatic (`ALTER TABLE ADD COLUMN` + new table, keyed on
  `user_version`); old rows keep NULL in the new columns (old rows: standard tier, no cache writes).
- `quota_obs` gets a row only when used % / plan changes or a reset time moves by > 120 s
  (upstream reset times jitter by a few seconds). Purge keeps each account's latest row.
- **No secondary index.** Time ranges are rowid range scans (`id BETWEEN from<<20 AND to<<20`),
  purge is a rowid range delete, "recent" is a reverse rowid scan. Strings (account e-mails,
  model names) are interned in `labels`, so a row is ~51 bytes on disk (1M rows ≈ 51 MB).
- `PRAGMA user_version = 4`.

## Retention
- `retention_days` (default 90; `0` = keep forever, no purge at all). Error details follow the
  same cutoff. The writer purges at startup and then hourly, in
  chunks of 20 000 rows per transaction (~20 ms each) so a large backlog never stalls inserts.
- Freed pages are reused; the file does not shrink (no VACUUM). Steady-state size ≈
  rows-per-retention-window × 51 B.
- **No hourly rollup** (decision from the benchmark): a 30-day summary over 1M rows takes
  ~0.4 s, 24 h over 10M rows ~0.13 s. Revisit (hourly rollup with histogram buckets) only if
  real data exceeds ~5M rows per retention window.

## Failure modes
| failure | effect |
|---|---|
| queue full (writer slow / disk stall / DB locked) | records dropped, `dropped` counter, warn log at powers of two; no request latency change (measured) |
| DB locked > 5 s (external writer) | that batch fails, `write_errors += rows`, writer continues |
| DB cannot be opened at startup | warning, usage log disabled, proxy runs normally |
| kill -9 / crash | committed rows survive; WAL replayed on next open; `integrity_check` ok (20/20 runs) |
| client disconnects mid-stream | recorded as `client_closed` when the stream task notices (next upstream chunk or idle timeout) |

## API (behind the same client key)
Filters on both list endpoints: `account=`, `model=`, `key=<label>`, `instance=`.
- `GET /usage/v1/records?limit=100&before=<ms>&since=<ms>&errors=1&<filters>`
  → `{"records":[…]}` newest first (limit ≤ 1000; `before` = pagination cursor). Rows include
  `key`, `instance`, `plan`, `primary_used_pct`, `primary_reset_at`, `secondary_*`,
  `cache_write_tokens`, `service_tier`, `requested_service_tier`, `response_model`, `response_effort`, `incomplete`,
  `model_mismatch` (null = no response model; false = same ignoring case and a `-YYYY-MM-DD` snapshot
  suffix; true = different), `thread_id`, `turn_id`, `cost_usd`, `price_status`
  (`ok` | `unknown_price` | `no_tokens`), `has_detail`.
- `GET /usage/v1/records/{request_id}/detail` → `{request_id, ts_ms, message, body, body_len,
  truncated, headers:{…}}` (404 if none, 400 if the id is not 16 hex chars). See *Error details*.
- `GET /usage/v1/summary?range=24h&group=key,instance&<filters>` (or `7d`, `30m`, …; or
  `from=<ms>&to=<ms>`). `group` = comma list of `account,model,key,instance,effort,transport,endpoint,tier,response_model`
  (default `account,model`; unknown → 400)
  → `{"summary":{"group_by":[…],"total":{…},"groups":[{group:{key:…,instance:…}, count, errors, error_rate,
  ttft_p50_ms, ttft_p90_ms, duration_p50_ms, duration_p90_ms, input_tokens, output_tokens,
  cached_tokens, reasoning_tokens, tps, cost_usd, unknown_price}]}, "unknown_price_models":[…]}, "recorder":{accepted, dropped, written,
  write_errors, purged}}`. Percentiles are exact (nearest rank).
- `GET /usage/v1/summary?…&bucket=day&tz=<minutes east of UTC>` additionally returns
  `"buckets":[{day:"YYYY-MM-DD", start_ms, count, errors, input_tokens, output_tokens,
  cached_tokens, reasoning_tokens, cost_usd, unknown_price}]` (ascending, empty days omitted;
  `tz` −840..840, default 0; other `bucket` values → 400). Used by the `/ui` trend chart.
- `GET /usage/v1/quota` → `{"accounts":[{account, email, disabled, needs_relogin, quota:
  {plan, observed_at_ms, observed_by, primary:{used_pct, resets_at, window_minutes, estimate},
  secondary:{…}}}], "note": …}`. Every configured account is listed (`quota: null` if never
  observed); accounts seen in the log but no longer configured carry `configured: false`.
- 404 when `[usage] enabled = false`.

## Quota estimate
**The used % comes from upstream and includes usage by other clients (Codex app, other
proxies) and other ciallopa instances on the same account.** The estimate only sees tokens
this DB recorded, so it is accurate only when this instance is (nearly) the only user, and
when several instances share one DB path / account it under-counts their share unless they
write to the same file.

Per account and window (primary / secondary):
1. Take the first and last `quota_obs` row in the *current* window (same reset ± 120 s).
2. Δ% = last − first. Weighted tokens = Σ over this account's requests with
   `first ≤ id < last` (the snapshot on a response precedes that request's own tokens) of
   `input·(in − cached) + cached·cached + output·out` (`out` includes reasoning).
3. Δ% < `min_delta_pct` (default 5) → `status: "insufficient_data"` (1 % steps are too coarse).
   Otherwise `tokens_per_pct = weighted / Δ%`, `capacity_tokens = ×100`,
   `remaining_tokens = (100 − used) × tokens_per_pct`, `status: "ok"`.
4. Reset time already passed → `window_reset`; window not reported → `no_window`.

Weights (defaults: input 1, cached 0.1, output 4 — output/reasoning cost more, cache hits
less) are in `[usage.quota_weights]`.

## Cost (notional)
**For subscription (ChatGPT plan) traffic the cost is notional**: what the same tokens would
cost on the OpenAI API at list price, not what is billed. Nothing is stored: rows keep exact
token counts and `cost_usd` is computed at query time from the price table, so a price fix
re-prices history.

Price table: `[usage] prices` (default `~/.ciallopa/prices.toml`; missing ⇒ all costs null).
Start from `prices.example.toml` (official prices from developers.openai.com/api/docs/pricing,
checked 2026-10-13). Entries are `[[model]]` with `model`, optional `tier`
(`standard` | `flex` | `priority-fast`), `in`, `cache` (cache read), `write` (cache write),
`out` in USD per 1M tokens, optional `long = {threshold, in_x, out_x}`, `flex_x`, `fast_x`,
`effective_from` / `effective_until` (`YYYY-MM-DD`, inclusive, UTC), `note`.

```
cost = (in − cached − cache_write)·P_in + cached·P_cache + cache_write·P_write + out·P_out
```
- `out` already includes reasoning tokens (not added again). Missing `cache` / `write` price ⇒ `in`.
- Long context: total input (incl. cached) > `long.threshold` ⇒ `P_in`, `P_cache`, `P_write`
  × `in_x`, `P_out` × `out_x` (OpenAI: 272K, ×2 / ×1.5).
- Tier: an entry with that tier wins; else the standard entry × `flex_x` (default 0.5) or
  `fast_x` (default 2; per model, e.g. gpt-5.5 = 2.5). `[defaults]` sets the global multipliers.
- Dates: on the request's UTC date a dated entry wins over an undated one; with no matching
  entry the row is unpriced. gpt-5.6-sol ships only as a promo entry until 2026-11-21
  (regular price not published — TODO in the example), so later rows are `unknown_price`.
- Unknown model / tier / date ⇒ `cost_usd: null`, `price_status: "unknown_price"`; summaries
  count those rows in `unknown_price` and list them in `unknown_price_models`. A group's
  `cost_usd` sums only priced rows (null if none) — never a fake 0; `unknown_price > 0` means
  it is a lower bound.
- Model names match exactly (case-insensitive); add aliases as extra entries.

`ciallopa prices sync --from models.dev [--provider openai] [--write]` fetches
`https://models.dev/api.json` through the configured `proxy_url`, prints the diff against the
local table (`+` new, `~` changed, `!` model has a dated/promo entry ⇒ never touched,
`?` local model missing upstream ⇒ kept) and writes only with `--write` (backup
`prices.toml.bak`; comments are not preserved; per-model `fast_x`/`flex_x`/`note` are kept).
`--from` also accepts a URL or a local `api.json`. Only standard-tier undated entries are synced.

Manual export/import is intentionally not provided (the DB is a plain SQLite file).

## Client keys & instance
```toml
instance_id = "vps-1"               # default: hostname
# plain list still works (label = key-<sha256 prefix>)
# api_keys = ["sk-..."]
[[api_keys]]
label = "laptop"                    # 1–64 chars, unique
key = "sk-..."                      # ≥ 16 chars, unique
```
Key matching is constant time across all keys; only the label reaches the forwarder / log.

## Config
```toml
[usage]
enabled = true                      # default true
path = "~/.ciallopa/usage.db"       # + -wal / -shm siblings
retention_days = 90                 # 0 = keep forever
queue_size = 8192                   # in-memory records between proxy and writer
batch_size = 256                    # rows per transaction (≈97% of max throughput)
flush_ms = 50                       # max wait before committing a partial batch

[usage.quota_weights]
input = 1.0                         # uncached input
cached = 0.1
output = 4.0                        # output incl. reasoning
min_delta_pct = 5.0                 # below → "insufficient_data"
```
`[usage] prices = "~/.ciallopa/prices.toml"` — price table (see "Cost").

## Web UI (`/ui`)
`[ui] enabled = true` (default). Static page, no data without the key; every call goes to the
endpoints above with the key from `sessionStorage`. Read-only, except the opt-in reset-credit
consume action below (off unless `[ui] allow_reset_consume = true`).
Headers: CSP `default-src 'none'; script-src 'sha256-…'; style-src 'sha256-…'; connect-src
'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'`,
`X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`,
`Cache-Control: no-store`. Hashes are computed from the embedded file at startup, so editing
`crates/server/src/ui.html` needs no manual CSP update.

## Reset credits (重置卡)
Manual only: nothing in ciallopa consumes a credit by itself (no failover / quota path calls it;
a test asserts the consume helper has a single caller, the admin handler).

Upstream (mirrors codex-rs `backend-client/src/client/rate_limit_resets.rs`), per account with
`Authorization: Bearer <access token>`, `ChatGPT-Account-Id`, `User-Agent: codex_cli_rs/<pinned>`,
`originator: codex_cli_rs`:
- `GET {upstream}/wham/rate-limit-reset-credits` (5 s) →
  `{available_count, credits: [{id, reset_type, status, granted_at, expires_at?, title?, description?}]}`
- `POST {upstream}/wham/rate-limit-reset-credits/consume` (10 s), body
  `{redeem_request_id, credit_id?}` → `{code: reset | nothing_to_reset | no_credit | already_redeemed, windows_reset}`

Config (`[ui]`):

| key | default | |
|---|---|---|
| `allow_reset_consume` | `false` | enable the consume API + page button |
| `reset_credits_refresh_mins` | `60` | background listing refresh (read-only); `0` = on demand only |

API (client key):
- `GET /usage/v1/reset-credits` → `{consume_enabled, refresh_mins, accounts: [{account, email,
  disabled, reset_credits: {fetched_ms, available_count, credits, nearest_expires_at, error,
  error_ms} | null}], attempts: [last 20 consume attempts]}`. Cache is in memory (re-fetched
  after restart); `nearest_expires_at` = earliest expiry among `status == "available"`.
- `POST /usage/v1/reset-credits/refresh` (optional `{"account": "<id>"}`) → same body.
- `POST /admin/v1/accounts/{id}/reset-credits/consume` — `{"confirm": "<id>", "credit_id"?,
  "redeem_request_id"?}`. Guards: switch off → 403; key only via header (middleware), JSON
  content type (browser cross-origin forms cannot send it without a CORS preflight, which is
  never granted), any `Cookie` header → 400, `confirm` must equal the path id → 400, unknown
  account → 404, usage log disabled → 503. One consume per account at a time.
  Response: `{account, redeem_request_id, reused_redeem_request_id, attempt, status, code,
  windows_reset, reset_credits, rate_limit?}`; after `reset` the listing is re-fetched and
  `/wham/usage` `rate_limit` is returned (quota cards update on the next proxied response).

Idempotency: every attempt is a row in `reset_consume` (schema v6) with status `pending` →
`done` (any of the 4 codes) / `failed` (definitive 4xx) / `superseded`. Unknown outcome (timeout,
transport error, 5xx, 408/429, unparseable 2xx) stays `pending`; the next consume for the same
account + `credit_id` within 24 h reuses that `redeem_request_id` (upstream answers
`already_redeemed` if the first one went through). A new id is minted only after a definitive
answer.
