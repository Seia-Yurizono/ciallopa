# Usage log (per-request records)

Status: implemented on branch `usage-log` (crate `crates/usage`). Benchmarks with fake data:
`<local-path>`.

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
| `key` | **label** of the client key that made the request (`[[api_keys]] label`, or `key-<sha256[..8]>` for plain keys). The raw key is never stored. |
| `instance` | `instance_id` of the cpa-lite that wrote the row (default: hostname) |
| `plan`, `p_pct`, `p_reset`, `s_pct`, `s_reset` | upstream quota snapshot seen on this response: `x-codex-plan-type`, primary/secondary `used-percent` and `reset-at` (unix s). SSE: response headers; WS: handshake headers, updated by `codex.rate_limits` frames. A window without `window-minutes` whose reset is "now" (free plan's empty secondary) is treated as absent. |
TPS is computed at query time: `out_tok / (duration_ms − ttft_ms)`.

Not recorded: `/models`, `/wham/*` and other passthrough, WS handshakes that succeed (only turns).

## Schema
```sql
CREATE TABLE labels(id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);  -- interned strings
CREATE TABLE requests(
  id INTEGER PRIMARY KEY, account INTEGER, first_account INTEGER, model INTEGER, effort INTEGER,
  transport INTEGER NOT NULL, endpoint INTEGER, status INTEGER NOT NULL, error INTEGER,
  retries INTEGER NOT NULL, switches INTEGER NOT NULL, ttft_ms INTEGER, duration_ms INTEGER NOT NULL,
  in_tok INTEGER, out_tok INTEGER, cached_tok INTEGER, reasoning_tok INTEGER,
  key INTEGER, instance INTEGER, plan INTEGER,                -- labels.id
  p_pct REAL, p_reset INTEGER, s_pct REAL, s_reset INTEGER);
-- one row per *change* of an account's quota (id = observing request id)
CREATE TABLE quota_obs(id INTEGER PRIMARY KEY, account INTEGER NOT NULL, instance INTEGER,
  plan INTEGER, p_pct REAL, p_reset INTEGER, p_win INTEGER, s_pct REAL, s_reset INTEGER, s_win INTEGER);
CREATE INDEX quota_obs_account ON quota_obs(account, id);
```
- v1 → v2 migration is automatic (`ALTER TABLE ADD COLUMN` + new table, keyed on
  `user_version`); old rows keep NULL key/instance/quota.
- `quota_obs` gets a row only when used % / plan changes or a reset time moves by > 120 s
  (upstream reset times jitter by a few seconds). Purge keeps each account's latest row.
- **No secondary index.** Time ranges are rowid range scans (`id BETWEEN from<<20 AND to<<20`),
  purge is a rowid range delete, "recent" is a reverse rowid scan. Strings (account e-mails,
  model names) are interned in `labels`, so a row is ~51 bytes on disk (1M rows ≈ 51 MB).
- `PRAGMA user_version = 2`.

## Retention
- `retention_days` (default 30, 0 = keep). The writer purges at startup and then hourly, in
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
  `key`, `instance`, `plan`, `primary_used_pct`, `primary_reset_at`, `secondary_*`, `cost_usd`.
- `GET /usage/v1/summary?range=24h&group=key,instance&<filters>` (or `7d`, `30m`, …; or
  `from=<ms>&to=<ms>`). `group` = comma list of `account,model,key,instance,effort,transport,endpoint`
  (default `account,model`; unknown → 400)
  → `{"summary":{"group_by":[…],"total":{…},"groups":[{group:{key:…,instance:…}, count, errors, error_rate,
  ttft_p50_ms, ttft_p90_ms, duration_p50_ms, duration_p90_ms, input_tokens, output_tokens,
  cached_tokens, reasoning_tokens, tps, cost_usd}]}, "recorder":{accepted, dropped, written,
  write_errors, purged}}`. Percentiles are exact (nearest rank).
- `GET /usage/v1/quota` → `{"accounts":[{account, email, disabled, needs_relogin, quota:
  {plan, observed_at_ms, observed_by, primary:{used_pct, resets_at, window_minutes, estimate},
  secondary:{…}}}], "note": …}`. Every configured account is listed (`quota: null` if never
  observed); accounts seen in the log but no longer configured carry `configured: false`.
- 404 when `[usage] enabled = false`.

## Quota estimate
**The used % comes from upstream and includes usage by other clients (Codex app, other
proxies) and other cpa-lite instances on the same account.** The estimate only sees tokens
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

## Cost hook
No cost column is stored: rows keep exact token counts, and `cost_usd` is computed at query
time from a `PriceTable` (model → USD per 1M input / cached input / output tokens;
`Recorder::set_prices`). The table is empty by default, so `cost_usd` is `null`. Prices can
change later without rewriting rows. Manual export/import is intentionally not provided
(the DB is a plain SQLite file).

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
path = "~/.cpa-lite/usage.db"       # + -wal / -shm siblings
retention_days = 30                 # 0 = keep forever
queue_size = 8192                   # in-memory records between proxy and writer
batch_size = 256                    # rows per transaction (≈97% of max throughput)
flush_ms = 50                       # max wait before committing a partial batch

[usage.quota_weights]
input = 1.0                         # uncached input
cached = 0.1
output = 4.0                        # output incl. reasoning
min_delta_pct = 5.0                 # below → "insufficient_data"
```
