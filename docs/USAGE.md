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
TPS is computed at query time: `out_tok / (duration_ms − ttft_ms)`.

Not recorded: `/models`, `/wham/*` and other passthrough, WS handshakes that succeed (only turns).

## Schema
```sql
CREATE TABLE labels(id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);  -- interned strings
CREATE TABLE requests(
  id INTEGER PRIMARY KEY, account INTEGER, first_account INTEGER, model INTEGER, effort INTEGER,
  transport INTEGER NOT NULL, endpoint INTEGER, status INTEGER NOT NULL, error INTEGER,
  retries INTEGER NOT NULL, switches INTEGER NOT NULL, ttft_ms INTEGER, duration_ms INTEGER NOT NULL,
  in_tok INTEGER, out_tok INTEGER, cached_tok INTEGER, reasoning_tok INTEGER);
```
- **No secondary index.** Time ranges are rowid range scans (`id BETWEEN from<<20 AND to<<20`),
  purge is a rowid range delete, "recent" is a reverse rowid scan. Strings (account e-mails,
  model names) are interned in `labels`, so a row is ~51 bytes on disk (1M rows ≈ 51 MB).
- `PRAGMA user_version = 1` for future migrations.

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
- `GET /usage/v1/records?limit=100&before=<ms>&since=<ms>&account=<id>&model=<m>&errors=1`
  → `{"records":[…]}` newest first (limit ≤ 1000; `before` = pagination cursor).
- `GET /usage/v1/summary?range=24h` (or `7d`, `30m`, …; or `from=<ms>&to=<ms>`)
  → `{"summary":{"total":{…},"groups":[{account, model, count, errors, error_rate,
  ttft_p50_ms, ttft_p90_ms, duration_p50_ms, duration_p90_ms, input_tokens, output_tokens,
  cached_tokens, reasoning_tokens, tps}]}, "recorder":{accepted, dropped, written,
  write_errors, purged}}`. Percentiles are exact (nearest rank).
- 404 when `[usage] enabled = false`.

## Config
```toml
[usage]
enabled = true                      # default true
path = "~/.cpa-lite/usage.db"       # + -wal / -shm siblings
retention_days = 30                 # 0 = keep forever
queue_size = 8192                   # in-memory records between proxy and writer
batch_size = 256                    # rows per transaction (≈97% of max throughput)
flush_ms = 50                       # max wait before committing a partial batch
```
