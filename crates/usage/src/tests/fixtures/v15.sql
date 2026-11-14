
CREATE TABLE IF NOT EXISTS labels(
  id   INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS requests(
  id            INTEGER PRIMARY KEY,   -- request id = start_ms << 20 | seq (time-ordered)
  account       INTEGER,               -- labels.id
  first_account INTEGER,               -- labels.id, only when failover changed the account
  model         INTEGER,               -- labels.id
  effort        INTEGER,               -- labels.id
  transport     INTEGER NOT NULL,      -- 1 sse, 2 ws, 3 http
  endpoint      INTEGER,               -- labels.id
  status        INTEGER NOT NULL,
  error         INTEGER,               -- labels.id (NULL = ok)
  retries       INTEGER NOT NULL,
  switches      INTEGER NOT NULL,
  ttft_ms       INTEGER,
  duration_ms   INTEGER NOT NULL,
  in_tok        INTEGER,
  out_tok       INTEGER,
  cached_tok    INTEGER,
  reasoning_tok INTEGER,
  -- v2
  key           INTEGER,               -- labels.id of the client key *label* (never the key)
  instance      INTEGER,               -- labels.id of instance_id
  plan          INTEGER,               -- labels.id, x-codex-plan-type
  p_pct         REAL,                  -- x-codex-primary-used-percent seen on this response
  p_reset       INTEGER,               -- unix s
  s_pct         REAL,
  s_reset       INTEGER,
  -- v3
  tier          INTEGER,               -- labels.id, service_tier
  cw_tok        INTEGER,               -- cache_write_tokens (part of in_tok)
  thread        INTEGER,               -- labels.id, Codex thread id
  turn          TEXT,                  -- Codex turn id
  -- v5
  req_tier      INTEGER,               -- labels.id, requested service_tier
  resp_model    INTEGER,               -- labels.id, response.model
  resp_effort   INTEGER,               -- labels.id, response.reasoning.effort
  -- v7
  incomplete    INTEGER,               -- labels.id, response.incomplete reason (NULL = not incomplete)
  -- v9
  err_source    INTEGER,               -- labels.id: client | local | network | upstream | stream
  up_status     INTEGER                -- last upstream HTTP status (may differ from status)
);
-- v13: session view (`thread` filter); partial, so rows without a thread cost nothing.
CREATE INDEX IF NOT EXISTS requests_thread ON requests(thread) WHERE thread IS NOT NULL;
-- Quota snapshots, one row only when an account's values change. id = observing request id.
CREATE TABLE IF NOT EXISTS quota_obs(
  id       INTEGER PRIMARY KEY,
  account  INTEGER NOT NULL,
  instance INTEGER,
  plan     INTEGER,
  p_pct    REAL, p_reset INTEGER, p_win INTEGER,
  s_pct    REAL, s_reset INTEGER, s_win INTEGER, cards INTEGER
);
CREATE INDEX IF NOT EXISTS quota_obs_account ON quota_obs(account, id);
-- v4: failed requests only (id = requests.id); body redacted + capped at 16 KiB.
CREATE TABLE IF NOT EXISTS error_detail(
  id        INTEGER PRIMARY KEY,
  message   TEXT,
  body      TEXT,
  body_len  INTEGER NOT NULL,
  truncated INTEGER NOT NULL,
  headers   TEXT NOT NULL,           -- JSON object
  -- v9
  up_req_id TEXT,                    -- upstream x-request-id / cf-ray
  attempts  TEXT                     -- JSON attempt chain (failed or switched requests only)
);
-- v6: manual reset-credit (重置卡) consume attempts. status: pending | done | failed.
-- A `pending` row (timeout / 5xx: outcome unknown) is retried with the same redeem_request_id.
CREATE TABLE IF NOT EXISTS reset_consume(
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  account           TEXT NOT NULL,
  credit_id         TEXT,
  redeem_request_id TEXT NOT NULL,
  key               TEXT,               -- client key label (never the key)
  instance          TEXT,
  started_ms        INTEGER NOT NULL,
  finished_ms       INTEGER,
  status            TEXT NOT NULL,
  http_status       INTEGER,
  code              TEXT,               -- reset | nothing_to_reset | no_credit | already_redeemed
  windows_reset     INTEGER,
  error             TEXT
);
CREATE INDEX IF NOT EXISTS reset_consume_account ON reset_consume(account, id);
-- v8: small key/value state (e.g. learned client fingerprint).
CREATE TABLE IF NOT EXISTS kv(
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
-- v10: requests refused by a local limit, one row per minute and group. Label columns
-- use 0 for "none" so the primary key can merge flushes of the same minute.
CREATE TABLE IF NOT EXISTS refusals(
  minute       INTEGER NOT NULL,      -- unix ms, start of the minute
  reason       INTEGER NOT NULL,      -- 1 inflight, 2 ws, 3 body, 4 ws_msg
  transport    INTEGER NOT NULL,      -- 2 ws, 3 http
  key          INTEGER NOT NULL,      -- labels.id or 0
  route        INTEGER NOT NULL,      -- labels.id or 0 (client path, ids collapsed)
  ua           INTEGER NOT NULL,      -- labels.id or 0
  model        INTEGER NOT NULL,      -- labels.id or 0 (WS messages only)
  account      INTEGER NOT NULL,      -- labels.id or 0 (WS messages only)
  instance     INTEGER NOT NULL,      -- labels.id or 0
  lim          INTEGER NOT NULL,      -- limit in effect (requests, connections or bytes; 0 = unlimited)
  count        INTEGER NOT NULL,
  first_ms     INTEGER NOT NULL,
  last_ms      INTEGER NOT NULL,
  max_inflight INTEGER NOT NULL,      -- highest admitted requests at a refusal
  max_ws       INTEGER NOT NULL,      -- highest client WS connections at a refusal
  max_buffered INTEGER NOT NULL,      -- highest buffered body bytes at a refusal
  max_bytes    INTEGER,               -- largest refused body / message (body kinds)
  sum_bytes    INTEGER,
  PRIMARY KEY(minute, reason, transport, key, route, ua, model, account, instance, lim)
) WITHOUT ROWID;
-- v11: process occupancy per minute and instance, sampled about once a second. Averages
-- are sum / samples; peaks are exact (tracked on every increment, not only at a sample).
CREATE TABLE IF NOT EXISTS occupancy(
  minute        INTEGER NOT NULL,     -- unix ms, start of the minute
  instance      INTEGER NOT NULL,     -- labels.id or 0
  samples       INTEGER NOT NULL,
  inflight_sum  INTEGER NOT NULL,     -- capped requests + running WS turns
  inflight_peak INTEGER NOT NULL,
  ws_sum        INTEGER NOT NULL,     -- client WebSocket connections
  ws_peak       INTEGER NOT NULL,
  buffered_sum  INTEGER NOT NULL,     -- buffered request body bytes
  buffered_peak INTEGER NOT NULL,
  lim_inflight  INTEGER NOT NULL,     -- limits in effect (latest in the minute; 0 = unlimited)
  lim_ws        INTEGER NOT NULL,
  lim_buffered  INTEGER NOT NULL,
  PRIMARY KEY(minute, instance)
) WITHOUT ROWID;

-- v14: gateway stream observation (SSE requests / WS turns only; id = requests.id).
CREATE TABLE IF NOT EXISTS request_obs(
  id                    INTEGER PRIMARY KEY,
  up_first_text_ms      INTEGER,
  up_first_reasoning_ms INTEGER,
  up_first_tool_ms      INTEGER,
  up_last_event_ms      INTEGER,
  up_terminal_ms        INTEGER,
  up_events             INTEGER,
  up_bytes              INTEGER,
  response_id           TEXT,
  up_req_id             TEXT,
  sse_first_queue_ms    INTEGER,
  sse_last_queue_ms     INTEGER,
  sse_queue_blocked_us  INTEGER,
  sse_queue_blocked_max_us INTEGER,
  sse_queue_chunks      INTEGER,
  sse_queue_bytes       INTEGER,
  sse_first_yield_ms    INTEGER,
  sse_last_yield_ms     INTEGER,
  sse_yield_chunks      INTEGER,
  sse_yield_bytes       INTEGER,
  ws_first_send_ms      INTEGER,
  ws_last_send_ms       INTEGER,
  ws_sink_blocked_us    INTEGER,
  ws_sink_blocked_max_us INTEGER,
  ws_send_msgs          INTEGER,
  ws_send_bytes         INTEGER,
  down_incomplete      INTEGER,
  ws_deflate_down       INTEGER,
  ws_ext_down           TEXT,
  ws_deflate_up         INTEGER,
  ws_ext_up             TEXT
);

CREATE TABLE IF NOT EXISTS quota_reset_events (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  account     INTEGER NOT NULL,
  window      TEXT NOT NULL,
  kind        TEXT NOT NULL,
  at_ms       INTEGER NOT NULL,
  obs_id      INTEGER NOT NULL,
  prev_pct    REAL,
  next_pct    REAL,
  prev_reset  INTEGER,
  next_reset  INTEGER,
  prev_cards  INTEGER,
  next_cards  INTEGER,
  card_source TEXT,
  cycle_id    INTEGER,
  UNIQUE(account, window, obs_id)
);
CREATE INDEX IF NOT EXISTS quota_reset_events_acct ON quota_reset_events(account, at_ms);
CREATE TABLE IF NOT EXISTS quota_cycle_estimates (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  account           INTEGER NOT NULL,
  window            TEXT NOT NULL,
  started_ms        INTEGER NOT NULL,
  ended_ms          INTEGER NOT NULL,
  ended_obs_id      INTEGER NOT NULL,
  cycle_reset_at    INTEGER,
  close_kind        TEXT NOT NULL,
  capacity_credits  REAL,
  capacity_lo       REAL,
  capacity_hi       REAL,
  delta_pct         REAL,
  credits_used      REAL,
  api_cost_usd      REAL,
  requests          INTEGER,
  snapshots         INTEGER,
  coverage_gap      INTEGER NOT NULL DEFAULT 0,
  confidence        TEXT NOT NULL,
  card_source       TEXT,
  UNIQUE(account, window, ended_obs_id)
);
CREATE INDEX IF NOT EXISTS quota_cycle_estimates_acct ON quota_cycle_estimates(account, ended_ms);
