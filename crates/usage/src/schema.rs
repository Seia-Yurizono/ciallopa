use crate::UsageError;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::path::Path;

pub(crate) const SCHEMA_VERSION: i64 = 15;

const CREATE_V3: &str = r#"
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
  s_pct    REAL, s_reset INTEGER, s_win INTEGER
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
"#;

const MIGRATE_1_TO_2: &str = r#"
ALTER TABLE requests ADD COLUMN key INTEGER;
ALTER TABLE requests ADD COLUMN instance INTEGER;
ALTER TABLE requests ADD COLUMN plan INTEGER;
ALTER TABLE requests ADD COLUMN p_pct REAL;
ALTER TABLE requests ADD COLUMN p_reset INTEGER;
ALTER TABLE requests ADD COLUMN s_pct REAL;
ALTER TABLE requests ADD COLUMN s_reset INTEGER;
"#;

const MIGRATE_2_TO_3: &str = r#"
ALTER TABLE requests ADD COLUMN tier INTEGER;
ALTER TABLE requests ADD COLUMN cw_tok INTEGER;
ALTER TABLE requests ADD COLUMN thread INTEGER;
ALTER TABLE requests ADD COLUMN turn TEXT;
"#;

const MIGRATE_6_TO_7: &str = "ALTER TABLE requests ADD COLUMN incomplete INTEGER;";

const MIGRATE_8_TO_9: &str = r#"
ALTER TABLE requests ADD COLUMN err_source INTEGER;
ALTER TABLE requests ADD COLUMN up_status INTEGER;
"#;
const MIGRATE_8_TO_9_DETAIL: &str = r#"
ALTER TABLE error_detail ADD COLUMN up_req_id TEXT;
ALTER TABLE error_detail ADD COLUMN attempts TEXT;
"#;

const MIGRATE_4_TO_5: &str = r#"
ALTER TABLE requests ADD COLUMN req_tier INTEGER;
ALTER TABLE requests ADD COLUMN resp_model INTEGER;
ALTER TABLE requests ADD COLUMN resp_effort INTEGER;
"#;

fn pragmas(c: &Connection) -> rusqlite::Result<()> {
    c.busy_timeout(std::time::Duration::from_secs(5))?;
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA temp_store=MEMORY;")
}

/// Create or migrate the schema to `SCHEMA_VERSION` (one transaction).
pub(crate) fn migrate(c: &mut Connection) -> rusqlite::Result<()> {
    let v: i64 = c.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if v >= SCHEMA_VERSION {
        return Ok(());
    }
    let tx = c.transaction()?;
    let has_requests: bool = tx
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='requests'",
            [],
            |_| Ok(true),
        )
        .optional()?
        .unwrap_or(false);
    if has_requests && v == 1 {
        tx.execute_batch(MIGRATE_1_TO_2)?;
    }
    if has_requests && (1..=2).contains(&v) {
        tx.execute_batch(MIGRATE_2_TO_3)?;
    }
    if has_requests && (1..=4).contains(&v) {
        tx.execute_batch(MIGRATE_4_TO_5)?;
    }
    if has_requests && (1..=6).contains(&v) {
        tx.execute_batch(MIGRATE_6_TO_7)?;
    }
    if has_requests && (1..=8).contains(&v) {
        tx.execute_batch(MIGRATE_8_TO_9)?;
    }
    let has_detail: bool = tx
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='error_detail'",
            [],
            |_| Ok(true),
        )
        .optional()?
        .unwrap_or(false);
    if has_detail && (4..=8).contains(&v) {
        tx.execute_batch(MIGRATE_8_TO_9_DETAIL)?;
    }
    tx.execute_batch(CREATE_V3)?;
    tx.execute_batch(crate::observation::CREATE)?;
    // v12: rewrite account labels to aliases and scrub historical error payloads.
    // Runs for every upgrade into 12 (including fresh DBs, where it only seeds alias_key).
    if v < 12 {
        tx.execute_batch("PRAGMA secure_delete = ON;")?;
        crate::migrate_v12::rewrite(&tx)?;
    }
    crate::history::migrate_v15(&tx, v)?;
    tx.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION};"))?;
    tx.commit()?;
    if v < 12 {
        // Drop free pages that still held pre-rewrite bytes; no VACUUM (disk doubling).
        let _ = c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
    }
    Ok(())
}

pub(crate) fn open_rw(p: &Path) -> Result<Connection, UsageError> {
    let mut c = Connection::open(p)?;
    pragmas(&c)?;
    migrate(&mut c)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o600));
    }
    Ok(c)
}

/// A short-lived query connection. All reads share the first SELECT's snapshot,
/// so a concurrent writer cannot insert rows after we have loaded their labels.
pub(crate) fn open_ro(p: &Path) -> Result<Connection, UsageError> {
    let c = Connection::open_with_flags(
        p,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    c.busy_timeout(std::time::Duration::from_secs(5))?;
    c.execute_batch("BEGIN DEFERRED")?;
    Ok(c)
}
