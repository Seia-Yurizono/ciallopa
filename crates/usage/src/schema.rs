use crate::UsageError;
use rusqlite::{Connection, OpenFlags};
use std::path::Path;

pub(crate) const SCHEMA_VERSION: i64 = 20;

pub(crate) const LATEST: &str = r#"
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
  thread        INTEGER,               -- threads.id, Codex thread id
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
-- id is the rowid alias, already carried by this index (including NULL accounts).
CREATE INDEX IF NOT EXISTS requests_account ON requests(account);
-- Quota snapshots, one row only when an account's values change. id = observing request id.
CREATE TABLE IF NOT EXISTS quota_obs(
  id       INTEGER PRIMARY KEY,
  account  INTEGER NOT NULL,
  instance INTEGER,
  plan     INTEGER,
  p_pct    REAL, p_reset INTEGER, p_win INTEGER,
  s_pct    REAL, s_reset INTEGER, s_win INTEGER, cards INTEGER,
  -- v19: purchased credits (点数). cr_flags bits: 1 has_credits, 2 unlimited,
  -- 4 overage_limit_reached, 8 spend_control_reached. NULL = never reported.
  cr_flags INTEGER, cr_balance REAL
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
  credit_expires_ms INTEGER,
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
  -- v19: purchased credits (点数) and API price of requests served while the window was full;
  -- NULL on cycles closed before v19 (their credits_used may include them). Since v20
  -- api_cost_usd is the price of the included requests only (v19 rows: minus over_api_usd).
  over_credits      REAL,
  over_api_usd      REAL,
  UNIQUE(account, window, ended_obs_id)
);
CREATE INDEX IF NOT EXISTS quota_cycle_estimates_acct ON quota_cycle_estimates(account, ended_ms);
-- v19: manual estimate resets. Estimates of (account, window) use only snapshots and requests
-- with id >= the latest from_id not undone; rows of one reset share grp (= its first id).
CREATE TABLE IF NOT EXISTS quota_est_epochs (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  grp         INTEGER NOT NULL,
  account     INTEGER NOT NULL,
  window      TEXT NOT NULL,
  from_id     INTEGER NOT NULL,
  created_ms  INTEGER NOT NULL,
  by_key      TEXT,
  undone_ms   INTEGER
);
CREATE INDEX IF NOT EXISTS quota_est_epochs_acct ON quota_est_epochs(account, window, from_id);

CREATE TABLE IF NOT EXISTS threads(id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);
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

/// Every upgrade has one explicit destination; fresh databases run none of these steps.
type Migration = fn(&rusqlite::Transaction<'_>) -> rusqlite::Result<()>;
const STEPS: &[(i64, Migration)] = &[
    (2, |tx| {
        tx.execute_batch(MIGRATE_1_TO_2)?;
        tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS quota_obs(id INTEGER PRIMARY KEY, account INTEGER NOT NULL, instance INTEGER, plan INTEGER, p_pct REAL, p_reset INTEGER, p_win INTEGER, s_pct REAL, s_reset INTEGER, s_win INTEGER);"
    )
    }),
    (3, |tx| tx.execute_batch(MIGRATE_2_TO_3)),
    (4, |tx| {
        tx.execute_batch("CREATE TABLE IF NOT EXISTS error_detail(id INTEGER PRIMARY KEY, message TEXT, body TEXT, body_len INTEGER NOT NULL, truncated INTEGER NOT NULL, headers TEXT NOT NULL);")
    }),
    (5, |tx| tx.execute_batch(MIGRATE_4_TO_5)),
    (7, |tx| tx.execute_batch(MIGRATE_6_TO_7)),
    (9, |tx| {
        tx.execute_batch(MIGRATE_8_TO_9)?;
        tx.execute_batch(MIGRATE_8_TO_9_DETAIL)
    }),
    (12, |tx| {
        // Tables introduced without ALTERs are needed by the v12 privacy rewrite.
        tx.execute_batch(LATEST)?;
        tx.execute_batch("PRAGMA secure_delete=ON")?;
        crate::migrate_v12::rewrite(tx)
    }),
    (13, |tx| {
        tx.execute_batch("CREATE INDEX IF NOT EXISTS requests_thread ON requests(thread) WHERE thread IS NOT NULL")
    }),
    (14, |tx| tx.execute_batch(crate::observation::CREATE)),
    (15, |tx| {
        let has: bool = tx.query_row(
            "SELECT count(*) FROM pragma_table_info('quota_obs') WHERE name='cards'",
            [],
            |r| r.get::<_, i64>(0).map(|n| n > 0),
        )?;
        if has {
            return Ok(());
        }
        tx.execute_batch("ALTER TABLE quota_obs ADD COLUMN cards INTEGER")
    }),
    (16, migrate_v16),
    (17, migrate_v17),
    (18, |tx| {
        let has: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('reset_consume') WHERE name='credit_expires_ms')",
            [], |r| r.get(0))?;
        if has {
            return Ok(());
        }
        tx.execute_batch("ALTER TABLE reset_consume ADD COLUMN credit_expires_ms INTEGER")
    }),
    (19, migrate_v19),
    // v19 wrote api_cost_usd as included + credit-funded; keep only the included part. Rows
    // closed before v19 have no split (over_api_usd NULL) and stay as they are.
    (20, |tx| {
        tx.execute_batch(
            "UPDATE quota_cycle_estimates SET api_cost_usd = max(0.0, api_cost_usd - over_api_usd)
             WHERE api_cost_usd IS NOT NULL AND over_api_usd IS NOT NULL",
        )
    }),
];

/// Columns may already exist when an older step ran `LATEST`.
fn migrate_v19(tx: &rusqlite::Transaction<'_>) -> rusqlite::Result<()> {
    for (table, column, ty) in [
        ("quota_obs", "cr_flags", "INTEGER"),
        ("quota_obs", "cr_balance", "REAL"),
        ("quota_cycle_estimates", "over_credits", "REAL"),
        ("quota_cycle_estimates", "over_api_usd", "REAL"),
    ] {
        let has: bool = tx.query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM pragma_table_info('{table}') WHERE name=?1)"),
            [column],
            |r| r.get(0),
        )?;
        if !has {
            tx.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} {ty}"))?;
        }
    }
    Ok(())
}

fn migrate_v17(tx: &rusqlite::Transaction<'_>) -> rusqlite::Result<()> {
    // v16 executes LATEST too: do not rebuild an index it just created. Inspect
    // only this fixed index, excluding SQLite's auxiliary rowid from its keys.
    let target: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_index_list('requests')
         WHERE name='requests_account' AND origin='c' AND \"unique\"=0 AND partial=0)
         AND (SELECT count(*) FROM pragma_index_xinfo('requests_account') WHERE key=1)=1
         AND EXISTS(SELECT 1 FROM pragma_index_xinfo('requests_account')
                    WHERE key=1 AND name='account' AND desc=0 AND coll='BINARY')",
        [],
        |r| r.get(0),
    )?;
    if target {
        return Ok(());
    }
    tx.execute_batch(
        "DROP INDEX IF EXISTS requests_account;
         CREATE INDEX requests_account ON requests(account);",
    )
}

// Every label reference except requests.thread, whose ids now belong to the independent
// threads dictionary. Used once by the v16 migration to drop the moved session names from
// `labels`.
const LABEL_REFERENCES: &[(&str, &str)] = &[
    ("requests", "account"),
    ("requests", "first_account"),
    ("requests", "model"),
    ("requests", "effort"),
    ("requests", "endpoint"),
    ("requests", "error"),
    ("requests", "key"),
    ("requests", "instance"),
    ("requests", "plan"),
    ("requests", "tier"),
    ("requests", "req_tier"),
    ("requests", "resp_model"),
    ("requests", "resp_effort"),
    ("requests", "incomplete"),
    ("requests", "err_source"),
    ("quota_obs", "account"),
    ("quota_obs", "instance"),
    ("quota_obs", "plan"),
    ("quota_reset_events", "account"),
    ("quota_cycle_estimates", "account"),
    ("quota_est_epochs", "account"),
    ("refusals", "key"),
    ("refusals", "route"),
    ("refusals", "ua"),
    ("refusals", "model"),
    ("refusals", "account"),
    ("refusals", "instance"),
    ("occupancy", "instance"),
];

fn migrate_v16(tx: &rusqlite::Transaction<'_>) -> rusqlite::Result<()> {
    tx.execute_batch(LATEST)?;
    tx.execute_batch(
        "INSERT INTO threads(id,name) SELECT DISTINCT l.id,l.name FROM labels l JOIN requests r ON r.thread=l.id;
         UPDATE reset_consume SET code=lower(code);"
    )?;
    // Collect the referenced ids column by column into a deduplicating table: one
    // `NOT IN (<27-arm UNION>)` held ~240 MiB of temp b-tree for 1M rows (temp_store=MEMORY).
    tx.execute_batch("CREATE TEMP TABLE label_keep(id INTEGER PRIMARY KEY)")?;
    for (table, column) in LABEL_REFERENCES {
        tx.execute_batch(&format!(
            "INSERT OR IGNORE INTO label_keep SELECT {column} FROM {table} WHERE {column} IS NOT NULL"
        ))?;
    }
    tx.execute_batch(
        "DELETE FROM labels WHERE id NOT IN (SELECT id FROM label_keep); DROP TABLE label_keep;",
    )
}

/// Create or migrate the schema atomically, preserving existing ids and values.
pub(crate) fn migrate(c: &mut Connection) -> rusqlite::Result<()> {
    let version: i64 = c.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version >= SCHEMA_VERSION {
        return Ok(());
    }
    let tx = c.transaction()?;
    if version == 0 {
        tx.execute_batch(LATEST)?;
        crate::migrate_v12::load_or_create_aliaser(&tx)?;
    } else {
        for &(to, step) in STEPS {
            if version < to {
                step(&tx)?;
            }
        }
        tx.execute_batch(LATEST)?;
    }
    tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    tx.commit()?;
    if (1..12).contains(&version) {
        let _ = c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)");
    }
    Ok(())
}

pub(crate) fn open_and_migrate(p: &Path) -> Result<Connection, UsageError> {
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

/// Open a reusable read-only connection; its caller owns the snapshot transaction.
pub(crate) fn open_read(p: &Path) -> Result<Connection, UsageError> {
    let c = Connection::open_with_flags(
        p,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    c.busy_timeout(std::time::Duration::from_secs(5))?;
    Ok(c)
}

#[cfg(test)]
pub(crate) use open_and_migrate as open_rw;

pub(crate) fn open_write(p: &Path) -> Result<Connection, UsageError> {
    let c = Connection::open(p)?;
    c.busy_timeout(std::time::Duration::from_secs(5))?;
    Ok(c)
}

#[cfg(test)]
pub(crate) fn open_ro(p: &Path) -> Result<Connection, UsageError> {
    let c = open_read(p)?;
    c.execute_batch("BEGIN DEFERRED")?;
    Ok(c)
}
