//! Schema (+ migrations), batch insert, purge and read queries.
use crate::problem::{Outcome, Preds, Problem};
use crate::{QuotaObs, Record, Transport, UsageError, SEQ_BITS};
use ciallopa_core::config::QuotaWeights;
use rusqlite::{params, params_from_iter, Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;

/// Endpoint label of a Codex WS prewarm turn (`response.create` with `generate:false`).
pub const PREWARM_ENDPOINT: &str = "prewarm";

pub(crate) const SCHEMA_VERSION: i64 = 13;

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
    // v12: rewrite account labels to aliases and scrub historical error payloads.
    // Runs for every upgrade into 12 (including fresh DBs, where it only seeds alias_key).
    if v < 12 {
        tx.execute_batch("PRAGMA secure_delete = ON;")?;
        crate::migrate_v12::rewrite(&tx)?;
    }
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

/// Writer-side caches: `labels` (name → id) and the last quota snapshot per account.
pub(crate) struct WriterState {
    labels: HashMap<String, i64>,
    last_quota: HashMap<i64, QuotaObs>,
    instance: String,
    loaded: bool,
}
impl WriterState {
    pub fn new(instance: String) -> Self {
        Self {
            labels: HashMap::new(),
            last_quota: HashMap::new(),
            instance,
            loaded: false,
        }
    }
    /// Drop caches (after a failed transaction they may reference rolled-back rows).
    pub fn reset(&mut self) {
        self.labels.clear();
        self.last_quota.clear();
        self.loaded = false;
    }
    fn label(&mut self, c: &Connection, name: Option<&str>) -> rusqlite::Result<Option<i64>> {
        let Some(name) = name else { return Ok(None) };
        if let Some(id) = self.labels.get(name) {
            return Ok(Some(*id));
        }
        c.execute("INSERT OR IGNORE INTO labels(name) VALUES (?1)", [name])?;
        let id: i64 = c.query_row("SELECT id FROM labels WHERE name = ?1", [name], |r| {
            r.get(0)
        })?;
        self.labels.insert(name.to_string(), id);
        Ok(Some(id))
    }
    fn load_quota(&mut self, c: &Connection) -> rusqlite::Result<()> {
        if self.loaded {
            return Ok(());
        }
        let mut st = c.prepare(
            "SELECT q.account, (SELECT name FROM labels WHERE id=q.plan), q.p_pct, q.p_reset, q.p_win, \
             q.s_pct, q.s_reset, q.s_win FROM quota_obs q \
             WHERE q.id = (SELECT max(id) FROM quota_obs WHERE account = q.account)",
        )?;
        let rows = st.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                QuotaObs {
                    plan: r.get(1)?,
                    p_pct: r.get(2)?,
                    p_reset: r.get(3)?,
                    p_window_min: r.get(4)?,
                    s_pct: r.get(5)?,
                    s_reset: r.get(6)?,
                    s_window_min: r.get(7)?,
                },
            ))
        })?;
        for row in rows {
            let (a, q) = row?;
            self.last_quota.insert(a, q);
        }
        self.loaded = true;
        Ok(())
    }
}

/// Reset times derived from `reset-after-seconds` jitter by a second or two between responses.
const RESET_TOLERANCE_S: i64 = 120;

fn same_reset(a: Option<i64>, b: Option<i64>) -> bool {
    match (a, b) {
        (Some(x), Some(y)) => (x - y).abs() <= RESET_TOLERANCE_S,
        (None, None) => true,
        _ => false,
    }
}

fn quota_changed(old: Option<&QuotaObs>, new: &QuotaObs) -> bool {
    let Some(o) = old else { return true };
    o.plan != new.plan
        || o.p_pct != new.p_pct
        || o.s_pct != new.s_pct
        || !same_reset(o.p_reset, new.p_reset)
        || !same_reset(o.s_reset, new.s_reset)
}

pub(crate) fn insert_batch(
    c: &mut Connection,
    ws: &mut WriterState,
    rows: &[Record],
) -> rusqlite::Result<()> {
    let tx = c.transaction()?;
    ws.load_quota(&tx)?;
    {
        let mut st = tx.prepare_cached(
            "INSERT OR IGNORE INTO requests(id,account,first_account,model,effort,transport,endpoint,\
             status,error,retries,switches,ttft_ms,duration_ms,in_tok,out_tok,cached_tok,reasoning_tok,\
             key,instance,plan,p_pct,p_reset,s_pct,s_reset,tier,cw_tok,thread,turn,req_tier,resp_model,resp_effort,incomplete,err_source,up_status) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31,?32,?33,?34)",
        )?;
        let mut qst = tx.prepare_cached(
            "INSERT OR IGNORE INTO quota_obs(id,account,instance,plan,p_pct,p_reset,p_win,s_pct,s_reset,s_win) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        )?;
        let mut dst = tx.prepare_cached(
            "INSERT OR IGNORE INTO error_detail(id,message,body,body_len,truncated,headers,up_req_id,attempts) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
        )?;
        let instance_name = ws.instance.clone();
        let instance = ws.label(&tx, Some(&instance_name))?;
        for r in rows {
            let account = ws.label(&tx, r.account.as_deref())?;
            let first = ws.label(&tx, r.first_account.as_deref())?;
            let model = ws.label(&tx, r.model.as_deref())?;
            let effort = ws.label(&tx, r.effort.as_deref())?;
            let endpoint = ws.label(&tx, Some(r.endpoint))?;
            let error = ws.label(&tx, r.error.as_deref())?;
            let key = ws.label(&tx, r.key.as_deref())?;
            let q = r.quota.as_ref();
            let plan = ws.label(&tx, q.and_then(|q| q.plan.as_deref()))?;
            let tier = ws.label(&tx, r.service_tier.as_deref())?;
            let thread = ws.label(&tx, r.thread_id.as_deref())?;
            let req_tier = ws.label(&tx, r.req_tier.as_deref())?;
            let resp_model = ws.label(&tx, r.resp_model.as_deref())?;
            let resp_effort = ws.label(&tx, r.resp_effort.as_deref())?;
            let incomplete = ws.label(&tx, r.incomplete.as_deref())?;
            let err_source = ws.label(&tx, r.err_source)?;
            let n = st.execute(params![
                r.id,
                account,
                first,
                model,
                effort,
                r.transport as i64,
                endpoint,
                r.status,
                error,
                r.retries,
                r.switches,
                r.ttft_ms,
                r.duration_ms,
                r.input_tokens,
                r.output_tokens,
                r.cached_tokens,
                r.reasoning_tokens,
                key,
                instance,
                plan,
                q.and_then(|q| q.p_pct),
                q.and_then(|q| q.p_reset),
                q.and_then(|q| q.s_pct),
                q.and_then(|q| q.s_reset),
                tier,
                r.cache_write_tokens,
                thread,
                r.turn_id,
                req_tier,
                resp_model,
                resp_effort,
                incomplete,
                err_source,
                r.upstream_status,
            ])?;
            if let (1, Some(d)) = (n, r.detail.as_deref()) {
                if r.status >= 400 || r.error.is_some() || d.attempts.is_some() {
                    let s = d.finish();
                    dst.execute(params![
                        r.id,
                        s.message,
                        s.body,
                        s.body_len,
                        s.truncated,
                        s.headers,
                        s.upstream_request_id,
                        s.attempts
                    ])?;
                }
            }
            if let (1, Some(q), Some(a)) = (n, q, account) {
                if quota_changed(ws.last_quota.get(&a), q) {
                    qst.execute(params![
                        r.id,
                        a,
                        instance,
                        plan,
                        q.p_pct,
                        q.p_reset,
                        q.p_window_min,
                        q.s_pct,
                        q.s_reset,
                        q.s_window_min
                    ])?;
                    ws.last_quota.insert(a, q.clone());
                }
            }
        }
    }
    tx.commit()
}

/// Quota snapshots read on request: always stored (their time is the freshness shown).
pub(crate) fn insert_quota(
    c: &mut Connection,
    ws: &mut WriterState,
    rows: &[(i64, String, String, QuotaObs)],
) -> rusqlite::Result<()> {
    let tx = c.transaction()?;
    ws.load_quota(&tx)?;
    {
        let mut qst = tx.prepare_cached(
            "INSERT OR IGNORE INTO quota_obs(id,account,instance,plan,p_pct,p_reset,p_win,s_pct,s_reset,s_win) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        )?;
        for (id, account, by, q) in rows {
            let Some(a) = ws.label(&tx, Some(account))? else {
                continue;
            };
            let by = ws.label(&tx, Some(by))?;
            let plan = ws.label(&tx, q.plan.as_deref())?;
            qst.execute(params![
                id,
                a,
                by,
                plan,
                q.p_pct,
                q.p_reset,
                q.p_window_min,
                q.s_pct,
                q.s_reset,
                q.s_window_min
            ])?;
            ws.last_quota.insert(a, q.clone());
        }
    }
    tx.commit()
}

/// Delete at most `chunk` rows (requests, then quota_obs) started before `cutoff_ms`.
pub(crate) fn purge_before(
    c: &mut Connection,
    cutoff_ms: i64,
    chunk: u64,
) -> rusqlite::Result<u64> {
    let cut = cutoff_ms << SEQ_BITS;
    let n = c.execute(
        "DELETE FROM requests WHERE id IN (SELECT id FROM requests WHERE id < ?1 ORDER BY id LIMIT ?2)",
        params![cut, chunk as i64],
    )?;
    c.execute(
        "DELETE FROM error_detail WHERE id IN (SELECT id FROM error_detail WHERE id < ?1 ORDER BY id LIMIT ?2)",
        params![cut, chunk as i64],
    )?;
    if (n as u64) < chunk {
        // quota_obs is tiny (changes only); keep each account's latest snapshot.
        c.execute(
            "DELETE FROM quota_obs WHERE id < ?1 AND id < (SELECT max(id) FROM quota_obs q WHERE q.account = quota_obs.account)",
            [cut],
        )?;
    }
    Ok(n as u64)
}

/// Delete refusal minutes before `cutoff_ms`, at most a day of minutes per call (the groups
/// per minute are bounded by the aggregator). Returns true when more may remain.
pub(crate) fn purge_refusals_before(c: &Connection, cutoff_ms: i64) -> rusqlite::Result<bool> {
    const MINUTES: i64 = 1440;
    let last: Option<i64> = c
        .query_row(
            "SELECT minute FROM (SELECT DISTINCT minute FROM refusals WHERE minute < ?1 ORDER BY minute LIMIT ?2) ORDER BY minute DESC LIMIT 1",
            params![cutoff_ms, MINUTES],
            |r| r.get(0),
        )
        .optional()?;
    let Some(last) = last else { return Ok(false) };
    c.execute("DELETE FROM refusals WHERE minute <= ?1", [last])?;
    Ok(last + 60_000 < cutoff_ms
        && c.query_row(
            "SELECT 1 FROM refusals WHERE minute < ?1 LIMIT 1",
            [cutoff_ms],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn label_map(c: &Connection) -> rusqlite::Result<HashMap<i64, String>> {
    let mut st = c.prepare_cached("SELECT id, name FROM labels")?;
    let rows = st.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
    rows.collect()
}

pub(crate) fn label_id(c: &Connection, name: &str) -> rusqlite::Result<Option<i64>> {
    c.query_row("SELECT id FROM labels WHERE name = ?1", [name], |r| {
        r.get(0)
    })
    .optional()
}

/// Filter on label-valued columns. `Err(())` = a requested label is unknown ⇒ nothing matches.
#[derive(Debug, Clone, Default)]
pub struct Filters {
    pub account: Option<String>,
    pub model: Option<String>,
    pub key: Option<String>,
    pub instance: Option<String>,
}
impl Filters {
    /// SQL fragment (" AND col = ?") + params, resolved to label ids.
    fn sql(&self, c: &Connection) -> rusqlite::Result<Option<(String, Vec<i64>)>> {
        let mut s = String::new();
        let mut p = vec![];
        for (col, v) in [
            ("account", &self.account),
            ("model", &self.model),
            ("key", &self.key),
            ("instance", &self.instance),
        ] {
            if let Some(v) = v {
                match label_id(c, v)? {
                    Some(id) => {
                        s.push_str(&format!(" AND {col} = ?"));
                        p.push(id);
                    }
                    None => return Ok(None),
                }
            }
        }
        Ok(Some((s, p)))
    }
}

#[derive(Debug, Clone, Default)]
pub struct RecentQuery {
    pub limit: usize,
    /// Only rows started before this time (ms); pagination cursor.
    pub before_ms: Option<i64>,
    /// Exclusive id cursor; unlike a millisecond cursor, preserves rows sharing a timestamp.
    pub before_id: Option<i64>,
    pub since_ms: Option<i64>,
    pub filters: Filters,
    pub errors_only: bool,
    /// Rows to skip (offset paging; `before_id` preserves rows sharing a timestamp).
    pub offset: usize,
    /// Served tier: `priority` (`fast`), `flex` or `standard`.
    pub tier: Option<String>,
    /// Only rows whose requested priority/flex tier was not served.
    pub downgraded: bool,
    /// Error sources (`client`, `local`, `network`, `upstream`, `stream`); empty = any.
    pub sources: Vec<String>,
    /// Exact error code.
    pub error: Option<String>,
    /// Only rows that retried or switched accounts.
    pub retried: bool,
    /// Mutually exclusive request outcome. Prewarm is excluded from the other outcomes.
    pub outcome: Option<Outcome>,
    /// Business requests only, matching summary.count; composes with other filters.
    pub exclude_prewarm: bool,
    /// Only failures of this [`failure_class`] (implies `outcome = failed`; another outcome
    /// matches nothing). An unknown class matches nothing.
    pub failure_class: Option<String>,
    /// Problem bucket ([`crate::problem`]); `Refused` matches no request row.
    pub problem: Option<Problem>,
    /// Cause within `problem`; a cause that `problem` does not have matches nothing.
    pub cause: Option<String>,
    /// Exact Codex thread (session) id; resolve a fragment first with [`resolve_thread`].
    pub thread: Option<String>,
}

/// WHERE clause (without ORDER/LIMIT) + params for [`recent`] / [`recent_count`].
/// `None` ⇒ nothing can match.
fn recent_where(c: &Connection, q: &RecentQuery) -> rusqlite::Result<Option<(String, Vec<i64>)>> {
    let Some((fsql, fparams)) = q.filters.sql(c)? else {
        return Ok(None);
    };
    let hi = q.before_ms.map(|b| (b << SEQ_BITS) - 1).unwrap_or(i64::MAX);
    let lo = q.since_ms.map(|s| s << SEQ_BITS).unwrap_or(0);
    let mut s = format!("id BETWEEN ? AND ?{fsql}");
    let mut ps: Vec<i64> = vec![lo, hi];
    ps.extend(fparams);
    if let Some(t) = q.thread.as_deref().map(str::trim).filter(|t| !t.is_empty()) {
        // Served by the partial `requests_thread` index.
        match label_id(c, t)? {
            Some(id) => {
                s.push_str(" AND thread = ?");
                ps.push(id);
            }
            None => return Ok(None),
        }
    }
    if let Some(id) = q.before_id {
        s.push_str(" AND id < ?");
        ps.push(id);
    }
    if q.errors_only {
        s.push_str(" AND (error IS NOT NULL OR status >= 400)");
    }
    // Unknown label ⇒ -1 (never a valid id).
    let prio = label_id(c, "priority")?.unwrap_or(-1);
    let flex = label_id(c, "flex")?.unwrap_or(-1);
    match q.tier.as_deref().map(|t| t.trim().to_ascii_lowercase()) {
        None => {}
        Some(t) if t.is_empty() => {}
        Some(t) if t == "priority" || t == "fast" => {
            s.push_str(" AND tier = ?");
            ps.push(prio);
        }
        Some(t) if t == "flex" => {
            s.push_str(" AND tier = ?");
            ps.push(flex);
        }
        Some(_) => {
            s.push_str(" AND (tier IS NULL OR tier NOT IN (?, ?))");
            ps.extend([prio, flex]);
        }
    }
    if q.downgraded {
        s.push_str(" AND req_tier IN (?, ?) AND (tier IS NULL OR tier != req_tier)");
        ps.extend([prio, flex]);
    }
    if !q.sources.is_empty() {
        let ids: Vec<i64> = q
            .sources
            .iter()
            .take(8)
            .map(|n| label_id(c, n.trim()).map(|x| x.unwrap_or(-1)))
            .collect::<rusqlite::Result<_>>()?;
        s.push_str(&format!(
            " AND err_source IN ({})",
            vec!["?"; ids.len()].join(",")
        ));
        ps.extend(ids);
    }
    if let Some(e) = q.error.as_deref().map(str::trim).filter(|e| !e.is_empty()) {
        s.push_str(" AND error = ?");
        ps.push(label_id(c, e)?.unwrap_or(-1));
    }
    if q.retried {
        s.push_str(" AND (retries > 0 OR switches > 0)");
    }
    if q.exclude_prewarm {
        s.push_str(" AND (endpoint IS NULL OR endpoint != ?)");
        ps.push(label_id(c, PREWARM_ENDPOINT)?.unwrap_or(-1));
    }
    let outcome = match (q.failure_class.as_deref(), q.outcome) {
        (None, o) => o,
        (Some(_), None | Some(Outcome::Failed)) => Some(Outcome::Failed),
        (Some(_), Some(_)) => return Ok(None),
    };
    let preds = Preds::new(c)?;
    if let Some(class) = q.failure_class.as_deref() {
        match preds.failure_class(class) {
            Some(p) => s.push_str(&format!(" AND {p}")),
            None => return Ok(None),
        }
    } else if let Some(outcome) = outcome {
        s.push_str(&format!(" AND {}", preds.outcome(outcome)));
    }
    if let Some(problem) = q.problem {
        let cause = q.cause.as_deref().map(str::trim).filter(|c| !c.is_empty());
        match preds.problem(problem, cause) {
            Some(p) => s.push_str(&format!(" AND {p}")),
            None => return Ok(None),
        }
    }
    Ok(Some((s, ps)))
}

/// Result of [`resolve_thread`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadMatch {
    /// The full thread id (input was the id itself or a unique fragment of it).
    Exact(String),
    /// No recorded request carries such a thread.
    None,
    /// The fragment fits more than one recorded thread.
    Ambiguous,
}

/// Shortest fragment accepted in place of a full thread id.
pub const THREAD_FRAGMENT_MIN: usize = 8;

/// Resolve a pasted session id: the full id, or a fragment of at least
/// [`THREAD_FRAGMENT_MIN`] chars (`[A-Za-z0-9-]`, ASCII case-insensitive, anywhere in the id,
/// since UUIDv7 ids of one minute share their leading chars). A leading `…` is ignored, so
/// the short form the UI shows can be pasted back.
pub(crate) fn resolve_thread(c: &Connection, input: &str) -> rusqlite::Result<ThreadMatch> {
    let t = input.trim();
    let t = t
        .strip_prefix('…')
        .or_else(|| t.strip_prefix("..."))
        .unwrap_or(t)
        .trim();
    if t.is_empty() || t.len() > 128 {
        return Ok(ThreadMatch::None);
    }
    let used = |id: i64| -> rusqlite::Result<bool> {
        c.query_row(
            "SELECT 1 FROM requests WHERE thread = ?1 LIMIT 1",
            [id],
            |_| Ok(true),
        )
        .optional()
        .map(|x| x.unwrap_or(false))
    };
    if let Some(id) = label_id(c, t)? {
        if used(id)? {
            return Ok(ThreadMatch::Exact(t.to_string()));
        }
    }
    if t.len() < THREAD_FRAGMENT_MIN || !t.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
        return Ok(ThreadMatch::None);
    }
    // One scan of `labels` (one row per distinct name); the EXISTS probe uses the thread index.
    let mut st = c.prepare_cached(
        "SELECT l.name FROM labels l WHERE l.name LIKE '%' || ?1 || '%' \
         AND EXISTS(SELECT 1 FROM requests r WHERE r.thread = l.id) LIMIT 2",
    )?;
    let names: Vec<String> = st
        .query_map([t], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let mut names = names.into_iter();
    Ok(match (names.next(), names.next()) {
        (None, _) => ThreadMatch::None,
        (Some(n), None) => ThreadMatch::Exact(n),
        (Some(_), Some(_)) => ThreadMatch::Ambiguous,
    })
}

/// Total rows matching `q` (ignores `limit`/`offset`).
pub(crate) fn recent_count(c: &Connection, q: &RecentQuery) -> Result<u64, UsageError> {
    let Some((w, ps)) = recent_where(c, q)? else {
        return Ok(0);
    };
    let n: i64 = c.query_row(
        &format!("SELECT COUNT(*) FROM requests WHERE {w}"),
        params_from_iter(ps),
        |r| r.get(0),
    )?;
    Ok(n as u64)
}

/// Requests per problem and cause under `q` (its own `problem`/`cause` and paging ignored);
/// `refused` is left for the caller, which reads the refusals table.
pub(crate) fn problem_counts(
    c: &Connection,
    q: &RecentQuery,
) -> Result<crate::problem::ProblemCounts, UsageError> {
    let q = RecentQuery {
        before_id: None,
        problem: None,
        cause: None,
        ..q.clone()
    };
    let Some((w, ps)) = recent_where(c, &q)? else {
        return Ok(crate::problem::ProblemCounts::zero());
    };
    let cols = crate::problem::count_columns(&Preds::new(c)?);
    Ok(c.query_row(
        &format!("SELECT {cols} FROM requests WHERE {w}"),
        params_from_iter(ps),
        crate::problem::counts_from,
    )?)
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RecordRow {
    pub request_id: String,
    pub ts_ms: i64,
    pub account: Option<String>,
    pub first_account: Option<String>,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub transport: &'static str,
    pub endpoint: Option<String>,
    pub status: u16,
    pub error: Option<String>,
    pub retries: u32,
    pub switches: u32,
    pub ttft_ms: Option<u32>,
    pub duration_ms: u32,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
    /// output_tokens / generation seconds (duration − ttft).
    pub tps: Option<f64>,
    pub key: Option<String>,
    pub instance: Option<String>,
    pub plan: Option<String>,
    pub primary_used_pct: Option<f64>,
    pub primary_reset_at: Option<i64>,
    pub secondary_used_pct: Option<f64>,
    pub secondary_reset_at: Option<i64>,
    pub cache_write_tokens: Option<i64>,
    /// Tier actually served (response; else the request's). Pricing uses this.
    pub service_tier: Option<String>,
    /// Tier the client asked for (`fast` normalised to `priority`).
    pub requested_service_tier: Option<String>,
    /// `response.model` reported by the upstream.
    pub response_model: Option<String>,
    /// `response.reasoning.effort` reported by the upstream.
    pub response_effort: Option<String>,
    /// `response.incomplete` reason; null = not incomplete. Tokens are recorded; not an error.
    pub incomplete: Option<String>,
    /// null: no response model; false: same (or a dated snapshot of it); true: different.
    pub model_mismatch: Option<bool>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    /// Notional USD from tokens × price table at query time; null when unpriced.
    pub cost_usd: Option<f64>,
    /// `client`, `local`, `network`, `upstream` or `stream` (null = ok or older rows).
    pub err_source: Option<String>,
    /// Last upstream HTTP status when it differs in meaning from `status`.
    pub upstream_status: Option<u16>,
    /// An `error_detail` row exists (`GET /usage/v1/records/{id}/detail`).
    pub has_detail: bool,
    /// [`failure_class`] when the row is a failure, else null.
    pub failure_class: Option<&'static str>,
    /// Mutually exclusive outcome ([`crate::problem::outcome`]); the UI shows this one.
    pub outcome: crate::problem::Outcome,
    /// `error` (报错) or `unfinished` (没答完); null for completed or prewarm rows.
    pub problem: Option<&'static str>,
    /// Cause within `problem` (see [`crate::problem`]).
    pub cause: Option<&'static str>,
    /// Counted under 上游出错: an upstream, stream or network failure, or rescued.
    pub upstream_fault: bool,
    /// Completed only after a retry or an account switch (已救回).
    pub rescued: bool,
    /// Suggested next steps (see [`crate::problem::Class::next`]).
    pub next: Vec<&'static str>,
    /// `ok`, `ignored` (model on the price table's `ignore` list: cost 0), `unknown_price`
    /// (model/tier/date not in the price table) or `no_tokens`.
    pub price_status: &'static str,
}

pub(crate) fn models_since(c: &Connection, since_ms: i64) -> Result<Vec<String>, UsageError> {
    let mut st = c.prepare(
        "SELECT DISTINCT l.name FROM requests r JOIN labels l ON l.id = r.model \
         WHERE r.id >= ? AND (r.in_tok IS NOT NULL OR r.out_tok IS NOT NULL)",
    )?;
    let rows = st.query_map([since_ms.max(0) << SEQ_BITS], |r| r.get::<_, String>(0))?;
    let mut out = std::collections::BTreeSet::new();
    for m in rows {
        out.insert(m?.trim().to_ascii_lowercase());
    }
    Ok(out.into_iter().filter(|m| !m.is_empty()).collect())
}

/// One distinct model / key label / instance from recent rows (scope-filter suggestions).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ScopeValue {
    pub value: String,
    pub count: u64,
    pub last_ms: i64,
}

/// Distinct `model`, `key` and `instance` labels over the newest `max_rows` rows (the table
/// only holds the retention window), each sorted by use count, then most recent.
/// One bounded scan of the primary key; callers cache the result.
pub(crate) fn scope_values(
    c: &Connection,
    max_rows: u32,
) -> Result<[Vec<ScopeValue>; 3], UsageError> {
    let names = label_map(c)?;
    let mut st = c.prepare(
        "SELECT model,key,instance,count(*),max(id) FROM \
         (SELECT model,key,instance,id FROM requests ORDER BY id DESC LIMIT ?) \
         GROUP BY model,key,instance",
    )?;
    let mut acc: [HashMap<i64, (u64, i64)>; 3] = Default::default();
    let mut rows = st.query([max_rows])?;
    while let Some(r) = rows.next()? {
        let n: i64 = r.get(3)?;
        let last: i64 = r.get(4)?;
        for (i, m) in acc.iter_mut().enumerate() {
            if let Some(id) = r.get::<_, Option<i64>>(i)? {
                let e = m.entry(id).or_insert((0, 0));
                e.0 += n.max(0) as u64;
                e.1 = e.1.max(last >> SEQ_BITS);
            }
        }
    }
    Ok(acc.map(|m| {
        let mut v: Vec<ScopeValue> = m
            .into_iter()
            .filter_map(|(id, (count, last_ms))| {
                let value = names.get(&id)?.trim();
                (!value.is_empty()).then(|| ScopeValue {
                    value: value.to_string(),
                    count,
                    last_ms,
                })
            })
            .collect();
        v.sort_by(|a, b| {
            b.count
                .cmp(&a.count)
                .then(b.last_ms.cmp(&a.last_ms))
                .then_with(|| a.value.cmp(&b.value))
        });
        v
    }))
}

/// Row cost + status. Rows without any token counts (errors before output) are `no_tokens`.
fn row_cost(
    prices: &crate::PriceTable,
    model: Option<&str>,
    tier: Option<&str>,
    id: i64,
    tok: [Option<i64>; 4],
    cw: Option<i64>,
) -> (Option<f64>, &'static str) {
    if tok[0].is_none() && tok[1].is_none() {
        return (None, "no_tokens");
    }
    if model.is_some_and(|m| prices.is_ignored(m)) {
        return (Some(0.0), "ignored");
    }
    let u = crate::prices::Usage {
        input: tok[0].unwrap_or(0),
        output: tok[1].unwrap_or(0),
        cached: tok[2].unwrap_or(0),
        cache_write: cw.unwrap_or(0),
    };
    match prices.cost(
        model,
        crate::Tier::from_service_tier(tier),
        id >> SEQ_BITS,
        u,
    ) {
        Some(c) => (Some(c), "ok"),
        None => (None, "unknown_price"),
    }
}

fn tps(out: Option<i64>, ttft: Option<u32>, dur: u32) -> Option<f64> {
    let out = out.filter(|o| *o > 0)?;
    let gen = i64::from(dur) - i64::from(ttft.unwrap_or(0));
    (gen > 0).then(|| out as f64 * 1000.0 / gen as f64)
}

pub(crate) fn recent(
    c: &Connection,
    q: &RecentQuery,
    prices: &crate::PriceTable,
) -> Result<Vec<RecordRow>, UsageError> {
    let Some((w, ps)) = recent_where(c, q)? else {
        return Ok(vec![]);
    };
    record_rows(c, &w, ps, q.limit, q.offset, prices)
}

pub(crate) fn record(
    c: &Connection,
    id: i64,
    prices: &crate::PriceTable,
) -> Result<Option<RecordRow>, UsageError> {
    Ok(record_rows(c, "id = ?", vec![id], 1, 0, prices)?.pop())
}

fn record_rows(
    c: &Connection,
    w: &str,
    mut ps: Vec<i64>,
    limit: usize,
    offset: usize,
    prices: &crate::PriceTable,
) -> Result<Vec<RecordRow>, UsageError> {
    let names = label_map(c)?;
    let sql = format!(
        "SELECT id,account,first_account,model,effort,transport,endpoint,status,error,retries,switches,\
         ttft_ms,duration_ms,in_tok,out_tok,cached_tok,reasoning_tok,key,instance,plan,p_pct,p_reset,s_pct,s_reset,\
         tier,cw_tok,thread,turn,EXISTS(SELECT 1 FROM error_detail d WHERE d.id = requests.id),\
         req_tier,resp_model,resp_effort,incomplete,err_source,up_status \
         FROM requests WHERE {w} ORDER BY id DESC LIMIT ? OFFSET ?"
    );
    // One extra row lets the public API detect a next page without another count query.
    ps.push(limit.clamp(1, 1001) as i64);
    ps.push(offset.min(1_000_000) as i64);
    let mut st = c.prepare(&sql)?;
    let name = |id: Option<i64>| id.and_then(|i| names.get(&i).cloned());
    let rows = st.query_map(params_from_iter(ps), |r| {
        let id: i64 = r.get(0)?;
        let ttft: Option<u32> = r.get(11)?;
        let dur: u32 = r.get(12)?;
        let out: Option<i64> = r.get(14)?;
        let model = name(r.get(3)?);
        let (inp, cached): (Option<i64>, Option<i64>) = (r.get(13)?, r.get(15)?);
        let reasoning: Option<i64> = r.get(16)?;
        let tier = name(r.get(24)?);
        let cw: Option<i64> = r.get(25)?;
        let (cost_usd, price_status) = row_cost(
            prices,
            model.as_deref(),
            tier.as_deref(),
            id,
            [inp, out, cached, reasoning],
            cw,
        );
        let resp_model = name(r.get(30)?);
        let model_mismatch = crate::model_mismatch(model.as_deref(), resp_model.as_deref());
        let (status, error, err_source, endpoint): (u16, _, _, _) = (
            r.get(7)?,
            name(r.get(8)?),
            name(r.get(33)?),
            name(r.get(6)?),
        );
        let upstream_status: Option<u16> = r.get(34)?;
        let failure_class = row_failure_class(
            status,
            error.as_deref(),
            err_source.as_deref(),
            upstream_status,
            endpoint.as_deref(),
        );
        let incomplete = name(r.get(32)?);
        let (retries, switches): (u32, u32) = (r.get(9)?, r.get(10)?);
        let class = crate::problem::classify(&crate::problem::Facts {
            status,
            error: error.as_deref(),
            source: err_source.as_deref(),
            upstream_status,
            endpoint: endpoint.as_deref(),
            incomplete: incomplete.as_deref(),
            retries,
            switches,
        });
        Ok(RecordRow {
            failure_class,
            outcome: class.outcome,
            problem: class.problem,
            cause: class.cause,
            upstream_fault: class.upstream_fault,
            rescued: class.rescued,
            next: class.next,
            requested_service_tier: name(r.get(29)?),
            response_model: resp_model,
            response_effort: name(r.get(31)?),
            incomplete,
            model_mismatch,
            request_id: format!("{id:016x}"),
            ts_ms: id >> SEQ_BITS,
            account: name(r.get(1)?),
            first_account: name(r.get(2)?),
            cost_usd,
            price_status,
            cache_write_tokens: cw,
            service_tier: tier,
            thread_id: name(r.get(26)?),
            turn_id: r.get(27)?,
            has_detail: r.get(28)?,
            model,
            reasoning_effort: name(r.get(4)?),
            transport: Transport::from_i64(r.get(5)?),
            endpoint,
            status,
            error,
            err_source,
            upstream_status,
            retries,
            switches,
            ttft_ms: ttft,
            duration_ms: dur,
            input_tokens: inp,
            output_tokens: out,
            cached_tokens: cached,
            reasoning_tokens: reasoning,
            tps: tps(out, ttft, dur),
            key: name(r.get(17)?),
            instance: name(r.get(18)?),
            plan: name(r.get(19)?),
            primary_used_pct: r.get(20)?,
            primary_reset_at: r.get(21)?,
            secondary_used_pct: r.get(22)?,
            secondary_reset_at: r.get(23)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Summary grouping dimensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dim {
    Account,
    Model,
    Key,
    Instance,
    Effort,
    Transport,
    Endpoint,
    Tier,
    RespModel,
    /// Error code (`usage_limit_reached`, `all_cooling`, …; null = no error).
    Error,
    /// Error source (`upstream`, `network`, `stream`, `local`, `client`; null = no error).
    ErrSource,
}
impl Dim {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.trim() {
            "account" => Dim::Account,
            "model" => Dim::Model,
            "key" => Dim::Key,
            "instance" => Dim::Instance,
            "effort" | "reasoning_effort" => Dim::Effort,
            "transport" => Dim::Transport,
            "endpoint" => Dim::Endpoint,
            "tier" | "service_tier" => Dim::Tier,
            "response_model" | "resp_model" => Dim::RespModel,
            "error" | "error_code" => Dim::Error,
            "err_source" | "error_source" => Dim::ErrSource,
            _ => return None,
        })
    }
    fn col(self) -> &'static str {
        match self {
            Dim::Account => "account",
            Dim::Model => "model",
            Dim::Key => "key",
            Dim::Instance => "instance",
            Dim::Effort => "effort",
            Dim::Transport => "transport",
            Dim::Endpoint => "endpoint",
            Dim::Tier => "tier",
            Dim::RespModel => "resp_model",
            Dim::Error => "error",
            Dim::ErrSource => "err_source",
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Dim::Effort => "reasoning_effort",
            Dim::Tier => "service_tier",
            Dim::RespModel => "response_model",
            d => d.col(),
        }
    }
}

/// Error codes that mean an account (or every account) is out of usage quota: the upstream
/// 429 / in-stream `usage_limit_reached`, and the gateway's own answer when the first account
/// to recover is out of quota.
pub const QUOTA_ERRORS: &[&str] = &[
    "usage_limit",
    "usage_limit_reached",
    "all_quota_exhausted",
    "first_quota_reset",
];

/// Rate-limit codes reported in-stream (no HTTP 429 on the row): cool the model only.
pub const RATE_LIMIT_ERRORS: &[&str] = &["rate_limit_exceeded", "turn_rate_limit_exceeded"];

/// Upstream codes that blame the request itself (too long, malformed): not an upstream or
/// account fault.
pub const REQUEST_ERRORS: &[&str] = &[
    "context_length_exceeded",
    "invalid_request_error",
    "invalid_prompt",
    "invalid_value",
    "unsupported_parameter",
    "unsupported_value",
    "string_above_max_length",
];

/// WebSocket lifecycle codes: Codex reconnects (connection age limit) or resends the full
/// history (unknown `previous_response_id`). Not failures; counted as cancelled.
pub const LIFECYCLE_ERRORS: &[&str] = &[
    "websocket_connection_limit_reached",
    "previous_response_not_found",
];

/// Why a failed request failed, coarse enough to chart: `quota` (usage limit, upstream or
/// local), `rate_limit` (another 429 or an in-stream rate limit), `request` (the upstream
/// rejected the request content), then by source: `upstream`, `network`, `stream`, `local`
/// (the gateway itself), `other`. Cancellations are not failures and have no class.
pub fn failure_class(
    status: u16,
    error: Option<&str>,
    source: Option<&str>,
    upstream_status: Option<u16>,
) -> &'static str {
    if error.is_some_and(|e| QUOTA_ERRORS.contains(&e)) {
        return "quota";
    }
    if status == 429
        || (source == Some("upstream") && upstream_status == Some(429))
        || error.is_some_and(|e| RATE_LIMIT_ERRORS.contains(&e))
    {
        return "rate_limit";
    }
    if error.is_some_and(|e| REQUEST_ERRORS.contains(&e)) {
        return "request";
    }
    match source {
        Some("upstream") => "upstream",
        Some("network") => "network",
        Some("stream") => "stream",
        Some("local") => "local",
        _ => "other",
    }
}

/// Failure classes in display order.
pub const FAILURE_CLASSES: &[&str] = &[
    "quota",
    "rate_limit",
    "request",
    "upstream",
    "network",
    "stream",
    "local",
    "other",
];

/// [`failure_class`] of a stored row, or `None` when the row is not a failure (completed,
/// incomplete, cancelled, unknown outcome or prewarm). Same rules as the summary counts.
pub fn row_failure_class(
    status: u16,
    error: Option<&str>,
    source: Option<&str>,
    upstream_status: Option<u16>,
    endpoint: Option<&str>,
) -> Option<&'static str> {
    let f = crate::problem::Facts {
        status,
        error,
        source,
        upstream_status,
        endpoint,
        ..Default::default()
    };
    (crate::problem::outcome(&f) == Outcome::Failed)
        .then(|| failure_class(status, error, source, upstream_status))
}

#[derive(Debug, Clone)]
pub struct SummaryQuery {
    pub from_ms: i64,
    pub to_ms: i64,
    /// Default: account, model.
    pub group_by: Vec<Dim>,
    pub filters: Filters,
    /// `Some(tz_offset_minutes)` ⇒ also return per-day buckets (local day = UTC + offset).
    pub bucket_day: Option<i32>,
    /// With `bucket_day`: hourly instead of daily buckets (`day` = `YYYY-MM-DD HH`).
    pub bucket_hour: bool,
    /// Leave out `endpoint=prewarm` rows entirely: their tokens, cost and `prewarm_count`
    /// (request counts and latency never include them). Same rows as the records filter.
    pub exclude_prewarm: bool,
    /// Only the total: no groups (cheaper; used for the previous-period comparison).
    pub total_only: bool,
    /// With `bucket_day` and no account / model filter: add local-limit refusals per bucket
    /// (key / instance scope only, like the refusal totals).
    pub bucket_refusals: bool,
}
impl Default for SummaryQuery {
    fn default() -> Self {
        Self {
            from_ms: 0,
            to_ms: i64::MAX >> SEQ_BITS,
            group_by: vec![Dim::Account, Dim::Model],
            filters: Filters::default(),
            bucket_day: None,
            bucket_hour: false,
            exclude_prewarm: false,
            total_only: false,
            bucket_refusals: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Default)]
pub struct SummaryGroup {
    /// Group key: dimension name → value (only the requested dimensions).
    pub group: std::collections::BTreeMap<&'static str, Option<String>>,
    pub count: u64,
    pub errors: u64,
    pub error_rate: f64,
    /// Outcome counts exclude prewarm. Unlike legacy `errors`, failures exclude cancellations.
    pub failures: u64,
    /// `failures` by [`failure_class`] (classes with no failure are omitted).
    pub failure_classes: std::collections::BTreeMap<&'static str, u64>,
    /// Failures whose error came from the upstream HTTP response (`err_source = upstream`,
    /// including upstream quota and rate-limit 429s); local, network and stream errors excluded.
    pub upstream_failures: u64,
    /// `upstream_failures / count`.
    pub upstream_failure_rate: Option<f64>,
    /// Business requests that retried or switched accounts at least once.
    pub retried: u64,
    /// Business requests that switched accounts at least once.
    pub switched: u64,
    /// Business requests that asked for priority / flex and were served another tier.
    pub downgraded: u64,
    pub cancelled: u64,
    pub incomplete: u64,
    /// The gateway could not observe enough of the response to determine its outcome.
    pub unknown: u64,
    pub prewarm_count: u64,
    pub failure_rate: Option<f64>,
    pub cancel_rate: Option<f64>,
    pub ttft_p50_ms: Option<u32>,
    pub ttft_p90_ms: Option<u32>,
    pub ttft_p99_ms: Option<u32>,
    pub duration_p50_ms: Option<u32>,
    pub duration_p90_ms: Option<u32>,
    pub duration_p99_ms: Option<u32>,
    pub duration_samples: u64,
    /// First model output of streaming rows, including rows that later fail or are cancelled.
    pub stream_ttft_p50_ms: Option<u32>,
    pub stream_ttft_p90_ms: Option<u32>,
    pub stream_ttft_p99_ms: Option<u32>,
    pub stream_ttft_samples: u64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cached_tokens: i64,
    pub reasoning_tokens: i64,
    /// Σ output_tokens / Σ generation seconds over rows that have both.
    pub tps: Option<f64>,
    /// Estimated output speed of complete, successful streaming samples with known timing.
    pub generation_tps: Option<f64>,
    pub tps_samples: u64,
    /// Rows with input or output usage (includes prewarm, as do the token totals).
    pub usage_samples: u64,
    /// Business requests (never prewarm) that ended without failure, cancellation or an
    /// unknown outcome, yet carry no input or output usage: the upstream did not report it.
    pub no_usage: u64,
    /// Token-weighted cache-read fraction over rows with both input and cached counts.
    pub cache_ratio: Option<f64>,
    pub cache_samples: u64,
    pub cache_input_tokens: i64,
    /// Σ notional cost of the priced rows; null when no row is priced (never a fake 0).
    pub cost_usd: Option<f64>,
    /// Rows with tokens but no price (model/tier/date missing from the table). > 0 ⇒
    /// `cost_usd` is a lower bound.
    pub unknown_price: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Default)]
pub struct Summary {
    pub from_ms: i64,
    pub to_ms: i64,
    pub group_by: Vec<&'static str>,
    pub total: SummaryGroup,
    pub groups: Vec<SummaryGroup>,
    /// Models (with tiers) that had rows without a price.
    pub unknown_price_models: Vec<String>,
    /// Entries in the price table (0 = no prices yet: every cost is unpriced).
    pub price_models: usize,
    /// Per-day totals (only with `bucket=day`), ascending; days without rows are omitted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buckets: Option<Vec<DayBucket>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Default)]
pub struct DayBucket {
    /// Local date `YYYY-MM-DD` (per `tz` offset).
    pub day: String,
    /// Start of that local day, unix ms.
    pub start_ms: i64,
    /// Exclusive end of the aligned bucket. Clip to the query window when drilling down.
    pub end_ms: i64,
    pub count: u64,
    pub errors: u64,
    pub failures: u64,
    /// `failures` by [`failure_class`].
    pub failure_classes: std::collections::BTreeMap<&'static str, u64>,
    /// Failures with `err_source = upstream` (see [`SummaryGroup::upstream_failures`]).
    pub upstream_failures: u64,
    pub cancelled: u64,
    pub incomplete: u64,
    pub unknown: u64,
    pub prewarm_count: u64,
    /// Nearest-rank percentiles of the bucket's business rows: stream first output and
    /// duration (null without samples).
    pub stream_ttft_p50_ms: Option<u32>,
    pub stream_ttft_p90_ms: Option<u32>,
    pub stream_ttft_p99_ms: Option<u32>,
    pub duration_p50_ms: Option<u32>,
    pub duration_p90_ms: Option<u32>,
    pub duration_p99_ms: Option<u32>,
    /// Local-limit refusals in the bucket (only with `bucket_refusals`; not requests, never
    /// failures). By reason in `refusals`.
    pub refused: u64,
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub refusals: std::collections::BTreeMap<&'static str, u64>,
    /// Rows reporting input or output usage, including prewarm and explicit zero usage.
    pub usage_samples: u64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cached_tokens: i64,
    pub reasoning_tokens: i64,
    pub cost_usd: Option<f64>,
    pub unknown_price: u64,
}

/// days since epoch → `YYYY-MM-DD` (civil_from_days).
fn civil(z: i64) -> String {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}")
}

#[derive(Default)]
struct Acc {
    count: u64,
    errors: u64,
    failures: u64,
    classes: std::collections::BTreeMap<&'static str, u64>,
    upstream: u64,
    retried: u64,
    switched: u64,
    downgraded: u64,
    cancelled: u64,
    incomplete: u64,
    unknown_outcome: u64,
    prewarm: u64,
    ttft: Vec<u32>,
    stream_ttft: Vec<u32>,
    dur: Vec<u32>,
    tok: [i64; 4],
    gen_ms: i64,
    gen_out: i64,
    stream_gen_ms: i64,
    stream_gen_out: i64,
    tps_samples: u64,
    usage_samples: u64,
    no_usage: u64,
    cache_samples: u64,
    cache_input: i64,
    cache_read: i64,
    cost: Option<f64>,
    unknown: u64,
}

struct Observation {
    counted: bool,
    err: bool,
    /// Set for failures.
    class: Option<&'static str>,
    upstream: bool,
    retried: bool,
    switched: bool,
    downgraded: bool,
    cancelled: bool,
    incomplete: bool,
    unknown: bool,
    streaming: bool,
    ttft: Option<u32>,
    dur: u32,
    tok: [Option<i64>; 4],
    cost: (Option<f64>, &'static str),
}

impl Acc {
    fn add(&mut self, o: Observation) {
        let Observation {
            counted,
            err,
            class,
            upstream,
            retried,
            switched,
            downgraded,
            cancelled,
            incomplete,
            unknown,
            streaming,
            ttft,
            dur,
            tok,
            cost,
        } = o;
        for (a, t) in self.tok.iter_mut().zip(tok) {
            *a += t.unwrap_or(0);
        }
        self.usage_samples += u64::from(tok[0].is_some() || tok[1].is_some());
        if let (Some(input), Some(cached)) = (tok[0], tok[2]) {
            if input >= 0 && cached >= 0 && cached <= input {
                self.cache_samples += 1;
                self.cache_input += input;
                self.cache_read += cached;
            }
        }
        match cost {
            (Some(c), _) => *self.cost.get_or_insert(0.0) += c,
            (None, "unknown_price") => self.unknown += 1,
            _ => {}
        }
        if !counted {
            self.prewarm += 1;
            return;
        }
        self.count += 1;
        self.errors += u64::from(err);
        self.cancelled += u64::from(cancelled);
        self.failures += u64::from(err && !cancelled && !unknown);
        if let Some(c) = class {
            *self.classes.entry(c).or_default() += 1;
            self.upstream += u64::from(upstream);
        }
        self.retried += u64::from(retried);
        self.switched += u64::from(switched);
        self.downgraded += u64::from(downgraded);
        self.unknown_outcome += u64::from(unknown);
        self.incomplete += u64::from(incomplete && !err && !cancelled);
        self.no_usage +=
            u64::from(!err && !cancelled && !unknown && tok[0].is_none() && tok[1].is_none());
        if let Some(t) = ttft {
            self.ttft.push(t);
            if streaming {
                self.stream_ttft.push(t);
            }
        }
        self.dur.push(dur);
        if let Some(o) = tok[1].filter(|o| *o > 0) {
            let g = i64::from(dur) - i64::from(ttft.unwrap_or(0));
            if g > 0 {
                self.gen_ms += g;
                self.gen_out += o;
            }
            if streaming && !err && !cancelled && !incomplete && ttft.is_some() && g > 0 {
                self.stream_gen_ms += g;
                self.stream_gen_out += o;
                self.tps_samples += 1;
            }
        }
    }
    fn merge(&mut self, o: &Acc) {
        self.count += o.count;
        self.errors += o.errors;
        self.failures += o.failures;
        for (c, n) in &o.classes {
            *self.classes.entry(c).or_default() += n;
        }
        self.upstream += o.upstream;
        self.retried += o.retried;
        self.switched += o.switched;
        self.downgraded += o.downgraded;
        self.cancelled += o.cancelled;
        self.incomplete += o.incomplete;
        self.unknown_outcome += o.unknown_outcome;
        self.prewarm += o.prewarm;
        self.ttft.extend_from_slice(&o.ttft);
        self.stream_ttft.extend_from_slice(&o.stream_ttft);
        self.dur.extend_from_slice(&o.dur);
        for i in 0..4 {
            self.tok[i] += o.tok[i];
        }
        self.gen_ms += o.gen_ms;
        self.gen_out += o.gen_out;
        self.stream_gen_ms += o.stream_gen_ms;
        self.stream_gen_out += o.stream_gen_out;
        self.tps_samples += o.tps_samples;
        self.usage_samples += o.usage_samples;
        self.no_usage += o.no_usage;
        self.cache_samples += o.cache_samples;
        self.cache_input += o.cache_input;
        self.cache_read += o.cache_read;
        if let Some(c) = o.cost {
            *self.cost.get_or_insert(0.0) += c;
        }
        self.unknown += o.unknown;
    }
    fn finish(
        mut self,
        group: std::collections::BTreeMap<&'static str, Option<String>>,
    ) -> SummaryGroup {
        SummaryGroup {
            group,
            count: self.count,
            errors: self.errors,
            failures: self.failures,
            failure_classes: std::mem::take(&mut self.classes),
            upstream_failures: self.upstream,
            upstream_failure_rate: (self.count > 0)
                .then(|| self.upstream as f64 / self.count as f64),
            retried: self.retried,
            switched: self.switched,
            downgraded: self.downgraded,
            cancelled: self.cancelled,
            incomplete: self.incomplete,
            unknown: self.unknown_outcome,
            prewarm_count: self.prewarm,
            failure_rate: (self.count > 0).then(|| self.failures as f64 / self.count as f64),
            cancel_rate: (self.count > 0).then(|| self.cancelled as f64 / self.count as f64),
            error_rate: if self.count == 0 {
                0.0
            } else {
                self.errors as f64 / self.count as f64
            },
            ttft_p50_ms: pct(&mut self.ttft, 0.5),
            ttft_p90_ms: pct(&mut self.ttft, 0.9),
            ttft_p99_ms: pct(&mut self.ttft, 0.99),
            duration_p50_ms: pct(&mut self.dur, 0.5),
            duration_p90_ms: pct(&mut self.dur, 0.9),
            duration_p99_ms: pct(&mut self.dur, 0.99),
            duration_samples: self.dur.len() as u64,
            stream_ttft_p50_ms: pct(&mut self.stream_ttft, 0.5),
            stream_ttft_p90_ms: pct(&mut self.stream_ttft, 0.9),
            stream_ttft_p99_ms: pct(&mut self.stream_ttft, 0.99),
            stream_ttft_samples: self.stream_ttft.len() as u64,
            input_tokens: self.tok[0],
            output_tokens: self.tok[1],
            cached_tokens: self.tok[2],
            reasoning_tokens: self.tok[3],
            tps: (self.gen_ms > 0).then(|| self.gen_out as f64 * 1000.0 / self.gen_ms as f64),
            generation_tps: (self.stream_gen_ms > 0)
                .then(|| self.stream_gen_out as f64 * 1000.0 / self.stream_gen_ms as f64),
            tps_samples: self.tps_samples,
            usage_samples: self.usage_samples,
            no_usage: self.no_usage,
            cache_ratio: (self.cache_input > 0)
                .then(|| self.cache_read as f64 / self.cache_input as f64),
            cache_samples: self.cache_samples,
            cache_input_tokens: self.cache_input,
            cost_usd: self.cost,
            unknown_price: self.unknown,
        }
    }
}

/// Nearest-rank percentile (selection, O(n)).
fn pct(v: &mut [u32], p: f64) -> Option<u32> {
    if v.is_empty() {
        return None;
    }
    let k = ((v.len() as f64 * p).ceil() as usize).clamp(1, v.len()) - 1;
    Some(*v.select_nth_unstable(k).1)
}

pub(crate) fn summary(
    c: &Connection,
    q: &SummaryQuery,
    prices: &crate::PriceTable,
) -> Result<Summary, UsageError> {
    let names = label_map(c)?;
    let dims = if q.total_only {
        vec![]
    } else if q.group_by.is_empty() {
        vec![Dim::Account, Dim::Model]
    } else {
        q.group_by.clone()
    };
    let mut out = Summary {
        from_ms: q.from_ms,
        to_ms: q.to_ms,
        group_by: dims.iter().map(|d| d.name()).collect(),
        ..Default::default()
    };
    let Some((mut fsql, mut fparams)) = q.filters.sql(c)? else {
        out.total = Acc::default().finish(Default::default());
        out.buckets = q.bucket_day.map(|_| vec![]);
        return Ok(out);
    };
    if q.exclude_prewarm {
        fsql.push_str(" AND (endpoint IS NULL OR endpoint != ?)");
        fparams.push(label_id(c, PREWARM_ENDPOINT)?.unwrap_or(-1));
    }
    let dim_cols: String = dims.iter().map(|d| format!(",{}", d.col())).collect();
    const DIM0: usize = 20;
    let sql = format!(
        "SELECT status,error,ttft_ms,duration_ms,in_tok,out_tok,cached_tok,reasoning_tok,model,id,tier,cw_tok,endpoint,err_source,incomplete,transport,up_status,\
         retries,switches,req_tier{dim_cols} FROM requests WHERE id BETWEEN ? AND ?{fsql}"
    );
    let prio = label_id(c, "priority")?;
    let flex = label_id(c, "flex")?;
    let interrupted = label_id(c, crate::problem::INCOMPLETE_INTERRUPTED)?;
    let mut ps: Vec<i64> = vec![
        q.from_ms << SEQ_BITS,
        if q.to_ms >= i64::MAX >> SEQ_BITS {
            i64::MAX
        } else {
            ((q.to_ms + 1) << SEQ_BITS) - 1
        },
    ];
    ps.extend(fparams);
    let mut st = c.prepare(&sql)?;
    let mut groups: HashMap<Vec<Option<i64>>, Acc> = HashMap::new();
    let mut rows = st.query(params_from_iter(ps))?;
    let mut unknown_models = std::collections::BTreeSet::new();
    let mut days: std::collections::BTreeMap<i64, DayBucket> = Default::default();
    // Per-bucket latency samples (stream first output, duration), turned into percentiles.
    let mut lat: HashMap<i64, (Vec<u32>, Vec<u32>)> = HashMap::new();
    let width = if q.bucket_hour { 3_600_000 } else { 86_400_000 };
    let off = i64::from(q.bucket_day.unwrap_or(0)) * 60_000;
    let bucket_of = |k: i64| {
        let local = k * width;
        DayBucket {
            day: if q.bucket_hour {
                format!(
                    "{} {:02}",
                    civil(local.div_euclid(86_400_000)),
                    local.rem_euclid(86_400_000) / 3_600_000
                )
            } else {
                civil(k)
            },
            start_ms: k * width - off,
            end_ms: (k + 1) * width - off,
            ..Default::default()
        }
    };
    while let Some(r) = rows.next()? {
        let key: Vec<Option<i64>> = (0..dims.len())
            .map(|i| r.get(DIM0 + i))
            .collect::<rusqlite::Result<_>>()?;
        let status: u16 = r.get(0)?;
        let err: Option<i64> = r.get(1)?;
        let tok: [Option<i64>; 4] = [r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?];
        let m: Option<i64> = r.get(8)?;
        let t: Option<i64> = r.get(10)?;
        let lbl = |x: Option<i64>| x.and_then(|x| names.get(&x)).map(String::as_str);
        let cost = row_cost(prices, lbl(m), lbl(t), r.get(9)?, tok, r.get(11)?);
        // Codex WS prewarm (`generate:false`): billed tokens count, the request itself does not.
        let counted = lbl(r.get(12)?) != Some(PREWARM_ENDPOINT);
        let (src, up): (Option<i64>, Option<u16>) = (r.get(13)?, r.get(16)?);
        // Same outcome rules as the request list ([`crate::problem::outcome`]).
        let o = crate::problem::outcome(&crate::problem::Facts {
            status,
            error: lbl(err),
            source: lbl(src),
            upstream_status: up,
            ..Default::default()
        });
        let cancelled = o == Outcome::Cancelled;
        let unknown = o == Outcome::Unknown;
        let failed = o == Outcome::Failed;
        let class = failed.then(|| failure_class(status, lbl(err), lbl(src), up));
        let upstream = failed && lbl(src) == Some("upstream") && class != Some("request");
        // `interrupted` is a normal end (Codex), not a truncated answer.
        let incomplete = r
            .get::<_, Option<i64>>(14)?
            .is_some_and(|i| Some(i) != interrupted);
        let streaming = matches!(r.get::<_, i64>(15)?, 1 | 2);
        let (retries, switches, req_tier): (i64, i64, Option<i64>) =
            (r.get(17)?, r.get(18)?, r.get(19)?);
        let downgraded =
            req_tier.is_some() && (req_tier == prio || req_tier == flex) && t != req_tier;
        let ttft: Option<u32> = r.get(2)?;
        let dur: u32 = r.get(3)?;
        if cost.1 == "unknown_price" {
            let k = match lbl(t) {
                Some(t) if crate::Tier::from_service_tier(Some(t)) != crate::Tier::Standard => {
                    format!(
                        "{} ({})",
                        lbl(m).unwrap_or("?"),
                        crate::Tier::from_service_tier(Some(t)).name()
                    )
                }
                _ => lbl(m).unwrap_or("?").to_string(),
            };
            unknown_models.insert(k);
        }
        if q.bucket_day.is_some() {
            let id: i64 = r.get(9)?;
            let k = ((id >> SEQ_BITS) + off).div_euclid(width);
            let b = days.entry(k).or_insert_with(|| bucket_of(k));
            if counted {
                b.count += 1;
                b.errors += u64::from(err.is_some() || status >= 400);
                b.failures += u64::from(failed);
                b.upstream_failures += u64::from(upstream);
                if let Some(c) = class {
                    *b.failure_classes.entry(c).or_default() += 1;
                }
                let l = lat.entry(k).or_default();
                if let (Some(t), true) = (ttft, streaming) {
                    l.0.push(t);
                }
                l.1.push(dur);
                b.cancelled += u64::from(cancelled);
                b.incomplete += u64::from(incomplete && !failed && !cancelled && !unknown);
                b.unknown += u64::from(unknown);
            } else {
                b.prewarm_count += 1;
            }
            b.input_tokens += tok[0].unwrap_or(0);
            b.usage_samples += u64::from(tok[0].is_some() || tok[1].is_some());
            b.output_tokens += tok[1].unwrap_or(0);
            b.cached_tokens += tok[2].unwrap_or(0);
            b.reasoning_tokens += tok[3].unwrap_or(0);
            match cost {
                (Some(c), _) => *b.cost_usd.get_or_insert(0.0) += c,
                (None, "unknown_price") => b.unknown_price += 1,
                _ => {}
            }
        }
        groups.entry(key).or_default().add(Observation {
            counted,
            err: err.is_some() || status >= 400,
            class,
            upstream,
            retried: retries > 0 || switches > 0,
            switched: switches > 0,
            downgraded,
            cancelled,
            incomplete,
            unknown,
            streaming,
            ttft,
            dur,
            tok,
            cost,
        });
    }
    let value = |d: Dim, id: Option<i64>| -> Option<String> {
        match d {
            Dim::Transport => id.map(|t| Transport::from_i64(t).to_string()),
            _ => id.and_then(|i| names.get(&i).cloned()),
        }
    };
    let mut total = Acc::default();
    let mut gs: Vec<SummaryGroup> = groups
        .into_iter()
        .map(|(k, acc)| {
            total.merge(&acc);
            let g = dims
                .iter()
                .zip(k)
                .map(|(d, id)| (d.name(), value(*d, id)))
                .collect();
            acc.finish(g)
        })
        .collect();
    gs.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.group.cmp(&b.group)));
    out.total = total.finish(Default::default());
    if !q.total_only {
        out.groups = gs;
    }
    out.unknown_price_models = unknown_models.into_iter().collect();
    for (k, (mut t, mut d)) in lat {
        if let Some(b) = days.get_mut(&k) {
            b.stream_ttft_p50_ms = pct(&mut t, 0.5);
            b.stream_ttft_p90_ms = pct(&mut t, 0.9);
            b.stream_ttft_p99_ms = pct(&mut t, 0.99);
            b.duration_p50_ms = pct(&mut d, 0.5);
            b.duration_p90_ms = pct(&mut d, 0.9);
            b.duration_p99_ms = pct(&mut d, 0.99);
        }
    }
    if q.bucket_day.is_some()
        && q.bucket_refusals
        && q.filters.account.is_none()
        && q.filters.model.is_none()
    {
        let rq = RefusalQuery {
            from_ms: q.from_ms,
            to_ms: q.to_ms,
            key: q.filters.key.clone(),
            instance: q.filters.instance.clone(),
            ..Default::default()
        };
        for (minute, reason, n) in refusal_minutes(c, &rq)? {
            // A minute row belongs to the bucket holding its start, clipped to the window.
            let k = (minute.max(q.from_ms) + off).div_euclid(width);
            let b = days.entry(k).or_insert_with(|| bucket_of(k));
            b.refused += n;
            *b.refusals.entry(reason.as_str()).or_default() += n;
        }
    }
    out.buckets = q.bucket_day.map(|_| days.into_values().collect());
    Ok(out)
}

// ---------------------------------------------------------------- quota

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Estimate {
    /// `ok`, `insufficient_data` (Δ% below threshold), `window_reset` (reset time passed),
    /// `no_window`.
    pub status: &'static str,
    pub delta_pct: Option<f64>,
    pub weighted_tokens: Option<f64>,
    pub tokens_per_pct: Option<f64>,
    /// Estimated capacity of the whole window (100%), in weighted tokens.
    pub capacity_tokens: Option<f64>,
    /// Estimated remaining weighted tokens until 100%.
    pub remaining_tokens: Option<f64>,
    pub sample_from_ms: Option<i64>,
    pub sample_to_ms: Option<i64>,
    pub requests: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct WindowQuota {
    pub used_pct: Option<f64>,
    pub resets_at: Option<i64>,
    pub window_minutes: Option<i64>,
    pub estimate: Estimate,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AccountQuota {
    pub account: String,
    pub plan: Option<String>,
    pub observed_at_ms: i64,
    /// Instance that made the latest observation.
    pub observed_by: Option<String>,
    pub primary: WindowQuota,
    pub secondary: WindowQuota,
}

fn empty_estimate(status: &'static str) -> Estimate {
    Estimate {
        status,
        delta_pct: None,
        weighted_tokens: None,
        tokens_per_pct: None,
        capacity_tokens: None,
        remaining_tokens: None,
        sample_from_ms: None,
        sample_to_ms: None,
        requests: None,
    }
}

/// Latest snapshot + capacity estimate per account (see docs/USAGE.md "Quota").
pub(crate) fn quota(
    c: &Connection,
    w: &QuotaWeights,
    now_ms: i64,
) -> Result<Vec<AccountQuota>, UsageError> {
    let names = label_map(c)?;
    let mut st = c.prepare(
        "SELECT q.id,q.account,q.instance,q.plan,q.p_pct,q.p_reset,q.p_win,q.s_pct,q.s_reset,q.s_win \
         FROM quota_obs q WHERE q.id = (SELECT max(id) FROM quota_obs WHERE account = q.account)",
    )?;
    type Latest = (
        i64,
        i64,
        Option<i64>,
        Option<i64>,
        [Option<f64>; 2],
        [Option<i64>; 2],
        [Option<i64>; 2],
    );
    let latest: Vec<Latest> = st
        .query_map([], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                [r.get(4)?, r.get(7)?],
                [r.get(5)?, r.get(8)?],
                [r.get(6)?, r.get(9)?],
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;
    let mut out = vec![];
    for (id, account, inst, plan, pcts, resets, wins) in latest {
        let win = |i: usize| -> Result<WindowQuota, UsageError> {
            let (used, reset, minutes) = (pcts[i], resets[i], wins[i]);
            let estimate = match (used, reset) {
                (None, _) => empty_estimate("no_window"),
                (Some(_), Some(r)) if r * 1000 <= now_ms => empty_estimate("window_reset"),
                (Some(_), r) => estimate(c, w, account, i, r)?,
            };
            Ok(WindowQuota {
                used_pct: used,
                resets_at: reset,
                window_minutes: minutes,
                estimate,
            })
        };
        out.push(AccountQuota {
            account: names.get(&account).cloned().unwrap_or_default(),
            plan: plan.and_then(|p| names.get(&p).cloned()),
            observed_at_ms: id >> SEQ_BITS,
            observed_by: inst.and_then(|p| names.get(&p).cloned()),
            primary: win(0)?,
            secondary: win(1)?,
        });
    }
    out.sort_by(|a, b| a.account.cmp(&b.account));
    Ok(out)
}

/// Weighted tokens between the first and last snapshot of the current window.
fn estimate(
    c: &Connection,
    w: &QuotaWeights,
    account: i64,
    which: usize,
    reset: Option<i64>,
) -> Result<Estimate, UsageError> {
    let (pc, rc) = if which == 0 {
        ("p_pct", "p_reset")
    } else {
        ("s_pct", "s_reset")
    };
    let (lo, hi) = match reset {
        Some(r) => (r - RESET_TOLERANCE_S, r + RESET_TOLERANCE_S),
        None => (i64::MIN, i64::MAX),
    };
    let sql = format!(
        "SELECT min(id), max(id) FROM quota_obs WHERE account = ?1 AND {pc} IS NOT NULL \
         AND ({rc} BETWEEN ?2 AND ?3 OR (?4 AND {rc} IS NULL))"
    );
    let (first, last): (Option<i64>, Option<i64>) =
        c.query_row(&sql, params![account, lo, hi, reset.is_none()], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?;
    let (Some(f), Some(l)) = (first, last) else {
        return Ok(empty_estimate("insufficient_data"));
    };
    let pct_at = |id: i64| -> rusqlite::Result<f64> {
        c.query_row(
            &format!("SELECT {pc} FROM quota_obs WHERE id = ?1"),
            [id],
            |r| r.get(0),
        )
    };
    let delta = pct_at(l)? - pct_at(f)?;
    let mut e = empty_estimate("insufficient_data");
    e.delta_pct = Some(delta);
    e.sample_from_ms = Some(f >> SEQ_BITS);
    e.sample_to_ms = Some(l >> SEQ_BITS);
    // Requests from the first snapshot (inclusive: its tokens were not in that snapshot yet)
    // up to the last one (exclusive).
    let (n, inp, cached, out): (i64, f64, f64, f64) = c.query_row(
        "SELECT count(*), coalesce(sum(max(coalesce(in_tok,0) - coalesce(cached_tok,0), 0)),0), \
         coalesce(sum(cached_tok),0), coalesce(sum(out_tok),0) \
         FROM requests WHERE account = ?1 AND id >= ?2 AND id < ?3",
        params![account, f, l],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )?;
    let weighted = w.input * inp + w.cached * cached + w.output * out;
    e.requests = Some(n);
    e.weighted_tokens = Some(weighted);
    if delta < w.min_delta_pct || weighted <= 0.0 {
        return Ok(e);
    }
    let tpp = weighted / delta;
    let used = pct_at(l)?;
    e.status = "ok";
    e.tokens_per_pct = Some(tpp);
    e.capacity_tokens = Some(tpp * 100.0);
    e.remaining_tokens = Some((100.0 - used).max(0.0) * tpp);
    Ok(e)
}

/// Error detail of one request (`id` = request id).
pub(crate) fn detail(c: &Connection, id: i64) -> Result<Option<crate::DetailRow>, UsageError> {
    let row = c
        .query_row(
            "SELECT message, body, body_len, truncated, headers, up_req_id, attempts FROM error_detail WHERE id = ?1",
            [id],
            |r| {
                let h: String = r.get(4)?;
                Ok(crate::DetailRow {
                    request_id: format!("{id:016x}"),
                    ts_ms: id >> SEQ_BITS,
                    message: r.get(0)?,
                    body: r.get(1)?,
                    body_len: r.get(2)?,
                    truncated: r.get(3)?,
                    headers: serde_json::from_str(&h).unwrap_or(serde_json::Value::Null),
                    upstream_request_id: r.get(5)?,
                    attempts: r
                        .get::<_, Option<String>>(6)?
                        .and_then(|a| serde_json::from_str(&a).ok())
                        .unwrap_or(serde_json::Value::Null),
                })
            },
        )
        .optional()?;
    Ok(row)
}

// ---- refusals (v10)

/// Merge aggregated refusal groups into their per-minute rows (one transaction).
pub(crate) fn upsert_refusals(
    c: &mut Connection,
    ws: &mut WriterState,
    rows: &HashMap<crate::refusals::Group, crate::refusals::Agg>,
) -> rusqlite::Result<()> {
    let tx = c.transaction()?;
    {
        let mut st = tx.prepare_cached(
            "INSERT INTO refusals(minute,reason,transport,key,route,ua,model,account,instance,lim,\
             count,first_ms,last_ms,max_inflight,max_ws,max_buffered,max_bytes,sum_bytes) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18) \
             ON CONFLICT(minute,reason,transport,key,route,ua,model,account,instance,lim) DO UPDATE SET \
             count = count + excluded.count, first_ms = min(first_ms, excluded.first_ms), \
             last_ms = max(last_ms, excluded.last_ms), max_inflight = max(max_inflight, excluded.max_inflight), \
             max_ws = max(max_ws, excluded.max_ws), max_buffered = max(max_buffered, excluded.max_buffered), \
             max_bytes = CASE WHEN max_bytes IS NULL THEN excluded.max_bytes WHEN excluded.max_bytes IS NULL THEN max_bytes ELSE max(max_bytes, excluded.max_bytes) END, \
             sum_bytes = CASE WHEN sum_bytes IS NULL AND excluded.sum_bytes IS NULL THEN NULL ELSE coalesce(sum_bytes,0) + coalesce(excluded.sum_bytes,0) END",
        )?;
        let instance_name = ws.instance.clone();
        let instance = ws.label(&tx, Some(&instance_name))?.unwrap_or(0);
        for (g, a) in rows {
            let key = ws.label(&tx, g.key.as_deref())?.unwrap_or(0);
            let route = ws
                .label(&tx, Some(g.route.as_str()).filter(|r| !r.is_empty()))?
                .unwrap_or(0);
            let ua = ws.label(&tx, g.ua.as_deref())?.unwrap_or(0);
            let model = ws.label(&tx, g.model.as_deref())?.unwrap_or(0);
            let account = ws.label(&tx, g.account.as_deref())?.unwrap_or(0);
            let as_i = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
            st.execute(params![
                g.minute,
                g.reason.code(),
                if g.ws {
                    Transport::Ws as i64
                } else {
                    Transport::Http as i64
                },
                key,
                route,
                ua,
                model,
                account,
                instance,
                as_i(g.limit),
                as_i(a.count),
                a.first_ms,
                a.last_ms,
                as_i(a.max_inflight),
                as_i(a.max_ws),
                as_i(a.max_buffered),
                a.max_bytes.map(as_i),
                a.sum_bytes.map(as_i),
            ])?;
        }
    }
    tx.commit()
}

/// Range and filters for refusal reads. Only key and instance apply (a refusal happens
/// before the account and, for HTTP, the model are known).
#[derive(Debug, Clone, Default)]
pub struct RefusalQuery {
    pub from_ms: i64,
    pub to_ms: i64,
    pub key: Option<String>,
    pub instance: Option<String>,
    pub reason: Option<crate::refusals::Reason>,
    pub limit: usize,
    pub offset: usize,
}

fn refusal_where(c: &Connection, q: &RefusalQuery) -> rusqlite::Result<Option<(String, Vec<i64>)>> {
    // A row covers [minute, minute + 60 s); keep minutes overlapping the range.
    let mut s = String::from(" WHERE minute > ? AND minute <= ?");
    let mut p = vec![q.from_ms - 60_000, q.to_ms];
    for (col, v) in [("key", &q.key), ("instance", &q.instance)] {
        if let Some(v) = v {
            match label_id(c, v)? {
                Some(id) => {
                    s.push_str(&format!(" AND {col} = ?"));
                    p.push(id);
                }
                None => return Ok(None),
            }
        }
    }
    if let Some(r) = q.reason {
        s.push_str(" AND reason = ?");
        p.push(r.code());
    }
    Ok(Some((s, p)))
}

pub(crate) fn refusal_totals(
    c: &Connection,
    q: &RefusalQuery,
) -> rusqlite::Result<crate::refusals::RefusalTotals> {
    let mut t = crate::refusals::RefusalTotals::default();
    let Some((w, p)) = refusal_where(c, q)? else {
        return Ok(t);
    };
    let mut st = c.prepare(&format!(
        "SELECT reason, sum(count), count(*) FROM refusals{w} GROUP BY reason"
    ))?;
    let mut rows = st.query(params_from_iter(p))?;
    while let Some(r) = rows.next()? {
        if let Some(reason) = crate::refusals::Reason::from_code(r.get(0)?) {
            t.add(
                reason,
                r.get::<_, i64>(1)?.max(0) as u64,
                r.get::<_, i64>(2)?.max(0) as u64,
            );
        }
    }
    Ok(t)
}

/// `(minute, reason, count)` per minute and reason, for bucketing.
fn refusal_minutes(
    c: &Connection,
    q: &RefusalQuery,
) -> rusqlite::Result<Vec<(i64, crate::refusals::Reason, u64)>> {
    let Some((w, p)) = refusal_where(c, q)? else {
        return Ok(vec![]);
    };
    let mut st = c.prepare(&format!(
        "SELECT minute, reason, sum(count) FROM refusals{w} GROUP BY minute, reason"
    ))?;
    let rows = st.query_map(params_from_iter(p), |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, i64>(2)?,
        ))
    })?;
    let mut out = vec![];
    for row in rows {
        let (m, reason, n) = row?;
        if let Some(reason) = crate::refusals::Reason::from_code(reason) {
            out.push((m, reason, n.max(0) as u64));
        }
    }
    Ok(out)
}

pub(crate) fn refusal_rows(
    c: &Connection,
    q: &RefusalQuery,
) -> rusqlite::Result<Vec<crate::refusals::RefusalRow>> {
    let Some((w, mut p)) = refusal_where(c, q)? else {
        return Ok(vec![]);
    };
    let labels = label_map(c)?;
    let name = |id: i64| (id != 0).then(|| labels.get(&id).cloned()).flatten();
    p.push(q.limit.clamp(1, 500) as i64);
    p.push(q.offset as i64);
    let mut st = c.prepare(&format!(
        "SELECT minute,reason,transport,key,route,ua,model,account,instance,lim,count,first_ms,last_ms,\
         max_inflight,max_ws,max_buffered,max_bytes,sum_bytes FROM refusals{w} \
         ORDER BY minute DESC, last_ms DESC, count DESC LIMIT ? OFFSET ?"
    ))?;
    let u = |v: i64| v.max(0) as u64;
    let rows = st.query_map(params_from_iter(p), |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, i64>(2)?,
            [
                r.get::<_, i64>(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
                r.get(8)?,
            ],
            [
                r.get::<_, i64>(9)?,
                r.get(10)?,
                r.get(11)?,
                r.get(12)?,
                r.get(13)?,
                r.get(14)?,
                r.get(15)?,
            ],
            (r.get::<_, Option<i64>>(16)?, r.get::<_, Option<i64>>(17)?),
        ))
    })?;
    let mut out = vec![];
    for row in rows {
        let (minute, reason, transport, l, n, (max_bytes, sum_bytes)) = row?;
        let Some(reason) = crate::refusals::Reason::from_code(reason) else {
            continue;
        };
        out.push(crate::refusals::RefusalRow {
            minute_ms: minute,
            reason,
            transport: if transport == Transport::Ws as i64 {
                "ws"
            } else {
                "http"
            },
            key: name(l[0]),
            route: name(l[1]),
            ua: name(l[2]),
            model: name(l[3]),
            account: name(l[4]),
            instance: name(l[5]),
            limit: u(n[0]),
            count: u(n[1]),
            first_ms: n[2],
            last_ms: n[3],
            max_inflight: u(n[4]),
            max_ws: u(n[5]),
            max_buffered_bytes: u(n[6]),
            max_bytes: max_bytes.map(u),
            sum_bytes: sum_bytes.map(u),
        });
    }
    Ok(out)
}

// ---- occupancy (v11)

/// Merge per-minute occupancy into its rows (one transaction).
pub(crate) fn upsert_occupancy(
    c: &mut Connection,
    ws: &mut WriterState,
    rows: &std::collections::BTreeMap<i64, crate::occupancy::Agg>,
) -> rusqlite::Result<()> {
    let tx = c.transaction()?;
    {
        let mut st = tx.prepare_cached(
            "INSERT INTO occupancy(minute,instance,samples,inflight_sum,inflight_peak,ws_sum,ws_peak,\
             buffered_sum,buffered_peak,lim_inflight,lim_ws,lim_buffered) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12) \
             ON CONFLICT(minute,instance) DO UPDATE SET \
             samples = samples + excluded.samples, inflight_sum = inflight_sum + excluded.inflight_sum, \
             inflight_peak = max(inflight_peak, excluded.inflight_peak), ws_sum = ws_sum + excluded.ws_sum, \
             ws_peak = max(ws_peak, excluded.ws_peak), buffered_sum = buffered_sum + excluded.buffered_sum, \
             buffered_peak = max(buffered_peak, excluded.buffered_peak), lim_inflight = excluded.lim_inflight, \
             lim_ws = excluded.lim_ws, lim_buffered = excluded.lim_buffered",
        )?;
        let instance_name = ws.instance.clone();
        let instance = ws.label(&tx, Some(&instance_name))?.unwrap_or(0);
        let as_i = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
        for (minute, a) in rows {
            st.execute(params![
                minute,
                instance,
                as_i(a.samples),
                as_i(a.inflight_sum),
                as_i(a.inflight_peak),
                as_i(a.ws_sum),
                as_i(a.ws_peak),
                as_i(a.buffered_sum),
                as_i(a.buffered_peak),
                as_i(a.limit_inflight),
                as_i(a.limit_ws),
                as_i(a.limit_buffered),
            ])?;
        }
    }
    tx.commit()
}

/// Range and bucket for occupancy reads.
#[derive(Debug, Clone, Default)]
pub struct OccupancyQuery {
    pub from_ms: i64,
    pub to_ms: i64,
    pub instance: Option<String>,
    /// Bucket width in minutes (at least 1).
    pub step_min: u32,
    /// UTC offset (minutes) that bucket boundaries align to, so hour / day buckets match
    /// the summary's local buckets.
    pub tz_min: i32,
}

/// Buckets per instance at most; a wider range gets a wider step.
pub const OCCUPANCY_MAX_BUCKETS: i64 = 1440;

/// The bucket width actually used for `q` (minutes).
pub fn occupancy_step(q: &OccupancyQuery) -> i64 {
    let span = (q.to_ms - q.from_ms).max(60_000) / 60_000;
    let min = (span + OCCUPANCY_MAX_BUCKETS - 1) / OCCUPANCY_MAX_BUCKETS;
    i64::from(q.step_min.max(1)).max(min)
}

pub(crate) fn occupancy_rows(
    c: &Connection,
    q: &OccupancyQuery,
) -> rusqlite::Result<Vec<crate::occupancy::OccupancyRow>> {
    let step = occupancy_step(q) * 60_000;
    // Non-negative offset modulo the step keeps the integer division floor-like.
    let off = (i64::from(q.tz_min) * 60_000).rem_euclid(step);
    let mut w = String::from(" WHERE minute > ? AND minute <= ?");
    let mut p = vec![q.from_ms - 60_000, q.to_ms];
    if let Some(i) = &q.instance {
        match label_id(c, i)? {
            Some(id) => {
                w.push_str(" AND instance = ?");
                p.push(id);
            }
            None => return Ok(vec![]),
        }
    }
    let labels = label_map(c)?;
    let mut st = c.prepare(&format!(
        "SELECT ((minute + {off}) / {step}) * {step} - {off} AS b, instance, sum(samples), sum(inflight_sum), max(inflight_peak), \
         sum(ws_sum), max(ws_peak), sum(buffered_sum), max(buffered_peak), max(lim_inflight), max(lim_ws), \
         max(lim_buffered), \
         sum(CASE WHEN (lim_inflight > 0 AND inflight_peak >= lim_inflight) OR (lim_ws > 0 AND ws_peak >= lim_ws) THEN 1 ELSE 0 END) \
         FROM occupancy{w} GROUP BY b, instance ORDER BY b, instance"
    ))?;
    let rows = st.query_map(params_from_iter(p), |r| {
        let mut n = [0i64; 11];
        for (i, v) in n.iter_mut().enumerate() {
            *v = r.get(i + 2)?;
        }
        Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, n))
    })?;
    let u = |v: i64| v.max(0) as u64;
    let mut out = vec![];
    for row in rows {
        let (b, inst, n) = row?;
        let samples = u(n[0]);
        let avg = |sum: i64| {
            if samples == 0 {
                0.0
            } else {
                (u(sum) as f64 / samples as f64 * 100.0).round() / 100.0
            }
        };
        out.push(crate::occupancy::OccupancyRow {
            minute_ms: b,
            instance: (inst != 0).then(|| labels.get(&inst).cloned()).flatten(),
            samples,
            inflight_avg: avg(n[1]),
            inflight_peak: u(n[2]),
            ws_avg: avg(n[3]),
            ws_peak: u(n[4]),
            buffered_avg_bytes: avg(n[5]).round(),
            buffered_peak_bytes: u(n[6]),
            max_inflight_requests: u(n[7]),
            ws_max_connections: u(n[8]),
            max_buffered_body_bytes: u(n[9]),
            full_minutes: u(n[10]),
        });
    }
    Ok(out)
}

/// Delete occupancy minutes before `cutoff_ms`, at most a day per call. Returns true when
/// more may remain.
pub(crate) fn purge_occupancy_before(c: &Connection, cutoff_ms: i64) -> rusqlite::Result<bool> {
    let oldest: Option<i64> = c
        .query_row(
            "SELECT min(minute) FROM occupancy WHERE minute < ?1",
            [cutoff_ms],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    let Some(oldest) = oldest else {
        return Ok(false);
    };
    let until = cutoff_ms.min(oldest.saturating_add(86_400_000));
    c.execute("DELETE FROM occupancy WHERE minute < ?1", [until])?;
    Ok(until < cutoff_ms)
}
