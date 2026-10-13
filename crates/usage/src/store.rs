//! Schema (+ migrations), batch insert, purge and read queries.
use crate::{QuotaObs, Record, Transport, UsageError, SEQ_BITS};
use ciallopa_core::config::QuotaWeights;
use rusqlite::{params, params_from_iter, Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;

pub(crate) const SCHEMA_VERSION: i64 = 5;

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
  resp_effort   INTEGER                -- labels.id, response.reasoning.effort
);
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
  headers   TEXT NOT NULL            -- JSON object
);
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
    tx.execute_batch(CREATE_V3)?;
    tx.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION};"))?;
    tx.commit()
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

pub(crate) fn open_ro(p: &Path) -> Result<Connection, UsageError> {
    let c = Connection::open_with_flags(
        p,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    c.busy_timeout(std::time::Duration::from_secs(5))?;
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
             key,instance,plan,p_pct,p_reset,s_pct,s_reset,tier,cw_tok,thread,turn,req_tier,resp_model,resp_effort) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31)",
        )?;
        let mut qst = tx.prepare_cached(
            "INSERT OR IGNORE INTO quota_obs(id,account,instance,plan,p_pct,p_reset,p_win,s_pct,s_reset,s_win) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        )?;
        let mut dst = tx.prepare_cached(
            "INSERT OR IGNORE INTO error_detail(id,message,body,body_len,truncated,headers) VALUES (?1,?2,?3,?4,?5,?6)",
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
            ])?;
            if let (1, Some(d)) = (n, r.detail.as_deref()) {
                if r.status >= 400 || r.error.is_some() {
                    let s = d.finish();
                    dst.execute(params![
                        r.id,
                        s.message,
                        s.body,
                        s.body_len,
                        s.truncated,
                        s.headers
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

fn label_map(c: &Connection) -> rusqlite::Result<HashMap<i64, String>> {
    let mut st = c.prepare_cached("SELECT id, name FROM labels")?;
    let rows = st.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
    rows.collect()
}

fn label_id(c: &Connection, name: &str) -> rusqlite::Result<Option<i64>> {
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
    pub since_ms: Option<i64>,
    pub filters: Filters,
    pub errors_only: bool,
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
    /// null: no response model; false: same (or a dated snapshot of it); true: different.
    pub model_mismatch: Option<bool>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    /// Notional USD from tokens × price table at query time; null when unpriced.
    pub cost_usd: Option<f64>,
    /// An `error_detail` row exists (`GET /usage/v1/records/{id}/detail`).
    pub has_detail: bool,
    /// `ok`, `unknown_price` (model/tier/date not in the price table) or `no_tokens`.
    pub price_status: &'static str,
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
    let names = label_map(c)?;
    let Some((fsql, fparams)) = q.filters.sql(c)? else {
        return Ok(vec![]);
    };
    let hi = q.before_ms.map(|b| (b << SEQ_BITS) - 1).unwrap_or(i64::MAX);
    let lo = q.since_ms.map(|s| s << SEQ_BITS).unwrap_or(0);
    let sql = format!(
        "SELECT id,account,first_account,model,effort,transport,endpoint,status,error,retries,switches,\
         ttft_ms,duration_ms,in_tok,out_tok,cached_tok,reasoning_tok,key,instance,plan,p_pct,p_reset,s_pct,s_reset,\
         tier,cw_tok,thread,turn,EXISTS(SELECT 1 FROM error_detail d WHERE d.id = requests.id),\
         req_tier,resp_model,resp_effort \
         FROM requests WHERE id BETWEEN ? AND ?{fsql}{} ORDER BY id DESC LIMIT ?",
        if q.errors_only { " AND (error IS NOT NULL OR status >= 400)" } else { "" }
    );
    let mut ps: Vec<i64> = vec![lo, hi];
    ps.extend(fparams);
    ps.push(q.limit.clamp(1, 1000) as i64);
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
        Ok(RecordRow {
            requested_service_tier: name(r.get(29)?),
            response_model: resp_model,
            response_effort: name(r.get(31)?),
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
            endpoint: name(r.get(6)?),
            status: r.get(7)?,
            error: name(r.get(8)?),
            retries: r.get(9)?,
            switches: r.get(10)?,
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

#[derive(Debug, Clone)]
pub struct SummaryQuery {
    pub from_ms: i64,
    pub to_ms: i64,
    /// Default: account, model.
    pub group_by: Vec<Dim>,
    pub filters: Filters,
    /// `Some(tz_offset_minutes)` ⇒ also return per-day buckets (local day = UTC + offset).
    pub bucket_day: Option<i32>,
}
impl Default for SummaryQuery {
    fn default() -> Self {
        Self {
            from_ms: 0,
            to_ms: i64::MAX >> SEQ_BITS,
            group_by: vec![Dim::Account, Dim::Model],
            filters: Filters::default(),
            bucket_day: None,
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
    pub ttft_p50_ms: Option<u32>,
    pub ttft_p90_ms: Option<u32>,
    pub duration_p50_ms: Option<u32>,
    pub duration_p90_ms: Option<u32>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cached_tokens: i64,
    pub reasoning_tokens: i64,
    /// Σ output_tokens / Σ generation seconds over rows that have both.
    pub tps: Option<f64>,
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
    pub count: u64,
    pub errors: u64,
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
    ttft: Vec<u32>,
    dur: Vec<u32>,
    tok: [i64; 4],
    gen_ms: i64,
    gen_out: i64,
    cost: Option<f64>,
    unknown: u64,
}
impl Acc {
    fn add(
        &mut self,
        err: bool,
        ttft: Option<u32>,
        dur: u32,
        tok: [Option<i64>; 4],
        cost: (Option<f64>, &'static str),
    ) {
        self.count += 1;
        self.errors += u64::from(err);
        if let Some(t) = ttft {
            self.ttft.push(t);
        }
        self.dur.push(dur);
        for (a, t) in self.tok.iter_mut().zip(tok) {
            *a += t.unwrap_or(0);
        }
        if let Some(o) = tok[1].filter(|o| *o > 0) {
            let g = i64::from(dur) - i64::from(ttft.unwrap_or(0));
            if g > 0 {
                self.gen_ms += g;
                self.gen_out += o;
            }
        }
        match cost {
            (Some(c), _) => *self.cost.get_or_insert(0.0) += c,
            (None, "unknown_price") => self.unknown += 1,
            _ => {}
        }
    }
    fn merge(&mut self, o: &Acc) {
        self.count += o.count;
        self.errors += o.errors;
        self.ttft.extend_from_slice(&o.ttft);
        self.dur.extend_from_slice(&o.dur);
        for i in 0..4 {
            self.tok[i] += o.tok[i];
        }
        self.gen_ms += o.gen_ms;
        self.gen_out += o.gen_out;
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
            error_rate: if self.count == 0 {
                0.0
            } else {
                self.errors as f64 / self.count as f64
            },
            ttft_p50_ms: pct(&mut self.ttft, 0.5),
            ttft_p90_ms: pct(&mut self.ttft, 0.9),
            duration_p50_ms: pct(&mut self.dur, 0.5),
            duration_p90_ms: pct(&mut self.dur, 0.9),
            input_tokens: self.tok[0],
            output_tokens: self.tok[1],
            cached_tokens: self.tok[2],
            reasoning_tokens: self.tok[3],
            tps: (self.gen_ms > 0).then(|| self.gen_out as f64 * 1000.0 / self.gen_ms as f64),
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
    let dims = if q.group_by.is_empty() {
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
    let Some((fsql, fparams)) = q.filters.sql(c)? else {
        out.total = Acc::default().finish(Default::default());
        return Ok(out);
    };
    let dim_cols: Vec<&str> = dims.iter().map(|d| d.col()).collect();
    let sql = format!(
        "SELECT status,error,ttft_ms,duration_ms,in_tok,out_tok,cached_tok,reasoning_tok,model,id,tier,cw_tok,{} \
         FROM requests WHERE id BETWEEN ? AND ?{fsql}",
        dim_cols.join(",")
    );
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
    while let Some(r) = rows.next()? {
        let key: Vec<Option<i64>> = (0..dims.len())
            .map(|i| r.get(12 + i))
            .collect::<rusqlite::Result<_>>()?;
        let status: u16 = r.get(0)?;
        let err: Option<i64> = r.get(1)?;
        let tok: [Option<i64>; 4] = [r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?];
        let m: Option<i64> = r.get(8)?;
        let t: Option<i64> = r.get(10)?;
        let lbl = |x: Option<i64>| x.and_then(|x| names.get(&x)).map(String::as_str);
        let cost = row_cost(prices, lbl(m), lbl(t), r.get(9)?, tok, r.get(11)?);
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
        if let Some(tz) = q.bucket_day {
            let off = i64::from(tz) * 60_000;
            let id: i64 = r.get(9)?;
            let day = ((id >> SEQ_BITS) + off).div_euclid(86_400_000);
            let b = days.entry(day).or_insert_with(|| DayBucket {
                day: civil(day),
                start_ms: day * 86_400_000 - off,
                ..Default::default()
            });
            b.count += 1;
            b.errors += u64::from(err.is_some() || status >= 400);
            b.input_tokens += tok[0].unwrap_or(0);
            b.output_tokens += tok[1].unwrap_or(0);
            b.cached_tokens += tok[2].unwrap_or(0);
            b.reasoning_tokens += tok[3].unwrap_or(0);
            match cost {
                (Some(c), _) => *b.cost_usd.get_or_insert(0.0) += c,
                (None, "unknown_price") => b.unknown_price += 1,
                _ => {}
            }
        }
        groups.entry(key).or_default().add(
            err.is_some() || status >= 400,
            r.get(2)?,
            r.get(3)?,
            tok,
            cost,
        );
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
    out.groups = gs;
    out.unknown_price_models = unknown_models.into_iter().collect();
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
            "SELECT message, body, body_len, truncated, headers FROM error_detail WHERE id = ?1",
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
                })
            },
        )
        .optional()?;
    Ok(row)
}
