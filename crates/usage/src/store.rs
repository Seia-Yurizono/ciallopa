//! Schema, batch insert, purge and read queries.
use crate::{Record, Transport, UsageError, SEQ_BITS};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;

pub(crate) const SCHEMA: &str = r#"
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
  reasoning_tok INTEGER
);
PRAGMA user_version = 1;
"#;

fn pragmas(c: &Connection) -> rusqlite::Result<()> {
    c.busy_timeout(std::time::Duration::from_secs(5))?;
    c.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA temp_store=MEMORY;",
    )
}

pub(crate) fn open_rw(p: &Path) -> Result<Connection, UsageError> {
    let c = Connection::open(p)?;
    pragmas(&c)?;
    c.execute_batch(SCHEMA)?;
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

/// Writer-side cache of `labels` (name → id).
#[derive(Default)]
pub(crate) struct Labels(HashMap<String, i64>);
impl Labels {
    fn id(&mut self, c: &Connection, name: Option<&str>) -> rusqlite::Result<Option<i64>> {
        let Some(name) = name else { return Ok(None) };
        if let Some(id) = self.0.get(name) {
            return Ok(Some(*id));
        }
        c.execute("INSERT OR IGNORE INTO labels(name) VALUES (?1)", [name])?;
        let id: i64 = c.query_row("SELECT id FROM labels WHERE name = ?1", [name], |r| r.get(0))?;
        self.0.insert(name.to_string(), id);
        Ok(Some(id))
    }
}

pub(crate) fn insert_batch(c: &mut Connection, labels: &mut Labels, rows: &[Record]) -> rusqlite::Result<()> {
    let tx = c.transaction()?;
    {
        let mut st = tx.prepare_cached(
            "INSERT OR IGNORE INTO requests(id,account,first_account,model,effort,transport,endpoint,\
             status,error,retries,switches,ttft_ms,duration_ms,in_tok,out_tok,cached_tok,reasoning_tok) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
        )?;
        for r in rows {
            let account = labels.id(&tx, r.account.as_deref())?;
            let first = labels.id(&tx, r.first_account.as_deref())?;
            let model = labels.id(&tx, r.model.as_deref())?;
            let effort = labels.id(&tx, r.effort.as_deref())?;
            let endpoint = labels.id(&tx, Some(r.endpoint))?;
            let error = labels.id(&tx, r.error.as_deref())?;
            st.execute(params![
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
                r.reasoning_tokens
            ])?;
        }
    }
    tx.commit()
}

/// Delete at most `chunk` rows with start time < `cutoff_ms`. Returns rows deleted.
pub(crate) fn purge_before(c: &mut Connection, cutoff_ms: i64, chunk: u64) -> rusqlite::Result<u64> {
    let n = c.execute(
        "DELETE FROM requests WHERE id IN (SELECT id FROM requests WHERE id < ?1 ORDER BY id LIMIT ?2)",
        params![cutoff_ms << SEQ_BITS, chunk as i64],
    )?;
    Ok(n as u64)
}

fn label_map(c: &Connection) -> rusqlite::Result<HashMap<i64, String>> {
    let mut st = c.prepare_cached("SELECT id, name FROM labels")?;
    let rows = st.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
    rows.collect()
}

fn label_id(c: &Connection, name: &str) -> rusqlite::Result<Option<i64>> {
    c.query_row("SELECT id FROM labels WHERE name = ?1", [name], |r| r.get(0))
        .optional()
}

#[derive(Debug, Clone, Default)]
pub struct RecentQuery {
    pub limit: usize,
    /// Only rows started before this time (ms); pagination cursor.
    pub before_ms: Option<i64>,
    pub since_ms: Option<i64>,
    pub account: Option<String>,
    pub model: Option<String>,
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
}

fn tps(out: Option<i64>, ttft: Option<u32>, dur: u32) -> Option<f64> {
    let out = out.filter(|o| *o > 0)?;
    let gen = i64::from(dur) - i64::from(ttft.unwrap_or(0));
    (gen > 0).then(|| out as f64 * 1000.0 / gen as f64)
}

pub(crate) fn recent(c: &Connection, q: &RecentQuery) -> Result<Vec<RecordRow>, UsageError> {
    let names = label_map(c)?;
    let lookup = |n: &Option<String>| -> rusqlite::Result<Option<Option<i64>>> {
        match n {
            None => Ok(None),
            Some(n) => Ok(Some(label_id(c, n)?)),
        }
    };
    let (acct, model) = (lookup(&q.account)?, lookup(&q.model)?);
    // Unknown label ⇒ nothing can match.
    if matches!(acct, Some(None)) || matches!(model, Some(None)) {
        return Ok(vec![]);
    }
    let hi = q.before_ms.map(|b| (b << SEQ_BITS) - 1).unwrap_or(i64::MAX);
    let lo = q.since_ms.map(|s| s << SEQ_BITS).unwrap_or(0);
    let mut st = c.prepare_cached(
        "SELECT id,account,first_account,model,effort,transport,endpoint,status,error,retries,switches,\
         ttft_ms,duration_ms,in_tok,out_tok,cached_tok,reasoning_tok FROM requests \
         WHERE id BETWEEN ?1 AND ?2 AND (?3 IS NULL OR account = ?3) AND (?4 IS NULL OR model = ?4) \
         AND (?5 = 0 OR error IS NOT NULL OR status >= 400) ORDER BY id DESC LIMIT ?6",
    )?;
    let name = |id: Option<i64>| id.and_then(|i| names.get(&i).cloned());
    let rows = st.query_map(
        params![lo, hi, acct.flatten(), model.flatten(), q.errors_only as i64, q.limit.clamp(1, 1000) as i64],
        |r| {
            let id: i64 = r.get(0)?;
            let ttft: Option<u32> = r.get(11)?;
            let dur: u32 = r.get(12)?;
            let out: Option<i64> = r.get(14)?;
            Ok(RecordRow {
                request_id: format!("{id:016x}"),
                ts_ms: id >> SEQ_BITS,
                account: name(r.get(1)?),
                first_account: name(r.get(2)?),
                model: name(r.get(3)?),
                reasoning_effort: name(r.get(4)?),
                transport: Transport::from_i64(r.get(5)?),
                endpoint: name(r.get(6)?),
                status: r.get(7)?,
                error: name(r.get(8)?),
                retries: r.get(9)?,
                switches: r.get(10)?,
                ttft_ms: ttft,
                duration_ms: dur,
                input_tokens: r.get(13)?,
                output_tokens: out,
                cached_tokens: r.get(15)?,
                reasoning_tokens: r.get(16)?,
                tps: tps(out, ttft, dur),
            })
        },
    )?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

#[derive(Debug, Clone, Default)]
pub struct SummaryQuery {
    pub from_ms: i64,
    pub to_ms: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Default)]
pub struct SummaryGroup {
    pub account: Option<String>,
    pub model: Option<String>,
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
}

#[derive(Debug, Clone, Serialize, PartialEq, Default)]
pub struct Summary {
    pub from_ms: i64,
    pub to_ms: i64,
    pub total: SummaryGroup,
    pub groups: Vec<SummaryGroup>,
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
}
impl Acc {
    fn add(&mut self, err: bool, ttft: Option<u32>, dur: u32, tok: [Option<i64>; 4]) {
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
    }
    fn finish(mut self, account: Option<String>, model: Option<String>) -> SummaryGroup {
        SummaryGroup {
            account,
            model,
            count: self.count,
            errors: self.errors,
            error_rate: if self.count == 0 { 0.0 } else { self.errors as f64 / self.count as f64 },
            ttft_p50_ms: pct(&mut self.ttft, 0.5),
            ttft_p90_ms: pct(&mut self.ttft, 0.9),
            duration_p50_ms: pct(&mut self.dur, 0.5),
            duration_p90_ms: pct(&mut self.dur, 0.9),
            input_tokens: self.tok[0],
            output_tokens: self.tok[1],
            cached_tokens: self.tok[2],
            reasoning_tokens: self.tok[3],
            tps: (self.gen_ms > 0).then(|| self.gen_out as f64 * 1000.0 / self.gen_ms as f64),
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

pub(crate) fn summary(c: &Connection, q: &SummaryQuery) -> Result<Summary, UsageError> {
    let names = label_map(c)?;
    let mut st = c.prepare_cached(
        "SELECT account,model,status,error,ttft_ms,duration_ms,in_tok,out_tok,cached_tok,reasoning_tok \
         FROM requests WHERE id BETWEEN ?1 AND ?2",
    )?;
    let mut groups: HashMap<(Option<i64>, Option<i64>), Acc> = HashMap::new();
    let mut rows = st.query(params![q.from_ms << SEQ_BITS, ((q.to_ms + 1) << SEQ_BITS) - 1])?;
    while let Some(r) = rows.next()? {
        let key = (r.get(0)?, r.get(1)?);
        let status: u16 = r.get(2)?;
        let err: Option<i64> = r.get(3)?;
        groups.entry(key).or_default().add(
            err.is_some() || status >= 400,
            r.get(4)?,
            r.get(5)?,
            [r.get(6)?, r.get(7)?, r.get(8)?, r.get(9)?],
        );
    }
    let name = |id: Option<i64>| id.and_then(|i| names.get(&i).cloned());
    let mut total = Acc::default();
    let mut out: Vec<SummaryGroup> = groups
        .into_iter()
        .map(|((a, m), acc)| {
            total.merge(&acc);
            acc.finish(name(a), name(m))
        })
        .collect();
    out.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.account.cmp(&b.account)));
    Ok(Summary {
        from_ms: q.from_ms,
        to_ms: q.to_ms,
        total: total.finish(None, None),
        groups: out,
    })
}
