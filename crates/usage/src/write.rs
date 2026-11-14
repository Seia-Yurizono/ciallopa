use crate::{QuotaObs, Record, UsageError, SEQ_BITS};
use rusqlite::{params, Connection, OptionalExtension};
use std::{collections::HashMap, path::Path};

/// Writer-side caches: `labels` (name → id) and the last quota snapshot per account.
pub(crate) struct WriterState {
    labels: HashMap<String, i64>,
    last_quota: HashMap<i64, QuotaObs>,
    pub(crate) instance: String,
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
    pub(crate) fn label(
        &mut self,
        c: &Connection,
        name: Option<&str>,
    ) -> rusqlite::Result<Option<i64>> {
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
             q.s_pct, q.s_reset, q.s_win, q.cards FROM quota_obs q \
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
                    cards: r.get(8)?,
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
    prices: &crate::prices::PriceTable,
) -> rusqlite::Result<()> {
    let tx = c.transaction()?;
    ws.load_quota(&tx)?;
    let mut pending: Vec<(i64, String, i64, Option<QuotaObs>, QuotaObs)> = Vec::new();
    {
        let mut st = tx.prepare_cached(
            "INSERT OR IGNORE INTO requests(id,account,first_account,model,effort,transport,endpoint,\
             status,error,retries,switches,ttft_ms,duration_ms,in_tok,out_tok,cached_tok,reasoning_tok,\
             key,instance,plan,p_pct,p_reset,s_pct,s_reset,tier,cw_tok,thread,turn,req_tier,resp_model,resp_effort,incomplete,err_source,up_status) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31,?32,?33,?34)",
        )?;
        let mut qst = tx.prepare_cached(
            "INSERT OR IGNORE INTO quota_obs(id,account,instance,plan,p_pct,p_reset,p_win,s_pct,s_reset,s_win,cards) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
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
            if let (1, Some(o)) = (n, r.obs.as_deref()) {
                if !o.is_empty() {
                    crate::observation::insert(&tx, r.id, o)?;
                }
            }
            if let (1, Some(q), Some(a)) = (n, q, account) {
                if quota_changed(ws.last_quota.get(&a), q) {
                    let old = ws.last_quota.get(&a).cloned();
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
                        q.s_window_min,
                        q.cards
                    ])?;
                    if let Some(name) = r.account.as_deref() {
                        pending.push((a, name.to_string(), r.id, old, q.clone()));
                    }
                    ws.last_quota.insert(a, q.clone());
                }
            }
        }
    }
    for (a, name, id, old, new) in &pending {
        crate::history::on_new_obs(&tx, prices, *a, name, *id, old.as_ref(), new)?;
    }
    tx.commit()
}

/// Quota snapshots read on request: always stored (their time is the freshness shown).
pub(crate) fn insert_quota(
    c: &mut Connection,
    ws: &mut WriterState,
    rows: &[crate::QuotaSnapshot],
    prices: &crate::prices::PriceTable,
) -> rusqlite::Result<()> {
    let tx = c.transaction()?;
    ws.load_quota(&tx)?;
    let mut pending: Vec<(i64, String, i64, Option<QuotaObs>, QuotaObs)> = Vec::new();
    {
        let mut qst = tx.prepare_cached(
            "INSERT OR IGNORE INTO quota_obs(id,account,instance,plan,p_pct,p_reset,p_win,s_pct,s_reset,s_win,cards) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        )?;
        for crate::QuotaSnapshot {
            id,
            account,
            observer: by,
            obs: q,
        } in rows
        {
            let Some(a) = ws.label(&tx, Some(account))? else {
                continue;
            };
            let by = ws.label(&tx, Some(by))?;
            let plan = ws.label(&tx, q.plan.as_deref())?;
            let old = ws.last_quota.get(&a).cloned();
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
                q.s_window_min,
                q.cards
            ])?;
            pending.push((a, account.clone(), *id, old, q.clone()));
            ws.last_quota.insert(a, q.clone());
        }
    }
    for (a, name, id, old, new) in &pending {
        crate::history::on_new_obs(&tx, prices, *a, name, *id, old.as_ref(), new)?;
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
    c.execute(
        "DELETE FROM request_obs WHERE id IN (SELECT id FROM request_obs WHERE id < ?1 ORDER BY id LIMIT ?2)",
        params![cut, chunk as i64],
    )?;
    if (n as u64) < chunk {
        // quota_obs is tiny (changes only); keep each account's latest snapshot.
        c.execute(
            "DELETE FROM quota_obs WHERE id < ?1 AND id < (SELECT max(id) FROM quota_obs q WHERE q.account = quota_obs.account)",
            [cut],
        )?;
    }
    crate::history::purge(c, cutoff_ms)?;
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

/// Maintenance: delete rows started before `cutoff_ms`, `chunk` rows per transaction.
pub fn purge_db(path: &Path, cutoff_ms: i64, chunk: u64) -> Result<u64, UsageError> {
    let mut c = crate::schema::open_rw(path)?;
    let mut n = 0;
    loop {
        let k = crate::write::purge_before(&mut c, cutoff_ms, chunk)?;
        n += k;
        if k < chunk {
            while crate::write::purge_refusals_before(&c, cutoff_ms)? {}
            while crate::occupancy::purge_occupancy_before(&c, cutoff_ms)? {}
            return Ok(n);
        }
    }
}

/// `PRAGMA integrity_check` result ("ok" when healthy) and row count.
pub fn integrity_check(path: &Path) -> Result<(String, i64), UsageError> {
    let c = crate::schema::open_read(path)?;
    let s: String = c.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    let n: i64 = c.query_row("SELECT count(*) FROM requests", [], |r| r.get(0))?;
    Ok((s, n))
}
