//! Reset-event and per-cycle capacity persistence (schema v15).
//!
//! A cycle-closing classification writes the event and the estimate in the same transaction
//! as the triggering `quota_obs` row. In-progress cycles are computed at read time.
use crate::classify::{self, CardSource, Class, Side, Transition, WindowKind};
use crate::credits;
use crate::estimate::{self, Capacity, Confidence};
use crate::prices::{PriceTable, Tier, Usage};
use crate::{QuotaObs, UsageError, SEQ_BITS};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::Serialize;

const CARD_LOOKBACK_MS: i64 = 5 * 60_000;

pub(crate) const CREATE_V15: &str = r#"
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
"#;

pub(crate) fn migrate_v15(tx: &Transaction, from: i64) -> rusqlite::Result<()> {
    if from >= 15 {
        return Ok(());
    }
    let has_obs: bool = tx
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='quota_obs'",
            [],
            |_| Ok(true),
        )
        .optional()?
        .unwrap_or(false);
    if has_obs {
        let has_cards: bool = tx
            .query_row(
                "SELECT 1 FROM pragma_table_info('quota_obs') WHERE name='cards'",
                [],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);
        if !has_cards {
            tx.execute_batch("ALTER TABLE quota_obs ADD COLUMN cards INTEGER;")?;
        }
    }
    tx.execute_batch(CREATE_V15)?;
    Ok(())
}

pub(crate) fn purge(c: &Connection, cutoff_ms: i64) -> rusqlite::Result<()> {
    c.execute(
        "DELETE FROM quota_reset_events WHERE at_ms < ?1",
        [cutoff_ms],
    )?;
    c.execute(
        "DELETE FROM quota_cycle_estimates WHERE ended_ms < ?1",
        [cutoff_ms],
    )?;
    Ok(())
}

fn win_cols(which: usize) -> (&'static str, &'static str) {
    if which == 0 {
        ("p_pct", "p_reset")
    } else {
        ("s_pct", "s_reset")
    }
}

fn side_of(q: Option<&QuotaObs>, which: usize) -> Side {
    let Some(q) = q else {
        return Side::default();
    };
    if which == 0 {
        Side {
            pct: q.p_pct,
            reset: q.p_reset,
            window_min: q.p_window_min,
            cards: q.cards,
        }
    } else {
        Side {
            pct: q.s_pct,
            reset: q.s_reset,
            window_min: q.s_window_min,
            cards: q.cards,
        }
    }
}

fn recent_card(
    tx: &Transaction,
    account: &str,
    now_ms: i64,
) -> rusqlite::Result<Option<CardSource>> {
    let row: Option<Option<String>> = tx
        .query_row(
            "SELECT key FROM reset_consume WHERE account=?1 AND status='done' \
             AND lower(coalesce(code,''))='reset' AND finished_ms IS NOT NULL \
             AND finished_ms>=?2 AND finished_ms<=?3 ORDER BY id DESC LIMIT 1",
            params![account, now_ms - CARD_LOOKBACK_MS, now_ms + 30_000],
            |r| r.get(0),
        )
        .optional()?;
    Ok(row.map(|k| CardSource::from_key(k.as_deref())))
}

/// After a new `quota_obs` row: classify each window and persist a close when needed.
pub(crate) fn on_new_obs(
    tx: &Transaction,
    prices: &PriceTable,
    account_id: i64,
    account_name: &str,
    obs_id: i64,
    old: Option<&QuotaObs>,
    new: &QuotaObs,
) -> rusqlite::Result<()> {
    let now_ms = obs_id >> SEQ_BITS;
    let now_s = now_ms.div_euclid(1000);
    let local = recent_card(tx, account_name, now_ms)?;
    for which in 0..2 {
        let prev = side_of(old, which);
        let next = side_of(Some(new), which);
        let class = classify::classify(&Transition {
            prev,
            next,
            now_s,
            local_card: local,
        });
        if class.closes_cycle() {
            close_one(
                tx, prices, account_id, obs_id, now_ms, which, &prev, &next, class,
            )?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn close_one(
    tx: &Transaction,
    prices: &PriceTable,
    account_id: i64,
    obs_id: i64,
    now_ms: i64,
    which: usize,
    prev: &Side,
    next: &Side,
    class: Class,
) -> rusqlite::Result<()> {
    let window = WindowKind::from_index(which).as_str();
    let src = class.card_source().map(CardSource::as_str);
    let n = tx.execute(
        "INSERT OR IGNORE INTO quota_reset_events(\
         account,window,kind,at_ms,obs_id,prev_pct,next_pct,prev_reset,next_reset,\
         prev_cards,next_cards,card_source) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
        params![
            account_id,
            window,
            class.as_str(),
            now_ms,
            obs_id,
            prev.pct,
            next.pct,
            prev.reset,
            next.reset,
            prev.cards,
            next.cards,
            src,
        ],
    )?;
    if n == 0 {
        return Ok(());
    }
    let event_id = tx.last_insert_rowid();
    let data = load_cycle(tx, prices, account_id, which, prev.reset, obs_id)?;
    let cap = if data.obs.len() >= 2 {
        estimate::estimate(&estimate::points_from(&data.obs, &data.req_credits), 5.0)
    } else {
        Capacity {
            capacity: None,
            lo: None,
            hi: None,
            delta_pct: data
                .obs
                .first()
                .and_then(|a| data.obs.last().map(|b| b.1 - a.1)),
            credits_used: data.credits_used,
            coverage_gap: false,
            confidence: Confidence::Insufficient,
        }
    };
    let started_ms = data
        .obs
        .first()
        .map(|(id, _)| *id >> SEQ_BITS)
        .unwrap_or(now_ms);
    tx.execute(
        "INSERT OR IGNORE INTO quota_cycle_estimates(\
         account,window,started_ms,ended_ms,ended_obs_id,cycle_reset_at,close_kind,\
         capacity_credits,capacity_lo,capacity_hi,delta_pct,credits_used,api_cost_usd,\
         requests,snapshots,coverage_gap,confidence,card_source) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",
        params![
            account_id,
            window,
            started_ms,
            now_ms,
            obs_id,
            prev.reset,
            class.as_str(),
            cap.capacity,
            cap.lo,
            cap.hi,
            cap.delta_pct,
            data.credits_used,
            data.api_cost,
            data.requests,
            data.obs.len() as i64,
            cap.coverage_gap as i64,
            cap.confidence.as_str(),
            src,
        ],
    )?;
    let cycle_id = tx.last_insert_rowid();
    tx.execute(
        "UPDATE quota_reset_events SET cycle_id=?2 WHERE id=?1",
        params![event_id, cycle_id],
    )?;
    Ok(())
}

struct CycleData {
    obs: Vec<(i64, f64)>,
    req_credits: Vec<(i64, f64)>,
    credits_used: f64,
    api_cost: Option<f64>,
    requests: i64,
}

fn load_cycle(
    c: &Connection,
    prices: &PriceTable,
    account_id: i64,
    which: usize,
    reset: Option<i64>,
    end_exclusive: i64,
) -> rusqlite::Result<CycleData> {
    let (pc, rc) = win_cols(which);
    let (lo, hi) = match reset {
        Some(r) => (
            r - classify::RESET_TOLERANCE_S,
            r + classify::RESET_TOLERANCE_S,
        ),
        None => (i64::MIN / 4, i64::MAX / 4),
    };
    let sql = format!(
        "SELECT id, {pc} FROM quota_obs WHERE account=?1 AND {pc} IS NOT NULL \
         AND id < ?2 AND ({rc} BETWEEN ?3 AND ?4 OR (?5 AND {rc} IS NULL)) ORDER BY id"
    );
    let mut st = c.prepare(&sql)?;
    let obs: Vec<(i64, f64)> = st
        .query_map(
            params![account_id, end_exclusive, lo, hi, reset.is_none()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?
        .collect::<rusqlite::Result<_>>()?;
    let first = obs.first().map(|(id, _)| *id).unwrap_or(end_exclusive);
    let mut rst = c.prepare(
        "SELECT r.id, coalesce(r.in_tok,0), coalesce(r.cached_tok,0), coalesce(r.cw_tok,0), \
         coalesce(r.out_tok,0), t.name, coalesce(rm.name, m.name) \
         FROM requests r \
         LEFT JOIN labels t ON t.id = r.tier \
         LEFT JOIN labels m ON m.id = r.model \
         LEFT JOIN labels rm ON rm.id = r.resp_model \
         WHERE r.account=?1 AND r.id>=?2 AND r.id<?3 ORDER BY r.id",
    )?;
    let rows = rst.query_map(params![account_id, first, end_exclusive], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, i64>(2)?,
            r.get::<_, i64>(3)?,
            r.get::<_, i64>(4)?,
            r.get::<_, Option<String>>(5)?,
            r.get::<_, Option<String>>(6)?,
        ))
    })?;
    let mut req_credits = Vec::new();
    let mut credits_used = 0.0;
    let mut api_cost = 0.0;
    let mut any_cost = false;
    let mut requests = 0i64;
    for row in rows {
        let (id, inp, cached, cw, out, tier, model) = row?;
        requests += 1;
        let u = Usage {
            input: inp,
            cached,
            cache_write: cw,
            output: out,
        };
        let tier = Tier::from_service_tier(tier.as_deref());
        let ts = id >> SEQ_BITS;
        if let Some(c) = credits::credits(model.as_deref(), tier, u) {
            req_credits.push((id, c));
            credits_used += c;
        }
        if let Some(usd) = prices.cost(model.as_deref(), tier, ts, u) {
            api_cost += usd;
            any_cost = true;
        }
    }
    Ok(CycleData {
        obs,
        req_credits,
        credits_used,
        api_cost: any_cost.then_some(api_cost),
        requests,
    })
}

/// Credit-capacity of the still-open window (read path; not persisted).
pub(crate) fn live_capacity(
    c: &Connection,
    prices: &PriceTable,
    account_id: i64,
    which: usize,
    reset: Option<i64>,
    end_inclusive: i64,
) -> Result<(Capacity, Option<f64>, i64), UsageError> {
    let data = load_cycle(
        c,
        prices,
        account_id,
        which,
        reset,
        end_inclusive.saturating_add(1),
    )?;
    let cap = if data.obs.len() >= 2 {
        let mut cap = estimate::estimate(&estimate::points_from(&data.obs, &data.req_credits), 5.0);
        cap.credits_used = data.credits_used;
        cap
    } else {
        Capacity {
            capacity: None,
            lo: None,
            hi: None,
            delta_pct: None,
            credits_used: data.credits_used,
            coverage_gap: false,
            confidence: Confidence::Insufficient,
        }
    };
    Ok((cap, data.api_cost, data.requests))
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ResetEvent {
    pub id: i64,
    pub window: String,
    pub kind: String,
    pub at_ms: i64,
    pub prev_pct: Option<f64>,
    pub next_pct: Option<f64>,
    pub prev_reset: Option<i64>,
    pub next_reset: Option<i64>,
    pub card_source: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CycleRow {
    pub id: Option<i64>,
    pub window: String,
    pub started_ms: i64,
    pub ended_ms: i64,
    pub cycle_reset_at: Option<i64>,
    pub close_kind: String,
    pub capacity_credits: Option<f64>,
    pub capacity_lo: Option<f64>,
    pub capacity_hi: Option<f64>,
    pub delta_pct: Option<f64>,
    pub credits_used: Option<f64>,
    pub api_cost_usd: Option<f64>,
    pub requests: Option<i64>,
    pub snapshots: Option<i64>,
    pub coverage_gap: bool,
    pub confidence: String,
    pub card_source: Option<String>,
    /// `ok` / `up` / `down` vs the median of earlier closed cycles; null if not enough history.
    pub trend: Option<&'static str>,
    pub open: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AccountHistory {
    pub account: String,
    pub from_ms: i64,
    pub to_ms: i64,
    pub events: Vec<ResetEvent>,
    pub cycles: Vec<CycleRow>,
}

/// Persisted events + cycles in `[from_ms, to_ms]`, plus the in-progress cycle when present.
pub(crate) fn history(
    c: &Connection,
    prices: &PriceTable,
    account: Option<&str>,
    from_ms: i64,
    to_ms: i64,
) -> Result<Vec<AccountHistory>, UsageError> {
    let names = crate::store::label_map(c)?;
    let acct_id = match account {
        Some(n) => match names.iter().find(|(_, v)| v.as_str() == n).map(|(k, _)| *k) {
            Some(id) => Some(id),
            None => {
                return Ok(vec![AccountHistory {
                    account: n.to_string(),
                    from_ms,
                    to_ms,
                    events: vec![],
                    cycles: vec![],
                }]);
            }
        },
        None => None,
    };
    let mut ev_st = c.prepare(
        "SELECT id,account,window,kind,at_ms,prev_pct,next_pct,prev_reset,next_reset,card_source \
         FROM quota_reset_events WHERE at_ms>=?1 AND at_ms<=?2 AND (?3 IS NULL OR account=?3) \
         AND kind != 'idle_slide' ORDER BY at_ms, id",
    )?;
    let ev_rows: Vec<(i64, ResetEvent)> = ev_st
        .query_map(params![from_ms, to_ms, acct_id], |r| {
            Ok((
                r.get::<_, i64>(1)?,
                ResetEvent {
                    id: r.get(0)?,
                    window: r.get(2)?,
                    kind: r.get(3)?,
                    at_ms: r.get(4)?,
                    prev_pct: r.get(5)?,
                    next_pct: r.get(6)?,
                    prev_reset: r.get(7)?,
                    next_reset: r.get(8)?,
                    card_source: r.get(9)?,
                },
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;
    let mut cy_st = c.prepare(
        "SELECT id,account,window,started_ms,ended_ms,cycle_reset_at,close_kind,\
         capacity_credits,capacity_lo,capacity_hi,delta_pct,credits_used,api_cost_usd,\
         requests,snapshots,coverage_gap,confidence,card_source \
         FROM quota_cycle_estimates WHERE ended_ms>=?1 AND ended_ms<=?2 \
         AND (?3 IS NULL OR account=?3) ORDER BY ended_ms, id",
    )?;
    let cy_rows: Vec<(i64, CycleRow)> = cy_st
        .query_map(params![from_ms, to_ms, acct_id], |r| {
            Ok((
                r.get::<_, i64>(1)?,
                CycleRow {
                    id: Some(r.get(0)?),
                    window: r.get(2)?,
                    started_ms: r.get(3)?,
                    ended_ms: r.get(4)?,
                    cycle_reset_at: r.get(5)?,
                    close_kind: r.get(6)?,
                    capacity_credits: r.get(7)?,
                    capacity_lo: r.get(8)?,
                    capacity_hi: r.get(9)?,
                    delta_pct: r.get(10)?,
                    credits_used: r.get(11)?,
                    api_cost_usd: r.get(12)?,
                    requests: r.get(13)?,
                    snapshots: r.get(14)?,
                    coverage_gap: r.get::<_, i64>(15)? != 0,
                    confidence: r.get(16)?,
                    card_source: r.get(17)?,
                    trend: None,
                    open: false,
                },
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;

    let mut by: std::collections::BTreeMap<i64, AccountHistory> = std::collections::BTreeMap::new();
    let blank = |id: i64| AccountHistory {
        account: names.get(&id).cloned().unwrap_or_default(),
        from_ms,
        to_ms,
        events: vec![],
        cycles: vec![],
    };
    for (id, e) in ev_rows {
        by.entry(id).or_insert_with(|| blank(id)).events.push(e);
    }
    for (id, e) in cy_rows {
        by.entry(id).or_insert_with(|| blank(id)).cycles.push(e);
    }
    // In-progress cycle for each (requested) account that has a latest snapshot.
    let targets: Vec<i64> = match acct_id {
        Some(id) => vec![id],
        None => {
            let mut st = c.prepare("SELECT DISTINCT account FROM quota_obs")?;
            let ids = st
                .query_map([], |r| r.get(0))?
                .collect::<rusqlite::Result<Vec<i64>>>()?;
            ids
        }
    };
    for id in targets {
        for which in 0..2 {
            if let Some(open) = open_cycle(c, prices, id, which)? {
                if open.ended_ms >= from_ms && open.started_ms <= to_ms {
                    by.entry(id).or_insert_with(|| blank(id)).cycles.push(open);
                }
            }
        }
    }
    for h in by.values_mut() {
        apply_trend(&mut h.cycles);
    }
    Ok(by.into_values().collect())
}

fn open_cycle(
    c: &Connection,
    prices: &PriceTable,
    account_id: i64,
    which: usize,
) -> Result<Option<CycleRow>, UsageError> {
    let (pc, rc) = win_cols(which);
    let latest: Option<(i64, f64, Option<i64>)> = c
        .query_row(
            &format!(
                "SELECT id, {pc}, {rc} FROM quota_obs WHERE account=?1 AND {pc} IS NOT NULL \
                 ORDER BY id DESC LIMIT 1"
            ),
            [account_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let Some((last_id, _, reset)) = latest else {
        return Ok(None);
    };
    // Already closed at this snapshot.
    let window = WindowKind::from_index(which).as_str();
    let closed: bool = c
        .query_row(
            "SELECT 1 FROM quota_cycle_estimates WHERE account=?1 AND window=?2 AND ended_obs_id=?3",
            params![account_id, window, last_id],
            |_| Ok(true),
        )
        .optional()?
        .unwrap_or(false);
    if closed {
        return Ok(None);
    }
    let (cap, api_cost, requests) = live_capacity(c, prices, account_id, which, reset, last_id)?;
    let data_start: Option<i64> = c
        .query_row(
            &format!(
                "SELECT min(id) FROM quota_obs WHERE account=?1 AND {pc} IS NOT NULL \
                 AND ({rc} BETWEEN ?2 AND ?3 OR (?4 AND {rc} IS NULL))"
            ),
            params![
                account_id,
                reset
                    .map(|r| r - classify::RESET_TOLERANCE_S)
                    .unwrap_or(i64::MIN / 4),
                reset
                    .map(|r| r + classify::RESET_TOLERANCE_S)
                    .unwrap_or(i64::MAX / 4),
                reset.is_none()
            ],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    let Some(start_id) = data_start else {
        return Ok(None);
    };
    if start_id == last_id && cap.credits_used <= 0.0 {
        return Ok(None);
    }
    Ok(Some(CycleRow {
        id: None,
        window: window.to_string(),
        started_ms: start_id >> SEQ_BITS,
        ended_ms: last_id >> SEQ_BITS,
        cycle_reset_at: reset,
        close_kind: "open".into(),
        capacity_credits: cap.capacity,
        capacity_lo: cap.lo,
        capacity_hi: cap.hi,
        delta_pct: cap.delta_pct,
        credits_used: Some(cap.credits_used),
        api_cost_usd: api_cost,
        requests: Some(requests),
        snapshots: None,
        coverage_gap: cap.coverage_gap,
        confidence: cap.confidence.as_str().into(),
        card_source: None,
        trend: None,
        open: true,
    }))
}

fn apply_trend(cycles: &mut [CycleRow]) {
    // Per window, in time order: compare each point estimate to the median of earlier closed ones.
    for window in ["primary", "secondary"] {
        let mut hist: Vec<f64> = Vec::new();
        let idxs: Vec<usize> = cycles
            .iter()
            .enumerate()
            .filter(|(_, c)| c.window == window)
            .map(|(i, _)| i)
            .collect();
        for i in idxs {
            if let Some(cap) = cycles[i].capacity_credits {
                if !hist.is_empty() {
                    cycles[i].trend =
                        estimate::median(&hist).and_then(|m| estimate::trend_flag(cap, m));
                }
                if !cycles[i].open {
                    hist.push(cap);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store;
    use crate::{Record, Recorder, Transport};
    use ciallopa_core::config::UsageConfig;

    fn cfg(dir: &std::path::Path) -> UsageConfig {
        UsageConfig {
            path: dir.join("u.db"),
            flush_ms: 10,
            ..Default::default()
        }
    }

    fn snap(r: &Recorder, ms: i64, acct: &str, model: &str, out: i64, p: f64, reset: i64) {
        let mut x = Record::new(r.ids.next_at(ms), Transport::Sse, "responses");
        x.account = Some(acct.into());
        x.model = Some(model.into());
        x.status = 200;
        x.input_tokens = Some(100_000);
        x.cached_tokens = Some(0);
        x.output_tokens = Some(out);
        x.quota = Some(QuotaObs {
            plan: Some("plus".into()),
            p_pct: Some(p),
            p_reset: Some(reset),
            p_window_min: Some(300),
            ..Default::default()
        });
        assert!(r.record(x));
    }

    #[test]
    fn migrates_from_v14_and_adds_tables() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("u.db");
        {
            let c = rusqlite::Connection::open(&p).unwrap();
            c.execute_batch(
                "CREATE TABLE labels(id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);
                 CREATE TABLE quota_obs(id INTEGER PRIMARY KEY, account INTEGER NOT NULL, instance INTEGER,
                   plan INTEGER, p_pct REAL, p_reset INTEGER, p_win INTEGER,
                   s_pct REAL, s_reset INTEGER, s_win INTEGER);
                 PRAGMA user_version=14;",
            )
            .unwrap();
        }
        let c = store::open_rw(&p).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, store::SCHEMA_VERSION);
        let cards: i64 = c
            .query_row(
                "SELECT count(*) FROM pragma_table_info('quota_obs') WHERE name='cards'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cards, 1);
        let n: i64 = c
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name='quota_reset_events'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
        let n: i64 = c
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name='quota_cycle_estimates'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn one_write_per_cycle_close() {
        let d = tempfile::tempdir().unwrap();
        let r = Recorder::open_as(&cfg(d.path()), "host-a").unwrap();
        // A scheduled reset in the past so the next snapshot is "due".
        let old_reset = crate::now_ms() / 1000 - 60;
        let t0 = crate::now_ms() - 120_000;
        let new_reset = crate::now_ms() / 1000 + 300 * 60;
        snap(&r, t0, "a", "gpt-6.1-sol", 1_000, 10.0, old_reset);
        snap(&r, t0 + 20_000, "a", "gpt-6.1-sol", 1_000, 25.0, old_reset);
        snap(&r, t0 + 40_000, "a", "gpt-6.1-sol", 1_000, 50.0, old_reset);
        snap(&r, t0 + 60_000, "a", "gpt-6.1-sol", 1_000, 2.0, new_reset);
        r.flush_blocking().unwrap();

        let c = store::open_ro(&d.path().join("u.db")).unwrap();
        let events: i64 = c
            .query_row("SELECT count(*) FROM quota_reset_events", [], |x| x.get(0))
            .unwrap();
        let cycles: i64 = c
            .query_row("SELECT count(*) FROM quota_cycle_estimates", [], |x| {
                x.get(0)
            })
            .unwrap();
        assert_eq!(events, 1, "one event for the primary reset");
        assert_eq!(cycles, 1, "one estimate row written with the event");
        let kind: String = c
            .query_row("SELECT kind FROM quota_reset_events", [], |x| x.get(0))
            .unwrap();
        assert_eq!(kind, "natural");

        // Unchanged refresh must not write another close.
        r.observe_quota(
            "a",
            "manual",
            QuotaObs {
                plan: Some("plus".into()),
                p_pct: Some(2.0),
                p_reset: Some(new_reset),
                p_window_min: Some(300),
                ..Default::default()
            },
        );
        r.flush_blocking().unwrap();
        let events2: i64 = c
            .query_row("SELECT count(*) FROM quota_reset_events", [], |x| x.get(0))
            .unwrap();
        assert_eq!(events2, 1);

        let hist = r
            .quota_history(Some("a"), t0 - 1_000, crate::now_ms() + 1_000)
            .await
            .unwrap();
        assert_eq!(hist.len(), 1);
        assert_eq!(hist[0].events.len(), 1);
        assert!(hist[0]
            .cycles
            .iter()
            .any(|c| !c.open && c.close_kind == "natural"));
        r.shutdown();
    }

    #[tokio::test]
    async fn idle_slide_and_limit_change() {
        let d = tempfile::tempdir().unwrap();
        let r = Recorder::open_as(&cfg(d.path()), "host-a").unwrap();
        let reset = crate::now_ms() / 1000 + 10_000;
        let t0 = crate::now_ms() - 60_000;
        // only resets_at moved
        snap(&r, t0, "a", "gpt-6.1-sol", 100, 40.0, reset);
        snap(&r, t0 + 10, "a", "gpt-6.1-sol", 100, 40.0, reset + 400);
        r.flush_blocking().unwrap();
        let c = store::open_ro(&d.path().join("u.db")).unwrap();
        let n: i64 = c
            .query_row("SELECT count(*) FROM quota_reset_events", [], |x| x.get(0))
            .unwrap();
        assert_eq!(n, 0, "idle slide is not persisted as a reset");

        // percent halved, same resets_at → limit change
        snap(&r, t0 + 20, "a", "gpt-6.1-sol", 100, 20.0, reset + 410);
        r.flush_blocking().unwrap();
        let c = store::open_ro(&d.path().join("u.db")).unwrap();
        let kind: String = c
            .query_row("SELECT kind FROM quota_reset_events", [], |x| x.get(0))
            .unwrap();
        assert_eq!(kind, "limit_change");
        let cycles: i64 = c
            .query_row("SELECT count(*) FROM quota_cycle_estimates", [], |x| {
                x.get(0)
            })
            .unwrap();
        assert_eq!(cycles, 1);
        r.shutdown();
    }

    #[tokio::test]
    async fn card_expiry_does_not_write_reset_event() {
        let d = tempfile::tempdir().unwrap();
        let r = Recorder::open_as(&cfg(d.path()), "host-a").unwrap();
        let reset = crate::now_ms() / 1000 + 10_000;
        let snap = |cards: i64| QuotaObs {
            plan: Some("plus".into()),
            p_pct: Some(55.0),
            p_reset: Some(reset),
            p_window_min: Some(300),
            cards: Some(cards),
            ..Default::default()
        };
        r.observe_quota("a", "manual", snap(2));
        r.observe_quota("a", "manual", snap(1));
        r.flush_blocking().unwrap();
        let c = store::open_ro(&d.path().join("u.db")).unwrap();
        let n: i64 = c
            .query_row("SELECT count(*) FROM quota_reset_events", [], |x| x.get(0))
            .unwrap();
        assert_eq!(n, 0, "card expiry without a quota restore is not a reset");
        r.shutdown();
    }
}
