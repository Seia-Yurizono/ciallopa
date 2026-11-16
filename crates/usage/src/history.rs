//! Reset-event and per-cycle capacity persistence (schema v15).
//!
//! A cycle-closing classification writes the event and the estimate in the same transaction
//! as the triggering `quota_obs` row. In-progress cycles are computed at read time.
use crate::classify::{self, CardSource, Class, Side, Transition, WindowKind};
use crate::credits;
use crate::estimate::{self, Capacity, Confidence};
use crate::prices::{PriceTable, QueryPrices, Usage};
use crate::{QuotaObs, UsageError, SEQ_BITS};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::Serialize;

const CARD_LOOKBACK_MS: i64 = 5 * 60_000;

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

fn recent_card(
    tx: &Transaction,
    account: &str,
    now_ms: i64,
) -> rusqlite::Result<Option<CardSource>> {
    let row: Option<Option<String>> = tx
        .query_row(
            "SELECT key FROM reset_consume WHERE account=?1 AND status='done' \
             AND code='reset' AND finished_ms IS NOT NULL \
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
    for which in WindowKind::ALL {
        let prev = which.side(old);
        let next = which.side(Some(new));
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
    which: WindowKind,
    prev: &Side,
    next: &Side,
    class: Class,
) -> rusqlite::Result<()> {
    let window = which.as_str();
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
    let data = load_cycle(tx, prices, account_id, which, prev.reset, Some(obs_id))?;
    let mut cap = data.capacity();
    if cap.delta_pct.is_none() {
        cap.delta_pct = data
            .obs
            .first()
            .and_then(|a| data.obs.last().map(|b| b.1 - a.1));
    }
    let started_ms = data
        .obs
        .first()
        .map(|(id, _)| *id >> SEQ_BITS)
        .unwrap_or(now_ms);
    tx.execute(
        "INSERT OR IGNORE INTO quota_cycle_estimates(\
         account,window,started_ms,ended_ms,ended_obs_id,cycle_reset_at,close_kind,\
         capacity_credits,capacity_lo,capacity_hi,delta_pct,credits_used,api_cost_usd,\
         requests,snapshots,coverage_gap,confidence,card_source,over_credits,over_api_usd) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)",
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
            data.over_credits,
            data.over_api_cost,
        ],
    )?;
    let cycle_id = tx.last_insert_rowid();
    tx.execute(
        "UPDATE quota_reset_events SET cycle_id=?2 WHERE id=?1",
        params![event_id, cycle_id],
    )?;
    Ok(())
}

pub(crate) struct CycleData {
    pub obs: Vec<(i64, f64)>,
    req_credits: Vec<(i64, f64)>,
    /// Included usage (额度) only; requests served after the window was full are excluded.
    credits_used: f64,
    /// API list price of every request in the cycle, credit-funded ones included.
    api_cost: Option<f64>,
    /// Purchased credits (点数) of requests served while the window was full.
    pub over_credits: Option<f64>,
    pub over_api_cost: Option<f64>,
    pub over_requests: i64,
    pub requests: i64,
    pub token_requests: i64,
    pub tokens: [f64; 3],
}

/// One request of a cycle, before the included / credit-funded split.
struct CycleReq {
    id: i64,
    credits: Option<f64>,
    bought: Option<f64>,
    usd: Option<f64>,
    tokens: [f64; 3],
    own_pct: Option<f64>,
    own_full: bool,
    other_full: bool,
}

/// A window whose own snapshot on the request says it is full (and not yet reset).
fn full(pct: Option<f64>, reset: Option<i64>, at_s: i64) -> bool {
    pct.is_some_and(|p| p >= estimate::FULL_PCT) && reset.is_none_or(|r| r > at_s)
}

/// Requests served while this window, or the other one, was full: the upstream keeps
/// answering from purchased credits, so they are not included usage of this window.
/// When only the other window is full (Plus 5h at 100%), a run counts as included if this
/// window's percent still moved across it.
fn credit_funded(reqs: &[CycleReq]) -> Vec<bool> {
    let n = reqs.len();
    let mut over = vec![false; n];
    let mut i = 0;
    while i < n {
        if reqs[i].own_full {
            over[i] = true;
            i += 1;
            continue;
        }
        if !reqs[i].other_full {
            i += 1;
            continue;
        }
        let start = i;
        while i < n && !reqs[i].own_full && reqs[i].other_full {
            i += 1;
        }
        let end = if i < n { i + 1 } else { i };
        let moved = reqs[start].own_pct.is_some_and(|first| {
            reqs[start..end]
                .iter()
                .filter_map(|r| r.own_pct)
                .any(|p| p - first >= 1.0)
        });
        if !moved {
            over[start..i].iter_mut().for_each(|o| *o = true);
        }
    }
    over
}

pub(crate) fn load_cycle(
    c: &Connection,
    prices: &PriceTable,
    account_id: i64,
    which: WindowKind,
    reset: Option<i64>,
    end_exclusive: Option<i64>,
) -> rusqlite::Result<CycleData> {
    let (pc, rc) = which.cols();
    let since = crate::epoch::since(c, account_id, which)?.unwrap_or(i64::MIN);
    let (lo, hi) = match reset {
        Some(r) => (
            r - classify::RESET_TOLERANCE_S,
            r + classify::RESET_TOLERANCE_S,
        ),
        None => (i64::MIN / 4, i64::MAX / 4),
    };
    let sql = format!(
        "SELECT id, {pc} FROM quota_obs WHERE account=?1 AND {pc} IS NOT NULL AND id >= ?6 \
         AND id < ?2 AND ({rc} BETWEEN ?3 AND ?4 OR (?5 AND {rc} IS NULL)) ORDER BY id"
    );
    let mut st = c.prepare(&sql)?;
    let obs: Vec<(i64, f64)> = st
        .query_map(
            params![
                account_id,
                end_exclusive.unwrap_or(i64::MAX),
                lo,
                hi,
                reset.is_none(),
                since
            ],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?
        .collect::<rusqlite::Result<_>>()?;
    let last = obs.last().map(|(id, _)| *id).unwrap_or(0);
    let end_exclusive = end_exclusive.unwrap_or_else(|| last.saturating_add(1));
    let first = obs.first().map(|(id, _)| *id).unwrap_or(end_exclusive);
    let mut names = crate::filter::LabelNames::new(c);
    let mut pricing = QueryPrices::new(prices);
    let mut rst = c.prepare(
        "SELECT r.id, coalesce(r.in_tok,0), coalesce(r.cached_tok,0), coalesce(r.cw_tok,0), \
         coalesce(r.out_tok,0), r.tier, coalesce(r.resp_model, r.model), \
         r.p_pct, r.p_reset, r.s_pct, r.s_reset \
         FROM requests r \
         WHERE r.account=?1 AND r.id>=?2 AND r.id<?3 ORDER BY r.id",
    )?;
    type Row = (
        i64,
        [i64; 4],
        Option<i64>,
        Option<i64>,
        [Option<f64>; 2],
        [Option<i64>; 2],
    );
    let rows = rst.query_map(params![account_id, first, end_exclusive], |r| {
        Ok::<Row, _>((
            r.get(0)?,
            [r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?],
            r.get(5)?,
            r.get(6)?,
            [r.get(7)?, r.get(9)?],
            [r.get(8)?, r.get(10)?],
        ))
    })?;
    let (own, other) = (which.index(), 1 - which.index());
    let mut reqs = Vec::new();
    for row in rows {
        let (id, [inp, cached, cw, out], tier, model, pct, rst) = row?;
        let u = Usage {
            input: inp,
            cached,
            cache_write: cw,
            output: out,
        };
        names.load([model, tier])?;
        let model_name = model.and_then(|id| names.get(&id)).map(String::as_str);
        let tier_name = tier.and_then(|id| names.get(&id)).map(String::as_str);
        let price = pricing.resolve((model, tier), id >> SEQ_BITS, model_name, tier_name);
        let tier = pricing.tier(tier, tier_name);
        let at_s = (id >> SEQ_BITS).div_euclid(1000);
        reqs.push(CycleReq {
            id,
            credits: credits::credits(model_name, tier, u),
            bought: credits::purchased(model_name, tier, u),
            usd: price.cost(u),
            tokens: [(inp - cached).max(0) as f64, cached as f64, out as f64],
            own_pct: pct[own],
            own_full: full(pct[own], rst[own], at_s),
            other_full: full(pct[other], rst[other], at_s),
        });
    }
    let over = credit_funded(&reqs);
    let mut d = CycleData {
        obs,
        req_credits: Vec::new(),
        credits_used: 0.0,
        api_cost: None,
        over_credits: None,
        over_api_cost: None,
        over_requests: 0,
        requests: reqs.len() as i64,
        token_requests: 0,
        tokens: [0.0; 3],
    };
    let add = |acc: &mut Option<f64>, v: f64| *acc = Some(acc.unwrap_or(0.0) + v);
    for (r, over) in reqs.iter().zip(over) {
        if let Some(usd) = r.usd {
            add(&mut d.api_cost, usd);
        }
        if over {
            d.over_requests += 1;
            if let Some(c) = r.bought {
                add(&mut d.over_credits, c);
            }
            if let Some(usd) = r.usd {
                add(&mut d.over_api_cost, usd);
            }
            continue;
        }
        // Legacy weighted-token estimates exclude the request observing the last snapshot.
        if r.id < last {
            d.token_requests += 1;
            for (t, v) in d.tokens.iter_mut().zip(r.tokens) {
                *t += v;
            }
        }
        if let Some(c) = r.credits {
            d.req_credits.push((r.id, c));
            d.credits_used += c;
        }
    }
    Ok(d)
}

/// The still-open window up to and including `end_inclusive` (read path; not persisted).
pub(crate) fn live_cycle(
    c: &Connection,
    prices: &PriceTable,
    account_id: i64,
    which: WindowKind,
    reset: Option<i64>,
    end_inclusive: i64,
) -> Result<CycleData, UsageError> {
    Ok(load_cycle(
        c,
        prices,
        account_id,
        which,
        reset,
        Some(end_inclusive.saturating_add(1)),
    )?)
}

impl CycleData {
    pub fn capacity(&self) -> Capacity {
        // Censored snapshots (window full) carry no percent information for the slope.
        let obs: Vec<(i64, f64)> = self
            .obs
            .iter()
            .copied()
            .filter(|(_, p)| *p < estimate::FULL_PCT)
            .collect();
        let mut cap = if obs.len() >= 2 {
            estimate::estimate(&estimate::points_from(&obs, &self.req_credits), 5.0)
        } else {
            Capacity {
                capacity: None,
                lo: None,
                hi: None,
                delta_pct: None,
                credits_used: 0.0,
                coverage_gap: false,
                confidence: Confidence::Insufficient,
            }
        };
        cap.credits_used = self.credits_used;
        cap
    }
    pub fn api_cost(&self) -> Option<f64> {
        self.api_cost
    }
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
    /// Purchased credits (点数) used while the window was full; None before v19.
    pub over_credits: Option<f64>,
    pub over_api_usd: Option<f64>,
    /// Ended before the active manual reset of this window: kept, not used for trends.
    pub archived: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AccountHistory {
    pub account: String,
    pub from_ms: i64,
    pub to_ms: i64,
    pub events: Vec<ResetEvent>,
    pub cycles: Vec<CycleRow>,
    /// Manual estimate resets: active ones and those still undoable.
    pub epochs: Vec<crate::epoch::EpochRow>,
}

/// Persisted events + cycles in `[from_ms, to_ms]`, plus the in-progress cycle when present.
pub(crate) fn history(
    c: &Connection,
    prices: &PriceTable,
    account: Option<&str>,
    from_ms: i64,
    to_ms: i64,
) -> Result<Vec<AccountHistory>, UsageError> {
    let mut names = crate::filter::LabelNames::new(c);
    let acct_id = match account {
        Some(n) => match crate::filter::label_id(c, n)? {
            Some(id) => Some(id),
            None => {
                return Ok(vec![AccountHistory {
                    account: n.to_string(),
                    from_ms,
                    to_ms,
                    events: vec![],
                    cycles: vec![],
                    epochs: vec![],
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
         requests,snapshots,coverage_gap,confidence,card_source,over_credits,over_api_usd \
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
                    over_credits: r.get(18)?,
                    over_api_usd: r.get(19)?,
                    archived: false,
                },
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;

    names.load(
        ev_rows
            .iter()
            .map(|(id, _)| Some(*id))
            .chain(cy_rows.iter().map(|(id, _)| Some(*id))),
    )?;
    let mut accounts = c.prepare_cached(
        "SELECT DISTINCT account FROM quota_obs WHERE (?1 IS NULL OR account=?1)",
    )?;
    let targets = accounts
        .query_map([acct_id], |r| r.get::<_, i64>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    names.load(targets.iter().copied().map(Some))?;
    let mut by: std::collections::BTreeMap<i64, AccountHistory> = std::collections::BTreeMap::new();
    let blank = |id: i64| AccountHistory {
        account: names.get(&id).cloned().unwrap_or_default(),
        from_ms,
        to_ms,
        events: vec![],
        cycles: vec![],
        epochs: vec![],
    };
    for (id, e) in ev_rows {
        by.entry(id).or_insert_with(|| blank(id)).events.push(e);
    }
    for (id, e) in cy_rows {
        by.entry(id).or_insert_with(|| blank(id)).cycles.push(e);
    }
    let now = crate::now_ms();
    // In-progress cycle for each (requested) account that has a latest snapshot.
    for id in targets {
        // Keep an account with a recent estimate reset listed so the reset can be undone.
        if !crate::epoch::rows(c, id, now)?.is_empty() {
            by.entry(id).or_insert_with(|| blank(id));
        }
        for which in WindowKind::ALL {
            if let Some(open) = open_cycle(c, prices, id, which)? {
                if open.ended_ms >= from_ms && open.started_ms <= to_ms {
                    by.entry(id).or_insert_with(|| blank(id)).cycles.push(open);
                }
            }
        }
    }
    for (id, h) in by.iter_mut() {
        for which in WindowKind::ALL {
            if let Some(from) = crate::epoch::since(c, *id, which)? {
                let from_ms = from >> SEQ_BITS;
                for cy in h.cycles.iter_mut() {
                    cy.archived = !cy.open && cy.window == which.as_str() && cy.ended_ms < from_ms;
                }
            }
        }
        h.epochs = crate::epoch::rows(c, *id, now)?;
        apply_trend(&mut h.cycles);
    }
    Ok(by.into_values().collect())
}

fn open_cycle(
    c: &Connection,
    prices: &PriceTable,
    account_id: i64,
    which: WindowKind,
) -> Result<Option<CycleRow>, UsageError> {
    let (pc, rc) = which.cols();
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
    let window = which.as_str();
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
    let data = live_cycle(c, prices, account_id, which, reset, last_id)?;
    let cap = data.capacity();
    let since = crate::epoch::since(c, account_id, which)?.unwrap_or(i64::MIN);
    let data_start: Option<i64> = c
        .query_row(
            &format!(
                "SELECT min(id) FROM quota_obs WHERE account=?1 AND {pc} IS NOT NULL AND id >= ?5 \
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
                reset.is_none(),
                since
            ],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    let Some(start_id) = data_start else {
        return Ok(None);
    };
    // Right after an estimate reset the cycle has one snapshot and no usage yet; still list it
    // (as 估算中) so the reset is visible.
    if start_id == last_id && cap.credits_used <= 0.0 && since == i64::MIN {
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
        api_cost_usd: data.api_cost(),
        requests: Some(data.requests),
        snapshots: None,
        coverage_gap: cap.coverage_gap,
        confidence: cap.confidence.as_str().into(),
        card_source: None,
        trend: None,
        open: true,
        over_credits: data.over_credits,
        over_api_usd: data.over_api_cost,
        archived: false,
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
            if cycles[i].archived {
                continue;
            }
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

    use crate::tests::cfg;
    use crate::{Record, Recorder, Transport};

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

        let c = crate::schema::open_ro(&d.path().join("u.db")).unwrap();
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
        let c = crate::schema::open_ro(&d.path().join("u.db")).unwrap();
        let n: i64 = c
            .query_row("SELECT count(*) FROM quota_reset_events", [], |x| x.get(0))
            .unwrap();
        assert_eq!(n, 0, "idle slide is not persisted as a reset");

        // percent halved, same resets_at → limit change
        snap(&r, t0 + 20, "a", "gpt-6.1-sol", 100, 20.0, reset + 410);
        r.flush_blocking().unwrap();
        let c = crate::schema::open_ro(&d.path().join("u.db")).unwrap();
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
        let c = crate::schema::open_ro(&d.path().join("u.db")).unwrap();
        let n: i64 = c
            .query_row("SELECT count(*) FROM quota_reset_events", [], |x| x.get(0))
            .unwrap();
        assert_eq!(n, 0, "card expiry without a quota restore is not a reset");
        r.shutdown();
    }
}
