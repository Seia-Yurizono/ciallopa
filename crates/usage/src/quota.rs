use crate::classify::RESET_TOLERANCE_S;
use crate::filter::label_map;
use crate::{UsageError, SEQ_BITS};
use ciallopa_core::config::QuotaWeights;
use rusqlite::{params, Connection};
use serde::Serialize;

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
    /// Δ%-weighted Theil–Sen capacity in included-usage credits. None = show nothing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capacity_credits: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remaining_credits: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credits_used: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capacity_lo: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capacity_hi: Option<f64>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub coverage_gap: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_cost_usd: Option<f64>,
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
        capacity_credits: None,
        remaining_credits: None,
        credits_used: None,
        capacity_lo: None,
        capacity_hi: None,
        coverage_gap: false,
        confidence: None,
        api_cost_usd: None,
    }
}

/// Latest snapshot + capacity estimate per account (see docs/USAGE.md "Quota").
pub(crate) fn quota(
    c: &Connection,
    w: &QuotaWeights,
    prices: &crate::prices::PriceTable,
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
                (Some(_), r) => estimate(c, w, prices, account, i, r)?,
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
    prices: &crate::prices::PriceTable,
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
    if let Ok((cap, api_cost, _)) =
        crate::history::live_capacity(c, prices, account, which, reset, l)
    {
        e.capacity_credits = cap.capacity;
        e.credits_used = Some(cap.credits_used);
        e.capacity_lo = cap.lo;
        e.capacity_hi = cap.hi;
        e.coverage_gap = cap.coverage_gap;
        e.confidence = Some(cap.confidence.as_str());
        e.api_cost_usd = api_cost;
        if let Some(cap_c) = cap.capacity {
            let used = pct_at(l)?;
            e.remaining_credits = Some((100.0 - used).max(0.0) / 100.0 * cap_c);
        }
    }
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
