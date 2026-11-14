use crate::filter::{label_map, recent_where, RecentQuery};
use crate::problem::row_failure_class;
use crate::{Transport, UsageError, SEQ_BITS};
use rusqlite::{params_from_iter, types::Value, Connection};
use serde::Serialize;
use std::collections::HashMap;

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
pub(crate) fn row_cost(
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

pub(crate) fn tps(out: Option<i64>, ttft: Option<u32>, dur: u32) -> Option<f64> {
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
    Ok(record_rows(c, "id = ?", vec![id.into()], 1, 0, prices)?.pop())
}

fn record_rows(
    c: &Connection,
    w: &str,
    mut ps: Vec<Value>,
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
    ps.push((limit.clamp(1, 1001) as i64).into());
    ps.push((offset.min(1_000_000) as i64).into());
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
