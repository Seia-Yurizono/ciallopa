use crate::filter::{label_id, Filters, LabelNames};
use crate::prices::{QueryPrices, Usage};
use crate::problem::{failure_class, Outcome};
use crate::refusals::refusal_minutes;
use crate::{RefusalQuery, PREWARM_ENDPOINT};
use crate::{Transport, UsageError, SEQ_BITS};
use rusqlite::{params_from_iter, types::Value, Connection};
use serde::Serialize;
use std::collections::HashMap;

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

struct BucketAcc {
    view: DayBucket,
    acc: Acc,
    incomplete: u64,
}
impl BucketAcc {
    fn add(&mut self, facts: RowFacts) {
        // Preserve the legacy day figure: unknown outcomes are excluded from incomplete.
        self.incomplete += u64::from(
            facts.counted && facts.incomplete && !facts.err && !facts.cancelled && !facts.unknown,
        );
        self.acc.add(facts);
    }
    fn finish(mut self) -> DayBucket {
        let figures = self.acc.finish(Default::default());
        self.view.count = figures.count;
        self.view.errors = figures.errors;
        self.view.failures = figures.failures;
        self.view.failure_classes = figures.failure_classes;
        self.view.upstream_failures = figures.upstream_failures;
        self.view.cancelled = figures.cancelled;
        self.view.unknown = figures.unknown;
        self.view.prewarm_count = figures.prewarm_count;
        self.view.stream_ttft_p50_ms = figures.stream_ttft_p50_ms;
        self.view.stream_ttft_p90_ms = figures.stream_ttft_p90_ms;
        self.view.stream_ttft_p99_ms = figures.stream_ttft_p99_ms;
        self.view.duration_p50_ms = figures.duration_p50_ms;
        self.view.duration_p90_ms = figures.duration_p90_ms;
        self.view.duration_p99_ms = figures.duration_p99_ms;
        self.view.usage_samples = figures.usage_samples;
        self.view.input_tokens = figures.input_tokens;
        self.view.output_tokens = figures.output_tokens;
        self.view.cached_tokens = figures.cached_tokens;
        self.view.reasoning_tokens = figures.reasoning_tokens;
        self.view.cost_usd = figures.cost_usd;
        self.view.unknown_price = figures.unknown_price;
        self.view.incomplete = self.incomplete;
        self.view
    }
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

#[derive(Clone, Copy)]
struct RowFacts {
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
    fn add(&mut self, o: RowFacts) {
        let RowFacts {
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
    let mut names = LabelNames::new(c);
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
        fparams.push(label_id(c, PREWARM_ENDPOINT)?.unwrap_or(-1).into());
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
    let mut ps: Vec<Value> = vec![
        (q.from_ms << SEQ_BITS).into(),
        if q.to_ms >= i64::MAX >> SEQ_BITS {
            i64::MAX
        } else {
            ((q.to_ms + 1) << SEQ_BITS) - 1
        }
        .into(),
    ];
    ps.extend(fparams);
    let mut st = c.prepare(&sql)?;
    let mut groups: HashMap<Vec<Option<i64>>, Acc> = HashMap::new();
    let mut rows = st.query(params_from_iter(ps))?;
    let mut pricing = QueryPrices::new(prices);
    let mut unknown_models = std::collections::BTreeSet::new();
    let mut days: std::collections::BTreeMap<i64, BucketAcc> = Default::default();
    let width = if q.bucket_hour { 3_600_000 } else { 86_400_000 };
    let off = i64::from(q.bucket_day.unwrap_or(0)) * 60_000;
    let bucket_of = |k: i64| {
        let local = k * width;
        BucketAcc {
            view: DayBucket {
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
            },
            acc: Acc::default(),
            incomplete: 0,
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
        names.load([err, m, t, r.get(12)?, r.get(13)?])?;
        names.load(
            dims.iter()
                .zip(&key)
                .filter_map(|(dim, id)| (*dim != Dim::Transport).then_some(*id)),
        )?;
        let lbl = |x: Option<i64>| x.and_then(|x| names.get(&x)).map(String::as_str);
        let price = pricing.resolve((m, t), r.get::<_, i64>(9)? >> SEQ_BITS, lbl(m), lbl(t));
        let cost = if tok[0].is_none() && tok[1].is_none() {
            (None, "no_tokens")
        } else {
            (
                price.cost(Usage {
                    input: tok[0].unwrap_or(0),
                    output: tok[1].unwrap_or(0),
                    cached: tok[2].unwrap_or(0),
                    cache_write: r.get::<_, Option<i64>>(11)?.unwrap_or(0),
                }),
                price.status(),
            )
        };
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
                Some(t) if pricing.tier(r.get(10)?, Some(t)) != crate::Tier::Standard => {
                    format!(
                        "{} ({})",
                        lbl(m).unwrap_or("?"),
                        pricing.tier(r.get(10)?, Some(t)).name()
                    )
                }
                _ => lbl(m).unwrap_or("?").to_string(),
            };
            unknown_models.insert(k);
        }
        let facts = RowFacts {
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
        };
        if q.bucket_day.is_some() {
            let id: i64 = r.get(9)?;
            let k = ((id >> SEQ_BITS) + off).div_euclid(width);
            days.entry(k).or_insert_with(|| bucket_of(k)).add(facts);
        }
        groups.entry(key).or_default().add(facts);
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
            b.view.refused += n;
            *b.view.refusals.entry(reason.as_str()).or_default() += n;
        }
    }
    out.buckets = q
        .bucket_day
        .map(|_| days.into_values().map(BucketAcc::finish).collect());
    Ok(out)
}

// ---------------------------------------------------------------- quota
