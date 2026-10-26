//! Request problem buckets: the single place that decides which user-facing problem a stored
//! request row shows under (报错 / 上游出错 / 没答完), its cause, whether a retry or account
//! switch rescued it, and which next step to suggest. The record list (`problem`/`cause`
//! filters), the per-bucket counts and every row's fields all use these rules, in Rust for one
//! row ([`classify`]) and as SQL predicates for many ([`Preds`]); a test checks they agree.
//!
//! Refusals (本地拒绝) are not request rows; their counts come from the refusals table.
use crate::store::{label_id, QUOTA_ERRORS};
use crate::PREWARM_ENDPOINT;
use rusqlite::Connection;
use serde::Serialize;

/// Mutually exclusive request outcome; the summary counts use the same rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Completed,
    Failed,
    Cancelled,
    Incomplete,
    Unknown,
    Prewarm,
}

impl Outcome {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            "incomplete" => Some(Self::Incomplete),
            "unknown" => Some(Self::Unknown),
            "prewarm" => Some(Self::Prewarm),
            _ => None,
        }
    }
}

/// Problem chips on the request list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Problem {
    /// Every business request (prewarm only with `exclude_prewarm` off).
    All,
    /// 报错: the client received an error (outcome `failed`).
    Error,
    /// 上游出错: the upstream side (HTTP, stream or network) failed at least once, including
    /// requests a retry or another account rescued.
    Upstream,
    /// 没答完: client disconnect, upstream `response.incomplete` or an unconfirmed end. Not
    /// counted as failures.
    Unfinished,
    /// 本地拒绝: refused by a local limit before a request row existed (refusals table).
    Refused,
}

impl Problem {
    pub const ALL: [Problem; 5] = [
        Problem::All,
        Problem::Error,
        Problem::Upstream,
        Problem::Unfinished,
        Problem::Refused,
    ];
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|p| p.name() == value.trim())
    }
    pub fn name(self) -> &'static str {
        match self {
            Problem::All => "all",
            Problem::Error => "error",
            Problem::Upstream => "upstream",
            Problem::Unfinished => "unfinished",
            Problem::Refused => "refused",
        }
    }
    /// Causes in display order (refusal reasons for [`Problem::Refused`]).
    pub fn causes(self) -> &'static [&'static str] {
        match self {
            Problem::All => &[],
            Problem::Error => ERROR_CAUSES,
            Problem::Upstream => UPSTREAM_CAUSES,
            Problem::Unfinished => UNFINISHED_CAUSES,
            Problem::Refused => &["inflight", "ws", "body", "ws_msg"],
        }
    }
}

/// 报错 causes: 额度 / 限流 / 上游 / 网络代理 / 网关 / 流中断.
pub const ERROR_CAUSES: &[&str] = &[
    "quota",
    "rate_limit",
    "upstream",
    "network",
    "gateway",
    "stream",
];
/// 上游出错 split: 未救回 / 已救回.
pub const UPSTREAM_CAUSES: &[&str] = &["unrescued", "rescued"];
/// 没答完 reasons: 客户端断开 / 上游截断 / 未确认结束.
pub const UNFINISHED_CAUSES: &[&str] = &["client_closed", "truncated", "unconfirmed"];

/// Error sources that count as the upstream side for 上游出错.
const UPSTREAM_SOURCES: &[&str] = &["upstream", "stream", "network"];
/// Codes that point at this host's egress (proxy or the network in front of the upstream).
const EGRESS_ERRORS: &[&str] = &["proxy_connect", "proxy_timeout", "cf_blocked", "challenge"];
/// Local answers that mean no account could take the request.
const ACCOUNT_ERRORS: &[&str] = &["all_cooling", "no_account", "no_usable_account"];

/// The stored columns the classification reads.
#[derive(Debug, Clone, Copy, Default)]
pub struct Facts<'a> {
    pub status: u16,
    pub error: Option<&'a str>,
    pub source: Option<&'a str>,
    pub upstream_status: Option<u16>,
    pub endpoint: Option<&'a str>,
    pub incomplete: bool,
    pub retries: u32,
    pub switches: u32,
}

/// One row's classification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Class {
    pub outcome: Outcome,
    /// `error`, `unfinished` or null; the user-facing bucket the row belongs to.
    pub problem: Option<&'static str>,
    /// The cause within `problem` ([`ERROR_CAUSES`] or [`UNFINISHED_CAUSES`]).
    pub cause: Option<&'static str>,
    /// In 上游出错 (an upstream-side failure or a rescued request).
    pub upstream_fault: bool,
    /// Completed only after a retry or an account switch.
    pub rescued: bool,
    /// Suggested next steps, most useful first: `refresh_quota`, `account`, `cooldown`,
    /// `attempts`, `egress`, `accounts`, `new_session`, `detail`. The UI only navigates to or
    /// opens existing views for them.
    pub next: Vec<&'static str>,
}

pub fn outcome(f: &Facts) -> Outcome {
    if f.endpoint == Some(PREWARM_ENDPOINT) {
        return Outcome::Prewarm;
    }
    if f.source == Some("client") || f.error == Some("client_closed") {
        return Outcome::Cancelled;
    }
    if f.error == Some("observation_incomplete") && f.source == Some("local") && f.status < 400 {
        return Outcome::Unknown;
    }
    if f.error.is_some() || f.status >= 400 {
        return Outcome::Failed;
    }
    if f.incomplete {
        Outcome::Incomplete
    } else {
        Outcome::Completed
    }
}

/// 报错 cause from a [`crate::failure_class`]: `local` and `other` are the gateway.
pub fn error_cause(failure_class: &str) -> &'static str {
    match failure_class {
        "quota" => "quota",
        "rate_limit" => "rate_limit",
        "upstream" => "upstream",
        "network" => "network",
        "stream" => "stream",
        _ => "gateway",
    }
}

pub fn classify(f: &Facts) -> Class {
    let outcome = outcome(f);
    let (problem, cause) = match outcome {
        Outcome::Failed => {
            let class = crate::failure_class(f.status, f.error, f.source, f.upstream_status);
            (Some("error"), Some(error_cause(class)))
        }
        Outcome::Cancelled => (Some("unfinished"), Some("client_closed")),
        Outcome::Incomplete => (Some("unfinished"), Some("truncated")),
        Outcome::Unknown => (Some("unfinished"), Some("unconfirmed")),
        Outcome::Completed | Outcome::Prewarm => (None, None),
    };
    let rescued = outcome == Outcome::Completed && (f.retries > 0 || f.switches > 0);
    let upstream_fault = rescued
        || (outcome == Outcome::Failed && f.source.is_some_and(|s| UPSTREAM_SOURCES.contains(&s)));
    let code = f.error.unwrap_or("");
    let auth = |s: Option<u16>| matches!(s, Some(401 | 403));
    let next: Vec<&'static str> = if EGRESS_ERRORS.contains(&code) {
        vec!["egress"]
    } else if rescued {
        vec!["attempts"]
    } else {
        match cause {
            Some("quota") => vec!["refresh_quota", "account"],
            Some("rate_limit") => vec!["cooldown"],
            Some("upstream") if code == "invalid_encrypted_content" => vec!["new_session"],
            Some("upstream") if auth(Some(f.status)) || auth(f.upstream_status) => {
                vec!["account"]
            }
            Some("upstream" | "stream") => vec!["attempts"],
            Some("network") => vec!["egress", "attempts"],
            Some("gateway") if ACCOUNT_ERRORS.contains(&code) => vec!["accounts"],
            Some("gateway" | "unconfirmed") => vec!["detail"],
            _ => vec![],
        }
    };
    Class {
        outcome,
        problem,
        cause,
        upstream_fault,
        rescued,
        next,
    }
}

/// Whether a classified row matches a `problem` (and optional `cause`) filter.
pub fn matches(c: &Class, problem: Problem, cause: Option<&str>) -> bool {
    let inb = match problem {
        Problem::All => true,
        Problem::Error => c.problem == Some("error"),
        Problem::Unfinished => c.problem == Some("unfinished"),
        Problem::Upstream => c.upstream_fault,
        Problem::Refused => false,
    };
    inb && match (problem, cause) {
        (_, None) => true,
        (Problem::Upstream, Some("rescued")) => c.rescued,
        (Problem::Upstream, Some("unrescued")) => !c.rescued,
        (Problem::Error | Problem::Unfinished, Some(x)) => c.cause == Some(x),
        _ => false,
    }
}

/// SQL predicates over `requests` columns with label ids resolved once. Ids are integers read
/// from this database, inlined as literals so predicates compose without parameter order.
/// An unknown label is `-1`, which no row has.
pub(crate) struct Preds {
    prewarm: i64,
    client: i64,
    closed: i64,
    obs: i64,
    local: i64,
    upstream: i64,
    network: i64,
    stream: i64,
    quota: String,
}

impl Preds {
    pub(crate) fn new(c: &Connection) -> rusqlite::Result<Self> {
        let id = |n: &str| label_id(c, n).map(|x| x.unwrap_or(-1));
        let mut quota = vec![];
        for e in QUOTA_ERRORS {
            quota.push(id(e)?.to_string());
        }
        Ok(Self {
            prewarm: id(PREWARM_ENDPOINT)?,
            client: id("client")?,
            closed: id("client_closed")?,
            obs: id("observation_incomplete")?,
            local: id("local")?,
            upstream: id("upstream")?,
            network: id("network")?,
            stream: id("stream")?,
            quota: quota.join(","),
        })
    }
    fn not_prewarm(&self) -> String {
        format!("(endpoint IS NULL OR endpoint != {})", self.prewarm)
    }
    fn cancelled(&self) -> String {
        format!(
            "(coalesce(err_source = {}, 0) OR coalesce(error = {}, 0))",
            self.client, self.closed
        )
    }
    fn unknown(&self) -> String {
        format!(
            "(coalesce(error = {}, 0) AND coalesce(err_source = {}, 0) AND status < 400)",
            self.obs, self.local
        )
    }
    fn errored() -> &'static str {
        "(error IS NOT NULL OR status >= 400)"
    }
    /// Rows of one outcome; mirrors [`outcome`].
    pub(crate) fn outcome(&self, o: Outcome) -> String {
        if o == Outcome::Prewarm {
            return format!("coalesce(endpoint = {}, 0)", self.prewarm);
        }
        let (np, ca, un, er) = (
            self.not_prewarm(),
            self.cancelled(),
            self.unknown(),
            Self::errored(),
        );
        match o {
            Outcome::Cancelled => format!("({np} AND {ca})"),
            Outcome::Unknown => format!("({np} AND NOT {ca} AND {un})"),
            Outcome::Failed => format!("({np} AND NOT {ca} AND NOT {un} AND {er})"),
            Outcome::Incomplete => {
                format!("({np} AND NOT {ca} AND NOT {er} AND incomplete IS NOT NULL)")
            }
            Outcome::Completed => {
                format!("({np} AND NOT {ca} AND NOT {er} AND incomplete IS NULL)")
            }
            Outcome::Prewarm => unreachable!(),
        }
    }
    fn is_quota(&self) -> String {
        format!("coalesce(error IN ({}), 0)", self.quota)
    }
    fn is_rate_limit(&self) -> String {
        format!(
            "(status = 429 OR coalesce(err_source = {} AND up_status = 429, 0))",
            self.upstream
        )
    }
    /// Failed rows of one [`crate::failure_class`]; mirrors that function.
    pub(crate) fn failure_class(&self, class: &str) -> Option<String> {
        let failed = self.outcome(Outcome::Failed);
        let (q, rl) = (self.is_quota(), self.is_rate_limit());
        let rest = format!("{failed} AND NOT {q} AND NOT {rl}");
        let srcs = format!(
            "{},{},{},{}",
            self.upstream, self.network, self.stream, self.local
        );
        Some(match class {
            "quota" => format!("({failed} AND {q})"),
            "rate_limit" => format!("({failed} AND NOT {q} AND {rl})"),
            "upstream" => format!("({rest} AND err_source = {})", self.upstream),
            "network" => format!("({rest} AND err_source = {})", self.network),
            "stream" => format!("({rest} AND err_source = {})", self.stream),
            "local" => format!("({rest} AND err_source = {})", self.local),
            "other" => format!("({rest} AND (err_source IS NULL OR err_source NOT IN ({srcs})))"),
            _ => return None,
        })
    }
    fn rescued(&self) -> String {
        format!(
            "({} AND (retries > 0 OR switches > 0))",
            self.outcome(Outcome::Completed)
        )
    }
    fn unrescued(&self) -> String {
        format!(
            "({} AND coalesce(err_source IN ({},{},{}), 0))",
            self.outcome(Outcome::Failed),
            self.upstream,
            self.stream,
            self.network
        )
    }
    /// Rows in `problem` (and `cause`); mirrors [`matches`]. `None`: the pair is invalid or
    /// names refusals, which are not request rows.
    pub(crate) fn problem(&self, problem: Problem, cause: Option<&str>) -> Option<String> {
        Some(match (problem, cause) {
            (Problem::Refused, _) | (Problem::All, Some(_)) => return None,
            (Problem::All, None) => "1".into(),
            (Problem::Error, None) => self.outcome(Outcome::Failed),
            (Problem::Error, Some("gateway")) => format!(
                "({} OR {})",
                self.failure_class("local")?,
                self.failure_class("other")?
            ),
            (Problem::Error, Some(c)) if ERROR_CAUSES.contains(&c) => self.failure_class(c)?,
            (Problem::Upstream, None) => format!("({} OR {})", self.unrescued(), self.rescued()),
            (Problem::Upstream, Some("rescued")) => self.rescued(),
            (Problem::Upstream, Some("unrescued")) => self.unrescued(),
            (Problem::Unfinished, None) => format!(
                "({} OR {} OR {})",
                self.outcome(Outcome::Cancelled),
                self.outcome(Outcome::Incomplete),
                self.outcome(Outcome::Unknown)
            ),
            (Problem::Unfinished, Some("client_closed")) => self.outcome(Outcome::Cancelled),
            (Problem::Unfinished, Some("truncated")) => self.outcome(Outcome::Incomplete),
            (Problem::Unfinished, Some("unconfirmed")) => self.outcome(Outcome::Unknown),
            _ => return None,
        })
    }
}

/// Request counts per problem and cause under one query (the request list's chips).
#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct ProblemCounts {
    pub all: u64,
    pub error: u64,
    pub upstream: u64,
    pub unfinished: u64,
    /// Refusals over the same range, key and instance; null when another filter is set,
    /// since refusals happen before an account or model is known.
    pub refused: Option<u64>,
    /// `{problem: {cause: count}}`, every cause present (zero included).
    pub causes:
        std::collections::BTreeMap<&'static str, std::collections::BTreeMap<&'static str, u64>>,
}

impl ProblemCounts {
    /// Every count zero, every cause present.
    pub fn zero() -> Self {
        let mut out = Self::default();
        for (problem, cause) in count_keys() {
            if let Some(c) = cause {
                out.causes.entry(problem.name()).or_default().insert(c, 0);
            }
        }
        out
    }
}

/// The SUM(CASE…) columns for [`ProblemCounts`], in the order [`counts_from`] reads them.
pub(crate) fn count_columns(p: &Preds) -> String {
    let mut cols = vec!["count(*)".to_string()];
    for (problem, cause) in count_keys() {
        let pred = p.problem(problem, cause).expect("valid count key");
        cols.push(format!(
            "coalesce(sum(CASE WHEN {pred} THEN 1 ELSE 0 END), 0)"
        ));
    }
    cols.join(",")
}

fn count_keys() -> Vec<(Problem, Option<&'static str>)> {
    let mut v = vec![];
    for p in [Problem::Error, Problem::Upstream, Problem::Unfinished] {
        v.push((p, None));
        for c in p.causes() {
            v.push((p, Some(*c)));
        }
    }
    v
}

pub(crate) fn counts_from(r: &rusqlite::Row) -> rusqlite::Result<ProblemCounts> {
    let mut out = ProblemCounts {
        all: r.get::<_, i64>(0)?.max(0) as u64,
        ..Default::default()
    };
    for (i, (problem, cause)) in count_keys().into_iter().enumerate() {
        let n = r.get::<_, i64>(i + 1)?.max(0) as u64;
        match cause {
            None => match problem {
                Problem::Error => out.error = n,
                Problem::Upstream => out.upstream = n,
                Problem::Unfinished => out.unfinished = n,
                _ => {}
            },
            Some(c) => {
                out.causes.entry(problem.name()).or_default().insert(c, n);
            }
        }
    }
    Ok(out)
}
