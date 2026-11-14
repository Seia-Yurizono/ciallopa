use crate::problem::{Outcome, Preds, Problem};
use crate::PREWARM_ENDPOINT;
use crate::{UsageError, SEQ_BITS};
use rusqlite::{params_from_iter, types::Value, Connection, OptionalExtension};
use std::collections::HashMap;

pub(crate) fn label_map(c: &Connection) -> rusqlite::Result<HashMap<i64, String>> {
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

/// Resolve literal, Unicode-lowercased substrings against the label dictionary, not
/// every request row. This matches the UI's `toLowerCase().includes()` semantics
/// (not locale-specific case folding or Unicode normalization). `%`, `_`, `\` and
/// SQL punctuation have no special meaning; user text never enters SQL.
///
/// Scan the distinct labels once for all active fields. Bind each matching-id set as
/// one JSON parameter so broad filters cannot exceed SQLite's bind-variable limit.
/// No UDF, schema change or search-index writes are needed. `col` is internal only.
pub(crate) fn contains_filters(
    c: &Connection,
    sql: &mut String,
    params: &mut Vec<Value>,
    fields: &[(&str, &Option<String>)],
) -> rusqlite::Result<bool> {
    let mut active: Vec<_> = fields
        .iter()
        .filter_map(|(col, v)| {
            v.as_ref()
                .map(|v| (*col, v.to_lowercase(), Vec::<i64>::new()))
        })
        .collect();
    if active.is_empty() {
        return Ok(true);
    }
    let mut st = c.prepare("SELECT id, name FROM labels")?;
    let mut rows = st.query([])?;
    while let Some(row) = rows.next()? {
        let name = row.get::<_, String>(1)?.to_lowercase();
        for (_, needle, ids) in &mut active {
            if name.contains(needle.as_str()) {
                ids.push(row.get::<_, i64>(0)?);
            }
        }
    }
    for (col, _, ids) in active {
        if ids.is_empty() {
            return Ok(false);
        }
        sql.push_str(&format!(" AND {col} IN (SELECT value FROM json_each(?))"));
        params.push(serde_json::json!(ids).to_string().into());
    }
    Ok(true)
}

/// Account is exact; model, key label and instance are literal, case-insensitive
/// substrings. An unknown exact account or a substring with no labels matches nothing.
#[derive(Debug, Clone, Default)]
pub struct Filters {
    pub account: Option<String>,
    pub model: Option<String>,
    pub key: Option<String>,
    pub instance: Option<String>,
}
impl Filters {
    pub(crate) fn sql(&self, c: &Connection) -> rusqlite::Result<Option<(String, Vec<Value>)>> {
        let mut s = String::new();
        let mut p = vec![];
        if let Some(v) = &self.account {
            let Some(id) = label_id(c, v)? else {
                return Ok(None);
            };
            s.push_str(" AND account = ?");
            p.push(id.into());
        }
        if !contains_filters(
            c,
            &mut s,
            &mut p,
            &[
                ("model", &self.model),
                ("key", &self.key),
                ("instance", &self.instance),
            ],
        )? {
            return Ok(None);
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
pub(crate) fn recent_where(
    c: &Connection,
    q: &RecentQuery,
) -> rusqlite::Result<Option<(String, Vec<Value>)>> {
    let Some((fsql, fparams)) = q.filters.sql(c)? else {
        return Ok(None);
    };
    let hi = q.before_ms.map(|b| (b << SEQ_BITS) - 1).unwrap_or(i64::MAX);
    let lo = q.since_ms.map(|s| s << SEQ_BITS).unwrap_or(0);
    let mut s = format!("id BETWEEN ? AND ?{fsql}");
    let mut ps: Vec<Value> = vec![lo.into(), hi.into()];
    ps.extend(fparams);
    if let Some(t) = q.thread.as_deref().map(str::trim).filter(|t| !t.is_empty()) {
        // Served by the partial `requests_thread` index.
        match label_id(c, t)? {
            Some(id) => {
                s.push_str(" AND thread = ?");
                ps.push(id.into());
            }
            None => return Ok(None),
        }
    }
    if let Some(id) = q.before_id {
        s.push_str(" AND id < ?");
        ps.push(id.into());
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
            ps.push(prio.into());
        }
        Some(t) if t == "flex" => {
            s.push_str(" AND tier = ?");
            ps.push(flex.into());
        }
        Some(_) => {
            s.push_str(" AND (tier IS NULL OR tier NOT IN (?, ?))");
            ps.extend([Value::from(prio), Value::from(flex)]);
        }
    }
    if q.downgraded {
        s.push_str(" AND req_tier IN (?, ?) AND (tier IS NULL OR tier != req_tier)");
        ps.extend([Value::from(prio), Value::from(flex)]);
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
        ps.extend(ids.into_iter().map(Value::from));
    }
    if let Some(e) = q.error.as_deref().map(str::trim).filter(|e| !e.is_empty()) {
        s.push_str(" AND error = ?");
        ps.push(label_id(c, e)?.unwrap_or(-1).into());
    }
    if q.retried {
        s.push_str(" AND (retries > 0 OR switches > 0)");
    }
    if q.exclude_prewarm {
        s.push_str(" AND (endpoint IS NULL OR endpoint != ?)");
        ps.push(label_id(c, PREWARM_ENDPOINT)?.unwrap_or(-1).into());
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
