//! Read-only usage API (behind the client key): `/usage/v1/records`, `/usage/v1/summary`,
//! `/usage/v1/quota`, `/usage/v1/records/{id}/detail`.
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use ciallopa_core::AccountStore;
use ciallopa_usage::{now_ms, Dim, Filters, RecentQuery, Recorder, SummaryQuery};
use serde::Deserialize;
use std::sync::Arc;

type Rec = Option<Arc<Recorder>>;
#[derive(Clone)]
struct St {
    rec: Rec,
    accounts: Arc<dyn AccountStore>,
}

pub fn routes(rec: Rec, accounts: Arc<dyn AccountStore>) -> Router {
    Router::new()
        .route("/usage/v1/records", get(records))
        .route("/usage/v1/records/{id}/detail", get(detail))
        .route("/usage/v1/summary", get(summary))
        .route("/usage/v1/quota", get(quota))
        .with_state(St { rec, accounts })
}

#[derive(Deserialize, Default)]
struct FiltersQ {
    account: Option<String>,
    model: Option<String>,
    /// Client key **label**.
    key: Option<String>,
    instance: Option<String>,
}
impl From<FiltersQ> for Filters {
    fn from(q: FiltersQ) -> Self {
        Filters {
            account: q.account,
            model: q.model,
            key: q.key,
            instance: q.instance,
        }
    }
}

fn err(status: StatusCode, msg: &str) -> Response {
    (status, Json(serde_json::json!({"error": {"message": msg}}))).into_response()
}

#[derive(Deserialize)]
struct RecordsQ {
    limit: Option<usize>,
    before: Option<i64>,
    since: Option<i64>,
    /// Inclusive time window (ms); aliases of `since` / `before` (= `to` + 1).
    from: Option<i64>,
    to: Option<i64>,
    /// `24h`, `7d`, … ending now (ignored when `from`/`since` is given).
    range: Option<String>,
    offset: Option<usize>,
    /// Served tier: `priority`/`fast`, `flex`, `standard`.
    tier: Option<String>,
    /// `1` ⇒ only requested Fast/Flex that was not served.
    downgraded: Option<u8>,
    errors: Option<u8>,
    #[serde(flatten)]
    f: FiltersQ,
}

async fn records(State(St { rec, .. }): State<St>, Query(q): Query<RecordsQ>) -> Response {
    let Some(rec) = rec else {
        return err(StatusCode::NOT_FOUND, "usage log disabled");
    };
    let since = match (q.since.or(q.from), q.range.as_deref()) {
        (Some(s), _) => Some(s),
        (None, None) => None,
        (None, Some(r)) => match parse_range(r) {
            Some(d) => Some(q.to.unwrap_or_else(now_ms) - d),
            None => return err(StatusCode::BAD_REQUEST, "bad range (use e.g. 30m, 24h, 7d)"),
        },
    };
    let before = match (q.before, q.to) {
        (Some(b), Some(t)) => Some(b.min(t.saturating_add(1))),
        (b, t) => b.or(t.map(|t| t.saturating_add(1))),
    };
    let limit = q.limit.unwrap_or(100).clamp(1, 1000);
    let offset = q.offset.unwrap_or(0);
    let rq = RecentQuery {
        limit,
        before_ms: before,
        since_ms: since,
        filters: q.f.into(),
        errors_only: q.errors.unwrap_or(0) != 0,
        offset,
        tier: q.tier,
        downgraded: q.downgraded.unwrap_or(0) != 0,
    };
    let total = match rec.recent_count(rq.clone()).await {
        Ok(n) => n,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    match rec.recent(rq).await {
        Ok(rows) => {
            let next = offset + rows.len();
            let more = (next as u64) < total;
            Json(serde_json::json!({
                "records": rows,
                "total": total,
                "offset": offset,
                "next_offset": if more { Some(next) } else { None },
            }))
            .into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// Redacted error detail of one failed request (`id` = `request_id`, 16 hex chars).
async fn detail(State(St { rec, .. }): State<St>, Path(id): Path<String>) -> Response {
    let Some(rec) = rec else {
        return err(StatusCode::NOT_FOUND, "usage log disabled");
    };
    if id.len() != 16 || !id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return err(StatusCode::BAD_REQUEST, "invalid request id");
    }
    match rec.detail(&id).await {
        Ok(Some(d)) => Json(d).into_response(),
        Ok(None) => err(StatusCode::NOT_FOUND, "no detail for this request"),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

#[derive(Deserialize)]
struct SummaryQ {
    /// `24h`, `7d`, `30m`, … (ignored when `from` is given). Default 24h.
    range: Option<String>,
    from: Option<i64>,
    to: Option<i64>,
    /// Comma list of account,model,key,instance,effort,transport,endpoint. Default `account,model`.
    group: Option<String>,
    /// `day` / `hour` ⇒ per-day / per-hour buckets (trend chart).
    bucket: Option<String>,
    /// Minutes east of UTC for day boundaries (e.g. 480). String: `flatten` breaks numbers.
    tz: Option<String>,
    #[serde(flatten)]
    f: FiltersQ,
}

/// `90s` / `30m` / `24h` / `7d` → ms.
pub fn parse_range(s: &str) -> Option<i64> {
    let s = s.trim();
    let (n, u) = s.split_at(s.find(|c: char| !c.is_ascii_digit())?);
    let n: i64 = n.parse().ok()?;
    let mul = match u {
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return None,
    };
    n.checked_mul(mul)
}

async fn summary(State(St { rec, .. }): State<St>, Query(q): Query<SummaryQ>) -> Response {
    let Some(rec) = rec else {
        return err(StatusCode::NOT_FOUND, "usage log disabled");
    };
    let to = q.to.unwrap_or_else(now_ms);
    let from = match (q.from, q.range.as_deref()) {
        (Some(f), _) => f,
        (None, r) => match parse_range(r.unwrap_or("24h")) {
            Some(d) => to - d,
            None => return err(StatusCode::BAD_REQUEST, "bad range (use e.g. 30m, 24h, 7d)"),
        },
    };
    if from > to {
        return err(StatusCode::BAD_REQUEST, "from > to");
    }
    let group_by =
        match q.group.as_deref() {
            None => SummaryQuery::default().group_by,
            Some(g) => {
                let mut v = vec![];
                for p in g.split(',').filter(|p| !p.trim().is_empty()) {
                    match Dim::parse(p) {
                        Some(d) if !v.contains(&d) => v.push(d),
                        Some(_) => {}
                        None => return err(
                            StatusCode::BAD_REQUEST,
                            "bad group (account,model,key,instance,effort,transport,endpoint,tier)",
                        ),
                    }
                }
                v
            }
        };
    let bucket_day = match (q.bucket.as_deref(), q.tz.as_deref()) {
        (None, _) => None,
        (Some("day" | "hour"), tz) => match tz.map(|t| t.trim().parse::<i32>()) {
            None => Some(0),
            Some(Ok(t)) if (-840..=840).contains(&t) => Some(t),
            _ => {
                return err(
                    StatusCode::BAD_REQUEST,
                    "bad tz (minutes east of UTC, -840..840)",
                )
            }
        },
        (Some(_), _) => return err(StatusCode::BAD_REQUEST, "bad bucket (`day` or `hour`)"),
    };
    let sq = SummaryQuery {
        from_ms: from,
        to_ms: to,
        group_by,
        filters: q.f.into(),
        bucket_day,
        bucket_hour: q.bucket.as_deref() == Some("hour"),
    };
    match rec.summary(sq).await {
        Ok(s) => Json(serde_json::json!({"summary": s, "recorder": rec.stats()})).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// Per account: latest upstream used %, reset times, capacity estimate, relogin flags.
async fn quota(State(St { rec, accounts }): State<St>) -> Response {
    let Some(rec) = rec else {
        return err(StatusCode::NOT_FOUND, "usage log disabled");
    };
    let obs = match rec.quota().await {
        Ok(o) => o,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    let relogin = accounts.relogin_required().await;
    let mut out: Vec<serde_json::Value> = vec![];
    let known = accounts.list().await;
    for a in &known {
        let q = obs.iter().find(|q| q.account == a.id);
        out.push(serde_json::json!({
            "account": a.id,
            "email": a.email,
            "disabled": a.disabled,
            "needs_relogin": relogin.contains(&a.id),
            "quota": q,
        }));
    }
    // Observed in the log but no longer configured.
    for q in obs
        .iter()
        .filter(|q| !known.iter().any(|a| a.id == q.account))
    {
        out.push(serde_json::json!({"account": q.account, "configured": false, "quota": q}));
    }
    Json(serde_json::json!({
        "accounts": out,
        "note": "used_percent comes from upstream and includes usage by other clients and other instances of the same account; capacity/remaining are estimates from this instance's weighted token deltas between snapshots",
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    #[test]
    fn ranges() {
        assert_eq!(super::parse_range("24h"), Some(86_400_000));
        assert_eq!(super::parse_range("7d"), Some(7 * 86_400_000));
        assert_eq!(super::parse_range("x"), None);
    }
}
