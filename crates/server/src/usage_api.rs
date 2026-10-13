//! Read-only usage API (behind the client key): `/usage/v1/records`, `/usage/v1/summary`,
//! `/usage/v1/quota`.
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use cpa_lite_core::AccountStore;
use cpa_lite_usage::{now_ms, Dim, Filters, RecentQuery, Recorder, SummaryQuery};
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
    errors: Option<u8>,
    #[serde(flatten)]
    f: FiltersQ,
}

async fn records(State(St { rec, .. }): State<St>, Query(q): Query<RecordsQ>) -> Response {
    let Some(rec) = rec else {
        return err(StatusCode::NOT_FOUND, "usage log disabled");
    };
    let q = RecentQuery {
        limit: q.limit.unwrap_or(100),
        before_ms: q.before,
        since_ms: q.since,
        filters: q.f.into(),
        errors_only: q.errors.unwrap_or(0) != 0,
    };
    match rec.recent(q).await {
        Ok(rows) => Json(serde_json::json!({"records": rows})).into_response(),
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
    let group_by = match q.group.as_deref() {
        None => SummaryQuery::default().group_by,
        Some(g) => {
            let mut v = vec![];
            for p in g.split(',').filter(|p| !p.trim().is_empty()) {
                match Dim::parse(p) {
                    Some(d) if !v.contains(&d) => v.push(d),
                    Some(_) => {}
                    None => {
                        return err(
                            StatusCode::BAD_REQUEST,
                            "bad group (account,model,key,instance,effort,transport,endpoint)",
                        )
                    }
                }
            }
            v
        }
    };
    let sq = SummaryQuery {
        from_ms: from,
        to_ms: to,
        group_by,
        filters: q.f.into(),
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
