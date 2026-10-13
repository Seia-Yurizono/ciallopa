//! Read-only usage API (behind the client key): `/usage/v1/records`, `/usage/v1/summary`.
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use cpa_lite_usage::{now_ms, RecentQuery, Recorder, SummaryQuery};
use serde::Deserialize;
use std::sync::Arc;

type St = Option<Arc<Recorder>>;

pub fn routes(rec: St) -> Router {
    Router::new()
        .route("/usage/v1/records", get(records))
        .route("/usage/v1/summary", get(summary))
        .with_state(rec)
}

fn err(status: StatusCode, msg: &str) -> Response {
    (status, Json(serde_json::json!({"error": {"message": msg}}))).into_response()
}

#[derive(Deserialize)]
struct RecordsQ {
    limit: Option<usize>,
    before: Option<i64>,
    since: Option<i64>,
    account: Option<String>,
    model: Option<String>,
    errors: Option<u8>,
}

async fn records(State(rec): State<St>, Query(q): Query<RecordsQ>) -> Response {
    let Some(rec) = rec else {
        return err(StatusCode::NOT_FOUND, "usage log disabled");
    };
    let q = RecentQuery {
        limit: q.limit.unwrap_or(100),
        before_ms: q.before,
        since_ms: q.since,
        account: q.account,
        model: q.model,
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

async fn summary(State(rec): State<St>, Query(q): Query<SummaryQ>) -> Response {
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
    match rec.summary(SummaryQuery { from_ms: from, to_ms: to }).await {
        Ok(s) => Json(serde_json::json!({"summary": s, "recorder": rec.stats()})).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
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
