//! Read-only usage API (behind the client key): records, record metadata/detail,
//! summary and quota under `/usage/v1`.
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use ciallopa_core::AccountStore;
use ciallopa_usage::{now_ms, Dim, Filters, Outcome, RecentQuery, Recorder, SummaryQuery};
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
        .route("/usage/v1/records/{id}", get(record))
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
    /// Exclusive record-id cursor (16 hex chars); takes precedence over offset.
    before_id: Option<String>,
    /// completed / failed / cancelled / incomplete / unknown / prewarm.
    outcome: Option<String>,
    /// `1` ⇒ business requests only, matching summary.count.
    exclude_prewarm: Option<u8>,
    /// Served tier: `priority`/`fast`, `flex`, `standard`.
    tier: Option<String>,
    /// `1` ⇒ only requested Fast/Flex that was not served.
    downgraded: Option<u8>,
    errors: Option<u8>,
    /// Comma-separated error sources (`client,local,network,upstream,stream`).
    source: Option<String>,
    /// Exact error code.
    error: Option<String>,
    /// `1` ⇒ only requests that retried or switched accounts.
    retried: Option<u8>,
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
    let before_id = match q.before_id.as_deref().map(parse_id) {
        Some(Some(id)) => Some(id),
        Some(None) => return err(StatusCode::BAD_REQUEST, "invalid before_id"),
        None => None,
    };
    let outcome = match q
        .outcome
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(Outcome::parse)
    {
        Some(Some(outcome)) => Some(outcome),
        Some(None) => {
            return err(
                StatusCode::BAD_REQUEST,
                "bad outcome (completed, failed, cancelled, incomplete, unknown, prewarm)",
            )
        }
        None => None,
    };
    let limit = q.limit.unwrap_or(100).clamp(1, 1000);
    let offset = if before_id.is_some() {
        0
    } else {
        q.offset.unwrap_or(0)
    };
    let rq = RecentQuery {
        limit: limit + 1,
        before_ms: before,
        before_id,
        since_ms: since,
        filters: q.f.into(),
        errors_only: q.errors.unwrap_or(0) != 0,
        offset,
        tier: q.tier,
        downgraded: q.downgraded.unwrap_or(0) != 0,
        sources: q
            .source
            .as_deref()
            .unwrap_or("")
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect(),
        error: q.error.filter(|e| !e.trim().is_empty()),
        retried: q.retried.unwrap_or(0) != 0,
        outcome,
        exclude_prewarm: q.exclude_prewarm.unwrap_or(0) != 0,
    };
    let total = match rec
        .recent_count(RecentQuery {
            before_id: None,
            ..rq.clone()
        })
        .await
    {
        Ok(n) => n,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    match rec.recent(rq).await {
        Ok(mut rows) => {
            let more = rows.len() > limit;
            rows.truncate(limit);
            let next = offset + rows.len();
            let next_before_id = if more {
                rows.last().map(|r| r.request_id.clone())
            } else {
                None
            };
            Json(serde_json::json!({
                "records": rows,
                "total": total,
                "offset": offset,
                "next_offset": if more && before_id.is_none() { Some(next) } else { None },
                "next_before_id": next_before_id,
            }))
            .into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

fn parse_id(id: &str) -> Option<i64> {
    (id.len() == 16 && id.bytes().all(|b| b.is_ascii_hexdigit()))
        .then(|| i64::from_str_radix(id, 16).ok())
        .flatten()
}

/// Metadata is available for successful requests too; a retained error body is optional.
async fn record(State(St { rec, .. }): State<St>, Path(id): Path<String>) -> Response {
    let Some(rec) = rec else {
        return err(StatusCode::NOT_FOUND, "usage log disabled");
    };
    if parse_id(&id).is_none() {
        return err(StatusCode::BAD_REQUEST, "invalid request id");
    }
    match rec.record_by_id(&id).await {
        Ok(Some(record)) => Json(serde_json::json!({"record": record})).into_response(),
        Ok(None) => err(
            StatusCode::NOT_FOUND,
            "record not found or no longer retained",
        ),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// Redacted error detail of one failed request (`id` = `request_id`, 16 hex chars).
async fn detail(State(St { rec, .. }): State<St>, Path(id): Path<String>) -> Response {
    let Some(rec) = rec else {
        return err(StatusCode::NOT_FOUND, "usage log disabled");
    };
    if parse_id(&id).is_none() {
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
    #![allow(clippy::unwrap_used)]
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use ciallopa_core::{Account, AccountId, AuthError, Credentials};
    use ciallopa_usage::{Record, Transport};
    use tower::ServiceExt;

    struct NoAccounts;
    #[async_trait::async_trait]
    impl AccountStore for NoAccounts {
        async fn list(&self) -> Vec<Account> {
            vec![]
        }
        async fn credentials(&self, _: &AccountId) -> Result<Credentials, AuthError> {
            Err(AuthError::Unimplemented)
        }
        async fn force_refresh(&self, _: &AccountId) -> Result<Credentials, AuthError> {
            Err(AuthError::Unimplemented)
        }
        async fn reload(&self) -> Result<(), AuthError> {
            Ok(())
        }
    }

    async fn query(app: &Router, url: &str) -> (StatusCode, serde_json::Value) {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(url).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), 128 * 1024).await.unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    #[tokio::test]
    async fn metadata_and_keyset_share_scope_and_keep_offset_compatibility() {
        let temp = tempfile::tempdir().unwrap();
        let rec = Recorder::open_as(
            &ciallopa_core::config::UsageConfig {
                path: temp.path().join("usage.db"),
                retention_days: 0,
                ..Default::default()
            },
            "demo",
        )
        .unwrap();
        let app = routes(Some(rec.clone()), Arc::new(NoAccounts));
        let base = now_ms();
        let make = |ms| {
            let mut row = Record::new(rec.ids.next_at(ms), Transport::Sse, "responses");
            row.status = 200;
            row.account = Some("a".into());
            row.model = Some("m".into());
            row.key = Some("k".into());
            row
        };
        let a = make(base);
        let b = make(base);
        let late = make(base);
        let d = make(base);
        let aid = format!("{:016x}", a.id);
        let bid = format!("{:016x}", b.id);
        let did = format!("{:016x}", d.id);
        for row in [a, b, d] {
            assert!(rec.record(row));
        }
        let mut prewarm = make(base);
        prewarm.endpoint = "prewarm";
        assert!(rec.record(prewarm));
        // A different account must not enter the shared scope.
        let mut other = make(base);
        other.account = Some("other".into());
        assert!(rec.record(other));
        rec.flush_blocking().unwrap();
        let scope = format!("from={base}&to={base}&account=a&model=m&key=k&instance=demo");
        let (_, all) = query(&app, &format!("/usage/v1/records?{scope}&limit=2")).await;
        assert_eq!(all["total"], 4, "default browsing includes prewarm");
        assert_eq!(all["records"][0]["endpoint"], "prewarm");
        let business = format!("{scope}&exclude_prewarm=1");
        let (status, first) = query(&app, &format!("/usage/v1/records?{business}&limit=2")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first["total"], 3);
        assert_eq!(first["records"][0]["request_id"], did);
        assert_eq!(first["next_before_id"], bid);
        assert_eq!(first["next_offset"], 2);
        let (_, summary) = query(&app, &format!("/usage/v1/summary?{scope}&bucket=hour")).await;
        assert_eq!(summary["summary"]["total"]["count"], first["total"]);
        let (status, metadata) = query(&app, &format!("/usage/v1/records/{did}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(metadata["record"], first["records"][0]);
        assert_eq!(metadata["record"]["has_detail"], false);
        assert_eq!(
            query(&app, &format!("/usage/v1/records/{did}/detail"))
                .await
                .0,
            StatusCode::NOT_FOUND
        );
        assert!(rec.record(late));
        assert!(rec.record(make(base + 1)));
        rec.flush_blocking().unwrap();
        let (_, next) = query(
            &app,
            &format!("/usage/v1/records?{business}&limit=2&before_id={bid}&offset=999"),
        )
        .await;
        assert_eq!(
            next["total"], 4,
            "scope total may change as old requests finish"
        );
        assert_eq!(next["records"].as_array().unwrap().len(), 1);
        assert_eq!(next["records"][0]["request_id"], aid);
        assert!(next["next_before_id"].is_null());
        assert!(next["next_offset"].is_null());
        let (_, legacy) = query(
            &app,
            &format!("/usage/v1/records?{business}&limit=2&offset=2"),
        )
        .await;
        assert_eq!(legacy["records"][0]["request_id"], bid);
        assert_eq!(legacy["records"][1]["request_id"], aid);
        assert_eq!(
            query(&app, "/usage/v1/records?before_id=broken").await.0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            query(&app, "/usage/v1/records?outcome=unknown").await.0,
            StatusCode::OK
        );
        assert_eq!(
            query(&app, "/usage/v1/records?outcome=invalid").await.0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            query(&app, "/usage/v1/records/not-an-id").await.0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            query(&app, "/usage/v1/records/0000000000000000").await.0,
            StatusCode::NOT_FOUND
        );
        rec.shutdown();
    }

    #[test]
    fn ranges() {
        assert_eq!(super::parse_range("24h"), Some(86_400_000));
        assert_eq!(super::parse_range("7d"), Some(7 * 86_400_000));
        assert_eq!(super::parse_range("x"), None);
    }
}
