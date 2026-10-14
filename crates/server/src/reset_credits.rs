//! Reset credits (重置卡): read-only listing (on demand + periodic) and the **manual-only**
//! consume action. Nothing in ciallopa ever consumes a credit on its own: the only call site
//! of [`consume_upstream`] is the admin handler below, reached by an explicit client request.
//!
//! Upstream calls mirror codex-rs `backend-client` (`client/rate_limit_resets.rs`): same paths,
//! `User-Agent` + `originator` of the latest learned Codex client (`ciallopa_core::fingerprint`), `Authorization: Bearer`,
//! `ChatGPT-Account-Id`; 5 s timeout for the listing, 10 s for consume.
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use ciallopa_forward::{AppState, ClientKey};
use ciallopa_usage::now_ms;
use ciallopa_usage::reset::ConsumeOutcome;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const LIST_PATH: &str = "/wham/rate-limit-reset-credits";
const CONSUME_PATH: &str = "/wham/rate-limit-reset-credits/consume";
const USAGE_PATH: &str = "/wham/usage";
const LIST_TIMEOUT: Duration = Duration::from_secs(5);
const CONSUME_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Credit {
    pub id: String,
    #[serde(default)]
    pub reset_type: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub granted_at: Option<String>,
    #[serde(default)]
    pub expires_at: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ListResp {
    #[serde(default)]
    credits: Vec<Credit>,
    available_count: i64,
}

/// Cached listing of one account.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Entry {
    pub fetched_ms: Option<i64>,
    pub available_count: Option<i64>,
    pub credits: Vec<Credit>,
    /// Earliest `expires_at` among `status == "available"` credits (RFC 3339 as sent upstream).
    pub nearest_expires_at: Option<String>,
    /// Last fetch error (previous data is kept).
    pub error: Option<String>,
    pub error_ms: Option<i64>,
}

/// Shared cache + per-account consume locks.
#[derive(Clone, Default)]
pub struct ResetCredits {
    cache: Arc<Mutex<HashMap<String, Entry>>>,
    locks: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

impl ResetCredits {
    /// Drop the cached listing of a removed account.
    pub fn forget(&self, account: &str) {
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(account);
        self.locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(account);
    }
    fn snapshot(&self) -> HashMap<String, Entry> {
        self.cache.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
    fn lock_for(&self, account: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(account.to_string())
            .or_default()
            .clone()
    }
}

fn nearest(credits: &[Credit]) -> Option<String> {
    credits
        .iter()
        .filter(|c| c.status.as_deref().is_none_or(|s| s == "available"))
        .filter_map(|c| c.expires_at.clone())
        // RFC 3339 in one zone (upstream uses UTC) sorts lexicographically.
        .min()
}

async fn upstream_req(
    st: &AppState,
    account: &str,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
    timeout: Duration,
) -> Result<(u16, String), String> {
    let creds = st
        .accounts
        .credentials(&account.to_string())
        .await
        .map_err(|e| format!("credentials: {e}"))?;
    // Latest learned Codex client fingerprint (built-in default until one is seen).
    let (ua, originator) = ciallopa_core::fingerprint::headers();
    let url = st.upstream.url(path);
    let mut req = st
        .upstream
        .http_for(account)
        .request(method, &url)
        .timeout(timeout)
        .header(reqwest::header::USER_AGENT, ua)
        .header("originator", originator)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", creds.access_token),
        );
    if let Some(a) = &creds.chatgpt_account_id {
        req = req.header("ChatGPT-Account-Id", a);
    }
    if let Some(b) = body {
        req = req.json(&b);
    }
    // Optional per-account infrastructure cookies; reqwest would append its jar's Cookie last.
    if let Some(c) = st.upstream.cookie_header(account, &url) {
        req = req.header(reqwest::header::COOKIE, c);
    }
    let r = req.send().await.map_err(|e| format!("send: {e}"))?;
    st.upstream.store_cookies(account, &url, r.headers());
    let s = r.status().as_u16();
    let t = r.text().await.map_err(|e| format!("read body: {e}"))?;
    Ok((s, t))
}

fn short(s: &str) -> String {
    let s: String = s.chars().take(300).collect();
    s.replace(|c: char| c.is_control(), " ")
}

/// GET the listing for one account and update the cache (read-only upstream call).
pub async fn refresh_account(st: &AppState, rc: &ResetCredits, account: &str) -> Entry {
    let res = upstream_req(
        st,
        account,
        reqwest::Method::GET,
        LIST_PATH,
        None,
        LIST_TIMEOUT,
    )
    .await;
    let mut cache = rc.cache.lock().unwrap_or_else(|e| e.into_inner());
    let e = cache.entry(account.to_string()).or_default();
    match res {
        Ok((200..=299, body)) => match serde_json::from_str::<ListResp>(&body) {
            Ok(l) => {
                e.fetched_ms = Some(now_ms());
                e.available_count = Some(l.available_count);
                e.nearest_expires_at = nearest(&l.credits);
                e.credits = l.credits;
                e.error = None;
                e.error_ms = None;
            }
            Err(err) => {
                e.error = Some(format!("decode: {err}"));
                e.error_ms = Some(now_ms());
            }
        },
        Ok((s, body)) => {
            e.error = Some(format!("upstream {s}: {}", short(&body)));
            e.error_ms = Some(now_ms());
        }
        Err(err) => {
            e.error = Some(err);
            e.error_ms = Some(now_ms());
        }
    }
    e.clone()
}

async fn refresh_all(st: &AppState, rc: &ResetCredits) {
    let relogin = st.accounts.relogin_required().await;
    for a in st.accounts.list().await {
        if a.disabled || relogin.contains(&a.id) {
            continue;
        }
        refresh_account(st, rc, &a.id).await;
    }
}

/// Periodic read-only refresh. Never consumes. The interval is read from the live config on
/// every pass (`0` = on demand only), so a reload takes effect within 30 s.
pub fn spawn_poller(st: AppState, rc: ResetCredits) {
    if tokio::runtime::Handle::try_current().is_err() {
        return;
    }
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(20)).await;
        let mut last: Option<std::time::Instant> = None;
        loop {
            let mins = st.config.load().ui.reset_credits_refresh_mins;
            let due = mins > 0
                && last.is_none_or(|t| t.elapsed() >= Duration::from_secs(mins.saturating_mul(60)));
            if due {
                last = Some(std::time::Instant::now());
                refresh_all(&st, &rc).await;
            }
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });
}

#[derive(Clone)]
struct S {
    st: AppState,
    rc: ResetCredits,
}

/// Read routes (`/usage/v1/reset-credits*`) and the admin consume route; all mounted behind
/// the client-key middleware.
pub fn routes(st: AppState, rc: ResetCredits) -> Router {
    Router::new()
        .route("/usage/v1/reset-credits", get(list))
        .route("/usage/v1/reset-credits/refresh", post(refresh))
        .route(
            "/admin/v1/accounts/{id}/reset-credits/consume",
            post(consume),
        )
        .with_state(S { st, rc })
}

fn err(status: StatusCode, msg: &str) -> Response {
    (status, Json(json!({"error": {"message": msg}}))).into_response()
}

async fn list_body(s: &S, admin: bool) -> Value {
    let cfg = s.st.config.load();
    let snap = s.rc.snapshot();
    let mut out = vec![];
    for a in s.st.accounts.list().await {
        out.push(json!({
            "account": a.id,
            "email": a.email,
            "disabled": a.disabled,
            "reset_credits": snap.get(&a.id),
        }));
    }
    let attempts = match &s.st.usage {
        Some(r) => r.reset_attempts(20).await.unwrap_or_default(),
        None => vec![],
    };
    let consume_enabled = cfg.ui.allow_reset_consume && s.st.usage.is_some();
    json!({
        "consume_enabled": consume_enabled,
        // UI: only show the consume button when this key is actually allowed to call it.
        "can_consume": consume_enabled && admin,
        "is_admin": admin,
        "refresh_mins": cfg.ui.reset_credits_refresh_mins,
        "accounts": out,
        "attempts": attempts,
    })
}

async fn list(State(s): State<S>, key: Option<axum::Extension<ClientKey>>) -> Response {
    let admin = key.as_ref().is_some_and(|k| k.0.admin);
    Json(list_body(&s, admin).await).into_response()
}

#[derive(Deserialize, Default)]
struct RefreshQ {
    account: Option<String>,
}

/// On-demand re-fetch (all accounts, or `{"account": "..."}`). Hits upstream; admin-only,
/// same CSRF rules as other management POSTs (JSON + no cookies).
async fn refresh(
    State(s): State<S>,
    key: Option<axum::Extension<ClientKey>>,
    headers: HeaderMap,
    body: Option<Json<RefreshQ>>,
) -> Response {
    // Admin is also enforced by middleware for /admin/*, but this route is under /usage/v1 —
    // so check here and apply CSRF.
    if !key.as_ref().is_some_and(|k| k.0.admin) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": {
                "type": "admin_required",
                "message": "this client key is not an admin key; set admin = true on its [[api_keys]] entry in the config file"
            }})),
        )
            .into_response();
    }
    if let Some(r) = crate::settings::csrf(&headers) {
        return r;
    }
    let q = body.map(|b| b.0).unwrap_or_default();
    match q.account {
        Some(a) => {
            if !s.st.accounts.list().await.iter().any(|x| x.id == a) {
                return err(StatusCode::NOT_FOUND, "unknown account");
            }
            refresh_account(&s.st, &s.rc, &a).await;
        }
        None => refresh_all(&s.st, &s.rc).await,
    }
    let admin = true;
    Json(list_body(&s, admin).await).into_response()
}

#[derive(Deserialize)]
struct ConsumeReq {
    /// Must equal the account id in the path (explicit confirmation).
    #[serde(default)]
    confirm: String,
    #[serde(default)]
    credit_id: Option<String>,
    /// Retry a previous attempt with its id (otherwise reused from a pending row or fresh).
    #[serde(default)]
    redeem_request_id: Option<String>,
}

fn uuid_v4() -> String {
    let mut b: [u8; 16] = rand::random();
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

fn valid_rid(s: &str) -> bool {
    !s.is_empty() && s.len() <= 64 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

/// Upstream consume. Only called from [`consume`] (manual admin action).
async fn consume_upstream(
    st: &AppState,
    account: &str,
    rid: &str,
    credit_id: Option<&str>,
) -> Result<(u16, String), String> {
    let mut body = json!({"redeem_request_id": rid});
    if let Some(c) = credit_id {
        body["credit_id"] = json!(c);
    }
    upstream_req(
        st,
        account,
        reqwest::Method::POST,
        CONSUME_PATH,
        Some(body),
        CONSUME_TIMEOUT,
    )
    .await
}

const CODES: [&str; 4] = ["reset", "nothing_to_reset", "no_credit", "already_redeemed"];

async fn consume(
    State(s): State<S>,
    key: Option<axum::Extension<ClientKey>>,
    Path(account): Path<String>,
    headers: HeaderMap,
    body: Option<Json<ConsumeReq>>,
) -> Response {
    if !s.st.config.load().ui.allow_reset_consume {
        return err(
            StatusCode::FORBIDDEN,
            "reset-credit consume is disabled; set [ui] allow_reset_consume = true",
        );
    }
    // CSRF: the key must come in a header (`Authorization: Bearer` / `x-api-key`; the middleware
    // already checked it) and the body must be JSON (forces a CORS preflight from browsers).
    let json_ct = headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.to_ascii_lowercase().starts_with("application/json"));
    if !json_ct || headers.contains_key(http::header::COOKIE) {
        return err(
            StatusCode::BAD_REQUEST,
            "send JSON with Authorization: Bearer (no cookies)",
        );
    }
    let Some(Json(req)) = body else {
        return err(
            StatusCode::BAD_REQUEST,
            "body must be JSON {\"confirm\": \"<account>\"}",
        );
    };
    if req.confirm != account {
        return err(StatusCode::BAD_REQUEST, "confirm must equal the account id");
    }
    let Some(rec) = s.st.usage.clone() else {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "consume needs [usage] enabled (idempotency key is persisted there)",
        );
    };
    if !s.st.accounts.list().await.iter().any(|a| a.id == account) {
        return err(StatusCode::NOT_FOUND, "unknown account");
    }
    if let Some(r) = &req.redeem_request_id {
        if !valid_rid(r) {
            return err(StatusCode::BAD_REQUEST, "invalid redeem_request_id");
        }
    }
    let credit = req.credit_id.as_deref().filter(|c| !c.is_empty());
    let lock = s.rc.lock_for(&account);
    let _g = lock.lock().await;

    let pending = match rec.reset_pending(&account).await {
        Ok(p) => p,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    // An earlier attempt on this account has an unknown outcome: the only allowed action is
    // retrying that exact attempt (same redeem id + credit), never a second spend.
    if let Some(p) = &pending {
        let same_credit = p.credit_id.as_deref() == credit;
        let same_rid = req
            .redeem_request_id
            .as_deref()
            .is_none_or(|r| r == p.redeem_request_id);
        if !same_credit || !same_rid {
            return (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": {"type": "pending_consume", "message": "an earlier consume on this account has an unknown outcome; retry it with the same redeem_request_id and credit_id"},
                    "status": "pending",
                    "pending": {
                        "redeem_request_id": p.redeem_request_id,
                        "credit_id": p.credit_id,
                        "attempt": p.id,
                        "started_ms": p.started_ms,
                    }
                })),
            )
                .into_response();
        }
    }
    let (rid, reused) = match (&req.redeem_request_id, &pending) {
        (_, Some(p)) => (p.redeem_request_id.clone(), true),
        (Some(r), None) => (r.clone(), true),
        (None, None) => (uuid_v4(), false),
    };
    let label = key.as_ref().map(|k| k.0.label.to_string());
    let row = match rec
        .reset_begin(
            &account,
            credit,
            &rid,
            label.as_deref(),
            &s.st.config.load().instance_id,
        )
        .await
    {
        Ok(r) => r,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    // Supersede older pending rows for this id: the newest row now carries the outcome.
    if let Some(p) = &pending {
        if p.redeem_request_id == rid {
            let _ = rec
                .reset_finish(
                    p.id,
                    ConsumeOutcome {
                        status: "superseded",
                        error: Some(format!("retried as attempt {row}")),
                        ..Default::default()
                    },
                )
                .await;
        }
    }
    tracing::warn!(%account, redeem_request_id = %rid, reused, "manual reset-credit consume");
    let res = consume_upstream(&s.st, &account, &rid, credit).await;
    let (outcome, status, payload) = match res {
        Ok((code @ 200..=299, body)) => {
            let v: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
            let c = v.get("code").and_then(Value::as_str).map(str::to_string);
            let w = v.get("windows_reset").and_then(Value::as_i64).unwrap_or(0);
            match c {
                Some(c) if CODES.contains(&c.as_str()) => (
                    ConsumeOutcome {
                        status: "done",
                        http_status: Some(code),
                        code: Some(c.clone()),
                        windows_reset: Some(w),
                        error: None,
                    },
                    StatusCode::OK,
                    json!({"code": c, "windows_reset": w}),
                ),
                _ => (
                    ConsumeOutcome {
                        status: "pending",
                        http_status: Some(code),
                        error: Some(format!("unexpected body: {}", short(&body))),
                        ..Default::default()
                    },
                    StatusCode::BAD_GATEWAY,
                    json!({"error": {"message": "unexpected upstream body; retry reuses the same redeem_request_id"}}),
                ),
            }
        }
        Ok((code, body)) if (400..500).contains(&code) && code != 408 && code != 429 => (
            ConsumeOutcome {
                status: "failed",
                http_status: Some(code),
                error: Some(short(&body)),
                ..Default::default()
            },
            StatusCode::BAD_GATEWAY,
            json!({"error": {"message": format!("upstream {code}: {}", short(&body))}}),
        ),
        Ok((code, body)) => (
            ConsumeOutcome {
                status: "pending",
                http_status: Some(code),
                error: Some(short(&body)),
                ..Default::default()
            },
            StatusCode::BAD_GATEWAY,
            json!({"error": {"message": format!("upstream {code}; outcome unknown, retry reuses the same redeem_request_id")}}),
        ),
        Err(e) => (
            ConsumeOutcome {
                status: "pending",
                error: Some(e.clone()),
                ..Default::default()
            },
            StatusCode::BAD_GATEWAY,
            json!({"error": {"message": format!("{e}; outcome unknown, retry reuses the same redeem_request_id")}}),
        ),
    };
    let st_name = outcome.status;
    let was_reset = outcome.code.as_deref() == Some("reset");
    if let Err(e) = rec.reset_finish(row, outcome).await {
        tracing::warn!(error = %e, "reset consume log write failed");
    }
    // Refresh credits (and fetch the usage windows) after any definitive answer.
    let mut extra = json!({});
    if st_name == "done" {
        let entry = refresh_account(&s.st, &s.rc, &account).await;
        extra["reset_credits"] = json!(entry);
        if was_reset {
            if let Ok((200..=299, b)) = upstream_req(
                &s.st,
                &account,
                reqwest::Method::GET,
                USAGE_PATH,
                None,
                LIST_TIMEOUT,
            )
            .await
            {
                if let Ok(v) = serde_json::from_str::<Value>(&b) {
                    extra["rate_limit"] = v.get("rate_limit").cloned().unwrap_or(Value::Null);
                }
            }
        }
    }
    let mut out = json!({
        "account": account,
        "redeem_request_id": rid,
        "reused_redeem_request_id": reused,
        "attempt": row,
        "status": st_name,
    });
    for src in [payload, extra] {
        if let (Some(o), Value::Object(m)) = (out.as_object_mut(), src) {
            o.extend(m);
        }
    }
    (status, Json(out)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_shape() {
        let u = uuid_v4();
        assert_eq!(u.len(), 36);
        assert_eq!(&u[14..15], "4");
        assert!(valid_rid(&u));
        assert_ne!(u, uuid_v4());
        assert!(!valid_rid("a b"));
    }

    #[test]
    fn nearest_available_only() {
        let c = |id: &str, st: &str, ex: Option<&str>| Credit {
            id: id.into(),
            reset_type: None,
            status: Some(st.into()),
            granted_at: None,
            expires_at: ex.map(str::to_string),
            title: None,
            description: None,
        };
        let v = vec![
            c("a", "redeemed", Some("2026-01-01T00:00:00Z")),
            c("b", "available", Some("2026-12-01T00:00:00Z")),
            c("c", "available", Some("2026-11-01T00:00:00Z")),
            c("d", "available", None),
        ];
        assert_eq!(nearest(&v).as_deref(), Some("2026-11-01T00:00:00Z"));
    }
}
