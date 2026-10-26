//! Reset credits (重置卡): read-only listing (on demand + periodic) and the consume action.
//! [`consume_upstream`] has one caller, [`consume_locked`], which runs under the account's
//! consume lock. It is reached from two places only: the manual admin handler below (an
//! explicit client request) and the opt-in automatic use in [`crate::auto_reset`] (accounts
//! whose 「快到期时自动使用」 switch is on, shortly before a card expires).
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
use ciallopa_usage::reset::ConsumeOutcome;
use ciallopa_usage::{now_ms, QuotaObs, Recorder};
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
    /// From the last `GET /wham/usage` (刷新额度): `rate_limit_reset_credits.available_count`.
    /// Display only; the listing above stays authoritative for the cards themselves.
    pub usage_available_count: Option<i64>,
    /// From the same answer: `rate_limit.allowed`.
    pub allowed: Option<bool>,
    /// From the same answer: `rate_limit.limit_reached`.
    pub limit_reached: Option<bool>,
    pub usage_observed_ms: Option<i64>,
}

/// The `/wham/usage` fields the quota snapshot does not keep: the reset-card count,
/// `rate_limit.allowed`, and `rate_limit.limit_reached`.
pub fn usage_extras(v: &Value) -> (Option<i64>, Option<bool>, Option<bool>) {
    let rc = v.get("rate_limit_reset_credits");
    let count = rc
        .and_then(|r| r.get("available_count"))
        .or(rc.filter(|r| r.is_number()))
        .and_then(Value::as_i64)
        .filter(|n| *n >= 0);
    let allowed = v.pointer("/rate_limit/allowed").and_then(Value::as_bool);
    let reached = v
        .pointer("/rate_limit/limit_reached")
        .and_then(Value::as_bool);
    (count, allowed, reached)
}

/// Shared cache + per-account consume locks.
#[derive(Clone, Default)]
pub struct ResetCredits {
    pub(crate) cache: Arc<Mutex<HashMap<String, Entry>>>,
    locks: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    /// Per-account automatic-use switches (see [`crate::auto_reset`]).
    pub(crate) auto: crate::auto_reset::AutoState,
}

impl ResetCredits {
    /// Drop the cached listing and the automatic-use switch of a removed account.
    ///
    /// The consume lock is only dropped when nobody holds or waits for it: a consume still in
    /// flight keeps it, so a re-added account with the same id still waits for that consume.
    pub fn forget(&self, account: &str) {
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(account);
        let mut locks = self.locks.lock().unwrap_or_else(|e| e.into_inner());
        if locks
            .get(account)
            .is_some_and(|l| Arc::strong_count(l) == 1)
        {
            locks.remove(account);
        }
        drop(locks);
        self.auto.set(account, false);
    }
    /// Per-account automatic-use switches.
    pub fn auto(&self) -> &crate::auto_reset::AutoState {
        &self.auto
    }
    /// Keep the display-only `/wham/usage` fields of one account.
    pub fn note_usage(&self, account: &str, v: &Value) {
        let (count, allowed, reached) = usage_extras(v);
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        let e = cache.entry(account.to_string()).or_default();
        e.usage_available_count = count;
        e.allowed = allowed;
        e.limit_reached = reached;
        e.usage_observed_ms = Some(now_ms());
    }
    pub(crate) fn snapshot(&self) -> HashMap<String, Entry> {
        self.cache.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
    /// The per-account consume lock (manual button and automatic use alike).
    pub(crate) fn lock_for(&self, account: &str) -> Arc<tokio::sync::Mutex<()>> {
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

pub(crate) async fn upstream_req(
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
    let r = req.send().await.map_err(|e| {
        if e.is_timeout() {
            format!("send: no answer within {} s", timeout.as_secs())
        } else {
            format!("send: {e}")
        }
    })?;
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

/// Lists cards for every account with a usable token. Disabled accounts are included: they
/// are only kept out of routing, and their cards can still be used from the management page.
async fn refresh_all(st: &AppState, rc: &ResetCredits) {
    let relogin = st.accounts.relogin_required().await;
    for a in st.accounts.list().await {
        if relogin.contains(&a.id) {
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
        .route("/admin/v1/accounts/{id}/reset-credits/auto", post(set_auto))
        .route("/admin/v1/accounts/{id}/quota-refresh", post(quota_one))
        .route("/admin/v1/accounts/quota-refresh", post(quota_all))
        .with_state(S { st, rc })
}

fn err(status: StatusCode, msg: &str) -> Response {
    (status, Json(json!({"error": {"message": msg}}))).into_response()
}

async fn list_body(s: &S, admin: bool) -> Value {
    let cfg = s.st.config.load();
    let snap = s.rc.snapshot();
    let mut attempts = match &s.st.usage {
        Some(r) => r
            .reset_attempts(crate::auto_reset::HISTORY_ROWS)
            .await
            .unwrap_or_default(),
        None => vec![],
    };
    let lead_ms = crate::auto_reset::lead_ms(&cfg);
    let now = now_ms();
    let mut out = vec![];
    for a in s.st.accounts.list().await {
        let auto = s.rc.auto.is_on(&a.id);
        let next = snap.get(&a.id).filter(|_| auto).and_then(|e| {
            let hist = s.rc.auto.history(&attempts, &a.id);
            crate::auto_reset::next_due(&e.credits, &hist, lead_ms, now)
        });
        out.push(json!({
            "account": a.id,
            "email": a.email,
            "disabled": a.disabled,
            "reset_credits": snap.get(&a.id),
            "auto": auto,
            "auto_next_ms": next,
        }));
    }
    attempts.truncate(20);
    let consume_enabled = cfg.ui.allow_reset_consume && s.st.usage.is_some();
    json!({
        "consume_enabled": consume_enabled,
        // UI: only show the consume button when this key is actually allowed to call it.
        "can_consume": consume_enabled && admin,
        "is_admin": admin,
        "refresh_mins": cfg.ui.reset_credits_refresh_mins,
        "auto_lead_mins": cfg.ui.reset_credits_auto_lead_mins,
        // Automatic use keeps its idempotency rows in the usage DB, like the manual action.
        "auto_available": s.st.usage.is_some(),
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
struct AutoReq {
    enabled: bool,
}

/// Per-account 「快到期时自动使用」 switch. A setting-like change: needs
/// `[ui] allow_config_write = true` and the CSRF rules (not `allow_reset_consume`, which only
/// gates the manual button). Kept in the usage DB, written debounced.
async fn set_auto(
    State(s): State<S>,
    Path(account): Path<String>,
    headers: HeaderMap,
    body: Option<Json<AutoReq>>,
) -> Response {
    if !s.st.config.load().ui.allow_config_write {
        return err(
            StatusCode::FORBIDDEN,
            "changes from the management page are disabled; set [ui] allow_config_write = true in the config file",
        );
    }
    if let Some(r) = crate::settings::csrf(&headers) {
        return r;
    }
    let Some(Json(req)) = body else {
        return err(
            StatusCode::BAD_REQUEST,
            "body must be JSON {\"enabled\": true|false}",
        );
    };
    if s.st.usage.is_none() {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "automatic use needs [usage] enabled (attempts are recorded there)",
        );
    }
    if !s.st.accounts.list().await.iter().any(|a| a.id == account) {
        return err(StatusCode::NOT_FOUND, "unknown account");
    }
    let changed = s.rc.auto.set(&account, req.enabled);
    if changed {
        tracing::info!(%account, enabled = req.enabled, "auto reset-card switch changed");
    }
    Json(json!({"account": account, "auto": req.enabled, "changed": changed})).into_response()
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

pub(crate) fn uuid_v4() -> String {
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

/// Upstream consume. Only called from [`consume_locked`].
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
    // CSRF: key in a header (checked by the middleware), JSON body, same-origin when a browser
    // says where the call comes from. Same rules as every other write action.
    if let Some(r) = crate::settings::csrf(&headers) {
        return r;
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
    let g = lock.lock().await;
    let label = key.as_ref().map(|k| k.0.label.to_string());
    let (status, out) = consume_locked(
        &s.st,
        &s.rc,
        &rec,
        &account,
        credit,
        req.redeem_request_id.as_deref(),
        label.as_deref(),
        &g,
    )
    .await;
    (status, Json(out)).into_response()
}

/// One consume (or a retry of the pending one) of `account`, shared by the manual button and
/// the automatic use. The caller holds the account's [`ResetCredits::lock_for`] lock (`_held`)
/// for the whole call. Returns the HTTP status and body the manual handler answers with.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn consume_locked(
    st: &AppState,
    rc: &ResetCredits,
    rec: &Recorder,
    account: &str,
    credit: Option<&str>,
    req_rid: Option<&str>,
    label: Option<&str>,
    _held: &tokio::sync::MutexGuard<'_, ()>,
) -> (StatusCode, Value) {
    let fail = |status: StatusCode, msg: String| (status, json!({"error": {"message": msg}}));
    let pending = match rec.reset_pending(account).await {
        Ok(p) => p,
        Err(e) => return fail(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    // An earlier attempt on this account has an unknown outcome: the only allowed action is
    // retrying that exact attempt (same redeem id + credit), never a second spend.
    if let Some(p) = &pending {
        let same_credit = p.credit_id.as_deref() == credit;
        let same_rid = req_rid.is_none_or(|r| r == p.redeem_request_id);
        if !same_credit || !same_rid {
            return (
                StatusCode::CONFLICT,
                json!({
                    "error": {"type": "pending_consume", "message": "an earlier consume on this account has an unknown outcome; retry it with the same redeem_request_id and credit_id"},
                    "status": "pending",
                    "pending": {
                        "redeem_request_id": p.redeem_request_id,
                        "credit_id": p.credit_id,
                        "attempt": p.id,
                        "started_ms": p.started_ms,
                    }
                }),
            );
        }
    }
    let (rid, reused) = match (req_rid, &pending) {
        (_, Some(p)) => (p.redeem_request_id.clone(), true),
        (Some(r), None) => (r.to_string(), true),
        (None, None) => (uuid_v4(), false),
    };
    let row = match rec
        .reset_begin(account, credit, &rid, label, &st.config.load().instance_id)
        .await
    {
        Ok(r) => r,
        Err(e) => return fail(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
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
    let auto = label == Some(crate::auto_reset::AUTO_LABEL);
    tracing::warn!(%account, redeem_request_id = %rid, reused, auto, "reset-credit consume");
    let res = consume_upstream(st, account, &rid, credit).await;
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
    if st_name == "done" && was_reset {
        // The usage windows were just reset upstream: a usage-limit cooldown taken earlier no
        // longer holds. Other cooldowns (auth, 402, 404, 5xx, overload) are kept.
        let cleared = st.router.clear_quota_cooldown(&account.to_string());
        tracing::info!(%account, cleared, "reset credit used: usage-limit cooldown cleared");
        extra["cooldown_cleared"] = json!(cleared);
    }
    if st_name == "done" {
        let entry = refresh_account(st, rc, account).await;
        extra["reset_credits"] = json!(entry);
        if was_reset {
            if let Ok((200..=299, b)) = upstream_req(
                st,
                account,
                reqwest::Method::GET,
                USAGE_PATH,
                None,
                LIST_TIMEOUT,
            )
            .await
            {
                if let Ok(v) = serde_json::from_str::<Value>(&b) {
                    rc.note_usage(account, &v);
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
    (status, out)
}

// ---------------- active quota query (刷新额度) ----------------

/// One account is asked at most this often; a refresh-all asks this many at a time.
pub const QUOTA_REFRESH_MIN: Duration = Duration::from_secs(30);
const QUOTA_REFRESH_PARALLEL: usize = 4;
/// Observer stored with snapshots read on request (`observed_by`).
pub const QUOTA_MANUAL: &str = "manual";

static QUOTA_LAST: std::sync::LazyLock<Mutex<HashMap<String, std::time::Instant>>> =
    std::sync::LazyLock::new(Default::default);

/// Drop the per-account 刷新额度 throttle. Integration tests only (`#[doc(hidden)]`).
#[doc(hidden)]
pub fn quota_refresh_reset_throttle_for_tests() {
    QUOTA_LAST.lock().unwrap_or_else(|e| e.into_inner()).clear();
}

/// `GET /wham/usage` (codex-rs `backend-client` `get_rate_limits`) → snapshot. Windows map like
/// the `x-codex-{primary,secondary}-*` headers: primary = short window, secondary = weekly.
pub fn usage_to_quota(v: &Value, now_s: i64) -> Option<QuotaObs> {
    let rl = v.get("rate_limit").filter(|r| r.is_object())?;
    let win = |k: &str| {
        let w = rl.get(k).filter(|w| w.is_object())?;
        let pct = w.get("used_percent").and_then(Value::as_f64)?;
        let reset = w
            .get("reset_at")
            .and_then(Value::as_i64)
            .filter(|r| *r > 0)
            .or_else(|| {
                w.get("reset_after_seconds")
                    .and_then(Value::as_i64)
                    .map(|a| now_s + a)
            });
        let mins = w
            .get("limit_window_seconds")
            .and_then(Value::as_i64)
            .filter(|s| *s > 0)
            .map(|s| s / 60);
        Some((pct, reset, mins))
    };
    let (p, s) = (win("primary_window"), win("secondary_window"));
    if p.is_none() && s.is_none() {
        return None;
    }
    Some(QuotaObs {
        plan: v
            .get("plan_type")
            .and_then(Value::as_str)
            .map(|p| p.trim().to_ascii_lowercase())
            .filter(|p| !p.is_empty()),
        p_pct: p.map(|w| w.0),
        p_reset: p.and_then(|w| w.1),
        p_window_min: p.and_then(|w| w.2),
        s_pct: s.map(|w| w.0),
        s_reset: s.and_then(|w| w.1),
        s_window_min: s.and_then(|w| w.2),
    })
}

fn quota_json(q: &QuotaObs) -> Value {
    let w = |pct: Option<f64>, reset: Option<i64>, mins: Option<i64>| {
        pct.map(|p| json!({"used_pct": p, "resets_at": reset, "window_minutes": mins}))
    };
    json!({
        "plan": q.plan,
        "primary": w(q.p_pct, q.p_reset, q.p_window_min),
        "secondary": w(q.s_pct, q.s_reset, q.s_window_min),
    })
}

/// Upstream error text: `detail` / `error.message` of a JSON body, else the start of the body.
fn upstream_msg(body: &str) -> String {
    let v: Option<Value> = serde_json::from_str(body).ok();
    let m = v.as_ref().and_then(|v| {
        v.pointer("/error/message")
            .or_else(|| v.pointer("/detail/message"))
            .or_else(|| v.get("detail"))
            .and_then(Value::as_str)
            .map(str::to_owned)
    });
    short(&m.unwrap_or_else(|| body.to_string()))
        .chars()
        .take(200)
        .collect()
}

/// Ask upstream for one account's usage windows. Read-only and spends no tokens. Stores the
/// snapshot (batched with the usage writer). Returns the per-account result object.
async fn quota_refresh_account(st: &AppState, rc: &ResetCredits, account: &str) -> Value {
    {
        let mut last = QUOTA_LAST.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(t) = last.get(account) {
            let left = QUOTA_REFRESH_MIN.saturating_sub(t.elapsed());
            if !left.is_zero() {
                return json!({"account": account, "ok": false, "error": {
                    "kind": "rate_limited", "retry_after_s": left.as_secs().max(1),
                    "message": format!("asked {}s ago; wait {}s", t.elapsed().as_secs(), left.as_secs().max(1))}});
            }
        }
        last.insert(account.to_string(), std::time::Instant::now());
    }
    let fail = |kind: &str, status: Option<u16>, message: String| {
        tracing::warn!(%account, kind, ?status, %message, "quota refresh failed");
        json!({"account": account, "ok": false, "error": {"kind": kind, "status": status, "message": message}})
    };
    match upstream_req(
        st,
        account,
        reqwest::Method::GET,
        USAGE_PATH,
        None,
        LIST_TIMEOUT,
    )
    .await
    {
        Ok((200..=299, body)) => {
            let v: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
            let Some(q) = usage_to_quota(&v, now_ms() / 1000) else {
                return fail("decode", None, "no rate_limit windows in the answer".into());
            };
            rc.note_usage(account, &v);
            let (cards, allowed, reached) = usage_extras(&v);
            if let Some(rec) = &st.usage {
                if !rec.observe_quota(account, QUOTA_MANUAL, q.clone()) {
                    tracing::warn!(%account, "quota snapshot dropped (usage queue full)");
                }
            }
            // Upstream rate_limit says this account can serve again: drop usage-limit /
            // rate-limit cooldowns (same as a successful reset-credit consume). Other
            // cooldowns stay. Both flags must be present — a missing `allowed` never clears.
            let mut out = json!({"account": account, "ok": true, "quota": quota_json(&q),
                "observed_at_ms": now_ms(), "usage_available_count": cards,
                "allowed": allowed, "limit_reached": reached});
            if allowed == Some(true) && reached == Some(false) {
                let cleared = st.router.clear_quota_cooldown(&account.to_string());
                tracing::info!(%account, cleared, "quota refresh: usage-limit cooldown cleared");
                out["cooldown_cleared"] = json!(cleared);
            }
            out
        }
        Ok((code, body)) => fail("http", Some(code), upstream_msg(&body)),
        Err(e) if e.starts_with("credentials:") => fail("credentials", None, e),
        Err(e) if e.starts_with("send: no answer") => fail("timeout", None, e),
        Err(e) => fail("network", None, e),
    }
}

async fn quota_one(State(s): State<S>, Path(id): Path<String>, headers: HeaderMap) -> Response {
    if let Some(r) = crate::settings::csrf(&headers) {
        return r;
    }
    if !s.st.accounts.list().await.iter().any(|a| a.id == id) {
        return err(StatusCode::NOT_FOUND, "account not found");
    }
    if s.st.accounts.relogin_required().await.contains(&id) {
        return Json(json!({"account": id, "ok": false, "error": {"kind": "token_invalid", "message": "refresh token rejected; log in again"}})).into_response();
    }
    Json(quota_refresh_account(&s.st, &s.rc, &id).await).into_response()
}

/// Every account with a usable token, a few at a time. Accounts asked within the last 30 s
/// are reported as `rate_limited` and counted in `skipped`, not in `failed`.
async fn quota_all(State(s): State<S>, headers: HeaderMap) -> Response {
    if let Some(r) = crate::settings::csrf(&headers) {
        return r;
    }
    let relogin = s.st.accounts.relogin_required().await;
    let ids: Vec<String> =
        s.st.accounts
            .list()
            .await
            .into_iter()
            .map(|a| a.id)
            .collect();
    let sem = Arc::new(tokio::sync::Semaphore::new(QUOTA_REFRESH_PARALLEL));
    let mut set = tokio::task::JoinSet::new();
    for id in ids {
        let (st, rc, sem, skip) = (
            s.st.clone(),
            s.rc.clone(),
            sem.clone(),
            relogin.contains(&id),
        );
        set.spawn(async move {
            if skip {
                return json!({"account": id, "ok": false, "error": {"kind": "token_invalid", "message": "refresh token rejected; log in again"}});
            }
            let _p = sem.acquire_owned().await;
            quota_refresh_account(&st, &rc, &id).await
        });
    }
    let mut results: Vec<Value> = set.join_all().await;
    results.sort_by(|a, b| a["account"].as_str().cmp(&b["account"].as_str()));
    let ok = results.iter().filter(|r| r["ok"] == true).count();
    let skipped = results
        .iter()
        .filter(|r| r["error"]["kind"] == "rate_limited")
        .count();
    Json(json!({"ok": ok, "skipped": skipped, "failed": results.len() - ok - skipped, "results": results}))
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wham_usage_to_snapshot() {
        let v = json!({"plan_type": "Plus", "rate_limit": {"allowed": true, "limit_reached": false,
            "primary_window": {"used_percent": 12, "limit_window_seconds": 18000, "reset_after_seconds": 600, "reset_at": 1700000600},
            "secondary_window": {"used_percent": 40.5, "limit_window_seconds": 604800, "reset_after_seconds": 3600, "reset_at": 0}}});
        let q = usage_to_quota(&v, 1_700_000_000).unwrap();
        assert_eq!(q.plan.as_deref(), Some("plus"));
        assert_eq!(
            (q.p_pct, q.p_reset, q.p_window_min),
            (Some(12.0), Some(1_700_000_600), Some(300))
        );
        assert_eq!(
            (q.s_pct, q.s_reset, q.s_window_min),
            (Some(40.5), Some(1_700_003_600), Some(10080))
        );
        assert!(usage_to_quota(&json!({"plan_type": "free", "rate_limit": null}), 0).is_none());
        assert_eq!(usage_extras(&v), (None, Some(true), Some(false)));
        let w = json!({"rate_limit": {"limit_reached": true}, "rate_limit_reset_credits": {"available_count": 2}});
        assert_eq!(usage_extras(&w), (Some(2), None, Some(true)));
        assert_eq!(
            usage_extras(&json!({"rate_limit_reset_credits": 3})),
            (Some(3), None, None)
        );
        assert_eq!(
            usage_extras(&json!({"rate_limit_reset_credits": {"available_count": -1}})),
            (None, None, None)
        );
        assert_eq!(
            usage_extras(&json!({"rate_limit": {"allowed": false, "limit_reached": false}})),
            (None, Some(false), Some(false))
        );
        assert_eq!(upstream_msg(r#"{"detail":"Unauthorized"}"#), "Unauthorized");
    }

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
