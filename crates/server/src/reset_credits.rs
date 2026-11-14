//! Reset-card listing and opt-in consumption share an account lock.
//! Only the manual handler and the automatic sweep can enter the private consume path.
//! Read-only card/quota polling uses bounded fan-out and live randomized schedules.
use crate::http::{csrf, err, require_account};
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
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

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

#[path = "auto_reset.rs"]
pub mod auto_reset;

const QUOTA_AUTO_KV: &str = "quota_refresh_auto_accounts";
const PERSIST_DEBOUNCE: Duration = Duration::from_secs(2);

#[derive(Default)]
struct Flags {
    on: BTreeSet<String>,
    gen: u64,
    saved: u64,
}

struct SwitchInner {
    key: &'static str,
    flags: Mutex<Flags>,
    saving: Mutex<()>,
    dirty: Notify,
    wake: Arc<Notify>,
}

/// Persisted opt-in account switches. Each instance owns its wakeup and flush state.
#[derive(Clone)]
pub struct Switches(Arc<SwitchInner>);

impl Switches {
    pub(crate) fn new(key: &'static str) -> Self {
        Self(Arc::new(SwitchInner {
            key,
            flags: Default::default(),
            saving: Default::default(),
            dirty: Notify::new(),
            wake: Arc::new(Notify::new()),
        }))
    }
    fn flags(&self) -> std::sync::MutexGuard<'_, Flags> {
        self.0.flags.lock().unwrap_or_else(|e| e.into_inner())
    }
    pub fn is_on(&self, id: &str) -> bool {
        self.flags().on.contains(id)
    }
    pub fn set(&self, id: &str, on: bool) -> bool {
        let mut f = self.flags();
        let changed = if on {
            f.on.insert(id.to_string())
        } else {
            f.on.remove(id)
        };
        if changed {
            f.gen += 1;
            drop(f);
            self.0.dirty.notify_one();
            self.0.wake.notify_one();
        }
        changed
    }
    pub fn ids(&self) -> Vec<String> {
        self.flags().on.iter().cloned().collect()
    }
    pub(crate) fn wake(&self) -> Arc<Notify> {
        self.0.wake.clone()
    }
    pub(crate) fn load(&self, rec: &Recorder) {
        match rec.kv_get(self.0.key) {
            Ok(Some(v)) => self.flags().on = serde_json::from_str(&v).unwrap_or_default(),
            Ok(None) => {}
            Err(e) => tracing::warn!(key = self.0.key, error = %e, "account switches not loaded"),
        }
    }
    /// Also used at shutdown, before the usage database is closed.
    pub fn flush(&self, rec: &Recorder) {
        // Serialize the snapshot and write together: an older debounce write must not
        // overwrite a newer shutdown flush.
        let _save = self.0.saving.lock().unwrap_or_else(|e| e.into_inner());
        let (gen, value) = {
            let f = self.flags();
            if f.gen == f.saved {
                return;
            }
            (
                f.gen,
                serde_json::to_string(&f.on).expect("string set serializes"),
            )
        };
        match rec.kv_set(self.0.key, &value) {
            Ok(()) => self.flags().saved = gen,
            Err(e) => tracing::warn!(key = self.0.key, error = %e, "account switches not saved"),
        }
    }
    pub(crate) fn spawn_persist(&self, rec: Arc<Recorder>) -> tokio::task::JoinHandle<()> {
        let a = self.clone();
        tokio::spawn(async move {
            loop {
                a.0.dirty.notified().await;
                tokio::time::sleep(PERSIST_DEBOUNCE).await;
                let (a, r) = (a.clone(), rec.clone());
                if let Err(e) = tokio::task::spawn_blocking(move || a.flush(&r)).await {
                    tracing::warn!(error = %e, "account switches not saved");
                }
            }
        })
    }
}

/// Shared cache + per-account consume locks.
#[derive(Clone)]
pub struct ResetCredits {
    pub(crate) cache: Arc<Mutex<HashMap<String, Entry>>>,
    locks: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    /// Per-account automatic-use switches (see [`crate::auto_reset`]).
    pub(crate) auto: crate::auto_reset::AutoState,
    /// Per-account 「自动刷新额度」 switches (see [`Switches`]).
    pub(crate) quota_auto: Switches,
    quota_last: Arc<Mutex<HashMap<String, std::time::Instant>>>,
    pub(crate) poll_wake: Arc<Notify>,
}

impl Default for ResetCredits {
    fn default() -> Self {
        Self {
            cache: Default::default(),
            locks: Default::default(),
            auto: Default::default(),
            quota_auto: Switches::new(QUOTA_AUTO_KV),
            quota_last: Default::default(),
            poll_wake: Default::default(),
        }
    }
}

impl ResetCredits {
    pub(crate) fn load(&self, rec: &Recorder) {
        self.auto.switches.load(rec);
        self.quota_auto.load(rec);
    }
    pub fn flush(&self, rec: &Recorder) {
        self.auto.switches.flush(rec);
        self.quota_auto.flush(rec);
    }
    pub(crate) fn wake(&self) {
        self.poll_wake.notify_one();
        self.quota_auto.wake().notify_one();
        self.auto.switches.wake().notify_one();
    }

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
        self.quota_last
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(account);
        self.auto.set(account, false);
        self.quota_auto.set(account, false);
    }
    /// Per-account automatic-use switches.
    pub fn auto(&self) -> &crate::auto_reset::AutoState {
        &self.auto
    }
    /// Per-account 「自动刷新额度」 switches.
    pub fn quota_auto(&self) -> &Switches {
        &self.quota_auto
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

#[derive(Debug)]
pub(crate) enum UpstreamErr {
    Credentials(String),
    Timeout(u64),
    Send(String),
    Body(String),
}
impl UpstreamErr {
    fn kind(&self) -> &'static str {
        match self {
            Self::Credentials(_) => "credentials",
            Self::Timeout(_) => "timeout",
            Self::Send(_) | Self::Body(_) => "network",
        }
    }
}
impl fmt::Display for UpstreamErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Credentials(e) => write!(f, "credentials: {e}"),
            Self::Timeout(s) => write!(f, "send: no answer within {s} s"),
            Self::Send(e) => write!(f, "send: {e}"),
            Self::Body(e) => write!(f, "read body: {e}"),
        }
    }
}

pub(crate) async fn upstream_req(
    st: &AppState,
    account: &str,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
    timeout: Duration,
) -> Result<(u16, String), UpstreamErr> {
    let creds = st
        .accounts
        .credentials(&account.to_string())
        .await
        .map_err(|e| UpstreamErr::Credentials(e.to_string()))?;
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
            UpstreamErr::Timeout(timeout.as_secs())
        } else {
            UpstreamErr::Send(e.to_string())
        }
    })?;
    st.upstream.store_cookies(account, &url, r.headers());
    let s = r.status().as_u16();
    let t = r
        .text()
        .await
        .map_err(|e| UpstreamErr::Body(e.to_string()))?;
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
            e.error = Some(err.to_string());
            e.error_ms = Some(now_ms());
        }
    }
    e.clone()
}

/// Keep at most `limit` futures in flight, without spawning waiting tasks per account.
async fn for_each_bounded<I, F, Fut, T>(items: I, limit: usize, f: F) -> Vec<T>
where
    I: IntoIterator,
    F: Fn(I::Item) -> Fut,
    Fut: Future<Output = T>,
{
    let mut items = items.into_iter();
    let mut active = Vec::new();
    let mut out = Vec::new();
    loop {
        while active.len() < limit.max(1) {
            let Some(item) = items.next() else { break };
            active.push(Box::pin(f(item)));
        }
        if active.is_empty() {
            return out;
        }
        let (index, value) = std::future::poll_fn(|cx| {
            for (index, fut) in active.iter_mut().enumerate() {
                if let std::task::Poll::Ready(value) = fut.as_mut().poll(cx) {
                    return std::task::Poll::Ready((index, value));
                }
            }
            std::task::Poll::Pending
        })
        .await;
        drop(active.swap_remove(index));
        out.push(value);
    }
}

/// Lists cards for every account with a usable token, including disabled accounts.
async fn refresh_all(st: &AppState, rc: &ResetCredits) {
    let relogin = st.accounts.relogin_required().await;
    let ids = st
        .accounts
        .list()
        .await
        .into_iter()
        .filter(|a| !relogin.contains(&a.id))
        .map(|a| a.id);
    for_each_bounded(ids, QUOTA_REFRESH_PARALLEL, |id| async move {
        refresh_account(st, rc, &id).await;
    })
    .await;
}

pub(crate) fn spawn_poller(st: AppState, rc: ResetCredits) -> tokio::task::JoinHandle<()> {
    let range_st = st.clone();
    crate::refresh_sched::spawn_scheduled(
        rc.poll_wake.clone(),
        move || range_st.config.load().ui.reset_credits_refresh_range(),
        || async { vec!["*".to_string()] },
        {
            let st = st.clone();
            move |_| {
                let (st, rc) = (st.clone(), rc.clone());
                async move { refresh_all(&st, &rc).await }
            }
        },
    )
}

pub(crate) fn spawn_quota_poller(st: AppState, rc: ResetCredits) -> tokio::task::JoinHandle<()> {
    let range_st = st.clone();
    let (key_st, key_rc) = (st.clone(), rc.clone());
    crate::refresh_sched::spawn_scheduled(
        rc.quota_auto.wake(),
        move || range_st.config.load().ui.quota_refresh_range(),
        move || {
            let (st, rc) = (key_st.clone(), key_rc.clone());
            async move { quota_opted_in_ids(&st, &rc).await }
        },
        move |ids| {
            let (st, rc) = (st.clone(), rc.clone());
            async move { quota_refresh_ids(&st, &rc, ids).await }
        },
    )
}

/// Opted-in accounts that still exist (disabled included).
async fn quota_opted_in_ids(st: &AppState, rc: &ResetCredits) -> Vec<String> {
    let known: BTreeSet<String> = st.accounts.list().await.into_iter().map(|a| a.id).collect();
    rc.quota_auto
        .ids()
        .into_iter()
        .filter(|id| known.contains(id))
        .collect()
}

/// Refresh every opted-in account that still exists (disabled included; re-login skipped).
/// Integration tests call this directly; the poller refreshes the due subset the same way.
pub async fn quota_refresh_opted_in(st: &AppState, rc: &ResetCredits) {
    let ids = quota_opted_in_ids(st, rc).await;
    quota_refresh_ids(st, rc, ids).await;
}

/// Four at a time; re-login skipped; the 30 s per-account throttle still applies.
async fn quota_refresh_ids(st: &AppState, rc: &ResetCredits, ids: Vec<String>) {
    if ids.is_empty() {
        return;
    }
    let relogin = st.accounts.relogin_required().await;
    for_each_bounded(ids, QUOTA_REFRESH_PARALLEL, |id| {
        let skip = relogin.contains(&id);
        async move {
            if skip {
                tracing::debug!(%id, "quota auto-refresh skipped: re-login required");
            } else {
                quota_refresh_account(st, rc, &id).await;
            }
        }
    })
    .await;
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
        .route(
            "/admin/v1/accounts/{id}/quota-refresh/auto",
            post(set_quota_auto),
        )
        .route("/admin/v1/accounts/{id}/quota-refresh", post(quota_one))
        .route("/admin/v1/accounts/quota-refresh", post(quota_all))
        .with_state(S { st, rc })
}

/// Non-admin callers get counts and the earliest expiry only — not the card list.
fn credits_public(e: &Entry, admin: bool) -> Value {
    if admin {
        return json!(e);
    }
    json!({
        "fetched_ms": e.fetched_ms,
        "available_count": e.available_count,
        "nearest_expires_at": e.nearest_expires_at,
        "error": e.error,
        "error_ms": e.error_ms,
        "usage_available_count": e.usage_available_count,
        "allowed": e.allowed,
        "limit_reached": e.limit_reached,
        "usage_observed_ms": e.usage_observed_ms,
    })
}

async fn list_body(s: &S, admin: bool) -> Value {
    let cfg = s.st.config.load();
    let snap = s.rc.snapshot();
    let accounts = s.st.accounts.list().await;
    let need_history = admin || accounts.iter().any(|a| s.rc.auto.is_on(&a.id));
    let mut attempts = match &s.st.usage {
        Some(r) if need_history => r
            .reset_attempts(crate::auto_reset::HISTORY_ROWS)
            .await
            .unwrap_or_default(),
        _ => vec![],
    };
    let mut grouped: HashMap<&str, Vec<&ciallopa_usage::reset::ConsumeAttempt>> = HashMap::new();
    for row in &attempts {
        grouped.entry(&row.account).or_default().push(row);
    }
    let lead_ms = crate::auto_reset::lead_ms(&cfg);
    let now = now_ms();
    let mut out = vec![];
    for a in accounts {
        let auto = s.rc.auto.is_on(&a.id);
        let next = snap.get(&a.id).filter(|_| auto).and_then(|e| {
            let hist = s.rc.auto.history(
                grouped.get(a.id.as_str()).into_iter().flatten().copied(),
                &a.id,
            );
            crate::auto_reset::next_due(&e.credits, &hist, lead_ms, now)
        });
        let alias = ciallopa_core::a(&a.id);
        let mut row = json!({
            "account": if admin { a.id.clone() } else { alias },
            "disabled": a.disabled,
            "reset_credits": snap.get(&a.id).map(|e| credits_public(e, admin)),
            "auto": auto,
            "auto_next_ms": next,
            "quota_auto": s.rc.quota_auto.is_on(&a.id),
        });
        if admin {
            row["email"] = json!(a.email);
        }
        out.push(row);
    }
    drop(grouped);
    if admin {
        attempts.truncate(20);
    } else {
        attempts.clear();
    }
    let consume_enabled = cfg.ui.allow_reset_consume && s.st.usage.is_some();
    json!({
        "consume_enabled": consume_enabled,
        // UI: only show the consume button when this key is actually allowed to call it.
        "can_consume": consume_enabled && admin,
        "is_admin": admin,
        "refresh_mins": cfg.ui.reset_credits_refresh_mins,
        "refresh_max_mins": cfg.ui.reset_credits_refresh_range().map_or(0, |r| r.1),
        "quota_refresh_mins": cfg.ui.quota_refresh_mins,
        "quota_refresh_max_mins": cfg.ui.quota_refresh_range().map_or(0, |r| r.1),
        "auto_lead_mins": cfg.ui.reset_credits_auto_lead_mins,
        // Automatic use / quota auto-refresh keep switches in the usage DB.
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
    if let Some(r) = csrf(&headers) {
        return r;
    }
    let q = body.map(|b| b.0).unwrap_or_default();
    match q.account {
        Some(a) => {
            if let Err(r) = require_account(&s.st, &a).await {
                return r;
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

#[derive(Clone, Copy)]
enum SwitchKind {
    Auto,
    Quota,
}

async fn set_auto(
    State(s): State<S>,
    Path(account): Path<String>,
    headers: HeaderMap,
    body: Option<Json<AutoReq>>,
) -> Response {
    set_switch(SwitchKind::Auto, s, account, headers, body).await
}
async fn set_quota_auto(
    State(s): State<S>,
    Path(account): Path<String>,
    headers: HeaderMap,
    body: Option<Json<AutoReq>>,
) -> Response {
    set_switch(SwitchKind::Quota, s, account, headers, body).await
}
async fn set_switch(
    kind: SwitchKind,
    s: S,
    account: String,
    headers: HeaderMap,
    body: Option<Json<AutoReq>>,
) -> Response {
    if let Some(r) = crate::http::write_guard(&s.st.config.load(), &headers) {
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
            match kind {
                SwitchKind::Auto => {
                    "automatic use needs [usage] enabled (attempts are recorded there)"
                }
                SwitchKind::Quota => {
                    "automatic quota refresh needs [usage] enabled (switches are stored there)"
                }
            },
        );
    }
    if let Err(r) = crate::http::require_account(&s.st, &account).await {
        return r;
    }
    let (changed, field) = match kind {
        SwitchKind::Auto => {
            let changed = s.rc.auto.set(&account, req.enabled);
            if changed {
                tracing::info!(%account, enabled = req.enabled, "auto reset-card switch changed");
            }
            (changed, "auto")
        }
        SwitchKind::Quota => {
            let changed = s.rc.quota_auto.set(&account, req.enabled);
            if changed {
                tracing::info!(%account, enabled = req.enabled, "quota auto-refresh switch changed");
            }
            (changed, "quota_auto")
        }
    };
    let mut out = json!({"account": account, "changed": changed});
    out[field] = json!(req.enabled);
    Json(out).into_response()
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
) -> Result<(u16, String), UpstreamErr> {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumeCode {
    Reset,
    NothingToReset,
    NoCredit,
    AlreadyRedeemed,
}
impl ConsumeCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reset => "reset",
            Self::NothingToReset => "nothing_to_reset",
            Self::NoCredit => "no_credit",
            Self::AlreadyRedeemed => "already_redeemed",
        }
    }
    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "reset" => Self::Reset,
            "nothing_to_reset" => Self::NothingToReset,
            "no_credit" => Self::NoCredit,
            "already_redeemed" => Self::AlreadyRedeemed,
            _ => return None,
        })
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumeStatus {
    Pending,
    Done,
    Failed,
    Superseded,
}
impl ConsumeStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Superseded => "superseded",
        }
    }
    pub(crate) fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "pending" => Self::Pending,
            "done" => Self::Done,
            "failed" => Self::Failed,
            "superseded" => Self::Superseded,
            _ => return None,
        })
    }
}

struct Classified {
    status: ConsumeStatus,
    code: Option<ConsumeCode>,
    outcome: ConsumeOutcome,
    http: StatusCode,
    body: Value,
}

/// One interpretation of an upstream answer, shared by persistence and both callers.
fn classify(res: Result<(u16, String), UpstreamErr>) -> Classified {
    let mut code = None;
    let (state, http_status, windows_reset, error, http, body) = match res {
        Ok((http @ 200..=299, body)) => {
            let v: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
            code = v
                .get("code")
                .and_then(Value::as_str)
                .and_then(ConsumeCode::parse);
            match code {
                Some(c) => {
                    let w = v.get("windows_reset").and_then(Value::as_i64).unwrap_or(0);
                    (
                        ConsumeStatus::Done,
                        Some(http),
                        Some(w),
                        None,
                        StatusCode::OK,
                        json!({"code": c.as_str(), "windows_reset": w}),
                    )
                }
                None => (
                    ConsumeStatus::Pending,
                    Some(http),
                    None,
                    Some(format!("unexpected body: {}", short(&body))),
                    StatusCode::BAD_GATEWAY,
                    json!({"error": {"message": "unexpected upstream body; retry reuses the same redeem_request_id"}}),
                ),
            }
        }
        Ok((http, body)) if (400..500).contains(&http) && http != 408 && http != 429 => (
            ConsumeStatus::Failed,
            Some(http),
            None,
            Some(short(&body)),
            StatusCode::BAD_GATEWAY,
            json!({"error": {"message": format!("upstream {http}: {}", short(&body))}}),
        ),
        Ok((http, body)) => (
            ConsumeStatus::Pending,
            Some(http),
            None,
            Some(short(&body)),
            StatusCode::BAD_GATEWAY,
            json!({"error": {"message": format!("upstream {http}; outcome unknown, retry reuses the same redeem_request_id")}}),
        ),
        Err(e) => (
            ConsumeStatus::Pending,
            None,
            None,
            Some(e.to_string()),
            StatusCode::BAD_GATEWAY,
            json!({"error": {"message": format!("{e}; outcome unknown, retry reuses the same redeem_request_id")}}),
        ),
    };
    Classified {
        status: state,
        code,
        outcome: ConsumeOutcome {
            status: state.as_str(),
            http_status,
            code: code.map(|c| c.as_str().to_string()),
            windows_reset,
            error,
        },
        http,
        body,
    }
}

struct ConsumeResult {
    status: Option<ConsumeStatus>,
    code: Option<ConsumeCode>,
    http: StatusCode,
    body: Value,
}
impl ConsumeResult {
    fn error(http: StatusCode, msg: String) -> Self {
        Self {
            status: None,
            code: None,
            http,
            body: json!({"error": {"message": msg}}),
        }
    }
}

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
    if let Some(r) = csrf(&headers) {
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
    if let Err(r) = require_account(&s.st, &account).await {
        return r;
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
    let out = consume_locked(
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
    (out.http, Json(out.body)).into_response()
}

/// One consume (or a retry of the pending one) of `account`, shared by the manual button and
/// the automatic use. The caller holds the account's [`ResetCredits::lock_for`] lock (`_held`)
/// for the whole call. Returns the HTTP status and body the manual handler answers with.
#[allow(clippy::too_many_arguments)]
async fn consume_locked(
    st: &AppState,
    rc: &ResetCredits,
    rec: &Recorder,
    account: &str,
    credit: Option<&str>,
    req_rid: Option<&str>,
    label: Option<&str>,
    _held: &tokio::sync::MutexGuard<'_, ()>,
) -> ConsumeResult {
    let fail = ConsumeResult::error;
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
            return ConsumeResult {
                status: Some(ConsumeStatus::Pending),
                code: None,
                http: StatusCode::CONFLICT,
                body: json!({
                    "error": {"type": "pending_consume", "message": "an earlier consume on this account has an unknown outcome; retry it with the same redeem_request_id and credit_id"},
                    "status": "pending",
                    "pending": {
                        "redeem_request_id": p.redeem_request_id,
                        "credit_id": p.credit_id,
                        "attempt": p.id,
                        "started_ms": p.started_ms,
                    }
                }),
            };
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
                        status: ConsumeStatus::Superseded.as_str(),
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
    let Classified {
        status,
        code,
        outcome,
        http,
        body: payload,
    } = classify(res);
    let was_reset = code == Some(ConsumeCode::Reset);
    if let Err(e) = rec.reset_finish(row, outcome).await {
        tracing::warn!(error = %e, "reset consume log write failed");
    }
    // Refresh credits (and fetch the usage windows) after any definitive answer.
    let mut extra = json!({});
    if status == ConsumeStatus::Done && was_reset {
        // The usage windows were just reset upstream: a usage-limit cooldown taken earlier no
        // longer holds. Other cooldowns (auth, 402, 404, 5xx, overload) are kept.
        let cleared = st.router.clear_quota_cooldown(&account.to_string());
        tracing::info!(%account, cleared, "reset credit used: usage-limit cooldown cleared");
        extra["cooldown_cleared"] = json!(cleared);
    }
    if status == ConsumeStatus::Done {
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
                    if let Some(q) = usage_to_quota(&v, now_ms() / 1000) {
                        let by = if auto { "auto" } else { QUOTA_MANUAL };
                        if !rec.observe_quota(account, by, q) {
                            tracing::warn!(%account, "post-consume quota snapshot dropped (usage queue full)");
                        }
                    }
                }
            }
        }
    }
    let mut out = json!({
        "account": account,
        "redeem_request_id": rid,
        "reused_redeem_request_id": reused,
        "attempt": row,
        "status": status.as_str(),
    });
    for src in [payload, extra] {
        if let (Some(o), Value::Object(m)) = (out.as_object_mut(), src) {
            o.extend(m);
        }
    }
    ConsumeResult {
        status: Some(status),
        code,
        http,
        body: out,
    }
}

// ---------------- active quota query (刷新额度) ----------------

/// One account is asked at most this often; a refresh-all asks this many at a time.
pub const QUOTA_REFRESH_MIN: Duration = Duration::from_secs(30);
const QUOTA_REFRESH_PARALLEL: usize = 4;
/// Observer stored with snapshots read on request (`observed_by`).
pub const QUOTA_MANUAL: &str = "manual";

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
    let (cards, _, _) = usage_extras(v);
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
        cards,
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
        let mut last = rc.quota_last.lock().unwrap_or_else(|e| e.into_inner());
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
        Err(e) => fail(e.kind(), None, e.to_string()),
    }
}

async fn quota_one(State(s): State<S>, Path(id): Path<String>, headers: HeaderMap) -> Response {
    if let Some(r) = csrf(&headers) {
        return r;
    }
    if require_account(&s.st, &id).await.is_err() {
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
    if let Some(r) = csrf(&headers) {
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
    let mut results = for_each_bounded(ids, QUOTA_REFRESH_PARALLEL, |id| {
        let skip = relogin.contains(&id);
        let s = &s;
        async move {
            if skip {
                json!({"account": id, "ok": false, "error": {"kind": "token_invalid", "message": "refresh token rejected; log in again"}})
            } else { quota_refresh_account(&s.st, &s.rc, &id).await }
        }
    }).await;
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

    #[tokio::test(start_paused = true)]
    async fn refresh_fanout_is_bounded_and_concurrent() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let running = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let start = tokio::time::Instant::now();
        let mut out = for_each_bounded(0..10, 4, |id| {
            let (running, peak) = (&running, &peak);
            async move {
                let n = running.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(n, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(5)).await;
                running.fetch_sub(1, Ordering::SeqCst);
                id
            }
        })
        .await;
        out.sort();
        assert_eq!(out, (0..10).collect::<Vec<_>>());
        assert_eq!(peak.load(Ordering::SeqCst), 4);
        assert_eq!(start.elapsed(), Duration::from_secs(15));
    }

    #[test]
    fn consume_classification_table() {
        for (name, code) in [
            ("reset", ConsumeCode::Reset),
            ("nothing_to_reset", ConsumeCode::NothingToReset),
            ("no_credit", ConsumeCode::NoCredit),
            ("already_redeemed", ConsumeCode::AlreadyRedeemed),
        ] {
            let c = classify(Ok((
                201,
                json!({"code": name, "windows_reset": 2}).to_string(),
            )));
            assert_eq!(
                (c.status, c.code, c.http),
                (ConsumeStatus::Done, Some(code), StatusCode::OK)
            );
            assert_eq!(c.outcome.status, "done");
            assert_eq!(c.outcome.http_status, Some(201));
            assert_eq!(c.outcome.code.as_deref(), Some(name));
            assert_eq!(c.body, json!({"code": name, "windows_reset": 2}));
        }
        for (reply, expected) in [
            ((200, "garbled"), ConsumeStatus::Pending),
            ((200, r#"{"code":"unknown"}"#), ConsumeStatus::Pending),
            ((400, "bad request"), ConsumeStatus::Failed),
            ((401, "unauthorized"), ConsumeStatus::Failed),
            ((408, "timeout"), ConsumeStatus::Pending),
            ((429, "limited"), ConsumeStatus::Pending),
            ((500, "server error"), ConsumeStatus::Pending),
        ] {
            let c = classify(Ok((reply.0, reply.1.into())));
            assert_eq!(c.status, expected, "{reply:?}");
            assert_eq!(c.http, StatusCode::BAD_GATEWAY);
            assert_eq!(c.outcome.status, expected.as_str());
            assert!(c.code.is_none());
            assert!(c.outcome.error.is_some());
        }
        for (error, kind, text) in [
            (
                UpstreamErr::Credentials("missing".into()),
                "credentials",
                "credentials: missing",
            ),
            (
                UpstreamErr::Timeout(10),
                "timeout",
                "send: no answer within 10 s",
            ),
            (
                UpstreamErr::Send("offline".into()),
                "network",
                "send: offline",
            ),
            (
                UpstreamErr::Body("closed".into()),
                "network",
                "read body: closed",
            ),
        ] {
            assert_eq!(error.kind(), kind);
            assert_eq!(error.to_string(), text);
            let c = classify(Err(error));
            assert_eq!(c.status, ConsumeStatus::Pending);
            assert_eq!(c.outcome.error.as_deref(), Some(text));
            assert!(c.outcome.http_status.is_none());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn switch_debounce_coalesces_and_flush_is_instance_owned() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = ciallopa_core::config::UsageConfig {
            path: dir.path().join("u.db"),
            ..Default::default()
        };
        let rec = Recorder::open_as(&cfg, "test").unwrap();
        let switches = Switches::new("test_switches");
        assert!(switches.set("x", true));
        assert!(!switches.set("x", true));
        let task = switches.spawn_persist(rec.clone());
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(rec.kv_get("test_switches").unwrap(), None);
        switches.set("y", true);
        tokio::time::advance(Duration::from_secs(1)).await;
        // Await the persister's blocking write without advancing a real wall clock.
        for _ in 0..100 {
            let r = rec.clone();
            if tokio::task::spawn_blocking(move || r.kv_get("test_switches"))
                .await
                .unwrap()
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            rec.kv_get("test_switches").unwrap().as_deref(),
            Some(r#"["x","y"]"#)
        );
        switches.set("x", false);
        switches.flush(&rec);
        assert_eq!(
            rec.kv_get("test_switches").unwrap().as_deref(),
            Some(r#"["y"]"#)
        );
        task.abort();
        rec.shutdown();
    }

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
    fn usage_to_quota_ignores_card_expires_at() {
        // openai/codex#60664: a reset-card expires_at must never be read as the
        // window resets_at. Decoys sit on the payload, the window object, and
        // the credits listing; only rate_limit.*.reset_at / reset_after_seconds
        // feed QuotaObs.
        let card_expiry = 1_699_000_000_i64;
        let window_reset = 1_700_000_600_i64;
        let v = json!({
            "plan_type": "Plus",
            "expires_at": card_expiry,
            "rate_limit": {
                "allowed": true,
                "limit_reached": false,
                "expires_at": card_expiry,
                "primary_window": {
                    "used_percent": 80.0,
                    "limit_window_seconds": 18000,
                    "reset_after_seconds": 600,
                    "reset_at": window_reset,
                    "expires_at": card_expiry,
                },
                "secondary_window": {
                    "used_percent": 40.5,
                    "limit_window_seconds": 604800,
                    "reset_after_seconds": 3600,
                    "reset_at": 0,
                    "expires_at": card_expiry,
                },
            },
            "rate_limit_reset_credits": {
                "available_count": 1,
                "credits": [{
                    "status": "available",
                    "expires_at": "2023-11-03T12:26:40Z",
                }],
            },
        });
        let q = usage_to_quota(&v, 1_700_000_000).unwrap();
        assert_eq!(q.p_reset, Some(window_reset));
        assert_ne!(q.p_reset, Some(card_expiry));
        // secondary reset_at is 0 (ignored) so we fall back to now + reset_after_seconds
        assert_eq!(q.s_reset, Some(1_700_003_600));
        assert_ne!(q.s_reset, Some(card_expiry));
        assert_eq!(q.cards, Some(1));

        // A window with only expires_at (no reset_at / reset_after_seconds) must
        // not invent a resets_at from the card date.
        let bare = json!({
            "rate_limit": {
                "primary_window": {
                    "used_percent": 80.0,
                    "limit_window_seconds": 18000,
                    "expires_at": card_expiry,
                }
            },
            "rate_limit_reset_credits": {"available_count": 1},
        });
        let q = usage_to_quota(&bare, 1_700_000_000).unwrap();
        assert_eq!(q.p_reset, None);
        assert_eq!(q.p_pct, Some(80.0));
        assert_eq!(q.cards, Some(1));
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
