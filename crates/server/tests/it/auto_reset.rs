//! Automatic reset-card use (「快到期时自动使用」) against a mock upstream: the switch API and
//! its persistence, the window, retries, give-up, restart, and the races listed in
//! `src/auto_reset.rs`. The sweep is driven directly with a shifted clock; card expiries are
//! hours ahead of the real clock, so the background sweep never finds anything due.
#![allow(clippy::unwrap_used)]
use super::support::FixedStore as Store;
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use ciallopa::auto_reset::{sweep_once, Event, SkipReason, AUTO_LABEL, MAX_TRIES, RETRY_MS};
use ciallopa::reset_credits::ResetCredits;
use ciallopa_core::*;
use ciallopa_forward::AppState;
use ciallopa_usage::reset::CreditRef;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

const KEY: &str = "sk-cpa-0123456789abcdef";
const MIN: i64 = 60_000;
const HOUR: i64 = 60 * MIN;

#[derive(Default)]
struct NoRoute(Mutex<Vec<String>>);
impl ciallopa_core::Router for NoRoute {
    fn cooling_until(&self, _: &AccountId) -> Option<std::time::SystemTime> {
        None
    }
    fn model_cooling_until(&self, _: &AccountId, _: &str) -> Option<std::time::SystemTime> {
        None
    }
    fn note_attempt(&self, _: &AccountId) {}
    fn content_ok(&self, _: &AccountId) {}

    fn clear_quota_cooldown(&self, id: &AccountId) -> bool {
        self.0.lock().unwrap().push(id.clone());
        true
    }
    fn select(&self, _: &RouteRequest, a: &[Account]) -> Result<Selection, RouteError> {
        Ok(Selection {
            account: a[0].id.clone(),
            from_binding: false,
        })
    }
    fn report(&self, _: &RouteRequest, _: &AccountId, _: &Outcome) {}
}

type Gate = Arc<Mutex<Option<(Arc<Notify>, Arc<Notify>)>>>;

#[derive(Clone, Copy)]
enum ConsumeFault {
    Disconnect,
    ResetThenDisconnect,
    Timeout,
}

/// Upstream whose listing reflects what was consumed.
#[derive(Clone, Default)]
struct Mock {
    /// (id, status, expires_at ms)
    credits: Arc<Mutex<Vec<(String, String, i64)>>>,
    replies: Arc<Mutex<VecDeque<(u16, Value)>>>,
    list_replies: Arc<Mutex<VecDeque<(u16, Value)>>>,
    faults: Arc<Mutex<VecDeque<ConsumeFault>>>,
    /// Receipts are scoped to the upstream account and request id, independently of listings.
    receipts: Arc<Mutex<HashMap<(String, String), String>>>,
    resets: Arc<Mutex<Vec<Value>>>,
    omit_redeemed: Arc<AtomicBool>,
    consumes: Arc<Mutex<Vec<Value>>>,
    lists: Arc<Mutex<usize>>,
    usage_calls: Arc<Mutex<usize>>,
    list_gate: Gate,
    consume_gate: Gate,
}

async fn pass(g: &Gate) {
    let gate = g.lock().unwrap().take();
    if let Some((entered, release)) = gate {
        entered.notify_one();
        release.notified().await;
    }
}

fn rfc(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

async fn up_list(State(m): State<Mock>) -> Response {
    pass(&m.list_gate).await;
    *m.lists.lock().unwrap() += 1;
    let reply = m.list_replies.lock().unwrap().pop_front();
    if let Some((status, body)) = reply {
        return (StatusCode::from_u16(status).unwrap(), Json(body)).into_response();
    }
    let cs = m.credits.lock().unwrap().clone();
    let avail = cs.iter().filter(|c| c.1 == "available").count();
    Json(json!({
        "available_count": avail,
        "credits": cs.iter().filter(|(_, st, _)| {
            !m.omit_redeemed.load(Ordering::SeqCst) || st != "redeemed"
        }).map(|(id, st, e)| json!({
            "id": id, "reset_type": "rate_limit", "status": st, "expires_at": rfc(*e)
        })).collect::<Vec<_>>()
    }))
    .into_response()
}

fn disconnected() -> Response {
    let body = futures::stream::once(async {
        Err::<Bytes, _>(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "mock upstream lost its response",
        ))
    });
    Response::builder()
        .header("content-type", "application/json")
        .body(Body::from_stream(body))
        .unwrap()
}

fn record_reset(m: &Mock, account: &str, body: &Value) {
    let rid = body["redeem_request_id"].as_str().unwrap().to_string();
    let credit = body["credit_id"].as_str().unwrap().to_string();
    let old = m
        .receipts
        .lock()
        .unwrap()
        .insert((account.into(), rid), credit.clone());
    assert!(old.is_none(), "one side effect per redeem request");
    m.resets.lock().unwrap().push(body.clone());
    for c in m.credits.lock().unwrap().iter_mut() {
        if c.0 == credit {
            c.1 = "redeemed".into();
        }
    }
}

async fn up_consume(State(m): State<Mock>, h: HeaderMap, Json(b): Json<Value>) -> Response {
    m.consumes.lock().unwrap().push(b.clone());
    pass(&m.consume_gate).await;
    let account = h["chatgpt-account-id"].to_str().unwrap();
    let rid = b["redeem_request_id"].as_str().unwrap();
    let receipt = m
        .receipts
        .lock()
        .unwrap()
        .get(&(account.to_string(), rid.to_string()))
        .cloned();
    if let Some(credit) = &receipt {
        assert_eq!(&b["credit_id"], credit, "retry must keep the same card");
    }
    let fault = m.faults.lock().unwrap().pop_front();
    if let Some(fault) = fault {
        if matches!(fault, ConsumeFault::ResetThenDisconnect) && receipt.is_none() {
            record_reset(&m, account, &b);
        }
        return match fault {
            ConsumeFault::Timeout => {
                // A real transport timeout: no HTTP error response substitutes for it.
                let body = futures::stream::pending::<Result<Bytes, std::io::Error>>();
                Response::builder().body(Body::from_stream(body)).unwrap()
            }
            _ => disconnected(),
        };
    }
    if receipt.is_some() {
        return Json(json!({"code": "already_redeemed", "windows_reset": 0})).into_response();
    }
    let used = m
        .credits
        .lock()
        .unwrap()
        .iter()
        .any(|c| b["credit_id"] == c.0 && c.1 == "redeemed");
    if used {
        return Json(json!({"code": "already_redeemed", "windows_reset": 0})).into_response();
    }
    let (s, v) = m
        .replies
        .lock()
        .unwrap()
        .pop_front()
        .unwrap_or((200, json!({"code": "reset", "windows_reset": 2})));
    if v["code"] == "reset" {
        record_reset(&m, account, &b);
    }
    (StatusCode::from_u16(s).unwrap(), Json(v)).into_response()
}

async fn up_usage(State(m): State<Mock>) -> Json<Value> {
    *m.usage_calls.lock().unwrap() += 1;
    Json(
        json!({"rate_limit": {"allowed": true, "limit_reached": false,
        "primary_window": {"used_percent": 0}}}),
    )
}

struct Env {
    base: String,
    mock: Mock,
    store: Arc<Store>,
    router: Arc<NoRoute>,
    st: AppState,
    rc: ResetCredits,
    http: reqwest::Client,
    /// Expiry of the soonest card (`soon`); `later` expires a day after it.
    e1: i64,
    shift: Arc<AtomicI64>,
    _dir: tempfile::TempDir,
}

async fn serve(app: Router) -> std::net::SocketAddr {
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = ln.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(ln, app).await.unwrap() });
    a
}

fn config(up: &str, db: &std::path::Path, write: bool, lead: u64) -> Config {
    Config::parse(&format!(
        "instance_id = \"inst\"\napi_keys = [{{label = \"admin\", key = \"{KEY}\", admin = true}}]\nupstream = \"http://{up}/backend-api\"\n\
         [usage]\npath = \"{}\"\n[ui]\nallow_reset_consume = false\nreset_credits_refresh_mins = 0\nallow_config_write = {write}\nreset_credits_auto_lead_mins = {lead}\n",
        db.display()
    ))
    .unwrap()
}

async fn env_with(write: bool, dir: Option<tempfile::TempDir>) -> Env {
    let mock = Mock::default();
    // whole seconds: the listing carries RFC 3339 seconds
    let e1 = (ciallopa_usage::now_ms() / 1000 + 3 * 3600) * 1000;
    *mock.credits.lock().unwrap() = vec![
        ("later".into(), "available".into(), e1 + 24 * HOUR),
        ("soon".into(), "available".into(), e1),
        ("old".into(), "redeemed".into(), e1 - 48 * HOUR),
    ];
    let up = Router::new()
        .route("/backend-api/wham/rate-limit-reset-credits", get(up_list))
        .route(
            "/backend-api/wham/rate-limit-reset-credits/consume",
            post(up_consume),
        )
        .route("/backend-api/wham/usage", get(up_usage))
        .with_state(mock.clone());
    let up = serve(up).await.to_string();
    let dir = dir.unwrap_or_else(|| tempfile::tempdir().unwrap());
    let cfg = config(&up, &dir.path().join("u.db"), write, 10);
    let rec = ciallopa_usage::Recorder::open_as(&cfg.usage, "inst").unwrap();
    rec.load_or_create_aliaser().unwrap();
    let usage = Some(rec);
    let router = Arc::new(NoRoute::default());
    let store = Arc::new(Store::with_accounts(
        ["a", "b"]
            .into_iter()
            .map(|id| Account {
                id: id.into(),
                email: Some(format!("{id}@example.test")),
                chatgpt_account_id: Some(format!("acct-{id}")),
                plan_type: None,
                priority: 0,
                weight: 1,
                disabled: id == "b",
            })
            .collect(),
    ));
    let st = AppState {
        accounts: store.clone(),
        router: router.clone(),
        upstream: Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap()),
        usage,
        config: Arc::new(cfg.into()),
        egress: Default::default(),
    };
    let s = ciallopa::settings::Settings::new(st.clone(), None, None);
    let rc = s.reset_credits();
    let p = serve(ciallopa::build_app_with(s)).await;
    Env {
        base: format!("http://{p}"),
        mock,
        store,
        router,
        st,
        rc,
        http: reqwest::Client::new(),
        e1,
        shift: Arc::new(AtomicI64::new(0)),
        _dir: dir,
    }
}

async fn env() -> Env {
    env_with(true, None).await
}

impl Env {
    async fn post(&self, path: &str, body: Value) -> (u16, Value) {
        let r = self
            .http
            .post(format!("{}{path}", self.base))
            .bearer_auth(KEY)
            .json(&body)
            .send()
            .await
            .unwrap();
        let s = r.status().as_u16();
        (s, r.json().await.unwrap_or(Value::Null))
    }
    async fn get(&self, path: &str) -> Value {
        self.http
            .get(format!("{}{path}", self.base))
            .bearer_auth(KEY)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }
    async fn switch(&self, acct: &str, on: bool) -> u16 {
        self.post(
            &format!("/admin/v1/accounts/{acct}/reset-credits/auto"),
            json!({"enabled": on}),
        )
        .await
        .0
    }
    /// Move the sweep's clock to `at` (ms, real clock keeps ticking from there).
    fn at(&self, at: i64) {
        self.shift
            .store(at - ciallopa_usage::now_ms(), Ordering::SeqCst);
    }
    async fn sweep(&self) -> Vec<(String, Event)> {
        let shift = self.shift.clone();
        let clock = move || ciallopa_usage::now_ms() + shift.load(Ordering::SeqCst);
        sweep_once(&self.st, &self.rc, &clock).await.0
    }
    fn consumes(&self) -> Vec<Value> {
        self.mock.consumes.lock().unwrap().clone()
    }
    fn lists(&self) -> usize {
        *self.mock.lists.lock().unwrap()
    }
    fn resets(&self) -> Vec<Value> {
        self.mock.resets.lock().unwrap().clone()
    }
    fn usage_calls(&self) -> usize {
        *self.mock.usage_calls.lock().unwrap()
    }
    /// Re-open persisted state, including the production aliaser, without reusing its cache.
    /// The process tests below separately exercise the actual shutdown/startup path.
    fn restart_state(&self) -> (AppState, ResetCredits) {
        let old = self.st.usage.as_ref().unwrap();
        old.kv_set(
            "reset_credits_auto_accounts",
            &serde_json::to_string(&self.rc.auto().ids()).unwrap(),
        )
        .unwrap();
        let rec = ciallopa_usage::Recorder::open_as(&self.st.config.load().usage, "inst").unwrap();
        rec.load_or_create_aliaser().unwrap();
        let st = AppState {
            usage: Some(rec),
            ..self.st.clone()
        };
        let settings = ciallopa::settings::Settings::new(st.clone(), None, None);
        let rc = settings.reset_credits();
        let _app = ciallopa::build_app_with(settings);
        (st, rc)
    }
    async fn sweep_state(&self, st: &AppState, rc: &ResetCredits) -> Vec<(String, Event)> {
        let shift = self.shift.clone();
        let clock = move || ciallopa_usage::now_ms() + shift.load(Ordering::SeqCst);
        sweep_once(st, rc, &clock).await.0
    }
    fn ev(evs: &[(String, Event)], acct: &str) -> Event {
        evs.iter().find(|(a, _)| a == acct).unwrap().1.clone()
    }
    async fn attempts(&self) -> Vec<Value> {
        self.get("/usage/v1/reset-credits").await["attempts"]
            .as_array()
            .unwrap()
            .clone()
    }
}

fn sent(credit: &str, status: &str, code: Option<&str>) -> Event {
    Event::Sent {
        credit: credit.into(),
        status: status.into(),
        code: code.map(str::to_string),
    }
}

fn gate(g: &Gate) -> (Arc<Notify>, Arc<Notify>) {
    let p = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    *g.lock().unwrap() = Some(p.clone());
    p
}

#[tokio::test]
async fn switch_api_is_gated_and_persisted() {
    let ro = env_with(false, None).await;
    assert_eq!(ro.switch("a", true).await, 403);
    let e = env().await;
    assert_eq!(e.switch("zz", true).await, 404);
    let r = e
        .http
        .post(format!("{}/admin/v1/accounts/a/reset-credits/auto", e.base))
        .bearer_auth(KEY)
        .body(r#"{"enabled":true}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400, "CSRF: JSON content type required");
    assert_eq!(e.switch("a", true).await, 200);
    let v = e.get("/usage/v1/reset-credits").await;
    assert_eq!(v["accounts"][0]["auto"], true);
    assert_eq!(v["accounts"][1]["auto"], false);
    assert_eq!(v["auto_lead_mins"], 10);
    assert_eq!(v["auto_available"], true);
    // toggling never spends
    assert!(e.consumes().is_empty());
    // debounced write: several flips, one stored value
    for on in [false, true, false, true] {
        e.switch("b", on).await;
    }
    let rec = e.st.usage.clone().unwrap();
    assert_eq!(
        rec.kv_get("reset_credits_auto_accounts").unwrap(),
        None,
        "not written immediately"
    );
    e.rc.flush(e.st.usage.as_ref().unwrap());
    assert_eq!(
        rec.kv_get("reset_credits_auto_accounts")
            .unwrap()
            .as_deref(),
        Some(r#"["a","b"]"#)
    );
    // a removed account loses its switch
    e.rc.forget("b");
    assert!(!e.rc.auto().is_on("b"));
}

#[tokio::test]
async fn due_card_is_used_once_and_skips_disabled() {
    let e = env().await;
    e.switch("a", true).await;
    e.switch("b", true).await;
    // before the window: nothing sent; the next time is shown
    e.at(e.e1 - 11 * MIN);
    let evs = e.sweep().await;
    assert_eq!(Env::ev(&evs, "a"), Event::Skipped(SkipReason::NotDue));
    assert_eq!(Env::ev(&evs, "b"), Event::Skipped(SkipReason::Disabled));
    let v = e.get("/usage/v1/reset-credits").await;
    assert_eq!(v["accounts"][0]["auto_next_ms"], e.e1 - 10 * MIN);
    assert!(e.consumes().is_empty());
    // inside: the soonest card, through the manual path's log with the auto label,
    // even though allow_reset_consume = false
    e.at(e.e1 - 9 * MIN);
    let evs = e.sweep().await;
    assert_eq!(Env::ev(&evs, "a"), sent("soon", "done", Some("reset")));
    let c = e.consumes();
    assert_eq!(c.len(), 1);
    assert_eq!(c[0]["credit_id"], "soon");
    let at = e.attempts().await;
    assert_eq!(at[0]["key"], AUTO_LABEL);
    assert_eq!(at[0]["code"], "reset");
    // next sweep: `soon` is redeemed, `later` is a day away
    e.at(e.e1 - 8 * MIN);
    let evs = e.sweep().await;
    assert_eq!(Env::ev(&evs, "a"), Event::Skipped(SkipReason::NotDue));
    assert_eq!(e.consumes().len(), 1);
    // the manual button stays off
    let (s, _) = e
        .post(
            "/admin/v1/accounts/a/reset-credits/consume",
            json!({"confirm": "a"}),
        )
        .await;
    assert_eq!(s, 403);
}

#[tokio::test]
async fn normal_sweeps_and_page_reads_do_not_add_upstream_refreshes() {
    let e = env().await;
    e.switch("a", true).await;
    e.at(e.e1 - 30 * MIN);
    e.sweep().await;
    assert_eq!(e.lists(), 1, "discover once, then use the fresh listing");
    for k in 0..30 {
        e.at(e.e1 - 30 * MIN + k * 1000);
        e.sweep().await;
        e.get("/usage/v1/reset-credits").await;
    }
    assert_eq!(e.lists(), 1, "frequent wakeups/page reads are not polls");
    assert_eq!(e.usage_calls(), 0);
    assert!(e.consumes().is_empty());

    e.at(e.e1 - 9 * MIN);
    assert_eq!(
        Env::ev(&e.sweep().await, "a"),
        sent("soon", "done", Some("reset"))
    );
    assert_eq!(e.lists(), 3, "one locked re-list and one result refresh");
    assert_eq!(e.usage_calls(), 1, "one quota query after a real reset");
    for k in 0..30 {
        e.at(e.e1 - 8 * MIN + k * 1000);
        e.sweep().await;
    }
    assert_eq!(e.consumes().len(), 1);
    assert_eq!(e.resets().len(), 1);
    assert_eq!(e.lists(), 3);
    assert_eq!(e.usage_calls(), 1);
}

async fn transport_error_recovery(fault: ConsumeFault) {
    let e = env().await;
    e.switch("a", true).await;
    // Another due card must not bypass this account's pending retry gap.
    e.mock.credits.lock().unwrap()[0].2 = e.e1 + MIN;
    e.mock.faults.lock().unwrap().push_back(fault);
    let t0 = e.e1 - 9 * MIN;
    e.at(t0);
    assert_eq!(
        Env::ev(
            &tokio::time::timeout(Duration::from_secs(15), e.sweep())
                .await
                .unwrap(),
            "a"
        ),
        sent("soon", "pending", None)
    );
    assert!(e.resets().is_empty());
    let lists = e.lists();
    e.at(t0 + RETRY_MS / 2);
    e.sweep().await;
    assert_eq!(
        e.consumes().len(),
        1,
        "no early POST after a transport error"
    );
    assert_eq!(
        e.lists(),
        lists,
        "backoff also avoids unnecessary re-listing"
    );
    assert_eq!(e.usage_calls(), 0);

    e.at(t0 + RETRY_MS + 1000);
    assert_eq!(
        Env::ev(&e.sweep().await, "a"),
        sent("soon", "done", Some("reset"))
    );
    let requests = e.consumes();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0]["redeem_request_id"],
        requests[1]["redeem_request_id"]
    );
    assert_eq!(e.resets().len(), 1);
    let rec = e.st.usage.as_ref().unwrap();
    let rows = rec.reset_attempts(10).await.unwrap();
    assert!(rows.iter().all(|r| r.account == rec.alias_account("a")));
    assert_ne!(rows[0].account, "a", "exercise production account aliases");
}

#[tokio::test]
async fn disconnect_retries_with_one_request_id_and_respects_the_gap() {
    transport_error_recovery(ConsumeFault::Disconnect).await;
}

#[tokio::test]
async fn timeout_retries_with_one_request_id_and_respects_the_gap() {
    transport_error_recovery(ConsumeFault::Timeout).await;
}

#[tokio::test]
async fn lost_success_response_is_reconciled_after_the_card_disappears() {
    let e = env().await;
    e.switch("a", true).await;
    e.mock.omit_redeemed.store(true, Ordering::SeqCst);
    e.mock
        .faults
        .lock()
        .unwrap()
        .push_back(ConsumeFault::ResetThenDisconnect);
    let t0 = e.e1 - 9 * MIN;
    e.at(t0);
    assert_eq!(
        Env::ev(&e.sweep().await, "a"),
        sent("soon", "pending", None)
    );
    assert_eq!(
        e.resets().len(),
        1,
        "the first POST really changed upstream state"
    );
    // A read-only refresh learns that the card is gone before the retry is due.
    e.post("/usage/v1/reset-credits/refresh", json!({"account": "a"}))
        .await;
    e.at(t0 + RETRY_MS + 1000);
    assert_eq!(
        Env::ev(&e.sweep().await, "a"),
        sent("soon", "done", Some("already_redeemed"))
    );
    let requests = e.consumes();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0]["redeem_request_id"],
        requests[1]["redeem_request_id"]
    );
    assert_eq!(
        e.resets().len(),
        1,
        "receipt replay must not reset a second time"
    );
    assert!(e
        .st
        .usage
        .as_ref()
        .unwrap()
        .reset_pending("a")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn restart_reconciles_a_lost_success_without_another_reset() {
    let e = env().await;
    e.switch("a", true).await;
    e.mock.omit_redeemed.store(true, Ordering::SeqCst);
    e.mock
        .faults
        .lock()
        .unwrap()
        .push_back(ConsumeFault::ResetThenDisconnect);
    let t0 = e.e1 - 9 * MIN;
    e.at(t0);
    e.sweep().await;
    let (st, rc) = e.restart_state();
    assert!(rc.auto().is_on("a"));
    e.at(t0 + RETRY_MS + 1000);
    assert_eq!(
        Env::ev(&e.sweep_state(&st, &rc).await, "a"),
        sent("soon", "done", Some("already_redeemed"))
    );
    let requests = e.consumes();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0]["redeem_request_id"],
        requests[1]["redeem_request_id"]
    );
    assert_eq!(e.resets().len(), 1);
    assert_eq!(
        e.usage_calls(),
        1,
        "reconcile quota once, without polling it repeatedly"
    );
    assert_eq!(*e.router.0.lock().unwrap(), ["a"]);
    let lists = e.lists();
    for _ in 0..10 {
        e.sweep_state(&st, &rc).await;
    }
    assert_eq!(e.consumes().len(), 2);
    assert_eq!(e.lists(), lists);
    assert_eq!(e.usage_calls(), 1);
}

#[tokio::test]
async fn receipt_replay_can_lose_its_response_again_without_another_reset() {
    let e = env().await;
    e.switch("a", true).await;
    e.mock.omit_redeemed.store(true, Ordering::SeqCst);
    e.mock
        .faults
        .lock()
        .unwrap()
        .extend([ConsumeFault::ResetThenDisconnect, ConsumeFault::Disconnect]);
    let t0 = e.e1 - 9 * MIN;
    for (k, status, code) in [
        (0, "pending", None),
        (1, "pending", None),
        (2, "done", Some("already_redeemed")),
    ] {
        e.at(t0 + k * (RETRY_MS + 1000));
        assert_eq!(Env::ev(&e.sweep().await, "a"), sent("soon", status, code));
    }
    let requests = e.consumes();
    assert_eq!(requests.len(), 3);
    assert!(requests
        .iter()
        .all(|r| r["redeem_request_id"] == requests[0]["redeem_request_id"]));
    assert_eq!(e.resets().len(), 1);
    assert_eq!(e.usage_calls(), 1);
    assert!(e
        .st
        .usage
        .as_ref()
        .unwrap()
        .reset_pending("a")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn failed_discovery_does_not_immediately_repeat_the_listing_for_pending() {
    let e = env().await;
    e.switch("a", true).await;
    e.st.usage
        .as_ref()
        .unwrap()
        .reset_begin(
            "a",
            Some(CreditRef {
                id: "soon",
                expires_ms: None,
            }),
            "known-rid",
            Some(AUTO_LABEL),
            "inst",
            ciallopa_usage::now_ms(),
        )
        .await
        .unwrap();
    e.mock
        .list_replies
        .lock()
        .unwrap()
        .push_back((503, json!({"detail": "temporary"})));
    e.at(e.e1 - 9 * MIN);
    assert_eq!(
        Env::ev(&e.sweep().await, "a"),
        Event::Skipped(SkipReason::ListFailed)
    );
    assert_eq!(e.lists(), 1, "one failed discovery per sweep");
    assert!(e.consumes().is_empty());
    assert_eq!(e.usage_calls(), 0);
}

#[tokio::test]
async fn failed_locked_listing_never_spends_from_stale_data_then_recovers() {
    let e = env().await;
    e.switch("a", true).await;
    e.post("/usage/v1/reset-credits/refresh", json!({"account": "a"}))
        .await;
    e.mock
        .list_replies
        .lock()
        .unwrap()
        .push_back((503, json!({"detail": "temporary"})));
    let t0 = e.e1 - 9 * MIN;
    e.at(t0);
    assert_eq!(
        Env::ev(&e.sweep().await, "a"),
        Event::Skipped(SkipReason::ListFailed)
    );
    assert!(e.consumes().is_empty());
    assert_eq!(e.usage_calls(), 0);
    e.at(t0 + RETRY_MS + 1000);
    assert_eq!(
        Env::ev(&e.sweep().await, "a"),
        sent("soon", "done", Some("reset"))
    );
    assert_eq!(e.resets().len(), 1);
}

#[tokio::test]
async fn cold_listing_failure_does_not_cause_a_request_storm() {
    let e = env().await;
    e.switch("a", true).await;
    e.mock
        .list_replies
        .lock()
        .unwrap()
        .push_back((503, json!({"detail": "temporary"})));
    e.at(e.e1 - 4 * MIN);
    for _ in 0..30 {
        e.sweep().await;
    }
    assert_eq!(e.lists(), 1, "failed discovery needs a bounded retry delay");
    assert!(e.consumes().is_empty());
    assert_eq!(e.usage_calls(), 0);
}

#[tokio::test]
async fn expired_pending_does_not_block_the_next_expiring_card() {
    let e = env().await;
    e.switch("a", true).await;
    e.mock.credits.lock().unwrap()[0].2 = e.e1 + 5 * MIN;
    e.mock
        .replies
        .lock()
        .unwrap()
        .push_back((503, json!({"detail": "temporary"})));
    e.at(e.e1 - 2 * MIN);
    assert_eq!(
        Env::ev(&e.sweep().await, "a"),
        sent("soon", "pending", None)
    );
    e.at(e.e1 + MIN);
    assert_eq!(
        Env::ev(&e.sweep().await, "a"),
        sent("later", "done", Some("reset"))
    );
    let requests = e.consumes();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["credit_id"], "soon");
    assert_eq!(
        requests[1]["credit_id"], "later",
        "never resend the expired card"
    );
    assert_eq!(e.resets().len(), 1);
}

#[tokio::test]
async fn exhausted_but_unexpired_pending_never_switches_to_another_card() {
    let e = env().await;
    e.switch("a", true).await;
    e.mock.credits.lock().unwrap()[0].2 = e.e1 + MIN;
    e.mock
        .replies
        .lock()
        .unwrap()
        .extend((0..MAX_TRIES).map(|_| (503, json!({"detail": "outcome unknown"}))));
    let t0 = e.e1 - 9 * MIN;
    for k in 0..MAX_TRIES {
        e.at(t0 + i64::from(k) * (RETRY_MS + 1000));
        e.sweep().await;
    }
    e.at(t0 + i64::from(MAX_TRIES) * (RETRY_MS + 1000));
    let lists = e.lists();
    for _ in 0..10 {
        e.sweep().await;
    }
    assert_eq!(
        e.lists(),
        lists,
        "an exhausted live pending must not repeatedly re-list"
    );
    let requests = e.consumes();
    assert_eq!(requests.len(), MAX_TRIES as usize);
    assert!(requests.iter().all(|r| r["credit_id"] == "soon"));
    assert!(
        e.resets().is_empty(),
        "an unknown live result must not cause a second spend"
    );
    assert!(e
        .st
        .usage
        .as_ref()
        .unwrap()
        .reset_pending("a")
        .await
        .unwrap()
        .is_some());
    assert_eq!(e.usage_calls(), 0);
}

#[tokio::test]
async fn expired_pending_is_closed_before_any_other_card_is_automatically_due() {
    let e = env().await;
    let mut cfg = (*e.st.config.load()).clone();
    cfg.ui.allow_reset_consume = true;
    e.st.config.store(cfg);
    e.switch("a", true).await;
    e.mock
        .replies
        .lock()
        .unwrap()
        .push_back((503, json!({"detail": "outcome unknown"})));
    e.at(e.e1 - 2 * MIN);
    e.sweep().await;
    e.at(e.e1 + MIN);
    let rec = e.st.usage.as_ref().unwrap();
    assert!(rec.reset_pending("a").await.unwrap().is_some());
    let body = json!({"confirm": "a", "credit_id": "later"});
    assert_eq!(
        e.post("/admin/v1/accounts/a/reset-credits/consume", body.clone())
            .await
            .0,
        409
    );

    let lists = e.lists();
    e.sweep().await;
    assert!(
        rec.reset_pending("a").await.unwrap().is_none(),
        "expired pending must not wait for the next card's automatic window"
    );
    assert_eq!(e.lists(), lists + 1, "one locked listing verifies expiry");
    assert_eq!(
        e.consumes().len(),
        1,
        "cleanup must not consume the later card"
    );
    let rows = rec.reset_recent_attempts("a").await.unwrap();
    assert_eq!(
        rows[0].status,
        ciallopa_usage::reset::ConsumeStatus::Expired
    );
    assert!(rows[0].error.as_deref().unwrap().contains("unknown"));
    for _ in 0..10 {
        e.sweep().await;
    }
    assert_eq!(
        e.lists(),
        lists + 1,
        "completed cleanup must not repeatedly refresh"
    );
    assert_eq!(e.consumes().len(), 1);
    let (status, result) = e
        .post("/admin/v1/accounts/a/reset-credits/consume", body)
        .await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(result["code"], "reset");
    assert_eq!(e.consumes().len(), 2);
    assert_eq!(e.resets().len(), 1);
}

#[tokio::test]
async fn saved_expiry_unblocks_a_missing_card_after_retries_and_restart() {
    let e = env().await;
    let mut cfg = (*e.st.config.load()).clone();
    cfg.ui.allow_reset_consume = true;
    e.st.config.store(cfg);
    e.switch("a", true).await;
    e.mock
        .replies
        .lock()
        .unwrap()
        .extend((0..MAX_TRIES).map(|_| (503, json!({"detail": "outcome unknown"}))));
    let t0 = e.e1 - 9 * MIN;
    for k in 0..MAX_TRIES {
        e.at(t0 + i64::from(k) * (RETRY_MS + 1000));
        assert_eq!(
            Env::ev(&e.sweep().await, "a"),
            sent("soon", "pending", None)
        );
        // The expiry must remain available to later attempts even when the card vanishes.
        e.mock.credits.lock().unwrap().retain(|c| c.0 != "soon");
    }
    let rec = e.st.usage.as_ref().unwrap();
    let rows = rec.reset_recent_attempts("a").await.unwrap();
    assert_eq!(rows.len(), MAX_TRIES as usize);
    assert!(rows.iter().all(|r| r.credit_expires_ms == Some(e.e1)));
    let rid = rows[0].redeem_request_id.clone();
    assert!(rows.iter().all(|r| r.redeem_request_id == rid));

    let (st, rc) = e.restart_state();
    e.at(e.e1 + MIN);
    e.sweep_state(&st, &rc).await;
    assert!(rec.reset_pending("a").await.unwrap().is_none());
    assert_eq!(
        e.consumes().len(),
        MAX_TRIES as usize,
        "expiry cleanup must not spend another card"
    );
    let rows = rec.reset_recent_attempts("a").await.unwrap();
    assert_eq!(
        rows[0].status,
        ciallopa_usage::reset::ConsumeStatus::Expired
    );
    assert_eq!(rows[0].credit_expires_ms, Some(e.e1));
    let lists = e.lists();
    for _ in 0..10 {
        e.sweep_state(&st, &rc).await;
    }
    assert_eq!(
        e.lists(),
        lists,
        "completed cleanup must not keep refreshing"
    );
    assert_eq!(e.usage_calls(), 0);

    let (status, result) = e
        .post(
            "/admin/v1/accounts/a/reset-credits/consume",
            json!({"confirm": "a", "credit_id": "later"}),
        )
        .await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(result["code"], "reset");
    assert_eq!(e.resets().len(), 1);
}

#[tokio::test]
async fn restart_preserves_the_card_retry_budget() {
    let e = env().await;
    e.switch("a", true).await;
    e.mock
        .replies
        .lock()
        .unwrap()
        .extend((0..10).map(|_| (503, json!({"detail": "outcome unknown"}))));
    let t0 = e.e1 - 9 * MIN;
    for k in 0..2 {
        e.at(t0 + k * (RETRY_MS + 1000));
        e.sweep().await;
    }
    let (st, rc) = e.restart_state();
    for k in 2..8 {
        e.at(t0 + k * (RETRY_MS + 1000));
        e.sweep_state(&st, &rc).await;
    }
    let requests = e.consumes();
    assert_eq!(requests.len(), MAX_TRIES as usize);
    assert!(requests
        .iter()
        .all(|r| r["redeem_request_id"] == requests[0]["redeem_request_id"]));
    assert!(e.resets().is_empty());
    assert_eq!(e.usage_calls(), 0);
}

#[tokio::test]
async fn toggling_auto_does_not_reset_the_retry_interval() {
    let e = env().await;
    e.switch("a", true).await;
    e.mock
        .replies
        .lock()
        .unwrap()
        .push_back((503, json!({"detail": "temporary"})));
    let t0 = e.e1 - 9 * MIN;
    e.at(t0);
    e.sweep().await;
    e.switch("a", false).await;
    e.switch("a", true).await;
    e.at(t0 + RETRY_MS / 2);
    let lists = e.lists();
    e.sweep().await;
    assert_eq!(e.consumes().len(), 1);
    assert_eq!(e.lists(), lists);
    e.at(t0 + RETRY_MS + 1000);
    assert_eq!(
        Env::ev(&e.sweep().await, "a"),
        sent("soon", "done", Some("reset"))
    );
    assert_eq!(e.resets().len(), 1);
}

#[tokio::test]
async fn terminal_card_state_survives_other_accounts_history_and_restart() {
    let e = env().await;
    e.switch("a", true).await;
    e.mock
        .replies
        .lock()
        .unwrap()
        .push_back((200, json!({"code": "nothing_to_reset"})));
    let t0 = e.e1 - 9 * MIN;
    e.at(t0);
    e.sweep().await;
    let rec = e.st.usage.as_ref().unwrap();
    for k in 0..501 {
        let id = rec
            .reset_begin(
                "busy-account",
                Some(CreditRef {
                    id: "other-card",
                    expires_ms: None,
                }),
                &format!("other-{k}"),
                Some(AUTO_LABEL),
                "inst",
                ciallopa_usage::now_ms(),
            )
            .await
            .unwrap();
        rec.reset_finish(
            id,
            ciallopa_usage::reset::ConsumeOutcome {
                status: ciallopa_usage::reset::ConsumeStatus::Done,
                code: Some("reset".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    let (st, rc) = e.restart_state();
    e.at(t0 + RETRY_MS + 1000);
    e.sweep_state(&st, &rc).await;
    assert_eq!(
        e.consumes().len(),
        1,
        "other accounts cannot erase this card's terminal state"
    );
    assert!(e.resets().is_empty());
    assert_eq!(e.usage_calls(), 0);
}

#[tokio::test]
async fn nothing_to_reset_gives_up_silently() {
    let e = env().await;
    e.switch("a", true).await;
    e.mock
        .replies
        .lock()
        .unwrap()
        .push_back((200, json!({"code": "nothing_to_reset", "windows_reset": 0})));
    e.at(e.e1 - 5 * MIN);
    let evs = e.sweep().await;
    assert_eq!(
        Env::ev(&evs, "a"),
        sent("soon", "done", Some("nothing_to_reset"))
    );
    for k in 1..4 {
        e.at(e.e1 - 5 * MIN + k * (RETRY_MS + 1000));
        let evs = e.sweep().await;
        assert_eq!(Env::ev(&evs, "a"), Event::Skipped(SkipReason::NotDue));
    }
    assert_eq!(e.consumes().len(), 1);
    let v = e.get("/usage/v1/reset-credits").await;
    assert_eq!(
        v["accounts"][0]["auto_next_ms"],
        e.e1 + 24 * HOUR - 10 * MIN,
        "only the later card is left"
    );
}

#[tokio::test]
async fn retries_are_capped_spaced_and_end_at_expiry() {
    let e = env().await;
    e.switch("a", true).await;
    for _ in 0..10 {
        e.mock
            .replies
            .lock()
            .unwrap()
            .push_back((503, json!({"detail": "busy"})));
    }
    let t0 = e.e1 - 10 * MIN;
    e.at(t0);
    assert_eq!(
        Env::ev(&e.sweep().await, "a"),
        sent("soon", "pending", None)
    );
    // too soon for a retry
    e.at(t0 + RETRY_MS / 2);
    assert_eq!(
        Env::ev(&e.sweep().await, "a"),
        Event::Skipped(SkipReason::NotDue)
    );
    for k in 1..8 {
        e.at(t0 + k * (RETRY_MS + 2000));
        e.sweep().await;
    }
    let c = e.consumes();
    assert_eq!(c.len(), MAX_TRIES as usize);
    // unknown outcomes are retried with the same redeem id
    assert!(c
        .iter()
        .all(|x| x["redeem_request_id"] == c[0]["redeem_request_id"]));

    // definitive failures (4xx): new id each time; nothing at or after the expiry
    let e = env().await;
    e.switch("a", true).await;
    for _ in 0..10 {
        e.mock
            .replies
            .lock()
            .unwrap()
            .push_back((400, json!({"detail": "bad"})));
    }
    let t0 = e.e1 - 2 * MIN - 30_000;
    for k in 0..6 {
        e.at(t0 + k * (RETRY_MS + 1000));
        e.sweep().await;
    }
    let c = e.consumes();
    assert_eq!(c.len(), 3, "t0, +61 s, +122 s; then the card has expired");
    assert_ne!(c[0]["redeem_request_id"], c[1]["redeem_request_id"]);
}

#[tokio::test]
async fn pending_is_retried_with_the_same_id_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let e = env_with(true, Some(dir)).await;
    e.switch("a", true).await;
    e.mock
        .replies
        .lock()
        .unwrap()
        .push_back((502, json!({"detail": "gateway"})));
    e.at(e.e1 - 9 * MIN);
    assert_eq!(
        Env::ev(&e.sweep().await, "a"),
        sent("soon", "pending", None)
    );
    let rid = e.consumes()[0]["redeem_request_id"].clone();
    // restart: fresh process state on the same usage DB (switches come back from it)
    e.rc.flush(e.st.usage.as_ref().unwrap());
    let cfg = config("127.0.0.1:1", &path.join("u.db"), true, 10);
    let rec = ciallopa_usage::Recorder::open_as(&cfg.usage, "inst").unwrap();
    rec.load_or_create_aliaser().unwrap();
    assert_eq!(
        rec.kv_get("reset_credits_auto_accounts")
            .unwrap()
            .as_deref(),
        Some(r#"["a"]"#)
    );
    let st = AppState {
        usage: Some(rec),
        ..e.st.clone()
    };
    let s = ciallopa::settings::Settings::new(st.clone(), None, None);
    let rc = s.reset_credits();
    let _app = ciallopa::build_app_with(s);
    assert!(rc.auto().is_on("a"), "loaded at startup");
    e.at(e.e1 - 7 * MIN);
    let shift = e.shift.clone();
    let clock = move || ciallopa_usage::now_ms() + shift.load(Ordering::SeqCst);
    let (evs, _) = sweep_once(&st, &rc, &clock).await;
    assert_eq!(Env::ev(&evs, "a"), sent("soon", "done", Some("reset")));
    let c = e.consumes();
    assert_eq!(c.len(), 2);
    assert_eq!(c[1]["redeem_request_id"], rid);
    let at = e.attempts().await;
    assert_eq!(at[0]["status"], "done");
    assert_eq!(at[1]["status"], "superseded");
}

#[tokio::test]
async fn manual_and_auto_share_the_lock() {
    let e = env().await;
    // manual button on for this test
    let mut cfg = (*e.st.config.load()).clone();
    cfg.ui.allow_reset_consume = true;
    e.st.config.store(cfg);
    e.switch("a", true).await;
    e.at(e.e1 - 9 * MIN);
    // a manual use of `soon` is in flight (held at the upstream)
    let (entered, release) = gate(&e.mock.consume_gate);
    let base = e.base.clone();
    let manual = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("{base}/admin/v1/accounts/a/reset-credits/consume"))
            .bearer_auth(KEY)
            .json(&json!({"confirm": "a", "credit_id": "soon"}))
            .send()
            .await
            .unwrap()
            .status()
            .as_u16()
    });
    entered.notified().await;
    let shift = e.shift.clone();
    let (st, rc) = (e.st.clone(), e.rc.clone());
    let auto = tokio::spawn(async move {
        let clock = move || ciallopa_usage::now_ms() + shift.load(Ordering::SeqCst);
        sweep_once(&st, &rc, &clock).await.0
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(e.consumes().len(), 1, "auto waits for the lock");
    release.notify_one();
    assert_eq!(manual.await.unwrap(), 200);
    let evs = auto.await.unwrap();
    // the sweep saw the manual attempt in flight and left the account alone
    assert_eq!(
        Env::ev(&evs, "a"),
        Event::Skipped(SkipReason::ManualPending)
    );
    assert_eq!(e.consumes().len(), 1);

    // the other order: an automatic use holds the lock (re-listing) when the button is pressed
    let e = Arc::new(env().await);
    let mut cfg = (*e.st.config.load()).clone();
    cfg.ui.allow_reset_consume = true;
    e.st.config.store(cfg);
    e.switch("a", true).await;
    e.post("/usage/v1/reset-credits/refresh", json!({})).await;
    e.at(e.e1 - 9 * MIN);
    let (entered, release) = gate(&e.mock.list_gate);
    let e2 = e.clone();
    let auto = tokio::spawn(async move { e2.sweep().await });
    entered.notified().await;
    let base = e.base.clone();
    let manual = tokio::spawn(async move {
        let r = reqwest::Client::new()
            .post(format!("{base}/admin/v1/accounts/a/reset-credits/consume"))
            .bearer_auth(KEY)
            .json(&json!({"confirm": "a", "credit_id": "soon"}))
            .send()
            .await
            .unwrap();
        r.json::<Value>().await.unwrap()
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(e.consumes().is_empty(), "the button waits for the lock");
    release.notify_one();
    assert_eq!(
        Env::ev(&auto.await.unwrap(), "a"),
        sent("soon", "done", Some("reset"))
    );
    let m = manual.await.unwrap();
    assert_eq!(m["code"], "already_redeemed", "{m}");
    assert_eq!(e.consumes().len(), 2);

    // an unresolved manual attempt is left to a person
    let e = env().await;
    let mut cfg = (*e.st.config.load()).clone();
    cfg.ui.allow_reset_consume = true;
    e.st.config.store(cfg);
    e.switch("a", true).await;
    e.mock
        .replies
        .lock()
        .unwrap()
        .push_back((502, json!({"detail": "gateway"})));
    let (s, v) = e
        .post(
            "/admin/v1/accounts/a/reset-credits/consume",
            json!({"confirm": "a", "credit_id": "later"}),
        )
        .await;
    assert_eq!((s, v["status"].as_str()), (502, Some("pending")));
    e.at(e.e1 - 9 * MIN);
    assert_eq!(
        Env::ev(&e.sweep().await, "a"),
        Event::Skipped(SkipReason::ManualPending)
    );
    assert_eq!(e.consumes().len(), 1);
}

#[tokio::test]
async fn relist_under_lock_decides() {
    let e = env().await;
    e.switch("a", true).await;
    // cache (from the poller / a refresh) says `soon` is available
    e.post("/usage/v1/reset-credits/refresh", json!({})).await;
    // ... then it is used elsewhere before the sweep acts
    e.mock.credits.lock().unwrap()[1].1 = "redeemed".into();
    e.at(e.e1 - 9 * MIN);
    let n = e.lists();
    let evs = e.sweep().await;
    assert_eq!(Env::ev(&evs, "a"), Event::Skipped(SkipReason::NotDue));
    assert_eq!(e.lists(), n + 1, "re-listed under the lock");
    assert!(e.consumes().is_empty());
}

#[tokio::test]
async fn checks_after_relist() {
    type Change = Box<dyn Fn(&Env) + Send + Sync>;
    let cases: Vec<(&str, Change, Event)> = vec![
        (
            "switch off",
            Box::new(|e: &Env| {
                e.rc.auto().set("a", false);
            }),
            Event::Skipped(SkipReason::SwitchedOff),
        ),
        (
            "deleted",
            Box::new(|e: &Env| e.store.update(|accounts| accounts.retain(|a| a.id != "a"))),
            Event::Skipped(SkipReason::AccountGone),
        ),
        (
            "disabled",
            Box::new(|e: &Env| e.store.update(|accounts| accounts[0].disabled = true)),
            Event::Skipped(SkipReason::Disabled),
        ),
        (
            "relogin",
            Box::new(|e: &Env| {
                e.store.relogin.lock().unwrap().insert("a".into());
            }),
            Event::Skipped(SkipReason::Relogin),
        ),
        (
            "lead shortened (hot reload)",
            Box::new(|e: &Env| {
                let mut cfg = (*e.st.config.load()).clone();
                cfg.ui.reset_credits_auto_lead_mins = 5;
                e.st.config.store(cfg);
            }),
            Event::Skipped(SkipReason::NotDue),
        ),
    ];
    for (name, change, want) in cases {
        let e = Arc::new(env().await);
        e.switch("a", true).await;
        e.post("/usage/v1/reset-credits/refresh", json!({})).await;
        e.at(e.e1 - 8 * MIN);
        let (entered, release) = gate(&e.mock.list_gate);
        let e2 = e.clone();
        let t = tokio::spawn(async move { e2.sweep().await });
        entered.notified().await;
        change(&e);
        release.notify_one();
        let evs = t.await.unwrap();
        assert_eq!(Env::ev(&evs, "a"), want, "{name}");
        assert!(e.consumes().is_empty(), "{name}");
    }
}

#[cfg(unix)]
mod process {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, BufReader, Lines};
    use tokio::process::{Child, ChildStderr, Command};

    type Logs = Lines<BufReader<ChildStderr>>;

    async fn wait_log(logs: &mut Logs, text: &str, wait_secs: u64) {
        tokio::time::timeout(Duration::from_secs(wait_secs), async {
            while let Some(line) = logs.next_line().await.unwrap() {
                if line.contains(text) {
                    return;
                }
            }
            panic!("server exited before logging {text}");
        })
        .await
        .expect("bounded wait for server lifecycle event");
    }

    async fn start(config: &Path) -> (Child, Logs) {
        let mut child = Command::new(env!("CARGO_BIN_EXE_ciallopa"))
            .arg("-c")
            .arg(config)
            .arg("serve")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut logs = BufReader::new(child.stderr.take().unwrap()).lines();
        wait_log(&mut logs, "ciallopa listening", 10).await;
        (child, logs)
    }

    async fn stop(mut child: Child) {
        let pid = child.id().unwrap() as libc::pid_t;
        // Only the child created above is signalled; kill_on_drop handles assertion failures.
        assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
        let status = tokio::time::timeout(Duration::from_secs(15), child.wait())
            .await
            .expect("bounded graceful shutdown")
            .unwrap();
        assert!(status.success());
    }

    fn config_file(e: &Env) -> (PathBuf, std::net::SocketAddr) {
        let auth = e._dir.path().join("process-auth");
        std::fs::create_dir(&auth).unwrap();
        std::fs::write(
            auth.join("a.json"),
            r#"{"type":"codex","email":"a@example.test","access_token":"fake-process-token","account_id":"acct-a","expired":"2099-01-01T00:00:00Z"}"#,
        )
        .unwrap();
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = reservation.local_addr().unwrap();
        let path = e._dir.path().join("process.toml");
        std::fs::write(
            &path,
            format!(
                r#"
listen = "{addr}"
instance_id = "process-test"
api_keys = [{{label = "admin", key = "{KEY}", admin = true}}]
upstream = "{}"
auth_dir = "{}"
[usage]
path = "{}"
prices_source = ""
[ui]
allow_config_write = true
reset_credits_refresh_mins = 0
quota_refresh_mins = 0
reset_credits_auto_lead_mins = 10
[log]
level = "info"
"#,
                e.st.config.load().upstream,
                auth.display(),
                e.st.usage.as_ref().unwrap().path().display()
            ),
        )
        .unwrap();
        e.mock.credits.lock().unwrap()[1].2 = (ciallopa_usage::now_ms() / 1000 + 5 * 60) * 1000;
        // The fixture's in-process switch stays off; only the real binary may consume.
        assert!(!e.rc.auto().is_on("a"));
        drop(reservation);
        (path, addr)
    }

    #[tokio::test]
    async fn graceful_restart_flushes_the_switch_and_runs_the_background_sweep() {
        let e = env().await;
        let (path, addr) = config_file(&e);
        let (child, _logs) = start(&path).await;
        let response = e
            .http
            .post(format!(
                "http://{addr}/admin/v1/accounts/a/reset-credits/auto"
            ))
            .bearer_auth(KEY)
            .json(&json!({"enabled": true}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        // Stop immediately: exercise the real shutdown flush, not a debounce sleep.
        stop(child).await;
        assert!(
            e.consumes().is_empty(),
            "the startup delay prevents an immediate POST"
        );
        assert_eq!(
            e.st.usage
                .as_ref()
                .unwrap()
                .kv_get("reset_credits_auto_accounts")
                .unwrap()
                .as_deref(),
            Some(r#"["a"]"#)
        );

        let (child, mut logs) = start(&path).await;
        wait_log(&mut logs, "auto reset-card used before expiry", 35).await;
        stop(child).await;
        assert_eq!(e.consumes().len(), 1);
        assert_eq!(e.resets().len(), 1);
        assert_eq!(e.usage_calls(), 1);
        let rows =
            e.st.usage
                .as_ref()
                .unwrap()
                .reset_attempts(10)
                .await
                .unwrap();
        assert_eq!(rows[0].status, ciallopa_usage::reset::ConsumeStatus::Done);
        assert_eq!(rows[0].key.as_deref(), Some(AUTO_LABEL));
        assert_eq!(
            rows[0].account,
            e.st.usage.as_ref().unwrap().alias_account("a")
        );
    }

    #[tokio::test]
    async fn real_restart_replays_a_lost_success_receipt_without_spending_twice() {
        let e = env().await;
        let (path, _addr) = config_file(&e);
        let rec = e.st.usage.as_ref().unwrap();
        rec.kv_set("reset_credits_auto_accounts", r#"["a"]"#)
            .unwrap();
        e.mock.omit_redeemed.store(true, Ordering::SeqCst);
        e.mock
            .faults
            .lock()
            .unwrap()
            .push_back(ConsumeFault::ResetThenDisconnect);
        let (child, mut logs) = start(&path).await;
        wait_log(&mut logs, "auto reset-card attempt not done", 35).await;
        stop(child).await;
        let pending = rec.reset_pending("a").await.unwrap().unwrap();
        assert_eq!(e.resets().len(), 1);

        let (child, mut logs) = start(&path).await;
        wait_log(&mut logs, "auto reset-card: card ended", 75).await;
        stop(child).await;
        let requests = e.consumes();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["redeem_request_id"], pending.redeem_request_id);
        assert_eq!(
            requests[0]["redeem_request_id"],
            requests[1]["redeem_request_id"]
        );
        assert_eq!(e.resets().len(), 1);
        assert!(rec.reset_pending("a").await.unwrap().is_none());
        assert_eq!(
            e.usage_calls(),
            1,
            "one reconciliation query after recovery"
        );
        let rows = rec.reset_attempts(10).await.unwrap();
        assert_eq!(rows[0].code.as_deref(), Some("already_redeemed"));
        assert_eq!(
            rows[1].status,
            ciallopa_usage::reset::ConsumeStatus::Superseded
        );
        assert!(
            rows[0].started_ms - rows[1].started_ms >= RETRY_MS,
            "restart must preserve the persisted retry interval"
        );
    }
}
