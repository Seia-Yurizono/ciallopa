//! Automatic reset-card use (「快到期时自动使用」) against a mock upstream: the switch API and
//! its persistence, the window, retries, give-up, restart, and the races listed in
//! `src/auto_reset.rs`. The sweep is driven directly with a shifted clock; card expiries are
//! hours ahead of the real clock, so the background sweep never finds anything due.
#![allow(clippy::unwrap_used)]
use async_trait::async_trait;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use ciallopa::auto_reset::{sweep_once, Event, AUTO_LABEL, MAX_TRIES, RETRY_MS};
use ciallopa::reset_credits::ResetCredits;
use ciallopa_core::*;
use ciallopa_forward::AppState;
use serde_json::{json, Value};
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

const KEY: &str = "sk-cpa-0123456789abcdef";
const MIN: i64 = 60_000;
const HOUR: i64 = 60 * MIN;

#[derive(Default)]
struct Store {
    ids: Mutex<Vec<(String, bool)>>,
    relogin: Mutex<HashSet<String>>,
}
#[async_trait]
impl AccountStore for Store {
    async fn list(&self) -> Vec<Account> {
        self.ids
            .lock()
            .unwrap()
            .iter()
            .map(|(i, disabled)| Account {
                id: i.clone(),
                email: Some(format!("{i}@example.test")),
                chatgpt_account_id: Some(format!("acct-{i}")),
                plan_type: None,
                priority: 0,
                weight: 1,
                disabled: *disabled,
            })
            .collect()
    }
    async fn credentials(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        Ok(Credentials {
            account: id.clone(),
            access_token: format!("tok-{id}"),
            chatgpt_account_id: Some(format!("acct-{id}")),
        })
    }
    async fn force_refresh(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        self.credentials(id).await
    }
    async fn reload(&self) -> Result<(), AuthError> {
        Ok(())
    }
    async fn relogin_required(&self) -> Vec<AccountId> {
        self.relogin.lock().unwrap().iter().cloned().collect()
    }
}

struct NoRoute;
impl ciallopa_core::Router for NoRoute {
    fn clear_quota_cooldown(&self, _: &AccountId) -> bool {
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

/// Upstream whose listing reflects what was consumed.
#[derive(Clone, Default)]
struct Mock {
    /// (id, status, expires_at ms)
    credits: Arc<Mutex<Vec<(String, String, i64)>>>,
    replies: Arc<Mutex<VecDeque<(u16, Value)>>>,
    consumes: Arc<Mutex<Vec<Value>>>,
    lists: Arc<Mutex<usize>>,
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
    chrono_like(ms)
}

/// RFC 3339 (UTC) without pulling a date crate into the test.
fn chrono_like(ms: i64) -> String {
    let s = ms.div_euclid(1000);
    let (days, rem) = (s.div_euclid(86_400), s.rem_euclid(86_400));
    // civil-from-days (H. Hinnant)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

async fn up_list(State(m): State<Mock>) -> Json<Value> {
    pass(&m.list_gate).await;
    *m.lists.lock().unwrap() += 1;
    let cs = m.credits.lock().unwrap().clone();
    let avail = cs.iter().filter(|c| c.1 == "available").count();
    Json(json!({
        "available_count": avail,
        "credits": cs.iter().map(|(id, st, e)| json!({
            "id": id, "reset_type": "rate_limit", "status": st, "expires_at": rfc(*e)
        })).collect::<Vec<_>>()
    }))
}

async fn up_consume(State(m): State<Mock>, _h: HeaderMap, Json(b): Json<Value>) -> Response {
    m.consumes.lock().unwrap().push(b.clone());
    pass(&m.consume_gate).await;
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
        for c in m.credits.lock().unwrap().iter_mut() {
            if b["credit_id"] == c.0 {
                c.1 = "redeemed".into();
            }
        }
    }
    (StatusCode::from_u16(s).unwrap(), Json(v)).into_response()
}

async fn up_usage() -> Json<Value> {
    Json(json!({"rate_limit": {"limit_reached": false}}))
}

struct Env {
    base: String,
    mock: Mock,
    store: Arc<Store>,
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
    let usage = Some(ciallopa_usage::Recorder::open_as(&cfg.usage, "inst").unwrap());
    let store = Arc::new(Store::default());
    *store.ids.lock().unwrap() = vec![("a".into(), false), ("b".into(), true)];
    let st = AppState {
        accounts: store.clone(),
        router: Arc::new(NoRoute),
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
    tokio::time::sleep(std::time::Duration::from_millis(2600)).await;
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
    assert_eq!(Env::ev(&evs, "a"), Event::Skipped("not_due"));
    assert_eq!(Env::ev(&evs, "b"), Event::Skipped("disabled"));
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
    assert_eq!(Env::ev(&evs, "a"), Event::Skipped("not_due"));
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
        assert_eq!(Env::ev(&evs, "a"), Event::Skipped("not_due"));
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
    assert_eq!(Env::ev(&e.sweep().await, "a"), Event::Skipped("not_due"));
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
    tokio::time::sleep(std::time::Duration::from_millis(2600)).await;
    let cfg = config("127.0.0.1:1", &path.join("u.db"), true, 10);
    let rec = ciallopa_usage::Recorder::open_as(&cfg.usage, "inst").unwrap();
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
    assert_eq!(Env::ev(&evs, "a"), Event::Skipped("manual_pending"));
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
        Event::Skipped("manual_pending")
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
    assert_eq!(Env::ev(&evs, "a"), Event::Skipped("not_due"));
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
            Event::Skipped("switched_off"),
        ),
        (
            "deleted",
            Box::new(|e: &Env| e.store.ids.lock().unwrap().retain(|(i, _)| i != "a")),
            Event::Skipped("account_gone"),
        ),
        (
            "disabled",
            Box::new(|e: &Env| e.store.ids.lock().unwrap()[0].1 = true),
            Event::Skipped("disabled"),
        ),
        (
            "relogin",
            Box::new(|e: &Env| {
                e.store.relogin.lock().unwrap().insert("a".into());
            }),
            Event::Skipped("relogin"),
        ),
        (
            "lead shortened (hot reload)",
            Box::new(|e: &Env| {
                let mut cfg = (*e.st.config.load()).clone();
                cfg.ui.reset_credits_auto_lead_mins = 5;
                e.st.config.store(cfg);
            }),
            Event::Skipped("not_due"),
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

#[test]
fn rfc_helper() {
    assert_eq!(chrono_like(0), "1970-01-01T00:00:00Z");
    assert_eq!(chrono_like(1_798_761_600_000), "2027-01-01T00:00:00Z");
}
