//! Reset credits (重置卡): listing, manual consume (each result code), idempotent retry,
//! disabled switch, CSRF guards, and "automatic only through the per-account switch"
//! against a mock upstream (the automatic use itself is tested in `tests/auto_reset.rs`).
#![allow(clippy::unwrap_used)]
use async_trait::async_trait;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use ciallopa_core::*;
use ciallopa_forward::AppState;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

const KEY: &str = "sk-cpa-0123456789abcdef";

// QUOTA_LAST is process-global; serialize quota-refresh tests and clear it first.
static QUOTA_REFRESH_TEST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Store;
#[async_trait]
impl AccountStore for Store {
    async fn list(&self) -> Vec<Account> {
        ["a", "b"]
            .iter()
            .map(|i| Account {
                id: i.to_string(),
                email: Some(format!("{i}@example.test")),
                chatgpt_account_id: Some(format!("acct-{i}")),
                plan_type: None,
                priority: 0,
                weight: 1,
                // a disabled account still lists (and can use) its reset cards
                disabled: *i == "b",
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
}

/// Records `clear_quota_cooldown` calls.
#[derive(Default)]
struct NoRoute(Mutex<Vec<String>>);
impl ciallopa_core::Router for NoRoute {
    fn clear_quota_cooldown(&self, id: &AccountId) -> bool {
        self.0.lock().unwrap().push(id.to_string());
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

type Hdrs = Vec<(String, String)>;
type Shared<T> = Arc<Mutex<Vec<T>>>;

#[derive(Clone, Default)]
struct Mock {
    /// Scripted consume replies (status, body); default `reset`.
    replies: Arc<Mutex<VecDeque<(u16, Value)>>>,
    /// Scripted `/wham/usage` bodies; default has `limit_reached: true` and no `allowed`.
    usage: Arc<Mutex<VecDeque<Value>>>,
    /// Bearer tokens seen by `/wham/usage` (order = call order).
    usage_hits: Arc<Mutex<Vec<String>>>,
    consumes: Shared<(Hdrs, Value)>,
    lists: Shared<Hdrs>,
}

fn hs(h: &HeaderMap) -> Vec<(String, String)> {
    h.iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
        .collect()
}

async fn up_list(State(m): State<Mock>, h: HeaderMap) -> Json<Value> {
    m.lists.lock().unwrap().push(hs(&h));
    Json(json!({
        "available_count": 2,
        "credits": [
            {"id": "rc_1", "reset_type": "rate_limit", "status": "available",
             "granted_at": "2026-10-01T00:00:00Z", "expires_at": "2026-12-01T00:00:00Z"},
            {"id": "rc_2", "reset_type": "rate_limit", "status": "available",
             "granted_at": "2026-10-02T00:00:00Z", "expires_at": "2026-11-01T00:00:00Z"},
            {"id": "rc_0", "reset_type": "rate_limit", "status": "redeemed",
             "granted_at": "2026-09-01T00:00:00Z", "expires_at": "2026-10-01T00:00:00Z"}
        ]
    }))
}

async fn up_consume(State(m): State<Mock>, h: HeaderMap, Json(b): Json<Value>) -> Response {
    m.consumes.lock().unwrap().push((hs(&h), b));
    let (s, v) = m
        .replies
        .lock()
        .unwrap()
        .pop_front()
        .unwrap_or((200, json!({"code": "reset", "windows_reset": 2})));
    (StatusCode::from_u16(s).unwrap(), Json(v)).into_response()
}

async fn up_usage(State(m): State<Mock>, h: HeaderMap) -> Json<Value> {
    let auth = h
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    m.usage_hits.lock().unwrap().push(auth);
    let v = m.usage.lock().unwrap().pop_front().unwrap_or_else(|| {
        json!({"rate_limit": {"limit_reached": true, "primary_window": {"used_percent": 0}},
            "rate_limit_reset_credits": {"available_count": 2}})
    });
    Json(v)
}

struct Env {
    base: String,
    mock: Mock,
    router: Arc<NoRoute>,
    st: AppState,
    rc: ciallopa::reset_credits::ResetCredits,
    http: reqwest::Client,
    _dir: tempfile::TempDir,
}

async fn serve(app: Router) -> std::net::SocketAddr {
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = ln.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(ln, app).await.unwrap() });
    a
}

async fn env(allow: bool) -> Env {
    env_cfg(allow, false).await
}

async fn env_cfg(allow: bool, write: bool) -> Env {
    let mock = Mock::default();
    let up = Router::new()
        .route("/backend-api/wham/rate-limit-reset-credits", get(up_list))
        .route(
            "/backend-api/wham/rate-limit-reset-credits/consume",
            post(up_consume),
        )
        .route("/backend-api/wham/usage", get(up_usage))
        .with_state(mock.clone());
    let up = serve(up).await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = Config::parse(&format!(
        "instance_id = \"inst\"\napi_keys = [{{label = \"admin\", key = \"{KEY}\", admin = true}}]\nupstream = \"http://{up}/backend-api\"\n\
         [usage]\npath = \"{}\"\n[ui]\nallow_reset_consume = {allow}\nreset_credits_refresh_mins = 0\nquota_refresh_mins = 0\nallow_config_write = {write}\n",
        dir.path().join("u.db").display()
    ))
    .unwrap();
    let usage = Some(ciallopa_usage::Recorder::open_as(&cfg.usage, "inst").unwrap());
    let router = Arc::new(NoRoute::default());
    let state = AppState {
        accounts: Arc::new(Store),
        router: router.clone(),
        upstream: Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap()),
        usage,
        config: Arc::new(cfg.into()),
        egress: Default::default(),
    };
    let s = ciallopa::settings::Settings::new(state.clone(), None, None);
    let rc = s.reset_credits();
    let p = serve(ciallopa::build_app_with(s)).await;
    Env {
        base: format!("http://{p}"),
        mock,
        router,
        st: state,
        rc,
        http: reqwest::Client::new(),
        _dir: dir,
    }
}

impl Env {
    async fn consume(&self, acct: &str, body: Value) -> (u16, Value) {
        let r = self
            .http
            .post(format!(
                "{}/admin/v1/accounts/{acct}/reset-credits/consume",
                self.base
            ))
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
    fn n_consumes(&self) -> usize {
        self.mock.consumes.lock().unwrap().len()
    }
}

#[tokio::test]
async fn list_refresh_and_headers() {
    let e = env(false).await;
    let v = e.get("/usage/v1/reset-credits").await;
    assert_eq!(v["consume_enabled"], false);
    assert!(v["accounts"][0]["reset_credits"].is_null());
    let r: Value = e
        .http
        .post(format!("{}/usage/v1/reset-credits/refresh", e.base))
        .bearer_auth(KEY)
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let rc = &r["accounts"][0]["reset_credits"];
    assert_eq!(rc["available_count"], 2);
    assert_eq!(rc["nearest_expires_at"], "2026-11-01T00:00:00Z");
    assert_eq!(rc["credits"].as_array().unwrap().len(), 3);
    let lists = e.mock.lists.lock().unwrap().clone();
    // both accounts, including the disabled "b"
    assert_eq!(lists.len(), 2);
    assert_eq!(r["accounts"][1]["reset_credits"]["available_count"], 2);
    let h = &lists[0];
    let get = |k: &str| h.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone());
    assert_eq!(get("authorization").as_deref(), Some("Bearer tok-a"));
    assert_eq!(get("chatgpt-account-id").as_deref(), Some("acct-a"));
    assert_eq!(get("originator").as_deref(), Some("codex_cli_rs"));
    assert!(get("user-agent").unwrap().starts_with("codex_cli_rs/"));
    assert!(get("cookie").is_none());
    // listing never consumes
    assert_eq!(e.n_consumes(), 0);
    // no key -> 401
    let s = e
        .http
        .get(format!("{}/usage/v1/reset-credits", e.base))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(s, 401);
}

#[tokio::test]
async fn disabled_switch_is_403() {
    let e = env(false).await;
    let (s, v) = e.consume("a", json!({"confirm": "a"})).await;
    assert_eq!(s, 403, "{v}");
    assert_eq!(e.n_consumes(), 0);
}

#[tokio::test]
async fn guards_confirm_key_json_origin() {
    let e = env(true).await;
    let url = format!("{}/admin/v1/accounts/a/reset-credits/consume", e.base);
    // wrong confirm
    assert_eq!(e.consume("a", json!({"confirm": "b"})).await.0, 400);
    assert_eq!(e.consume("a", json!({})).await.0, 400);
    // no key
    let s = e
        .http
        .post(&url)
        .json(&json!({"confirm": "a"}))
        .send()
        .await
        .unwrap();
    assert_eq!(s.status(), 401);
    // form body (no preflight) rejected
    let s = e
        .http
        .post(&url)
        .bearer_auth(KEY)
        .header("content-type", "text/plain")
        .body(r#"{"confirm":"a"}"#)
        .send()
        .await
        .unwrap();
    assert!(matches!(s.status().as_u16(), 400 | 415), "{}", s.status());
    // a cross-site browser call is rejected even with a valid key (cookies play no part)
    for (h, v) in [
        ("origin", "https://evil.example"),
        ("sec-fetch-site", "cross-site"),
    ] {
        let s = e
            .http
            .post(&url)
            .bearer_auth(KEY)
            .header("cookie", "CF_Authorization=x")
            .header(h, v)
            .json(&json!({"confirm": "a"}))
            .send()
            .await
            .unwrap();
        assert_eq!(s.status(), 403, "{h}: {v}");
    }
    // unknown account
    assert_eq!(e.consume("zz", json!({"confirm": "zz"})).await.0, 404);
    assert_eq!(e.n_consumes(), 0);
}

#[tokio::test]
async fn each_result_code() {
    let e = env(true).await;
    for (code, w) in [
        ("reset", 2),
        ("nothing_to_reset", 0),
        ("no_credit", 0),
        ("already_redeemed", 0),
    ] {
        e.mock
            .replies
            .lock()
            .unwrap()
            .push_back((200, json!({"code": code, "windows_reset": w})));
        let (s, v) = e.consume("a", json!({"confirm": "a"})).await;
        assert_eq!(s, 200, "{v}");
        assert_eq!(v["code"], code);
        assert_eq!(v["windows_reset"], w);
        assert_eq!(v["status"], "done");
        assert_eq!(v["reused_redeem_request_id"], false);
        assert_eq!(v["reset_credits"]["available_count"], 2);
        if code == "reset" {
            assert!(v["rate_limit"].is_object());
            assert_eq!(v["cooldown_cleared"], true);
        } else {
            assert!(v.get("cooldown_cleared").is_none(), "{v}");
        }
    }
    // Only the successful reset lifts the usage-limit cooldown.
    assert_eq!(*e.router.0.lock().unwrap(), ["a"]);
    let c = e.mock.consumes.lock().unwrap().clone();
    assert_eq!(c.len(), 4);
    let ids: std::collections::HashSet<String> = c
        .iter()
        .map(|(_, b)| b["redeem_request_id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids.len(), 4, "fresh id per definitive attempt");
    assert!(c[0].1.get("credit_id").is_none());
    let h = &c[0].0;
    assert!(h
        .iter()
        .any(|(k, v)| k == "authorization" && v == "Bearer tok-a"));
    assert!(h
        .iter()
        .any(|(k, v)| k == "content-type" && v == "application/json"));
    let v = e.get("/usage/v1/reset-credits").await;
    let at = v["attempts"].as_array().unwrap();
    assert_eq!(at.len(), 4);
    assert_eq!(at[0]["code"], "already_redeemed");
    assert_eq!(at[3]["code"], "reset");
    assert_eq!(at[0]["key"], "admin");
    assert_eq!(at[0]["instance"], "inst");
}

#[tokio::test]
async fn idempotent_retry_reuses_id() {
    let e = env(true).await;
    {
        let mut r = e.mock.replies.lock().unwrap();
        r.push_back((502, json!({"error": "bad gateway"})));
        r.push_back((200, json!({"code": "already_redeemed", "windows_reset": 0})));
    }
    let (s1, v1) = e
        .consume("a", json!({"confirm": "a", "credit_id": "rc_2"}))
        .await;
    assert_eq!(s1, 502, "{v1}");
    assert_eq!(v1["status"], "pending");
    // another account / credit gets its own id
    let (s2, v2) = e
        .consume("a", json!({"confirm": "a", "credit_id": "rc_2"}))
        .await;
    assert_eq!(s2, 200, "{v2}");
    assert_eq!(v2["reused_redeem_request_id"], true);
    assert_eq!(v1["redeem_request_id"], v2["redeem_request_id"]);
    let c = e.mock.consumes.lock().unwrap().clone();
    assert_eq!(c[0].1["redeem_request_id"], c[1].1["redeem_request_id"]);
    assert_eq!(c[1].1["credit_id"], "rc_2");
    // next attempt after a definitive answer is fresh
    let (_, v3) = e
        .consume("a", json!({"confirm": "a", "credit_id": "rc_2"}))
        .await;
    assert_ne!(v3["redeem_request_id"], v2["redeem_request_id"]);
    let at = e.get("/usage/v1/reset-credits").await["attempts"].clone();
    let st: Vec<&str> = at
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["status"].as_str().unwrap())
        .collect();
    assert_eq!(st, ["done", "done", "superseded"]);
    // explicit id from the client is honoured
    let (_, v4) = e
        .consume("b", json!({"confirm": "b", "redeem_request_id": "abc-123"}))
        .await;
    assert_eq!(v4["redeem_request_id"], "abc-123");
    assert_eq!(
        e.consume("b", json!({"confirm": "b", "redeem_request_id": "x y"}))
            .await
            .0,
        400
    );
}

#[tokio::test]
async fn pending_blocks_a_different_spend() {
    // Core #2: account-level attempt with unknown outcome, then a card-level click must not
    // create a second redeem id (would spend two cards).
    let e = env(true).await;
    {
        let mut r = e.mock.replies.lock().unwrap();
        r.push_back((502, json!({"error": "bad gateway"})));
        r.push_back((200, json!({"code": "reset", "windows_reset": 1})));
    }
    let (s1, v1) = e.consume("a", json!({"confirm": "a"})).await;
    assert_eq!(s1, 502, "{v1}");
    let rid = v1["redeem_request_id"].clone();
    // different credit → 409 with the pending attempt, upstream untouched
    let (s2, v2) = e
        .consume("a", json!({"confirm": "a", "credit_id": "rc_1"}))
        .await;
    assert_eq!(s2, 409, "{v2}");
    assert_eq!(v2["pending"]["redeem_request_id"], rid);
    assert!(v2["pending"]["credit_id"].is_null());
    // a foreign explicit id is refused too
    let (s3, _) = e
        .consume("a", json!({"confirm": "a", "redeem_request_id": "other-1"}))
        .await;
    assert_eq!(s3, 409);
    assert_eq!(e.n_consumes(), 1);
    // retrying the same attempt is allowed and reuses the id
    let (s4, v4) = e
        .consume("a", json!({"confirm": "a", "redeem_request_id": rid}))
        .await;
    assert_eq!(s4, 200, "{v4}");
    assert_eq!(v4["redeem_request_id"], rid);
    let c = e.mock.consumes.lock().unwrap().clone();
    assert_eq!(c.len(), 2);
    assert_eq!(c[0].1["redeem_request_id"], c[1].1["redeem_request_id"]);
}

#[tokio::test]
async fn refresh_admin_csrf_and_can_consume() {
    let e = env(true).await;
    let v = e.get("/usage/v1/reset-credits").await;
    assert_eq!(v["can_consume"], true);
    let url = format!("{}/usage/v1/reset-credits/refresh", e.base);
    // no JSON content type
    let r = e.http.post(&url).bearer_auth(KEY).send().await.unwrap();
    assert_eq!(r.status(), 400);
    // a cookie alone never authenticates (Cloudflare Access adds CF_Authorization)
    let r = e
        .http
        .post(&url)
        .header("cookie", "CF_Authorization=x")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    // cross-site browser calls are rejected even with a valid key
    for hs in [
        &[("origin", "https://evil.example")][..],
        &[("origin", "null")][..],
        &[("sec-fetch-site", "cross-site")][..],
        &[
            ("sec-fetch-site", "same-site"),
            ("origin", "https://a.example"),
        ][..],
    ] {
        let mut rb = e.http.post(&url).bearer_auth(KEY).json(&json!({}));
        for (h, v) in hs {
            rb = rb.header(*h, *v);
        }
        assert_eq!(rb.send().await.unwrap().status(), 403, "{hs:?}");
    }
    assert!(e.mock.lists.lock().unwrap().is_empty());
    // the management page behind Cloudflare Access: same-origin fetch that carries the Access
    // cookie, the key in Authorization, and a public Origin that differs from the proxied Host
    let r = e
        .http
        .post(&url)
        .bearer_auth(KEY)
        .header("cookie", "CF_Authorization=eyJ.x.y")
        .header("origin", "https://cpa.example.com")
        .header("x-forwarded-host", "cpa.example.com")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = e
        .http
        .post(&url)
        .bearer_auth(KEY)
        .header("cookie", "CF_Authorization=eyJ.x.y")
        .header("origin", "https://cpa.example.com")
        .header("sec-fetch-site", "same-origin")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_ne!(r.status(), 400);
    assert_ne!(r.status(), 403);
}

#[test]
fn no_automatic_consume_path() {
    // The upstream consume helper has one caller, `consume_locked`, which is reached only
    // from the manual admin handler and the opt-in automatic use (`auto_reset::attempt`).
    let src = include_str!("../src/reset_credits.rs");
    assert_eq!(
        src.matches("consume_upstream(").count(),
        2,
        "definition + one call"
    );
    let locked = src.split("async fn consume_locked(").nth(1).unwrap();
    assert!(locked.contains("consume_upstream(st"));
    assert_eq!(
        src.matches("consume_locked(").count(),
        2,
        "definition + manual"
    );
    let handler = src.split("async fn consume(").nth(1).unwrap();
    assert!(handler[..handler.find("\n}\n").unwrap()].contains("consume_locked("));
    let auto = include_str!("../src/auto_reset.rs");
    assert_eq!(
        auto.matches("consume_locked(").count(),
        1,
        "one automatic call"
    );
    assert!(!auto.contains("consume_upstream"));
    let attempt = auto.split("async fn attempt(").nth(1).unwrap();
    let attempt = &attempt[..attempt.find("\n}\n").unwrap()];
    assert!(
        attempt.contains("consume_locked("),
        "only attempt() uses a card"
    );
    // ...and attempt() only runs for accounts whose switch is on
    assert!(attempt.contains("is_on("));
    for f in [
        "refresh_all",
        "spawn_poller",
        "refresh_account",
        "quota_refresh_account",
        "note_usage",
    ] {
        let body = src.split(&format!("fn {f}(")).nth(1).unwrap();
        let body = &body[..body.find("\n}\n").unwrap()];
        assert!(
            !body.contains("CONSUME_PATH")
                && !body.contains("consume_upstream")
                && !body.contains("consume_locked"),
            "{f}"
        );
    }
    // nothing else in the workspace references the consume endpoint
    for other in [
        include_str!("../../forward/src/proxy.rs"),
        include_str!("../../forward/src/lib.rs"),
        include_str!("../../routing/src/lib.rs"),
        include_str!("../src/main.rs"),
    ] {
        assert!(!other.contains("reset-credits/consume") && !other.contains("consume_locked"));
    }
}

#[tokio::test]
async fn quota_refresh_stores_snapshot_and_rate_limits() {
    let _gate = QUOTA_REFRESH_TEST.lock().await;
    ciallopa::reset_credits::quota_refresh_reset_throttle_for_tests();
    let e = env(false).await;
    let post = |path: &str| {
        e.http
            .post(format!("{}{path}", e.base))
            .bearer_auth(KEY)
            .json(&json!({}))
            .send()
    };
    let r: Value = post("/admin/v1/accounts/a/quota-refresh")
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["ok"], true, "{r}");
    assert_eq!(r["quota"]["primary"]["used_pct"], 0.0);
    // display-only fields of the same answer, also kept for the management page
    assert_eq!(r["usage_available_count"], 2, "{r}");
    assert!(r["allowed"].is_null(), "default mock omits allowed: {r}");
    assert_eq!(r["limit_reached"], true, "{r}");
    assert!(
        r.get("cooldown_cleared").is_none(),
        "missing allowed must not clear: {r}"
    );
    assert!(
        e.router.0.lock().unwrap().is_empty(),
        "clear_quota not called"
    );
    let l = e.get("/usage/v1/reset-credits").await;
    let a = l["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["account"] == "a")
        .unwrap();
    assert_eq!(a["reset_credits"]["usage_available_count"], 2, "{l}");
    assert_eq!(a["reset_credits"]["limit_reached"], true, "{l}");
    assert!(
        a["reset_credits"]["available_count"].is_null(),
        "listing untouched: {l}"
    );
    // the same account again within 30 s: not asked upstream
    let r: Value = post("/admin/v1/accounts/a/quota-refresh")
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["error"]["kind"], "rate_limited", "{r}");
    let all: Value = post("/admin/v1/accounts/quota-refresh")
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(all["ok"], 1, "b asked, a skipped: {all}");
    assert_eq!(all["skipped"], 1, "{all}");
    assert_eq!(
        all["failed"], 0,
        "a rate-limited account is not a failure: {all}"
    );
    assert_eq!(all["results"][0]["error"]["kind"], "rate_limited");
    // stored with observer "manual" (usage writer batch)
    let mut seen = false;
    for _ in 0..50 {
        let q = e.get("/usage/v1/quota").await;
        if q["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|x| x["quota"]["observed_by"] == "manual")
        {
            seen = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(seen, "manual snapshot stored");
    // CSRF rules as other management POSTs
    let s = e
        .http
        .post(format!("{}/admin/v1/accounts/a/quota-refresh", e.base))
        .bearer_auth(KEY)
        .body("{}")
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(s, 400);
    assert_eq!(e.n_consumes(), 0);
}

#[tokio::test]
async fn quota_refresh_clears_cooldown_only_when_restored() {
    let _gate = QUOTA_REFRESH_TEST.lock().await;
    ciallopa::reset_credits::quota_refresh_reset_throttle_for_tests();
    let e = env(false).await;
    let post = |path: &str| {
        e.http
            .post(format!("{}{path}", e.base))
            .bearer_auth(KEY)
            .json(&json!({}))
            .send()
    };
    // One scripted answer per account (a/b): the 30 s throttle is per account, so both run.
    // Restored on `a` clears; `allowed: false` on `b` does not.
    {
        let mut u = e.mock.usage.lock().unwrap();
        u.push_back(json!({
            "rate_limit": {
                "allowed": true,
                "limit_reached": false,
                "primary_window": {"used_percent": 10.0, "reset_after_seconds": 600, "reset_at": 1700000600}
            },
            "rate_limit_reset_credits": {"available_count": 1}
        }));
        u.push_back(json!({
            "rate_limit": {
                "allowed": false,
                "limit_reached": false,
                "primary_window": {"used_percent": 100.0, "reset_after_seconds": 600, "reset_at": 1700000600}
            },
            "rate_limit_reset_credits": {"available_count": 0}
        }));
    }
    let r: Value = post("/admin/v1/accounts/a/quota-refresh")
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["ok"], true, "{r}");
    assert_eq!(r["allowed"], true, "{r}");
    assert_eq!(r["limit_reached"], false, "{r}");
    assert_eq!(r["cooldown_cleared"], true, "{r}");
    assert_eq!(*e.router.0.lock().unwrap(), ["a"]);

    let r: Value = post("/admin/v1/accounts/b/quota-refresh")
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["ok"], true, "{r}");
    assert_eq!(r["allowed"], false, "{r}");
    assert_eq!(r["limit_reached"], false, "{r}");
    assert!(
        r.get("cooldown_cleared").is_none(),
        "allowed false must not clear: {r}"
    );
    assert_eq!(*e.router.0.lock().unwrap(), ["a"], "b must not clear");
}

#[tokio::test]
async fn quota_refresh_no_clear_when_limit_reached_or_allowed_absent() {
    let _gate = QUOTA_REFRESH_TEST.lock().await;
    ciallopa::reset_credits::quota_refresh_reset_throttle_for_tests();
    let e = env(false).await;
    let post = |acct: &str| {
        e.http
            .post(format!("{}/admin/v1/accounts/{acct}/quota-refresh", e.base))
            .bearer_auth(KEY)
            .json(&json!({}))
            .send()
    };
    {
        let mut u = e.mock.usage.lock().unwrap();
        // a: allowed true but still limit_reached
        u.push_back(json!({
            "rate_limit": {
                "allowed": true,
                "limit_reached": true,
                "primary_window": {"used_percent": 100.0}
            },
            "rate_limit_reset_credits": {"available_count": 0}
        }));
        // b: limit_reached false but allowed omitted
        u.push_back(json!({
            "rate_limit": {
                "limit_reached": false,
                "primary_window": {"used_percent": 0.0}
            },
            "rate_limit_reset_credits": {"available_count": 1}
        }));
    }
    let r: Value = post("a").await.unwrap().json().await.unwrap();
    assert_eq!(r["ok"], true, "{r}");
    assert_eq!(r["allowed"], true);
    assert_eq!(r["limit_reached"], true);
    assert!(r.get("cooldown_cleared").is_none(), "{r}");

    let r: Value = post("b").await.unwrap().json().await.unwrap();
    assert_eq!(r["ok"], true, "{r}");
    assert!(r["allowed"].is_null(), "{r}");
    assert_eq!(r["limit_reached"], false);
    assert!(r.get("cooldown_cleared").is_none(), "absent allowed: {r}");
    assert!(e.router.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn quota_auto_switch_and_opted_in_refresh() {
    let _gate = QUOTA_REFRESH_TEST.lock().await;
    ciallopa::reset_credits::quota_refresh_reset_throttle_for_tests();
    let e = env_cfg(false, true).await;
    let list = e.get("/usage/v1/reset-credits").await;
    assert_eq!(list["quota_refresh_mins"], 0, "{list}");
    assert_eq!(list["refresh_mins"], 0, "{list}");
    assert_eq!(list["accounts"][0]["quota_auto"], false);
    assert_eq!(list["accounts"][1]["quota_auto"], false);

    // needs allow_config_write
    let ro = env_cfg(false, false).await;
    let s = ro
        .http
        .post(format!(
            "{}/admin/v1/accounts/a/quota-refresh/auto",
            ro.base
        ))
        .bearer_auth(KEY)
        .json(&json!({"enabled": true}))
        .send()
        .await
        .unwrap()
        .status()
        .as_u16();
    assert_eq!(s, 403);

    let r = e
        .http
        .post(format!("{}/admin/v1/accounts/a/quota-refresh/auto", e.base))
        .bearer_auth(KEY)
        .json(&json!({"enabled": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "{}", r.text().await.unwrap_or_default());
    let list = e.get("/usage/v1/reset-credits").await;
    let a = list["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["account"] == "a")
        .unwrap();
    let b = list["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["account"] == "b")
        .unwrap();
    assert_eq!(a["quota_auto"], true, "{list}");
    assert_eq!(b["quota_auto"], false, "{list}");

    // Only opted-in `a` is asked (disabled `b` is not opted in).
    e.mock.usage_hits.lock().unwrap().clear();
    ciallopa::reset_credits::quota_refresh_opted_in(&e.st, &e.rc).await;
    let hits = e.mock.usage_hits.lock().unwrap().clone();
    assert_eq!(hits.len(), 1, "only opted-in account: {hits:?}");
    assert_eq!(hits[0], "Bearer tok-a");

    // Opt in `b` (disabled still refreshes when opted in, like manual).
    let r = e
        .http
        .post(format!("{}/admin/v1/accounts/b/quota-refresh/auto", e.base))
        .bearer_auth(KEY)
        .json(&json!({"enabled": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    ciallopa::reset_credits::quota_refresh_reset_throttle_for_tests();
    e.mock.usage_hits.lock().unwrap().clear();
    ciallopa::reset_credits::quota_refresh_opted_in(&e.st, &e.rc).await;
    let mut hits = e.mock.usage_hits.lock().unwrap().clone();
    hits.sort();
    assert_eq!(
        hits,
        vec!["Bearer tok-a".to_string(), "Bearer tok-b".to_string()],
        "{hits:?}"
    );

    // Debounced persist
    let rec = e.st.usage.clone().unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2600)).await;
    let stored = rec.kv_get("quota_refresh_auto_accounts").unwrap();
    let mut ids: Vec<String> = serde_json::from_str(stored.as_deref().unwrap_or("[]")).unwrap();
    ids.sort();
    assert_eq!(ids, vec!["a", "b"]);
}

#[tokio::test]
async fn quota_auto_forget_on_account_removal_via_switch_off() {
    let _gate = QUOTA_REFRESH_TEST.lock().await;
    let e = env_cfg(false, true).await;
    e.rc.quota_auto().set("a", true);
    assert!(e.rc.quota_auto().is_on("a"));
    e.rc.forget("a");
    assert!(!e.rc.quota_auto().is_on("a"));
}
