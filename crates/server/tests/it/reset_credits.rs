//! Reset credits (重置卡): listing, manual consume (each result code), idempotent retry,
//! disabled switch, CSRF guards, and "automatic only through the per-account switch"
//! against a mock upstream (the automatic use itself is tested in `tests/auto_reset.rs`).
#![allow(clippy::unwrap_used)]
use super::support::FixedStore as Store;
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

/// Records calls to clear usage-limit cooldowns.
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
    let rec = ciallopa_usage::Recorder::open_as(&cfg.usage, "inst").unwrap();
    // Match the real server's write-time account aliases.
    rec.load_or_create_aliaser().unwrap();
    let usage = Some(rec);
    let router = Arc::new(NoRoute::default());
    let state = AppState {
        accounts: Arc::new(Store::with_accounts(
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
        )),
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
    // Refreshes run concurrently; arrival order does not identify the account.
    let h = lists
        .iter()
        .find(|headers| {
            headers
                .iter()
                .any(|(n, v)| n == "chatgpt-account-id" && v == "acct-a")
        })
        .expect("account a was refreshed");
    let get = |k: &str| h.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone());
    assert_eq!(get("authorization").as_deref(), Some("Bearer tok-a"));
    assert_eq!(get("chatgpt-account-id").as_deref(), Some("acct-a"));
    assert!(ciallopa_core::fingerprint::validate(
        &get("user-agent").unwrap(),
        &get("originator").unwrap()
    )
    .is_some());
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
async fn reset_result_updates_cache_cooldown_and_audit() {
    let e = env(true).await;
    let (code, w) = ("reset", 2);
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
    // Only the successful reset lifts the usage-limit cooldown.
    assert_eq!(*e.router.0.lock().unwrap(), ["a"]);
    let c = e.mock.consumes.lock().unwrap().clone();
    assert_eq!(c.len(), 1);
    let ids: std::collections::HashSet<String> = c
        .iter()
        .map(|(_, b)| b["redeem_request_id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids.len(), 1);
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
    assert_eq!(at.len(), 1);
    assert_eq!(at[0]["code"], "reset");
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
async fn recovered_receipt_checks_quota_once_before_clearing_cooldown() {
    let e = env(true).await;
    e.mock.replies.lock().unwrap().extend([
        (502, json!({"error": "response lost"})),
        (200, json!({"code": "already_redeemed", "windows_reset": 0})),
    ]);
    e.mock
        .usage
        .lock()
        .unwrap()
        .push_back(json!({"rate_limit": {
            "allowed": true, "limit_reached": false, "primary_window": {"used_percent": 0}
        }}));
    let body = json!({"confirm": "a", "credit_id": "rc_2"});
    let (_, first) = e.consume("a", body.clone()).await;
    let (_, recovered) = e.consume("a", body).await;
    assert_eq!(first["redeem_request_id"], recovered["redeem_request_id"]);
    assert_eq!(recovered["code"], "already_redeemed");
    assert_eq!(recovered["cooldown_cleared"], true);
    assert_eq!(*e.router.0.lock().unwrap(), ["a"]);
    assert_eq!(e.mock.usage_hits.lock().unwrap().len(), 1);
    let lists = e.mock.lists.lock().unwrap().len();
    for _ in 0..10 {
        e.get("/usage/v1/reset-credits").await;
    }
    assert_eq!(
        e.n_consumes(),
        2,
        "a receipt must not trigger another reset"
    );
    assert_eq!(e.mock.usage_hits.lock().unwrap().len(), 1);
    assert_eq!(e.mock.lists.lock().unwrap().len(), lists);
}

#[tokio::test]
async fn transient_http_errors_keep_the_request_id_and_do_not_query_quota() {
    for status in [408, 429, 500, 502, 503] {
        let e = env(true).await;
        e.mock
            .replies
            .lock()
            .unwrap()
            .push_back((status, json!({"detail": "retry later"})));
        let body = json!({"confirm": "a", "credit_id": "rc_2"});
        let (_, first) = e.consume("a", body.clone()).await;
        assert_eq!(first["status"], "pending", "HTTP {status}: {first}");
        assert!(e.mock.usage_hits.lock().unwrap().is_empty());
        assert!(e.mock.lists.lock().unwrap().is_empty());
        let (_, retry) = e.consume("a", body).await;
        assert_eq!(first["redeem_request_id"], retry["redeem_request_id"]);
        assert_eq!(retry["code"], "reset");
        assert_eq!(e.n_consumes(), 2);
    }
}

#[tokio::test]
async fn recovered_receipt_does_not_clear_cooldown_without_restored_quota() {
    for rate_limit in [
        json!({"allowed": false, "limit_reached": false}),
        json!({"allowed": true, "limit_reached": true}),
        json!({"limit_reached": false}),
    ] {
        let e = env(true).await;
        e.mock
            .replies
            .lock()
            .unwrap()
            .push_back((200, json!({"code": "already_redeemed"})));
        e.mock
            .usage
            .lock()
            .unwrap()
            .push_back(json!({"rate_limit": rate_limit}));
        let (_, response) = e
            .consume("a", json!({"confirm": "a", "credit_id": "rc_2"}))
            .await;
        assert_eq!(response["status"], "done");
        assert!(e.router.0.lock().unwrap().is_empty());
        assert!(response.get("cooldown_cleared").is_none());
        assert_eq!(e.mock.usage_hits.lock().unwrap().len(), 1);
        assert_eq!(e.n_consumes(), 1);
    }
}

#[tokio::test]
async fn failed_quota_refresh_keeps_the_same_throttle_and_never_consumes() {
    let e = env(true).await;
    e.mock
        .usage
        .lock()
        .unwrap()
        .push_back(json!({"unexpected": "no quota windows"}));
    let post = || {
        e.http
            .post(format!("{}/admin/v1/accounts/a/quota-refresh", e.base))
            .bearer_auth(KEY)
            .json(&json!({}))
            .send()
    };
    let first: Value = post().await.unwrap().json().await.unwrap();
    assert_eq!(first["error"]["kind"], "decode");
    for _ in 0..10 {
        let result: Value = post().await.unwrap().json().await.unwrap();
        assert_eq!(result["error"]["kind"], "rate_limited");
        assert!(result["error"]["retry_after_s"].as_u64().unwrap() <= 30);
    }
    assert_eq!(e.mock.usage_hits.lock().unwrap().len(), 1);
    assert!(e.mock.lists.lock().unwrap().is_empty());
    assert_eq!(e.n_consumes(), 0);
    assert!(!e.rc.auto().is_on("a"));
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

#[tokio::test]
async fn listing_and_quota_refresh_never_spend_an_enabled_manual_credit() {
    let e = env(true).await;
    for path in [
        "/usage/v1/reset-credits/refresh",
        "/admin/v1/accounts/a/quota-refresh",
    ] {
        let response = e
            .http
            .post(format!("{}{path}", e.base))
            .bearer_auth(KEY)
            .json(&json!({"account": "a"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }
    for _ in 0..10 {
        e.get("/usage/v1/reset-credits").await;
    }
    assert_eq!(e.n_consumes(), 0);
    assert_eq!(e.mock.lists.lock().unwrap().len(), 1);
    assert_eq!(e.mock.usage_hits.lock().unwrap().len(), 1);
    assert!(!e.rc.auto().is_on("a"));
}

#[tokio::test]
async fn quota_refresh_stores_snapshot_and_rate_limits() {
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
async fn quota_refresh_cooldown_table() {
    // Each pair uses distinct accounts, so the per-account throttle remains exercised.
    for cases in [
        [
            (Some(true), Some(false), true),
            (Some(false), Some(false), false),
        ],
        [(Some(true), Some(true), false), (None, Some(false), false)],
    ] {
        let e = env(false).await;
        for (account, (allowed, reached, clears)) in ["a", "b"].into_iter().zip(cases) {
            let mut rate_limit =
                json!({"primary_window": {"used_percent": 10.0, "reset_after_seconds": 600}});
            if let Some(v) = allowed {
                rate_limit["allowed"] = json!(v);
            }
            if let Some(v) = reached {
                rate_limit["limit_reached"] = json!(v);
            }
            e.mock
                .usage
                .lock()
                .unwrap()
                .push_back(json!({"rate_limit": rate_limit,
                "rate_limit_reset_credits": {"available_count": 1}}));
            let r: Value = e
                .http
                .post(format!(
                    "{}/admin/v1/accounts/{account}/quota-refresh",
                    e.base
                ))
                .bearer_auth(KEY)
                .json(&json!({}))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(r["ok"], true, "{r}");
            assert_eq!(r["allowed"], json!(allowed), "{r}");
            assert_eq!(r["limit_reached"], json!(reached), "{r}");
            assert_eq!(r.get("cooldown_cleared").is_some(), clears, "{r}");
            assert_eq!(
                e.router.0.lock().unwrap().iter().any(|a| a == account),
                clears
            );
        }
        assert_eq!(e.n_consumes(), 0, "quota refresh never consumes a card");
    }
}

#[tokio::test]
async fn quota_auto_switch_and_opted_in_refresh() {
    let e = env_cfg(false, true).await;
    let list = e.get("/usage/v1/reset-credits").await;
    assert_eq!(list["quota_refresh_mins"], 0, "{list}");
    assert_eq!(list["refresh_mins"], 0, "{list}");
    assert_eq!(list["quota_refresh_max_mins"], 0, "{list}");
    assert_eq!(list["refresh_max_mins"], 0, "{list}");
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
    e.mock.usage_hits.lock().unwrap().clear();
    ciallopa::reset_credits::quota_refresh_opted_in(&e.st, &e.rc).await;
    let mut hits = e.mock.usage_hits.lock().unwrap().clone();
    hits.sort();
    assert_eq!(hits, vec!["Bearer tok-b".to_string()], "{hits:?}");

    // Debounced persist
    let rec = e.st.usage.clone().unwrap();
    e.rc.flush(e.st.usage.as_ref().unwrap());
    let stored = rec.kv_get("quota_refresh_auto_accounts").unwrap();
    let mut ids: Vec<String> = serde_json::from_str(stored.as_deref().unwrap_or("[]")).unwrap();
    ids.sort();
    assert_eq!(ids, vec!["a", "b"]);
}

#[tokio::test]
async fn quota_auto_forget_on_account_removal_via_switch_off() {
    let e = env_cfg(false, true).await;
    e.rc.quota_auto().set("a", true);
    assert!(e.rc.quota_auto().is_on("a"));
    e.rc.forget("a");
    assert!(!e.rc.quota_auto().is_on("a"));
}

#[tokio::test]
async fn quota_shows_credits_and_estimate_reset_round_trip() {
    let e = env(false).await;
    e.mock.usage.lock().unwrap().push_back(json!({
        "plan_type": "pro",
        "rate_limit": {"limit_reached": true, "primary_window":
            {"used_percent": 100, "limit_window_seconds": 604800, "reset_after_seconds": 86400}},
        "credits": {"has_credits": true, "unlimited": false, "balance": "812.5"}
    }));
    let post = |path: String, body: Value| {
        e.http
            .post(format!("{}{path}", e.base))
            .bearer_auth(KEY)
            .json(&body)
            .send()
    };
    let r: Value = post("/admin/v1/accounts/a/quota-refresh".into(), json!({}))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["ok"], true, "{r}");
    let mut credits = Value::Null;
    for _ in 0..50 {
        let q = e.get("/usage/v1/quota").await;
        credits = q["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|x| x["quota"]["observed_by"] == "manual")
            .map(|x| x["quota"]["credits"].clone())
            .unwrap_or(Value::Null);
        if !credits.is_null() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(credits["has_credits"], true, "{credits}");
    assert_eq!(credits["unlimited"], false, "{credits}");
    assert_eq!(credits["balance"], 812.5, "{credits}");

    let url = |acct: &str| format!("/admin/v1/accounts/{acct}/quota-estimate/reset");
    let status = |r: reqwest::Response| r.status().as_u16();
    // Confirmation, window and JSON are required; unknown accounts are refused.
    for (acct, body, want) in [
        ("a", json!({"window": "primary", "confirm": "b"}), 400),
        ("a", json!({"window": "monthly", "confirm": "a"}), 400),
        ("a", json!({"confirm": "a"}), 422),
        ("zz", json!({"window": "all", "confirm": "zz"}), 404),
    ] {
        assert_eq!(status(post(url(acct), body).await.unwrap()), want, "{acct}");
    }
    let s = e
        .http
        .post(format!("{}{}", e.base, url("a")))
        .bearer_auth(KEY)
        .header("content-type", "text/plain")
        .body(r#"{"window":"all","confirm":"a"}"#)
        .send()
        .await
        .unwrap();
    assert!(matches!(s.status().as_u16(), 400 | 415), "{}", s.status());
    let s = e
        .http
        .post(format!("{}{}", e.base, url("a")))
        .json(&json!({"window": "all", "confirm": "a"}))
        .send()
        .await
        .unwrap();
    assert_eq!(s.status(), 401);
    // b is configured but never observed: nothing to reset.
    assert_eq!(
        status(
            post(url("b"), json!({"window": "all", "confirm": "b"}))
                .await
                .unwrap()
        ),
        404
    );

    let done: Value = post(url("a"), json!({"window": "primary", "confirm": "a"}))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(done["windows"], json!(["primary"]), "{done}");
    let epoch = done["epoch_id"].as_i64().unwrap();
    let h = e.get("/usage/v1/quota/history").await;
    let ep = &h["accounts"][0]["epochs"][0];
    assert_eq!(ep["id"], epoch, "{h}");
    assert_eq!(ep["active"], true, "{h}");
    let q = e.get("/usage/v1/quota").await;
    // The test's per-process alias may differ from the recorder's, so match by observer.
    let a = q["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["quota"]["observed_by"] == "manual")
        .unwrap();
    assert_eq!(
        a["quota"]["primary"]["estimate"]["since_ms"], done["from_ms"],
        "{a}"
    );

    let undo = format!("/admin/v1/accounts/a/quota-estimate/reset/{epoch}/undo");
    let r: Value = post(undo.clone(), json!({}))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["undone"], 1, "{r}");
    assert_eq!(status(post(undo, json!({})).await.unwrap()), 404);
    let h = e.get("/usage/v1/quota/history").await;
    assert_eq!(h["accounts"][0]["epochs"][0]["active"], false, "{h}");
}
