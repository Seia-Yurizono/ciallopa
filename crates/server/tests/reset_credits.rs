//! Reset credits (重置卡): listing, manual consume (each result code), idempotent retry,
//! disabled switch, CSRF guards, and "never automatic" against a mock upstream.
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

async fn up_usage() -> Json<Value> {
    Json(json!({"rate_limit": {"primary_window": {"used_percent": 0}}}))
}

struct Env {
    base: String,
    mock: Mock,
    router: Arc<NoRoute>,
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
         [usage]\npath = \"{}\"\n[ui]\nallow_reset_consume = {allow}\nreset_credits_refresh_mins = 0\n",
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
    let p = serve(ciallopa::build_app(state)).await;
    Env {
        base: format!("http://{p}"),
        mock,
        router,
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
async fn guards_confirm_key_json_cookie() {
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
    // cookies rejected
    let s = e
        .http
        .post(&url)
        .bearer_auth(KEY)
        .header("cookie", "x=1")
        .json(&json!({"confirm": "a"}))
        .send()
        .await
        .unwrap();
    assert_eq!(s.status(), 400);
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
    // cookie
    let r = e
        .http
        .post(&url)
        .bearer_auth(KEY)
        .header("cookie", "x=1")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert!(e.mock.lists.lock().unwrap().is_empty());
}

#[test]
fn no_automatic_consume_path() {
    // The upstream consume helper has exactly one caller: the manual admin handler.
    let src = include_str!("../src/reset_credits.rs");
    assert_eq!(
        src.matches("consume_upstream(").count(),
        2,
        "definition + one call"
    );
    let handler = src.split("async fn consume(").nth(1).unwrap();
    assert!(handler.contains("consume_upstream(&s.st"));
    for f in ["refresh_all", "spawn_poller", "refresh_account"] {
        let body = src.split(&format!("fn {f}(")).nth(1).unwrap();
        let body = &body[..body.find("\n}\n").unwrap()];
        assert!(
            !body.contains("CONSUME_PATH") && !body.contains("consume_upstream"),
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
        assert!(!other.contains("reset-credits/consume"));
    }
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
