//! Learned client fingerprint: validation, pairing, HTTP + WS learning, use by internal
//! upstream requests (reset-credit list, token refresh), persistence across restart (usage DB
//! and state file). One test: the fingerprint is process-global.
#![allow(clippy::unwrap_used)]
use async_trait::async_trait;
use axum::{
    body::Body,
    http::HeaderMap,
    response::Response,
    routing::{get, post},
    Json, Router,
};
use ciallopa_core::*;
use ciallopa_forward::AppState;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const KEY: &str = "sk-cpa-0123456789abcdef";

struct Store;
#[async_trait]
impl AccountStore for Store {
    async fn list(&self) -> Vec<Account> {
        vec![Account {
            id: "a".into(),
            email: None,
            chatgpt_account_id: Some("acct-a".into()),
            plan_type: None,
            priority: 0,
            weight: 1,
            disabled: false,
        }]
    }
    async fn credentials(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        Ok(Credentials {
            account: id.clone(),
            access_token: "tok-a".into(),
            chatgpt_account_id: Some("acct-a".into()),
        })
    }
    async fn force_refresh(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        self.credentials(id).await
    }
    async fn reload(&self) -> Result<(), AuthError> {
        Ok(())
    }
}

type Seen = Arc<Mutex<Vec<(String, Option<String>, Option<String>)>>>;

fn rec(s: &Seen, what: &str, h: &HeaderMap) {
    let g = |k: &str| h.get(k).and_then(|v| v.to_str().ok()).map(str::to_string);
    s.lock()
        .unwrap()
        .push((what.into(), g("user-agent"), g("originator")));
}

async fn serve(app: Router) -> std::net::SocketAddr {
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = ln.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(ln, app).await.unwrap() });
    a
}

fn last(s: &Seen, what: &str) -> (Option<String>, Option<String>) {
    let v = s.lock().unwrap();
    let x = v.iter().rev().find(|x| x.0 == what).unwrap();
    (x.1.clone(), x.2.clone())
}

async fn wait_persisted(f: impl Fn() -> bool) {
    for _ in 0..100 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("not persisted");
}

#[tokio::test]
async fn learned_fingerprint_end_to_end() {
    fingerprint::reset_for_tests();
    let seen: Seen = Default::default();
    let (s1, s2, s3) = (seen.clone(), seen.clone(), seen.clone());
    let up = Router::new()
        .route(
            "/backend-api/codex/responses",
            post(move |h: HeaderMap| async move {
                rec(&s1, "fwd", &h);
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from("data: {\"type\":\"response.completed\"}\n\n"))
                    .unwrap()
            }),
        )
        .route(
            "/backend-api/wham/rate-limit-reset-credits",
            get(move |h: HeaderMap| async move {
                rec(&s2, "list", &h);
                Json(json!({"available_count": 0, "credits": []}))
            }),
        )
        .route(
            "/oauth/token",
            post(move |h: HeaderMap| async move {
                rec(&s3, "refresh", &h);
                Json(json!({"access_token": "x", "refresh_token": "y", "id_token": "z"}))
            }),
        );
    let upa = serve(up).await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = Config::parse(&format!(
        "api_keys = [{{label = \"laptop\", key = \"{KEY}\"}}]\nupstream = \"http://{upa}/backend-api\"\n\
         [usage]\npath = \"{}\"\n[ui]\nreset_credits_refresh_mins = 0\n",
        dir.path().join("u.db").display()
    ))
    .unwrap();
    let usage = ciallopa_usage::Recorder::open_as(&cfg.usage, "i").unwrap();
    ciallopa::client_fp::install(&cfg, Some(usage.clone()));
    let state = AppState {
        accounts: Arc::new(Store),
        router: Arc::new(ciallopa_routing::CpaRouter::new(cfg.routing.clone())),
        upstream: Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap()),
        usage: Some(usage.clone()),
        config: Arc::new(cfg.clone()),
    };
    let p = serve(ciallopa::build_app(state)).await;
    let http = reqwest::Client::new();
    let oauth =
        ciallopa_auth::OAuthClient::with_issuer(&format!("http://{upa}"), reqwest::Client::new());
    let fp = || async {
        http.get(format!("http://{p}/usage/v1/client-fingerprint"))
            .bearer_auth(KEY)
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()
    };
    let refresh_list = || async {
        http.post(format!("http://{p}/usage/v1/reset-credits/refresh"))
            .bearer_auth(KEY)
            .send()
            .await
            .unwrap();
    };
    let fwd = |ua: &'static str, orig: Option<&'static str>| {
        let http = http.clone();
        async move {
            let mut rb = http
                .post(format!("http://{p}/v1/responses"))
                .bearer_auth(KEY)
                .header("user-agent", ua)
                .body("{}");
            if let Some(o) = orig {
                rb = rb.header("originator", o);
            }
            rb.send().await.unwrap().text().await.unwrap();
        }
    };

    // 1. default before anything is learned
    let v = fp().await;
    assert_eq!(v["source"], "default");
    assert_eq!(v["user_agent"], "codex_cli_rs/0.162.1");
    refresh_list().await;
    assert_eq!(
        last(&seen, "list"),
        (
            Some("codex_cli_rs/0.162.1".into()),
            Some("codex_cli_rs".into())
        )
    );

    // 2. rejected: curl, third party, pair mismatch, missing originator, dev build
    fwd("curl/8.5.0", None).await;
    fwd("Mozilla/5.0", Some("codex_cli_rs")).await;
    fwd("codex-tui/0.170.0 (Linux; x86_64)", Some("codex_cli_rs")).await;
    fwd("codex_cli_rs/0.170.0 (Linux; x86_64)", None).await;
    fwd("codex_cli_rs/0.0.0 (dev)", Some("codex_cli_rs")).await;
    fwd("codex_exec/0.170.0", Some("codex_exec")).await;
    assert_eq!(fp().await["source"], "default");
    // forwarded requests themselves are untouched (client UA passes through)
    assert_eq!(last(&seen, "fwd").0.as_deref(), Some("codex_exec/0.170.0"));
    // unauthenticated requests never teach
    http.post(format!("http://{p}/v1/responses"))
        .header("user-agent", "codex_cli_rs/9.9.9")
        .header("originator", "codex_cli_rs")
        .send()
        .await
        .unwrap();
    assert_eq!(fp().await["source"], "default");

    // 3. learned over HTTP, used by internal requests
    let ua1 = "codex_cli_rs/0.170.0 (Ubuntu 24.4.0; x86_64) xterm-256color";
    fwd(ua1, Some("codex_cli_rs")).await;
    let v = fp().await;
    assert_eq!(v["source"], "learned");
    assert_eq!(v["user_agent"], ua1);
    assert_eq!(v["originator"], "codex_cli_rs");
    assert_eq!(v["key"], "laptop");
    assert!(v["learned_at_ms"].as_i64().unwrap() > 0);
    refresh_list().await;
    assert_eq!(
        last(&seen, "list"),
        (Some(ua1.into()), Some("codex_cli_rs".into()))
    );
    oauth.refresh("rt").await.unwrap();
    assert_eq!(
        last(&seen, "refresh"),
        (Some(ua1.into()), Some("codex_cli_rs".into()))
    );

    // 4. learned over a WS handshake (pair switches together)
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let ua2 = "codex-tui/0.171.0 (Mac OS 15.1.0; arm64) iTerm.app/3.5";
    let mut req = format!("ws://{p}/v1/responses")
        .into_client_request()
        .unwrap();
    let h = req.headers_mut();
    h.insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    h.insert("user-agent", ua2.parse().unwrap());
    h.insert("originator", "codex-tui".parse().unwrap());
    let _ = tokio_tungstenite::connect_async(req).await; // upstream has no WS: fine
    let v = fp().await;
    assert_eq!(v["user_agent"], ua2);
    assert_eq!(v["originator"], "codex-tui");
    refresh_list().await;
    assert_eq!(
        last(&seen, "list"),
        (Some(ua2.into()), Some("codex-tui".into()))
    );

    // 5. persisted in the usage DB; restored after a restart
    wait_persisted(|| {
        usage
            .kv_get("client_fingerprint")
            .unwrap()
            .is_some_and(|s| s.contains("codex-tui/0.171.0"))
    })
    .await;
    fingerprint::reset_for_tests();
    assert_eq!(fingerprint::current().source, "default");
    let usage2 = ciallopa_usage::Recorder::open_as(&cfg.usage, "i").unwrap();
    ciallopa::client_fp::install(&cfg, Some(usage2));
    let f = fingerprint::current();
    assert_eq!((f.user_agent.as_str(), f.source.as_str()), (ua2, "learned"));

    // 6. usage log disabled → state file next to the usage path
    fingerprint::reset_for_tests();
    let mut cfg2 = cfg.clone();
    cfg2.usage.enabled = false;
    cfg2.usage.path = dir.path().join("off").join("u.db");
    ciallopa::client_fp::install(&cfg2, None);
    let mut h = HeaderMap::new();
    h.insert("user-agent", ua1.parse().unwrap());
    h.insert("originator", "codex_cli_rs".parse().unwrap());
    assert!(fingerprint::observe(&h, None));
    let file = dir.path().join("off").join("client_fingerprint.json");
    wait_persisted(|| file.exists()).await;
    fingerprint::reset_for_tests();
    ciallopa::client_fp::install(&cfg2, None);
    assert_eq!(fingerprint::current().user_agent, ua1);
    // a tampered state file is ignored (re-validated on restore)
    std::fs::write(&file, r#"{"user_agent":"curl/8","originator":"x","source":"learned","learned_at_ms":1,"key":null}"#).unwrap();
    fingerprint::reset_for_tests();
    ciallopa::client_fp::install(&cfg2, None);
    assert_eq!(fingerprint::current().source, "default");
    fingerprint::reset_for_tests();
}
