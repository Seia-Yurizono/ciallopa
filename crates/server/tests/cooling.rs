//! Every account cooling down: short cooldowns are held and routed once one recovers; longer
//! ones are answered at once (503 without Retry-After, or `usage_limit_reached` when the first
//! account to recover is out of quota), on both the HTTP and the WebSocket path.
#![allow(clippy::unwrap_used)]
use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::{body::Body, response::Response, routing::post, Router};
use ciallopa_core::*;
use ciallopa_forward::AppState;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

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

/// Every account cools until `free_at`; `earliest_quota` = the first to recover is out of quota.
struct Cool {
    free_at: Mutex<Instant>,
    earliest_quota: bool,
    selects: AtomicUsize,
}
impl ciallopa_core::Router for Cool {
    fn select(&self, _: &RouteRequest, _: &[Account]) -> Result<Selection, RouteError> {
        self.selects.fetch_add(1, Ordering::SeqCst);
        let free = *self.free_at.lock().unwrap();
        let now = Instant::now();
        if now < free {
            return Err(RouteError::AllCoolingDown {
                retry_after: free - now,
                quota: false,
                earliest_quota: self.earliest_quota,
            });
        }
        Ok(Selection {
            account: "a".into(),
            from_binding: false,
        })
    }
    fn report(&self, _: &RouteRequest, _: &AccountId, _: &Outcome) {}
}

struct Env {
    proxy: String,
    router: Arc<Cool>,
    hits: Arc<AtomicUsize>,
}

async fn serve(app: Router) -> String {
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ln.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(ln, app).await.unwrap() });
    format!("127.0.0.1:{}", addr.port())
}

async fn env(cool_for: Duration, earliest_quota: bool, wait_secs: u64) -> Env {
    let hits = Arc::new(AtomicUsize::new(0));
    let (h1, h2) = (hits.clone(), hits.clone());
    let up = Router::new().route(
        "/backend-api/codex/responses",
        post(move || async move {
            h1.fetch_add(1, Ordering::SeqCst);
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from(concat!(
                    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\"}}\n\n",
                    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\"}}\n\n"
                )))
                .unwrap()
        })
        .get(move |ws: WebSocketUpgrade| async move {
            h2.fetch_add(1, Ordering::SeqCst);
            ws.on_upgrade(|mut s| async move {
                while let Some(Ok(m)) = s.recv().await {
                    if let Message::Text(_) = m {
                        let _ = s
                            .send(Message::Text(
                                r#"{"type":"response.completed","response":{"id":"r"}}"#.into(),
                            ))
                            .await;
                    }
                }
            })
        }),
    );
    let up_addr = serve(up).await;
    let cfg = Config::parse(&format!(
        "api_keys = [\"{KEY}\"]\nupstream = \"http://{up_addr}/backend-api\"\n[routing]\ncooldown_wait_secs = {wait_secs}\n"
    ))
    .unwrap();
    let router = Arc::new(Cool {
        free_at: Mutex::new(Instant::now() + cool_for),
        earliest_quota,
        selects: AtomicUsize::new(0),
    });
    let state = AppState {
        accounts: Arc::new(Store),
        router: router.clone(),
        upstream: {
            let up = ciallopa_upstream::UpstreamClient::new(&cfg).unwrap();
            up.pools.allow_any_cookie_host_for_tests();
            Arc::new(up)
        },
        usage: None,
        config: Arc::new(cfg.into()),
        egress: Default::default(),
    };
    let proxy = serve(ciallopa::build_app(state)).await;
    Env {
        proxy,
        router,
        hits,
    }
}

async fn post_http(e: &Env) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{}/v1/responses", e.proxy))
        .bearer_auth(KEY)
        .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
        .send()
        .await
        .unwrap()
}

async fn ws(e: &Env) -> Result<u16, (u16, Option<String>, String)> {
    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    match tokio_tungstenite::connect_async(req).await {
        Ok((_, r)) => Ok(r.status().as_u16()),
        Err(tokio_tungstenite::tungstenite::Error::Http(r)) => Err((
            r.status().as_u16(),
            r.headers()
                .get("retry-after")
                .map(|v| v.to_str().unwrap().to_string()),
            String::from_utf8(r.body().clone().unwrap_or_default()).unwrap(),
        )),
        Err(e) => panic!("{e}"),
    }
}

#[tokio::test]
async fn short_cooldown_is_held_then_routed_http() {
    let e = env(Duration::from_millis(1200), false, 20).await;
    let t = Instant::now();
    let r = post_http(&e).await;
    assert_eq!(r.status(), 200);
    assert!(r.text().await.unwrap().contains("response.completed"));
    assert!(
        t.elapsed() >= Duration::from_millis(1150),
        "{:?}",
        t.elapsed()
    );
    assert_eq!(e.hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn short_cooldown_is_held_then_routed_ws() {
    let e = env(Duration::from_millis(1200), false, 20).await;
    let t = Instant::now();
    assert_eq!(ws(&e).await.unwrap(), 101);
    assert!(t.elapsed() >= Duration::from_millis(1150));
    assert_eq!(e.hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn long_cooldown_answers_503_without_retry_after() {
    let e = env(Duration::from_secs(1800), false, 20).await;
    let t = Instant::now();
    let r = post_http(&e).await;
    assert_eq!(r.status(), 503);
    assert!(r.headers().get("retry-after").is_none());
    let b = r.text().await.unwrap();
    assert!(b.contains("server_is_overloaded"), "{b}");
    assert!(!b.to_lowercase().contains("ciallopa") && !b.contains("account"));
    let (st, ra, b) = ws(&e).await.unwrap_err();
    assert_eq!((st, ra), (503, None));
    assert!(b.contains("server_is_overloaded"));
    assert!(t.elapsed() < Duration::from_secs(2));
    assert_eq!(e.hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn first_recovery_out_of_quota_answers_usage_limit() {
    let e = env(Duration::from_secs(3600), true, 20).await;
    let r = post_http(&e).await;
    assert_eq!(r.status(), 429);
    assert!(r.headers().get("retry-after").is_none());
    let v: serde_json::Value = r.json().await.unwrap();
    assert_eq!(v["error"]["type"], "usage_limit_reached");
    assert!(v["error"].get("plan_type").is_none());
    let secs = v["error"]["resets_in_seconds"].as_u64().unwrap();
    assert!((3590..=3600).contains(&secs), "{secs}");
    let (st, ra, b) = ws(&e).await.unwrap_err();
    assert_eq!((st, ra), (429, None));
    assert!(b.contains("usage_limit_reached"));
}

#[tokio::test]
async fn ws_handshake_beyond_hold_gets_short_retry_after() {
    // 12 s: within the 20 s wait but longer than a handshake may be held.
    let e = env(Duration::from_secs(12), false, 20).await;
    let t = Instant::now();
    let (st, ra, _) = ws(&e).await.unwrap_err();
    assert_eq!(st, 503);
    let ra: u64 = ra.unwrap().parse().unwrap();
    assert!((12..=13).contains(&ra), "{ra}");
    assert!(t.elapsed() < Duration::from_secs(2));
    // HTTP holds the full wait instead.
    *e.router.free_at.lock().unwrap() = Instant::now() + Duration::from_millis(300);
    assert_eq!(post_http(&e).await.status(), 200);
}

#[tokio::test]
async fn zero_wait_answers_at_once() {
    let e = env(Duration::from_secs(2), false, 0).await;
    let t = Instant::now();
    let r = post_http(&e).await;
    assert_eq!(r.status(), 503);
    assert!(r.headers().get("retry-after").is_none());
    assert!(t.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn client_disconnect_ends_the_hold() {
    let e = env(Duration::from_secs(2), false, 20).await;
    let c = reqwest::Client::builder()
        .timeout(Duration::from_millis(300))
        .build()
        .unwrap();
    let r = c
        .post(format!("http://{}/v1/responses", e.proxy))
        .bearer_auth(KEY)
        .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
        .send()
        .await;
    assert!(r.is_err());
    tokio::time::sleep(Duration::from_secs(3)).await;
    // One selection (cooling) and no second one after the cooldown ended.
    assert_eq!(e.router.selects.load(Ordering::SeqCst), 1);
    assert_eq!(e.hits.load(Ordering::SeqCst), 0);
}
