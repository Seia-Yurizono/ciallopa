//! WebSocket turns honour per-model cooldowns: the handshake has no model, so each
//! `response.create` re-checks its model on the pinned account (review item 16).
#![allow(clippy::unwrap_used)]
use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::{body::Body, response::Response, routing::post, Router};
use ciallopa_core::Router as _;
use ciallopa_core::*;
use ciallopa_forward::AppState;
use futures::{SinkExt, StreamExt};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

const KEY: &str = "sk-cpa-0123456789abcdef";
const MODEL: &str = "gpt-5.5";

struct Store(Vec<&'static str>);
#[async_trait]
impl AccountStore for Store {
    async fn list(&self) -> Vec<Account> {
        self.0
            .iter()
            .map(|id| Account {
                id: (*id).into(),
                email: None,
                chatgpt_account_id: Some(format!("acct-{id}")),
                plan_type: None,
                priority: 0,
                weight: 1,
                disabled: false,
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

type Log = Arc<Mutex<Vec<(String, &'static str, Duration)>>>;

async fn serve(app: Router) -> String {
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ln.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(ln, app).await.unwrap() });
    format!("127.0.0.1:{}", addr.port())
}

fn acct(h: &axum::http::HeaderMap) -> String {
    h.get("chatgpt-account-id")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default()
}

async fn env(ids: Vec<&'static str>) -> (String, Arc<ciallopa_routing::CpaRouter>, Log, Instant) {
    let t0 = Instant::now();
    let log: Log = Default::default();
    let (l1, l2) = (log.clone(), log.clone());
    let up = Router::new().route(
        "/backend-api/codex/responses",
        post(move |h: axum::http::HeaderMap| async move {
            l1.lock().unwrap().push((acct(&h), "http", t0.elapsed()));
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from(concat!(
                    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\"}}\n\n",
                    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\"}}\n\n"
                )))
                .unwrap()
        })
        .get(
            move |h: axum::http::HeaderMap, ws: WebSocketUpgrade| async move {
                let a = acct(&h);
                l2.lock()
                    .unwrap()
                    .push((a.clone(), "ws-handshake", t0.elapsed()));
                ws.on_upgrade(move |mut s| async move {
                    while let Some(Ok(m)) = s.recv().await {
                        if let Message::Text(_) = m {
                            l2.lock()
                                .unwrap()
                                .push((a.clone(), "ws-turn", t0.elapsed()));
                            let _ = s
                                .send(Message::Text(
                                    r#"{"type":"response.completed","response":{"id":"r"}}"#.into(),
                                ))
                                .await;
                        }
                    }
                })
            },
        ),
    );
    let up_addr = serve(up).await;
    let cfg = Config::parse(&format!(
        "api_keys = [\"{KEY}\"]\nupstream = \"http://{up_addr}/backend-api\"\n[routing]\ncooldown_wait_secs = 20\n"
    ))
    .unwrap();
    let router = Arc::new(ciallopa_routing::CpaRouter::new(cfg.routing.clone()));
    let state = AppState {
        accounts: Arc::new(Store(ids)),
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
    (serve(ciallopa::build_app(state)).await, router, log, t0)
}

/// A per-model cooldown on `id` for MODEL, of `d` (404 not-found cooldown: model-scoped).
fn cool_model(r: &ciallopa_routing::CpaRouter, id: &str, d: Duration) {
    let req = RouteRequest {
        model: MODEL.into(),
        session: Default::default(),
        exclude: vec![],
        attempt_at: None,
    };
    r.report(
        &req,
        &id.to_string(),
        &Outcome::NotFound {
            retry_after: None,
            cooldown: d,
        },
    );
}

async fn http(proxy: &str) -> u16 {
    reqwest::Client::new()
        .post(format!("http://{proxy}/v1/responses"))
        .bearer_auth(KEY)
        .body(format!(r#"{{"model":"{MODEL}","stream":true,"input":[]}}"#))
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

async fn ws_turn(proxy: &str) -> (u16, String) {
    ws_turn_with(proxy, "").await
}

async fn ws_turn_with(proxy: &str, extra: &str) -> (u16, String) {
    let mut req = format!("ws://{proxy}/v1/responses")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut s, r) = tokio_tungstenite::connect_async(req).await.unwrap();
    s.send(tokio_tungstenite::tungstenite::Message::Text(
        format!(r#"{{"type":"response.create","model":"{MODEL}"{extra},"input":[]}}"#).into(),
    ))
    .await
    .unwrap();
    let m = tokio::time::timeout(Duration::from_secs(15), s.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let text = match m {
        tokio_tungstenite::tungstenite::Message::Close(Some(f)) => {
            format!("close {}", u16::from(f.code))
        }
        m => m.to_text().unwrap().to_string(),
    };
    (r.status().as_u16(), text)
}

fn turns(log: &Log, from: usize) -> Vec<String> {
    log.lock().unwrap()[from..]
        .iter()
        .filter(|x| x.1 == "ws-turn")
        .map(|x| x.0.clone())
        .collect()
}

#[tokio::test]
async fn ws_turn_moves_off_an_account_cooling_for_its_model() {
    let (proxy, router, log, _) = env(vec!["a", "b"]).await;
    for _ in 0..4 {
        cool_model(&router, "a", Duration::from_secs(60));
        let n = log.lock().unwrap().len();
        let (st, first) = ws_turn(&proxy).await;
        assert_eq!(st, 101);
        assert!(first.contains("response.completed"), "{first}");
        assert_eq!(
            turns(&log, n),
            ["acct-b"],
            "{:?}",
            &log.lock().unwrap()[n..]
        );
    }
    // Another model is not affected by the cooldown: no redial is needed for it.
    assert!(router
        .cooldown_until(&"a".to_string(), "gpt-other")
        .is_none());
}

#[tokio::test]
async fn ws_turn_waits_out_a_short_model_cooldown_on_the_only_account() {
    let (proxy, router, log, _) = env(vec!["a"]).await;
    cool_model(&router, "a", Duration::from_secs(2));
    let t = Instant::now();
    let (st, first) = ws_turn(&proxy).await;
    assert_eq!(st, 101);
    assert!(first.contains("response.completed"), "{first}");
    // Like HTTP: held until the cooldown ends, then sent on the (re-dialled) account.
    assert!(
        t.elapsed() >= Duration::from_millis(1500),
        "{:?}",
        t.elapsed()
    );
    assert_eq!(turns(&log, 0), ["acct-a"]);
}

#[tokio::test]
async fn ws_turn_reports_a_long_model_cooldown_like_http_without_sending() {
    let (proxy, router, log, _) = env(vec!["a"]).await;
    cool_model(&router, "a", Duration::from_secs(60));
    let code = http(&proxy).await;
    let (st, first) = ws_turn(&proxy).await;
    assert_eq!(st, 101);
    let v: serde_json::Value = serde_json::from_str(&first).unwrap();
    assert_eq!(v["type"], "error", "{first}");
    assert_eq!(v["status"], code, "same status as HTTP: {first}");
    assert!(
        turns(&log, 0).is_empty(),
        "nothing reached the cooling account"
    );
}

#[tokio::test]
async fn ws_incremental_turn_on_a_cooling_account_closes_for_replay() {
    let (proxy, router, log, _) = env(vec!["a", "b"]).await;
    // Pin the next handshake to `a` and cool it for the model afterwards: the turn's
    // previous_response_id lives on this connection only, so the client must replay.
    cool_model(&router, "b", Duration::from_secs(60));
    cool_model(&router, "a", Duration::from_secs(60));
    let (st, first) = ws_turn_with(&proxy, r#","previous_response_id":"resp_1""#).await;
    assert_eq!(st, 101);
    assert_eq!(first, "close 1012");
    assert!(turns(&log, 0).is_empty());
}
