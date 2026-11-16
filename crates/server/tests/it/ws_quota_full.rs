//! A WebSocket pinned to an account whose window is full (the upstream keeps serving from
//! purchased credits) moves to an account with quota at the start of the next turn.
#![allow(clippy::unwrap_used)]
use super::support::{proxy_state, serve, FixedStore as Store};
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::{routing::get, Router};
use ciallopa_core::Router as _;
use ciallopa_core::*;
use futures::{SinkExt, StreamExt};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message as C;

const KEY: &str = "sk-cpa-0123456789abcdef";
const MODEL: &str = "gpt-5.5";

type Log = Arc<Mutex<Vec<(String, &'static str)>>>;

async fn env(ids: Vec<&'static str>) -> (String, Arc<ciallopa_routing::CpaRouter>, Log) {
    let log: Log = Default::default();
    let l = log.clone();
    let up = Router::new().route(
        "/backend-api/codex/responses",
        get(
            move |h: axum::http::HeaderMap, ws: WebSocketUpgrade| async move {
                let a = h
                    .get("chatgpt-account-id")
                    .map(|v| v.to_str().unwrap().to_string())
                    .unwrap_or_default();
                l.lock().unwrap().push((a.clone(), "handshake"));
                ws.on_upgrade(move |mut s| async move {
                    while let Some(Ok(m)) = s.recv().await {
                        if let Message::Text(_) = m {
                            l.lock().unwrap().push((a.clone(), "turn"));
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
        "api_keys = [\"{KEY}\"]\nupstream = \"http://{up_addr}/backend-api\"\n"
    ))
    .unwrap();
    let router = Arc::new(ciallopa_routing::CpaRouter::new(cfg.routing.clone()));
    let state = proxy_state(cfg, Arc::new(Store::new(&ids)), router.clone(), None);
    (serve(ciallopa::build_app(state)).await, router, log)
}

fn full(r: &ciallopa_routing::CpaRouter, id: &str) {
    let req = RouteRequest {
        model: MODEL.into(),
        ..Default::default()
    };
    let q = QuotaSnapshot {
        primary: Some(QuotaWindow {
            used_percent: 100.0,
            window_minutes: Some(10080),
            resets_at: Some(SystemTime::now() + Duration::from_secs(86_400)),
        }),
        secondary: None,
    };
    r.report(&req, &id.to_string(), &Outcome::Success { quota: Some(q) });
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(proxy: &str) -> Ws {
    let mut req = format!("ws://{proxy}/v1/responses")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    tokio_tungstenite::connect_async(req).await.unwrap().0
}

async fn turn(s: &mut Ws, extra: &str) -> String {
    s.send(C::Text(
        format!(r#"{{"type":"response.create","model":"{MODEL}"{extra},"input":[]}}"#).into(),
    ))
    .await
    .unwrap();
    match tokio::time::timeout(Duration::from_secs(15), s.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    {
        C::Close(Some(f)) => format!("close {}", u16::from(f.code)),
        m => m.to_text().unwrap().to_string(),
    }
}

fn turns(log: &Log) -> Vec<String> {
    log.lock()
        .unwrap()
        .iter()
        .filter(|x| x.1 == "turn")
        .map(|x| x.0.clone())
        .collect()
}

#[tokio::test]
async fn ws_full_turn_moves_to_an_account_with_quota() {
    let (proxy, router, log) = env(vec!["a", "b"]).await;
    let mut s = connect(&proxy).await;
    assert!(turn(&mut s, "").await.contains("response.completed"));
    let pinned = turns(&log)[0].trim_start_matches("acct-").to_string();
    let other = if pinned == "a" { "b" } else { "a" };
    full(&router, &pinned);
    assert!(turn(&mut s, "").await.contains("response.completed"));
    assert_eq!(
        turns(&log),
        [format!("acct-{pinned}"), format!("acct-{other}")]
    );
    // Stays on the new account for the following turns.
    assert!(turn(&mut s, "").await.contains("response.completed"));
    assert_eq!(turns(&log).last().unwrap(), &format!("acct-{other}"));
}

#[tokio::test]
async fn ws_full_incremental_turn_closes_for_replay() {
    let (proxy, router, log) = env(vec!["a", "b"]).await;
    let mut s = connect(&proxy).await;
    assert!(turn(&mut s, "").await.contains("response.completed"));
    let pinned = turns(&log)[0].trim_start_matches("acct-").to_string();
    full(&router, &pinned);
    // previous_response_id lives on this connection only: the client replays elsewhere.
    let r = turn(&mut s, r#","previous_response_id":"resp_1""#).await;
    assert_eq!(r, "close 1012");
    assert_eq!(turns(&log).len(), 1, "nothing sent to the full account");
}

#[tokio::test]
async fn ws_stays_when_every_account_is_full() {
    let (proxy, router, log) = env(vec!["a", "b"]).await;
    let mut s = connect(&proxy).await;
    assert!(turn(&mut s, "").await.contains("response.completed"));
    full(&router, "a");
    full(&router, "b");
    let r = turn(&mut s, r#","previous_response_id":"resp_1""#).await;
    assert!(
        r.contains("response.completed"),
        "credits are the last resort: {r}"
    );
    let t = turns(&log);
    assert_eq!(t.len(), 2);
    assert_eq!(t[0], t[1]);
}
