//! Account switches belong to one turn: a later turn may go back to an account the
//! connection left, while a client `x-codex-turn-state` of unknown owner never follows the
//! connection to another account. Failover checks the same eligibility as selection.
#![allow(clippy::unwrap_used)]
use super::support::{proxy_state, serve, units, FixedStore as Store};
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::{body::Body, response::Response, routing::post, Router};
use ciallopa_core::Router as _;
use ciallopa_core::*;
use futures::{SinkExt, StreamExt};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message as C;

const KEY: &str = "sk-cpa-0123456789abcdef";
const MODEL: &str = "gpt-5.5";
const TURN_STATE: &str = "x-codex-turn-state";

#[derive(Debug, Clone)]
struct Hit {
    account: String,
    kind: &'static str,
    turn_state: Option<String>,
    at: Duration,
}

type Log = Arc<Mutex<Vec<Hit>>>;

struct Env {
    proxy: String,
    router: Arc<ciallopa_routing::CpaRouter>,
    log: Log,
    t0: Instant,
}

impl Env {
    fn hits(&self, kind: &str) -> Vec<Hit> {
        let log = self.log.lock().unwrap();
        log.iter().filter(|h| h.kind == kind).cloned().collect()
    }
    fn accounts(&self, kind: &str) -> Vec<String> {
        self.hits(kind).into_iter().map(|h| h.account).collect()
    }
}

fn header(h: &axum::http::HeaderMap, name: &str) -> Option<String> {
    h.get(name).map(|v| v.to_str().unwrap().to_string())
}

/// `a` for `acct-a`.
fn account(h: &axum::http::HeaderMap) -> String {
    header(h, "chatgpt-account-id")
        .unwrap_or_default()
        .trim_start_matches("acct-")
        .to_string()
}

/// Frames of one upstream turn: the message's `tag` lists `kind@account` for the accounts
/// that fail it (`over`: overloaded, `quota`: usage limit, `late`: a delta, then usage
/// limit); any other account completes it.
fn frames(tag: &str, account: &str) -> Vec<String> {
    const USAGE_LIMIT: &str = r#"{"type":"error","status":429,"error":{"type":"usage_limit_reached","resets_in_seconds":45}}"#;
    let created = format!(r#"{{"type":"response.created","response":{{"id":"r-{account}"}}}}"#);
    let delta = r#"{"type":"response.output_text.delta","delta":"ok"}"#.to_string();
    let kind = tag
        .split_whitespace()
        .find_map(|t| t.strip_suffix(&format!("@{account}")));
    match kind {
        Some("over") => vec![
            created,
            r#"{"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"overloaded"}}"#.into(),
        ],
        Some("quota") => vec![created, USAGE_LIMIT.into()],
        Some("late") => vec![delta, USAGE_LIMIT.into()],
        _ => vec![
            created,
            delta,
            format!(r#"{{"type":"response.completed","response":{{"id":"r-{account}"}}}}"#),
        ],
    }
}

/// HTTP answers overloaded for the accounts in `x-test-over`. A WS handshake answers 402
/// (deactivated workspace) for the accounts in `x-test-refuse`, and with `x-test-issue: <p>`
/// hands out the turn state `<p>-<account>`.
async fn env(ids: &[&str], extra: &str) -> Env {
    let t0 = Instant::now();
    let log: Log = Default::default();
    let (l1, l2) = (log.clone(), log.clone());
    let hit = move |log: &Log, h: &axum::http::HeaderMap, kind| {
        log.lock().unwrap().push(Hit {
            account: account(h),
            kind,
            turn_state: header(h, TURN_STATE),
            at: t0.elapsed(),
        })
    };
    let up = Router::new().route(
        "/backend-api/codex/responses",
        post(move |h: axum::http::HeaderMap| async move {
            hit(&l1, &h, "http");
            let a = account(&h);
            let body = if header(&h, "x-test-over").is_some_and(|v| v.split(',').any(|x| x == a)) {
                concat!(
                    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-over\"}}\n\n",
                    "event: error\ndata: {\"type\":\"service_unavailable_error\",\"code\":\"server_is_overloaded\",\"message\":\"overloaded\",\"sequence_number\":1}\n\n",
                    "data: {\"type\":\"response.failed\",\"sequence_number\":2,\"response\":{\"id\":\"r-over\",\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"overloaded\"}}}\n\n"
                )
                .to_string()
            } else {
                format!(
                    "data: {{\"type\":\"response.created\",\"response\":{{\"id\":\"r-{a}\"}}}}\n\ndata: {{\"type\":\"response.completed\",\"response\":{{\"id\":\"r-{a}\"}}}}\n\n"
                )
            };
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from(body))
                .unwrap()
        })
        .get(
            move |h: axum::http::HeaderMap, ws: WebSocketUpgrade| async move {
                hit(&l2, &h, "handshake");
                let a = account(&h);
                if header(&h, "x-test-refuse").is_some_and(|v| v.split(',').any(|x| x == a)) {
                    return Response::builder()
                        .status(402)
                        .header("content-type", "application/json")
                        .body(Body::from(r#"{"detail":{"code":"deactivated_workspace"}}"#))
                        .unwrap();
                }
                let issue = header(&h, "x-test-issue").map(|p| format!("{p}-{a}"));
                let (log, h2) = (l2.clone(), h.clone());
                let mut r = ws.on_upgrade(move |mut s| async move {
                    while let Some(Ok(m)) = s.recv().await {
                        let Message::Text(t) = m else { continue };
                        hit(&log, &h2, "turn");
                        let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
                        for f in frames(v["tag"].as_str().unwrap_or(""), &a) {
                            let _ = s.send(Message::Text(f.into())).await;
                        }
                    }
                });
                if let Some(v) = issue {
                    r.headers_mut().insert(TURN_STATE, v.parse().unwrap());
                }
                r
            },
        ),
    );
    let up_addr = serve(up).await;
    let cfg = Config::parse(&format!(
        "api_keys = [\"{KEY}\"]\nupstream = \"http://{up_addr}/backend-api\"\n[routing]\ncooldown_wait_secs = 20\n{extra}"
    ))
    .unwrap();
    let router = Arc::new(ciallopa_routing::CpaRouter::new(cfg.routing.clone()));
    let state = proxy_state(cfg, Arc::new(Store::new(ids)), router.clone(), None);
    Env {
        proxy: serve(ciallopa::build_app(state)).await,
        router,
        log,
        t0,
    }
}

fn route() -> RouteRequest {
    RouteRequest {
        model: MODEL.into(),
        ..Default::default()
    }
}

/// A cooldown of `d` on `id` for `model` only (404 not-found cooldown).
fn cool_model(r: &ciallopa_routing::CpaRouter, id: &str, model: &str, d: Duration) {
    let req = RouteRequest {
        model: model.into(),
        ..Default::default()
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

/// Sets the account's last observed window use (100 = full, serving from credits).
fn used(r: &ciallopa_routing::CpaRouter, id: &str, percent: f64) {
    let q = QuotaSnapshot {
        primary: Some(QuotaWindow {
            used_percent: percent,
            window_minutes: Some(10080),
            resets_at: Some(SystemTime::now() + Duration::from_secs(86_400)),
        }),
        secondary: None,
    };
    r.report(
        &route(),
        &id.to_string(),
        &Outcome::Success { quota: Some(q) },
    );
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(e: &Env, headers: &[(&'static str, &str)]) -> Ws {
    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    for (k, v) in headers {
        req.headers_mut().insert(*k, v.parse().unwrap());
    }
    tokio_tungstenite::connect_async(req).await.unwrap().0
}

/// Frames of one client turn, up to `response.completed` or the close (`close <code>`).
async fn turn(s: &mut Ws, tag: &str, extra: &str) -> Vec<String> {
    s.send(C::Text(
        format!(
            r#"{{"type":"response.create","model":"{MODEL}","tag":"{tag}"{extra},"input":[]}}"#
        )
        .into(),
    ))
    .await
    .unwrap();
    let mut got = Vec::new();
    loop {
        let m = tokio::time::timeout(Duration::from_secs(15), s.next())
            .await
            .unwrap();
        match m {
            Some(Ok(C::Close(f))) => {
                got.push(format!("close {}", f.map_or(1005, |f| u16::from(f.code))));
                return got;
            }
            Some(Ok(C::Text(t))) => {
                let done = t.contains("response.completed");
                got.push(t.to_string());
                if done {
                    return got;
                }
            }
            Some(Ok(_)) => {}
            other => panic!("{other:?} after {got:?}"),
        }
    }
}

#[tokio::test]
async fn http_overload_switches_to_an_account_cooling_only_for_another_model() {
    // Fill-first: a wait-and-retry would stay on `a`; only a switch reaches `b`.
    let e = env(&["a", "b"], "strategy = \"fill-first\"\n").await;
    cool_model(&e.router, "b", "gpt-other", units(600));
    let r = super::support::http_client()
        .post(format!("http://{}/v1/responses", e.proxy))
        .bearer_auth(KEY)
        .header("x-test-over", "a")
        .body(format!(r#"{{"model":"{MODEL}","stream":true,"input":[]}}"#))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let text = r.text().await.unwrap();
    assert!(
        text.contains("r-b") && !text.contains("overloaded"),
        "{text}"
    );
    assert_eq!(e.accounts("http"), ["a", "b"]);
}

/// Turn 1 leaves `a` (overloaded) for `b`; turn 2 hits a hard quota on `b` before output.
#[tokio::test]
async fn ws_later_turn_may_return_to_an_account_left_in_an_earlier_turn() {
    let e = env(&["a", "b"], "").await;
    let mut s = connect(&e, &[]).await;
    let first = turn(&mut s, "over@a", "").await;
    assert!(first.last().unwrap().contains("r-b"), "{first:?}");
    // Exclusions of turn 1 are gone: the replay goes back to `a` on the same connection.
    let second = turn(&mut s, "quota@b", "").await;
    assert!(second.last().unwrap().contains("r-a"), "{second:?}");
    assert_eq!(e.accounts("turn"), ["a", "b", "b", "a"]);
    assert_eq!(e.accounts("handshake"), ["a", "b", "a"]);
}

#[tokio::test]
async fn ws_return_to_an_earlier_account_waits_for_its_cooldown() {
    let e = env(&["a", "b"], "").await;
    let mut s = connect(&e, &[]).await;
    assert!(turn(&mut s, "over@a", "")
        .await
        .last()
        .unwrap()
        .contains("r-b"));
    cool_model(&e.router, "a", MODEL, units(8));
    let cooled_until = e.t0.elapsed() + units(8);
    let second = turn(&mut s, "quota@b", "").await;
    assert!(second.last().unwrap().contains("r-a"), "{second:?}");
    let back = e.hits("handshake").pop().unwrap();
    assert_eq!(back.account, "a");
    assert!(back.at >= cooled_until, "{:?} < {cooled_until:?}", back.at);
    assert_eq!(e.accounts("turn"), ["a", "b", "b", "a"]);
}

/// The 1012 boundary is unchanged: a turn that cannot be replayed here closes for the client.
#[tokio::test]
async fn ws_unreplayable_later_turn_still_closes_for_replay() {
    for (tag, extra, client) in [
        ("quota@b", r#","previous_response_id":"resp_1""#, 1),
        ("late@b", "", 2),
    ] {
        let e = env(&["a", "b"], "").await;
        let mut s = connect(&e, &[]).await;
        assert!(turn(&mut s, "over@a", "")
            .await
            .last()
            .unwrap()
            .contains("r-b"));
        let second = turn(&mut s, tag, extra).await;
        assert_eq!(second.len(), client, "{tag}: {second:?}");
        assert_eq!(second.last().unwrap(), "close 1012", "{tag}");
        assert_eq!(
            e.accounts("turn"),
            ["a", "b", "b"],
            "{tag}: nothing sent to a"
        );
    }
}

fn turn_states(e: &Env) -> Vec<(String, Option<String>)> {
    e.hits("handshake")
        .into_iter()
        .map(|h| (h.account, h.turn_state))
        .collect()
}

/// After a handshake failover a → b, a quota-full move to c keeps the client's turn state
/// only where it is known to belong.
#[tokio::test]
async fn ws_turn_state_is_dropped_after_a_handshake_failover_and_a_later_move() {
    let some = |v: &str| Some(v.to_string());
    for known in [false, true] {
        let e = env(&["a", "b", "c"], "").await;
        let state = if known {
            // One handshake per account (round robin a, b, c) records who issued which value.
            for _ in 0..3 {
                connect(&e, &[("x-test-issue", "ts-failover")]).await;
            }
            e.log.lock().unwrap().clear();
            "ts-failover-c"
        } else {
            "ts-failover-unknown"
        };
        let mut s = connect(&e, &[("x-test-refuse", "a"), (TURN_STATE, state)]).await;
        used(&e.router, "b", 100.0);
        let got = turn(&mut s, "", "").await;
        assert!(got.last().unwrap().contains("r-c"), "{got:?}");
        let expected = if known {
            [("a", None), ("b", None), ("c", some(state))]
        } else {
            [("a", some(state)), ("b", None), ("c", None)]
        };
        assert_eq!(
            turn_states(&e),
            expected.map(|(a, v)| (a.to_string(), v)),
            "known owner: {known}"
        );
    }
}

/// A quota-full move from the handshake account leaves the client's turn state behind, also
/// when a later turn returns to the first account.
#[tokio::test]
async fn ws_turn_state_is_dropped_after_a_quota_full_move() {
    let e = env(&["a", "b"], "").await;
    let state = "ts-quota-full-unknown";
    let mut s = connect(&e, &[(TURN_STATE, state)]).await;
    used(&e.router, "a", 100.0);
    assert!(turn(&mut s, "", "").await.last().unwrap().contains("r-b"));
    used(&e.router, "a", 10.0);
    used(&e.router, "b", 100.0);
    assert!(turn(&mut s, "", "").await.last().unwrap().contains("r-a"));
    assert_eq!(
        turn_states(&e),
        [
            ("a".to_string(), Some(state.to_string())),
            ("b".to_string(), None),
            ("a".to_string(), None),
        ]
    );
}

#[tokio::test]
async fn ws_turn_state_is_kept_on_a_redial_to_the_same_account() {
    let e = env(&["a"], "").await;
    let state = "ts-same-account-unknown";
    let mut s = connect(&e, &[(TURN_STATE, state)]).await;
    cool_model(&e.router, "a", MODEL, units(2));
    assert!(turn(&mut s, "", "").await.last().unwrap().contains("r-a"));
    let kept = Some(state.to_string());
    assert_eq!(
        turn_states(&e),
        [("a".to_string(), kept.clone()), ("a".to_string(), kept)]
    );
}
