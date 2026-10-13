//! Forwarding integration tests against a mock upstream (SSE, retry, 429 failover, 401
//! refresh, idle timeout, models cache, WebSocket relay + quota replay/close, Images API).
#![allow(clippy::unwrap_used)]
use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::{
    body::Body,
    extract::State,
    http::HeaderMap,
    response::Response,
    routing::{get, post},
    Router,
};
use bytes::Bytes;
use cpa_lite_core::*;
use cpa_lite_forward::AppState;
use futures::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const KEY: &str = "sk-cpa-0123456789abcdef";

struct Store {
    ids: Vec<&'static str>,
    refreshed: AtomicUsize,
}
#[async_trait]
impl AccountStore for Store {
    async fn list(&self) -> Vec<Account> {
        self.ids
            .iter()
            .map(|i| Account {
                id: i.to_string(),
                email: None,
                chatgpt_account_id: Some(format!("acct-{i}")),
                plan_type: None,
                priority: 0,
                weight: 1,
                disabled: false,
            })
            .collect()
    }
    async fn credentials(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        let gen = if self.refreshed.load(Ordering::SeqCst) > 0 {
            "2"
        } else {
            "1"
        };
        Ok(Credentials {
            account: id.clone(),
            access_token: format!("tok-{id}-{gen}"),
            chatgpt_account_id: Some(format!("acct-{id}")),
        })
    }
    async fn force_refresh(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        self.refreshed.fetch_add(1, Ordering::SeqCst);
        self.credentials(id).await
    }
    async fn reload(&self) -> Result<(), AuthError> {
        Ok(())
    }
}

#[derive(Default)]
struct Rec(Mutex<Vec<(String, Outcome)>>);
impl cpa_lite_core::Router for Rec {
    fn select(&self, req: &RouteRequest, accounts: &[Account]) -> Result<Selection, RouteError> {
        accounts
            .iter()
            .find(|a| !req.exclude.contains(&a.id))
            .map(|a| Selection {
                account: a.id.clone(),
                from_binding: false,
            })
            .ok_or(RouteError::NoAccounts)
    }
    fn report(&self, _: &RouteRequest, a: &AccountId, o: &Outcome) {
        self.0.lock().unwrap().push((a.clone(), o.clone()));
    }
}

type Seen = (Vec<(String, String)>, Bytes);

#[derive(Clone, Default)]
struct Mock {
    hits: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<Seen>>>,
}
impl Mock {
    fn record(&self, h: &HeaderMap, b: &Bytes) -> usize {
        let hs = h
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        self.seen.lock().unwrap().push((hs, b.clone()));
        self.hits.fetch_add(1, Ordering::SeqCst)
    }
}

fn sse_resp(body: &'static str) -> Response {
    Response::builder()
        .header("content-type", "text/event-stream")
        .header("x-codex-primary-used-percent", "12")
        .body(Body::from(body))
        .unwrap()
}

async fn up_responses(State(m): State<Mock>, h: HeaderMap, b: Bytes) -> Response {
    let n = m.record(&h, &b);
    let auth = h
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let mode = h
        .get("x-test-mode")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    match mode.as_str() {
        "retry" if n < 2 => Response::builder().status(503).body(Body::from("busy")).unwrap(),
        "limit" if auth.contains("tok-a") => Response::builder()
            .status(429)
            .body(Body::from(r#"{"error":{"type":"usage_limit_reached","resets_in_seconds":30}}"#))
            .unwrap(),
        "midlimit" if auth.contains("tok-a") => sse_resp(concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-a\"}}\n\n",
            "data: {\"type\":\"error\",\"status\":429,\"error\":{\"type\":\"usage_limit_reached\",\"resets_in_seconds\":45}}\n\n"
        )),
        "midlimit" | "latelimit" if !auth.contains("tok-a") || mode == "latelimit" => sse_resp(if mode == "latelimit" {
            concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-a\"}}\n\n",
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
                "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"usage_limit_reached\"}}}\n\n"
            )
        } else {
            concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-b\"}}\n\n",
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r-b\"}}\n\n"
            )
        }),
        "unauth" if auth.ends_with("-1") => Response::builder().status(401).body(Body::empty()).unwrap(),
        "idle" => {
            let s = futures::stream::unfold(0, |i| async move {
                if i == 0 {
                    Some((Ok::<_, std::io::Error>(Bytes::from_static(b"data: {\"type\":\"response.created\"}\n\n")), 1))
                } else {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    None
                }
            });
            Response::builder().header("content-type", "text/event-stream").body(Body::from_stream(s)).unwrap()
        }
        "image" => sse_resp(concat!(
            "data: {\"type\":\"response.image_generation_call.partial_image\",\"partial_image_b64\":\"UA==\",\"partial_image_index\":0}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"image_generation_call\",\"result\":\"SU1H\",\"output_format\":\"png\",\"size\":\"1024x1024\"}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"created_at\":7,\"output\":[],\"tool_usage\":{\"image_gen\":{\"images\":1}}}}\n\n"
        )),
        _ => sse_resp("event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n"),
    }
}

async fn up_models(State(m): State<Mock>, h: HeaderMap) -> Response {
    m.record(&h, &Bytes::new());
    Response::builder()
        .header("content-type", "application/json")
        .body(Body::from(r#"{"models":[]}"#))
        .unwrap()
}

async fn up_images(State(m): State<Mock>, h: HeaderMap, b: Bytes) -> Response {
    m.record(&h, &b);
    Response::builder()
        .header("content-type", "application/json")
        .body(Body::from(r#"{"created":1,"data":[{"b64_json":"X"}]}"#))
        .unwrap()
}

async fn up_ws(State(m): State<Mock>, h: HeaderMap, ws: WebSocketUpgrade) -> Response {
    m.record(&h, &Bytes::new());
    let auth = h
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let mut r = ws.on_upgrade(move |mut s| async move {
        while let Some(Ok(msg)) = s.recv().await {
            if let Message::Text(t) = msg {
                if t.as_str().contains("limit") && (auth.contains("tok-a") || t.as_str().contains("prevlimit")) {
                    let _ = s.send(Message::Text(r#"{"type":"response.created","response":{"id":"r-a"}}"#.into())).await;
                    let _ = s.send(Message::Text(r#"{"type":"error","status":429,"error":{"type":"usage_limit_reached","resets_in_seconds":45}}"#.into())).await;
                } else if t.as_str().contains("limit") {
                    for f in [
                        r#"{"type":"response.created","response":{"id":"r-b"}}"#,
                        r#"{"type":"response.output_text.delta","delta":"ok"}"#,
                        r#"{"type":"response.completed","response":{"id":"r-b"}}"#,
                    ] {
                        let _ = s.send(Message::Text(f.into())).await;
                    }
                } else {
                    let _ = s.send(Message::Text(format!(r#"{{"type":"response.completed","echo":{t}}}"#).into())).await;
                }
            }
        }
    });
    r.headers_mut()
        .insert("x-codex-turn-state", "ts-1".parse().unwrap());
    r
}

async fn serve(app: Router) -> String {
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ln.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(ln, app).await.unwrap() });
    format!("127.0.0.1:{}", addr.port())
}

struct Env {
    proxy: String,
    mock: Mock,
    rec: Arc<Rec>,
    store: Arc<Store>,
    http: reqwest::Client,
}

async fn env(idle: u64) -> Env {
    let mock = Mock::default();
    let up = Router::new()
        .route(
            "/backend-api/codex/responses",
            post(up_responses).get(up_ws),
        )
        .route("/backend-api/codex/models", get(up_models))
        .route("/backend-api/codex/images/generations", post(up_images))
        .route("/backend-api/codex/images/edits", post(up_images))
        .route("/backend-api/wham/usage", get(up_models))
        .with_state(mock.clone());
    let up_addr = serve(up).await;
    let cfg = Config::parse(&format!(
        "api_keys = [\"{KEY}\"]\nupstream = \"http://{up_addr}/backend-api\"\n[upstream_opts]\nstream_idle_timeout_secs = {idle}\n"
    ))
    .unwrap();
    let rec = Arc::new(Rec::default());
    let store = Arc::new(Store {
        ids: vec!["a", "b"],
        refreshed: AtomicUsize::new(0),
    });
    let state = AppState {
        accounts: store.clone(),
        router: rec.clone(),
        upstream: Arc::new(cpa_lite_upstream::UpstreamClient::new(&cfg).unwrap()),
        config: Arc::new(cfg),
    };
    let proxy = serve(cpa_lite::build_app(state)).await;
    Env {
        proxy,
        mock,
        rec,
        store,
        http: reqwest::Client::new(),
    }
}

impl Env {
    fn post(&self, path: &str) -> reqwest::RequestBuilder {
        self.http
            .post(format!("http://{}{path}", self.proxy))
            .bearer_auth(KEY)
    }
    fn outcomes(&self) -> Vec<(String, Outcome)> {
        self.rec.0.lock().unwrap().clone()
    }
}

#[tokio::test]
async fn sse_passthrough_headers_and_zstd() {
    let e = env(300).await;
    let raw = br#"{"model":"gpt-5.5","prompt_cache_key":"pk","input":[]}"#;
    let z = zstd::encode_all(&raw[..], 3).unwrap();
    let r = e
        .post("/backend-api/codex/responses")
        .header("originator", "codex_cli_rs")
        .header("chatgpt-account-id", "client-acct")
        .header("x-forwarded-for", "9.9.9.9")
        .header("content-encoding", "zstd")
        .header("session_id", "s1")
        .body(z.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["content-type"], "text/event-stream");
    assert!(r.text().await.unwrap().contains("response.completed"));
    let seen = e.mock.seen.lock().unwrap();
    let (h, b) = &seen[0];
    assert_eq!(
        b.as_ref(),
        z.as_slice(),
        "zstd body forwarded byte-identical"
    );
    let names: Vec<&str> = h.iter().map(|(k, _)| k.as_str()).collect();
    let get = |k: &str| h.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
    assert_eq!(get("authorization"), Some("Bearer tok-a-1"));
    assert_eq!(get("chatgpt-account-id"), Some("acct-a"));
    assert_eq!(get("content-encoding"), Some("zstd"));
    assert!(get("x-forwarded-for").is_none());
    let pos = |k: &str| names.iter().position(|n| *n == k).unwrap();
    assert!(
        pos("originator") < pos("chatgpt-account-id")
            && pos("chatgpt-account-id") < pos("content-encoding")
    );
    assert!(matches!(
        &e.outcomes()[0].1,
        Outcome::Success { quota: Some(_) }
    ));
}

#[tokio::test]
async fn retries_5xx_then_succeeds() {
    let e = env(300).await;
    let r = e
        .post("/v1/responses")
        .header("x-test-mode", "retry")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 3);
    let o = e.outcomes();
    assert_eq!(o[0].1, Outcome::Transient { status: Some(503) });
    assert!(matches!(o[2].1, Outcome::Success { .. }));
}

#[tokio::test]
async fn rate_limit_fails_over() {
    let e = env(300).await;
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "limit")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let o = e.outcomes();
    assert_eq!(o[0].0, "a");
    assert!(
        matches!(o[0].1, Outcome::RateLimited { usage_limit: true, retry_after: Some(d), .. } if d == Duration::from_secs(30))
    );
    assert_eq!(o[1].0, "b");
}

#[tokio::test]
async fn unauthorized_forces_refresh_once() {
    let e = env(300).await;
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "unauth")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(e.store.refreshed.load(Ordering::SeqCst), 1);
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn idle_timeout_aborts_stream() {
    let e = env(1).await;
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "idle")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let t0 = std::time::Instant::now();
    assert!(r.bytes().await.is_err(), "stream must abort");
    assert!(t0.elapsed() < Duration::from_secs(4));
    assert!(e
        .outcomes()
        .iter()
        .any(|(_, o)| *o == Outcome::Transient { status: None }));
}

#[tokio::test]
async fn models_cached_and_wham_passthrough() {
    let e = env(300).await;
    for _ in 0..2 {
        let r = e
            .http
            .get(format!(
                "http://{}/backend-api/codex/models?client_version=1",
                e.proxy
            ))
            .bearer_auth(KEY)
            .send()
            .await
            .unwrap();
        assert_eq!(r.text().await.unwrap(), r#"{"models":[]}"#);
    }
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
    let r = e
        .http
        .get(format!("http://{}/api/codex/usage", e.proxy))
        .header("x-api-key", KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn websocket_relay_and_limit_close() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let e = env(300).await;
    let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    req.headers_mut()
        .insert("session_id", "ws-1".parse().unwrap());
    let (mut ws, resp) = tokio_tungstenite::connect_async(req).await.unwrap();
    assert_eq!(resp.headers()["x-codex-turn-state"], "ts-1");
    ws.send(M::text(r#"{"type":"response.create","n":1}"#))
        .await
        .unwrap();
    let m = ws.next().await.unwrap().unwrap();
    assert!(m.to_text().unwrap().contains("\"echo\""));
    // Quota error after response.created, before output: replayed on account b.
    ws.send(M::text(r#"{"type":"response.create","tag":"limit"}"#))
        .await
        .unwrap();
    let mut got = Vec::new();
    loop {
        let m = ws.next().await.unwrap().unwrap();
        let t = m.to_text().unwrap().to_string();
        assert!(
            !t.contains("usage_limit_reached"),
            "quota frame leaked: {t}"
        );
        got.push(t.clone());
        if t.contains("response.completed") {
            break;
        }
    }
    assert_eq!(got.len(), 3, "{got:?}");
    assert!(got[0].contains("r-b") && got[1].contains("delta"));
    // Incremental request hits a quota error: no frame forwarded, close 1012 for replay.
    ws.send(M::text(
        r#"{"type":"response.create","previous_response_id":"r-b","tag":"prevlimit"}"#,
    ))
    .await
    .unwrap();
    match ws.next().await.unwrap().unwrap() {
        M::Close(Some(f)) => {
            assert_eq!(u16::from(f.code), 1012);
            assert_eq!(f.reason.as_str(), "upstream requires HTTP replay");
        }
        other => panic!("expected close, got {other:?}"),
    }
    let seen = e.mock.seen.lock().unwrap();
    let auth = seen[0]
        .0
        .iter()
        .find(|(k, _)| k == "authorization")
        .unwrap()
        .1
        .clone();
    assert_eq!(auth, "Bearer tok-a-1");
    drop(seen);
    assert!(e.outcomes().iter().any(|(a, o)| a == "a"
        && matches!(
            o,
            Outcome::RateLimited {
                usage_limit: true,
                retry_after: Some(d),
                ..
            } if *d == Duration::from_secs(45)
        )));
}

async fn sse_text(e: &Env, mode: &str) -> String {
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", mode)
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    r.text().await.unwrap()
}

#[tokio::test]
async fn sse_quota_before_output_switches_transparently() {
    let e = env(300).await;
    let t = sse_text(&e, "midlimit").await;
    assert!(!t.contains("usage_limit_reached"), "{t}");
    assert!(t.contains("r-b") && t.contains("response.completed"), "{t}");
    assert!(!t.contains("r-a"), "{t}");
    let seen = e.mock.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].1, seen[1].1, "same body replayed");
    drop(seen);
    assert!(e.outcomes().iter().any(|(a, o)| a == "a" && matches!(
        o,
        Outcome::RateLimited { usage_limit: true, retry_after: Some(d), .. } if *d == Duration::from_secs(45)
    )));
}

#[tokio::test]
async fn sse_quota_after_output_is_forwarded_and_cools() {
    let e = env(300).await;
    let t = sse_text(&e, "latelimit").await;
    assert!(
        t.contains("delta") && t.contains("usage_limit_reached"),
        "{t}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
    assert!(e.outcomes().iter().any(|(a, o)| a == "a"
        && matches!(
            o,
            Outcome::RateLimited {
                usage_limit: true,
                ..
            }
        )));
}

#[tokio::test]
async fn images_wrap_and_direct() {
    let e = env(300).await;
    let r = e
        .post("/v1/images/generations")
        .header("x-test-mode", "image")
        .json(&serde_json::json!({"prompt":"cat","size":"1024x1024"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let v: serde_json::Value = r.json().await.unwrap();
    assert_eq!(v["data"][0]["b64_json"], "SU1H");
    assert_eq!(v["created"], 7);
    assert_eq!(v["usage"]["images"], 1);
    let sent: serde_json::Value =
        serde_json::from_slice(&e.mock.seen.lock().unwrap()[0].1).unwrap();
    assert_eq!(sent["tools"][0]["type"], "image_generation");
    assert_eq!(sent["tools"][0]["model"], "gpt-image-2");
    assert_eq!(sent["model"], "gpt-5.4-mini");

    // streaming wrap
    let r = e
        .post("/v1/images/generations")
        .header("x-test-mode", "image")
        .json(&serde_json::json!({"prompt":"cat","stream":true,"model":"gpt-image-1"}))
        .send()
        .await
        .unwrap();
    let t = r.text().await.unwrap();
    assert!(
        t.contains("event: image_generation.partial_image")
            && t.contains("event: image_generation.completed"),
        "{t}"
    );

    // direct (gpt-image-2) multipart edit → JSON
    let mp = "--XB\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\ngpt-image-2\r\n--XB\r\nContent-Disposition: form-data; name=\"prompt\"\r\n\r\nedit it\r\n--XB\r\nContent-Disposition: form-data; name=\"image[]\"; filename=\"a.png\"\r\n\r\n\x7fPNGxx\r\n--XB--\r\n";
    let mp = mp.replace('\x7f', "\u{89}");
    let r = e
        .post("/v1/images/edits")
        .header("content-type", "multipart/form-data; boundary=XB")
        .body(mp)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let seen = e.mock.seen.lock().unwrap();
    let j: serde_json::Value = serde_json::from_slice(&seen.last().unwrap().1).unwrap();
    assert_eq!(j["model"], "gpt-image-2");
    assert!(j["images"][0]["image_url"]
        .as_str()
        .unwrap()
        .starts_with("data:"));
    assert_eq!(j["prompt"], "edit it");
    assert!(seen
        .last()
        .unwrap()
        .0
        .iter()
        .any(|(k, v)| k == "originator" && v == "codex_exec"));
}
