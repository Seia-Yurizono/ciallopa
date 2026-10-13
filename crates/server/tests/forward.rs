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
use ciallopa_core::*;
use ciallopa_forward::AppState;
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
impl ciallopa_core::Router for Rec {
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

/// `x-test-noct: 1` mimics chatgpt.com, which sends `/responses` SSE without `Content-Type`.
async fn up_responses(st: State<Mock>, h: HeaderMap, b: Bytes) -> Response {
    let noct = h.contains_key("x-test-noct");
    let mut r = up_responses_inner(st, h, b).await;
    if noct {
        r.headers_mut().remove("content-type");
    }
    r
}

async fn up_responses_inner(State(m): State<Mock>, h: HeaderMap, b: Bytes) -> Response {
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
        "errbody" => Response::builder()
            .status(400)
            .header("content-type", "application/json")
            .header("cf-ray", "ray-123")
            .header("x-request-id", "req_abc")
            .header("set-cookie", "session=supersecret")
            .header("x-codex-primary-used-percent", "40")
            .body(Body::from(
                r#"{"error":{"message":"bad input from alice@example.com <img src=x onerror=alert(1)>","auth":"Bearer sk-live-SECRETSECRETSECRET"},"refresh_token":"rt_SECRETSECRETSECRET"}"#,
            ))
            .unwrap(),
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
        "usage" => {
            let mut r = sse_resp(concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-u\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
            "event: response.completed\r\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r-u\",\"usage\":{\"input_tokens\":120,\"input_tokens_details\":{\"cached_tokens\":100,\"cache_write_tokens\":10},\"output_tokens\":30,\"output_tokens_details\":{\"reasoning_tokens\":12}}}}\r\n\r\n"
            ));
            let reset = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() + 3600).to_string();
            let h = r.headers_mut();
            h.insert("x-codex-plan-type", "plus".parse().unwrap());
            h.insert("x-codex-primary-window-minutes", "300".parse().unwrap());
            h.insert("x-codex-primary-reset-at", reset.parse().unwrap());
            r
        }
        "meta" => sse_resp(concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-m\",\"model\":\"gpt-5.5\",\"service_tier\":\"auto\",\"reasoning\":{\"effort\":\"low\"}}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r-m\",\"model\":\"gpt-5.5-2026-09-30\",\"service_tier\":\"default\",\"reasoning\":{\"effort\":\"medium\"},\"usage\":{\"input_tokens\":5,\"output_tokens\":2}}}\n\n"
        )),
        "meta_created_only" => sse_resp(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-c\",\"model\":\"gpt-5.5\",\"service_tier\":\"auto\",\"reasoning\":{\"effort\":\"high\"}}}\n\n",
        ),
        "compact" => sse_resp(concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-c\"}}\n\n",
            "data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"compaction\"}}\n\n",
            "data: {\"type\":\"response.compaction.compacting\"}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"compaction\",\"encrypted_content\":\"gAAA\"}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r-c\"}}\n\n"
        )),
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
                if t.as_str().contains("\"meta\"") {
                    for f in [
                        r#"{"type":"response.created","response":{"id":"r-m","model":"gpt-5.5","service_tier":"auto","reasoning":{"effort":"high"}}}"#,
                        r#"{"type":"response.output_text.delta","delta":"ok"}"#,
                        r#"{"type":"response.completed","response":{"id":"r-m","model":"gpt-5.6-luna","service_tier":"flex","reasoning":{"effort":"high"},"usage":{"input_tokens":5,"output_tokens":2}}}"#,
                    ] {
                        let _ = s.send(Message::Text(f.into())).await;
                    }
                } else if t.as_str().contains("limit") && (auth.contains("tok-a") || t.as_str().contains("prevlimit")) {
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
    r.headers_mut()
        .insert("set-cookie", "__cf_bm=secret".parse().unwrap());
    r.headers_mut()
        .insert("set-cookie2", "__oailb=secret".parse().unwrap());
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
    env_usage(idle, None).await
}

async fn env_usage(idle: u64, usage: Option<Arc<ciallopa_usage::Recorder>>) -> Env {
    env_cfg(idle, usage, "").await
}

async fn env_cfg(idle: u64, usage: Option<Arc<ciallopa_usage::Recorder>>, extra: &str) -> Env {
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
        "api_keys = [\"{KEY}\"]\nupstream = \"http://{up_addr}/backend-api\"\n[upstream_opts]\nstream_idle_timeout_secs = {idle}\n{extra}"
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
        upstream: Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap()),
        usage,
        config: Arc::new(cfg),
    };
    let proxy = serve(ciallopa::build_app(state)).await;
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
    assert!(resp.headers().get("set-cookie").is_none());
    assert!(resp.headers().get("set-cookie2").is_none());
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
    assert_eq!(sent["model"], "gpt-5.5");
    assert!(
        !e.mock.seen.lock().unwrap()[0]
            .0
            .iter()
            .any(|(k, _)| k == "content-encoding"),
        "images wrap is never compressed"
    );

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

/// Codex 0.162 remote compaction v2: a normal `/responses` turn whose input ends with
/// `{"type":"compaction_trigger"}`; reply carries one `compaction` output item.
#[tokio::test]
async fn compaction_v2_passthrough() {
    let e = env(300).await;
    let body = r#"{"model":"gpt-5.5","stream":true,"input":[{"type":"message","role":"user","content":[]},{"type":"compaction_trigger"}],"client_metadata":{"x-codex-turn-metadata":"{\"request_kind\":\"compaction\"}"}}"#;
    for path in ["/v1/responses", "/backend-api/codex/responses"] {
        let r = e
            .post(path)
            .header("x-test-mode", "compact")
            .header("x-codex-beta-features", "remote_compaction_v2")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        let t = r.text().await.unwrap();
        assert!(
            t.contains("response.compaction.compacting")
                && t.contains("\"encrypted_content\":\"gAAA\""),
            "{t}"
        );
    }
    let seen = e.mock.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    for (h, b) in seen.iter() {
        assert!(h
            .iter()
            .any(|(k, v)| k == "content-encoding" && v == "zstd"));
        assert_eq!(
            zstd::decode_all(b.as_ref()).unwrap(),
            body.as_bytes(),
            "compressed compaction turn decodes to the original body"
        );
        assert!(h
            .iter()
            .any(|(k, v)| k == "x-codex-beta-features" && v == "remote_compaction_v2"));
    }
}

fn usage_recorder(dir: &std::path::Path) -> Arc<ciallopa_usage::Recorder> {
    ciallopa_usage::Recorder::open(&ciallopa_core::config::UsageConfig {
        path: dir.join("usage.db"),
        flush_ms: 5,
        ..Default::default()
    })
    .unwrap()
}

async fn usage_rows(e: &Env) -> Vec<serde_json::Value> {
    // the record is emitted when the stream task ends; poll the API
    for _ in 0..100 {
        let r = e
            .http
            .get(format!("http://{}/usage/v1/records?limit=50", e.proxy))
            .bearer_auth(KEY)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        let v: serde_json::Value = r.json().await.unwrap();
        let rows = v["records"].as_array().unwrap().clone();
        if !rows.is_empty() {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    vec![]
}

async fn wait_rows(e: &Env, n: usize) -> Vec<serde_json::Value> {
    for _ in 0..100 {
        let rows = usage_rows(e).await;
        if rows.len() >= n {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    usage_rows(e).await
}

#[tokio::test]
async fn usage_sse_rows() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let r = e
        .post("/v1/responses")
        .header("x-test-mode", "usage")
        .body(r#"{"model":"gpt-5.5","reasoning":{"effort":"low"},"input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.text().await.unwrap().contains("r-u"));
    let rows = wait_rows(&e, 1).await;
    let u = &rows[0];
    assert_eq!(u["account"], "a");
    assert_eq!(u["model"], "gpt-5.5");
    assert_eq!(u["reasoning_effort"], "low");
    assert_eq!(u["transport"], "sse");
    assert_eq!(u["endpoint"], "responses");
    assert_eq!(u["status"], 200);
    assert!(u["error"].is_null(), "{u}");
    assert_eq!(u["input_tokens"], 120);
    assert_eq!(u["cached_tokens"], 100);
    assert_eq!(u["output_tokens"], 30);
    assert_eq!(u["reasoning_tokens"], 12);
    assert!(u["ttft_ms"].is_u64());
    assert_eq!(u["switches"], 0);

    // quota before output → transparent switch a→b
    sse_text(&e, "midlimit").await;
    // quota after output → recorded as usage_limit
    sse_text(&e, "latelimit").await;
    // 429 on a → failover to b
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "limit")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    r.text().await.unwrap();
    let rows = wait_rows(&e, 4).await;
    assert_eq!(rows.len(), 4, "{rows:?}");
    let (limit, late, mid) = (&rows[0], &rows[1], &rows[2]);
    assert_eq!(mid["first_account"], "a");
    assert_eq!(mid["account"], "b");
    assert_eq!(mid["switches"], 1);
    assert!(mid["error"].is_null(), "{mid}");
    assert_eq!(late["error"], "usage_limit");
    assert_eq!(limit["account"], "b");
    assert_eq!(limit["switches"], 1);

    let s: serde_json::Value = e
        .http
        .get(format!("http://{}/usage/v1/summary?range=1h", e.proxy))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(s["summary"]["total"]["count"], 4);
    assert_eq!(s["summary"]["total"]["errors"], 1);
    assert_eq!(s["recorder"]["dropped"], 0);
    // API is behind the key
    let r = e
        .http
        .get(format!("http://{}/usage/v1/summary", e.proxy))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
}

#[tokio::test]
async fn usage_counts_retries() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    sse_text(&e, "retry").await; // two 503s, then ok
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["retries"], 2, "{rows:?}");
    assert!(rows[0]["error"].is_null());
}

#[tokio::test]
async fn usage_ws_turn_rows() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","model":"gpt-5.5","reasoning":{"effort":"high"}}"#,
    ))
    .await
    .unwrap();
    ws.next().await.unwrap().unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","model":"gpt-5.5","tag":"limit"}"#,
    ))
    .await
    .unwrap();
    loop {
        let t = ws
            .next()
            .await
            .unwrap()
            .unwrap()
            .to_text()
            .unwrap()
            .to_string();
        if t.contains("response.completed") {
            break;
        }
    }
    ws.send(M::text(
        r#"{"type":"response.create","previous_response_id":"r-b","tag":"prevlimit"}"#,
    ))
    .await
    .unwrap();
    let _ = ws.next().await; // 1012 close
    let rows = wait_rows(&e, 3).await;
    assert_eq!(rows.len(), 3, "{rows:?}");
    let (t3, t2, t1) = (&rows[0], &rows[1], &rows[2]);
    assert_eq!(t1["transport"], "ws");
    assert_eq!(t1["reasoning_effort"], "high");
    assert_eq!(t1["account"], "a");
    assert!(t1["error"].is_null(), "{t1}");
    assert_eq!(t2["first_account"], "a");
    assert_eq!(t2["account"], "b");
    assert_eq!(t2["switches"], 1);
    assert!(t2["error"].is_null(), "{t2}");
    assert!(t2["ttft_ms"].is_u64());
    assert_eq!(t3["error"], "usage_limit");
}

#[tokio::test]
async fn usage_key_label_instance_and_quota_api() {
    let d = tempfile::tempdir().unwrap();
    let recorder = usage_recorder(d.path());
    recorder.set_prices(
        ciallopa_usage::PriceTable::parse(
            "[[model]]\nmodel=\"gpt-5.5\"\nin=5.0\ncache=0.5\nout=30.0\nfast_x=2.5\n",
        )
        .unwrap(),
    );
    let e = env_usage(300, Some(recorder)).await;
    let r = e
        .post("/v1/responses")
        .header("x-test-mode", "usage")
        .header("thread-id", "th-abc")
        .header(
            "x-codex-turn-metadata",
            r#"{"turn_id":"turn-1","thread_id":"th-abc"}"#,
        )
        .body(r#"{"model":"gpt-5.5","service_tier":"priority","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    r.text().await.unwrap();
    let rows = wait_rows(&e, 1).await;
    let u = &rows[0];
    let label = ciallopa_core::config::default_key_label(KEY);
    assert_eq!(u["key"], label.as_str());
    assert_eq!(u["instance"], ciallopa_core::config::hostname().as_str());
    assert_eq!(u["plan"], "plus");
    assert_eq!(u["primary_used_pct"], 12.0);
    assert!(u["primary_reset_at"].is_i64());
    assert_eq!(u["thread_id"], "th-abc");
    assert_eq!(u["turn_id"], "turn-1");
    assert_eq!(u["service_tier"], "priority");
    assert_eq!(u["cache_write_tokens"], 10);
    // (120-100-10)·5 + 100·0.5 + 10·5 (no write price ⇒ input) + 30·30, fast ×2.5
    let want = (10.0 * 5.0 + 100.0 * 0.5 + 10.0 * 5.0 + 30.0 * 30.0) * 2.5 / 1e6;
    assert!(
        (u["cost_usd"].as_f64().unwrap() - want).abs() < 1e-12,
        "{u}"
    );
    assert_eq!(u["price_status"], "ok");
    let get = |path: String| {
        let h = e.http.clone();
        async move {
            h.get(path)
                .bearer_auth(KEY)
                .send()
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap()
        }
    };
    // raw key never stored or returned
    assert!(!std::fs::read(d.path().join("usage.db"))
        .unwrap()
        .windows(KEY.len())
        .any(|w| w == KEY.as_bytes()));
    let all = get(format!("http://{}/usage/v1/records?key={label}", e.proxy)).await;
    assert_eq!(all["records"].as_array().unwrap().len(), 1);
    assert!(!all.to_string().contains(KEY));
    let none = get(format!("http://{}/usage/v1/records?key=other", e.proxy)).await;
    assert!(none["records"].as_array().unwrap().is_empty());
    let s = get(format!(
        "http://{}/usage/v1/summary?group=key,instance",
        e.proxy
    ))
    .await;
    assert_eq!(s["summary"]["groups"][0]["group"]["key"], label.as_str());
    assert!((s["summary"]["groups"][0]["cost_usd"].as_f64().unwrap() - want).abs() < 1e-12);
    assert_eq!(s["summary"]["total"]["unknown_price"], 0);
    let s = get(format!(
        "http://{}/usage/v1/summary?group=account,tier",
        e.proxy
    ))
    .await;
    assert_eq!(
        s["summary"]["groups"][0]["group"]["service_tier"],
        "priority"
    );
    let r = e
        .http
        .get(format!("http://{}/usage/v1/summary?group=bogus", e.proxy))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);

    let q = get(format!("http://{}/usage/v1/quota", e.proxy)).await;
    assert!(q["note"].as_str().unwrap().contains("other clients"));
    let accts = q["accounts"].as_array().unwrap();
    assert_eq!(accts.len(), 2, "{q}");
    let a = accts.iter().find(|x| x["account"] == "a").unwrap();
    assert_eq!(a["needs_relogin"], false);
    assert_eq!(a["quota"]["primary"]["used_pct"], 12.0);
    assert_eq!(
        a["quota"]["primary"]["estimate"]["status"],
        "insufficient_data"
    );
    let b = accts.iter().find(|x| x["account"] == "b").unwrap();
    assert!(b["quota"].is_null());
}

#[tokio::test]
async fn ui_page_csp_and_toggle() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    // static page needs no key and contains no data
    let r = e
        .http
        .get(format!("http://{}/ui", e.proxy))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let csp = r.headers()["content-security-policy"]
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        csp.contains("default-src 'none'")
            && csp.contains("script-src 'sha256-")
            && csp.contains("connect-src 'self'"),
        "{csp}"
    );
    assert!(!csp.contains("unsafe-inline"));
    assert_eq!(r.headers()["x-content-type-options"], "nosniff");
    assert_eq!(r.headers()["cache-control"], "no-store");
    let body = r.text().await.unwrap();
    assert!(body.contains("管理面板") && !body.contains(KEY));
    let r = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
        .get(format!("http://{}/", e.proxy))
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers()["location"], "/ui");
    // APIs the page calls stay behind the key
    let r = e
        .http
        .get(format!("http://{}/usage/v1/quota", e.proxy))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);

    let off = env_cfg(300, None, "[ui]\nenabled = false\n").await;
    let r = off
        .http
        .get(format!("http://{}/ui", off.proxy))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn summary_day_buckets_api() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    sse_text(&e, "usage").await;
    wait_rows(&e, 1).await;
    let get = |q: &str| {
        let (h, u) = (
            e.http.clone(),
            format!("http://{}/usage/v1/summary?{q}", e.proxy),
        );
        async move { h.get(u).bearer_auth(KEY).send().await.unwrap() }
    };
    let v: serde_json::Value = get("range=7d&bucket=day&tz=480&group=account")
        .await
        .json()
        .await
        .unwrap();
    let b = v["summary"]["buckets"].as_array().unwrap();
    assert_eq!(b.len(), 1, "{v}");
    assert_eq!(b[0]["count"], 1);
    assert_eq!(b[0]["input_tokens"], 120);
    assert!(b[0]["day"].as_str().unwrap().len() == 10);
    let v: serde_json::Value = get("range=7d").await.json().await.unwrap();
    assert!(v["summary"].get("buckets").is_none());
    assert_eq!(get("bucket=hour").await.status(), 400);
    assert_eq!(get("bucket=day&tz=9999").await.status(), 400);
    // numeric params next to the flattened filters
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let v: serde_json::Value = get(&format!("from={}&to={}", now - 60_000, now + 1000))
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(v["summary"]["total"]["count"], 1, "{v}");
}

async fn detail_of(e: &Env, id: &str, auth: bool) -> reqwest::Response {
    let r = e
        .http
        .get(format!("http://{}/usage/v1/records/{id}/detail", e.proxy));
    let r = if auth { r.bearer_auth(KEY) } else { r };
    r.send().await.unwrap()
}

#[tokio::test]
async fn usage_error_detail_capture_and_api() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(1, Some(usage_recorder(d.path()))).await;
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "errbody")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let t = r.text().await.unwrap();
    assert!(t.contains("bad input"), "{t}"); // client still sees the raw upstream body
    sse_text(&e, "latelimit").await; // in-stream response.failed
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "idle")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert!(r.text().await.is_err()); // local idle timeout (1s) aborts the stream
    sse_text(&e, "usage").await; // success: no detail
    let rows = wait_rows(&e, 4).await;
    assert_eq!(rows.len(), 4, "{rows:?}");
    let (ok, idle, late, bad) = (&rows[0], &rows[1], &rows[2], &rows[3]);
    assert_eq!(ok["has_detail"], false);
    assert_eq!(bad["status"], 400);
    for r in [idle, late, bad] {
        assert_eq!(r["has_detail"], true, "{r}");
    }
    let id = |r: &serde_json::Value| r["request_id"].as_str().unwrap().to_string();

    // auth required; malformed / unknown ids
    assert_eq!(detail_of(&e, &id(bad), false).await.status(), 401);
    assert_eq!(detail_of(&e, "nothex", true).await.status(), 400);
    assert_eq!(detail_of(&e, &id(ok), true).await.status(), 404);

    let v: serde_json::Value = detail_of(&e, &id(bad), true).await.json().await.unwrap();
    let raw = v.to_string();
    for secret in ["SECRETSECRET", "alice@example.com", "supersecret"] {
        assert!(!raw.contains(secret), "{secret} leaked: {raw}");
    }
    assert_eq!(v["headers"]["cf-ray"], "ray-123");
    assert_eq!(v["headers"]["x-request-id"], "req_abc");
    assert_eq!(v["headers"]["x-codex-primary-used-percent"], "40");
    assert!(v["headers"].get("set-cookie").is_none());
    assert_eq!(v["truncated"], false);
    let body: serde_json::Value = serde_json::from_str(v["body"].as_str().unwrap()).unwrap();
    assert_eq!(body["refresh_token"], "[REDACTED]");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("<img src=x"),
        "stored as data, escaped by the UI"
    );

    let v: serde_json::Value = detail_of(&e, &id(late), true).await.json().await.unwrap();
    assert!(
        v["body"].as_str().unwrap().contains("usage_limit_reached"),
        "{v}"
    );
    let v: serde_json::Value = detail_of(&e, &id(idle), true).await.json().await.unwrap();
    assert!(v["message"].as_str().unwrap().contains("idle"), "{v}");
}

fn hdr<'a>(h: &'a [(String, String)], k: &str) -> Option<&'a str> {
    h.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str())
}

#[tokio::test]
async fn responses_compressed_like_codex_and_retries_reuse_bytes() {
    let e = env(300).await;
    let body = r#"{"model":"gpt-5.5","prompt_cache_key":"pk","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hello hello hello"}]}]}"#;
    // 503, 503, 200: three attempts must carry the same compressed bytes.
    let r = e
        .post("/v1/responses")
        .header("x-test-mode", "retry")
        .header("accept-encoding", "gzip, br")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.headers().get("content-encoding").is_none());
    assert!(r.text().await.unwrap().contains("response.completed"));
    let seen = e.mock.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 3);
    for (h, b) in &seen {
        assert_eq!(hdr(h, "content-encoding"), Some("zstd"));
        assert!(
            hdr(h, "accept-encoding").is_none(),
            "accept-encoding stripped"
        );
        assert_eq!(hdr(h, "content-length"), Some(b.len().to_string().as_str()));
        assert_eq!(zstd::decode_all(b.as_ref()).unwrap(), body.as_bytes());
        assert_eq!(b, &seen[0].1, "retries reuse the same bytes");
    }
    assert_eq!(
        seen[0].1.as_ref(),
        zstd::encode_all(body.as_bytes(), 3).unwrap().as_slice(),
        "zstd level 3"
    );
    // Session routing still sees the plaintext body.
    assert!(matches!(
        &e.outcomes().last().unwrap().1,
        Outcome::Success { .. }
    ));
}

#[tokio::test]
async fn compression_skips_compact_models_and_preencoded() {
    let e = env(300).await;
    let r = e
        .post("/v1/responses/compact")
        .header("accept-encoding", "gzip")
        .body(r#"{"model":"m"}"#)
        .send()
        .await
        .unwrap();
    // mock has no /compact route; only the request shape matters
    let _ = r.status();
    let pre = zstd::encode_all(&b"{\"model\":\"m\"}"[..], 19).unwrap();
    e.post("/backend-api/codex/responses")
        .header("content-encoding", "zstd")
        .body(pre.clone())
        .send()
        .await
        .unwrap();
    e.http
        .get(format!("http://{}/v1/models", e.proxy))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap();
    let seen = e.mock.seen.lock().unwrap().clone();
    let (h, b) = &seen[0];
    assert_eq!(
        b.as_ref(),
        pre.as_slice(),
        "pre-encoded body byte-identical"
    );
    assert_eq!(hdr(h, "content-encoding"), Some("zstd"));
    let (h, _) = &seen[1];
    assert!(
        hdr(h, "content-encoding").is_none(),
        "models never compressed"
    );
}

#[tokio::test]
async fn compression_can_be_disabled() {
    let e = env_cfg(300, None, "compress_requests = false").await;
    let body = r#"{"model":"gpt-5.5","input":[]}"#;
    e.post("/v1/responses")
        .header("accept-encoding", "gzip")
        .body(body)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let seen = e.mock.seen.lock().unwrap().clone();
    let (h, b) = &seen[0];
    assert_eq!(b.as_ref(), body.as_bytes());
    assert!(hdr(h, "content-encoding").is_none());
    assert!(hdr(h, "accept-encoding").is_none());
}

async fn noct_post(e: &Env, path: &str, mode: &str, body: &str, accept: bool) -> (String, String) {
    let mut rb = e
        .post(path)
        .header("x-test-mode", mode)
        .header("x-test-noct", "1");
    if accept {
        rb = rb.header("accept", "text/event-stream");
    }
    let r = rb.body(body.to_string()).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let ct = r
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    (ct, r.text().await.unwrap())
}

#[tokio::test]
async fn sse_without_content_type_usage_and_failover() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let stream = r#"{"model":"gpt-5.5","stream":true,"input":[]}"#;
    // usage tokens: stream:true hint (v1) and Accept hint (backend-api, no stream field)
    let (ct, t) = noct_post(&e, "/v1/responses", "usage", stream, false).await;
    assert_eq!(ct, "text/event-stream", "content-type added downstream");
    assert!(t.contains("r-u"));
    let (_, t) = noct_post(
        &e,
        "/backend-api/codex/responses",
        "usage",
        r#"{"model":"gpt-5.5","input":[]}"#,
        true,
    )
    .await;
    assert!(t.contains("r-u"));
    let rows = wait_rows(&e, 2).await;
    for u in &rows {
        assert_eq!(u["input_tokens"], 120, "{u}");
        assert_eq!(u["output_tokens"], 30, "{u}");
        assert!(u["ttft_ms"].is_u64(), "{u}");
    }
    // pre-output usage_limit_reached replays on the next account, both paths
    for path in ["/v1/responses", "/backend-api/codex/responses"] {
        let before = e.mock.seen.lock().unwrap().len();
        let (_, t) = noct_post(&e, path, "midlimit", stream, false).await;
        assert!(!t.contains("usage_limit_reached"), "{path}: {t}");
        assert!(t.contains("r-b") && !t.contains("r-a"), "{path}: {t}");
        assert_eq!(e.mock.seen.lock().unwrap().len(), before + 2, "{path}");
    }
    // compaction turn without content-type
    let body = r#"{"model":"gpt-5.5","stream":true,"input":[{"type":"compaction_trigger"}]}"#;
    let (ct, t) = noct_post(&e, "/v1/responses", "compact", body, false).await;
    assert_eq!(ct, "text/event-stream");
    assert!(t.contains("\"encrypted_content\":\"gAAA\""), "{t}");
}

#[tokio::test]
async fn no_content_type_without_stream_hint_stays_passthrough() {
    let e = env(300).await;
    let (ct, t) = noct_post(
        &e,
        "/v1/responses",
        "midlimit",
        r#"{"model":"gpt-5.5","input":[]}"#,
        false,
    )
    .await;
    assert!(ct.is_empty(), "no hint: headers untouched");
    assert!(t.contains("usage_limit_reached"), "{t}");
}

#[tokio::test]
async fn usage_captures_response_model_effort_and_tier() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    // SSE: Fast requested (legacy "fast"), downgraded; no request effort; dated snapshot.
    let r = e
        .post("/v1/responses")
        .header("x-test-mode", "meta")
        .body(r#"{"model":"gpt-5.5","service_tier":"fast","stream":true,"input":[]}"#)
        .send()
        .await
        .unwrap();
    r.text().await.unwrap();
    // created-only stream: auto never becomes the served tier
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "meta_created_only")
        .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
        .send()
        .await
        .unwrap();
    r.text().await.unwrap();
    // WS turn: real mismatch + flex
    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","model":"gpt-5.5","service_tier":"flex","reasoning":{"effort":"high"},"tag":"meta"}"#,
    ))
    .await
    .unwrap();
    loop {
        let t = ws
            .next()
            .await
            .unwrap()
            .unwrap()
            .to_text()
            .unwrap()
            .to_string();
        if t.contains("response.completed") {
            break;
        }
    }
    drop(ws);
    let rows = wait_rows(&e, 3).await;
    assert_eq!(rows.len(), 3, "{rows:?}");
    let (w, created, sse) = (&rows[0], &rows[1], &rows[2]);
    assert_eq!(sse["requested_service_tier"], "priority");
    assert_eq!(sse["service_tier"], "default");
    assert_eq!(sse["response_model"], "gpt-5.5-2026-09-30");
    assert_eq!(
        sse["response_effort"], "medium",
        "completed wins over created"
    );
    assert!(sse["reasoning_effort"].is_null());
    assert_eq!(sse["model_mismatch"], false);
    assert_eq!(created["response_model"], "gpt-5.5");
    assert_eq!(created["response_effort"], "high");
    assert!(created["service_tier"].is_null(), "auto ignored: {created}");
    assert!(created["requested_service_tier"].is_null());
    assert_eq!(w["transport"], "ws");
    assert_eq!(w["requested_service_tier"], "flex");
    assert_eq!(w["service_tier"], "flex");
    assert_eq!(w["response_model"], "gpt-5.6-luna");
    assert_eq!(w["model_mismatch"], true);
    // summary grouped by response model
    let v: serde_json::Value = e
        .http
        .get(format!(
            "http://{}/usage/v1/summary?range=1h&group=response_model",
            e.proxy
        ))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let gs = v["summary"]["groups"].as_array().unwrap();
    assert!(
        gs.iter()
            .any(|g| g["group"]["response_model"] == "gpt-5.6-luna"),
        "{v}"
    );
}
