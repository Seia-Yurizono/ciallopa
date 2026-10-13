//! E2E mock of chatgpt.com/backend-api (Codex Responses SSE + WS, models, wham) and
//! auth.openai.com device-code login. State dir ($MOCK_DIR, default /tmp/cpa-e2e):
//!   limited      — account ids (one per line) that answer 429 usage_limit_reached
//!   midlimited   — account ids that answer 200 + `response.created`, then an in-stream
//!                  usage_limit_reached error (SSE `error` event / WS error frame)
//!   quota-<acct> — value for x-codex-primary-used-percent
//!   upstream.log — one JSON line per upstream request / WS frame
//! Usage: cargo run --example mock_upstream -- 127.0.0.1:18080
#![allow(clippy::unwrap_used)]
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{body::Bytes, Router};
use base64::Engine;
use serde_json::{json, Value};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone)]
struct St {
    dir: PathBuf,
    logins: Arc<AtomicUsize>,
    resp: Arc<AtomicUsize>,
}
impl St {
    fn log(&self, v: Value) {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("upstream.log"))
            .unwrap();
        writeln!(f, "{v}").unwrap();
    }
    fn limited(&self, acct: &str) -> bool {
        std::fs::read_to_string(self.dir.join("limited"))
            .unwrap_or_default()
            .lines()
            .any(|l| l.trim() == acct)
    }
    fn midlimited(&self, acct: &str) -> bool {
        std::fs::read_to_string(self.dir.join("midlimited"))
            .unwrap_or_default()
            .lines()
            .any(|l| l.trim() == acct)
    }
    fn quota(&self, acct: &str) -> String {
        std::fs::read_to_string(self.dir.join(format!("quota-{acct}")))
            .unwrap_or_else(|_| "10".into())
            .trim()
            .to_string()
    }
}

fn hv(h: &HeaderMap, k: &str) -> String {
    h.get(k)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

const LIMIT: &str =
    r#"{"error":{"type":"usage_limit_reached","message":"limit","resets_in_seconds":3600}}"#;

fn events(id: &str, text: &str) -> Vec<Value> {
    vec![
        json!({"type":"response.created","response":{"id":id,"status":"in_progress","output":[]}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg_1","role":"assistant","content":[]}}),
        json!({"type":"response.output_text.delta","delta":text,"item_id":"msg_1","output_index":0,"content_index":0}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":"msg_1","role":"assistant","content":[{"type":"output_text","text":text}]}}),
        json!({"type":"response.completed","response":{"id":id,"status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}),
    ]
}

fn mid_limit(id: &str) -> Vec<Value> {
    vec![
        json!({"type":"response.created","response":{"id":id,"status":"in_progress","output":[]}}),
        json!({"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"limit","resets_in_seconds":3600}}),
    ]
}

async fn responses(State(s): State<St>, h: HeaderMap, b: Bytes) -> Response {
    let acct = hv(&h, "chatgpt-account-id");
    let plain = if hv(&h, "content-encoding") == "zstd" {
        zstd::decode_all(&b[..]).unwrap_or_default()
    } else {
        b.to_vec()
    };
    let body: Value = serde_json::from_slice(&plain).unwrap_or_default();
    s.log(json!({"t":"sse","acct":acct,"session":hv(&h,"session-id"),"auth":hv(&h,"authorization").len()>20,"zstd":hv(&h,"content-encoding"),"prev":body.get("previous_response_id")}));
    if s.limited(&acct) {
        return (axum::http::StatusCode::TOO_MANY_REQUESTS, LIMIT).into_response();
    }
    let n = s.resp.fetch_add(1, Ordering::SeqCst);
    let id = format!("resp_{n}");
    let mut out = String::new();
    let evs = if s.midlimited(&acct) {
        mid_limit(&id)
    } else {
        events(&id, &format!("ok from {acct}"))
    };
    for e in evs {
        out.push_str(&format!(
            "event: {}\ndata: {e}\n\n",
            e["type"].as_str().unwrap()
        ));
    }
    Response::builder()
        .header("content-type", "text/event-stream")
        .header("x-codex-primary-used-percent", s.quota(&acct))
        .header("x-codex-primary-window-minutes", "300")
        .body(out.into())
        .unwrap()
}

async fn ws(State(s): State<St>, h: HeaderMap, up: WebSocketUpgrade) -> Response {
    let acct = hv(&h, "chatgpt-account-id");
    s.log(json!({"t":"ws","acct":acct,"session":hv(&h,"session-id"),"ext":hv(&h,"sec-websocket-extensions")}));
    if s.limited(&acct) {
        return (axum::http::StatusCode::TOO_MANY_REQUESTS, LIMIT).into_response();
    }
    let q = s.quota(&acct);
    let mut r = up.on_upgrade(move |mut sock| async move {
        while let Some(Ok(m)) = sock.recv().await {
            let Message::Text(t) = m else { continue };
            let v: Value = serde_json::from_str(t.as_str()).unwrap_or_default();
            s.log(json!({"t":"ws-frame","acct":acct,"type":v.get("type"),"prev":v.get("previous_response_id")}));
            if s.limited(&acct) {
                let _ = sock.send(Message::Text(json!({"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"limit"}}).to_string().into())).await;
                continue;
            }
            let id = format!("resp_{}", s.resp.fetch_add(1, Ordering::SeqCst));
            let evs = if s.midlimited(&acct) {
                mid_limit(&id)
            } else {
                events(&id, &format!("ok from {acct}"))
            };
            for e in evs {
                if sock.send(Message::Text(e.to_string().into())).await.is_err() {
                    return;
                }
            }
        }
    });
    r.headers_mut()
        .insert("x-codex-primary-used-percent", q.parse().unwrap());
    r
}

async fn models(State(s): State<St>, h: HeaderMap) -> Response {
    s.log(json!({"t":"models","acct":hv(&h,"chatgpt-account-id")}));
    ([("content-type", "application/json")], r#"{"models":[]}"#).into_response()
}

fn b64(v: &Value) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(v.to_string())
}

async fn usercode() -> Response {
    (
        [("content-type", "application/json")],
        r#"{"device_auth_id":"dev-1","user_code":"ABCD-EFGH","interval":"1"}"#,
    )
        .into_response()
}
async fn devtoken() -> Response {
    (
        [("content-type", "application/json")],
        r#"{"authorization_code":"code-1","code_verifier":"ver-1"}"#,
    )
        .into_response()
}
async fn oauth_token(State(s): State<St>, b: Bytes) -> Response {
    let body = String::from_utf8_lossy(&b).to_string();
    let n = s.logins.fetch_add(1, Ordering::SeqCst) + 1;
    s.log(json!({"t":"oauth","grant":if body.contains("refresh_token") {"refresh"} else {"code"},"n":n}));
    let claims = json!({"email":format!("u{n}@e2e.test"),"exp":4102444800i64,
        "https://api.openai.com/auth":{"chatgpt_account_id":format!("acc-{n}"),"chatgpt_plan_type":"plus"}});
    let jwt = format!("e30.{}.sig", b64(&claims));
    Json(json!({"access_token":jwt,"id_token":jwt,"refresh_token":format!("rt-{n}"),"expires_in":864000})).into_response()
}
use axum::Json;

#[tokio::main]
async fn main() {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:18080".into());
    let dir = PathBuf::from(std::env::var("MOCK_DIR").unwrap_or_else(|_| "/tmp/cpa-e2e".into()));
    std::fs::create_dir_all(&dir).unwrap();
    let st = St {
        dir,
        logins: Arc::default(),
        resp: Arc::default(),
    };
    let app = Router::new()
        .route("/backend-api/codex/responses", post(responses).get(ws))
        .route("/backend-api/codex/models", get(models))
        .route("/backend-api/wham/usage", get(models))
        .route("/api/accounts/deviceauth/usercode", post(usercode))
        .route("/api/accounts/deviceauth/token", post(devtoken))
        .route("/oauth/token", post(oauth_token))
        .with_state(st);
    let ln = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!("mock on {addr}");
    axum::serve(ln, app).await.unwrap();
}
