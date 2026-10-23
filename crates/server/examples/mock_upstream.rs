//! E2E mock of chatgpt.com/backend-api (Codex Responses SSE + WS, models, wham) and
//! auth.openai.com device-code login. State dir ($MOCK_DIR, default /tmp/cpa-e2e):
//!   limited      — account ids (one per line) that answer 429 usage_limit_reached
//!   midlimited   — account ids that answer 200 + `response.created`, then an in-stream
//!                  usage_limit_reached error (SSE `error` event / WS error frame)
//!   midlimit_business_only — when present, WS `generate:false` prewarm skips midlimited
//!   encrypted    — account ids that reject the request's encrypted reasoning content the way
//!                  the real upstream does (SSE: 200 + `error` event `invalid_encrypted_content`;
//!                  WS: rate-limit + metadata frames, then the error frame with status 400)
//!   lateencrypted — same rejection, but after some output (SSE / WS)
//!   quota-<acct> — value for x-codex-primary-used-percent
//!   delay_ms     — pause between WS response events (slow turns)
//!   toolcalls    — N: answer with a shell tool call (`sleep 2`) until the input carries N
//!                  tool outputs (multi-request turns)
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
    fn listed(&self, file: &str, acct: &str) -> bool {
        std::fs::read_to_string(self.dir.join(file))
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

/// `toolcalls` mode: a shell call (whichever shell tool the client offers) while the request
/// has fewer than N tool outputs.
fn tool_events(dir: &std::path::Path, id: &str, body: &Value) -> Option<Vec<Value>> {
    let n: usize = std::fs::read_to_string(dir.join("toolcalls"))
        .ok()?
        .trim()
        .parse()
        .ok()?;
    let input = body.get("input").and_then(|v| v.as_array())?;
    let done = input
        .iter()
        .filter(|i| i.get("type").and_then(|t| t.as_str()) == Some("function_call_output"))
        .count();
    if done >= n {
        return None;
    }
    let names: Vec<&str> = body
        .get("tools")
        .and_then(|t| t.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
                .collect()
        })
        .unwrap_or_default();
    let (name, args) = if names.contains(&"exec_command") {
        ("exec_command", json!({"cmd":"sleep 2; echo hi"}))
    } else if names.contains(&"shell_command") {
        ("shell_command", json!({"command":"sleep 2; echo hi"}))
    } else {
        (
            "shell",
            json!({"command":["bash","-lc","sleep 2; echo hi"]}),
        )
    };
    let call = format!("call_{id}");
    let item = json!({"type":"function_call","id":format!("fc_{id}"),"call_id":call,"name":name,"arguments":args.to_string()});
    Some(vec![
        json!({"type":"response.created","response":{"id":id,"status":"in_progress","output":[]}}),
        json!({"type":"response.output_item.added","output_index":0,"item":item}),
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
        json!({"type":"response.completed","response":{"id":id,"status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}),
    ])
}

/// Upstream `invalid_encrypted_content` (real wording); `status` is set on WS only.
fn enc_error(status: Option<u16>) -> Value {
    let mut v = json!({"type":"error","error":{"message":"The encrypted content for item rs_0e2e could not be verified. Reason: Encrypted content organization_id did not match the target organization.","type":"invalid_request_error","param":"input","code":"invalid_encrypted_content"}});
    if let Some(s) = status {
        v["status"] = s.into();
    }
    v
}

/// The encrypted-content rejection, before (`late = false`) or after some output.
fn encrypted(id: &str, late: bool, ws: bool) -> Vec<Value> {
    let mut v = Vec::new();
    if ws {
        v.push(json!({"type":"codex.rate_limits","rate_limits":{}}));
        v.push(json!({"type":"codex.response.metadata","headers":{}}));
    }
    if late {
        let mut out = events(id, "partial ");
        out.truncate(3);
        v.extend(out);
    }
    v.push(enc_error(ws.then_some(400)));
    v
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
    } else if s.listed("encrypted", &acct) || s.listed("lateencrypted", &acct) {
        encrypted(&id, s.listed("lateencrypted", &acct), false)
    } else if let Some(t) = tool_events(&s.dir, &id, &body) {
        t
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
            s.log(json!({"t":"ws-frame","acct":acct,"type":v.get("type"),"prev":v.get("previous_response_id"),"generate":v.get("generate")}));
            if s.limited(&acct) {
                let _ = sock.send(Message::Text(json!({"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"limit"}}).to_string().into())).await;
                continue;
            }
            let id = format!("resp_{}", s.resp.fetch_add(1, Ordering::SeqCst));
            let prewarm = v.get("generate").and_then(Value::as_bool) == Some(false);
            let business_only = s.dir.join("midlimit_business_only").exists();
            let evs = if s.midlimited(&acct) && !(prewarm && business_only) {
                mid_limit(&id)
            } else if s.listed("encrypted", &acct) || s.listed("lateencrypted", &acct) {
                encrypted(&id, s.listed("lateencrypted", &acct), true)
            } else if let Some(t) = tool_events(&s.dir, &id, &v) {
                t
            } else {
                events(&id, &format!("ok from {acct}"))
            };
            let delay = std::fs::read_to_string(s.dir.join("delay_ms"))
                .ok()
                .and_then(|x| x.trim().parse::<u64>().ok())
                .unwrap_or(0);
            for e in evs {
                if delay > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                }
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
        .route(
            "/cdn-cgi/trace",
            get(|| async {
                "fl=1\nh=mock\nip=203.0.113.9\nts=0\nhttp=http/1.1\nloc=ZZ\ncolo=MCK\n"
            }),
        )
        .with_state(st);
    let ln = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!("mock on {addr}");
    axum::serve(ln, app).await.unwrap();
}
