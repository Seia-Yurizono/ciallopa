#![allow(clippy::unwrap_used)]
//! Mock OAuth server on 127.0.0.1 (no real network).
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::post, Json, Router};
use base64::Engine;
use cpa_lite_auth::{login, FileAccountStore, OAuthClient};
use cpa_lite_core::{AccountStore, AuthError};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct Mock {
    refreshes: AtomicUsize,
    polls: AtomicUsize,
    /// "ok" | "invalid_grant" | "500"
    mode: Mutex<String>,
    last_body: Mutex<String>,
    last_ct: Mutex<String>,
}

fn jwt(v: Value) -> String {
    let b = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(v.to_string());
    format!("e30.{b}.sig")
}
fn tokens(n: usize) -> Value {
    let exp = chrono::Utc::now().timestamp() + 3600;
    json!({
        "access_token": jwt(json!({"exp": exp, "n": n})),
        "refresh_token": format!("rt-{n}"),
        "id_token": jwt(json!({"email":"a@x.com","https://api.openai.com/auth":{"chatgpt_account_id":"acc-1","chatgpt_plan_type":"plus"}})),
        "expires_in": 3600
    })
}

async fn token(
    State(m): State<Arc<Mock>>,
    headers: axum::http::HeaderMap,
    body: String,
) -> axum::response::Response {
    *m.last_ct.lock().unwrap() = headers
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    *m.last_body.lock().unwrap() = body.clone();
    if body.contains("authorization_code") {
        return Json(tokens(0)).into_response();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let n = m.refreshes.fetch_add(1, Ordering::SeqCst) + 1;
    match m.mode.lock().unwrap().as_str() {
        "invalid_grant" => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"invalid_grant"})),
        )
            .into_response(),
        "500" => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        _ => Json(tokens(n)).into_response(),
    }
}
async fn usercode(Json(b): Json<Value>) -> Json<Value> {
    assert_eq!(b["client_id"], "app_EMoamEEZ73f0CkXaXp7hrann");
    Json(json!({"device_auth_id":"dev-1","user_code":"ABCD-EFGH","interval":"0"}))
}
async fn devtoken(State(m): State<Arc<Mock>>, Json(b): Json<Value>) -> axum::response::Response {
    assert_eq!(b["device_auth_id"], "dev-1");
    if m.polls.fetch_add(1, Ordering::SeqCst) < 2 {
        return StatusCode::FORBIDDEN.into_response();
    }
    Json(json!({"authorization_code":"code-1","code_challenge":"c","code_verifier":"v"}))
        .into_response()
}

async fn spawn() -> (Arc<Mock>, OAuthClient) {
    let m = Arc::new(Mock::default());
    *m.mode.lock().unwrap() = "ok".into();
    let app = Router::new()
        .route("/oauth/token", post(token))
        .route("/api/accounts/deviceauth/usercode", post(usercode))
        .route("/api/accounts/deviceauth/token", post(devtoken))
        .with_state(m.clone());
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ln.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(ln, app).await.unwrap() });
    (
        m,
        OAuthClient::with_issuer(&format!("http://{addr}"), reqwest::Client::new()),
    )
}

fn expired_file(dir: &std::path::Path) {
    cpa_lite_auth::file::write_auth_file(
        dir,
        "acct",
        &json!({"type":"codex","access_token":"old","refresh_token":"rt-0","account_id":"acc-1",
                "expired":"2000-01-01T00:00:00Z","priority":3,"custom":"keep"}),
    )
    .unwrap();
}

#[tokio::test]
async fn proactive_refresh_single_flight_and_persist() {
    let (m, oauth) = spawn().await;
    let d = tempfile::tempdir().unwrap();
    expired_file(d.path());
    let s = Arc::new(
        FileAccountStore::open_with(d.path().into(), oauth)
            .await
            .unwrap(),
    );
    let id = "acct".to_string();
    let hs: Vec<_> = (0..8)
        .map(|_| {
            let (s, id) = (s.clone(), id.clone());
            tokio::spawn(async move { s.credentials(&id).await.unwrap() })
        })
        .collect();
    let mut toks = vec![];
    for h in hs {
        toks.push(h.await.unwrap().access_token);
    }
    assert_eq!(m.refreshes.load(Ordering::SeqCst), 1, "single flight");
    assert!(toks.iter().all(|t| t == &toks[0] && t != "old"));
    // codex-rs refresh wire format: JSON body
    assert_eq!(*m.last_ct.lock().unwrap(), "application/json");
    let b: Value = serde_json::from_str(&m.last_body.lock().unwrap()).unwrap();
    assert_eq!(
        b,
        json!({"client_id":"app_EMoamEEZ73f0CkXaXp7hrann","grant_type":"refresh_token","refresh_token":"rt-0"})
    );
    // persisted, 0600, extra fields kept
    let p = d.path().join("acct.json");
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        std::fs::metadata(&p).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let v: Value = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
    assert_eq!(v["refresh_token"], "rt-1");
    assert_eq!(v["custom"], "keep");
    assert_eq!(v["priority"], 3);
    assert_eq!(v["email"], "a@x.com");
    // fresh now: no more refresh
    s.credentials(&id).await.unwrap();
    assert_eq!(m.refreshes.load(Ordering::SeqCst), 1);
    // force_refresh dedups concurrent callers right after a refresh
    let a = s.force_refresh(&id).await.unwrap();
    assert_eq!(m.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(a.access_token, toks[0]);
}

#[tokio::test]
async fn force_refresh_and_invalid_grant_disables() {
    let (m, oauth) = spawn().await;
    let d = tempfile::tempdir().unwrap();
    cpa_lite_auth::file::write_auth_file(
        d.path(),
        "acct",
        &json!({"type":"codex","access_token":"cur","refresh_token":"rt-0","expired":"2999-01-01T00:00:00Z"}),
    )
    .unwrap();
    let s = FileAccountStore::open_with(d.path().into(), oauth)
        .await
        .unwrap();
    let id = "acct".to_string();
    assert_eq!(s.credentials(&id).await.unwrap().access_token, "cur");
    assert_ne!(s.force_refresh(&id).await.unwrap().access_token, "cur");
    assert_eq!(m.refreshes.load(Ordering::SeqCst), 1);

    // transient failure on force -> Refresh error, account untouched
    *m.mode.lock().unwrap() = "500".into();
    tokio::time::sleep(Duration::from_secs(11)).await; // past force dedup window
    assert!(matches!(
        s.force_refresh(&id).await,
        Err(AuthError::Refresh(_))
    ));
    assert!(!s.list().await[0].disabled);

    *m.mode.lock().unwrap() = "invalid_grant".into();
    assert!(matches!(
        s.force_refresh(&id).await,
        Err(AuthError::ReloginRequired)
    ));
    assert!(s.list().await[0].disabled);
    assert!(matches!(
        s.credentials(&id).await,
        Err(AuthError::ReloginRequired)
    ));
    // survives reload
    s.reload().await.unwrap();
    assert!(s.list().await[0].disabled);
}

#[tokio::test]
async fn proactive_transient_failure_keeps_valid_token() {
    let (m, oauth) = spawn().await;
    *m.mode.lock().unwrap() = "500".into();
    let d = tempfile::tempdir().unwrap();
    let soon = (chrono::Utc::now() + chrono::Duration::minutes(2)).to_rfc3339();
    cpa_lite_auth::file::write_auth_file(
        d.path(),
        "acct",
        &json!({"access_token":"cur","refresh_token":"r","expired":soon}),
    )
    .unwrap();
    let s = FileAccountStore::open_with(d.path().into(), oauth)
        .await
        .unwrap();
    assert_eq!(
        s.credentials(&"acct".into()).await.unwrap().access_token,
        "cur"
    );
}

#[tokio::test]
async fn device_code_login() {
    let (m, oauth) = spawn().await;
    let d = tempfile::tempdir().unwrap();
    let mut shown = None;
    let id = login::device_code_with(&oauth, d.path(), Duration::from_secs(5), |u, c| {
        shown = Some((u.to_string(), c.to_string()))
    })
    .await
    .unwrap();
    let (u, c) = shown.unwrap();
    assert!(u.ends_with("/codex/device"));
    assert_eq!(c, "ABCD-EFGH");
    assert_eq!(m.polls.load(Ordering::SeqCst), 3);
    assert_eq!(id, "codex-a@x.com-plus");
    assert_eq!(
        *m.last_ct.lock().unwrap(),
        "application/x-www-form-urlencoded"
    );
    let body = m.last_body.lock().unwrap().clone();
    assert!(
        body.contains("grant_type=authorization_code")
            && body.contains("code=code-1")
            && body.contains("code_verifier=v")
    );
    assert!(body.contains("deviceauth%2Fcallback"));
    let s = FileAccountStore::open(d.path().into()).await.unwrap();
    let a = &s.list().await[0];
    assert_eq!(a.chatgpt_account_id.as_deref(), Some("acc-1"));
    assert_eq!(a.plan_type.as_deref(), Some("plus"));
}

fn state_of(u: &str) -> String {
    url::Url::parse(u)
        .unwrap()
        .query_pairs()
        .find(|(k, _)| k == "state")
        .unwrap()
        .1
        .into_owned()
}

#[tokio::test]
async fn paste_callback_login() {
    let (m, oauth) = spawn().await;
    let d = tempfile::tempdir().unwrap();
    let (mut w, r) = tokio::io::duplex(4096);
    let shown: Arc<Mutex<Option<String>>> = Default::default();
    let sh = shown.clone();
    let t = tokio::spawn({
        let oauth = oauth.clone();
        let dir = d.path().to_path_buf();
        async move {
            login::paste_callback_with(&oauth, &dir, tokio::io::BufReader::new(r), move |u| {
                *sh.lock().unwrap() = Some(u.into())
            })
            .await
        }
    });
    while shown.lock().unwrap().is_none() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let st = state_of(shown.lock().unwrap().as_ref().unwrap());
    use tokio::io::AsyncWriteExt;
    w.write_all(format!("http://localhost:1455/auth/callback?code=pc&state={st}\n").as_bytes())
        .await
        .unwrap();
    assert_eq!(t.await.unwrap().unwrap(), "codex-a@x.com-plus");
    let body = m.last_body.lock().unwrap().clone();
    assert!(
        body.contains("code=pc")
            && body.contains("redirect_uri=http%3A%2F%2Flocalhost%3A1455%2Fauth%2Fcallback")
    );
    assert!(matches!(
        login::parse_callback("?code=x&state=bad", "st"),
        Err(AuthError::Invalid(_))
    ));
}

#[tokio::test]
async fn pkce_localhost_login_and_timeout() {
    let (_m, oauth) = spawn().await;
    let d = tempfile::tempdir().unwrap();
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let shown: Arc<Mutex<Option<String>>> = Default::default();
    let sh = shown.clone();
    let t = tokio::spawn({
        let oauth = oauth.clone();
        let dir = d.path().to_path_buf();
        async move {
            login::pkce_localhost_with(&oauth, &dir, port, Duration::from_secs(5), move |u| {
                *sh.lock().unwrap() = Some(u.into())
            })
            .await
        }
    });
    while shown.lock().unwrap().is_none() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let st = state_of(shown.lock().unwrap().as_ref().unwrap());
    let http = reqwest::Client::new();
    // wrong state is ignored, then the real callback succeeds
    let r = http
        .get(format!(
            "http://127.0.0.1:{port}/auth/callback?code=c&state=nope"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let r = http
        .get(format!(
            "http://127.0.0.1:{port}/auth/callback?code=c&state={st}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(t.await.unwrap().unwrap(), "codex-a@x.com-plus");

    let e = login::pkce_localhost_with(&oauth, d.path(), port, Duration::from_millis(100), |_| {})
        .await;
    assert!(matches!(e, Err(AuthError::Invalid(_))));
}

#[tokio::test]
async fn import_codex_auth() {
    let d = tempfile::tempdir().unwrap();
    let src = d.path().join("auth.json");
    let exp = chrono::Utc::now().timestamp() + 999;
    std::fs::write(&src, json!({"OPENAI_API_KEY":null,"tokens":{
        "id_token": jwt(json!({"email":"b@y.com","https://api.openai.com/auth":{"chatgpt_plan_type":"pro"}})),
        "access_token": jwt(json!({"exp":exp})), "refresh_token":"r", "account_id":"acc-9"},
        "last_refresh":"2026-10-01T00:00:00Z"}).to_string()).unwrap();
    let dir = d.path().join("auths");
    let id = login::import_codex_auth_json(&src, &dir).await.unwrap();
    assert_eq!(id, "codex-b@y.com-pro");
    let s = FileAccountStore::open(dir).await.unwrap();
    let a = &s.list().await[0];
    assert_eq!(a.chatgpt_account_id.as_deref(), Some("acc-9"));
    assert_eq!(
        s.credentials(&id)
            .await
            .unwrap()
            .chatgpt_account_id
            .as_deref(),
        Some("acc-9")
    );
    std::fs::write(&src, r#"{"OPENAI_API_KEY":"sk-x"}"#).unwrap();
    assert!(login::import_codex_auth_json(&src, d.path()).await.is_err());
}
