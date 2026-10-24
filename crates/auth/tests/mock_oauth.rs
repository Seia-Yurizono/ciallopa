#![allow(clippy::unwrap_used)]
//! Mock OAuth server on 127.0.0.1 (no real network).
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::post, Json, Router};
use base64::Engine;
use ciallopa_auth::{login, FileAccountStore, OAuthClient};
use ciallopa_core::{AccountStore, AuthError};
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
    /// Simulates a writer that ignores our flock: written while the refresh is in flight.
    foreign: Mutex<Option<(std::path::PathBuf, Value)>>,
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
    if let Some((dir, v)) = m.foreign.lock().unwrap().take() {
        std::fs::write(dir.join("acct.json"), v.to_string()).unwrap();
    }
    match m.mode.lock().unwrap().as_str() {
        "reused" => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":{"code":"refresh_token_reused","message":"x"}})),
        )
            .into_response(),
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
    ciallopa_auth::file::write_auth_file(
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
    // codex-rs 0.164: keys sorted (BTreeMap), `refresh_reason` for an expiring token
    assert_eq!(
        *m.last_body.lock().unwrap(),
        r#"{"client_id":"app_EMoamEEZ73f0CkXaXp7hrann","grant_type":"refresh_token","refresh_reason":"proactive_expiry","refresh_token":"rt-0"}"#
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
    ciallopa_auth::file::write_auth_file(
        d.path(),
        "acct",
        &json!({"type":"codex","access_token":"cur","refresh_token":"rt-0","expired":"2999-01-01T00:00:00Z"}),
    )
    .unwrap();
    let s = FileAccountStore::open_with(d.path().into(), oauth)
        .await
        .unwrap()
        .with_force_dedup(Duration::from_millis(200));
    let id = "acct".to_string();
    assert_eq!(s.credentials(&id).await.unwrap().access_token, "cur");
    assert_ne!(s.force_refresh(&id).await.unwrap().access_token, "cur");
    assert_eq!(m.refreshes.load(Ordering::SeqCst), 1);

    // transient failure on force -> Refresh error, account untouched
    *m.mode.lock().unwrap() = "500".into();
    tokio::time::sleep(Duration::from_millis(250)).await; // past force dedup window
    let r = s.force_refresh(&id).await;
    assert!(matches!(r, Err(AuthError::Refresh(_))), "{r:?}");
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
    ciallopa_auth::file::write_auth_file(
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
    assert_eq!(id, "codex-cf9df9b9-a@x.com-plus");
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
    assert_eq!(t.await.unwrap().unwrap(), "codex-cf9df9b9-a@x.com-plus");
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
    assert_eq!(t.await.unwrap().unwrap(), "codex-cf9df9b9-a@x.com-plus");

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
    assert_eq!(id, "codex-82a524c4-b@y.com-pro");
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

#[tokio::test]
async fn eight_day_rule_refreshes_even_if_not_expired() {
    let (m, oauth) = spawn().await;
    let d = tempfile::tempdir().unwrap();
    let old = (chrono::Utc::now() - chrono::Duration::days(9)).to_rfc3339();
    ciallopa_auth::file::write_auth_file(
        d.path(),
        "acct",
        &json!({"access_token":"cur","refresh_token":"r",
        "expired":"2999-01-01T00:00:00Z","last_refresh":old}),
    )
    .unwrap();
    let s = FileAccountStore::open_with(d.path().into(), oauth)
        .await
        .unwrap();
    assert_ne!(
        s.credentials(&"acct".into()).await.unwrap().access_token,
        "cur"
    );
    assert_eq!(m.refreshes.load(Ordering::SeqCst), 1);
    s.credentials(&"acct".into()).await.unwrap();
    assert_eq!(m.refreshes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn legacy_names_migrate() {
    let (_m, oauth) = spawn().await;
    let d = tempfile::tempdir().unwrap();
    // legacy file name from the first ciallopa version, with a routing field
    ciallopa_auth::file::write_auth_file(
        d.path(),
        "codex-a@x.com-plus",
        &json!({"access_token":"t","refresh_token":"r",
        "account_id":"acc-1","email":"a@x.com","plan_type":"plus","weight":7}),
    )
    .unwrap();
    // re-login merges into the hashed name and drops the legacy file
    let id = login::save_tokens(d.path(), &serde_json::from_value(tokens(0)).unwrap()).unwrap();
    assert_eq!(id, "codex-cf9df9b9-a@x.com-plus");
    assert!(!d.path().join("codex-a@x.com-plus.json").exists());
    let v: Value =
        serde_json::from_slice(&std::fs::read(d.path().join(format!("{id}.json"))).unwrap())
            .unwrap();
    assert_eq!(v["weight"], 7);
    // startup migration renames
    ciallopa_auth::file::write_auth_file(
        d.path(),
        "codex-b@y.com",
        &json!({"access_token":"t","account_id":"acc-9","email":"b@y.com"}),
    )
    .unwrap();
    let s = FileAccountStore::open_with(d.path().into(), oauth)
        .await
        .unwrap();
    let ids: Vec<_> = s.list().await.into_iter().map(|a| a.id).collect();
    assert!(
        ids.contains(&"codex-82a524c4-b@y.com".to_string()),
        "{ids:?}"
    );
    assert_eq!(ids.len(), 2);
}

#[tokio::test]
async fn refresh_waits_for_cross_process_lock_and_rereads() {
    let (m, oauth) = spawn().await;
    let d = tempfile::tempdir().unwrap();
    expired_file(d.path());
    let s = Arc::new(
        FileAccountStore::open_with(d.path().into(), oauth)
            .await
            .unwrap(),
    );
    // "other process": holds the flock, refreshes the file on disk, then releases.
    let g = ciallopa_auth::file::FileLock::acquire(d.path(), "acct").unwrap();
    let t = tokio::spawn({
        let s = s.clone();
        async move { s.credentials(&"acct".into()).await.unwrap() }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!t.is_finished());
    let fresh = (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
    ciallopa_auth::file::write_auth_file(
        d.path(),
        "acct",
        &json!({"access_token":"by-other","refresh_token":"rt-x","expired":fresh}),
    )
    .unwrap();
    drop(g);
    assert_eq!(t.await.unwrap().access_token, "by-other");
    assert_eq!(m.refreshes.load(Ordering::SeqCst), 0);
}

fn valid_file(dir: &std::path::Path, at: &str, rt: &str) -> Value {
    let v = json!({"type":"codex","access_token":at,"refresh_token":rt,"account_id":"acc-1",
                   "expired":"2999-01-01T00:00:00Z","priority":2});
    ciallopa_auth::file::write_auth_file(dir, "acct", &v).unwrap();
    v
}

#[tokio::test]
async fn reused_refresh_token_needs_relogin_without_retry() {
    let (m, oauth) = spawn().await;
    *m.mode.lock().unwrap() = "reused".into();
    let d = tempfile::tempdir().unwrap();
    valid_file(d.path(), "cur", "rt-0");
    let s = FileAccountStore::open_with(d.path().into(), oauth)
        .await
        .unwrap()
        .with_force_dedup(Duration::ZERO);
    let id = "acct".to_string();
    assert!(s.relogin_required().await.is_empty());
    assert!(matches!(
        s.force_refresh(&id).await,
        Err(AuthError::ReloginRequired)
    ));
    for _ in 0..3 {
        assert!(matches!(
            s.force_refresh(&id).await,
            Err(AuthError::ReloginRequired)
        ));
        assert!(matches!(
            s.credentials(&id).await,
            Err(AuthError::ReloginRequired)
        ));
    }
    assert_eq!(m.refreshes.load(Ordering::SeqCst), 1, "no retry loop");
    assert_eq!(s.relogin_required().await, vec![id.clone()]);
    assert!(s.list().await[0].disabled, "excluded from routing");
    let v: Value =
        serde_json::from_slice(&std::fs::read(d.path().join("acct.json")).unwrap()).unwrap();
    assert_eq!(v["needs_relogin"], true);
    assert_eq!(v["priority"], 2);
    assert!(v["relogin_reason"]
        .as_str()
        .unwrap()
        .contains("refresh_token_reused"));
}

#[tokio::test]
async fn force_refresh_adopts_newer_disk_token() {
    let (m, oauth) = spawn().await;
    let d = tempfile::tempdir().unwrap();
    valid_file(d.path(), "stale", "rt-0");
    let s = FileAccountStore::open_with(d.path().into(), oauth)
        .await
        .unwrap();
    // another process refreshed after we loaded the file
    valid_file(d.path(), "disk-new", "rt-9");
    let c = s.force_refresh(&"acct".to_string()).await.unwrap();
    assert_eq!(c.access_token, "disk-new");
    assert_eq!(m.refreshes.load(Ordering::SeqCst), 0, "no extra refresh");
}

#[tokio::test]
async fn foreign_write_during_refresh_is_not_clobbered() {
    let (m, oauth) = spawn().await;
    let d = tempfile::tempdir().unwrap();
    valid_file(d.path(), "cur", "rt-0");
    let s = FileAccountStore::open_with(d.path().into(), oauth)
        .await
        .unwrap();
    let mut foreign = valid_file(d.path(), "cur", "rt-0");
    foreign["access_token"] = "foreign".into();
    foreign["refresh_token"] = "rt-f".into();
    *m.foreign.lock().unwrap() = Some((d.path().into(), foreign));
    let c = s.force_refresh(&"acct".to_string()).await.unwrap();
    assert_eq!(c.access_token, "foreign");
    let v: Value =
        serde_json::from_slice(&std::fs::read(d.path().join("acct.json")).unwrap()).unwrap();
    assert_eq!(
        (v["access_token"].as_str(), v["refresh_token"].as_str()),
        (Some("foreign"), Some("rt-f"))
    );
    // normal case: disk unchanged during refresh → our tokens written, other keys kept
    tokio::time::sleep(Duration::from_millis(10)).await;
    let s2 = FileAccountStore::open_with(d.path().into(), spawn().await.1)
        .await
        .unwrap()
        .with_force_dedup(Duration::ZERO);
    let c = s2.force_refresh(&"acct".to_string()).await.unwrap();
    assert_ne!(c.access_token, "foreign");
    let v: Value =
        serde_json::from_slice(&std::fs::read(d.path().join("acct.json")).unwrap()).unwrap();
    assert_eq!(v["refresh_token"], "rt-1");
    assert_eq!(v["priority"], 2);
}

#[tokio::test]
async fn rejected_but_rotated_elsewhere_adopts_disk() {
    let (m, oauth) = spawn().await;
    *m.mode.lock().unwrap() = "reused".into();
    let d = tempfile::tempdir().unwrap();
    valid_file(d.path(), "cur", "rt-0");
    let s = FileAccountStore::open_with(d.path().into(), oauth)
        .await
        .unwrap();
    let mut foreign = valid_file(d.path(), "cur", "rt-0");
    foreign["access_token"] = "rotated".into();
    foreign["refresh_token"] = "rt-r".into();
    *m.foreign.lock().unwrap() = Some((d.path().into(), foreign));
    let c = s.force_refresh(&"acct".to_string()).await.unwrap();
    assert_eq!(c.access_token, "rotated");
    assert!(s.relogin_required().await.is_empty());
    assert!(!s.list().await[0].disabled);
}

fn reason(m: &Mock) -> String {
    let b: Value = serde_json::from_str(&m.last_body.lock().unwrap()).unwrap();
    b["refresh_reason"].as_str().unwrap_or_default().to_string()
}

/// codex-rs 0.164 `refresh_reason`: scheduled (old `last_refresh`), unauthorized_recovery
/// (after a 401), forced (manual refresh).
#[tokio::test]
async fn refresh_reason_per_path() {
    let (m, oauth) = spawn().await;
    let d = tempfile::tempdir().unwrap();
    let exp = chrono::Utc::now().timestamp() + 3 * 86400;
    let old = (chrono::Utc::now() - chrono::Duration::days(9)).to_rfc3339();
    ciallopa_auth::file::write_auth_file(
        d.path(),
        "acct",
        &json!({"type":"codex","access_token":jwt(json!({"exp":exp})),"refresh_token":"rt-0",
                "last_refresh":old}),
    )
    .unwrap();
    let s = FileAccountStore::open_with(d.path().into(), oauth)
        .await
        .unwrap()
        .with_force_dedup(Duration::from_millis(1));
    let id = "acct".to_string();
    s.credentials(&id).await.unwrap();
    assert_eq!(m.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(reason(&m), "scheduled");
    tokio::time::sleep(Duration::from_millis(5)).await;
    s.force_refresh(&id).await.unwrap();
    assert_eq!(m.refreshes.load(Ordering::SeqCst), 2);
    assert_eq!(reason(&m), "unauthorized_recovery");
    tokio::time::sleep(Duration::from_millis(5)).await;
    s.manual_refresh(&id).await.unwrap();
    assert_eq!(m.refreshes.load(Ordering::SeqCst), 3);
    assert_eq!(reason(&m), "forced");
}
