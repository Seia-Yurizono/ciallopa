//! Login flows; each ends with `file::write_auth_file` and returns the account id.
//! - device code: `{issuer}/api/accounts/deviceauth/{usercode,token}` (codex-rs device_code_auth.rs)
//! - paste callback URL: print PKCE auth URL, user pastes `http://localhost:1455/auth/callback?code=..&state=..`
//! - PKCE 1455: listener on 127.0.0.1 only, 5 min lifetime
//! - import: `~/.codex/auth.json`
use crate::file::{account_id_for, existing_for, merge_tokens, write_auth_file, FileLock};
use crate::oauth::{random_state, OAuthClient, Pkce, TokenResponse, LOCAL_REDIRECT_URI};
use ciallopa_core::AuthError;
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

pub const DEVICE_CODE_MAX_WAIT: Duration = Duration::from_secs(15 * 60);
pub const CALLBACK_LIFETIME: Duration = Duration::from_secs(5 * 60);
pub const CALLBACK_PORT: u16 = 1455;

/// Persist tokens as a new (or updated, merging routing fields) account file.
pub fn save_tokens(auth_dir: &Path, t: &TokenResponse) -> Result<String, AuthError> {
    let fresh = merge_tokens(None, t);
    let id = account_id_for(&fresh);
    let _g = FileLock::acquire(auth_dir, &id)?;
    let (id, prev, legacy) = existing_for(auth_dir, &fresh);
    let v = merge_tokens(prev.as_ref(), t);
    write_auth_file(auth_dir, &id, &v)?;
    remove_legacy(auth_dir, legacy);
    Ok(id)
}

fn remove_legacy(dir: &Path, legacy: Option<String>) {
    if let Some(l) = legacy {
        let _ = std::fs::remove_file(dir.join(format!("{l}.json")));
        tracing::info!(from = %l, "migrated legacy auth file name");
    }
}

// ---------- device code ----------
#[derive(Deserialize)]
struct UserCodeResp {
    device_auth_id: String,
    #[serde(alias = "usercode")]
    user_code: String,
    #[serde(default, deserialize_with = "de_interval")]
    interval: u64,
}
fn de_interval<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    let v = Value::deserialize(d)?;
    Ok(match v {
        Value::String(s) => s.trim().parse().unwrap_or(5),
        Value::Number(n) => n.as_u64().unwrap_or(5),
        _ => 5,
    })
}
#[derive(Deserialize)]
struct CodeSuccessResp {
    authorization_code: String,
    code_verifier: String,
}

pub async fn device_code(oauth: &OAuthClient, auth_dir: &Path) -> Result<String, AuthError> {
    device_code_with(oauth, auth_dir, DEVICE_CODE_MAX_WAIT, |url, code| {
        println!(
            "\n1. Open this link in your browser and sign in:\n   {url}\n\n2. Enter this one-time code (expires in 15 minutes):\n   {code}\n\nContinue only if you started this login yourself.\n"
        )
    })
    .await
}

pub async fn device_code_with(
    oauth: &OAuthClient,
    auth_dir: &Path,
    max_wait: Duration,
    show: impl FnOnce(&str, &str),
) -> Result<String, AuthError> {
    let api = format!("{}/api/accounts", oauth.issuer);
    let err = |m: String| AuthError::Refresh(m);
    let resp = oauth
        .http
        .post(format!("{api}/deviceauth/usercode"))
        .headers(crate::oauth::client_headers())
        .header("Content-Type", "application/json")
        .body(json!({ "client_id": oauth.client_id }).to_string())
        .send()
        .await
        .map_err(|e| err(format!("device code request: {}", e.without_url())))?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(err(
            "device code login is not enabled for this server".into()
        ));
    }
    if !resp.status().is_success() {
        return Err(err(format!(
            "device code request failed: {}",
            resp.status()
        )));
    }
    let uc: UserCodeResp = resp
        .json()
        .await
        .map_err(|_| err("device code: invalid response".into()))?;
    show(&format!("{}/codex/device", oauth.issuer), &uc.user_code);

    let start = Instant::now();
    let code = loop {
        let r = oauth
            .http
            .post(format!("{api}/deviceauth/token"))
            .headers(crate::oauth::client_headers())
            .header("Content-Type", "application/json")
            .body(
                json!({ "device_auth_id": uc.device_auth_id, "user_code": uc.user_code })
                    .to_string(),
            )
            .send()
            .await;
        let r = match r {
            Ok(r) => r,
            Err(e) => {
                // transient network error while polling: keep polling until timeout
                let el = start.elapsed();
                if el >= max_wait {
                    return Err(err(format!("device auth poll: {}", e.without_url())));
                }
                tracing::debug!("device auth poll transient error: {}", e.without_url());
                tokio::time::sleep(Duration::from_secs(uc.interval.max(1)).min(max_wait - el))
                    .await;
                continue;
            }
        };
        let st = r.status();
        if st.is_success() {
            break r
                .json::<CodeSuccessResp>()
                .await
                .map_err(|_| err("device auth: invalid response".into()))?;
        }
        if st == reqwest::StatusCode::FORBIDDEN || st == reqwest::StatusCode::NOT_FOUND {
            let el = start.elapsed();
            if el >= max_wait {
                return Err(err("device auth timed out".into()));
            }
            tokio::time::sleep(Duration::from_secs(uc.interval).min(max_wait - el)).await;
            continue;
        }
        return Err(err(format!("device auth failed: {st}")));
    };
    let redirect = format!("{}/deviceauth/callback", oauth.issuer);
    let t = oauth
        .exchange_code(&code.authorization_code, &redirect, &code.code_verifier)
        .await?;
    save_tokens(auth_dir, &t)
}

// ---------- PKCE (paste / localhost) ----------
/// Parse `...?code=..&state=..` (full URL, or just the query) and check state.
pub fn parse_callback(input: &str, expected_state: &str) -> Result<String, AuthError> {
    let input = input.trim();
    let query = input.split_once('?').map_or(input, |(_, q)| q);
    let query = query.split('#').next().unwrap_or("");
    let mut code = None;
    let mut state = None;
    let mut error = None;
    for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
        match &*k {
            "code" => code = Some(v.into_owned()),
            "state" => state = Some(v.into_owned()),
            "error" => error = Some(v.into_owned()),
            _ => {}
        }
    }
    if let Some(e) = error {
        return Err(AuthError::Refresh(format!("authorization failed: {e}")));
    }
    if state.as_deref() != Some(expected_state) {
        return Err(AuthError::Invalid("state mismatch".into()));
    }
    code.filter(|c| !c.is_empty())
        .ok_or_else(|| AuthError::Invalid("callback has no code".into()))
}

pub async fn paste_callback(oauth: &OAuthClient, auth_dir: &Path) -> Result<String, AuthError> {
    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    paste_callback_with(oauth, auth_dir, stdin, |u| {
        println!("\nOpen this URL in a browser on any machine and sign in:\n\n{u}\n\nThe browser will end on a page that fails to load (http://localhost:1455/auth/callback?...).\nCopy that full URL from the address bar and paste it here:");
    })
    .await
}

pub async fn paste_callback_with<R: AsyncBufRead + Unpin>(
    oauth: &OAuthClient,
    auth_dir: &Path,
    mut input: R,
    show: impl FnOnce(&str),
) -> Result<String, AuthError> {
    let pkce = Pkce::generate();
    let state = random_state();
    show(&oauth.authorize_url(LOCAL_REDIRECT_URI, &pkce, &state));
    let mut line = String::new();
    tokio::time::timeout(CALLBACK_LIFETIME * 2, input.read_line(&mut line))
        .await
        .map_err(|_| AuthError::Invalid("timed out waiting for callback URL".into()))??;
    let code = parse_callback(&line, &state)?;
    let t = oauth
        .exchange_code(&code, LOCAL_REDIRECT_URI, &pkce.verifier)
        .await?;
    save_tokens(auth_dir, &t)
}

pub async fn pkce_localhost(oauth: &OAuthClient, auth_dir: &Path) -> Result<String, AuthError> {
    pkce_localhost_with(oauth, auth_dir, CALLBACK_PORT, CALLBACK_LIFETIME, |u| {
        println!("\nOpen this URL in your browser to sign in (listening on 127.0.0.1:{CALLBACK_PORT} for 5 minutes):\n\n{u}\n");
    })
    .await
}

pub async fn pkce_localhost_with(
    oauth: &OAuthClient,
    auth_dir: &Path,
    port: u16,
    lifetime: Duration,
    show: impl FnOnce(&str),
) -> Result<String, AuthError> {
    // Loopback only, never exposed.
    let ln = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    let pkce = Pkce::generate();
    let state = random_state();
    // Redirect stays `localhost:1455` (registered for the Codex client id).
    show(&oauth.authorize_url(LOCAL_REDIRECT_URI, &pkce, &state));
    let deadline = tokio::time::Instant::now() + lifetime;
    loop {
        let (mut sock, _) = tokio::time::timeout_at(deadline, ln.accept())
            .await
            .map_err(|_| AuthError::Invalid("login timed out".into()))??;
        let mut buf = vec![0u8; 8192];
        let n = match tokio::time::timeout(Duration::from_secs(10), sock.read(&mut buf)).await {
            Ok(Ok(n)) => n,
            _ => continue,
        };
        let req = String::from_utf8_lossy(&buf[..n]);
        let target = req
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .unwrap_or("");
        if !target.starts_with("/auth/callback") {
            let _ = respond(&mut sock, "404 Not Found", "Not found").await;
            continue;
        }
        match parse_callback(target, &state) {
            Ok(code) => {
                let r = oauth
                    .exchange_code(&code, LOCAL_REDIRECT_URI, &pkce.verifier)
                    .await;
                let (st, msg) = if r.is_ok() {
                    (
                        "200 OK",
                        "Signed in to ciallopa. You can close this window.",
                    )
                } else {
                    (
                        "500 Internal Server Error",
                        "Token exchange failed. Check the terminal.",
                    )
                };
                let _ = respond(&mut sock, st, msg).await;
                return save_tokens(auth_dir, &r?);
            }
            Err(e) => {
                let _ = respond(
                    &mut sock,
                    "400 Bad Request",
                    "Login failed. Check the terminal.",
                )
                .await;
                // A wrong-state hit could be noise; keep waiting. Explicit errors abort.
                if !matches!(e, AuthError::Invalid(_)) {
                    return Err(e);
                }
            }
        }
    }
}

async fn respond(sock: &mut tokio::net::TcpStream, status: &str, msg: &str) -> std::io::Result<()> {
    let body = format!("<!doctype html><meta charset=utf-8><title>ciallopa</title><p>{msg}</p>");
    let r = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    sock.write_all(r.as_bytes()).await?;
    sock.shutdown().await
}

// ---------- import ----------
/// Import a Codex CLI `auth.json` (`{"tokens":{id_token,access_token,refresh_token,account_id},"last_refresh"}`).
/// Also accepts a CPA codex auth file (flat fields).
pub async fn import_codex_auth_json(src: &Path, auth_dir: &Path) -> Result<String, AuthError> {
    let v: Value = serde_json::from_slice(&tokio::fs::read(src).await?)
        .map_err(|e| AuthError::Invalid(format!("{}: {e}", src.display())))?;
    let tok = v.get("tokens").unwrap_or(&v);
    let s = |k: &str| tok.get(k).and_then(Value::as_str).map(str::to_owned);
    let t = TokenResponse {
        access_token: s("access_token").filter(|x| !x.is_empty()).ok_or_else(|| {
            AuthError::Invalid("no ChatGPT tokens (API-key auth.json is not supported)".into())
        })?,
        refresh_token: s("refresh_token"),
        id_token: s("id_token"),
        expires_in: None,
    };
    let mut fresh = merge_tokens(None, &t);
    if let (Some(o), Some(acc)) = (fresh.as_object_mut(), s("account_id")) {
        o.insert("account_id".into(), acc.into());
    }
    let id = account_id_for(&fresh);
    let _g = FileLock::acquire(auth_dir, &id)?;
    let (id, prev, legacy) = existing_for(auth_dir, &fresh);
    if let Some(o) = fresh.as_object_mut() {
        if let Some(lr) = v.get("last_refresh").cloned() {
            o.insert("last_refresh".into(), lr);
        }
        if let Some(Value::Object(p)) = prev {
            for (k, val) in p {
                o.entry(k).or_insert(val);
            }
        }
    }
    write_auth_file(auth_dir, &id, &fresh)?;
    remove_legacy(auth_dir, legacy);
    Ok(id)
}
