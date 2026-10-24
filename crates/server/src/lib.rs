//! HTTP server wiring: /healthz (no auth) + client-key middleware + forward routes.
//! **Owner: lead.**
use axum::{
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use ciallopa_forward::AppState;
use std::sync::Arc;
use subtle::ConstantTimeEq;

pub mod accounts_api;
pub mod affinity_store;
pub mod client_fp;
pub mod host;
pub mod init;
pub mod prices;
pub mod reset_credits;
pub mod settings;
pub mod ui;
pub mod usage_api;

#[derive(Clone)]
struct Keys(Arc<ciallopa_core::settings::SharedConfig>);

fn presented_key(h: &HeaderMap) -> Option<&str> {
    if let Some(v) = h.get("x-api-key").and_then(|v| v.to_str().ok()) {
        return Some(v.trim());
    }
    let v = h.get(http::header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, rest) = v.split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then(|| rest.trim())
}

/// Constant-time over each configured key; does not short-circuit on match.
pub fn key_matches(keys: &[String], got: &str) -> bool {
    keys.iter().fold(0u8, |acc, k| {
        acc | k.as_bytes().ct_eq(got.as_bytes()).unwrap_u8()
    }) == 1
}

/// Index of the matching key; compares against every key (no early exit).
pub fn key_index(keys: &[ciallopa_core::config::ApiKey], got: &str) -> Option<usize> {
    let mut found = None;
    for (i, k) in keys.iter().enumerate() {
        if bool::from(k.key.as_bytes().ct_eq(got.as_bytes())) {
            found = Some(i);
        }
    }
    found
}

/// `/admin/...` (management API). Paths are matched after percent-decoding-free routing, so a
/// plain prefix check is exact for axum's router.
pub fn is_admin_path(p: &str) -> bool {
    p == "/admin" || p.starts_with("/admin/")
}

/// Body limit for the management and usage APIs, read from live config so a hot
/// `max_body_mb` change applies to the next request. Forwarding routes enforce the same
/// limit (and the buffered-body budget) in their own extractor.
async fn live_body_limit(
    State(cfg): State<Arc<ciallopa_core::settings::SharedConfig>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let limit = cfg.load().max_body_mb.saturating_mul(1024 * 1024) as usize;
    let (parts, body) = req.into_parts();
    match axum::body::to_bytes(body, limit).await {
        Ok(b) => next.run(Request::from_parts(parts, Body::from(b))).await,
        Err(_) => (
            StatusCode::PAYLOAD_TOO_LARGE,
            axum::Json(serde_json::json!({"error": {
                "type": "invalid_request_error",
                "message": "Request body is too large.",
            }})),
        )
            .into_response(),
    }
}

async fn require_key(State(keys): State<Keys>, mut req: Request<Body>, next: Next) -> Response {
    // Current snapshot on every request: added / removed keys apply immediately.
    let keys = keys.0.keys();
    match presented_key(req.headers()).and_then(|k| key_index(&keys, k)) {
        Some(i) => {
            // Only the label travels with the request (usage log); never the key.
            let label: Arc<str> = Arc::from(keys[i].label.as_str());
            // Management API (settings, keys, accounts, reset-credit consume): admin keys only.
            // No admin key configured ⇒ nobody can call it (never falls back to "any key").
            if is_admin_path(req.uri().path()) && !keys[i].admin {
                return (
                    StatusCode::FORBIDDEN,
                    axum::Json(serde_json::json!({"error": {
                        "type": "admin_required",
                        "message": "this client key is not an admin key; set admin = true on its [[api_keys]] entry in the config file"
                    }})),
                )
                    .into_response();
            }
            // Learn the Codex client fingerprint (validated UA + originator pair; HTTP and WS).
            ciallopa_core::fingerprint::observe(req.headers(), Some(&label));
            req.extensions_mut().insert(ciallopa_forward::ClientKey {
                label,
                admin: keys[i].admin,
            });
            next.run(req).await
        }
        None => (StatusCode::UNAUTHORIZED, r#"{"error":"invalid api key"}"#).into_response(),
    }
}

/// `ciallopa healthcheck`: GET /healthz on the configured listen address (an unspecified
/// address is probed on loopback); true on a 200 within `timeout`. Plain HTTP/1.1 over TCP, no
/// client dependencies.
pub async fn healthcheck(listen: std::net::SocketAddr, timeout: std::time::Duration) -> bool {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut addr = listen;
    if addr.ip().is_unspecified() {
        addr.set_ip(match addr {
            std::net::SocketAddr::V4(_) => std::net::Ipv4Addr::LOCALHOST.into(),
            std::net::SocketAddr::V6(_) => std::net::Ipv6Addr::LOCALHOST.into(),
        });
    }
    let probe = async {
        let mut s = tokio::net::TcpStream::connect(addr).await.ok()?;
        let req = format!("GET /healthz HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
        s.write_all(req.as_bytes()).await.ok()?;
        let mut buf = [0u8; 64];
        let mut n = 0;
        while n < 12 {
            match s.read(&mut buf[n..]).await.ok()? {
                0 => break,
                m => n += m,
            }
        }
        let line = std::str::from_utf8(&buf[..n]).ok()?;
        Some(line.starts_with("HTTP/1.1 200") || line.starts_with("HTTP/1.0 200"))
    };
    matches!(tokio::time::timeout(timeout, probe).await, Ok(Some(true)))
}

async fn healthz() -> &'static str {
    "ok\n"
}

/// Production OAuth client for `proxy_url`; `CIALLOPA_OAUTH_ISSUER` (legacy
/// `CPA_LITE_OAUTH_ISSUER`) overrides the issuer (tests / mocks only).
/// OAuth client on the effective egress of `cfg`.
pub fn oauth_client(
    cfg: &ciallopa_core::Config,
) -> Result<ciallopa_auth::OAuthClient, ciallopa_core::AuthError> {
    let c = ciallopa_auth::OAuthClient::new(&cfg.effective_proxy(), cfg.ipv6)?;
    Ok(match ciallopa_core::config::env_var("OAUTH_ISSUER") {
        Some((i, _)) if !i.is_empty() => {
            let mut o = ciallopa_auth::OAuthClient::with_issuer(&i, c.http.clone());
            o.pools = c.pools.clone();
            o
        }
        _ => c,
    })
}

/// App without a config file (built-in defaults; reload and writes unavailable).
pub fn build_app(state: AppState) -> Router {
    let s = settings::Settings::new(state, None, None);
    build_app_with(s)
}

/// App with live settings (config file, reload pipeline, admin config / account API).
pub fn build_app_with(s: Arc<settings::Settings>) -> Router {
    let state = s.st.clone();
    let cfg = state.config.load();
    let keys = Keys(state.config.clone());
    let usage = usage_api::routes(
        state.usage.clone(),
        state.accounts.clone(),
        state.egress.clone(),
    );
    let ui_on = cfg.ui.enabled;
    let rc = s.rc.clone();
    if tokio::runtime::Handle::try_current().is_ok() {
        let s2 = s.clone();
        tokio::spawn(async move { s2.init_known_accounts().await });
    }
    reset_credits::spawn_poller(state.clone(), rc.clone());
    let resets = reset_credits::routes(state.clone(), rc);
    let live_cfg = state.config.clone();
    let admin = usage
        .merge(resets)
        .merge(client_fp::routes())
        .merge(settings::routes(s.clone()))
        .merge(accounts_api::routes(s))
        .layer(middleware::from_fn_with_state(live_cfg, live_body_limit))
        .layer(DefaultBodyLimit::disable());
    let protected = ciallopa_forward::routes(state)
        .merge(admin)
        .layer(middleware::from_fn_with_state(keys, require_key));
    let mut app = Router::new().route("/healthz", get(healthz));
    if ui_on {
        // Static page (no secrets); it calls the key-protected /usage/v1/* APIs itself.
        app = app.merge(ui::routes());
    }
    app.merge(protected)
        .fallback(|| async { StatusCode::NOT_FOUND })
}
