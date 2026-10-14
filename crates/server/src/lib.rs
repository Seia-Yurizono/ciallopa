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
pub mod client_fp;
pub mod prices_cli;
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

async fn require_key(State(keys): State<Keys>, mut req: Request<Body>, next: Next) -> Response {
    // Current snapshot on every request: added / removed keys apply immediately.
    let keys = keys.0.keys();
    match presented_key(req.headers()).and_then(|k| key_index(&keys, k)) {
        Some(i) => {
            // Only the label travels with the request (usage log); never the key.
            let label: Arc<str> = Arc::from(keys[i].label.as_str());
            // Learn the Codex client fingerprint (validated UA + originator pair; HTTP and WS).
            ciallopa_core::fingerprint::observe(req.headers(), Some(&label));
            req.extensions_mut()
                .insert(ciallopa_forward::ClientKey(label));
            next.run(req).await
        }
        None => (StatusCode::UNAUTHORIZED, r#"{"error":"invalid api key"}"#).into_response(),
    }
}

async fn healthz() -> &'static str {
    "ok\n"
}

/// Production OAuth client for `proxy_url`; `CIALLOPA_OAUTH_ISSUER` (legacy
/// `CPA_LITE_OAUTH_ISSUER`) overrides the issuer (tests / mocks only).
pub fn oauth_client(
    proxy_url: &str,
) -> Result<ciallopa_auth::OAuthClient, ciallopa_core::AuthError> {
    let c = ciallopa_auth::OAuthClient::new(proxy_url)?;
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
    let limit = (cfg.max_body_mb.max(20) as usize) * 1024 * 1024;
    let usage = usage_api::routes(state.usage.clone(), state.accounts.clone());
    let ui_on = cfg.ui.enabled;
    let rc = s.rc.clone();
    if tokio::runtime::Handle::try_current().is_ok() {
        let s2 = s.clone();
        tokio::spawn(async move { s2.init_known_accounts().await });
    }
    reset_credits::spawn_poller(state.clone(), rc.clone());
    let resets = reset_credits::routes(state.clone(), rc);
    let protected = ciallopa_forward::routes(state)
        .merge(usage)
        .merge(resets)
        .merge(client_fp::routes())
        .merge(settings::routes(s.clone()))
        .merge(accounts_api::routes(s))
        .layer(middleware::from_fn_with_state(keys, require_key));
    let mut app = Router::new().route("/healthz", get(healthz));
    if ui_on {
        // Static page (no secrets); it calls the key-protected /usage/v1/* APIs itself.
        app = app.merge(ui::routes());
    }
    app.merge(protected)
        .fallback(|| async { StatusCode::NOT_FOUND })
        .layer(DefaultBodyLimit::max(limit))
}
