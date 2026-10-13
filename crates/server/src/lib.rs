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
use cpa_lite_forward::AppState;
use std::sync::Arc;
use subtle::ConstantTimeEq;

pub mod prices_cli;
pub mod ui;
pub mod usage_api;

#[derive(Clone)]
struct Keys(Arc<Vec<cpa_lite_core::config::ApiKey>>);

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
pub fn key_index(keys: &[cpa_lite_core::config::ApiKey], got: &str) -> Option<usize> {
    let mut found = None;
    for (i, k) in keys.iter().enumerate() {
        if bool::from(k.key.as_bytes().ct_eq(got.as_bytes())) {
            found = Some(i);
        }
    }
    found
}

async fn require_key(State(keys): State<Keys>, mut req: Request<Body>, next: Next) -> Response {
    match presented_key(req.headers()).and_then(|k| key_index(&keys.0, k)) {
        Some(i) => {
            // Only the label travels with the request (usage log); never the key.
            let label: Arc<str> = Arc::from(keys.0[i].label.as_str());
            req.extensions_mut()
                .insert(cpa_lite_forward::ClientKey(label));
            next.run(req).await
        }
        None => (StatusCode::UNAUTHORIZED, r#"{"error":"invalid api key"}"#).into_response(),
    }
}

async fn healthz() -> &'static str {
    "ok\n"
}

pub fn build_app(state: AppState) -> Router {
    let keys = Keys(Arc::new(state.config.keys()));
    let limit = (state.config.max_body_mb.max(20) as usize) * 1024 * 1024;
    let usage = usage_api::routes(state.usage.clone(), state.accounts.clone());
    let ui_on = state.config.ui.enabled;
    let protected = cpa_lite_forward::routes(state)
        .merge(usage)
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
