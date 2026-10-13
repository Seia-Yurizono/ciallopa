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

pub mod usage_api;

#[derive(Clone)]
struct Keys(Arc<Vec<String>>);

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

async fn require_key(State(keys): State<Keys>, req: Request<Body>, next: Next) -> Response {
    match presented_key(req.headers()) {
        Some(k) if key_matches(&keys.0, k) => next.run(req).await,
        _ => (StatusCode::UNAUTHORIZED, r#"{"error":"invalid api key"}"#).into_response(),
    }
}

async fn healthz() -> &'static str {
    "ok\n"
}

pub fn build_app(state: AppState) -> Router {
    let keys = Keys(Arc::new(state.config.api_keys.clone()));
    let limit = (state.config.max_body_mb.max(20) as usize) * 1024 * 1024;
    let usage = usage_api::routes(state.usage.clone());
    let protected = cpa_lite_forward::routes(state)
        .merge(usage)
        .layer(middleware::from_fn_with_state(keys, require_key));
    Router::new()
        .route("/healthz", get(healthz))
        .merge(protected)
        .fallback(|| async { StatusCode::NOT_FOUND })
        .layer(DefaultBodyLimit::max(limit))
}
