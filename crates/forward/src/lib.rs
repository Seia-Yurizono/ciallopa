//! Client-facing proxy routes. **Owner: forwarding worker.**
//!
//! Codex CLI with `chatgpt_base_url = "https://<proxy>/backend-api"` hits
//! `/backend-api/codex/{responses,responses/compact,models}` and `/backend-api/wham/*`;
//! with a non-`/backend-api` base its backend-client uses `/api/codex/*` (mapped to `/wham/*`).
//! A custom Responses provider with `base_url = "https://<proxy>/v1"` hits `/v1/responses`,
//! `/v1/responses/compact`, `/v1/models` (mapped to `/codex/*`). Everything else under
//! `/backend-api/*` is a sticky passthrough.
mod headers;
mod images;
mod models;
mod proxy;
mod ws;

pub use cpa_lite_routing::parse_quota;
pub use headers::{downstream_headers, upstream_headers};

use axum::{
    body::Bytes,
    extract::{OriginalUri, State},
    http::{HeaderMap, Method},
    response::Response,
    routing::{any, get, post},
    Router,
};
use cpa_lite_core::{AccountStore, Config, RouteRequest, Router as AccountRouter};
use cpa_lite_upstream::UpstreamClient;
use std::sync::Arc;

/// Everything handlers need. Built once in `cpa-lite` (server) main.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub accounts: Arc<dyn AccountStore>,
    pub router: Arc<dyn AccountRouter>,
    pub upstream: Arc<UpstreamClient>,
}

/// Routes mounted *behind* the client-key middleware.
pub fn routes(state: AppState) -> Router {
    let cache = models::ModelsCache::default();
    Router::new()
        .route(
            "/backend-api/codex/responses",
            post(responses).get(ws::handler),
        )
        .route("/v1/responses", post(responses).get(ws::handler))
        .route("/backend-api/codex/responses/compact", post(responses))
        .route("/v1/responses/compact", post(responses))
        .route("/backend-api/codex/models", get(models::handler))
        .route("/v1/models", get(models::handler))
        .route("/backend-api/{*rest}", any(passthrough))
        .route("/api/codex/{*rest}", any(passthrough))
        .route("/v1/images/generations", post(images::generations))
        .route("/v1/images/edits", post(images::edits))
        .layer(axum::Extension(cache))
        .with_state(state)
}

/// Client path → upstream path (relative to `upstream`, default `https://chatgpt.com/backend-api`).
pub fn map_path(path_and_query: &str) -> Option<String> {
    if let Some(r) = path_and_query.strip_prefix("/backend-api") {
        return r.starts_with('/').then(|| r.to_string());
    }
    if let Some(r) = path_and_query.strip_prefix("/api/codex/") {
        return Some(format!("/wham/{r}"));
    }
    if let Some(r) = path_and_query.strip_prefix("/v1/") {
        if r.starts_with("responses") || r.starts_with("models") {
            return Some(format!("/codex/{r}"));
        }
    }
    None
}

fn pq(uri: &http::Uri) -> &str {
    uri.path_and_query().map(|p| p.as_str()).unwrap_or("/")
}

pub(crate) fn session_route(headers: &HeaderMap, body: Option<&serde_json::Value>) -> RouteRequest {
    RouteRequest {
        model: body
            .and_then(|b| b.get("model"))
            .and_then(|m| m.as_str())
            .unwrap_or_default()
            .to_string(),
        session: cpa_lite_routing::extract_session(headers, body),
        exclude: vec![],
    }
}

async fn responses(
    State(st): State<AppState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    forward(st, Method::POST, &uri, headers, body, true).await
}

async fn passthrough(
    State(st): State<AppState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    forward(st, method, &uri, headers, body, false).await
}

async fn forward(
    st: AppState,
    method: Method,
    uri: &http::Uri,
    headers: HeaderMap,
    body: Bytes,
    inspect: bool,
) -> Response {
    let Some(path) = map_path(pq(uri)) else {
        return proxy::json_error(http::StatusCode::NOT_FOUND, "not_found", "unknown path");
    };
    let parsed = if inspect {
        proxy::inspect_json(&headers, &body)
    } else {
        None
    };
    let route = session_route(&headers, parsed.as_ref());
    let ob = proxy::Outbound {
        method,
        path,
        headers,
        body,
        route,
    };
    proxy::send_streaming(&st, ob).await
}

#[cfg(test)]
mod tests {
    #[test]
    fn maps() {
        use super::map_path as m;
        assert_eq!(
            m("/backend-api/codex/responses").as_deref(),
            Some("/codex/responses")
        );
        assert_eq!(m("/backend-api/wham/usage").as_deref(), Some("/wham/usage"));
        assert_eq!(
            m("/api/codex/usage?x=1").as_deref(),
            Some("/wham/usage?x=1")
        );
        assert_eq!(
            m("/v1/models?client_version=1").as_deref(),
            Some("/codex/models?client_version=1")
        );
        assert_eq!(
            m("/v1/responses/compact").as_deref(),
            Some("/codex/responses/compact")
        );
        assert_eq!(m("/v1/chat/completions"), None);
        assert_eq!(m("/backend-apix"), None);
    }
}
