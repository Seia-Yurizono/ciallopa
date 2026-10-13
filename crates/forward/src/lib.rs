//! Client-facing proxy routes. **Owner: forwarding worker.**
//! M1: SSE /responses, compact, models (5 min cache), catch-all passthrough,
//! WS /responses, /v1/images/{generations,edits}. M0 answers 501.
use axum::{http::StatusCode, routing::any, Router};
use cpa_lite_core::{AccountStore, Config, Router as AccountRouter};
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
    Router::new()
        .route("/backend-api/{*rest}", any(not_implemented))
        .route("/v1/images/generations", any(not_implemented))
        .route("/v1/images/edits", any(not_implemented))
        .with_state(state)
}

async fn not_implemented() -> (StatusCode, &'static str) {
    (
        StatusCode::NOT_IMPLEMENTED,
        "cpa-lite M0: forwarding not implemented\n",
    )
}
