//! Small live snapshot for the management pages' shared header.
use axum::{extract::State, routing::get, Json, Router};
use ciallopa_forward::AppState;
use ciallopa_usage::{now_ms, RecentQuery};
use serde_json::{json, Value};

pub fn routes(state: AppState) -> Router {
    Router::new()
        .route("/admin/v1/runtime", get(snapshot))
        .with_state(state)
}

async fn snapshot(State(st): State<AppState>) -> Json<Value> {
    let cfg = st.config.load();
    let proxy = cfg.effective_proxy();
    let drain = &st.upstream.drain;
    let rejected = drain.rejected_by();
    let proxy_errors_1h = if let Some(rec) = &st.usage {
        let since_ms = Some(now_ms() - 3_600_000);
        let count = |error: &str| {
            rec.recent_count(RecentQuery {
                since_ms,
                error: Some(error.into()),
                ..Default::default()
            })
        };
        let (connect, timeout) = tokio::join!(count("proxy_connect"), count("proxy_timeout"));
        connect.unwrap_or(0) + timeout.unwrap_or(0)
    } else {
        0
    };
    Json(json!({
        "egress": {"proxy": ciallopa_core::settings::mask_proxy(&proxy), "direct": proxy.is_empty()},
        "runtime": {
            "inflight": drain.inflight(),
            "limit": cfg.max_inflight_requests,
            "rejected": drain.rejected(),
            "rejected_by": {"inflight": rejected[0], "ws": rejected[1], "body": rejected[2], "ws_msg": rejected[3]},
            "buffered_body_bytes": drain.body_bytes(),
            "ws_connections": drain.active(),
            "draining": drain.phase() != ciallopa_upstream::drain::Phase::Running,
        },
        "proxy_errors_1h": proxy_errors_1h,
    }))
}
