//! `/codex/models` passthrough with a 5 minute cache (successful responses only).
use crate::headers::downstream_headers;
use crate::{map_path, pq, proxy, session_route, AppState};
use axum::body::Body;
use axum::extract::{OriginalUri, State};
use axum::response::Response;
use axum::Extension;
use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TTL: Duration = Duration::from_secs(300);

type Entry = (Instant, HeaderMap, Bytes);

#[derive(Clone, Default)]
pub(crate) struct ModelsCache(Arc<Mutex<HashMap<String, Entry>>>);

fn respond(h: &HeaderMap, b: Bytes) -> Response {
    let mut r = Response::new(Body::from(b));
    *r.headers_mut() = h.clone();
    r
}

pub(crate) async fn handler(
    State(st): State<AppState>,
    Extension(cache): Extension<ModelsCache>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Response {
    let Some(path) = map_path(pq(&uri)) else {
        return proxy::json_error(StatusCode::NOT_FOUND, "not_found", "unknown path");
    };
    // Conditional requests go straight through (client ETag semantics).
    let conditional = headers.contains_key(http::header::IF_NONE_MATCH);
    if !conditional {
        if let Ok(m) = cache.0.lock() {
            if let Some((t, h, b)) = m.get(&path) {
                if t.elapsed() < TTL {
                    return respond(h, b.clone());
                }
            }
        }
    }
    let route = session_route(&headers, None);
    let ob = proxy::Outbound {
        method: Method::GET,
        path: path.clone(),
        headers,
        body: Bytes::new(),
        route,
    };
    let c = match proxy::send(&st, ob).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    if c.resp.status() != StatusCode::OK {
        return proxy::stream_back(&st, c);
    }
    let mut h = downstream_headers(c.resp.headers());
    h.remove(http::header::CONTENT_LENGTH);
    let b = match c.resp.bytes().await {
        Ok(b) => b,
        Err(_) => {
            return proxy::json_error(
                StatusCode::BAD_GATEWAY,
                "upstream_error",
                "models read failed",
            )
        }
    };
    if let Ok(mut m) = cache.0.lock() {
        m.insert(path, (Instant::now(), h.clone(), b.clone()));
    }
    respond(&h, b)
}
