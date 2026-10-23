//! `/codex/models` passthrough with a 5 minute cache (successful responses only).
use crate::headers::downstream_headers;
use crate::limits::{read_body, BODY_TIMEOUT, MODELS_BODY_LIMIT};
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
// Query parameters are part of the cache key; clients can supply arbitrarily many versions.
const MAX_ENTRIES: usize = 64;
const MAX_CACHE_BYTES: usize = 16 * 1024 * 1024;

type Entry = (Instant, HeaderMap, Bytes);

#[derive(Clone, Default)]
pub(crate) struct ModelsCache(Arc<Mutex<HashMap<String, Entry>>>);

fn cache_response(
    cache: &mut HashMap<String, Entry>,
    path: String,
    headers: HeaderMap,
    body: Bytes,
) {
    cache.retain(|_, (t, _, _)| t.elapsed() < TTL);
    // Replacing one query variant should not evict an unrelated entry unnecessarily.
    cache.remove(&path);
    let mut bytes: usize = cache.values().map(|(_, _, b)| b.len()).sum();
    while cache.len() >= MAX_ENTRIES || bytes + body.len() > MAX_CACHE_BYTES {
        let Some(oldest) = cache
            .iter()
            .min_by_key(|(_, (t, _, _))| *t)
            .map(|(k, _)| k.clone())
        else {
            return;
        };
        if let Some((_, _, old)) = cache.remove(&oldest) {
            bytes -= old.len();
        }
    }
    cache.insert(path, (Instant::now(), headers, body));
}

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
    let path = with_client_version(path);
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
        expect_sse: false,
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
    let b = match read_body(c.resp, MODELS_BODY_LIMIT, BODY_TIMEOUT).await {
        Ok(b) => b,
        Err(failure) => {
            tracing::warn!(
                code = failure.code(true),
                "upstream models body incomplete; not cached"
            );
            return failure.response(200, &h, true);
        }
    };
    if let Ok(mut m) = cache.0.lock() {
        cache_response(&mut m, path, h.clone(), b.clone());
    }
    respond(&h, b)
}

/// Upstream `/codex/models` requires `client_version`; plain OpenAI clients don't send it.
const DEFAULT_CLIENT_VERSION: &str = "0.163.0";

fn with_client_version(path: String) -> String {
    let q = path.split_once('?').map(|(_, q)| q).unwrap_or("");
    if q.split('&').any(|kv| kv.starts_with("client_version=")) {
        return path;
    }
    let sep = if path.contains('?') { '&' } else { '?' };
    format!("{path}{sep}client_version={DEFAULT_CLIENT_VERSION}")
}

#[cfg(test)]
mod tests {
    use super::with_client_version as w;
    use super::*;

    #[test]
    fn cache_evicts_oldest_to_keep_total_bytes_bounded() {
        let now = Instant::now();
        let large = Bytes::from(vec![b'x'; MODELS_BODY_LIMIT]);
        let mut cache = HashMap::from([
            (
                "old".into(),
                (
                    now - Duration::from_secs(2),
                    HeaderMap::new(),
                    large.clone(),
                ),
            ),
            (
                "recent".into(),
                (
                    now - Duration::from_secs(1),
                    HeaderMap::new(),
                    large.clone(),
                ),
            ),
        ]);
        cache_response(&mut cache, "new".into(), HeaderMap::new(), large);
        assert!(!cache.contains_key("old"));
        assert!(cache.contains_key("recent") && cache.contains_key("new"));
        assert_eq!(
            cache.values().map(|(_, _, body)| body.len()).sum::<usize>(),
            MAX_CACHE_BYTES
        );
        cache_response(
            &mut cache,
            "recent".into(),
            HeaderMap::new(),
            Bytes::from_static(b"small"),
        );
        assert_eq!(
            cache.len(),
            2,
            "replacing a variant does not evict another one"
        );
    }

    #[test]
    fn client_version_default() {
        assert_eq!(
            w("/codex/models".into()),
            "/codex/models?client_version=0.163.0"
        );
        assert_eq!(
            w("/codex/models?a=1".into()),
            "/codex/models?a=1&client_version=0.163.0"
        );
        assert_eq!(
            w("/codex/models?client_version=9".into()),
            "/codex/models?client_version=9"
        );
    }
}
