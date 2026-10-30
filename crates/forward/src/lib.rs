//! Client-facing proxy routes. **Owner: forwarding worker.**
//!
//! Codex CLI with `chatgpt_base_url = "https://<proxy>/backend-api"` hits
//! `/backend-api/codex/{responses,responses/compact,models}` and `/backend-api/wham/*`;
//! with a non-`/backend-api` base its backend-client uses `/api/codex/*` (mapped to `/wham/*`).
//! A custom Responses provider with `base_url = "https://<proxy>/v1"` hits `/v1/responses`,
//! `/v1/responses/compact`, `/v1/models` (mapped to `/codex/*`). Everything else under
//! `/backend-api/*` is a sticky passthrough.
mod admission;
mod budget;
pub mod codes;
mod egress;
mod encrypted;
mod errors;
mod headers;
mod image_events;
mod images;
mod limits;
mod models;
pub mod occupancy;
mod proxy;
mod refusal;
mod reqbody;
mod turn_state;
mod usage;
mod ws;
mod ws_upgrade;

pub use ciallopa_routing::parse_quota;
pub use egress::{EgressStatus, Seen as EgressBlockSeen};
pub use headers::{downstream_headers, upstream_headers};
pub use turn_state::forget_account as forget_turn_state;

use axum::{
    body::Bytes,
    extract::{OriginalUri, State},
    http::{HeaderMap, Method},
    response::Response,
    routing::{any, get, post},
    Router,
};
use ciallopa_core::settings::SharedConfig;
use ciallopa_core::{AccountStore, RouteRequest, Router as AccountRouter};
use ciallopa_upstream::UpstreamClient;
use std::sync::Arc;

/// Label (and admin flag) of the client key that authenticated the request (inserted by the
/// server's key middleware as a request extension). Never the key itself.
#[derive(Clone, Debug)]
pub struct ClientKey {
    pub label: Arc<str>,
    pub admin: bool,
}

/// Everything handlers need. Built once in `ciallopa` (server) main.
#[derive(Clone)]
pub struct AppState {
    /// Live configuration. Retain request-scoped options across awaits; routing/error
    /// policies can read a newer snapshot at their documented decision boundaries.
    pub config: Arc<SharedConfig>,
    pub accounts: Arc<dyn AccountStore>,
    pub router: Arc<dyn AccountRouter>,
    pub upstream: Arc<UpstreamClient>,
    /// Per-request usage log; `None` = disabled.
    pub usage: Option<Arc<ciallopa_usage::Recorder>>,
    /// Last Cloudflare challenge / block page seen instead of the upstream (in memory).
    pub egress: Arc<EgressStatus>,
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
        .layer(axum::middleware::map_response(errors::clamp_retry_after))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            admission::admit,
        ))
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
        session: ciallopa_routing::extract_session(headers, body),
        exclude: vec![],
        attempt_at: None,
    }
}

pub(crate) type Key = Option<axum::Extension<ClientKey>>;

pub(crate) fn key_label(k: &Key) -> Option<&str> {
    k.as_ref().map(|e| &*e.0.label)
}

async fn responses(
    State(st): State<AppState>,
    key: Key,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: reqbody::ReqBody,
) -> Response {
    forward(st, key, Method::POST, &uri, headers, body, true).await
}

async fn passthrough(
    State(st): State<AppState>,
    key: Key,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: reqbody::ReqBody,
) -> Response {
    forward(st, key, method, &uri, headers, body, false).await
}

async fn forward(
    st: AppState,
    key: Key,
    method: Method,
    uri: &http::Uri,
    headers: HeaderMap,
    req: reqbody::ReqBody,
    inspect: bool,
) -> Response {
    let Some(path) = map_path(pq(uri)) else {
        return proxy::json_error(http::StatusCode::NOT_FOUND, "not_found", "unknown path");
    };
    let parsed = if inspect {
        let limit = st.config.load().max_body_mb.saturating_mul(1024 * 1024);
        proxy::inspect_json(&headers, &req.bytes, limit)
    } else {
        None
    };
    let route = session_route(&headers, parsed.as_ref());
    let trk = if inspect {
        usage::endpoint_of(&path).and_then(|ep| {
            let mut t =
                usage::Tracker::start(&st, ciallopa_usage::Transport::Sse, ep, key_label(&key))?;
            t.headers(&headers);
            t.meta(parsed.as_ref());
            Some(t)
        })
    } else {
        None
    };
    let expect_sse = inspect && client_wants_sse(&path, &headers, parsed.as_ref());
    // The parsed request is as large as the body; free it before making another copy.
    drop(parsed);
    let (headers, body) = if inspect {
        prepare_responses_body(
            &path,
            &method,
            headers,
            req.bytes.clone(),
            st.config.load().upstream_opts.compress_requests,
            |z| req.wrap(z),
        )
    } else {
        (headers, req.bytes.clone())
    };
    drop(req);
    let ob = proxy::Outbound {
        method,
        path,
        headers,
        body,
        route,
        expect_sse,
    };
    proxy::send_streaming(&st, ob, trk).await
}

/// `/responses`-family request that asked for a stream (`"stream": true` or
/// `Accept: text/event-stream`).
pub(crate) fn client_wants_sse(
    path: &str,
    headers: &HeaderMap,
    body: Option<&serde_json::Value>,
) -> bool {
    if !path.starts_with("/codex/responses") {
        return false;
    }
    let accept = headers
        .get_all(http::header::ACCEPT)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|v| v.to_ascii_lowercase().contains("text/event-stream"));
    accept
        || body
            .and_then(|b| b.get("stream"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
}

/// Official Codex compression level (`codex-rs/http-client/src/request.rs`).
const ZSTD_LEVEL: i32 = 3;

/// `/responses` request shaping (after the plaintext was inspected): drop the client's
/// `Accept-Encoding` so the upstream SSE stays identity (usage/TTFT parsing), and zstd-compress
/// plaintext `POST /codex/responses` bodies like official Codex. The returned bytes are reused
/// verbatim for every retry / failover attempt.
pub(crate) fn prepare_responses_body(
    path: &str,
    method: &Method,
    mut headers: HeaderMap,
    body: Bytes,
    compress: bool,
    wrap: impl FnOnce(Vec<u8>) -> Bytes,
) -> (HeaderMap, Bytes) {
    headers.remove(http::header::ACCEPT_ENCODING);
    let route = path.split('?').next().unwrap_or(path);
    if !compress || method != Method::POST || route != "/codex/responses" || body.is_empty() {
        return (headers, body);
    }
    let encs: Vec<String> = headers
        .get_all(http::header::CONTENT_ENCODING)
        .iter()
        .map(|v| v.to_str().unwrap_or("?").trim().to_ascii_lowercase())
        .collect();
    if encs.iter().any(|e| !e.is_empty() && e != "identity") {
        return (headers, body);
    }
    match zstd::stream::encode_all(&body[..], ZSTD_LEVEL) {
        Ok(z) => {
            headers.remove(http::header::CONTENT_ENCODING);
            headers.remove(http::header::CONTENT_LENGTH);
            headers.insert(
                http::header::CONTENT_ENCODING,
                http::HeaderValue::from_static("zstd"),
            );
            (headers, wrap(z))
        }
        Err(e) => {
            tracing::warn!(error = %e, "zstd request compression failed; forwarding plaintext");
            (headers, body)
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes as B;
    fn prep(p: &str, m: &http::Method, h: http::HeaderMap, b: B, c: bool) -> (http::HeaderMap, B) {
        super::prepare_responses_body(p, m, h, b, c, B::from)
    }
    use bytes::Bytes;
    use http::{HeaderMap, HeaderValue, Method};

    fn hm(kv: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in kv {
            h.append(*k, HeaderValue::from_static(v));
        }
        h
    }

    #[test]
    fn compresses_plain_responses() {
        let raw = Bytes::from_static(br#"{"model":"m","input":[]}"#);
        let (h, b) = prep(
            "/codex/responses",
            &Method::POST,
            hm(&[
                ("accept-encoding", "gzip"),
                ("content-encoding", "identity"),
            ]),
            raw.clone(),
            true,
        );
        assert_eq!(h["content-encoding"], "zstd");
        assert!(h.get("accept-encoding").is_none());
        assert_eq!(zstd::decode_all(&b[..]).unwrap(), raw.to_vec());
    }

    #[test]
    fn leaves_other_bodies() {
        let raw = Bytes::from_static(b"{}");
        for (path, h, on) in [
            (
                "/codex/responses",
                hm(&[("content-encoding", "gzip")]),
                true,
            ),
            ("/codex/responses/compact", HeaderMap::new(), true),
            ("/codex/responses", HeaderMap::new(), false),
        ] {
            let (o, b) = prep(path, &Method::POST, h.clone(), raw.clone(), on);
            assert_eq!(b, raw, "{path}");
            assert_eq!(
                o.get("content-encoding"),
                h.get("content-encoding"),
                "{path}"
            );
        }
    }

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
