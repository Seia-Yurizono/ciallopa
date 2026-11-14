//! HTTP forwarding core: account selection, failover, pre-first-byte retries, idle timeout.
pub(crate) use crate::attempt::{
    others_usable, overload_outcome, send, send_tracked, Outbound, WS_HOLD_MAX,
};
pub(crate) use crate::classify::{event_kind, overload_code, quota_event, EventKind};
pub(crate) use crate::http::exchange::send_streaming;
pub(crate) use crate::http::pump::{
    async_stream, shutdown_failed_event, stream_back, stream_image_body, SHUTDOWN_RETRY_AFTER_SECS,
};
pub(crate) use crate::sse::{memfind, BOOTSTRAP_MAX};
use axum::response::{IntoResponse, Response};
use http::{HeaderMap, HeaderValue, StatusCode};

pub(crate) fn json_error(status: StatusCode, kind: &str, msg: &str) -> Response {
    let body = serde_json::json!({"error": {"type": kind, "message": msg}}).to_string();
    let mut r = (status, body).into_response();
    r.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    r
}

/// Decode the request body for *inspection only* (zstd bodies are still forwarded as-is).
/// zstd bodies are inspected only up to `limit` decoded bytes (`max_body_mb`); a larger one
/// is forwarded uninspected rather than decoded in full.
pub(crate) fn inspect_json(
    headers: &HeaderMap,
    body: &[u8],
    limit: u64,
) -> Option<serde_json::Value> {
    let enc = headers
        .get(http::header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    match enc.as_str() {
        "" | "identity" => serde_json::from_slice(body).ok(),
        "zstd" => {
            let mut out = Vec::new();
            let mut dec = zstd::stream::read::Decoder::new(body).ok()?;
            use std::io::Read;
            (&mut dec)
                .take(limit.saturating_add(1))
                .read_to_end(&mut out)
                .ok()?;
            if out.len() as u64 > limit {
                return None;
            }
            serde_json::from_slice(&out).ok()
        }
        _ => None,
    }
}
