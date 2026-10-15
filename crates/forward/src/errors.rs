//! Error provenance for logs / the panel, and neutral client-facing error bodies.
//!
//! The client only ever sees either the upstream's own error (status, body, key headers) or an
//! upstream-shaped error with a standard code. Where the error came from (`source`), the short
//! code and the attempt chain live in the usage log only.
use axum::response::{IntoResponse, Response};
use http::{HeaderMap, HeaderValue, StatusCode};
use serde::Serialize;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub(crate) const SRC_CLIENT: &str = "client";
pub(crate) const SRC_LOCAL: &str = "local";
pub(crate) const SRC_NETWORK: &str = "network";
pub(crate) const SRC_UPSTREAM: &str = "upstream";
pub(crate) const SRC_STREAM: &str = "stream";

/// Attached to an error `Response` (as an extension) so the tracker can log provenance.
#[derive(Clone, Debug)]
pub(crate) struct ErrMeta {
    pub source: &'static str,
    pub code: String,
    pub upstream_status: Option<u16>,
    pub upstream_request_id: Option<String>,
}

/// Max attempt entries kept per request.
pub(crate) const MAX_ATTEMPTS: usize = 8;
const ATTEMPT_BODY: usize = 512;

#[derive(Clone, Debug, Serialize, PartialEq)]
pub(crate) struct Attempt {
    pub account: String,
    /// Milliseconds since the request started.
    pub t_ms: u64,
    /// `http`, `transport`, `refresh`, `stream`.
    pub kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// `ok`, `retry`, `switch`, `cooldown(15s)+switch`, `stop`, …
    pub action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
}

/// Bounded attempt chain (first `MAX_ATTEMPTS - 1` plus the latest).
#[derive(Debug)]
pub(crate) struct Attempts {
    pub start: Instant,
    pub list: Vec<Attempt>,
    pub dropped: u32,
}
impl Default for Attempts {
    fn default() -> Self {
        Self {
            start: Instant::now(),
            list: Vec::new(),
            dropped: 0,
        }
    }
}
impl Attempts {
    pub fn push(&mut self, mut a: Attempt) {
        a.t_ms = self.start.elapsed().as_millis() as u64;
        if self.list.len() >= MAX_ATTEMPTS {
            self.list.remove(MAX_ATTEMPTS - 1);
            self.dropped += 1;
        }
        self.list.push(a);
    }
    /// Set the action of the latest attempt (decided after the outcome is known).
    pub fn act(&mut self, action: impl Into<String>) {
        if let Some(a) = self.list.last_mut() {
            a.action = action.into();
        }
    }
    /// Anything other than a single clean success.
    pub fn eventful(&self) -> bool {
        self.list.len() > 1 || self.list.iter().any(|a| a.action != "ok")
    }
    pub fn to_json(&self) -> serde_json::Value {
        let mut v = serde_json::to_value(&self.list).unwrap_or_default();
        if self.dropped > 0 {
            if let Some(a) = v.as_array_mut() {
                a.insert(
                    MAX_ATTEMPTS - 1,
                    serde_json::json!({"dropped": self.dropped}),
                );
            }
        }
        v
    }
}

pub(crate) fn attempt(account: &str, kind: &'static str) -> Attempt {
    Attempt {
        account: account.to_string(),
        t_ms: 0,
        kind,
        status: None,
        code: None,
        action: String::new(),
        body: None,
    }
}

pub(crate) fn body_snippet(b: &[u8]) -> Option<String> {
    if b.is_empty() {
        return None;
    }
    let s = String::from_utf8_lossy(&b[..b.len().min(ATTEMPT_BODY)]);
    Some(s.into_owned())
}

/// `error.code` / `error.type` / top-level `code` of an upstream JSON error body.
pub(crate) fn body_code(b: &[u8]) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(b).ok()?;
    let e = v.get("error").unwrap_or(&v);
    let pick = |k: &str| {
        e.get(k)
            .and_then(|x| x.as_str())
            .map(str::trim)
            .filter(|x| !x.is_empty() && x.len() <= 64)
            .map(str::to_string)
    };
    pick("code").or_else(|| pick("type"))
}

/// Upstream request id for correlating with upstream logs.
pub(crate) fn request_id(h: &HeaderMap) -> Option<String> {
    ["x-request-id", "x-oai-request-id", "cf-ray"]
        .iter()
        .find_map(|k| h.get(*k).and_then(|v| v.to_str().ok()))
        .map(|v| v.chars().take(128).collect())
}

/// Code for an upstream HTTP error (body code if structured, else `http_<status>`).
pub(crate) fn upstream_code(status: u16, h: &HeaderMap, b: &[u8]) -> String {
    let html = h
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("html"));
    if html && (h.contains_key("cf-mitigated") || h.contains_key("cf-ray")) && status == 403 {
        return "challenge".into();
    }
    body_code(b).unwrap_or_else(|| format!("http_{status}"))
}

pub(crate) fn meta_upstream(status: u16, h: &HeaderMap, b: &[u8]) -> ErrMeta {
    ErrMeta {
        source: SRC_UPSTREAM,
        code: upstream_code(status, h, b),
        upstream_status: Some(status),
        upstream_request_id: request_id(h),
    }
}

fn with_json(status: StatusCode, v: serde_json::Value) -> Response {
    let mut r = (status, v.to_string()).into_response();
    r.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    r
}

fn retry_after(r: &mut Response, d: Duration) {
    if let Ok(v) = HeaderValue::from_str(&d.as_secs().max(1).to_string()) {
        r.headers_mut().insert(http::header::RETRY_AFTER, v);
    }
}

/// A locally generated error with an upstream-like body. `code` is logged only.
pub(crate) fn local(
    status: StatusCode,
    source: &'static str,
    code: &str,
    retry: Option<Duration>,
) -> Response {
    let body = match status.as_u16() {
        503 => serde_json::json!({"error": {
            "type": "server_error",
            "code": "server_is_overloaded",
            "message": "The server is overloaded or not ready yet. Please retry your request.",
            "param": null}}),
        504 => serde_json::json!({"error": {
            "type": "server_error", "code": null,
            "message": "Gateway timeout. Please retry your request.", "param": null}}),
        _ => serde_json::json!({"error": {
            "type": "server_error", "code": null,
            "message": "Bad gateway. Please retry your request.", "param": null}}),
    };
    let mut r = with_json(status, body);
    if let Some(d) = retry {
        retry_after(&mut r, d);
    }
    r.extensions_mut().insert(ErrMeta {
        source,
        code: code.to_string(),
        upstream_status: None,
        upstream_request_id: None,
    });
    r
}

/// Every account is out of usage quota: the Codex `usage_limit_reached` shape with the
/// earliest reset, so the client shows when service resumes.
pub(crate) fn quota_exhausted(retry: Duration) -> Response {
    let secs = retry.as_secs().max(1);
    let resets_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() + secs)
        .unwrap_or(0);
    let mut r = with_json(
        StatusCode::TOO_MANY_REQUESTS,
        serde_json::json!({"error": {
            "type": "usage_limit_reached",
            "message": "The usage limit has been reached",
            "resets_at": resets_at,
            "resets_in_seconds": secs}}),
    );
    retry_after(&mut r, retry);
    r.extensions_mut().insert(ErrMeta {
        source: SRC_LOCAL,
        code: "all_quota_exhausted".into(),
        upstream_status: None,
        upstream_request_id: None,
    });
    r
}

/// Network-level failure code of a reqwest error.
pub(crate) fn net_code(e: &reqwest::Error) -> &'static str {
    let s = format!("{e:?}").to_ascii_lowercase();
    if e.is_timeout() {
        if e.is_connect() {
            "timeout_connect"
        } else {
            "timeout"
        }
    } else if s.contains("dns") || s.contains("failed to lookup") {
        "dns"
    } else if s.contains("tls") || s.contains("certificate") || s.contains("handshake") {
        "tls"
    } else if e.is_connect() {
        "connect"
    } else {
        "transport"
    }
}

pub(crate) fn network(code: &'static str) -> Response {
    let st = if code.starts_with("timeout") {
        StatusCode::GATEWAY_TIMEOUT
    } else {
        StatusCode::BAD_GATEWAY
    };
    local(st, SRC_NETWORK, code, None)
}

/// SSE event that ends a cut-off stream so Codex retries the turn (it treats an unknown
/// `response.failed` code as a retryable stream error).
pub(crate) fn truncated_event() -> bytes::Bytes {
    let v = serde_json::json!({
        "type": "response.failed",
        "response": {"status": "failed", "error": {
            "code": "server_error",
            "message": "stream disconnected before completion"}}});
    bytes::Bytes::from(format!("event: response.failed\ndata: {v}\n\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn neutral_bodies() {
        for st in [503u16, 502, 504] {
            let r = local(
                StatusCode::from_u16(st).unwrap(),
                SRC_LOCAL,
                "no_account",
                Some(Duration::from_secs(3)),
            );
            let m = r.extensions().get::<ErrMeta>().unwrap();
            assert_eq!(m.code, "no_account");
            assert!(r.headers().keys().all(|k| !k.as_str().contains("ciallopa")));
        }
        let ev = String::from_utf8(truncated_event().to_vec()).unwrap();
        assert!(ev.starts_with("event: response.failed\ndata: {"));
        assert!(!ev.contains("ciallopa"));
    }
    #[test]
    fn attempts_bounded() {
        let mut a = Attempts::default();
        for i in 0..12 {
            let mut x = attempt(&format!("a{i}"), "http");
            x.action = "retry".into();
            a.push(x);
        }
        assert_eq!(a.list.len(), MAX_ATTEMPTS);
        assert_eq!(a.dropped, 4);
        assert_eq!(a.list.last().unwrap().account, "a11");
        let j = a.to_json();
        assert_eq!(j.as_array().unwrap().len(), MAX_ATTEMPTS + 1);
        assert_eq!(
            body_code(br#"{"error":{"code":"server_is_overloaded"}}"#).as_deref(),
            Some("server_is_overloaded")
        );
        assert_eq!(upstream_code(502, &HeaderMap::new(), b"<html>"), "http_502");
    }
}
