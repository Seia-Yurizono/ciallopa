//! Error provenance and attempt chains stay in the log; client errors keep the upstream shape.
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
    /// Detailed reason for the log / panel (never sent to the client).
    pub message: Option<String>,
}

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
    pub action: crate::attempt::Action,
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
    /// Anything other than a single clean success.
    pub fn eventful(&self) -> bool {
        self.list.len() > 1
            || self
                .list
                .iter()
                .any(|a| a.action != crate::attempt::Next::Ok.into())
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
        account: ciallopa_core::a(account),
        t_ms: 0,
        kind,
        status: None,
        code: None,
        action: Default::default(),
        body: None,
    }
}

pub(crate) fn body_snippet(b: &[u8]) -> Option<String> {
    if b.is_empty() {
        return None;
    }
    let s = String::from_utf8_lossy(&b[..b.len().min(ATTEMPT_BODY)]);
    let stripped = ciallopa_usage::detail::keep_content_safe_str(&s);
    let bytes = stripped.as_bytes();
    Some(String::from_utf8_lossy(&bytes[..bytes.len().min(ATTEMPT_BODY)]).into_owned())
}

/// `error.code` / `error.type` / top-level `code` of an upstream JSON error body.
pub(crate) fn body_code(b: &[u8]) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(b).ok()?;
    // `{"error":{…}}` (OpenAI), `{"detail":{"code":…}}` (ChatGPT backend, e.g. 402), else top level.
    let e = v
        .get("error")
        .or_else(|| v.get("detail").filter(|d| d.is_object()))
        .unwrap_or(&v);
    let pick = |k: &str| {
        e.get(k)
            .and_then(|x| x.as_str())
            .map(str::trim)
            .filter(|x| !x.is_empty() && x.len() <= 64)
            .map(str::to_string)
    };
    pick("code").or_else(|| pick("type"))
}

/// Prefer nested error codes over types; normalize quota and code-less 429 failures.
pub(crate) fn event_code(v: &serde_json::Value) -> String {
    if crate::classify::quota_event(v) == Some("usage_limit") {
        return "usage_limit".into();
    }
    if let Some(c) = crate::classify::overload_code(v) {
        return c;
    }
    let pick = |o: Option<&serde_json::Value>, k: &str| {
        o.and_then(|o| o.get(k))
            .and_then(|x| x.as_str())
            .map(str::trim)
            .filter(|x| !x.is_empty() && x.len() <= 64)
            .map(str::to_string)
    };
    let objs = [v.pointer("/response/error"), v.get("error")];
    let found = objs
        .iter()
        .find_map(|o| pick(*o, "code"))
        .or_else(|| objs.iter().find_map(|o| pick(*o, "type")))
        .or_else(|| pick(Some(v), "code"));
    if let Some(c) = found {
        return c;
    }
    let status = v
        .get("status")
        .or_else(|| v.get("status_code"))
        .and_then(|s| s.as_u64());
    if status == Some(429) {
        return RATE_LIMIT_CODE.into();
    }
    match v.get("type").and_then(|t| t.as_str()) {
        Some("response.failed") => "response_failed".into(),
        _ => "upstream_error_event".into(),
    }
}

/// Logged for a 429 / rate limit that carries no quota code (cools the model only).
pub(crate) const RATE_LIMIT_CODE: &str = "rate_limit_exceeded";

pub(crate) fn sse_block_code(event: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(event).ok()?;
    let data: String = text
        .lines()
        .filter_map(|l| l.strip_prefix("data:"))
        .map(str::trim_start)
        .collect::<Vec<_>>()
        .join("\n");
    let v: serde_json::Value = serde_json::from_str(&data).ok()?;
    Some(event_code(&v))
}

pub(crate) fn request_id(h: &HeaderMap) -> Option<String> {
    ["x-request-id", "x-oai-request-id", "cf-ray"]
        .iter()
        .find_map(|k| h.get(*k).and_then(|v| v.to_str().ok()))
        .map(|v| v.chars().take(128).collect())
}

/// Codex prints `x-request-id` (else `x-oai-request-id`) as "request id: …" on a non-2xx
/// response; the gateway's own ID goes there so that text finds the record on /ui/requests.
const REQUEST_ID: http::HeaderName = http::HeaderName::from_static("x-request-id");
const UPSTREAM_REQUEST_ID: http::HeaderName =
    http::HeaderName::from_static("x-upstream-request-id");

pub(crate) fn client_request_id(record_id: &str) -> String {
    format!("cp-{record_id}")
}

/// Stamp a non-2xx response with the gateway record ID. An upstream `x-request-id` moves to
/// `x-upstream-request-id`; `x-oai-request-id` and the body stay untouched.
pub(crate) fn stamp_request_id(r: &mut Response, record_id: &str) {
    if r.status().is_success() {
        return;
    }
    let Ok(id) = HeaderValue::from_str(&client_request_id(record_id)) else {
        return;
    };
    let h = r.headers_mut();
    if let Some(up) = h.remove(&REQUEST_ID) {
        h.insert(UPSTREAM_REQUEST_ID, up);
    }
    h.insert(REQUEST_ID, id);
}

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
        message: None,
    }
}

pub(crate) fn with_json(status: StatusCode, v: serde_json::Value) -> Response {
    (status, axum::Json(v)).into_response()
}

fn retry_after(r: &mut Response, d: Duration) {
    set_retry_after(r.headers_mut(), d.as_secs().max(1));
}

/// OpenAI SDKs prefer retry-after-ms; keep it consistent with Retry-After.
pub(crate) const RETRY_AFTER_MS: http::HeaderName = http::HeaderName::from_static("retry-after-ms");

/// `Retry-After: secs` plus the matching `retry-after-ms`.
pub(crate) fn set_retry_after(h: &mut http::HeaderMap, secs: u64) {
    h.insert(http::header::RETRY_AFTER, HeaderValue::from(secs));
    h.insert(RETRY_AFTER_MS, HeaderValue::from(secs.saturating_mul(1000)));
}

/// Log (at most once a minute) that a request found no account to route to. The client gets
/// the neutral upstream-style 503; the operator learns the reason from the log.
pub(crate) fn warn_unroutable(code: &str, accounts: usize) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static LAST: AtomicU64 = AtomicU64::new(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let last = LAST.load(Ordering::Relaxed);
    if now.saturating_sub(last) < 60
        || LAST
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
    {
        return;
    }
    if accounts == 0 {
        tracing::warn!(code, "request rejected: no accounts configured; add one with `ciallopa login` or on /ui/accounts");
    } else {
        tracing::warn!(
            code,
            accounts,
            "request rejected: no usable account (all disabled, excluded or needing a new login)"
        );
    }
}

/// Keep local diagnostic codes out of client-facing error bodies.
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
        message: None,
    });
    r
}

/// Quota refusals include reset time; omit Retry-After because Codex stops on quota errors.
pub(crate) fn quota_exhausted(retry: Duration, all: bool) -> Response {
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
    let (code, message) = if all {
        (
            "all_quota_exhausted",
            format!("every account is out of usage quota; the first resets in {secs}s"),
        )
    } else {
        (
            "first_quota_reset",
            format!(
                "every account is cooling down; the first to recover is out of usage quota and resets in {secs}s"
            ),
        )
    };
    r.extensions_mut().insert(ErrMeta {
        source: SRC_LOCAL,
        code: code.into(),
        upstream_status: None,
        upstream_request_id: None,
        message: Some(message),
    });
    r
}

/// Advise retry only when the first recovery fits the client wait bound.
pub(crate) fn all_cooling(
    recovers_in: Duration,
    wait_secs: u64,
    retry: Option<Duration>,
) -> Response {
    let mut r = local(
        StatusCode::SERVICE_UNAVAILABLE,
        SRC_LOCAL,
        "all_cooling",
        retry,
    );
    let secs = recovers_in.as_secs_f64().ceil() as u64;
    let message = if retry.is_some() {
        format!("every account is cooling down after upstream errors; the first recovers in {secs}s (client asked to retry then)")
    } else {
        format!("every account is cooling down after upstream errors; the first recovers in {secs}s, beyond the {wait_secs}s wait")
    };
    if let Some(m) = r.extensions_mut().get_mut::<ErrMeta>() {
        m.message = Some(message);
    }
    r
}

/// Bound advertised delay independently of retry budgets; omit zero.
pub(crate) const MAX_CLIENT_RETRY_AFTER: u64 = 20;

/// On 429/5xx keep bounded positive retry advice; synchronize seconds and milliseconds.
pub(crate) async fn clamp_retry_after(mut r: Response) -> Response {
    let s = r.status().as_u16();
    if s == 429 || s >= 500 {
        let h = r.headers_mut();
        let secs = h
            .get(http::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|n| (1..=MAX_CLIENT_RETRY_AFTER).contains(n));
        let ms = h
            .get(&RETRY_AFTER_MS)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|m| m.is_finite() && *m > 0.0 && *m <= (MAX_CLIENT_RETRY_AFTER * 1000) as f64)
            .map(|m| m.ceil() as u64);
        h.remove(http::header::RETRY_AFTER);
        h.remove(&RETRY_AFTER_MS);
        match (secs, ms) {
            (Some(secs), _) => set_retry_after(h, secs),
            (None, Some(ms)) => {
                h.insert(
                    http::header::RETRY_AFTER,
                    HeaderValue::from(ms.div_ceil(1000)),
                );
                h.insert(RETRY_AFTER_MS, HeaderValue::from(ms));
            }
            (None, None) => {}
        }
    }
    r
}

/// Connect failures belong to configured egress; TLS after the tunnel stays tls.
pub(crate) fn net_code(e: &reqwest::Error, via_proxy: bool) -> &'static str {
    let s = format!("{e:?}").to_ascii_lowercase();
    if via_proxy && e.is_connect() {
        if s.contains("certificate") || s.contains("invalidcontenttype") || s.contains("tls") {
            "tls"
        } else if e.is_timeout() {
            "proxy_timeout"
        } else {
            "proxy_connect"
        }
    } else if e.is_timeout() {
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

/// Failure of the (shared) egress proxy: switching accounts cannot help.
pub(crate) fn is_proxy_code(code: &str) -> bool {
    code.starts_with("proxy_")
}

/// `e: source: source…` — reqwest's top-level message alone ("error sending request") hides
/// the cause.
pub(crate) fn chain(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut cur = e.source();
    while let Some(c) = cur {
        let m = c.to_string();
        if !out.ends_with(&m) {
            out.push_str(": ");
            out.push_str(&m);
        }
        cur = c.source();
    }
    out
}

pub(crate) fn network(code: &'static str) -> Response {
    let st = if code.ends_with("timeout") || code.starts_with("timeout") {
        StatusCode::GATEWAY_TIMEOUT
    } else {
        StatusCode::BAD_GATEWAY
    };
    local(st, SRC_NETWORK, code, None)
}

/// Preparation expiry is a local deadline, not a network fault.
pub(crate) fn preparation_timeout() -> Response {
    local(
        StatusCode::GATEWAY_TIMEOUT,
        SRC_LOCAL,
        "preparation_timeout",
        None,
    )
}

/// Same buckets as [`net_code`] for a failed WebSocket dial (no egress proxy involved).
pub(crate) fn ws_connect_code(e: &ciallopa_upstream::UpstreamError) -> &'static str {
    use ciallopa_upstream::UpstreamError as U;
    match e {
        U::ConnectTimeout => return "timeout_connect",
        U::Ws(b) if matches!(b.as_ref(), tungstenite::Error::Tls(_)) => return "tls",
        _ => {}
    }
    let s = chain(e).to_ascii_lowercase();
    if s.contains("failed to lookup") || s.contains("dns") || s.contains("resolve") {
        "dns"
    } else if s.contains("tls") || s.contains("certificate") {
        "tls"
    } else {
        "connect"
    }
}

/// Unknown response.failed codes make Codex retry a truncated stream.
pub(crate) fn truncated_event() -> bytes::Bytes {
    let v = serde_json::json!({
        "type": "response.failed",
        "response": {"status": "failed", "error": {
            "code": "server_error",
            "message": "stream disconnected before completion"}}});
    bytes::Bytes::from(format!("event: response.failed\ndata: {v}\n\n"))
}
pub(crate) fn json_error(status: StatusCode, kind: &str, msg: &str) -> Response {
    with_json(
        status,
        serde_json::json!({"error": {"type": kind, "message": msg}}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ws_connect_failures_are_broken_down_like_http() {
        use ciallopa_upstream::UpstreamError as U;
        use std::io::{Error, ErrorKind};
        assert_eq!(ws_connect_code(&U::ConnectTimeout), "timeout_connect");
        assert_eq!(
            ws_connect_code(&U::Io(Error::other(
                "failed to lookup address information: Name or service not known"
            ))),
            "dns"
        );
        assert_eq!(
            ws_connect_code(&U::Ws(Box::new(tungstenite::Error::Tls(
                tungstenite::error::TlsError::InvalidDnsName
            )))),
            "tls"
        );
        assert_eq!(
            ws_connect_code(&U::Io(Error::new(
                ErrorKind::ConnectionRefused,
                "connection refused"
            ))),
            "connect"
        );
        assert_eq!(
            ws_connect_code(&U::Ws(Box::new(tungstenite::Error::ConnectionClosed))),
            "connect"
        );
    }

    #[test]
    fn preparation_timeout_is_local() {
        let r = preparation_timeout();
        assert_eq!(r.status(), StatusCode::GATEWAY_TIMEOUT);
        let m = r.extensions().get::<ErrMeta>().expect("meta");
        assert_eq!(m.source, SRC_LOCAL);
        assert_eq!(m.code, "preparation_timeout");
    }

    #[test]
    fn event_code_buckets_like_body_code() {
        let c = |t: &str| event_code(&serde_json::from_str(t).unwrap());
        assert_eq!(
            c(
                r#"{"type":"error","status":429,"error":{"type":"rate_limit_error","code":"rate_limit_exceeded"}}"#
            ),
            "rate_limit_exceeded"
        );
        assert_eq!(
            c(r#"{"type":"error","status":429,"error":{"message":"x"}}"#),
            "rate_limit_exceeded"
        );
        assert_eq!(
            c(r#"{"type":"error","error":{"type":"usage_limit_reached"}}"#),
            "usage_limit"
        );
        assert_eq!(
            c(
                r#"{"type":"response.failed","response":{"error":{"code":"context_length_exceeded"}}}"#
            ),
            "context_length_exceeded"
        );
        assert_eq!(
            c(r#"{"type":"error","error":{"code":"server_is_overloaded"}}"#),
            "server_is_overloaded"
        );
        assert_eq!(
            c(r#"{"type":"error","error":{"type":"invalid_request_error"}}"#),
            "invalid_request_error"
        );
        assert_eq!(c(r#"{"type":"error","code":"top_level"}"#), "top_level");
        assert_eq!(
            c(r#"{"type":"error","error":{"message":"x"}}"#),
            "upstream_error_event"
        );
        assert_eq!(
            c(r#"{"type":"response.failed","response":{}}"#),
            "response_failed"
        );
        let long = format!(
            r#"{{"type":"error","error":{{"code":"{}"}}}}"#,
            "x".repeat(65)
        );
        assert_eq!(c(&long), "upstream_error_event");
        assert_eq!(
            sse_block_code(
                b"event: error\ndata: {\"type\":\"error\",\"error\":{\"code\":\"image_failed\"}}"
            )
            .as_deref(),
            Some("image_failed")
        );
    }
    #[tokio::test]
    async fn retry_after_clamped_on_429_and_5xx() {
        let mk = |st: u16, ra: &str| {
            let mut r = Response::new(axum::body::Body::empty());
            *r.status_mut() = StatusCode::from_u16(st).unwrap();
            r.headers_mut().insert(
                http::header::RETRY_AFTER,
                HeaderValue::from_str(ra).unwrap(),
            );
            r
        };
        let ra = |r: &Response| {
            r.headers()
                .get(http::header::RETRY_AFTER)
                .map(|v| v.to_str().unwrap().to_string())
        };
        for (st, v, keep) in [
            (503, "5", true),
            (429, "20", true),
            (503, "21", false),
            (500, "3600", false),
            (429, "0", false),
            (502, "Wed, 21 Oct 2026 07:28:00 GMT", false),
            (200, "3600", true),
            (400, "0", true),
        ] {
            let r = clamp_retry_after(mk(st, v)).await;
            assert_eq!(ra(&r).is_some(), keep, "{st} {v}");
        }
    }
    #[tokio::test]
    async fn retry_after_ms_matches_the_kept_retry_after() {
        let mk = |st: u16, ra: Option<&str>, ms: Option<&str>| {
            let mut r = Response::new(axum::body::Body::empty());
            *r.status_mut() = StatusCode::from_u16(st).unwrap();
            if let Some(v) = ra {
                r.headers_mut()
                    .insert(http::header::RETRY_AFTER, HeaderValue::from_str(v).unwrap());
            }
            if let Some(v) = ms {
                r.headers_mut()
                    .insert(RETRY_AFTER_MS, HeaderValue::from_str(v).unwrap());
            }
            r
        };
        let get = |r: &Response| {
            let g = |n: &str| r.headers().get(n).map(|v| v.to_str().unwrap().to_string());
            (g("retry-after"), g("retry-after-ms"))
        };
        let s = |v: &str| Some(v.to_string());
        for (st, ra, ms, want) in [
            // Retry-After wins; the ms value always states the same delay.
            (503, Some("5"), None, (s("5"), s("5000"))),
            (429, Some("2"), Some("1500"), (s("2"), s("2000"))),
            (429, Some("2"), Some("90000"), (s("2"), s("2000"))),
            // Only retry-after-ms upstream: kept within the bound, seconds rounded up.
            (429, None, Some("1500"), (s("2"), s("1500"))),
            (503, None, Some("250.4"), (s("1"), s("251"))),
            (503, None, Some("20000"), (s("20"), s("20000"))),
            // Out of bound or invalid: both dropped.
            (503, Some("3600"), Some("3600000"), (None, None)),
            (503, None, Some("20001"), (None, None)),
            (503, None, Some("0"), (None, None)),
            (503, None, Some("soon"), (None, None)),
            (
                502,
                Some("Wed, 21 Oct 2026 07:28:00 GMT"),
                None,
                (None, None),
            ),
            // Not an error the client retries: left alone.
            (200, Some("3600"), None, (s("3600"), None)),
            (400, None, Some("0"), (None, s("0"))),
        ] {
            let r = clamp_retry_after(mk(st, ra, ms)).await;
            assert_eq!(get(&r), want, "{st} {ra:?} {ms:?}");
        }
    }
    #[test]
    fn local_retry_after_sets_both_headers() {
        let mut r = Response::new(axum::body::Body::empty());
        retry_after(&mut r, Duration::from_millis(300));
        assert_eq!(r.headers()[http::header::RETRY_AFTER], "1");
        assert_eq!(r.headers()[RETRY_AFTER_MS], "1000");
        retry_after(&mut r, Duration::from_secs(7));
        assert_eq!(r.headers()[http::header::RETRY_AFTER], "7");
        assert_eq!(r.headers()[RETRY_AFTER_MS], "7000");
    }
    #[test]
    fn quota_exhausted_has_no_retry_after_or_plan_type() {
        let r = quota_exhausted(Duration::from_secs(5400), false);
        assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(r.headers().get(http::header::RETRY_AFTER).is_none());
        let m = r.extensions().get::<ErrMeta>().unwrap();
        assert_eq!(m.code, "first_quota_reset");
        assert!(m.message.as_deref().unwrap().contains("5400s"));
        let r = all_cooling(Duration::from_secs(1800), 20, None);
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(r.headers().get(http::header::RETRY_AFTER).is_none());
    }
    #[tokio::test]
    async fn dead_proxy_is_proxy_code() {
        for proxy in ["socks5h://127.0.0.1:1", "http://127.0.0.1:1"] {
            let c = reqwest::Client::builder()
                .no_proxy()
                .proxy(reqwest::Proxy::all(proxy).unwrap())
                .build()
                .unwrap();
            let e = c.get("https://example.invalid/").send().await.unwrap_err();
            assert_eq!(net_code(&e, true), "proxy_connect", "{proxy}: {e:?}");
            assert!(chain(&e).contains("127.0.0.1:1") || chain(&e).len() > e.to_string().len());
            let r = network(net_code(&e, true));
            assert_eq!(r.status(), StatusCode::BAD_GATEWAY);
        }
        assert_eq!(
            network("proxy_timeout").status(),
            StatusCode::GATEWAY_TIMEOUT
        );
    }
    #[test]
    fn request_id_stamped_on_errors_only() {
        let mut r = json_error(StatusCode::BAD_GATEWAY, "x", "y");
        r.headers_mut()
            .insert("x-request-id", "up".parse().unwrap());
        r.headers_mut()
            .insert("x-oai-request-id", "oai".parse().unwrap());
        stamp_request_id(&mut r, "00ff");
        assert_eq!(r.headers()["x-request-id"], "cp-00ff");
        assert_eq!(r.headers()["x-upstream-request-id"], "up");
        assert_eq!(r.headers()["x-oai-request-id"], "oai");
        let mut ok = Response::new(axum::body::Body::empty());
        stamp_request_id(&mut ok, "00ff");
        assert!(ok.headers().get("x-request-id").is_none());
    }

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
            x.action = crate::attempt::Next::Retry.into();
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
