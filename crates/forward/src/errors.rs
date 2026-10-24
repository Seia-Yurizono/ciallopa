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
    /// Detailed reason for the log / panel (never sent to the client).
    pub message: Option<String>,
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
        message: None,
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
    set_retry_after(r.headers_mut(), d.as_secs().max(1));
}

/// `retry-after-ms`: the same delay in milliseconds, as OpenAI's API sends it next to
/// `Retry-After`. Clients that read it (OpenAI SDKs, AI SDK / opencode) prefer it; the
/// others ignore it, so it never changes what a client does.
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
        message: None,
    });
    r
}

/// Every account is cooling down and the first one to recover is out of usage quota (`all` =
/// every account is): the Codex `usage_limit_reached` shape with that reset, so the client
/// shows when service resumes. No `plan_type` (Codex then shows the plain "try again at"
/// copy) and no Retry-After (Codex stops on this error; see [`MAX_CLIENT_RETRY_AFTER`]).
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

/// Every account is cooling down for reasons other than usage quota. `retry` = advice for a
/// recovery the client may wait out (short, see [`MAX_CLIENT_RETRY_AFTER`]); `None` = the first
/// recovery is too far away, so the client stops at once instead of retrying for hours.
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

/// Longest `Retry-After` forwarded on a 429 / 5xx. This bounds each advertised delay,
/// not the client's total recovery cost: HTTP retries, stream recovery and transport
/// fallback have separate budgets. Zero is omitted to avoid immediate retry advice.
pub(crate) const MAX_CLIENT_RETRY_AFTER: u64 = 20;

/// Response filter for every client route: on a 429 / 5xx keep `Retry-After` only when it is a
/// delay of 1..=[`MAX_CLIENT_RETRY_AFTER`] seconds; drop it otherwise (zero, long, HTTP date).
/// `retry-after-ms` always states the same delay: derived from the kept `Retry-After`, or,
/// when the upstream sent only `retry-after-ms`, kept if it is within the same bound (then
/// `Retry-After` is its rounded-up seconds). Otherwise both are dropped.
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

/// Network-level failure code of a reqwest error.
/// Transport error → client-facing code. With an egress proxy every connect-phase failure
/// (proxy unreachable, auth / tunnel refused, proxy-side DNS) is the proxy's: `proxy_timeout`
/// / `proxy_connect`; only a TLS failure past the tunnel stays `tls`.
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
