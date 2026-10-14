//! HTTP forwarding core: account selection, failover, pre-first-byte retries, idle timeout.
use crate::headers::{downstream_headers, upstream_headers};
use crate::usage::Tracker;
use crate::AppState;
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use ciallopa_core::{AccountId, AuthError, Outcome, RouteError, RouteRequest};
use ciallopa_routing::{is_cloudflare_challenge, is_usage_limit, parse_quota, retry_after_for_429};
use ciallopa_upstream::retry::{delay_for, is_retryable_status};
use futures::StreamExt;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

pub(crate) fn json_error(status: StatusCode, kind: &str, msg: &str) -> Response {
    let body = serde_json::json!({"error": {"type": kind, "message": msg}}).to_string();
    let mut r = (status, body).into_response();
    r.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    r
}

/// An outbound request, independent of the account it ends up on.
#[derive(Clone)]
pub(crate) struct Outbound {
    pub method: Method,
    /// Relative to `upstream.base_url`, with query.
    pub path: String,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub route: RouteRequest,
    /// The client asked for an SSE stream (`/responses` with `stream:true` or
    /// `Accept: text/event-stream`). chatgpt.com sends SSE without any `Content-Type`, so this
    /// hint decides SSE handling when the upstream omits it.
    pub expect_sse: bool,
}

/// The upstream response chosen for the client (2xx, or a non-retryable / final error).
pub(crate) struct Chosen {
    pub resp: reqwest::Response,
    pub account: AccountId,
    pub route: RouteRequest,
}

fn buffered(status: u16, headers: &HeaderMap, body: Bytes) -> Response {
    let mut r = Response::new(Body::from(body));
    *r.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    *r.headers_mut() = downstream_headers(headers);
    r.headers_mut().remove(http::header::CONTENT_LENGTH);
    r
}

fn cooling_down(retry_after: Duration) -> Response {
    let mut r = json_error(
        StatusCode::TOO_MANY_REQUESTS,
        "usage_limit_reached",
        "all accounts are cooling down",
    );
    let secs = retry_after.as_secs().max(1).to_string();
    if let Ok(v) = HeaderValue::from_str(&secs) {
        r.headers_mut().insert(http::header::RETRY_AFTER, v);
    }
    r
}

/// What `send_info` observed (for the usage log).
#[derive(Default)]
pub(crate) struct SendInfo {
    pub retries: u32,
    /// Accounts tried and abandoned (failover), in order.
    pub tried: Vec<AccountId>,
}

/// Run the selection / retry loop. `Err` is a ready-made downstream response.
pub(crate) async fn send(st: &AppState, ob: Outbound) -> Result<Chosen, Response> {
    send_info(st, ob, &mut SendInfo::default()).await
}

pub(crate) async fn send_info(
    st: &AppState,
    ob: Outbound,
    info: &mut SendInfo,
) -> Result<Chosen, Response> {
    let r = send_inner(st, ob, info).await;
    info.tried = match &r {
        Ok(c) => c.route.exclude.clone(),
        Err(_) => std::mem::take(&mut info.tried),
    };
    r
}

async fn send_inner(st: &AppState, ob: Outbound, info: &mut SendInfo) -> Result<Chosen, Response> {
    let retries = u64::from(st.config.upstream_opts.max_attempts);
    let mut route = ob.route;
    let mut transient: u64 = 0;
    let mut last: Option<Response> = None;
    loop {
        info.retries = transient as u32;
        info.tried.clone_from(&route.exclude);
        let accounts = st.accounts.list().await;
        st.upstream
            .pools
            .retain(|k| accounts.iter().any(|a| a.id == k));
        let sel = match st.router.select(&route, &accounts) {
            Ok(s) => s,
            Err(RouteError::AllCoolingDown { retry_after }) => {
                return Err(last.unwrap_or_else(|| cooling_down(retry_after)))
            }
            Err(e) => {
                return Err(last.unwrap_or_else(|| {
                    json_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "no_account",
                        &e.to_string(),
                    )
                }))
            }
        };
        let id = sel.account.clone();
        let mut creds = match st.accounts.credentials(&id).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(account = %id, error = %e, "credentials unavailable");
                if !matches!(e, AuthError::Io(_)) {
                    st.router.report(&route, &id, &Outcome::AuthFailed);
                }
                route.exclude.push(id);
                continue;
            }
        };
        let mut refreshed = false;
        loop {
            let url = st.upstream.url(&ob.path);
            let res = st
                .upstream
                .http_for(&id)
                .request(ob.method.clone(), &url)
                .headers({
                    let mut h = upstream_headers(
                        &crate::turn_state::scope(&ob.headers, &id, !route.exclude.is_empty()),
                        &creds,
                    );
                    if let Some(c) = st.upstream.cookie_header(&id, &url) {
                        h.insert(http::header::COOKIE, c);
                    }
                    h
                })
                .body(ob.body.clone())
                .send()
                .await;
            let r = match res {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(account = %id, error = %e, "upstream transport error");
                    st.router
                        .report(&route, &id, &Outcome::Transient { status: None });
                    transient += 1;
                    info.retries = transient as u32;
                    if transient > retries {
                        return Err(json_error(
                            StatusCode::BAD_GATEWAY,
                            "upstream_error",
                            "upstream connection failed",
                        ));
                    }
                    tokio::time::sleep(delay_for(transient, None)).await;
                    break;
                }
            };
            st.upstream.store_cookies(&id, &url, r.headers());
            let status = r.status().as_u16();
            match status {
                s if s < 400 => {
                    crate::turn_state::learn(r.headers(), &id);
                    let quota = parse_quota(r.headers());
                    st.router.report(&route, &id, &Outcome::Success { quota });
                    return Ok(Chosen {
                        resp: r,
                        account: id,
                        route,
                    });
                }
                401 if !refreshed => {
                    refreshed = true;
                    match st.accounts.force_refresh(&id).await {
                        Ok(c) => {
                            creds = c;
                            continue;
                        }
                        Err(e) => {
                            tracing::warn!(account = %id, error = %e, "force refresh failed");
                        }
                    }
                    let h = r.headers().clone();
                    let b = r.bytes().await.unwrap_or_default();
                    st.router.report(&route, &id, &Outcome::AuthFailed);
                    last = Some(buffered(status, &h, b));
                    route.exclude.push(id.clone());
                    break;
                }
                401 | 403 | 404 | 429 => {
                    let h = r.headers().clone();
                    let b = r.bytes().await.unwrap_or_default();
                    let outcome = classify_error(status, &h, &b);
                    st.router.report(&route, &id, &outcome);
                    last = Some(buffered(status, &h, b));
                    route.exclude.push(id.clone());
                    break;
                }
                s if s == 408 || is_retryable_status(s) => {
                    st.router
                        .report(&route, &id, &Outcome::Transient { status: Some(s) });
                    transient += 1;
                    info.retries = transient as u32;
                    if transient > retries {
                        return Ok(Chosen {
                            resp: r,
                            account: id,
                            route,
                        });
                    }
                    let d = delay_for(transient, Some(r.headers()));
                    drop(r);
                    tokio::time::sleep(d).await;
                    break;
                }
                s => {
                    st.router
                        .report(&route, &id, &Outcome::ClientError { status: s });
                    return Ok(Chosen {
                        resp: r,
                        account: id,
                        route,
                    });
                }
            }
        }
    }
}

/// Stream an upstream response downstream untouched, with a per-chunk idle timeout.
/// In-stream `usage_limit_reached` (e.g. `response.failed`) is reported to the router.
pub(crate) fn stream_back(st: &AppState, c: Chosen) -> Response {
    stream_back_tracked(st, c, None, false)
}

pub(crate) fn stream_back_tracked(
    st: &AppState,
    c: Chosen,
    mut trk: Option<Tracker>,
    expect_sse: bool,
) -> Response {
    let status = c.resp.status();
    let sse = status.is_success() && is_sse(c.resp.headers(), expect_sse);
    let mut headers = downstream_headers(c.resp.headers());
    if sse {
        ensure_sse_content_type(&mut headers);
    }
    if let Some(t) = trk.as_mut() {
        t.status(status.as_u16());
        if status.as_u16() >= 400 {
            t.error_headers(c.resp.headers());
        }
        if !sse {
            t.not_stream();
        }
    }
    let up = c.resp.bytes_stream().boxed();
    stream_body(
        st,
        status,
        headers,
        c.route,
        c.account,
        Bytes::new(),
        up,
        trk,
    )
}

type UpStream = futures::stream::BoxStream<'static, reqwest::Result<Bytes>>;

#[allow(clippy::too_many_arguments)]
fn stream_body(
    st: &AppState,
    status: StatusCode,
    headers: HeaderMap,
    route: RouteRequest,
    account: AccountId,
    prefix: Bytes,
    mut up: UpStream,
    mut trk: Option<Tracker>,
) -> Response {
    let idle = Duration::from_secs(st.config.upstream_opts.stream_idle_timeout_secs.max(1));
    let router = Arc::clone(&st.router);
    let mut reported = false;
    let body = async_stream(move |tx| async move {
        let report_limit = |chunk: &[u8]| {
            router.report(
                &route,
                &account,
                &Outcome::RateLimited {
                    retry_after: ciallopa_routing::parse_codex_retry_after(
                        429,
                        chunk,
                        SystemTime::now(),
                    ),
                    usage_limit: true,
                    quota: None,
                },
            );
        };
        let fail = |trk: &mut Option<Tracker>, k: &str| {
            if let Some(t) = trk.as_mut() {
                t.fail(k);
            }
        };
        if !prefix.is_empty() {
            if memfind(&prefix, b"usage_limit_reached") {
                reported = true;
                report_limit(&prefix);
                fail(&mut trk, "usage_limit");
            }
            if let Some(t) = trk.as_mut() {
                t.feed_sse(&prefix);
            }
            if tx.send(Ok(prefix)).await.is_err() {
                fail(&mut trk, "client_closed");
                return;
            }
        }
        loop {
            match tokio::time::timeout(idle, up.next()).await {
                Err(_) => {
                    if let Some(t) = trk.as_mut() {
                        t.fail_msg(
                            "idle_timeout",
                            format!("upstream stream idle for {}s (timeout)", idle.as_secs()),
                        );
                    }
                    tracing::warn!(account = %account, "upstream stream idle timeout");
                    router.report(&route, &account, &Outcome::Transient { status: None });
                    let _ = tx
                        .send(Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "stream idle timeout",
                        )))
                        .await;
                    return;
                }
                Ok(None) => return,
                Ok(Some(Err(e))) => {
                    if let Some(t) = trk.as_mut() {
                        t.fail_msg("stream_error", format!("upstream stream error: {e}"));
                    }
                    router.report(&route, &account, &Outcome::Transient { status: None });
                    let _ = tx.send(Err(std::io::Error::other(e))).await;
                    return;
                }
                Ok(Some(Ok(chunk))) => {
                    if !reported && memfind(&chunk, b"usage_limit_reached") {
                        reported = true;
                        report_limit(&chunk);
                        fail(&mut trk, "usage_limit");
                    }
                    if let Some(t) = trk.as_mut() {
                        t.feed_sse(&chunk);
                    }
                    if tx.send(Ok(chunk)).await.is_err() {
                        fail(&mut trk, "client_closed");
                        return;
                    }
                }
            }
        }
    });
    let mut r = Response::new(body);
    *r.status_mut() = status;
    *r.headers_mut() = headers;
    r
}

/// Max bytes held while waiting for the first meaningful SSE event.
const BOOTSTRAP_MAX: usize = 1 << 20;

/// What a Responses stream event means for bootstrap / failover.
#[derive(Debug, PartialEq)]
pub(crate) enum EventKind {
    /// Envelope only (`response.created`, `in_progress`, rate-limit snapshots, keepalive).
    Preamble,
    /// Account-level quota / rate-limit failure: cool down and switch.
    Quota(Outcome),
    /// Anything else: real output (or a non-quota terminal event).
    Content,
}

/// Quota-type error codes that justify switching accounts.
pub(crate) fn is_quota_code(text: &str) -> bool {
    [
        "usage_limit_reached",
        "insufficient_quota",
        "usage_not_included",
    ]
    .iter()
    .any(|c| text.contains(c))
}

/// Classify one Responses event JSON (`data:` payload of SSE, or a WS text frame).
pub(crate) fn event_kind(text: &str) -> EventKind {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return EventKind::Content;
    };
    let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
    let quota_outcome = || {
        let quota = ciallopa_routing::parse_quota_event_headers(text.as_bytes())
            .and_then(|h| parse_quota(&h));
        Outcome::RateLimited {
            retry_after: ciallopa_routing::parse_codex_retry_after(
                429,
                text.as_bytes(),
                SystemTime::now(),
            ),
            usage_limit: is_usage_limit(text.as_bytes()) || text.contains("usage_limit_reached"),
            quota,
        }
    };
    match ty {
        "response.created" | "response.in_progress" | "codex.rate_limits" | "" => {
            EventKind::Preamble
        }
        "error" => {
            let status = v
                .get("status")
                .or_else(|| v.get("status_code"))
                .and_then(|s| s.as_u64());
            if status == Some(429) || is_quota_code(text) {
                EventKind::Quota(quota_outcome())
            } else {
                EventKind::Content
            }
        }
        "response.failed" if is_quota_code(text) => EventKind::Quota(quota_outcome()),
        _ => EventKind::Content,
    }
}

/// Split complete SSE events off the front of `buf`; yields each event's joined `data:`.
fn sse_events(buf: &[u8], from: usize) -> (Vec<String>, usize) {
    let mut out = Vec::new();
    let mut pos = from;
    loop {
        let rest = &buf[pos..];
        let Some((end, sep)) = find_sep(rest) else {
            break;
        };
        let ev = String::from_utf8_lossy(&rest[..end]);
        let data: Vec<&str> = ev
            .lines()
            .filter_map(|l| l.strip_prefix("data:"))
            .map(|d| d.strip_prefix(' ').unwrap_or(d))
            .collect();
        out.push(data.join("\n"));
        pos += end + sep;
    }
    (out, pos)
}

fn find_sep(b: &[u8]) -> Option<(usize, usize)> {
    let a = b.windows(2).position(|w| w == b"\n\n").map(|i| (i, 2));
    let c = b.windows(4).position(|w| w == b"\r\n\r\n").map(|i| (i, 4));
    match (a, c) {
        (Some(x), Some(y)) => Some(if x.0 <= y.0 { x } else { y }),
        (x, y) => x.or(y),
    }
}

/// SSE if the upstream says `text/event-stream`, or if it sent no / an unrecognised
/// `Content-Type` and the client asked for a stream. chatgpt.com's Codex endpoint currently
/// omits `Content-Type` on `/responses` SSE; an explicit JSON / text / HTML type always wins.
pub(crate) fn is_sse(h: &HeaderMap, expect_sse: bool) -> bool {
    let ct = h
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_ascii_lowercase());
    match ct.as_deref() {
        Some(v) if v.contains("text/event-stream") => true,
        None | Some("") => expect_sse,
        Some(v) => {
            expect_sse
                && !(v.contains("json")
                    || v.starts_with("text/")
                    || v.contains("html")
                    || v.contains("xml"))
        }
    }
}

/// Clients other than Codex (browsers, SDKs) need the media type to parse a stream; Codex itself
/// parses SSE regardless of `Content-Type`, so adding it is harmless.
fn ensure_sse_content_type(h: &mut HeaderMap) {
    let known = h
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.to_ascii_lowercase().contains("text/event-stream"));
    if !known {
        h.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        );
    }
}

/// `send` + stream back, with an SSE bootstrap window: until the first meaningful event,
/// upstream bytes are held; a quota / usage-limit error inside that window cools the
/// account down and transparently replays the same request on another account.
pub(crate) async fn send_streaming(
    st: &AppState,
    mut ob: Outbound,
    mut trk: Option<Tracker>,
) -> Response {
    let max_switches = st.accounts.list().await.len().max(1);
    let idle = Duration::from_secs(st.config.upstream_opts.stream_idle_timeout_secs.max(1));
    let mut switches = 0usize;
    loop {
        let mut info = SendInfo::default();
        let res = send_info(st, ob.clone(), &mut info).await;
        if let Some(t) = trk.as_mut() {
            t.retries(info.retries);
            t.accounts(&info.tried, res.as_ref().ok().map(|c| &c.account));
            if let Ok(c) = &res {
                t.quota(c.resp.headers());
            }
        }
        let c = match res {
            Ok(c) => c,
            Err(r) => {
                let Some(t) = trk.as_mut() else { return r };
                t.status(r.status().as_u16());
                t.not_stream();
                // Local / buffered upstream errors: the body is small and already in memory.
                let (parts, body) = r.into_parts();
                t.error_headers(&parts.headers);
                let b = axum::body::to_bytes(body, 1 << 20)
                    .await
                    .unwrap_or_default();
                t.error_body(&b);
                return Response::from_parts(parts, Body::from(b));
            }
        };
        if !c.resp.status().is_success() || !is_sse(c.resp.headers(), ob.expect_sse) {
            if let Some(t) = trk.as_mut() {
                t.ttft();
            }
            return stream_back_tracked(st, c, trk, ob.expect_sse);
        }
        let status = c.resp.status();
        let mut headers = downstream_headers(c.resp.headers());
        ensure_sse_content_type(&mut headers);
        let (route, account) = (c.route, c.account);
        let mut up: UpStream = c.resp.bytes_stream().boxed();
        let mut buf: Vec<u8> = Vec::new();
        let mut parsed = 0usize;
        let mut switch: Option<Outcome> = None;
        let mut ended = false;
        'boot: loop {
            if buf.len() > BOOTSTRAP_MAX {
                break;
            }
            match tokio::time::timeout(idle, up.next()).await {
                Ok(Some(Ok(chunk))) => {
                    buf.extend_from_slice(&chunk);
                    let (evs, np) = sse_events(&buf, parsed);
                    parsed = np;
                    for e in evs {
                        match event_kind(&e) {
                            EventKind::Preamble => {}
                            EventKind::Content => {
                                if let Some(t) = trk.as_mut() {
                                    t.ttft();
                                }
                                break 'boot;
                            }
                            EventKind::Quota(o) => {
                                switch = Some(o);
                                break 'boot;
                            }
                        }
                    }
                }
                Ok(None) => {
                    ended = true;
                    break;
                }
                // Timeouts / transport errors: hand over to the normal streamer which
                // reports them; the held prefix is forwarded first.
                Ok(Some(Err(_))) | Err(_) => break,
            }
        }
        if let Some(o) = switch {
            st.router.report(&route, &account, &o);
            switches += 1;
            if switches < max_switches {
                tracing::info!(account = %account, "quota error before first output; switching account");
                ob.route = route;
                ob.route.exclude.push(account);
                continue;
            }
            // Out of accounts: deliver what we have (the quota error) as-is.
            if let Some(t) = trk.as_mut() {
                t.status(status.as_u16());
                t.fail_msg(
                    "usage_limit",
                    "usage limit reached on every available account",
                );
                t.not_stream();
                t.error_body(&buf);
            }
            let mut r = Response::new(Body::from(buf));
            *r.status_mut() = status;
            *r.headers_mut() = headers;
            return r;
        }
        if ended {
            up = futures::stream::empty().boxed();
        }
        if let Some(t) = trk.as_mut() {
            t.status(status.as_u16());
        }
        return stream_body(
            st,
            status,
            headers,
            route,
            account,
            Bytes::from(buf),
            up,
            trk,
        );
    }
}

/// Tiny channel-backed body stream (avoids an async-stream dependency).
pub(crate) fn async_stream<F, Fut>(f: F) -> Body
where
    F: FnOnce(tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    tokio::spawn(f(tx));
    Body::from_stream(futures::stream::poll_fn(move |cx| rx.poll_recv(cx)))
}

pub(crate) fn memfind(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

/// Decode the request body for *inspection only* (zstd bodies are still forwarded as-is).
pub(crate) fn inspect_json(headers: &HeaderMap, body: &[u8]) -> Option<serde_json::Value> {
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
                .take(256 * 1024 * 1024)
                .read_to_end(&mut out)
                .ok()?;
            serde_json::from_slice(&out).ok()
        }
        _ => None,
    }
}

/// Account-level failure classification (shared with WS handshake).
pub(crate) fn classify_error(status: u16, h: &HeaderMap, body: &[u8]) -> Outcome {
    let now = SystemTime::now();
    match status {
        429 => Outcome::RateLimited {
            retry_after: retry_after_for_429(h, body, now),
            usage_limit: is_usage_limit(body),
            quota: parse_quota(h),
        },
        403 if h.contains_key("cf-mitigated")
            || is_cloudflare_challenge(&String::from_utf8_lossy(body)) =>
        {
            Outcome::Challenge
        }
        404 => Outcome::ModelUnsupported {
            retry_after: h
                .get(http::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| ciallopa_routing::parse_retry_after_header(v, now)),
        },
        _ => Outcome::AuthFailed,
    }
}
