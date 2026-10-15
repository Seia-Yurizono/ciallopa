//! HTTP forwarding core: account selection, failover, pre-first-byte retries, idle timeout.
use crate::errors;
use crate::headers::{downstream_headers, upstream_headers};
use crate::usage::Tracker;
use crate::AppState;
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use ciallopa_core::{AccountId, AuthError, Outcome, RouteError, RouteRequest};
use ciallopa_routing::{is_cloudflare_challenge, is_usage_limit, parse_quota, retry_after_for_429};
use ciallopa_upstream::retry::{is_retryable_status, switch_jitter};
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

/// What `send_info` observed (for the usage log).
#[derive(Default)]
pub(crate) struct SendInfo {
    pub retries: u32,
    /// Accounts tried and abandoned (failover), in order.
    pub tried: Vec<AccountId>,
    /// Every upstream attempt with ciallopa's reaction (bounded).
    pub attempts: crate::errors::Attempts,
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

/// The most meaningful upstream error seen so far (returned verbatim if nothing succeeds).
/// Priority: usage-limit 429 > other 429 > 5xx/408 > 401 > 403 > 404; later wins on ties.
#[derive(Default)]
struct Best(Option<(u8, Response)>);
impl Best {
    fn offer(&mut self, status: u16, h: &HeaderMap, b: Bytes) {
        let rank = match status {
            429 if is_usage_limit(&b) => 6,
            429 => 5,
            408 | 500..=599 => 4,
            401 => 3,
            403 => 2,
            _ => 1,
        };
        if self.0.as_ref().is_some_and(|(r, _)| *r > rank) {
            return;
        }
        let meta = errors::meta_upstream(status, h, &b);
        let mut r = buffered(status, h, b);
        r.extensions_mut().insert(meta);
        self.0 = Some((rank, r));
    }
    fn or(self, f: impl FnOnce() -> Response) -> Response {
        self.0.map(|(_, r)| r).unwrap_or_else(f)
    }
}

/// Another account (not `id`, not excluded, enabled, not cooling) could take the request.
pub(crate) fn others_usable(
    st: &AppState,
    accounts: &[ciallopa_core::Account],
    route: &RouteRequest,
    id: &AccountId,
) -> bool {
    accounts.iter().any(|a| {
        &a.id != id
            && !a.disabled
            && !route.exclude.contains(&a.id)
            && st.router.cooling_until(&a.id).is_none()
    })
}

async fn send_inner(st: &AppState, ob: Outbound, info: &mut SendInfo) -> Result<Chosen, Response> {
    let (retries, cool_secs) = {
        let c = st.config.load();
        (
            u64::from(c.upstream_opts.max_attempts),
            c.upstream_opts.transient_cooldown_secs,
        )
    };
    let mut route = ob.route;
    // Transport / 5xx retry budget is per client request: carried over account switches
    // done by `send_streaming` (quota failover), never reset per account.
    let mut transient: u64 = u64::from(info.retries);
    let mut best = Best::default();
    let mut last_net: Option<&'static str> = None;
    loop {
        info.retries = transient as u32;
        info.tried.clone_from(&route.exclude);
        let accounts = st.accounts.list().await;
        st.upstream
            .pools
            .retain(|k| accounts.iter().any(|a| a.id == k));
        let sel = match st.router.select(&route, &accounts) {
            Ok(s) => s,
            Err(RouteError::AllCoolingDown { retry_after, quota }) => {
                return Err(best.or(|| match (quota, last_net) {
                    (true, _) => errors::quota_exhausted(retry_after),
                    (false, Some(code)) => errors::network(code),
                    (false, None) => errors::local(
                        StatusCode::SERVICE_UNAVAILABLE,
                        errors::SRC_LOCAL,
                        "all_cooling",
                        Some(retry_after.max(Duration::from_secs(1))),
                    ),
                }))
            }
            Err(_) => {
                let code = if accounts.is_empty() {
                    "no_account"
                } else {
                    "no_usable_account"
                };
                return Err(best.or(|| match last_net {
                    Some(n) => errors::network(n),
                    None => errors::local(
                        StatusCode::SERVICE_UNAVAILABLE,
                        errors::SRC_LOCAL,
                        code,
                        // No Retry-After: nothing will change by itself, so the client gives up after its
                        // short request retries instead of waiting out server advice on every reconnect.
                        None,
                    ),
                }));
            }
        };
        let id = sel.account.clone();
        let mut creds = match st.accounts.credentials(&id).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(account = %id, error = %e, "credentials unavailable");
                let mut a = errors::attempt(&id, "refresh");
                a.code = Some(
                    if cools_on_credentials_error(&e) {
                        "refresh_revoked"
                    } else {
                        "refresh_transient"
                    }
                    .into(),
                );
                a.action = "switch".into();
                info.attempts.push(a);
                if cools_on_credentials_error(&e) {
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
                    let code = errors::net_code(&e);
                    tracing::warn!(account = %id, error = %e, code, "upstream transport error");
                    last_net = Some(code);
                    let mut a = errors::attempt(&id, "transport");
                    a.code = Some(code.into());
                    a.body = Some(e.to_string().chars().take(300).collect());
                    info.attempts.push(a);
                    // Network errors never cool the account.
                    st.router
                        .report(&route, &id, &Outcome::Transient { status: None });
                    // Local retries only fail over to another account; the client owns
                    // same-backend retries and their backoff (no layered amplification).
                    if transient >= retries || !others_usable(st, &accounts, &route, &id) {
                        info.attempts.act("stop");
                        return Err(best.or(|| errors::network(code)));
                    }
                    transient += 1;
                    info.retries = transient as u32;
                    info.attempts.act("switch");
                    route.exclude.push(id.clone());
                    tokio::time::sleep(switch_jitter()).await;
                    break;
                }
            };
            st.upstream.store_cookies(&id, &url, r.headers());
            let status = r.status().as_u16();
            let mut a = errors::attempt(&id, "http");
            a.status = Some(status);
            match status {
                s if s < 400 => {
                    a.action = "ok".into();
                    info.attempts.push(a);
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
                            a.action = "refresh+retry".into();
                            info.attempts.push(a);
                            creds = c;
                            continue;
                        }
                        Err(e) => {
                            tracing::warn!(account = %id, error = %e, "force refresh failed");
                        }
                    }
                    let h = r.headers().clone();
                    let b = r.bytes().await.unwrap_or_default();
                    a.code = Some(errors::upstream_code(status, &h, &b));
                    a.body = errors::body_snippet(&b);
                    a.action = "switch".into();
                    info.attempts.push(a);
                    st.router.report(&route, &id, &Outcome::AuthFailed);
                    best.offer(status, &h, b);
                    route.exclude.push(id.clone());
                    break;
                }
                401 | 403 | 404 | 429 => {
                    let h = r.headers().clone();
                    let b = r.bytes().await.unwrap_or_default();
                    let outcome = classify_error(status, &h, &b);
                    a.code = Some(match &outcome {
                        Outcome::Challenge => "challenge".into(),
                        _ => errors::upstream_code(status, &h, &b),
                    });
                    a.body = errors::body_snippet(&b);
                    a.action = "switch".into();
                    info.attempts.push(a);
                    st.router.report(&route, &id, &outcome);
                    best.offer(status, &h, b);
                    route.exclude.push(id.clone());
                    break;
                }
                s if s == 408 || is_retryable_status(s) => {
                    let h = r.headers().clone();
                    let b = r.bytes().await.unwrap_or_default();
                    let others = others_usable(st, &accounts, &route, &id);
                    let ra = h
                        .get(http::header::RETRY_AFTER)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| {
                            ciallopa_routing::parse_retry_after_header(v, SystemTime::now())
                        });
                    // Never cool the last usable account; 0 disables 5xx cooldown entirely.
                    let cooldown = (others && cool_secs > 0)
                        .then(|| ra.unwrap_or(Duration::from_secs(cool_secs)));
                    let cooled_before = st.router.cooling_until(&id).is_some();
                    st.router.report(
                        &route,
                        &id,
                        &Outcome::ServerError {
                            status: s,
                            cooldown,
                        },
                    );
                    let cooled = !cooled_before && st.router.cooling_until(&id).is_some();
                    a.code = Some(errors::upstream_code(s, &h, &b));
                    a.body = errors::body_snippet(&b);
                    info.attempts.push(a);
                    best.offer(s, &h, b);
                    let cool_tag = if cooled {
                        let secs = st
                            .router
                            .cooling_until(&id)
                            .and_then(|t| t.duration_since(SystemTime::now()).ok())
                            .map(|d| d.as_secs() + 1)
                            .unwrap_or(0);
                        format!("cooldown({secs}s)+")
                    } else {
                        String::new()
                    };
                    // No other account, or the per-request budget is spent: hand the upstream
                    // response (status, body, Retry-After) to the client, which retries it.
                    if transient >= retries || !others {
                        info.attempts.act(format!("{cool_tag}stop"));
                        return Err(best.or(unreachable_response));
                    }
                    transient += 1;
                    info.retries = transient as u32;
                    info.attempts.act(format!("{cool_tag}switch"));
                    route.exclude.push(id.clone());
                    tokio::time::sleep(switch_jitter()).await;
                    break;
                }
                s => {
                    a.action = "stop".into();
                    info.attempts.push(a);
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

fn unreachable_response() -> Response {
    errors::local(StatusCode::BAD_GATEWAY, errors::SRC_LOCAL, "internal", None)
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
            t.upstream_error(status.as_u16(), c.resp.headers());
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
        sse,
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
    sse: bool,
) -> Response {
    let idle = Duration::from_secs(
        st.config
            .load()
            .upstream_opts
            .stream_idle_timeout_secs
            .max(1),
    );
    let router = Arc::clone(&st.router);
    let mut reported = false;
    let body = async_stream(move |tx| async move {
        // Detect usage_limit only from complete SSE events (`error` / `response.failed`),
        // never by searching raw bytes (user text containing the string is fine).
        let mut pending: Vec<u8> = Vec::new();
        let mut parsed = 0usize;
        // Terminal-event detection for the truncation guard (tail carried across chunks).
        let mut term = TermScan::default();
        let report_quota = |o: &Outcome| {
            router.report(&route, &account, o);
        };
        let fail = |trk: &mut Option<Tracker>, k: &str| {
            if let Some(t) = trk.as_mut() {
                t.fail(k);
            }
        };
        let check = |buf: &[u8], from: usize, trk: &mut Option<Tracker>, reported: &mut bool| {
            let (evs, np) = sse_events(buf, from);
            for e in &evs {
                if *reported {
                    break;
                }
                if let EventKind::Quota(o) = event_kind(e) {
                    *reported = true;
                    report_quota(&o);
                    fail(trk, "usage_limit");
                }
            }
            np
        };
        if !prefix.is_empty() {
            term.feed(&prefix);
            pending.extend_from_slice(&prefix);
            parsed = check(&pending, parsed, &mut trk, &mut reported);
            if let Some(t) = trk.as_mut() {
                t.feed_sse(&prefix);
            }
            if tx.send(Ok(prefix)).await.is_err() {
                fail(&mut trk, "client_closed");
                return;
            }
            // Drop consumed bytes so the buffer stays bounded under long streams.
            if parsed > 0 {
                pending.drain(..parsed);
                parsed = 0;
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
                    if let Some(t) = trk.as_mut() {
                        t.source(errors::SRC_NETWORK);
                    }
                    if sse && !term.seen {
                        let _ = tx.send(Ok(errors::truncated_event())).await;
                        return;
                    }
                    let _ = tx
                        .send(Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "stream idle timeout",
                        )))
                        .await;
                    return;
                }
                Ok(None) => {
                    if sse && !term.seen {
                        if let Some(t) = trk.as_mut() {
                            t.fail_msg(
                                "stream_truncated",
                                "upstream stream ended without a terminal event",
                            );
                            t.source(errors::SRC_STREAM);
                        }
                        tracing::warn!(account = %account, "upstream stream ended without a terminal event");
                        let _ = tx.send(Ok(errors::truncated_event())).await;
                    }
                    return;
                }
                Ok(Some(Err(e))) => {
                    if let Some(t) = trk.as_mut() {
                        t.fail_msg("stream_truncated", format!("upstream stream error: {e}"));
                        t.source(errors::SRC_STREAM);
                    }
                    router.report(&route, &account, &Outcome::Transient { status: None });
                    if sse && !term.seen {
                        let _ = tx.send(Ok(errors::truncated_event())).await;
                        return;
                    }
                    let _ = tx.send(Err(std::io::Error::other(e))).await;
                    return;
                }
                Ok(Some(Ok(chunk))) => {
                    term.feed(&chunk);
                    if !reported {
                        pending.extend_from_slice(&chunk);
                        // Cap the hold: if an event never completes, give up scanning.
                        if pending.len() > BOOTSTRAP_MAX {
                            pending.clear();
                            parsed = 0;
                        } else {
                            parsed = check(&pending, parsed, &mut trk, &mut reported);
                            if parsed > 0 {
                                pending.drain(..parsed);
                                parsed = 0;
                            }
                        }
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

/// Cheap "a terminal Responses event went by" detector over raw SSE bytes.
#[derive(Default)]
struct TermScan {
    tail: Vec<u8>,
    seen: bool,
}
impl TermScan {
    const PATS: [&'static [u8]; 4] = [
        b"\"response.completed\"",
        b"\"response.failed\"",
        b"\"response.incomplete\"",
        b"\"response.done\"",
    ];
    fn feed(&mut self, chunk: &[u8]) {
        if self.seen {
            return;
        }
        self.tail.extend_from_slice(chunk);
        if Self::PATS.iter().any(|p| memfind(&self.tail, p))
            || (memfind(&self.tail, b"\"type\":\"error\"") && memfind(&self.tail, b"data:"))
        {
            self.seen = true;
            self.tail = Vec::new();
            return;
        }
        let keep = 32.min(self.tail.len());
        self.tail.drain(..self.tail.len() - keep);
    }
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

/// Whether a `credentials()` failure should cool the account down (shared by HTTP and WS).
/// Disk IO and transient refresh failures (network / auth 5xx) are not a bad token.
pub(crate) fn cools_on_credentials_error(e: &AuthError) -> bool {
    !matches!(e, AuthError::Io(_) | AuthError::Refresh(_))
}

/// Quota-type error codes that justify switching accounts.
const QUOTA_CODES: [&str; 3] = [
    "usage_limit_reached",
    "insufficient_quota",
    "usage_not_included",
];

fn quota_code(v: Option<&serde_json::Value>) -> bool {
    let Some(o) = v else { return false };
    ["type", "code"].iter().any(|k| {
        o.get(*k)
            .and_then(|t| t.as_str())
            .is_some_and(|t| QUOTA_CODES.contains(&t.trim()))
    })
}

/// Structured quota check shared by HTTP SSE, WS and the usage tracker. Only these fields
/// count (never free text such as instructions / messages):
/// `error` event: `status`/`status_code == 429`, `error.type|code`, top-level `type|code`;
/// `response.failed`: `response.error.type|code`.
/// Returns `(is_quota, has_quota_code)`.
pub(crate) fn quota_event(v: &serde_json::Value) -> (bool, bool) {
    match v.get("type").and_then(|t| t.as_str()).unwrap_or("") {
        "error" => {
            let status = v
                .get("status")
                .or_else(|| v.get("status_code"))
                .and_then(|s| s.as_u64());
            let code = quota_code(v.get("error"));
            (status == Some(429) || code, code)
        }
        "response.failed" => {
            let code = quota_code(v.pointer("/response/error"));
            (code, code)
        }
        _ => (false, false),
    }
}

/// Classify one Responses event JSON (`data:` payload of SSE, or a WS text frame).
pub(crate) fn event_kind(text: &str) -> EventKind {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return EventKind::Content;
    };
    let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
    if matches!(
        ty,
        "response.created" | "response.in_progress" | "codex.rate_limits" | ""
    ) {
        return EventKind::Preamble;
    }
    let (quota, code) = quota_event(&v);
    if !quota {
        return EventKind::Content;
    }
    let quota_h =
        ciallopa_routing::parse_quota_event_headers(text.as_bytes()).and_then(|h| parse_quota(&h));
    EventKind::Quota(Outcome::RateLimited {
        retry_after: ciallopa_routing::parse_codex_retry_after(
            429,
            text.as_bytes(),
            SystemTime::now(),
        ),
        usage_limit: code || is_usage_limit(text.as_bytes()),
        quota: quota_h,
    })
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
    let idle = Duration::from_secs(
        st.config
            .load()
            .upstream_opts
            .stream_idle_timeout_secs
            .max(1),
    );
    let mut switches = 0usize;
    let mut carried_retries = 0u32;
    loop {
        let mut info = SendInfo {
            retries: carried_retries,
            ..Default::default()
        };
        let res = send_info(st, ob.clone(), &mut info).await;
        carried_retries = info.retries;
        if let Some(t) = trk.as_mut() {
            t.retries(info.retries);
            t.attempts(&info.attempts);
            // A failed request that never switched still ran on one account: log it.
            let last_tried = info
                .attempts
                .list
                .last()
                .map(|a| a.account.clone())
                .filter(|a| !info.tried.contains(a));
            t.accounts(
                &info.tried,
                res.as_ref()
                    .ok()
                    .map(|c| &c.account)
                    .or(last_tried.as_ref()),
            );
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
                if let Some(m) = parts.extensions.get::<errors::ErrMeta>() {
                    t.meta_err(m);
                }
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
                Ok(Some(Err(e))) => {
                    // Consuming the Err means stream_body would only see EOF — surface it here.
                    if let Some(t) = trk.as_mut() {
                        t.status(status.as_u16());
                        t.fail_msg("stream_truncated", format!("upstream stream error: {e}"));
                        t.source(errors::SRC_STREAM);
                    }
                    st.router
                        .report(&route, &account, &Outcome::Transient { status: None });
                    let err_body = async_stream(move |tx| async move {
                        if !buf.is_empty() {
                            let _ = tx.send(Ok(Bytes::from(buf))).await;
                        }
                        let _ = tx.send(Ok(errors::truncated_event())).await;
                    });
                    let mut r = Response::new(err_body);
                    *r.status_mut() = status;
                    *r.headers_mut() = headers;
                    return r;
                }
                Err(_) => {
                    // Bootstrap already waited one full idle; do not hand over to stream_body
                    // (that would wait another idle — L1).
                    if let Some(t) = trk.as_mut() {
                        t.status(status.as_u16());
                        t.fail_msg(
                            "idle_timeout",
                            format!("upstream stream idle for {}s (timeout)", idle.as_secs()),
                        );
                        t.source(errors::SRC_NETWORK);
                    }
                    tracing::warn!(account = %account, "upstream stream idle timeout (bootstrap)");
                    st.router
                        .report(&route, &account, &Outcome::Transient { status: None });
                    let err_body = async_stream(move |tx| async move {
                        if !buf.is_empty() {
                            let _ = tx.send(Ok(Bytes::from(buf))).await;
                        }
                        let _ = tx.send(Ok(errors::truncated_event())).await;
                    });
                    let mut r = Response::new(err_body);
                    *r.status_mut() = status;
                    *r.headers_mut() = headers;
                    return r;
                }
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
                t.source(errors::SRC_STREAM);
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
            true,
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

#[cfg(test)]
mod quota_tests {
    use super::*;
    #[test]
    fn quota_only_from_structured_fields() {
        let q = |t: &str| matches!(event_kind(t), EventKind::Quota(_));
        assert!(!q(
            r#"{"type":"response.failed","response":{"error":{"code":"server_error"},"instructions":"usage_limit_reached"}}"#
        ));
        assert!(!q(
            r#"{"type":"error","status":500,"error":{"message":"see usage_limit_reached docs"}}"#
        ));
        assert!(q(
            r#"{"type":"response.failed","response":{"error":{"code":"usage_limit_reached"}}}"#
        ));
        assert!(q(
            r#"{"type":"error","error":{"type":"insufficient_quota"}}"#
        ));
        assert!(q(
            r#"{"type":"error","status":429,"error":{"message":"x"}}"#
        ));
        assert!(!q(
            r#"{"type":"response.output_text.delta","delta":"usage_limit_reached"}"#
        ));
    }
    #[test]
    fn credential_errors_that_cool() {
        assert!(!cools_on_credentials_error(&AuthError::Refresh(
            "timeout".into()
        )));
        assert!(!cools_on_credentials_error(&AuthError::Io(
            std::io::Error::other("x")
        )));
        assert!(cools_on_credentials_error(&AuthError::ReloginRequired));
    }
}
