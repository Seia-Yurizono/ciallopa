//! HTTP forwarding core: account selection, failover, pre-first-byte retries, idle timeout.
use crate::errors;
use crate::headers::{downstream_headers, upstream_headers};
use crate::limits::{read_body, BODY_TIMEOUT, ERROR_BODY_LIMIT};
use crate::usage::Tracker;
use crate::AppState;
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use ciallopa_core::{AccountId, AuthError, Outcome, RouteError, RouteRequest};
use ciallopa_routing::{is_usage_limit, parse_quota, retry_after_for_429};
use ciallopa_upstream::retry::{is_retryable_status, switch_jitter};
use futures::StreamExt;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use std::borrow::Cow;
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
    /// Captured before upstream work; keep the same idle limit through body completion.
    pub idle: Duration,
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
    /// The single same-account overload wait-and-retry of this client request is spent.
    pub overload_waited: bool,
    /// Time spent holding the request while every account was cooling down.
    pub held: Duration,
    /// The account whose preparation is in flight, including credential acquisition.
    active: Option<AccountId>,
    /// Headers already observed when the shared budget interrupts an error body / refresh.
    active_response: Option<(u16, Option<String>)>,
}

/// Charged only while preparing an HTTP attempt or waiting to retry. Successful SSE
/// bootstrap/streaming is deliberately outside this budget, even before its first output.
struct Preparation {
    budget: crate::budget::PreparationBudget,
    header_timeout: Duration,
    idle: Duration,
}

impl Preparation {
    fn new(st: &AppState) -> Self {
        let cfg = st.config.load();
        Self {
            budget: crate::budget::PreparationBudget::new(Duration::from_secs(
                cfg.upstream_opts.preparation_timeout_secs,
            )),
            header_timeout: Duration::from_secs(cfg.upstream_opts.response_header_timeout_secs),
            idle: Duration::from_secs(cfg.upstream_opts.stream_idle_timeout_secs.max(1)),
        }
    }

    async fn run<T>(
        &mut self,
        future: impl std::future::Future<Output = T>,
    ) -> Result<T, Response> {
        self.budget.run(future).await
    }
}

/// What to do when every candidate account is cooling down.
pub(crate) enum Cooling {
    /// Hold the request this long, then select again.
    Hold(Duration),
    /// The first account to recover is out of usage quota: `usage_limit_reached` with its reset.
    Quota,
    /// Answer 503 with a short Retry-After (a WebSocket handshake that cannot be held that long).
    RetrySoon(Duration),
    /// The first recovery is beyond the wait: answer 503 without Retry-After.
    Long,
}

/// Longest hold of a WebSocket handshake: Codex abandons a handshake after 15 s.
pub(crate) const WS_HOLD_MAX: Duration = Duration::from_secs(10);

impl Cooling {
    /// `recovers_in` = until the first account recovers; `held` = already held for this
    /// request; `hold_cap` = longest single hold this transport allows.
    pub(crate) fn decide(
        st: &AppState,
        recovers_in: Duration,
        earliest_quota: bool,
        held: Duration,
        hold_cap: Duration,
    ) -> Self {
        let budget = Duration::from_secs(st.config.load().routing.cooldown_wait_secs);
        let remaining = budget.saturating_sub(held);
        if !budget.is_zero() && recovers_in <= remaining.min(hold_cap) {
            // A little past the recovery instant so the next selection finds the account free.
            return Self::Hold(
                (recovers_in + Duration::from_millis(50)).max(Duration::from_millis(100)),
            );
        }
        if earliest_quota {
            return Self::Quota;
        }
        if !budget.is_zero()
            && recovers_in <= remaining
            && recovers_in.as_secs() < errors::MAX_CLIENT_RETRY_AFTER
        {
            return Self::RetrySoon(Duration::from_secs(recovers_in.as_secs() + 1));
        }
        Self::Long
    }

    /// The client response for a non-`Hold` verdict (`quota` = every account is out of quota).
    pub(crate) fn respond(&self, st: &AppState, recovers_in: Duration, quota: bool) -> Response {
        let wait = st.config.load().routing.cooldown_wait_secs;
        match self {
            Self::Quota => errors::quota_exhausted(recovers_in, quota),
            Self::RetrySoon(d) => errors::all_cooling(recovers_in, wait, Some(*d)),
            Self::Hold(_) | Self::Long => errors::all_cooling(recovers_in, wait, None),
        }
    }
}

/// Hold for `d` while every account is cooling down, adding the time to `held`. Dropping the
/// future (client gone) ends the hold.
pub(crate) async fn hold(d: Duration, held: &mut Duration) {
    tracing::info!(
        wait_ms = d.as_millis() as u64,
        "every account is cooling down; holding the request until the first one recovers"
    );
    let t = std::time::Instant::now();
    tokio::time::sleep(d).await;
    *held += t.elapsed();
}

/// Run the selection / retry loop. `Err` is a ready-made downstream response.
pub(crate) async fn send(st: &AppState, ob: Outbound) -> Result<Chosen, Response> {
    send_info(
        st,
        ob,
        &mut SendInfo::default(),
        &mut Preparation::new(st),
        None,
    )
    .await
}

/// One preparation sequence for Images calls, retaining the same diagnostics.
pub(crate) async fn send_tracked(
    st: &AppState,
    ob: Outbound,
    trk: &mut Option<Tracker>,
) -> Result<Chosen, Response> {
    let mut info = SendInfo::default();
    let result = send_info(st, ob, &mut info, &mut Preparation::new(st), trk.as_mut()).await;
    if let Some(t) = trk.as_mut() {
        t.retries(info.retries);
        t.attempts(&info.attempts);
        let last = info.attempts.list.last().map(|a| &a.account);
        t.accounts(
            &info.tried,
            result.as_ref().ok().map(|c| &c.account).or(last),
        );
        if let Ok(c) = &result {
            t.quota(c.resp.headers());
            t.error_headers(c.resp.headers());
        } else if let Err(r) = &result {
            t.error_headers(r.headers());
        }
    }
    result
}

async fn send_info(
    st: &AppState,
    ob: Outbound,
    info: &mut SendInfo,
    budget: &mut Preparation,
    trk: Option<&mut Tracker>,
) -> Result<Chosen, Response> {
    let header_timeout = budget.header_timeout;
    let idle = budget.idle;
    info.active_response = None;
    let r = match budget
        .run(send_inner(st, ob, info, header_timeout, idle, trk))
        .await
    {
        Ok(r) => r,
        Err(mut r) => {
            if let Some(account) = &info.active {
                let mut a = errors::attempt(account, "preparation");
                a.code = Some("preparation_timeout".into());
                a.action = "stop".into();
                if let Some((status, request_id)) = &info.active_response {
                    a.status = Some(*status);
                    if let Some(meta) = r.extensions_mut().get_mut::<errors::ErrMeta>() {
                        meta.upstream_status = Some(*status);
                        meta.upstream_request_id.clone_from(request_id);
                        meta.message = Some(format!("HTTP preparation budget exhausted after upstream HTTP {status}; no complete response forwarded"));
                    }
                }
                info.attempts.push(a);
            }
            Err(r)
        }
    };
    info.tried = match &r {
        Ok(c) => c.route.exclude.clone(),
        Err(_) => std::mem::take(&mut info.tried),
    };
    r
}

/// The most meaningful upstream error seen so far (returned verbatim if nothing succeeds).
/// Priority: usage-limit 429 > other 429 > 5xx/408 > 401/402 > 403 > 404; later wins on ties.
#[derive(Default)]
struct Best(Option<(u8, Response)>);
impl Best {
    fn offer(&mut self, status: u16, h: &HeaderMap, b: Bytes) {
        let rank = match status {
            429 if is_usage_limit(&b) => 6,
            429 => 5,
            408 | 500..=599 => 4,
            401 | 402 => 3,
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
    /// An overloaded answer: kept verbatim, ranked like a 5xx (a later quota 429 still wins).
    fn overloaded(&mut self, status: u16, h: &HeaderMap, b: Bytes, code: String) {
        if self.0.as_ref().is_some_and(|(r, _)| *r > 4) {
            return;
        }
        let mut meta = errors::meta_upstream(status, h, &b);
        meta.code = code;
        let mut r = buffered(status, h, b);
        r.extensions_mut().insert(meta);
        self.0 = Some((4, r));
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

async fn send_inner(
    st: &AppState,
    ob: Outbound,
    info: &mut SendInfo,
    header_timeout: Duration,
    idle: Duration,
    mut trk: Option<&mut Tracker>,
) -> Result<Chosen, Response> {
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
            Err(RouteError::AllCoolingDown {
                retry_after,
                quota,
                earliest_quota,
            }) => {
                let c = Cooling::decide(st, retry_after, earliest_quota, info.held, Duration::MAX);
                if let Cooling::Hold(d) = c {
                    hold(d, &mut info.held).await;
                    continue;
                }
                return Err(best.or(|| match (&c, last_net) {
                    (Cooling::Quota, _) | (_, None) => c.respond(st, retry_after, quota),
                    (_, Some(code)) => errors::network(code),
                }));
            }
            Err(_) => {
                let code = if accounts.is_empty() {
                    "no_account"
                } else {
                    "no_usable_account"
                };
                errors::warn_unroutable(code, accounts.len());
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
        info.active = Some(id.clone());
        info.active_response = None;
        // Preserve the selected account if cancelled before headers. Selection alone
        // does not create an HTTP attempt or imply that credentials were available.
        if let Some(t) = trk.as_deref_mut() {
            t.accounts(&route.exclude, Some(&id));
        }
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
            route.attempt_at = Some(SystemTime::now());
            info.active_response = None;
            let url = st.upstream.url(&ob.path);
            let res = match st.upstream.http_for_attempt(&id) {
                Ok((client, via_proxy)) => {
                    let request = client
                        .request(ob.method.clone(), &url)
                        .headers({
                            let mut h = upstream_headers(
                                &crate::turn_state::scope(
                                    &ob.headers,
                                    &id,
                                    !route.exclude.is_empty(),
                                ),
                                &creds,
                            );
                            if let Some(c) = st.upstream.cookie_header(&id, &url) {
                                h.insert(http::header::COOKIE, c);
                            }
                            h
                        })
                        .body(ob.body.clone());
                    tokio::time::timeout(header_timeout, request.send())
                        .await
                        .map_err(|_| {
                            (
                                "header_timeout",
                                format!(
                                    "waiting for upstream response headers exceeded {}s",
                                    header_timeout.as_secs()
                                ),
                            )
                        })
                        .and_then(|r| {
                            r.map_err(|e| (errors::net_code(&e, via_proxy), errors::chain(&e)))
                        })
                }
                Err(e) => Err(("transport", errors::chain(&e))),
            };
            let r = match res {
                Ok(r) => r,
                Err((code, cause)) => {
                    if errors::is_proxy_code(code) {
                        tracing::warn!(account = %id, error = %cause, code, "egress proxy error");
                    } else {
                        tracing::warn!(account = %id, error = %cause, code, "upstream transport error");
                    }
                    last_net = Some(code);
                    let mut a = errors::attempt(&id, "transport");
                    a.code = Some(code.into());
                    a.body = Some(cause.chars().take(300).collect());
                    info.attempts.push(a);
                    // Network errors never cool the account.
                    st.router
                        .report(&route, &id, &Outcome::Transient { status: None });
                    // Local retries only fail over to another account; the client owns
                    // same-backend retries and their backoff (no layered amplification).
                    // The egress proxy is shared by every account: switching cannot help.
                    if transient >= retries
                        || errors::is_proxy_code(code)
                        || !others_usable(st, &accounts, &route, &id)
                    {
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
            st.router.note_attempt(&id);
            let status = r.status().as_u16();
            info.active_response = Some((status, errors::request_id(r.headers())));
            let mut a = errors::attempt(&id, "http");
            a.status = Some(status);
            // Overload answered as an HTTP error (503 `server_is_overloaded`, 502/400 with only
            // an "overloaded" message): account-level throttling, so try every other usable
            // account once (does not use the transient budget); the last one passes it through.
            // Every HTTP error is buffered once under the same time/byte bounds. Incomplete
            // errors cannot safely be classified or returned as if they were the full body.
            let r = if status >= 400 {
                let h = r.headers().clone();
                let b = match read_body(r, ERROR_BODY_LIMIT, BODY_TIMEOUT).await {
                    Ok(b) => b,
                    Err(failure) => {
                        tracing::warn!(account = %id, status, code = failure.code(false), "upstream error body incomplete; stopping request");
                        a.code = Some(failure.code(false).into());
                        a.body = errors::body_snippet(&failure.prefix);
                        a.action = "stop".into();
                        info.attempts.push(a);
                        return Err(failure.response(status, &h, false));
                    }
                };
                if let Some(code) = overload_body(status, &b) {
                    let others = others_usable(st, &accounts, &route, &id);
                    let cooled_before = st.router.cooling_until(&id).is_some();
                    st.router.report(&route, &id, &overload_outcome(st, others));
                    let cooled = !cooled_before && st.router.cooling_until(&id).is_some();
                    a.code = Some(code.clone());
                    a.body = errors::body_snippet(&b);
                    info.attempts.push(a);
                    best.overloaded(status, &h, b, code);
                    let tag = if cooled { "cooldown+" } else { "" };
                    if !others {
                        if let Some(w) = overload_retry_wait(st, info.overload_waited, &h) {
                            info.overload_waited = true;
                            info.attempts.act(format!("{tag}wait+retry"));
                            tracing::info!(account = %id, wait_ms = w.as_millis() as u64, "overloaded on last usable account; waiting to retry once");
                            tokio::time::sleep(w).await;
                            continue;
                        }
                        info.attempts.act(format!("{tag}stop"));
                        return Err(best.or(unreachable_response));
                    }
                    info.attempts.act(format!("{tag}switch"));
                    route.exclude.push(id.clone());
                    tokio::time::sleep(switch_jitter()).await;
                    break;
                }
                rebuild(status, &h, b)
            } else {
                r
            };
            match status {
                s if s < 400 => {
                    st.egress.ok();
                    a.action = "ok".into();
                    info.attempts.push(a);
                    crate::turn_state::learn(r.headers(), &id);
                    let quota = parse_quota(r.headers());
                    st.router.report(&route, &id, &Outcome::Success { quota });
                    return Ok(Chosen {
                        resp: r,
                        account: id,
                        route,
                        idle,
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
                401..=404 | 429 => {
                    let h = r.headers().clone();
                    let b = r.bytes().await.unwrap_or_default();
                    // A Cloudflare page blocks the shared egress, not this account: switching
                    // would only show the same page to every account. Stop with a neutral 502.
                    if let Some(blk) = crate::egress::detect(status, &h, &b) {
                        a.code = Some(blk.code.into());
                        a.body = errors::body_snippet(&b);
                        a.action = "stop".into();
                        info.attempts.push(a);
                        st.router.report(&route, &id, &Outcome::EgressBlocked);
                        st.egress.note(&blk, status, &h, &id);
                        return Err(crate::egress::response(&blk, status, &h));
                    }
                    let outcome = classify_error(status, &h, &b, &st.config.load().upstream_opts);
                    log_account_error(&id, &outcome);
                    a.code = Some(errors::upstream_code(status, &h, &b));
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
                        idle,
                    });
                }
            }
        }
    }
}

/// An upstream response whose body was already read, rebuilt for the normal handling.
fn rebuild(status: u16, h: &HeaderMap, b: Bytes) -> reqwest::Response {
    let mut r = http::Response::new(b);
    *r.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    *r.headers_mut() = h.clone();
    reqwest::Response::from(r)
}

fn unreachable_response() -> Response {
    errors::local(StatusCode::BAD_GATEWAY, errors::SRC_LOCAL, "internal", None)
}

/// Stream an upstream response downstream untouched, with a per-chunk idle timeout.
/// In-stream `usage_limit_reached` (e.g. `response.failed`) is reported to the router.
pub(crate) fn stream_back(st: &AppState, c: Chosen) -> Response {
    stream_back_tracked(st, c, None, false)
}

/// Images SSE has a different event vocabulary. Observe only explicit error events;
/// never apply Responses terminal detection, replay or error rewriting.
pub(crate) fn stream_image_body(st: &AppState, c: Chosen, mut trk: Option<Tracker>) -> Response {
    let status = c.resp.status();
    let image_sse = status.is_success() && is_sse(c.resp.headers(), false);
    if let Some(t) = trk.as_mut() {
        t.status(status.as_u16());
        if is_sse(c.resp.headers(), false) {
            t.sse_response();
        } else {
            t.http_response();
        }
        if status.as_u16() >= 400 {
            t.upstream_error(status.as_u16(), c.resp.headers());
        }
    }
    let headers = downstream_headers(c.resp.headers());
    stream_body(
        st,
        status,
        headers,
        c.route,
        c.account,
        Bytes::new(),
        c.resp.bytes_stream().boxed(),
        trk,
        if image_sse {
            Observation::Images
        } else {
            Observation::Http
        },
        c.idle,
    )
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
            t.http_response();
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
        if sse {
            Observation::Responses(crate::usage::StreamObservation::new())
        } else {
            Observation::Http
        },
        c.idle,
    )
}

type UpStream = futures::stream::BoxStream<'static, reqwest::Result<Bytes>>;

#[derive(Clone, Copy)]
enum Observation {
    Http,
    Responses(crate::usage::StreamObservation),
    Images,
}

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
    observation: Observation,
    idle: Duration,
) -> Response {
    let sse = matches!(observation, Observation::Responses(_));
    let mut progress = match observation {
        Observation::Responses(progress) => progress,
        _ => crate::usage::StreamObservation::new(),
    };
    // Hyper may never poll a 204 / Content-Length: 0 body. Its HTTP framing already
    // proves EOF; finish before handing it over so a dropped receiver is not cancellation.
    // Responses SSE still requires a terminal event, even when its body is empty.
    let empty = status == StatusCode::NO_CONTENT
        || status == StatusCode::NOT_MODIFIED
        || headers
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            == Some(0);
    if !sse && prefix.is_empty() && empty {
        if let Some(t) = trk.as_mut() {
            t.finish_response();
        }
        let mut r = Response::new(Body::empty());
        *r.status_mut() = status;
        *r.headers_mut() = headers;
        return r;
    }
    let router = Arc::clone(&st.router);
    let mut reported = false;
    let upstream_request_id = errors::request_id(&headers);
    let image_headers =
        (matches!(observation, Observation::Images) && trk.is_some()).then(|| headers.clone());
    let body = async_stream(st.upstream.drain.clone(), move |tx| async move {
        let mut image_events = image_headers
            .as_ref()
            .map(|_| crate::image_events::ImageErrors::default());
        // Detect usage_limit only from complete SSE events (`error` / `response.failed`),
        // never by searching raw bytes (user text containing the string is fine).
        let mut pending: Vec<u8> = Vec::new();
        let mut parsed = 0usize;
        // Complete events are authoritative. Beyond the observation bound, bytes still
        // pass through, but an unobserved terminal event leaves the result unknown.
        let mut terminal_seen = false;
        let mut observation_limited = false;
        // Bytes held back until their SSE event is complete (see `Aligner`).
        let mut align = Aligner::new(sse);
        let report_quota = |o: &Outcome| {
            router.report(&route, &account, o);
        };
        let fail = |trk: &mut Option<Tracker>, k: &str| {
            if let Some(t) = trk.as_mut() {
                t.fail(k);
            }
        };
        let check = |buf: &[u8],
                     from: usize,
                     search_from: usize,
                     trk: &mut Option<Tracker>,
                     reported: &mut bool,
                     terminal: &mut bool,
                     progress: &mut crate::usage::StreamObservation,
                     observed: &mut u64| {
            let (evs, np) = sse_events_after(buf, from, search_from);
            for e in &evs {
                let ty = crate::usage::event_type(e);
                if let Some(ty) = ty.as_deref() {
                    if *observed > 0 {
                        *observed -= 1;
                    } else {
                        progress.event(ty);
                    }
                }
                *terminal |= ty.as_deref().is_some_and(crate::usage::is_terminal_type);
                if let Some(t) = trk.as_mut() {
                    t.sse_event(e, ty.as_deref());
                }
                // Only these types can produce Quota; do not allocate a full Value for
                // every output delta just to discard its Content classification. Keep
                // the old classifier fallback when the typed envelope was inconclusive.
                if !*reported && matches!(ty.as_deref(), None | Some("error" | "response.failed")) {
                    if let EventKind::Quota(o) = event_kind(e) {
                        *reported = true;
                        report_quota(&o);
                        fail(trk, "usage_limit");
                    }
                }
            }
            np
        };
        if !prefix.is_empty() {
            // Bootstrap already observed these complete envelopes at their arrival.
            // Reusing buffered bytes must not count them twice or refresh their clocks.
            let mut observed = progress.events;
            if prefix.len() <= OBSERVATION_MAX {
                pending.extend_from_slice(&prefix);
                parsed = check(
                    &pending,
                    parsed,
                    0,
                    &mut trk,
                    &mut reported,
                    &mut terminal_seen,
                    &mut progress,
                    &mut observed,
                );
            } else {
                observation_limited = true;
                progress.limit_reached();
            }
            let out = align.push(&prefix);
            if !out.is_empty() && tx.send(Ok(out)).await.is_err() {
                if let Some(t) = trk.as_mut() {
                    t.client_gone();
                }
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
                    if sse {
                        progress.warn(
                            "sse",
                            "idle_timeout",
                            &account,
                            upstream_request_id.as_deref(),
                            trk.as_ref(),
                        );
                    } else {
                        tracing::warn!(account = %account, record_id = trk.as_ref().map(Tracker::record_id), upstream_request_id = upstream_request_id.as_deref(), "upstream stream idle timeout");
                    }
                    router.report(&route, &account, &Outcome::Transient { status: None });
                    if let Some(t) = trk.as_mut() {
                        t.source(errors::SRC_NETWORK);
                    }
                    if let Some(rest) = align.finish() {
                        let _ = tx.send(Ok(rest)).await;
                    }
                    if sse && !terminal_seen {
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
                    if let Some(rest) = align.finish() {
                        if tx.send(Ok(rest)).await.is_err() {
                            if let Some(t) = trk.as_mut() {
                                t.client_gone();
                            }
                            return;
                        }
                    }
                    if sse && !terminal_seen && observation_limited {
                        progress.warn(
                            "sse",
                            "observation_limit",
                            &account,
                            upstream_request_id.as_deref(),
                            trk.as_ref(),
                        );
                        if let Some(t) = trk.as_mut() {
                            t.observation_incomplete();
                        }
                    } else if sse && !terminal_seen {
                        if let Some(t) = trk.as_mut() {
                            t.fail_msg(
                                "stream_truncated",
                                "upstream stream ended without a terminal event",
                            );
                            t.source(errors::SRC_STREAM);
                        }
                        progress.warn(
                            "sse",
                            "eof",
                            &account,
                            upstream_request_id.as_deref(),
                            trk.as_ref(),
                        );
                        let _ = tx.send(Ok(errors::truncated_event())).await;
                    }
                    if !sse {
                        if let Some(t) = trk.as_mut() {
                            t.finish_response();
                        }
                    }
                    return;
                }
                Ok(Some(Err(e))) => {
                    if sse {
                        progress.warn(
                            "sse",
                            "read_error",
                            &account,
                            upstream_request_id.as_deref(),
                            trk.as_ref(),
                        );
                    }
                    if let Some(t) = trk.as_mut() {
                        t.fail_msg("stream_truncated", format!("upstream stream error: {e}"));
                        t.source(errors::SRC_STREAM);
                    }
                    router.report(&route, &account, &Outcome::Transient { status: None });
                    if let Some(rest) = align.finish() {
                        let _ = tx.send(Ok(rest)).await;
                    }
                    if sse && !terminal_seen {
                        let _ = tx.send(Ok(errors::truncated_event())).await;
                        return;
                    }
                    let _ = tx.send(Err(std::io::Error::other(e))).await;
                    return;
                }
                Ok(Some(Ok(chunk))) => {
                    if let (Some(observer), Some(t), Some(h)) =
                        (image_events.as_mut(), trk.as_mut(), image_headers.as_ref())
                    {
                        if let Some(error) = observer.feed(&chunk) {
                            t.meta_err(&errors::ErrMeta {
                                source: errors::SRC_STREAM,
                                code: "upstream_error_event".into(),
                                upstream_status: Some(status.as_u16()),
                                upstream_request_id: upstream_request_id.clone(),
                                message: Some(error.message),
                            });
                            t.error_headers(h);
                            t.error_text(&error.event);
                        }
                    }
                    if sse {
                        if !chunk.is_empty() {
                            progress.activity();
                        }
                        // One shared observation buffer replaces the Tracker's separate
                        // 8 MiB buffer. Oversized payloads still pass through untouched.
                        if pending.len().saturating_add(chunk.len()) > OBSERVATION_MAX {
                            pending.clear();
                            parsed = 0;
                            observation_limited = true;
                            progress.limit_reached();
                        } else {
                            // The old tail had no complete delimiter. Only its last three
                            // bytes can join the new chunk's LF / CRLF event boundary.
                            let search_from = pending.len().saturating_sub(3);
                            pending.extend_from_slice(&chunk);
                            parsed = check(
                                &pending,
                                parsed,
                                search_from,
                                &mut trk,
                                &mut reported,
                                &mut terminal_seen,
                                &mut progress,
                                &mut 0,
                            );
                            if parsed > 0 {
                                pending.drain(..parsed);
                                parsed = 0;
                            }
                        }
                    }
                    if let Some(t) = trk.as_mut() {
                        if status.as_u16() >= 400 {
                            t.error_body(&chunk);
                        }
                    }
                    // The response decoder yields an empty chunk at end of body; forwarding it
                    // after the client hung up would look like a disconnect.
                    if chunk.is_empty() {
                        continue;
                    }
                    let out = align.push(&chunk);
                    if out.is_empty() {
                        continue;
                    }
                    if tx.send(Ok(out)).await.is_err() {
                        if let Some(t) = trk.as_mut() {
                            t.client_gone();
                        }
                        return;
                    }
                }
            }
        }
    });
    let mut r = Response::new(body);
    *r.status_mut() = status;
    *r.headers_mut() = headers;
    // The SSE body may differ from the upstream's (rewritten or appended terminal events).
    if sse {
        r.headers_mut().remove(http::header::CONTENT_LENGTH);
    }
    r
}

/// Forwards an SSE body in whole events, so an upstream encrypted-content rejection that
/// arrives after output can be replaced by a `response.failed` carrying the clear message
/// (`encrypted::failed_event`) before any of its bytes leave. Everything else is forwarded
/// unchanged; only the incomplete tail of the current event is held (Codex cannot use it
/// before it completes anyway). An event larger than `BOOTSTRAP_MAX` is flushed as is.
/// Non-SSE bodies pass straight through.
struct Aligner {
    sse: bool,
    tail: Vec<u8>,
}
impl Aligner {
    fn new(sse: bool) -> Self {
        Self {
            sse,
            tail: Vec::new(),
        }
    }

    /// Bytes ready to send after `chunk` (possibly empty).
    fn push(&mut self, chunk: &[u8]) -> Bytes {
        if !self.sse {
            return Bytes::copy_from_slice(chunk);
        }
        self.tail.extend_from_slice(chunk);
        match last_boundary(&self.tail) {
            Some(end) => {
                let rest = self.tail.split_off(end);
                let ready = std::mem::replace(&mut self.tail, rest);
                rewrite_rejections(ready)
            }
            None if self.tail.len() > BOOTSTRAP_MAX => Bytes::from(std::mem::take(&mut self.tail)),
            None => Bytes::new(),
        }
    }

    /// Whatever is still held (an unterminated last event), at end of stream.
    fn finish(&mut self) -> Option<Bytes> {
        (!self.tail.is_empty()).then(|| rewrite_rejections(std::mem::take(&mut self.tail)))
    }
}

/// End (exclusive, separator included) of the last complete SSE event in `b`.
fn last_boundary(b: &[u8]) -> Option<usize> {
    let mut i = b.len();
    while i >= 2 {
        if b[i - 1] == b'\n' {
            if b[i - 2] == b'\n' {
                return Some(i);
            }
            if i >= 3 && b[i - 2] == b'\r' && b[i - 3] == b'\n' {
                return Some(i);
            }
        }
        i -= 1;
    }
    None
}

/// Replace encrypted-content rejection events in complete SSE bytes (cheap byte check first).
fn rewrite_rejections(ready: Vec<u8>) -> Bytes {
    let maybe = memfind(&ready, crate::encrypted::CODE.as_bytes())
        || memfind(&ready, b"ncrypted content")
        || memfind(&ready, b"thinking_signature_invalid");
    if !maybe {
        return Bytes::from(ready);
    }
    let mut out = Vec::with_capacity(ready.len());
    let mut pos = 0;
    while pos < ready.len() {
        let (end, sep) = find_sep(&ready[pos..]).unwrap_or((ready.len() - pos, 0));
        let ev = &ready[pos..pos + end];
        let hit = crate::encrypted::sse_data(ev).and_then(|d| {
            crate::encrypted::rejection_text_json(&d)
                .and_then(|_| serde_json::from_str::<serde_json::Value>(&d).ok())
        });
        match hit {
            Some(v) => out.extend_from_slice(&crate::encrypted::failed_event(Some(&v))),
            None => out.extend_from_slice(&ready[pos..pos + end + sep]),
        }
        pos += end + sep;
    }
    Bytes::from(out)
}

/// Max bytes held while waiting for the first meaningful SSE event.
pub(crate) const BOOTSTRAP_MAX: usize = 1 << 20;
const OBSERVATION_MAX: usize = 8 << 20;

/// What a Responses stream event means for bootstrap / failover.
#[derive(Debug, PartialEq)]
pub(crate) enum EventKind {
    /// Envelope only (`response.created`, `in_progress`, rate-limit snapshots, keepalive).
    Preamble,
    /// Account-level quota / rate-limit failure: cool down and switch.
    Quota(Outcome),
    /// Upstream capacity / per-account throttling (`server_is_overloaded`, `slow_down`, …):
    /// switch account if another one is usable, else pass through. Carries the logged code.
    Overloaded(String),
    /// The upstream rejected the encrypted reasoning content in the request's history
    /// (`invalid_encrypted_content`): a request problem, never retried or held against the
    /// account. Carries the upstream message.
    Encrypted(String),
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

/// Structured codes that mean "upstream capacity / account throttled", never quota.
const OVERLOAD_CODES: [&str; 3] = [
    "server_is_overloaded",
    "slow_down",
    "service_unavailable_error",
];

/// Overload detection on an error event (`error`, `response.failed`, or an event whose own
/// `type`/`code` is an overload code) or an HTTP error body. Structured `code`/`type` first;
/// a bare `message` ("…currently overloaded…", "…at capacity…") counts only when the error
/// object carries no other code. Output events (deltas, items) are never inspected.
/// Returns the code to log (`server_is_overloaded` for message-only matches).
pub(crate) fn overload_code(v: &serde_json::Value) -> Option<String> {
    let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
    let errorish = ty == "error"
        || ty == "response.failed"
        || OVERLOAD_CODES.contains(&ty)
        || (ty.is_empty() && v.get("error").is_some());
    if !errorish {
        return None;
    }
    let objs = [Some(v), v.get("error"), v.pointer("/response/error")];
    let field = |o: &serde_json::Value, k: &str| {
        o.get(k)
            .and_then(|t| t.as_str())
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .map(str::to_string)
    };
    let mut other_code = false;
    for o in objs.iter().flatten() {
        if let Some(c) = field(o, "code") {
            if OVERLOAD_CODES.contains(&c.as_str()) {
                return Some(c);
            }
            other_code = true;
        }
    }
    for o in objs.iter().flatten() {
        if let Some(t) = field(o, "type").filter(|t| OVERLOAD_CODES.contains(&t.as_str())) {
            return Some(t);
        }
    }
    if other_code {
        return None;
    }
    let overloaded_msg = objs.iter().flatten().any(|o| {
        field(o, "message").is_some_and(|m| {
            let m = m.to_ascii_lowercase();
            m.contains("overloaded") || m.contains("at capacity")
        })
    });
    overloaded_msg.then(|| "server_is_overloaded".to_string())
}

/// Overload in an HTTP error body (400 / 429 / 5xx JSON).
pub(crate) fn overload_body(status: u16, b: &[u8]) -> Option<String> {
    if !(status == 400 || status == 429 || status >= 500) {
        return None;
    }
    let mut v: serde_json::Value = serde_json::from_slice(b).ok()?;
    if v.get("type").is_none() && v.get("error").is_some() {
        if let Some(o) = v.as_object_mut() {
            o.insert("type".into(), "error".into());
        }
    }
    overload_code(&v)
}

/// `x-retry-metadata: NO_MORE_RETRY`: do not retry on the same account (switching is fine).
pub(crate) fn no_more_retry(h: &HeaderMap) -> bool {
    h.get_all("x-retry-metadata")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|v| v.to_ascii_uppercase().contains("NO_MORE_RETRY"))
}

/// Pause for the one same-account retry after an overload with no other usable account, or
/// `None` when it is spent, disabled, or forbidden by `x-retry-metadata: NO_MORE_RETRY`.
pub(crate) fn overload_retry_wait(st: &AppState, waited: bool, h: &HeaderMap) -> Option<Duration> {
    if waited || no_more_retry(h) {
        return None;
    }
    let base = st.config.load().upstream_opts.overload_retry_wait_secs;
    ciallopa_upstream::retry::overload_wait(base, Some(h))
}

/// The router outcome for one overloaded answer on `id` (cooling never hits the last usable
/// account).
pub(crate) fn overload_outcome(st: &AppState, others: bool) -> Outcome {
    let c = st.config.load();
    let o = &c.upstream_opts;
    Outcome::Overloaded {
        streak: o.overload_streak,
        base: Duration::from_secs(o.overload_cooldown_secs),
        max: Duration::from_secs(o.overload_cooldown_max_secs),
        cool: others,
    }
}

/// Classify one Responses event JSON (`data:` payload of SSE, or a WS text frame).
pub(crate) fn event_kind(text: &str) -> EventKind {
    event_kind_observed(text, None)
}

fn event_kind_observed(
    text: &str,
    progress: Option<&mut crate::usage::StreamObservation>,
) -> EventKind {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return EventKind::Content;
    };
    let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
    if let Some(progress) = progress.filter(|_| v.get("type").and_then(|t| t.as_str()).is_some()) {
        progress.event(ty);
    }
    // `response.queued` / `response.metadata` / `keepalive` are chatgpt Codex bookkeeping
    // before any model output (moderation info, queue status, heartbeats). Treating them as
    // Content would commit the attempt and block pre-output overload switch / encrypted 400
    // (sub2api #8139). Exact names only; unknown types stay Content.
    if matches!(
        ty,
        "response.created"
            | "response.in_progress"
            | "response.queued"
            | "response.metadata"
            | "codex.rate_limits"
            | "keepalive"
            | ""
    ) {
        return EventKind::Preamble;
    }
    if let Some(m) = crate::encrypted::rejection(&v) {
        return EventKind::Encrypted(m);
    }
    let (quota, code) = quota_event(&v);
    if !quota {
        return match overload_code(&v) {
            Some(c) => EventKind::Overloaded(c),
            // Transport metadata (`codex.response.metadata`, `responsesapi.websocket_timing`).
            None if ty.starts_with("codex.") || ty.starts_with("responsesapi.") => {
                EventKind::Preamble
            }
            None => EventKind::Content,
        };
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
fn sse_events(buf: &[u8], from: usize) -> (Vec<Cow<'_, str>>, usize) {
    sse_events_after(buf, from, from)
}

/// `from` is the unconsumed event's start; `search_from` skips its previously searched
/// prefix. They differ only for an unfinished event carried across chunks.
fn sse_events_after(buf: &[u8], from: usize, search_from: usize) -> (Vec<Cow<'_, str>>, usize) {
    let mut out = Vec::new();
    let mut pos = from;
    let mut search = search_from.max(from);
    loop {
        let rest = &buf[search..];
        let Some((end, sep)) = find_sep(rest) else {
            break;
        };
        let end = search + end;
        // The usual single data line borrows the existing event buffer. Multi-line SSE
        // needs joining, but verifying the type never builds a second large JSON Value.
        out.push(match String::from_utf8_lossy(&buf[pos..end]) {
            Cow::Borrowed(ev) => sse_data(ev),
            Cow::Owned(ev) => Cow::Owned(sse_data(&ev).into_owned()),
        });
        pos = end + sep;
        search = pos;
    }
    (out, pos)
}

fn sse_data(ev: &str) -> Cow<'_, str> {
    let mut lines = ev
        .lines()
        .filter_map(|l| l.strip_prefix("data:"))
        .map(|d| d.strip_prefix(' ').unwrap_or(d));
    let first = lines.next().unwrap_or("");
    let Some(second) = lines.next() else {
        return Cow::Borrowed(first);
    };
    let mut text = first.to_string();
    for line in std::iter::once(second).chain(lines) {
        text.push('\n');
        text.push_str(line);
    }
    Cow::Owned(text)
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

fn is_json(h: &HeaderMap) -> bool {
    h.get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.to_ascii_lowercase().contains("json"))
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
    let mut budget = Preparation::new(st);
    let idle = budget.idle;
    let max_switches = st.accounts.list().await.len().max(1);
    let mut switches = 0usize;
    let mut info = SendInfo::default();
    // Keep the attempt chain even after each send_info batch has been consumed below.
    let mut chain = errors::Attempts::default();
    loop {
        let res = send_info(st, ob.clone(), &mut info, &mut budget, trk.as_mut()).await;
        for a in info.attempts.list.drain(..) {
            chain.push(a);
        }
        if let Some(t) = trk.as_mut() {
            t.retries(info.retries);
            t.attempts(&chain);
            // A failed request that never switched still ran on one account: log it.
            let last_tried = chain
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
        let mut c = match res {
            Ok(c) => c,
            Err(r) => {
                let Some(t) = trk.as_mut() else { return r };
                t.status(r.status().as_u16());
                t.http_response();
                // Local / buffered upstream errors are already complete and small.
                t.finish_response();
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
            if c.resp.status() == StatusCode::BAD_REQUEST && is_json(c.resp.headers()) {
                // A plain 400 rejection of the encrypted history: same clear message.
                let h = c.resp.headers().clone();
                let b = c.resp.bytes().await.unwrap_or_default();
                let v = serde_json::from_slice::<serde_json::Value>(&b).ok();
                if let Some(msg) = v.as_ref().and_then(crate::encrypted::rejection) {
                    tracing::info!(account = %c.account, "upstream rejected the encrypted content in the request history (HTTP 400)");
                    if let Some(t) = trk.as_mut() {
                        t.status(400);
                        t.http_response();
                        t.error_headers(&h);
                        t.upstream_error(400, &h);
                        t.encrypted_rejected(&msg, &b, &h);
                    }
                    return crate::encrypted::http_response(v.as_ref());
                }
                c.resp = rebuild(400, &h, b);
            }
            return stream_back_tracked(st, c, trk, ob.expect_sse);
        }
        let status = c.resp.status();
        let up_headers = c.resp.headers().clone();
        let mut headers = downstream_headers(c.resp.headers());
        ensure_sse_content_type(&mut headers);
        let (route, account) = (c.route, c.account);
        let mut up: UpStream = c.resp.bytes_stream().boxed();
        let mut buf: Vec<u8> = Vec::new();
        let mut parsed = 0usize;
        let mut switch: Option<Outcome> = None;
        let mut overload: Option<String> = None;
        let mut rejected: Option<(String, String)> = None;
        let mut ended = false;
        let mut progress = crate::usage::StreamObservation::new();
        'boot: loop {
            if buf.len() > BOOTSTRAP_MAX {
                break;
            }
            match tokio::time::timeout(idle, up.next()).await {
                Ok(Some(Ok(chunk))) => {
                    if !chunk.is_empty() {
                        progress.activity();
                    }
                    buf.extend_from_slice(&chunk);
                    let (evs, np) = sse_events(&buf, parsed);
                    parsed = np;
                    for e in evs {
                        match event_kind_observed(&e, Some(&mut progress)) {
                            EventKind::Preamble => {}
                            EventKind::Content => {
                                st.router.content_ok(&account);
                                break 'boot;
                            }
                            EventKind::Quota(o) => {
                                switch = Some(o);
                                break 'boot;
                            }
                            EventKind::Overloaded(code) => {
                                overload = Some(code);
                                break 'boot;
                            }
                            EventKind::Encrypted(m) => {
                                rejected = Some((m, e.into_owned()));
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
                    progress.warn(
                        "sse",
                        "read_error",
                        &account,
                        errors::request_id(&up_headers).as_deref(),
                        trk.as_ref(),
                    );
                    // Consuming the Err means stream_body would only see EOF — surface it here.
                    if let Some(t) = trk.as_mut() {
                        t.status(status.as_u16());
                        t.fail_msg("stream_truncated", format!("upstream stream error: {e}"));
                        t.source(errors::SRC_STREAM);
                    }
                    st.router
                        .report(&route, &account, &Outcome::Transient { status: None });
                    let err_body = async_stream(st.upstream.drain.clone(), move |tx| async move {
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
                    progress.warn(
                        "sse",
                        "idle_timeout",
                        &account,
                        errors::request_id(&up_headers).as_deref(),
                        trk.as_ref(),
                    );
                    st.router
                        .report(&route, &account, &Outcome::Transient { status: None });
                    let err_body = async_stream(st.upstream.drain.clone(), move |tx| async move {
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
        if let Some((msg, ev)) = rejected {
            // Nothing reached the client: answer with a plain 400 (Codex shows it once instead
            // of retrying the stream). No account outcome: the request content is at fault.
            drop(up);
            let v = serde_json::from_str::<serde_json::Value>(&ev).ok();
            tracing::info!(account = %account, "upstream rejected the encrypted content in the request history");
            let mut a = errors::attempt(&account, "stream");
            a.status = Some(status.as_u16());
            a.code = Some(crate::encrypted::CODE.into());
            a.body = errors::body_snippet(ev.as_bytes());
            a.action = "stop".into();
            chain.push(a);
            if let Some(t) = trk.as_mut() {
                t.attempts(&chain);
                t.status(StatusCode::BAD_REQUEST.as_u16());
                t.http_response();
                t.encrypted_rejected(&msg, ev.as_bytes(), &up_headers);
            }
            return crate::encrypted::http_response(v.as_ref());
        }
        if let Some(code) = overload {
            // In-stream overload before any output: nothing reached the client yet, so the same
            // request is replayed on another usable account (each account at most once).
            let accounts = st.accounts.list().await;
            let others = others_usable(st, &accounts, &route, &account);
            let cooled_before = st.router.cooling_until(&account).is_some();
            st.router
                .report(&route, &account, &overload_outcome(st, others));
            let cooled = !cooled_before && st.router.cooling_until(&account).is_some();
            let mut a = errors::attempt(&account, "stream");
            a.status = Some(status.as_u16());
            a.code = Some(code.clone());
            a.body = errors::body_snippet(&buf);
            let tag = if cooled { "cooldown+" } else { "" };
            let wait = (!others)
                .then(|| overload_retry_wait(st, info.overload_waited, &up_headers))
                .flatten();
            let next = match (others, wait.is_some()) {
                (true, _) => "switch",
                (false, true) => "wait+retry",
                (false, false) => "stop",
            };
            a.action = format!("{tag}{next}");
            chain.push(a);
            if let Some(t) = trk.as_mut() {
                t.attempts(&chain);
            }
            if others {
                tracing::info!(account = %account, code = %code, "overloaded before first output; switching account");
                ob.route = route;
                ob.route.exclude.push(account);
                // Exhaustion is handled at the next send_info before it polls an attempt.
                let _ = budget.run(tokio::time::sleep(switch_jitter())).await;
                continue;
            }
            if let Some(w) = wait {
                // Only usable account left: nothing reached the client, so wait once and replay
                // on the same account (Codex does not retry an in-stream overload itself).
                tracing::info!(account = %account, code = %code, wait_ms = w.as_millis() as u64, "overloaded before first output on last usable account; waiting to retry once");
                info.overload_waited = true;
                drop(up);
                ob.route = route;
                let _ = budget.run(tokio::time::sleep(w)).await;
                continue;
            }
            // Last usable account: the upstream error goes to the client verbatim (rest of the
            // stream included); the tracker logs it as stream / <code>.
            if let Some(t) = trk.as_mut() {
                t.status(status.as_u16());
                t.fail_msg(&code, "upstream overloaded on every usable account");
                t.source(errors::SRC_STREAM);
            }
            if ended {
                up = futures::stream::empty().boxed();
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
                Observation::Responses(progress),
                idle,
            );
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
                t.finish_response();
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
            Observation::Responses(progress),
            idle,
        );
    }
}

/// Tiny channel-backed body stream (avoids an async-stream dependency).
pub(crate) fn async_stream<F, Fut>(drain: Arc<ciallopa_upstream::drain::Drain>, f: F) -> Body
where
    F: FnOnce(tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let closed = tx.clone();
    let produce = f(tx);
    let worker = drain.worker();
    tokio::spawn(async move {
        let _worker = worker;
        // A disconnected client must release the upstream and tracker even while the
        // producer is waiting for a chunk (and has no send on which to notice closure).
        tokio::select! {
            biased;
            _ = drain.forced() => {}
            _ = closed.closed() => {}
            _ = produce => {}
        }
    });
    Body::from_stream(futures::stream::poll_fn(move |cx| rx.poll_recv(cx)))
}

#[cfg(test)]
mod stream_lifecycle_tests {
    use super::*;

    #[tokio::test]
    async fn exhausted_preparation_budget_does_not_start_another_attempt() {
        let polled = std::cell::Cell::new(false);
        let mut budget = Preparation {
            budget: crate::budget::PreparationBudget::new(Duration::ZERO),
            header_timeout: Duration::from_secs(1),
            idle: Duration::from_secs(1),
        };
        let response = budget.run(async { polled.set(true) }).await.unwrap_err();
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        assert!(
            !polled.get(),
            "no credential lookup, routing or send after budget exhaustion"
        );
    }

    #[tokio::test]
    async fn body_drop_cancels_an_idle_producer() {
        struct OnDrop(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                if let Some(tx) = self.0.take() {
                    let _ = tx.send(());
                }
            }
        }

        let (started, ready) = tokio::sync::oneshot::channel();
        let (dropped, cancelled) = tokio::sync::oneshot::channel();
        let body = async_stream(Arc::default(), move |_tx| async move {
            let _guard = OnDrop(Some(dropped));
            let _ = started.send(());
            futures::future::pending::<()>().await;
        });
        tokio::time::timeout(Duration::from_secs(1), ready)
            .await
            .expect("producer started")
            .expect("start notification");
        drop(body);
        tokio::time::timeout(Duration::from_secs(1), cancelled)
            .await
            .expect("body drop releases an idle upstream without waiting for its timeout")
            .expect("producer dropped");
    }
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
/// Account-scoped outcome of an upstream 401/402/403/404/429 (cooldowns from `[upstream_opts]`).
pub(crate) fn classify_error(
    status: u16,
    h: &HeaderMap,
    body: &[u8],
    opts: &ciallopa_core::config::UpstreamOpts,
) -> Outcome {
    let now = SystemTime::now();
    let retry_after = || {
        h.get(http::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| ciallopa_routing::parse_retry_after_header(v, now))
    };
    match status {
        402 => {
            let deactivated = is_workspace_deactivated(body);
            let secs = if deactivated {
                opts.deactivated_cooldown_secs
            } else {
                opts.payment_required_cooldown_secs
            };
            Outcome::PaymentRequired {
                deactivated,
                cooldown: Duration::from_secs(secs),
            }
        }
        404 if !is_model_unsupported(body) => Outcome::NotFound {
            retry_after: retry_after(),
            cooldown: Duration::from_secs(opts.not_found_cooldown_secs),
        },
        429 => Outcome::RateLimited {
            retry_after: retry_after_for_429(h, body, now),
            usage_limit: is_usage_limit(body),
            quota: parse_quota(h),
        },
        403 if crate::egress::detect(status, h, body).is_some() => Outcome::EgressBlocked,
        404 => Outcome::ModelUnsupported {
            retry_after: retry_after(),
        },
        _ => Outcome::AuthFailed,
    }
}

/// One warn line for account-level upstream problems an operator has to act on.
pub(crate) fn log_account_error(account: &AccountId, o: &Outcome) {
    match o {
        Outcome::PaymentRequired {
            deactivated: true,
            cooldown,
        } => {
            tracing::warn!(%account, cooldown_secs = cooldown.as_secs(), "upstream 402 deactivated_workspace: the ChatGPT workspace of this account was deactivated; switching account")
        }
        Outcome::PaymentRequired { cooldown, .. } => {
            tracing::warn!(%account, cooldown_secs = cooldown.as_secs(), "upstream 402 payment required; switching account")
        }
        _ => {}
    }
}

/// String fields that carry an upstream error code / message: `detail` (string or
/// `{code,message}`), `error` (string or `{code,type,message}`), top-level `code` / `message`.
fn error_texts(body: &[u8]) -> (Vec<String>, Vec<String>) {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(body) else {
        return (
            vec![],
            vec![String::from_utf8_lossy(body).to_ascii_lowercase()],
        );
    };
    let s = |x: Option<&serde_json::Value>| x.and_then(|x| x.as_str()).map(str::to_ascii_lowercase);
    let mut codes = vec![];
    let mut msgs = vec![];
    for obj in [Some(&v), v.get("detail"), v.get("error")]
        .into_iter()
        .flatten()
    {
        if let Some(t) = obj.as_str() {
            msgs.push(t.to_ascii_lowercase());
            continue;
        }
        codes.extend(s(obj.get("code")));
        codes.extend(s(obj.get("type")));
        msgs.extend(s(obj.get("message")));
    }
    (codes, msgs)
}

/// 402 `deactivated_workspace`: the ChatGPT workspace of this account was deactivated.
pub(crate) fn is_workspace_deactivated(body: &[u8]) -> bool {
    error_texts(body)
        .0
        .iter()
        .any(|c| c == "deactivated_workspace")
}

/// A 404 whose body explicitly says the requested model is unsupported / unknown for this
/// account (`model_not_found`, "model … not supported / does not exist"). A bare
/// `{"detail":"Not Found"}` is not.
pub(crate) fn is_model_unsupported(body: &[u8]) -> bool {
    let (codes, msgs) = error_texts(body);
    codes.iter().any(|c| {
        matches!(
            c.as_str(),
            "model_not_found" | "model_not_supported" | "unsupported_model" | "invalid_model"
        )
    }) || msgs.iter().any(|m| {
        m.contains("model")
            && [
                "not supported",
                "unsupported",
                "does not exist",
                "not found",
                "not available",
            ]
            .iter()
            .any(|p| m.contains(p))
    })
}

#[cfg(test)]
mod quota_tests {
    use super::*;
    #[test]
    fn classify_402_and_404() {
        let o = ciallopa_core::config::UpstreamOpts::default();
        let h = HeaderMap::new();
        let c = |s: u16, b: &str| classify_error(s, &h, b.as_bytes(), &o);
        assert!(matches!(
            c(402, r#"{"detail":{"code":"deactivated_workspace"}}"#),
            Outcome::PaymentRequired { deactivated: true, cooldown } if cooldown == Duration::from_secs(86_400)
        ));
        assert!(matches!(
            c(
                402,
                r#"{"error":{"code":"deactivated_workspace","message":"x"}}"#
            ),
            Outcome::PaymentRequired {
                deactivated: true,
                ..
            }
        ));
        for b in [r#"{"detail":"Payment Required"}"#, "", "<html>402</html>"] {
            assert!(matches!(
                c(402, b),
                Outcome::PaymentRequired { deactivated: false, cooldown } if cooldown == Duration::from_secs(1_800)
            ));
        }
        for b in [
            r#"{"detail":"Not Found"}"#,
            "",
            r#"{"error":{"message":"route not found"}}"#,
        ] {
            assert!(
                matches!(c(404, b), Outcome::NotFound { cooldown, .. } if cooldown == Duration::from_secs(600)),
                "{b}"
            );
        }
        for b in [
            r#"{"error":{"code":"model_not_found","message":"x"}}"#,
            r#"{"detail":"The model 'gpt-x' does not exist or you do not have access to it."}"#,
            r#"{"detail":"The 'gpt-x' model is not supported when using Codex with a ChatGPT account."}"#,
            r#"{"error":{"type":"invalid_request_error","message":"Model gpt-x not found"}}"#,
        ] {
            assert!(
                matches!(c(404, b), Outcome::ModelUnsupported { retry_after: None }),
                "{b}"
            );
        }
        let mut h2 = HeaderMap::new();
        h2.insert(http::header::RETRY_AFTER, "30".parse().unwrap());
        assert!(matches!(
            classify_error(404, &h2, b"{}", &o),
            Outcome::NotFound { retry_after: Some(d), .. } if d == Duration::from_secs(30)
        ));
        assert_eq!(
            errors::upstream_code(402, &h, br#"{"detail":{"code":"deactivated_workspace"}}"#),
            "deactivated_workspace"
        );
        assert_eq!(
            errors::upstream_code(404, &h, br#"{"detail":"Not Found"}"#),
            "http_404"
        );
    }
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
    fn sse_types_survive_fragmented_crlf_and_multiline_data() {
        let wire = concat!(
            ": comment\r\nevent: response.failed\r\n",
            "data: {\"metadata\":{\"type\":\"response.failed\"},\r\n",
            "data: \"type\": \"response.output_text.delta\",\"delta\":\"response.completed\"}\r\n\r\n",
            "data: { \"type\": \"response.done\" }\n\n"
        );
        let mut pending = Vec::new();
        let mut kinds = Vec::new();
        for b in wire.bytes() {
            let search_from = pending.len().saturating_sub(3);
            pending.push(b);
            let (events, used) = sse_events_after(&pending, 0, search_from);
            for event in events {
                kinds.push(crate::usage::event_type(&event).unwrap().into_owned());
            }
            pending.drain(..used);
        }
        assert!(pending.is_empty());
        assert_eq!(kinds, ["response.output_text.delta", "response.done"]);
        let (events, _) = sse_events(b"data: {\"type\":\"response.completed\"}\n\n", 0);
        assert!(
            matches!(events[0], Cow::Borrowed(_)),
            "single data lines borrow the event buffer"
        );
    }
    #[test]
    fn overload_detection() {
        let k = |t: &str| event_kind(t);
        let ov = |t: &str| matches!(k(t), EventKind::Overloaded(_));
        assert!(ov(
            r#"{"type":"service_unavailable_error","code":"server_is_overloaded","message":"x"}"#
        ));
        assert!(ov(r#"{"type":"error","error":{"code":"slow_down"}}"#));
        assert!(ov(
            r#"{"type":"response.failed","response":{"error":{"code":"server_is_overloaded"}}}"#
        ));
        assert!(ov(
            r#"{"type":"error","error":{"message":"Selected model is at capacity."}}"#
        ));
        assert!(ov(
            r#"{"type":"error","error":{"type":"service_unavailable_error"}}"#
        ));
        // Never from output text, and an explicit other code wins over the message.
        assert!(!ov(
            r#"{"type":"response.output_text.delta","delta":"server is overloaded"}"#
        ));
        assert!(!ov(
            r#"{"type":"error","error":{"code":"context_length_exceeded","message":"overloaded"}}"#
        ));
        assert!(matches!(
            k(
                r#"{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"overloaded"}}"#
            ),
            EventKind::Quota(_)
        ));
        assert_eq!(
            overload_body(
                400,
                br#"{"error":{"message":"Our servers are currently overloaded."}}"#
            )
            .as_deref(),
            Some("server_is_overloaded")
        );
        assert_eq!(
            overload_body(
                503,
                br#"{"error":{"type":"server_error","code":"server_is_overloaded"}}"#
            )
            .as_deref(),
            Some("server_is_overloaded")
        );
        assert_eq!(overload_body(503, b"busy"), None);
        assert_eq!(
            overload_body(401, br#"{"error":{"message":"overloaded"}}"#),
            None
        );
        let mut h = HeaderMap::new();
        h.insert(
            "x-retry-metadata",
            HeaderValue::from_static("NO_MORE_RETRY"),
        );
        assert!(no_more_retry(&h));
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

    #[test]
    fn encrypted_rejection_kind() {
        let ev = r#"{"type":"error","error":{"message":"The encrypted content for item rs_1 could not be verified. Reason: Encrypted content could not be decrypted or parsed.","type":"invalid_request_error","param":"input","code":"invalid_encrypted_content"}}"#;
        assert!(
            matches!(event_kind(ev), EventKind::Encrypted(m) if m.contains("decrypted or parsed"))
        );
        // Not an overload / quota / content event.
        let v: serde_json::Value = serde_json::from_str(ev).unwrap();
        assert!(overload_code(&v).is_none() && !quota_event(&v).0);
    }

    #[test]
    fn bookkeeping_events_are_preamble() {
        let p = |t: &str| matches!(event_kind(t), EventKind::Preamble);
        assert!(p(
            r#"{"type":"response.metadata","response_id":"r1","metadata":{"openai_chatgpt_moderation_metadata":{"prompt":{}}}}"#
        ));
        assert!(p(
            r#"{"type":"response.queued","response":{"id":"r1","status":"queued"}}"#
        ));
        assert!(p(r#"{"type":"keepalive"}"#));
        // Real content / unknown events stay Content.
        assert!(!p(r#"{"type":"response.output_text.delta","delta":"hi"}"#));
        assert!(!p(r#"{"type":"unknown_event","x":1}"#));
    }

    #[test]
    fn aligner_rewrites_split_rejection_only() {
        let ev = concat!(
            "event: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"The encrypted content for item rs_1 could not be verified.\",",
            "\"type\":\"invalid_request_error\",\"param\":\"input\",\"code\":\"invalid_encrypted_content\"}}\n\n"
        );
        let head = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"invalid_encrypted_content\"}\r\n\r\n";
        let all = format!("{head}{ev}");
        for cut in [1, head.len() - 1, head.len() + 5, all.len() - 1] {
            let mut a = Aligner::new(true);
            let mut out = a.push(&all.as_bytes()[..cut]).to_vec();
            out.extend_from_slice(&a.push(&all.as_bytes()[cut..]));
            if let Some(r) = a.finish() {
                out.extend_from_slice(&r);
            }
            let out = String::from_utf8(out).unwrap();
            assert!(out.starts_with(head), "{cut}: {out}");
            assert!(
                !out.contains("could not be verified") && !out.contains("event: error"),
                "{out}"
            );
            assert!(
                out.contains("event: response.failed")
                    && out.contains(crate::encrypted::MESSAGE.split(' ').next().unwrap())
            );
        }
        // Partial events are held; whole ones flow unchanged; non-SSE is untouched.
        let mut a = Aligner::new(true);
        assert!(a.push(b"data: {\"a\":1}").is_empty());
        assert_eq!(&a.push(b"\n\ndata: x")[..], b"data: {\"a\":1}\n\n");
        assert_eq!(&a.finish().unwrap()[..], b"data: x");
        let mut a = Aligner::new(false);
        assert_eq!(&a.push(b"partial")[..], b"partial");
        assert!(a.finish().is_none());
    }
}
