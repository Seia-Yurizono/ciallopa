use crate::classify::{classify_error, log_account_error, overload_body};
use crate::errors;
use crate::headers::{downstream_headers, upstream_headers};
use crate::limits::{read_body, BODY_TIMEOUT, ERROR_BODY_LIMIT};
use crate::timing;
use crate::usage::Tracker;
use crate::AppState;
use axum::body::Body;
use axum::response::Response;
use bytes::Bytes;
use ciallopa_core::{AccountId, AuthError, Outcome, RouteError, RouteRequest};
use ciallopa_routing::{is_usage_limit, parse_quota};
use ciallopa_upstream::retry::is_retryable_status;
use http::{HeaderMap, Method, StatusCode};
use std::time::{Duration, SystemTime};

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
    /// Direct Responses HTTP requests return identity downstream, even if gzip negotiation
    /// is disabled. Other routes retain their existing passthrough policy.
    pub decode_response: bool,
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
pub(crate) struct Preparation {
    budget: crate::budget::PreparationBudget,
    header_timeout: Duration,
    pub(crate) idle: Duration,
}

impl Preparation {
    pub(crate) fn new(st: &AppState) -> Self {
        let timing = timing::Timing::capture(&st.config.load());
        Self {
            budget: crate::budget::PreparationBudget::new(timing.preparation),
            header_timeout: timing.header,
            idle: timing.idle,
        }
    }

    pub(crate) async fn run<T>(
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
        let budget = timing::Timing::capture(&st.config.load()).cooldown_wait;
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

pub(crate) async fn send_info(
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
        let accounts = st.accounts.accounts();
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
                tracing::warn!(account = %ciallopa_core::a(&id), error = %e, "credentials unavailable");
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
                        tracing::warn!(account = %ciallopa_core::a(&id), error = %cause, code, "egress proxy error");
                    } else {
                        tracing::warn!(account = %ciallopa_core::a(&id), error = %cause, code, "upstream transport error");
                    }
                    last_net = Some(code);
                    let mut a = errors::attempt(&id, "transport");
                    a.code = Some(code.into());
                    a.body = Some(cause.chars().take(300).collect());
                    info.attempts.push(a);
                    // Network errors never cool the account.
                    st.router.report(&route, &id, &Outcome::Transport);
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
                    tokio::time::sleep(timing::switch_jitter(st)).await;
                    break;
                }
            };
            st.upstream.store_cookies(&id, &url, r.headers());
            st.router.note_attempt(&id);
            let status = r.status().as_u16();
            info.active_response = Some((status, errors::request_id(r.headers())));
            let mut a = errors::attempt(&id, "http");
            a.status = Some(status);
            let r = if ob.decode_response {
                match crate::response_body::prepare(r) {
                    Ok(r) => r,
                    Err(e) => {
                        a.code = Some("unsupported_response_encoding".into());
                        a.action = "stop".into();
                        info.attempts.push(a);
                        let mut response = errors::local(
                            StatusCode::BAD_GATEWAY,
                            errors::SRC_UPSTREAM,
                            "unsupported_response_encoding",
                            None,
                        );
                        if let Some(meta) = response.extensions_mut().get_mut::<errors::ErrMeta>() {
                            meta.upstream_status = Some(status);
                            meta.upstream_request_id =
                                info.active_response.as_ref().and_then(|x| x.1.clone());
                            meta.message = Some(e.to_string());
                        }
                        return Err(response);
                    }
                }
            } else {
                r
            };
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
                        tracing::warn!(account = %ciallopa_core::a(&id), status, code = failure.code(false), "upstream error body incomplete; stopping request");
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
                            tracing::info!(account = %ciallopa_core::a(&id), wait_ms = w.as_millis() as u64, "overloaded on last usable account; waiting to retry once");
                            tokio::time::sleep(w).await;
                            continue;
                        }
                        info.attempts.act(format!("{tag}stop"));
                        return Err(best.or(unreachable_response));
                    }
                    info.attempts.act(format!("{tag}switch"));
                    route.exclude.push(id.clone());
                    tokio::time::sleep(timing::switch_jitter(st)).await;
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
                            tracing::warn!(account = %ciallopa_core::a(&id), error = %e, "force refresh failed");
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
                s if is_retryable_status(s) => {
                    let h = r.headers().clone();
                    let b = r.bytes().await.unwrap_or_default();
                    let others = others_usable(st, &accounts, &route, &id);
                    let ra = h
                        .get(http::header::RETRY_AFTER)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| {
                            ciallopa_core::retry_after::parse(
                                v,
                                SystemTime::now(),
                                ciallopa_core::retry_after::Format::Http,
                            )
                            .map(|d| d.min(ciallopa_routing::quota::MAX_HEADER_RETRY_AFTER))
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
                    tokio::time::sleep(timing::switch_jitter(st)).await;
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
pub(crate) fn rebuild(status: u16, h: &HeaderMap, b: Bytes) -> reqwest::Response {
    let mut r = http::Response::new(b);
    *r.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    *r.headers_mut() = h.clone();
    reqwest::Response::from(r)
}

fn unreachable_response() -> Response {
    errors::local(StatusCode::BAD_GATEWAY, errors::SRC_LOCAL, "internal", None)
}

/// Whether a `credentials()` failure should cool the account down (shared by HTTP and WS).
/// Disk IO and transient refresh failures (network / auth 5xx) are not a bad token.
pub(crate) fn cools_on_credentials_error(e: &AuthError) -> bool {
    !matches!(e, AuthError::Io(_) | AuthError::Refresh(_))
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
    timing::overload_wait(st, base, h)
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
}

#[cfg(test)]
mod quota_tests {
    use super::*;

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
