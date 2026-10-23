//! Responses WebSocket: one downstream WS ↔ one upstream WS pinned to one account.
use crate::headers::upstream_headers;
use crate::usage::{endpoint_of, Tracker};
use crate::ws_upgrade::{DownWs, Upgrade};
use crate::{map_path, pq, proxy, session_route, AppState};
use axum::extract::{OriginalUri, State};
use axum::response::Response;
use ciallopa_core::{AccountId, Outcome, RouteError, RouteRequest};
use ciallopa_routing::{parse_quota, parse_quota_event_headers};
use ciallopa_upstream::drain::Phase;
use ciallopa_upstream::retry::{delay_for, is_retryable_status, switch_jitter};
use ciallopa_upstream::ws::handshake_status;
use ciallopa_upstream::{UpstreamWs, WsMessage};
use ciallopa_usage::Transport;
use futures::{SinkExt, StreamExt};
use http::{HeaderMap, StatusCode};
use std::time::Duration;
use tungstenite_close::{CloseCode, UpCloseFrame};

mod tungstenite_close {
    pub use ciallopa_upstream::ws_close::{CloseCode, CloseFrame as UpCloseFrame};
}

/// Close code used when the upstream account hit its limit mid-turn and the turn cannot be
/// replayed here (incremental `previous_response_id`, or output already sent). Mirrors CPA's
/// "upstream requires HTTP replay" (1012). Codex treats any Close before `response.completed`
/// as a retryable stream error, reconnects, and resends the full input (its websocket session
/// is reset on reconnect, so no `previous_response_id`).
const CLOSE_REPLAY: u16 = 1012;
const CLOSE_REPLAY_REASON: &str = "upstream requires HTTP replay";
const CLOSE_INTERNAL: u16 = 1011;

pub(crate) async fn handler(
    State(st): State<AppState>,
    key: crate::Key,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    lease: Option<axum::Extension<ciallopa_upstream::drain::Lease>>,
    ws: Upgrade,
) -> Response {
    let Some(path) = map_path(pq(&uri)) else {
        return proxy::json_error(StatusCode::NOT_FOUND, "not_found", "unknown path");
    };
    let mut route = session_route(&headers, None);
    let (up, resp_headers, account, gen) =
        match connect(&st, &path, &headers, &mut route, proxy::WS_HOLD_MAX).await {
            Ok(v) => v,
            Err(r) => {
                // Handshake failure: one usage row for the attempt.
                if let Some(mut t) = endpoint_of(&path)
                    .and_then(|ep| Tracker::start(&st, Transport::Ws, ep, crate::key_label(&key)))
                {
                    t.status(r.status().as_u16());
                    t.accounts(&route.exclude, None);
                    t.finish_response();
                    let (parts, body) = r.into_parts();
                    t.error_headers(&parts.headers);
                    if let Some(m) = parts.extensions.get::<crate::errors::ErrMeta>() {
                        t.meta_err(m);
                    }
                    let b = axum::body::to_bytes(body, 1 << 20)
                        .await
                        .unwrap_or_default();
                    t.error_body(&b);
                    return Response::from_parts(parts, axum::body::Body::from(b));
                }
                return r;
            }
        };
    let idle = Duration::from_secs(
        st.config
            .load()
            .upstream_opts
            .stream_idle_timeout_secs
            .max(1),
    );
    let st2 = st.clone();
    let ctx = Ctx {
        path,
        headers,
        key: crate::key_label(&key).map(str::to_string),
    };
    let hs_headers = resp_headers.clone();
    let compression = st.config.load().ws_compression;
    let drain = st.upstream.drain.clone();
    let mut resp = ws.on_upgrade(compression, move |socket| async move {
        let _lease = lease;
        finish_on_shutdown(
            drain,
            Duration::from_secs(2),
            relay(st2, ctx, route, account, socket, up, gen, idle, hs_headers),
        )
        .await;
    });
    for (k, v) in resp_headers.iter() {
        let n = k.as_str();
        if n.starts_with("sec-websocket-")
            || matches!(
                n,
                "connection"
                    | "upgrade"
                    | "content-length"
                    | "transfer-encoding"
                    // upstream cookies belong to the shared account session (same as HTTP)
                    | "set-cookie"
                    | "set-cookie2"
            )
        {
            continue;
        }
        resp.headers_mut().append(k.clone(), v.clone());
    }
    resp
}

/// The relay observes shutdown between frames, but a send/connect branch can itself be
/// stuck. Give it time to write the normal close frame, then drop it before the process
/// finishes its three-second close grace and flushes the usage writer.
async fn finish_on_shutdown(
    drain: std::sync::Arc<ciallopa_upstream::drain::Drain>,
    grace: Duration,
    relay: impl std::future::Future<Output = ()>,
) {
    tokio::pin!(relay);
    tokio::select! {
        _ = &mut relay => {}
        _ = drain.forced() => {
            if tokio::time::timeout(grace, &mut relay).await.is_err() {
                tracing::warn!("shutdown close grace reached; dropping blocked websocket relay");
            }
        }
    }
}

/// Handshake retries for transport errors / 408 / 5xx (Codex `request_max_retries` default).
const WS_HANDSHAKE_RETRIES: u64 = 4;

async fn connect(
    st: &AppState,
    path: &str,
    headers: &HeaderMap,
    route: &mut RouteRequest,
    hold_cap: Duration,
) -> Result<(UpstreamWs, HeaderMap, AccountId, u64), Response> {
    // Codex does not retry a failed WebSocket handshake at the request level (it gives up on the
    // turn or falls back to HTTPS), so this is the only retry layer for it: up to
    // WS_HANDSHAKE_RETRIES with backoff, on another usable account when there is one.
    let retries = WS_HANDSHAKE_RETRIES;
    let mut transient = 0u64;
    let mut overload_waited = false;
    let mut last: Option<Response> = None;
    let mut held = Duration::ZERO;
    loop {
        let accounts = st.accounts.list().await;
        let id = match st.router.select(route, &accounts) {
            Ok(s) => s.account,
            Err(RouteError::AllCoolingDown {
                retry_after,
                quota,
                earliest_quota,
            }) => {
                let c = proxy::Cooling::decide(st, retry_after, earliest_quota, held, hold_cap);
                if let proxy::Cooling::Hold(d) = c {
                    proxy::hold(d, &mut held).await;
                    continue;
                }
                return Err(last.unwrap_or_else(|| c.respond(st, retry_after, quota)));
            }
            Err(_) => {
                let code = if accounts.is_empty() {
                    "no_account"
                } else {
                    "no_usable_account"
                };
                crate::errors::warn_unroutable(code, accounts.len());
                return Err(last.unwrap_or_else(|| {
                    crate::errors::local(
                        StatusCode::SERVICE_UNAVAILABLE,
                        crate::errors::SRC_LOCAL,
                        code,
                        // No Retry-After: nothing will change by itself, so the client gives up after its
                        // short request retries instead of waiting out server advice on every reconnect.
                        None,
                    )
                }));
            }
        };
        let mut creds = match st.accounts.credentials(&id).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(account = %id, error = %e, "credentials unavailable");
                // IO / transient refresh failures are not a revoked token: skip without cooldown.
                if proxy::cools_on_credentials_error(&e) {
                    st.router.report(route, &id, &Outcome::AuthFailed);
                }
                route.exclude.push(id);
                continue;
            }
        };
        let mut refreshed = false;
        loop {
            route.attempt_at = Some(std::time::SystemTime::now());
            // Before the dial reads the egress options (see `egress_gen`).
            let gen = st.upstream.egress_gen();
            match st
                .upstream
                .ws_connect_as(
                    &id,
                    path,
                    upstream_headers(
                        &crate::turn_state::scope(headers, &id, !route.exclude.is_empty()),
                        &creds,
                    ),
                )
                .await
            {
                Ok((ws, resp)) => {
                    crate::turn_state::learn(resp.headers(), &id);
                    let quota = parse_quota(resp.headers());
                    st.router.report(route, &id, &Outcome::Success { quota });
                    return Ok((ws, resp.headers().clone(), id, gen));
                }
                Err(e) => {
                    let Some((status, h, body)) = handshake_status(&e) else {
                        let proxy_code = match &e {
                            ciallopa_upstream::UpstreamError::Proxy(_) => Some("proxy_connect"),
                            ciallopa_upstream::UpstreamError::ProxyTimeout => Some("proxy_timeout"),
                            _ => None,
                        };
                        let cause = crate::errors::chain(&e);
                        st.router
                            .report(route, &id, &Outcome::Transient { status: None });
                        if let Some(code) = proxy_code {
                            // Shared by every account: no switching, no retry here.
                            tracing::warn!(account = %id, error = %cause, code, "egress proxy error (ws)");
                            return Err(crate::errors::network(code));
                        }
                        tracing::warn!(account = %id, error = %cause, "upstream ws connect failed");
                        if transient >= retries {
                            return Err(crate::errors::network("connect"));
                        }
                        transient += 1;
                        if proxy::others_usable(st, &accounts, route, &id) {
                            route.exclude.push(id.clone());
                            tokio::time::sleep(switch_jitter()).await;
                        } else {
                            tokio::time::sleep(delay_for(transient, None)).await;
                        }
                        break;
                    };
                    let resp = {
                        let mut r = Response::new(axum::body::Body::from(body.clone()));
                        *r.status_mut() =
                            StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
                        *r.headers_mut() = crate::headers::downstream_headers(&h);
                        r.headers_mut().remove(http::header::CONTENT_LENGTH);
                        r.extensions_mut()
                            .insert(crate::errors::meta_upstream(status, &h, &body));
                        r
                    };
                    st.router.note_attempt(&id);
                    if let Some(code) = proxy::overload_body(status, &body) {
                        // Account-level throttling: try every other usable account once, no
                        // same-account retry; the last one hands the error to the client.
                        let others = proxy::others_usable(st, &accounts, route, &id);
                        st.router
                            .report(route, &id, &proxy::overload_outcome(st, others));
                        tracing::info!(account = %id, code = %code, "ws handshake overloaded");
                        let mut resp = resp;
                        if let Some(m) = resp.extensions_mut().get_mut::<crate::errors::ErrMeta>() {
                            m.code = code;
                        }
                        if !others {
                            if let Some(w) = proxy::overload_retry_wait(st, overload_waited, &h) {
                                overload_waited = true;
                                tracing::info!(account = %id, wait_ms = w.as_millis() as u64, "ws handshake overloaded on last usable account; waiting to retry once");
                                tokio::time::sleep(w).await;
                                continue;
                            }
                            return Err(resp);
                        }
                        route.exclude.push(id.clone());
                        last = Some(resp);
                        tokio::time::sleep(switch_jitter()).await;
                        break;
                    }
                    match status {
                        401 if !refreshed => {
                            refreshed = true;
                            if let Ok(c) = st.accounts.force_refresh(&id).await {
                                creds = c;
                                continue;
                            }
                            st.router.report(route, &id, &Outcome::AuthFailed);
                            route.exclude.push(id.clone());
                            last = Some(resp);
                            break;
                        }
                        401..=404 | 429 => {
                            let outcome = proxy::classify_error(
                                status,
                                &h,
                                &body,
                                &st.config.load().upstream_opts,
                            );
                            proxy::log_account_error(&id, &outcome);
                            st.router.report(route, &id, &outcome);
                            route.exclude.push(id.clone());
                            last = Some(resp);
                            break;
                        }
                        s if s == 408 || is_retryable_status(s) => {
                            let others = proxy::others_usable(st, &accounts, route, &id);
                            let secs = st.config.load().upstream_opts.transient_cooldown_secs;
                            let cooldown =
                                (others && secs > 0).then(|| std::time::Duration::from_secs(secs));
                            st.router.report(
                                route,
                                &id,
                                &Outcome::ServerError {
                                    status: s,
                                    cooldown,
                                },
                            );
                            // NO_MORE_RETRY forbids another try on this account only.
                            if transient >= retries || (!others && proxy::no_more_retry(&h)) {
                                return Err(resp);
                            }
                            transient += 1;
                            if others {
                                route.exclude.push(id.clone());
                                tokio::time::sleep(switch_jitter()).await;
                            } else {
                                tokio::time::sleep(delay_for(transient, Some(&h))).await;
                            }
                            break;
                        }
                        s => {
                            st.router
                                .report(route, &id, &Outcome::ClientError { status: s });
                            return Err(resp);
                        }
                    }
                }
            }
        }
    }
}

/// Both legs are tungstenite streams; raw frames are never relayed.
type Message = WsMessage;

fn to_up(m: Message) -> Option<WsMessage> {
    match m {
        WsMessage::Frame(_) => None,
        m => Some(m),
    }
}

fn to_down(m: WsMessage) -> Option<Message> {
    to_up(m)
}

fn close(code: u16, reason: &str) -> Message {
    Message::Close(Some(UpCloseFrame {
        code: CloseCode::from(code),
        reason: reason.into(),
    }))
}

/// What the relay needs to redial upstream on another account.
struct Ctx {
    path: String,
    headers: HeaderMap,
    /// Client key label (usage log).
    key: Option<String>,
}

/// Inspect an upstream text frame: (kind, request finished, quota snapshot outcome).
fn classify(text: &str) -> (proxy::EventKind, bool, Option<Outcome>) {
    let looks = text.contains("\"error\"")
        || text.contains("response.")
        || text.contains("codex.rate_limits");
    if !looks {
        return (proxy::EventKind::Content, false, None);
    }
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return (proxy::EventKind::Content, false, None);
    };
    let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
    let done = matches!(
        ty,
        "error"
            | "response.completed"
            | "response.failed"
            | "response.incomplete"
            | "response.done"
    );
    let snapshot = (ty == "codex.rate_limits")
        .then(|| parse_quota_event_headers(text.as_bytes()).and_then(|h| parse_quota(&h)))
        .flatten()
        .map(|q| Outcome::Success { quota: Some(q) });
    (proxy::event_kind(text), done, snapshot)
}

/// A client `response.create` that may be replayed on another account.
fn replayable(text: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(text).is_ok_and(|v| {
        v.get("type").and_then(|t| t.as_str()) == Some("response.create")
            && v.get("previous_response_id")
                .and_then(|p| p.as_str())
                .is_none_or(|p| p.trim().is_empty())
    })
}

#[allow(clippy::too_many_arguments)]
async fn relay(
    st: AppState,
    ctx: Ctx,
    mut route: RouteRequest,
    mut account: AccountId,
    down: DownWs,
    up: UpstreamWs,
    mut gen: u64,
    idle: Duration,
    handshake: HeaderMap,
) {
    // Egress proxy switch: the upstream WebSocket keeps the proxy it was dialed with, so once
    // the egress generation moves past `gen`, the session is closed (normal close, both legs)
    // between turns and the client reconnects through the new egress. A running turn finishes
    // on the old connection first. Not an error: no cooldown, no usage record.
    let mut egress = st.upstream.egress_watch();
    let mut egress_live = true;
    // Graceful shutdown: same rule (close between turns, after the running one), and after
    // the drain bound a 1001 close. A session opened during the drain sees it at once.
    let _session = st.upstream.drain.enter();
    let mut drain = st.upstream.drain.watch();
    drain.mark_changed();
    let mut drain_live = true;
    // Latest quota headers on this connection: handshake, then `codex.rate_limits` frames.
    let mut quota_h = handshake;
    let (mut dtx, mut drx) = down.split();
    let (mut utx, mut urx) = up.split();
    let max_switches = st.accounts.list().await.len().max(1);
    let mut switches = 0usize;
    let mut awaiting = false;
    // Current turn: the client request (if replayable), upstream preamble frames held back
    // until the first meaningful event, and whether anything meaningful was sent.
    let mut pending: Option<String> = None;
    let mut held: Vec<Message> = Vec::new();
    let mut started = false;
    // The single same-account overload wait-and-retry of the current turn is spent.
    let mut overload_waited = false;
    // Usage: one tracker per turn (client `response.create` → terminal event). Dropping it
    // emits the record, so every `break` below records the in-flight turn with its error.
    let endpoint = endpoint_of(&ctx.path);
    let mut turn: Option<Tracker> = None;
    let sleep = tokio::time::sleep(idle);
    tokio::pin!(sleep);
    'outer: loop {
        tokio::select! {
            m = drx.next() => match m {
                Some(Ok(m)) => {
                    let is_close = matches!(m, Message::Close(_));
                    if matches!(m, Message::Text(_) | Message::Binary(_)) {
                        if !awaiting {
                            sleep.as_mut().reset(tokio::time::Instant::now() + idle);
                            held.clear();
                            started = false;
                            overload_waited = false;
                        }
                        pending = match &m {
                            Message::Text(t) if !awaiting && replayable(t.as_str()) => Some(t.as_str().to_string()),
                            _ => None,
                        };
                        if !awaiting {
                            if let (Message::Text(t), Some(ep)) = (&m, endpoint) {
                                if t.as_str().contains("response.create") {
                                    st.router.note_attempt(&account);
                                    turn = Tracker::start(&st, Transport::Ws, ep, ctx.key.as_deref()).map(|mut tr| {
                                        let v = serde_json::from_str::<serde_json::Value>(t.as_str()).ok();
                                        tr.headers(&ctx.headers);
                                        tr.meta(v.as_ref());
                                        if v.as_ref().and_then(|v| v.get("generate")).and_then(|g| g.as_bool()) == Some(false) {
                                            tr.prewarm();
                                        }
                                        tr.accounts(&[], Some(&account));
                                        tr.quota(&quota_h);
                                        tr.status(200);
                                        tr
                                    });
                                }
                            }
                        }
                        awaiting = true;
                    }
                    if let Some(u) = to_up(m) {
                        if let Err(e) = utx.send(u).await {
                            // Client Close and forced shutdown retain their cancellation cause.
                            if !is_close && st.upstream.drain.phase() != Phase::Force {
                                if let Some(t) = turn.as_mut() {
                                    t.fail_msg("upstream_closed", format!("upstream websocket send failed (transport error: {e})"));
                                    t.source(crate::errors::SRC_NETWORK);
                                }
                            }
                            let _ = dtx.send(close(CLOSE_INTERNAL, "upstream gone")).await;
                            break;
                        }
                    }
                    if is_close {
                        if let Some(t) = turn.as_mut() { t.client_gone(); }
                        break;
                    }
                }
                _ => {
                    if let Some(t) = turn.as_mut() { t.client_gone(); }
                    let _ = utx.send(WsMessage::Close(None)).await;
                    break;
                }
            },
            m = urx.next() => {
                sleep.as_mut().reset(tokio::time::Instant::now() + idle);
                match m {
                    Some(Ok(WsMessage::Close(f))) => {
                        let code = f.as_ref().map(|f| u16::from(f.code)).unwrap_or(1000);
                        if let Some(t) = turn.as_mut() {
                            let reason = f.as_ref().map(|f| f.reason.to_string()).unwrap_or_default();
                            t.fail_msg("upstream_closed", format!("upstream websocket closed mid-turn (code {code}) {reason}").trim_end().to_string()); t.source(crate::errors::SRC_STREAM);
                        }
                        if code != 1000 {
                            st.router.report(&route, &account, &Outcome::Transient { status: None });
                        }
                        for h in held.drain(..) { let _ = dtx.send(h).await; }
                        if let Some(d) = to_down(WsMessage::Close(f)) {
                            let _ = dtx.send(d).await;
                        }
                        break;
                    }
                    Some(Ok(m)) => {
                        let (kind, done) = match &m {
                            WsMessage::Text(t) => {
                                let (k, d, snap) = classify(t.as_str());
                                if let Some(o) = snap {
                                    st.router.report(&route, &account, &o);
                                    if let Some(h) = parse_quota_event_headers(t.as_str().as_bytes()) {
                                        if let Some(tr) = turn.as_mut() { tr.quota(&h); }
                                        quota_h = h;
                                    }
                                }
                                (k, d)
                            }
                            _ => (proxy::EventKind::Preamble, false),
                        };
                        if let (proxy::EventKind::Quota(_), Some(t), WsMessage::Text(txt)) = (&kind, turn.as_mut(), &m) {
                            t.error_text(txt.as_str().as_bytes());
                        }
                        if let proxy::EventKind::Overloaded(code) = &kind {
                            let accounts = st.accounts.list().await;
                            let others = proxy::others_usable(&st, &accounts, &route, &account);
                            st.router.report(&route, &account, &proxy::overload_outcome(&st, others));
                            let fresh = awaiting && !started && pending.is_some();
                            // Only usable account left: wait once and replay on it (Codex does not
                            // retry an in-stream overload itself).
                            let wait = (fresh && !others)
                                .then(|| proxy::overload_retry_wait(&st, overload_waited, &HeaderMap::new()))
                                .flatten();
                            let can_replay = fresh && ((others && switches + 1 < max_switches.max(2)) || wait.is_some());
                            if let (true, Some(req)) = (can_replay, pending.clone()) {
                                let _ = utx.send(WsMessage::Close(None)).await;
                                if let Some(w) = wait {
                                    overload_waited = true;
                                    tracing::info!(account = %account, code = %code, wait_ms = w.as_millis() as u64, "ws overloaded before output on last usable account; waiting to retry once");
                                    tokio::time::sleep(w).await;
                                } else {
                                    switches += 1;
                                    route.exclude.push(account.clone());
                                }
                                match connect(&st, &ctx.path, &ctx.headers, &mut route, Duration::MAX).await {
                                    Ok((ws, h, id, g)) => {
                                        quota_h = h;
                                        gen = g;
                                        tracing::info!(from = %account, to = %id, code = %code, "ws overloaded before output; replayed");
                                        if let Some(t) = turn.as_mut() { t.switch_to(&id); t.quota(&quota_h); }
                                        account = id;
                                        let (t, r) = ws.split();
                                        let old_tx = std::mem::replace(&mut utx, t);
                                        let old_rx = std::mem::replace(&mut urx, r);
                                        tokio::spawn(drain_close(old_tx, old_rx));
                                        held.clear();
                                        st.router.note_attempt(&account);
                                        if utx.send(WsMessage::text(req.as_str())).await.is_ok() {
                                            continue 'outer;
                                        }
                                    }
                                    Err(_) => tracing::warn!("ws redial after overload failed"),
                                }
                                // Redial failed: the old socket is closed; let the client replay.
                                if let Some(t) = turn.as_mut() { t.fail_msg(code, "upstream overloaded; redial failed, closed for client replay"); t.source(crate::errors::SRC_STREAM); }
                                let _ = dtx.send(close(CLOSE_REPLAY, CLOSE_REPLAY_REASON)).await;
                                break;
                            }
                            // Output already sent, or no other account: pass it through verbatim.
                            if let Some(t) = turn.as_mut() {
                                if let WsMessage::Text(txt) = &m { t.error_text(txt.as_str().as_bytes()); }
                                t.fail_msg(code, "upstream overloaded");
                                t.source(crate::errors::SRC_STREAM);
                            }
                        }
                        if let proxy::EventKind::Quota(o) = kind {
                            // Never forward: Codex would end the turn (UsageLimitReached is fatal).
                            st.router.report(&route, &account, &o);
                            let _ = utx.send(WsMessage::Close(None)).await;
                            switches += 1;
                            if awaiting && !started && switches < max_switches {
                                if let Some(req) = pending.clone() {
                                    route.exclude.push(account.clone());
                                    match connect(&st, &ctx.path, &ctx.headers, &mut route, Duration::MAX).await {
                                        Ok((ws, h, id, g)) => {
                                            quota_h = h;
                                            gen = g;
                                            tracing::info!(from = %account, to = %id, "ws quota error before output; replayed on another account");
                                            if let Some(t) = turn.as_mut() { t.switch_to(&id); t.quota(&quota_h); }
                                            account = id;
                                            let (t, r) = ws.split();
                                            // Finish the old socket's close handshake in the
                                            // background so the replay is not delayed.
                                            let old_tx = std::mem::replace(&mut utx, t);
                                            let old_rx = std::mem::replace(&mut urx, r);
                                            tokio::spawn(drain_close(old_tx, old_rx));
                                            held.clear();
                                            if utx.send(WsMessage::text(req.as_str())).await.is_ok() {
                                                continue 'outer;
                                            }
                                        }
                                        Err(_) => tracing::warn!("ws redial after quota error failed"),
                                    }
                                }
                            }
                            tracing::info!(account = %account, "ws quota error mid-turn; closing for client replay");
                            if let Some(t) = turn.as_mut() { t.fail_msg("usage_limit", "usage limit reached mid-turn; closed for client replay"); t.source(crate::errors::SRC_STREAM); }
                            let _ = dtx.send(close(CLOSE_REPLAY, CLOSE_REPLAY_REASON)).await;
                            break;
                        }
                        if let (Some(t), WsMessage::Text(txt)) = (turn.as_mut(), &m) {
                            if !done && proxy::memfind(txt.as_bytes(), b"\"response.created\"") {
                                if let Ok(v) = serde_json::from_str::<serde_json::Value>(txt.as_str()) {
                                    if v.get("type").and_then(|t| t.as_str()) == Some("response.created") {
                                        t.created_event(&v);
                                    }
                                }
                            }
                        }
                        if let (Some(t), WsMessage::Text(txt)) = (turn.as_mut(), &m) {
                            t.output_event(txt.as_str().as_bytes());
                        }
                        let preamble = matches!(kind, proxy::EventKind::Preamble) && !done;
                        if awaiting && !started && preamble {
                            if let Some(d) = to_down(m) {
                                let add = match &d {
                                    Message::Text(t) => t.len(),
                                    Message::Binary(b) => b.len(),
                                    _ => 0,
                                };
                                let held_bytes: usize = held.iter().map(|h| match h {
                                    Message::Text(t) => t.len(),
                                    Message::Binary(b) => b.len(),
                                    _ => 0,
                                }).sum();
                                // Same cap as HTTP bootstrap: unbounded bookkeeping must not
                                // grow the hold forever. Over the cap we commit and flush.
                                if held_bytes + add > proxy::BOOTSTRAP_MAX {
                                    held.push(d);
                                    started = true;
                                    for h in held.drain(..) {
                                        if dtx.send(h).await.is_err() { break 'outer; }
                                    }
                                    continue;
                                }
                                held.push(d);
                            }
                            continue;
                        }
                        if awaiting {
                            if !started && matches!(kind, proxy::EventKind::Content) && !done {
                                st.router.content_ok(&account);
                            }
                            started = true;
                        }
                        for h in held.drain(..) {
                            if dtx.send(h).await.is_err() { break 'outer; }
                        }
                        if done {
                            awaiting = false;
                            pending = None;
                            if let (Some(mut t), WsMessage::Text(txt)) = (turn.take(), &m) {
                                if let Ok(v) = serde_json::from_str::<serde_json::Value>(txt.as_str()) {
                                    t.terminal_event(&v);
                                }
                                // `t` dropped here → record emitted.
                            }
                        }
                        // Encrypted-content rejection: same frame, clear message (logged above
                        // with the upstream one). No account outcome: the request is at fault.
                        let m = match (&kind, m) {
                            (proxy::EventKind::Encrypted(_), WsMessage::Text(txt)) => {
                                tracing::info!(account = %account, "upstream rejected the encrypted content in the request history");
                                match serde_json::from_str::<serde_json::Value>(txt.as_str()) {
                                    Ok(v) => WsMessage::text(crate::encrypted::ws_frame(&v)),
                                    Err(_) => WsMessage::Text(txt),
                                }
                            }
                            (_, m) => m,
                        };
                        if let Some(d) = to_down(m) {
                            if dtx.send(d).await.is_err() {
                                let _ = utx.send(WsMessage::Close(None)).await;
                                break;
                            }
                        }
                        if done && st.upstream.egress_gen() != gen {
                            close_for_egress(&account, &mut dtx, &mut utx).await;
                            break;
                        }
                        if done && st.upstream.drain.phase() != Phase::Running {
                            close_for_shutdown(&account, 1000, &mut dtx, &mut utx).await;
                            break;
                        }
                    }
                    other => {
                        let cause = match &other {
                            Some(Err(e)) => e.to_string(),
                            _ => "connection ended without a close frame".to_string(),
                        };
                        if awaiting {
                            tracing::warn!(account = %account, record_id = turn.as_ref().map(Tracker::record_id), error = %cause, "upstream websocket transport error mid-turn");
                        } else {
                            tracing::info!(account = %account, error = %cause, "upstream websocket transport error between turns");
                        }
                        if let Some(t) = turn.as_mut() { t.fail_msg("upstream_closed", format!("upstream websocket disconnected (transport error: {cause})")); t.source(crate::errors::SRC_NETWORK); }
                        st.router.report(&route, &account, &Outcome::Transient { status: None });
                        let _ = dtx.send(close(CLOSE_INTERNAL, "upstream disconnected")).await;
                        break;
                    }
                }
            },
            r = egress.changed(), if egress_live => {
                if r.is_err() {
                    egress_live = false;
                } else if !awaiting && *egress.borrow_and_update() != gen {
                    close_for_egress(&account, &mut dtx, &mut utx).await;
                    break;
                }
            }
            r = drain.changed(), if drain_live => {
                let phase = *drain.borrow_and_update();
                if r.is_err() {
                    drain_live = false;
                } else if phase == Phase::Force || (phase == Phase::Draining && !awaiting) {
                    if awaiting {
                        if let Some(t) = turn.as_mut() { t.fail_msg("shutdown", "closed by server shutdown before the turn finished".to_string()); t.source(crate::errors::SRC_LOCAL); }
                    }
                    close_for_shutdown(&account, if phase == Phase::Force { 1001 } else { 1000 }, &mut dtx, &mut utx).await;
                    break;
                }
            }
            _ = &mut sleep, if awaiting => {
                tracing::warn!(account = %account, record_id = turn.as_ref().map(Tracker::record_id), "upstream websocket idle timeout");
                if let Some(t) = turn.as_mut() { t.fail_msg("idle_timeout", format!("upstream websocket idle for {}s (timeout)", idle.as_secs())); t.source(crate::errors::SRC_NETWORK); }
                st.router.report(&route, &account, &Outcome::Transient { status: None });
                let _ = dtx.send(close(CLOSE_INTERNAL, "stream idle timeout")).await;
                let _ = utx.send(WsMessage::Close(None)).await;
                break;
            }
        }
    }
    // Finish both closing handshakes: flush the close frame (tungstenite queues the reply to a
    // peer Close; without this the TCP stream is dropped with "reset without closing
    // handshake"). Bounded so a dead peer cannot hold the task.
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        let _ = dtx.close().await;
        let _ = utx.close().await;
    })
    .await;
}

/// Between turns, after an egress proxy change: normal close on both legs (the client
/// reconnects on its next request, through the new egress).
async fn close_for_egress<D, U>(account: &AccountId, dtx: &mut D, utx: &mut U)
where
    D: futures::Sink<Message> + Unpin,
    U: futures::Sink<WsMessage> + Unpin,
{
    tracing::info!(account = %account, "egress proxy changed; closing the idle websocket session so the client reconnects through the new egress");
    let _ = dtx.send(close(1000, "")).await;
    let _ = utx.send(WsMessage::Close(None)).await;
}

/// Server shutdown: 1000 between turns (after the running one), 1001 when the drain bound
/// cut a turn short.
async fn close_for_shutdown<D, U>(account: &AccountId, code: u16, dtx: &mut D, utx: &mut U)
where
    D: futures::Sink<Message> + Unpin,
    U: futures::Sink<WsMessage> + Unpin,
{
    if code == 1000 {
        tracing::info!(account = %account, "shutting down; closing the websocket session between turns");
    } else {
        tracing::warn!(account = %account, "shutting down; drain bound reached, closing the websocket session mid-turn");
    }
    let _ = dtx.send(close(code, "")).await;
    let _ = utx.send(WsMessage::Close(None)).await;
}

/// Wait (bounded) for the peer's Close after we sent ours, then drop both halves.
async fn drain_close<T, R, E>(tx: T, mut rx: R)
where
    R: futures::Stream<Item = Result<WsMessage, E>> + Unpin,
{
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    while let Ok(Some(Ok(m))) = tokio::time::timeout_at(deadline, rx.next()).await {
        if matches!(m, WsMessage::Close(_)) {
            break;
        }
    }
    drop(tx);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn force_releases_a_relay_stuck_inside_a_send_or_connect() {
        let drain = std::sync::Arc::new(ciallopa_upstream::drain::Drain::default());
        let session = drain.enter();
        let lease = drain.admit(1).expect("one available slot");
        let (started, ready) = tokio::sync::oneshot::channel();
        let blocked = async move {
            let (_session, _lease) = (session, lease);
            let _ = started.send(());
            std::future::pending::<()>().await;
        };
        let task = tokio::spawn(finish_on_shutdown(
            drain.clone(),
            Duration::from_millis(20),
            blocked,
        ));
        ready.await.expect("relay entered its blocked branch");
        drain.force();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("force must bound a branch that cannot observe drain itself")
            .expect("relay task");
        assert_eq!(drain.inflight(), 0);
        assert_eq!(drain.active(), 0);
    }

    #[test]
    fn classifies() {
        let (k, d, _) = classify(
            r#"{"type":"error","status":429,"error":{"type":"usage_limit_reached","resets_in_seconds":60},"headers":{"x-codex-primary-used-percent":"100"}}"#,
        );
        assert!(d);
        match k {
            proxy::EventKind::Quota(Outcome::RateLimited {
                usage_limit,
                retry_after,
                quota,
            }) => {
                assert!(usage_limit);
                assert_eq!(retry_after, Some(Duration::from_secs(60)));
                assert!(quota.is_some());
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            classify(
                r#"{"type":"response.failed","response":{"error":{"code":"insufficient_quota"}}}"#
            )
            .0,
            proxy::EventKind::Quota(_)
        ));
        assert!(classify(r#"{"type":"response.completed"}"#).1);
        assert_eq!(
            classify(r#"{"type":"response.created"}"#).0,
            proxy::EventKind::Preamble
        );
        assert_eq!(
            classify(r#"{"type":"response.output_text.delta"}"#).0,
            proxy::EventKind::Content
        );
        assert!(replayable(r#"{"type":"response.create","input":[]}"#));
        assert!(!replayable(
            r#"{"type":"response.create","previous_response_id":"r1"}"#
        ));
    }
}
