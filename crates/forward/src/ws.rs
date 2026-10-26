//! Responses WebSocket: one downstream WS ↔ one upstream WS pinned to one account.
use crate::budget::PreparationBudget;
use crate::headers::upstream_headers;
use crate::usage::{endpoint_of, StreamObservation, Tracker};
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
const CLOSE_TOO_BIG: u16 = 1009;

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
    // Each client connection can buffer several copies of its largest message before the
    // relay can count it in the body budget: cap them (`ws_max_connections`) before dialing.
    let (ws_limit, max_message) = {
        let cfg = st.config.load();
        let mib = usize::try_from(cfg.ws_max_message_mb).unwrap_or(usize::MAX);
        (cfg.ws_max_connections, mib.saturating_mul(1 << 20))
    };
    let slot = match st.upstream.drain.ws_slot(ws_limit) {
        Ok(s) => s,
        Err(r) => {
            crate::refusal::note(
                &st,
                crate::refusal::Refusal {
                    kind: ciallopa_upstream::drain::RefusalKind::Ws,
                    ws: true,
                    key: crate::key_label(&key),
                    path: uri.path(),
                    headers: &headers,
                    model: None,
                    account: None,
                    limit: ws_limit as u64,
                    bytes: None,
                },
            );
            return crate::admission::refused(r);
        }
    };
    let mut route = session_route(&headers, None);
    // Preparing a connection can be cancelled before any upstream response exists.
    // Keep the selected account and start time in the ordinary drop-safe tracker.
    let mut handshake = endpoint_of(&path).and_then(|ep| {
        Tracker::start(&st, Transport::Ws, ep, crate::key_label(&key)).map(|mut t| {
            t.headers(&headers);
            t
        })
    });
    // Keep these limits for this connection, including later turns and replays.
    let (preparation_limit, idle) = {
        let cfg = st.config.load();
        (
            Duration::from_secs(cfg.upstream_opts.preparation_timeout_secs),
            Duration::from_secs(cfg.upstream_opts.stream_idle_timeout_secs.max(1)),
        )
    };
    let mut budget = PreparationBudget::new(preparation_limit);
    let (up, resp_headers, account, gen) = match budget
        .run(connect(
            &st,
            &path,
            &headers,
            &mut route,
            proxy::WS_HOLD_MAX,
            handshake.as_mut(),
        ))
        .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(r)) | Err(r) => {
            // One row for the failed handshake, including preparation and all retries.
            if let Some(mut t) = handshake {
                t.status(r.status().as_u16());
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
    if let Some(t) = handshake {
        t.discard();
    }
    let st2 = st.clone();
    let ctx = Ctx {
        path,
        headers,
        key: crate::key_label(&key).map(str::to_string),
        preparation_limit,
    };
    let hs_headers = resp_headers.clone();
    let compression = st.config.load().ws_compression;
    let drain = st.upstream.drain.clone();
    let mut resp = ws.on_upgrade(compression, max_message, move |socket| async move {
        let _slot = slot;
        let handoff = lease.map(|axum::Extension(l)| l);
        finish_on_shutdown(
            drain,
            Duration::from_secs(2),
            relay(
                st2, ctx, route, account, socket, up, gen, idle, hs_headers, handoff,
            ),
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
    mut tracker: Option<&mut Tracker>,
) -> Result<(UpstreamWs, HeaderMap, AccountId, u64), Response> {
    // This gateway's independent handshake retry budget can switch usable accounts.
    // Codex 0.163/0.164 also has outer stream recovery and HTTP fallback; this is
    // not the only retry layer, nor a bound on the client's entire turn.
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
        if let Some(t) = tracker.as_deref_mut() {
            t.accounts(&route.exclude, Some(&id));
        }
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
                    st.egress.ok();
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
                            if let Some(blk) = crate::egress::detect(status, &h, &body) {
                                st.router.report(route, &id, &Outcome::EgressBlocked);
                                st.egress.note(&blk, status, &h, &id);
                                return Err(crate::egress::response(&blk, status, &h));
                            }
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

/// Answer to a client message refused by the buffered-body budget: the same 503
/// `gateway_busy` wording and `Retry-After` as HTTP ([`crate::admission::BUSY_RETRY_AFTER_SECS`]),
/// as an upstream-style wrapped error event (Codex maps it to a retryable HTTP 503). The
/// message is not forwarded and no turn starts; the connection stays open.
fn busy_frame() -> Message {
    let secs = crate::admission::BUSY_RETRY_AFTER_SECS;
    Message::text(
        serde_json::json!({
            "type": "error",
            "status": 503,
            "error": {"type": "gateway_busy", "message": "Gateway is busy; retry shortly."},
            "headers": {
                "retry-after": secs.to_string(),
                "retry-after-ms": (secs.saturating_mul(1000)).to_string(),
            },
        })
        .to_string(),
    )
}

/// A local error response (e.g. every account cooling down for the turn's model) as the
/// upstream-style wrapped error event, same shape as [`busy_frame`]. Records it on `turn`.
async fn error_event(resp: Response, turn: Option<Tracker>) -> Message {
    let (parts, body) = resp.into_parts();
    let b = axum::body::to_bytes(body, 1 << 20)
        .await
        .unwrap_or_default();
    if let Some(mut t) = turn {
        t.status(parts.status.as_u16());
        t.finish_response();
        t.error_headers(&parts.headers);
        if let Some(m) = parts.extensions.get::<crate::errors::ErrMeta>() {
            t.meta_err(m);
        }
        t.error_body(&b);
    }
    let error = serde_json::from_slice::<serde_json::Value>(&b)
        .ok()
        .and_then(|v| v.get("error").cloned())
        .unwrap_or_else(
            || serde_json::json!({"type": "unavailable", "message": "Service unavailable."}),
        );
    let mut headers = serde_json::Map::new();
    for k in ["retry-after", "retry-after-ms"] {
        if let Some(v) = parts.headers.get(k).and_then(|v| v.to_str().ok()) {
            headers.insert(k.to_string(), v.into());
        }
    }
    let mut ev =
        serde_json::json!({"type": "error", "status": parts.status.as_u16(), "error": error});
    if !headers.is_empty() {
        ev["headers"] = serde_json::Value::Object(headers);
    }
    Message::text(ev.to_string())
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
    preparation_limit: Duration,
}

/// Inspect an upstream text frame: (kind, request finished, quota snapshot outcome).
#[cfg(test)]
fn classify(text: &str) -> (proxy::EventKind, bool, Option<Outcome>) {
    classify_observed(text, &mut StreamObservation::new())
}

fn classify_observed(
    text: &str,
    progress: &mut StreamObservation,
) -> (proxy::EventKind, bool, Option<Outcome>) {
    let looks = text.contains("\"error\"")
        || text.contains("response.")
        || text.contains("codex.rate_limits");
    if !looks {
        if let Some(ty) = crate::usage::event_type(text) {
            progress.event(&ty);
        }
        return (proxy::EventKind::Content, false, None);
    }
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return (proxy::EventKind::Content, false, None);
    };
    let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
    if v.get("type").and_then(|t| t.as_str()).is_some() {
        progress.event(ty);
    }
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
/// Top-level fields of a client message that replay and usage read. `input`, `tools` and
/// other large fields are skipped while parsing, so a large message is not materialised
/// again as a JSON tree.
#[derive(serde::Deserialize)]
struct Head {
    #[serde(rename = "type")]
    ty: Option<serde_json::Value>,
    previous_response_id: Option<serde_json::Value>,
    generate: Option<serde_json::Value>,
    service_tier: Option<serde_json::Value>,
    client_metadata: Option<serde_json::Value>,
    model: Option<serde_json::Value>,
    reasoning: Option<serde_json::Value>,
}

impl Head {
    fn parse(text: &str) -> Option<Head> {
        serde_json::from_str(text).ok()
    }

    fn to_value(&self) -> serde_json::Value {
        let mut m = serde_json::Map::new();
        for (k, v) in [
            ("type", &self.ty),
            ("previous_response_id", &self.previous_response_id),
            ("generate", &self.generate),
            ("service_tier", &self.service_tier),
            ("client_metadata", &self.client_metadata),
            ("model", &self.model),
            ("reasoning", &self.reasoning),
        ] {
            if let Some(v) = v {
                m.insert(k.to_string(), v.clone());
            }
        }
        serde_json::Value::Object(m)
    }
}

fn replayable(text: &str) -> bool {
    Head::parse(text).is_some_and(|h| {
        h.ty.as_ref().and_then(|t| t.as_str()) == Some("response.create")
            && h.previous_response_id
                .as_ref()
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
    handoff: Option<ciallopa_upstream::drain::Lease>,
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
    // The handshake's (exempt) admission ends once the session is counted. An idle
    // connection holds no inflight slot; each turn takes one (`turn_slot`).
    drop(handoff);
    let mut turn_slot: Option<ciallopa_upstream::drain::Lease> = None;
    let mut drain = st.upstream.drain.watch();
    drain.mark_changed();
    let mut drain_live = true;
    // Latest quota headers on this connection: handshake, then `codex.rate_limits` frames.
    let mut upstream_request_id = crate::errors::request_id(&handshake);
    let mut quota_h = handshake;
    let mut progress = StreamObservation::new();
    let (mut dtx, mut drx) = down.split();
    let (mut utx, mut urx) = up.split();
    let mut queued_down = None;
    let mut abandon_upstream = false;
    let max_switches = st.accounts.list().await.len().max(1);
    let mut switches = 0usize;
    let mut awaiting = false;
    // Current turn: the client request (if replayable), upstream preamble frames held back
    // until the first meaningful event, and whether anything meaningful was sent.
    // Shares the message's buffer (no copy).
    let mut pending: Option<tokio_tungstenite::tungstenite::Utf8Bytes> = None;
    // Budget reservation for the latest client message, held while it (or `pending`, its
    // replay copy) is in memory.
    let mut msg_permit: Option<ciallopa_upstream::drain::BodyPermit> = None;
    let mut held: Vec<Message> = Vec::new();
    let mut started = false;
    // The single same-account overload wait-and-retry of the current turn is spent.
    let mut overload_waited = false;
    let mut preparation = PreparationBudget::new(ctx.preparation_limit);
    // Usage: one tracker per turn (client `response.create` → terminal event). Dropping it
    // emits the record, so every `break` below records the in-flight turn with its error.
    let endpoint = endpoint_of(&ctx.path);
    let mut turn: Option<Tracker> = None;
    let sleep = tokio::time::sleep(idle);
    tokio::pin!(sleep);
    'outer: loop {
        tokio::select! {
            m = next_downstream(&mut drx, &mut queued_down) => match m {
                Some(Ok(m)) => {
                    let size = match &m {
                        Message::Text(t) => Some(t.len()),
                        Message::Binary(b) => Some(b.len()),
                        _ => None,
                    };
                    if let Some(n) = size {
                        // This message replaces the previous one and its replay copy.
                        drop(msg_permit.take());
                        match crate::reqbody::reserve_message(&st, n).await {
                            Some(p) => msg_permit = Some(p),
                            None => {
                                let budget = st.config.load().max_buffered_body_mb.saturating_mul(1 << 20);
                                crate::refusal::note(&st, crate::refusal::Refusal {
                                    kind: ciallopa_upstream::drain::RefusalKind::WsMsg,
                                    ws: true,
                                    key: ctx.key.as_deref(),
                                    path: &ctx.path,
                                    headers: &ctx.headers,
                                    model: Some(route.model.as_str()).filter(|m| !m.is_empty()),
                                    account: Some(&account.to_string()),
                                    limit: budget,
                                    bytes: Some(n as u64),
                                });
                                if dtx.send(busy_frame()).await.is_err() { break; }
                                continue 'outer;
                            }
                        }
                    }
                    let is_close = matches!(m, Message::Close(_));
                    if matches!(m, Message::Text(_) | Message::Binary(_)) && !awaiting {
                        // A turn holds one `max_inflight_requests` slot from its client
                        // request to its terminal event (or cancel / close).
                        let limit = st.config.load().max_inflight_requests;
                        match st.upstream.drain.admit(limit) {
                            Ok(l) => turn_slot = Some(l),
                            Err(ciallopa_upstream::drain::Refused::Busy) => {
                                let model = match &m {
                                    Message::Text(t) => Head::parse(t.as_str()).and_then(|h| h.model.and_then(|v| v.as_str().map(str::to_string))),
                                    _ => None,
                                };
                                crate::refusal::note(&st, crate::refusal::Refusal {
                                    kind: ciallopa_upstream::drain::RefusalKind::Inflight,
                                    ws: true,
                                    key: ctx.key.as_deref(),
                                    path: &ctx.path,
                                    headers: &ctx.headers,
                                    model: model.as_deref().filter(|m| !m.is_empty()),
                                    account: Some(&account.to_string()),
                                    limit: limit as u64,
                                    bytes: None,
                                });
                                drop(msg_permit.take());
                                if dtx.send(busy_frame()).await.is_err() { break; }
                                continue 'outer;
                            }
                            Err(ciallopa_upstream::drain::Refused::Draining) => {
                                drop(msg_permit.take());
                                close_for_shutdown(&account, 1000, &mut dtx, &mut utx).await;
                                break;
                            }
                        }
                    }
                    if matches!(m, Message::Text(_) | Message::Binary(_)) {
                        if !awaiting {
                            progress = StreamObservation::new();
                            sleep.as_mut().reset(tokio::time::Instant::now() + idle);
                            held.clear();
                            started = false;
                            overload_waited = false;
                            preparation = PreparationBudget::new(ctx.preparation_limit);
                        }
                        pending = match &m {
                            Message::Text(t) if !awaiting && replayable(t.as_str()) => Some(t.clone()),
                            _ => None,
                        };
                        if !awaiting {
                            if let Message::Text(t) = &m {
                                if t.as_str().contains("response.create") {
                                    let head = Head::parse(t.as_str());
                                    // The handshake carries no model: each turn's model scopes this
                                    // connection's routing reports and cooldown checks.
                                    if let Some(model) = head.as_ref().and_then(|h| h.model.as_ref()).and_then(|v| v.as_str()).filter(|v| !v.is_empty()) {
                                        route.model = model.to_string();
                                    }
                                    turn = endpoint.and_then(|ep| Tracker::start(&st, Transport::Ws, ep, ctx.key.as_deref())).map(|mut tr| {
                                        let v = head.as_ref().map(Head::to_value);
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
                                    if !route.model.is_empty() && st.router.model_cooling_until(&account, &route.model).is_some() {
                                        // This account is cooling down for the turn's model (HTTP would
                                        // not pick it): move to an account that may serve it, or wait
                                        // out a short cooldown, before anything is sent upstream.
                                        if !replayable(t.as_str()) {
                                            // Its `previous_response_id` lives on this connection only.
                                            tracing::info!(account = %account, model = %route.model, "ws turn model cooling on this account; closing for client replay");
                                            if let Some(tr) = turn.as_mut() { tr.fail_msg("model_cooldown", "account cooling down for this model; closed for client replay"); tr.source(crate::errors::SRC_LOCAL); }
                                            let _ = dtx.send(close(CLOSE_REPLAY, CLOSE_REPLAY_REASON)).await;
                                            break;
                                        }
                                        let accounts = st.accounts.list().await;
                                        let mut r = route.clone();
                                        if accounts.iter().any(|a| a.id != account && !a.disabled && !r.exclude.contains(&a.id) && st.router.model_cooling_until(&a.id, &r.model).is_none()) {
                                            r.exclude.push(account.clone());
                                        }
                                        match preparation.run(connect(&st, &ctx.path, &ctx.headers, &mut r, proxy::WS_HOLD_MAX, None)).await {
                                            Ok(Ok((ws, h, id, g))) => {
                                                upstream_request_id = crate::errors::request_id(&h);
                                                quota_h = h;
                                                gen = g;
                                                tracing::info!(from = %account, to = %id, model = %route.model, "ws turn model cooling on this account; redialed");
                                                if let Some(tr) = turn.as_mut() {
                                                    if id != account { tr.replay(&account, "model_cooldown", "switch"); tr.switch_to(&id); }
                                                    tr.quota(&quota_h);
                                                }
                                                account = id;
                                                let (tx, rx) = ws.split();
                                                let old_tx = std::mem::replace(&mut utx, tx);
                                                let old_rx = std::mem::replace(&mut urx, rx);
                                                tokio::spawn(drain_close(old_tx, old_rx));
                                            }
                                            Ok(Err(resp)) => {
                                                // What HTTP would answer, as an upstream-style error event;
                                                // the connection stays open and no turn starts.
                                                let ev = error_event(resp, turn.take()).await;
                                                pending = None;
                                                drop(msg_permit.take());
                                                if dtx.send(ev).await.is_err() { break; }
                                                continue 'outer;
                                            }
                                            Err(_) => {
                                                close_after_preparation_timeout(turn.as_mut(), &mut dtx).await;
                                                break;
                                            }
                                        }
                                    }
                                    st.router.note_attempt(&account);
                                }
                            }
                        }
                        awaiting = true;
                    }
                    if let Some(u) = to_up(m) {
                        match write_with_client_close(utx.send(u), &mut drx, &mut queued_down).await {
                            WriteResult::Sent(Ok(())) => {}
                            WriteResult::ClientClosed => {
                                if let Some(t) = turn.as_mut() { t.client_gone(); }
                                abandon_upstream = true;
                                break;
                            }
                            WriteResult::Sent(Err(e)) => {
                                // Client Close and forced shutdown retain their cancellation cause.
                                if !is_close && st.upstream.drain.phase() != Phase::Force {
                                    if awaiting {
                                        progress.warn("ws", "write_error", &account, upstream_request_id.as_deref(), turn.as_ref());
                                    }
                                    if let Some(t) = turn.as_mut() {
                                        t.fail_msg("upstream_closed", format!("upstream websocket send failed (transport error: {e})"));
                                        t.source(crate::errors::SRC_NETWORK);
                                    }
                                }
                                let _ = dtx.send(close(CLOSE_INTERNAL, "upstream gone")).await;
                                break;
                            }
                        }
                    }
                    if pending.is_none() {
                        // Forwarded and not kept for replay.
                        drop(msg_permit.take());
                    }
                    if is_close {
                        if let Some(t) = turn.as_mut() { t.client_gone(); }
                        break;
                    }
                }
                other => {
                    if let Some(Err(tokio_tungstenite::tungstenite::Error::Capacity(e))) = &other {
                        // Over `ws_max_message_mb` (checked while reading and inflating, so it
                        // never grows past the limit). 1009 = message too big.
                        tracing::warn!(account = %account, error = %e, "websocket message over ws_max_message_mb; closing 1009");
                        let _ = dtx.send(close(CLOSE_TOO_BIG, "message too big")).await;
                    }
                    if let Some(t) = turn.as_mut() { t.client_gone(); }
                    abandon_upstream = true;
                    break;
                }
            },
            m = urx.next() => {
                sleep.as_mut().reset(tokio::time::Instant::now() + idle);
                match m {
                    Some(Ok(WsMessage::Close(f))) => {
                        if awaiting {
                            progress.warn("ws", "close_frame", &account, upstream_request_id.as_deref(), turn.as_ref());
                        }
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
                        if awaiting {
                            progress.activity();
                        }
                        let (kind, done) = match &m {
                            WsMessage::Text(t) => {
                                let (k, d, snap) = classify_observed(t.as_str(), &mut progress);
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
                                if let Some(w) = wait {
                                    overload_waited = true;
                                    tracing::info!(account = %account, code = %code, wait_ms = w.as_millis() as u64, "ws overloaded before output on last usable account; waiting to retry once");
                                } else {
                                    switches += 1;
                                    route.exclude.push(account.clone());
                                }
                                let redial = preparation.run(async {
                                    if let Some(w) = wait { tokio::time::sleep(w).await; }
                                    connect(&st, &ctx.path, &ctx.headers, &mut route, Duration::MAX, None).await
                                }).await;
                                match redial {
                                    Ok(Ok((ws, h, id, g))) => {
                                        upstream_request_id = crate::errors::request_id(&h);
                                        quota_h = h;
                                        gen = g;
                                        tracing::info!(from = %account, to = %id, code = %code, "ws overloaded before output; replayed");
                                        if let Some(t) = turn.as_mut() {
                                            if id == account {
                                                t.replay(&account, code, "wait+retry");
                                            } else {
                                                t.replay(&account, code, "switch");
                                                t.switch_to(&id);
                                            }
                                            t.quota(&quota_h);
                                        }
                                        account = id;
                                        let (t, r) = ws.split();
                                        let old_tx = std::mem::replace(&mut utx, t);
                                        let old_rx = std::mem::replace(&mut urx, r);
                                        tokio::spawn(drain_close(old_tx, old_rx));
                                        held.clear();
                                        st.router.note_attempt(&account);
                                        match write_replay(utx.send(WsMessage::Text(req)), &mut drx, &mut queued_down, &st.upstream.drain, &mut progress, &account, upstream_request_id.as_deref(), turn.as_mut()).await {
                                            WriteResult::Sent(Ok(())) => {
                                                sleep.as_mut().reset(tokio::time::Instant::now() + idle);
                                                continue 'outer;
                                            }
                                            WriteResult::ClientClosed => {
                                                if let Some(t) = turn.as_mut() { t.client_gone(); }
                                                abandon_upstream = true;
                                                break 'outer;
                                            }
                                            WriteResult::Sent(Err(_)) => {}
                                        }
                                    }
                                    Ok(Err(_)) => tracing::warn!("ws redial after overload failed"),
                                    Err(_) => {
                                        close_after_preparation_timeout(turn.as_mut(), &mut dtx).await;
                                        break;
                                    }
                                }
                                // Redial failed: stop using the old socket and let the client replay.
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
                            switches += 1;
                            if awaiting && !started && switches < max_switches {
                                if let Some(req) = pending.clone() {
                                    route.exclude.push(account.clone());
                                    match preparation.run(connect(&st, &ctx.path, &ctx.headers, &mut route, Duration::MAX, None)).await {
                                        Ok(Ok((ws, h, id, g))) => {
                                            upstream_request_id = crate::errors::request_id(&h);
                                            quota_h = h;
                                            gen = g;
                                            tracing::info!(from = %account, to = %id, "ws quota error before output; replayed on another account");
                                            if let Some(t) = turn.as_mut() { t.replay(&account, "usage_limit", "switch"); t.switch_to(&id); t.quota(&quota_h); }
                                            account = id;
                                            let (t, r) = ws.split();
                                            // Finish the old socket's close handshake in the
                                            // background so the replay is not delayed.
                                            let old_tx = std::mem::replace(&mut utx, t);
                                            let old_rx = std::mem::replace(&mut urx, r);
                                            tokio::spawn(drain_close(old_tx, old_rx));
                                            held.clear();
                                            match write_replay(utx.send(WsMessage::Text(req)), &mut drx, &mut queued_down, &st.upstream.drain, &mut progress, &account, upstream_request_id.as_deref(), turn.as_mut()).await {
                                                WriteResult::Sent(Ok(())) => {
                                                    sleep.as_mut().reset(tokio::time::Instant::now() + idle);
                                                    continue 'outer;
                                                }
                                                WriteResult::ClientClosed => {
                                                    if let Some(t) = turn.as_mut() { t.client_gone(); }
                                                    abandon_upstream = true;
                                                    break 'outer;
                                                }
                                                WriteResult::Sent(Err(_)) => {}
                                            }
                                        }
                                        Ok(Err(_)) => tracing::warn!("ws redial after quota error failed"),
                                        Err(_) => {
                                            close_after_preparation_timeout(turn.as_mut(), &mut dtx).await;
                                            break;
                                        }
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
                            drop(turn_slot.take());
                            pending = None;
                            drop(msg_permit.take());
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
                            progress.warn("ws", if other.is_some() { "read_error" } else { "eof" }, &account, upstream_request_id.as_deref(), turn.as_ref());
                        } else {
                            tracing::info!(account = %account, error_class = if other.is_some() { "read_error" } else { "eof" }, "upstream websocket transport error between turns");
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
                progress.warn("ws", "idle_timeout", &account, upstream_request_id.as_deref(), turn.as_ref());
                if let Some(t) = turn.as_mut() { t.fail_msg("idle_timeout", format!("upstream websocket idle for {}s (timeout)", idle.as_secs())); t.source(crate::errors::SRC_NETWORK); }
                st.router.report(&route, &account, &Outcome::Transient { status: None });
                let _ = dtx.send(close(CLOSE_INTERNAL, "stream idle timeout")).await;
                let _ = utx.send(WsMessage::Close(None)).await;
                break;
            }
        }
    }
    if abandon_upstream {
        // A cancelled send may have written part of a frame. Do not flush or reuse
        // that connection: even close() can flush the remaining request bytes.
        drop(utx);
        drop(urx);
        let _ = tokio::time::timeout(Duration::from_secs(2), dtx.close()).await;
        return;
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

enum WriteResult<T> {
    Sent(T),
    ClientClosed,
}

/// A replay starts a new observed attempt even if its first write fails. Keep the
/// old terminal error from suppressing that failure, and preserve cancellation.
#[allow(clippy::too_many_arguments)]
async fn write_replay<F, R, E, W>(
    write: F,
    rx: &mut R,
    queued: &mut Option<WsMessage>,
    drain: &ciallopa_upstream::drain::Drain,
    progress: &mut StreamObservation,
    account: &str,
    upstream_request_id: Option<&str>,
    tracker: Option<&mut Tracker>,
) -> WriteResult<Result<(), W>>
where
    F: std::future::Future<Output = Result<(), W>>,
    R: futures::Stream<Item = Result<WsMessage, E>> + Unpin,
{
    *progress = StreamObservation::new();
    let result = write_with_client_close(write, rx, queued).await;
    if matches!(&result, WriteResult::Sent(Err(_))) {
        if drain.phase() == Phase::Force {
            if let Some(t) = tracker {
                t.client_gone();
            }
        } else {
            progress.warn(
                "ws",
                "write_error",
                account,
                upstream_request_id,
                tracker.as_deref(),
            );
        }
    }
    result
}

async fn next_downstream<R, E>(
    rx: &mut R,
    queued: &mut Option<WsMessage>,
) -> Option<Result<WsMessage, E>>
where
    R: futures::Stream<Item = Result<WsMessage, E>> + Unpin,
{
    match queued.take() {
        Some(message) => Some(Ok(message)),
        None => rx.next().await,
    }
}

/// Keep a client Close/EOF visible while an upstream write is backpressured.
/// One following message may be retained in order; then reading pauses until the
/// write finishes. This is bounded lookahead, not a promise to find Close behind
/// arbitrarily many queued messages. Never time out or restart a partial frame.
async fn write_with_client_close<F, R, E>(
    write: F,
    rx: &mut R,
    queued: &mut Option<WsMessage>,
) -> WriteResult<F::Output>
where
    F: std::future::Future,
    R: futures::Stream<Item = Result<WsMessage, E>> + Unpin,
{
    tokio::pin!(write);
    loop {
        tokio::select! {
            result = &mut write => return WriteResult::Sent(result),
            message = rx.next(), if queued.is_none() => match message {
                Some(Ok(WsMessage::Close(_))) | Some(Err(_)) | None => return WriteResult::ClientClosed,
                Some(Ok(message)) => *queued = Some(message),
            },
        }
    }
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

/// Preserve the preparation failure while asking the client to replay on a new connection.
async fn close_after_preparation_timeout<T>(turn: Option<&mut Tracker>, dtx: &mut T)
where
    T: futures::Sink<WsMessage> + Unpin,
{
    if let Some(t) = turn {
        t.fail_msg(
            "preparation_timeout",
            "websocket preparation budget exhausted across retries/account switches",
        );
        t.source(crate::errors::SRC_NETWORK);
    }
    // This is cleanup after a bounded preparation, not a deadline for ordinary data writes.
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        dtx.send(close(CLOSE_REPLAY, CLOSE_REPLAY_REASON)),
    )
    .await;
}

/// Send Close and wait for its reply within one bound, then drop both halves.
async fn drain_close<T, R, E>(mut tx: T, mut rx: R)
where
    T: futures::Sink<WsMessage> + Unpin,
    R: futures::Stream<Item = Result<WsMessage, E>> + Unpin,
{
    // Closing the abandoned connection must not hold up preparing its replacement.
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        let _ = tx.send(WsMessage::Close(None)).await;
        while let Some(Ok(m)) = rx.next().await {
            if matches!(m, WsMessage::Close(_)) {
                break;
            }
        }
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Default)]
    struct StreamWarnings(std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>);

    impl tracing::Subscriber for StreamWarnings {
        fn enabled(&self, m: &tracing::Metadata<'_>) -> bool {
            m.fields().field("terminal_received").is_some()
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Fields(serde_json::Map<String, serde_json::Value>);
            impl tracing::field::Visit for Fields {
                fn record_u64(&mut self, f: &tracing::field::Field, v: u64) {
                    self.0.insert(f.name().into(), v.into());
                }
                fn record_bool(&mut self, f: &tracing::field::Field, v: bool) {
                    self.0.insert(f.name().into(), v.into());
                }
                fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
                    self.0.insert(f.name().into(), v.into());
                }
                fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                    self.0.insert(f.name().into(), format!("{v:?}").into());
                }
            }
            let mut fields = Fields(serde_json::Map::new());
            event.record(&mut fields);
            self.0.lock().unwrap().push(fields.0.into());
        }
    }

    #[tokio::test]
    // Tungstenite's handshake callback requires its concrete HTTP error response.
    #[allow(clippy::result_large_err)]
    async fn replay_write_failure_has_fresh_progress_and_preserves_cancellation() {
        let warnings = StreamWarnings::default();
        // Retain both dispatchers, as in server/tests/forward.rs: parallel tests
        // must not cache `never` for this shared diagnostic callsite.
        let _ordinary = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        let _capture = tracing::subscriber::set_default(warnings.clone());
        tokio::time::timeout(Duration::from_secs(3), async {
            for cause in ["write_error", "force", "client_close"] {
                warnings.0.lock().unwrap().clear();
                let mut progress = StreamObservation::new();
                progress.activity();
                classify_observed(r#"{"type":"response.created"}"#, &mut progress);
                classify_observed(
                    r#"{"type":"error","error":{"code":"server_is_overloaded"}}"#,
                    &mut progress,
                );
                assert_eq!(progress.events, 2);

                // A real WS handshake on a tiny in-memory transport. The redialed
                // server stops reading, so a small request deterministically blocks.
                let (client, server) = tokio::io::duplex(64);
                let (up, server) = tokio::join!(
                    tokio_tungstenite::client_async("ws://127.0.0.1/replay", client),
                    tokio_tungstenite::accept_hdr_async(
                        server,
                        |_: &tungstenite::handshake::server::Request,
                         mut r: tungstenite::handshake::server::Response| {
                            r.headers_mut()
                                .insert("x-request-id", "replay-handshake-id".parse().unwrap());
                            Ok(r)
                        }
                    )
                );
                let (mut up, handshake) = up.unwrap();
                let server = server.unwrap();
                let request_id = crate::errors::request_id(handshake.headers());
                let drain = ciallopa_upstream::drain::Drain::default();
                let (client_tx, mut client_rx) =
                    futures::channel::mpsc::unbounded::<Result<WsMessage, ()>>();
                let mut queued = None;
                let request = format!(
                    r#"{{"type":"response.create","input":"{}"}}"#,
                    "private-input".repeat(16)
                );
                let outcome = {
                    let send = write_replay(
                        up.send(WsMessage::text(request)),
                        &mut client_rx,
                        &mut queued,
                        &drain,
                        &mut progress,
                        "b",
                        request_id.as_deref(),
                        None,
                    );
                    tokio::pin!(send);
                    assert!(
                        futures::poll!(&mut send).is_pending(),
                        "fixture must backpressure the replay write"
                    );
                    if cause == "client_close" {
                        client_tx
                            .unbounded_send(Ok(WsMessage::Close(None)))
                            .unwrap();
                        let outcome = send.await;
                        drop(server);
                        outcome
                    } else {
                        if cause == "force" {
                            drain.force();
                        }
                        drop(server);
                        send.await
                    }
                };
                if cause == "client_close" {
                    assert!(matches!(outcome, WriteResult::ClientClosed));
                } else {
                    assert!(matches!(outcome, WriteResult::Sent(Err(_))));
                }
                assert_eq!(progress.events, 0);
                let logs = warnings.0.lock().unwrap().clone();
                if cause != "write_error" {
                    assert!(logs.is_empty(), "{cause}: {logs:?}");
                    continue;
                }
                assert_eq!(logs.len(), 1, "{logs:?}");
                let log = &logs[0];
                assert_eq!(log["transport"], "ws");
                assert_eq!(log["error_class"], "write_error");
                assert_eq!(log["account"], "b");
                assert_eq!(log["upstream_request_id"], "replay-handshake-id");
                assert_eq!(log["event_count"], 0);
                assert_eq!(log["terminal_received"], false);
                assert_eq!(log["activity_received"], false);
                assert!(log["last_event"].is_null());
                assert!(log.get("error").is_none());
                assert!(!log.to_string().contains("private-input"));
            }
        })
        .await
        .expect("replay write fixtures must finish within one bound");
    }

    #[tokio::test]
    async fn pending_upstream_write_observes_client_close_or_eof() {
        for messages in [vec![Ok::<_, ()>(WsMessage::Close(None))], vec![]] {
            let mut rx = futures::stream::iter(messages);
            let mut queued = None;
            let outcome = tokio::time::timeout(
                Duration::from_secs(1),
                write_with_client_close(std::future::pending::<()>(), &mut rx, &mut queued),
            )
            .await
            .expect("client cancellation must not wait for a blocked write");
            assert!(matches!(outcome, WriteResult::ClientClosed));
            assert!(queued.is_none());
        }
    }

    #[tokio::test]
    async fn pending_upstream_write_keeps_one_following_message_in_order() {
        let mut rx = futures::stream::iter([
            Ok::<_, ()>(WsMessage::text("first")),
            Ok(WsMessage::text("second")),
        ]);
        let mut queued = None;
        let outcome = write_with_client_close(
            tokio::time::sleep(Duration::from_millis(20)),
            &mut rx,
            &mut queued,
        )
        .await;
        assert!(matches!(outcome, WriteResult::Sent(())));
        assert_eq!(queued.as_ref().unwrap().to_text().unwrap(), "first");
        let first = next_downstream(&mut rx, &mut queued)
            .await
            .unwrap()
            .unwrap();
        let second = next_downstream(&mut rx, &mut queued)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.to_text().unwrap(), "first");
        assert_eq!(second.to_text().unwrap(), "second");
        assert!(queued.is_none());
    }

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
