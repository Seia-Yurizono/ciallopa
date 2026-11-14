use super::connect::connect;
use super::frames::{busy_frame, error_event, replayable, to_down, to_up, Head, Message};
use super::Ctx;
use crate::attempt::{quota_plan, Next, OverloadPlan, Policy};
use crate::budget::PreparationBudget;
use crate::classify::classify_observed;
use crate::timing;
use crate::usage::{endpoint_of, StreamObservation, Tracker};
use crate::ws_upgrade::DownWs;
use crate::{proxy, AppState};
use ciallopa_core::{AccountId, Outcome, RouteRequest};
use ciallopa_routing::parse_quota_event_headers;
use ciallopa_upstream::drain::{Phase, CUT_FLUSH_MAX};
use ciallopa_upstream::{UpstreamWs, WsMessage};
use ciallopa_usage::Transport;
use futures::{SinkExt, StreamExt};
use http::HeaderMap;
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

struct UpLink {
    tx: futures::stream::SplitSink<UpstreamWs, WsMessage>,
    rx: futures::stream::SplitStream<UpstreamWs>,
    account: AccountId,
    gen: u64,
    request_id: Option<String>,
    ext: Option<String>,
    quota: HeaderMap,
}

impl UpLink {
    fn new(ws: UpstreamWs, quota: HeaderMap, account: AccountId, gen: u64) -> Self {
        let (tx, rx) = ws.split();
        Self {
            tx,
            rx,
            account,
            gen,
            request_id: crate::errors::request_id(&quota),
            ext: ext_answer(&quota),
            quota,
        }
    }
}

enum Redial<'a> {
    ModelCooldown,
    Overload {
        code: &'a str,
        wait: Option<Duration>,
    },
    Quota(&'a Outcome),
}

enum RedialFailed {
    Budget,
    Refused(axum::response::Response),
}

#[allow(clippy::too_many_arguments)]
async fn redial(
    st: &AppState,
    ctx: &Ctx,
    route: &mut RouteRequest,
    up: &mut UpLink,
    budget: &mut PreparationBudget,
    tracker: Option<&mut Tracker>,
    why: Redial<'_>,
) -> Result<(), RedialFailed> {
    let (hold, wait) = match why {
        Redial::ModelCooldown => (proxy::WS_HOLD_MAX, None),
        Redial::Overload { wait, .. } => (Duration::MAX, wait),
        Redial::Quota(_) => (Duration::MAX, None),
    };
    let (ws, headers, account, gen) = budget
        .run(async {
            if let Some(wait) = wait {
                tokio::time::sleep(wait).await;
            }
            connect(st, &ctx.path, &ctx.headers, route, hold, None).await
        })
        .await
        .map_err(|_| RedialFailed::Budget)?
        .map_err(RedialFailed::Refused)?;
    let replacement = UpLink::new(ws, headers, account, gen);
    let code = match why {
        Redial::ModelCooldown => "model_cooldown",
        Redial::Overload { code, .. } => code,
        Redial::Quota(outcome) => crate::errors::quota_outcome_code(outcome),
    };
    tracing::info!(from = %up.account, to = %replacement.account, code, "ws redialed before output");
    if let Some(tracker) = tracker {
        tracker.obs_upstream_request_id(replacement.request_id.as_deref());
        tracker.obs_ws_negotiation(ctx.ws_ext_down.as_deref(), replacement.ext.as_deref());
        if !matches!(why, Redial::ModelCooldown) || replacement.account != up.account {
            tracker.replay(
                &up.account,
                code,
                if replacement.account == up.account {
                    Next::WaitRetry
                } else {
                    Next::Switch
                },
            );
        }
        if replacement.account != up.account {
            tracker.switch_to(&replacement.account);
        }
        tracker.quota(&replacement.quota);
    }
    let old = std::mem::replace(up, replacement);
    tokio::spawn(drain_close(
        old.tx,
        old.rx,
        timing::scale(st, Duration::from_secs(2)),
    ));
    Ok(())
}

/// The relay observes shutdown between frames, but a send/connect branch can itself be
/// stuck. Give it time to write the normal close frame, then drop it before the process
/// finishes its three-second close grace and flushes the usage writer.
pub(super) async fn finish_on_shutdown(
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

fn close_frame(code: u16, reason: &str) -> Message {
    Message::Close(Some(UpCloseFrame {
        code: CloseCode::from(code),
        reason: reason.into(),
    }))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn relay(
    st: AppState,
    ctx: Ctx,
    mut route: RouteRequest,
    account: AccountId,
    down: DownWs,
    up: UpstreamWs,
    gen: u64,
    idle: Duration,
    handshake: HeaderMap,
    handoff: Option<ciallopa_upstream::drain::Lease>,
) {
    // Egress proxy switch: the upstream WebSocket keeps the proxy it was dialed with, so once
    // the egress generation moves past `gen`, the session is closed (normal close, both legs)
    // between turns and the client reconnects through the new egress. A running turn finishes
    // on the old connection first. Not an error: no cooldown, no usage record.
    let mut egress = st.upstream.egress_watch();
    // Graceful shutdown: same rule (close between turns, after the running one), and after
    // the drain bound a 1001 close. A session opened during the drain sees it at once.
    let _session = st.upstream.drain.enter();
    // The handshake's (exempt) admission ends once the session is counted. An idle
    // connection holds no inflight slot; each turn takes one (`turn_slot`).
    drop(handoff);
    let mut turn_slot: Option<ciallopa_upstream::drain::Lease> = None;
    let mut drain = st.upstream.drain.watch();
    drain.mark_changed();
    let mut up = UpLink::new(up, handshake, account, gen);
    let mut progress = StreamObservation::new();
    let (mut dtx, mut drx) = down.split();
    let mut queued_down = None;
    let max_switches = st.accounts.len().max(1);
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
    // Shutdown cut a running turn: forward what upstream already delivered, then end the turn
    // with a retryable response.failed and close (see `next_upstream`).
    let mut cut: Option<tokio::time::Instant> = None;
    let reason = 'outer: loop {
        tokio::select! {
            m = next_downstream(&mut drx, &mut queued_down), if cut.is_none() => match m {
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
                                    account: Some(&up.account.to_string()),
                                    limit: budget,
                                    bytes: Some(n as u64),
                                });
                                if dtx.send(busy_frame()).await.is_err() { break 'outer Close::ClientGone; }
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
                                    account: Some(&up.account.to_string()),
                                    limit: limit as u64,
                                    bytes: None,
                                });
                                drop(msg_permit.take());
                                if dtx.send(busy_frame()).await.is_err() { break 'outer Close::ClientGone; }
                                continue 'outer;
                            }
                            Err(ciallopa_upstream::drain::Refused::Draining) => {
                                drop(msg_permit.take());
                                // The client already sent this turn: ask it to replay later.
                                break 'outer Close::Shutdown(ShutdownClose::Refused);
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
                            switches = 0;
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
                                        tr.accounts(&[], Some(&up.account));
                                        tr.quota(&up.quota);
                                        tr.status(200);
                                        tr.obs_upstream_request_id(up.request_id.as_deref());
                                        tr.obs_ws_negotiation(ctx.ws_ext_down.as_deref(), up.ext.as_deref());
                                        tr
                                    });
                                    if !route.model.is_empty() && st.router.model_cooling_until(&up.account, &route.model).is_some() {
                                        // This account is cooling down for the turn's model (HTTP would
                                        // not pick it): move to an account that may serve it, or wait
                                        // out a short cooldown, before anything is sent upstream.
                                        if !replayable(t.as_str()) {
                                            // Its `previous_response_id` lives on this connection only.
                                            tracing::info!(account = %ciallopa_core::a(&up.account), model = %route.model, "ws turn model cooling on this account; closing for client replay");
                                            if let Some(tr) = turn.as_mut() { tr.fail_msg("model_cooldown", "account cooling down for this model; closed for client replay"); tr.source(crate::errors::SRC_LOCAL); }
                                            break 'outer Close::Replay;
                                        }
                                        let accounts = st.accounts.accounts();
                                        let mut r = route.clone();
                                        if accounts.iter().any(|a| a.id != up.account && !a.disabled && !r.exclude.contains(&a.id) && st.router.model_cooling_until(&a.id, &r.model).is_none()) {
                                            r.exclude.push(up.account.clone());
                                        }
                                        match redial(&st, &ctx, &mut r, &mut up, &mut preparation, turn.as_mut(), Redial::ModelCooldown).await {
                                            Ok(()) => {}
                                            Err(RedialFailed::Refused(resp)) => {
                                                // What HTTP would answer, as an upstream-style error event;
                                                // the connection stays open and no turn starts.
                                                let ev = error_event(resp, turn.take()).await;
                                                pending = None;
                                                drop(msg_permit.take());
                                                if dtx.send(ev).await.is_err() { break 'outer Close::ClientGone; }
                                                continue 'outer;
                                            }
                                            Err(RedialFailed::Budget) => {
                                                preparation_timeout(turn.as_mut());
                                                break 'outer Close::Replay;
                                            }
                                        }
                                    }
                                    st.router.note_attempt(&up.account);
                                }
                            }
                        }
                        awaiting = true;
                    }
                    if let Some(u) = to_up(m) {
                        match write_with_client_close(up.tx.send(u), &mut drx, &mut queued_down).await {
                            WriteResult::Sent(Ok(())) => {}
                            WriteResult::ClientClosed => {
                                if let Some(t) = turn.as_mut() { t.client_gone(); }
                                break 'outer Close::Interrupted;
                            }
                            WriteResult::Sent(Err(e)) => {
                                // Client Close and forced shutdown retain their cancellation cause.
                                if !is_close && st.upstream.drain.phase() != Phase::Force {
                                    if awaiting {
                                        progress.warn("ws", "write_error", &up.account, up.request_id.as_deref(), turn.as_ref());
                                    }
                                    if let Some(t) = turn.as_mut() {
                                        t.fail_msg("upstream_ws_send_error", format!("upstream websocket send failed (transport error: {e})"));
                                        t.source(crate::errors::SRC_NETWORK);
                                    }
                                }
                                break 'outer Close::Internal("upstream gone");
                            }
                        }
                    }
                    if pending.is_none() {
                        // Forwarded and not kept for replay.
                        drop(msg_permit.take());
                    }
                    if is_close {
                        if let Some(t) = turn.as_mut() { t.client_gone(); }
                        break 'outer Close::ClientGone;
                    }
                }
                other => {
                    if let Some(t) = turn.as_mut() { t.client_gone(); }
                    if let Some(Err(tokio_tungstenite::tungstenite::Error::Capacity(e))) = &other {
                        tracing::warn!(account = %ciallopa_core::a(&up.account), error = %e, "websocket message over ws_max_message_mb; closing 1009");
                        break 'outer Close::TooBig;
                    }
                    break 'outer Close::Interrupted;
                }
            },
            m = next_upstream(&mut up.rx, cut) => {
                let Upstream::Next(m) = m else {
                    // Everything upstream already delivered has been forwarded: end the turn.
                    if awaiting {
                        if let Some(t) = turn.as_mut() { t.fail_msg("shutdown", "closed by server shutdown before the turn finished".to_string()); t.source(crate::errors::SRC_LOCAL); }
                    }
                    for h in held.drain(..) { let _ = send_down(&mut dtx, h, turn.as_mut()).await; }
                    break 'outer Close::Shutdown(ShutdownClose::Cut);
                };
                sleep.as_mut().reset(tokio::time::Instant::now() + idle);
                match m {
                    Some(Ok(WsMessage::Close(f))) => {
                        if awaiting {
                            progress.warn("ws", "close_frame", &up.account, up.request_id.as_deref(), turn.as_ref());
                        }
                        let code = f.as_ref().map(|f| u16::from(f.code)).unwrap_or(1000);
                        if let Some(t) = turn.as_mut() {
                            let reason = f.as_ref().map(|f| f.reason.to_string()).unwrap_or_default();
                            t.fail_msg("upstream_ws_close", format!("upstream websocket closed mid-turn (code {code}) {reason}").trim_end().to_string()); t.source(crate::errors::SRC_STREAM);
                        }
                        if code != 1000 {
                            st.router.report(&route, &up.account, &Outcome::Transport);
                        }
                        for h in held.drain(..) { let _ = send_down(&mut dtx, h, turn.as_mut()).await; }
                        break 'outer Close::Upstream(f);
                    }
                    Some(Ok(m)) => {
                        if awaiting {
                            progress.activity();
                            if let Some(t) = turn.as_mut() { t.upstream_activity(); }
                        }
                        if let Some(t) = turn.as_mut() {
                            match &m {
                                WsMessage::Text(x) => {
                                    t.obs_up_bytes(x.len());
                                    if let Some(ty) = crate::usage::event_type(x.as_str()) {
                                        t.obs_event(&ty);
                                    }
                                }
                                WsMessage::Binary(b) => t.obs_up_bytes(b.len()),
                                _ => {}
                            }
                        }
                        let (kind, done) = match &m {
                            WsMessage::Text(t) => {
                                let (k, d, snap) = classify_observed(t.as_str(), &mut progress);
                                if let Some(o) = snap {
                                    st.router.report(&route, &up.account, &o);
                                    if let Some(h) = parse_quota_event_headers(t.as_str().as_bytes()) {
                                        if let Some(tr) = turn.as_mut() { tr.quota(&h); }
                                        up.quota = h;
                                    }
                                }
                                // An error frame Codex ignores: forward it, but it neither
                                // commits the attempt nor counts as model output.
                                let soft = !d && crate::usage::event_type(t.as_str()).as_deref() == Some("error");
                                if soft {
                                    tracing::warn!(account = %ciallopa_core::a(&up.account), record_id = turn.as_ref().map(Tracker::record_id), "upstream websocket error frame without status; forwarded, turn continues");
                                    if let Some(tr) = turn.as_mut() { tr.error_text(t.as_str().as_bytes()); }
                                    (proxy::EventKind::Preamble, d)
                                } else {
                                    (k, d)
                                }
                            }
                            _ => (proxy::EventKind::Preamble, false),
                        };
                        if let (proxy::EventKind::Quota(_), Some(t), WsMessage::Text(txt)) = (&kind, turn.as_mut(), &m) {
                            t.error_text(txt.as_str().as_bytes());
                        }
                        if let proxy::EventKind::Overloaded(code) = &kind {
                            let accounts = st.accounts.accounts();
                            let others = proxy::others_usable(&st, &accounts, &route, &up.account);
                            st.router.report(&route, &up.account, &proxy::overload_outcome(&st, others));
                            let fresh = awaiting && !started && pending.is_some();
                            // Only usable account left: wait once and replay on it (Codex does not
                            // retry an in-stream overload itself).
                            let plan = if fresh {
                                Policy::ws_redial().overload_plan(others, overload_waited, &HeaderMap::new(),
                                    st.config.load().upstream_opts.overload_retry_wait_secs, switches, max_switches)
                            } else { OverloadPlan::PassThrough };
                            let wait = match plan { OverloadPlan::WaitRetry(wait) => Some(timing::scale(&st, wait)), _ => None };
                            let can_replay = plan != OverloadPlan::PassThrough;
                            if let (true, Some(req)) = (can_replay, pending.clone()) {
                                if let Some(w) = wait {
                                    overload_waited = true;
                                    tracing::info!(account = %ciallopa_core::a(&up.account), code = %code, wait_ms = w.as_millis() as u64, "ws overloaded before output on last usable account; waiting to retry once");
                                } else {
                                    switches += 1;
                                    route.exclude.push(up.account.clone());
                                }
                                match redial(&st, &ctx, &mut route, &mut up, &mut preparation, turn.as_mut(), Redial::Overload { code, wait }).await {
                                    Ok(()) => {
                                        held.clear();
                                        st.router.note_attempt(&up.account);
                                        match write_replay(up.tx.send(WsMessage::Text(req)), &mut drx, &mut queued_down, &st.upstream.drain, &mut progress, &up.account, up.request_id.as_deref(), turn.as_mut()).await {
                                            WriteResult::Sent(Ok(())) => {
                                                sleep.as_mut().reset(tokio::time::Instant::now() + idle);
                                                continue 'outer;
                                            }
                                            WriteResult::ClientClosed => {
                                                if let Some(t) = turn.as_mut() { t.client_gone(); }
                                                break 'outer Close::Interrupted;
                                            }
                                            WriteResult::Sent(Err(_)) => {}
                                        }
                                    }
                                    Err(RedialFailed::Refused(_)) => tracing::warn!("ws redial after overload failed"),
                                    Err(RedialFailed::Budget) => {
                                        preparation_timeout(turn.as_mut());
                                                break 'outer Close::Replay;
                                    }
                                }
                                // Redial failed: stop using the old socket and let the client replay.
                                if let Some(t) = turn.as_mut() { t.fail_msg(code, "upstream overloaded; redial failed, closed for client replay"); t.source(crate::errors::SRC_STREAM); }
                                break 'outer Close::Replay;
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
                            st.router.report(&route, &up.account, &o);
                            let next = quota_plan(&mut switches, max_switches);
                            if awaiting && !started && next == Next::Switch {
                                if let Some(req) = pending.clone() {
                                    route.exclude.push(up.account.clone());
                                    match redial(&st, &ctx, &mut route, &mut up, &mut preparation, turn.as_mut(), Redial::Quota(&o)).await {
                                        Ok(()) => {
                                            held.clear();
                                            match write_replay(up.tx.send(WsMessage::Text(req)), &mut drx, &mut queued_down, &st.upstream.drain, &mut progress, &up.account, up.request_id.as_deref(), turn.as_mut()).await {
                                                WriteResult::Sent(Ok(())) => {
                                                    sleep.as_mut().reset(tokio::time::Instant::now() + idle);
                                                    continue 'outer;
                                                }
                                                WriteResult::ClientClosed => {
                                                    if let Some(t) = turn.as_mut() { t.client_gone(); }
                                                    break 'outer Close::Interrupted;
                                                }
                                                WriteResult::Sent(Err(_)) => {}
                                            }
                                        }
                                        Err(RedialFailed::Refused(_)) => tracing::warn!("ws redial after quota error failed"),
                                        Err(_) => {
                                            preparation_timeout(turn.as_mut());
                                                break 'outer Close::Replay;
                                        }
                                    }
                                }
                            }
                            tracing::info!(account = %ciallopa_core::a(&up.account), "ws quota error mid-turn; closing for client replay");
                            if let Some(t) = turn.as_mut() { t.fail_msg(crate::errors::quota_outcome_code(&o), "usage limit reached mid-turn; closed for client replay"); t.source(crate::errors::SRC_STREAM); }
                            break 'outer Close::Replay;
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
                                        if send_down(&mut dtx, h, turn.as_mut()).await.is_err() { break 'outer Close::ClientGone; }
                                    }
                                    continue;
                                }
                                held.push(d);
                            }
                            continue;
                        }
                        if awaiting {
                            if !started && matches!(kind, proxy::EventKind::Content) && !done {
                                st.router.content_ok(&up.account);
                            }
                            started = true;
                        }
                        for h in held.drain(..) {
                            if send_down(&mut dtx, h, turn.as_mut()).await.is_err() { break 'outer Close::ClientGone; }
                        }
                        // The finished turn's tracker lives until its terminal frame was sent
                        // downstream (observed send), with `duration_ms` fixed here as before.
                        let mut finished: Option<Tracker> = None;
                        if done {
                            awaiting = false;
                            drop(turn_slot.take());
                            pending = None;
                            drop(msg_permit.take());
                            if let Some(mut t) = turn.take() {
                                if let WsMessage::Text(txt) = &m {
                                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(txt.as_str()) {
                                        t.terminal_event(&v);
                                    }
                                }
                                t.freeze_duration();
                                finished = Some(t);
                            }
                        }
                        // Encrypted-content rejection: same frame, clear message (logged above
                        // with the upstream one). No account outcome: the request is at fault.
                        let m = match (&kind, m) {
                            (proxy::EventKind::Encrypted(_), WsMessage::Text(txt)) => {
                                tracing::info!(account = %ciallopa_core::a(&up.account), "upstream rejected the encrypted content in the request history");
                                match serde_json::from_str::<serde_json::Value>(txt.as_str()) {
                                    Ok(v) => WsMessage::text(crate::encrypted::ws_frame(&v)),
                                    Err(_) => WsMessage::Text(txt),
                                }
                            }
                            (_, m) => m,
                        };
                        if let Some(d) = to_down(m) {
                            let t = finished.as_mut().or(turn.as_mut());
                            if send_down(&mut dtx, d, t).await.is_err() {
                                drop(finished);
                                let _ = up.tx.send(WsMessage::Close(None)).await;
                                break 'outer Close::ClientGone;
                            }
                        }
                        // `finished` dropped here → record emitted.
                        drop(finished);
                        if done && st.upstream.egress_gen() != up.gen {
                            break 'outer Close::Egress;
                        }
                        if done && st.upstream.drain.phase() != Phase::Running {
                            break 'outer Close::Shutdown(ShutdownClose::BetweenTurns);
                        }
                    }
                    other => {
                        let (class, code, msg) = upstream_end(&other);
                        if awaiting {
                            progress.warn("ws", class, &up.account, up.request_id.as_deref(), turn.as_ref());
                        } else {
                            tracing::info!(account = %ciallopa_core::a(&up.account), error_class = class, "upstream websocket transport error between turns");
                        }
                        if let Some(t) = turn.as_mut() { t.fail_msg(code, msg); t.source(crate::errors::SRC_NETWORK); }
                        st.router.report(&route, &up.account, &Outcome::Transport);
                        break 'outer Close::Internal("upstream disconnected");
                    }
                }
            },
            _ = egress.changed() => {
                if !awaiting && *egress.borrow_and_update() != up.gen {
                    break 'outer Close::Egress;
                }
            }
            _ = drain.changed(), if cut.is_none() => {
                let phase = *drain.borrow_and_update();
                if phase == Phase::Force && awaiting {
                    tracing::warn!(account = %ciallopa_core::a(&up.account), "shutting down; drain bound reached, cutting the running websocket turn");
                    cut = Some(tokio::time::Instant::now() + timing::scale(&st, CUT_FLUSH_MAX));
                } else if phase == Phase::Force || (phase == Phase::Draining && !awaiting) {
                    break 'outer Close::Shutdown(ShutdownClose::BetweenTurns);
                }
            }
            _ = &mut sleep, if awaiting => {
                progress.warn("ws", "idle_timeout", &up.account, up.request_id.as_deref(), turn.as_ref());
                if let Some(t) = turn.as_mut() { t.fail_msg("idle_timeout", format!("upstream websocket idle for {}s (timeout)", idle.as_secs())); t.source(crate::errors::SRC_NETWORK); }
                st.router.report(&route, &up.account, &Outcome::Transport);
                break 'outer Close::Internal("stream idle timeout");
            }
        }
    };
    close(
        reason,
        dtx,
        drx,
        up,
        timing::scale(&st, Duration::from_secs(2)),
    )
    .await;
}

/// Downstream send of a turn's message, observed on its tracker (`sink.send` completion:
/// frame written and tungstenite's flush returned; not a client receive).
async fn send_down<S>(dtx: &mut S, m: Message, t: Option<&mut Tracker>) -> Result<(), S::Error>
where
    S: futures::Sink<Message> + Unpin,
{
    let n = match &m {
        Message::Text(x) => Some(x.len()),
        Message::Binary(b) => Some(b.len()),
        _ => None,
    };
    let (r, blocked) = crate::downstream::timed(dtx.send(m)).await;
    if let (Some(t), Some(n)) = (t, n) {
        t.obs_ws_sent(n, blocked, r.is_ok());
    }
    r
}

/// The `Sec-WebSocket-Extensions` value of a handshake response.
fn ext_answer(h: &HeaderMap) -> Option<String> {
    h.get(http::header::SEC_WEBSOCKET_EXTENSIONS)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
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

enum Close {
    ClientGone,
    /// A cancelled upstream write may contain a partial frame and must never be flushed.
    Interrupted,
    Upstream(Option<UpCloseFrame>),
    Replay,
    Internal(&'static str),
    TooBig,
    Egress,
    Shutdown(ShutdownClose),
}

type DownTx = futures::stream::SplitSink<DownWs, Message>;
type DownRx = futures::stream::SplitStream<DownWs>;

async fn close(reason: Close, tx: DownTx, rx: DownRx, up: UpLink, grace: Duration) {
    let mut down = tx
        .reunite(rx)
        .expect("downstream halves belong to this session");
    let frame = match &reason {
        Close::ClientGone | Close::Interrupted => Message::Close(None),
        Close::Upstream(frame) => Message::Close(frame.clone()),
        Close::Replay => close_frame(CLOSE_REPLAY, CLOSE_REPLAY_REASON),
        Close::Internal(reason) => close_frame(CLOSE_INTERNAL, reason),
        Close::TooBig => close_frame(CLOSE_TOO_BIG, "message too big"),
        Close::Egress => {
            tracing::info!(account = %ciallopa_core::a(&up.account), "egress proxy changed; closing the idle websocket session");
            close_frame(1000, "")
        }
        Close::Shutdown(ShutdownClose::BetweenTurns) => close_frame(1000, ""),
        Close::Shutdown(ShutdownClose::Cut | ShutdownClose::Refused) => close_frame(1001, ""),
    };
    let downstream = async {
        let _ = tokio::time::timeout(grace, async {
            if matches!(
                reason,
                Close::Shutdown(ShutdownClose::Cut | ShutdownClose::Refused)
            ) {
                let _ = down
                    .send(Message::text(proxy::shutdown_failed_event().to_string()))
                    .await;
            }
            close_downstream(&mut down, frame).await;
        })
        .await;
    };
    let upstream = async {
        match reason {
            Close::Interrupted | Close::TooBig => drop(up),
            _ => drain_close(up.tx, up.rx, grace).await,
        }
    };
    tokio::join!(downstream, upstream);
}

/// Flush our Close, then consume in-flight client data until the reply or EOF.
async fn close_downstream<S>(down: &mut tokio_tungstenite::WebSocketStream<S>, frame: Message)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    // A peer Close already queued its reply; send then reports SendAfterClosing.
    let _ = down.send(frame).await;
    if down.flush().await.is_err() {
        return;
    }
    while let Some(Ok(message)) = down.next().await {
        if matches!(message, Message::Close(_)) {
            return;
        }
    }
    // tokio-tungstenite fuses its Stream after Capacity (including oversized compressed
    // messages). Read the transport directly so unread TCP bytes cannot reset our Close.
    let mut discard = [0; 8192];
    loop {
        match tokio::io::AsyncReadExt::read(down.get_mut(), &mut discard).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

/// How a session ends on server shutdown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ShutdownClose {
    /// No turn running (or it just finished): normal close 1000.
    BetweenTurns,
    /// The drain bound cut a running turn: retryable `response.failed`, then 1001.
    Cut,
    /// A new turn arrived while draining: retryable `response.failed`, then 1001.
    Refused,
}

/// Next upstream item for the relay loop, or the end of a shutdown cut.
enum Upstream<T> {
    Next(Option<T>),
    /// The flush window of a cut closed: finish the cut.
    CutDone,
}

/// Upstream frames for the relay loop. While a turn is being cut (`cut` is its flush
/// deadline), frames keep coming in order until the deadline, then [`Upstream::CutDone`].
async fn next_upstream<R>(rx: &mut R, cut: Option<tokio::time::Instant>) -> Upstream<R::Item>
where
    R: futures::Stream + Unpin,
{
    let Some(deadline) = cut else {
        return Upstream::Next(rx.next().await);
    };
    match tokio::time::timeout_at(deadline, rx.next()).await {
        Ok(item) => Upstream::Next(item),
        Err(_) => Upstream::CutDone,
    }
}

fn preparation_timeout(turn: Option<&mut Tracker>) {
    if let Some(t) = turn {
        t.fail_msg(
            "preparation_timeout",
            "websocket preparation budget exhausted across retries/account switches",
        );
        t.source(crate::errors::SRC_LOCAL);
    }
}

/// Send Close and wait for its reply within one bound, then drop both halves.
async fn drain_close<T, R, E>(mut tx: T, mut rx: R, grace: Duration)
where
    T: futures::Sink<WsMessage> + Unpin,
    R: futures::Stream<Item = Result<WsMessage, E>> + Unpin,
{
    // Closing the abandoned connection must not hold up preparing its replacement.
    let _ = tokio::time::timeout(grace, async {
        let _ = tx.send(WsMessage::Close(None)).await;
        while let Some(Ok(m)) = rx.next().await {
            if matches!(m, WsMessage::Close(_)) {
                break;
            }
        }
    })
    .await;
}

/// How an upstream read ended without a frame: `(diagnostic class, record code, message)`.
/// The peer dropping TCP without a close handshake (tungstenite reports a clean EOF as
/// `ResetWithoutClosingHandshake`) is `upstream_ws_eof`; a reset, TLS or protocol failure
/// while reading is `upstream_ws_read_error`. An upstream close frame is `upstream_ws_close`
/// and a failed forward of a client frame `upstream_ws_send_error` (both at their call site).
fn upstream_end(
    r: &Option<Result<WsMessage, tokio_tungstenite::tungstenite::Error>>,
) -> (&'static str, &'static str, String) {
    use tokio_tungstenite::tungstenite::error::{Error as E, ProtocolError};
    match r {
        None => (
            "eof",
            "upstream_ws_eof",
            "upstream websocket ended without a close frame (connection closed)".into(),
        ),
        Some(Err(
            e @ (E::ConnectionClosed
            | E::AlreadyClosed
            | E::Protocol(ProtocolError::ResetWithoutClosingHandshake)),
        )) => (
            "eof",
            "upstream_ws_eof",
            format!("upstream websocket ended without a close frame ({e})"),
        ),
        Some(Err(e)) => (
            "read_error",
            "upstream_ws_read_error",
            format!("upstream websocket read failed (transport error: {e})"),
        ),
        // Callers handle every message; kept total for the signature.
        Some(Ok(_)) => (
            "eof",
            "upstream_ws_eof",
            "upstream websocket ended without a close frame".into(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn close_drains_inflight_data_even_after_capacity_fuses_the_stream() {
        use tungstenite::protocol::{Role, WebSocketConfig};
        for capacity in [false, true] {
            let (server, client) = tokio::io::duplex(64);
            let mut cfg = WebSocketConfig::default();
            if capacity {
                cfg.max_message_size = Some(8);
                cfg.max_frame_size = Some(8);
            }
            let mut server = tokio_tungstenite::WebSocketStream::from_raw_socket(
                server,
                Role::Server,
                Some(cfg),
            )
            .await;
            let mut client =
                tokio_tungstenite::WebSocketStream::from_raw_socket(client, Role::Client, None)
                    .await;
            let code = if capacity { 1009 } else { 1011 };
            let closing = async {
                if capacity {
                    assert!(matches!(
                        server.next().await,
                        Some(Err(tungstenite::Error::Capacity(_)))
                    ));
                }
                close_downstream(&mut server, close_frame(code, "test")).await;
            };
            let sending = async {
                client
                    .send(Message::text("x".repeat(64 * 1024)))
                    .await
                    .unwrap();
                assert!(
                    matches!(client.next().await, Some(Ok(Message::Close(Some(f)))) if u16::from(f.code) == code)
                );
                client.flush().await.unwrap();
                drop(client);
            };
            tokio::time::timeout(Duration::from_secs(1), async {
                tokio::join!(closing, sending);
            })
            .await
            .expect("close must drain bytes while the client is still sending");
        }
    }

    #[test]
    fn upstream_end_splits_eof_from_read_error() {
        use tokio_tungstenite::tungstenite::error::{Error as E, ProtocolError};
        let eof = |r: Option<Result<WsMessage, E>>| upstream_end(&r);
        assert_eq!(eof(None).0, "eof");
        assert_eq!(eof(None).1, "upstream_ws_eof");
        assert_eq!(
            eof(Some(Err(E::Protocol(
                ProtocolError::ResetWithoutClosingHandshake
            ))))
            .1,
            "upstream_ws_eof"
        );
        assert_eq!(eof(Some(Err(E::ConnectionClosed))).1, "upstream_ws_eof");
        let reset = eof(Some(Err(E::Io(std::io::Error::from(
            std::io::ErrorKind::ConnectionReset,
        )))));
        assert_eq!((reset.0, reset.1), ("read_error", "upstream_ws_read_error"));
        assert!(reset
            .2
            .starts_with("upstream websocket read failed (transport error: "));
    }

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
        // Retain both dispatchers, as in server/tests/it/forward.rs: parallel tests
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
}
