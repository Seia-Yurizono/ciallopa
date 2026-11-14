use super::connect::connect;
use super::frames::{busy_frame, error_event, to_down, to_up, Head, Message};
use super::Ctx;
use crate::attempt::{quota_plan, Next, OverloadPlan, Policy};
use crate::budget::PreparationBudget;
#[cfg(test)]
use crate::classify::classify_observed;
use crate::classify::{classify, Class, Terminal};
use crate::timing;
use crate::usage::{endpoint_of, End, StreamObservation, Tracker};
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
    ClientClosed,
    WriteFailed,
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

struct Down {
    tx: DownTx,
    rx: DownRx,
    queued: Option<Message>,
}

enum Conn {
    Idle,
    Turn(Turn),
    Cutting {
        turn: Turn,
        until: tokio::time::Instant,
    },
}

impl Conn {
    fn turn(&self) -> Option<&Turn> {
        match self {
            Self::Idle => None,
            Self::Turn(t) | Self::Cutting { turn: t, .. } => Some(t),
        }
    }

    fn turn_mut(&mut self) -> Option<&mut Turn> {
        match self {
            Self::Idle => None,
            Self::Turn(t) | Self::Cutting { turn: t, .. } => Some(t),
        }
    }

    fn take_turn(&mut self) -> Option<Turn> {
        match std::mem::replace(self, Self::Idle) {
            Self::Idle => None,
            Self::Turn(t) | Self::Cutting { turn: t, .. } => Some(t),
        }
    }

    fn cut_until(&self) -> Option<tokio::time::Instant> {
        match self {
            Self::Cutting { until, .. } => Some(*until),
            _ => None,
        }
    }

    fn cut(&mut self, until: tokio::time::Instant) {
        if let Some(turn) = self.take_turn() {
            *self = Self::Cutting { turn, until };
        }
    }
}

struct Turn {
    _slot: ciallopa_upstream::drain::Lease,
    tracker: Option<Tracker>,
    budget: PreparationBudget,
    switches: usize,
    overload_waited: bool,
    progress: StreamObservation,
    phase: TurnPhase,
}

enum TurnPhase {
    Prelude {
        request: Option<Replayable>,
        held: Held,
    },
    Committed {
        // Keep the turn's budget reservation until terminal or the next client message.
        permit: Option<ciallopa_upstream::drain::BodyPermit>,
    },
}

struct Replayable {
    text: tungstenite::Utf8Bytes,
    _permit: ciallopa_upstream::drain::BodyPermit,
}

#[derive(Default)]
struct Held {
    frames: Vec<Message>,
    bytes: usize,
}

impl Held {
    fn push(&mut self, message: Message) {
        self.bytes += match &message {
            Message::Text(t) => t.len(),
            Message::Binary(b) => b.len(),
            _ => 0,
        };
        self.frames.push(message);
    }
}

impl Turn {
    fn request(&self) -> Option<tungstenite::Utf8Bytes> {
        match &self.phase {
            TurnPhase::Prelude {
                request: Some(r), ..
            } => Some(r.text.clone()),
            _ => None,
        }
    }

    fn discard_request(&mut self) {
        match &mut self.phase {
            TurnPhase::Prelude { request, .. } => *request = None,
            TurnPhase::Committed { permit } => *permit = None,
        }
    }

    fn held(&mut self) -> Vec<Message> {
        match &mut self.phase {
            TurnPhase::Prelude { held, .. } => std::mem::take(held).frames,
            TurnPhase::Committed { .. } => Vec::new(),
        }
    }

    fn commit(&mut self) -> Vec<Message> {
        match std::mem::replace(&mut self.phase, TurnPhase::Committed { permit: None }) {
            TurnPhase::Prelude { request, held } => {
                self.phase = TurnPhase::Committed {
                    permit: request.map(|r| r._permit),
                };
                held.frames
            }
            TurnPhase::Committed { permit } => {
                self.phase = TurnPhase::Committed { permit };
                Vec::new()
            }
        }
    }
}

enum Flow {
    Continue,
    Close(Close),
}

struct Session {
    st: AppState,
    ctx: Ctx,
    route: RouteRequest,
    up: UpLink,
    down: Down,
    state: Conn,
    max_switches: usize,
    idle: Duration,
    sleep: std::pin::Pin<Box<tokio::time::Sleep>>,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn relay(
    st: AppState,
    ctx: Ctx,
    route: RouteRequest,
    account: AccountId,
    down: DownWs,
    up: UpstreamWs,
    gen: u64,
    idle: Duration,
    handshake: HeaderMap,
    handoff: Option<ciallopa_upstream::drain::Lease>,
) {
    let _session = st.upstream.drain.enter();
    drop(handoff);
    let (tx, rx) = down.split();
    let max_switches = st.accounts.len().max(1);
    Session {
        st,
        ctx,
        route,
        up: UpLink::new(up, handshake, account, gen),
        down: Down {
            tx,
            rx,
            queued: None,
        },
        state: Conn::Idle,
        max_switches,
        idle,
        sleep: Box::pin(tokio::time::sleep(idle)),
    }
    .run()
    .await;
}

impl Session {
    async fn run(mut self) {
        let mut egress = self.st.upstream.egress_watch();
        let mut drain = self.st.upstream.drain.watch();
        drain.mark_changed();
        let reason = loop {
            let flow = tokio::select! {
                m = next_downstream(&mut self.down.rx, &mut self.down.queued), if self.state.cut_until().is_none() => self.on_client(m).await,
                m = next_upstream(&mut self.up.rx, self.state.cut_until()) => self.on_upstream(m).await,
                _ = egress.changed() => self.on_egress(*egress.borrow_and_update()),
                _ = drain.changed(), if self.state.cut_until().is_none() => self.on_drain(*drain.borrow_and_update()),
                _ = &mut self.sleep, if self.state.turn().is_some() => self.on_idle(),
            };
            if let Flow::Close(reason) = flow {
                break reason;
            }
        };
        self.finish_turn(End::ClientGone);
        close(
            reason,
            self.down.tx,
            self.down.rx,
            self.up,
            timing::scale(&self.st, Duration::from_secs(2)),
        )
        .await;
    }

    fn tracker(&self) -> Option<&Tracker> {
        self.state.turn().and_then(|t| t.tracker.as_ref())
    }
    fn tracker_mut(&mut self) -> Option<&mut Tracker> {
        self.state.turn_mut().and_then(|t| t.tracker.as_mut())
    }

    fn finish_turn(&mut self, end: End) {
        if let Some(mut turn) = self.state.take_turn() {
            if let Some(tracker) = turn.tracker.take() {
                tracker.finish(end);
            }
        }
    }

    fn fail(
        &mut self,
        code: impl Into<std::borrow::Cow<'static, str>>,
        source: &'static str,
        message: impl Into<String>,
    ) {
        self.finish_turn(End::Failed {
            code: code.into(),
            source,
            message: Some(message.into()),
        });
    }

    fn reset_idle(&mut self) {
        self.sleep
            .as_mut()
            .reset(tokio::time::Instant::now() + self.idle);
    }

    fn warn(&self, class: &'static str) {
        if let Some(turn) = self.state.turn() {
            turn.progress.warn(
                "ws",
                class,
                &self.up.account,
                self.up.request_id.as_deref(),
                turn.tracker.as_ref(),
            );
        }
    }

    fn note_refusal(
        &self,
        kind: ciallopa_upstream::drain::RefusalKind,
        limit: u64,
        bytes: Option<u64>,
        model: Option<&str>,
    ) {
        crate::refusal::note(
            &self.st,
            crate::refusal::Refusal {
                kind,
                ws: true,
                key: self.ctx.key.as_deref(),
                path: &self.ctx.path,
                headers: &self.ctx.headers,
                model: model.filter(|m| !m.is_empty()),
                account: Some(&self.up.account.to_string()),
                limit,
                bytes,
            },
        );
    }

    async fn send(&mut self, message: Message) -> Flow {
        let tracker = self.state.turn_mut().and_then(|t| t.tracker.as_mut());
        match send_down(&mut self.down.tx, message, tracker).await {
            Ok(()) => Flow::Continue,
            Err(_) => Flow::Close(Close::ClientGone),
        }
    }

    async fn flush_held(&mut self, commit: bool) -> Flow {
        let frames = self
            .state
            .turn_mut()
            .map(|t| if commit { t.commit() } else { t.held() })
            .unwrap_or_default();
        for frame in frames {
            if let Flow::Close(reason) = self.send(frame).await {
                return Flow::Close(reason);
            }
        }
        Flow::Continue
    }

    async fn redial(&mut self, why: Redial<'_>) -> Result<(), RedialFailed> {
        let turn = self.state.turn_mut().expect("redial needs a running turn");
        let mut model_route;
        let route = if matches!(why, Redial::ModelCooldown) {
            model_route = self.route.clone();
            if self.st.accounts.accounts().iter().any(|a| {
                a.id != self.up.account
                    && !a.disabled
                    && !model_route.exclude.contains(&a.id)
                    && self
                        .st
                        .router
                        .model_cooling_until(&a.id, &model_route.model)
                        .is_none()
            }) {
                model_route.exclude.push(self.up.account.clone());
            }
            &mut model_route
        } else {
            &mut self.route
        };
        let (hold, wait) = match why {
            Redial::ModelCooldown => (proxy::WS_HOLD_MAX, None),
            Redial::Overload { wait, .. } => (Duration::MAX, wait),
            Redial::Quota(_) => (Duration::MAX, None),
        };
        let (ws, headers, account, gen) = turn
            .budget
            .run(async {
                if let Some(wait) = wait {
                    tokio::time::sleep(wait).await;
                }
                connect(
                    &self.st,
                    &self.ctx.path,
                    &self.ctx.headers,
                    route,
                    hold,
                    None,
                )
                .await
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
        tracing::info!(from = %self.up.account, to = %replacement.account, code, "ws redialed before output");
        if let Some(tracker) = turn.tracker.as_mut() {
            tracker.obs_upstream_request_id(replacement.request_id.as_deref());
            tracker.obs_ws_negotiation(self.ctx.ws_ext_down.as_deref(), replacement.ext.as_deref());
            if !matches!(why, Redial::ModelCooldown) || replacement.account != self.up.account {
                tracker.replay(
                    &self.up.account,
                    code,
                    if replacement.account == self.up.account {
                        Next::WaitRetry
                    } else {
                        Next::Switch
                    },
                );
            }
            if replacement.account != self.up.account {
                tracker.switch_to(&replacement.account);
            }
            tracker.quota(&replacement.quota);
        }
        let old = std::mem::replace(&mut self.up, replacement);
        tokio::spawn(drain_close(
            old.tx,
            old.rx,
            timing::scale(&self.st, Duration::from_secs(2)),
        ));
        Ok(())
    }

    async fn replay(
        &mut self,
        request: tungstenite::Utf8Bytes,
        why: Redial<'_>,
    ) -> Result<(), RedialFailed> {
        let overload = matches!(why, Redial::Overload { .. });
        self.redial(why).await?;
        if overload {
            self.st.router.note_attempt(&self.up.account);
        }
        let turn = self.state.turn_mut().expect("replay needs a running turn");
        turn.held();
        match write_replay(
            self.up.tx.send(WsMessage::Text(request)),
            &mut self.down.rx,
            &mut self.down.queued,
            &self.st.upstream.drain,
            &mut turn.progress,
            &self.up.account,
            self.up.request_id.as_deref(),
            turn.tracker.as_mut(),
        )
        .await
        {
            WriteResult::Sent(Ok(())) => {
                self.reset_idle();
                Ok(())
            }
            WriteResult::ClientClosed => Err(RedialFailed::ClientClosed),
            WriteResult::Sent(Err(_)) => {
                if self.st.upstream.drain.phase() == Phase::Force {
                    self.finish_turn(End::Forced);
                }
                Err(RedialFailed::WriteFailed)
            }
        }
    }

    fn replay_failed(&mut self, failure: RedialFailed, code: &str, message: &str) -> Flow {
        match failure {
            RedialFailed::ClientClosed => Flow::Close(Close::Interrupted),
            RedialFailed::Budget => {
                self.fail(
                    "preparation_timeout",
                    crate::errors::SRC_LOCAL,
                    "websocket preparation budget exhausted across retries/account switches",
                );
                Flow::Close(Close::Replay)
            }
            RedialFailed::Refused(_) | RedialFailed::WriteFailed => {
                self.fail(code.to_owned(), crate::errors::SRC_STREAM, message);
                Flow::Close(Close::Replay)
            }
        }
    }

    async fn on_client(&mut self, item: Option<Result<Message, tungstenite::Error>>) -> Flow {
        let message = match item {
            Some(Ok(message)) => message,
            Some(Err(tungstenite::Error::Capacity(error))) => {
                tracing::warn!(account = %ciallopa_core::a(&self.up.account), %error, "websocket message over ws_max_message_mb; closing 1009");
                return Flow::Close(Close::TooBig);
            }
            _ => return Flow::Close(Close::Interrupted),
        };
        if matches!(message, Message::Close(_)) {
            return Flow::Close(Close::ClientGone);
        }
        let size = match &message {
            Message::Text(t) => Some(t.len()),
            Message::Binary(b) => Some(b.len()),
            _ => None,
        };
        let head = match &message {
            Message::Text(text) if self.state.turn().is_none() => Head::parse(text.as_str()),
            _ => None,
        };
        let mut permit;
        if let Some(size) = size {
            if let Some(turn) = self.state.turn_mut() {
                turn.discard_request();
            }
            permit = crate::reqbody::reserve_message(&self.st, size).await;
            if permit.is_none() {
                self.note_refusal(
                    ciallopa_upstream::drain::RefusalKind::WsMsg,
                    self.st
                        .config
                        .load()
                        .max_buffered_body_mb
                        .saturating_mul(1 << 20),
                    Some(size as u64),
                    Some(&self.route.model),
                );
                return match self.down.tx.send(busy_frame()).await {
                    Ok(()) => Flow::Continue,
                    Err(_) => Flow::Close(Close::ClientGone),
                };
            }
            if self.state.turn().is_none() {
                let limit = self.st.config.load().max_inflight_requests;
                let slot = match self.st.upstream.drain.admit(limit) {
                    Ok(slot) => slot,
                    Err(ciallopa_upstream::drain::Refused::Busy) => {
                        self.note_refusal(
                            ciallopa_upstream::drain::RefusalKind::Inflight,
                            limit as u64,
                            None,
                            head.as_ref()
                                .and_then(|h| h.model.as_ref())
                                .and_then(|m| m.as_str()),
                        );
                        return match self.down.tx.send(busy_frame()).await {
                            Ok(()) => Flow::Continue,
                            Err(_) => Flow::Close(Close::ClientGone),
                        };
                    }
                    Err(ciallopa_upstream::drain::Refused::Draining) => {
                        return Flow::Close(Close::Shutdown(ShutdownClose::Refused))
                    }
                };
                let request = match &message {
                    Message::Text(text) if head.as_ref().is_some_and(Head::replayable) => {
                        Some(Replayable {
                            text: text.clone(),
                            _permit: permit.take().expect("reserved message"),
                        })
                    }
                    _ => None,
                };
                let tracker = if matches!(&message, Message::Text(text) if text.contains("response.create"))
                {
                    if let Some(model) = head
                        .as_ref()
                        .and_then(|h| h.model.as_ref())
                        .and_then(|m| m.as_str())
                        .filter(|m| !m.is_empty())
                    {
                        self.route.model = model.to_owned();
                    }
                    endpoint_of(&self.ctx.path)
                        .and_then(|ep| {
                            Tracker::start(&self.st, Transport::Ws, ep, self.ctx.key.as_deref())
                        })
                        .map(|mut tr| {
                            let value = head.as_ref().map(Head::to_value);
                            tr.headers(&self.ctx.headers);
                            tr.meta(value.as_ref());
                            if value
                                .as_ref()
                                .and_then(|v| v.get("generate"))
                                .and_then(|g| g.as_bool())
                                == Some(false)
                            {
                                tr.prewarm();
                            }
                            tr.accounts(&[], Some(&self.up.account));
                            tr.quota(&self.up.quota);
                            tr.status(200);
                            tr.obs_upstream_request_id(self.up.request_id.as_deref());
                            tr.obs_ws_negotiation(
                                self.ctx.ws_ext_down.as_deref(),
                                self.up.ext.as_deref(),
                            );
                            tr
                        })
                } else {
                    None
                };
                self.state = Conn::Turn(Turn {
                    _slot: slot,
                    tracker,
                    budget: PreparationBudget::new(self.ctx.preparation_limit),
                    switches: 0,
                    overload_waited: false,
                    progress: StreamObservation::new(),
                    phase: TurnPhase::Prelude {
                        request,
                        held: Held::default(),
                    },
                });
                self.reset_idle();
                if matches!(&message, Message::Text(text) if text.contains("response.create")) {
                    if !self.route.model.is_empty()
                        && self
                            .st
                            .router
                            .model_cooling_until(&self.up.account, &self.route.model)
                            .is_some()
                    {
                        if self.state.turn().and_then(Turn::request).is_none() {
                            self.fail(
                                "model_cooldown",
                                crate::errors::SRC_LOCAL,
                                "account cooling down for this model; closed for client replay",
                            );
                            return Flow::Close(Close::Replay);
                        }
                        match self.redial(Redial::ModelCooldown).await {
                            Ok(()) => {}
                            Err(RedialFailed::Refused(response)) => {
                                let tracker =
                                    self.state.take_turn().and_then(|mut t| t.tracker.take());
                                return self.send(error_event(response, tracker).await).await;
                            }
                            Err(failure) => {
                                return self.replay_failed(
                                    failure,
                                    "model_cooldown",
                                    "account cooling down for this model",
                                )
                            }
                        }
                    }
                    self.st.router.note_attempt(&self.up.account);
                }
            }
        }
        if let Some(message) = to_up(message) {
            match write_with_client_close(
                self.up.tx.send(message),
                &mut self.down.rx,
                &mut self.down.queued,
            )
            .await
            {
                WriteResult::Sent(Ok(())) => {}
                WriteResult::ClientClosed => return Flow::Close(Close::Interrupted),
                WriteResult::Sent(Err(error)) => {
                    if self.st.upstream.drain.phase() != Phase::Force {
                        self.warn("write_error");
                        self.fail(
                            "upstream_ws_send_error",
                            crate::errors::SRC_NETWORK,
                            format!("upstream websocket send failed (transport error: {error})"),
                        );
                    }
                    return Flow::Close(Close::Internal("upstream gone"));
                }
            }
        }
        Flow::Continue
    }

    async fn on_upstream(&mut self, item: Upstream<Result<Message, tungstenite::Error>>) -> Flow {
        let item = match item {
            Upstream::Next(item) => item,
            Upstream::CutDone => {
                let _ = self.flush_held(false).await;
                self.fail(
                    "shutdown",
                    crate::errors::SRC_LOCAL,
                    "closed by server shutdown before the turn finished",
                );
                return Flow::Close(Close::Shutdown(ShutdownClose::Cut));
            }
        };
        self.reset_idle();
        let message = match item {
            Some(Ok(Message::Close(frame))) => {
                self.warn("close_frame");
                let code = frame.as_ref().map(|f| u16::from(f.code)).unwrap_or(1000);
                if code != 1000 {
                    self.st
                        .router
                        .report(&self.route, &self.up.account, &Outcome::Transport);
                }
                let _ = self.flush_held(false).await;
                let reason = frame.as_ref().map(|f| f.reason.as_str()).unwrap_or("");
                self.fail(
                    "upstream_ws_close",
                    crate::errors::SRC_STREAM,
                    format!("upstream websocket closed mid-turn (code {code}) {reason}").trim_end(),
                );
                return Flow::Close(Close::Upstream(frame));
            }
            Some(Ok(message)) => message,
            other => {
                let (class, code, message) = upstream_end(&other);
                self.warn(class);
                self.fail(code, crate::errors::SRC_NETWORK, message);
                self.st
                    .router
                    .report(&self.route, &self.up.account, &Outcome::Transport);
                return Flow::Close(Close::Internal("upstream disconnected"));
            }
        };
        if let Some(turn) = self.state.turn_mut() {
            turn.progress.activity();
            if let Some(tracker) = turn.tracker.as_mut() {
                tracker.upstream_activity();
                match &message {
                    Message::Text(t) => tracker.obs_up_bytes(t.len()),
                    Message::Binary(b) => tracker.obs_up_bytes(b.len()),
                    _ => {}
                }
            }
        }
        let mut classified = match &message {
            Message::Text(text) => Some(classify(None, text.as_str()).ws_view(text.as_str())),
            _ => None,
        };
        if let Some(ty) = classified.as_ref().and_then(|c| c.ty.as_deref()) {
            if let Some(turn) = self.state.turn_mut() {
                turn.progress.event(ty);
                if let Some(tracker) = turn.tracker.as_mut() {
                    tracker.obs_event(ty);
                }
            }
        }
        let class = classified
            .as_ref()
            .map(|c| &c.class)
            .unwrap_or(&Class::Preamble);
        let done = classified.as_ref().is_some_and(|c| c.ends_ws_turn());
        if let Class::RateLimits(Some(quota)) = class {
            self.st.router.report(
                &self.route,
                &self.up.account,
                &Outcome::Success {
                    quota: Some(quota.clone()),
                },
            );
            if let Message::Text(text) = &message {
                if let Some(headers) = parse_quota_event_headers(text.as_bytes()) {
                    if let Some(tracker) = self.tracker_mut() {
                        tracker.quota(&headers);
                    }
                    self.up.quota = headers;
                }
            }
        }
        if matches!(
            class,
            Class::SoftError | Class::Terminal(Terminal::Quota(_))
        ) {
            if let (Some(tracker), Message::Text(text)) = (self.tracker_mut(), &message) {
                tracker.error_text(text.as_bytes());
            }
        }
        if matches!(class, Class::SoftError) {
            tracing::warn!(account = %ciallopa_core::a(&self.up.account), record_id = self.tracker().map(Tracker::record_id), "upstream websocket error frame without status; forwarded, turn continues");
        }
        if let Class::Terminal(Terminal::Overloaded(code)) = class {
            let accounts = self.st.accounts.accounts();
            let others = proxy::others_usable(&self.st, &accounts, &self.route, &self.up.account);
            self.st.router.report(
                &self.route,
                &self.up.account,
                &proxy::overload_outcome(&self.st, others),
            );
            if let Some(request) = self.state.turn().and_then(Turn::request) {
                let turn = self.state.turn_mut().expect("replayable turn");
                let plan = Policy::ws_redial().overload_plan(
                    others,
                    turn.overload_waited,
                    &HeaderMap::new(),
                    self.st.config.load().upstream_opts.overload_retry_wait_secs,
                    turn.switches,
                    self.max_switches,
                );
                let wait = match plan {
                    OverloadPlan::WaitRetry(wait) => Some(timing::scale(&self.st, wait)),
                    _ => None,
                };
                if plan != OverloadPlan::PassThrough {
                    if wait.is_some() {
                        turn.overload_waited = true;
                    } else {
                        turn.switches += 1;
                        self.route.exclude.push(self.up.account.clone());
                    }
                    return match self.replay(request, Redial::Overload { code, wait }).await {
                        Ok(()) => Flow::Continue,
                        Err(failure) => self.replay_failed(
                            failure,
                            code,
                            "upstream overloaded; redial failed, closed for client replay",
                        ),
                    };
                }
            }
            if let Some(tracker) = self.tracker_mut() {
                if let Message::Text(text) = &message {
                    tracker.error_text(text.as_bytes());
                }
                tracker.fail_msg(code, "upstream overloaded");
                tracker.source(crate::errors::SRC_STREAM);
            }
        }
        if let Class::Terminal(Terminal::Quota(outcome)) = class {
            self.st
                .router
                .report(&self.route, &self.up.account, outcome);
            if let Some(turn) = self.state.turn_mut() {
                if quota_plan(&mut turn.switches, self.max_switches) == Next::Switch {
                    if let Some(request) = turn.request() {
                        self.route.exclude.push(self.up.account.clone());
                        return match self.replay(request, Redial::Quota(outcome)).await {
                            Ok(()) => Flow::Continue,
                            Err(failure) => self.replay_failed(
                                failure,
                                crate::errors::quota_outcome_code(outcome),
                                "usage limit reached mid-turn; closed for client replay",
                            ),
                        };
                    }
                }
            }
            self.fail(
                crate::errors::quota_outcome_code(outcome),
                crate::errors::SRC_STREAM,
                "usage limit reached mid-turn; closed for client replay",
            );
            return Flow::Close(Close::Replay);
        }
        let preamble = matches!(
            class,
            Class::Preamble | Class::Created | Class::RateLimits(_) | Class::SoftError
        ) && !done;
        let content = matches!(
            class,
            Class::Output
                | Class::Terminal(
                    Terminal::Completed | Terminal::Incomplete | Terminal::Failed | Terminal::Error
                )
        );
        let encrypted = matches!(class, Class::Terminal(Terminal::Encrypted(_)));
        if let (Some(tracker), Some(classified), Message::Text(text)) =
            (self.tracker_mut(), classified.as_mut(), &message)
        {
            if !done && classified.class == Class::Created {
                if let Some(value) = classified.value(text.as_str()) {
                    tracker.created_event(value);
                }
            }
        }
        let rewritten = if encrypted {
            if let (Some(classified), Message::Text(text)) = (classified.as_mut(), &message) {
                classified
                    .value(text.as_str())
                    .map(crate::encrypted::ws_frame)
            } else {
                None
            }
        } else {
            None
        };
        if preamble {
            if let Some(Turn {
                phase: TurnPhase::Prelude { held, .. },
                ..
            }) = self.state.turn_mut()
            {
                if let Some(frame) = to_down(message) {
                    held.push(frame);
                }
                if held.bytes > proxy::BOOTSTRAP_MAX {
                    return self.flush_held(true).await;
                }
                return Flow::Continue;
            }
        }
        if content
            && !done
            && matches!(
                self.state.turn().map(|t| &t.phase),
                Some(TurnPhase::Prelude { .. })
            )
        {
            self.st.router.content_ok(&self.up.account);
        }
        if let Flow::Close(reason) = self.flush_held(true).await {
            return Flow::Close(reason);
        }
        if done {
            if let (Some(tracker), Some(classified), Message::Text(text)) =
                (self.tracker_mut(), classified.as_mut(), &message)
            {
                if let Some(value) = classified.value(text.as_str()) {
                    tracker.terminal_event(value);
                }
            }
        }
        let mut finished = if done {
            self.state.take_turn().and_then(|mut t| t.tracker.take())
        } else {
            None
        };
        if let Some(tracker) = finished.as_mut() {
            tracker.freeze_duration();
        }
        let message = rewritten.map(Message::text).unwrap_or(message);
        let sent = match to_down(message) {
            Some(message) => {
                let tracker = finished
                    .as_mut()
                    .or_else(|| self.state.turn_mut().and_then(|t| t.tracker.as_mut()));
                send_down(&mut self.down.tx, message, tracker).await.is_ok()
            }
            None => true,
        };
        if let Some(tracker) = finished {
            tracker.finish(if sent {
                End::Completed
            } else {
                End::ClientGone
            });
        }
        if !sent {
            return Flow::Close(Close::ClientGone);
        }
        if done {
            if self.st.upstream.egress_gen() != self.up.gen {
                return Flow::Close(Close::Egress);
            }
            if self.st.upstream.drain.phase() != Phase::Running {
                return Flow::Close(Close::Shutdown(ShutdownClose::BetweenTurns));
            }
        }
        Flow::Continue
    }

    fn on_egress(&self, generation: u64) -> Flow {
        if self.state.turn().is_none() && generation != self.up.gen {
            Flow::Close(Close::Egress)
        } else {
            Flow::Continue
        }
    }

    fn on_drain(&mut self, phase: Phase) -> Flow {
        match (phase, self.state.turn().is_some()) {
            (Phase::Force, true) => {
                self.state
                    .cut(tokio::time::Instant::now() + timing::scale(&self.st, CUT_FLUSH_MAX));
                Flow::Continue
            }
            (Phase::Force | Phase::Draining, false) => {
                Flow::Close(Close::Shutdown(ShutdownClose::BetweenTurns))
            }
            _ => Flow::Continue,
        }
    }

    fn on_idle(&mut self) -> Flow {
        self.warn("idle_timeout");
        self.fail(
            "idle_timeout",
            crate::errors::SRC_NETWORK,
            format!(
                "upstream websocket idle for {}s (timeout)",
                self.idle.as_secs()
            ),
        );
        self.st
            .router
            .report(&self.route, &self.up.account, &Outcome::Transport);
        Flow::Close(Close::Internal("stream idle timeout"))
    }
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
    if matches!(&result, WriteResult::Sent(Err(_))) && drain.phase() != Phase::Force {
        progress.warn(
            "ws",
            "write_error",
            account,
            upstream_request_id,
            tracker.as_deref(),
        );
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
