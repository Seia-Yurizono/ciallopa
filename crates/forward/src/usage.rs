//! Per-request/turn usage: Drop submits without waiting for persistence.
use crate::AppState;
use ciallopa_core::AccountId;
use ciallopa_usage::{
    detail::keep_header, observation::clean_id, ErrorDetail, Observation, QuotaObs, Record,
    Recorder, Transport,
};
use http::HeaderMap;
use serde_json::Value;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) struct Tracker {
    pub(crate) rec: Record,
    sink: Arc<Recorder>,
    pub(crate) start: Instant,
    outcome: Outcome,
    response_meta: ResponseMeta,
    drain: Arc<ciallopa_upstream::drain::Drain>,
    emit: Emit,
    /// WS turn replays (switch / wait+retry); allocated on the first replay only, so a turn
    /// without one stores no attempt chain.
    replays: Option<crate::errors::Attempts>,
    /// Silence at cancellation distinguishes client idle timeout from user cancellation.
    last_upstream: Option<Instant>,
    obs: Observation,
    credentials: Option<Duration>,
    waits: Option<Duration>,
    /// Freeze before the terminal WS send to exclude downstream wait.
    frozen_ms: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Open,
    Completed,
    Settled,
}

enum Emit {
    OnDrop,
    ViaBody(Arc<crate::downstream::DownLink>),
    Never,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResponseMeta {
    None,
    Provisional,
    Final,
}

#[derive(Clone)]
pub(crate) enum End {
    Completed,
    ClientGone,
    Forced,
    Failed {
        code: Cow<'static, str>,
        source: &'static str,
        message: Option<String>,
    },
    ObservationIncomplete,
}

#[derive(Clone, Copy)]
pub(crate) struct Started {
    id: i64,
    at: Instant,
}

/// SSE comments and WS ping/pong are activity; only complete envelopes are events.
#[derive(Clone, Copy)]
pub(crate) struct StreamObservation {
    started: Instant,
    last_activity: Option<Instant>,
    last_event_at: Option<Instant>,
    last_event: Option<&'static str>,
    pub(crate) events: u64,
    terminal: bool,
    observation_limited: bool,
}

impl StreamObservation {
    pub(crate) fn new() -> Self {
        Self {
            started: Instant::now(),
            last_activity: None,
            last_event_at: None,
            last_event: None,
            events: 0,
            terminal: false,
            observation_limited: false,
        }
    }

    pub(crate) fn activity(&mut self) {
        self.last_activity = Some(Instant::now());
    }

    pub(crate) fn limit_reached(&mut self) {
        self.observation_limited = true;
    }

    pub(crate) fn event(&mut self, ty: &str) {
        self.events = self.events.saturating_add(1);
        self.last_event_at = Some(Instant::now());
        self.terminal |= is_terminal_type(ty);
        // Never reflect an arbitrary upstream type string into process logs.
        self.last_event = Some(match ty {
            "response.created" => "response.created",
            "response.in_progress" => "response.in_progress",
            "response.queued" => "response.queued",
            "response.metadata" => "response.metadata",
            "response.output_item.added" => "response.output_item.added",
            "response.output_item.done" => "response.output_item.done",
            "response.content_part.added" => "response.content_part.added",
            "response.content_part.done" => "response.content_part.done",
            "response.output_text.delta" => "response.output_text.delta",
            "response.output_text.done" => "response.output_text.done",
            "response.reasoning_text.delta" => "response.reasoning_text.delta",
            "response.reasoning_summary_text.delta" => "response.reasoning_summary_text.delta",
            "response.function_call_arguments.delta" => "response.function_call_arguments.delta",
            "response.function_call_arguments.done" => "response.function_call_arguments.done",
            "response.completed" => "response.completed",
            "response.done" => "response.done",
            "response.failed" => "response.failed",
            "response.incomplete" => "response.incomplete",
            "error" => "error",
            "service_unavailable_error" => "service_unavailable_error",
            "codex.rate_limits" => "codex.rate_limits",
            "codex.response.metadata" => "codex.response.metadata",
            "responsesapi.websocket_timing" => "responsesapi.websocket_timing",
            "keepalive" => "keepalive",
            _ => "unknown",
        });
    }

    /// Warn with static metadata only; never log upstream payloads or close reasons.
    pub(crate) fn warn(
        &self,
        transport: &'static str,
        error_class: &'static str,
        account: &str,
        upstream_request_id: Option<&str>,
        tracker: Option<&Tracker>,
    ) {
        if self.terminal {
            return;
        }
        tracing::warn!(
            transport,
            error_class,
            account,
            upstream_request_id,
            record_id = tracker.map(Tracker::record_id),
            thread_id = tracker.and_then(|t| t.rec.thread_id.as_deref()),
            turn_id = tracker.and_then(|t| t.rec.turn_id.as_deref()),
            terminal_received = self.terminal,
            observation_limited = self.observation_limited,
            event_count = self.events,
            last_event = self.last_event,
            activity_received = self.last_activity.is_some(),
            activity_idle_ms = elapsed_ms(self.last_activity.unwrap_or(self.started)),
            event_idle_ms = elapsed_ms(self.last_event_at.unwrap_or(self.started)),
            "upstream stream ended without an observed terminal event"
        );
    }
}

impl Started {
    pub(crate) fn capture(st: &AppState) -> Option<Self> {
        Some(Self {
            id: st.usage.as_ref()?.ids.next(),
            at: Instant::now(),
        })
    }
}

pub(crate) fn endpoint_of(path: &str) -> Option<&'static str> {
    let p = path.split('?').next().unwrap_or(path);
    Some(match p {
        "/codex/responses" => "responses",
        "/codex/responses/compact" => "compact",
        _ => return None,
    })
}

impl Tracker {
    pub(crate) fn record_id(&self) -> String {
        format!("{:016x}", self.rec.id)
    }

    pub(crate) fn start_at(
        st: &AppState,
        transport: Transport,
        endpoint: &'static str,
        key: Option<&str>,
        started: Started,
    ) -> Option<Self> {
        let sink = st.usage.clone()?;
        let mut rec = Record::new(started.id, transport, endpoint);
        rec.key = key.map(str::to_string);
        Some(Self {
            rec,
            sink,
            start: started.at,
            outcome: Outcome::Open,
            response_meta: ResponseMeta::None,
            drain: st.upstream.drain.clone(),
            emit: Emit::OnDrop,
            replays: None,
            last_upstream: None,
            obs: Observation::default(),
            credentials: None,
            waits: None,
            frozen_ms: None,
        })
    }

    pub(crate) fn prep(&mut self, at: Instant) {
        self.obs
            .prep_ms
            .get_or_insert_with(|| crate::downstream::ms_since(self.start, at));
    }

    pub(crate) fn reset_headers(&mut self) {
        self.obs.headers_at_ms = None;
        self.obs.send_to_headers_ms = None;
    }

    pub(crate) fn received_headers(&mut self, sent: Instant) {
        let now = Instant::now();
        self.obs.headers_at_ms = Some(crate::downstream::ms_since(self.start, now));
        self.obs.send_to_headers_ms = Some(crate::downstream::ms_since(sent, now));
    }

    pub(crate) async fn credential_time<T>(
        tracker: Option<&mut Self>,
        future: impl std::future::Future<Output = T>,
    ) -> T {
        crate::timing::accumulate(tracker.map(|t| &mut t.credentials), future).await
    }

    pub(crate) async fn wait(tracker: Option<&mut Self>, delay: Duration) {
        crate::timing::accumulate(tracker.map(|t| &mut t.waits), tokio::time::sleep(delay)).await;
    }

    /// Discard successful handshakes: turns own usage, but preparation cancellation remains observable.
    pub(crate) fn discard(mut self) {
        self.emit = Emit::Never;
    }

    /// Codex `thread-id` header and `x-codex-turn-metadata` (JSON: `thread_id`, `turn_id`).
    pub(crate) fn headers(&mut self, h: &HeaderMap) {
        let get = |k: &str| h.get(k).and_then(|v| v.to_str().ok());
        if let Some(t) = get("thread-id").and_then(short) {
            self.rec.thread_id = Some(t);
        }
        if let Some(m) = get("x-codex-turn-metadata") {
            self.turn_metadata(m);
        }
    }

    fn turn_metadata(&mut self, json: &str) {
        let Ok(v) = serde_json::from_str::<Value>(json) else {
            return;
        };
        let s = |k: &str| v.get(k).and_then(|x| x.as_str()).and_then(short);
        if let Some(t) = s("thread_id") {
            self.rec.thread_id = Some(t);
        }
        if let Some(t) = s("turn_id") {
            self.rec.turn_id = Some(t);
        }
    }

    /// `model` / `reasoning.effort` / `service_tier` / `client_metadata` ids from a Responses
    /// request body or WS `response.create` (body values win over handshake headers).
    pub(crate) fn meta(&mut self, body: Option<&Value>) {
        let Some(b) = body else { return };
        if let Some(t) = b
            .get("service_tier")
            .and_then(Value::as_str)
            .and_then(short)
        {
            let t = if t.eq_ignore_ascii_case("fast") {
                "priority".to_string()
            } else {
                t
            };
            self.rec.req_tier = Some(t.clone());
            self.rec.service_tier = Some(t);
        }
        if let Some(cm) = b.get("client_metadata") {
            if let Some(t) = cm.get("thread_id").and_then(|v| v.as_str()).and_then(short) {
                self.rec.thread_id = Some(t);
            }
            if let Some(m) = cm.get("x-codex-turn-metadata").and_then(|v| v.as_str()) {
                self.turn_metadata(m);
            }
        }
        if let Some(m) = b.get("model").and_then(|v| v.as_str()) {
            self.rec.model = Some(m.to_string());
        }
        if let Some(e) = b.pointer("/reasoning/effort").and_then(|v| v.as_str()) {
            self.rec.effort = Some(e.to_string());
        }
    }

    pub(crate) fn accounts(&mut self, tried: &[AccountId], fin: Option<&AccountId>) {
        self.rec.switches = tried.len() as u32;
        let first = tried.first().or(fin);
        self.rec.account = fin.cloned().or_else(|| tried.last().cloned());
        if first != self.rec.account.as_ref() {
            self.rec.first_account = first.cloned();
        }
    }

    /// Record pre-output replay; reset observations for the final attempt.
    pub(crate) fn replay(&mut self, account: &str, code: &str, action: crate::attempt::Next) {
        let chain = self.replays.get_or_insert_with(|| crate::errors::Attempts {
            start: self.start,
            ..Default::default()
        });
        let mut a = crate::errors::attempt(account, "stream");
        a.code = Some(code.to_string());
        a.action = action.into();
        chain.push(a);
        // Upstream observation describes the final attempt only.
        self.reset_upstream_obs();
    }

    fn reset_upstream_obs(&mut self) {
        let o = &mut self.obs;
        o.up_first_text_ms = None;
        o.up_first_reasoning_ms = None;
        o.up_first_tool_ms = None;
        o.up_last_event_ms = None;
        o.up_terminal_ms = None;
        o.up_events = None;
        o.up_bytes = None;
        o.response_id = None;
    }

    /// Observe the event and report whether it carries this turn/request's first output.
    pub(crate) fn obs_event(&mut self, ty: &str) -> bool {
        let now = elapsed_ms(self.start);
        let o = &mut self.obs;
        o.up_events = Some(o.up_events.unwrap_or(0).saturating_add(1));
        o.up_last_event_ms = Some(now);
        if crate::downstream::is_text_type(ty) {
            o.up_first_text_ms.get_or_insert(now);
        } else if crate::downstream::is_reasoning_type(ty) {
            o.up_first_reasoning_ms.get_or_insert(now);
        } else if crate::downstream::is_tool_type(ty) {
            o.up_first_tool_ms.get_or_insert(now);
        }
        if is_terminal_type(ty) {
            o.up_terminal_ms.get_or_insert(now);
        }
        let first =
            self.rec.ttft_ms.is_none() && self.outcome == Outcome::Open && is_output_type(ty);
        if first {
            self.rec.ttft_ms = Some(now);
        }
        first
    }

    pub(crate) fn obs_up_bytes(&mut self, n: usize) {
        let o = &mut self.obs;
        o.up_bytes = Some(o.up_bytes.unwrap_or(0).saturating_add(n as u64));
    }

    pub(crate) fn obs_upstream_request_id(&mut self, id: Option<&str>) {
        self.obs.upstream_request_id = id.and_then(clean_id);
    }

    fn obs_response_id(&mut self, r: &Value) {
        if let Some(id) = r.get("id").and_then(|v| v.as_str()).and_then(clean_id) {
            self.obs.response_id = Some(id);
        }
    }

    /// WS: one downstream `sink.send` for this turn finished (`ok`), with its Pending time.
    pub(crate) fn obs_ws_sent(
        &mut self,
        bytes: usize,
        blocked: Option<std::time::Duration>,
        ok: bool,
        first_content: bool,
    ) {
        if !ok {
            self.obs.down_incomplete = Some(true);
            return;
        }
        let now = elapsed_ms(self.start);
        let b = blocked.map(crate::downstream::us).unwrap_or(0);
        let o = &mut self.obs;
        if first_content {
            o.down_first_content_ms = Some(now);
        }
        o.ws_first_send_ms.get_or_insert(now);
        o.ws_last_send_ms = Some(now);
        o.ws_sink_blocked_us = Some(o.ws_sink_blocked_us.unwrap_or(0).saturating_add(b));
        o.ws_sink_blocked_max_us = Some(o.ws_sink_blocked_max_us.unwrap_or(0).max(b));
        o.ws_send_msgs = Some(o.ws_send_msgs.unwrap_or(0).saturating_add(1));
        o.ws_send_bytes = Some(o.ws_send_bytes.unwrap_or(0).saturating_add(bytes as u64));
        o.down_incomplete.get_or_insert(false);
    }

    pub(crate) fn obs_ws_negotiation(&mut self, down: Option<&str>, up: Option<&str>) {
        (self.obs.ws_deflate_down, self.obs.ws_ext_down) = crate::downstream::negotiation(down);
        (self.obs.ws_deflate_up, self.obs.ws_ext_up) = crate::downstream::negotiation(up);
    }

    pub(crate) fn freeze_duration(&mut self) {
        self.frozen_ms.get_or_insert_with(|| elapsed_ms(self.start));
    }

    pub(crate) fn defer_to(&mut self, link: Arc<crate::downstream::DownLink>) {
        self.emit = Emit::ViaBody(link);
    }

    pub(crate) fn switch_to(&mut self, acct: &AccountId) {
        self.rec.switches += 1;
        if self.rec.first_account.is_none() {
            self.rec.first_account = self.rec.account.clone();
        }
        self.rec.account = Some(acct.clone());
    }

    /// Latest upstream quota snapshot for this request/turn (headers or `codex.rate_limits`).
    pub(crate) fn quota(&mut self, h: &HeaderMap) {
        if let Some(q) = quota_obs(h) {
            self.rec.quota = Some(q);
        }
    }

    pub(crate) fn finish(mut self, end: End) {
        self.conclude(end);
    }

    pub(crate) fn conclude(&mut self, end: End) {
        match end {
            End::Completed if self.outcome != Outcome::Settled => {
                self.outcome = Outcome::Completed;
            }
            End::ClientGone if self.outcome == Outcome::Open => {
                if self.drain.phase() == ciallopa_upstream::drain::Phase::Force {
                    self.conclude(End::Forced);
                } else {
                    cancel_record(&mut self.rec, self.start, self.last_upstream, false);
                }
            }
            End::Forced if self.outcome == Outcome::Open => {
                cancel_record(&mut self.rec, self.start, self.last_upstream, true);
            }
            End::Failed {
                code,
                source,
                message,
            } => {
                if let Some(message) = message {
                    self.fail_msg(&code, message);
                } else {
                    self.fail(&code);
                }
                self.source(source);
            }
            End::ObservationIncomplete
                if self.outcome == Outcome::Open
                    && self.rec.error.is_none()
                    && self.rec.status < 400 =>
            {
                self.fail_msg(
                    "observation_incomplete",
                    "response transfer ended, but an event exceeded the observation limit; final outcome is unknown",
                );
                self.source(crate::errors::SRC_LOCAL);
            }
            _ => {}
        }
    }

    /// Buffered responses are complete; WS handshake/turn errors keep their transport.
    pub(crate) fn record_response(&mut self, parts: &http::response::Parts, body: &[u8]) {
        self.rec.status = parts.status.as_u16();
        if self.rec.transport != Transport::Ws {
            self.rec.transport = Transport::Http;
        }
        self.conclude(End::Completed);
        self.error_headers(&parts.headers);
        if let Some(meta) = parts.extensions.get::<crate::errors::ErrMeta>() {
            self.meta_err(meta);
        }
        self.error_body(body);
    }

    pub(crate) fn upstream_activity(&mut self) {
        self.last_upstream = Some(Instant::now());
    }

    pub(crate) fn ttft(&mut self) {
        if self.rec.ttft_ms.is_none() {
            self.rec.ttft_ms = Some(elapsed_ms(self.start));
        }
    }

    /// First error wins (later ones are usually consequences).
    pub(crate) fn fail(&mut self, kind: &str) {
        if self.outcome == Outcome::Settled {
            return;
        }
        if self.rec.error.is_none() {
            self.rec.error = Some(kind.to_string());
        }
    }

    /// `fail` plus a human-readable message for the error detail (first message wins).
    pub(crate) fn fail_msg(&mut self, kind: &str, msg: impl Into<String>) {
        if self.outcome == Outcome::Settled {
            return;
        }
        self.fail(kind);
        let d = self.detail();
        if d.message.is_none() {
            d.message = Some(msg.into());
        }
    }

    fn detail(&mut self) -> &mut ErrorDetail {
        self.rec.detail.get_or_insert_with(Default::default)
    }

    /// Where the (first) error came from; first one wins.
    pub(crate) fn source(&mut self, src: &'static str) {
        if self.outcome == Outcome::Settled {
            return;
        }
        if self.rec.err_source.is_none() {
            self.rec.err_source = Some(src);
        }
    }

    pub(crate) fn upstream_error(&mut self, status: u16, h: &HeaderMap) {
        self.rec.upstream_status = Some(status);
        self.source(crate::errors::SRC_UPSTREAM);
        if let Some(id) = crate::errors::request_id(h) {
            self.detail().upstream_request_id = Some(id);
        }
    }

    /// Keep the upstream encrypted-history rejection and original event in the log.
    pub(crate) fn encrypted_rejected(&mut self, msg: &str, event: &[u8], h: &HeaderMap) {
        self.fail_msg(crate::encrypted::CODE, msg);
        self.source(crate::errors::SRC_UPSTREAM);
        self.error_text(event);
        if let Some(id) = crate::errors::request_id(h) {
            self.detail().upstream_request_id = Some(id);
        }
    }

    pub(crate) fn meta_err(&mut self, m: &crate::errors::ErrMeta) {
        if self.outcome == Outcome::Settled {
            return;
        }
        self.source(m.source);
        if self.rec.error.is_none() {
            self.rec.error = Some(m.code.clone());
        }
        if m.upstream_status.is_some() {
            self.rec.upstream_status = m.upstream_status;
        }
        if let Some(id) = &m.upstream_request_id {
            self.detail().upstream_request_id = Some(id.clone());
        }
        if let Some(msg) = &m.message {
            self.detail().message = Some(msg.clone());
        }
    }

    /// Attempt chain; stored only when something other than one clean success happened.
    pub(crate) fn attempts(&mut self, a: &crate::errors::Attempts) {
        if a.eventful() {
            self.detail().attempts = Some(a.to_json());
        }
        // Last upstream status seen, for "502←上游 503"-style display.
        if let Some(s) = a.list.iter().rev().find_map(|x| x.status) {
            if s >= 400 {
                self.rec.upstream_status = Some(s);
            }
        }
    }

    /// Selected upstream response headers (only kept if the request ends up failed).
    pub(crate) fn error_headers(&mut self, h: &HeaderMap) {
        let d = self.detail();
        d.headers.clear();
        for (k, v) in h {
            if keep_header(k.as_str()) {
                d.add_header(k.as_str(), &String::from_utf8_lossy(v.as_bytes()));
            }
        }
    }

    pub(crate) fn error_body(&mut self, b: &[u8]) {
        self.detail().push_body(b);
    }

    pub(crate) fn error_text(&mut self, b: &[u8]) {
        self.detail().set_body(b);
    }

    /// `response.created`: provisional response model / effort / tier (terminal events win).
    pub(crate) fn created_event(&mut self, v: &Value) {
        if self.response_meta != ResponseMeta::None || self.outcome != Outcome::Open {
            return;
        }
        self.response_meta = ResponseMeta::Provisional;
        if let Some(r) = v.get("response") {
            self.obs_response_id(r);
            self.response_meta(r, ResponseMeta::Provisional);
        }
    }

    fn response_meta(&mut self, r: &Value, stage: ResponseMeta) {
        let s = |k: &str| r.get(k).and_then(|v| v.as_str()).and_then(short);
        if let Some(m) = s("model") {
            if stage == ResponseMeta::Final || self.rec.resp_model.is_none() {
                self.rec.resp_model = Some(m);
            }
        }
        if let Some(e) = r
            .pointer("/reasoning/effort")
            .and_then(|v| v.as_str())
            .and_then(short)
        {
            if stage == ResponseMeta::Final || self.rec.resp_effort.is_none() {
                self.rec.resp_effort = Some(e);
            }
        }
        // `auto` is a request-side alias, never the served tier.
        if let Some(t) = s("service_tier").filter(|t| !t.eq_ignore_ascii_case("auto")) {
            if stage == ResponseMeta::Final || self.response_meta != ResponseMeta::Final {
                self.rec.service_tier = Some(t);
            }
        }
    }

    /// Keep the incomplete reason when the caller constructs its own response.
    pub(crate) fn incomplete(&mut self, v: &Value) {
        if self.outcome != Outcome::Settled {
            self.rec.incomplete = Some(incomplete_reason(v));
        }
    }

    pub(crate) fn terminal_event(&mut self, v: &Value) {
        if self.outcome == Outcome::Settled {
            return;
        }
        self.terminal_outcome(v);
        self.outcome = Outcome::Settled;
        self.response_meta = ResponseMeta::Final;
    }

    fn terminal_outcome(&mut self, v: &Value) {
        let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
        let resp = v.get("response");
        if let Some(u) = resp.and_then(|r| r.get("usage")) {
            let n = |v: Option<&Value>| v.and_then(|x| x.as_i64());
            self.rec.input_tokens = n(u.get("input_tokens"));
            self.rec.output_tokens = n(u.get("output_tokens"));
            self.rec.cached_tokens = n(u.pointer("/input_tokens_details/cached_tokens"));
            self.rec.reasoning_tokens = n(u.pointer("/output_tokens_details/reasoning_tokens"));
            self.rec.cache_write_tokens = n(u.pointer("/input_tokens_details/cache_write_tokens"));
        }
        // The tier actually used (e.g. `priority` may be downgraded) wins over the request's.
        if let Some(r) = resp {
            self.obs_response_id(r);
            self.response_meta(r, ResponseMeta::Final);
        }
        match ty {
            "response.completed" | "response.done" => {}
            // Terminal for Codex (bc9cad3) but not a proxy/upstream failure: keep tokens, flag it.
            "response.incomplete" => self.rec.incomplete = Some(incomplete_reason(v)),
            _ => {
                let text = v.to_string();
                // A request-content problem the upstream reports in-stream: upstream source.
                if let Some(msg) = crate::encrypted::rejection(v) {
                    self.encrypted_rejected(&msg, text.as_bytes(), &HeaderMap::new());
                    return;
                }
                let msg = v
                    .pointer("/error/message")
                    .or_else(|| v.pointer("/response/error/message"))
                    .and_then(|m| m.as_str())
                    .map(str::to_string);
                let d = self.detail();
                d.set_body(text.as_bytes());
                if d.message.is_none() {
                    d.message = msg.or_else(|| Some(format!("upstream event {ty}")));
                }
                self.source(crate::errors::SRC_STREAM);
                // Same split as the router outcome: quota code or usage-limit text cools
                // the account (`usage_limit`); any other 429 is a rate limit.
                if crate::classify::quota_event(v).0
                    && ciallopa_routing::is_usage_limit(text.as_bytes())
                {
                    self.fail("usage_limit");
                } else {
                    self.fail(&crate::errors::event_code(v));
                }
            }
        }
    }

    /// Only created/terminal events need a JSON tree; the forwarder owns the bounded event buffer.
    pub(crate) fn sse_event(&mut self, text: &str, ty: Option<&str>) {
        let Some(ty) = ty else { return };
        self.obs_event(ty);
        if self.outcome != Outcome::Open {
            return;
        }
        if is_terminal_type(ty) {
            if let Ok(v) = serde_json::from_str(text) {
                self.terminal_event(&v);
            }
        } else if self.response_meta == ResponseMeta::None && ty == "response.created" {
            if let Ok(v) = serde_json::from_str(text) {
                self.created_event(&v);
            }
        }
    }
}

/// Read only the top-level type so quoted/nested strings have no protocol meaning.
pub(crate) fn event_type(text: &str) -> Option<Cow<'_, str>> {
    #[derive(serde::Deserialize)]
    struct Envelope<'a> {
        #[serde(rename = "type", borrow)]
        ty: Cow<'a, str>,
    }
    serde_json::from_str::<Envelope<'_>>(text)
        .ok()
        .map(|v| v.ty)
}

#[cfg(test)]
fn is_output_event(ev: &[u8]) -> bool {
    std::str::from_utf8(ev)
        .ok()
        .and_then(event_type)
        .is_some_and(|ty| is_output_type(&ty))
}

/// TTFT starts on generated deltas or output_item.done, excluding protocol metadata.
fn is_output_type(ty: &str) -> bool {
    !ty.starts_with("codex.")
        && !ty.starts_with("responsesapi.")
        && (ty.ends_with(".delta") || ty == "response.output_item.done")
}

pub(crate) fn is_terminal_type(ty: &str) -> bool {
    matches!(
        ty,
        "response.completed"
            | "response.done"
            | "response.failed"
            | "response.incomplete"
            | "error"
            | "service_unavailable_error"
    )
}

/// `x-codex-*` headers → snapshot. A window without `window-minutes` whose reset is "now"
/// (free plans send `secondary-window-minutes: 0`, `reset-after-seconds: 0`) is treated as absent.
pub(crate) fn quota_obs(h: &HeaderMap) -> Option<QuotaObs> {
    let now = std::time::SystemTime::now();
    let snap = ciallopa_routing::parse_quota_at(h, now)?;
    let secs = |t: std::time::SystemTime| {
        t.duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|d| d.as_secs() as i64)
    };
    let real = |w: &Option<ciallopa_core::QuotaWindow>| {
        w.clone().filter(|w| {
            w.window_minutes.is_some()
                || w.resets_at
                    .is_some_and(|r| r > now + std::time::Duration::from_secs(1))
        })
    };
    let (p, s) = (real(&snap.primary), real(&snap.secondary));
    if p.is_none() && s.is_none() {
        return None;
    }
    let plan = h
        .get("x-codex-plan-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty());
    let mut q = QuotaObs {
        plan,
        p_pct: p.as_ref().map(|w| w.used_percent),
        p_reset: p.as_ref().and_then(|w| w.resets_at).and_then(secs),
        p_window_min: p.as_ref().and_then(|w| w.window_minutes).map(|m| m as i64),
        s_pct: s.as_ref().map(|w| w.used_percent),
        s_reset: s.as_ref().and_then(|w| w.resets_at).and_then(secs),
        s_window_min: s.as_ref().and_then(|w| w.window_minutes).map(|m| m as i64),
        ..Default::default()
    };
    if let Some(c) = ciallopa_routing::parse_credits(h) {
        q.set_credits(
            Some(c.has_credits),
            Some(c.unlimited),
            c.balance,
            None,
            None,
        );
    }
    Some(q)
}

/// Ids / tiers are short tokens; refuse junk so a client can't bloat the label table.
fn short(s: &str) -> Option<String> {
    let s = s.trim();
    (!s.is_empty() && s.len() <= 128).then(|| s.to_string())
}

fn elapsed_ms(t: Instant) -> u32 {
    t.elapsed().as_millis().min(u128::from(u32::MAX)) as u32
}

/// A cancellation keeps the first error, message and source, just like an explicit failure.
fn cancel_record(rec: &mut Record, start: Instant, last_upstream: Option<Instant>, forced: bool) {
    let (code, source, message) = if forced {
        if rec.status == 0 && rec.transport != Transport::Ws {
            // Admission returns 503 when force interrupts a handler before headers.
            rec.status = 503;
        }
        ("shutdown", crate::errors::SRC_LOCAL, None)
    } else {
        let message = match last_upstream {
            Some(t) => format!(
                "client disconnected before the response completed; upstream silent for {:.1}s",
                t.elapsed().as_secs_f64()
            ),
            None => format!(
                "client disconnected before the response completed; upstream silent for {:.1}s (no upstream data yet)",
                start.elapsed().as_secs_f64()
            ),
        };
        ("client_closed", crate::errors::SRC_CLIENT, Some(message))
    };
    rec.error.get_or_insert_with(|| code.into());
    rec.err_source.get_or_insert(source);
    if let Some(message) = message {
        rec.detail
            .get_or_insert_with(Default::default)
            .message
            .get_or_insert(message);
    }
}

/// Finalize a row independently of persistence and downstream emission.
fn into_record(
    mut rec: Record,
    outcome: Outcome,
    replays: Option<crate::errors::Attempts>,
    obs: Observation,
    start: Instant,
    last_upstream: Option<Instant>,
    forced: bool,
) -> Record {
    if outcome == Outcome::Open && rec.error.is_none() && rec.status < 400 {
        cancel_record(&mut rec, start, last_upstream, forced);
    }
    if rec.status >= 400 && rec.error.is_none() {
        let code = rec
            .detail
            .as_ref()
            .and_then(|d| crate::errors::body_code(&d.body))
            .unwrap_or_else(|| format!("http_{}", rec.status));
        rec.error = Some(code);
        if outcome != Outcome::Settled {
            let source = if rec.upstream_status.is_some() {
                crate::errors::SRC_UPSTREAM
            } else {
                crate::errors::SRC_LOCAL
            };
            rec.err_source.get_or_insert(source);
        }
    }
    if rec.error.is_some() && rec.err_source.is_none() {
        rec.err_source = Some(crate::errors::SRC_LOCAL);
    }
    if rec.error.is_none() {
        rec.upstream_status = None;
    }
    if let Some(mut chain) = replays {
        let mut a = crate::errors::attempt(rec.account.as_deref().unwrap_or(""), "stream");
        a.code = rec.error.clone();
        a.action = if rec.error.is_none() {
            crate::attempt::Next::Ok
        } else {
            crate::attempt::Next::Stop
        }
        .into();
        chain.push(a);
        let d = rec.detail.get_or_insert_with(Default::default);
        if d.attempts.is_none() {
            d.attempts = Some(chain.to_json());
        }
    }
    match rec.error.as_deref() {
        None => {
            // Success keeps only the attempt chain.
            let attempts = rec.detail.take().and_then(|d| d.attempts);
            if let Some(attempts) = attempts {
                rec.detail = Some(Box::new(ErrorDetail {
                    attempts: Some(attempts),
                    ..Default::default()
                }));
            }
        }
        Some(kind) => {
            let detail = rec.detail.get_or_insert_with(Default::default);
            if detail.message.is_none() {
                detail.message = Some(describe(kind, rec.status));
            }
        }
    }
    if !obs.is_empty() {
        rec.obs = Some(Box::new(obs));
    }
    rec
}

impl Drop for Tracker {
    fn drop(&mut self) {
        let emit = std::mem::replace(&mut self.emit, Emit::Never);
        if matches!(emit, Emit::Never) {
            return;
        }
        self.rec.duration_ms = self.frozen_ms.unwrap_or_else(|| elapsed_ms(self.start));
        let forced = self.drain.phase() == ciallopa_upstream::drain::Phase::Force;
        let millis = |d: Duration| d.as_millis().min(u128::from(u32::MAX)) as u32;
        self.obs.cred_ms = self.credentials.map(millis);
        self.obs.hold_ms = self.waits.map(millis);
        let rec = into_record(
            std::mem::replace(&mut self.rec, Record::new(0, Transport::Http, "")),
            self.outcome,
            self.replays.take(),
            std::mem::take(&mut self.obs),
            self.start,
            self.last_upstream,
            forced,
        );
        match emit {
            Emit::ViaBody(link) => {
                let forced = self.drain.phase() == ciallopa_upstream::drain::Phase::Force;
                link.finish(rec, self.sink.clone(), forced);
            }
            Emit::OnDrop => {
                self.sink.record(rec);
            }
            Emit::Never => {}
        }
    }
}

/// Default message for an error kind (local errors have no upstream body).
/// Default detail message per locally recorded code (a recorded message wins).
pub(crate) const DESCRIPTIONS: &[(&str, &str)] = &[
    (
        "client_closed",
        "client disconnected before the response completed",
    ),
    (
        "shutdown",
        "server shutdown interrupted the response before completion",
    ),
    (
        "upstream_ws_close",
        "upstream websocket sent a close frame before a terminal event",
    ),
    (
        "upstream_ws_eof",
        "upstream websocket ended without a close frame",
    ),
    (
        "upstream_ws_read_error",
        "upstream websocket read error before a terminal event",
    ),
    (
        "upstream_ws_send_error",
        "could not forward the client frame to the upstream websocket",
    ),
    (
        "upstream_ws_too_big",
        "upstream websocket closed with 1009: the message is over the upstream size limit",
    ),
    ("idle_timeout", "upstream stream idle timeout"),
    ("usage_limit", "upstream usage limit reached"),
    ("no_account", "no account configured"),
    (
        "no_usable_account",
        "no usable account (all disabled, excluded or need relogin)",
    ),
    (
        "all_cooling",
        "every account is cooling down after upstream errors",
    ),
    ("all_quota_exhausted", "every account is out of usage quota"),
    (
        "first_quota_reset",
        "every account is cooling down; the first to recover is out of usage quota",
    ),
    (
        "stream_truncated",
        "upstream stream ended without a terminal event",
    ),
    (
        "stream_read_error",
        "upstream stream read error before a terminal event",
    ),
    (
        "response_decode_error",
        "upstream gzip response is invalid or incomplete",
    ),
    (
        "unsupported_response_encoding",
        "upstream response used an unsupported content encoding",
    ),
    ("connect", "could not connect to upstream"),
    ("timeout", "upstream timed out"),
    ("timeout_connect", "upstream timed out"),
    (
        "header_timeout",
        "upstream did not return response headers within the attempt timeout",
    ),
    (
        "preparation_timeout",
        "preparation budget exhausted across retries/account switches; no further attempt started",
    ),
    ("dns", "upstream DNS resolution failed"),
    ("tls", "TLS handshake with upstream failed"),
];

fn describe(kind: &str, status: u16) -> String {
    match DESCRIPTIONS.iter().find(|(k, _)| *k == kind) {
        Some((_, d)) => (*d).into(),
        None if status > 0 => format!("HTTP {status} ({kind})"),
        None => kind.to_string(),
    }
}

/// `response.incomplete_details.reason`, bounded; `unknown` when missing.
pub(crate) fn incomplete_reason(v: &Value) -> String {
    v.pointer("/response/incomplete_details/reason")
        .and_then(|r| r.as_str())
        .filter(|r| !r.is_empty())
        .unwrap_or("unknown")
        .chars()
        .take(64)
        .collect()
}

/// Error code for an incomplete response the client could not use: `content_filter` keeps its
/// own code, other reasons become `incomplete_<reason>`.
pub(crate) fn incomplete_code(reason: &str) -> String {
    if reason == "content_filter" {
        reason.into()
    } else {
        format!("incomplete_{reason}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_outcome_and_status_table() {
        for (outcome, status, upstream_status, body, code, source) in [
            (
                Outcome::Open,
                200,
                None,
                "",
                Some("client_closed"),
                Some("client"),
            ),
            (
                Outcome::Open,
                0,
                None,
                "",
                Some("client_closed"),
                Some("client"),
            ),
            (Outcome::Completed, 200, Some(503), "", None, None),
            (Outcome::Settled, 200, Some(503), "", None, None),
            (
                Outcome::Open,
                400,
                None,
                "",
                Some("http_400"),
                Some("local"),
            ),
            (
                Outcome::Completed,
                503,
                Some(503),
                "",
                Some("http_503"),
                Some("upstream"),
            ),
            (
                Outcome::Completed,
                403,
                Some(403),
                r#"{"error":{"code":"denied"}}"#,
                Some("denied"),
                Some("upstream"),
            ),
            (
                Outcome::Completed,
                502,
                None,
                "not JSON",
                Some("http_502"),
                Some("local"),
            ),
            // Even a settled record still gets a status-derived code on drop; source() is frozen.
            (
                Outcome::Settled,
                503,
                Some(503),
                "",
                Some("http_503"),
                Some("local"),
            ),
        ] {
            let mut rec = Record::new(7, Transport::Sse, "responses");
            rec.status = status;
            rec.upstream_status = upstream_status;
            rec.duration_ms = 123;
            rec.detail = Some(Box::new(ErrorDetail::default()));
            rec.detail.as_mut().unwrap().push_body(body.as_bytes());
            let start = Instant::now();
            let rec = into_record(
                rec,
                outcome,
                None,
                Observation::default(),
                start,
                None,
                false,
            );
            assert_eq!(
                (rec.error.as_deref(), rec.err_source),
                (code, source),
                "{outcome:?}, {status}"
            );
            assert_eq!(rec.upstream_status, code.and(upstream_status));
            assert_eq!(rec.duration_ms, 123);
            if code.is_none() {
                assert!(rec.detail.is_none());
            } else {
                let detail = rec.detail.unwrap();
                assert_eq!(detail.body, body.as_bytes());
                assert!(detail.message.is_some());
            }
        }
    }

    #[test]
    fn drop_cancellation_and_force_table() {
        for outcome in [Outcome::Open, Outcome::Completed, Outcome::Settled] {
            for transport in [Transport::Http, Transport::Sse, Transport::Ws] {
                for status in [0, 200] {
                    for forced in [false, true] {
                        for upstream_data in [false, true] {
                            let mut rec = Record::new(7, transport, "responses");
                            rec.status = status;
                            let start = Instant::now();
                            let rec = into_record(
                                rec,
                                outcome,
                                None,
                                Observation::default(),
                                start,
                                upstream_data.then_some(start),
                                forced,
                            );
                            if outcome != Outcome::Open {
                                assert_eq!(rec.error, None);
                                assert_eq!(rec.status, status);
                                assert_eq!(rec.detail, None);
                            } else if forced {
                                assert_eq!(rec.error.as_deref(), Some("shutdown"));
                                assert_eq!(rec.err_source, Some("local"));
                                assert_eq!(
                                    rec.status,
                                    if status == 0 && transport != Transport::Ws {
                                        503
                                    } else {
                                        status
                                    }
                                );
                                assert_eq!(rec.detail.unwrap().message.as_deref(), Some("server shutdown interrupted the response before completion"));
                            } else {
                                assert_eq!(rec.error.as_deref(), Some("client_closed"));
                                assert_eq!(rec.err_source, Some("client"));
                                assert_eq!(rec.status, status);
                                let message = rec.detail.unwrap().message.unwrap();
                                assert!(message.starts_with("client disconnected before the response completed; upstream silent for "));
                                assert_eq!(
                                    message.ends_with("(no upstream data yet)"),
                                    !upstream_data
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn drop_preserves_first_failure_and_success_keeps_only_attempts() {
        let start = Instant::now();
        let mut rec = Record::new(7, Transport::Ws, "responses");
        rec.status = 200;
        rec.upstream_status = Some(503);
        rec.error = Some("idle_timeout".into());
        rec.err_source = Some("network");
        let mut detail = ErrorDetail {
            message: Some("first failure".into()),
            attempts: Some(serde_json::json!([{"action":"retry"}])),
            ..Default::default()
        };
        detail.push_body(b"upstream body");
        rec.detail = Some(Box::new(detail));
        let obs = Observation {
            up_events: Some(3),
            ..Default::default()
        };
        let mut expected = rec.clone();
        expected.obs = Some(Box::new(obs.clone()));
        for outcome in [Outcome::Open, Outcome::Completed, Outcome::Settled] {
            let got = into_record(rec.clone(), outcome, None, obs.clone(), start, None, true);
            assert_eq!(got, expected);
        }
        rec.error = None;
        rec.err_source = None;
        expected = rec.clone();
        expected.upstream_status = None;
        expected.detail = Some(Box::new(ErrorDetail {
            attempts: rec.detail.as_ref().unwrap().attempts.clone(),
            ..Default::default()
        }));
        let got = into_record(
            rec,
            Outcome::Completed,
            None,
            Observation::default(),
            start,
            None,
            false,
        );
        assert_eq!(got, expected);
    }

    #[test]
    fn drop_appends_the_final_replay_outcome() {
        for error in [None, Some("usage_limit")] {
            let start = Instant::now();
            let mut rec = Record::new(7, Transport::Ws, "responses");
            rec.status = 200;
            rec.account = Some("last".into());
            rec.error = error.map(str::to_owned);
            let mut chain = crate::errors::Attempts::default();
            let mut first = crate::errors::attempt("first", "stream");
            first.code = Some("overloaded".into());
            first.action = crate::attempt::Next::Switch.into();
            chain.push(first);
            let rec = into_record(
                rec,
                Outcome::Settled,
                Some(chain),
                Observation::default(),
                start,
                None,
                false,
            );
            let attempts = rec.detail.unwrap().attempts.unwrap();
            assert_eq!(attempts[0]["action"], "switch");
            assert_eq!(attempts[1]["account"], "last");
            assert_eq!(attempts[1]["kind"], "stream");
            assert_eq!(
                attempts[1]["action"],
                if error.is_some() { "stop" } else { "ok" }
            );
            assert_eq!(attempts[1].get("code").and_then(Value::as_str), error);
        }
    }

    #[test]
    fn terminal_detection() {
        for ty in ["response.completed", "response.done", "error"] {
            assert!(is_terminal_type(ty));
        }
        let ty =
            event_type(r#"{"type":"response.output_text.delta","delta":"response.completed"}"#)
                .unwrap();
        assert!(!is_terminal_type(&ty));
        assert!(matches!(ty, Cow::Borrowed(_)));
        assert!(!is_output_event(
            br#"{"type":"response.metadata","note":"foo.delta"}"#
        ));
        assert!(is_output_event(br#"{"metadata":{"type":"response.completed"}, "type": "response.output_text.delta", "delta":"ok"}"#));
        assert_eq!(
            event_type(r#"{ "type": "error" }"#).as_deref(),
            Some("error")
        );
        assert_eq!(endpoint_of("/codex/responses/compact?x=1"), Some("compact"));
        assert_eq!(endpoint_of("/wham/usage"), None);
        let mut h = HeaderMap::new();
        for (k, v) in [
            ("x-codex-plan-type", "Free"),
            ("x-codex-primary-used-percent", "6"),
            ("x-codex-primary-window-minutes", "43200"),
            ("x-codex-primary-reset-at", "1794464253"),
            ("x-codex-secondary-used-percent", "0"),
            ("x-codex-secondary-window-minutes", "0"),
            ("x-codex-secondary-reset-after-seconds", "0"),
        ] {
            h.insert(k, v.parse().unwrap());
        }
        let q = quota_obs(&h).unwrap();
        assert_eq!(q.plan.as_deref(), Some("free"));
        assert_eq!(
            (q.p_pct, q.p_reset, q.p_window_min),
            (Some(6.0), Some(1794464253), Some(43200))
        );
        assert_eq!(
            (q.s_pct, q.s_reset),
            (None, None),
            "free plan's empty secondary dropped"
        );
        assert!(quota_obs(&HeaderMap::new()).is_none());
        // pro: no 5-hour window. The weekly one arrives in the primary slot; the slot is kept as
        // sent and `window_minutes` (not the position) tells the UI it is the weekly window.
        let mut h = HeaderMap::new();
        for (k, v) in [
            ("x-codex-plan-type", "pro"),
            ("x-codex-primary-used-percent", "30"),
            ("x-codex-primary-window-minutes", "10080"),
            ("x-codex-primary-reset-at", "1794464253"),
        ] {
            h.insert(k, v.parse().unwrap());
        }
        let q = quota_obs(&h).unwrap();
        assert_eq!((q.p_pct, q.p_window_min), (Some(30.0), Some(10080)));
        assert_eq!(
            (q.s_pct, q.s_window_min),
            (None, None),
            "no invented 5h window"
        );
        assert_eq!(
            (q.cr_flags, q.cr_balance),
            (None, None),
            "no credit headers"
        );
    }

    #[test]
    fn quota_obs_keeps_purchased_credits() {
        let mut h = HeaderMap::new();
        for (k, v) in [
            ("x-codex-primary-used-percent", "100"),
            ("x-codex-primary-window-minutes", "10080"),
            ("x-codex-primary-reset-at", "1794464253"),
            ("x-codex-credits-has-credits", "True"),
            ("x-codex-credits-unlimited", "false"),
            ("x-codex-credits-balance", " 812.5 "),
        ] {
            h.insert(k, v.parse().unwrap());
        }
        let q = quota_obs(&h).unwrap();
        assert_eq!(
            (q.cr_flags, q.cr_balance),
            (Some(ciallopa_usage::CR_HAS), Some(812.5))
        );
        // Like Codex: without both flags there is no credit snapshot.
        h.remove("x-codex-credits-unlimited");
        assert_eq!(quota_obs(&h).unwrap().cr_flags, None);
    }
}
