//! Glue between forwarding and the usage log: one `Tracker` per HTTP request / WS turn.
//! The record is emitted when the tracker is dropped (stream end, client disconnect, error),
//! via a non-blocking `try_send`; forwarding never waits for the persistence queue.
use crate::AppState;
use ciallopa_core::AccountId;
use ciallopa_usage::{detail::keep_header, ErrorDetail, QuotaObs, Record, Recorder, Transport};
use http::HeaderMap;
use serde_json::Value;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Instant;

pub(crate) struct Tracker {
    rec: Record,
    sink: Arc<Recorder>,
    start: Instant,
    /// A terminal Responses event was seen (or the response is not a stream).
    terminal: bool,
    /// `response.created` already parsed.
    created: bool,
    /// A terminal event set the served tier (`response.created` must not override it).
    tier_final: bool,
    drain: Arc<ciallopa_upstream::drain::Drain>,
}

/// Map an upstream path (`/codex/responses/compact?..`) to a short endpoint label.
pub(crate) fn endpoint_of(path: &str) -> Option<&'static str> {
    let p = path.split('?').next().unwrap_or(path);
    Some(match p {
        "/codex/responses" => "responses",
        "/codex/responses/compact" => "compact",
        _ => return None,
    })
}

impl Tracker {
    /// Existing usage record ID, in the same form shown by the request inspector.
    pub(crate) fn record_id(&self) -> String {
        format!("{:016x}", self.rec.id)
    }

    pub(crate) fn start(
        st: &AppState,
        transport: Transport,
        endpoint: &'static str,
        key: Option<&str>,
    ) -> Option<Self> {
        let sink = st.usage.clone()?;
        let id = sink.ids.next();
        let mut rec = Record::new(id, transport, endpoint);
        rec.key = key.map(str::to_string);
        Some(Self {
            rec,
            sink,
            start: Instant::now(),
            terminal: false,
            created: false,
            tier_final: false,
            drain: st.upstream.drain.clone(),
        })
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
            .and_then(|v| v.as_str())
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
        if let Some(e) = b
            .get("reasoning")
            .and_then(|r| r.get("effort"))
            .and_then(|v| v.as_str())
        {
            self.rec.effort = Some(e.to_string());
        }
    }

    /// Final account plus the accounts tried (and abandoned) before it.
    pub(crate) fn accounts(&mut self, tried: &[AccountId], fin: Option<&AccountId>) {
        self.rec.switches = tried.len() as u32;
        let first = tried.first().or(fin);
        self.rec.account = fin.cloned().or_else(|| tried.last().cloned());
        if first != self.rec.account.as_ref() {
            self.rec.first_account = first.cloned();
        }
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

    /// Cumulative transport / 5xx retries for this request, including earlier SSE attempts.
    pub(crate) fn retries(&mut self, n: u32) {
        self.rec.retries = n;
    }

    pub(crate) fn status(&mut self, s: u16) {
        self.rec.status = s;
    }

    /// The response completed; this is independent of HTTP / SSE / WS transport.
    pub(crate) fn finish_response(&mut self) {
        self.terminal = true;
    }

    /// The actual client response is ordinary HTTP, even when a Responses request was
    /// initially expected to stream. Keep header timing separate from SSE/WS output timing.
    pub(crate) fn http_response(&mut self) {
        self.rec.transport = Transport::Http;
    }

    pub(crate) fn sse_response(&mut self) {
        self.rec.transport = Transport::Sse;
    }

    /// The client went away. After a terminal event that is a normal end of stream (Codex closes
    /// as soon as it has `response.completed`), not an error; before it the client is the source.
    pub(crate) fn client_gone(&mut self) {
        if self.terminal {
            return;
        }
        if self.drain.phase() == ciallopa_upstream::drain::Phase::Force {
            if self.rec.status == 0 && self.rec.transport != Transport::Ws {
                // Admission returns 503 when force interrupts a handler before headers.
                self.rec.status = 503;
            }
            self.fail("shutdown");
            self.source(crate::errors::SRC_LOCAL);
        } else {
            self.fail("client_closed");
            self.source(crate::errors::SRC_CLIENT);
        }
    }

    /// Codex WS prewarm (`generate:false`): logged under its own endpoint so it stays out of
    /// request counts, error rate and latency percentiles while its tokens still count.
    pub(crate) fn prewarm(&mut self) {
        self.rec.endpoint = ciallopa_usage::PREWARM_ENDPOINT;
    }

    /// TTFT is the first event that carries model output (see `is_output_event`).
    pub(crate) fn output_event(&mut self, ev: &[u8]) {
        if self.rec.ttft_ms.is_none() && is_output_event(ev) {
            self.ttft();
        }
    }

    pub(crate) fn ttft(&mut self) {
        if self.rec.ttft_ms.is_none() {
            self.rec.ttft_ms = Some(elapsed_ms(self.start));
        }
    }

    /// First error wins (later ones are usually consequences).
    pub(crate) fn fail(&mut self, kind: &str) {
        if self.rec.error.is_none() {
            self.rec.error = Some(kind.to_string());
        }
    }

    /// `fail` plus a human-readable message for the error detail (first message wins).
    pub(crate) fn fail_msg(&mut self, kind: &str, msg: impl Into<String>) {
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
        if self.rec.err_source.is_none() {
            self.rec.err_source = Some(src);
        }
    }

    /// The upstream answered with an error status that is passed through to the client.
    pub(crate) fn upstream_error(&mut self, status: u16, h: &HeaderMap) {
        self.rec.upstream_status = Some(status);
        self.source(crate::errors::SRC_UPSTREAM);
        if let Some(id) = crate::errors::request_id(h) {
            self.detail().upstream_request_id = Some(id);
        }
    }

    /// The upstream rejected the encrypted reasoning content in the request history: logged as
    /// an upstream error (`invalid_encrypted_content`) with the upstream message and event.
    pub(crate) fn encrypted_rejected(&mut self, msg: &str, event: &[u8], h: &HeaderMap) {
        self.fail_msg(crate::encrypted::CODE, msg);
        self.source(crate::errors::SRC_UPSTREAM);
        self.error_text(event);
        if let Some(id) = crate::errors::request_id(h) {
            self.detail().upstream_request_id = Some(id);
        }
    }

    /// Provenance attached to a ready-made error response.
    pub(crate) fn meta_err(&mut self, m: &crate::errors::ErrMeta) {
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

    /// Raw error body bytes (capped / redacted when written).
    pub(crate) fn error_body(&mut self, b: &[u8]) {
        self.detail().push_body(b);
    }

    /// A WS error frame / handshake failure body.
    pub(crate) fn error_text(&mut self, b: &[u8]) {
        self.detail().set_body(b);
    }

    /// `response.created`: provisional response model / effort / tier (terminal events win).
    pub(crate) fn created_event(&mut self, v: &Value) {
        if self.created || self.terminal {
            return;
        }
        self.created = true;
        if let Some(r) = v.get("response") {
            self.response_meta(r, false);
        }
    }

    fn response_meta(&mut self, r: &Value, terminal: bool) {
        let s = |k: &str| r.get(k).and_then(|v| v.as_str()).and_then(short);
        if let Some(m) = s("model") {
            if terminal || self.rec.resp_model.is_none() {
                self.rec.resp_model = Some(m);
            }
        }
        if let Some(e) = r
            .get("reasoning")
            .and_then(|x| x.get("effort"))
            .and_then(|v| v.as_str())
            .and_then(short)
        {
            if terminal || self.rec.resp_effort.is_none() {
                self.rec.resp_effort = Some(e);
            }
        }
        // `auto` is a request-side alias, never the served tier.
        if let Some(t) = s("service_tier").filter(|t| !t.eq_ignore_ascii_case("auto")) {
            if terminal || !self.tier_final {
                self.rec.service_tier = Some(t);
                self.tier_final |= terminal;
            }
        }
    }

    /// A terminal Responses event (`response.completed` / `failed` / `incomplete` / `error`).
    pub(crate) fn terminal_event(&mut self, v: &Value) {
        self.terminal = true;
        let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
        let resp = v.get("response");
        if let Some(u) = resp.and_then(|r| r.get("usage")) {
            let n = |v: Option<&Value>| v.and_then(|x| x.as_i64());
            self.rec.input_tokens = n(u.get("input_tokens"));
            self.rec.output_tokens = n(u.get("output_tokens"));
            self.rec.cached_tokens = n(u
                .get("input_tokens_details")
                .and_then(|d| d.get("cached_tokens")));
            self.rec.reasoning_tokens = n(u
                .get("output_tokens_details")
                .and_then(|d| d.get("reasoning_tokens")));
            self.rec.cache_write_tokens = n(u
                .get("input_tokens_details")
                .and_then(|d| d.get("cache_write_tokens")));
        }
        // The tier actually used (e.g. `priority` may be downgraded) wins over the request's.
        if let Some(r) = resp {
            self.response_meta(r, true);
        }
        match ty {
            "response.completed" | "response.done" => {}
            // Terminal for Codex (bc9cad3) but not a proxy/upstream failure: keep tokens, flag it.
            "response.incomplete" => {
                let reason = resp
                    .and_then(|r| r.pointer("/incomplete_details/reason"))
                    .and_then(|r| r.as_str())
                    .filter(|r| !r.is_empty())
                    .unwrap_or("unknown");
                self.rec.incomplete = Some(reason.chars().take(64).collect());
            }
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
                if crate::proxy::quota_event(v).0 {
                    self.fail("usage_limit");
                } else if let Some(c) = crate::proxy::overload_code(v) {
                    self.fail(&c);
                } else if ty == "error" {
                    self.fail("upstream_error_event");
                } else {
                    self.fail("response_failed");
                }
            }
        }
    }

    /// Observe one complete SSE data payload. The forwarding observer owns its bounded
    /// event buffer; only created / terminal events need a full JSON value here.
    pub(crate) fn sse_event(&mut self, text: &str, ty: Option<&str>) {
        if self.terminal {
            return;
        }
        let Some(ty) = ty else { return };
        if is_output_type(ty) {
            self.ttft();
        }
        if is_terminal_type(ty) {
            if let Ok(v) = serde_json::from_str(text) {
                self.terminal_event(&v);
            }
        } else if !self.created && ty == "response.created" {
            if let Ok(v) = serde_json::from_str(text) {
                self.created_event(&v);
            }
        }
    }
}

/// Read the actual top-level type without allocating the rest of an event's payload.
/// A type-looking string in model output or nested metadata has no protocol meaning.
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

/// First token = first event carrying generated output: any `*.delta` (output text, reasoning
/// summary / text, tool-call arguments, …) or a finished output item (items streamed without
/// deltas). `response.created`, `in_progress`, `output_item.added`, `codex.*` metadata and
/// rate-limit events are not output.
pub(crate) fn is_output_event(ev: &[u8]) -> bool {
    std::str::from_utf8(ev)
        .ok()
        .and_then(event_type)
        .is_some_and(|ty| is_output_type(&ty))
}

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
    Some(QuotaObs {
        plan,
        p_pct: p.as_ref().map(|w| w.used_percent),
        p_reset: p.as_ref().and_then(|w| w.resets_at).and_then(secs),
        p_window_min: p.as_ref().and_then(|w| w.window_minutes).map(|m| m as i64),
        s_pct: s.as_ref().map(|w| w.used_percent),
        s_reset: s.as_ref().and_then(|w| w.resets_at).and_then(secs),
        s_window_min: s.as_ref().and_then(|w| w.window_minutes).map(|m| m as i64),
    })
}

/// Ids / tiers are short tokens; refuse junk so a client can't bloat the label table.
fn short(s: &str) -> Option<String> {
    let s = s.trim();
    (!s.is_empty() && s.len() <= 128).then(|| s.to_string())
}

fn elapsed_ms(t: Instant) -> u32 {
    t.elapsed().as_millis().min(u128::from(u32::MAX)) as u32
}

impl Drop for Tracker {
    fn drop(&mut self) {
        self.rec.duration_ms = elapsed_ms(self.start);
        if !self.terminal && self.rec.error.is_none() && self.rec.status < 400 {
            self.client_gone();
        }
        if self.rec.status >= 400 && self.rec.error.is_none() {
            // No structured classification: an upstream status passed through keeps the
            // upstream's own code; never guess a local cause from the status alone.
            let code = self
                .rec
                .detail
                .as_ref()
                .and_then(|d| crate::errors::body_code(&d.body))
                .unwrap_or_else(|| format!("http_{}", self.rec.status));
            self.rec.error = Some(code);
            if self.rec.upstream_status.is_some() {
                self.source(crate::errors::SRC_UPSTREAM);
            } else {
                self.source(crate::errors::SRC_LOCAL);
            }
        }
        if self.rec.error.is_some() && self.rec.err_source.is_none() {
            self.rec.err_source = Some(crate::errors::SRC_LOCAL);
        }
        if self.rec.error.is_none() {
            self.rec.upstream_status = None;
        }
        match self.rec.error.as_deref() {
            None => {
                // Success: keep only the attempt chain (retried / switched requests).
                let a = self.rec.detail.take().and_then(|d| d.attempts);
                if let Some(a) = a {
                    self.rec.detail = Some(Box::new(ErrorDetail {
                        attempts: Some(a),
                        ..Default::default()
                    }));
                }
            }
            Some(kind) => {
                let (kind, status) = (kind.to_string(), self.rec.status);
                let d = self.detail();
                if d.message.is_none() {
                    d.message = Some(describe(&kind, status));
                }
            }
        }
        let rec = std::mem::replace(&mut self.rec, Record::new(0, Transport::Http, ""));
        self.sink.record(rec);
    }
}

/// Default message for an error kind (local errors have no upstream body).
fn describe(kind: &str, status: u16) -> String {
    match kind {
        "client_closed" => "client disconnected before the response completed".into(),
        "shutdown" => "server shutdown interrupted the response before completion".into(),
        "upstream_closed" => "upstream closed the connection before a terminal event".into(),
        "idle_timeout" => "upstream stream idle timeout".into(),
        "stream_error" => "upstream stream transport error".into(),
        "incomplete" => "response.incomplete from upstream".into(),
        "usage_limit" => "upstream usage limit reached".into(),
        "no_account" => "no account configured".into(),
        "no_usable_account" => "no usable account (all disabled, excluded or need relogin)".into(),
        "all_cooling" => "every account is cooling down after upstream errors".into(),
        "all_quota_exhausted" => "every account is out of usage quota".into(),
        "first_quota_reset" => {
            "every account is cooling down; the first to recover is out of usage quota".into()
        }
        "stream_truncated" => "upstream stream ended without a terminal event".into(),
        "connect" => "could not connect to upstream".into(),
        "timeout" | "timeout_connect" => "upstream timed out".into(),
        "header_timeout" => "upstream did not return response headers within the attempt timeout".into(),
        "preparation_timeout" => "HTTP preparation budget exhausted across retries/account switches; no further attempt started".into(),
        "dns" => "upstream DNS resolution failed".into(),
        "tls" => "TLS handshake with upstream failed".into(),
        _ if status > 0 => format!("HTTP {status} ({kind})"),
        _ => kind.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    }
}
