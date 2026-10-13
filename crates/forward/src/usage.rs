//! Glue between forwarding and the usage log: one `Tracker` per HTTP request / WS turn.
//! The record is emitted when the tracker is dropped (stream end, client disconnect, error),
//! via a non-blocking `try_send`; nothing here can stall a stream.
use crate::AppState;
use cpa_lite_core::AccountId;
use cpa_lite_usage::{QuotaObs, Record, Recorder, Transport};
use http::HeaderMap;
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;

pub(crate) struct Tracker {
    rec: Record,
    sink: Arc<Recorder>,
    start: Instant,
    /// A terminal Responses event was seen (or the response is not a stream).
    terminal: bool,
    /// Bytes of a not-yet-complete SSE event (only the tail is kept).
    scan: Vec<u8>,
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
            scan: Vec::new(),
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

    pub(crate) fn retries(&mut self, n: u32) {
        self.rec.retries += n;
    }

    pub(crate) fn status(&mut self, s: u16) {
        self.rec.status = s;
    }

    pub(crate) fn not_stream(&mut self) {
        self.terminal = true;
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
        if let Some(t) = resp
            .and_then(|r| r.get("service_tier"))
            .and_then(|v| v.as_str())
            .and_then(short)
        {
            self.rec.service_tier = Some(t);
        }
        match ty {
            "response.completed" | "response.done" => {}
            "response.incomplete" => self.fail("incomplete"),
            _ => {
                let text = v.to_string();
                if crate::proxy::is_quota_code(&text) {
                    self.fail("usage_limit");
                } else if ty == "error" {
                    self.fail("upstream_error_event");
                } else {
                    self.fail("response_failed");
                }
            }
        }
    }

    /// Feed raw SSE bytes; parses only terminal events (cheap byte search first).
    pub(crate) fn feed_sse(&mut self, chunk: &[u8]) {
        if self.terminal {
            return;
        }
        // Fast path: no event boundary in this chunk and nothing pending that could matter.
        self.scan.extend_from_slice(chunk);
        let mut start = 0;
        let buf = std::mem::take(&mut self.scan);
        while let Some((end, sep)) = find_boundary(&buf[start..]) {
            let ev = &buf[start..start + end];
            if is_terminal(ev) {
                if let Some(v) = sse_data_json(ev) {
                    self.terminal_event(&v);
                }
            }
            start += end + sep;
        }
        let mut rest = buf;
        rest.drain(..start);
        // A single event larger than 8 MiB is not a terminal event we can afford to parse.
        if rest.len() > 8 << 20 {
            rest.clear();
        }
        self.scan = rest;
    }
}

/// End of the first SSE event: `\n\n` or `\n\r\n` (CRLF streams). Returns (offset, sep len).
fn find_boundary(b: &[u8]) -> Option<(usize, usize)> {
    let mut i = 0;
    while let Some(p) = b[i..].iter().position(|&c| c == b'\n') {
        let at = i + p;
        match b.get(at + 1) {
            Some(b'\n') => return Some((at, 2)),
            Some(b'\r') if b.get(at + 2) == Some(&b'\n') => return Some((at, 3)),
            _ => i = at + 1,
        }
    }
    None
}

fn is_terminal(ev: &[u8]) -> bool {
    const PATS: [&[u8]; 4] = [
        b"\"response.completed\"",
        b"\"response.failed\"",
        b"\"response.incomplete\"",
        b"\"type\":\"error\"",
    ];
    PATS.iter().any(|p| crate::proxy::memfind(ev, p))
}

fn sse_data_json(ev: &[u8]) -> Option<Value> {
    let s = std::str::from_utf8(ev).ok()?;
    let data: Vec<&str> = s
        .lines()
        .filter_map(|l| l.trim_end_matches('\r').strip_prefix("data:"))
        .map(|d| d.strip_prefix(' ').unwrap_or(d))
        .collect();
    serde_json::from_str(&data.join("\n")).ok()
}

/// `x-codex-*` headers → snapshot. A window without `window-minutes` whose reset is "now"
/// (free plans send `secondary-window-minutes: 0`, `reset-after-seconds: 0`) is treated as absent.
pub(crate) fn quota_obs(h: &HeaderMap) -> Option<QuotaObs> {
    let now = std::time::SystemTime::now();
    let snap = cpa_lite_routing::parse_quota_at(h, now)?;
    let secs = |t: std::time::SystemTime| {
        t.duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|d| d.as_secs() as i64)
    };
    let real = |w: &Option<cpa_lite_core::QuotaWindow>| {
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
            self.rec.error = Some("client_closed".into());
        }
        if self.rec.status >= 400 && self.rec.error.is_none() {
            self.rec.error = Some(error_for_status(self.rec.status).into());
        }
        let rec = std::mem::replace(&mut self.rec, Record::new(0, Transport::Http, ""));
        self.sink.record(rec);
    }
}

pub(crate) fn error_for_status(s: u16) -> &'static str {
    match s {
        401 | 403 => "auth",
        404 => "not_found",
        408 => "timeout",
        429 => "rate_limited",
        400..=499 => "client_error",
        502 => "upstream_unreachable",
        503 => "no_account",
        _ => "upstream_5xx",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn terminal_detection() {
        assert!(is_terminal(
            br#"data: {"type":"response.completed","response":{}}"#
        ));
        assert!(!is_terminal(
            br#"data: {"type":"response.output_text.delta","delta":"x"}"#
        ));
        let v =
            sse_data_json(b"event: response.completed\ndata: {\"type\":\"response.completed\"}")
                .unwrap();
        assert_eq!(v["type"], "response.completed");
        assert_eq!(endpoint_of("/codex/responses/compact?x=1"), Some("compact"));
        assert_eq!(endpoint_of("/wham/usage"), None);
        assert_eq!(find_boundary(b"a\nb\n\nc"), Some((3, 2)));
        assert_eq!(find_boundary(b"a\r\n\r\nc"), Some((2, 3)));
        assert_eq!(find_boundary(b"a\nb"), None);
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
