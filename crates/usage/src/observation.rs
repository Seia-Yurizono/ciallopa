//! Gateway-side stream observation for one SSE request or WS turn (schema v14).
//!
//! Every time is milliseconds (or microseconds for `*_us`) since the record's monotonic
//! start (the same zero as `ttft_ms` / `duration_ms`). These are **gateway** observations:
//! `up_*` is when ciallopa parsed an upstream event; `down_*` is when ciallopa handed bytes
//! to its own downstream layers. None of them is a client receive, parse or render time,
//! and none is a socket flush or TCP acknowledgement.
//!
//! `None` means not observed / not applicable (older rows, other transport, nothing sent);
//! it is never written as 0.
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

/// Longest stored id / extension string (bytes).
pub const MAX_ID_LEN: usize = 128;
pub const MAX_EXT_LEN: usize = 160;

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Observation {
    // ---- upstream → gateway (final attempt; parse time of complete events / frames)
    /// First `response.output_text.delta` / `response.refusal.delta`.
    pub up_first_text_ms: Option<u32>,
    /// First `response.reasoning*.delta` (summary or text).
    pub up_first_reasoning_ms: Option<u32>,
    /// First tool-call delta (`*_call*.delta`, e.g. function_call_arguments, custom_tool_call_input).
    pub up_first_tool_ms: Option<u32>,
    /// Last complete upstream event / text frame (any type).
    pub up_last_event_ms: Option<u32>,
    /// Terminal event (`response.completed` / `failed` / `incomplete` / `done` / `error`).
    pub up_terminal_ms: Option<u32>,
    /// Complete upstream events (SSE) / text frames (WS) observed.
    pub up_events: Option<u32>,
    /// Upstream payload bytes: SSE body bytes as delivered by the HTTP client (after any
    /// content decoding; bootstrap bytes of abandoned attempts excluded); WS
    /// text/binary message payload bytes (after permessage-deflate decompression).
    pub up_bytes: Option<u64>,
    /// `response.id` from `response.created` / the terminal event.
    pub response_id: Option<String>,
    /// Upstream `x-request-id` / `x-oai-request-id` / `cf-ray` (SSE response or WS handshake).
    pub upstream_request_id: Option<String>,

    // ---- gateway → downstream, SSE (body channel → hyper)
    /// First / last chunk accepted into the gateway's bounded body queue.
    pub sse_first_queue_ms: Option<u32>,
    pub sse_last_queue_ms: Option<u32>,
    /// Total / longest time a chunk waited because that queue was full (Pending on send).
    pub sse_queue_blocked_us: Option<u64>,
    pub sse_queue_blocked_max_us: Option<u64>,
    pub sse_queue_chunks: Option<u32>,
    pub sse_queue_bytes: Option<u64>,
    /// First / last chunk handed from the queue to hyper's body poll (not a socket write).
    pub sse_first_yield_ms: Option<u32>,
    pub sse_last_yield_ms: Option<u32>,
    pub sse_yield_chunks: Option<u32>,
    pub sse_yield_bytes: Option<u64>,

    // ---- gateway → downstream, WS (tungstenite sink)
    /// First / last turn message whose `sink.send` completed: frame written to the
    /// connection and tungstenite's flush returned (bytes accepted by the OS send buffer).
    pub ws_first_send_ms: Option<u32>,
    pub ws_last_send_ms: Option<u32>,
    /// Total / longest time a `sink.send` stayed Pending (write buffer / socket backpressure).
    pub ws_sink_blocked_us: Option<u64>,
    pub ws_sink_blocked_max_us: Option<u64>,
    pub ws_send_msgs: Option<u32>,
    /// Message payload bytes (before permessage-deflate) of completed sends.
    pub ws_send_bytes: Option<u64>,
    /// Downstream ended before the gateway's last write: SSE response body dropped before
    /// its end (client gone, including a normal close right after the terminal event), or a
    /// WS turn send returned an error.
    pub down_incomplete: Option<bool>,

    // ---- WS permessage-deflate negotiation (handshake result, not per-frame use)
    /// Client ↔ ciallopa: whether permessage-deflate was agreed, and the answered parameters.
    pub ws_deflate_down: Option<bool>,
    pub ws_ext_down: Option<String>,
    /// ciallopa ↔ upstream: whether the upstream accepted it, and its answer.
    pub ws_deflate_up: Option<bool>,
    pub ws_ext_up: Option<String>,
}

impl Observation {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// Keep an id only if it is short and made of `[A-Za-z0-9._:-]`; else None.
pub fn clean_id(s: &str) -> Option<String> {
    let s = s.trim();
    (!s.is_empty()
        && s.len() <= MAX_ID_LEN
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-')))
    .then(|| s.to_string())
}

/// Normalise a `Sec-WebSocket-Extensions` answer: only a single `permessage-deflate`
/// with its four RFC 7692 parameters (window bits 8..=15) is kept, re-serialised.
/// Anything else → `Some("unrecognized")`; empty → None.
pub fn clean_ext(s: &str) -> Option<String> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let bad = || Some("unrecognized".to_string());
    if s.contains(',') {
        return bad();
    }
    let mut it = s.split(';').map(str::trim);
    if !it
        .next()
        .is_some_and(|n| n.eq_ignore_ascii_case("permessage-deflate"))
    {
        return bad();
    }
    let mut out = String::from("permessage-deflate");
    for p in it.filter(|p| !p.is_empty()) {
        let (k, v) = match p.split_once('=') {
            Some((k, v)) => (k.trim(), Some(v.trim().trim_matches('"'))),
            None => (p, None),
        };
        let k = k.to_ascii_lowercase();
        match (k.as_str(), v) {
            ("server_no_context_takeover" | "client_no_context_takeover", None) => {}
            ("server_max_window_bits" | "client_max_window_bits", None) => {}
            ("server_max_window_bits" | "client_max_window_bits", Some(v))
                if v.parse::<u8>().is_ok_and(|n| (8..=15).contains(&n)) => {}
            _ => return bad(),
        }
        out.push_str("; ");
        out.push_str(&k);
        if let Some(v) = v {
            out.push('=');
            out.push_str(v);
        }
    }
    (out.len() <= MAX_EXT_LEN).then_some(out).or_else(bad)
}

pub(crate) const CREATE: &str = r#"
-- v14: gateway stream observation (SSE requests / WS turns only; id = requests.id).
CREATE TABLE IF NOT EXISTS request_obs(
  id                    INTEGER PRIMARY KEY,
  up_first_text_ms      INTEGER,
  up_first_reasoning_ms INTEGER,
  up_first_tool_ms      INTEGER,
  up_last_event_ms      INTEGER,
  up_terminal_ms        INTEGER,
  up_events             INTEGER,
  up_bytes              INTEGER,
  response_id           TEXT,
  up_req_id             TEXT,
  sse_first_queue_ms    INTEGER,
  sse_last_queue_ms     INTEGER,
  sse_queue_blocked_us  INTEGER,
  sse_queue_blocked_max_us INTEGER,
  sse_queue_chunks      INTEGER,
  sse_queue_bytes       INTEGER,
  sse_first_yield_ms    INTEGER,
  sse_last_yield_ms     INTEGER,
  sse_yield_chunks      INTEGER,
  sse_yield_bytes       INTEGER,
  ws_first_send_ms      INTEGER,
  ws_last_send_ms       INTEGER,
  ws_sink_blocked_us    INTEGER,
  ws_sink_blocked_max_us INTEGER,
  ws_send_msgs          INTEGER,
  ws_send_bytes         INTEGER,
  down_incomplete      INTEGER,
  ws_deflate_down       INTEGER,
  ws_ext_down           TEXT,
  ws_deflate_up         INTEGER,
  ws_ext_up             TEXT
);
"#;

const COLS: &str =
    "up_first_text_ms,up_first_reasoning_ms,up_first_tool_ms,up_last_event_ms,up_terminal_ms,\
up_events,up_bytes,response_id,up_req_id,sse_first_queue_ms,sse_last_queue_ms,sse_queue_blocked_us,\
sse_queue_blocked_max_us,sse_queue_chunks,sse_queue_bytes,sse_first_yield_ms,sse_last_yield_ms,\
sse_yield_chunks,sse_yield_bytes,ws_first_send_ms,ws_last_send_ms,ws_sink_blocked_us,\
ws_sink_blocked_max_us,ws_send_msgs,ws_send_bytes,down_incomplete,ws_deflate_down,ws_ext_down,\
ws_deflate_up,ws_ext_up";

fn i(v: Option<u64>) -> Option<i64> {
    v.map(|v| v.min(i64::MAX as u64) as i64)
}

pub(crate) fn insert(c: &Connection, id: i64, o: &Observation) -> rusqlite::Result<()> {
    let mut st = c.prepare_cached(&format!(
        "INSERT OR IGNORE INTO request_obs(id,{COLS}) VALUES \
         (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31)"
    ))?;
    st.execute(params![
        id,
        o.up_first_text_ms,
        o.up_first_reasoning_ms,
        o.up_first_tool_ms,
        o.up_last_event_ms,
        o.up_terminal_ms,
        o.up_events,
        i(o.up_bytes),
        o.response_id,
        o.upstream_request_id,
        o.sse_first_queue_ms,
        o.sse_last_queue_ms,
        i(o.sse_queue_blocked_us),
        i(o.sse_queue_blocked_max_us),
        o.sse_queue_chunks,
        i(o.sse_queue_bytes),
        o.sse_first_yield_ms,
        o.sse_last_yield_ms,
        o.sse_yield_chunks,
        i(o.sse_yield_bytes),
        o.ws_first_send_ms,
        o.ws_last_send_ms,
        i(o.ws_sink_blocked_us),
        i(o.ws_sink_blocked_max_us),
        o.ws_send_msgs,
        i(o.ws_send_bytes),
        o.down_incomplete,
        o.ws_deflate_down,
        o.ws_ext_down,
        o.ws_deflate_up,
        o.ws_ext_up,
    ])?;
    Ok(())
}

/// API view: one observation by request id.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ObservationRow {
    pub request_id: String,
    pub ts_ms: i64,
    #[serde(flatten)]
    pub obs: Observation,
}

pub(crate) fn read(c: &Connection, id: i64) -> rusqlite::Result<Option<ObservationRow>> {
    let u = |r: &rusqlite::Row<'_>, n: usize| -> rusqlite::Result<Option<u64>> {
        Ok(r.get::<_, Option<i64>>(n)?.map(|v| v.max(0) as u64))
    };
    c.query_row(
        &format!("SELECT {COLS} FROM request_obs WHERE id = ?1"),
        [id],
        |r| {
            Ok(ObservationRow {
                request_id: format!("{id:016x}"),
                ts_ms: id >> crate::SEQ_BITS,
                obs: Observation {
                    up_first_text_ms: r.get(0)?,
                    up_first_reasoning_ms: r.get(1)?,
                    up_first_tool_ms: r.get(2)?,
                    up_last_event_ms: r.get(3)?,
                    up_terminal_ms: r.get(4)?,
                    up_events: r.get(5)?,
                    up_bytes: u(r, 6)?,
                    response_id: r.get(7)?,
                    upstream_request_id: r.get(8)?,
                    sse_first_queue_ms: r.get(9)?,
                    sse_last_queue_ms: r.get(10)?,
                    sse_queue_blocked_us: u(r, 11)?,
                    sse_queue_blocked_max_us: u(r, 12)?,
                    sse_queue_chunks: r.get(13)?,
                    sse_queue_bytes: u(r, 14)?,
                    sse_first_yield_ms: r.get(15)?,
                    sse_last_yield_ms: r.get(16)?,
                    sse_yield_chunks: r.get(17)?,
                    sse_yield_bytes: u(r, 18)?,
                    ws_first_send_ms: r.get(19)?,
                    ws_last_send_ms: r.get(20)?,
                    ws_sink_blocked_us: u(r, 21)?,
                    ws_sink_blocked_max_us: u(r, 22)?,
                    ws_send_msgs: r.get(23)?,
                    ws_send_bytes: u(r, 24)?,
                    down_incomplete: r.get(25)?,
                    ws_deflate_down: r.get(26)?,
                    ws_ext_down: r.get(27)?,
                    ws_deflate_up: r.get(28)?,
                    ws_ext_up: r.get(29)?,
                },
            })
        },
    )
    .optional()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ext_is_normalised_or_flagged() {
        assert_eq!(clean_ext(""), None);
        assert_eq!(
            clean_ext("permessage-deflate").as_deref(),
            Some("permessage-deflate")
        );
        assert_eq!(
            clean_ext("permessage-deflate; client_max_window_bits=15; Server_No_Context_Takeover")
                .as_deref(),
            Some("permessage-deflate; client_max_window_bits=15; server_no_context_takeover")
        );
        assert_eq!(
            clean_ext("permessage-deflate; server_max_window_bits=\"11\"").as_deref(),
            Some("permessage-deflate; server_max_window_bits=11")
        );
        for bad in [
            "x-webkit-deflate-frame",
            "permessage-deflate; client_max_window_bits=16",
            "permessage-deflate; foo",
            "permessage-deflate, permessage-deflate",
            "permessage-deflate; client_max_window_bits=1\r\nx: y",
        ] {
            assert_eq!(clean_ext(bad).as_deref(), Some("unrecognized"), "{bad}");
        }
    }

    #[test]
    fn ids_are_restricted() {
        assert_eq!(
            clean_id(" resp_abc-1.2:3 ").as_deref(),
            Some("resp_abc-1.2:3")
        );
        assert_eq!(clean_id("a b"), None);
        assert_eq!(clean_id("x\ny"), None);
        assert_eq!(clean_id(&"a".repeat(MAX_ID_LEN + 1)), None);
        assert_eq!(clean_id(""), None);
    }
}
