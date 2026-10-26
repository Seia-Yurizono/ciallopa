//! Requests refused by a local limit (inflight, WebSocket connections, buffered body
//! budget), aggregated in memory per minute and group, then upserted by the writer thread
//! at most every [`FLUSH_EVERY`] and on shutdown. Never blocks the proxy path; a full
//! aggregator collapses new groups into one detail-free group instead of growing.
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

/// Writer flush interval for aggregated refusals.
pub const FLUSH_EVERY: Duration = Duration::from_secs(5);
/// Distinct groups kept between flushes before details are dropped.
const MAX_GROUPS: usize = 4096;
/// Longest stored user agent / route.
const MAX_TEXT: usize = 200;

/// Which limit refused the request (stored as 1..=4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    /// `max_inflight_requests`
    Inflight,
    /// `ws_max_connections`
    Ws,
    /// `max_buffered_body_mb`, HTTP request body
    Body,
    /// `max_buffered_body_mb`, client WebSocket message
    WsMsg,
}
impl Reason {
    pub const ALL: [Self; 4] = [Self::Inflight, Self::Ws, Self::Body, Self::WsMsg];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inflight => "inflight",
            Self::Ws => "ws",
            Self::Body => "body",
            Self::WsMsg => "ws_msg",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|r| r.as_str() == s)
    }
    pub(crate) fn code(self) -> i64 {
        self as i64 + 1
    }
    pub(crate) fn from_code(c: i64) -> Option<Self> {
        Self::ALL.get(usize::try_from(c - 1).ok()?).copied()
    }
}

/// One refusal as seen by the proxy, with the occupancy at that moment.
#[derive(Debug, Clone, Default)]
pub struct Refusal {
    pub ts_ms: i64,
    pub reason: Option<Reason>,
    /// WebSocket handshake or message (else plain HTTP).
    pub ws: bool,
    pub key: Option<String>,
    /// Client path without query, ids collapsed (see [`route_of`]).
    pub route: String,
    pub ua: Option<String>,
    /// Known only for WebSocket messages (the connection's last model).
    pub model: Option<String>,
    pub account: Option<String>,
    /// Limit in effect: requests, connections or bytes (0 = unlimited).
    pub limit: u64,
    /// Admitted requests (capped ones; WebSockets included) at refusal.
    pub inflight: u64,
    /// Client WebSocket connections at refusal.
    pub ws_conns: u64,
    /// Buffered body bytes at refusal.
    pub buffered: u64,
    /// Requested body / message bytes (body kinds).
    pub bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Group {
    pub minute: i64,
    pub reason: Reason,
    pub ws: bool,
    pub key: Option<String>,
    pub route: String,
    pub ua: Option<String>,
    pub model: Option<String>,
    pub account: Option<String>,
    pub limit: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Agg {
    pub count: u64,
    pub first_ms: i64,
    pub last_ms: i64,
    pub max_inflight: u64,
    pub max_ws: u64,
    pub max_buffered: u64,
    pub max_bytes: Option<u64>,
    pub sum_bytes: Option<u64>,
}

#[derive(Default)]
pub(crate) struct Aggregator {
    map: Mutex<HashMap<Group, Agg>>,
}

fn clip(s: &str) -> String {
    match s.char_indices().nth(MAX_TEXT) {
        Some((i, _)) => s[..i].to_string(),
        None => s.to_string(),
    }
}

/// Route label for a client path: no query, id-like segments replaced by `:id`, so
/// passthrough paths with task or file ids do not create unbounded groups.
pub fn route_of(path_and_query: &str) -> String {
    let path = path_and_query.split('?').next().unwrap_or("/");
    let parts: Vec<&str> = path
        .split('/')
        .map(|seg| {
            let digits = seg.bytes().filter(u8::is_ascii_digit).count();
            if seg.len() >= 16 || (digits > 0 && seg.len() >= 8) {
                ":id"
            } else {
                seg
            }
        })
        .collect();
    clip(&parts.join("/"))
}

impl Aggregator {
    pub fn add(&self, r: Refusal) {
        let Some(reason) = r.reason else { return };
        let mut map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        let mut g = Group {
            minute: r.ts_ms - r.ts_ms.rem_euclid(60_000),
            reason,
            ws: r.ws,
            key: r.key,
            route: clip(&r.route),
            ua: r.ua.as_deref().map(clip),
            model: r.model,
            account: r.account,
            limit: r.limit,
        };
        if map.len() >= MAX_GROUPS && !map.contains_key(&g) {
            g.key = None;
            g.ua = None;
            g.model = None;
            g.account = None;
            g.route = "*".into();
        }
        let a = map.entry(g).or_insert_with(|| Agg {
            first_ms: r.ts_ms,
            ..Agg::default()
        });
        a.count += 1;
        a.first_ms = a.first_ms.min(r.ts_ms);
        a.last_ms = a.last_ms.max(r.ts_ms);
        a.max_inflight = a.max_inflight.max(r.inflight);
        a.max_ws = a.max_ws.max(r.ws_conns);
        a.max_buffered = a.max_buffered.max(r.buffered);
        if let Some(b) = r.bytes {
            a.max_bytes = Some(a.max_bytes.unwrap_or(0).max(b));
            a.sum_bytes = Some(a.sum_bytes.unwrap_or(0).saturating_add(b));
        }
    }
    pub fn take(&self) -> HashMap<Group, Agg> {
        std::mem::take(&mut *self.map.lock().unwrap_or_else(|e| e.into_inner()))
    }
    /// Put back groups whose write failed, merged with anything added meanwhile.
    pub fn restore(&self, rows: HashMap<Group, Agg>) {
        let mut map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        for (g, b) in rows {
            if map.len() >= MAX_GROUPS && !map.contains_key(&g) {
                continue;
            }
            let a = map.entry(g).or_insert_with(|| Agg {
                first_ms: b.first_ms,
                ..Agg::default()
            });
            a.count += b.count;
            a.first_ms = a.first_ms.min(b.first_ms);
            a.last_ms = a.last_ms.max(b.last_ms);
            a.max_inflight = a.max_inflight.max(b.max_inflight);
            a.max_ws = a.max_ws.max(b.max_ws);
            a.max_buffered = a.max_buffered.max(b.max_buffered);
            a.max_bytes = a.max_bytes.max(b.max_bytes);
            a.sum_bytes = match (a.sum_bytes, b.sum_bytes) {
                (None, None) => None,
                (x, y) => Some(x.unwrap_or(0).saturating_add(y.unwrap_or(0))),
            };
        }
    }
    pub fn is_empty(&self) -> bool {
        self.map
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }
}

/// One stored per-minute group (`/usage/v1/refusals`).
#[derive(Debug, Clone, Serialize)]
pub struct RefusalRow {
    /// Start of the minute (unix ms).
    pub minute_ms: i64,
    pub reason: Reason,
    /// `http` or `ws`.
    pub transport: &'static str,
    pub key: Option<String>,
    pub route: Option<String>,
    pub ua: Option<String>,
    pub model: Option<String>,
    pub account: Option<String>,
    pub instance: Option<String>,
    /// Limit in effect: requests (`inflight`), connections (`ws`) or bytes (body kinds); 0 = unlimited.
    pub limit: u64,
    pub count: u64,
    pub first_ms: i64,
    pub last_ms: i64,
    /// Highest occupancy seen at a refusal in this minute.
    pub max_inflight: u64,
    pub max_ws: u64,
    pub max_buffered_bytes: u64,
    /// Largest refused body / message, and the total (body kinds only).
    pub max_bytes: Option<u64>,
    pub sum_bytes: Option<u64>,
}

/// Totals for a time range (`/usage/v1/summary` → `refusals`, `/usage/v1/refusals`).
#[derive(Debug, Clone, Default, Serialize)]
pub struct RefusalTotals {
    pub count: u64,
    pub inflight: u64,
    pub ws: u64,
    pub body: u64,
    pub ws_msg: u64,
    /// Per-minute groups in range.
    pub rows: u64,
}
impl RefusalTotals {
    pub(crate) fn add(&mut self, r: Reason, n: u64, rows: u64) {
        self.count += n;
        self.rows += rows;
        match r {
            Reason::Inflight => self.inflight += n,
            Reason::Ws => self.ws += n,
            Reason::Body => self.body += n,
            Reason::WsMsg => self.ws_msg += n,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(ts: i64, reason: Reason, ua: &str, inflight: u64) -> Refusal {
        Refusal {
            ts_ms: ts,
            reason: Some(reason),
            route: "/v1/responses".into(),
            ua: Some(ua.into()),
            limit: 64,
            inflight,
            ..Default::default()
        }
    }

    #[test]
    fn groups_per_minute_and_keeps_maxima() {
        let a = Aggregator::default();
        a.add(r(60_000 * 5 + 10, Reason::Inflight, "codex", 64));
        a.add(r(60_000 * 5 + 50_000, Reason::Inflight, "codex", 63));
        a.add(r(60_000 * 6 + 1, Reason::Inflight, "codex", 64));
        a.add(r(60_000 * 5 + 20, Reason::Ws, "codex", 10));
        let m = a.take();
        assert_eq!(m.len(), 3);
        let g = m
            .iter()
            .find(|(g, _)| g.minute == 300_000 && g.reason == Reason::Inflight)
            .unwrap()
            .1;
        assert_eq!(
            (g.count, g.first_ms, g.last_ms, g.max_inflight),
            (2, 300_010, 350_000, 64)
        );
        assert!(a.is_empty());
    }

    #[test]
    fn route_collapses_ids_and_query() {
        assert_eq!(route_of("/v1/models?client_version=1"), "/v1/models");
        assert_eq!(
            route_of("/backend-api/wham/tasks/task_e_0123456789abcdef/turns"),
            "/backend-api/wham/tasks/:id/turns"
        );
        assert_eq!(
            route_of("/backend-api/wham/usage"),
            "/backend-api/wham/usage"
        );
    }

    #[test]
    fn full_aggregator_collapses_new_groups() {
        let a = Aggregator::default();
        for i in 0..MAX_GROUPS + 10 {
            a.add(r(0, Reason::Inflight, &format!("ua{i}"), 1));
        }
        let m = a.take();
        assert_eq!(m.len(), MAX_GROUPS + 1);
        assert_eq!(
            m.values().map(|a| a.count).sum::<u64>(),
            (MAX_GROUPS + 10) as u64
        );
    }
}
