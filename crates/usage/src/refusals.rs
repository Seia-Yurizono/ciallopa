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
/// Longest stored route label.
const MAX_ROUTE: usize = 200;
/// Longest stored user agent (write-time trim; drop long tokens via redact).
const MAX_UA: usize = 60;

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

fn clip(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((i, _)) => s[..i].to_string(),
        None => s.to_string(),
    }
}

fn clip_ua(s: &str) -> String {
    // Strip bearer-like tokens before capping length.
    clip(&crate::detail::redact_text(s), MAX_UA)
}

/// Same clip used at write time; exposed for the v12 history rewrite.
pub(crate) fn clip_ua_for_migrate(s: &str) -> String {
    clip_ua(s)
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
    clip(&parts.join("/"), MAX_ROUTE)
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
            route: clip(&r.route, MAX_ROUTE),
            ua: r.ua.as_deref().map(clip_ua),
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
    fn ua_truncated_to_sixty() {
        let long = "Mozilla/5.0 ".to_string() + &"x".repeat(80) + " sk-proj-ABCDEFGHIJKLMNOP";
        let a = Aggregator::default();
        a.add(r(0, Reason::Inflight, &long, 1));
        let m = a.take();
        let (g, _) = m.iter().next().unwrap();
        let ua = g.ua.as_deref().unwrap();
        assert!(
            ua.chars().count() <= MAX_UA,
            "ua len {}",
            ua.chars().count()
        );
        assert!(!ua.contains("sk-proj-ABCDEF"), "token leaked: {ua}");
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

use crate::filter::{contains_filters, LabelNames};
use crate::write::WriterState;
use crate::Transport;
use rusqlite::{params, params_from_iter, types::Value, Connection};

/// Merge aggregated refusal groups into their per-minute rows (one transaction).
pub(crate) fn upsert_refusals(
    tx: &rusqlite::Transaction<'_>,
    ws: &mut WriterState,
    rows: &HashMap<crate::refusals::Group, crate::refusals::Agg>,
) -> rusqlite::Result<()> {
    {
        let mut st = tx.prepare_cached(
            "INSERT INTO refusals(minute,reason,transport,key,route,ua,model,account,instance,lim,\
             count,first_ms,last_ms,max_inflight,max_ws,max_buffered,max_bytes,sum_bytes) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18) \
             ON CONFLICT(minute,reason,transport,key,route,ua,model,account,instance,lim) DO UPDATE SET \
             count = count + excluded.count, first_ms = min(first_ms, excluded.first_ms), \
             last_ms = max(last_ms, excluded.last_ms), max_inflight = max(max_inflight, excluded.max_inflight), \
             max_ws = max(max_ws, excluded.max_ws), max_buffered = max(max_buffered, excluded.max_buffered), \
             max_bytes = CASE WHEN max_bytes IS NULL THEN excluded.max_bytes WHEN excluded.max_bytes IS NULL THEN max_bytes ELSE max(max_bytes, excluded.max_bytes) END, \
             sum_bytes = CASE WHEN sum_bytes IS NULL AND excluded.sum_bytes IS NULL THEN NULL ELSE coalesce(sum_bytes,0) + coalesce(excluded.sum_bytes,0) END",
        )?;
        let instance_name = ws.instance.clone();
        let instance = ws.label(tx, Some(&instance_name))?.unwrap_or(0);
        for (g, a) in rows {
            let key = ws.label(tx, g.key.as_deref())?.unwrap_or(0);
            let route = ws
                .label(tx, Some(g.route.as_str()).filter(|r| !r.is_empty()))?
                .unwrap_or(0);
            let ua = ws.label(tx, g.ua.as_deref())?.unwrap_or(0);
            let model = ws.label(tx, g.model.as_deref())?.unwrap_or(0);
            let account = ws.label(tx, g.account.as_deref())?.unwrap_or(0);
            let as_i = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
            st.execute(params![
                g.minute,
                g.reason.code(),
                if g.ws {
                    Transport::Ws as i64
                } else {
                    Transport::Http as i64
                },
                key,
                route,
                ua,
                model,
                account,
                instance,
                as_i(g.limit),
                as_i(a.count),
                a.first_ms,
                a.last_ms,
                as_i(a.max_inflight),
                as_i(a.max_ws),
                as_i(a.max_buffered),
                a.max_bytes.map(as_i),
                a.sum_bytes.map(as_i),
            ])?;
        }
    }
    Ok(())
}

/// Range and filters for refusal reads. Only key and instance apply (a refusal happens
/// before the account and, for HTTP, the model are known).
#[derive(Debug, Clone, Default)]
pub struct RefusalQuery {
    pub from_ms: i64,
    pub to_ms: i64,
    pub key: Option<String>,
    pub instance: Option<String>,
    pub reason: Option<crate::refusals::Reason>,
    pub limit: usize,
    pub offset: usize,
}

fn refusal_where(
    c: &Connection,
    q: &RefusalQuery,
) -> rusqlite::Result<Option<(String, Vec<Value>)>> {
    // A row covers [minute, minute + 60 s); keep minutes overlapping the range.
    let mut s = String::from(" WHERE minute > ? AND minute <= ?");
    let mut p = vec![(q.from_ms - 60_000).into(), q.to_ms.into()];
    if !contains_filters(
        c,
        &mut s,
        &mut p,
        &[("key", &q.key), ("instance", &q.instance)],
    )? {
        return Ok(None);
    }
    if let Some(r) = q.reason {
        s.push_str(" AND reason = ?");
        p.push(r.code().into());
    }
    Ok(Some((s, p)))
}

pub(crate) fn refusal_totals(
    c: &Connection,
    q: &RefusalQuery,
) -> rusqlite::Result<crate::refusals::RefusalTotals> {
    let mut t = crate::refusals::RefusalTotals::default();
    let Some((w, p)) = refusal_where(c, q)? else {
        return Ok(t);
    };
    let mut st = c.prepare(&format!(
        "SELECT reason, sum(count), count(*) FROM refusals{w} GROUP BY reason"
    ))?;
    let mut rows = st.query(params_from_iter(p))?;
    while let Some(r) = rows.next()? {
        if let Some(reason) = crate::refusals::Reason::from_code(r.get(0)?) {
            t.add(
                reason,
                r.get::<_, i64>(1)?.max(0) as u64,
                r.get::<_, i64>(2)?.max(0) as u64,
            );
        }
    }
    Ok(t)
}

/// `(minute, reason, count)` per minute and reason, for bucketing.
pub(crate) fn refusal_minutes(
    c: &Connection,
    q: &RefusalQuery,
) -> rusqlite::Result<Vec<(i64, crate::refusals::Reason, u64)>> {
    let Some((w, p)) = refusal_where(c, q)? else {
        return Ok(vec![]);
    };
    let mut st = c.prepare(&format!(
        "SELECT minute, reason, sum(count) FROM refusals{w} GROUP BY minute, reason"
    ))?;
    let rows = st.query_map(params_from_iter(p), |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, i64>(2)?,
        ))
    })?;
    let mut out = vec![];
    for row in rows {
        let (m, reason, n) = row?;
        if let Some(reason) = crate::refusals::Reason::from_code(reason) {
            out.push((m, reason, n.max(0) as u64));
        }
    }
    Ok(out)
}

pub(crate) fn refusal_rows(
    c: &Connection,
    q: &RefusalQuery,
) -> rusqlite::Result<Vec<crate::refusals::RefusalRow>> {
    let Some((w, mut p)) = refusal_where(c, q)? else {
        return Ok(vec![]);
    };
    let mut labels = LabelNames::new(c);
    p.push((q.limit.clamp(1, 500) as i64).into());
    p.push((q.offset as i64).into());
    let mut st = c.prepare(&format!(
        "SELECT minute,reason,transport,key,route,ua,model,account,instance,lim,count,first_ms,last_ms,\
         max_inflight,max_ws,max_buffered,max_bytes,sum_bytes FROM refusals{w} \
         ORDER BY minute DESC, last_ms DESC, count DESC LIMIT ? OFFSET ?"
    ))?;
    let u = |v: i64| v.max(0) as u64;
    let rows = st.query_map(params_from_iter(p), |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, i64>(2)?,
            [
                r.get::<_, i64>(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
                r.get(8)?,
            ],
            [
                r.get::<_, i64>(9)?,
                r.get(10)?,
                r.get(11)?,
                r.get(12)?,
                r.get(13)?,
                r.get(14)?,
                r.get(15)?,
            ],
            (r.get::<_, Option<i64>>(16)?, r.get::<_, Option<i64>>(17)?),
        ))
    })?;
    let mut out = vec![];
    for row in rows {
        let (minute, reason, transport, l, n, (max_bytes, sum_bytes)) = row?;
        let Some(reason) = crate::refusals::Reason::from_code(reason) else {
            continue;
        };
        labels.load(l.into_iter().filter(|id| *id != 0).map(Some))?;
        let name = |id: i64| (id != 0).then(|| labels.get(&id).cloned()).flatten();
        out.push(crate::refusals::RefusalRow {
            minute_ms: minute,
            reason,
            transport: if transport == Transport::Ws as i64 {
                "ws"
            } else {
                "http"
            },
            key: name(l[0]),
            route: name(l[1]),
            ua: name(l[2]),
            model: name(l[3]),
            account: name(l[4]),
            instance: name(l[5]),
            limit: u(n[0]),
            count: u(n[1]),
            first_ms: n[2],
            last_ms: n[3],
            max_inflight: u(n[4]),
            max_ws: u(n[5]),
            max_buffered_bytes: u(n[6]),
            max_bytes: max_bytes.map(u),
            sum_bytes: sum_bytes.map(u),
        });
    }
    Ok(out)
}
