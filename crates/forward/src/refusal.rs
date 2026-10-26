//! Local-limit refusals: counted per minute in the usage log (when enabled) and logged at
//! most once per reason per minute with the count since the previous line. The in-memory
//! per-reason counters in [`ciallopa_upstream::drain::Drain`] are the fallback when usage
//! is off. Draining refusals never reach this module.
use crate::AppState;
use ciallopa_upstream::drain::RefusalKind;
use ciallopa_usage::RefusalReason;
use http::HeaderMap;
use std::sync::Mutex;

/// Minimum spacing of WARN lines per reason.
const LOG_EVERY_MS: i64 = 60_000;

/// (last line ms, refusals since that line) per [`RefusalKind`].
static LOG: [Mutex<(i64, u64)>; 4] = [
    Mutex::new((i64::MIN, 0)),
    Mutex::new((i64::MIN, 0)),
    Mutex::new((i64::MIN, 0)),
    Mutex::new((i64::MIN, 0)),
];

pub(crate) struct Refusal<'a> {
    pub kind: RefusalKind,
    pub ws: bool,
    pub key: Option<&'a str>,
    pub path: &'a str,
    pub headers: &'a HeaderMap,
    pub model: Option<&'a str>,
    pub account: Option<&'a str>,
    pub limit: u64,
    pub bytes: Option<u64>,
}

fn reason(k: RefusalKind) -> RefusalReason {
    match k {
        RefusalKind::Inflight => RefusalReason::Inflight,
        RefusalKind::Ws => RefusalReason::Ws,
        RefusalKind::Body => RefusalReason::Body,
        RefusalKind::WsMsg => RefusalReason::WsMsg,
    }
}

pub(crate) fn note(st: &AppState, r: Refusal<'_>) {
    let d = &st.upstream.drain;
    let (inflight, ws_conns, buffered) = (
        d.inflight() as u64,
        d.ws_connections() as u64,
        d.body_bytes(),
    );
    let now = ciallopa_usage::now_ms();
    let route = ciallopa_usage::refusals::route_of(r.path);
    let ua = r
        .headers
        .get(http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty());
    let count = {
        let mut l = LOG[r.kind as usize]
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        l.1 += 1;
        if now.saturating_sub(l.0) >= LOG_EVERY_MS {
            let n = l.1;
            *l = (now, 0);
            Some(n)
        } else {
            None
        }
    };
    if let Some(n) = count {
        let what = match r.kind {
            RefusalKind::Inflight => "inflight request cap reached; refusing request",
            RefusalKind::Ws => "websocket connection cap reached",
            RefusalKind::Body => "buffered body budget full; refusing request",
            RefusalKind::WsMsg => "buffered body budget full; refusing websocket message",
        };
        tracing::warn!(
            reason = r.kind.as_str(),
            refused = n,
            limit = r.limit,
            inflight,
            ws = ws_conns,
            buffered,
            bytes = r.bytes,
            route = %route,
            key = r.key.unwrap_or(""),
            "{what} (at most one line per reason per minute; refused = count since the last line)"
        );
    }
    if let Some(u) = &st.usage {
        u.refuse(ciallopa_usage::Refusal {
            ts_ms: now,
            reason: Some(reason(r.kind)),
            ws: r.ws,
            key: r.key.map(str::to_string),
            route,
            ua: ua.map(str::to_string),
            model: r.model.map(str::to_string),
            account: r.account.map(str::to_string),
            limit: r.limit,
            inflight,
            ws_conns,
            buffered,
            bytes: r.bytes,
        });
    }
}
