//! Responses WebSocket: one downstream WS ↔ one upstream WS pinned to one account.
use crate::timing;
mod connect;
pub(crate) mod frames;
mod session;

use self::connect::connect;
use self::session::{finish_on_shutdown, relay};
use crate::budget::PreparationBudget;
use crate::usage::{endpoint_of, Tracker};
use crate::ws_upgrade::Upgrade;
use crate::{errors, map_path, pq, session_route, AppState};
use axum::extract::{OriginalUri, State};
use axum::response::Response;
use ciallopa_usage::Transport;
use http::{HeaderMap, StatusCode};
use std::time::Duration;

pub(crate) async fn handler(
    State(st): State<AppState>,
    key: crate::Key,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    lease: Option<axum::Extension<ciallopa_upstream::drain::Lease>>,
    ws: Upgrade,
) -> Response {
    let Some(path) = map_path(pq(&uri)) else {
        return errors::json_error(StatusCode::NOT_FOUND, "not_found", "unknown path");
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
            timing::Timing::capture(&cfg).preparation,
            timing::Timing::capture(&cfg).idle,
        )
    };
    let mut budget = PreparationBudget::new(preparation_limit);
    let (up, resp_headers, account, gen) = match budget
        .run(connect(
            &st,
            &path,
            &headers,
            &mut route,
            crate::attempt::WS_HOLD_MAX,
            handshake.as_mut(),
        ))
        .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(r)) | Err(r) => {
            // One row for the failed handshake, including preparation and all retries.
            if let Some(mut t) = handshake {
                let (parts, body) = r.into_parts();
                let b = axum::body::to_bytes(body, 1 << 20)
                    .await
                    .unwrap_or_default();
                t.record_response(&parts, &b);
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
        ws_ext_down: None,
    };
    let hs_headers = resp_headers.clone();
    let compression = st.config.load().ws_compression;
    let ctx = Ctx {
        ws_ext_down: ws.agreed_ext(compression),
        ..ctx
    };
    let drain = st.upstream.drain.clone();
    let close_grace = timing::scale(&st, Duration::from_secs(2));
    let advertised = advertised_max_message(&resp_headers, max_message);
    let mut resp = ws.on_upgrade(compression, max_message, move |socket| async move {
        let _slot = slot;
        let handoff = lease.map(|axum::Extension(l)| l);
        finish_on_shutdown(
            drain,
            close_grace,
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
            // replaced by the capped value below
            || n == MAX_MESSAGE_BYTES_HEADER
        {
            continue;
        }
        resp.headers_mut().append(k.clone(), v.clone());
    }
    resp.headers_mut().insert(
        MAX_MESSAGE_BYTES_HEADER,
        http::HeaderValue::from(advertised),
    );
    resp
}

/// Handshake response header with the largest request message the server accepts, in bytes
/// (serialized text before compression). Codex 0.170+ reads it from the 101 and sends a
/// larger turn over HTTP instead of uploading it to be refused.
const MAX_MESSAGE_BYTES_HEADER: &str = "x-codex-websocket-max-message-bytes";

/// Value of [`MAX_MESSAGE_BYTES_HEADER`] for the client: the upstream's own (first value,
/// parsed like Codex: a decimal `usize`, anything else ignored), never above the local
/// `ws_max_message_mb` limit, which a larger message cannot pass anyway.
fn advertised_max_message(upstream: &HeaderMap, local: usize) -> usize {
    upstream
        .get(MAX_MESSAGE_BYTES_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .map_or(local, |u| u.min(local))
}

/// What the relay needs to redial upstream on another account.
struct Ctx {
    path: String,
    headers: HeaderMap,
    /// Client key label (usage log).
    key: Option<String>,
    preparation_limit: Duration,
    /// permessage-deflate answer sent to the client (None = not agreed).
    ws_ext_down: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advertised_max_message_is_min_of_upstream_and_local() {
        const MIB: usize = 1 << 20;
        let h = |vals: &[&str]| {
            let mut h = HeaderMap::new();
            for v in vals {
                h.append(MAX_MESSAGE_BYTES_HEADER, v.parse().unwrap());
            }
            h
        };
        assert_eq!(advertised_max_message(&h(&[]), 16 * MIB), 16 * MIB);
        assert_eq!(advertised_max_message(&h(&["1048576"]), 16 * MIB), MIB);
        assert_eq!(
            advertised_max_message(&h(&["33554432"]), 16 * MIB),
            16 * MIB
        );
        assert_eq!(
            advertised_max_message(&h(&["16777216"]), 16 * MIB),
            16 * MIB
        );
        assert_eq!(
            advertised_max_message(&h(&["16777216"]), 4096 * MIB),
            16 * MIB
        );
        // Parsed like Codex (`str::parse::<usize>`): invalid values are ignored.
        for bad in [
            "",
            "abc",
            "-1",
            " 1024",
            "1024 ",
            "1e6",
            "99999999999999999999999",
        ] {
            assert_eq!(
                advertised_max_message(&h(&[bad]), 2 * MIB),
                2 * MIB,
                "{bad:?}"
            );
        }
        // The first value counts, as with `HeaderMap::get`.
        assert_eq!(
            advertised_max_message(&h(&["2097152", "1024"]), 16 * MIB),
            2 * MIB
        );
        // An upstream that says 0 is passed on: it is the upstream's real limit.
        assert_eq!(advertised_max_message(&h(&["0"]), 16 * MIB), 0);
    }
}
