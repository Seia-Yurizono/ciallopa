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
use crate::{map_path, pq, proxy, session_route, AppState};
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
        return proxy::json_error(StatusCode::NOT_FOUND, "not_found", "unknown path");
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
            proxy::WS_HOLD_MAX,
            handshake.as_mut(),
        ))
        .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(r)) | Err(r) => {
            // One row for the failed handshake, including preparation and all retries.
            if let Some(mut t) = handshake {
                t.status(r.status().as_u16());
                t.finish_response();
                let (parts, body) = r.into_parts();
                t.error_headers(&parts.headers);
                if let Some(m) = parts.extensions.get::<crate::errors::ErrMeta>() {
                    t.meta_err(m);
                }
                let b = axum::body::to_bytes(body, 1 << 20)
                    .await
                    .unwrap_or_default();
                t.error_body(&b);
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
    let mut resp = ws.on_upgrade(compression, max_message, move |socket| async move {
        let _slot = slot;
        let handoff = lease.map(|axum::Extension(l)| l);
        finish_on_shutdown(
            drain,
            Duration::from_secs(2),
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
        {
            continue;
        }
        resp.headers_mut().append(k.clone(), v.clone());
    }
    resp
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
