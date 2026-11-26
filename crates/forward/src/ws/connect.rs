use crate::attempt::{Accepted, Dialed, Loop, Phase, Policy, SendInfo, Transport};
use crate::headers::upstream_headers;
use crate::usage::Tracker;
use crate::{errors, AppState};
use axum::response::Response;
use ciallopa_core::{AccountId, Credentials, RouteRequest};
use ciallopa_upstream::ws::handshake_status;
use ciallopa_upstream::UpstreamWs;
use http::HeaderMap;
use std::time::Duration;

/// Accounts a downstream connection has served, for `x-codex-turn-state` of unknown owner:
/// the client's value can only belong to the account the connection was opened on.
#[derive(Clone, Copy)]
pub(super) enum TurnStateOrigin<'a> {
    /// The handshake: the first account tried may own it.
    Initial,
    /// A redial; the connection has only served this account so far.
    Pinned(&'a AccountId),
    /// A redial after the connection has served another account.
    Switched,
}

impl TurnStateOrigin<'_> {
    /// Whether an attempt on `id` with `route` has left the owner.
    fn switched(self, id: &AccountId, route: &RouteRequest) -> bool {
        match self {
            Self::Initial => !route.exclude.is_empty(),
            Self::Pinned(orig) => id != orig || !route.exclude.is_empty(),
            Self::Switched => true,
        }
    }
}

pub(super) async fn connect(
    st: &AppState,
    path: &str,
    headers: &HeaderMap,
    origin: TurnStateOrigin<'_>,
    route: &mut RouteRequest,
    hold_cap: Duration,
    tracker: Option<&mut Tracker>,
) -> Result<(UpstreamWs, HeaderMap, AccountId, u64), Response> {
    let mut policy = if hold_cap == crate::attempt::WS_HOLD_MAX {
        Policy::ws_handshake()
    } else {
        Policy::ws_redial()
    };
    policy.hold_cap = hold_cap;
    let transport = WsTransport {
        st,
        path,
        headers,
        origin,
    };
    let (accepted, id) = Loop {
        st,
        transport,
        policy,
        route,
        info: &mut SendInfo::default(),
        tracker,
    }
    .run()
    .await?;
    match accepted {
        Accepted::Connected((ws, headers, generation)) => Ok((ws, headers, id, generation)),
        Accepted::Passthrough {
            status,
            headers,
            body,
        } => Err(crate::attempt::buffered(status, &headers, body)),
    }
}

struct WsTransport<'a> {
    st: &'a AppState,
    path: &'a str,
    headers: &'a HeaderMap,
    origin: TurnStateOrigin<'a>,
}

impl Transport for WsTransport<'_> {
    type Conn = (UpstreamWs, HeaderMap, u64);

    async fn dial(
        &mut self,
        id: &AccountId,
        creds: &Credentials,
        route: &RouteRequest,
        phase: &mut Phase,
        policy: &Policy,
        mut tracker: Option<&mut Tracker>,
    ) -> Dialed<Self::Conn> {
        // Capture before the dial reads egress options.
        let generation = self.st.upstream.egress_gen();
        let headers = upstream_headers(
            &crate::turn_state::scope(self.headers, id, self.origin.switched(id, route)),
            creds,
        );
        let sent = std::time::Instant::now();
        if let Some(t) = tracker.as_deref_mut() {
            t.prep(sent);
        }
        match self.st.upstream.ws_connect_as(id, self.path, headers).await {
            Ok((ws, response)) => {
                if let Some(t) = tracker {
                    t.received_headers(sent);
                }
                if policy.note_successful_attempt {
                    self.st.router.note_attempt(id);
                }
                Dialed::Connected {
                    conn: (ws, response.headers().clone(), generation),
                    status: 101,
                }
            }
            Err(e) => {
                if let Some((status, headers, body)) = handshake_status(&e) {
                    self.st.router.note_attempt(id);
                    *phase = Phase::ReadingError {
                        account: id.clone(),
                        status,
                        request_id: errors::request_id(&headers),
                    };
                    Dialed::Refused {
                        status,
                        headers,
                        body: body.into(),
                    }
                } else {
                    let code = match &e {
                        ciallopa_upstream::UpstreamError::Proxy(_) => "proxy_connect",
                        ciallopa_upstream::UpstreamError::ProxyTimeout => "proxy_timeout",
                        _ => errors::ws_connect_code(&e),
                    };
                    Dialed::Transport {
                        code,
                        cause: errors::chain(&e),
                    }
                }
            }
        }
    }

    fn headers(conn: &Self::Conn) -> &HeaderMap {
        &conn.1
    }
}
