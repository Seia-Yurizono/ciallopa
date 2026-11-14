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

pub(super) async fn connect(
    st: &AppState,
    path: &str,
    headers: &HeaderMap,
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
    let transport = WsTransport { st, path, headers };
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
    ) -> Dialed<Self::Conn> {
        // Capture before the dial reads egress options.
        let generation = self.st.upstream.egress_gen();
        let headers = upstream_headers(
            &crate::turn_state::scope(self.headers, id, !route.exclude.is_empty()),
            creds,
        );
        match self.st.upstream.ws_connect_as(id, self.path, headers).await {
            Ok((ws, response)) => {
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
