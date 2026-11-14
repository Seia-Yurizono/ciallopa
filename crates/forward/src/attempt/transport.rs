use super::{Dialed, Outbound, Phase, Policy, Transport};
use crate::headers::upstream_headers;
use crate::limits::{read_body, BODY_TIMEOUT, ERROR_BODY_LIMIT};
use crate::{errors, AppState};
use ciallopa_core::{AccountId, Credentials, RouteRequest};
use http::HeaderMap;
use std::time::Duration;

pub(super) struct HttpTransport<'a> {
    pub st: &'a AppState,
    pub ob: &'a Outbound,
    pub header_timeout: Duration,
}

impl Transport for HttpTransport<'_> {
    type Conn = reqwest::Response;

    async fn dial(
        &mut self,
        id: &AccountId,
        creds: &Credentials,
        route: &RouteRequest,
        phase: &mut Phase,
        policy: &Policy,
    ) -> Dialed<Self::Conn> {
        let url = self.st.upstream.url(&self.ob.path);
        let (client, via_proxy) = match self.st.upstream.http_for_attempt(id) {
            Ok(client) => client,
            Err(e) => {
                return Dialed::Transport {
                    code: "transport",
                    cause: errors::chain(&e),
                }
            }
        };
        let mut headers = upstream_headers(
            &crate::turn_state::scope(&self.ob.headers, id, !route.exclude.is_empty()),
            creds,
        );
        if let Some(cookie) = self.st.upstream.cookie_header(id, &url) {
            headers.insert(http::header::COOKIE, cookie);
        }
        let request = client
            .request(self.ob.method.clone(), &url)
            .headers(headers)
            .body(self.ob.body.clone());
        let response = match tokio::time::timeout(self.header_timeout, request.send()).await {
            Err(_) => {
                return Dialed::Transport {
                    code: "header_timeout",
                    cause: format!(
                        "waiting for upstream response headers exceeded {}s",
                        self.header_timeout.as_secs()
                    ),
                }
            }
            Ok(Err(e)) => {
                return Dialed::Transport {
                    code: errors::net_code(&e, via_proxy),
                    cause: errors::chain(&e),
                }
            }
            Ok(Ok(response)) => response,
        };
        self.st.upstream.store_cookies(id, &url, response.headers());
        let status = response.status().as_u16();
        if status >= 400 || policy.note_successful_attempt {
            self.st.router.note_attempt(id);
        }
        *phase = Phase::ReadingError {
            account: id.clone(),
            status,
            request_id: errors::request_id(response.headers()),
        };
        let response = if self.ob.decode_response {
            match crate::response_body::prepare(response) {
                Ok(response) => response,
                Err(e) => {
                    return Dialed::UnsupportedEncoding {
                        status,
                        request_id: match phase {
                            Phase::ReadingError { request_id, .. } => request_id.clone(),
                            _ => None,
                        },
                        message: e.to_string(),
                    }
                }
            }
        } else {
            response
        };
        if status < 400 {
            return Dialed::Connected {
                conn: response,
                status,
            };
        }
        let headers = response.headers().clone();
        match read_body(response, ERROR_BODY_LIMIT, BODY_TIMEOUT).await {
            Ok(body) => Dialed::Refused {
                status,
                headers,
                body,
            },
            Err(failure) => Dialed::BodyIncomplete {
                status,
                headers,
                failure,
            },
        }
    }

    fn headers(conn: &Self::Conn) -> &HeaderMap {
        conn.headers()
    }
}
