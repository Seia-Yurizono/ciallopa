//! Responses WebSocket: one downstream WS ↔ one upstream WS pinned to one account.
use crate::headers::upstream_headers;
use crate::quota::{parse_quota, parse_quota_json};
use crate::{map_path, pq, proxy, session_route, AppState};
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{OriginalUri, State};
use axum::response::Response;
use cpa_lite_core::{AccountId, Outcome, RouteError, RouteRequest};
use cpa_lite_upstream::retry::{delay_for, is_retryable_status, parse_limit_body, retry_after};
use cpa_lite_upstream::ws::handshake_status;
use cpa_lite_upstream::{UpstreamWs, WsMessage};
use futures::{SinkExt, StreamExt};
use http::{HeaderMap, StatusCode};
use std::time::Duration;
use tungstenite_close::{CloseCode, UpCloseFrame};

mod tungstenite_close {
    pub use cpa_lite_upstream::ws_close::{CloseCode, CloseFrame as UpCloseFrame};
}

/// Close code used when the upstream account hit its limit; the client reconnects.
const CLOSE_TRY_AGAIN: u16 = 1013;
const CLOSE_INTERNAL: u16 = 1011;

pub(crate) async fn handler(
    State(st): State<AppState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let Some(path) = map_path(pq(&uri)) else {
        return proxy::json_error(StatusCode::NOT_FOUND, "not_found", "unknown path");
    };
    let mut route = session_route(&headers, None);
    let (up, resp_headers, account) = match connect(&st, &path, &headers, &mut route).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    let idle = Duration::from_secs(st.config.upstream_opts.stream_idle_timeout_secs.max(1));
    let st2 = st.clone();
    let mut resp = ws.on_upgrade(move |socket| relay(st2, route, account, socket, up, idle));
    for (k, v) in resp_headers.iter() {
        let n = k.as_str();
        if n.starts_with("sec-websocket-")
            || matches!(
                n,
                "connection" | "upgrade" | "content-length" | "transfer-encoding"
            )
        {
            continue;
        }
        resp.headers_mut().append(k.clone(), v.clone());
    }
    resp
}

async fn connect(
    st: &AppState,
    path: &str,
    headers: &HeaderMap,
    route: &mut RouteRequest,
) -> Result<(UpstreamWs, HeaderMap, AccountId), Response> {
    let retries = u64::from(st.config.upstream_opts.max_attempts);
    let mut transient = 0u64;
    let mut last: Option<Response> = None;
    loop {
        let accounts = st.accounts.list().await;
        let id = match st.router.select(route, &accounts) {
            Ok(s) => s.account,
            Err(RouteError::AllCoolingDown { retry_after }) => {
                return Err(last.unwrap_or_else(|| {
                    let mut r = proxy::json_error(
                        StatusCode::TOO_MANY_REQUESTS,
                        "usage_limit_reached",
                        "all accounts are cooling down",
                    );
                    if let Ok(v) = retry_after.as_secs().max(1).to_string().parse() {
                        r.headers_mut().insert(http::header::RETRY_AFTER, v);
                    }
                    r
                }))
            }
            Err(e) => {
                return Err(last.unwrap_or_else(|| {
                    proxy::json_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "no_account",
                        &e.to_string(),
                    )
                }))
            }
        };
        let mut creds = match st.accounts.credentials(&id).await {
            Ok(c) => c,
            Err(_) => {
                st.router.report(route, &id, &Outcome::AuthFailed);
                route.exclude.push(id);
                continue;
            }
        };
        let mut refreshed = false;
        loop {
            match st
                .upstream
                .ws_connect(path, upstream_headers(headers, &creds))
                .await
            {
                Ok((ws, resp)) => {
                    let quota = parse_quota(resp.headers());
                    st.router.report(route, &id, &Outcome::Success { quota });
                    return Ok((ws, resp.headers().clone(), id));
                }
                Err(e) => {
                    let Some((status, h, body)) = handshake_status(&e) else {
                        tracing::warn!(account = %id, error = %e, "upstream ws connect failed");
                        st.router
                            .report(route, &id, &Outcome::Transient { status: None });
                        transient += 1;
                        if transient > retries {
                            return Err(proxy::json_error(
                                StatusCode::BAD_GATEWAY,
                                "upstream_error",
                                "upstream websocket connect failed",
                            ));
                        }
                        tokio::time::sleep(delay_for(transient, None)).await;
                        break;
                    };
                    let resp = {
                        let mut r = Response::new(axum::body::Body::from(body.clone()));
                        *r.status_mut() =
                            StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
                        *r.headers_mut() = crate::headers::downstream_headers(&h);
                        r.headers_mut().remove(http::header::CONTENT_LENGTH);
                        r
                    };
                    match status {
                        401 if !refreshed => {
                            refreshed = true;
                            if let Ok(c) = st.accounts.force_refresh(&id).await {
                                creds = c;
                                continue;
                            }
                            st.router.report(route, &id, &Outcome::AuthFailed);
                            route.exclude.push(id.clone());
                            last = Some(resp);
                            break;
                        }
                        401 | 403 => {
                            st.router.report(route, &id, &Outcome::AuthFailed);
                            route.exclude.push(id.clone());
                            last = Some(resp);
                            break;
                        }
                        429 => {
                            let (usage_limit, ra) = parse_limit_body(&body);
                            st.router.report(
                                route,
                                &id,
                                &Outcome::RateLimited {
                                    retry_after: ra.or_else(|| retry_after(&h)),
                                    usage_limit,
                                    quota: parse_quota(&h),
                                },
                            );
                            route.exclude.push(id.clone());
                            last = Some(resp);
                            break;
                        }
                        s if s == 408 || is_retryable_status(s) => {
                            st.router
                                .report(route, &id, &Outcome::Transient { status: Some(s) });
                            transient += 1;
                            if transient > retries {
                                return Err(resp);
                            }
                            tokio::time::sleep(delay_for(transient, Some(&h))).await;
                            break;
                        }
                        s => {
                            st.router
                                .report(route, &id, &Outcome::ClientError { status: s });
                            return Err(resp);
                        }
                    }
                }
            }
        }
    }
}

fn to_up(m: Message) -> Option<WsMessage> {
    Some(match m {
        Message::Text(t) => WsMessage::text(t.as_str()),
        Message::Binary(b) => WsMessage::Binary(b),
        Message::Ping(b) => WsMessage::Ping(b),
        Message::Pong(b) => WsMessage::Pong(b),
        Message::Close(f) => WsMessage::Close(f.map(|f| UpCloseFrame {
            code: CloseCode::from(f.code),
            reason: f.reason.as_str().into(),
        })),
    })
}

fn to_down(m: WsMessage) -> Option<Message> {
    Some(match m {
        WsMessage::Text(t) => Message::Text(t.as_str().into()),
        WsMessage::Binary(b) => Message::Binary(b),
        WsMessage::Ping(b) => Message::Ping(b),
        WsMessage::Pong(b) => Message::Pong(b),
        WsMessage::Close(f) => Message::Close(f.map(|f| CloseFrame {
            code: u16::from(f.code),
            reason: f.reason.as_str().into(),
        })),
        WsMessage::Frame(_) => return None,
    })
}

fn close(code: u16, reason: &str) -> Message {
    Message::Close(Some(CloseFrame {
        code,
        reason: reason.into(),
    }))
}

/// Inspect an upstream text frame. Returns (limit outcome, request finished).
fn classify(text: &str) -> (Option<Outcome>, bool) {
    let looks = text.contains("\"error\"") || text.contains("response.");
    if !looks {
        return (None, false);
    }
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return (None, false);
    };
    let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
    match ty {
        "error" => {
            let status = v
                .get("status")
                .or_else(|| v.get("status_code"))
                .and_then(|s| s.as_u64());
            let err = v.get("error").cloned().unwrap_or_default();
            let code = err
                .get("code")
                .or_else(|| err.get("type"))
                .and_then(|c| c.as_str())
                .unwrap_or("");
            let usage = code.contains("usage_limit");
            if usage || status == Some(429) {
                let (_, ra) = parse_limit_body(text.as_bytes());
                let quota = v.get("headers").and_then(parse_quota_json);
                return (
                    Some(Outcome::RateLimited {
                        retry_after: ra,
                        usage_limit: usage,
                        quota,
                    }),
                    true,
                );
            }
            (None, true)
        }
        "response.completed" | "response.failed" | "response.incomplete" | "response.done" => {
            let usage = ty == "response.failed" && text.contains("usage_limit_reached");
            (
                usage.then_some(Outcome::RateLimited {
                    retry_after: None,
                    usage_limit: true,
                    quota: None,
                }),
                true,
            )
        }
        _ => (None, false),
    }
}

async fn relay(
    st: AppState,
    route: RouteRequest,
    account: AccountId,
    down: WebSocket,
    up: UpstreamWs,
    idle: Duration,
) {
    let (mut dtx, mut drx) = down.split();
    let (mut utx, mut urx) = up.split();
    let mut awaiting = false;
    let sleep = tokio::time::sleep(idle);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            m = drx.next() => match m {
                Some(Ok(m)) => {
                    let is_close = matches!(m, Message::Close(_));
                    if matches!(m, Message::Text(_) | Message::Binary(_)) {
                        if !awaiting {
                            sleep.as_mut().reset(tokio::time::Instant::now() + idle);
                        }
                        awaiting = true;
                    }
                    if let Some(u) = to_up(m) {
                        if utx.send(u).await.is_err() {
                            let _ = dtx.send(close(CLOSE_INTERNAL, "upstream gone")).await;
                            break;
                        }
                    }
                    if is_close { break; }
                }
                _ => {
                    let _ = utx.send(WsMessage::Close(None)).await;
                    break;
                }
            },
            m = urx.next() => {
                sleep.as_mut().reset(tokio::time::Instant::now() + idle);
                match m {
                    Some(Ok(WsMessage::Close(f))) => {
                        let code = f.as_ref().map(|f| u16::from(f.code)).unwrap_or(1000);
                        if code != 1000 {
                            st.router.report(&route, &account, &Outcome::Transient { status: None });
                        }
                        if let Some(d) = to_down(WsMessage::Close(f)) {
                            let _ = dtx.send(d).await;
                        }
                        break;
                    }
                    Some(Ok(m)) => {
                        let mut limit = None;
                        if let WsMessage::Text(t) = &m {
                            let (o, done) = classify(t.as_str());
                            if done { awaiting = false; }
                            limit = o;
                        }
                        if let Some(d) = to_down(m) {
                            if dtx.send(d).await.is_err() {
                                let _ = utx.send(WsMessage::Close(None)).await;
                                break;
                            }
                        }
                        if let Some(o) = limit {
                            st.router.report(&route, &account, &o);
                            let _ = dtx.send(close(CLOSE_TRY_AGAIN, "account rate limited; reconnect")).await;
                            let _ = utx.send(WsMessage::Close(None)).await;
                            break;
                        }
                    }
                    _ => {
                        st.router.report(&route, &account, &Outcome::Transient { status: None });
                        let _ = dtx.send(close(CLOSE_INTERNAL, "upstream disconnected")).await;
                        break;
                    }
                }
            },
            _ = &mut sleep, if awaiting => {
                tracing::warn!(account = %account, "upstream websocket idle timeout");
                st.router.report(&route, &account, &Outcome::Transient { status: None });
                let _ = dtx.send(close(CLOSE_INTERNAL, "stream idle timeout")).await;
                let _ = utx.send(WsMessage::Close(None)).await;
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn classifies() {
        let (o, d) = classify(
            r#"{"type":"error","status":429,"error":{"type":"usage_limit_reached","resets_in_seconds":60},"headers":{"x-codex-primary-used-percent":"100"}}"#,
        );
        assert!(d);
        match o {
            Some(Outcome::RateLimited {
                usage_limit,
                retry_after,
                quota,
            }) => {
                assert!(usage_limit);
                assert_eq!(retry_after, Some(Duration::from_secs(60)));
                assert!(quota.is_some());
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(classify(r#"{"type":"response.completed"}"#), (None, true));
        assert_eq!(
            classify(r#"{"type":"response.output_text.delta"}"#),
            (None, false)
        );
    }
}
