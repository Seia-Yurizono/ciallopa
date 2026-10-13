//! HTTP forwarding core: account selection, failover, pre-first-byte retries, idle timeout.
use crate::headers::{downstream_headers, upstream_headers};
use crate::quota::parse_quota;
use crate::AppState;
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use cpa_lite_core::{AccountId, AuthError, Outcome, RouteError, RouteRequest};
use cpa_lite_upstream::retry::{delay_for, is_retryable_status, parse_limit_body};
use futures::StreamExt;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use std::sync::Arc;
use std::time::Duration;

pub(crate) fn json_error(status: StatusCode, kind: &str, msg: &str) -> Response {
    let body = serde_json::json!({"error": {"type": kind, "message": msg}}).to_string();
    let mut r = (status, body).into_response();
    r.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    r
}

/// An outbound request, independent of the account it ends up on.
pub(crate) struct Outbound {
    pub method: Method,
    /// Relative to `upstream.base_url`, with query.
    pub path: String,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub route: RouteRequest,
}

/// The upstream response chosen for the client (2xx, or a non-retryable / final error).
pub(crate) struct Chosen {
    pub resp: reqwest::Response,
    pub account: AccountId,
    pub route: RouteRequest,
}

fn buffered(status: u16, headers: &HeaderMap, body: Bytes) -> Response {
    let mut r = Response::new(Body::from(body));
    *r.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    *r.headers_mut() = downstream_headers(headers);
    r.headers_mut().remove(http::header::CONTENT_LENGTH);
    r
}

fn cooling_down(retry_after: Duration) -> Response {
    let mut r = json_error(
        StatusCode::TOO_MANY_REQUESTS,
        "usage_limit_reached",
        "all accounts are cooling down",
    );
    let secs = retry_after.as_secs().max(1).to_string();
    if let Ok(v) = HeaderValue::from_str(&secs) {
        r.headers_mut().insert(http::header::RETRY_AFTER, v);
    }
    r
}

/// Run the selection / retry loop. `Err` is a ready-made downstream response.
pub(crate) async fn send(st: &AppState, ob: Outbound) -> Result<Chosen, Response> {
    let retries = u64::from(st.config.upstream_opts.max_attempts);
    let mut route = ob.route;
    let mut transient: u64 = 0;
    let mut last: Option<Response> = None;
    loop {
        let accounts = st.accounts.list().await;
        let sel = match st.router.select(&route, &accounts) {
            Ok(s) => s,
            Err(RouteError::AllCoolingDown { retry_after }) => {
                return Err(last.unwrap_or_else(|| cooling_down(retry_after)))
            }
            Err(e) => {
                return Err(last.unwrap_or_else(|| {
                    json_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "no_account",
                        &e.to_string(),
                    )
                }))
            }
        };
        let id = sel.account.clone();
        let mut creds = match st.accounts.credentials(&id).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(account = %id, error = %e, "credentials unavailable");
                if !matches!(e, AuthError::Io(_)) {
                    st.router.report(&route, &id, &Outcome::AuthFailed);
                }
                route.exclude.push(id);
                continue;
            }
        };
        let mut refreshed = false;
        loop {
            let url = st.upstream.url(&ob.path);
            let res = st
                .upstream
                .http
                .request(ob.method.clone(), &url)
                .headers(upstream_headers(&ob.headers, &creds))
                .body(ob.body.clone())
                .send()
                .await;
            let r = match res {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(account = %id, error = %e, "upstream transport error");
                    st.router
                        .report(&route, &id, &Outcome::Transient { status: None });
                    transient += 1;
                    if transient > retries {
                        return Err(json_error(
                            StatusCode::BAD_GATEWAY,
                            "upstream_error",
                            "upstream connection failed",
                        ));
                    }
                    tokio::time::sleep(delay_for(transient, None)).await;
                    break;
                }
            };
            let status = r.status().as_u16();
            match status {
                s if s < 400 => {
                    let quota = parse_quota(r.headers());
                    st.router.report(&route, &id, &Outcome::Success { quota });
                    return Ok(Chosen {
                        resp: r,
                        account: id,
                        route,
                    });
                }
                401 if !refreshed => {
                    refreshed = true;
                    match st.accounts.force_refresh(&id).await {
                        Ok(c) => {
                            creds = c;
                            continue;
                        }
                        Err(e) => {
                            tracing::warn!(account = %id, error = %e, "force refresh failed");
                        }
                    }
                    let h = r.headers().clone();
                    let b = r.bytes().await.unwrap_or_default();
                    st.router.report(&route, &id, &Outcome::AuthFailed);
                    last = Some(buffered(status, &h, b));
                    route.exclude.push(id.clone());
                    break;
                }
                401 | 403 => {
                    let h = r.headers().clone();
                    let b = r.bytes().await.unwrap_or_default();
                    st.router.report(&route, &id, &Outcome::AuthFailed);
                    last = Some(buffered(status, &h, b));
                    route.exclude.push(id.clone());
                    break;
                }
                429 => {
                    let h = r.headers().clone();
                    let b = r.bytes().await.unwrap_or_default();
                    let (usage_limit, body_ra) = parse_limit_body(&b);
                    let outcome = Outcome::RateLimited {
                        retry_after: body_ra.or_else(|| cpa_lite_upstream::retry::retry_after(&h)),
                        usage_limit,
                        quota: parse_quota(&h),
                    };
                    st.router.report(&route, &id, &outcome);
                    last = Some(buffered(status, &h, b));
                    route.exclude.push(id.clone());
                    break;
                }
                s if s == 408 || is_retryable_status(s) => {
                    st.router
                        .report(&route, &id, &Outcome::Transient { status: Some(s) });
                    transient += 1;
                    if transient > retries {
                        return Ok(Chosen {
                            resp: r,
                            account: id,
                            route,
                        });
                    }
                    let d = delay_for(transient, Some(r.headers()));
                    drop(r);
                    tokio::time::sleep(d).await;
                    break;
                }
                s => {
                    st.router
                        .report(&route, &id, &Outcome::ClientError { status: s });
                    return Ok(Chosen {
                        resp: r,
                        account: id,
                        route,
                    });
                }
            }
        }
    }
}

/// Stream an upstream response downstream untouched, with a per-chunk idle timeout.
/// In-stream `usage_limit_reached` (e.g. `response.failed`) is reported to the router.
pub(crate) fn stream_back(st: &AppState, c: Chosen) -> Response {
    let status = c.resp.status();
    let headers = downstream_headers(c.resp.headers());
    let idle = Duration::from_secs(st.config.upstream_opts.stream_idle_timeout_secs.max(1));
    let router = Arc::clone(&st.router);
    let (route, account) = (c.route, c.account);
    let mut up = c.resp.bytes_stream();
    let mut reported = false;
    let body = async_stream(move |tx| async move {
        loop {
            match tokio::time::timeout(idle, up.next()).await {
                Err(_) => {
                    tracing::warn!(account = %account, "upstream stream idle timeout");
                    router.report(&route, &account, &Outcome::Transient { status: None });
                    let _ = tx
                        .send(Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "stream idle timeout",
                        )))
                        .await;
                    return;
                }
                Ok(None) => return,
                Ok(Some(Err(e))) => {
                    router.report(&route, &account, &Outcome::Transient { status: None });
                    let _ = tx.send(Err(std::io::Error::other(e))).await;
                    return;
                }
                Ok(Some(Ok(chunk))) => {
                    if !reported && memfind(&chunk, b"usage_limit_reached") {
                        reported = true;
                        router.report(
                            &route,
                            &account,
                            &Outcome::RateLimited {
                                retry_after: None,
                                usage_limit: true,
                                quota: None,
                            },
                        );
                    }
                    if tx.send(Ok(chunk)).await.is_err() {
                        return;
                    }
                }
            }
        }
    });
    let mut r = Response::new(body);
    *r.status_mut() = status;
    *r.headers_mut() = headers;
    r
}

/// Tiny channel-backed body stream (avoids an async-stream dependency).
pub(crate) fn async_stream<F, Fut>(f: F) -> Body
where
    F: FnOnce(tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    tokio::spawn(f(tx));
    Body::from_stream(futures::stream::poll_fn(move |cx| rx.poll_recv(cx)))
}

pub(crate) fn memfind(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

/// Decode the request body for *inspection only* (zstd bodies are still forwarded as-is).
pub(crate) fn inspect_json(headers: &HeaderMap, body: &[u8]) -> Option<serde_json::Value> {
    let enc = headers
        .get(http::header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    match enc.as_str() {
        "" | "identity" => serde_json::from_slice(body).ok(),
        "zstd" => {
            let mut out = Vec::new();
            let mut dec = zstd::stream::read::Decoder::new(body).ok()?;
            use std::io::Read;
            (&mut dec)
                .take(256 * 1024 * 1024)
                .read_to_end(&mut out)
                .ok()?;
            serde_json::from_slice(&out).ok()
        }
        _ => None,
    }
}
