use super::pump::{
    async_stream, ensure_sse_content_type, is_sse, response_read_failure, stream_back_tracked,
    stream_body, Observation, UpStream,
};
use crate::attempt::{
    others_usable, overload_outcome, overload_retry_wait, rebuild, send_info, Outbound,
    Preparation, SendInfo,
};
use crate::classify::{event_kind_observed, EventKind};
use crate::errors;
use crate::headers::downstream_headers;
use crate::sse::{sse_events, BOOTSTRAP_MAX};
use crate::usage::Tracker;
use crate::AppState;
use axum::body::Body;
use axum::response::Response;
use bytes::Bytes;
use ciallopa_core::Outcome;
use ciallopa_upstream::retry::switch_jitter;
use futures::StreamExt;
use http::{HeaderMap, StatusCode};

fn is_json(h: &HeaderMap) -> bool {
    h.get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.to_ascii_lowercase().contains("json"))
}

/// `send` + stream back, with an SSE bootstrap window: until the first meaningful event,
/// upstream bytes are held; a quota / usage-limit error inside that window cools the
/// account down and transparently replays the same request on another account.
pub(crate) async fn send_streaming(
    st: &AppState,
    mut ob: Outbound,
    mut trk: Option<Tracker>,
) -> Response {
    let mut budget = Preparation::new(st);
    let idle = budget.idle;
    let max_switches = st.accounts.len().max(1);
    let mut switches = 0usize;
    let mut info = SendInfo::default();
    // Keep the attempt chain even after each send_info batch has been consumed below.
    let mut chain = errors::Attempts::default();
    loop {
        let res = send_info(st, ob.clone(), &mut info, &mut budget, trk.as_mut()).await;
        for a in info.attempts.list.drain(..) {
            chain.push(a);
        }
        if let Some(t) = trk.as_mut() {
            t.retries(info.retries);
            t.attempts(&chain);
            // A failed request that never switched still ran on one account: log it.
            let last_tried = chain
                .list
                .last()
                .map(|a| a.account.clone())
                .filter(|a| !info.tried.contains(a));
            t.accounts(
                &info.tried,
                res.as_ref()
                    .ok()
                    .map(|c| &c.account)
                    .or(last_tried.as_ref()),
            );
            if let Ok(c) = &res {
                t.quota(c.resp.headers());
            }
        }
        let mut c = match res {
            Ok(c) => c,
            Err(r) => {
                let Some(t) = trk.as_mut() else { return r };
                t.status(r.status().as_u16());
                t.http_response();
                // Local / buffered upstream errors are already complete and small.
                t.finish_response();
                let (parts, body) = r.into_parts();
                t.error_headers(&parts.headers);
                if let Some(m) = parts.extensions.get::<errors::ErrMeta>() {
                    t.meta_err(m);
                }
                let b = axum::body::to_bytes(body, 1 << 20)
                    .await
                    .unwrap_or_default();
                t.error_body(&b);
                return Response::from_parts(parts, Body::from(b));
            }
        };
        if !c.resp.status().is_success() || !is_sse(c.resp.headers(), ob.expect_sse) {
            if let Some(t) = trk.as_mut() {
                t.ttft();
            }
            if c.resp.status() == StatusCode::BAD_REQUEST && is_json(c.resp.headers()) {
                // A plain 400 rejection of the encrypted history: same clear message.
                let h = c.resp.headers().clone();
                let b = c.resp.bytes().await.unwrap_or_default();
                let v = serde_json::from_slice::<serde_json::Value>(&b).ok();
                if let Some(msg) = v.as_ref().and_then(crate::encrypted::rejection) {
                    tracing::info!(account = %ciallopa_core::a(&c.account), "upstream rejected the encrypted content in the request history (HTTP 400)");
                    if let Some(t) = trk.as_mut() {
                        t.status(400);
                        t.http_response();
                        t.error_headers(&h);
                        t.upstream_error(400, &h);
                        t.encrypted_rejected(&msg, &b, &h);
                    }
                    return crate::encrypted::http_response(v.as_ref());
                }
                c.resp = rebuild(400, &h, b);
            }
            return stream_back_tracked(st, c, trk, ob.expect_sse);
        }
        let status = c.resp.status();
        let up_headers = c.resp.headers().clone();
        let mut headers = downstream_headers(c.resp.headers());
        ensure_sse_content_type(&mut headers);
        let (route, account) = (c.route, c.account);
        let mut up: UpStream = c.resp.bytes_stream().boxed();
        let mut buf: Vec<u8> = Vec::new();
        let mut parsed = 0usize;
        let mut switch: Option<Outcome> = None;
        let mut overload: Option<String> = None;
        let mut rejected: Option<(String, String)> = None;
        let mut ended = false;
        let mut progress = crate::usage::StreamObservation::new();
        'boot: loop {
            if buf.len() > BOOTSTRAP_MAX {
                break;
            }
            match tokio::time::timeout(idle, up.next()).await {
                Ok(Some(Ok(chunk))) => {
                    if !chunk.is_empty() {
                        progress.activity();
                    }
                    buf.extend_from_slice(&chunk);
                    let (evs, np) = sse_events(&buf, parsed);
                    parsed = np;
                    for e in evs {
                        match event_kind_observed(&e, Some(&mut progress)) {
                            EventKind::Preamble => {}
                            EventKind::Content => {
                                st.router.content_ok(&account);
                                break 'boot;
                            }
                            EventKind::Quota(o) => {
                                switch = Some(o);
                                break 'boot;
                            }
                            EventKind::Overloaded(code) => {
                                overload = Some(code);
                                break 'boot;
                            }
                            EventKind::Encrypted(m) => {
                                rejected = Some((m, e.into_owned()));
                                break 'boot;
                            }
                        }
                    }
                }
                Ok(None) => {
                    ended = true;
                    break;
                }
                Ok(Some(Err(e))) => {
                    progress.warn(
                        "sse",
                        "read_error",
                        &account,
                        errors::request_id(&up_headers).as_deref(),
                        trk.as_ref(),
                    );
                    // Consuming the Err means stream_body would only see EOF — surface it here.
                    if let Some(t) = trk.as_mut() {
                        t.status(status.as_u16());
                        let (code, source) = response_read_failure(&e);
                        t.fail_msg(code, format!("upstream stream error: {e}"));
                        t.source(source);
                    }
                    if !crate::response_body::is_decode_error(&e) {
                        st.router.report(&route, &account, &Outcome::Transport);
                    }
                    let err_body = async_stream(st.upstream.drain.clone(), move |tx| async move {
                        if !buf.is_empty() {
                            let _ = tx.send(Ok(Bytes::from(buf))).await;
                        }
                        let _ = tx.send(Ok(errors::truncated_event())).await;
                    });
                    let mut r = Response::new(err_body);
                    *r.status_mut() = status;
                    *r.headers_mut() = headers;
                    return r;
                }
                Err(_) => {
                    // Bootstrap already waited one full idle; do not hand over to stream_body
                    // (that would wait another idle — L1).
                    if let Some(t) = trk.as_mut() {
                        t.status(status.as_u16());
                        t.fail_msg(
                            "idle_timeout",
                            format!(
                                "upstream response body produced no data for {}s (idle timeout)",
                                idle.as_secs()
                            ),
                        );
                        t.source(errors::SRC_NETWORK);
                    }
                    progress.warn(
                        "sse",
                        "idle_timeout",
                        &account,
                        errors::request_id(&up_headers).as_deref(),
                        trk.as_ref(),
                    );
                    st.router.report(&route, &account, &Outcome::Transport);
                    let err_body = async_stream(st.upstream.drain.clone(), move |tx| async move {
                        if !buf.is_empty() {
                            let _ = tx.send(Ok(Bytes::from(buf))).await;
                        }
                        let _ = tx.send(Ok(errors::truncated_event())).await;
                    });
                    let mut r = Response::new(err_body);
                    *r.status_mut() = status;
                    *r.headers_mut() = headers;
                    return r;
                }
            }
        }
        if let Some((msg, ev)) = rejected {
            // Nothing reached the client: answer with a plain 400 (Codex shows it once instead
            // of retrying the stream). No account outcome: the request content is at fault.
            drop(up);
            let v = serde_json::from_str::<serde_json::Value>(&ev).ok();
            tracing::info!(account = %ciallopa_core::a(&account), "upstream rejected the encrypted content in the request history");
            let mut a = errors::attempt(&account, "stream");
            a.status = Some(status.as_u16());
            a.code = Some(crate::encrypted::CODE.into());
            a.body = errors::body_snippet(ev.as_bytes());
            a.action = "stop".into();
            chain.push(a);
            if let Some(t) = trk.as_mut() {
                t.attempts(&chain);
                t.status(StatusCode::BAD_REQUEST.as_u16());
                t.http_response();
                t.encrypted_rejected(&msg, ev.as_bytes(), &up_headers);
            }
            return crate::encrypted::http_response(v.as_ref());
        }
        if let Some(code) = overload {
            // In-stream overload before any output: nothing reached the client yet, so the same
            // request is replayed on another usable account (each account at most once).
            let accounts = st.accounts.accounts();
            let others = others_usable(st, &accounts, &route, &account);
            let cooled_before = st.router.cooling_until(&account).is_some();
            st.router
                .report(&route, &account, &overload_outcome(st, others));
            let cooled = !cooled_before && st.router.cooling_until(&account).is_some();
            let mut a = errors::attempt(&account, "stream");
            a.status = Some(status.as_u16());
            a.code = Some(code.clone());
            a.body = errors::body_snippet(&buf);
            let tag = if cooled { "cooldown+" } else { "" };
            let wait = (!others)
                .then(|| overload_retry_wait(st, info.overload_waited, &up_headers))
                .flatten();
            let next = match (others, wait.is_some()) {
                (true, _) => "switch",
                (false, true) => "wait+retry",
                (false, false) => "stop",
            };
            a.action = format!("{tag}{next}");
            chain.push(a);
            if let Some(t) = trk.as_mut() {
                t.attempts(&chain);
            }
            if others {
                tracing::info!(account = %ciallopa_core::a(&account), code = %code, "overloaded before first output; switching account");
                ob.route = route;
                ob.route.exclude.push(account);
                // Exhaustion is handled at the next send_info before it polls an attempt.
                let _ = budget.run(tokio::time::sleep(switch_jitter())).await;
                continue;
            }
            if let Some(w) = wait {
                // Only usable account left: nothing reached the client, so wait once and replay
                // on the same account (Codex does not retry an in-stream overload itself).
                tracing::info!(account = %ciallopa_core::a(&account), code = %code, wait_ms = w.as_millis() as u64, "overloaded before first output on last usable account; waiting to retry once");
                info.overload_waited = true;
                drop(up);
                ob.route = route;
                let _ = budget.run(tokio::time::sleep(w)).await;
                continue;
            }
            // Last usable account: the upstream error goes to the client verbatim (rest of the
            // stream included); the tracker logs it as stream / <code>.
            if let Some(t) = trk.as_mut() {
                t.status(status.as_u16());
                t.fail_msg(&code, "upstream overloaded on every usable account");
                t.source(errors::SRC_STREAM);
            }
            if ended {
                up = futures::stream::empty().boxed();
            }
            return stream_body(
                st,
                status,
                headers,
                route,
                account,
                Bytes::from(buf),
                up,
                trk,
                Observation::Responses(progress),
                idle,
                true,
            );
        }
        if let Some(o) = switch {
            let cooled_before = st.router.cooling_until(&account).is_some();
            st.router.report(&route, &account, &o);
            let cooled = !cooled_before && st.router.cooling_until(&account).is_some();
            switches += 1;
            // Recorded so a request rescued by this switch shows how (attempt chain).
            let mut a = errors::attempt(&account, "stream");
            a.status = Some(status.as_u16());
            a.code = Some(errors::quota_outcome_code(&o).into());
            a.body = errors::body_snippet(&buf);
            let tag = if cooled { "cooldown+" } else { "" };
            let next = if switches < max_switches {
                "switch"
            } else {
                "stop"
            };
            a.action = format!("{tag}{next}");
            chain.push(a);
            if let Some(t) = trk.as_mut() {
                t.attempts(&chain);
            }
            if switches < max_switches {
                tracing::info!(account = %ciallopa_core::a(&account), "quota error before first output; switching account");
                ob.route = route;
                ob.route.exclude.push(account);
                continue;
            }
            // Out of accounts: deliver what we have (the quota error) as-is.
            if let Some(t) = trk.as_mut() {
                t.status(status.as_u16());
                t.fail_msg(
                    errors::quota_outcome_code(&o),
                    "usage limit reached on every available account",
                );
                t.source(errors::SRC_STREAM);
                t.finish_response();
                t.error_body(&buf);
            }
            let mut r = Response::new(Body::from(buf));
            *r.status_mut() = status;
            *r.headers_mut() = headers;
            return r;
        }
        if ended {
            up = futures::stream::empty().boxed();
        }
        if let Some(t) = trk.as_mut() {
            t.status(status.as_u16());
        }
        return stream_body(
            st,
            status,
            headers,
            route,
            account,
            Bytes::from(buf),
            up,
            trk,
            Observation::Responses(progress),
            idle,
            false,
        );
    }
}
