use crate::attempt::{overload_outcome, Chosen};
use crate::classify::{event_kind, EventKind};
use crate::errors;
use crate::headers::downstream_headers;
use crate::sse::{sse_events_after, Aligner};
use crate::usage::Tracker;
use crate::AppState;
use axum::body::Body;
use axum::response::Response;
use bytes::Bytes;
use ciallopa_core::{AccountId, Outcome, RouteRequest};
use futures::StreamExt;
use http::{HeaderMap, HeaderValue, StatusCode};
use std::sync::Arc;
use std::time::Duration;

/// Stream an upstream response downstream untouched, with a per-chunk idle timeout.
/// In-stream `usage_limit_reached` (e.g. `response.failed`) is reported to the router.
pub(crate) fn stream_back(st: &AppState, c: Chosen) -> Response {
    stream_back_tracked(st, c, None, false)
}

/// Images SSE has a different event vocabulary. Observe only explicit error events;
/// never apply Responses terminal detection, replay or error rewriting.
pub(crate) fn stream_image_body(st: &AppState, c: Chosen, mut trk: Option<Tracker>) -> Response {
    let status = c.resp.status();
    let image_sse = status.is_success() && is_sse(c.resp.headers(), false);
    if let Some(t) = trk.as_mut() {
        t.status(status.as_u16());
        if is_sse(c.resp.headers(), false) {
            t.sse_response();
        } else {
            t.http_response();
        }
        if status.as_u16() >= 400 {
            t.upstream_error(status.as_u16(), c.resp.headers());
        }
    }
    let headers = downstream_headers(c.resp.headers());
    stream_body(
        st,
        status,
        headers,
        c.route,
        c.account,
        Bytes::new(),
        c.resp.bytes_stream().boxed(),
        trk,
        if image_sse {
            Observation::Images
        } else {
            Observation::Http
        },
        c.idle,
        false,
    )
}

pub(crate) fn stream_back_tracked(
    st: &AppState,
    c: Chosen,
    mut trk: Option<Tracker>,
    expect_sse: bool,
) -> Response {
    let status = c.resp.status();
    let sse = status.is_success() && is_sse(c.resp.headers(), expect_sse);
    let mut headers = downstream_headers(c.resp.headers());
    if sse {
        ensure_sse_content_type(&mut headers);
    }
    if let Some(t) = trk.as_mut() {
        t.status(status.as_u16());
        if status.as_u16() >= 400 {
            t.error_headers(c.resp.headers());
            t.upstream_error(status.as_u16(), c.resp.headers());
        }
        if !sse {
            t.http_response();
        }
    }
    let up = c.resp.bytes_stream().boxed();
    stream_body(
        st,
        status,
        headers,
        c.route,
        c.account,
        Bytes::new(),
        up,
        trk,
        if sse {
            Observation::Responses(crate::usage::StreamObservation::new())
        } else {
            Observation::Http
        },
        c.idle,
        false,
    )
}

pub(crate) type UpStream = futures::stream::BoxStream<'static, reqwest::Result<Bytes>>;

pub(crate) fn response_read_failure(error: &reqwest::Error) -> (&'static str, &'static str) {
    if crate::response_body::is_decode_error(error) {
        ("response_decode_error", errors::SRC_STREAM)
    } else {
        ("stream_read_error", errors::SRC_NETWORK)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Observation {
    Http,
    Responses(crate::usage::StreamObservation),
    Images,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn stream_body(
    st: &AppState,
    status: StatusCode,
    headers: HeaderMap,
    route: RouteRequest,
    account: AccountId,
    prefix: Bytes,
    mut up: UpStream,
    mut trk: Option<Tracker>,
    observation: Observation,
    idle: Duration,
    // The account outcome of an in-stream error was already reported (bootstrap passthrough).
    mut reported: bool,
) -> Response {
    let sse = matches!(observation, Observation::Responses(_));
    let mut progress = match observation {
        Observation::Responses(progress) => progress,
        _ => crate::usage::StreamObservation::new(),
    };
    // Hyper may never poll a 204 / Content-Length: 0 body. Its HTTP framing already
    // proves EOF; finish before handing it over so a dropped receiver is not cancellation.
    // Responses SSE still requires a terminal event, even when its body is empty.
    let empty = status == StatusCode::NO_CONTENT
        || status == StatusCode::NOT_MODIFIED
        || headers
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            == Some(0);
    if !sse && prefix.is_empty() && empty {
        if let Some(t) = trk.as_mut() {
            t.finish_response();
        }
        let mut r = Response::new(Body::empty());
        *r.status_mut() = status;
        *r.headers_mut() = headers;
        return r;
    }
    let router = Arc::clone(&st.router);
    let late_overload = overload_outcome(st, false);
    let upstream_request_id = errors::request_id(&headers);
    let image_headers =
        (matches!(observation, Observation::Images) && trk.is_some()).then(|| headers.clone());
    // Responses SSE: gateway downstream observation (queue / body yield); the record waits
    // for the body to end so its yield fields are complete.
    let link = match (sse, trk.as_mut()) {
        (true, Some(t)) => {
            t.obs_upstream_request_id(upstream_request_id.as_deref());
            t.obs_up_bytes(prefix.len());
            let l = crate::downstream::DownLink::new(t.start_instant());
            t.defer_to(l.clone());
            Some(l)
        }
        _ => None,
    };
    let drain = st.upstream.drain.clone();
    // A Responses SSE cut by shutdown ends with a retryable response.failed (see ws.rs).
    let on_force = sse.then(|| {
        Bytes::from(format!(
            "\n\nevent: response.failed\ndata: {}\n\n",
            shutdown_failed_event()
        ))
    });
    let body = crate::downstream::observed(drain, link, move |tx| async move {
        let mut image_events = image_headers
            .as_ref()
            .map(|_| crate::image_events::ImageErrors::default());
        // Detect usage_limit only from complete SSE events (`error` / `response.failed`),
        // never by searching raw bytes (user text containing the string is fine).
        let mut pending: Vec<u8> = Vec::new();
        let mut parsed = 0usize;
        // Complete events are authoritative. Beyond the observation bound, bytes still
        // pass through, but an unobserved terminal event leaves the result unknown.
        let mut terminal_seen = false;
        let mut observation_limited = false;
        // Bytes held back until their SSE event is complete (see `Aligner`).
        let mut align = Aligner::new(sse);
        let report_quota = |o: &Outcome| {
            router.report(&route, &account, o);
        };
        // An overload after output reached the client cannot be replayed; it still counts
        // against the account (streak only, no mid-stream cooldown) instead of the 200's success.
        let late_overload = late_overload.clone();
        let fail = |trk: &mut Option<Tracker>, k: &str| {
            if let Some(t) = trk.as_mut() {
                t.fail(k);
            }
        };
        let check = |buf: &[u8],
                     from: usize,
                     search_from: usize,
                     trk: &mut Option<Tracker>,
                     reported: &mut bool,
                     terminal: &mut bool,
                     progress: &mut crate::usage::StreamObservation,
                     observed: &mut u64| {
            let (evs, np) = sse_events_after(buf, from, search_from);
            for e in &evs {
                let ty = crate::usage::event_type(e);
                if let Some(ty) = ty.as_deref() {
                    if *observed > 0 {
                        *observed -= 1;
                    } else {
                        progress.event(ty);
                    }
                }
                *terminal |= ty.as_deref().is_some_and(crate::usage::is_terminal_type);
                if let Some(t) = trk.as_mut() {
                    t.sse_event(e, ty.as_deref());
                }
                // Only these types can produce Quota; do not allocate a full Value for
                // every output delta just to discard its Content classification. Keep
                // the old classifier fallback when the typed envelope was inconclusive.
                if !*reported
                    && matches!(
                        ty.as_deref(),
                        None | Some("error" | "response.failed" | "service_unavailable_error")
                    )
                {
                    match event_kind(e) {
                        EventKind::Quota(o) => {
                            *reported = true;
                            report_quota(&o);
                            fail(trk, errors::quota_outcome_code(&o));
                        }
                        EventKind::Overloaded(_) if sse => {
                            *reported = true;
                            report_quota(&late_overload);
                        }
                        _ => {}
                    }
                }
            }
            np
        };
        if !prefix.is_empty() {
            // Bootstrap already observed these complete envelopes at their arrival.
            // Reusing buffered bytes must not count them twice or refresh their clocks.
            let mut observed = progress.events;
            if prefix.len() <= OBSERVATION_MAX {
                pending.extend_from_slice(&prefix);
                parsed = check(
                    &pending,
                    parsed,
                    0,
                    &mut trk,
                    &mut reported,
                    &mut terminal_seen,
                    &mut progress,
                    &mut observed,
                );
            } else {
                observation_limited = true;
                progress.limit_reached();
            }
            let out = align.push(&prefix);
            if !out.is_empty() && tx.send(Ok(out)).await.is_err() {
                if let Some(t) = trk.as_mut() {
                    t.client_gone();
                }
                return;
            }
            // Drop consumed bytes so the buffer stays bounded under long streams.
            if parsed > 0 {
                pending.drain(..parsed);
                parsed = 0;
            }
        }
        loop {
            match tokio::time::timeout(idle, up.next()).await {
                // After a terminal event the answer is complete: a later idle or read error is
                // a normal end of the stream, not an account or request failure.
                Err(_) | Ok(Some(Err(_))) if sse && terminal_seen => {
                    if let Some(rest) = align.finish() {
                        let _ = tx.send(Ok(rest)).await;
                    }
                    return;
                }
                Err(_) => {
                    if let Some(t) = trk.as_mut() {
                        t.fail_msg(
                            "idle_timeout",
                            format!(
                                "upstream response body produced no data for {}s (idle timeout)",
                                idle.as_secs()
                            ),
                        );
                    }
                    if sse {
                        progress.warn(
                            "sse",
                            "idle_timeout",
                            &account,
                            upstream_request_id.as_deref(),
                            trk.as_ref(),
                        );
                    } else {
                        tracing::warn!(account = %ciallopa_core::a(&account), record_id = trk.as_ref().map(Tracker::record_id), upstream_request_id = upstream_request_id.as_deref(), "upstream stream idle timeout");
                    }
                    router.report(&route, &account, &Outcome::Transport);
                    if let Some(t) = trk.as_mut() {
                        t.source(errors::SRC_NETWORK);
                    }
                    if let Some(rest) = align.finish() {
                        let _ = tx.send(Ok(rest)).await;
                    }
                    if sse && !terminal_seen {
                        let _ = tx.send(Ok(errors::truncated_event())).await;
                        return;
                    }
                    let _ = tx
                        .send(Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "stream idle timeout",
                        )))
                        .await;
                    return;
                }
                Ok(None) => {
                    if let Some(rest) = align.finish() {
                        if tx.send(Ok(rest)).await.is_err() {
                            if let Some(t) = trk.as_mut() {
                                t.client_gone();
                            }
                            return;
                        }
                    }
                    if sse && !terminal_seen && observation_limited {
                        progress.warn(
                            "sse",
                            "observation_limit",
                            &account,
                            upstream_request_id.as_deref(),
                            trk.as_ref(),
                        );
                        if let Some(t) = trk.as_mut() {
                            t.observation_incomplete();
                        }
                    } else if sse && !terminal_seen {
                        if let Some(t) = trk.as_mut() {
                            t.fail_msg(
                                "stream_truncated",
                                "upstream stream ended without a terminal event",
                            );
                            t.source(errors::SRC_STREAM);
                        }
                        progress.warn(
                            "sse",
                            "eof",
                            &account,
                            upstream_request_id.as_deref(),
                            trk.as_ref(),
                        );
                        let _ = tx.send(Ok(errors::truncated_event())).await;
                    }
                    if !sse {
                        if let Some(t) = trk.as_mut() {
                            t.finish_response();
                        }
                    }
                    return;
                }
                Ok(Some(Err(e))) => {
                    if sse {
                        progress.warn(
                            "sse",
                            "read_error",
                            &account,
                            upstream_request_id.as_deref(),
                            trk.as_ref(),
                        );
                    }
                    if let Some(t) = trk.as_mut() {
                        let (code, source) = response_read_failure(&e);
                        t.fail_msg(code, format!("upstream stream error: {e}"));
                        t.source(source);
                    }
                    if !crate::response_body::is_decode_error(&e) {
                        router.report(&route, &account, &Outcome::Transport);
                    }
                    if let Some(rest) = align.finish() {
                        let _ = tx.send(Ok(rest)).await;
                    }
                    if sse && !terminal_seen {
                        let _ = tx.send(Ok(errors::truncated_event())).await;
                        return;
                    }
                    let _ = tx.send(Err(std::io::Error::other(e))).await;
                    return;
                }
                Ok(Some(Ok(chunk))) => {
                    if let (Some(observer), Some(t), Some(h)) =
                        (image_events.as_mut(), trk.as_mut(), image_headers.as_ref())
                    {
                        if let Some(error) = observer.feed(&chunk) {
                            t.meta_err(&errors::ErrMeta {
                                source: errors::SRC_STREAM,
                                code: errors::sse_block_code(&error.event)
                                    .unwrap_or_else(|| "upstream_error_event".into()),
                                upstream_status: Some(status.as_u16()),
                                upstream_request_id: upstream_request_id.clone(),
                                message: Some(error.message),
                            });
                            t.error_headers(h);
                            t.error_text(&error.event);
                        }
                    }
                    if !chunk.is_empty() {
                        if let Some(t) = trk.as_mut() {
                            t.upstream_activity();
                            if sse {
                                t.obs_up_bytes(chunk.len());
                            }
                        }
                    }
                    if sse {
                        if !chunk.is_empty() {
                            progress.activity();
                        }
                        // One shared observation buffer replaces the Tracker's separate
                        // 8 MiB buffer. Oversized payloads still pass through untouched.
                        if pending.len().saturating_add(chunk.len()) > OBSERVATION_MAX {
                            pending.clear();
                            parsed = 0;
                            observation_limited = true;
                            progress.limit_reached();
                        } else {
                            // The old tail had no complete delimiter. Only its last three
                            // bytes can join the new chunk's LF / CRLF event boundary.
                            let search_from = pending.len().saturating_sub(3);
                            pending.extend_from_slice(&chunk);
                            parsed = check(
                                &pending,
                                parsed,
                                search_from,
                                &mut trk,
                                &mut reported,
                                &mut terminal_seen,
                                &mut progress,
                                &mut 0,
                            );
                            if parsed > 0 {
                                pending.drain(..parsed);
                                parsed = 0;
                            }
                        }
                    }
                    if let Some(t) = trk.as_mut() {
                        if status.as_u16() >= 400 {
                            t.error_body(&chunk);
                        }
                    }
                    // The response decoder yields an empty chunk at end of body; forwarding it
                    // after the client hung up would look like a disconnect.
                    if chunk.is_empty() {
                        continue;
                    }
                    let out = align.push(&chunk);
                    if out.is_empty() {
                        continue;
                    }
                    if tx.send(Ok(out)).await.is_err() {
                        if let Some(t) = trk.as_mut() {
                            t.client_gone();
                        }
                        return;
                    }
                }
            }
        }
    });
    let mut r = Response::new(body);
    *r.status_mut() = status;
    *r.headers_mut() = headers;
    // The SSE body may differ from the upstream's (rewritten or appended terminal events).
    if sse {
        r.headers_mut().remove(http::header::CONTENT_LENGTH);
    }
    if let Some(b) = on_force {
        r.extensions_mut().insert(crate::admission::OnForce(b));
    }
    r
}

const OBSERVATION_MAX: usize = 8 << 20;

/// SSE if the upstream says `text/event-stream`, or if it sent no / an unrecognised
/// `Content-Type` and the client asked for a stream. chatgpt.com's Codex endpoint currently
/// omits `Content-Type` on `/responses` SSE; an explicit JSON / text / HTML type always wins.
pub(crate) fn is_sse(h: &HeaderMap, expect_sse: bool) -> bool {
    let ct = h
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_ascii_lowercase());
    match ct.as_deref() {
        Some(v) if v.contains("text/event-stream") => true,
        None | Some("") => expect_sse,
        Some(v) => {
            expect_sse
                && !(v.contains("json")
                    || v.starts_with("text/")
                    || v.contains("html")
                    || v.contains("xml"))
        }
    }
}

/// Clients other than Codex (browsers, SDKs) need the media type to parse a stream; Codex itself
/// parses SSE regardless of `Content-Type`, so adding it is harmless.
pub(crate) fn ensure_sse_content_type(h: &mut HeaderMap) {
    // SSE error/timeout handling can replace or append events on every exit path.
    h.remove(http::header::CONTENT_LENGTH);
    let known = h
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.to_ascii_lowercase().contains("text/event-stream"));
    if !known {
        h.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        );
    }
}

/// Seconds a client should wait before replaying a turn cut by shutdown (restart time).
pub(crate) const SHUTDOWN_RETRY_AFTER_SECS: u64 = 2;

/// Terminal event for a turn cut by shutdown: an unknown `response.failed` code is retryable
/// in Codex (sse/responses_error.rs), with `error.headers.retry-after` as the delay.
pub(crate) fn shutdown_failed_event() -> serde_json::Value {
    serde_json::json!({
        "type": "response.failed",
        "response": {
            "status": "failed",
            "error": {
                "code": "gateway_restarting",
                "message": "ciallopa is restarting; retrying shortly.",
                "headers": {"retry-after": SHUTDOWN_RETRY_AFTER_SECS.to_string()}
            }
        }
    })
}

#[cfg(test)]
mod stream_lifecycle_tests {
    use super::*;

    #[tokio::test]
    async fn body_drop_cancels_an_idle_producer() {
        struct OnDrop(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                if let Some(tx) = self.0.take() {
                    let _ = tx.send(());
                }
            }
        }

        let (started, ready) = tokio::sync::oneshot::channel();
        let (dropped, cancelled) = tokio::sync::oneshot::channel();
        let body = crate::downstream::observed(Arc::default(), None, move |_tx| async move {
            let _guard = OnDrop(Some(dropped));
            let _ = started.send(());
            futures::future::pending::<()>().await;
        });
        tokio::time::timeout(Duration::from_secs(1), ready)
            .await
            .expect("producer started")
            .expect("start notification");
        drop(body);
        tokio::time::timeout(Duration::from_secs(1), cancelled)
            .await
            .expect("body drop releases an idle upstream without waiting for its timeout")
            .expect("producer dropped");
    }
}
