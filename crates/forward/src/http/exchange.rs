use super::pump::{
    ensure_sse_content_type, is_sse, response_read_failure, stream_back_tracked, stream_body,
    Observation, Up,
};
use crate::attempt::{
    others_usable, overload_outcome, rebuild, report_cooling, send_info, Action, Next, Outbound,
    OverloadPlan, Policy, Preparation, SendInfo,
};
use crate::classify::{classify, Class, Terminal};
use crate::downstream::observed;
use crate::errors;
use crate::headers::downstream_headers;
use crate::sse::BOOTSTRAP_MAX;
use crate::timing::switch_jitter;
use crate::usage::{End, StreamObservation, Tracker};
use crate::AppState;
use axum::body::Body;
use axum::response::Response;
use bytes::BytesMut;
use ciallopa_core::Outcome;
use futures::StreamExt;
use http::{HeaderMap, StatusCode};

#[derive(Debug)]
enum PreludeEnd {
    Commit(Commit),
    Quota(Outcome),
    Overloaded(String),
    Encrypted { message: String, event: String },
    ReadError(reqwest::Error),
    Idle,
}

#[derive(Debug, Clone, Copy)]
enum Commit {
    Content,
    Oversize,
    UpstreamEof,
}

struct Prelude {
    buf: BytesMut,
    up: Up,
    progress: StreamObservation,
}

impl Prelude {
    async fn read(up: Up, idle: std::time::Duration) -> (Self, PreludeEnd) {
        let mut prelude = Self {
            buf: BytesMut::new(),
            up,
            progress: StreamObservation::new(),
        };
        let mut parsed = 0;
        let end = loop {
            if prelude.buf.len() > BOOTSTRAP_MAX {
                break PreludeEnd::Commit(Commit::Oversize);
            }
            match tokio::time::timeout(idle, prelude.up.next()).await {
                Ok(Some(Ok(chunk))) => {
                    if !chunk.is_empty() {
                        prelude.progress.activity();
                    }
                    let search_from = prelude.buf.len().saturating_sub(3);
                    prelude.buf.extend_from_slice(&chunk);
                    let (events, used) =
                        crate::sse::sse_events_after(&prelude.buf, parsed, search_from);
                    parsed = used;
                    let end = events.into_iter().find_map(|event| {
                        let classified = classify(None, &event);
                        if let Some(ty) = classified.ty.as_deref() {
                            prelude.progress.event(ty);
                        }
                        match classified.class {
                            Class::Preamble | Class::RateLimits(_) | Class::Created => None,
                            Class::Terminal(Terminal::Quota(outcome)) => {
                                Some(PreludeEnd::Quota(outcome))
                            }
                            Class::Terminal(Terminal::Overloaded(code)) => {
                                Some(PreludeEnd::Overloaded(code))
                            }
                            Class::Terminal(Terminal::Encrypted(message)) => {
                                Some(PreludeEnd::Encrypted {
                                    message,
                                    event: event.into_owned(),
                                })
                            }
                            _ => Some(PreludeEnd::Commit(Commit::Content)),
                        }
                    });
                    if let Some(end) = end {
                        break end;
                    }
                }
                Ok(None) => break PreludeEnd::Commit(Commit::UpstreamEof),
                Ok(Some(Err(error))) => break PreludeEnd::ReadError(error),
                Err(_) => break PreludeEnd::Idle,
            }
        };
        (prelude, end)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Replay {
    SwitchAfterQuota,
    SwitchAfterOverload,
    WaitRetry(std::time::Duration),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreludeVerdict {
    Stream { account_reported: bool },
    Replay(Replay),
    Answer400Encrypted,
    DeliverBuffered,
    TruncatedTail { code: &'static str },
}

struct ReplayCtx {
    others: bool,
    overload_wait: Option<std::time::Duration>,
    quota_switches_left: bool,
}

fn prelude_verdict(end: &PreludeEnd, ctx: &ReplayCtx) -> PreludeVerdict {
    match end {
        PreludeEnd::Commit(_) => PreludeVerdict::Stream {
            account_reported: false,
        },
        PreludeEnd::Encrypted { .. } => PreludeVerdict::Answer400Encrypted,
        PreludeEnd::Overloaded(_) if ctx.others => {
            PreludeVerdict::Replay(Replay::SwitchAfterOverload)
        }
        PreludeEnd::Overloaded(_) => match ctx.overload_wait {
            Some(wait) => PreludeVerdict::Replay(Replay::WaitRetry(wait)),
            None => PreludeVerdict::Stream {
                account_reported: true,
            },
        },
        PreludeEnd::Quota(_) if ctx.quota_switches_left => {
            PreludeVerdict::Replay(Replay::SwitchAfterQuota)
        }
        PreludeEnd::Quota(_) => PreludeVerdict::DeliverBuffered,
        PreludeEnd::ReadError(error) => PreludeVerdict::TruncatedTail {
            code: response_read_failure(error).0,
        },
        PreludeEnd::Idle => PreludeVerdict::TruncatedTail {
            code: "idle_timeout",
        },
    }
}

fn is_json(h: &HeaderMap) -> bool {
    h.get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.to_ascii_lowercase().contains("json"))
}

/// `send` + stream back, with an SSE bootstrap window: until the first meaningful event,
/// upstream bytes are held; a quota / usage-limit error inside that window cools the
/// account down and transparently replays the same request on another account.
pub(crate) async fn send_streaming(st: &AppState, ob: Outbound, trk: Option<Tracker>) -> Response {
    let record_id = trk.as_ref().map(Tracker::record_id);
    let mut response = send(st, ob, trk).await;
    if let Some(id) = record_id {
        errors::stamp_request_id(&mut response, &id);
    }
    response
}

async fn send(st: &AppState, mut ob: Outbound, mut trk: Option<Tracker>) -> Response {
    let mut budget = Preparation::new(st);
    let idle = budget.idle;
    let max_switches = st.accounts.len().max(1);
    let mut switches = 0usize;
    let mut info = SendInfo::default();
    // Keep the attempt chain even after each send_info batch has been consumed below.
    let mut chain = errors::Attempts::default();
    loop {
        let res = send_info(st, &ob, &mut info, &mut budget, trk.as_mut()).await;
        for a in info.attempts.list.drain(..) {
            chain.push(a);
        }
        if let Some(t) = trk.as_mut() {
            t.rec.retries = info.retries;
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
                let (parts, body) = r.into_parts();
                let b = axum::body::to_bytes(body, 1 << 20)
                    .await
                    .unwrap_or_default();
                t.record_response(&parts, &b);
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
                        t.rec.status = 400;
                        t.rec.transport = ciallopa_usage::Transport::Http;
                        t.error_headers(&h);
                        t.upstream_error(400, &h);
                        t.encrypted_rejected(&msg, &b, &h);
                    }
                    return crate::encrypted::http_response(v.as_ref());
                }
                c.resp = rebuild(400, &h, b);
            }
            return stream_back_tracked(st, c, trk, ob.expect_sse, false);
        }
        let status = c.resp.status();
        let up_headers = c.resp.headers().clone();
        let mut headers = downstream_headers(c.resp.headers());
        ensure_sse_content_type(&mut headers);
        let (route, account) = (c.route, c.account);
        let (prelude, end) = Prelude::read(Up::Live(c.resp.bytes_stream().boxed()), idle).await;
        if matches!(end, PreludeEnd::Commit(Commit::Content)) {
            st.router.content_ok(&account);
        }
        let others = matches!(end, PreludeEnd::Overloaded(_))
            && others_usable(st, &st.accounts.accounts(), &route, &account);
        let overload = if matches!(end, PreludeEnd::Overloaded(_)) {
            Policy::http(&st.config.load().upstream_opts).overload_plan(
                others,
                info.overload_waited,
                &up_headers,
                st.config.load().upstream_opts.overload_retry_wait_secs,
                switches,
                max_switches,
            )
        } else {
            OverloadPlan::PassThrough
        };
        let ctx = ReplayCtx {
            others,
            overload_wait: match overload {
                OverloadPlan::WaitRetry(wait) => Some(wait),
                _ => None,
            },
            quota_switches_left: switches + 1 < max_switches,
        };
        let verdict = prelude_verdict(&end, &ctx);
        // Report and record once, before choosing replay or downstream delivery.
        let failure = match &end {
            PreludeEnd::Overloaded(code) => {
                Some((overload_outcome(st, others), code.clone(), overload.next()))
            }
            PreludeEnd::Quota(outcome) => {
                switches += 1;
                Some((
                    outcome.clone(),
                    errors::quota_outcome_code(outcome).into(),
                    if ctx.quota_switches_left {
                        Next::Switch
                    } else {
                        Next::Stop
                    },
                ))
            }
            _ => None,
        };
        if let Some((outcome, code, next)) = failure {
            let cooled = report_cooling(st, &route, &account, &outcome);
            let mut attempt = errors::attempt(&account, "stream");
            attempt.status = Some(status.as_u16());
            attempt.code = Some(code);
            attempt.body = errors::body_snippet(&prelude.buf);
            attempt.action = Action { cooled, next };
            chain.push(attempt);
            if let Some(t) = trk.as_mut() {
                t.attempts(&chain);
            }
        }
        match verdict {
            PreludeVerdict::Replay(replay) => {
                drop(prelude);
                ob.route = route;
                match replay {
                    Replay::SwitchAfterQuota => {
                        ob.route.exclude.push(account);
                    }
                    Replay::SwitchAfterOverload => {
                        ob.route.exclude.push(account);
                        let _ = budget
                            .run(Tracker::wait(trk.as_mut(), switch_jitter(st)))
                            .await;
                    }
                    Replay::WaitRetry(wait) => {
                        info.overload_waited = true;
                        let _ = budget
                            .run(Tracker::wait(trk.as_mut(), crate::timing::scale(st, wait)))
                            .await;
                    }
                }
            }
            PreludeVerdict::Answer400Encrypted => {
                let PreludeEnd::Encrypted { message, event } = end else {
                    unreachable!()
                };
                let value = serde_json::from_str::<serde_json::Value>(&event).ok();
                tracing::info!(account = %ciallopa_core::a(&account), "upstream rejected the encrypted content in the request history");
                let mut attempt = errors::attempt(&account, "stream");
                attempt.status = Some(status.as_u16());
                attempt.code = Some(crate::encrypted::CODE.into());
                attempt.body = errors::body_snippet(event.as_bytes());
                attempt.action = Next::Stop.into();
                chain.push(attempt);
                if let Some(t) = trk.as_mut() {
                    t.attempts(&chain);
                    t.rec.status = 400;
                    t.rec.transport = ciallopa_usage::Transport::Http;
                    t.encrypted_rejected(&message, event.as_bytes(), &up_headers);
                }
                return crate::encrypted::http_response(value.as_ref());
            }
            PreludeVerdict::DeliverBuffered => {
                let PreludeEnd::Quota(outcome) = end else {
                    unreachable!()
                };
                if let Some(mut t) = trk.take() {
                    t.rec.status = status.as_u16();
                    t.error_body(&prelude.buf);
                    t.finish(End::Failed {
                        code: errors::quota_outcome_code(&outcome).into(),
                        source: errors::SRC_STREAM,
                        message: Some("usage limit reached on every available account".into()),
                    });
                }
                let mut response = Response::new(Body::from(prelude.buf.freeze()));
                *response.status_mut() = status;
                *response.headers_mut() = headers;
                return response;
            }
            PreludeVerdict::TruncatedTail { code } => {
                let (source, message, warn, report) = match &end {
                    PreludeEnd::ReadError(error) => (
                        response_read_failure(error).1,
                        format!("upstream stream error: {error}"),
                        "read_error",
                        !crate::response_body::is_decode_error(error),
                    ),
                    _ => (
                        errors::SRC_NETWORK,
                        format!(
                            "upstream response body produced no data for {}s (idle timeout)",
                            idle.as_secs()
                        ),
                        "idle_timeout",
                        true,
                    ),
                };
                prelude.progress.warn(
                    "sse",
                    warn,
                    &account,
                    errors::request_id(&up_headers).as_deref(),
                    trk.as_ref(),
                );
                if let Some(mut t) = trk.take() {
                    t.rec.status = status.as_u16();
                    t.finish(End::Failed {
                        code: code.into(),
                        source,
                        message: Some(message),
                    });
                }
                if report {
                    st.router.report(&route, &account, &Outcome::Transport);
                }
                let buf = prelude.buf.freeze();
                let body = observed(st.upstream.drain.clone(), None, move |tx| async move {
                    if !buf.is_empty() {
                        let _ = tx.send(Ok(buf)).await;
                    }
                    let _ = tx.send(Ok(errors::truncated_event())).await;
                });
                let mut response = Response::new(body);
                *response.status_mut() = status;
                *response.headers_mut() = headers;
                return response;
            }
            PreludeVerdict::Stream { account_reported } => {
                if let Some(t) = trk.as_mut() {
                    t.rec.status = status.as_u16();
                    if let PreludeEnd::Overloaded(code) = &end {
                        t.fail_msg(code, "upstream overloaded on every usable account");
                        t.source(errors::SRC_STREAM);
                    }
                }
                return stream_body(
                    st,
                    status,
                    headers,
                    route,
                    account,
                    prelude.buf.freeze(),
                    prelude.up,
                    trk,
                    Observation::Responses(prelude.progress),
                    idle,
                    account_reported,
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn read_error(decode: bool) -> reqwest::Error {
        let body = if decode {
            reqwest::Body::from("invalid gzip")
        } else {
            reqwest::Body::wrap_stream(futures::stream::iter([Err::<BytesMut, _>(
                std::io::Error::other("read failed"),
            )]))
        };
        let mut response = http::Response::new(body);
        if decode {
            response.headers_mut().insert(
                http::header::CONTENT_ENCODING,
                http::HeaderValue::from_static("gzip"),
            );
        }
        crate::response_body::prepare(response.into())
            .unwrap()
            .bytes()
            .await
            .unwrap_err()
    }

    #[tokio::test]
    async fn prelude_verdict_covers_the_complete_replay_table() {
        let wait = Duration::from_secs(2);
        let quota = Outcome::RateLimited {
            retry_after: None,
            usage_limit: true,
            quota: None,
        };
        let ends = [
            PreludeEnd::Commit(Commit::Content),
            PreludeEnd::Commit(Commit::Oversize),
            PreludeEnd::Commit(Commit::UpstreamEof),
            PreludeEnd::Encrypted {
                message: String::new(),
                event: String::new(),
            },
            PreludeEnd::Overloaded("overloaded".into()),
            PreludeEnd::Quota(quota),
            PreludeEnd::ReadError(read_error(false).await),
            PreludeEnd::ReadError(read_error(true).await),
            PreludeEnd::Idle,
        ];
        for others in [false, true] {
            for wait_available in [false, true] {
                for quota_switches_left in [false, true] {
                    let ctx = ReplayCtx {
                        others,
                        overload_wait: wait_available.then_some(wait),
                        quota_switches_left,
                    };
                    let overloaded = if others {
                        PreludeVerdict::Replay(Replay::SwitchAfterOverload)
                    } else if wait_available {
                        PreludeVerdict::Replay(Replay::WaitRetry(wait))
                    } else {
                        PreludeVerdict::Stream {
                            account_reported: true,
                        }
                    };
                    let expected = [
                        PreludeVerdict::Stream {
                            account_reported: false,
                        },
                        PreludeVerdict::Stream {
                            account_reported: false,
                        },
                        PreludeVerdict::Stream {
                            account_reported: false,
                        },
                        PreludeVerdict::Answer400Encrypted,
                        overloaded,
                        if quota_switches_left {
                            PreludeVerdict::Replay(Replay::SwitchAfterQuota)
                        } else {
                            PreludeVerdict::DeliverBuffered
                        },
                        PreludeVerdict::TruncatedTail {
                            code: "stream_read_error",
                        },
                        PreludeVerdict::TruncatedTail {
                            code: "response_decode_error",
                        },
                        PreludeVerdict::TruncatedTail {
                            code: "idle_timeout",
                        },
                    ];
                    for (end, expected) in ends.iter().zip(expected) {
                        assert_eq!(prelude_verdict(end, &ctx), expected, "{end:?}; others={others}, wait={wait_available}, quota={quota_switches_left}");
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn prelude_read_preserves_the_bootstrap_exit_and_bytes() {
        for (wire, expected) in [
            ("data: {\"type\":\"response.created\"}\n\n", "eof"),
            ("data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n", "content"),
            ("data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"usage_limit_reached\"}}}\n\n", "quota"),
            ("data: {\"type\":\"service_unavailable_error\"}\n\n", "overload"),
        ] {
            let up = Up::Live(futures::stream::iter(wire.bytes().map(|byte| Ok(bytes::Bytes::from(vec![byte]))).collect::<Vec<_>>()).boxed());
            let (prelude, end) = Prelude::read(up, Duration::from_secs(1)).await;
            let actual = match end {
                PreludeEnd::Commit(Commit::UpstreamEof) => "eof",
                PreludeEnd::Commit(Commit::Content) => "content",
                PreludeEnd::Quota(_) => "quota",
                PreludeEnd::Overloaded(_) => "overload",
                other => panic!("unexpected exit: {other:?}"),
            };
            assert_eq!(actual, expected);
            assert_eq!(prelude.buf, wire);
            assert_eq!(prelude.progress.events, 1);
            if expected == "eof" {
                assert!(matches!(prelude.up, Up::Ended));
            }
        }
        let wire = bytes::Bytes::from(vec![b'x'; BOOTSTRAP_MAX + 1]);
        let up = Up::Live(
            futures::stream::iter([Ok(wire.clone())])
                .chain(futures::stream::pending())
                .boxed(),
        );
        let (prelude, end) = Prelude::read(up, Duration::from_secs(1)).await;
        assert!(matches!(end, PreludeEnd::Commit(Commit::Oversize)));
        assert_eq!(prelude.buf, wire);
    }
}
