use crate::attempt::{overload_outcome, Chosen};
use crate::classify::{classify, Class, Terminal};
use crate::errors;
use crate::headers::downstream_headers;
use crate::sse::{Frame, Framer};
use crate::usage::{End, StreamObservation, Tracker};
use crate::AppState;
use axum::body::Body;
use axum::response::Response;
use bytes::Bytes;
use ciallopa_core::{AccountId, Outcome, RouteRequest};
use futures::StreamExt;
use http::{HeaderMap, HeaderValue, StatusCode};
use std::sync::Arc;
use std::time::Duration;

// Images have their own event vocabulary and must not use Responses terminal policy.
pub(crate) fn stream_back_tracked(
    st: &AppState,
    c: Chosen,
    mut trk: Option<Tracker>,
    expect_sse: bool,
    images: bool,
) -> Response {
    let status = c.resp.status();
    let is_sse = is_sse(c.resp.headers(), expect_sse);
    let sse = status.is_success() && is_sse;
    let mut headers = downstream_headers(c.resp.headers());
    if sse && !images {
        ensure_sse_content_type(&mut headers);
    }
    if let Some(t) = trk.as_mut() {
        t.rec.status = status.as_u16();
        if status.as_u16() >= 400 {
            if !images {
                t.error_headers(c.resp.headers());
            }
            t.upstream_error(status.as_u16(), c.resp.headers());
        }
        if images && is_sse {
            t.rec.transport = ciallopa_usage::Transport::Sse;
        } else if images || !sse {
            t.rec.transport = ciallopa_usage::Transport::Http;
        }
    }
    let observation = match (sse, images) {
        (false, _) => Observation::Http,
        (true, true) => Observation::Images,
        (true, false) => Observation::Responses(StreamObservation::new()),
    };
    stream_body(
        st,
        status,
        headers,
        c.route,
        c.account,
        Bytes::new(),
        Up::Live(c.resp.bytes_stream().boxed()),
        trk,
        observation,
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

/// Retain EOF explicitly so later reads cannot invent a new upstream.
pub(crate) enum Up {
    Live(UpStream),
    Ended,
}

impl Up {
    pub(crate) async fn next(&mut self) -> Option<reqwest::Result<Bytes>> {
        match self {
            Self::Live(stream) => {
                let next = stream.next().await;
                if next.is_none() {
                    *self = Self::Ended;
                }
                next
            }
            Self::Ended => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Window {
    Tracking,
    Limited,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Seen {
    Open,
    Terminal,
}

struct ResponsesObserver {
    framer: Framer,
    // Only events larger than the forwarding window need a separate observation copy.
    large: bytes::BytesMut,
    window: Window,
    seen: Seen,
    account_reported: bool,
    progress: StreamObservation,
}

impl ResponsesObserver {
    fn new(progress: StreamObservation, account_reported: bool) -> Self {
        Self {
            framer: Framer::new(crate::sse::BOOTSTRAP_MAX),
            large: bytes::BytesMut::new(),
            window: Window::Tracking,
            seen: Seen::Open,
            account_reported,
            progress,
        }
    }

    fn on_event(
        &mut self,
        event: &[u8],
        trk: &mut Option<Tracker>,
        observed: &mut u64,
        late_overload: &Outcome,
    ) -> Option<Outcome> {
        let text = String::from_utf8_lossy(event);
        let data = crate::sse::data(text.as_bytes()).unwrap_or_default();
        let ty = crate::usage::event_type(&data);
        if let Some(ty) = ty.as_deref() {
            if *observed > 0 {
                *observed -= 1;
            } else {
                self.progress.event(ty);
            }
            if crate::usage::is_terminal_type(ty) {
                self.seen = Seen::Terminal;
            }
        }
        if let Some(t) = trk.as_mut() {
            t.sse_event(&data, ty.as_deref());
        }
        if !self.account_reported
            && matches!(
                ty.as_deref(),
                None | Some("error" | "response.failed" | "service_unavailable_error")
            )
        {
            match classify(ty.as_deref(), &data).class {
                Class::Terminal(Terminal::Quota(outcome)) => {
                    self.account_reported = true;
                    if let Some(t) = trk.as_mut() {
                        t.fail(errors::quota_outcome_code(&outcome));
                    }
                    return Some(outcome);
                }
                Class::Terminal(Terminal::Overloaded(_)) => {
                    self.account_reported = true;
                    return Some(late_overload.clone());
                }
                _ => {}
            }
        }
        None
    }

    fn on_chunk(
        &mut self,
        chunk: Bytes,
        trk: &mut Option<Tracker>,
        observed: &mut u64,
        late_overload: &Outcome,
    ) -> (Bytes, Option<Outcome>) {
        let pending = self.large.len().max(self.framer.buffered_len());
        let skip = pending.saturating_add(chunk.len()) > OBSERVATION_MAX;
        if skip {
            self.large.clear();
            self.window = Window::Limited;
            self.progress.limit_reached();
        }
        let original = (self.framer.buffered_len() == 0).then(|| chunk.clone());
        let frames: Vec<_> = self.framer.push_parts(chunk).collect();
        let mut parts = Vec::with_capacity(frames.len());
        let mut rewritten = false;
        let mut ready = 0;
        let mut report = None;
        for (frame, boundary) in frames {
            let bytes = match frame {
                Frame::Event(event) => {
                    if !skip {
                        let outcome = self.on_event(&event, trk, observed, late_overload);
                        report = report.or(outcome);
                    }
                    let replacement = crate::sse::rewrite_rejection(&event);
                    rewritten |= replacement.is_some();
                    replacement.unwrap_or(event)
                }
                Frame::Oversize(bytes) => {
                    if !skip && self.large.len().saturating_add(bytes.len()) <= OBSERVATION_MAX {
                        self.large.extend_from_slice(&bytes);
                        if boundary {
                            let event = self.large.split().freeze();
                            let outcome = self.on_event(&event, trk, observed, late_overload);
                            report = report.or(outcome);
                        }
                    } else {
                        self.large.clear();
                    }
                    bytes
                }
            };
            ready += bytes.len();
            parts.push(bytes);
        }
        // Preserve upstream chunk grouping (and downstream observation fields) whenever possible.
        let out = if !rewritten && original.is_some() {
            original.unwrap_or_default().slice(..ready)
        } else if parts.len() == 1 {
            parts.pop().unwrap_or_default()
        } else {
            let mut out = bytes::BytesMut::with_capacity(ready);
            for part in parts {
                out.extend_from_slice(&part);
            }
            out.freeze()
        };
        (out, report)
    }
}

enum Observer {
    Passthrough,
    Responses(ResponsesObserver),
    Images(crate::image_events::ImageErrors, HeaderMap),
}

impl Observer {
    fn progress(&self) -> Option<&StreamObservation> {
        match self {
            Self::Responses(observer) => Some(&observer.progress),
            _ => None,
        }
    }

    fn tail(&mut self) -> Option<Bytes> {
        let Self::Responses(observer) = self else {
            return None;
        };
        let tail = observer.framer.finish()?;
        Some(crate::sse::rewrite_rejection(&tail).unwrap_or(tail))
    }
}

enum BodyEnd {
    Eof,
    Idle,
    ReadError(reqwest::Error),
    ClientGone,
}

#[derive(Debug, PartialEq, Eq)]
enum Tail {
    None,
    Truncated,
    Io(std::io::ErrorKind),
}

struct Finish {
    record: End,
    router: Option<Outcome>,
    warn: Option<&'static str>,
    tail: Tail,
}

/// Keep terminal outcomes independent of subsequent body failures.
fn finish(observer: &Observer, end: &BodyEnd) -> Finish {
    let responses = matches!(observer, Observer::Responses(_));
    let mut result = Finish {
        record: End::Completed,
        router: None,
        warn: None,
        tail: Tail::None,
    };
    if matches!(
        observer,
        Observer::Responses(ResponsesObserver {
            seen: Seen::Terminal,
            ..
        })
    ) {
        return result;
    }
    let failed = |code: &'static str, source| End::Failed {
        code: code.into(),
        source,
        message: None,
    };
    match end {
        BodyEnd::ClientGone => result.record = End::ClientGone,
        BodyEnd::Eof if responses => {
            if matches!(
                observer,
                Observer::Responses(ResponsesObserver {
                    window: Window::Limited,
                    ..
                })
            ) {
                result.record = End::ObservationIncomplete;
                result.warn = Some("observation_limit");
            } else {
                result.record = End::Failed {
                    code: "stream_truncated".into(),
                    source: errors::SRC_STREAM,
                    message: Some("upstream stream ended without a terminal event".into()),
                };
                result.warn = Some("eof");
                result.tail = Tail::Truncated;
            }
        }
        BodyEnd::Eof => {}
        BodyEnd::Idle => {
            result.record = failed("idle_timeout", errors::SRC_NETWORK);
            result.router = Some(Outcome::Transport);
            result.warn = Some("idle_timeout");
            result.tail = if responses {
                Tail::Truncated
            } else {
                Tail::Io(std::io::ErrorKind::TimedOut)
            };
        }
        BodyEnd::ReadError(error) => {
            let (code, source) = response_read_failure(error);
            result.record = End::Failed {
                code: code.into(),
                source,
                message: Some(format!("upstream stream error: {error}")),
            };
            if !crate::response_body::is_decode_error(error) {
                result.router = Some(Outcome::Transport);
            }
            result.warn = responses.then_some("read_error");
            result.tail = if responses {
                Tail::Truncated
            } else {
                Tail::Io(std::io::ErrorKind::Other)
            };
        }
    }
    result
}

struct BodyPump {
    up: Up,
    observer: Observer,
    trk: Option<Tracker>,
    router: Arc<dyn ciallopa_core::Router>,
    route: RouteRequest,
    account: AccountId,
    late_overload: Outcome,
    upstream_request_id: Option<String>,
    status: StatusCode,
    idle: Duration,
}

impl BodyPump {
    async fn send_chunk(
        &mut self,
        tx: &crate::downstream::ObsTx,
        chunk: Bytes,
        prefix: bool,
    ) -> bool {
        if chunk.is_empty() {
            return true;
        }
        if !prefix {
            if let Some(t) = self.trk.as_mut() {
                t.upstream_activity();
                if matches!(self.observer, Observer::Responses(_)) {
                    t.obs_up_bytes(chunk.len());
                }
            }
        }
        let (out, report) = match &mut self.observer {
            Observer::Passthrough => (chunk, None),
            Observer::Images(events, headers) => {
                if let (Some(error), Some(t)) = (events.feed(&chunk), self.trk.as_mut()) {
                    t.meta_err(&errors::ErrMeta {
                        source: errors::SRC_STREAM,
                        code: errors::sse_block_code(&error.event)
                            .unwrap_or_else(|| "upstream_error_event".into()),
                        upstream_status: Some(self.status.as_u16()),
                        upstream_request_id: self.upstream_request_id.clone(),
                        message: Some(error.message),
                    });
                    t.error_headers(headers);
                    t.error_text(&error.event);
                }
                (chunk, None)
            }
            Observer::Responses(observer) => {
                let mut observed = if prefix { observer.progress.events } else { 0 };
                if !prefix {
                    observer.progress.activity();
                }
                observer.on_chunk(chunk, &mut self.trk, &mut observed, &self.late_overload)
            }
        };
        if let Some(outcome) = report {
            self.router.report(&self.route, &self.account, &outcome);
        }
        if self.status.as_u16() >= 400 {
            if let Some(t) = self.trk.as_mut() {
                t.error_body(&out);
            }
        }
        out.is_empty() || tx.send(Ok(out)).await.is_ok()
    }

    fn observe_end(&mut self, result: &Finish) {
        // Preserve the upstream failure if cancellation interrupts the tail flush.
        if let (Some(t), end @ End::Failed { .. }) = (self.trk.as_mut(), &result.record) {
            t.conclude(end.clone());
        }
        if let Some(reason) = result.warn {
            if let Some(progress) = self.observer.progress() {
                progress.warn(
                    "sse",
                    reason,
                    &self.account,
                    self.upstream_request_id.as_deref(),
                    self.trk.as_ref(),
                );
            } else {
                tracing::warn!(account = %ciallopa_core::a(&self.account), record_id = self.trk.as_ref().map(Tracker::record_id), upstream_request_id = self.upstream_request_id.as_deref(), "upstream stream idle timeout");
            }
        }
        if let Some(outcome) = &result.router {
            self.router.report(&self.route, &self.account, outcome);
        }
    }

    async fn end(mut self, tx: &crate::downstream::ObsTx, end: BodyEnd) {
        let eof = matches!(end, BodyEnd::Eof);
        let mut result = finish(&self.observer, &end);
        if let End::Failed { message, .. } = &mut result.record {
            if matches!(end, BodyEnd::Idle) {
                *message = Some(format!(
                    "upstream response body produced no data for {}s (idle timeout)",
                    self.idle.as_secs()
                ));
            }
        }
        if !eof {
            self.observe_end(&result);
        }
        // On EOF, failed tail delivery is cancellation. On read/idle failure it must
        // preserve the upstream error that was already observed before flushing.
        if !matches!(end, BodyEnd::ClientGone) {
            if let Some(tail) = self.observer.tail() {
                if tx.send(Ok(tail)).await.is_err() && eof {
                    self.record(finish(&self.observer, &BodyEnd::ClientGone).record);
                    return;
                }
            }
        }
        if eof {
            self.observe_end(&result);
        }
        match result.tail {
            Tail::None => {}
            Tail::Truncated => {
                let _ = tx.send(Ok(errors::truncated_event())).await;
            }
            Tail::Io(kind) => {
                let error = match end {
                    BodyEnd::ReadError(error) => std::io::Error::other(error),
                    _ => std::io::Error::new(kind, "stream idle timeout"),
                };
                let _ = tx.send(Err(error)).await;
            }
        }
        self.record(result.record);
    }

    fn record(&mut self, end: End) {
        if let Some(t) = self.trk.take() {
            t.finish(end);
        }
    }

    async fn run(mut self, tx: crate::downstream::ObsTx, prefix: Bytes) {
        if !self.send_chunk(&tx, prefix, true).await {
            self.end(&tx, BodyEnd::ClientGone).await;
            return;
        }
        let end = loop {
            match tokio::time::timeout(self.idle, self.up.next()).await {
                Err(_) => break BodyEnd::Idle,
                Ok(None) => break BodyEnd::Eof,
                Ok(Some(Err(error))) => break BodyEnd::ReadError(error),
                Ok(Some(Ok(chunk))) => {
                    if !self.send_chunk(&tx, chunk, false).await {
                        break BodyEnd::ClientGone;
                    }
                }
            }
        };
        self.end(&tx, end).await;
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn stream_body(
    st: &AppState,
    status: StatusCode,
    headers: HeaderMap,
    route: RouteRequest,
    account: AccountId,
    prefix: Bytes,
    up: Up,
    mut trk: Option<Tracker>,
    observation: Observation,
    idle: Duration,
    account_reported: bool,
) -> Response {
    let sse = matches!(observation, Observation::Responses(_));
    // Hyper need not poll bodies whose HTTP framing already proves EOF.
    let empty = status == StatusCode::NO_CONTENT
        || status == StatusCode::NOT_MODIFIED
        || headers
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            == Some(0);
    if !sse && prefix.is_empty() && empty {
        if let Some(t) = trk.take() {
            t.finish(End::Completed);
        }
        let mut response = Response::new(Body::empty());
        *response.status_mut() = status;
        *response.headers_mut() = headers;
        return response;
    }
    let upstream_request_id = errors::request_id(&headers);
    let link = if let (true, Some(t)) = (sse, trk.as_mut()) {
        t.obs_upstream_request_id(upstream_request_id.as_deref());
        t.obs_up_bytes(prefix.len());
        let link = crate::downstream::DownLink::new(t.start);
        t.defer_to(link.clone());
        Some(link)
    } else {
        None
    };
    let observer = match observation {
        Observation::Responses(progress) => {
            Observer::Responses(ResponsesObserver::new(progress, account_reported))
        }
        Observation::Images if trk.is_some() => {
            Observer::Images(crate::image_events::ImageErrors::default(), headers.clone())
        }
        _ => Observer::Passthrough,
    };
    let pump = BodyPump {
        up,
        observer,
        trk,
        router: st.router.clone(),
        route,
        account,
        late_overload: overload_outcome(st, false),
        upstream_request_id,
        status,
        idle,
    };
    let body = crate::downstream::observed(st.upstream.drain.clone(), link, move |tx| {
        pump.run(tx, prefix)
    });
    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    if sse {
        response.headers_mut().remove(http::header::CONTENT_LENGTH);
        response
            .extensions_mut()
            .insert(crate::admission::OnForce(Bytes::from(format!(
                "\n\nevent: response.failed\ndata: {}\n\n",
                shutdown_failed_event()
            ))));
    }
    response
}

const OBSERVATION_MAX: usize = 8 << 20;

/// Missing/unknown Content-Type uses the SSE hint; explicit JSON/text/HTML/XML wins.
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

/// Browsers and SDKs need an explicit SSE media type.
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

/// Codex retries unknown response.failed codes using error.headers.retry-after.
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

#[cfg(test)]
mod finish_tests {
    use super::*;

    async fn read_error(decode: bool) -> reqwest::Error {
        let body = if decode {
            reqwest::Body::from("invalid gzip")
        } else {
            reqwest::Body::wrap_stream(futures::stream::iter([Err::<Bytes, _>(
                std::io::Error::other("read failed"),
            )]))
        };
        let mut response = http::Response::new(body);
        if decode {
            response.headers_mut().insert(
                http::header::CONTENT_ENCODING,
                HeaderValue::from_static("gzip"),
            );
        }
        crate::response_body::prepare(response.into())
            .unwrap()
            .bytes()
            .await
            .unwrap_err()
    }

    fn record(end: &End) -> (&str, Option<&str>) {
        match end {
            End::Completed => ("completed", None),
            End::ClientGone => ("client_closed", Some(errors::SRC_CLIENT)),
            End::ObservationIncomplete => ("observation_incomplete", Some(errors::SRC_LOCAL)),
            End::Failed { code, source, .. } => (code, Some(source)),
            End::Forced => ("forced", None),
        }
    }

    #[tokio::test]
    async fn finish_covers_every_observer_and_body_end() {
        let ends = [
            BodyEnd::Eof,
            BodyEnd::Idle,
            BodyEnd::ReadError(read_error(false).await),
            BodyEnd::ReadError(read_error(true).await),
            BodyEnd::ClientGone,
        ];
        for window in [Window::Tracking, Window::Limited] {
            for seen in [Seen::Open, Seen::Terminal] {
                let mut responses = ResponsesObserver::new(StreamObservation::new(), false);
                responses.window = window;
                responses.seen = seen;
                let observers = [
                    Observer::Responses(responses),
                    Observer::Passthrough,
                    Observer::Images(
                        crate::image_events::ImageErrors::default(),
                        HeaderMap::new(),
                    ),
                ];
                for observer in observers {
                    let sse = matches!(observer, Observer::Responses(_));
                    for (index, end) in ends.iter().enumerate() {
                        let normal = sse && seen == Seen::Terminal;
                        let expected = if normal {
                            ("completed", None, false, None, Tail::None)
                        } else {
                            match index {
                                0 if !sse => ("completed", None, false, None, Tail::None),
                                0 if window == Window::Limited => (
                                    "observation_incomplete",
                                    Some(errors::SRC_LOCAL),
                                    false,
                                    Some("observation_limit"),
                                    Tail::None,
                                ),
                                0 => (
                                    "stream_truncated",
                                    Some(errors::SRC_STREAM),
                                    false,
                                    Some("eof"),
                                    Tail::Truncated,
                                ),
                                1 => (
                                    "idle_timeout",
                                    Some(errors::SRC_NETWORK),
                                    true,
                                    Some("idle_timeout"),
                                    if sse {
                                        Tail::Truncated
                                    } else {
                                        Tail::Io(std::io::ErrorKind::TimedOut)
                                    },
                                ),
                                2 => (
                                    "stream_read_error",
                                    Some(errors::SRC_NETWORK),
                                    true,
                                    sse.then_some("read_error"),
                                    if sse {
                                        Tail::Truncated
                                    } else {
                                        Tail::Io(std::io::ErrorKind::Other)
                                    },
                                ),
                                3 => (
                                    "response_decode_error",
                                    Some(errors::SRC_STREAM),
                                    false,
                                    sse.then_some("read_error"),
                                    if sse {
                                        Tail::Truncated
                                    } else {
                                        Tail::Io(std::io::ErrorKind::Other)
                                    },
                                ),
                                _ => (
                                    "client_closed",
                                    Some(errors::SRC_CLIENT),
                                    false,
                                    None,
                                    Tail::None,
                                ),
                            }
                        };
                        let actual = finish(&observer, end);
                        let (code, source) = record(&actual.record);
                        assert_eq!(
                            (
                                code,
                                source,
                                actual.router.is_some(),
                                actual.warn,
                                actual.tail
                            ),
                            expected,
                            "sse={sse}, {window:?}, {seen:?}, end={index}"
                        );
                        if let Some(router) = actual.router {
                            assert_eq!(router, Outcome::Transport);
                        }
                    }
                }
            }
        }
    }
    fn push(observer: &mut ResponsesObserver, bytes: Bytes) -> Bytes {
        observer
            .on_chunk(bytes, &mut None, &mut 0, &Outcome::Transport)
            .0
    }

    #[test]
    fn observer_preserves_chunk_grouping_and_borrows_complete_chunks() {
        let first = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n";
        let second = "data: {\"type\":\"response.completed\"}\n\n";
        let bytes = Bytes::from(format!("{first}{second}data: tail"));
        let mut observer = ResponsesObserver::new(StreamObservation::new(), false);
        let out = push(&mut observer, bytes.clone());
        assert_eq!(out, bytes.slice(..first.len() + second.len()));
        assert_eq!(out.as_ptr(), bytes.as_ptr());
        assert_eq!(observer.progress.events, 2);
        assert_eq!(observer.seen, Seen::Terminal);
        assert_eq!(
            observer.framer.finish().as_deref(),
            Some(&b"data: tail"[..])
        );

        let mut observer = ResponsesObserver::new(StreamObservation::new(), false);
        assert!(push(&mut observer, bytes.slice(..3)).is_empty());
        let out = push(&mut observer, bytes.slice(3..));
        assert_eq!(out, bytes.slice(..first.len() + second.len()));
        assert_eq!(observer.progress.events, 2);
    }

    #[test]
    fn forwarding_window_does_not_shrink_the_observation_window() {
        let wire = Bytes::from(format!(
            "data: {{\"type\":\"response.completed\",\"padding\":\"{}\"}}\n\n",
            "x".repeat(crate::sse::BOOTSTRAP_MAX + 100)
        ));
        for cut in [
            0,
            1,
            crate::sse::BOOTSTRAP_MAX - 1,
            crate::sse::BOOTSTRAP_MAX + 1,
            wire.len(),
        ] {
            let mut observer = ResponsesObserver::new(StreamObservation::new(), false);
            let mut out = push(&mut observer, wire.slice(..cut)).to_vec();
            out.extend_from_slice(&push(&mut observer, wire.slice(cut..)));
            assert_eq!(out, wire);
            assert_eq!(observer.seen, Seen::Terminal);
            assert_eq!(observer.window, Window::Tracking);
            assert_eq!(observer.progress.events, 1);
        }
    }

    #[test]
    fn observer_rewrites_split_rejection_only() {
        let ev = concat!(
            "event: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"The encrypted content for item rs_1 could not be verified.\",",
            "\"type\":\"invalid_request_error\",\"param\":\"input\",\"code\":\"invalid_encrypted_content\"}}\n\n"
        );
        let head = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"invalid_encrypted_content\"}\r\n\r\n";
        let all = format!("{head}{ev}");
        for cut in 0..=all.len() {
            let mut a = ResponsesObserver::new(StreamObservation::new(), false);
            let mut out = push(&mut a, Bytes::copy_from_slice(&all.as_bytes()[..cut])).to_vec();
            out.extend_from_slice(&push(
                &mut a,
                Bytes::copy_from_slice(&all.as_bytes()[cut..]),
            ));
            if let Some(r) = a
                .framer
                .finish()
                .map(|tail| crate::sse::rewrite_rejection(&tail).unwrap_or(tail))
            {
                out.extend_from_slice(&r);
            }
            let out = String::from_utf8(out).unwrap();
            assert!(out.starts_with(head), "{cut}: {out}");
            assert!(
                !out.contains("could not be verified") && !out.contains("event: error"),
                "{out}"
            );
            assert!(
                out.contains("event: response.failed")
                    && out.contains(crate::encrypted::MESSAGE.split(' ').next().unwrap())
            );
        }
        // Partial events are held; whole ones flow unchanged; non-SSE is untouched.
        let mut a = ResponsesObserver::new(StreamObservation::new(), false);
        assert!(push(&mut a, Bytes::from_static(b"data: {\"a\":1}")).is_empty());
        assert_eq!(
            &push(&mut a, Bytes::from_static(b"\n\ndata: x"))[..],
            b"data: {\"a\":1}\n\n"
        );
        assert_eq!(
            &a.framer
                .finish()
                .map(|tail| crate::sse::rewrite_rejection(&tail).unwrap_or(tail))
                .unwrap()[..],
            b"data: x"
        );
    }
}
