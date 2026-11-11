//! One slot from before body extraction through HTTP EOF/drop. A WebSocket handshake is
//! admitted outside the inflight cap (`ws_max_connections` bounds connections); each WS turn
//! takes its own slot in the relay.
use crate::{errors, AppState};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use ciallopa_upstream::drain::{Lease, Refused};
use hyper::body::{Body as HttpBody, Frame, SizeHint};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// `Retry-After` on every 503 `gateway_busy` (HTTP admission, body budget, WS connection
/// cap, and the WS turn busy frame). Chosen so Codex's ~4 fast 5xx retries wait between
/// attempts instead of re-hitting a full gate within one second.
pub(crate) const BUSY_RETRY_AFTER_SECS: u64 = 3;

pub(crate) fn refused(reason: Refused) -> Response {
    let (code, message) = match reason {
        Refused::Busy => ("gateway_busy", "Gateway is busy; retry shortly."),
        Refused::Draining => ("shutdown", "Gateway is shutting down; retry shortly."),
    };
    let mut r = crate::proxy::json_error(StatusCode::SERVICE_UNAVAILABLE, code, message);
    // Outside the Retry-After filter: set both headers here.
    // Busy: 3 s so Codex's fast 5xx retries space out instead of hammering the cap.
    // Draining: the same wait as the shutdown event (about one container restart).
    let ra = match reason {
        Refused::Busy => BUSY_RETRY_AFTER_SECS,
        Refused::Draining => crate::proxy::SHUTDOWN_RETRY_AFTER_SECS,
    };
    errors::set_retry_after(r.headers_mut(), ra);
    r.extensions_mut().insert(errors::ErrMeta {
        source: errors::SRC_LOCAL,
        code: code.into(),
        upstream_status: None,
        upstream_request_id: None,
        message: Some(message.into()),
    });
    r
}

pub(crate) async fn admit(
    State(st): State<AppState>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let drain = st.upstream.drain.clone();
    let admitted = if exempt(&req) || is_ws_upgrade(&req) {
        drain.admit_exempt()
    } else {
        let limit = st.config.load().max_inflight_requests;
        drain.admit(limit).inspect_err(|r| {
            if *r == Refused::Busy {
                note_inflight(&st, &req, limit);
            }
        })
    };
    let lease = match admitted {
        Ok(lease) => lease,
        Err(reason) => return refused(reason),
    };
    // The WS handler hands this clone to the relay until its session is counted. For HTTP
    // it is dropped with the extracted request, while this middleware retains the slot.
    req.extensions_mut().insert(lease.clone());
    let response = tokio::select! {
        biased;
        _ = drain.forced() => return refused(Refused::Draining),
        response = next.run(req) => response,
    };
    let (mut parts, body) = response.into_parts();
    let on_force = parts.extensions.remove::<OnForce>().map(|o| o.0);
    Response::from_parts(
        parts,
        Body::new(LeasedBody {
            inner: Some(body),
            on_force,
            flush_deadline: None,
            cut: false,
            lease: Some(lease),
            forced: Box::pin(async move { drain.forced().await }),
        }),
    )
}

/// Model lists and quota polls are small, frequent and needed to show the user what is
/// going on, so they never take or wait for an inflight slot.
fn exempt(req: &Request<Body>) -> bool {
    req.method() == http::Method::GET
        && matches!(
            req.uri().path(),
            "/v1/models"
                | "/backend-api/codex/models"
                | "/backend-api/wham/usage"
                | "/api/codex/usage"
        )
}

fn is_ws_upgrade(req: &Request<Body>) -> bool {
    req.headers()
        .get(http::header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
}

fn note_inflight(st: &AppState, req: &Request<Body>, limit: usize) {
    let ws = is_ws_upgrade(req);
    let key = req
        .extensions()
        .get::<crate::ClientKey>()
        .map(|k| &*k.label);
    crate::refusal::note(
        st,
        crate::refusal::Refusal {
            kind: ciallopa_upstream::drain::RefusalKind::Inflight,
            ws,
            key,
            path: req.uri().path(),
            headers: req.headers(),
            model: None,
            account: None,
            limit: limit as u64,
            bytes: None,
        },
    );
}

/// Last frame for a body cut by shutdown (a retryable Responses `response.failed`), set by
/// the Responses SSE forwarder as a response extension.
#[derive(Clone)]
pub(crate) struct OnForce(pub(crate) Bytes);

/// Longest a cut SSE body keeps passing on already-queued chunks before its terminal event.
const CUT_FLUSH_MAX: std::time::Duration = std::time::Duration::from_millis(500);

struct LeasedBody {
    inner: Option<Body>,
    on_force: Option<Bytes>,
    flush_deadline: Option<Pin<Box<tokio::time::Sleep>>>,
    /// Shutdown force was observed.
    cut: bool,
    lease: Option<Lease>,
    forced: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl HttpBody for LeasedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        if self.inner.is_none() {
            return Poll::Ready(None);
        }
        // `forced` completes once and must not be polled again after that.
        if self.cut || self.forced.as_mut().poll(cx).is_ready() {
            self.cut = true;
            if self.on_force.is_some() {
                // A Responses SSE cut by shutdown: pass on everything the producer already
                // queued (it stops on force, so the queue ends), then the retryable
                // terminal event, then a clean end of body.
                let deadline = self
                    .flush_deadline
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep(CUT_FLUSH_MAX)));
                let timed_out = deadline.as_mut().poll(cx).is_ready();
                if !timed_out {
                    let inner = self.inner.as_mut().expect("body present");
                    match Pin::new(inner).poll_frame(cx) {
                        Poll::Ready(Some(Ok(frame))) => return Poll::Ready(Some(Ok(frame))),
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(None | Some(Err(_))) => {}
                    }
                }
                self.inner.take();
                let event = self.on_force.take().expect("checked above");
                return Poll::Ready(Some(Ok(Frame::data(event))));
            }
            self.inner.take();
            self.lease.take();
            return Poll::Ready(Some(Err(axum::Error::new(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "server shutdown",
            )))));
        }
        let result = Pin::new(self.inner.as_mut().expect("body present")).poll_frame(cx);
        if matches!(&result, Poll::Ready(None | Some(Err(_)))) {
            self.inner.take();
            self.lease.take();
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.as_ref().is_none_or(HttpBody::is_end_stream)
    }

    fn size_hint(&self) -> SizeHint {
        self.inner
            .as_ref()
            .map(HttpBody::size_hint)
            .unwrap_or_default()
    }
}
