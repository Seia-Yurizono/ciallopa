//! One slot from before body extraction through HTTP EOF/drop or WebSocket close.
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

pub(crate) fn refused(reason: Refused) -> Response {
    let (code, message) = match reason {
        Refused::Busy => ("gateway_busy", "Gateway is busy; retry shortly."),
        Refused::Draining => ("shutdown", "Gateway is shutting down; retry shortly."),
    };
    let mut r = crate::proxy::json_error(StatusCode::SERVICE_UNAVAILABLE, code, message);
    // Outside the Retry-After filter: set both headers here.
    errors::set_retry_after(r.headers_mut(), 1);
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
    let lease = match drain.admit(st.config.load().max_inflight_requests) {
        Ok(lease) => lease,
        Err(reason) => return refused(reason),
    };
    // The WS handler retains this clone in its detached upgrade task. For HTTP it is
    // dropped with the extracted request, while this middleware retains the slot.
    req.extensions_mut().insert(lease.clone());
    let response = tokio::select! {
        biased;
        _ = drain.forced() => return refused(Refused::Draining),
        response = next.run(req) => response,
    };
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        Body::new(LeasedBody {
            inner: Some(body),
            lease: Some(lease),
            forced: Box::pin(async move { drain.forced().await }),
        }),
    )
}

struct LeasedBody {
    inner: Option<Body>,
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
        if self.forced.as_mut().poll(cx).is_ready() {
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
