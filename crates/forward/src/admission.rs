//! Hold admission through body EOF/drop; WS handshakes are exempt and turns take their own slot.
use crate::{errors, AppState};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use ciallopa_upstream::drain::{Lease, Refused, CUT_FLUSH_MAX};
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
    let mut r = crate::errors::json_error(StatusCode::SERVICE_UNAVAILABLE, code, message);
    // Outside the Retry-After filter: set both headers here.
    // Busy: 3 s so Codex's fast 5xx retries space out instead of hammering the cap.
    // Draining: the same wait as the shutdown event (about one container restart).
    let ra = match reason {
        Refused::Busy => BUSY_RETRY_AFTER_SECS,
        Refused::Draining => crate::http::pump::SHUTDOWN_RETRY_AFTER_SECS,
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
    let exempt = exempt(&req);
    if !exempt {
        if let Some(start) = crate::usage::Started::capture(&st) {
            req.extensions_mut().insert(start);
        }
    }
    let drain = st.upstream.drain.clone();
    let admitted = if exempt || is_ws_upgrade(&req) {
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
        Body::new(LeasedBody(Leased::Running {
            body,
            lease,
            forced: Box::pin(async move { drain.forced().await }),
            on_force,
            cut_flush: crate::timing::scale(&st, CUT_FLUSH_MAX),
        })),
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

struct LeasedBody(Leased);

enum Leased {
    Running {
        body: Body,
        lease: Lease,
        forced: Pin<Box<dyn Future<Output = ()> + Send>>,
        on_force: Option<Bytes>,
        /// How long a cut body keeps passing on queued chunks (captured at admission).
        cut_flush: std::time::Duration,
    },
    Flushing {
        body: Body,
        lease: Lease,
        deadline: Pin<Box<tokio::time::Sleep>>,
        event: Bytes,
    },
    /// Keep the slot while hyper writes the terminal event.
    Terminal {
        lease: Lease,
    },
    Done,
}

impl HttpBody for LeasedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        loop {
            match std::mem::replace(&mut self.0, Leased::Done) {
                Leased::Running {
                    mut body,
                    lease,
                    mut forced,
                    on_force,
                    cut_flush,
                } => {
                    if forced.as_mut().poll(cx).is_ready() {
                        if let Some(event) = on_force {
                            self.0 = Leased::Flushing {
                                body,
                                lease,
                                deadline: Box::pin(tokio::time::sleep(cut_flush)),
                                event,
                            };
                            continue;
                        }
                        drop(body);
                        drop(lease);
                        return Poll::Ready(Some(Err(axum::Error::new(std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "server shutdown",
                        )))));
                    }
                    let frame = Pin::new(&mut body).poll_frame(cx);
                    if !matches!(&frame, Poll::Ready(None | Some(Err(_)))) {
                        self.0 = Leased::Running {
                            body,
                            lease,
                            forced,
                            on_force,
                            cut_flush,
                        };
                    } else {
                        drop(body);
                        drop(lease);
                    }
                    return frame;
                }
                Leased::Flushing {
                    mut body,
                    lease,
                    mut deadline,
                    event,
                } => {
                    if deadline.as_mut().poll(cx).is_pending() {
                        match Pin::new(&mut body).poll_frame(cx) {
                            frame @ (Poll::Ready(Some(Ok(_))) | Poll::Pending) => {
                                self.0 = Leased::Flushing {
                                    body,
                                    lease,
                                    deadline,
                                    event,
                                };
                                return frame;
                            }
                            Poll::Ready(None | Some(Err(_))) => {}
                        }
                    }
                    self.0 = Leased::Terminal { lease };
                    return Poll::Ready(Some(Ok(Frame::data(event))));
                }
                Leased::Terminal { lease } => {
                    drop(lease);
                    return Poll::Ready(None);
                }
                Leased::Done => return Poll::Ready(None),
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.0 {
            Leased::Running { body, .. } | Leased::Flushing { body, .. } => body.is_end_stream(),
            Leased::Terminal { .. } | Leased::Done => true,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.0 {
            Leased::Running { body, .. } | Leased::Flushing { body, .. } => body.size_hint(),
            Leased::Terminal { .. } | Leased::Done => SizeHint::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ciallopa_upstream::drain::Drain;
    use std::sync::Arc;

    fn leased(drain: &Arc<Drain>, body: Body, on_force: Option<Bytes>) -> LeasedBody {
        let lease = drain.admit(1).unwrap();
        let drain = drain.clone();
        LeasedBody(Leased::Running {
            body,
            lease,
            forced: Box::pin(async move { drain.forced().await }),
            on_force,
            cut_flush: CUT_FLUSH_MAX,
        })
    }

    async fn frame(body: &mut LeasedBody) -> Option<Result<Frame<Bytes>, axum::Error>> {
        std::future::poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)).await
    }

    #[tokio::test]
    async fn pending_and_data_hold_the_lease_until_drop() {
        for pending in [false, true] {
            let drain = Arc::new(Drain::default());
            let inner = if pending {
                Body::from_stream(futures::stream::pending::<Result<Bytes, std::io::Error>>())
            } else {
                Body::from("chunk")
            };
            let mut body = leased(&drain, inner, None);
            let mut cx = Context::from_waker(std::task::Waker::noop());
            let frame = Pin::new(&mut body).poll_frame(&mut cx);
            assert_eq!(frame.is_pending(), pending);
            assert_eq!(drain.inflight(), 1);
            drop(body);
            assert_eq!(drain.inflight(), 0);
        }
    }

    #[tokio::test]
    async fn eof_and_read_error_release_the_lease() {
        for error in [false, true] {
            let drain = Arc::new(Drain::default());
            let chunks = error.then(|| Err::<Bytes, _>(std::io::Error::other("read error")));
            let mut body = leased(
                &drain,
                Body::from_stream(futures::stream::iter(chunks)),
                None,
            );
            let end = frame(&mut body).await;
            assert_eq!(end.is_some(), error);
            if let Some(end) = end {
                assert!(end.is_err());
            }
            assert_eq!(drain.inflight(), 0);
            assert!(frame(&mut body).await.is_none());
        }
    }

    #[tokio::test]
    async fn force_without_terminal_event_interrupts_and_releases_the_lease() {
        let drain = Arc::new(Drain::default());
        let mut body = leased(&drain, Body::from("queued"), None);
        drain.force();
        let error = frame(&mut body).await.unwrap().unwrap_err();
        assert!(error.to_string().contains("server shutdown"));
        assert_eq!(drain.inflight(), 0);
        assert!(frame(&mut body).await.is_none());
    }

    #[tokio::test]
    async fn force_flushes_queued_data_and_holds_the_lease_through_the_terminal_event() {
        let drain = Arc::new(Drain::default());
        let mut body = leased(
            &drain,
            Body::from("queued"),
            Some(Bytes::from_static(b"terminal")),
        );
        drain.force();
        let queued = frame(&mut body)
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        assert_eq!(queued, "queued");
        assert_eq!(drain.inflight(), 1);
        let terminal = frame(&mut body)
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        assert_eq!(terminal, "terminal");
        assert!(body.is_end_stream());
        assert_eq!(drain.inflight(), 1);
        assert!(frame(&mut body).await.is_none());
        assert_eq!(drain.inflight(), 0);
    }

    #[tokio::test]
    async fn expired_flush_deadline_emits_terminal_event_without_polling_the_inner_body() {
        let drain = Arc::new(Drain::default());
        let mut body = LeasedBody(Leased::Flushing {
            body: Body::from_stream(futures::stream::pending::<Result<Bytes, std::io::Error>>()),
            lease: drain.admit(1).unwrap(),
            deadline: Box::pin(tokio::time::sleep_until(
                tokio::time::Instant::now() - std::time::Duration::from_secs(1),
            )),
            event: Bytes::from_static(b"terminal"),
        });
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let Poll::Ready(Some(Ok(terminal))) = Pin::new(&mut body).poll_frame(&mut cx) else {
            panic!("expired flush emits immediately");
        };
        assert_eq!(terminal.into_data().unwrap(), "terminal");
        assert_eq!(drain.inflight(), 1);
        drop(body);
        assert_eq!(drain.inflight(), 0);
    }
}
