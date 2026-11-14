//! OpenAI Images API on top of Codex, mirroring CPA `executor/codex_openai_images.go`:
//! (a) gpt-image-* models → direct `/codex/images/{generations,edits}` (multipart edits → JSON);
//! (b) anything else → `/codex/responses` with the `image_generation` tool, results reassembled.
mod request;
mod wrapper;

use self::request::Kind;
use self::wrapper::run;
use crate::{
    errors,
    usage::{End, Tracker},
    AppState,
};
use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use bytes::Bytes;
use http::HeaderMap;
use hyper::body::{Body as HttpBody, Frame, SizeHint};
use serde_json::Value;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

pub(crate) async fn generations(
    st: State<AppState>,
    key: crate::Key,
    h: HeaderMap,
    b: crate::reqbody::ReqBody,
) -> Response {
    tracked(st.0, key, h, b.bytes, Kind::Generate).await
}

pub(crate) async fn edits(
    st: State<AppState>,
    key: crate::Key,
    h: HeaderMap,
    b: crate::reqbody::ReqBody,
) -> Response {
    tracked(st.0, key, h, b.bytes, Kind::Edit).await
}

/// One tracker: streaming branches take it into their producer; buffered responses retain
/// it here until their body is consumed or dropped. Header arrival is never completion.
async fn tracked(st: AppState, key: crate::Key, h: HeaderMap, b: Bytes, kind: Kind) -> Response {
    let mut trk = Tracker::start(
        &st,
        ciallopa_usage::Transport::Http,
        "images",
        crate::key_label(&key),
    );
    if let Some(t) = trk.as_mut() {
        t.headers(&h);
        t.meta(serde_json::from_slice::<Value>(&b).ok().as_ref());
    }
    let r = run(st, h, b, kind, &mut trk).await;
    let Some(mut t) = trk else { return r };
    t.status(r.status().as_u16());
    t.http_response();
    if let Some(meta) = r.extensions().get::<errors::ErrMeta>() {
        t.meta_err(meta);
    }
    let capture_error = r.status().as_u16() >= 400;
    let (parts, body) = r.into_parts();
    if body.is_end_stream() {
        t.finish(End::Completed);
        return Response::from_parts(parts, body);
    }
    Response::from_parts(
        parts,
        Body::new(RecordedBody::Open {
            body,
            tracker: t,
            capture_error,
        }),
    )
}

/// Ready-made JSON/errors have no detached producer to own their tracker.
// The tracker stays in the body's existing allocation until completion.
#[allow(clippy::large_enum_variant)]
enum RecordedBody {
    Open {
        body: Body,
        tracker: Tracker,
        capture_error: bool,
    },
    Closed {
        body: Body,
    },
}

impl HttpBody for RecordedBody {
    type Data = Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let (frame, end) = match &mut *self {
            Self::Open {
                body,
                tracker,
                capture_error,
            } => {
                let frame = Pin::new(&mut *body).poll_frame(cx);
                let end = match &frame {
                    Poll::Ready(Some(Ok(frame))) => {
                        if *capture_error {
                            if let Some(data) = frame.data_ref() {
                                tracker.error_body(data);
                            }
                        }
                        body.is_end_stream().then_some(End::Completed)
                    }
                    Poll::Ready(Some(Err(e))) => Some(End::Failed {
                        code: "stream_read_error".into(),
                        source: errors::SRC_NETWORK,
                        message: Some(format!("response body error: {e}")),
                    }),
                    Poll::Ready(None) => Some(End::Completed),
                    Poll::Pending => None,
                };
                (frame, end)
            }
            Self::Closed { body } => return Pin::new(body).poll_frame(cx),
        };
        if let Some(end) = end {
            match std::mem::replace(
                &mut *self,
                Self::Closed {
                    body: Body::empty(),
                },
            ) {
                Self::Open { body, tracker, .. } => {
                    *self = Self::Closed { body };
                    tracker.finish(end);
                }
                closed => *self = closed,
            }
        }
        frame
    }
    fn is_end_stream(&self) -> bool {
        match self {
            Self::Open { .. } => false,
            Self::Closed { body } => body.is_end_stream(),
        }
    }
    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Open { body, .. } | Self::Closed { body } => body.size_hint(),
        }
    }
}

fn fail(trk: &mut Option<Tracker>, code: &str, source: &'static str, message: impl Into<String>) {
    if let Some(t) = trk.as_mut() {
        t.fail_msg(code, message);
        t.source(source);
    }
}
