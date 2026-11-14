//! OpenAI Images API on top of Codex, mirroring CPA `executor/codex_openai_images.go`:
//! (a) gpt-image-* models → direct `/codex/images/{generations,edits}` (multipart edits → JSON);
//! (b) anything else → `/codex/responses` with the `image_generation` tool, results reassembled.
mod request;
mod wrapper;

use self::request::Kind;
use self::wrapper::run;
use crate::{errors, usage::Tracker, AppState};
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
        t.finish_response();
        return Response::from_parts(parts, body);
    }
    Response::from_parts(
        parts,
        Body::new(RecordedBody {
            body,
            tracker: Some(t),
            capture_error,
        }),
    )
}

/// Ready-made JSON/errors have no detached producer to own their tracker.
struct RecordedBody {
    body: Body,
    tracker: Option<Tracker>,
    capture_error: bool,
}

impl HttpBody for RecordedBody {
    type Data = Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let frame = Pin::new(&mut self.body).poll_frame(cx);
        match &frame {
            Poll::Ready(Some(Ok(frame))) if self.capture_error => {
                if let (Some(t), Some(data)) = (self.tracker.as_mut(), frame.data_ref()) {
                    t.error_body(data);
                }
            }
            Poll::Ready(Some(Err(e))) => {
                fail(
                    &mut self.tracker,
                    "stream_read_error",
                    errors::SRC_NETWORK,
                    format!("response body error: {e}"),
                );
                self.tracker.take();
            }
            Poll::Ready(None) => {
                if let Some(mut t) = self.tracker.take() {
                    t.finish_response();
                }
            }
            _ => {}
        }
        if matches!(&frame, Poll::Ready(Some(Ok(_)))) && self.body.is_end_stream() {
            if let Some(mut t) = self.tracker.take() {
                t.finish_response();
            }
        }
        frame
    }
    fn is_end_stream(&self) -> bool {
        self.tracker.is_none() && self.body.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

fn fail(trk: &mut Option<Tracker>, code: &str, source: &'static str, message: impl Into<String>) {
    if let Some(t) = trk.as_mut() {
        t.fail_msg(code, message);
        t.source(source);
    }
}
