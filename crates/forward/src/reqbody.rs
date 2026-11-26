//! Body bytes and compressed copies retain their budget reservation across retries.
use crate::timing;
use crate::AppState;
use axum::extract::{FromRequest, Request};
use axum::response::Response;
use bytes::Bytes;
use ciallopa_upstream::drain::BodyPermit;
use futures::StreamExt;
use http::StatusCode;
use std::sync::Arc;
use std::time::Duration;

/// Bounded wait for budget room before 503 `gateway_busy` (`Retry-After: 3`).
pub(crate) const BUDGET_WAIT: Duration = Duration::from_secs(1);
const DISCARD_TIME: Duration = Duration::from_secs(10);
const MIB: u64 = 1024 * 1024;

pub(crate) struct ReqBody {
    pub bytes: Bytes,
    pub started: Option<crate::usage::Started>,
    permit: Option<Arc<BodyPermit>>,
}

struct Owned<T> {
    data: T,
    _permit: Arc<BodyPermit>,
}
impl<T: AsRef<[u8]>> AsRef<[u8]> for Owned<T> {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

impl ReqBody {
    /// Bytes derived from this body (e.g. compressed) that keep its reservation alive.
    pub fn wrap(&self, data: Vec<u8>) -> Bytes {
        match &self.permit {
            Some(p) => Bytes::from_owner(Owned {
                data,
                _permit: p.clone(),
            }),
            None => Bytes::from(data),
        }
    }
}

fn too_large() -> Response {
    crate::errors::json_error(
        StatusCode::PAYLOAD_TOO_LARGE,
        "invalid_request_error",
        "Request body is too large.",
    )
}

fn unreadable() -> Response {
    crate::errors::json_error(
        StatusCode::BAD_REQUEST,
        "invalid_request_error",
        "Failed to read the request body.",
    )
}

/// Drain with a deadline so clients that finish writing before reading receive the error.
async fn discard(mut stream: axum::body::BodyDataStream, r: Response) -> Response {
    let _ = tokio::time::timeout(DISCARD_TIME, async {
        while let Some(Ok(_)) = stream.next().await {}
    })
    .await;
    r
}

/// Whole body up to `max` bytes, for routes outside the buffered-body budget (management and
/// usage APIs). Over the limit it answers 413 after [`discard`], like the forwarding routes.
pub async fn read_limited(
    headers: &http::HeaderMap,
    body: axum::body::Body,
    max: u64,
) -> Result<Bytes, Response> {
    let declared = headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok());
    let mut stream = body.into_data_stream();
    if declared.is_none_or(|n| n <= max) {
        let mut buf = Vec::with_capacity(declared.unwrap_or(0) as usize);
        loop {
            match stream.next().await {
                None => return Ok(buf.into()),
                Some(Ok(c)) if (buf.len() + c.len()) as u64 <= max => buf.extend_from_slice(&c),
                _ => break,
            }
        }
    }
    Err(discard(stream, too_large()).await)
}

/// Oversized WS messages reserve the whole budget so they can still fit.
pub(crate) async fn reserve_message(st: &AppState, n: usize) -> Option<BodyPermit> {
    let budget = st.config.load().max_buffered_body_mb.saturating_mul(MIB);
    let drain = &st.upstream.drain;
    let mut permit = drain.ws_permit();
    let n = n as u64;
    let want = if budget == 0 { n } else { n.min(budget) };
    drain
        .reserve_body(&mut permit, want, budget, timing::scale(st, BUDGET_WAIT))
        .await
        .ok()
        .map(|()| permit)
}

impl FromRequest<AppState> for ReqBody {
    type Rejection = Response;

    async fn from_request(req: Request, st: &AppState) -> Result<Self, Response> {
        let (max, budget) = {
            let c = st.config.load();
            (
                c.max_body_mb.saturating_mul(MIB),
                c.max_buffered_body_mb.saturating_mul(MIB),
            )
        };
        let declared = req
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok());
        let (mut parts, body) = req.into_parts();
        let started = parts.extensions.remove::<crate::usage::Started>();
        let mut stream = body.into_data_stream();
        let note = |n: u64| {
            crate::refusal::note(
                st,
                crate::refusal::Refusal {
                    kind: ciallopa_upstream::drain::RefusalKind::Body,
                    ws: false,
                    key: parts
                        .extensions
                        .get::<crate::ClientKey>()
                        .map(|k| &*k.label),
                    path: parts.uri.path(),
                    headers: &parts.headers,
                    model: None,
                    account: None,
                    limit: budget,
                    bytes: Some(n),
                },
            )
        };
        if declared.is_some_and(|n| n > max) {
            return Err(discard(stream, too_large()).await);
        }
        let drain = &st.upstream.drain;
        let mut permit = drain.body_permit();
        if let Some(n) = declared {
            if let Err(r) = drain
                .reserve_body(&mut permit, n, budget, timing::scale(st, BUDGET_WAIT))
                .await
            {
                note(n);
                return Err(discard(stream, crate::admission::refused(r)).await);
            }
        }
        let mut buf = Vec::with_capacity(declared.unwrap_or(0) as usize);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| unreadable())?;
            let len = (buf.len() + chunk.len()) as u64;
            // Release the buffered bytes and their reservation before draining the rest,
            // which can take up to `DISCARD_TIME`.
            if len > max {
                drop((buf, chunk, permit));
                return Err(discard(stream, too_large()).await);
            }
            if len > permit.held() {
                let more = len - permit.held();
                if let Err(r) = drain
                    .reserve_body(&mut permit, more, budget, timing::scale(st, BUDGET_WAIT))
                    .await
                {
                    drop((buf, chunk, permit));
                    note(len);
                    return Err(discard(stream, crate::admission::refused(r)).await);
                }
            }
            buf.extend_from_slice(&chunk);
        }
        if permit.held() == 0 {
            return Ok(Self {
                bytes: Bytes::from(buf),
                permit: None,
                started,
            });
        }
        let permit = Arc::new(permit);
        Ok(Self {
            bytes: Bytes::from_owner(Owned {
                data: buf,
                _permit: permit.clone(),
            }),
            permit: Some(permit),
            started,
        })
    }
}
