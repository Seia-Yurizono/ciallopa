//! Buffered HTTP request bodies, bounded per request (`max_body_mb`) and in total
//! (`max_buffered_body_mb`).
//!
//! A body reserves its declared `Content-Length` before any byte is read (chunked bodies
//! reserve as they arrive), waiting at most [`BUDGET_WAIT`] for room. The returned bytes own
//! the reservation, so it lasts as long as any clone (retries, in-stream replay) or a
//! compressed copy made with [`ReqBody::wrap`]; the JSON inspected from it lives inside it.
use crate::AppState;
use axum::extract::{FromRequest, Request};
use axum::response::Response;
use bytes::Bytes;
use ciallopa_upstream::drain::BodyPermit;
use futures::StreamExt;
use http::StatusCode;
use std::sync::Arc;
use std::time::Duration;

/// Bounded wait for budget room before 503 `gateway_busy` (`Retry-After: 1`).
pub(crate) const BUDGET_WAIT: Duration = Duration::from_secs(1);
/// Upper bound for [`discard`].
const DISCARD_TIME: Duration = Duration::from_secs(10);
const MIB: u64 = 1024 * 1024;

pub(crate) struct ReqBody {
    pub bytes: Bytes,
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
    crate::proxy::json_error(
        StatusCode::PAYLOAD_TOO_LARGE,
        "invalid_request_error",
        "Request body is too large.",
    )
}

fn unreadable() -> Response {
    crate::proxy::json_error(
        StatusCode::BAD_REQUEST,
        "invalid_request_error",
        "Failed to read the request body.",
    )
}

/// Read and drop what the client is still sending (bounded by [`DISCARD_TIME`]) before
/// answering, so a client that writes its whole body first sees the response instead of
/// a reset connection (nginx's lingering close). Nothing is kept in memory.
async fn discard(mut stream: axum::body::BodyDataStream, r: Response) -> Response {
    let _ = tokio::time::timeout(DISCARD_TIME, async {
        while let Some(Ok(_)) = stream.next().await {}
    })
    .await;
    r
}

/// Reserve a WebSocket client message that is already buffered (tungstenite reads whole
/// messages) in the same budget, with the same bounded wait. A message larger than the whole
/// budget waits for all of it instead of never fitting, so WS message limits stay unchanged.
/// `None` = still no room after [`BUDGET_WAIT`] (counted as rejected).
pub(crate) async fn reserve_message(st: &AppState, n: usize) -> Option<BodyPermit> {
    let budget = st.config.load().max_buffered_body_mb.saturating_mul(MIB);
    let drain = &st.upstream.drain;
    let mut permit = drain.body_permit();
    let n = n as u64;
    let want = if budget == 0 { n } else { n.min(budget) };
    drain
        .reserve_body(&mut permit, want, budget, BUDGET_WAIT)
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
        let mut stream = req.into_body().into_data_stream();
        if declared.is_some_and(|n| n > max) {
            return Err(discard(stream, too_large()).await);
        }
        let drain = &st.upstream.drain;
        let mut permit = drain.body_permit();
        if let Some(n) = declared {
            if let Err(r) = drain
                .reserve_body(&mut permit, n, budget, BUDGET_WAIT)
                .await
            {
                return Err(discard(stream, crate::admission::refused(r)).await);
            }
        }
        let mut buf = Vec::with_capacity(declared.unwrap_or(0) as usize);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| unreadable())?;
            let len = (buf.len() + chunk.len()) as u64;
            if len > max {
                drop(buf);
                return Err(discard(stream, too_large()).await);
            }
            if len > permit.held() {
                let more = len - permit.held();
                if let Err(r) = drain
                    .reserve_body(&mut permit, more, budget, BUDGET_WAIT)
                    .await
                {
                    drop(buf);
                    return Err(discard(stream, crate::admission::refused(r)).await);
                }
            }
            buf.extend_from_slice(&chunk);
        }
        if permit.held() == 0 {
            return Ok(Self {
                bytes: Bytes::from(buf),
                permit: None,
            });
        }
        let permit = Arc::new(permit);
        Ok(Self {
            bytes: Bytes::from_owner(Owned {
                data: buf,
                _permit: permit.clone(),
            }),
            permit: Some(permit),
        })
    }
}
