//! Gateway downstream observation (see `ciallopa_usage::Observation`).
//!
//! SSE: `ObsTx` times each chunk entering the bounded body queue (`queue`), and the body
//! stream records when hyper polls a chunk out of it (`yield`). Hyper's socket writes have
//! no hook, so a yield is "handed to hyper", not "written" or "received".
//! WS: `timed` wraps `sink.send` (poll_ready + start_send + flush); completion means the
//! frame was written and tungstenite's flush returned, i.e. accepted by the OS send buffer.
use axum::body::Body;
use bytes::Bytes;
use ciallopa_usage::{Record, Recorder};
use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant};

/// Run `f`; also report how long it stayed Pending (first Pending → Ready), or None when it
/// completed on its first poll.
pub(crate) async fn timed<F: Future>(f: F) -> (F::Output, Option<Duration>) {
    let mut f = pin!(f);
    let mut blocked: Option<Instant> = None;
    let out = std::future::poll_fn(|cx| match f.as_mut().poll(cx) {
        Poll::Ready(v) => Poll::Ready(v),
        Poll::Pending => {
            blocked.get_or_insert_with(Instant::now);
            Poll::Pending
        }
    })
    .await;
    (out, blocked.map(|t| t.elapsed()))
}

pub(crate) fn ms_since(start: Instant, at: Instant) -> u32 {
    at.saturating_duration_since(start)
        .as_millis()
        .min(u128::from(u32::MAX)) as u32
}

pub(crate) fn us(d: Duration) -> u64 {
    d.as_micros().min(u128::from(u64::MAX)) as u64
}

/// Shared between one SSE request's producer, its tracker and its response body.
pub(crate) struct DownLink {
    start: Instant,
    s: Mutex<LinkState>,
}

#[derive(Default)]
struct LinkState {
    body_done: bool,
    parked: Option<(Record, Arc<Recorder>)>,
    first_queue: Option<u32>,
    last_queue: Option<u32>,
    queue_blocked_us: u64,
    queue_blocked_max_us: u64,
    queue_chunks: u32,
    queue_bytes: u64,
    first_yield: Option<u32>,
    last_yield: Option<u32>,
    yield_chunks: u32,
    yield_bytes: u64,
    incomplete: bool,
}

impl DownLink {
    pub(crate) fn new(start: Instant) -> Arc<Self> {
        Arc::new(Self {
            start,
            s: Mutex::new(LinkState::default()),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LinkState> {
        self.s.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn queued(&self, bytes: usize, blocked: Option<Duration>, ok: bool) {
        let now = ms_since(self.start, Instant::now());
        let mut s = self.lock();
        if !ok {
            s.incomplete = true;
            return;
        }
        s.first_queue.get_or_insert(now);
        s.last_queue = Some(now);
        let b = blocked.map(us).unwrap_or(0);
        s.queue_blocked_us = s.queue_blocked_us.saturating_add(b);
        s.queue_blocked_max_us = s.queue_blocked_max_us.max(b);
        s.queue_chunks = s.queue_chunks.saturating_add(1);
        s.queue_bytes = s.queue_bytes.saturating_add(bytes as u64);
    }

    fn yielded(&self, bytes: usize) {
        let now = ms_since(self.start, Instant::now());
        let mut s = self.lock();
        s.first_yield.get_or_insert(now);
        s.last_yield = Some(now);
        s.yield_chunks = s.yield_chunks.saturating_add(1);
        s.yield_bytes = s.yield_bytes.saturating_add(bytes as u64);
    }

    /// The response body ended (`eof`) or was dropped before its end (client gone or
    /// hyper stopped polling): emit a parked record.
    fn body_done(&self, eof: bool) {
        let parked = {
            let mut s = self.lock();
            if s.body_done {
                return;
            }
            s.body_done = true;
            s.incomplete |= !eof;
            s.parked.take().map(|(mut r, sink)| {
                merge(&s, &mut r);
                (r, sink)
            })
        };
        if let Some((r, sink)) = parked {
            sink.record(r);
        }
    }

    /// Called from the tracker's Drop with the finished record. `duration_ms` is already
    /// final; only the downstream fields may still change, so the record waits for the
    /// body unless it already ended or the process is force-draining.
    pub(crate) fn finish(&self, mut rec: Record, sink: Arc<Recorder>, now: bool) {
        let mut s = self.lock();
        if s.body_done || now {
            merge(&s, &mut rec);
            drop(s);
            sink.record(rec);
        } else {
            s.parked = Some((rec, sink));
        }
    }
}

fn merge(s: &LinkState, rec: &mut Record) {
    if s.queue_chunks == 0 && s.yield_chunks == 0 && !s.incomplete {
        return;
    }
    let o = rec.obs.get_or_insert_with(Default::default);
    let n = |v: u32| (v > 0).then_some(v);
    o.sse_first_queue_ms = s.first_queue;
    o.sse_last_queue_ms = s.last_queue;
    o.sse_queue_chunks = n(s.queue_chunks);
    o.sse_queue_bytes = (s.queue_chunks > 0).then_some(s.queue_bytes);
    o.sse_queue_blocked_us = (s.queue_chunks > 0).then_some(s.queue_blocked_us);
    o.sse_queue_blocked_max_us = (s.queue_chunks > 0).then_some(s.queue_blocked_max_us);
    o.sse_first_yield_ms = s.first_yield;
    o.sse_last_yield_ms = s.last_yield;
    o.sse_yield_chunks = n(s.yield_chunks);
    o.sse_yield_bytes = (s.yield_chunks > 0).then_some(s.yield_bytes);
    o.down_incomplete = Some(s.incomplete);
}

impl Drop for DownLink {
    fn drop(&mut self) {
        // Last reference gone with a record still parked (body never polled to the end
        // and never dropped separately): emit it rather than lose the row.
        let s = self.s.get_mut().unwrap_or_else(|e| e.into_inner());
        if let Some((mut r, sink)) = s.parked.take() {
            merge(s, &mut r);
            sink.record(r);
        }
    }
}

pub(crate) type Item = Result<Bytes, std::io::Error>;

/// The producer's side of the SSE body queue; same `send` shape as `mpsc::Sender`.
pub(crate) struct ObsTx {
    tx: tokio::sync::mpsc::Sender<Item>,
    link: Option<Arc<DownLink>>,
}

impl ObsTx {
    pub(crate) async fn send(
        &self,
        v: Item,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<Item>> {
        let Some(link) = &self.link else {
            return self.tx.send(v).await;
        };
        let n = v.as_ref().map(|b| b.len()).ok();
        let (r, blocked) = timed(self.tx.send(v)).await;
        if let Some(n) = n {
            link.queued(n, blocked, r.is_ok());
        }
        r
    }
}

struct BodyEnd(Arc<DownLink>);
impl Drop for BodyEnd {
    fn drop(&mut self) {
        self.0.body_done(false);
    }
}

/// `proxy::async_stream` with downstream observation when `link` is set.
pub(crate) fn observed<F, Fut>(
    drain: Arc<ciallopa_upstream::drain::Drain>,
    link: Option<Arc<DownLink>>,
    f: F,
) -> Body
where
    F: FnOnce(ObsTx) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let closed = tx.clone();
    let produce = f(ObsTx {
        tx,
        link: link.clone(),
    });
    let worker = drain.worker();
    tokio::spawn(async move {
        let _worker = worker;
        tokio::select! {
            biased;
            _ = drain.forced() => {}
            _ = closed.closed() => {}
            _ = produce => {}
        }
    });
    let end = link.map(BodyEnd);
    Body::from_stream(futures::stream::poll_fn(move |cx| {
        let r = rx.poll_recv(cx);
        if let Some(e) = &end {
            match &r {
                Poll::Ready(Some(Ok(b))) => e.0.yielded(b.len()),
                Poll::Ready(None) => e.0.body_done(true),
                _ => {}
            }
        }
        r
    }))
}

/// Upstream event classes for the first-event times.
pub(crate) fn is_text_type(ty: &str) -> bool {
    matches!(ty, "response.output_text.delta" | "response.refusal.delta")
}
pub(crate) fn is_reasoning_type(ty: &str) -> bool {
    ty.starts_with("response.reasoning") && ty.ends_with(".delta")
}
pub(crate) fn is_tool_type(ty: &str) -> bool {
    ty.starts_with("response.") && ty.ends_with(".delta") && ty.contains("_call")
}

/// Fill the per-leg permessage-deflate negotiation fields from an answered
/// `Sec-WebSocket-Extensions` value (None = not agreed).
pub(crate) fn negotiation(answer: Option<&str>) -> (Option<bool>, Option<String>) {
    let ext = answer.and_then(ciallopa_usage::observation::clean_ext);
    let on = ext
        .as_deref()
        .is_some_and(|e| e.starts_with("permessage-deflate"));
    (Some(on), ext)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes() {
        assert!(is_text_type("response.output_text.delta"));
        assert!(is_text_type("response.refusal.delta"));
        assert!(!is_text_type("response.output_text.done"));
        assert!(is_reasoning_type("response.reasoning_summary_text.delta"));
        assert!(is_reasoning_type("response.reasoning_text.delta"));
        assert!(!is_reasoning_type("response.reasoning_summary_part.added"));
        assert!(is_tool_type("response.function_call_arguments.delta"));
        assert!(is_tool_type("response.custom_tool_call_input.delta"));
        assert!(!is_tool_type("response.output_text.delta"));
        assert!(!is_tool_type("codex.tool_call.delta"));
    }

    #[test]
    fn negotiation_fields() {
        assert_eq!(negotiation(None), (Some(false), None));
        assert_eq!(
            negotiation(Some("permessage-deflate; client_max_window_bits=15")),
            (
                Some(true),
                Some("permessage-deflate; client_max_window_bits=15".into())
            )
        );
        assert_eq!(
            negotiation(Some("evil\r\nx")),
            (Some(false), Some("unrecognized".into()))
        );
    }

    struct Tmp(std::path::PathBuf);
    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn recorder(tag: &str) -> (Tmp, Arc<Recorder>) {
        let dir = std::env::temp_dir().join(format!(
            "cpa-obs-{tag}-{}-{:?}",
            std::process::id(),
            Instant::now()
        ));
        let r = Recorder::open(&ciallopa_core::config::UsageConfig {
            path: dir.join("u.db"),
            ..Default::default()
        })
        .unwrap();
        (Tmp(dir), r)
    }
    async fn obs(r: &Recorder, id: i64) -> Option<ciallopa_usage::Observation> {
        r.flush_blocking().unwrap();
        r.observation(&format!("{id:016x}"))
            .await
            .unwrap()
            .map(|o| o.obs)
    }
    fn chunks(
        n: usize,
    ) -> impl FnOnce(ObsTx) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> {
        move |tx| {
            Box::pin(async move {
                for _ in 0..n {
                    if tx
                        .send(Ok(Bytes::from_static(b"0123456789")))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            })
        }
    }

    /// Slow consumer: the 16-slot queue fills, sends block; the record waits for the body
    /// end and then carries complete queue + yield stats.
    #[tokio::test]
    async fn slow_consumer_blocks_queue_and_parks_record() {
        use futures::StreamExt;
        let (_d, r) = recorder("slow");
        let link = DownLink::new(Instant::now());
        let body = observed(Arc::default(), Some(link.clone()), chunks(40));
        let id = r.ids.next();
        let mut rec = Record::new(id, ciallopa_usage::Transport::Sse, "responses");
        rec.status = 200;
        tokio::time::sleep(Duration::from_millis(30)).await; // producer fills the queue
                                                             // Tracker finished first (like a terminal event before the client drained it).
        link.finish(rec, r.clone(), false);
        drop(link);
        assert!(obs(&r, id).await.is_none(), "parked until the body ends");
        let mut s = body.into_data_stream();
        let mut n = 0;
        while let Some(c) = s.next().await {
            n += c.unwrap().len();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert_eq!(n, 400);
        let o = obs(&r, id).await.expect("emitted at body EOF");
        assert_eq!(o.sse_queue_chunks, Some(40));
        assert_eq!(o.sse_queue_bytes, Some(400));
        assert_eq!(o.sse_yield_chunks, Some(40));
        assert_eq!(o.sse_yield_bytes, Some(400));
        assert!(o.sse_queue_blocked_us.unwrap() > 0, "{o:?}");
        assert!(o.sse_queue_blocked_max_us <= o.sse_queue_blocked_us);
        assert!(o.sse_first_yield_ms <= o.sse_last_yield_ms);
        assert_eq!(o.down_incomplete, Some(false));
        r.shutdown();
    }

    /// Client gone (body dropped) before the tracker finished: the producer's next send
    /// fails, and the record is emitted immediately when the tracker finishes.
    #[tokio::test]
    async fn dropped_body_marks_failed_send_and_emits() {
        let (_d, r) = recorder("drop");
        let link = DownLink::new(Instant::now());
        let body = observed(Arc::default(), Some(link.clone()), chunks(40));
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(body);
        tokio::time::sleep(Duration::from_millis(30)).await;
        let id = r.ids.next();
        link.finish(
            Record::new(id, ciallopa_usage::Transport::Sse, "responses"),
            r.clone(),
            false,
        );
        let o = obs(&r, id).await.expect("emitted at once");
        assert_eq!(o.sse_yield_chunks, None, "nothing was handed to hyper");
        assert_eq!(o.sse_queue_chunks, Some(16));
        assert_eq!(o.down_incomplete, Some(true));
        r.shutdown();
    }

    /// Force drain: emit without waiting for the body.
    #[tokio::test]
    async fn force_emits_without_body_end() {
        let (_d, r) = recorder("force");
        let link = DownLink::new(Instant::now());
        let _body = observed(Arc::default(), Some(link.clone()), chunks(1));
        tokio::time::sleep(Duration::from_millis(20)).await;
        let id = r.ids.next();
        link.finish(
            Record::new(id, ciallopa_usage::Transport::Sse, "responses"),
            r.clone(),
            true,
        );
        let o = obs(&r, id).await.expect("emitted at once");
        assert_eq!(o.sse_queue_chunks, Some(1));
        assert_eq!(o.sse_yield_chunks, None);
        r.shutdown();
    }

    #[tokio::test]
    async fn timed_reports_only_pending_time() {
        let (r, b) = timed(async { 1 }).await;
        assert_eq!((r, b), (1, None));
        let (_, b) = timed(tokio::time::sleep(Duration::from_millis(20))).await;
        assert!(b.unwrap() >= Duration::from_millis(15));
    }
}
