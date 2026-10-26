//! Forwarding admission and graceful shutdown.
//!
//! Upgraded WebSockets are detached from the HTTP server, so its graceful shutdown does not
//! wait for them. [`Drain`] tracks open sessions and tells them to stop: on
//! [`Drain::start`] an idle session closes at once (1000) and a session in a turn closes
//! after the turn's terminal event; [`Drain::force`] (after the drain bound) closes the rest
//! with 1001.
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Notify};

/// Drain phase broadcast to sessions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Phase {
    #[default]
    Running,
    /// Finish the running turn, then close normally.
    Draining,
    /// Drain bound reached: close now.
    Force,
}

#[derive(Default)]
pub struct Drain {
    phase: watch::Sender<Phase>,
    active: AtomicUsize,
    inflight: AtomicUsize,
    workers: AtomicUsize,
    /// Refusals per [`RefusalKind`] since start (in memory; persisted per minute by usage).
    rejected: [AtomicU64; 4],
    /// Admitted requests exempt from the inflight cap (model lists, quota polls).
    exempt: AtomicUsize,
    idle: Notify,
    /// Bytes of buffered HTTP request bodies currently reserved (see [`BodyPermit`]).
    body_bytes: AtomicU64,
    body_freed: Notify,
    /// Client WebSocket connections from handshake to close (see [`WsSlot`]).
    ws: AtomicUsize,
    /// Part of `body_bytes` held by client WebSocket messages.
    ws_bytes: AtomicU64,
}

/// A share of the global buffered-request-body budget. Dropping it returns the bytes.
pub struct BodyPermit {
    drain: Arc<Drain>,
    held: u64,
    /// Also counted in [`Drain::ws_bytes`].
    ws: bool,
}
impl BodyPermit {
    pub fn held(&self) -> u64 {
        self.held
    }
}
impl Drop for BodyPermit {
    fn drop(&mut self) {
        if self.held > 0 {
            self.drain.body_bytes.fetch_sub(self.held, Ordering::AcqRel);
            if self.ws {
                self.drain.ws_bytes.fetch_sub(self.held, Ordering::AcqRel);
            }
            self.drain.body_freed.notify_waiters();
        }
    }
}

/// Held by a session for its lifetime.
pub struct Session(Arc<Drain>);
impl Drop for Session {
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

/// One client WebSocket connection counted against `ws_max_connections`, from the
/// handshake until the relay ends.
pub struct WsSlot(Arc<Drain>);
impl Drop for WsSlot {
    fn drop(&mut self) {
        self.0.ws.fetch_sub(1, Ordering::AcqRel);
    }
}

/// A single admitted operation. Clones share one slot, including the HTTP → WS handoff.
#[derive(Clone)]
pub struct Lease {
    _inner: Arc<LeaseInner>,
}
struct LeaseInner {
    drain: Arc<Drain>,
    exempt: bool,
}
impl Drop for LeaseInner {
    fn drop(&mut self) {
        let n = if self.exempt {
            &self.drain.exempt
        } else {
            &self.drain.inflight
        };
        n.fetch_sub(1, Ordering::AcqRel);
        self.drain.idle.notify_waiters();
    }
}

/// A detached body producer must finish dropping its upstream and tracker before flush.
pub struct Worker(Arc<Drain>);
impl Drop for Worker {
    fn drop(&mut self) {
        self.0.workers.fetch_sub(1, Ordering::AcqRel);
        self.0.idle.notify_waiters();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    Busy,
    Draining,
}

/// Which limit refused an operation. Draining refusals are never counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefusalKind {
    /// `max_inflight_requests`.
    Inflight = 0,
    /// `ws_max_connections`.
    Ws = 1,
    /// `max_buffered_body_mb`, HTTP request body.
    Body = 2,
    /// `max_buffered_body_mb`, client WebSocket message.
    WsMsg = 3,
}
impl RefusalKind {
    pub const ALL: [Self; 4] = [Self::Inflight, Self::Ws, Self::Body, Self::WsMsg];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inflight => "inflight",
            Self::Ws => "ws",
            Self::Body => "body",
            Self::WsMsg => "ws_msg",
        }
    }
}

impl Drain {
    /// No queue. A lower hot limit affects the next operation; zero still counts occupancy.
    pub fn admit(self: &Arc<Self>, limit: usize) -> Result<Lease, Refused> {
        if self.phase() != Phase::Running {
            return Err(Refused::Draining);
        }
        let result = self
            .inflight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (limit == 0 || n < limit)
                    .then(|| n.checked_add(1))
                    .flatten()
            });
        if result.is_err() {
            self.count(RefusalKind::Inflight);
            return Err(Refused::Busy);
        }
        let lease = Lease {
            _inner: Arc::new(LeaseInner {
                drain: self.clone(),
                exempt: false,
            }),
        };
        // Close the race with shutdown starting between the phase check and increment.
        if self.phase() != Phase::Running {
            drop(lease);
            return Err(Refused::Draining);
        }
        Ok(lease)
    }
    /// Admit a light request (model list, quota poll) outside the inflight cap. It is still
    /// refused while draining and still waited for on shutdown.
    pub fn admit_exempt(self: &Arc<Self>) -> Result<Lease, Refused> {
        if self.phase() != Phase::Running {
            return Err(Refused::Draining);
        }
        self.exempt.fetch_add(1, Ordering::AcqRel);
        let lease = Lease {
            _inner: Arc::new(LeaseInner {
                drain: self.clone(),
                exempt: true,
            }),
        };
        if self.phase() != Phase::Running {
            drop(lease);
            return Err(Refused::Draining);
        }
        Ok(lease)
    }
    fn count(&self, kind: RefusalKind) {
        self.rejected[kind as usize].fetch_add(1, Ordering::Relaxed);
    }
    /// An empty budget share; grow it with [`Self::reserve_body`].
    pub fn body_permit(self: &Arc<Self>) -> BodyPermit {
        BodyPermit {
            drain: self.clone(),
            held: 0,
            ws: false,
        }
    }
    /// Like [`Self::body_permit`], for a client WebSocket message (also counted in
    /// [`Self::ws_bytes`]).
    pub fn ws_permit(self: &Arc<Self>) -> BodyPermit {
        BodyPermit {
            drain: self.clone(),
            held: 0,
            ws: true,
        }
    }
    /// Count a client WebSocket connection, at most `limit` at once (0 = unlimited). No
    /// queue; a refusal counts as [`Self::rejected`]. A lower hot limit only affects new
    /// connections.
    pub fn ws_slot(self: &Arc<Self>, limit: usize) -> Result<WsSlot, Refused> {
        let r = self
            .ws
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (limit == 0 || n < limit)
                    .then(|| n.checked_add(1))
                    .flatten()
            });
        if r.is_err() {
            self.count(RefusalKind::Ws);
            return Err(Refused::Busy);
        }
        Ok(WsSlot(self.clone()))
    }
    /// Client WebSocket connections currently counted (handshaking or open).
    pub fn ws_connections(&self) -> usize {
        self.ws.load(Ordering::Acquire)
    }
    /// Buffered body bytes currently held by client WebSocket messages.
    pub fn ws_bytes(&self) -> u64 {
        self.ws_bytes.load(Ordering::Acquire)
    }

    /// Grow `permit` by `n` bytes within `limit` total buffered body bytes (0 = unlimited),
    /// waiting at most `wait` for other bodies to be released. A share that can never fit
    /// (`held + n > limit`) is refused at once. Refusals count as [`Self::rejected`].
    pub async fn reserve_body(
        &self,
        permit: &mut BodyPermit,
        n: u64,
        limit: u64,
        wait: Duration,
    ) -> Result<(), Refused> {
        if n == 0 {
            return Ok(());
        }
        let fits = |c: u64| c.checked_add(n).filter(|&t| limit == 0 || t <= limit);
        let kind = if permit.ws {
            RefusalKind::WsMsg
        } else {
            RefusalKind::Body
        };
        if limit != 0 && permit.held.saturating_add(n) > limit {
            self.count(kind);
            return Err(Refused::Busy);
        }
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let freed = self.body_freed.notified();
            tokio::pin!(freed);
            freed.as_mut().enable();
            if self
                .body_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, fits)
                .is_ok()
            {
                permit.held += n;
                if permit.ws {
                    self.ws_bytes.fetch_add(n, Ordering::AcqRel);
                }
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, freed).await.is_err() {
                self.count(kind);
                return Err(Refused::Busy);
            }
        }
    }

    /// Buffered request body bytes currently reserved.
    pub fn body_bytes(&self) -> u64 {
        self.body_bytes.load(Ordering::Acquire)
    }
    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::Acquire)
    }
    /// All refusals since start (draining excluded).
    pub fn rejected(&self) -> u64 {
        self.rejected_by().iter().sum()
    }
    /// Refusals since start, indexed by [`RefusalKind`].
    pub fn rejected_by(&self) -> [u64; 4] {
        RefusalKind::ALL.map(|k| self.rejected[k as usize].load(Ordering::Relaxed))
    }
    /// Exempt requests currently admitted (see [`Self::admit_exempt`]).
    pub fn exempt(&self) -> usize {
        self.exempt.load(Ordering::Acquire)
    }
    pub fn worker(self: &Arc<Self>) -> Worker {
        self.workers.fetch_add(1, Ordering::AcqRel);
        Worker(self.clone())
    }
    /// Listen independently of downstream body polling/backpressure.
    pub async fn forced(&self) {
        let mut rx = self.watch();
        while *rx.borrow_and_update() != Phase::Force {
            if rx.changed().await.is_err() {
                return;
            }
        }
    }
    pub fn enter(self: &Arc<Self>) -> Session {
        self.active.fetch_add(1, Ordering::AcqRel);
        Session(self.clone())
    }
    pub fn watch(&self) -> watch::Receiver<Phase> {
        self.phase.subscribe()
    }
    pub fn phase(&self) -> Phase {
        *self.phase.borrow()
    }
    pub fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }
    pub fn start(&self) {
        self.phase.send_if_modified(|p| {
            let m = *p == Phase::Running;
            if m {
                *p = Phase::Draining;
            }
            m
        });
    }
    pub fn force(&self) {
        self.phase.send_replace(Phase::Force);
    }
    fn pending(&self) -> usize {
        self.active() + self.inflight() + self.exempt() + self.workers.load(Ordering::Acquire)
    }
    /// Wait for admitted requests, WS sessions and body producers, at most `bound`.
    /// The return value is remaining work, not a request count (a WS has both guards).
    pub async fn wait_idle(&self, bound: Duration) -> usize {
        let deadline = tokio::time::Instant::now() + bound;
        loop {
            let n = self.idle.notified();
            tokio::pin!(n);
            n.as_mut().enable();
            let left = self.pending();
            if left == 0 {
                return 0;
            }
            if tokio::time::timeout_at(deadline, n).await.is_err() {
                return self.pending();
            }
        }
    }
    /// Wait for normal completion, then cancel remaining HTTP work and close WS sessions.
    pub async fn shutdown(&self, bound: Duration, grace: Duration) -> usize {
        self.start();
        let left = self.wait_idle(bound).await;
        if left == 0 {
            return 0;
        }
        self.force();
        self.wait_idle(grace).await;
        left
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn body_budget_reserves_waits_and_releases() {
        let d = Arc::new(Drain::default());
        let wait = Duration::from_millis(50);
        let mut a = d.body_permit();
        d.reserve_body(&mut a, 600, 1000, wait).await.unwrap();
        d.reserve_body(&mut a, 100, 1000, wait).await.unwrap();
        assert_eq!((a.held(), d.body_bytes()), (700, 700));
        let mut b = d.body_permit();
        assert_eq!(
            d.reserve_body(&mut b, 400, 1000, wait).await,
            Err(Refused::Busy)
        );
        assert_eq!(d.rejected(), 1);
        let mut never = d.body_permit();
        let t = tokio::time::Instant::now();
        assert_eq!(
            d.reserve_body(&mut never, 1001, 1000, Duration::from_secs(5))
                .await,
            Err(Refused::Busy)
        );
        assert!(
            t.elapsed() < Duration::from_secs(1),
            "a share that cannot fit fails at once"
        );
        let d2 = d.clone();
        let waiter = tokio::spawn(async move {
            let mut p = d2.body_permit();
            d2.reserve_body(&mut p, 400, 1000, Duration::from_secs(5))
                .await
                .map(|()| p.held())
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(a);
        assert_eq!(waiter.await.unwrap(), Ok(400));
        assert_eq!(d.body_bytes(), 0, "every permit returned its bytes");
        let mut u = d.body_permit();
        d.reserve_body(&mut u, u64::MAX / 2, 0, wait).await.unwrap();
        assert_eq!(d.body_bytes(), u64::MAX / 2, "0 = unlimited still counts");
    }

    #[tokio::test]
    async fn ws_slots_and_bytes_are_counted_and_released() {
        let d = Arc::new(Drain::default());
        let a = d.ws_slot(2).unwrap();
        let b = d.ws_slot(2).unwrap();
        assert_eq!(d.ws_slot(2).err(), Some(Refused::Busy));
        assert_eq!((d.ws_connections(), d.rejected()), (2, 1));
        drop(a);
        let c = d.ws_slot(2).unwrap();
        let unlimited: Vec<_> = (0..5).map(|_| d.ws_slot(0).unwrap()).collect();
        assert_eq!(d.ws_connections(), 7);
        drop((b, c, unlimited));
        assert_eq!(d.ws_connections(), 0);
        let mut m = d.ws_permit();
        let mut h = d.body_permit();
        d.reserve_body(&mut m, 300, 0, Duration::ZERO)
            .await
            .unwrap();
        d.reserve_body(&mut h, 200, 0, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!((d.body_bytes(), d.ws_bytes()), (500, 300));
        drop(m);
        assert_eq!((d.body_bytes(), d.ws_bytes()), (200, 0));
    }

    #[tokio::test]
    async fn waits_for_sessions_and_forces_after_bound() {
        let d = Arc::new(Drain::default());
        assert_eq!(d.wait_idle(Duration::from_millis(10)).await, 0);
        let s = d.enter();
        let mut w = d.watch();
        let d2 = d.clone();
        let h = tokio::spawn(async move {
            d2.shutdown(Duration::from_millis(50), Duration::from_secs(1))
                .await
        });
        w.changed().await.unwrap();
        assert_eq!(*w.borrow_and_update(), Phase::Draining);
        w.changed().await.unwrap();
        assert_eq!(*w.borrow_and_update(), Phase::Force);
        drop(s);
        assert_eq!(h.await.unwrap(), 1, "one session needed the forced close");
        assert_eq!(d.active(), 0);

        let d = Arc::new(Drain::default());
        let s = d.enter();
        let d2 = d.clone();
        let h = tokio::spawn(async move {
            d2.shutdown(Duration::from_secs(5), Duration::from_secs(1))
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(d.phase(), Phase::Draining);
        drop(s);
        assert_eq!(h.await.unwrap(), 0);
        assert_eq!(
            d.phase(),
            Phase::Draining,
            "no forced close when sessions finish in time"
        );
    }
}
