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
    rejected: AtomicU64,
    idle: Notify,
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

/// A single admitted operation. Clones share one slot, including the HTTP → WS handoff.
#[derive(Clone)]
pub struct Lease {
    _inner: Arc<LeaseInner>,
}
struct LeaseInner(Arc<Drain>);
impl Drop for LeaseInner {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, Ordering::AcqRel);
        self.0.idle.notify_waiters();
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
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(Refused::Busy);
        }
        let lease = Lease {
            _inner: Arc::new(LeaseInner(self.clone())),
        };
        // Close the race with shutdown starting between the phase check and increment.
        if self.phase() != Phase::Running {
            drop(lease);
            return Err(Refused::Draining);
        }
        Ok(lease)
    }
    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::Acquire)
    }
    pub fn rejected(&self) -> u64 {
        self.rejected.load(Ordering::Relaxed)
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
        self.active() + self.inflight() + self.workers.load(Ordering::Acquire)
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
