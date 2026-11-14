//! The single writer thread: batch inserts + periodic retention purge.
use crate::{write, Counters, Msg, QuotaSnapshot, Record};
use rusqlite::{Connection, Transaction};
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) const PURGE_CHUNK: u64 = 20_000;
const PURGE_EVERY: Duration = Duration::from_secs(3600);

enum PurgeState {
    Idle(Instant),
    Backlog(Instant),
    Disabled(Instant),
}
impl PurgeState {
    fn deadline(&self) -> Instant {
        match self {
            Self::Idle(t) | Self::Backlog(t) | Self::Disabled(t) => *t,
        }
    }
    fn tick(&mut self, days: u32, mut purge: impl FnMut() -> bool) -> Instant {
        let now = Instant::now();
        if now >= self.deadline() {
            *self = if days == 0 {
                Self::Disabled(now + Duration::from_secs(60))
            } else if purge() {
                Self::Backlog(Instant::now() + Duration::from_millis(5))
            } else {
                Self::Idle(Instant::now() + PURGE_EVERY)
            };
        }
        self.deadline()
    }
}

struct Batch {
    rows: Vec<Record>,
    quotas: Vec<QuotaSnapshot>,
    acks: Vec<SyncSender<()>>,
    stop: bool,
}
impl Batch {
    fn take(&mut self, msg: Msg) {
        match msg {
            Msg::Rec(row) => self.rows.push(*row),
            Msg::Quota(q) => self.quotas.push(*q),
            Msg::Flush(ack) => self.acks.push(ack),
            Msg::Stop => self.stop = true,
        }
    }
}

pub(crate) struct Writer {
    pub conn: Connection,
    pub rx: Receiver<Msg>,
    pub batch_size: usize,
    pub flush: Duration,
    pub retention_days: Arc<std::sync::atomic::AtomicU32>,
    pub counters: Arc<Counters>,
    pub state: write::WriterState,
    pub refusals: Arc<crate::refusals::Aggregator>,
    pub occupancy: Arc<crate::occupancy::Aggregator>,
    pub prices: Arc<arc_swap::ArcSwap<crate::PriceTable>>,
}

impl Writer {
    pub fn run(mut self) {
        let mut purge = PurgeState::Idle(Instant::now());
        let mut next_refusals = Instant::now() + crate::refusals::FLUSH_EVERY;
        loop {
            if Instant::now() >= next_refusals {
                self.flush_refusals();
                self.flush_occupancy(false);
                next_refusals = Instant::now() + crate::refusals::FLUSH_EVERY;
            }
            // Invariant: the first purge chunk runs before receiving or acknowledging Flush.
            let days = self.retention_days.load(Ordering::Relaxed);
            let next_purge = purge.tick(days, || self.purge(days));
            let wait = next_purge
                .min(next_refusals)
                .saturating_duration_since(Instant::now())
                .max(Duration::from_millis(1));
            let first = match self.rx.recv_timeout(wait) {
                Ok(msg) => msg,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return,
            };
            let batch = self.collect_batch(first);
            if !batch.rows.is_empty() || !batch.quotas.is_empty() {
                let n = batch.rows.len() as u64;
                let prices = self.prices.load_full();
                let result = self.in_tx(|tx, state| {
                    if !batch.rows.is_empty() {
                        write::insert_batch(tx, state, &batch.rows, &prices)?;
                    }
                    if !batch.quotas.is_empty() {
                        write::insert_quota(tx, state, &batch.quotas, &prices)?;
                    }
                    Ok(())
                });
                match result {
                    Ok(()) => {
                        self.counters.written.fetch_add(n, Ordering::Relaxed);
                    }
                    Err(error) => {
                        self.counters.write_errors.fetch_add(n, Ordering::Relaxed);
                        tracing::warn!(%error, rows=n, quotas=batch.quotas.len(), "usage batch write failed; rows dropped");
                    }
                }
            }
            if batch.stop || !batch.acks.is_empty() {
                self.flush_refusals();
                self.flush_occupancy(true);
            }
            for ack in batch.acks {
                let _ = ack.send(());
            }
            if batch.stop {
                let _ = self.conn.execute_batch("PRAGMA optimize");
                return;
            }
        }
    }

    fn collect_batch(&self, first: Msg) -> Batch {
        let mut batch = Batch {
            rows: Vec::with_capacity(self.batch_size),
            quotas: Vec::new(),
            acks: Vec::new(),
            stop: false,
        };
        batch.take(first);
        let deadline = Instant::now() + self.flush;
        while !batch.stop
            && batch.rows.len() + batch.quotas.len() < self.batch_size
            && batch.acks.is_empty()
        {
            match self.rx.try_recv() {
                Ok(msg) => batch.take(msg),
                Err(_) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        break;
                    }
                    match self.rx.recv_timeout(left) {
                        Ok(msg) => batch.take(msg),
                        Err(_) => break,
                    }
                }
            }
        }
        if batch.stop {
            while let Ok(msg) = self.rx.try_recv() {
                batch.take(msg);
            }
        }
        batch
    }

    fn in_tx<T>(
        &mut self,
        f: impl FnOnce(&Transaction<'_>, &mut write::WriterState) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let result = (|| {
            // IMMEDIATE: the first statement reads `data_version`; a deferred read that later
            // upgrades to a write fails at once (no busy wait) if another connection wrote.
            let tx = self
                .conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            self.state.refresh(&tx)?;
            let value = f(&tx, &mut self.state)?;
            tx.commit()?;
            Ok(value)
        })();
        if result.is_err() {
            self.state.reset();
        }
        result
    }

    fn flush_refusals(&mut self) {
        if self.refusals.is_empty() {
            return;
        }
        let rows = self.refusals.take();
        if let Err(error) =
            self.in_tx(|tx, state| crate::refusals::upsert_refusals(tx, state, &rows))
        {
            tracing::warn!(%error, groups=rows.len(), "refusal write failed; retrying later");
            self.refusals.restore(rows);
        }
    }
    fn flush_occupancy(&mut self, all: bool) {
        let rows = self.occupancy.take((!all).then(crate::now_ms));
        if rows.is_empty() {
            return;
        }
        if let Err(error) =
            self.in_tx(|tx, state| crate::occupancy::upsert_occupancy(tx, state, &rows))
        {
            tracing::warn!(%error, minutes=rows.len(), "occupancy write failed; retrying later");
            self.occupancy.restore(rows);
        }
    }
    fn purge(&mut self, days: u32) -> bool {
        let cutoff = crate::now_ms() - i64::from(days) * 86_400_000;
        match write::purge_step(&mut self.conn, cutoff, PURGE_CHUNK) {
            Ok(progress) => {
                self.state.reset(); // A finished purge can remove cached dictionary ids.
                self.counters
                    .purged
                    .fetch_add(progress.removed, Ordering::Relaxed);
                if !progress.more {
                    let _ = self.conn.execute_batch("PRAGMA optimize");
                }
                progress.more
            }
            Err(error) => {
                tracing::warn!(%error,"usage purge failed");
                false
            }
        }
    }
}
