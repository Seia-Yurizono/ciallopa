//! The single writer thread: batch inserts + periodic retention purge.
use crate::{store, Counters, Msg, Record};
use rusqlite::Connection;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) const PURGE_CHUNK: u64 = 20_000;
const PURGE_EVERY: Duration = Duration::from_secs(3600);

pub(crate) struct Writer {
    pub conn: Connection,
    pub rx: Receiver<Msg>,
    pub batch_size: usize,
    pub flush: Duration,
    pub retention_days: Arc<std::sync::atomic::AtomicU32>,
    pub counters: Arc<Counters>,
    pub state: store::WriterState,
}

impl Writer {
    pub fn run(mut self) {
        let mut batch: Vec<Record> = Vec::with_capacity(self.batch_size);
        let mut acks = Vec::new();
        let mut next_purge = Instant::now();
        loop {
            let days = self.retention_days.load(Ordering::Relaxed);
            if days == 0 && Instant::now() >= next_purge {
                // Retention off (hot-reloadable): look again in a minute instead of spinning.
                next_purge = Instant::now() + Duration::from_secs(60);
            }
            if days > 0 && Instant::now() >= next_purge {
                // One bounded chunk per pass so a large backlog never stalls inserts.
                next_purge = if self.purge() {
                    Instant::now() + Duration::from_millis(5)
                } else {
                    Instant::now() + PURGE_EVERY
                };
            }
            // Wait for the first message of a batch (wake up for purges).
            let wait = next_purge
                .saturating_duration_since(Instant::now())
                .max(Duration::from_millis(1));
            let first = match self.rx.recv_timeout(wait) {
                Ok(m) => m,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return,
            };
            let mut stop = false;
            let mut quotas = vec![];
            let mut take = |m: Msg, batch: &mut Vec<Record>, acks: &mut Vec<_>| match m {
                Msg::Rec(r) => batch.push(*r),
                Msg::Quota(q) => quotas.push(*q),
                Msg::Flush(a) => acks.push(a),
                Msg::Stop => stop = true,
            };
            take(first, &mut batch, &mut acks);
            let deadline = Instant::now() + self.flush;
            while batch.len() < self.batch_size && acks.is_empty() {
                // Drain what is already queued without waiting, then wait up to the deadline.
                match self.rx.try_recv() {
                    Ok(m) => take(m, &mut batch, &mut acks),
                    Err(_) => {
                        let left = deadline.saturating_duration_since(Instant::now());
                        if left.is_zero() {
                            break;
                        }
                        match self.rx.recv_timeout(left) {
                            Ok(m) => take(m, &mut batch, &mut acks),
                            Err(_) => break,
                        }
                    }
                }
            }
            if stop {
                // Drain everything still queued.
                while let Ok(m) = self.rx.try_recv() {
                    match m {
                        Msg::Rec(r) => batch.push(*r),
                        Msg::Quota(q) => quotas.push(*q),
                        _ => {}
                    }
                }
            }
            if !batch.is_empty() {
                let n = batch.len() as u64;
                match store::insert_batch(&mut self.conn, &mut self.state, &batch) {
                    Ok(()) => {
                        self.counters.written.fetch_add(n, Ordering::Relaxed);
                    }
                    Err(e) => {
                        // Labels cache may reference rolled-back rows.
                        self.state.reset();
                        self.counters.write_errors.fetch_add(n, Ordering::Relaxed);
                        tracing::warn!(error = %e, rows = n, "usage batch write failed; rows dropped");
                    }
                }
                batch.clear();
            }
            if !quotas.is_empty() {
                if let Err(e) = store::insert_quota(&mut self.conn, &mut self.state, &quotas) {
                    self.state.reset();
                    tracing::warn!(error = %e, "quota snapshot write failed");
                }
            }
            for a in acks.drain(..) {
                let _ = a.send(());
            }
            if stop {
                let _ = self.conn.execute_batch("PRAGMA optimize;");
                return;
            }
        }
    }

    /// Returns true when more rows may remain.
    fn purge(&mut self) -> bool {
        let cutoff =
            crate::now_ms() - i64::from(self.retention_days.load(Ordering::Relaxed)) * 86_400_000;
        match store::purge_before(&mut self.conn, cutoff, PURGE_CHUNK) {
            Ok(n) => {
                if n > 0 {
                    self.counters.purged.fetch_add(n, Ordering::Relaxed);
                    tracing::debug!(rows = n, "usage retention purge chunk");
                }
                n >= PURGE_CHUNK
            }
            Err(e) => {
                tracing::warn!(error = %e, "usage purge failed");
                false
            }
        }
    }
}
