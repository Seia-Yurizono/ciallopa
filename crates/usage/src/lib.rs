//! Per-request usage log. **Never blocks forwarding**: `Recorder::record` is a non-blocking
//! `try_send` into a bounded queue (full ⇒ drop + count); one dedicated OS thread batch-writes
//! to SQLite (WAL). Reads open their own read-only connections. Design: docs/USAGE.md.
mod store;
mod writer;

pub use store::{RecentQuery, RecordRow, Summary, SummaryGroup, SummaryQuery};

use cpa_lite_core::config::UsageConfig;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Bits of the request id below the millisecond timestamp (per-ms sequence).
pub const SEQ_BITS: u32 = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Sse = 1,
    Ws = 2,
    Http = 3,
}
impl Transport {
    pub fn as_str(self) -> &'static str {
        match self {
            Transport::Sse => "sse",
            Transport::Ws => "ws",
            Transport::Http => "http",
        }
    }
    pub(crate) fn from_i64(v: i64) -> &'static str {
        match v {
            1 => "sse",
            2 => "ws",
            _ => "http",
        }
    }
}

/// One finished request (HTTP) or turn (WS `response.create` → terminal event).
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    /// Time-ordered unique id: `start_ms << 20 | seq` (see [`RequestIds`]). Also the SQLite
    /// rowid, so time ranges are rowid range scans and duplicates are ignored on insert.
    pub id: i64,
    /// Account that served the final attempt.
    pub account: Option<String>,
    /// First account tried, only when different from `account` (failover).
    pub first_account: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub transport: Transport,
    /// `responses`, `compact`, `images`, …
    pub endpoint: &'static str,
    /// HTTP status sent to the client (WS turns: 200, or the handshake status).
    pub status: u16,
    /// Short machine-readable kind (`usage_limit`, `idle_timeout`, `client_closed`, …).
    pub error: Option<String>,
    /// Pre-first-byte retries (transport / 408 / 5xx).
    pub retries: u32,
    /// Account switches (401/403/404/429 failover + quota-before-output replay).
    pub switches: u32,
    pub ttft_ms: Option<u32>,
    pub duration_ms: u32,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
}
impl Record {
    pub fn new(id: i64, transport: Transport, endpoint: &'static str) -> Self {
        Self {
            id,
            account: None,
            first_account: None,
            model: None,
            effort: None,
            transport,
            endpoint,
            status: 0,
            error: None,
            retries: 0,
            switches: 0,
            ttft_ms: None,
            duration_ms: 0,
            input_tokens: None,
            output_tokens: None,
            cached_tokens: None,
            reasoning_tokens: None,
        }
    }
    pub fn ts_ms(&self) -> i64 {
        self.id >> SEQ_BITS
    }
}

/// Generates time-ordered unique ids (monotonic within the process, 2^20 per ms).
#[derive(Default)]
pub struct RequestIds(Mutex<i64>);
impl RequestIds {
    pub fn next(&self) -> i64 {
        self.next_at(now_ms())
    }
    pub fn next_at(&self, ms: i64) -> i64 {
        let base = ms << SEQ_BITS;
        let mut last = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let id = if *last >= base { *last + 1 } else { base };
        *last = id;
        id
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Default)]
pub struct Counters {
    pub accepted: AtomicU64,
    pub dropped: AtomicU64,
    pub written: AtomicU64,
    pub write_errors: AtomicU64,
    pub purged: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct Stats {
    pub accepted: u64,
    pub dropped: u64,
    pub written: u64,
    pub write_errors: u64,
    pub purged: u64,
}

pub(crate) enum Msg {
    Rec(Box<Record>),
    Flush(SyncSender<()>),
    Stop,
}

#[derive(Debug, thiserror::Error)]
pub enum UsageError {
    #[error("sqlite: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("writer thread gone")]
    Gone,
}

pub struct Recorder {
    fast: SyncSender<Msg>,
    path: PathBuf,
    pub ids: RequestIds,
    counters: Arc<Counters>,
    join: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Recorder {
    /// Open/create the DB (schema + pragmas) and start the writer thread.
    pub fn open(cfg: &UsageConfig) -> Result<Arc<Self>, UsageError> {
        if let Some(dir) = cfg.path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir)?;
        }
        let conn = store::open_rw(&cfg.path)?;
        let (tx, rx) = sync_channel(cfg.queue_size.max(1));
        let counters = Arc::new(Counters::default());
        let w = writer::Writer {
            conn,
            rx,
            batch_size: cfg.batch_size.max(1),
            flush: std::time::Duration::from_millis(cfg.flush_ms),
            retention_days: cfg.retention_days,
            counters: counters.clone(),
        };
        let join = std::thread::Builder::new()
            .name("usage-writer".into())
            .spawn(move || w.run())?;
        Ok(Arc::new(Self {
            fast: tx,
            path: cfg.path.clone(),
            ids: RequestIds::default(),
            counters,
            join: Mutex::new(Some(join)),
        }))
    }

    /// Non-blocking. Returns false if the record was dropped (queue full / writer gone).
    pub fn record(&self, r: Record) -> bool {
        match self.fast.try_send(Msg::Rec(Box::new(r))) {
            Ok(()) => {
                self.counters.accepted.fetch_add(1, Ordering::Relaxed);
                true
            }
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                let n = self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                if n.is_power_of_two() {
                    tracing::warn!(dropped = n + 1, "usage queue full; dropping records");
                }
                false
            }
        }
    }

    pub fn stats(&self) -> Stats {
        let c = &self.counters;
        Stats {
            accepted: c.accepted.load(Ordering::Relaxed),
            dropped: c.dropped.load(Ordering::Relaxed),
            written: c.written.load(Ordering::Relaxed),
            write_errors: c.write_errors.load(Ordering::Relaxed),
            purged: c.purged.load(Ordering::Relaxed),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Block until everything queued before this call is committed (tests / shutdown).
    pub fn flush_blocking(&self) -> Result<(), UsageError> {
        let (tx, rx) = sync_channel(1);
        self.fast.send(Msg::Flush(tx)).map_err(|_| UsageError::Gone)?;
        rx.recv().map_err(|_| UsageError::Gone)
    }

    /// Flush and stop the writer (drains the queue first).
    pub fn shutdown(&self) {
        if let Some(j) = self.join.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = self.fast.send(Msg::Stop);
            let _ = j.join();
        }
    }

    pub async fn recent(&self, q: RecentQuery) -> Result<Vec<RecordRow>, UsageError> {
        let p = self.path.clone();
        tokio::task::spawn_blocking(move || store::recent(&store::open_ro(&p)?, &q))
            .await
            .map_err(|_| UsageError::Gone)?
    }

    pub async fn summary(&self, q: SummaryQuery) -> Result<Summary, UsageError> {
        let p = self.path.clone();
        tokio::task::spawn_blocking(move || store::summary(&store::open_ro(&p)?, &q))
            .await
            .map_err(|_| UsageError::Gone)?
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn cfg(dir: &Path) -> UsageConfig {
        UsageConfig {
            path: dir.join("u.db"),
            ..UsageConfig::default()
        }
    }

    fn rec(r: &Recorder, ms: i64, acct: &str, model: &str, ttft: u32, dur: u32, out: i64) -> i64 {
        let id = r.ids.next_at(ms);
        let mut x = Record::new(id, Transport::Sse, "responses");
        x.account = Some(acct.into());
        x.model = Some(model.into());
        x.effort = Some("low".into());
        x.status = 200;
        x.ttft_ms = Some(ttft);
        x.duration_ms = dur;
        x.input_tokens = Some(10);
        x.output_tokens = Some(out);
        x.cached_tokens = Some(4);
        x.reasoning_tokens = Some(2);
        assert!(r.record(x));
        id
    }

    #[test]
    fn ids_are_time_ordered_and_unique() {
        let g = RequestIds::default();
        let a = g.next_at(1000);
        let b = g.next_at(1000);
        let c = g.next_at(999); // clock went backwards: still increasing
        assert!(a < b && b < c);
        assert_eq!(a >> SEQ_BITS, 1000);
    }

    #[tokio::test]
    async fn roundtrip_dedupe_summary_purge() {
        let d = tempfile::tempdir().unwrap();
        let r = Recorder::open(&cfg(d.path())).unwrap();
        let t0 = now_ms() - 10_000;
        let id = rec(&r, t0, "a", "m1", 100, 1100, 50);
        for i in 0..9 {
            rec(&r, t0 + i, "a", "m1", 200 + i as u32 * 10, 2000, 100);
        }
        rec(&r, t0, "b", "m2", 300, 1300, 0);
        // duplicate id is ignored
        let mut dup = Record::new(id, Transport::Ws, "responses");
        dup.status = 500;
        r.record(dup);
        let mut e = Record::new(r.ids.next_at(t0), Transport::Ws, "responses");
        e.account = Some("b".into());
        e.model = Some("m2".into());
        e.status = 200;
        e.error = Some("usage_limit".into());
        r.record(e);
        r.flush_blocking().unwrap();
        let st = r.stats();
        assert_eq!(st.written, 13);
        assert_eq!(st.dropped, 0);

        let rows = r.recent(RecentQuery { limit: 100, ..Default::default() }).await.unwrap();
        assert_eq!(rows.len(), 12, "duplicate ignored");
        assert!(rows.windows(2).all(|w| w[0].ts_ms >= w[1].ts_ms));
        let first = rows.iter().find(|x| x.request_id == format!("{id:016x}")).unwrap();
        assert_eq!(first.status, 200);
        assert_eq!(first.reasoning_effort.as_deref(), Some("low"));
        assert_eq!(first.tps, Some(50.0)); // 50 tok / 1.0 s
        let errs = r
            .recent(RecentQuery { limit: 10, errors_only: true, ..Default::default() })
            .await
            .unwrap();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].error.as_deref(), Some("usage_limit"));
        let by = r
            .recent(RecentQuery { limit: 10, account: Some("b".into()), ..Default::default() })
            .await
            .unwrap();
        assert_eq!(by.len(), 2);
        assert!(r
            .recent(RecentQuery { limit: 10, account: Some("nobody".into()), ..Default::default() })
            .await
            .unwrap()
            .is_empty());

        let s = r.summary(SummaryQuery { from_ms: t0 - 1, to_ms: now_ms() }).await.unwrap();
        assert_eq!(s.total.count, 12);
        assert_eq!(s.total.errors, 1);
        let a = s.groups.iter().find(|g| g.account.as_deref() == Some("a")).unwrap();
        assert_eq!(a.count, 10);
        assert_eq!(a.ttft_p50_ms, Some(230)); // 100,200..280 → 5th of 10
        assert_eq!(a.ttft_p90_ms, Some(270));
        assert_eq!(a.duration_p90_ms, Some(2000));
        assert_eq!(a.output_tokens, 950);
        assert_eq!(a.cached_tokens, 40);
        assert!(a.tps.unwrap() > 50.0);
        // time range excludes everything
        let none = r.summary(SummaryQuery { from_ms: 0, to_ms: t0 - 1 }).await.unwrap();
        assert_eq!(none.total.count, 0);

        r.shutdown();
        // purge: everything older than "now" (direct store call, chunked)
        let mut c = store::open_rw(&d.path().join("u.db")).unwrap();
        let mut n = 0;
        loop {
            let k = store::purge_before(&mut c, now_ms(), 5).unwrap();
            n += k;
            if k < 5 {
                break;
            }
        }
        assert_eq!(n, 12);
    }

    #[test]
    fn full_queue_drops_without_blocking() {
        let d = tempfile::tempdir().unwrap();
        let mut c = cfg(d.path());
        c.queue_size = 4;
        c.batch_size = 1;
        let r = Recorder::open(&c).unwrap();
        let t = std::time::Instant::now();
        for _ in 0..20_000 {
            let id = r.ids.next();
            r.record(Record::new(id, Transport::Sse, "responses"));
        }
        assert!(t.elapsed() < std::time::Duration::from_secs(1));
        let s = r.stats();
        assert_eq!(s.accepted + s.dropped, 20_000);
        assert!(s.dropped > 0);
        r.flush_blocking().unwrap();
        assert_eq!(r.stats().written, r.stats().accepted);
        r.shutdown();
    }

    #[test]
    fn retention_purge_runs_on_start() {
        let d = tempfile::tempdir().unwrap();
        let c = cfg(d.path());
        {
            let r = Recorder::open(&UsageConfig { retention_days: 0, ..c.clone() }).unwrap();
            let old = r.ids.next_at(now_ms() - 40 * 86_400_000);
            r.record(Record::new(old, Transport::Sse, "responses"));
            let new = r.ids.next();
            r.record(Record::new(new, Transport::Sse, "responses"));
            r.shutdown();
        }
        let r = Recorder::open(&c).unwrap();
        r.flush_blocking().unwrap();
        // purge happens before the first batch; give the thread a moment
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert_eq!(r.stats().purged, 1);
        r.shutdown();
    }
}
