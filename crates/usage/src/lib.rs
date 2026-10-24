//! Per-request usage log. **Never blocks forwarding**: `Recorder::record` is a non-blocking
//! `try_send` into a bounded queue (full ⇒ drop + count); one dedicated OS thread batch-writes
//! to SQLite (WAL). Reads open their own read-only connections. Design: docs/USAGE.md.
pub mod detail;
pub mod prices;
pub mod reset;
mod store;
mod writer;

pub use detail::{DetailRow, ErrorDetail};
pub use prices::{PriceTable, Tier};
pub use store::{
    AccountQuota, DayBucket, Dim, Estimate, Filters, Outcome, RecentQuery, RecordRow, Summary,
    SummaryGroup, SummaryQuery, WindowQuota, PREWARM_ENDPOINT,
};

use ciallopa_core::config::{QuotaWeights, UsageConfig};
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
    /// Short machine-readable error code (`usage_limit`, `idle_timeout`, `client_closed`, …).
    pub error: Option<String>,
    /// Where the error came from: `client`, `local`, `network`, `upstream` or `stream`.
    pub err_source: Option<&'static str>,
    /// Last upstream HTTP status seen (may differ from `status`, which the client received).
    pub upstream_status: Option<u16>,
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
    /// `input_tokens_details.cache_write_tokens` (part of `input_tokens`).
    pub cache_write_tokens: Option<i64>,
    /// Response `service_tier` (else the request's), e.g. `default`, `flex`, `priority`.
    pub service_tier: Option<String>,
    /// `service_tier` the client asked for (`fast` ≡ `priority`), kept even when downgraded.
    pub req_tier: Option<String>,
    /// `response.model` reported by the upstream (terminal event wins over `response.created`).
    pub resp_model: Option<String>,
    /// `response.reasoning.effort` actually used.
    pub resp_effort: Option<String>,
    /// `response.incomplete` reason (`incomplete_details.reason`, else `unknown`); None = not incomplete.
    pub incomplete: Option<String>,
    /// Codex `thread-id` / `x-codex-turn-metadata` (for a future authoritative cost lookup).
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    /// Label of the client key (never the key).
    pub key: Option<String>,
    /// Upstream quota snapshot seen on this response (headers / `codex.rate_limits`).
    pub quota: Option<QuotaObs>,
    /// Only for failed requests (status ≥ 400 or `error`); redacted when written.
    pub detail: Option<Box<ErrorDetail>>,
}

/// One upstream quota snapshot (`x-codex-{primary,secondary}-*`, `x-codex-plan-type`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct QuotaObs {
    pub plan: Option<String>,
    pub p_pct: Option<f64>,
    /// Unix seconds.
    pub p_reset: Option<i64>,
    pub p_window_min: Option<i64>,
    pub s_pct: Option<f64>,
    pub s_reset: Option<i64>,
    pub s_window_min: Option<i64>,
}

/// `gpt-5.5-2026-01-15` → `gpt-5.5` (a dated snapshot is the same model).
fn strip_snapshot(m: &str) -> &str {
    let b = m.as_bytes();
    if b.len() > 11 && b[b.len() - 11] == b'-' {
        let d = &b[b.len() - 10..];
        let ok = d.iter().enumerate().all(|(i, c)| {
            if i == 4 || i == 7 {
                *c == b'-'
            } else {
                c.is_ascii_digit()
            }
        });
        if ok {
            return &m[..m.len() - 11];
        }
    }
    m
}

/// Requested vs response model: `None` when the upstream reported no model; `Some(false)` when
/// equal ignoring case and a `-YYYY-MM-DD` snapshot suffix; `Some(true)` otherwise.
pub fn model_mismatch(requested: Option<&str>, response: Option<&str>) -> Option<bool> {
    let resp = response.map(str::trim).filter(|s| !s.is_empty())?;
    let Some(req) = requested.map(str::trim).filter(|s| !s.is_empty()) else {
        return Some(true);
    };
    Some(!strip_snapshot(req).eq_ignore_ascii_case(strip_snapshot(resp)))
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
            err_source: None,
            upstream_status: None,
            retries: 0,
            switches: 0,
            ttft_ms: None,
            duration_ms: 0,
            input_tokens: None,
            output_tokens: None,
            cached_tokens: None,
            reasoning_tokens: None,
            cache_write_tokens: None,
            service_tier: None,
            req_tier: None,
            resp_model: None,
            resp_effort: None,
            incomplete: None,
            thread_id: None,
            turn_id: None,
            key: None,
            quota: None,
            detail: None,
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
    /// Quota snapshot read on request (not from a proxied response): `(id, account, by, obs)`.
    Quota(Box<(i64, String, String, QuotaObs)>),
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
    weights: std::sync::RwLock<QuotaWeights>,
    retention_days: Arc<std::sync::atomic::AtomicU32>,
    prices: std::sync::RwLock<Arc<PriceTable>>,
    pub ids: RequestIds,
    counters: Arc<Counters>,
    join: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Recorder {
    /// Open/create the DB (schema + pragmas) and start the writer thread.
    pub fn open(cfg: &UsageConfig) -> Result<Arc<Self>, UsageError> {
        Self::open_as(cfg, &ciallopa_core::config::hostname())
    }

    /// Like [`open`](Self::open) with an explicit `instance_id` stored on every record.
    pub fn open_as(cfg: &UsageConfig, instance: &str) -> Result<Arc<Self>, UsageError> {
        if let Some(dir) = cfg.path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir)?;
        }
        let conn = store::open_rw(&cfg.path)?;
        let (tx, rx) = sync_channel(cfg.queue_size.max(1));
        let counters = Arc::new(Counters::default());
        let retention_days = Arc::new(std::sync::atomic::AtomicU32::new(cfg.retention_days));
        let w = writer::Writer {
            conn,
            rx,
            batch_size: cfg.batch_size.max(1),
            flush: std::time::Duration::from_millis(cfg.flush_ms),
            retention_days: retention_days.clone(),
            counters: counters.clone(),
            state: store::WriterState::new(instance.to_string()),
        };
        let join = std::thread::Builder::new()
            .name("usage-writer".into())
            .spawn(move || w.run())?;
        Ok(Arc::new(Self {
            fast: tx,
            path: cfg.path.clone(),
            weights: std::sync::RwLock::new(cfg.quota_weights.clone()),
            retention_days,
            prices: Default::default(),
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

    /// Queue a quota snapshot fetched outside a proxied request (management page). Written
    /// with the next batch even when unchanged, so its time shows how fresh the data is;
    /// `by` is stored as the observer. Non-blocking; false if dropped.
    pub fn observe_quota(&self, account: &str, by: &str, q: QuotaObs) -> bool {
        let id = self.ids.next();
        self.fast
            .try_send(Msg::Quota(Box::new((
                id,
                account.to_string(),
                by.to_string(),
                q,
            ))))
            .is_ok()
    }

    /// Blocking send (bulk import / benchmarks only — never call from the proxy path).
    pub fn record_blocking(&self, r: Record) -> bool {
        let ok = self.fast.send(Msg::Rec(Box::new(r))).is_ok();
        if ok {
            self.counters.accepted.fetch_add(1, Ordering::Relaxed);
        }
        ok
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
        self.fast
            .send(Msg::Flush(tx))
            .map_err(|_| UsageError::Gone)?;
        rx.recv().map_err(|_| UsageError::Gone)
    }

    /// Flush and stop the writer (drains the queue first).
    pub fn shutdown(&self) {
        if let Some(j) = self.join.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = self.fast.send(Msg::Stop);
            let _ = j.join();
        }
    }

    /// Distinct models (lowercase) of rows with token counts since `since_ms`.
    pub async fn models_since(&self, since_ms: i64) -> Result<Vec<String>, UsageError> {
        let p = self.path.clone();
        tokio::task::spawn_blocking(move || store::models_since(&store::open_ro(&p)?, since_ms))
            .await
            .map_err(|_| UsageError::Gone)?
    }

    /// Hot-reload: quota-capacity estimation weights.
    pub fn set_weights(&self, w: QuotaWeights) {
        *self.weights.write().unwrap_or_else(|e| e.into_inner()) = w;
    }
    /// Hot-reload: raw-row retention (0 = keep forever); the writer picks it up within a minute.
    pub fn set_retention_days(&self, days: u32) {
        self.retention_days.store(days, Ordering::Relaxed);
    }
    pub fn retention_days(&self) -> u32 {
        self.retention_days.load(Ordering::Relaxed)
    }
    /// Install / replace the price table (applies to every query, history included).
    pub fn set_prices(&self, p: PriceTable) {
        *self.prices.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(p);
    }
    pub fn prices(&self) -> Arc<PriceTable> {
        self.prices
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub async fn recent(&self, q: RecentQuery) -> Result<Vec<RecordRow>, UsageError> {
        let (p, pr) = (self.path.clone(), self.prices());
        tokio::task::spawn_blocking(move || store::recent(&store::open_ro(&p)?, &q, &pr))
            .await
            .map_err(|_| UsageError::Gone)?
    }

    /// Metadata for any recorded request, including a clean success without error detail.
    pub async fn record_by_id(&self, request_id: &str) -> Result<Option<RecordRow>, UsageError> {
        let Ok(id) = i64::from_str_radix(request_id, 16) else {
            return Ok(None);
        };
        let (p, pr) = (self.path.clone(), self.prices());
        tokio::task::spawn_blocking(move || store::record(&store::open_ro(&p)?, id, &pr))
            .await
            .map_err(|_| UsageError::Gone)?
    }

    /// Number of rows matching `q` (ignores limit/offset).
    pub async fn recent_count(&self, q: RecentQuery) -> Result<u64, UsageError> {
        let p = self.path.clone();
        tokio::task::spawn_blocking(move || store::recent_count(&store::open_ro(&p)?, &q))
            .await
            .map_err(|_| UsageError::Gone)?
    }

    pub async fn summary(&self, q: SummaryQuery) -> Result<Summary, UsageError> {
        let (p, pr) = (self.path.clone(), self.prices());
        let n = pr.models.len();
        let mut s =
            tokio::task::spawn_blocking(move || store::summary(&store::open_ro(&p)?, &q, &pr))
                .await
                .map_err(|_| UsageError::Gone)??;
        s.price_models = n;
        Ok(s)
    }

    /// Error detail by request id (16 hex chars as shown in records).
    pub async fn detail(&self, request_id: &str) -> Result<Option<DetailRow>, UsageError> {
        let Ok(id) = i64::from_str_radix(request_id, 16) else {
            return Ok(None);
        };
        let p = self.path.clone();
        tokio::task::spawn_blocking(move || store::detail(&store::open_ro(&p)?, id))
            .await
            .map_err(|_| UsageError::Gone)?
    }

    /// Latest quota snapshot + capacity estimate per observed account. Snapshots come from
    /// upstream and include usage by *other* clients / instances of the same account.
    pub async fn quota(&self) -> Result<Vec<AccountQuota>, UsageError> {
        let (p, w) = (
            self.path.clone(),
            self.weights
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        );
        tokio::task::spawn_blocking(move || store::quota(&store::open_ro(&p)?, &w, now_ms()))
            .await
            .map_err(|_| UsageError::Gone)?
    }
}

/// Maintenance: delete rows started before `cutoff_ms`, `chunk` rows per transaction.
pub fn purge_db(path: &Path, cutoff_ms: i64, chunk: u64) -> Result<u64, UsageError> {
    let mut c = store::open_rw(path)?;
    let mut n = 0;
    loop {
        let k = store::purge_before(&mut c, cutoff_ms, chunk)?;
        n += k;
        if k < chunk {
            return Ok(n);
        }
    }
}

/// `PRAGMA integrity_check` result ("ok" when healthy) and row count.
pub fn integrity_check(path: &Path) -> Result<(String, i64), UsageError> {
    let c = store::open_ro(path)?;
    let s: String = c.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    let n: i64 = c.query_row("SELECT count(*) FROM requests", [], |r| r.get(0))?;
    Ok((s, n))
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

    #[test]
    fn read_snapshot_keeps_labels_and_rows_together_during_write() {
        let d = tempfile::tempdir().unwrap();
        let r = Recorder::open(&cfg(d.path())).unwrap();
        let reader = store::open_ro(r.path()).unwrap();
        // Pin the same snapshot that label_map reads before fetching request rows.
        let labels: i64 = reader
            .query_row("SELECT count(*) FROM labels", [], |row| row.get(0))
            .unwrap();
        assert_eq!(labels, 0);
        let now = now_ms();
        qrec(&r, now, "new-account", "new-key", 10, 25.0, now / 1000 + 60);
        r.flush_blocking().unwrap(); // Writer commits while the reader is still open.
        let prices = PriceTable::default();
        assert!(store::recent(&reader, &RecentQuery::default(), &prices)
            .unwrap()
            .is_empty());
        assert_eq!(
            store::summary(&reader, &SummaryQuery::default(), &prices)
                .unwrap()
                .total
                .count,
            0
        );
        assert!(store::quota(&reader, &QuotaWeights::default(), now)
            .unwrap()
            .is_empty());
        drop(reader);

        let fresh = store::open_ro(r.path()).unwrap();
        let rows = store::recent(&fresh, &RecentQuery::default(), &prices).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].account.as_deref(), Some("new-account"));
        assert_eq!(rows[0].key.as_deref(), Some("new-key"));
        assert_eq!(rows[0].model.as_deref(), Some("m"));
        assert_eq!(
            store::summary(&fresh, &SummaryQuery::default(), &prices)
                .unwrap()
                .total
                .count,
            1
        );
        assert_eq!(
            store::quota(&fresh, &QuotaWeights::default(), now)
                .unwrap()
                .len(),
            1
        );
        drop(fresh);
        r.shutdown();
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

        let rows = r
            .recent(RecentQuery {
                limit: 100,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(rows.len(), 12, "duplicate ignored");
        assert!(rows.windows(2).all(|w| w[0].ts_ms >= w[1].ts_ms));
        let first = rows
            .iter()
            .find(|x| x.request_id == format!("{id:016x}"))
            .unwrap();
        assert_eq!(first.status, 200);
        assert_eq!(first.reasoning_effort.as_deref(), Some("low"));
        assert_eq!(first.tps, Some(50.0)); // 50 tok / 1.0 s
        let errs = r
            .recent(RecentQuery {
                limit: 10,
                errors_only: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].error.as_deref(), Some("usage_limit"));
        let by = r
            .recent(RecentQuery {
                limit: 10,
                filters: Filters {
                    account: Some("b".into()),
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(by.len(), 2);
        assert!(r
            .recent(RecentQuery {
                limit: 10,
                filters: Filters {
                    account: Some("nobody".into()),
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .unwrap()
            .is_empty());

        let s = r
            .summary(SummaryQuery {
                from_ms: t0 - 1,
                to_ms: now_ms(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(s.total.count, 12);
        assert_eq!(s.total.errors, 1);
        let a = s
            .groups
            .iter()
            .find(|g| g.group["account"].as_deref() == Some("a"))
            .unwrap();
        assert_eq!(a.count, 10);
        assert_eq!(a.ttft_p50_ms, Some(230)); // 100,200..280 → 5th of 10
        assert_eq!(a.ttft_p90_ms, Some(270));
        assert_eq!(a.duration_p90_ms, Some(2000));
        assert_eq!(a.output_tokens, 950);
        assert_eq!(a.cached_tokens, 40);
        assert!(a.tps.unwrap() > 50.0);
        // time range excludes everything
        let none = r
            .summary(SummaryQuery {
                from_ms: 0,
                to_ms: t0 - 1,
                ..Default::default()
            })
            .await
            .unwrap();
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
            let r = Recorder::open(&UsageConfig {
                retention_days: 0,
                ..c.clone()
            })
            .unwrap();
            let old = r.ids.next_at(now_ms() - 100 * 86_400_000);
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

    #[test]
    fn shutdown_flushes_without_waiting_for_batch_deadline() {
        for count in [0, 2] {
            let d = tempfile::tempdir().unwrap();
            let mut c = cfg(d.path());
            c.flush_ms = 60_000;
            c.batch_size = 256;
            let r = Arc::new(Recorder::open(&c).unwrap());
            for _ in 0..count {
                assert!(r.record(Record::new(r.ids.next(), Transport::Sse, "responses")));
            }
            let (done, finished) = sync_channel(1);
            let writer = r.clone();
            let task = std::thread::spawn(move || {
                writer.shutdown();
                done.send(()).unwrap();
            });
            let stopped = finished
                .recv_timeout(std::time::Duration::from_secs(2))
                .is_ok();
            if !stopped {
                // Wake the old implementation so a failing regression does not leave a
                // writer sleeping for a minute or holding its temporary database open.
                let (wake, _) = sync_channel(1);
                let _ = r.fast.send(Msg::Flush(wake));
            }
            task.join().unwrap();
            assert!(
                stopped,
                "shutdown waited for the batch deadline ({count} records)"
            );
            assert_eq!(r.stats().written, count);
            let conn = store::open_ro(r.path()).unwrap();
            let stored: i64 = conn
                .query_row("SELECT COUNT(*) FROM requests", [], |row| row.get(0))
                .unwrap();
            assert_eq!(stored, count as i64);
        }
    }

    fn qrec(r: &Recorder, ms: i64, acct: &str, key: &str, out: i64, p: f64, reset: i64) {
        let mut x = Record::new(r.ids.next_at(ms), Transport::Sse, "responses");
        x.account = Some(acct.into());
        x.model = Some("m".into());
        x.key = Some(key.into());
        x.status = 200;
        x.input_tokens = Some(1000);
        x.cached_tokens = Some(500);
        x.output_tokens = Some(out);
        x.quota = Some(QuotaObs {
            plan: Some("plus".into()),
            p_pct: Some(p),
            p_reset: Some(reset),
            p_window_min: Some(300),
            ..Default::default()
        });
        assert!(r.record(x));
    }

    #[tokio::test]
    async fn keys_instances_quota_and_cost() {
        let d = tempfile::tempdir().unwrap();
        let r = Recorder::open_as(&cfg(d.path()), "host-a").unwrap();
        let t0 = now_ms() - 60_000;
        let reset = now_ms() / 1000 + 3600;
        // 10 requests, % unchanged for pairs (only changes create quota_obs rows); jitter in reset
        for i in 0..10 {
            let key = if i % 2 == 0 { "laptop" } else { "ci" };
            qrec(
                &r,
                t0 + i * 10,
                "a",
                key,
                100,
                10.0 + (i / 2) as f64 * 2.0,
                reset + (i % 3),
            );
        }
        // other account: tiny delta → insufficient data
        qrec(&r, t0, "b", "laptop", 10, 1.0, reset);
        qrec(&r, t0 + 1, "b", "laptop", 10, 2.0, reset);
        r.flush_blocking().unwrap();

        let c = store::open_ro(&d.path().join("u.db")).unwrap();
        let obs: i64 = c
            .query_row("SELECT count(*) FROM quota_obs", [], |x| x.get(0))
            .unwrap();
        assert_eq!(obs, 5 + 2, "rows only on change (reset jitter tolerated)");

        let rows = r
            .recent(RecentQuery {
                limit: 100,
                filters: Filters {
                    key: Some("ci".into()),
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(rows.len(), 5);
        assert!(rows
            .iter()
            .all(|x| x.key.as_deref() == Some("ci") && x.instance.as_deref() == Some("host-a")));
        assert_eq!(rows[0].primary_used_pct, Some(18.0));
        assert_eq!(rows[0].cost_usd, None, "no price table → no cost");

        let s = r
            .summary(SummaryQuery {
                from_ms: t0 - 1,
                to_ms: now_ms(),
                group_by: vec![Dim::Key, Dim::Instance],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(s.groups.len(), 2);
        let lap = s
            .groups
            .iter()
            .find(|g| g.group["key"].as_deref() == Some("laptop"))
            .unwrap();
        assert_eq!(lap.count, 7);
        assert_eq!(lap.group["instance"].as_deref(), Some("host-a"));

        // estimate: first obs at %10 (req 0) .. last at %18 (req 8): reqs 0..8 → 8 requests
        // weighted = 8 * (1.0*500 + 0.1*500 + 4.0*100) = 7600, Δ=8% → 950/%.
        let q = r.quota().await.unwrap();
        let a = q.iter().find(|x| x.account == "a").unwrap();
        assert_eq!(a.plan.as_deref(), Some("plus"));
        assert_eq!(a.primary.used_pct, Some(18.0));
        let e = &a.primary.estimate;
        assert_eq!(e.status, "ok");
        assert_eq!(e.requests, Some(8));
        assert!((e.tokens_per_pct.unwrap() - 950.0).abs() < 1e-6);
        assert!((e.capacity_tokens.unwrap() - 95_000.0).abs() < 1e-6);
        assert!((e.remaining_tokens.unwrap() - 82.0 * 950.0).abs() < 1e-6);
        assert_eq!(a.secondary.estimate.status, "no_window");
        let b = q.iter().find(|x| x.account == "b").unwrap();
        assert_eq!(b.primary.estimate.status, "insufficient_data");

        // cost hook
        let pt =
            PriceTable::parse("[[model]]\nmodel=\"m\"\nin=1.0\ncache=0.1\nout=10.0\n").unwrap();
        r.set_prices(pt);
        let rows = r
            .recent(RecentQuery {
                limit: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        let want = (500.0 * 1.0 + 500.0 * 0.1 + 10.0 * 10.0) / 1e6;
        assert!((rows[0].cost_usd.unwrap() - want).abs() < 1e-12);
        let s = r
            .summary(SummaryQuery {
                from_ms: t0 - 1,
                to_ms: now_ms(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(s.total.cost_usd.unwrap() > 0.0);
        r.shutdown();
    }

    #[test]
    fn model_mismatch_rule() {
        assert_eq!(model_mismatch(Some("gpt-5.5"), None), None);
        assert_eq!(model_mismatch(Some("gpt-5.5"), Some("")), None);
        assert_eq!(
            model_mismatch(Some("gpt-5.5"), Some("gpt-5.5")),
            Some(false)
        );
        assert_eq!(
            model_mismatch(Some("GPT-5.5"), Some("gpt-5.5")),
            Some(false)
        );
        assert_eq!(
            model_mismatch(Some("gpt-5.5"), Some("gpt-5.5-2026-09-30")),
            Some(false)
        );
        assert_eq!(
            model_mismatch(Some("gpt-5.5"), Some("gpt-5.6-luna")),
            Some(true)
        );
        assert_eq!(model_mismatch(None, Some("gpt-5.5")), Some(true));
        assert_eq!(
            model_mismatch(Some("gpt-5.5"), Some("gpt-5.5-mini")),
            Some(true)
        );
    }

    #[test]
    fn migrates_v4_db_and_roundtrips_v5_fields() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("u.db");
        let old_id = (now_ms() - 5_000) << SEQ_BITS;
        {
            let mut c = rusqlite::Connection::open(&p).unwrap();
            store::migrate(&mut c).unwrap();
            c.execute_batch(&format!(
                "ALTER TABLE requests DROP COLUMN req_tier;
                 ALTER TABLE requests DROP COLUMN resp_model;
                 ALTER TABLE requests DROP COLUMN resp_effort;
                 ALTER TABLE requests DROP COLUMN incomplete;
                 ALTER TABLE requests DROP COLUMN err_source;
                 ALTER TABLE requests DROP COLUMN up_status;
                 ALTER TABLE error_detail DROP COLUMN up_req_id;
                 ALTER TABLE error_detail DROP COLUMN attempts;
                 INSERT INTO labels VALUES(1,'old-acct'),(2,'gpt-5.5'),(3,'default');
                 INSERT INTO requests(id,account,model,transport,status,retries,switches,duration_ms,out_tok,tier)
                   VALUES({old_id},1,2,1,200,0,0,5,7,3);
                 PRAGMA user_version=4;"
            ))
            .unwrap();
        }
        let r = Recorder::open_as(
            &UsageConfig {
                path: p.clone(),
                ..UsageConfig::default()
            },
            "h",
        )
        .unwrap();
        let mut x = Record::new(r.ids.next_at(now_ms()), Transport::Sse, "responses");
        x.status = 200;
        x.model = Some("gpt-5.5".into());
        x.req_tier = Some("priority".into());
        x.service_tier = Some("default".into());
        x.resp_model = Some("gpt-5.5-2026-09-30".into());
        x.resp_effort = Some("medium".into());
        x.incomplete = Some("max_output_tokens".into());
        assert!(r.record(x));
        r.flush_blocking().unwrap();
        let c = store::open_ro(&p).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |x| x.get(0))
            .unwrap();
        assert_eq!(v, store::SCHEMA_VERSION);
        let rows = store::recent(
            &c,
            &RecentQuery {
                limit: 10,
                ..Default::default()
            },
            &PriceTable::default(),
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        let (new, old) = (&rows[0], &rows[1]);
        assert_eq!(new.incomplete.as_deref(), Some("max_output_tokens"));
        assert_eq!(old.incomplete, None);
        assert_eq!(old.account.as_deref(), Some("old-acct"));
        assert_eq!(old.service_tier.as_deref(), Some("default"));
        assert_eq!(old.requested_service_tier, None);
        assert_eq!(old.response_model, None);
        assert_eq!(old.model_mismatch, None);
        assert_eq!(new.requested_service_tier.as_deref(), Some("priority"));
        assert_eq!(new.service_tier.as_deref(), Some("default"));
        assert_eq!(new.response_model.as_deref(), Some("gpt-5.5-2026-09-30"));
        assert_eq!(new.response_effort.as_deref(), Some("medium"));
        assert_eq!(new.model_mismatch, Some(false));
        let q = |tier: Option<&str>, downgraded| RecentQuery {
            limit: 10,
            tier: tier.map(Into::into),
            downgraded,
            ..Default::default()
        };
        let cnt = |q| store::recent_count(&c, &q).unwrap();
        assert_eq!(cnt(q(None, true)), 1);
        assert_eq!(cnt(q(Some("standard"), false)), 2);
        assert_eq!(cnt(q(Some("fast"), false)), 0);
        let page = store::recent(
            &c,
            &RecentQuery {
                offset: 1,
                ..q(None, false)
            },
            &PriceTable::default(),
        )
        .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].account.as_deref(), Some("old-acct"));
    }

    #[test]
    fn migrates_v1_db() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("u.db");
        let old_id = (now_ms() - 5_000) << SEQ_BITS;
        {
            let c = rusqlite::Connection::open(&p).unwrap();
            c.execute_batch(&format!(
                "CREATE TABLE labels(id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);
                 CREATE TABLE requests(id INTEGER PRIMARY KEY, account INTEGER, first_account INTEGER,
                   model INTEGER, effort INTEGER, transport INTEGER NOT NULL, endpoint INTEGER,
                   status INTEGER NOT NULL, error INTEGER, retries INTEGER NOT NULL,
                   switches INTEGER NOT NULL, ttft_ms INTEGER, duration_ms INTEGER NOT NULL,
                   in_tok INTEGER, out_tok INTEGER, cached_tok INTEGER, reasoning_tok INTEGER);
                 INSERT INTO labels VALUES(1,'old-acct');
                 INSERT INTO requests(id,account,transport,status,retries,switches,duration_ms,out_tok)
                   VALUES({old_id},1,1,200,0,0,5,7);
                 PRAGMA user_version=1;"
            ))
            .unwrap();
        }
        let r = Recorder::open_as(
            &UsageConfig {
                path: p.clone(),
                ..UsageConfig::default()
            },
            "h",
        )
        .unwrap();
        qrec(&r, now_ms(), "new", "k", 1, 1.0, now_ms() / 1000 + 60);
        r.flush_blocking().unwrap();
        let c = store::open_ro(&p).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |x| x.get(0))
            .unwrap();
        assert_eq!(v, store::SCHEMA_VERSION);
        let n: i64 = c
            .query_row("SELECT count(*) FROM requests", [], |x| x.get(0))
            .unwrap();
        assert_eq!(n, 2, "old rows kept");
        let rows = store::recent(
            &c,
            &RecentQuery {
                limit: 10,
                ..Default::default()
            },
            &PriceTable::default(),
        )
        .unwrap();
        assert_eq!(rows[1].account.as_deref(), Some("old-acct"));
        assert_eq!(rows[1].key, None);
        r.shutdown();
    }

    #[tokio::test]
    async fn cost_tier_cache_write_unknown_and_grouping() {
        let d = tempfile::tempdir().unwrap();
        let r = Recorder::open_as(&cfg(d.path()), "h1").unwrap();
        r.set_prices(
            PriceTable::parse("[[model]]\nmodel=\"m\"\nin=2.0\ncache=0.2\nwrite=2.5\nout=12.0\n")
                .unwrap(),
        );
        let t0 = now_ms() - 1000;
        let mk = |model: &str, key: &str, tier: Option<&str>| {
            let mut x = Record::new(r.ids.next_at(t0), Transport::Sse, "responses");
            x.account = Some("a".into());
            x.model = Some(model.into());
            x.key = Some(key.into());
            x.status = 200;
            x.input_tokens = Some(1000);
            x.cached_tokens = Some(500);
            x.cache_write_tokens = Some(100);
            x.output_tokens = Some(100);
            x.reasoning_tokens = Some(60);
            x.service_tier = tier.map(str::to_string);
            x.thread_id = Some("th-1".into());
            x.turn_id = Some("tu-1".into());
            x
        };
        assert!(r.record(mk("m", "k1", None)));
        assert!(r.record(mk("m", "k2", Some("priority"))));
        assert!(r.record(mk("zz", "k2", Some("flex"))));
        let mut e = Record::new(r.ids.next_at(t0), Transport::Sse, "responses");
        e.model = Some("zz".into());
        e.status = 502;
        assert!(r.record(e)); // no tokens: neither priced nor unknown
        r.flush_blocking().unwrap();
        let base = (400.0 * 2.0 + 500.0 * 0.2 + 100.0 * 2.5 + 100.0 * 12.0) / 1e6;
        let rows = r
            .recent(RecentQuery {
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        let by = |k: &str, m: &str| {
            rows.iter()
                .find(|x| x.key.as_deref() == Some(k) && x.model.as_deref() == Some(m))
                .unwrap()
        };
        let a = by("k1", "m");
        assert!((a.cost_usd.unwrap() - base).abs() < 1e-12);
        assert_eq!(
            (a.thread_id.as_deref(), a.turn_id.as_deref()),
            (Some("th-1"), Some("tu-1"))
        );
        assert_eq!(a.cache_write_tokens, Some(100));
        assert!(
            (by("k2", "m").cost_usd.unwrap() - base * 2.0).abs() < 1e-12,
            "fast ×2"
        );
        let z = by("k2", "zz");
        assert_eq!((z.cost_usd, z.price_status), (None, "unknown_price"));
        assert_eq!(
            rows.iter().find(|x| x.status == 502).unwrap().price_status,
            "no_tokens"
        );

        let s = r
            .summary(SummaryQuery {
                group_by: vec![Dim::Key],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(s.total.unknown_price, 1);
        assert!((s.total.cost_usd.unwrap() - base * 3.0).abs() < 1e-12);
        assert_eq!(s.unknown_price_models, vec!["zz (flex)".to_string()]);
        let k2 = s
            .groups
            .iter()
            .find(|g| g.group["key"].as_deref() == Some("k2"))
            .unwrap();
        assert_eq!(k2.unknown_price, 1);
        assert!((k2.cost_usd.unwrap() - base * 2.0).abs() < 1e-12);
        let s = r
            .summary(SummaryQuery {
                group_by: vec![Dim::Tier],
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(s
            .groups
            .iter()
            .any(|g| g.group["service_tier"].as_deref() == Some("priority")));
        // ignored ⇒ exact 0 with status "ignored": totals stay exact (no ≥), hint gone
        let mut t = r.prices().as_ref().clone();
        t.edit("zz", &crate::prices::Edit::Ignore).unwrap();
        r.set_prices(t);
        let rows = r
            .recent(RecentQuery {
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        let z = rows
            .iter()
            .find(|x| x.model.as_deref() == Some("zz") && x.status != 502)
            .unwrap();
        assert_eq!((z.cost_usd, z.price_status), (Some(0.0), "ignored"));
        let s = r
            .summary(SummaryQuery {
                group_by: vec![Dim::Key],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(s.total.unknown_price, 0);
        assert!(s.unknown_price_models.is_empty());
        assert!((s.total.cost_usd.unwrap() - base * 3.0).abs() < 1e-12);
        // nothing priced ⇒ null, never 0
        r.set_prices(PriceTable::default());
        let s = r.summary(SummaryQuery::default()).await.unwrap();
        assert_eq!((s.total.cost_usd, s.total.unknown_price), (None, 3));
        r.shutdown();
    }

    #[tokio::test]
    async fn models_since_lists_models_with_tokens() {
        let d = tempfile::tempdir().unwrap();
        let r = Recorder::open_as(&cfg(d.path()), "h").unwrap();
        let t0 = now_ms() - 1000;
        let mk = |model: &str, tokens: bool| {
            let mut x = Record::new(r.ids.next_at(t0), Transport::Sse, "responses");
            x.model = Some(model.into());
            x.status = 200;
            if tokens {
                x.input_tokens = Some(10);
                x.output_tokens = Some(1);
            }
            x
        };
        assert!(r.record(mk("Gpt-A", true)));
        assert!(r.record(mk("gpt-a", true)));
        assert!(r.record(mk("err-only", false)));
        assert!(r.record(mk("gpt-b", true)));
        r.flush_blocking().unwrap();
        assert_eq!(r.models_since(0).await.unwrap(), vec!["gpt-a", "gpt-b"]);
        assert!(r.models_since(now_ms() + 1000).await.unwrap().is_empty());
        r.shutdown();
    }

    #[tokio::test]
    async fn observed_metrics_separate_outcomes_and_missing_samples() {
        let d = tempfile::tempdir().unwrap();
        let r = Recorder::open_as(
            &UsageConfig {
                retention_days: 0,
                ..cfg(d.path())
            },
            "h",
        )
        .unwrap();
        let base = now_ms();
        let mut rows = Vec::new();
        let samples = [
            (
                Transport::Sse,
                Some(100),
                1100,
                Some(100),
                Some(20),
                Some(50),
            ),
            (Transport::Sse, None, 1000, Some(200), Some(40), None),
            (
                Transport::Http,
                Some(50),
                1050,
                Some(100),
                Some(10),
                Some(0),
            ),
            (Transport::Sse, Some(200), 400, None, None, None),
            (Transport::Sse, Some(300), 500, None, None, None),
            (Transport::Ws, Some(400), 1400, Some(10), Some(2), Some(0)),
            (Transport::Ws, Some(10), 20, Some(1000), Some(0), Some(1000)),
            (
                Transport::Ws,
                Some(200),
                2200,
                Some(300),
                Some(60),
                Some(150),
            ),
        ];
        for (transport, ttft, dur, input, output, cached) in samples {
            let mut x = Record::new(r.ids.next_at(base), transport, "responses");
            x.status = 200;
            x.ttft_ms = ttft;
            x.duration_ms = dur;
            x.input_tokens = input;
            x.output_tokens = output;
            x.cached_tokens = cached;
            rows.push(x);
        }
        rows[3].error = Some("upstream_error_event".into());
        rows[3].err_source = Some("stream");
        rows[4].error = Some("client_closed".into());
        rows[4].err_source = Some("client");
        rows[5].incomplete = Some("max_output_tokens".into());
        rows[6].endpoint = PREWARM_ENDPOINT;
        for row in rows {
            assert!(r.record(row));
        }
        r.flush_blocking().unwrap();
        let summary = r
            .summary(SummaryQuery {
                from_ms: base,
                to_ms: base,
                bucket_day: Some(0),
                ..Default::default()
            })
            .await
            .unwrap();
        let t = summary.total;
        assert_eq!(
            (
                t.count,
                t.failures,
                t.cancelled,
                t.incomplete,
                t.prewarm_count
            ),
            (7, 1, 1, 1, 1)
        );
        assert_eq!(t.errors, 2, "legacy errors still include cancellation");
        assert_eq!(t.failure_rate, Some(1.0 / 7.0));
        assert_eq!(t.cancel_rate, Some(1.0 / 7.0));
        assert_eq!(
            (t.stream_ttft_samples, t.duration_samples, t.tps_samples),
            (5, 7, 2)
        );
        assert_eq!(
            (t.stream_ttft_p50_ms, t.stream_ttft_p90_ms),
            (Some(200), Some(400))
        );
        assert!((t.generation_tps.unwrap() - 80.0 / 3.0).abs() < 1e-9);
        assert_eq!(
            (t.usage_samples, t.cache_samples, t.cache_input_tokens),
            (6, 5, 1510)
        );
        assert!((t.cache_ratio.unwrap() - 1200.0 / 1510.0).abs() < 1e-9);
        assert_eq!(t.input_tokens, 1710, "prewarm usage is included");
        let b = &summary.buckets.unwrap()[0];
        assert_eq!(
            (
                b.count,
                b.failures,
                b.cancelled,
                b.incomplete,
                b.prewarm_count
            ),
            (7, 1, 1, 1, 1)
        );
        assert_eq!(b.end_ms - b.start_ms, 86_400_000);
        for (outcome, expected) in [
            (Outcome::Completed, 4),
            (Outcome::Failed, 1),
            (Outcome::Cancelled, 1),
            (Outcome::Incomplete, 1),
            (Outcome::Prewarm, 1),
        ] {
            assert_eq!(
                r.recent_count(RecentQuery {
                    outcome: Some(outcome),
                    ..Default::default()
                })
                .await
                .unwrap(),
                expected
            );
        }
        let business = RecentQuery {
            limit: 100,
            exclude_prewarm: true,
            ..Default::default()
        };
        assert_eq!(r.recent_count(business.clone()).await.unwrap(), t.count);
        let business_rows = r.recent(business).await.unwrap();
        assert_eq!(business_rows.len() as u64, t.count);
        assert!(business_rows
            .iter()
            .all(|row| row.endpoint.as_deref() != Some(PREWARM_ENDPOINT)));
        assert_eq!(
            r.recent_count(RecentQuery::default()).await.unwrap(),
            t.count + t.prewarm_count,
            "default browsing includes prewarm"
        );
        assert_eq!(
            r.recent_count(RecentQuery {
                exclude_prewarm: true,
                outcome: Some(Outcome::Prewarm),
                ..Default::default()
            })
            .await
            .unwrap(),
            0,
            "request filters compose as AND"
        );
        let empty = r
            .summary(SummaryQuery {
                filters: Filters {
                    model: Some("absent".into()),
                    ..Default::default()
                },
                bucket_day: Some(0),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            (
                empty.total.failure_rate,
                empty.total.cancel_rate,
                empty.total.cache_ratio,
                empty.total.generation_tps
            ),
            (None, None, None, None)
        );
        assert_eq!(empty.total.stream_ttft_samples, 0);
        assert_eq!(empty.total.stream_ttft_p50_ms, None);
        assert_eq!(empty.buckets, Some(vec![]));
        r.shutdown();
    }

    #[tokio::test]
    async fn bucket_usage_samples_distinguish_missing_zero_and_prewarm() {
        let d = tempfile::tempdir().unwrap();
        let r = Recorder::open_as(
            &UsageConfig {
                retention_days: 0,
                ..cfg(d.path())
            },
            "h",
        )
        .unwrap();
        let hour = 3_600_000;
        let base = now_ms() / hour * hour;
        for (offset, endpoint, input, output) in [
            (1, "responses", None, None),
            (2, "responses", Some(0), Some(0)),
            (3, "responses", Some(20), Some(5)),
            (3, "responses", None, None),
            (3, PREWARM_ENDPOINT, Some(0), Some(0)),
            (4, PREWARM_ENDPOINT, None, None),
        ] {
            let mut row = Record::new(
                r.ids.next_at(base + offset * hour),
                Transport::Sse,
                endpoint,
            );
            row.status = 200;
            row.input_tokens = input;
            row.output_tokens = output;
            assert!(r.record(row));
        }
        r.flush_blocking().unwrap();
        let summary = r
            .summary(SummaryQuery {
                from_ms: base,
                to_ms: base + 5 * hour - 1,
                bucket_day: Some(0),
                bucket_hour: true,
                ..Default::default()
            })
            .await
            .unwrap();
        let json = serde_json::to_value(&summary).unwrap();
        let buckets = json["buckets"].as_array().unwrap();
        assert_eq!(
            buckets.len(),
            4,
            "an empty bucket is omitted, not an unknown sample"
        );
        for (index, (count, prewarm, samples, input, output)) in [
            (1, 0, 0, 0, 0),
            (1, 0, 1, 0, 0),
            (2, 1, 2, 20, 5),
            (0, 1, 0, 0, 0),
        ]
        .into_iter()
        .enumerate()
        {
            let b = &buckets[index];
            assert_eq!(b["start_ms"], base + (index as i64 + 1) * hour);
            assert_eq!(b["count"], count);
            assert_eq!(b["prewarm_count"], prewarm);
            assert_eq!(
                b["usage_samples"], samples,
                "bucket {index} usage population"
            );
            assert_eq!(b["input_tokens"], input);
            assert_eq!(b["output_tokens"], output);
        }
        assert_eq!(summary.total.usage_samples, 3);
        r.shutdown();
    }

    #[tokio::test]
    async fn unobserved_outcome_is_separate_in_queries_groups_and_buckets() {
        let d = tempfile::tempdir().unwrap();
        let r = Recorder::open_as(&cfg(d.path()), "h").unwrap();
        let base = now_ms();
        for (endpoint, error, source, incomplete) in [
            ("responses", None, None, None),
            (
                "responses",
                Some("upstream_error_event"),
                Some("stream"),
                None,
            ),
            ("responses", Some("client_closed"), Some("client"), None),
            ("responses", None, None, Some("max_output_tokens")),
            (
                "responses",
                Some("observation_incomplete"),
                Some("local"),
                None,
            ),
            (
                PREWARM_ENDPOINT,
                Some("observation_incomplete"),
                Some("local"),
                None,
            ),
        ] {
            let mut row = Record::new(r.ids.next_at(base), Transport::Sse, endpoint);
            row.status = 200;
            row.error = error.map(str::to_owned);
            row.err_source = source;
            row.incomplete = incomplete.map(str::to_owned);
            row.input_tokens = Some(10);
            row.output_tokens = Some(100);
            row.ttft_ms = Some(100);
            row.duration_ms = 1100;
            assert!(r.record(row));
        }
        // This diagnostic name is not reserved in an upstream's error vocabulary, and
        // an observed failing HTTP status must remain a failure even for a local source.
        for (source, status) in [("upstream", 400), ("upstream", 200), ("local", 500)] {
            let mut row = Record::new(r.ids.next_at(base), Transport::Sse, "responses");
            row.status = status;
            row.error = Some("observation_incomplete".into());
            row.err_source = Some(source);
            row.input_tokens = Some(10);
            row.output_tokens = Some(100);
            row.ttft_ms = Some(100);
            row.duration_ms = 1100;
            assert!(r.record(row));
        }
        r.flush_blocking().unwrap();
        let summary = r
            .summary(SummaryQuery {
                from_ms: base,
                to_ms: base,
                bucket_day: Some(0),
                ..Default::default()
            })
            .await
            .unwrap();
        let json = serde_json::to_value(&summary).unwrap();
        for stats in [&json["total"], &json["groups"][0], &json["buckets"][0]] {
            assert_eq!(stats["count"], 8);
            assert_eq!(stats["failures"], 4);
            assert_eq!(stats["cancelled"], 1);
            assert_eq!(stats["incomplete"], 1);
            assert_eq!(stats["unknown"], 1);
            assert_eq!(stats["prewarm_count"], 1);
            assert_eq!(
                stats["errors"], 6,
                "legacy errors retain the observation diagnostic"
            );
        }
        assert_eq!(summary.total.tps_samples, 1);
        assert_eq!(summary.total.generation_tps, Some(100.0));
        assert_eq!(
            summary.total.usage_samples, 9,
            "observed usage is still counted"
        );
        for value in [
            "completed",
            "failed",
            "cancelled",
            "incomplete",
            "unknown",
            "prewarm",
        ] {
            let query = RecentQuery {
                outcome: Some(Outcome::parse(value).unwrap()),
                limit: 20,
                ..Default::default()
            };
            assert_eq!(
                r.recent_count(query.clone()).await.unwrap(),
                if value == "failed" { 4 } else { 1 },
                "outcome={value}"
            );
            assert_eq!(
                r.recent(query).await.unwrap().len(),
                if value == "failed" { 4 } else { 1 },
                "outcome={value} records"
            );
        }
        r.shutdown();
    }

    #[tokio::test]
    async fn keyset_preserves_same_millisecond_rows_and_ignores_late_higher_ids() {
        let d = tempfile::tempdir().unwrap();
        let r = Recorder::open_as(
            &UsageConfig {
                retention_days: 0,
                ..cfg(d.path())
            },
            "h",
        )
        .unwrap();
        let base = now_ms();
        let mut rows: Vec<_> = (0..4)
            .map(|_| {
                let mut x = Record::new(r.ids.next_at(base), Transport::Sse, "responses");
                x.status = 200;
                x
            })
            .collect();
        let late = rows.remove(2);
        let expected: Vec<_> = rows
            .iter()
            .rev()
            .map(|r| format!("{:016x}", r.id))
            .collect();
        for x in rows {
            assert!(r.record(x));
        }
        r.flush_blocking().unwrap();
        let q = RecentQuery {
            limit: 2,
            since_ms: Some(base),
            before_ms: Some(base + 1),
            ..Default::default()
        };
        let first = r.recent(q.clone()).await.unwrap();
        assert_eq!(
            first.iter().map(|r| &r.request_id).collect::<Vec<_>>(),
            expected[..2].iter().collect::<Vec<_>>()
        );
        // No error/source labels exist yet: NULL must never match the missing-label sentinel.
        assert_eq!(
            r.recent_count(RecentQuery {
                outcome: Some(Outcome::Completed),
                ..q.clone()
            })
            .await
            .unwrap(),
            3
        );
        assert_eq!(
            r.recent_count(RecentQuery {
                outcome: Some(Outcome::Cancelled),
                ..q.clone()
            })
            .await
            .unwrap(),
            0
        );
        let detail = r.record_by_id(&expected[0]).await.unwrap().unwrap();
        assert!(!detail.has_detail);
        assert_eq!(detail, first[0]);
        assert!(r.record_by_id("0000000000000000").await.unwrap().is_none());
        assert!(r.record(late));
        let mut newer = Record::new(r.ids.next_at(base + 1), Transport::Sse, "responses");
        newer.status = 200;
        assert!(r.record(newer));
        r.flush_blocking().unwrap();
        let second = r
            .recent(RecentQuery {
                before_id: Some(i64::from_str_radix(&first[1].request_id, 16).unwrap()),
                ..q.clone()
            })
            .await
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].request_id, expected[2]);
        assert_eq!(
            r.recent_count(q).await.unwrap(),
            4,
            "late completions become visible on refresh; new starts stay outside the fixed window"
        );
        r.shutdown();
    }

    #[tokio::test]
    async fn day_buckets_with_tz() {
        let d = tempfile::tempdir().unwrap();
        let r = Recorder::open_as(
            &UsageConfig {
                retention_days: 0,
                ..cfg(d.path())
            },
            "h",
        )
        .unwrap();
        r.set_prices(PriceTable::parse("[[model]]\nmodel=\"m\"\nin=1.0\nout=1.0\n").unwrap());
        let day = 86_400_000i64;
        let base = (now_ms() / day - 3) * day; // 00:00 UTC three days ago
                                               // 23:30 UTC (day 0), 00:30 UTC (day 1), 01:30 UTC (day 1)
        for (off, model) in [
            (23 * 3_600_000 + 1_800_000, "m"),
            (day + 1_800_000, "m"),
            (day + 5_400_000, "zz"),
        ] {
            let mut x = Record::new(r.ids.next_at(base + off), Transport::Sse, "responses");
            x.model = Some(model.into());
            x.status = 200;
            x.input_tokens = Some(1_000_000);
            x.output_tokens = Some(10);
            assert!(r.record(x));
        }
        r.flush_blocking().unwrap();
        let q = |tz| SummaryQuery {
            from_ms: base,
            to_ms: now_ms(),
            bucket_day: Some(tz),
            ..Default::default()
        };
        let b = r.summary(q(0)).await.unwrap().buckets.unwrap();
        assert_eq!(b.len(), 2);
        assert_eq!((b[0].count, b[1].count), (1, 2));
        assert_eq!(b[0].start_ms, base);
        assert!((b[1].cost_usd.unwrap() - 1.00001).abs() < 1e-9);
        assert_eq!(b[1].unknown_price, 1);
        assert_eq!(b[0].day, store_day(base));
        // UTC+8: all three fall on the same local day
        let b8 = r.summary(q(480)).await.unwrap().buckets.unwrap();
        assert_eq!(b8.len(), 1);
        assert_eq!(b8[0].count, 3);
        assert_eq!(b8[0].start_ms, base + day - 480 * 60_000);
        assert!(r
            .summary(SummaryQuery::default())
            .await
            .unwrap()
            .buckets
            .is_none());
        r.shutdown();
    }

    fn store_day(ms: i64) -> String {
        let days = ms.div_euclid(86_400_000);
        let p = |d: &str| prices::parse_date(d).unwrap();
        // find via parse_date round-trip over a small range
        (2020..2100)
            .flat_map(|y| {
                (1..=12).flat_map(move |m| (1..=31).map(move |d| format!("{y:04}-{m:02}-{d:02}")))
            })
            .find(|s| prices::parse_date(s).is_some_and(|x| x == days) && p(s) == days)
            .unwrap()
    }

    #[tokio::test]
    async fn error_detail_stored_redacted_and_purged() {
        let d = tempfile::tempdir().unwrap();
        let c = UsageConfig {
            retention_days: 0,
            ..cfg(d.path())
        };
        let r = Recorder::open(&c).unwrap();
        let old_ms = now_ms() - 200 * 86_400_000;
        let mk = |ms: i64, status: u16, err: Option<&str>| {
            let mut x = Record::new(r.ids.next_at(ms), Transport::Sse, "responses");
            x.status = status;
            x.error = err.map(str::to_string);
            let mut det = ErrorDetail {
                message: Some("upstream 401 for me@corp.com".into()),
                ..Default::default()
            };
            det.set_body(br#"{"error":{"message":"bad token Bearer abc123"},"access_token":"eyJx.eyJy.zzz"}"#);
            det.add_header("cf-ray", "ray-1");
            det.add_header("set-cookie", "s=1");
            x.detail = Some(Box::new(det));
            let id = x.id;
            assert!(r.record(x));
            id
        };
        let old = mk(old_ms, 500, Some("upstream_5xx")); // ids are monotonic: oldest first
        let ok = mk(now_ms(), 200, None); // detail ignored for successes
        let bad = mk(now_ms(), 401, Some("auth"));
        r.flush_blocking().unwrap();
        let rows = r
            .recent(RecentQuery {
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        let has = |id: i64| {
            rows.iter()
                .find(|x| x.request_id == format!("{id:016x}"))
                .unwrap()
                .has_detail
        };
        assert!(!has(ok) && has(bad) && has(old));
        assert!(r.detail(&format!("{ok:016x}")).await.unwrap().is_none());
        assert!(r.detail("zz").await.unwrap().is_none());
        let dt = r.detail(&format!("{bad:016x}")).await.unwrap().unwrap();
        assert_eq!(
            dt.message.as_deref(),
            Some("upstream 401 for [REDACTED_EMAIL]")
        );
        let body = dt.body.unwrap();
        assert!(!body.contains("abc123") && !body.contains("eyJx"), "{body}");
        assert_eq!(dt.headers["cf-ray"], "ray-1");
        assert!(dt.headers.get("set-cookie").is_none());
        assert!(!dt.truncated);
        r.shutdown();
        // retention 0 kept everything; 90 days purges the old row and its detail
        let r = Recorder::open(&UsageConfig {
            retention_days: 90,
            ..c.clone()
        })
        .unwrap();
        r.flush_blocking().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert_eq!(r.stats().purged, 1);
        assert!(r.detail(&format!("{old:016x}")).await.unwrap().is_none());
        assert!(r.detail(&format!("{bad:016x}")).await.unwrap().is_some());
        r.shutdown();
    }

    #[test]
    fn retention_zero_keeps_forever_and_default_is_90() {
        assert_eq!(UsageConfig::default().retention_days, 90);
        let d = tempfile::tempdir().unwrap();
        let c = UsageConfig {
            retention_days: 0,
            ..cfg(d.path())
        };
        let r = Recorder::open(&c).unwrap();
        r.record(Record::new(
            r.ids.next_at(now_ms() - 3650 * 86_400_000),
            Transport::Sse,
            "responses",
        ));
        r.shutdown();
        let r = Recorder::open(&c).unwrap();
        r.flush_blocking().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert_eq!(r.stats().purged, 0);
        let conn = store::open_ro(&d.path().join("u.db")).unwrap();
        let n: i64 = conn
            .query_row("SELECT count(*) FROM requests", [], |x| x.get(0))
            .unwrap();
        assert_eq!(n, 1);
        r.shutdown();
    }
}
