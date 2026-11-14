//! Per-request usage log. **Never blocks forwarding**: `Recorder::record` is a non-blocking
//! `try_send` into a bounded queue (full ⇒ drop + count); one dedicated OS thread batch-writes
//! to SQLite (WAL). Reads open their own read-only connections. Design: docs/USAGE.md.
pub mod classify;
pub mod credits;
pub mod detail;
pub mod estimate;
mod filter;
mod history;
mod migrate_v12;
pub mod observation;
pub mod occupancy;
pub mod prices;
pub mod problem;
mod quota;
mod records;
pub mod refusals;
pub mod reset;
mod schema;
mod store;
mod summary;
mod write;
mod writer;

pub use detail::{DetailRow, ErrorDetail};
pub use history::{AccountHistory, CycleRow, ResetEvent};
pub use observation::{Observation, ObservationRow};
pub use occupancy::{OccupancyRow, Sample as OccupancySample};
pub use prices::{PriceTable, Tier};
pub use problem::{Outcome, Problem, ProblemCounts};
pub use refusals::{Reason as RefusalReason, Refusal, RefusalRow, RefusalTotals};
pub use store::{
    failure_class, row_failure_class, AccountQuota, DayBucket, Dim, Estimate, Filters, RecentQuery,
    RecordRow, ScopeValue, Summary, SummaryGroup, SummaryQuery, WindowQuota, FAILURE_CLASSES,
    LIFECYCLE_ERRORS, PREWARM_ENDPOINT, QUOTA_ERRORS, RATE_LIMIT_ERRORS, REQUEST_ERRORS,
};
pub use store::{occupancy_step, OccupancyQuery, RefusalQuery, ThreadMatch, THREAD_FRAGMENT_MIN};
pub use write::{integrity_check, purge_db};

use ciallopa_core::config::{QuotaWeights, UsageConfig};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::sync::OnceLock;
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
    /// Gateway stream observation (SSE requests / WS turns); stored in `request_obs`.
    pub obs: Option<Box<Observation>>,
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
    /// `rate_limit_reset_credits.available_count` when the snapshot came from `/wham/usage`.
    pub cards: Option<i64>,
}

/// `gpt-5.5-2026-01-15` → `gpt-5.5` (a dated snapshot is the same model).
pub(crate) fn strip_snapshot(m: &str) -> &str {
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
            obs: None,
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
    prices: Arc<std::sync::RwLock<Arc<PriceTable>>>,
    pub ids: RequestIds,
    counters: Arc<Counters>,
    refusals: Arc<refusals::Aggregator>,
    occupancy: Arc<occupancy::Aggregator>,
    join: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Installed after open via [`Self::install_aliaser`]; write paths map account ids through it.
    aliaser: Arc<OnceLock<Arc<ciallopa_core::Aliaser>>>,
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
        let refusals = Arc::new(refusals::Aggregator::default());
        let occupancy = Arc::new(occupancy::Aggregator::default());
        let prices = Arc::new(std::sync::RwLock::new(Arc::new(PriceTable::default())));
        let w = writer::Writer {
            refusals: refusals.clone(),
            occupancy: occupancy.clone(),
            conn,
            rx,
            batch_size: cfg.batch_size.max(1),
            flush: std::time::Duration::from_millis(cfg.flush_ms),
            retention_days: retention_days.clone(),
            counters: counters.clone(),
            state: store::WriterState::new(instance.to_string()),
            prices: prices.clone(),
        };
        let join = std::thread::Builder::new()
            .name("usage-writer".into())
            .spawn(move || w.run())?;
        Ok(Arc::new(Self {
            fast: tx,
            path: cfg.path.clone(),
            weights: std::sync::RwLock::new(cfg.quota_weights.clone()),
            retention_days,
            ids: RequestIds::default(),
            counters,
            refusals,
            occupancy,
            prices,
            join: Mutex::new(Some(join)),
            aliaser: Arc::new(OnceLock::new()),
        }))
    }

    /// Non-blocking. Returns false if the record was dropped (queue full / writer gone).
    pub fn record(&self, mut r: Record) -> bool {
        self.alias_record(&mut r);
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

    /// Count a refused request into its per-minute group. Non-blocking (a short mutex); the
    /// writer upserts the groups every [`refusals::FLUSH_EVERY`] and on shutdown.
    pub fn refuse(&self, mut r: Refusal) {
        if let Some(ref mut a) = r.account {
            *a = self.alias_account(a);
        }
        self.refusals.add(r);
    }

    /// Add one occupancy sample to its minute. Non-blocking (a short mutex); the writer
    /// stores each minute once it is complete, and everything on shutdown or flush.
    pub fn occupancy(&self, s: occupancy::Sample) {
        self.occupancy.add(s);
    }

    /// Occupancy buckets for a range, oldest first, one per bucket and instance.
    pub async fn occupancy_rows(&self, q: OccupancyQuery) -> Result<Vec<OccupancyRow>, UsageError> {
        let p = self.path.clone();
        tokio::task::spawn_blocking(move || Ok(store::occupancy_rows(&store::open_ro(&p)?, &q)?))
            .await
            .map_err(|_| UsageError::Gone)?
    }

    /// Refusal totals by reason for a range (key / instance filters only).
    pub async fn refusal_totals(&self, q: RefusalQuery) -> Result<RefusalTotals, UsageError> {
        let p = self.path.clone();
        tokio::task::spawn_blocking(move || Ok(store::refusal_totals(&store::open_ro(&p)?, &q)?))
            .await
            .map_err(|_| UsageError::Gone)?
    }

    /// Per-minute refusal groups, newest first.
    pub async fn refusal_rows(&self, q: RefusalQuery) -> Result<Vec<RefusalRow>, UsageError> {
        let p = self.path.clone();
        tokio::task::spawn_blocking(move || Ok(store::refusal_rows(&store::open_ro(&p)?, &q)?))
            .await
            .map_err(|_| UsageError::Gone)?
    }

    /// Queue a quota snapshot fetched outside a proxied request (management page). Written
    /// with the next batch even when unchanged, so its time shows how fresh the data is;
    /// `by` is stored as the observer. Non-blocking; false if dropped.
    pub fn observe_quota(&self, account: &str, by: &str, q: QuotaObs) -> bool {
        let id = self.ids.next();
        self.fast
            .try_send(Msg::Quota(Box::new((
                id,
                self.alias_account(account),
                by.to_string(),
                q,
            ))))
            .is_ok()
    }

    /// Blocking send (bulk import / benchmarks only — never call from the proxy path).
    pub fn record_blocking(&self, mut r: Record) -> bool {
        self.alias_record(&mut r);
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

    /// Persist / reuse the HMAC key in kv and return the shared aliaser (also installs it).
    pub fn load_or_create_aliaser(&self) -> Result<Arc<ciallopa_core::Aliaser>, UsageError> {
        const K: &str = "alias_key";
        let al = match self.kv_get(K)? {
            Some(s) => ciallopa_core::Aliaser::from_b64(&s).map_err(|e| {
                UsageError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            })?,
            None => {
                let al = ciallopa_core::Aliaser::ephemeral();
                self.kv_set(K, &al.key_b64())?;
                al
            }
        };
        self.install_aliaser(al.clone());
        Ok(al)
    }

    /// Attach an aliaser so subsequent writes store `acct_…` instead of raw account ids.
    pub fn install_aliaser(&self, aliaser: Arc<ciallopa_core::Aliaser>) {
        let _ = self.aliaser.set(aliaser);
    }

    pub fn aliaser(&self) -> Option<Arc<ciallopa_core::Aliaser>> {
        self.aliaser.get().cloned()
    }

    pub fn alias_account(&self, id: &str) -> String {
        match self.aliaser.get() {
            Some(a) => a.alias(id),
            None => id.to_string(),
        }
    }

    fn alias_record(&self, r: &mut Record) {
        if let Some(ref mut a) = r.account {
            *a = self.alias_account(a);
        }
        if let Some(ref mut a) = r.first_account {
            *a = self.alias_account(a);
        }
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

    /// Distinct models, key labels and instances of the newest `max_rows` rows, most used
    /// first (see [`ScopeValue`]).
    pub async fn scope_values(&self, max_rows: u32) -> Result<[Vec<ScopeValue>; 3], UsageError> {
        let p = self.path.clone();
        tokio::task::spawn_blocking(move || store::scope_values(&store::open_ro(&p)?, max_rows))
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
    /// Request counts per problem chip and cause ([`problem`]); `refused` left null.
    pub async fn problem_counts(&self, q: RecentQuery) -> Result<ProblemCounts, UsageError> {
        let p = self.path.clone();
        tokio::task::spawn_blocking(move || store::problem_counts(&store::open_ro(&p)?, &q))
            .await
            .map_err(|_| UsageError::Gone)?
    }

    /// Resolve a pasted session id or unique fragment ([`store::resolve_thread`]).
    pub async fn resolve_thread(&self, input: String) -> Result<ThreadMatch, UsageError> {
        let p = self.path.clone();
        tokio::task::spawn_blocking(move || {
            Ok(store::resolve_thread(&store::open_ro(&p)?, &input)?)
        })
        .await
        .map_err(|_| UsageError::Gone)?
    }

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

    /// Gateway stream observation by request id (16 hex chars); None when not recorded.
    pub async fn observation(
        &self,
        request_id: &str,
    ) -> Result<Option<ObservationRow>, UsageError> {
        let Ok(id) = i64::from_str_radix(request_id, 16) else {
            return Ok(None);
        };
        let p = self.path.clone();
        tokio::task::spawn_blocking(move || Ok(observation::read(&store::open_ro(&p)?, id)?))
            .await
            .map_err(|_| UsageError::Gone)?
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
        let (p, w, pr) = (
            self.path.clone(),
            self.weights
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
            self.prices(),
        );
        tokio::task::spawn_blocking(move || store::quota(&store::open_ro(&p)?, &w, &pr, now_ms()))
            .await
            .map_err(|_| UsageError::Gone)?
    }

    /// Reset timeline and per-cycle capacity/API-cost records (`from_ms`/`to_ms` inclusive).
    pub async fn quota_history(
        &self,
        account: Option<&str>,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<AccountHistory>, UsageError> {
        let (p, pr, acct) = (
            self.path.clone(),
            self.prices(),
            account.map(str::to_string),
        );
        tokio::task::spawn_blocking(move || {
            crate::history::history(&store::open_ro(&p)?, &pr, acct.as_deref(), from_ms, to_ms)
        })
        .await
        .map_err(|_| UsageError::Gone)?
    }
}

#[cfg(test)]
mod tests;
