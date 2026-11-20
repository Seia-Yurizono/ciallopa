pub(crate) mod classify;
pub(crate) mod credits;
pub mod detail;
mod epoch;
pub(crate) mod estimate;
mod filter;
mod history;
mod migrate_v12;
pub mod observation;
pub mod occupancy;
mod pool;
pub mod prices;
pub mod problem;
mod quota;
mod records;
pub mod refusals;
pub mod reset;
mod schema;
mod summary;
mod write;
mod writer;

pub use classify::WindowKind;
pub use detail::{DetailRow, ErrorDetail};
pub use epoch::{EpochReset, EpochRow, UNDO_WINDOW_MS};
pub use filter::{Filters, RecentQuery, ThreadMatch, THREAD_FRAGMENT_MIN};
pub use history::{AccountHistory, CycleRow, ResetEvent};
pub use observation::{Observation, ObservationRow};
pub use occupancy::{occupancy_step, OccupancyQuery};
pub use occupancy::{OccupancyRow, Sample as OccupancySample};
pub use prices::{PriceTable, Tier};
pub use problem::FAILURE_CLASSES;
pub use problem::{Outcome, Problem, ProblemCounts};
pub use quota::{AccountQuota, CreditState, Estimate, WindowQuota};
pub use records::{RecordRow, ScopeValue};
pub use refusals::RefusalQuery;
pub use refusals::{Reason as RefusalReason, Refusal, RefusalRow, RefusalTotals};
pub use schema::SCHEMA_VERSION;
pub use summary::{DayBucket, Dim, Summary, SummaryGroup, SummaryQuery};
pub use write::{integrity_check, purge_db};
/// Endpoint label of a Codex WS prewarm turn.
pub const PREWARM_ENDPOINT: &str = "prewarm";

use ciallopa_core::config::{QuotaWeights, UsageConfig};
use futures::{
    future::{BoxFuture, WeakShared},
    FutureExt,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::sync::OnceLock;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Bits of the request id below the millisecond timestamp (per-ms sequence).
pub const SEQ_BITS: u32 = 20;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Sse = 1,
    Ws = 2,
    #[default]
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
#[derive(Debug, Default, Clone, PartialEq)]
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
    pub obs: Option<Box<Observation>>,
}

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
    /// Purchased credits (点数) state, `CR_*` bits; None = not reported on this snapshot.
    pub cr_flags: Option<i64>,
    /// Purchased credits balance; None = not reported.
    pub cr_balance: Option<f64>,
}

/// `credits.has_credits`.
pub const CR_HAS: i64 = 1;
/// `credits.unlimited`.
pub const CR_UNLIMITED: i64 = 2;
/// `credits.overage_limit_reached` (`/wham/usage` only).
pub const CR_OVERAGE_REACHED: i64 = 4;
/// `spend_control.reached` (`/wham/usage` only).
pub const CR_SPEND_CONTROL: i64 = 8;

impl QuotaObs {
    /// Fold the purchased-credit fields of a snapshot into `cr_flags` / `cr_balance`.
    /// `None` inputs leave the state unreported; returns whether anything was set.
    pub fn set_credits(
        &mut self,
        has: Option<bool>,
        unlimited: Option<bool>,
        balance: Option<f64>,
        overage_reached: Option<bool>,
        spend_control: Option<bool>,
    ) -> bool {
        let bits = [
            (has, CR_HAS),
            (unlimited, CR_UNLIMITED),
            (overage_reached, CR_OVERAGE_REACHED),
            (spend_control, CR_SPEND_CONTROL),
        ];
        if bits.iter().all(|(v, _)| v.is_none()) && balance.is_none() {
            return false;
        }
        self.cr_flags = Some(
            bits.iter()
                .filter(|(v, _)| *v == Some(true))
                .fold(0, |acc, (_, bit)| acc | bit),
        );
        self.cr_balance = balance.filter(|b| b.is_finite());
        true
    }
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
            transport,
            endpoint,
            ..Default::default()
        }
    }

    pub fn ts_ms(&self) -> i64 {
        self.id >> SEQ_BITS
    }
}

/// Generates time-ordered unique ids (monotonic within the process, 2^20 per ms).
#[derive(Default)]
pub struct RequestIds(AtomicI64);
impl RequestIds {
    pub fn next(&self) -> i64 {
        self.next_at(now_ms())
    }
    pub fn next_at(&self, ms: i64) -> i64 {
        let base = ms << SEQ_BITS;
        let last = self
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |last| {
                Some((last + 1).max(base))
            })
            .expect("request id update always succeeds");
        (last + 1).max(base)
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Default)]
pub(crate) struct Counters {
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
    Quota(Box<QuotaSnapshot>),
    /// Manual estimate reset / undo, applied in its own transaction and acknowledged.
    Epoch(Box<epoch::Command>),
    Flush(SyncSender<()>),
    Stop,
}

pub(crate) struct QuotaSnapshot {
    pub id: i64,
    pub account: String,
    pub observer: String,
    pub obs: QuotaObs,
}

#[derive(Debug, thiserror::Error)]
pub enum UsageError {
    #[error("sqlite: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("usage read cancelled")]
    Cancelled,
    #[error(transparent)]
    SharedQuota(Arc<UsageError>),
    #[error("writer thread gone")]
    Gone,
    #[error("usage blocking task: {0}")]
    Join(#[from] tokio::task::JoinError),
    /// A request named something the database has never seen (account, reset).
    #[error("not found: {0}")]
    NotFound(&'static str),
}

type QuotaRead = BoxFuture<'static, Result<Vec<AccountQuota>, Arc<UsageError>>>;

pub struct Recorder {
    fast: SyncSender<Msg>,
    path: PathBuf,
    reads: Arc<pool::ReadPool>,
    quota_read: Mutex<Option<WeakShared<QuotaRead>>>,
    kv: Mutex<Option<rusqlite::Connection>>,
    weights: arc_swap::ArcSwap<QuotaWeights>,
    retention_days: Arc<std::sync::atomic::AtomicU32>,
    prices: Arc<arc_swap::ArcSwap<PriceTable>>,
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
        let conn = schema::open_and_migrate(&cfg.path)?;
        let (tx, rx) = sync_channel(cfg.queue_size.max(1));
        let counters = Arc::new(Counters::default());
        let retention_days = Arc::new(std::sync::atomic::AtomicU32::new(cfg.retention_days));
        let refusals = Arc::new(refusals::Aggregator::default());
        let occupancy = Arc::new(occupancy::Aggregator::default());
        let prices = Arc::new(arc_swap::ArcSwap::from_pointee(PriceTable::default()));
        let w = writer::Writer {
            refusals: refusals.clone(),
            occupancy: occupancy.clone(),
            conn,
            rx,
            batch_size: cfg.batch_size.max(1),
            flush: std::time::Duration::from_millis(cfg.flush_ms),
            retention_days: retention_days.clone(),
            counters: counters.clone(),
            state: crate::write::WriterState::new(instance.to_string()),
            prices: prices.clone(),
        };
        let join = std::thread::Builder::new()
            .name("usage-writer".into())
            .spawn(move || w.run())?;
        Ok(Arc::new(Self {
            fast: tx,
            path: cfg.path.clone(),
            reads: pool::ReadPool::new(cfg.path.clone()),
            quota_read: Mutex::new(None),
            kv: Mutex::new(None),
            weights: arc_swap::ArcSwap::from_pointee(cfg.quota_weights.clone()),
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

    /// Aggregate refusals in memory; the writer flushes periodically and on shutdown.
    pub fn refuse(&self, mut r: Refusal) {
        if let Some(ref mut a) = r.account {
            *a = self.alias_account(a);
        }
        self.refusals.add(r);
    }

    /// Aggregate occupancy in memory; persist completed minutes or an explicit flush.
    pub fn occupancy(&self, s: occupancy::Sample) {
        self.occupancy.add(s);
    }

    /// Occupancy buckets for a range, oldest first, one per bucket and instance.
    pub async fn occupancy_rows(&self, q: OccupancyQuery) -> Result<Vec<OccupancyRow>, UsageError> {
        self.read(move |c| Ok(occupancy::occupancy_rows(c, &q)?))
            .await
    }

    /// Refusal totals by reason for a range (key / instance filters only).
    pub async fn refusal_totals(&self, q: RefusalQuery) -> Result<RefusalTotals, UsageError> {
        self.read(move |c| Ok(refusals::refusal_totals(c, &q)?))
            .await
    }

    /// Per-minute refusal groups, newest first.
    pub async fn refusal_rows(&self, q: RefusalQuery) -> Result<Vec<RefusalRow>, UsageError> {
        self.read(move |c| Ok(refusals::refusal_rows(c, &q)?)).await
    }

    /// Persist management quota observations even when unchanged so freshness stays visible.
    pub fn observe_quota(&self, account: &str, by: &str, q: QuotaObs) -> bool {
        let id = self.ids.next();
        self.fast
            .try_send(Msg::Quota(Box::new(QuotaSnapshot {
                id,
                account: self.alias_account(account),
                observer: by.to_string(),
                obs: q,
            })))
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
        let c = schema::open_read(&self.path)?;
        let al = migrate_v12::load_or_create_aliaser(&c)?;
        self.install_aliaser(al.clone());
        Ok(al)
    }

    /// Attach an aliaser so subsequent writes store `acct_…` instead of raw account ids.
    fn install_aliaser(&self, aliaser: Arc<ciallopa_core::Aliaser>) {
        let _ = self.aliaser.set(aliaser);
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
        self.read(move |c| records::models_since(c, since_ms)).await
    }

    /// Distinct models, key labels and instances of the newest `max_rows` rows, most used
    /// first (see [`ScopeValue`]).
    pub async fn scope_values(&self, max_rows: u32) -> Result<[Vec<ScopeValue>; 3], UsageError> {
        self.read(move |c| records::scope_values(c, max_rows)).await
    }

    /// Hot-reload: quota-capacity estimation weights.
    pub fn set_weights(&self, w: QuotaWeights) {
        self.weights.store(Arc::new(w));
        self.invalidate_quota_read();
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
        self.prices.store(Arc::new(p));
        self.invalidate_quota_read();
    }
    pub fn prices(&self) -> Arc<PriceTable> {
        self.prices.load_full()
    }

    async fn read<T: Send + 'static>(
        &self,
        f: impl FnOnce(&rusqlite::Connection) -> Result<T, UsageError> + Send + 'static,
    ) -> Result<T, UsageError> {
        self.reads.read(f).await
    }

    pub async fn recent(&self, q: RecentQuery) -> Result<Vec<RecordRow>, UsageError> {
        let prices = self.prices();
        self.read(move |c| records::recent(c, &q, &prices)).await
    }

    /// Count ignores the exclusive keyset cursor, as on the HTTP records endpoint.
    pub async fn records_page(&self, q: RecentQuery) -> Result<(u64, Vec<RecordRow>), UsageError> {
        let prices = self.prices();
        self.read(move |c| {
            let total = filter::recent_count(
                c,
                &RecentQuery {
                    before_id: None,
                    ..q.clone()
                },
            )?;
            Ok((total, records::recent(c, &q, &prices)?))
        })
        .await
    }

    /// Metadata for any recorded request, including successes without error detail.
    pub async fn record_by_id(&self, id: i64) -> Result<Option<RecordRow>, UsageError> {
        let prices = self.prices();
        self.read(move |c| records::record(c, id, &prices)).await
    }

    /// Request counts per problem chip and cause; refused is filled by the HTTP caller.
    pub async fn problem_counts(&self, q: RecentQuery) -> Result<ProblemCounts, UsageError> {
        self.read(move |c| filter::problem_counts(c, &q)).await
    }

    pub async fn problem_counts_with_refusals(
        &self,
        q: RecentQuery,
        refusals: Option<RefusalQuery>,
    ) -> Result<ProblemCounts, UsageError> {
        self.read(move |c| {
            let mut counts = filter::problem_counts(c, &q)?;
            if let Some(q) = refusals {
                let totals = crate::refusals::refusal_totals(c, &q)?;
                counts.refused = Some(totals.count);
                counts.causes.insert(
                    "refused",
                    [
                        ("inflight", totals.inflight),
                        ("ws", totals.ws),
                        ("body", totals.body),
                        ("ws_msg", totals.ws_msg),
                    ]
                    .into_iter()
                    .collect(),
                );
            }
            Ok(counts)
        })
        .await
    }

    /// Resolve a pasted session id or unique fragment.
    pub async fn resolve_thread(&self, input: String) -> Result<ThreadMatch, UsageError> {
        self.read(move |c| Ok(filter::resolve_thread(c, &input)?))
            .await
    }

    pub async fn recent_count(&self, q: RecentQuery) -> Result<u64, UsageError> {
        self.read(move |c| filter::recent_count(c, &q)).await
    }

    pub async fn summary(&self, q: SummaryQuery) -> Result<Summary, UsageError> {
        let prices = self.prices();
        self.read(move |c| {
            let mut summary = summary::summary(c, &q, &prices)?;
            summary.price_models = prices.models.len();
            Ok(summary)
        })
        .await
    }

    /// Current and optional previous period, including refusal totals, share one snapshot.
    pub async fn summary_with_refusals(
        &self,
        q: SummaryQuery,
        previous: Option<SummaryQuery>,
    ) -> Result<SummaryPeriods, UsageError> {
        let prices = self.prices();
        self.read(move |c| {
            let period = |q: &SummaryQuery| -> Result<SummaryPeriod, UsageError> {
                let mut summary = summary::summary(c, q, &prices)?;
                summary.price_models = prices.models.len();
                let refusals = if q.filters.account.is_none() && q.filters.model.is_none() {
                    Some(refusals::refusal_totals(
                        c,
                        &RefusalQuery {
                            from_ms: q.from_ms,
                            to_ms: q.to_ms,
                            key: q.filters.key.clone(),
                            instance: q.filters.instance.clone(),
                            ..Default::default()
                        },
                    )?)
                } else {
                    None
                };
                Ok(SummaryPeriod { summary, refusals })
            };
            Ok(SummaryPeriods {
                current: period(&q)?,
                previous: previous.as_ref().map(period).transpose()?,
            })
        })
        .await
    }

    /// Gateway observation by numeric request id; parse external ids at the HTTP edge.
    pub async fn observation(&self, id: i64) -> Result<Option<ObservationRow>, UsageError> {
        self.read(move |c| Ok(observation::read(c, id)?)).await
    }

    pub async fn detail(&self, id: i64) -> Result<Option<DetailRow>, UsageError> {
        self.read(move |c| detail::detail(c, id)).await
    }

    pub async fn quota(&self) -> Result<Vec<AccountQuota>, UsageError> {
        let read = {
            let mut slot = self.quota_read.lock().expect("quota read lock");
            if let Some(read) = slot
                .as_ref()
                .and_then(WeakShared::upgrade)
                .filter(|read| read.peek().is_none())
            {
                read
            } else {
                let (weights, prices) = (self.weights.load_full(), self.prices());
                let pool = self.reads.clone();
                let read = async move {
                    pool.read(move |c| quota::quota(c, &weights, &prices, now_ms()))
                        .await
                        .map_err(Arc::new)
                }
                .boxed()
                .shared();
                // Only waiters own the read, so dropping the last one cancels it.
                *slot = read.downgrade();
                read
            }
        };
        read.await
            .map_err(|e| Arc::try_unwrap(e).unwrap_or_else(UsageError::SharedQuota))
    }

    fn invalidate_quota_read(&self) {
        *self.quota_read.lock().expect("quota read lock") = None;
    }

    /// Start a new estimate epoch for `account` (one window, or both when `None`). Older
    /// snapshots and requests stop counting for those windows; nothing is deleted.
    pub async fn reset_estimate(
        &self,
        account: &str,
        window: Option<WindowKind>,
        by_key: Option<String>,
    ) -> Result<EpochReset, UsageError> {
        let from_id = self.ids.next();
        let op = epoch::Op::Reset {
            account: self.alias_account(account),
            windows: window.map_or(WindowKind::ALL.to_vec(), |w| vec![w]),
            from_id,
            by_key,
        };
        self.epoch(op).await?.reset()
    }

    /// Undo a reset (all windows it covered) within [`UNDO_WINDOW_MS`]; returns rows restored.
    pub async fn undo_estimate_reset(&self, account: &str, epoch: i64) -> Result<u64, UsageError> {
        let op = epoch::Op::Undo {
            account: self.alias_account(account),
            grp: epoch,
        };
        self.epoch(op).await?.undone()
    }

    async fn epoch(&self, op: epoch::Op) -> Result<epoch::Reply, UsageError> {
        let fast = self.fast.clone();
        let reply = tokio::task::spawn_blocking(move || {
            let (tx, rx) = sync_channel(1);
            fast.send(Msg::Epoch(Box::new(epoch::Command { op, reply: tx })))
                .map_err(|_| UsageError::Gone)?;
            rx.recv().map_err(|_| UsageError::Gone)?
        })
        .await??;
        self.invalidate_quota_read();
        Ok(reply)
    }

    pub async fn quota_history(
        &self,
        account: Option<&str>,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<AccountHistory>, UsageError> {
        let (prices, account) = (self.prices(), account.map(str::to_string));
        self.read(move |c| history::history(c, &prices, account.as_deref(), from_ms, to_ms))
            .await
    }
}

pub struct SummaryPeriod {
    pub summary: Summary,
    pub refusals: Option<RefusalTotals>,
}
pub struct SummaryPeriods {
    pub current: SummaryPeriod,
    pub previous: Option<SummaryPeriod>,
}

#[cfg(test)]
mod tests;
