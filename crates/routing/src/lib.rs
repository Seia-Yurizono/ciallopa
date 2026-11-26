//! Account selection with CPA semantics. **Owner: routing worker.**
//!
//! Ports `sdk/cliproxy/auth/selector.go` (RoundRobin / WeightedRoundRobin / FillFirst /
//! SessionAffinity), `session_cache.go`, `session/info.go` and the cooldown rules of
//! `conductor_cooldown.go` (`MarkResult` / `applyAuthFailureState`).
mod cache;
pub mod quota;
pub mod session;

#[cfg(test)]
use quota::is_cloudflare_challenge;
pub use quota::{
    cloudflare_block, is_usage_limit, parse_codex_retry_after, parse_credits, parse_quota,
    parse_quota_at, parse_quota_event_headers, retry_after_for_429, CloudflareBlock, CreditsHeader,
};
#[cfg(test)]
use session::bound_session_identity;
pub use session::extract_session;

pub use cache::AffinityBinding;
use cache::SessionCache;
use ciallopa_core::config::{RebindOn, RoutingConfig, Strategy};
use ciallopa_core::{
    Account, AccountId, Outcome, QuotaSnapshot, RouteError, RouteRequest, Router, Selection,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

const PROVIDER: &str = "codex";
const MAX_KEYS: usize = 4096;
const MAX_WEIGHT: i64 = 1_000_000;
/// conductor_refresh.go constants.
pub const QUOTA_BACKOFF_BASE: Duration = Duration::from_secs(1);
pub const QUOTA_BACKOFF_MAX: Duration = Duration::from_secs(30 * 60);
pub const MIN_QUOTA_COOLDOWN_FLOOR: Duration = Duration::from_secs(10);
/// Default cooldown after `TRANSIENT_STREAK` consecutive upstream 5xx
/// (`[upstream_opts] transient_cooldown_secs`).
pub const TRANSIENT_COOLDOWN: Duration = Duration::from_secs(15);
pub const TRANSIENT_COOLDOWN_MAX: Duration = Duration::from_secs(60);
/// Consecutive upstream 5xx on one account before it is cooled.
pub const TRANSIENT_STREAK: u32 = 3;
pub const AUTH_COOLDOWN: Duration = Duration::from_secs(30 * 60);
/// 404 that explicitly says the model is not supported for the account.
pub const NOT_FOUND_COOLDOWN: Duration = Duration::from_secs(12 * 3600);

pub type Clock = Arc<dyn Fn() -> SystemTime + Send + Sync>;

/// Failure classes that matter for `rebind-on: quota` (CPA `shouldRebindSessionAffinityOnError`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrKind {
    Quota,
    UsageLimit,
    Auth,
    ModelUnsupported,
    NotFound,
    PaymentRequired,
    WorkspaceDeactivated,
    Transient,
    Overloaded,
}
impl ErrKind {
    fn rebinds(self) -> bool {
        self != ErrKind::Transient
    }
    fn reason(self) -> &'static str {
        match self {
            ErrKind::Quota => "rate_limited",
            ErrKind::UsageLimit => "usage_limit",
            ErrKind::Auth => "auth",
            ErrKind::ModelUnsupported => "model_unsupported",
            ErrKind::NotFound => "not_found",
            ErrKind::PaymentRequired => "payment_required",
            ErrKind::WorkspaceDeactivated => "workspace_deactivated",
            ErrKind::Transient => "transient",
            ErrKind::Overloaded => "overloaded",
        }
    }
}

/// Quota backoff is remembered after expiry, until success or a manual reset.
#[derive(Debug, Clone, Copy)]
enum Failure {
    Quota { window_end: SystemTime, level: u32 },
    UsageLimit { window_end: SystemTime, level: u32 },
    Auth,
    ModelUnsupported,
    NotFound,
    PaymentRequired,
    WorkspaceDeactivated,
    Transient,
    Overloaded,
}
impl Failure {
    fn kind(self) -> ErrKind {
        match self {
            Self::Quota { .. } => ErrKind::Quota,
            Self::UsageLimit { .. } => ErrKind::UsageLimit,
            Self::Auth => ErrKind::Auth,
            Self::ModelUnsupported => ErrKind::ModelUnsupported,
            Self::NotFound => ErrKind::NotFound,
            Self::PaymentRequired => ErrKind::PaymentRequired,
            Self::WorkspaceDeactivated => ErrKind::WorkspaceDeactivated,
            Self::Transient => ErrKind::Transient,
            Self::Overloaded => ErrKind::Overloaded,
        }
    }
    fn quota_window(self) -> Option<(SystemTime, u32)> {
        match self {
            Self::Quota { window_end, level } | Self::UsageLimit { window_end, level } => {
                Some((window_end, level))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
enum Health {
    #[default]
    Healthy,
    Cooling {
        until: SystemTime,
        failure: Failure,
    },
    Remembered(Failure),
}
impl Health {
    fn failure(&self) -> Option<Failure> {
        match self {
            Self::Healthy => None,
            Self::Cooling { failure, .. } | Self::Remembered(failure) => Some(*failure),
        }
    }
    fn live_until(&self, now: SystemTime) -> Option<SystemTime> {
        match self {
            Self::Cooling { until, .. } if *until > now => Some(*until),
            _ => None,
        }
    }
    fn expire(&mut self, now: SystemTime) {
        if let Self::Cooling { until, failure } = self {
            if *until <= now {
                *self = Self::Remembered(*failure);
            }
        }
    }
    /// Never shorten a live cooldown. Quota advice must extend it to replace its reason.
    fn merge_longer(&mut self, now: SystemTime, until: SystemTime, mut failure: Failure) {
        self.expire(now);
        let previous = self.live_until(now);
        if previous.is_some_and(|old| old >= until) && failure.quota_window().is_some() {
            return;
        }
        let until = previous.map_or(until, |old| old.max(until));
        match &mut failure {
            Failure::Quota { window_end, .. } | Failure::UsageLimit { window_end, .. } => {
                *window_end = until
            }
            _ => {}
        }
        *self = Self::Cooling { until, failure };
    }
    fn on_rate_limit(&mut self, now: SystemTime, hint: Option<Duration>, usage: bool) {
        let previous = self.failure().and_then(Failure::quota_window);
        let level = previous.map_or(0, |(_, level)| level);
        let (until, level) = if let Some(d) = hint {
            (now + d.max(MIN_QUOTA_COOLDOWN_FLOOR), level)
        } else if let Some((end, level)) = previous.filter(|(end, _)| *end > now) {
            (end, level)
        } else {
            let (delay, level) = next_quota_cooldown(level);
            (now + delay, level)
        };
        let failure = if usage {
            Failure::UsageLimit {
                window_end: until,
                level,
            }
        } else {
            Failure::Quota {
                window_end: until,
                level,
            }
        };
        self.merge_longer(now, until, failure);
    }
    fn clear_quota(&mut self, now: SystemTime) -> bool {
        if !matches!(self.failure(), Some(Failure::UsageLimit { .. })) {
            return false;
        }
        let live = self.live_until(now).is_some();
        *self = Self::Healthy;
        live
    }
}

#[derive(Default)]
struct OverloadState {
    streak: u32,
    level: u32,
}

#[derive(Default)]
struct AccountState {
    health: Health,
    models: HashMap<String, Health>,
    quota: Option<QuotaSnapshot>,
    server_streak: u32,
    overload: OverloadState,
    /// (hour index, attempts, overloaded), last 24 h (memory only).
    stats: [(u64, u64, u64); 24],
    cooled_at: Option<SystemTime>,
}
impl AccountState {
    fn bump(&mut self, now: SystemTime, attempts: u64, overloaded: u64) {
        let h = hour_of(now);
        let slot = &mut self.stats[(h % 24) as usize];
        if slot.0 != h {
            *slot = (h, 0, 0);
        }
        slot.1 += attempts;
        slot.2 += overloaded;
    }
    fn cooldowns(&self) -> impl Iterator<Item = &Health> {
        std::iter::once(&self.health).chain(self.models.values())
    }
    fn on_success(&mut self, model: &str, sent_at: Option<SystemTime>) -> bool {
        if sent_at
            .zip(self.cooled_at)
            .is_some_and(|(sent, cooled)| cooled >= sent)
        {
            return false;
        }
        self.health = Health::Healthy;
        self.models.remove(model);
        self.server_streak = 0;
        true
    }
    fn clear_cooldowns(&mut self, now: SystemTime) -> bool {
        let live = self.cooldowns().any(|s| s.live_until(now).is_some());
        self.health = Health::Healthy;
        self.models.clear();
        self.server_streak = 0;
        self.overload = OverloadState::default();
        self.cooled_at = None;
        live
    }
}

#[derive(Default)]
struct SwState {
    current: HashMap<String, i64>,
    weights: HashMap<String, i64>,
}

struct State {
    rr_last: HashMap<String, String>,
    wrr: HashMap<String, SwState>,
    cache: SessionCache,
    accounts: HashMap<AccountId, AccountState>,
}

/// Both result reporting and selection use the same alias/fork policy.
struct BindingKeys {
    primary: String,
    fallback: Option<String>,
    aliased: bool,
    inherits_parent: bool,
}
impl BindingKeys {
    fn new(req: &RouteRequest, model: &str, cfg: &RoutingConfig) -> Option<Self> {
        let session = &req.session;
        let primary = session
            .key
            .as_deref()
            .filter(|k| !k.is_empty() && cfg.session_affinity)?;
        let fallback = session
            .parent_key
            .as_deref()
            .filter(|k| !k.is_empty() && *k != primary)
            .map(|k| format!("{PROVIDER}::{k}::{model}"));
        Some(Self {
            primary: format!("{PROVIDER}::{primary}::{model}"),
            fallback,
            aliased: !session.is_subagent && !session.is_fork,
            inherits_parent: session.is_fork
                || !session.is_subagent
                || cfg.session_affinity_subagents,
        })
    }
    fn bind(&self, cache: &mut SessionCache, id: &str, now: SystemTime) {
        if let Some(fallback) = self.fallback.as_ref().filter(|_| self.aliased) {
            cache.set_aliases(id, &[self.primary.clone(), fallback.clone()], now);
        } else {
            cache.set(&self.primary, id, now);
        }
    }
    fn reported_keys(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.primary.as_str())
            .chain(self.fallback.as_deref().filter(|_| self.aliased))
    }
}

fn hour_of(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() / 3600)
        .unwrap_or(0)
}

pub struct CpaRouter {
    cfg: arc_swap::ArcSwap<RoutingConfig>,
    clock: Clock,
    st: Mutex<State>,
}

/// CPA `canonicalModelKey`: trim and strip a trailing `(thinking)` suffix.
pub fn canonical_model_key(model: &str) -> String {
    let m = model.trim();
    if let (Some(open), true) = (m.rfind('('), m.ends_with(')')) {
        let name = m[..open].trim();
        if !name.is_empty() {
            return name.to_string();
        }
    }
    m.to_string()
}

fn eff_weight(a: &Account) -> i64 {
    if a.weight > MAX_WEIGHT {
        0
    } else {
        a.weight
    }
}

/// Whether `select` considers the account at all (before cooldowns): enabled, not excluded
/// and, under weighted round robin, with a positive weight.
fn eligible(a: &Account, exclude: &[AccountId], weighted: bool) -> bool {
    !a.disabled && !exclude.contains(&a.id) && (!weighted || eff_weight(a) > 0)
}

/// CPA `nextQuotaCooldown`.
fn next_quota_cooldown(prev: u32) -> (Duration, u32) {
    let c = QUOTA_BACKOFF_BASE.saturating_mul(1u32.checked_shl(prev).unwrap_or(u32::MAX));
    if c >= QUOTA_BACKOFF_MAX {
        (QUOTA_BACKOFF_MAX, prev)
    } else {
        (c, prev + 1)
    }
}

impl CpaRouter {
    pub fn new(cfg: RoutingConfig) -> Self {
        Self::with_clock(cfg, Arc::new(SystemTime::now))
    }

    pub fn with_clock(cfg: RoutingConfig, clock: Clock) -> Self {
        let ttl = cfg
            .ttl()
            .ok()
            .filter(|d| !d.is_zero())
            .unwrap_or(Duration::from_secs(24 * 3600));
        Self {
            cfg: arc_swap::ArcSwap::from_pointee(cfg),
            clock,
            st: Mutex::new(State {
                rr_last: HashMap::new(),
                wrr: HashMap::new(),
                cache: SessionCache::new(ttl, cache::DEFAULT_MAX_ENTRIES),
                accounts: HashMap::new(),
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.st.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Whether `select` could pick another of `accounts` than `current` for `req` now and it
    /// passes `extra`. Read-only: no cooldown expiry, binding or rotation change.
    fn any_other(
        &self,
        req: &RouteRequest,
        current: &AccountId,
        accounts: &[Account],
        extra: impl Fn(&State, &AccountId, SystemTime) -> bool,
    ) -> bool {
        let now = (self.clock)();
        let model = canonical_model_key(&req.model);
        let weighted = self.cfg.load().strategy == Strategy::WeightedRoundRobin;
        let st = self.lock();
        accounts.iter().any(|a| {
            a.id != *current
                && eligible(a, &req.exclude, weighted)
                && blocked(&st, &a.id, &model, now).is_none()
                && extra(&st, &a.id, now)
        })
    }

    /// Last quota snapshot per account (for `status`).
    pub fn quota_snapshot(&self, id: &AccountId) -> Option<QuotaSnapshot> {
        self.lock().accounts.get(id).and_then(|a| a.quota.clone())
    }

    /// Changes whenever session bindings change (persistence writes only then).
    pub fn affinity_generation(&self) -> u64 {
        self.lock().cache.generation()
    }

    /// Live session bindings, oldest first, for persisting across restarts.
    pub fn affinity_export(&self) -> Vec<AffinityBinding> {
        let now = (self.clock)();
        self.lock().cache.export(now)
    }

    /// Restore persisted bindings at startup: expired ones and ones whose account `known`
    /// rejects are dropped. Returns the number restored.
    pub fn affinity_import(
        &self,
        bindings: Vec<AffinityBinding>,
        known: &dyn Fn(&str) -> bool,
    ) -> usize {
        let now = (self.clock)();
        self.lock().cache.import(bindings, now, known)
    }

    fn pick_strategy(
        st: &mut State,
        strategy: Strategy,
        model: &str,
        avail: &[&Account],
    ) -> Option<AccountId> {
        if avail.is_empty() {
            return None;
        }
        let key = format!("{PROVIDER}:{model}");
        match strategy {
            Strategy::FillFirst => Some(avail[0].id.clone()),
            Strategy::RoundRobin => {
                if !st.rr_last.contains_key(&key) && st.rr_last.len() >= MAX_KEYS {
                    st.rr_last.clear();
                }
                let last = st.rr_last.get(&key).cloned().unwrap_or_default();
                let idx = if last.is_empty() {
                    0
                } else {
                    let i = avail.partition_point(|a| a.id <= last);
                    if i >= avail.len() {
                        0
                    } else {
                        i
                    }
                };
                let id = avail[idx].id.clone();
                st.rr_last.insert(key, id.clone());
                Some(id)
            }
            Strategy::WeightedRoundRobin => {
                if !st.wrr.contains_key(&key) && st.wrr.len() >= MAX_KEYS {
                    st.wrr.clear();
                }
                let s = st.wrr.entry(key).or_default();
                let weights: HashMap<String, i64> = avail
                    .iter()
                    .filter(|a| eff_weight(a) > 0)
                    .map(|a| (a.id.clone(), eff_weight(a)))
                    .collect();
                // prepare(): reset credits only when a configured weight changed.
                let changed = !s.weights.is_empty()
                    && weights
                        .iter()
                        .any(|(k, w)| s.weights.get(k).is_some_and(|p| p != w));
                if changed {
                    s.current.clear();
                }
                s.weights
                    .extend(weights.iter().map(|(k, v)| (k.clone(), *v)));
                if s.current.len() > 1024 || s.weights.len() > 1024 {
                    s.current.retain(|k, _| weights.contains_key(k));
                    s.weights.retain(|k, _| weights.contains_key(k));
                }
                let mut picked: Option<(&str, i64)> = None;
                let mut total: i64 = 0;
                for a in avail {
                    let w = eff_weight(a);
                    if w <= 0 {
                        continue;
                    }
                    let c = s.current.entry(a.id.clone()).or_insert(0);
                    *c = c.saturating_add(w);
                    total = total.saturating_add(w);
                    if picked.is_none_or(|(_, pc)| *c > pc) {
                        picked = Some((&a.id, *c));
                    }
                }
                let (id, _) = picked?;
                let id = id.to_string();
                if let Some(c) = s.current.get_mut(&id) {
                    *c = c.saturating_sub(total);
                }
                Some(id)
            }
        }
    }
}

/// Error kind of the state that blocks the account the longest.
fn blocked_kind(st: &State, id: &AccountId, model: &str, now: SystemTime) -> Option<ErrKind> {
    let a = st.accounts.get(id)?;
    std::iter::once(&a.health)
        .chain(a.models.get(model))
        .filter_map(|s| Some((s.live_until(now)?, s.failure()?.kind())))
        .max_by_key(|(t, _)| *t)
        .map(|(_, k)| k)
}

/// Last observed used fraction of the account's fullest live window (0 = unknown).
fn used(st: &State, id: &str, now: SystemTime) -> f64 {
    st.accounts
        .get(id)
        .and_then(|a| a.quota.as_ref())
        .map_or(0.0, |q| quota::max_used_fraction(q, now))
}

fn blocked(st: &State, id: &AccountId, model: &str, now: SystemTime) -> Option<SystemTime> {
    let a = st.accounts.get(id)?;
    std::iter::once(&a.health)
        .chain(a.models.get(model))
        .filter_map(|s| s.live_until(now))
        .max()
}

/// Expired auth/payment failures still prevent preserving an unavailable binding.
fn blocked_by_quota_or_auth(st: &State, a: &Account, model: &str, now: SystemTime) -> bool {
    if a.disabled {
        return true;
    }
    let Some(state) = st.accounts.get(&a.id) else {
        return false;
    };
    match state.health.failure().map(Failure::kind) {
        Some(ErrKind::Auth | ErrKind::PaymentRequired | ErrKind::WorkspaceDeactivated) => {
            return true
        }
        Some(ErrKind::UsageLimit) if state.health.live_until(now).is_some() => return true,
        _ => {}
    }
    state
        .models
        .get(model)
        .and_then(Health::failure)
        .is_some_and(|f| f.kind().rebinds())
}

impl Router for CpaRouter {
    fn update_config(&self, cfg: &RoutingConfig) {
        if let Some(ttl) = cfg.ttl().ok().filter(|d| !d.is_zero()) {
            self.lock().cache.set_ttl(ttl);
        }
        self.cfg.store(Arc::new(cfg.clone()));
    }
    fn forget_account(&self, id: &AccountId) {
        let mut st = self.lock();
        st.accounts.remove(id);
        st.cache.invalidate_auth(id);
        st.rr_last.retain(|_, v| v != id);
        for state in st.wrr.values_mut() {
            state.current.remove(id);
            state.weights.remove(id);
        }
    }
    fn cooling_until(&self, id: &AccountId) -> Option<SystemTime> {
        let now = (self.clock)();
        self.lock()
            .accounts
            .get(id)?
            .cooldowns()
            .filter_map(|s| s.live_until(now))
            .max()
    }
    fn cooling_reason(&self, id: &AccountId) -> Option<&'static str> {
        let now = (self.clock)();
        self.lock()
            .accounts
            .get(id)?
            .cooldowns()
            .filter_map(|s| Some((s.live_until(now)?, s.failure()?.kind())))
            .max_by_key(|(until, _)| *until)
            .map(|(_, k)| k.reason())
    }
    fn model_cooling_until(&self, id: &AccountId, model: &str) -> Option<SystemTime> {
        let now = (self.clock)();
        let model = canonical_model_key(model);
        blocked(&self.lock(), id, &model, now)
    }
    fn usable_other(&self, req: &RouteRequest, current: &AccountId, accounts: &[Account]) -> bool {
        self.any_other(req, current, accounts, |_, _, _| true)
    }
    fn prefer_other(&self, req: &RouteRequest, current: &AccountId, accounts: &[Account]) -> bool {
        self.any_other(req, current, accounts, |st, id, now| {
            used(st, current, now) >= quota::FULL_FRACTION
                && used(st, id, now) < quota::FULL_FRACTION
        })
    }
    fn clear_cooldown(&self, id: &AccountId) -> bool {
        let now = (self.clock)();
        self.lock()
            .accounts
            .get_mut(id)
            .is_some_and(|a| a.clear_cooldowns(now))
    }
    fn clear_quota_cooldown(&self, id: &AccountId) -> bool {
        let now = (self.clock)();
        let mut st = self.lock();
        let Some(a) = st.accounts.get_mut(id) else {
            return false;
        };
        let mut live = a.health.clear_quota(now);
        a.models.retain(|_, s| {
            live |= s.clear_quota(now);
            !matches!(s, Health::Healthy)
        });
        live
    }
    fn note_attempt(&self, id: &AccountId) {
        let now = (self.clock)();
        self.lock()
            .accounts
            .entry(id.clone())
            .or_default()
            .bump(now, 1, 0);
    }
    fn content_ok(&self, id: &AccountId) {
        if let Some(a) = self.lock().accounts.get_mut(id) {
            a.overload = OverloadState::default();
        }
    }
    fn overload_stats(&self, id: &AccountId) -> (u64, u64) {
        let h = hour_of((self.clock)());
        self.lock().accounts.get(id).map_or((0, 0), |a| {
            a.stats
                .iter()
                .filter(|s| s.0 + 24 > h && s.0 <= h)
                .fold((0, 0), |a, s| (a.0 + s.1, a.1 + s.2))
        })
    }
    fn select(&self, req: &RouteRequest, accounts: &[Account]) -> Result<Selection, RouteError> {
        let now = (self.clock)();
        let model = canonical_model_key(&req.model);
        let cfg = self.cfg.load_full();
        let keys = BindingKeys::new(req, &model, &cfg);
        let weighted = cfg.strategy == Strategy::WeightedRoundRobin;
        let by_id: HashMap<&str, &Account> = accounts.iter().map(|a| (a.id.as_str(), a)).collect();

        let mut cands: Vec<&Account> = accounts
            .iter()
            .filter(|a| eligible(a, &req.exclude, weighted))
            .collect();
        if cands.is_empty() {
            return Err(RouteError::NoAccounts);
        }
        cands.sort_by(|a, b| a.id.cmp(&b.id));
        let mut st = self.lock();
        for a in &cands {
            if let Some(state) = st.accounts.get_mut(&a.id) {
                state.health.expire(now);
                if let Some(health) = state.models.get_mut(&model) {
                    health.expire(now);
                }
            }
        }
        let mut earliest: Option<(SystemTime, &AccountId)> = None;
        let available: Vec<&Account> = cands
            .iter()
            .copied()
            .filter(|a| match blocked(&st, &a.id, &model, now) {
                Some(t) => {
                    if earliest.is_none_or(|(e, _)| t < e) {
                        earliest = Some((t, &a.id));
                    }
                    false
                }
                None => true,
            })
            .collect();
        if available.is_empty() {
            let retry_after = earliest
                .and_then(|(e, _)| e.duration_since(now).ok())
                .unwrap_or_default();
            let is_quota =
                |id: &AccountId| blocked_kind(&st, id, &model, now) == Some(ErrKind::UsageLimit);
            let quota = cands.iter().all(|a| is_quota(&a.id));
            let earliest_quota = earliest.is_some_and(|(_, id)| is_quota(id));
            return Err(RouteError::AllCoolingDown {
                retry_after,
                quota,
                earliest_quota,
            });
        }

        // Cold pick: avoid accounts over the quota threshold, then full ones (a full window
        // keeps serving from purchased credits: last resort), then highest priority tier.
        let thr = cfg.quota_avoid_threshold;
        let limits: Vec<f64> = [thr, quota::FULL_FRACTION]
            .into_iter()
            .filter(|l| *l > 0.0 && *l <= quota::FULL_FRACTION)
            .collect();
        let cold = |st: &mut State| -> Option<AccountId> {
            let filtered: Vec<&Account>;
            let mut pool = available.as_slice();
            if let Some(below) = limits
                .iter()
                .map(|l| {
                    pool.iter()
                        .copied()
                        .filter(|a| used(st, &a.id, now) < *l)
                        .collect::<Vec<_>>()
                })
                .find(|p| !p.is_empty())
            {
                filtered = below;
                pool = &filtered;
            }
            let best = pool.iter().map(|a| a.priority).max()?;
            let tier: Vec<&Account> = pool
                .iter()
                .copied()
                .filter(|a| a.priority == best)
                .collect();
            Self::pick_strategy(st, cfg.strategy, &model, &tier)
        };
        // A bound session leaves a full account when another available one is not full.
        let moving = |st: &State, id: &str| {
            used(st, id, now) >= quota::FULL_FRACTION
                && available
                    .iter()
                    .any(|a| used(st, &a.id, now) < quota::FULL_FRACTION)
        };
        let fresh = |account: AccountId| {
            Ok(Selection {
                account,
                from_binding: false,
            })
        };

        let Some(keys) = keys else {
            return cold(&mut st)
                .map(fresh)
                .unwrap_or(Err(RouteError::NoAccounts));
        };
        let preserve_quota = cfg.session_affinity_rebind_on == RebindOn::Quota;
        let bind = |st: &mut State, id: &str| keys.bind(&mut st.cache, id, now);
        let preserve = |st: &State, id: &str| {
            preserve_quota
                && by_id
                    .get(id)
                    .is_some_and(|a| !blocked_by_quota_or_auth(st, a, &model, now))
        };
        let in_avail = |id: &str| {
            available
                .binary_search_by(|a| a.id.as_str().cmp(id))
                .is_ok()
        };

        if let Some(cached) = st.cache.get_and_refresh(&keys.primary, now) {
            let leave = in_avail(&cached) && moving(&st, &cached);
            if in_avail(&cached) && !leave {
                bind(&mut st, &cached);
                return Ok(Selection {
                    account: cached,
                    from_binding: true,
                });
            }
            let Some(pick) = cold(&mut st) else {
                return Err(RouteError::NoAccounts);
            };
            if !leave && preserve(&st, &cached) {
                bind(&mut st, &cached);
            } else {
                bind(&mut st, &pick);
            }
            return fresh(pick);
        }
        if let Some(fk) = &keys.fallback {
            if let Some(cached) = st.cache.get(fk, now) {
                let leave = in_avail(&cached) && moving(&st, &cached);
                if in_avail(&cached) && keys.inherits_parent && !leave {
                    bind(&mut st, &cached);
                    return Ok(Selection {
                        account: cached,
                        from_binding: true,
                    });
                }
                if keys.aliased && !leave && preserve(&st, &cached) {
                    let Some(pick) = cold(&mut st) else {
                        return Err(RouteError::NoAccounts);
                    };
                    bind(&mut st, &cached);
                    return fresh(pick);
                }
            }
        }
        let Some(pick) = cold(&mut st) else {
            return Err(RouteError::NoAccounts);
        };
        bind(&mut st, &pick);
        fresh(pick)
    }

    fn report(&self, req: &RouteRequest, account: &AccountId, outcome: &Outcome) {
        let now = (self.clock)();
        let cfg = self.cfg.load_full();
        let model = canonical_model_key(&req.model);
        let keys = BindingKeys::new(req, &model, &cfg);
        // These failures never cool credentials or change bindings.
        if matches!(
            outcome,
            Outcome::Transport | Outcome::ClientError { .. } | Outcome::EgressBlocked
        ) {
            return;
        }
        let mut st = self.lock();
        let a = st.accounts.entry(account.clone()).or_default();
        let kind = match outcome {
            Outcome::Success { quota } => {
                if let Some(q) = quota {
                    a.quota = Some(q.clone());
                }
                if !a.on_success(&model, req.attempt_at) {
                    return;
                }
                None
            }
            Outcome::RateLimited {
                retry_after,
                usage_limit,
                quota,
            } => {
                if let Some(q) = quota {
                    a.quota = Some(q.clone());
                }
                if *usage_limit {
                    a.health.on_rate_limit(now, *retry_after, true);
                    let until = a.health.live_until(now).expect("new usage cooldown");
                    let model = a.models.entry(model).or_default();
                    let level = model
                        .failure()
                        .and_then(Failure::quota_window)
                        .map_or(0, |(_, level)| level);
                    model.merge_longer(
                        now,
                        until,
                        Failure::UsageLimit {
                            window_end: until,
                            level,
                        },
                    );
                    Some(ErrKind::UsageLimit)
                } else {
                    a.models
                        .entry(model)
                        .or_default()
                        .on_rate_limit(now, *retry_after, false);
                    Some(ErrKind::Quota)
                }
            }
            Outcome::AuthFailed => {
                a.health
                    .merge_longer(now, now + AUTH_COOLDOWN, Failure::Auth);
                Some(ErrKind::Auth)
            }
            Outcome::ModelUnsupported { retry_after } => {
                let delay = retry_after
                    .filter(|d| !d.is_zero())
                    .unwrap_or(NOT_FOUND_COOLDOWN);
                a.models.entry(model).or_default().merge_longer(
                    now,
                    now + delay,
                    Failure::ModelUnsupported,
                );
                Some(ErrKind::ModelUnsupported)
            }
            Outcome::NotFound {
                retry_after,
                cooldown,
            } => {
                let d = retry_after.filter(|d| !d.is_zero()).unwrap_or(*cooldown);
                if d.is_zero() {
                    return;
                }
                a.models
                    .entry(model)
                    .or_default()
                    .merge_longer(now, now + d, Failure::NotFound);
                Some(ErrKind::NotFound)
            }
            Outcome::PaymentRequired {
                deactivated,
                cooldown,
            } => {
                if cooldown.is_zero() {
                    return;
                }
                let failure = if *deactivated {
                    Failure::WorkspaceDeactivated
                } else {
                    Failure::PaymentRequired
                };
                a.health.merge_longer(now, now + *cooldown, failure);
                Some(failure.kind())
            }
            Outcome::Overloaded {
                streak,
                base,
                max,
                cool,
                retry_after,
            } => {
                a.bump(now, 0, 1);
                let e = &mut a.overload;
                e.streak = e.streak.saturating_add(1);
                if !*cool || *streak == 0 || base.is_zero() || e.streak < *streak {
                    return;
                }
                let step = base.saturating_mul(1u32.checked_shl(e.level).unwrap_or(u32::MAX));
                let d = retry_after
                    .map_or(step, |ra| step.max(ra))
                    .min((*max).max(*base));
                e.streak = 0;
                e.level = e.level.saturating_add(1).min(16);
                a.health.merge_longer(now, now + d, Failure::Overloaded);
                Some(ErrKind::Overloaded)
            }
            Outcome::ServerError { cooldown, .. } => {
                a.server_streak = a.server_streak.saturating_add(1);
                let Some(d) = cooldown
                    .filter(|d| !d.is_zero() && a.server_streak >= TRANSIENT_STREAK)
                    .map(|d| d.min(TRANSIENT_COOLDOWN_MAX))
                else {
                    return;
                };
                a.server_streak = 0;
                a.models
                    .entry(model)
                    .or_default()
                    .merge_longer(now, now + d, Failure::Transient);
                Some(ErrKind::Transient)
            }
            Outcome::Transport | Outcome::ClientError { .. } | Outcome::EgressBlocked => {
                unreachable!()
            }
        };
        if kind.is_some() {
            a.cooled_at = Some(now);
        }
        let Some(keys) = keys else {
            return;
        };
        if kind.is_some_and(|k| cfg.session_affinity_rebind_on == RebindOn::Quota && !k.rebinds()) {
            return;
        }
        for key in keys.reported_keys() {
            if kind.is_none() {
                st.cache.touch(key, account, now);
            } else {
                st.cache.compare_and_delete(key, account);
            }
        }
    }
}

#[cfg(test)]
mod tests;
