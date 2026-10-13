//! Account selection with CPA semantics. **Owner: routing worker.**
//!
//! Ports `sdk/cliproxy/auth/selector.go` (RoundRobin / WeightedRoundRobin / FillFirst /
//! SessionAffinity), `session_cache.go`, `session/info.go` and the cooldown rules of
//! `conductor_cooldown.go` (`MarkResult` / `applyAuthFailureState`).
mod cache;
pub mod quota;
pub mod session;

pub use quota::{
    is_cloudflare_challenge, is_usage_limit, parse_codex_retry_after, parse_quota, parse_quota_at,
    parse_quota_event_headers, parse_retry_after_header, retry_after_for_429,
};
pub use session::{bound_session_identity, extract_session, normalize_explicit_id};

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
pub const TRANSIENT_COOLDOWN: Duration = Duration::from_secs(60);
pub const AUTH_COOLDOWN: Duration = Duration::from_secs(30 * 60);
pub const NOT_FOUND_COOLDOWN: Duration = Duration::from_secs(12 * 3600);
pub const CHALLENGE_MIN_COOLDOWN: Duration = Duration::from_secs(10);

pub type Clock = Arc<dyn Fn() -> SystemTime + Send + Sync>;

/// Failure classes that matter for `rebind-on: quota` (CPA `shouldRebindSessionAffinityOnError`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrKind {
    Quota,
    UsageLimit,
    Auth,
    ModelUnsupported,
    Challenge,
    Transient,
}
impl ErrKind {
    fn rebinds(self) -> bool {
        self != ErrKind::Transient
    }
}

#[derive(Debug, Clone, Default)]
struct CoolState {
    next_retry: Option<SystemTime>,
    /// Quota window (`Quota.NextRecoverAt`) — used for "advance backoff once per window".
    recover_at: Option<SystemTime>,
    backoff_level: u32,
    last_err: Option<ErrKind>,
}
impl CoolState {
    fn live_until(&self, now: SystemTime) -> Option<SystemTime> {
        self.next_retry.filter(|t| *t > now)
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
    account: HashMap<AccountId, CoolState>,
    model: HashMap<(AccountId, String), CoolState>,
    quota: HashMap<AccountId, QuotaSnapshot>,
}

pub struct CpaRouter {
    cfg: RoutingConfig,
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

/// CPA `nextQuotaCooldown`.
fn next_quota_cooldown(prev: u32) -> (Duration, u32) {
    let c = QUOTA_BACKOFF_BASE.saturating_mul(1u32.checked_shl(prev).unwrap_or(u32::MAX));
    if c >= QUOTA_BACKOFF_MAX {
        (QUOTA_BACKOFF_MAX, prev)
    } else {
        (c, prev + 1)
    }
}

/// CPA `quotaCooldownAfterFailure`: reuse an open window instead of escalating.
fn quota_after_failure(s: &CoolState, now: SystemTime) -> (SystemTime, u32) {
    if let Some(r) = s.recover_at.filter(|r| *r > now) {
        return (r, s.backoff_level);
    }
    let (c, lvl) = next_quota_cooldown(s.backoff_level);
    (now + c, lvl)
}

fn max_opt(a: Option<SystemTime>, b: SystemTime) -> SystemTime {
    match a {
        Some(a) if a > b => a,
        _ => b,
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
            .unwrap_or(Duration::from_secs(3600));
        Self {
            cfg,
            clock,
            st: Mutex::new(State {
                rr_last: HashMap::new(),
                wrr: HashMap::new(),
                cache: SessionCache::new(ttl, cache::DEFAULT_MAX_ENTRIES),
                account: HashMap::new(),
                model: HashMap::new(),
                quota: HashMap::new(),
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.st.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Last quota snapshot per account (for `status`).
    pub fn quota_snapshot(&self, id: &AccountId) -> Option<QuotaSnapshot> {
        self.lock().quota.get(id).cloned()
    }

    /// Earliest time the account is usable again for `model` (None = available).
    pub fn cooldown_until(&self, id: &AccountId, model: &str) -> Option<SystemTime> {
        let now = (self.clock)();
        blocked(&self.lock(), id, &canonical_model_key(model), now)
    }

    /// Drop all session bindings of an account (e.g. account removed).
    pub fn invalidate_account(&self, id: &AccountId) {
        self.lock().cache.invalidate_auth(id);
    }

    fn pick_strategy(&self, st: &mut State, model: &str, avail: &[&Account]) -> Option<AccountId> {
        if avail.is_empty() {
            return None;
        }
        let key = format!("{PROVIDER}:{model}");
        match self.cfg.strategy {
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

fn blocked(st: &State, id: &AccountId, model: &str, now: SystemTime) -> Option<SystemTime> {
    let a = st.account.get(id).and_then(|s| s.live_until(now));
    let m = st
        .model
        .get(&(id.clone(), model.to_string()))
        .and_then(|s| s.live_until(now));
    match (a, m) {
        (Some(a), Some(m)) => Some(a.max(m)),
        (x, y) => x.or(y),
    }
}

/// CPA `isAuthBlockedByQuotaOrAuthForModel` (drives `shouldPreserveBindingWhenUnavailable`).
fn blocked_by_quota_or_auth(st: &State, a: &Account, model: &str, now: SystemTime) -> bool {
    if a.disabled {
        return true;
    }
    if let Some(s) = st.account.get(&a.id) {
        match s.last_err {
            Some(ErrKind::Auth) => return true,
            Some(ErrKind::UsageLimit) if s.live_until(now).is_some() => return true,
            _ => {}
        }
    }
    if let Some(s) = st.model.get(&(a.id.clone(), model.to_string())) {
        return s.last_err.is_some_and(ErrKind::rebinds);
    }
    false
}

impl Router for CpaRouter {
    fn select(&self, req: &RouteRequest, accounts: &[Account]) -> Result<Selection, RouteError> {
        let now = (self.clock)();
        let model = canonical_model_key(&req.model);
        let mut st = self.lock();
        let weighted = self.cfg.strategy == Strategy::WeightedRoundRobin;

        let mut cands: Vec<&Account> = accounts
            .iter()
            .filter(|a| {
                !a.disabled && !req.exclude.contains(&a.id) && (!weighted || eff_weight(a) > 0)
            })
            .collect();
        if cands.is_empty() {
            return Err(RouteError::NoAccounts);
        }
        cands.sort_by(|a, b| a.id.cmp(&b.id));
        let mut earliest: Option<SystemTime> = None;
        let available: Vec<&Account> = cands
            .iter()
            .copied()
            .filter(|a| match blocked(&st, &a.id, &model, now) {
                Some(t) => {
                    earliest = Some(earliest.map_or(t, |e| e.min(t)));
                    false
                }
                None => true,
            })
            .collect();
        if available.is_empty() {
            let retry_after = earliest
                .and_then(|e| e.duration_since(now).ok())
                .unwrap_or_default();
            return Err(RouteError::AllCoolingDown { retry_after });
        }

        // Cold pick: quota-threshold avoidance (new bindings only), then highest priority tier.
        let thr = self.cfg.quota_avoid_threshold;
        let cold = |st: &mut State| -> Option<AccountId> {
            let mut pool: Vec<&Account> = available.clone();
            if thr > 0.0 {
                let ok: Vec<&Account> = pool
                    .iter()
                    .copied()
                    .filter(|a| {
                        st.quota
                            .get(&a.id)
                            .is_none_or(|q| quota::max_used_fraction(q, now) < thr)
                    })
                    .collect();
                if !ok.is_empty() {
                    pool = ok;
                }
            }
            let best = pool.iter().map(|a| a.priority).max()?;
            let tier: Vec<&Account> = pool.into_iter().filter(|a| a.priority == best).collect();
            self.pick_strategy(st, &model, &tier)
        };
        let fresh = |account: AccountId| {
            Ok(Selection {
                account,
                from_binding: false,
            })
        };

        let primary = match (&req.session.key, self.cfg.session_affinity) {
            (Some(k), true) if !k.is_empty() => k.clone(),
            _ => {
                return cold(&mut st)
                    .map(fresh)
                    .unwrap_or(Err(RouteError::NoAccounts))
            }
        };
        let fallback = req.session.parent_key.clone().unwrap_or_default();
        let is_fork = req.session.is_fork;
        let is_sub = !is_fork && session::is_subagent_session(&primary, &fallback);
        let cache_key = format!("{PROVIDER}::{primary}::{model}");
        let fallback_key = (!fallback.is_empty() && fallback != primary)
            .then(|| format!("{PROVIDER}::{fallback}::{model}"));
        let preserve_quota = self.cfg.session_affinity_rebind_on == RebindOn::Quota;
        let bind = |st: &mut State, id: &str| match (&fallback_key, is_sub || is_fork) {
            (Some(fk), false) => st
                .cache
                .set_aliases(id, &[cache_key.clone(), fk.clone()], now),
            _ => st.cache.set(&cache_key, id, now),
        };
        let preserve = |st: &State, id: &str| {
            preserve_quota
                && accounts
                    .iter()
                    .find(|a| a.id == id)
                    .is_some_and(|a| !blocked_by_quota_or_auth(st, a, &model, now))
        };
        let in_avail = |id: &str| available.iter().any(|a| a.id == id);

        if let Some(cached) = st.cache.get_and_refresh(&cache_key, now) {
            if in_avail(&cached) {
                bind(&mut st, &cached);
                return Ok(Selection {
                    account: cached,
                    from_binding: true,
                });
            }
            let Some(pick) = cold(&mut st) else {
                return Err(RouteError::NoAccounts);
            };
            if preserve(&st, &cached) {
                bind(&mut st, &cached);
            } else {
                bind(&mut st, &pick);
            }
            return fresh(pick);
        }
        if let Some(fk) = &fallback_key {
            if let Some(cached) = st.cache.get(fk, now) {
                if in_avail(&cached) && (!is_sub || self.cfg.session_affinity_subagents) {
                    bind(&mut st, &cached);
                    return Ok(Selection {
                        account: cached,
                        from_binding: true,
                    });
                }
                if !is_sub && !is_fork && preserve(&st, &cached) {
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
        let model = canonical_model_key(&req.model);
        let mut st = self.lock();
        let mkey = (account.clone(), model.clone());

        // ---- cooldown state (conductor_cooldown.go MarkResult) ----
        let kind: Option<ErrKind> = match outcome {
            Outcome::Success { quota } => {
                if let Some(q) = quota {
                    st.quota.insert(account.clone(), q.clone());
                }
                st.model.remove(&mkey);
                st.account.remove(account);
                None
            }
            Outcome::RateLimited {
                retry_after,
                usage_limit,
                quota,
            } => {
                if let Some(q) = quota {
                    st.quota.insert(account.clone(), q.clone());
                }
                if *usage_limit {
                    // credential-scoped: whole account.
                    let a = st.account.entry(account.clone()).or_default();
                    let base = if a.last_err == Some(ErrKind::UsageLimit) {
                        a.clone()
                    } else {
                        CoolState::default()
                    };
                    let (next, lvl) = match retry_after {
                        Some(d) => (now + (*d).max(MIN_QUOTA_COOLDOWN_FLOOR), base.backoff_level),
                        None => quota_after_failure(&base, now),
                    };
                    let next = max_opt(a.live_until(now), next);
                    *a = CoolState {
                        next_retry: Some(next),
                        recover_at: Some(next),
                        backoff_level: lvl,
                        last_err: Some(ErrKind::UsageLimit),
                    };
                    let m = st.model.entry(mkey).or_default();
                    m.next_retry = Some(max_opt(m.recover_at.filter(|r| *r > now), next));
                    m.recover_at = m.next_retry;
                    m.last_err = Some(ErrKind::UsageLimit);
                    Some(ErrKind::UsageLimit)
                } else {
                    let m = st.model.entry(mkey).or_default();
                    let (next, lvl) = match retry_after {
                        Some(d) => (now + (*d).max(MIN_QUOTA_COOLDOWN_FLOOR), m.backoff_level),
                        None => quota_after_failure(m, now),
                    };
                    let next = max_opt(m.recover_at.filter(|r| *r > now), next);
                    *m = CoolState {
                        next_retry: Some(next),
                        recover_at: Some(next),
                        backoff_level: lvl,
                        last_err: Some(ErrKind::Quota),
                    };
                    Some(ErrKind::Quota)
                }
            }
            Outcome::AuthFailed => {
                let a = st.account.entry(account.clone()).or_default();
                a.next_retry = Some(max_opt(a.live_until(now), now + AUTH_COOLDOWN));
                a.last_err = Some(ErrKind::Auth);
                Some(ErrKind::Auth)
            }
            Outcome::ModelUnsupported { retry_after } => {
                let m = st.model.entry(mkey).or_default();
                m.next_retry = Some(
                    now + retry_after
                        .filter(|d| !d.is_zero())
                        .unwrap_or(NOT_FOUND_COOLDOWN),
                );
                m.last_err = Some(ErrKind::ModelUnsupported);
                Some(ErrKind::ModelUnsupported)
            }
            Outcome::Challenge => {
                let m = st.model.entry(mkey).or_default();
                let (c, lvl) = next_quota_cooldown(m.backoff_level);
                let next = now + c.max(CHALLENGE_MIN_COOLDOWN);
                *m = CoolState {
                    next_retry: Some(next),
                    recover_at: Some(next),
                    backoff_level: lvl,
                    last_err: Some(ErrKind::Challenge),
                };
                Some(ErrKind::Challenge)
            }
            // Transport errors (no status) and request-scoped 4xx never cool the credential
            // and never touch session bindings (CPA shouldSkipCredentialCooldown).
            Outcome::Transient { status: None } | Outcome::ClientError { .. } => return,
            Outcome::Transient { status: Some(_) } => {
                let m = st.model.entry(mkey).or_default();
                m.next_retry = Some(max_opt(m.live_until(now), now + TRANSIENT_COOLDOWN));
                m.last_err = Some(ErrKind::Transient);
                Some(ErrKind::Transient)
            }
        };

        // ---- session binding (SessionAffinitySelector.OnResult) ----
        if !self.cfg.session_affinity {
            return;
        }
        if let Some(k) = kind {
            if self.cfg.session_affinity_rebind_on == RebindOn::Quota && !k.rebinds() {
                return;
            }
        }
        let Some(primary) = req.session.key.as_deref().filter(|k| !k.is_empty()) else {
            return;
        };
        let fallback = req.session.parent_key.as_deref().unwrap_or("");
        let cache_key = format!("{PROVIDER}::{primary}::{model}");
        let fallback_key = (!fallback.is_empty()
            && fallback != primary
            && !session::is_subagent_session(primary, fallback))
        .then(|| format!("{PROVIDER}::{fallback}::{model}"));
        if kind.is_none() {
            st.cache.touch(&cache_key, account, now);
            if let Some(fk) = &fallback_key {
                st.cache.touch(fk, account, now);
            }
        } else {
            st.cache.compare_and_delete(&cache_key, account);
            if let Some(fk) = &fallback_key {
                st.cache.compare_and_delete(fk, account);
            }
        }
    }
}

#[cfg(test)]
mod tests;
