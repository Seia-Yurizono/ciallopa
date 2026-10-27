//! Automatic reset-card (重置卡) use: opt-in per account (「快到期时自动使用」, default off).
//! Shortly before an available card expires (`[ui] reset_credits_auto_lead_mins`, default 10),
//! it is used through the same [`consume_locked`] path and the same per-account lock as the
//! manual button, so a card that would otherwise expire unused is not wasted.
//! `[ui] allow_reset_consume` only gates the manual button; this switch is the only gate here.
//!
//! One background sweep (sleeps at most [`SWEEP_MAX`]) walks the accounts whose switch is on.
//! Per attempt, under the account's consume lock: re-list upstream, then re-check the switch,
//! the account (still there, enabled, no re-login needed), the live lead time and the card's
//! window `[expires_at − lead, expires_at)`, and only then POST. Each POST is one row in the
//! usage DB consume log (key label [`AUTO_LABEL`]); retry counts, give-ups and the idempotent
//! redeem id of an unknown outcome are all read back from those rows, so a restart neither
//! forgets them nor spends twice. Only the 60 s spacing between tries is kept in memory (on the
//! sweep's clock), so the first retry after a restart may come sooner.
//!
//! Races and how each one is handled (tests in `tests/auto_reset.rs` where noted):
//! - **manual vs automatic**: both take [`ResetCredits::lock_for`]; whichever comes second sees
//!   the first one's result in its fresh listing (auto) or in the pending row (manual gets the
//!   usual 409 `pending_consume` and can retry that attempt). An unresolved *manual* attempt is
//!   never retried automatically: the sweep skips the account until a person resolves it
//!   (or it ages out after 24 h); a manual attempt still in flight is skipped the same way.
//!   (test `manual_and_auto_share_the_lock`, both orders)
//! - **card list changes between decision and use**: the decision is made again from a listing
//!   fetched under the lock; a card used or gone meanwhile is not sent, a newer card is picked
//!   only if it is in its own window. A failed re-list skips the attempt (nothing is spent on
//!   stale data). (test `relist_under_lock_decides`)
//! - **account deleted / disabled / re-login needed / token refreshed** mid-flight: checked
//!   after the re-list, right before the POST; removal also drops the switch
//!   ([`ResetCredits::forget`]), and the consume lock is kept while in use so a re-added id
//!   still serialises. Credentials are read at send time, so a refreshed token is used as is;
//!   a 401 is a definitive `failed` and counts as one try. (test `checks_after_relist`)
//! - **switch turned off / lead time changed mid-flight**: same re-check right before the POST
//!   (the live config is read there). Once sent, the outcome is recorded and never undone.
//!   (test `checks_after_relist`)
//! - **hot reload**: the lead time and the account list are read live on every sweep and again
//!   under the lock. An account that vanishes outside the 账号 page (auth file removed, reload)
//!   keeps its switch but is skipped (`account_gone`) while absent; the same id coming back
//!   (normally the same upstream account) is automatic again. Deleting on the page drops it.
//! - **restart during an unknown outcome**: the `pending` row survives; the next sweep retries
//!   it with the same `redeem_request_id` (upstream dedups) and supersedes the old row.
//!   (test `pending_is_retried_with_the_same_id`)
//! - **poller vs sweep**: both only write the read-only listing cache under its mutex; the
//!   spend decision never uses the cache, only the listing fetched under the lock.
//! - **clock skew**: the window is computed on the local clock. With the lead time of at least
//!   5 min, skew below that still lands inside the window; a clock running ahead fires early
//!   by the skew, one running behind may fire after the real expiry, which upstream answers
//!   with `no_credit` / 4xx (recorded, never retried past the local expiry). (unit tests)
//! - **multiple instances on one account**: not handled (each instance has its own lock); the
//!   shared redeem id only dedups retries of one attempt.
//!
//! Retries: at most [`MAX_TRIES`] POSTs per card, at least [`RETRY_MS`] apart, never at or after
//! its expiry. `nothing_to_reset` gives up on that card silently; `reset`, `no_credit`,
//! `already_redeemed` end it too.
use crate::reset_credits::{consume_locked, refresh_account, Credit, ResetCredits};
use ciallopa_core::Config;
use ciallopa_forward::AppState;
use ciallopa_usage::reset::ConsumeAttempt;
use ciallopa_usage::{now_ms, Recorder};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

/// Key label of automatic attempts in the consume log.
pub const AUTO_LABEL: &str = "(auto)";
/// POSTs per card at most.
pub const MAX_TRIES: u32 = 5;
/// Minimum gap between two POSTs for the same card.
pub const RETRY_MS: i64 = 60_000;
/// The sweep sleeps at most this long.
pub const SWEEP_MAX: Duration = Duration::from_secs(60);
const SWEEP_MIN: Duration = Duration::from_secs(1);
const START_DELAY: Duration = Duration::from_secs(20);
/// Listings of switched-on accounts older than this are re-read by the sweep (read-only).
const LIST_MAX_AGE_MS: i64 = 15 * 60_000;
/// A failed listing is not asked again sooner than this.
const LIST_RETRY_MS: i64 = 5 * 60_000;
/// Switch changes are written at most this often.
const PERSIST_DEBOUNCE: Duration = Duration::from_secs(2);
/// Consume-log rows read back for retry counts.
pub(crate) const HISTORY_ROWS: usize = 500;
const KV_KEY: &str = "reset_credits_auto_accounts";

// ---------------- switches ----------------

#[derive(Default)]
struct Flags {
    on: BTreeSet<String>,
    gen: u64,
    saved: u64,
}

#[derive(Default)]
struct Inner {
    flags: Mutex<Flags>,
    /// Last automatic POST per (account, card), on the sweep's clock. Memory only: after a
    /// restart the first retry may come sooner than [`RETRY_MS`]; the try count is in the DB.
    last: Mutex<HashMap<(String, String), i64>>,
    dirty: Notify,
    wake: Notify,
}

/// Per-account switches, shared by the API, the sweep and the persister.
#[derive(Clone, Default)]
pub struct AutoState(Arc<Inner>);

impl AutoState {
    fn flags(&self) -> std::sync::MutexGuard<'_, Flags> {
        self.0.flags.lock().unwrap_or_else(|e| e.into_inner())
    }
    pub fn is_on(&self, id: &str) -> bool {
        self.flags().on.contains(id)
    }
    /// Returns whether anything changed. Changes are persisted (debounced) and wake the sweep.
    pub fn set(&self, id: &str, on: bool) -> bool {
        let mut f = self.flags();
        let changed = if on {
            f.on.insert(id.to_string())
        } else {
            f.on.remove(id)
        };
        if !on {
            self.0
                .last
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .retain(|(a, _), _| a != id);
        }
        if changed {
            f.gen += 1;
            drop(f);
            self.0.dirty.notify_one();
            self.0.wake.notify_one();
        }
        changed
    }
    pub fn ids(&self) -> Vec<String> {
        self.flags().on.iter().cloned().collect()
    }
    fn tried(&self, id: &str, credit: &str, at: i64) {
        self.0
            .last
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((id.to_string(), credit.to_string()), at);
    }
    /// [`history`] of one account with the last-try times filled in.
    pub(crate) fn history(&self, rows: &[ConsumeAttempt], id: &str) -> HashMap<String, Hist> {
        let mut h = history(rows, id);
        let last = self.0.last.lock().unwrap_or_else(|e| e.into_inner());
        for (c, e) in h.iter_mut() {
            e.last_ms = last
                .get(&(id.to_string(), c.clone()))
                .copied()
                .unwrap_or(i64::MIN / 2);
        }
        h
    }
    fn load(&self, ids: Vec<String>) {
        let mut f = self.flags();
        f.on = ids.into_iter().collect();
    }
    fn unsaved(&self) -> Option<(u64, String)> {
        let f = self.flags();
        (f.gen != f.saved).then(|| {
            let ids: Vec<&String> = f.on.iter().collect();
            (
                f.gen,
                serde_json::to_string(&ids).unwrap_or_else(|_| "[]".into()),
            )
        })
    }
    fn saved(&self, gen: u64) {
        let mut f = self.flags();
        f.saved = f.saved.max(gen);
    }
}

static REG: Mutex<Option<(Arc<Recorder>, AutoState)>> = Mutex::new(None);

/// Graceful shutdown: write a switch change still waiting for the debounce.
pub fn flush() {
    let reg = REG.lock().unwrap_or_else(|e| e.into_inner()).clone();
    if let Some((rec, a)) = reg {
        if let Some((gen, v)) = a.unsaved() {
            match rec.kv_set(KV_KEY, &v) {
                Ok(()) => a.saved(gen),
                Err(e) => tracing::warn!(error = %e, "auto reset-card switches not saved"),
            }
        }
    }
}

async fn persist(rec: Arc<Recorder>, a: AutoState) {
    loop {
        a.0.dirty.notified().await;
        tokio::time::sleep(PERSIST_DEBOUNCE).await;
        let Some((gen, v)) = a.unsaved() else {
            continue;
        };
        let r = rec.clone();
        match tokio::task::spawn_blocking(move || r.kv_set(KV_KEY, &v)).await {
            Ok(Ok(())) => a.saved(gen),
            Ok(Err(e)) => tracing::warn!(error = %e, "auto reset-card switches not saved"),
            Err(e) => tracing::warn!(error = %e, "auto reset-card switches not saved"),
        }
    }
}

// ---------------- decisions (pure) ----------------

pub(crate) fn lead_ms(cfg: &Config) -> i64 {
    i64::try_from(cfg.ui.reset_credits_auto_lead_mins)
        .unwrap_or(1440)
        .saturating_mul(60_000)
}

fn parse_ms(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|t| t.timestamp_millis())
}

/// Expiry of an available card (`None`: not available or no parsable expiry).
fn expiry(c: &Credit) -> Option<i64> {
    if c.status.as_deref().is_some_and(|s| s != "available") {
        return None;
    }
    c.expires_at.as_deref().and_then(parse_ms)
}

/// Automatic tries of one card, from the consume log.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct Hist {
    pub tries: u32,
    /// A definitive answer was recorded (`reset`, `nothing_to_reset`, `no_credit`,
    /// `already_redeemed`): never again.
    pub stop: bool,
    pub last_ms: i64,
}

pub(crate) fn history(rows: &[ConsumeAttempt], account: &str) -> HashMap<String, Hist> {
    let mut h: HashMap<String, Hist> = HashMap::new();
    for r in rows {
        if r.account != account || r.key.as_deref() != Some(AUTO_LABEL) {
            continue;
        }
        let Some(c) = &r.credit_id else { continue };
        let e = h.entry(c.clone()).or_default();
        // every row is one POST (a superseded row was a real send whose answer was lost)
        e.tries += 1;
        e.stop |= r.status == "done";
        e.last_ms = e.last_ms.max(r.started_ms);
        // (`AutoState::history` replaces `last_ms` with the sweep-clock time)
    }
    h
}

/// Earliest time a card may be sent: inside its window, after the retry gap.
fn earliest(e: i64, h: Option<&Hist>, lead: i64) -> Option<i64> {
    let h = h.cloned().unwrap_or_default();
    if h.stop || h.tries >= MAX_TRIES {
        return None;
    }
    let mut t = e.saturating_sub(lead);
    if h.tries > 0 {
        t = t.max(h.last_ms.saturating_add(RETRY_MS));
    }
    (t < e).then_some(t)
}

/// Whether `now` is inside the card's window `[e − lead, e)` (and past the retry gap).
pub(crate) fn due(e: i64, h: Option<&Hist>, lead: i64, now: i64) -> bool {
    earliest(e, h, lead).is_some_and(|t| now >= t) && now < e
}

/// When the next automatic use of this account will happen (for the page), if any.
pub(crate) fn next_due(
    credits: &[Credit],
    hist: &HashMap<String, Hist>,
    lead: i64,
    now: i64,
) -> Option<i64> {
    credits
        .iter()
        .filter_map(|c| {
            let e = expiry(c).filter(|e| *e > now)?;
            earliest(e, hist.get(&c.id), lead).map(|t| t.max(now))
        })
        .min()
}

/// The card to use now: the one expiring first among those inside their window.
fn pick<'a>(
    credits: &'a [Credit],
    hist: &HashMap<String, Hist>,
    lead: i64,
    now: i64,
) -> Option<&'a Credit> {
    credits
        .iter()
        .filter_map(|c| Some((expiry(c)?, c)))
        .filter(|(e, c)| due(*e, hist.get(&c.id), lead, now))
        .min_by_key(|(e, _)| *e)
        .map(|(_, c)| c)
}

// ---------------- sweep ----------------

/// What one account's turn in a sweep did (tests and logs).
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Skipped(&'static str),
    /// Sent: the recorded status (`done` / `failed` / `pending`) and the upstream code.
    Sent {
        credit: String,
        status: String,
        code: Option<String>,
    },
}

/// Clock in ms (injected by tests).
pub type Clock = dyn Fn() -> i64 + Send + Sync;

async fn account_ok(st: &AppState, id: &str) -> Result<(), &'static str> {
    let Some(a) = st.accounts.list().await.into_iter().find(|a| a.id == id) else {
        return Err("account_gone");
    };
    if a.disabled {
        return Err("disabled");
    }
    if st.accounts.relogin_required().await.iter().any(|r| r == id) {
        return Err("relogin");
    }
    Ok(())
}

/// One sweep over the switched-on accounts. Returns what happened per account and when the
/// next card becomes due (ms), if known.
pub async fn sweep_once(
    st: &AppState,
    rc: &ResetCredits,
    clock: &Clock,
) -> (Vec<(String, Event)>, Option<i64>) {
    let mut events = vec![];
    let mut next: Option<i64> = None;
    let Some(rec) = st.usage.clone() else {
        return (events, None);
    };
    let ids = rc.auto.ids();
    if ids.is_empty() {
        return (events, None);
    }
    let rows = rec.reset_attempts(HISTORY_ROWS).await.unwrap_or_default();
    for id in ids {
        if let Err(why) = account_ok(st, &id).await {
            events.push((id, Event::Skipped(why)));
            continue;
        }
        let lead = lead_ms(&st.config.load());
        let now = clock();
        let cached = rc.snapshot().remove(&id);
        let wall = now_ms();
        let stale = cached.as_ref().is_none_or(|e| {
            let fresh = e.fetched_ms.is_some_and(|f| wall - f < LIST_MAX_AGE_MS);
            let failed_recently = e.error_ms.is_some_and(|f| wall - f < LIST_RETRY_MS);
            !fresh && !failed_recently
        });
        let credits = if stale {
            refresh_account(st, rc, &id).await.credits
        } else {
            cached.map(|e| e.credits).unwrap_or_default()
        };
        let hist = rc.auto.history(&rows, &id);
        // An automatic attempt with an unknown outcome is retried (same id) even when the
        // listing no longer shows its card, so the outcome gets recorded.
        // (Rows come newest first; only a recent one blocks, as in `reset_pending`.)
        let pending = rows.iter().find(|r| {
            r.account == id
                && r.status == "pending"
                && wall - r.started_ms < ciallopa_usage::reset::PENDING_REUSE_MS
        });
        if pending.is_some_and(|p| p.key.as_deref() != Some(AUTO_LABEL)) {
            events.push((id, Event::Skipped("manual_pending")));
            continue;
        }
        let pending_at = pending.and_then(|p| {
            let c = p.credit_id.as_deref()?;
            let h = hist.get(c)?;
            let exp = credits
                .iter()
                .find(|x| x.id == c)
                .and_then(|x| x.expires_at.as_deref().and_then(parse_ms));
            (h.tries < MAX_TRIES && exp.is_none_or(|e| now < e))
                .then_some(h.last_ms.saturating_add(RETRY_MS))
        });
        let at = [next_due(&credits, &hist, lead, now), pending_at]
            .into_iter()
            .flatten()
            .min();
        match at {
            Some(t) if t <= now => {
                let ev = attempt(st, rc, &rec, &id, clock).await;
                events.push((id, ev));
                let t = clock().saturating_add(RETRY_MS);
                next = Some(next.map_or(t, |n| n.min(t)));
            }
            Some(t) => {
                next = Some(next.map_or(t, |n| n.min(t)));
                events.push((id, Event::Skipped("not_due")));
            }
            None => events.push((id, Event::Skipped("nothing_due"))),
        }
    }
    (events, next)
}

/// One automatic attempt, entirely under the account's consume lock.
async fn attempt(
    st: &AppState,
    rc: &ResetCredits,
    rec: &Recorder,
    id: &str,
    clock: &Clock,
) -> Event {
    let lock = rc.lock_for(id);
    let g = lock.lock().await;
    // Re-list upstream first: the card may have been used (manually) or replaced meanwhile.
    let t0 = now_ms();
    let entry = refresh_account(st, rc, id).await;
    if entry.error_ms.is_some_and(|m| m >= t0) {
        tracing::warn!(account = %ciallopa_core::a(id), error = ?entry.error, "auto reset-card: listing failed; not sending");
        return Event::Skipped("list_failed");
    }
    // Everything that may have changed while waiting for the lock or the listing.
    if !rc.auto.is_on(id) {
        return Event::Skipped("switched_off");
    }
    if let Err(why) = account_ok(st, id).await {
        return Event::Skipped(why);
    }
    let lead = lead_ms(&st.config.load());
    let now = clock();
    let rows = match rec.reset_attempts(HISTORY_ROWS).await {
        Ok(r) => r,
        Err(_) => return Event::Skipped("db"),
    };
    let hist = rc.auto.history(&rows, id);
    let pending = match rec.reset_pending(id).await {
        Ok(p) => p,
        Err(_) => return Event::Skipped("db"),
    };
    let (credit, rid) = match pending {
        Some(p) if p.key.as_deref() != Some(AUTO_LABEL) => {
            return Event::Skipped("manual_pending");
        }
        Some(p) => {
            let Some(c) = p.credit_id.clone() else {
                return Event::Skipped("manual_pending");
            };
            let h = hist.get(&c).cloned().unwrap_or_default();
            if h.tries >= MAX_TRIES {
                return Event::Skipped("given_up");
            }
            if now < h.last_ms.saturating_add(RETRY_MS) {
                return Event::Skipped("backoff");
            }
            let exp = entry
                .credits
                .iter()
                .find(|x| x.id == c)
                .and_then(|x| x.expires_at.as_deref().and_then(parse_ms));
            if exp.is_some_and(|e| now >= e) {
                return Event::Skipped("expired");
            }
            (c, Some(p.redeem_request_id))
        }
        None => match pick(&entry.credits, &hist, lead, now) {
            Some(c) => (c.id.clone(), None),
            None => return Event::Skipped("not_due"),
        },
    };
    rc.auto.tried(id, &credit, now);
    let (_, out) = consume_locked(
        st,
        rc,
        rec,
        id,
        Some(&credit),
        rid.as_deref(),
        Some(AUTO_LABEL),
        &g,
    )
    .await;
    let status = out["status"].as_str().unwrap_or("error").to_string();
    let code = out.get("code").and_then(Value::as_str).map(str::to_string);
    match (status.as_str(), code.as_deref()) {
        ("done", Some("reset")) => {
            tracing::info!(account = %ciallopa_core::a(id), %credit, "auto reset-card used before expiry")
        }
        // gives up on this card without a warning
        ("done", Some("nothing_to_reset")) => {
            tracing::debug!(account = %ciallopa_core::a(id), %credit, "auto reset-card: nothing to reset; giving up on this card")
        }
        ("done", _) => {
            tracing::info!(account = %ciallopa_core::a(id), %credit, ?code, "auto reset-card: card ended")
        }
        _ => {
            tracing::warn!(account = %ciallopa_core::a(id), %credit, %status, error = ?out.get("error"), "auto reset-card attempt not done; will retry")
        }
    }
    Event::Sent {
        credit,
        status,
        code,
    }
}

/// Load the switches and start the persister and the sweep (needs `[usage]`).
pub fn spawn(st: AppState, rc: ResetCredits) {
    if tokio::runtime::Handle::try_current().is_err() {
        return;
    }
    let Some(rec) = st.usage.clone() else {
        return;
    };
    match rec.kv_get(KV_KEY) {
        Ok(Some(v)) => rc
            .auto
            .load(serde_json::from_str::<Vec<String>>(&v).unwrap_or_default()),
        Ok(None) => {}
        Err(e) => tracing::warn!(error = %e, "auto reset-card switches not loaded"),
    }
    *REG.lock().unwrap_or_else(|e| e.into_inner()) = Some((rec.clone(), rc.auto.clone()));
    tokio::spawn(persist(rec, rc.auto.clone()));
    tokio::spawn(async move {
        tokio::time::sleep(START_DELAY).await;
        let clock: &Clock = &now_ms;
        loop {
            let (_, next) = sweep_once(&st, &rc, clock).await;
            let wait = next
                .map(|n| {
                    Duration::from_millis(u64::try_from(n.saturating_sub(now_ms())).unwrap_or(0))
                })
                .unwrap_or(SWEEP_MAX)
                .clamp(SWEEP_MIN, SWEEP_MAX);
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = rc.auto.0.wake.notified() => {}
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    const E: i64 = 1_800_000_000_000;
    const LEAD: i64 = 10 * 60_000;

    fn h(tries: u32, stop: bool, last_ms: i64) -> Hist {
        Hist {
            tries,
            stop,
            last_ms,
        }
    }

    #[test]
    fn window_is_lead_before_expiry() {
        assert!(!due(E, None, LEAD, E - LEAD - 1));
        assert!(due(E, None, LEAD, E - LEAD));
        assert!(due(E, None, LEAD, E - 1));
        // never at or after expiry
        assert!(!due(E, None, LEAD, E));
        assert!(!due(E, None, LEAD, E + 1));
    }

    #[test]
    fn retries_are_spaced_capped_and_stop() {
        let t = E - LEAD;
        assert!(!due(E, Some(&h(1, false, t)), LEAD, t + RETRY_MS - 1));
        assert!(due(E, Some(&h(1, false, t)), LEAD, t + RETRY_MS));
        assert!(due(E, Some(&h(4, false, t)), LEAD, t + RETRY_MS));
        assert!(!due(E, Some(&h(5, false, t)), LEAD, t + RETRY_MS));
        assert!(!due(E, Some(&h(1, true, t)), LEAD, t + RETRY_MS));
        // a retry gap that would end at/after expiry: no more tries
        assert!(!due(E, Some(&h(1, false, E - 1000)), LEAD, E - 1));
    }

    #[test]
    fn clock_skew_bounds() {
        // a local clock 4 min ahead fires 4 min early, still before the real expiry
        let skew = 4 * 60_000;
        assert!(due(E, None, LEAD, (E - LEAD) + skew - skew));
        // a clock behind by more than the lead never fires at a local time >= expiry
        assert!(!due(E, None, LEAD, E + skew));
    }

    fn credit(id: &str, status: &str, exp: &str) -> Credit {
        Credit {
            id: id.into(),
            reset_type: None,
            status: Some(status.into()),
            granted_at: None,
            expires_at: Some(exp.into()),
            title: None,
            description: None,
        }
    }

    #[test]
    fn pick_and_next() {
        let e1 = parse_ms("2027-01-01T00:10:00Z").unwrap();
        let cs = vec![
            credit("used", "redeemed", "2027-01-01T00:05:00Z"),
            credit("c1", "available", "2027-01-01T00:10:00Z"),
            credit("c2", "available", "2027-01-01T00:12:00Z"),
            credit("bad", "available", "soon"),
        ];
        let none = HashMap::new();
        assert_eq!(next_due(&cs, &none, LEAD, 0), Some(e1 - LEAD));
        assert!(pick(&cs, &none, LEAD, e1 - LEAD - 1).is_none());
        assert_eq!(pick(&cs, &none, LEAD, e1 - LEAD).unwrap().id, "c1");
        // c1 gave up (nothing_to_reset): c2 is next, once inside its own window
        let gave = HashMap::from([("c1".to_string(), h(1, true, e1 - LEAD))]);
        assert_eq!(pick(&cs, &gave, LEAD, e1 - 60_000).unwrap().id, "c2");
        assert_eq!(
            next_due(&cs, &gave, LEAD, 0),
            Some(parse_ms("2027-01-01T00:02:00Z").unwrap())
        );
        // nothing after every card expired
        assert_eq!(next_due(&cs, &none, LEAD, e1 + 3_600_000), None);
    }

    #[test]
    fn history_counts_auto_rows_only() {
        let row = |key: &str, credit: &str, status: &str, t: i64| ConsumeAttempt {
            id: t,
            account: "a".into(),
            credit_id: Some(credit.into()),
            redeem_request_id: "r".into(),
            key: Some(key.into()),
            instance: None,
            started_ms: t,
            finished_ms: None,
            status: status.into(),
            http_status: None,
            code: None,
            windows_reset: None,
            error: None,
        };
        let rows = vec![
            row(AUTO_LABEL, "c1", "superseded", 1),
            row(AUTO_LABEL, "c1", "failed", 3),
            row("admin", "c1", "done", 4),
            row(AUTO_LABEL, "c2", "done", 2),
        ];
        let hs = history(&rows, "a");
        assert_eq!(hs["c1"], h(2, false, 3));
        assert_eq!(hs["c2"], h(1, true, 2));
        assert!(history(&rows, "b").is_empty());
    }

    #[test]
    fn switches_are_debounced_state() {
        let a = AutoState::default();
        assert!(a.unsaved().is_none());
        assert!(a.set("x", true));
        assert!(!a.set("x", true));
        assert!(a.set("y", true));
        let (g, v) = a.unsaved().unwrap();
        assert_eq!(v, r#"["x","y"]"#);
        a.saved(g);
        assert!(a.unsaved().is_none());
        assert!(a.set("x", false));
        assert_eq!(a.ids(), vec!["y".to_string()]);
    }
}
