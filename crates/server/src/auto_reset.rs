//! Recheck live gates under the consume lock after relisting; retry unknown outcomes with the persisted id.
use crate::reset_credits::{
    consume_locked, refresh_account, ConsumeCode, ConsumeStatus, Credit, ResetCredits, Switches,
};
use ciallopa_core::Config;
use ciallopa_forward::AppState;
use ciallopa_usage::reset::ConsumeAttempt;
use ciallopa_usage::{now_ms, Recorder};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
/// Consume-log rows read back for retry counts.
pub(crate) const HISTORY_ROWS: usize = 500;
const KV_KEY: &str = "reset_credits_auto_accounts";

/// Switches plus the sweep-clock spacing of automatic attempts (retry counts live in DB).
#[derive(Clone)]
pub struct AutoState {
    pub(crate) switches: Switches,
    last: Arc<Mutex<HashMap<(String, String), i64>>>,
}
impl Default for AutoState {
    fn default() -> Self {
        Self {
            switches: Switches::new(KV_KEY),
            last: Default::default(),
        }
    }
}
impl AutoState {
    pub fn is_on(&self, id: &str) -> bool {
        self.switches.is_on(id)
    }
    pub fn ids(&self) -> Vec<String> {
        self.switches.ids()
    }
    pub fn set(&self, id: &str, on: bool) -> bool {
        // Keep the retry map and switch transition ordered against concurrent toggles.
        let mut last = self.last.lock().unwrap_or_else(|e| e.into_inner());
        if !on {
            last.retain(|(a, _), _| a != id);
        }
        self.switches.set(id, on)
    }
    fn tried(&self, id: &str, credit: &str, at: i64) {
        self.last
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((id.into(), credit.into()), at);
    }
    pub(crate) fn history<'a>(
        &self,
        rows: impl IntoIterator<Item = &'a ConsumeAttempt>,
        id: &str,
    ) -> HashMap<String, Hist> {
        let mut h = history(rows, id);
        let last = self.last.lock().unwrap_or_else(|e| e.into_inner());
        for (c, e) in h.iter_mut() {
            e.last_ms = last
                .get(&(id.to_string(), c.clone()))
                .copied()
                .unwrap_or(i64::MIN / 2);
        }
        h
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

pub(crate) fn history<'a>(
    rows: impl IntoIterator<Item = &'a ConsumeAttempt>,
    account: &str,
) -> HashMap<String, Hist> {
    let mut h: HashMap<String, Hist> = HashMap::new();
    for r in rows {
        if r.account != account || r.key.as_deref() != Some(AUTO_LABEL) {
            continue;
        }
        let Some(c) = &r.credit_id else { continue };
        let e = h.entry(c.clone()).or_default();
        // every row is one POST (a superseded row was a real send whose answer was lost)
        e.tries += 1;
        e.stop |= r.status == ConsumeStatus::Done;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    AccountGone,
    Disabled,
    Relogin,
    ManualPending,
    GivenUp,
    Backoff,
    Expired,
    NotDue,
    NothingDue,
    ListFailed,
    SwitchedOff,
    Db,
}
impl SkipReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AccountGone => "account_gone",
            Self::Disabled => "disabled",
            Self::Relogin => "relogin",
            Self::ManualPending => "manual_pending",
            Self::GivenUp => "given_up",
            Self::Backoff => "backoff",
            Self::Expired => "expired",
            Self::NotDue => "not_due",
            Self::NothingDue => "nothing_due",
            Self::ListFailed => "list_failed",
            Self::SwitchedOff => "switched_off",
            Self::Db => "db",
        }
    }
}

#[derive(Debug, PartialEq)]
enum Decision<'a> {
    Retry { credit: &'a str, rid: &'a str },
    Fresh { credit: &'a str },
    Wait(i64),
    Skip(SkipReason),
}

/// A pending attempt blocks fresh spends, including when its card disappeared from the list.
fn decide<'a>(
    now: i64,
    lead: i64,
    credits: &'a [Credit],
    hist: &HashMap<String, Hist>,
    pending: Option<&'a ConsumeAttempt>,
) -> Decision<'a> {
    if let Some(p) = pending {
        let Some(c) = p
            .credit_id
            .as_deref()
            .filter(|_| p.key.as_deref() == Some(AUTO_LABEL))
        else {
            return Decision::Skip(SkipReason::ManualPending);
        };
        let h = hist.get(c).cloned().unwrap_or_default();
        if h.stop || h.tries >= MAX_TRIES {
            return Decision::Skip(SkipReason::GivenUp);
        }
        let at = h.last_ms.saturating_add(RETRY_MS);
        if now < at {
            return Decision::Wait(at);
        }
        let exp = credits
            .iter()
            .find(|x| x.id == c)
            .and_then(|x| x.expires_at.as_deref().and_then(parse_ms));
        if exp.is_some_and(|e| now >= e) {
            return Decision::Skip(SkipReason::Expired);
        }
        return Decision::Retry {
            credit: c,
            rid: &p.redeem_request_id,
        };
    }
    if let Some(c) = pick(credits, hist, lead, now) {
        Decision::Fresh { credit: &c.id }
    } else if let Some(at) = next_due(credits, hist, lead, now) {
        Decision::Wait(at)
    } else {
        Decision::Skip(SkipReason::NothingDue)
    }
}

/// Public sweep observations retain the existing status/code strings for callers and logs.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Skipped(SkipReason),
    Sent {
        credit: String,
        status: String,
        code: Option<String>,
    },
}

/// Clock in ms (injected by tests).
pub type Clock = dyn Fn() -> i64 + Send + Sync;

fn account_ok(account: Option<&ciallopa_core::Account>, relogin: bool) -> Result<(), SkipReason> {
    let Some(a) = account else {
        return Err(SkipReason::AccountGone);
    };
    if a.disabled {
        return Err(SkipReason::Disabled);
    }
    if relogin {
        return Err(SkipReason::Relogin);
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
    let accounts: HashMap<_, _> = st
        .accounts
        .list()
        .await
        .into_iter()
        .map(|a| (a.id.clone(), a))
        .collect();
    let relogin = st.accounts.relogin_required().await;
    let mut cache = rc.snapshot();
    for id in ids {
        if let Err(why) = account_ok(accounts.get(&id), relogin.contains(&id)) {
            events.push((id, Event::Skipped(why)));
            continue;
        }
        let lead = lead_ms(&st.config.load());
        let now = clock();
        let cached = cache.remove(&id);
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
        let pending = rows.iter().find(|r| {
            r.account == id
                && r.status == ConsumeStatus::Pending
                && wall - r.started_ms <= ciallopa_usage::reset::PENDING_REUSE_MS
        });
        match decide(now, lead, &credits, &hist, pending) {
            Decision::Retry { .. } | Decision::Fresh { .. } => {
                let ev = attempt(st, rc, &rec, &id, clock).await;
                events.push((id, ev));
                let t = clock().saturating_add(RETRY_MS);
                next = Some(next.map_or(t, |n| n.min(t)));
            }
            Decision::Wait(t) => {
                next = Some(next.map_or(t, |n| n.min(t)));
                events.push((id, Event::Skipped(SkipReason::NotDue)));
            }
            Decision::Skip(why) => events.push((id, Event::Skipped(why))),
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
        return Event::Skipped(SkipReason::ListFailed);
    }
    // Everything that may have changed while waiting for the lock or the listing.
    if !rc.auto.is_on(id) {
        return Event::Skipped(SkipReason::SwitchedOff);
    }
    let accounts = st.accounts.accounts();
    let account = accounts.iter().find(|a| a.id == id);
    if let Err(why) = account_ok(
        account,
        st.accounts.relogin_required().await.iter().any(|r| r == id),
    ) {
        return Event::Skipped(why);
    }
    let lead = lead_ms(&st.config.load());
    let now = clock();
    let rows = match rec.reset_attempts(HISTORY_ROWS).await {
        Ok(r) => r,
        Err(_) => return Event::Skipped(SkipReason::Db),
    };
    let hist = rc.auto.history(&rows, id);
    let pending = match rec.reset_pending(id).await {
        Ok(p) => p,
        Err(_) => return Event::Skipped(SkipReason::Db),
    };
    let (credit, rid) = match decide(now, lead, &entry.credits, &hist, pending.as_ref()) {
        Decision::Retry { credit, rid } => (credit.to_string(), Some(rid)),
        Decision::Fresh { credit } => (credit.to_string(), None),
        Decision::Wait(_) => {
            return Event::Skipped(if pending.is_some() {
                SkipReason::Backoff
            } else {
                SkipReason::NotDue
            })
        }
        Decision::Skip(SkipReason::NothingDue) => return Event::Skipped(SkipReason::NotDue),
        Decision::Skip(why) => return Event::Skipped(why),
    };
    rc.auto.tried(id, &credit, now);
    let out = consume_locked(st, rc, rec, id, Some(&credit), rid, Some(AUTO_LABEL), &g).await;
    match (out.status, out.code) {
        (Some(ConsumeStatus::Done), Some(ConsumeCode::Reset)) => {
            tracing::info!(account = %ciallopa_core::a(id), %credit, "auto reset-card used before expiry")
        }
        (Some(ConsumeStatus::Done), Some(ConsumeCode::NothingToReset)) => {
            tracing::debug!(account = %ciallopa_core::a(id), %credit, "auto reset-card: nothing to reset; giving up on this card")
        }
        (Some(ConsumeStatus::Done), code) => {
            tracing::info!(account = %ciallopa_core::a(id), %credit, ?code, "auto reset-card: card ended")
        }
        _ => {
            tracing::warn!(account = %ciallopa_core::a(id), %credit, status = out.status.map_or("error", ConsumeStatus::as_str),
            error = ?out.body.get("error"), "auto reset-card attempt not done; will retry")
        }
    }
    Event::Sent {
        credit,
        status: out.status.map_or("error", ConsumeStatus::as_str).into(),
        code: out.code.map(|c| c.as_str().into()),
    }
}

pub(crate) fn spawn(st: AppState, rc: ResetCredits) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let wake = rc.auto.switches.wake();
        tokio::select! { _ = tokio::time::sleep(START_DELAY) => {}, _ = wake.notified() => {} }
        let clock: &Clock = &now_ms;
        loop {
            let (_, next) = sweep_once(&st, &rc, clock).await;
            let wait = next
                .map(|n| {
                    Duration::from_millis(u64::try_from(n.saturating_sub(now_ms())).unwrap_or(0))
                })
                .unwrap_or(SWEEP_MAX)
                .clamp(SWEEP_MIN, SWEEP_MAX);
            tokio::select! { _ = tokio::time::sleep(wait) => {}, _ = wake.notified() => {} }
        }
    })
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
    fn pending_decision_table() {
        let now = E - 5 * 60_000;
        let cards = vec![credit(
            "c",
            "available",
            &chrono::DateTime::from_timestamp_millis(E)
                .unwrap()
                .to_rfc3339(),
        )];
        let base = ConsumeAttempt {
            id: 1,
            account: "a".into(),
            credit_id: Some("c".into()),
            redeem_request_id: "same-id".into(),
            key: Some(AUTO_LABEL.into()),
            instance: None,
            started_ms: 0,
            finished_ms: None,
            status: ConsumeStatus::Pending,
            http_status: None,
            code: None,
            windows_reset: None,
            error: None,
        };
        for (label, tries, stop, last, at, credit_id, key, expected) in [
            (
                "retry",
                1,
                false,
                now - RETRY_MS,
                now,
                Some("c"),
                AUTO_LABEL,
                Decision::Retry {
                    credit: "c",
                    rid: "same-id",
                },
            ),
            (
                "spacing",
                1,
                false,
                now,
                now,
                Some("c"),
                AUTO_LABEL,
                Decision::Wait(now + RETRY_MS),
            ),
            (
                "cap",
                MAX_TRIES,
                false,
                0,
                now,
                Some("c"),
                AUTO_LABEL,
                Decision::Skip(SkipReason::GivenUp),
            ),
            (
                "terminal",
                1,
                true,
                0,
                now,
                Some("c"),
                AUTO_LABEL,
                Decision::Skip(SkipReason::GivenUp),
            ),
            (
                "expired",
                1,
                false,
                0,
                E,
                Some("c"),
                AUTO_LABEL,
                Decision::Skip(SkipReason::Expired),
            ),
            (
                "manual",
                1,
                false,
                0,
                now,
                Some("c"),
                "admin",
                Decision::Skip(SkipReason::ManualPending),
            ),
            (
                "no card id",
                1,
                false,
                0,
                now,
                None,
                AUTO_LABEL,
                Decision::Skip(SkipReason::ManualPending),
            ),
        ] {
            let p = ConsumeAttempt {
                credit_id: credit_id.map(str::to_string),
                key: Some(key.into()),
                ..base.clone()
            };
            let hist = HashMap::from([("c".into(), h(tries, stop, last))]);
            assert_eq!(
                decide(at, LEAD, &cards, &hist, Some(&p)),
                expected,
                "{label}"
            );
        }
        assert_eq!(
            decide(now, LEAD, &[], &HashMap::new(), Some(&base)),
            Decision::Retry {
                credit: "c",
                rid: "same-id"
            }
        );
        assert_eq!(
            decide(now, LEAD, &cards, &HashMap::new(), None),
            Decision::Fresh { credit: "c" }
        );
        assert_eq!(
            decide(E - LEAD - 1, LEAD, &cards, &HashMap::new(), None),
            Decision::Wait(E - LEAD)
        );
        assert_eq!(
            decide(E, LEAD, &cards, &HashMap::new(), None),
            Decision::Skip(SkipReason::NothingDue)
        );
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
            status: status.try_into().unwrap(),
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
}
