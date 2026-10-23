//! `x-codex-turn-state` ownership. The upstream hands it out per account (response header on
//! the first request of a turn) and Codex replays it on every later request of that turn.
//! Since codex-rs binds turn state to the auth owner (`ResponsesHttpExecutor`, auth owner
//! generation), it must never reach a *different* account: when ciallopa fails over or a
//! session rebinds, the client still sends the old account's value, so it is dropped here.
use http::HeaderMap;
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

pub(crate) const HEADER: &str = "x-codex-turn-state";
const TTL: Duration = Duration::from_secs(6 * 3600);
const CAP: usize = 50_000;

static OWNERS: LazyLock<Mutex<HashMap<String, (String, Instant)>>> =
    LazyLock::new(Default::default);

fn value(h: &HeaderMap) -> Option<&str> {
    h.get(HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
}

/// Remember which account issued the turn state in an upstream response (HTTP or WS handshake).
pub(crate) fn learn(resp: &HeaderMap, account: &str) {
    let Some(v) = value(resp) else { return };
    let mut m = OWNERS.lock().unwrap_or_else(|e| e.into_inner());
    if m.len() >= CAP {
        let now = Instant::now();
        m.retain(|_, (_, t)| now.duration_since(*t) < TTL);
        if m.len() >= CAP {
            m.clear();
        }
    }
    m.insert(v.to_string(), (account.to_string(), Instant::now()));
}

/// Drop every turn state issued to `account` (account removed).
pub fn forget_account(account: &str) {
    let mut m = OWNERS.lock().unwrap_or_else(|e| e.into_inner());
    m.retain(|_, (a, _)| a != account);
}

fn owner(v: &str) -> Option<String> {
    let m = OWNERS.lock().unwrap_or_else(|e| e.into_inner());
    m.get(v)
        .filter(|(_, t)| t.elapsed() < TTL)
        .map(|(a, _)| a.clone())
}

/// Client headers for an attempt on `account`. The turn state is kept only when it belongs to
/// `account`; with an unknown owner it is kept on the first attempt and dropped after a
/// failover (`switched`), because it was then issued to an account we left.
pub(crate) fn scope<'a>(
    client: &'a HeaderMap,
    account: &str,
    switched: bool,
) -> std::borrow::Cow<'a, HeaderMap> {
    let Some(v) = value(client) else {
        return std::borrow::Cow::Borrowed(client);
    };
    let keep = match owner(v) {
        Some(o) => o == account,
        None => !switched,
    };
    if keep {
        return std::borrow::Cow::Borrowed(client);
    }
    tracing::debug!(%account, "dropping x-codex-turn-state issued to another account");
    let mut h = client.clone();
    h.remove(HEADER);
    std::borrow::Cow::Owned(h)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn hm(v: &'static str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(HEADER, v.parse().unwrap());
        h
    }
    #[test]
    fn owner_rules() {
        learn(&hm("ts-unit-a"), "a");
        assert!(scope(&hm("ts-unit-a"), "a", true).contains_key(HEADER));
        assert!(!scope(&hm("ts-unit-a"), "b", false).contains_key(HEADER));
        // unknown owner: kept first, dropped after failover
        assert!(scope(&hm("ts-unit-x"), "b", false).contains_key(HEADER));
        assert!(!scope(&hm("ts-unit-x"), "b", true).contains_key(HEADER));
        assert!(scope(&HeaderMap::new(), "b", true).is_empty());
    }
}
