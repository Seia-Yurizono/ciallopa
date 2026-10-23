//! One global "latest Codex client" fingerprint (`User-Agent` + `originator`), learned from
//! authenticated forwarded requests (HTTP and WS handshakes) and used for ciallopa's *own*
//! upstream calls (token refresh / exchange, `/wham/*` usage + reset credits), so those look
//! like the Codex build actually in use instead of a hard-coded one.
//!
//! Validation mirrors codex-rs `login/src/auth/default_client.rs`: the UA is
//! `{originator}/{version} (...)`, so the UA prefix must equal the originator. Only the
//! interactive first-party originators `codex_cli_rs` / `codex-tui` are accepted (curl,
//! third-party clients, `codex_exec`, IDE/desktop originators are ignored).
use http::HeaderMap;
use serde::{Deserialize, Serialize};
use std::sync::{LazyLock, RwLock};

/// Built-in fallback (Codex version the network stack is pinned to).
pub const DEFAULT_ORIGINATOR: &str = "codex_cli_rs";
pub fn default_user_agent() -> String {
    format!(
        "{DEFAULT_ORIGINATOR}/{}",
        crate::config::PINNED_CODEX_VERSION
    )
}
const ACCEPTED: [&str; 2] = ["codex_cli_rs", "codex-tui"];
const MAX_UA: usize = 512;
/// Re-persist an unchanged pair at most this often (refreshes `learned_at`).
const PERSIST_EVERY_MS: i64 = 3_600_000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fingerprint {
    pub user_agent: String,
    pub originator: String,
    /// `learned` or `default`.
    pub source: String,
    /// Last time a matching client request was seen (unix ms); None for the default.
    pub learned_at_ms: Option<i64>,
    /// Client key *label* of that request (never the key).
    pub key: Option<String>,
}

impl Fingerprint {
    pub fn builtin() -> Self {
        Self {
            user_agent: default_user_agent(),
            originator: DEFAULT_ORIGINATOR.into(),
            source: "default".into(),
            learned_at_ms: None,
            key: None,
        }
    }
}

/// Persistence policy (see docs/USAGE.md "Disk writes"): `observe` never touches the disk.
/// It only updates memory and, when the pair changed (or an unchanged pair's timestamp is
/// older than [`PERSIST_EVERY_MS`]), marks the state dirty and pokes a single background
/// writer. The writer coalesces to the latest value and saves at most once per
/// [`MIN_SAVE_INTERVAL`], like Redis `save <seconds> <changes>` / a debounced settings store.
pub const MIN_SAVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

type Saver = Box<dyn Fn(&Fingerprint) + Send + Sync>;

struct State {
    fp: Fingerprint,
    /// `learned_at_ms` of the last value handed to the writer.
    persisted_ms: i64,
    dirty: bool,
    notify: Option<std::sync::mpsc::SyncSender<()>>,
    last_log_ms: i64,
}

static STATE: LazyLock<RwLock<State>> = LazyLock::new(|| {
    RwLock::new(State {
        fp: Fingerprint::builtin(),
        persisted_ms: 0,
        dirty: false,
        notify: None,
        last_log_ms: 0,
    })
});

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// `MAJOR.MINOR.PATCH` with an optional `-prerelease` / `+build`; not `0.0.0` (dev builds).
fn sane_version(v: &str) -> bool {
    let core = v.split(['-', '+']).next().unwrap_or("");
    let rest = &v[core.len()..];
    let parts: Vec<&str> = core.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.len() <= 6 && p.bytes().all(|b| b.is_ascii_digit()))
        && parts.iter().any(|p| p.bytes().any(|b| b != b'0'))
        && rest
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'+'))
}

/// Valid (UA, originator) pair or None. The originator header may be absent: Codex always
/// sends it, so a missing one is rejected too.
pub fn validate(ua: &str, originator: &str) -> Option<(String, String)> {
    let (ua, orig) = (ua.trim(), originator.trim());
    if ua.is_empty() || ua.len() > MAX_UA || !ua.bytes().all(|b| (b' '..=b'~').contains(&b)) {
        return None;
    }
    if !ACCEPTED.contains(&orig) {
        return None;
    }
    let rest = ua.strip_prefix(orig)?.strip_prefix('/')?;
    let version = rest.split([' ', '(']).next().unwrap_or("");
    sane_version(version).then(|| (ua.to_string(), orig.to_string()))
}

/// Learn from an authenticated client request. Returns true when the request was accepted.
/// Memory only; never blocks on or spawns disk I/O (see [`MIN_SAVE_INTERVAL`]).
pub fn observe(h: &HeaderMap, key_label: Option<&str>) -> bool {
    let get = |k: &str| h.get(k).and_then(|v| v.to_str().ok()).unwrap_or("");
    let Some((ua, orig)) = validate(get("user-agent"), get("originator")) else {
        return false;
    };
    let now = now_ms();
    let mut w = STATE.write().unwrap_or_else(|e| e.into_inner());
    let changed = w.fp.user_agent != ua || w.fp.originator != orig || w.fp.source != "learned";
    if changed {
        w.fp.user_agent = ua;
        w.fp.originator = orig;
        w.fp.source = "learned".into();
        // Alternating clients must not flood the log either.
        if now - w.last_log_ms >= MIN_SAVE_INTERVAL.as_millis() as i64 {
            w.last_log_ms = now;
            tracing::info!(user_agent = %w.fp.user_agent, originator = %w.fp.originator, "learned client fingerprint");
        }
    }
    w.fp.learned_at_ms = Some(now);
    if w.fp.key.as_deref() != key_label {
        w.fp.key = key_label.map(str::to_string);
    }
    if !w.dirty && (changed || now - w.persisted_ms >= PERSIST_EVERY_MS) {
        w.dirty = true;
        if let Some(tx) = &w.notify {
            // Capacity 1: a pending wake-up already covers this change.
            let _ = tx.try_send(());
        }
    }
    true
}

/// Current fingerprint (learned or built-in default).
pub fn current() -> Fingerprint {
    STATE.read().unwrap_or_else(|e| e.into_inner()).fp.clone()
}

/// `(User-Agent, originator)` for internal upstream requests.
pub fn headers() -> (String, String) {
    let f = current();
    (f.user_agent, f.originator)
}

/// Restore a persisted fingerprint at startup (re-validated; invalid ones are ignored).
pub fn restore(fp: Fingerprint) -> bool {
    if fp.source != "learned" || validate(&fp.user_agent, &fp.originator).is_none() {
        return false;
    }
    let mut w = STATE.write().unwrap_or_else(|e| e.into_inner());
    w.persisted_ms = fp.learned_at_ms.unwrap_or(0);
    w.dirty = false;
    w.fp = fp;
    true
}

/// Take the latest dirty value (clears the flag). Used by the writer.
fn take_dirty() -> Option<Fingerprint> {
    let mut w = STATE.write().unwrap_or_else(|e| e.into_inner());
    if !w.dirty {
        return None;
    }
    w.dirty = false;
    w.persisted_ms = w.fp.learned_at_ms.unwrap_or_else(now_ms);
    Some(w.fp.clone())
}

/// Install the persistence hook: starts the single writer thread. `save` runs only on that
/// thread, at most once per `min_interval`, always with the newest value.
pub fn set_saver(s: Saver) {
    set_saver_with(s, MIN_SAVE_INTERVAL);
}

/// [`set_saver`] with a custom minimum interval (tests).
pub fn set_saver_with(s: Saver, min_interval: std::time::Duration) {
    let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);
    {
        let mut w = STATE.write().unwrap_or_else(|e| e.into_inner());
        w.notify = Some(tx.clone());
        if w.dirty {
            let _ = tx.try_send(());
        }
    }
    let _ = std::thread::Builder::new()
        .name("fp-writer".into())
        .spawn(move || {
            let mut last: Option<std::time::Instant> = None;
            // Ends when the sender is replaced (`set_saver` again / reset_for_tests).
            while rx.recv().is_ok() {
                if let Some(t) = last {
                    let wait = min_interval.saturating_sub(t.elapsed());
                    if !wait.is_zero() {
                        std::thread::sleep(wait);
                    }
                }
                if let Some(fp) = take_dirty() {
                    s(&fp);
                    last = Some(std::time::Instant::now());
                }
            }
        });
}

/// Persist a pending change now (graceful shutdown). No-op when clean or no saver.
pub fn flush_with(save: impl Fn(&Fingerprint)) {
    if let Some(fp) = take_dirty() {
        save(&fp);
    }
}

/// Tests only: back to the built-in default, no saver.
pub fn reset_for_tests() {
    let mut w = STATE.write().unwrap_or_else(|e| e.into_inner());
    w.fp = Fingerprint::builtin();
    w.persisted_ms = 0;
    w.dirty = false;
    w.notify = None;
    w.last_log_ms = 0;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validation() {
        let ok = "codex_cli_rs/0.163.0 (Ubuntu 24.4.0; x86_64) xterm-256color";
        assert!(validate(ok, "codex_cli_rs").is_some());
        assert!(validate(
            "codex-tui/0.163.0-alpha.2 (Mac OS 15.1; arm64) iTerm",
            "codex-tui"
        )
        .is_some());
        assert!(validate("codex_cli_rs/0.163.0", "codex_cli_rs").is_some());
        for (ua, o) in [
            (ok, "codex-tui"),                     // pair mismatch
            ("codex-tui/0.163.0", "codex_cli_rs"), // pair mismatch
            (ok, ""),                              // no originator
            ("curl/8.5.0", "codex_cli_rs"),
            ("codex_exec/0.163.0", "codex_exec"), // non-interactive originator
            ("codex_vscode/1.2.3", "codex_vscode"),
            ("codex_cli_rs/0.0.0 (x)", "codex_cli_rs"), // dev build
            ("codex_cli_rs/1.2 (x)", "codex_cli_rs"),
            ("codex_cli_rs/v1.2.3", "codex_cli_rs"),
            ("codex_cli_rs/1.2.3\u{7f}", "codex_cli_rs"),
            ("codex_cli_rsx/1.2.3", "codex_cli_rs"),
        ] {
            assert!(validate(ua, o).is_none(), "{ua:?} {o:?}");
        }
        assert!(validate(
            &format!("codex_cli_rs/1.2.3 {}", "a".repeat(600)),
            "codex_cli_rs"
        )
        .is_none());
    }

    #[test]
    fn writes_are_coalesced_and_rate_limited() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        reset_for_tests();
        let n = Arc::new(AtomicUsize::new(0));
        let saved = Arc::new(std::sync::Mutex::new(String::new()));
        let (n2, s2) = (n.clone(), saved.clone());
        set_saver_with(
            Box::new(move |fp| {
                n2.fetch_add(1, Ordering::SeqCst);
                *s2.lock().unwrap() = fp.user_agent.clone();
            }),
            std::time::Duration::from_millis(300),
        );
        let mut h = HeaderMap::new();
        h.insert("originator", "codex_cli_rs".parse().unwrap());
        // Two clients alternating on every request (the reported worst case).
        for i in 0..200 {
            let ua = if i % 2 == 0 {
                "codex_cli_rs/0.163.0 (A)"
            } else {
                "codex_cli_rs/0.164.0 (B)"
            };
            h.insert("user-agent", ua.parse().unwrap());
            assert!(observe(&h, Some("k")));
        }
        std::thread::sleep(std::time::Duration::from_millis(900));
        let writes = n.load(Ordering::SeqCst);
        assert!((1..=2).contains(&writes), "200 requests → {writes} writes");
        assert_eq!(
            &*saved.lock().unwrap(),
            "codex_cli_rs/0.164.0 (B)",
            "latest wins"
        );
        // Same pair again: no new write (only the hourly timestamp bump would).
        for _ in 0..50 {
            observe(&h, Some("k"));
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
        assert_eq!(n.load(Ordering::SeqCst), writes);
        reset_for_tests();
    }
}
