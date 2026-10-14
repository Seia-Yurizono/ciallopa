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

type Saver = Box<dyn Fn(&Fingerprint) + Send + Sync>;

struct State {
    fp: Fingerprint,
    persisted_ms: i64,
    saver: Option<Saver>,
}

static STATE: LazyLock<RwLock<State>> = LazyLock::new(|| {
    RwLock::new(State {
        fp: Fingerprint::builtin(),
        persisted_ms: 0,
        saver: None,
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
pub fn observe(h: &HeaderMap, key_label: Option<&str>) -> bool {
    let get = |k: &str| h.get(k).and_then(|v| v.to_str().ok()).unwrap_or("");
    let Some((ua, orig)) = validate(get("user-agent"), get("originator")) else {
        return false;
    };
    let now = now_ms();
    let save = {
        let r = STATE.read().unwrap_or_else(|e| e.into_inner());
        let same = r.fp.user_agent == ua && r.fp.originator == orig && r.fp.source == "learned";
        if same && now - r.persisted_ms < PERSIST_EVERY_MS {
            drop(r);
            let mut w = STATE.write().unwrap_or_else(|e| e.into_inner());
            w.fp.learned_at_ms = Some(now);
            w.fp.key = key_label.map(str::to_string);
            return true;
        }
        !same || now - r.persisted_ms >= PERSIST_EVERY_MS
    };
    let mut w = STATE.write().unwrap_or_else(|e| e.into_inner());
    let changed = w.fp.user_agent != ua || w.fp.originator != orig;
    w.fp = Fingerprint {
        user_agent: ua,
        originator: orig,
        source: "learned".into(),
        learned_at_ms: Some(now),
        key: key_label.map(str::to_string),
    };
    if changed {
        tracing::info!(user_agent = %w.fp.user_agent, originator = %w.fp.originator, "learned client fingerprint");
    }
    if save {
        w.persisted_ms = now;
        if let Some(s) = &w.saver {
            s(&w.fp);
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
    w.fp = fp;
    true
}

/// Install the persistence hook (called synchronously on change; keep it cheap / spawn).
pub fn set_saver(s: Saver) {
    STATE.write().unwrap_or_else(|e| e.into_inner()).saver = Some(s);
}

/// Tests only: back to the built-in default, no saver.
pub fn reset_for_tests() {
    let mut w = STATE.write().unwrap_or_else(|e| e.into_inner());
    w.fp = Fingerprint::builtin();
    w.persisted_ms = 0;
    w.saver = None;
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
}
