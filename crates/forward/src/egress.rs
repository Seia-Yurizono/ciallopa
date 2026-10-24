//! Cloudflare challenge / block pages in front of the upstream (e.g. error 1010, browser
//! signature banned). They block this host's egress (IP, TLS or HTTP fingerprint), not an
//! account: no account is cooled, the request is not moved to another account (every account
//! shares the egress), the client gets a neutral upstream-style 502 and the operator sees a
//! warning in the log and on the overview. State is in memory only.
use axum::response::Response;
use ciallopa_routing::{cloudflare_block, CloudflareBlock};
use http::{HeaderMap, StatusCode};
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// A warn line at most this often while the block lasts (plus one when it changes kind).
const LOG_EVERY_MS: u64 = 60_000;

/// A Cloudflare page detected in an upstream answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Block {
    /// Record / log code: `challenge` (managed challenge) or `cf_blocked` (Cloudflare error).
    pub code: &'static str,
    /// Cloudflare `1xxx` error code when the page gives one.
    pub cf_code: Option<u16>,
}

/// Only 403s are considered; other statuses keep their own handling.
pub(crate) fn detect(status: u16, h: &HeaderMap, body: &[u8]) -> Option<Block> {
    if status != 403 {
        return None;
    }
    match cloudflare_block(h.contains_key("cf-mitigated"), body)? {
        CloudflareBlock::Challenge => Some(Block {
            code: "challenge",
            cf_code: None,
        }),
        CloudflareBlock::Error(n) => Some(Block {
            code: "cf_blocked",
            cf_code: n,
        }),
    }
}

impl Block {
    pub fn describe(&self) -> String {
        match (self.code, self.cf_code) {
            ("challenge", _) => "Cloudflare challenge page instead of the upstream: this host's egress (IP / TLS fingerprint) is challenged; accounts are not cooled".into(),
            (_, Some(n)) => format!("Cloudflare error {n} instead of the upstream: this host's egress is blocked; accounts are not cooled"),
            _ => "Cloudflare block page instead of the upstream: this host's egress is blocked; accounts are not cooled".into(),
        }
    }
}

/// Snapshot for the management API.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Seen {
    pub code: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cf_code: Option<u16>,
    pub upstream_status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cf_ray: Option<String>,
    pub account: String,
    pub first_at_ms: u64,
    pub last_at_ms: u64,
    pub count: u64,
    /// No upstream success since the last block.
    pub active: bool,
}

#[derive(Default)]
struct Inner {
    last: Option<Seen>,
    logged_ms: u64,
}

/// Last Cloudflare block seen by this instance.
#[derive(Default)]
pub struct EgressStatus {
    inner: Mutex<Inner>,
    last_ok_ms: AtomicU64,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl EgressStatus {
    /// An upstream answered normally (cheap: one relaxed store).
    pub fn ok(&self) {
        self.last_ok_ms.store(now_ms(), Ordering::Relaxed);
    }

    pub(crate) fn note(&self, b: &Block, status: u16, h: &HeaderMap, account: &str) {
        let now = now_ms();
        let ray = h
            .get("cf-ray")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.chars().take(64).collect::<String>());
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let ok = self.last_ok_ms.load(Ordering::Relaxed);
        let (first, count, same) = match &g.last {
            // The same episode continues until an upstream success.
            Some(s) if s.last_at_ms >= ok => (
                s.first_at_ms,
                s.count.saturating_add(1),
                s.code == b.code && s.cf_code == b.cf_code,
            ),
            _ => (now, 1, false),
        };
        g.last = Some(Seen {
            code: b.code,
            cf_code: b.cf_code,
            upstream_status: status,
            cf_ray: ray.clone(),
            account: account.to_string(),
            first_at_ms: first,
            last_at_ms: now,
            count,
            active: true,
        });
        if !same || now.saturating_sub(g.logged_ms) >= LOG_EVERY_MS {
            g.logged_ms = now;
            drop(g);
            tracing::warn!(
                code = b.code,
                cf_code = b.cf_code,
                status,
                cf_ray = ray.as_deref().unwrap_or(""),
                %account,
                count,
                "{}; failing the request without switching accounts (check the egress IP, proxy or client fingerprint)",
                b.describe()
            );
        }
    }

    pub fn snapshot(&self) -> Option<Seen> {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut s = g.last.clone()?;
        s.active = s.last_at_ms >= self.last_ok_ms.load(Ordering::Relaxed);
        Some(s)
    }
}

/// The client-facing answer: the same neutral upstream-style 502 as any other gateway failure.
/// The Cloudflare details stay in the record and the log.
pub(crate) fn response(b: &Block, status: u16, h: &HeaderMap) -> Response {
    let mut r = crate::errors::local(
        StatusCode::BAD_GATEWAY,
        crate::errors::SRC_NETWORK,
        b.code,
        None,
    );
    if let Some(m) = r.extensions_mut().get_mut::<crate::errors::ErrMeta>() {
        m.upstream_status = Some(status);
        m.upstream_request_id = crate::errors::request_id(h);
        m.message = Some(b.describe());
    }
    r
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn h() -> HeaderMap {
        HeaderMap::new()
    }

    #[test]
    fn detects_cloudflare_pages_only() {
        let blk = |s: u16, b: &str| detect(s, &h(), b.as_bytes());
        let e = |n| {
            Some(Block {
                code: "cf_blocked",
                cf_code: n,
            })
        };
        assert_eq!(blk(403, "error code: 1010"), e(Some(1010)));
        assert_eq!(blk(403, "error code: 1020\n"), e(Some(1020)));
        assert_eq!(
            blk(
                403,
                r#"{"cloudflare_error":true,"error_code":1010,"error_name":"browser_signature_banned"}"#
            ),
            e(Some(1010))
        );
        assert_eq!(
            blk(403, r#"{"error_name":"browser_signature_banned"}"#),
            e(Some(1010))
        );
        assert_eq!(
            blk(
                403,
                r#"<html><title>Attention Required! | Cloudflare</title><span class="cf-error-code">1020</span></html>"#
            ),
            e(Some(1020))
        );
        assert_eq!(
            blk(403, "<html>Just a moment... cloudflare</html>")
                .unwrap()
                .code,
            "challenge"
        );
        let mut mh = h();
        mh.insert("cf-mitigated", "challenge".parse().unwrap());
        assert_eq!(detect(403, &mh, b"").unwrap().code, "challenge");
        // Real auth failures and other statuses are not Cloudflare blocks.
        for b in [
            r#"{"detail":"Unauthorized"}"#,
            r#"{"error":{"message":"You do not have access to this workspace","code":"forbidden"}}"#,
            r#"{"error":{"message":"error code: 1010 mentioned in a sentence"}}"#,
            "error code: 1010 and more",
            "forbidden",
            "",
        ] {
            assert_eq!(blk(403, b), None, "{b}");
        }
        assert_eq!(blk(401, "error code: 1010"), None);
        assert_eq!(blk(429, "error code: 1015"), None);
    }

    #[test]
    fn status_tracks_an_episode_until_success() {
        let s = EgressStatus::default();
        assert!(s.snapshot().is_none());
        let b = detect(403, &h(), b"error code: 1010").unwrap();
        s.note(&b, 403, &h(), "a");
        s.note(&b, 403, &h(), "b");
        let x = s.snapshot().unwrap();
        assert_eq!((x.count, x.active, x.account.as_str()), (2, true, "b"));
        std::thread::sleep(std::time::Duration::from_millis(2));
        s.ok();
        let x = s.snapshot().unwrap();
        assert!(!x.active);
        std::thread::sleep(std::time::Duration::from_millis(2));
        s.note(&b, 403, &h(), "a");
        assert_eq!(s.snapshot().unwrap().count, 1);
    }
}
