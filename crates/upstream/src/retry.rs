//! Retry policy mirroring codex-rs `codex-api` / `async-utils::backoff`.
use ciallopa_core::retry_after::{parse, Format};
use http::HeaderMap;
use rand::Rng;
use std::time::{Duration, SystemTime};

const INITIAL_DELAY_MS: u64 = 200;
const BACKOFF_FACTOR: f64 = 2.0;
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);

/// Same formula as codex-rs: 200ms * 2^(attempt-1), jitter 0.9..1.1. `attempt` is 1-based.
fn backoff(attempt: u64) -> Duration {
    let exp = BACKOFF_FACTOR.powi(attempt.saturating_sub(1) as i32);
    let base = (INITIAL_DELAY_MS as f64 * exp) as u64;
    let jitter = rand::rng().random_range(0.9..1.1);
    Duration::from_millis((base as f64 * jitter) as u64)
}

/// WS and overload advice retain floating-point delta-seconds semantics.
pub fn retry_after(h: &HeaderMap) -> Option<Duration> {
    let v = h.get(http::header::RETRY_AFTER)?.to_str().ok()?.trim();
    parse(v, SystemTime::now(), Format::Seconds)
}

/// Delay before retry `attempt` (1-based): Retry-After (capped) if present, else backoff.
pub fn delay_for(attempt: u64, h: Option<&HeaderMap>) -> Duration {
    h.and_then(retry_after)
        .map(|d| d.min(MAX_RETRY_AFTER))
        .unwrap_or_else(|| backoff(attempt))
}

/// Short jittered pause (20–120 ms) before failing over to another account, so a burst of
/// failures does not hit the next account in lockstep.
pub fn switch_jitter() -> Duration {
    Duration::from_millis(rand::rng().random_range(20..120))
}

pub const OVERLOAD_RETRY_AFTER_MAX: Duration = Duration::from_secs(10);

/// Pause before the one same-account retry after an overload: `Retry-After` (capped at
/// [`OVERLOAD_RETRY_AFTER_MAX`]) if present, else `base_secs` jittered to 2/3–5/3 of it.
/// `None` when disabled (`base_secs == 0`).
pub fn overload_wait(base_secs: u64, h: Option<&HeaderMap>) -> Option<Duration> {
    if base_secs == 0 {
        return None;
    }
    if let Some(ra) = h.and_then(retry_after) {
        return Some(ra.min(OVERLOAD_RETRY_AFTER_MAX));
    }
    let ms = base_secs.saturating_mul(1000);
    Some(Duration::from_millis(
        rand::rng().random_range(ms * 2 / 3..=ms * 5 / 3),
    ))
}

/// HTTP statuses retried before the first downstream byte (408 / 5xx).
pub fn is_retryable_status(s: u16) -> bool {
    s == 408 || (500..600).contains(&s)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn backoff_matches_codex() {
        for (n, base) in [(1u64, 200u64), (2, 400), (3, 800), (4, 1600)] {
            let d = backoff(n).as_millis() as u64;
            assert!(d >= base * 9 / 10 && d <= base * 11 / 10, "{n}: {d}");
        }
    }
    #[test]
    fn overload_wait_bounds() {
        assert_eq!(overload_wait(0, None), None);
        for _ in 0..50 {
            let d = overload_wait(3, None).unwrap();
            assert!(d >= Duration::from_secs(2) && d <= Duration::from_secs(5));
        }
        let mut h = HeaderMap::new();
        h.insert(http::header::RETRY_AFTER, "1".parse().unwrap());
        assert_eq!(overload_wait(3, Some(&h)), Some(Duration::from_secs(1)));
        h.insert(http::header::RETRY_AFTER, "120".parse().unwrap());
        assert_eq!(overload_wait(3, Some(&h)), Some(OVERLOAD_RETRY_AFTER_MAX));
    }

    #[test]
    fn retry_advice_caps_and_fallback() {
        let mut h = HeaderMap::new();
        for (raw, expected) in [
            ("3", Duration::from_secs(3)),
            ("0.25", Duration::from_millis(250)),
            ("999999", MAX_RETRY_AFTER),
        ] {
            h.insert(http::header::RETRY_AFTER, raw.parse().unwrap());
            assert_eq!(delay_for(1, Some(&h)), expected);
        }
        h.insert(http::header::RETRY_AFTER, "junk".parse().unwrap());
        let delay = delay_for(1, Some(&h));
        assert!((Duration::from_millis(180)..=Duration::from_millis(220)).contains(&delay));
    }

    #[test]
    fn retryable_statuses() {
        for (status, expected) in [
            (400, false),
            (408, true),
            (429, false),
            (499, false),
            (500, true),
            (599, true),
            (600, false),
        ] {
            assert_eq!(is_retryable_status(status), expected, "{status}");
        }
    }
}
