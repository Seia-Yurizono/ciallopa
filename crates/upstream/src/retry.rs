//! Retry policy mirroring codex-rs `codex-api` / `async-utils::backoff`.
use http::HeaderMap;
use rand::Rng;
use std::time::{Duration, SystemTime};

const INITIAL_DELAY_MS: u64 = 200;
const BACKOFF_FACTOR: f64 = 2.0;
/// Upper bound for an honored `Retry-After` before the first byte (keeps requests bounded).
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);

/// Same formula as codex-rs: 200ms * 2^(attempt-1), jitter 0.9..1.1. `attempt` is 1-based.
pub fn backoff(attempt: u64) -> Duration {
    let exp = BACKOFF_FACTOR.powi(attempt.saturating_sub(1) as i32);
    let base = (INITIAL_DELAY_MS as f64 * exp) as u64;
    let jitter = rand::rng().random_range(0.9..1.1);
    Duration::from_millis((base as f64 * jitter) as u64)
}

/// `Retry-After` as delta-seconds (RFC 9110 also allows an HTTP-date; Codex backend uses seconds).
pub fn retry_after(h: &HeaderMap) -> Option<Duration> {
    let v = h.get(http::header::RETRY_AFTER)?.to_str().ok()?.trim();
    if let Ok(s) = v.parse::<f64>() {
        return (s >= 0.0).then(|| Duration::from_secs_f64(s));
    }
    None
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

/// Upper bound for an honored `Retry-After` on the single overload wait-and-retry.
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

/// Status codes retried before the first downstream byte (Codex: 5xx + transport only).
pub fn is_retryable_status(s: u16) -> bool {
    (500..600).contains(&s)
}

/// Parse `usage_limit_reached` style bodies: `{"error":{"type":..,"resets_at":..,"resets_in_seconds":..}}`.
/// Returns (is_usage_limit, retry_after).
pub fn parse_limit_body(body: &[u8]) -> (bool, Option<Duration>) {
    let Ok(v) = serde_json_lite(body) else {
        return (false, None);
    };
    let e = v.get("error").unwrap_or(&v);
    let ty = e
        .get("type")
        .or_else(|| e.get("code"))
        .and_then(|t| t.as_str())
        .unwrap_or("");
    let usage = ty.contains("usage_limit");
    let ra = e
        .get("resets_in_seconds")
        .and_then(|x| x.as_f64())
        .map(|s| Duration::from_secs_f64(s.max(0.0)))
        .or_else(|| {
            let at = e.get("resets_at").and_then(|x| x.as_f64())?;
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .ok()?
                .as_secs_f64();
            Some(Duration::from_secs_f64((at - now).max(0.0)))
        });
    (usage, ra)
}

fn serde_json_lite(b: &[u8]) -> Result<serde_json::Value, serde_json::Error> {
    serde_json::from_slice(b)
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
    fn retry_after_and_body() {
        let mut h = HeaderMap::new();
        h.insert("retry-after", "3".parse().expect("hv"));
        assert_eq!(delay_for(1, Some(&h)), Duration::from_secs(3));
        let (u, ra) = parse_limit_body(
            br#"{"error":{"type":"usage_limit_reached","resets_in_seconds":120}}"#,
        );
        assert!(u);
        assert_eq!(ra, Some(Duration::from_secs(120)));
        assert_eq!(parse_limit_body(b"nope"), (false, None));
    }
}
