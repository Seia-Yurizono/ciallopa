//! Codex quota headers (`x-codex-{primary,secondary}-{used-percent,window-minutes,reset-at}`),
//! same names as codex-rs `codex-api/src/rate_limits.rs`.
//! TODO(lead): switch to `cpa_lite_routing::parse_quota` once worker B lands it.
use cpa_lite_core::{QuotaSnapshot, QuotaWindow};
use http::HeaderMap;
use std::time::{Duration, SystemTime};

fn num(h: &HeaderMap, k: &str) -> Option<f64> {
    h.get(k)?.to_str().ok()?.trim().parse().ok()
}

fn window(h: &HeaderMap, p: &str) -> Option<QuotaWindow> {
    let used = num(h, &format!("x-codex-{p}-used-percent"))?;
    Some(QuotaWindow {
        used_percent: used,
        window_minutes: num(h, &format!("x-codex-{p}-window-minutes")).map(|v| v as u64),
        resets_at: num(h, &format!("x-codex-{p}-reset-at"))
            .filter(|v| *v > 0.0)
            .map(|v| SystemTime::UNIX_EPOCH + Duration::from_secs(v as u64)),
    })
}

pub fn parse_quota(h: &HeaderMap) -> Option<QuotaSnapshot> {
    let primary = window(h, "primary");
    let secondary = window(h, "secondary");
    (primary.is_some() || secondary.is_some()).then_some(QuotaSnapshot { primary, secondary })
}

/// Quota from a WS `error` event `headers` object.
pub fn parse_quota_json(v: &serde_json::Value) -> Option<QuotaSnapshot> {
    let obj = v.as_object()?;
    let mut h = HeaderMap::new();
    for (k, val) in obj {
        let s = match val {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        if let (Ok(n), Ok(v)) = (
            http::HeaderName::from_bytes(k.to_ascii_lowercase().as_bytes()),
            http::HeaderValue::from_str(&s),
        ) {
            h.insert(n, v);
        }
    }
    parse_quota(&h)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    #[test]
    fn parses() {
        let mut h = HeaderMap::new();
        h.insert("x-codex-primary-used-percent", "42.5".parse().unwrap());
        h.insert("x-codex-primary-window-minutes", "300".parse().unwrap());
        h.insert("x-codex-secondary-used-percent", "3".parse().unwrap());
        let q = parse_quota(&h).unwrap();
        assert_eq!(q.primary.unwrap().window_minutes, Some(300));
        assert_eq!(q.secondary.unwrap().used_percent, 3.0);
        assert!(parse_quota(&HeaderMap::new()).is_none());
    }
}
