//! Retry-After parsing with the gateway's existing HTTP and WS policies.
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone, Copy)]
pub enum Format {
    /// HTTP forwarding: signed integer seconds (nonnegative) or an HTTP-date.
    Http,
    /// WS / overload retries: floating-point seconds, without HTTP-dates.
    Seconds,
}

/// Parse advice without imposing a cap; each caller owns its retry budget.
/// Separate formats preserve fractional HTTP and dated WS advice being ignored.
pub fn parse(raw: &str, now: SystemTime, format: Format) -> Option<Duration> {
    let raw = raw.trim();
    match format {
        Format::Seconds => Duration::try_from_secs_f64(raw.parse().ok()?).ok(),
        Format::Http => {
            if let Ok(secs) = raw.parse::<i64>() {
                return u64::try_from(secs).ok().map(Duration::from_secs);
            }
            let at = httpdate::parse_http_date(raw).ok()?;
            Some(at.duration_since(now).unwrap_or(Duration::ZERO))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advice_formats() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let date = httpdate::fmt_http_date(now + Duration::from_secs(90));
        let past = httpdate::fmt_http_date(now - Duration::from_secs(90));
        let secs = |n| Some(Duration::from_secs(n));
        for (raw, http, seconds) in [
            ("", None, None),
            (" 120 ", secs(120), secs(120)),
            ("0", secs(0), secs(0)),
            ("-0", secs(0), secs(0)),
            ("+1", secs(1), secs(1)),
            ("-1", None, None),
            ("junk", None, None),
            ("inf", None, None),
            ("NaN", None, None),
            ("1e309", None, None),
            ("1e100", None, None),
            ("0.25", None, Some(Duration::from_millis(250))),
            ("1e2", None, secs(100)),
            ("999999", secs(999999), secs(999999)),
            (date.as_str(), secs(90), None),
            (past.as_str(), secs(0), None),
        ] {
            assert_eq!(parse(raw, now, Format::Http), http, "HTTP {raw}");
            assert_eq!(parse(raw, now, Format::Seconds), seconds, "seconds {raw}");
        }
    }
}
