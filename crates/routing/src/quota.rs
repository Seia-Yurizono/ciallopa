//! Quota / retry-after parsing. Ports CPA `helps/codex_quota.go`
//! (`ParseCodexQuotaEventHeaders`), `codex_retry_after.go` and
//! `codex_executor_terminal.go` (`parseCodexRetryAfter`, `isCodexUsageLimitError`).
use cpa_lite_core::{QuotaSnapshot, QuotaWindow};
use http::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;
use std::time::{Duration, SystemTime};

const MAX_ADDITIONAL: usize = 8;
/// CPA `codexMaxHeaderRetryAfter`.
pub const MAX_HEADER_RETRY_AFTER: Duration = Duration::from_secs(6 * 3600);

fn hget<'a>(h: &'a HeaderMap, n: &str) -> Option<&'a str> {
    h.get(n).and_then(|v| v.to_str().ok()).map(str::trim).filter(|s| !s.is_empty())
}

fn window(h: &HeaderMap, prefix: &str, now: SystemTime) -> Option<QuotaWindow> {
    let used: f64 = hget(h, &format!("{prefix}-used-percent"))?.parse().ok()?;
    if !(0.0..=100.0).contains(&used) || used.is_nan() {
        return None;
    }
    let window_minutes = hget(h, &format!("{prefix}-window-minutes")).and_then(|s| s.parse::<u64>().ok()).filter(|m| *m > 0);
    let resets_at = hget(h, &format!("{prefix}-reset-at"))
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|s| *s > 0)
        .map(|s| SystemTime::UNIX_EPOCH + Duration::from_secs(s as u64))
        .or_else(|| {
            hget(h, &format!("{prefix}-reset-after-seconds"))
                .and_then(|s| s.parse::<f64>().ok())
                .filter(|s| *s >= 0.0 && s.is_finite())
                .map(|s| now + Duration::from_secs_f64(s))
        });
    Some(QuotaWindow { used_percent: used, window_minutes, resets_at })
}

/// Base (`x-codex-primary-*` / `x-codex-secondary-*`) windows from response headers.
pub fn parse_quota(headers: &HeaderMap) -> Option<QuotaSnapshot> {
    parse_quota_at(headers, SystemTime::now())
}

pub fn parse_quota_at(headers: &HeaderMap, now: SystemTime) -> Option<QuotaSnapshot> {
    let s = QuotaSnapshot {
        primary: window(headers, "x-codex-primary", now),
        secondary: window(headers, "x-codex-secondary", now),
    };
    (s.primary.is_some() || s.secondary.is_some()).then_some(s)
}

/// Highest live used fraction (0..=1) across windows; windows whose reset passed are ignored.
pub fn max_used_fraction(q: &QuotaSnapshot, now: SystemTime) -> f64 {
    [&q.primary, &q.secondary]
        .into_iter()
        .flatten()
        .filter(|w| w.resets_at.is_none_or(|r| r > now))
        .map(|w| w.used_percent / 100.0)
        .fold(0.0, f64::max)
}

// ---------------- event frames (SSE / WS) → headers ----------------

#[derive(PartialEq)]
enum Kind {
    Other,
    Error,
    RateLimits,
}

fn event_kind(p: &[u8]) -> Kind {
    let end = p.len().min(256);
    let ws = |c: u8| matches!(c, b' ' | b'\t' | b'\r' | b'\n');
    let mut i = 0;
    while i + 6 < end {
        if &p[i..i + 6] != b"\"type\"" {
            i += 1;
            continue;
        }
        let mut j = i + 6;
        while j < end && ws(p[j]) {
            j += 1;
        }
        if j >= end || p[j] != b':' {
            i += 1;
            continue;
        }
        j += 1;
        while j < end && ws(p[j]) {
            j += 1;
        }
        if j + 7 <= end && &p[j..j + 7] == b"\"error\"" {
            return Kind::Error;
        }
        let rl = b"\"codex.rate_limits\"";
        if j + rl.len() <= end && &p[j..j + rl.len()] == rl {
            return Kind::RateLimits;
        }
        i += 1;
    }
    Kind::Other
}

fn first<'a>(v: &'a Value, paths: &[&str]) -> Option<&'a Value> {
    paths.iter().find_map(|p| v.get(*p).filter(|x| !x.is_null()))
}

fn scalar(v: &Value) -> Option<String> {
    let s = match v {
        Value::String(s) => s.trim().to_string(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => return None,
    };
    (!s.is_empty()).then_some(s)
}

fn first_str(v: &Value, paths: &[&str]) -> Option<String> {
    paths.iter().find_map(|p| first(v, &[p]).and_then(scalar))
}

fn set(h: &mut HeaderMap, name: &str, val: &str) -> bool {
    match (HeaderName::from_bytes(name.as_bytes()), HeaderValue::from_str(val)) {
        (Ok(n), Ok(v)) => {
            h.insert(n, v);
            true
        }
        _ => false,
    }
}

fn set_from(h: &mut HeaderMap, name: &str, obj: &Value, paths: &[&str]) -> bool {
    match first(obj, paths).and_then(scalar) {
        Some(s) => set(h, name, &s),
        None => false,
    }
}

fn as_i64(v: &Value) -> i64 {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)).unwrap_or(0),
        Value::String(s) => s.trim().parse().unwrap_or(0),
        _ => 0,
    }
}
fn as_f64(v: &Value) -> f64 {
    match v {
        Value::Number(n) => n.as_f64().unwrap_or(0.0),
        Value::String(s) => s.trim().parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn add_rate_limit(h: &mut HeaderMap, prefix: &str, info: Option<&Value>) -> bool {
    let Some(info) = info.filter(|v| v.is_object()) else { return false };
    let mut changed = set_from(h, &format!("{prefix}Allowed"), info, &["allowed"]);
    changed |= set_from(h, &format!("{prefix}Limit-Reached"), info, &["limit_reached", "limitReached"]);
    for (name, cap) in [("primary", "Primary"), ("secondary", "Secondary")] {
        let Some(w) = first(info, &[name]).filter(|v| v.is_object()) else { continue };
        let used = first(w, &["used_percent", "usedPercent"]);
        let mins = first(w, &["window_minutes", "windowMinutes"]);
        let ra = first(w, &["reset_after_seconds", "resetAfterSeconds"]);
        let rat = first(w, &["reset_at", "resetAt"]);
        let has_ra = ra.is_some_and(|v| as_i64(v) >= 0);
        let has_rat = rat.is_some_and(|v| as_i64(v) > 0);
        let (Some(used), Some(mins)) = (used, mins) else { continue };
        let u = as_f64(used);
        if !(0.0..=100.0).contains(&u) || as_i64(mins) <= 0 || (!has_ra && !has_rat) {
            continue;
        }
        let wp = format!("{prefix}{cap}-");
        set_from(h, &format!("{wp}Used-Percent"), w, &["used_percent", "usedPercent"]);
        set_from(h, &format!("{wp}Window-Minutes"), w, &["window_minutes", "windowMinutes"]);
        if has_ra {
            set_from(h, &format!("{wp}Reset-After-Seconds"), w, &["reset_after_seconds", "resetAfterSeconds"]);
        }
        if has_rat {
            set_from(h, &format!("{wp}Reset-At"), w, &["reset_at", "resetAt"]);
        }
        changed = true;
    }
    changed
}

fn valid_ident(v: &str) -> bool {
    let v = v.trim();
    !v.is_empty() && v.len() <= 256 && v.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
}
fn valid_text(v: &str) -> bool {
    let v = v.trim();
    !v.is_empty() && v.len() <= 256 && !v.contains(['\r', '\n'])
}
fn normalize_ident(v: &str) -> Option<String> {
    let v = v.trim();
    if v.is_empty() || v.len() > 128 {
        return None;
    }
    let mut out = String::new();
    let mut last_dash = false;
    for c in v.chars() {
        if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
            out.push(c);
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    let id = out.trim_matches(|c| matches!(c, '-' | '_' | '.')).to_string();
    (valid_ident(&id)).then_some(id)
}

fn is_quota_header_name(name: &str) -> bool {
    let l = name.trim().to_ascii_lowercase();
    if l == "retry-after" || l.starts_with("x-ratelimit-") {
        return true;
    }
    if l == "x-codex-active-limit" || l == "x-codex-plan-type" || l.starts_with("x-codex-credits-") {
        return true;
    }
    if !l.starts_with("x-codex-") {
        return false;
    }
    [
        "-allowed", "-limit-reached", "-limit-name", "-used-percent", "-window-minutes",
        "-reset-after-seconds", "-reset-at", "-over-secondary-limit-percent",
    ]
    .iter()
    .any(|m| l.contains(m))
}

/// CPA `ParseCodexQuotaEventHeaders`: `codex.rate_limits` frames / `error` frames
/// (with a `headers` object) → synthetic quota headers. `None` for anything else.
pub fn parse_quota_event_headers(payload: &[u8]) -> Option<HeaderMap> {
    if payload.is_empty() {
        return None;
    }
    let kind = event_kind(payload);
    if kind == Kind::Other {
        return None;
    }
    let root: Value = serde_json::from_slice(payload).ok()?;
    let mut h = HeaderMap::new();
    if kind == Kind::Error {
        let obj = root.get("headers")?.as_object()?;
        for (k, v) in obj {
            if is_quota_header_name(k) {
                if let Some(s) = scalar(v) {
                    set(&mut h, &k.trim().to_ascii_lowercase(), &s);
                }
            }
        }
        return (!h.is_empty()).then_some(h);
    }
    let mut has = add_rate_limit(&mut h, "X-Codex-", first(&root, &["rate_limits", "rateLimit"]));
    if let Some(add) = first(&root, &["additional_rate_limits", "additionalRateLimits"]) {
        let items: Vec<(String, &Value)> = match add {
            Value::Object(m) => m.iter().map(|(k, v)| (k.trim().to_string(), v)).collect(),
            Value::Array(a) => a
                .iter()
                .map(|v| (first_str(v, &["limit_name", "limitName", "name"]).unwrap_or_default(), v))
                .collect(),
            _ => vec![],
        };
        let mut n = 0;
        for (name, v) in items {
            if n >= MAX_ADDITIONAL {
                break;
            }
            if name.is_empty() {
                continue;
            }
            let Some(id) = normalize_ident(&name) else { continue };
            let info = first(v, &["rate_limit", "rateLimit"]).unwrap_or(v);
            let prefix = format!("X-Codex-Additional-{id}-");
            if add_rate_limit(&mut h, &prefix, Some(info)) {
                if valid_text(&name) {
                    set(&mut h, &format!("{prefix}Limit-Name"), &name);
                }
                has = true;
                n += 1;
            }
        }
    }
    has |= add_rate_limit(&mut h, "X-Codex-Code-Review-", first(&root, &["code_review_rate_limits", "codeReviewRateLimits"]));
    if let Some(c) = first(&root, &["credits"]).filter(|v| v.is_object()) {
        has |= set_from(&mut h, "X-Codex-Credits-Has-Credits", c, &["has_credits", "hasCredits"]);
        has |= set_from(&mut h, "X-Codex-Credits-Unlimited", c, &["unlimited"]);
        has |= set_from(&mut h, "X-Codex-Credits-Balance", c, &["balance"]);
    }
    if !has {
        return None;
    }
    if let Some(a) = first_str(&root, &["metered_limit_name", "meteredLimitName", "limit_name", "limitName"]) {
        if valid_ident(&a) {
            set(&mut h, "X-Codex-Active-Limit", &a);
        }
    }
    if let Some(p) = first_str(&root, &["plan_type", "planType"]) {
        if valid_text(&p) {
            set(&mut h, "X-Codex-Plan-Type", &p);
        }
    }
    Some(h)
}

// ---------------- 429 handling ----------------

/// CPA `parseRetryAfterHeader`: delta-seconds or HTTP-date, capped at 6h.
pub fn parse_retry_after_header(raw: &str, now: SystemTime) -> Option<Duration> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let d = if let Ok(secs) = raw.parse::<i64>() {
        if secs < 0 {
            return None;
        }
        Duration::from_secs(secs as u64)
    } else if let Ok(at) = httpdate::parse_http_date(raw) {
        at.duration_since(now).unwrap_or(Duration::ZERO)
    } else {
        return None;
    };
    Some(d.min(MAX_HEADER_RETRY_AFTER))
}

fn usage_limit_objs(body: &Value) -> impl Iterator<Item = &Value> {
    [body.get("error"), Some(body)]
        .into_iter()
        .flatten()
        .filter(|q| q.get("type").and_then(Value::as_str).is_some_and(|t| t.trim().eq_ignore_ascii_case("usage_limit_reached")))
}

/// CPA `isCodexUsageLimitError`: `error.type` or `type` == usage_limit_reached.
pub fn is_usage_limit(body: &[u8]) -> bool {
    serde_json::from_slice::<Value>(body).is_ok_and(|v| usage_limit_objs(&v).next().is_some())
}

/// CPA `parseCodexRetryAfter`: 429 usage_limit body → `resets_at` (if future) else `resets_in_seconds`.
pub fn parse_codex_retry_after(status: u16, body: &[u8], now: SystemTime) -> Option<Duration> {
    if status != 429 || body.is_empty() {
        return None;
    }
    let v: Value = serde_json::from_slice(body).ok()?;
    for q in usage_limit_objs(&v) {
        let at = q.get("resets_at").map(as_i64).unwrap_or(0);
        if at > 0 {
            let t = SystemTime::UNIX_EPOCH + Duration::from_secs(at as u64);
            if let Ok(d) = t.duration_since(now) {
                if !d.is_zero() {
                    return Some(d);
                }
            }
        }
        let secs = q.get("resets_in_seconds").map(as_i64).unwrap_or(0);
        if secs > 0 {
            return Some(Duration::from_secs(secs as u64));
        }
    }
    None
}

/// Body reset time first, then `Retry-After` header (CPA `withCodexRetryAfterHeader`).
pub fn retry_after_for_429(headers: &HeaderMap, body: &[u8], now: SystemTime) -> Option<Duration> {
    parse_codex_retry_after(429, body, now).or_else(|| hget(headers, "retry-after").and_then(|r| parse_retry_after_header(r, now)))
}

/// CPA `isCloudflareChallengeErrorMessage` (callers: only for status < 500).
pub fn is_cloudflare_challenge(body_or_msg: &str) -> bool {
    let l = body_or_msg.trim().to_lowercase();
    l.contains("challenge-platform")
        || l.contains("cf-mitigated")
        || l.contains("cloudflare challenge")
        || (l.contains("just a moment") && l.contains("cloudflare"))
}
