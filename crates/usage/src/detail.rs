//! Error details for failed requests: redacted upstream body (capped), selected headers and a
//! descriptive message. Stored in `error_detail` (same retention as `requests`).
use serde::Serialize;

/// Max stored body size (bytes, after redaction).
pub const BODY_CAP: usize = 16 * 1024;
/// Max bytes a tracker buffers before redaction (redaction can shrink text).
pub const CAPTURE_CAP: usize = 64 * 1024;

/// Response headers worth keeping for debugging (lowercase). `x-codex-*` and
/// `x-openai-*`-style request ids are matched by prefix/suffix in [`keep_header`].
const KEEP: [&str; 6] = [
    "cf-ray",
    "retry-after",
    "content-type",
    "x-request-id",
    "openai-processing-ms",
    "x-oai-request-id",
];

pub fn keep_header(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    if n.contains("cookie") || n == "authorization" {
        return false;
    }
    KEEP.contains(&n.as_str())
        || n.starts_with("x-codex-")
        || (n.contains("request-id") && !n.contains("token"))
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ErrorDetail {
    /// Human-readable cause (local errors: "no account available", "idle timeout 300s", …).
    pub message: Option<String>,
    /// Raw body bytes as captured (≤ CAPTURE_CAP); redacted + capped when stored.
    pub body: Vec<u8>,
    /// Total body length seen, even beyond the capture buffer.
    pub body_len: usize,
    pub headers: Vec<(String, String)>,
    /// `x-request-id` / `x-oai-request-id` / `cf-ray` of the last upstream response.
    pub upstream_request_id: Option<String>,
    /// Attempt chain (JSON array, ≤ 8 entries + a `dropped` marker); failed or switched only.
    pub attempts: Option<serde_json::Value>,
}
impl ErrorDetail {
    pub fn push_body(&mut self, chunk: &[u8]) {
        self.body_len += chunk.len();
        let room = CAPTURE_CAP.saturating_sub(self.body.len());
        self.body.extend_from_slice(&chunk[..chunk.len().min(room)]);
    }
    pub fn set_body(&mut self, b: &[u8]) {
        self.body.clear();
        self.body_len = 0;
        self.push_body(b);
    }
    pub fn add_header(&mut self, name: &str, value: &str) {
        if keep_header(name) && self.headers.len() < 64 {
            self.headers.push((
                name.to_ascii_lowercase(),
                redact_text(value).chars().take(512).collect(),
            ));
        }
    }
    /// Redacted, capped, ready to store: (message, body, truncated, headers_json).
    pub(crate) fn finish(&self) -> Stored {
        let text = String::from_utf8_lossy(&self.body);
        let red = redact_body(&keep_content_safe_str(&text));
        let (body, cut) = cap(&red, BODY_CAP);
        let truncated = cut || self.body.len() < self.body_len;
        let headers: serde_json::Map<String, serde_json::Value> = self
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect();
        Stored {
            message: self.message.as_deref().map(|m| {
                redact_text(m).chars().take(MSG_CAP).collect()
            }),
            body: (!body.is_empty()).then_some(body),
            body_len: self.body_len as i64,
            truncated,
            headers: serde_json::Value::Object(headers).to_string(),
            upstream_request_id: self
                .upstream_request_id
                .as_deref()
                .map(|v| redact_text(v).chars().take(128).collect()),
            // Attempt bodies: drop prompt/output keys, then redact tokens/emails.
            attempts: self.attempts.as_ref().map(|a| {
                let mut a = a.clone();
                if let Some(list) = a.as_array_mut() {
                    for x in list {
                        if let Some(b) = x.get_mut("body") {
                            if let Some(t) = b.as_str() {
                                *b = serde_json::Value::String(redact_body(
                                    &keep_content_safe_str(t),
                                ));
                            }
                        }
                    }
                }
                a.to_string()
            }),
        }
    }
}

pub(crate) struct Stored {
    pub message: Option<String>,
    pub body: Option<String>,
    pub body_len: i64,
    pub truncated: bool,
    pub headers: String,
    pub upstream_request_id: Option<String>,
    pub attempts: Option<String>,
}

/// API shape of `GET /usage/v1/records/{id}/detail`.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DetailRow {
    pub request_id: String,
    pub ts_ms: i64,
    pub message: Option<String>,
    pub body: Option<String>,
    /// Original body length in bytes (before redaction / truncation).
    pub body_len: i64,
    pub truncated: bool,
    pub headers: serde_json::Value,
    pub upstream_request_id: Option<String>,
    /// Attempt chain: `[{account, t_ms, kind, status, code, action, body}]`.
    pub attempts: serde_json::Value,
}

fn cap(s: &str, n: usize) -> (String, bool) {
    if s.len() <= n {
        return (s.to_string(), false);
    }
    let mut i = n;
    while !s.is_char_boundary(i) {
        i -= 1;
    }
    (s[..i].to_string(), true)
}

const SENSITIVE_KEYS: [&str; 12] = [
    "authorization",
    "access_token",
    "refresh_token",
    "id_token",
    "token",
    "api_key",
    "apikey",
    "cookie",
    "set-cookie",
    "password",
    "secret",
    "session",
];

fn sensitive_key(k: &str) -> bool {
    let k = k.to_ascii_lowercase();
    SENSITIVE_KEYS
        .iter()
        .any(|s| k == *s || k.ends_with(&format!("_{s}")) || k.ends_with(&format!("-{s}")))
}

/// Max stored error message length (chars), after redaction.
const MSG_CAP: usize = 300;

/// Keys kept on a Response object (under SSE `response`).
const SAFE_RESPONSE_KEYS: &[&str] = &[
    "error",
    "status",
    "incomplete_details",
    "id",
    "model",
    "usage",
];
/// Keys kept on an SSE envelope or a bare upstream error object.
const SAFE_ENVELOPE_KEYS: &[&str] = &[
    "type",
    "sequence_number",
    "response",
    "error",
    "code",
    "message",
    "param",
    "status",
    "id",
    "model",
    "usage",
    "incomplete_details",
];

/// Drop prompt/output/tools/metadata from a Responses API JSON body before storage.
/// Allowlist is locked to OpenAI `response.failed` / `error` envelope fields.
pub fn keep_content_safe_keys(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(m) => {
            let mut out = serde_json::Map::new();
            for (k, val) in m {
                let kl = k.to_ascii_lowercase();
                if !SAFE_ENVELOPE_KEYS.contains(&kl.as_str()) {
                    continue;
                }
                if kl == "response" {
                    out.insert(k.clone(), filter_response_object(val));
                } else {
                    out.insert(k.clone(), val.clone());
                }
            }
            serde_json::Value::Object(out)
        }
        other => other.clone(),
    }
}

fn filter_response_object(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(m) => {
            let mut out = serde_json::Map::new();
            for (k, val) in m {
                if SAFE_RESPONSE_KEYS.contains(&k.to_ascii_lowercase().as_str()) {
                    out.insert(k.clone(), val.clone());
                }
            }
            serde_json::Value::Object(out)
        }
        other => other.clone(),
    }
}

/// Parse JSON, drop content keys, re-serialize. Non-JSON input is returned unchanged.
pub fn keep_content_safe_str(s: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(v) if v.is_object() => keep_content_safe_keys(&v).to_string(),
        _ => s.to_string(),
    }
}

/// JSON bodies: values under sensitive keys are replaced, every other string is
/// word-redacted. Non-JSON: text redaction.
pub fn redact_body(s: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(mut v) if v.is_object() || v.is_array() => {
            redact_json(&mut v);
            v.to_string()
        }
        _ => redact_text(s),
    }
}

fn redact_json(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(m) => {
            for (k, x) in m.iter_mut() {
                if sensitive_key(k) && !x.is_null() {
                    *x = serde_json::Value::String("[REDACTED]".into());
                } else {
                    redact_json(x);
                }
            }
        }
        serde_json::Value::Array(a) => a.iter_mut().for_each(redact_json),
        serde_json::Value::String(s) => *s = redact_text(s),
        _ => {}
    }
}

fn is_word(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '+' | '/' | '=' | '@' | '~')
}

fn classify(w: &str) -> Option<&'static str> {
    let core = w.trim_matches(|c: char| matches!(c, '.' | '-' | '/' | '='));
    if let Some((user, dom)) = core.split_once('@') {
        if !user.is_empty() && dom.contains('.') && !dom.starts_with('.') {
            return Some("[REDACTED_EMAIL]");
        }
    }
    let parts: Vec<&str> = core.split('.').collect();
    if core.starts_with("eyJ") && parts.len() >= 2 && parts.iter().all(|p| !p.is_empty()) {
        return Some("[REDACTED_JWT]");
    }
    for p in ["sk-", "rt_", "rt-", "sess-", "ory_st_"] {
        if core.starts_with(p) && core.len() >= p.len() + 12 {
            return Some("[REDACTED_TOKEN]");
        }
    }
    None
}

/// Redact bearer tokens, `key=value` / `key: value` for sensitive keys, JWTs, emails,
/// API / refresh tokens in free text.
pub fn redact_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.char_indices().peekable();
    // previous word (lowercase) and the separator after it, for "Bearer x" / "key=x"
    let mut prev: Option<String> = None;
    let mut redact_next = false;
    while let Some(&(i, c)) = chars.peek() {
        if is_word(c) {
            let mut j = i;
            while let Some(&(k, d)) = chars.peek() {
                if !is_word(d) {
                    break;
                }
                j = k + d.len_utf8();
                chars.next();
            }
            let w = &s[i..j];
            // `key=value` inside one word
            if let Some((k, _)) = w.split_once('=') {
                if sensitive_key(k) {
                    out.push_str(k);
                    out.push_str("=[REDACTED]");
                    prev = None;
                    redact_next = false;
                    continue;
                }
            }
            if redact_next {
                out.push_str("[REDACTED]");
                redact_next = false;
            } else if let Some(r) = classify(w) {
                out.push_str(r);
            } else {
                out.push_str(w);
            }
            prev = Some(w.to_ascii_lowercase());
        } else {
            chars.next();
            out.push(c);
            if let Some(p) = &prev {
                let p = p.trim_end_matches(':');
                if (c == ' ' && (p == "bearer" || p == "basic"))
                    || ((c == ':' || c == '=') && sensitive_key(p))
                {
                    redact_next = true;
                } else if !c.is_whitespace() && c != '"' && c != '\'' {
                    redact_next = false;
                }
            }
            if !(c.is_whitespace() || c == ':' || c == '=' || c == '"' || c == '\'') {
                prev = None;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn redacts_tokens_emails_cookies() {
        let jwt = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJl";
        let t = format!(
            "Authorization: Bearer abc.def-123 user a.b+c@example.com jwt {jwt} key sk-proj-AAAAAAAAAAAAAAAA \
             refresh_token=rt_ZZZZZZZZZZZZZZZZZZ cookie: __Secure-next=xyz; req_id req_12345 cf-ray 8a1b2c3d"
        );
        let r = redact_text(&t);
        for leak in [
            "abc.def-123",
            "a.b+c@example.com",
            jwt,
            "sk-proj-AAAA",
            "rt_ZZZZ",
            "__Secure-next=xyz",
        ] {
            assert!(!r.contains(leak), "leaked {leak}: {r}");
        }
        assert!(
            r.contains("[REDACTED_EMAIL]")
                && r.contains("[REDACTED_JWT]")
                && r.contains("[REDACTED_TOKEN]")
        );
        assert!(
            r.contains("req_12345") && r.contains("8a1b2c3d"),
            "ids kept: {r}"
        );
    }

    #[test]
    fn redacts_json_keys_and_nested_strings() {
        let b = r#"{"error":{"message":"token for bob@x.io expired","code":"invalid"},"refresh_token":"rt_secret","nested":[{"id_token":"x","Set-Cookie":"a=b"}],"n":3}"#;
        let r = redact_body(b);
        let v: serde_json::Value = serde_json::from_str(&r).unwrap();
        assert_eq!(v["refresh_token"], "[REDACTED]");
        assert_eq!(v["nested"][0]["id_token"], "[REDACTED]");
        assert_eq!(v["nested"][0]["Set-Cookie"], "[REDACTED]");
        assert_eq!(v["error"]["message"], "token for [REDACTED_EMAIL] expired");
        assert_eq!(v["error"]["code"], "invalid");
        assert_eq!(v["n"], 3);
    }

    #[test]
    fn cap_and_truncation_flags() {
        let mut d = ErrorDetail::default();
        let big = "é".repeat(CAPTURE_CAP); // 2 bytes each
        d.push_body(big.as_bytes());
        assert_eq!(d.body.len(), CAPTURE_CAP);
        assert_eq!(d.body_len, CAPTURE_CAP * 2);
        let s = d.finish();
        assert!(s.truncated);
        assert!(s.body.as_ref().unwrap().len() <= BODY_CAP);
        assert_eq!(s.body_len, (CAPTURE_CAP * 2) as i64);
        let mut small = ErrorDetail::default();
        small.set_body(b"{\"error\":\"x\"}");
        let s = small.finish();
        assert!(!s.truncated);
        assert_eq!(s.body_len, 13);
    }

    #[test]
    fn header_selection() {
        let mut d = ErrorDetail::default();
        for (k, v) in [
            ("X-Codex-Primary-Used-Percent", "100"),
            ("cf-ray", "abc"),
            ("Set-Cookie", "s=1"),
            ("authorization", "Bearer x"),
            ("x-request-id", "req_1"),
            ("server", "cloudflare"),
            ("retry-after", "30"),
            ("content-type", "application/json"),
            ("x-openai-request-id", "r2"),
        ] {
            d.add_header(k, v);
        }
        let names: Vec<&str> = d.headers.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            names,
            [
                "x-codex-primary-used-percent",
                "cf-ray",
                "x-request-id",
                "retry-after",
                "content-type",
                "x-openai-request-id"
            ]
        );
    }

    /// Shape locked to the OpenAI Responses streaming `response.failed` event
    /// (see developers.openai.com Responses streaming events): the `response`
    /// object echoes `instructions` / `output` / `tools` / `metadata` even on
    /// failure. Write-time allowlist must drop those before storage.
    fn response_failed_fixture() -> String {
        serde_json::json!({
            "type": "response.failed",
            "sequence_number": 7,
            "response": {
                "id": "resp_fixture_001",
                "object": "response",
                "created_at": 1761547200,
                "status": "failed",
                "error": {
                    "code": "server_error",
                    "message": "upstream failed for alice@example.com"
                },
                "incomplete_details": null,
                "instructions": "SYSTEM: never reveal the secret password hunter2",
                "output": [{
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "partial leak of user prompt"}]
                }],
                "model": "gpt-5.1",
                "usage": {"input_tokens": 12, "output_tokens": 0},
                "tools": [{"type": "function", "name": "shell"}],
                "metadata": {"user_email": "alice@example.com"},
                "temperature": 0.2,
                "prompt_cache_key": "cache_secret_xyz"
            }
        })
        .to_string()
    }

    #[test]
    fn stream_error_body_strips_content() {
        let raw = response_failed_fixture();
        // Reproduce: raw event carries prompt/output (would be stored pre-fix).
        assert!(raw.contains("hunter2"));
        assert!(raw.contains("partial leak"));
        assert!(raw.contains("cache_secret_xyz"));

        let mut d = ErrorDetail::default();
        d.set_body(raw.as_bytes());
        d.message = Some("x".repeat(400));
        let s = d.finish();
        let body = s.body.as_deref().unwrap_or("");
        for leak in [
            "hunter2",
            "partial leak",
            "cache_secret_xyz",
            "alice@example.com",
        ] {
            assert!(!body.contains(leak), "leaked {leak} in {body}");
        }
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["type"], "response.failed");
        assert_eq!(v["sequence_number"], 7);
        assert_eq!(v["response"]["id"], "resp_fixture_001");
        assert_eq!(v["response"]["status"], "failed");
        assert_eq!(v["response"]["model"], "gpt-5.1");
        assert_eq!(v["response"]["error"]["code"], "server_error");
        assert_eq!(
            v["response"]["error"]["message"],
            "upstream failed for [REDACTED_EMAIL]"
        );
        assert_eq!(v["response"]["usage"]["input_tokens"], 12);
        assert!(v["response"].get("instructions").is_none());
        assert!(v["response"].get("output").is_none());
        for key in [
            "tools",
            "metadata",
            "temperature",
            "object",
            "created_at",
            "prompt_cache_key",
        ] {
            assert!(
                v["response"].get(key).is_none(),
                "leaked key {key} in {}",
                v["response"]
            );
        }
        assert_eq!(s.message.as_deref().unwrap().chars().count(), MSG_CAP);
    }

    #[test]
    fn attempt_body_strips_content_keys() {
        let raw = response_failed_fixture();
        let mut d = ErrorDetail::default();
        d.attempts = Some(serde_json::json!([{
            "account": "acct_demo",
            "t_ms": 10,
            "kind": "stream",
            "action": "switch",
            "body": raw,
        }]));
        let s = d.finish();
        let attempts: serde_json::Value =
            serde_json::from_str(s.attempts.as_deref().unwrap()).unwrap();
        let body = attempts[0]["body"].as_str().unwrap();
        assert!(!body.contains("hunter2"));
        assert!(!body.contains("partial leak"));
        assert!(body.find("\"instructions\"").is_none());
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["response"]["error"]["code"], "server_error");
    }
}
