use ciallopa_core::retry_after::{parse as parse_retry_after, Format};
use ciallopa_core::{AccountId, Outcome};
use ciallopa_routing::{
    is_usage_limit, parse_quota, parse_quota_event_headers, retry_after_for_429,
};
use http::{HeaderMap, HeaderValue};
use std::borrow::Cow;
use std::time::{Duration, SystemTime};

#[derive(Debug, PartialEq)]
pub(crate) enum Class {
    Preamble,
    RateLimits(Option<ciallopa_core::QuotaSnapshot>),
    Created,
    Output,
    SoftError,
    Terminal(Terminal),
}

#[derive(Debug, PartialEq)]
pub(crate) enum Terminal {
    Completed,
    Incomplete,
    Failed,
    Error,
    Quota(Outcome, &'static str),
    Overloaded(String),
    Encrypted(String),
}

pub(crate) struct Classified<'a> {
    pub(crate) ty: Option<Cow<'a, str>>,
    pub(crate) class: Class,
    value: Option<serde_json::Value>,
}

impl Classified<'_> {
    pub(crate) fn value(&mut self, text: &str) -> Option<&serde_json::Value> {
        if self.value.is_none() {
            self.value = serde_json::from_str(text).ok();
        }
        self.value.as_ref()
    }

    pub(crate) fn ws_view(mut self, text: &str) -> Self {
        // Preserve the legacy WS view of unrelated frames. This adjusts protocol
        // semantics after classification; it never selects the JSON-tree parsing path.
        if !text.contains("response.")
            && !text.contains("codex.rate_limits")
            && !text.contains("\"error\"")
        {
            self.class = Class::Output;
        }
        if self.class == Class::Terminal(Terminal::Error)
            && !self.value.as_ref().is_some_and(error_frame_ends_turn)
        {
            self.class = Class::SoftError;
        }
        self
    }

    /// Retry advice carried in the classified event's JSON `headers` (see [`frame_headers`]).
    pub(crate) fn frame_headers(&self) -> HeaderMap {
        self.value.as_ref().map(frame_headers).unwrap_or_default()
    }

    pub(crate) fn ends_ws_turn(&self) -> bool {
        // Standalone overload types (including service_unavailable_error) do not end a WS turn.
        matches!(self.class, Class::Terminal(_))
            && matches!(
                self.ty.as_deref(),
                Some(
                    "response.completed"
                        | "response.done"
                        | "response.incomplete"
                        | "response.failed"
                        | "error"
                )
            )
    }
}

/// Quota-type error codes that justify switching accounts. Codex 0.172.0 maps
/// `credit_balance_exhausted` to the same `QuotaExceeded` as `insufficient_quota`.
const QUOTA_CODES: [&str; 4] = [
    "usage_limit_reached",
    "insufficient_quota",
    "usage_not_included",
    "credit_balance_exhausted",
];

fn quota_code(v: Option<&serde_json::Value>) -> bool {
    let Some(o) = v else { return false };
    ["type", "code"].iter().any(|k| {
        o.get(*k)
            .and_then(|t| t.as_str())
            .is_some_and(|t| QUOTA_CODES.contains(&t.trim()))
    })
}

/// Temporary (per-model) limits, which Codex retries: `rate_limit_exceeded` and the per-turn
/// capacity limit `turn_rate_limit_exceeded` ("Codex is at capacity").
const RATE_LIMIT_CODES: [&str; 2] = [crate::errors::RATE_LIMIT_CODE, "turn_rate_limit_exceeded"];

fn rate_limit_code(v: Option<&serde_json::Value>) -> Option<&'static str> {
    let code = v?.get("code")?.as_str()?.trim();
    RATE_LIMIT_CODES.into_iter().find(|&known| known == code)
}

/// Return the normalized code for a structured quota or rate-limit error.
pub(crate) fn quota_event(v: &serde_json::Value) -> Option<&'static str> {
    let (error, status_429) = match v.get("type").and_then(|t| t.as_str()).unwrap_or("") {
        "error" => {
            let status = v
                .get("status")
                .or_else(|| v.get("status_code"))
                .and_then(|s| s.as_u64());
            (v.get("error"), status == Some(429))
        }
        "response.failed" => (v.pointer("/response/error"), false),
        _ => return None,
    };
    if quota_code(error) {
        Some("usage_limit")
    } else {
        rate_limit_code(error).or(status_429.then_some(crate::errors::RATE_LIMIT_CODE))
    }
}

/// Header names kept from an event's JSON `headers`.
const FRAME_RETRY_AFTER: &str = "retry-after";
const FRAME_RETRY_METADATA: &str = "x-retry-metadata";

/// Retry advice in a streamed error event's JSON `headers`, read like Codex 0.172.0
/// (`map_wrapped_websocket_error_event`, `parse_error_event`, `parse_failed_response`):
/// `error.headers` (or `response.error.headers`) before the event's own `headers`, and the
/// first object with a valid `Retry-After` (delay seconds or an HTTP date, a value that is a
/// valid header) wins. Names match case-insensitively. `retry-after-ms`, which Codex does
/// not read, stands in when that object has no valid `Retry-After`. The result holds only a
/// normalized `retry-after` (whole seconds, rounded up) and every `x-retry-metadata` value,
/// so [`header_retry_after`](ciallopa_routing::quota::header_retry_after),
/// `overload_wait` and `no_more_retry` read it like response headers.
pub(crate) fn frame_headers(v: &serde_json::Value) -> HeaderMap {
    let now = SystemTime::now();
    let mut out = HeaderMap::new();
    let objects = [
        v.pointer("/error/headers"),
        v.pointer("/response/error/headers"),
        v.get("headers"),
    ];
    for obj in objects.into_iter().flatten().filter_map(|o| o.as_object()) {
        let (mut seconds, mut millis) = (None, None);
        for (name, value) in obj {
            let value = match value {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                _ => continue,
            };
            let Ok(header) = HeaderValue::from_str(&value) else {
                continue;
            };
            match name.trim().to_ascii_lowercase().as_str() {
                FRAME_RETRY_AFTER => seconds = Some(value),
                "retry-after-ms" => millis = Some(value),
                FRAME_RETRY_METADATA => {
                    out.append(FRAME_RETRY_METADATA, header);
                }
                _ => {}
            }
        }
        if out.contains_key(FRAME_RETRY_AFTER) {
            continue;
        }
        let delay = seconds
            .and_then(|s| parse_retry_after(&s, now, Format::Http))
            .or_else(|| {
                let ms = millis?.trim().parse::<f64>().ok()?;
                Duration::try_from_secs_f64(ms / 1000.0).ok()
            });
        if let Some(d) = delay {
            let secs = d.as_secs() + u64::from(d.subsec_nanos() > 0);
            out.insert(FRAME_RETRY_AFTER, HeaderValue::from(secs));
        }
    }
    out
}

/// An event's retry advice, then the response's own headers for any name the event lacks.
pub(crate) fn with_response_retry_headers(mut frame: HeaderMap, response: &HeaderMap) -> HeaderMap {
    for name in [FRAME_RETRY_AFTER, FRAME_RETRY_METADATA] {
        if !frame.contains_key(name) {
            for value in response.get_all(name) {
                frame.append(name, value.clone());
            }
        }
    }
    frame
}

/// Structured codes that mean "upstream capacity / account throttled", never quota.
const OVERLOAD_CODES: [&str; 4] = [
    "server_is_overloaded",
    "slow_down",
    "service_unavailable_error",
    // Codex 0.166 retries it like `server_is_overloaded`.
    "policy_capacity_unavailable",
];

/// Inspect only errors; explicit codes beat message-only overload wording.
pub(crate) fn overload_code(v: &serde_json::Value) -> Option<String> {
    let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
    let errorish = ty == "error"
        || ty == "response.failed"
        || OVERLOAD_CODES.contains(&ty)
        || (ty.is_empty() && v.get("error").is_some());
    if !errorish {
        return None;
    }
    let objs = [Some(v), v.get("error"), v.pointer("/response/error")];
    let field = |o: &serde_json::Value, k: &str| {
        o.get(k)
            .and_then(|t| t.as_str())
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .map(str::to_string)
    };
    let mut other_code = false;
    for o in objs.iter().flatten() {
        if let Some(c) = field(o, "code") {
            if OVERLOAD_CODES.contains(&c.as_str()) {
                return Some(c);
            }
            other_code = true;
        }
    }
    for o in objs.iter().flatten() {
        if let Some(t) = field(o, "type").filter(|t| OVERLOAD_CODES.contains(&t.as_str())) {
            return Some(t);
        }
    }
    if other_code {
        return None;
    }
    let overloaded_msg = objs.iter().flatten().any(|o| {
        field(o, "message").is_some_and(|m| {
            let m = m.to_ascii_lowercase();
            m.contains("overloaded") || m.contains("at capacity")
        })
    });
    overloaded_msg.then(|| "server_is_overloaded".to_string())
}

/// Overload in an HTTP error body (400 / 429 / 5xx JSON).
pub(crate) fn overload_body(status: u16, b: &[u8]) -> Option<String> {
    if !(status == 400 || status == 429 || status >= 500) {
        return None;
    }
    overload_code(&serde_json::from_slice(b).ok()?)
}

fn needs_value(ty: &str) -> bool {
    matches!(
        ty,
        "" | "error"
            | "response.failed"
            | "response.output_item.added"
            | "response.content_part.added"
            | "response.reasoning_summary_part.added"
            | "codex.rate_limits"
    ) || OVERLOAD_CODES.contains(&ty)
}

fn simple_class(ty: &str) -> Class {
    match ty {
        "response.created" => Class::Created,
        "response.in_progress" | "response.queued" | "response.metadata" | "keepalive" => {
            Class::Preamble
        }
        "response.completed" | "response.done" => Class::Terminal(Terminal::Completed),
        "response.incomplete" => Class::Terminal(Terminal::Incomplete),
        _ if ty.starts_with("codex.") || ty.starts_with("responsesapi.") => Class::Preamble,
        _ => Class::Output,
    }
}

/// Reuses a known envelope type; ordinary deltas only deserialize the borrowed envelope.
/// The pre-check selects structured events, rather than matching every `response.*` frame.
pub(crate) fn classify<'a>(ty: Option<&'a str>, text: &'a str) -> Classified<'a> {
    let structured = ty.map(needs_value).unwrap_or_else(|| {
        [
            "\"error\"",
            "response.failed",
            "response.output_item.added",
            "response.content_part.added",
            "response.reasoning_summary_part.added",
            "codex.rate_limits",
        ]
        .iter()
        .chain(OVERLOAD_CODES.iter())
        .any(|needle| text.contains(needle))
    });
    if !structured {
        let ty = ty
            .map(Cow::Borrowed)
            .or_else(|| crate::usage::event_type(text));
        if let Some(ty) = ty.filter(|ty| !needs_value(ty)) {
            return Classified {
                class: simple_class(&ty),
                ty: Some(ty),
                value: None,
            };
        }
    }
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return Classified {
            ty: None,
            class: Class::Output,
            value: None,
        };
    };
    let ty = v.get("type").and_then(|t| t.as_str());
    let class = classify_value(ty.unwrap_or(""), text, &v);
    Classified {
        ty: ty.map(|ty| Cow::Owned(ty.to_owned())),
        class,
        value: Some(v),
    }
}

fn classify_value(ty: &str, text: &str, v: &serde_json::Value) -> Class {
    match ty {
        "" => return Class::Preamble,
        "codex.rate_limits" => {
            return Class::RateLimits(
                parse_quota_event_headers(text.as_bytes()).and_then(|h| parse_quota(&h)),
            );
        }
        _ if !needs_value(ty) => return simple_class(ty),
        _ => {}
    }
    if empty_announcement(ty, v) {
        return Class::Preamble;
    }
    if let Some(message) = crate::encrypted::rejection(v) {
        return Class::Terminal(Terminal::Encrypted(message));
    }
    if let Some(code) = quota_event(v) {
        let quota = parse_quota_event_headers(text.as_bytes()).and_then(|h| parse_quota(&h));
        let usage_limit = code == "usage_limit" || is_usage_limit(text.as_bytes());
        return Class::Terminal(Terminal::Quota(
            Outcome::RateLimited {
                // Body reset time first, then the event's `Retry-After`, as for an HTTP 429.
                retry_after: retry_after_for_429(
                    &frame_headers(v),
                    text.as_bytes(),
                    SystemTime::now(),
                ),
                usage_limit,
                quota,
            },
            if usage_limit { "usage_limit" } else { code },
        ));
    }
    if let Some(code) = overload_code(v) {
        return Class::Terminal(Terminal::Overloaded(code));
    }
    match ty {
        "error" => Class::Terminal(Terminal::Error),
        "response.failed" => Class::Terminal(Terminal::Failed),
        _ => simple_class(ty),
    }
}

/// Empty announcements precede possible overloads; holding them preserves pre-output failover.
fn empty_announcement(ty: &str, v: &serde_json::Value) -> bool {
    let empty = |x: Option<&serde_json::Value>| match x {
        None | Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::Array(a)) => a.is_empty(),
        Some(serde_json::Value::String(s)) => s.is_empty(),
        _ => false,
    };
    match ty {
        "response.output_item.added" => {
            let Some(item) = v.get("item") else {
                return false;
            };
            match item.get("type").and_then(|t| t.as_str()) {
                Some("message") => empty(item.get("content")),
                Some("reasoning") => {
                    empty(item.get("summary"))
                        && empty(item.get("content"))
                        && empty(item.get("encrypted_content"))
                }
                // Announced with its name; the client acts on the arguments, which come later.
                Some("function_call") => empty(item.get("arguments")),
                Some("custom_tool_call") => empty(item.get("input")),
                _ => false,
            }
        }
        "response.content_part.added" | "response.reasoning_summary_part.added" => {
            v.get("part").is_some_and(|p| {
                matches!(
                    p.get("type").and_then(|t| t.as_str()),
                    Some("output_text" | "summary_text" | "reasoning_text")
                ) && empty(p.get("text"))
            })
        }
        _ => false,
    }
}

/// Classify account/model failures using the captured upstream policy.
pub(crate) fn classify_error(
    status: u16,
    h: &HeaderMap,
    body: &[u8],
    opts: &ciallopa_core::config::UpstreamOpts,
) -> Outcome {
    let now = SystemTime::now();
    match status {
        402 => {
            let deactivated = error_texts(body)
                .0
                .iter()
                .any(|c| c == "deactivated_workspace");
            let secs = if deactivated {
                opts.deactivated_cooldown_secs
            } else {
                opts.payment_required_cooldown_secs
            };
            Outcome::PaymentRequired {
                deactivated,
                cooldown: Duration::from_secs(secs),
            }
        }
        404 if !is_model_unsupported(body) => Outcome::NotFound {
            retry_after: ciallopa_routing::quota::header_retry_after(h, now),
            cooldown: Duration::from_secs(opts.not_found_cooldown_secs),
        },
        429 => Outcome::RateLimited {
            retry_after: retry_after_for_429(h, body, now),
            usage_limit: is_usage_limit(body),
            quota: parse_quota(h),
        },
        403 if crate::egress::detect(status, h, body).is_some() => Outcome::EgressBlocked,
        404 => Outcome::ModelUnsupported {
            retry_after: ciallopa_routing::quota::header_retry_after(h, now),
        },
        _ => Outcome::AuthFailed,
    }
}

pub(crate) fn log_account_error(account: &AccountId, o: &Outcome) {
    match o {
        Outcome::PaymentRequired {
            deactivated: true,
            cooldown,
        } => {
            tracing::warn!(%account, cooldown_secs = cooldown.as_secs(), "upstream 402 deactivated_workspace: the ChatGPT workspace of this account was deactivated; switching account")
        }
        Outcome::PaymentRequired { cooldown, .. } => {
            tracing::warn!(%account, cooldown_secs = cooldown.as_secs(), "upstream 402 payment required; switching account")
        }
        _ => {}
    }
}

/// String fields that carry an upstream error code / message: `detail` (string or
/// `{code,message}`), `error` (string or `{code,type,message}`), top-level `code` / `message`.
fn error_texts(body: &[u8]) -> (Vec<String>, Vec<String>) {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(body) else {
        return (
            vec![],
            vec![String::from_utf8_lossy(body).to_ascii_lowercase()],
        );
    };
    let s = |x: Option<&serde_json::Value>| x.and_then(|x| x.as_str()).map(str::to_ascii_lowercase);
    let mut codes = vec![];
    let mut msgs = vec![];
    for obj in [Some(&v), v.get("detail"), v.get("error")]
        .into_iter()
        .flatten()
    {
        if let Some(t) = obj.as_str() {
            msgs.push(t.to_ascii_lowercase());
            continue;
        }
        codes.extend(s(obj.get("code")));
        codes.extend(s(obj.get("type")));
        msgs.extend(s(obj.get("message")));
    }
    (codes, msgs)
}

/// Match explicit model unavailability; a bare Not Found is insufficient.
pub(crate) fn is_model_unsupported(body: &[u8]) -> bool {
    let (codes, msgs) = error_texts(body);
    codes.iter().any(|c| {
        matches!(
            c.as_str(),
            "model_not_found" | "model_not_supported" | "unsupported_model" | "invalid_model"
        )
    }) || msgs.iter().any(|m| {
        m.contains("model")
            && [
                "not supported",
                "unsupported",
                "does not exist",
                "not found",
                "not available",
            ]
            .iter()
            .any(|p| m.contains(p))
    })
}

#[cfg(test)]
use crate::usage::StreamObservation;

#[cfg(test)]
#[derive(Debug, PartialEq)]
pub(crate) enum EventKind {
    Preamble,
    Quota(Outcome, &'static str),
    Overloaded(String),
    Encrypted(String),
    Content,
}

#[cfg(test)]
impl From<Class> for EventKind {
    fn from(class: Class) -> Self {
        match class {
            Class::Preamble | Class::RateLimits(_) | Class::Created => Self::Preamble,
            Class::Terminal(Terminal::Quota(o, code)) => Self::Quota(o, code),
            Class::Terminal(Terminal::Overloaded(code)) => Self::Overloaded(code),
            Class::Terminal(Terminal::Encrypted(message)) => Self::Encrypted(message),
            Class::Output | Class::SoftError | Class::Terminal(_) => Self::Content,
        }
    }
}

#[cfg(test)]
pub(crate) fn event_kind(text: &str) -> EventKind {
    event_kind_observed(text, None)
}

#[cfg(test)]
pub(crate) fn event_kind_observed(
    text: &str,
    progress: Option<&mut crate::usage::StreamObservation>,
) -> EventKind {
    let classified = classify(None, text);
    if let (Some(progress), Some(ty)) = (progress, classified.ty.as_deref()) {
        progress.event(ty);
    }
    classified.class.into()
}

/// Compatibility view for the existing WS session driver.
#[cfg(test)]
pub(crate) fn classify_observed(
    text: &str,
    progress: &mut crate::usage::StreamObservation,
) -> (EventKind, bool, Option<Outcome>) {
    let classified = classify(None, text).ws_view(text);
    if let Some(ty) = classified.ty.as_deref() {
        progress.event(ty);
    }
    let done = classified.ends_ws_turn();
    let snapshot = match &classified.class {
        Class::RateLimits(Some(quota)) => Some(Outcome::Success {
            quota: Some(quota.clone()),
        }),
        _ => None,
    };
    (classified.class.into(), done, snapshot)
}

/// Whether Codex ends the turn on this `error` frame. Mirrors codex-api 0.172.0's WS reader:
/// `map_wrapped_websocket_error_event` first (the two lifecycle codes, then a `status` /
/// `status_code` that is a valid non-2xx `http::StatusCode`, 100..=999), then
/// `process_responses_event`, which calls `parse_terminal_provider_error` with status None, so
/// its codes end the turn whatever the frame's own `status` says. Exact names only. Frames the
/// proxy classifies (quota, overload, encrypted-content rejection) also end it, as before.
/// Anything else Codex ignores and keeps reading, so the proxy forwards it and the turn goes on.
fn error_frame_ends_turn(v: &serde_json::Value) -> bool {
    let code = v.pointer("/error/code").and_then(|c| c.as_str());
    if matches!(
        code,
        Some("websocket_connection_limit_reached" | "previous_response_not_found")
    ) {
        return true;
    }
    let status = ["status", "status_code"]
        .iter()
        .find_map(|k| v.get(*k).and_then(|s| s.as_u64()));
    if status.is_some_and(|s| (100..=999).contains(&s) && !(200..300).contains(&s)) {
        return true;
    }
    code.is_some_and(codex_terminal_code)
}

/// codex-api 0.172.0 `parse_terminal_provider_error` codes (error.rs), checked there with
/// status None on the WS path. The quota codes among them are classified as quota first.
fn codex_terminal_code(code: &str) -> bool {
    matches!(
        code,
        "insufficient_quota"
            | "credit_balance_exhausted"
            | "usage_not_included"
            | "invalid_prompt"
            | "misalignment_policy_violation"
            | "api_key_spend_limit_exceeded"
            | "project_spend_limit_exceeded"
            | "organization_spend_limit_exceeded"
            | "cyber_policy"
            | "bio_policy"
            | "inference_hook_denied"
            | "context_length_exceeded"
            | "invalid_access_program"
            | "unsupported_access_program"
            | "content_policy_violation"
            | "access_program_not_enabled"
            | "budget_reached"
            | "audio_too_large"
            | "audio_too_short"
            | "invalid_audio"
            | "invalid_audio_data"
            | "invalid_audio_format"
            | "missing_audio_sample_rate"
            | "flex_unavailable"
            | "model_not_found"
    )
}

#[cfg(test)]
mod quota_tests {
    use super::*;
    use crate::attempt::no_more_retry;
    use crate::errors;
    use http::HeaderValue;

    #[test]
    fn classify_402_and_404() {
        let o = ciallopa_core::config::UpstreamOpts::default();
        let h = HeaderMap::new();
        let c = |s: u16, b: &str| classify_error(s, &h, b.as_bytes(), &o);
        assert!(matches!(
            c(402, r#"{"detail":{"code":"deactivated_workspace"}}"#),
            Outcome::PaymentRequired { deactivated: true, cooldown } if cooldown == Duration::from_secs(86_400)
        ));
        assert!(matches!(
            c(
                402,
                r#"{"error":{"code":"deactivated_workspace","message":"x"}}"#
            ),
            Outcome::PaymentRequired {
                deactivated: true,
                ..
            }
        ));
        for b in [r#"{"detail":"Payment Required"}"#, "", "<html>402</html>"] {
            assert!(matches!(
                c(402, b),
                Outcome::PaymentRequired { deactivated: false, cooldown } if cooldown == Duration::from_secs(1_800)
            ));
        }
        for b in [
            r#"{"detail":"Not Found"}"#,
            "",
            r#"{"error":{"message":"route not found"}}"#,
        ] {
            assert!(
                matches!(c(404, b), Outcome::NotFound { cooldown, .. } if cooldown == Duration::from_secs(600)),
                "{b}"
            );
        }
        for b in [
            r#"{"error":{"code":"model_not_found","message":"x"}}"#,
            r#"{"detail":"The model 'gpt-x' does not exist or you do not have access to it."}"#,
            r#"{"detail":"The 'gpt-x' model is not supported when using Codex with a ChatGPT account."}"#,
            r#"{"error":{"type":"invalid_request_error","message":"Model gpt-x not found"}}"#,
        ] {
            assert!(
                matches!(c(404, b), Outcome::ModelUnsupported { retry_after: None }),
                "{b}"
            );
        }
        let mut h2 = HeaderMap::new();
        h2.insert(http::header::RETRY_AFTER, "30".parse().unwrap());
        assert!(matches!(
            classify_error(404, &h2, b"{}", &o),
            Outcome::NotFound { retry_after: Some(d), .. } if d == Duration::from_secs(30)
        ));
        assert_eq!(
            errors::upstream_code(402, &h, br#"{"detail":{"code":"deactivated_workspace"}}"#),
            "deactivated_workspace"
        );
        assert_eq!(
            errors::upstream_code(404, &h, br#"{"detail":"Not Found"}"#),
            "http_404"
        );
    }

    #[test]
    fn frame_classification_table() {
        use Class::{Created, Output, Preamble};
        let failed = || Class::Terminal(Terminal::Failed);
        let error = || Class::Terminal(Terminal::Error);
        let quota = |usage_limit, code| {
            Class::Terminal(Terminal::Quota(
                Outcome::RateLimited {
                    retry_after: None,
                    usage_limit,
                    quota: None,
                },
                code,
            ))
        };
        let overloaded = |code: &str| Class::Terminal(Terminal::Overloaded(code.to_owned()));
        let encrypted = Class::Terminal(Terminal::Encrypted("The encrypted content for item rs_1 could not be verified. Reason: Encrypted content could not be decrypted or parsed.".to_owned()));
        for (text, expected) in [
            (
                r#"{"type":"response.failed","response":{"error":{"code":"server_error"},"instructions":"usage_limit_reached"}}"#,
                failed(),
            ),
            (
                r#"{"type":"error","status":500,"error":{"message":"see usage_limit_reached docs"}}"#,
                error(),
            ),
            (
                r#"{"type":"response.failed","response":{"error":{"code":"usage_limit_reached"}}}"#,
                quota(true, "usage_limit"),
            ),
            (
                r#"{"type":"error","error":{"type":"insufficient_quota"}}"#,
                quota(true, "usage_limit"),
            ),
            (
                r#"{"type":"error","error":{"code":"credit_balance_exhausted","message":"x"}}"#,
                quota(true, "usage_limit"),
            ),
            (
                r#"{"type":"response.failed","response":{"error":{"code":"credit_balance_exhausted"}}}"#,
                quota(true, "usage_limit"),
            ),
            (
                r#"{"type":"error","status":429,"error":{"message":"x"}}"#,
                quota(false, "rate_limit_exceeded"),
            ),
            (
                r#"{"type":"error","status":429,"error":{"code":"unrecognized_limit"}}"#,
                quota(false, "rate_limit_exceeded"),
            ),
            (
                r#"{"type":"response.output_text.delta","delta":"usage_limit_reached"}"#,
                Output,
            ),
            (
                r#"{"type":"response.failed","response":{"error":{"code":"rate_limit_exceeded","message":"Rate limit reached for gpt-5.1 on tokens per min (TPM). Please try again in 11.054s."}}}"#,
                quota(false, "rate_limit_exceeded"),
            ),
            (
                r#"{"type":"error","error":{"type":"rate_limit_error","code":"rate_limit_exceeded"}}"#,
                quota(false, "rate_limit_exceeded"),
            ),
            (
                r#"{"type":"response.failed","response":{"error":{"code":"turn_rate_limit_exceeded","message":"Codex is at capacity, please try again later"}}}"#,
                quota(false, "turn_rate_limit_exceeded"),
            ),
            (
                r#"{"type":"error","error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"Codex is at capacity, please try again later"}}"#,
                quota(false, "turn_rate_limit_exceeded"),
            ),
            (
                r#"{"type":"response.failed","response":{"error":{"code":"policy_capacity_unavailable","message":"x"}}}"#,
                overloaded("policy_capacity_unavailable"),
            ),
            (
                r#"{"type":"error","error":{"code":"policy_capacity_unavailable"}}"#,
                overloaded("policy_capacity_unavailable"),
            ),
            (
                r#"{"type":"service_unavailable_error","code":"server_is_overloaded","message":"x"}"#,
                overloaded("server_is_overloaded"),
            ),
            (
                r#"{"type":"error","error":{"code":"slow_down"}}"#,
                overloaded("slow_down"),
            ),
            (
                r#"{"type":"response.failed","response":{"error":{"code":"server_is_overloaded"}}}"#,
                overloaded("server_is_overloaded"),
            ),
            (
                r#"{"type":"error","error":{"message":"Selected model is at capacity."}}"#,
                overloaded("server_is_overloaded"),
            ),
            (
                r#"{"type":"error","error":{"type":"service_unavailable_error"}}"#,
                overloaded("service_unavailable_error"),
            ),
            (
                r#"{"type":"response.output_text.delta","delta":"server is overloaded"}"#,
                Output,
            ),
            (
                r#"{"type":"error","error":{"code":"context_length_exceeded","message":"overloaded"}}"#,
                error(),
            ),
            (
                r#"{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"overloaded"}}"#,
                quota(true, "usage_limit"),
            ),
            (
                r#"{"type":"error","error":{"message":"The encrypted content for item rs_1 could not be verified. Reason: Encrypted content could not be decrypted or parsed.","type":"invalid_request_error","param":"input","code":"invalid_encrypted_content"}}"#,
                encrypted,
            ),
            (
                r#"{"type":"response.metadata","response_id":"r1","metadata":{"openai_chatgpt_moderation_metadata":{"prompt":{}}}}"#,
                Preamble,
            ),
            (
                r#"{"type":"response.queued","response":{"id":"r1","status":"queued"}}"#,
                Preamble,
            ),
            (r#"{"type":"keepalive"}"#, Preamble),
            (
                r#"{"type":"response.output_text.delta","delta":"hi"}"#,
                Output,
            ),
            (r#"{"type":"unknown_event","x":1}"#, Output),
            (
                r#"{"type":"response.output_item.added","item":{"type":"message","role":"assistant","content":[]}}"#,
                Preamble,
            ),
            (
                r#"{"type":"response.output_item.added","item":{"type":"reasoning","summary":[],"encrypted_content":null}}"#,
                Preamble,
            ),
            (
                r#"{"type":"response.content_part.added","part":{"type":"output_text","annotations":[],"text":""}}"#,
                Preamble,
            ),
            (
                r#"{"type":"response.reasoning_summary_part.added","part":{"type":"summary_text","text":""}}"#,
                Preamble,
            ),
            (
                r#"{"type":"response.output_item.added","item":{"type":"function_call","name":"shell","arguments":""}}"#,
                Preamble,
            ),
            (
                r#"{"type":"response.output_item.added","item":{"type":"custom_tool_call","name":"apply_patch","input":""}}"#,
                Preamble,
            ),
            (
                r#"{"type":"response.output_item.added","item":{"type":"function_call","name":"shell","arguments":"{}"}}"#,
                Output,
            ),
            (
                r#"{"type":"response.output_item.added","item":{"type":"message","content":[{"type":"output_text","text":"hi"}]}}"#,
                Output,
            ),
            (
                r#"{"type":"response.output_item.added","item":{"type":"reasoning","summary":[],"encrypted_content":"gAAA"}}"#,
                Output,
            ),
            (
                r#"{"type":"response.content_part.added","part":{"type":"output_text","text":"hi"}}"#,
                Output,
            ),
            (
                r#"{"type":"response.output_item.added","item":{"type":"web_search_call"}}"#,
                Output,
            ),
            (r#"{"type":"response.created"}"#, Created),
            (r#"{"type":"response.in_progress"}"#, Preamble),
            (r#"{"type":"codex.response.metadata"}"#, Preamble),
            (r#"{"type":"responsesapi.websocket_timing"}"#, Preamble),
            (r#"{"type":"codex.rate_limits"}"#, Class::RateLimits(None)),
            (
                r#"{"type":"response.completed"}"#,
                Class::Terminal(Terminal::Completed),
            ),
            (
                r#"{"type":"response.done"}"#,
                Class::Terminal(Terminal::Completed),
            ),
            (
                r#"{"type":"response.incomplete"}"#,
                Class::Terminal(Terminal::Incomplete),
            ),
            (r#"{}"#, Preamble),
            (r#"{"type":null}"#, Preamble),
            (r#"{"type":""}"#, Preamble),
            ("invalid JSON", Output),
            (
                r#"{"type":"response.output_text.delta","delta":"hi"} trailing"#,
                Output,
            ),
            (
                r#"{"type":"response.\u0066ailed","response":{"error":{"code":"insufficient_quota"}}}"#,
                quota(true, "usage_limit"),
            ),
        ] {
            let classified = super::classify(None, text);
            assert_eq!(classified.class, expected, "{text}");
            // A previously read envelope must produce the same classification.
            let ty = crate::usage::event_type(text);
            if let Some(ty) = ty.as_deref() {
                assert_eq!(super::classify(Some(ty), text).class, expected, "{text}");
            }
            if let Class::Terminal(Terminal::Quota(_, code)) = &expected {
                let value = serde_json::from_str(text).unwrap();
                assert_eq!(quota_event(&value), Some(*code), "{text}");
            }
            assert_eq!(event_kind(text), EventKind::from(expected), "{text}");
        }
    }

    #[test]
    fn overload_http_bodies_and_retry_marker() {
        for (status, body, expected) in [
            (
                400,
                r#"{"error":{"message":"Our servers are currently overloaded."}}"#,
                Some("server_is_overloaded"),
            ),
            (
                503,
                r#"{"error":{"type":"server_error","code":"server_is_overloaded"}}"#,
                Some("server_is_overloaded"),
            ),
            (
                503,
                r#"{"error":{"code":"policy_capacity_unavailable"}}"#,
                Some("policy_capacity_unavailable"),
            ),
            (
                429,
                r#"{"error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"Codex is at capacity, please try again later"}}"#,
                None,
            ),
            (503, "busy", None),
            (401, r#"{"error":{"message":"overloaded"}}"#, None),
        ] {
            assert_eq!(overload_body(status, body.as_bytes()).as_deref(), expected);
        }
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-retry-metadata",
            HeaderValue::from_static("NO_MORE_RETRY"),
        );
        assert!(no_more_retry(&headers));
    }

    #[test]
    fn new_codex_terminal_codes_stay_content() {
        // Codex marks these as terminal (retry_delay = None). ciallopa does not
        // classify them as quota or overload: the in-stream event stays Content,
        // the attempt is committed, and the code is forwarded as-is (no switch).
        const CODES: &[&str] = &[
            "inference_hook_denied",
            "content_policy_violation",
            "invalid_access_program",
            "unsupported_access_program",
            "access_program_not_enabled",
            "invalid_workspace_selected",
            "codex_workspace_access_denied",
            "budget_reached",
            "audio_too_large",
            "audio_too_short",
            "invalid_audio",
            "invalid_audio_data",
            "invalid_audio_format",
            "missing_audio_sample_rate",
            "model_not_found",
            // Codex 0.172.0: terminal, no cooldown policy of their own here.
            "invalid_prompt",
            "misalignment_policy_violation",
            "cyber_policy",
            "bio_policy",
            "api_key_spend_limit_exceeded",
            "project_spend_limit_exceeded",
            "organization_spend_limit_exceeded",
        ];
        for code in CODES {
            for ev in [
                format!(
                    r#"{{"type":"response.failed","response":{{"error":{{"code":"{code}","message":"x"}}}}}}"#
                ),
                format!(r#"{{"type":"error","error":{{"code":"{code}","message":"x"}}}}"#),
            ] {
                assert!(matches!(event_kind(&ev), EventKind::Content), "{ev}");
            }
            let body = format!(r#"{{"error":{{"code":"{code}","message":"x"}}}}"#);
            assert_eq!(overload_body(400, body.as_bytes()), None, "{code}");
        }
    }

    fn advice(text: &str) -> HeaderMap {
        frame_headers(&serde_json::from_str(text).unwrap())
    }

    fn ra(h: &HeaderMap) -> Option<u64> {
        h.get("retry-after")
            .map(|v| v.to_str().unwrap().parse().unwrap())
    }

    /// Codex 0.172.0 order: `error.headers` (or `response.error.headers`) before the event's
    /// own `headers`; the first object with a valid `Retry-After` wins.
    #[test]
    fn frame_headers_follow_codex_precedence() {
        for (text, want) in [
            (
                r#"{"type":"error","error":{"code":"x","headers":{"retry-after":"5"}},"headers":{"retry-after":"12"}}"#.to_string(),
                Some(5),
            ),
            (
                r#"{"type":"error","error":{"code":"x","headers":{"Retry-After":"7"}},"headers":{"retry-after":"12"}}"#.to_string(),
                Some(7),
            ),
            // Invalid nested advice (not a header value, not seconds) falls back to the event's.
            (
                r#"{"type":"error","error":{"code":"x","headers":{"retry-after":"\n5\n"}},"headers":{"retry-after":"12"}}"#.to_string(),
                Some(12),
            ),
            (
                r#"{"type":"error","error":{"code":"x","headers":"invalid"},"headers":{"retry-after":"12"}}"#.to_string(),
                Some(12),
            ),
            (
                r#"{"type":"error","error":{"code":"x","headers":{"retry-after":"soon"}},"headers":{"RETRY-AFTER":12}}"#.to_string(),
                Some(12),
            ),
            (
                r#"{"type":"response.failed","response":{"error":{"code":"x","headers":{"retry-after":"3"}}}}"#.to_string(),
                Some(3),
            ),
            // retry-after-ms stands in within its object, rounded up to whole seconds.
            (
                r#"{"type":"error","error":{"code":"x","headers":{"retry-after-ms":"1500"}},"headers":{"retry-after":"12"}}"#.to_string(),
                Some(2),
            ),
            (
                r#"{"type":"error","headers":{"retry-after-ms":"3000","retry-after":"bad"}}"#.to_string(),
                Some(3),
            ),
            (r#"{"type":"error","error":{"code":"x"}}"#.to_string(), None),
            (r#"{"type":"error","headers":{"retry-after":"-1"}}"#.to_string(), None),
        ] {
            assert_eq!(ra(&advice(&text)), want, "{text}");
        }
        let h = advice(
            r#"{"type":"error","error":{"code":"server_is_overloaded","headers":{"X-Retry-Metadata":"NO_MORE_RETRY"}}}"#,
        );
        assert!(no_more_retry(&h));
        let h = advice(
            r#"{"type":"error","error":{"code":"server_is_overloaded"},"headers":{"x-retry-metadata":"no_more_retry","x-request-id":"r"}}"#,
        );
        assert!(no_more_retry(&h));
        assert!(h.get("x-request-id").is_none(), "only retry advice is kept");
        assert!(!no_more_retry(&advice(
            r#"{"type":"error","error":{"code":"x"}}"#
        )));
    }

    #[test]
    fn response_headers_fill_only_what_the_event_lacks() {
        let mut http = HeaderMap::new();
        http.insert("retry-after", HeaderValue::from_static("9"));
        http.insert(
            "x-retry-metadata",
            HeaderValue::from_static("NO_MORE_RETRY"),
        );
        let merged = with_response_retry_headers(
            advice(r#"{"type":"error","headers":{"retry-after":"2"}}"#),
            &http,
        );
        assert_eq!(ra(&merged), Some(2));
        assert!(no_more_retry(&merged));
        let merged = with_response_retry_headers(HeaderMap::new(), &http);
        assert_eq!(ra(&merged), Some(9));
    }

    /// A rate-limit event with no body reset time takes the event's `Retry-After`; a usage
    /// limit's body reset time still wins over it.
    #[test]
    fn stream_rate_limit_takes_the_event_retry_after() {
        let retry = |text: &str| match super::classify(None, text).class {
            Class::Terminal(Terminal::Quota(Outcome::RateLimited { retry_after, .. }, _)) => {
                retry_after
            }
            other => panic!("{text}: {other:?}"),
        };
        for text in [
            r#"{"type":"error","error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"Codex is at capacity","headers":{"retry-after":"30"}},"headers":{"retry-after":"99"}}"#,
            r#"{"type":"response.failed","response":{"error":{"code":"turn_rate_limit_exceeded","headers":{"retry-after":"30"}}}}"#,
            r#"{"type":"error","status":429,"error":{"code":"rate_limit_exceeded"},"headers":{"Retry-After":"30"}}"#,
        ] {
            assert_eq!(retry(text), Some(Duration::from_secs(30)), "{text}");
        }
        assert_eq!(
            retry(r#"{"type":"error","error":{"code":"turn_rate_limit_exceeded"}}"#),
            None
        );
        assert_eq!(
            retry(
                r#"{"type":"error","status":429,"error":{"type":"usage_limit_reached","resets_in_seconds":60},"headers":{"retry-after":"5"}}"#
            ),
            Some(Duration::from_secs(60))
        );
        // Same cap as an HTTP 429's Retry-After.
        assert_eq!(
            retry(
                r#"{"type":"error","error":{"code":"rate_limit_exceeded"},"headers":{"retry-after":"99999999"}}"#
            ),
            Some(ciallopa_routing::quota::MAX_HEADER_RETRY_AFTER)
        );
    }

    #[test]
    fn new_codex_workspace_403_still_auth_failed() {
        // HTTP 403 (including workspace / access-program / inference-hook codes)
        // stays AuthFailed: cool the account and switch. Do not mark needs_relogin.
        let o = ciallopa_core::config::UpstreamOpts::default();
        let h = HeaderMap::new();
        for code in [
            "invalid_workspace_selected",
            "codex_workspace_access_denied",
            "access_program_not_enabled",
            "inference_hook_denied",
        ] {
            let body = format!(r#"{{"error":{{"code":"{code}","message":"x"}}}}"#);
            assert!(
                matches!(
                    classify_error(403, &h, body.as_bytes(), &o),
                    Outcome::AuthFailed
                ),
                "{code}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ws::frames::replayable;

    fn classify(text: &str) -> (EventKind, bool, Option<Outcome>) {
        classify_observed(text, &mut StreamObservation::new())
    }

    #[test]
    fn classifies() {
        let (k, d, _) = classify(
            r#"{"type":"error","status":429,"error":{"type":"usage_limit_reached","resets_in_seconds":60},"headers":{"x-codex-primary-used-percent":"100"}}"#,
        );
        assert!(d);
        match k {
            EventKind::Quota(
                Outcome::RateLimited {
                    usage_limit,
                    retry_after,
                    quota,
                },
                "usage_limit",
            ) => {
                assert!(usage_limit);
                assert_eq!(retry_after, Some(Duration::from_secs(60)));
                assert!(quota.is_some());
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            classify(
                r#"{"type":"response.failed","response":{"error":{"code":"insufficient_quota"}}}"#
            )
            .0,
            EventKind::Quota(_, "usage_limit")
        ));
        assert!(classify(r#"{"type":"response.completed"}"#).1);
        assert_eq!(
            classify(r#"{"type":"response.created"}"#).0,
            EventKind::Preamble
        );
        assert_eq!(
            classify(r#"{"type":"response.output_text.delta"}"#).0,
            EventKind::Content
        );
        assert!(replayable(r#"{"type":"response.create","input":[]}"#));
    }

    /// Codex (`map_wrapped_websocket_error_event`) ends a turn on an `error` frame only for
    /// a non-2xx `status`, or the codes it maps itself; any other error frame is ignored and
    /// it keeps reading. The proxy must not settle the turn on such a frame.
    #[test]
    fn ws_bookkeeping_and_standalone_overload_keep_their_existing_view() {
        for (text, expected, done) in [
            (r#"{"type":"keepalive"}"#, EventKind::Content, false),
            (r#"{"type":"codex.metadata"}"#, EventKind::Content, false),
            (
                r#"{"type":"responsesapi.websocket_timing"}"#,
                EventKind::Content,
                false,
            ),
            (
                r#"{"type":"codex.response.metadata"}"#,
                EventKind::Preamble,
                false,
            ),
            (
                r#"{"type":"response.metadata"}"#,
                EventKind::Preamble,
                false,
            ),
            (
                r#"{"type":"codex.rate_limits"}"#,
                EventKind::Preamble,
                false,
            ),
            (r#"{}"#, EventKind::Content, false),
            (
                r#"{"type":"slow_down","message":"busy"}"#,
                EventKind::Content,
                false,
            ),
            (
                r#"{"type":"service_unavailable_error","code":"server_is_overloaded"}"#,
                EventKind::Content,
                false,
            ),
            (
                r#"{"type":"service_unavailable_error","error":{"code":"server_is_overloaded"}}"#,
                EventKind::Overloaded("server_is_overloaded".into()),
                false,
            ),
            (
                r#"{"type":"error","error":{"code":"server_is_overloaded"}}"#,
                EventKind::Overloaded("server_is_overloaded".into()),
                true,
            ),
        ] {
            let (kind, finished, _) = classify(text);
            assert_eq!(kind, expected, "{text}");
            assert_eq!(finished, done, "{text}");
        }
        let soft = super::classify(None, r#"{"type":"error","error":{"code":"notice"}}"#)
            .ws_view(r#"{"type":"error","error":{"code":"notice"}}"#);
        assert_eq!(soft.class, Class::SoftError);
    }

    /// Codex 0.172.0 `parse_terminal_provider_error`, which the WS reader calls for every
    /// `error` frame with `status` = None (after `map_wrapped_websocket_error_event`): these
    /// exact codes end the turn whatever the frame's own `status` says. Exact names, no
    /// prefix matching (`audio_*` is a fixed list of six). The quota codes end it as quota.
    const CODEX_0_172_TERMINAL_CODES: &[&str] = &[
        "insufficient_quota",
        "credit_balance_exhausted",
        "usage_not_included",
        "invalid_prompt",
        "misalignment_policy_violation",
        "api_key_spend_limit_exceeded",
        "project_spend_limit_exceeded",
        "organization_spend_limit_exceeded",
        "cyber_policy",
        "bio_policy",
        "inference_hook_denied",
        "context_length_exceeded",
        "invalid_access_program",
        "unsupported_access_program",
        "content_policy_violation",
        "access_program_not_enabled",
        "budget_reached",
        "audio_too_large",
        "audio_too_short",
        "invalid_audio",
        "invalid_audio_data",
        "invalid_audio_format",
        "missing_audio_sample_rate",
        "flex_unavailable",
        "model_not_found",
    ];

    #[test]
    fn error_frame_ends_turn_on_codex_0_172_terminal_codes() {
        let done = |t: &str| classify(t).1;
        let mut wrong = Vec::new();
        for code in CODEX_0_172_TERMINAL_CODES {
            if !codex_terminal_code(code) {
                wrong.push(format!("not listed: {code}"));
            }
            for frame in [
                format!(r#"{{"type":"error","error":{{"code":"{code}","message":"x"}}}}"#),
                // A 2xx status is not mapped by the wrapped-error path, so Codex still
                // reaches parse_terminal_provider_error(status = None).
                format!(r#"{{"type":"error","status":200,"error":{{"code":"{code}"}}}}"#),
            ] {
                if !done(&frame) {
                    wrong.push(format!("not terminal: {frame}"));
                }
            }
        }
        // Not prefixes: near names stay non-terminal, like any unknown code.
        for code in [
            "audio_too_long",
            "audio_x",
            "model_not_found_x",
            "invalid_audio_rate",
            "spend_limit_exceeded",
            "cyber_policy_x",
        ] {
            let frame = format!(r#"{{"type":"error","error":{{"code":"{code}"}}}}"#);
            if done(&frame) {
                wrong.push(format!("terminal: {frame}"));
            }
        }
        // `status` outside http::StatusCode's 100..=999 is not mapped by Codex either.
        for (frame, want) in [
            (
                r#"{"type":"error","status":1000,"error":{"code":"x"}}"#,
                false,
            ),
            (
                r#"{"type":"error","status":100,"error":{"code":"x"}}"#,
                true,
            ),
            (
                r#"{"type":"error","status_code":999,"error":{"code":"x"}}"#,
                true,
            ),
        ] {
            if done(frame) != want {
                wrong.push(format!("want terminal={want}: {frame}"));
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }

    #[test]
    fn error_frame_ends_turn_like_codex() {
        let done = |t: &str| classify(t).1;
        assert!(!done(
            r#"{"type":"error","error":{"code":"some_notice","message":"x"}}"#
        ));
        assert!(!done(
            r#"{"type":"error","status":200,"error":{"code":"x"}}"#
        ));
        assert!(done(
            r#"{"type":"error","status":400,"error":{"code":"x"}}"#
        ));
        assert!(done(
            r#"{"type":"error","status_code":502,"error":{"code":"x"}}"#
        ));
        for code in [
            "websocket_connection_limit_reached",
            "previous_response_not_found",
            "flex_unavailable",
            "budget_reached",
        ] {
            assert!(
                done(&format!(
                    r#"{{"type":"error","error":{{"code":"{code}"}}}}"#
                )),
                "{code}"
            );
        }
        // Classified kinds the proxy acts on keep ending the turn.
        assert!(done(
            r#"{"type":"error","error":{"code":"server_is_overloaded"}}"#
        ));
        assert!(done(
            r#"{"type":"error","error":{"type":"usage_limit_reached","resets_in_seconds":30}}"#
        ));
        assert!(!replayable(
            r#"{"type":"response.create","previous_response_id":"r1"}"#
        ));
    }
}
