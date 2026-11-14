use crate::proxy;
use crate::usage::StreamObservation;
use ciallopa_core::{AccountId, Outcome};
use ciallopa_routing::{
    is_usage_limit, parse_quota, parse_quota_event_headers, retry_after_for_429,
};
use http::HeaderMap;
use std::time::{Duration, SystemTime};

/// What a Responses stream event means for bootstrap / failover.
#[derive(Debug, PartialEq)]
pub(crate) enum EventKind {
    /// Envelope only (`response.created`, `in_progress`, rate-limit snapshots, keepalive).
    Preamble,
    /// Account-level quota / rate-limit failure: cool down and switch.
    Quota(Outcome),
    /// Upstream capacity / per-account throttling (`server_is_overloaded`, `slow_down`, …):
    /// switch account if another one is usable, else pass through. Carries the logged code.
    Overloaded(String),
    /// The upstream rejected the encrypted reasoning content in the request's history
    /// (`invalid_encrypted_content`): a request problem, never retried or held against the
    /// account. Carries the upstream message.
    Encrypted(String),
    /// Anything else: real output (or a non-quota terminal event).
    Content,
}

/// Quota-type error codes that justify switching accounts.
const QUOTA_CODES: [&str; 3] = [
    "usage_limit_reached",
    "insufficient_quota",
    "usage_not_included",
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

/// `code` is one of [`RATE_LIMIT_CODES`].
fn rate_limit_code(v: Option<&serde_json::Value>) -> bool {
    v.and_then(|o| o.get("code"))
        .and_then(|c| c.as_str())
        .is_some_and(|c| RATE_LIMIT_CODES.contains(&c.trim()))
}

/// Structured quota check shared by HTTP SSE, WS and the usage tracker. Only these fields
/// count (never free text such as instructions / messages):
/// `error` event: `status`/`status_code == 429`, `error.type|code`, top-level `type|code`;
/// `response.failed`: `response.error.type|code`. A [`RATE_LIMIT_CODES`] code (either
/// shape) is a rate limit without a quota code: it cools the model, never the account.
/// Returns `(is_quota, has_quota_code)`.
pub(crate) fn quota_event(v: &serde_json::Value) -> (bool, bool) {
    match v.get("type").and_then(|t| t.as_str()).unwrap_or("") {
        "error" => {
            let status = v
                .get("status")
                .or_else(|| v.get("status_code"))
                .and_then(|s| s.as_u64());
            let code = quota_code(v.get("error"));
            (
                status == Some(429) || code || rate_limit_code(v.get("error")),
                code,
            )
        }
        "response.failed" => {
            let e = v.pointer("/response/error");
            let code = quota_code(e);
            (code || rate_limit_code(e), code)
        }
        _ => (false, false),
    }
}

/// Structured codes that mean "upstream capacity / account throttled", never quota.
const OVERLOAD_CODES: [&str; 4] = [
    "server_is_overloaded",
    "slow_down",
    "service_unavailable_error",
    // Codex 0.166 retries it like `server_is_overloaded`.
    "policy_capacity_unavailable",
];

/// Overload detection on an error event (`error`, `response.failed`, or an event whose own
/// `type`/`code` is an overload code) or an HTTP error body. Structured `code`/`type` first;
/// a bare `message` ("…currently overloaded…", "…at capacity…") counts only when the error
/// object carries no other code. Output events (deltas, items) are never inspected.
/// Returns the code to log (`server_is_overloaded` for message-only matches).
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
    let mut v: serde_json::Value = serde_json::from_slice(b).ok()?;
    if v.get("type").is_none() && v.get("error").is_some() {
        if let Some(o) = v.as_object_mut() {
            o.insert("type".into(), "error".into());
        }
    }
    overload_code(&v)
}

/// Classify one Responses event JSON (`data:` payload of SSE, or a WS text frame).
pub(crate) fn event_kind(text: &str) -> EventKind {
    event_kind_observed(text, None)
}

pub(crate) fn event_kind_observed(
    text: &str,
    progress: Option<&mut crate::usage::StreamObservation>,
) -> EventKind {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return EventKind::Content;
    };
    let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
    if let Some(progress) = progress.filter(|_| v.get("type").and_then(|t| t.as_str()).is_some()) {
        progress.event(ty);
    }
    // `response.queued` / `response.metadata` / `keepalive` are chatgpt Codex bookkeeping
    // before any model output (moderation info, queue status, heartbeats). Treating them as
    // Content would commit the attempt and block pre-output overload switch / encrypted 400
    // (sub2api #8139). Exact names only; unknown types stay Content.
    if matches!(
        ty,
        "response.created"
            | "response.in_progress"
            | "response.queued"
            | "response.metadata"
            | "codex.rate_limits"
            | "keepalive"
            | ""
    ) {
        return EventKind::Preamble;
    }
    if empty_announcement(ty, &v) {
        return EventKind::Preamble;
    }
    if let Some(m) = crate::encrypted::rejection(&v) {
        return EventKind::Encrypted(m);
    }
    let (quota, code) = quota_event(&v);
    if !quota {
        return match overload_code(&v) {
            Some(c) => EventKind::Overloaded(c),
            // Transport metadata (`codex.response.metadata`, `responsesapi.websocket_timing`).
            None if ty.starts_with("codex.") || ty.starts_with("responsesapi.") => {
                EventKind::Preamble
            }
            None => EventKind::Content,
        };
    }
    let quota_h =
        ciallopa_routing::parse_quota_event_headers(text.as_bytes()).and_then(|h| parse_quota(&h));
    EventKind::Quota(Outcome::RateLimited {
        retry_after: ciallopa_routing::parse_codex_retry_after(
            429,
            text.as_bytes(),
            SystemTime::now(),
        ),
        usage_limit: code || is_usage_limit(text.as_bytes()),
        quota: quota_h,
    })
}

/// An item or part announced before it carries anything: a `message` with no content, a
/// `reasoning` item with no summary / content, or an empty text part. chatgpt sends these
/// right before a possible `server_is_overloaded`; they are not output, so holding them keeps
/// the pre-output account switch possible; a switch drops them, so the client never sees them.
/// Same set as CPA 9e79552f: also a `function_call` with empty arguments and a
/// `custom_tool_call` with empty input. Non-empty items / parts and unknown types stay Content.
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

/// Account-level failure classification (shared with WS handshake).
/// Account-scoped outcome of an upstream 401/402/403/404/429 (cooldowns from `[upstream_opts]`).
pub(crate) fn classify_error(
    status: u16,
    h: &HeaderMap,
    body: &[u8],
    opts: &ciallopa_core::config::UpstreamOpts,
) -> Outcome {
    let now = SystemTime::now();
    let retry_after = || {
        h.get(http::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| {
                ciallopa_core::retry_after::parse(v, now, ciallopa_core::retry_after::Format::Http)
                    .map(|d| d.min(ciallopa_routing::quota::MAX_HEADER_RETRY_AFTER))
            })
    };
    match status {
        402 => {
            let deactivated = is_workspace_deactivated(body);
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
            retry_after: retry_after(),
            cooldown: Duration::from_secs(opts.not_found_cooldown_secs),
        },
        429 => Outcome::RateLimited {
            retry_after: retry_after_for_429(h, body, now),
            usage_limit: is_usage_limit(body),
            quota: parse_quota(h),
        },
        403 if crate::egress::detect(status, h, body).is_some() => Outcome::EgressBlocked,
        404 => Outcome::ModelUnsupported {
            retry_after: retry_after(),
        },
        _ => Outcome::AuthFailed,
    }
}

/// One warn line for account-level upstream problems an operator has to act on.
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

/// 402 `deactivated_workspace`: the ChatGPT workspace of this account was deactivated.
pub(crate) fn is_workspace_deactivated(body: &[u8]) -> bool {
    error_texts(body)
        .0
        .iter()
        .any(|c| c == "deactivated_workspace")
}

/// A 404 whose body explicitly says the requested model is unsupported / unknown for this
/// account (`model_not_found`, "model … not supported / does not exist"). A bare
/// `{"detail":"Not Found"}` is not.
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

/// Inspect an upstream text frame: (kind, request finished, quota snapshot outcome).
#[cfg(test)]
fn classify(text: &str) -> (proxy::EventKind, bool, Option<Outcome>) {
    classify_observed(text, &mut StreamObservation::new())
}

pub(crate) fn classify_observed(
    text: &str,
    progress: &mut StreamObservation,
) -> (proxy::EventKind, bool, Option<Outcome>) {
    let looks = text.contains("\"error\"")
        || text.contains("response.")
        || text.contains("codex.rate_limits");
    if !looks {
        if let Some(ty) = crate::usage::event_type(text) {
            progress.event(&ty);
        }
        return (proxy::EventKind::Content, false, None);
    }
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return (proxy::EventKind::Content, false, None);
    };
    let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
    if v.get("type").and_then(|t| t.as_str()).is_some() {
        progress.event(ty);
    }
    let kind = proxy::event_kind(text);
    let done = match ty {
        "response.completed" | "response.failed" | "response.incomplete" | "response.done" => true,
        "error" => error_frame_ends_turn(&v, &kind),
        _ => false,
    };
    let snapshot = (ty == "codex.rate_limits")
        .then(|| parse_quota_event_headers(text.as_bytes()).and_then(|h| parse_quota(&h)))
        .flatten()
        .map(|q| Outcome::Success { quota: Some(q) });
    (kind, done, snapshot)
}

/// Whether Codex ends the turn on this `error` frame (codex-api
/// `map_wrapped_websocket_error_event` + `flex_unavailable`): a non-2xx `status` /
/// `status_code`, or a code it maps itself. Frames the proxy classifies (quota, overload,
/// encrypted-content rejection) also end it, as before. Anything else Codex ignores and
/// keeps reading, so the proxy forwards it and the turn goes on.
fn error_frame_ends_turn(v: &serde_json::Value, kind: &proxy::EventKind) -> bool {
    if !matches!(kind, proxy::EventKind::Content | proxy::EventKind::Preamble) {
        return true;
    }
    let status = ["status", "status_code"]
        .iter()
        .find_map(|k| v.get(*k).and_then(|s| s.as_u64()));
    if status.is_some_and(|s| !(200..300).contains(&s)) {
        return true;
    }
    let code = v
        .get("error")
        .and_then(|e| e.get("code"))
        .and_then(|c| c.as_str());
    matches!(
        code,
        Some(
            "websocket_connection_limit_reached"
                | "previous_response_not_found"
                | "flex_unavailable"
        )
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
    fn quota_only_from_structured_fields() {
        let q = |t: &str| matches!(event_kind(t), EventKind::Quota(_));
        assert!(!q(
            r#"{"type":"response.failed","response":{"error":{"code":"server_error"},"instructions":"usage_limit_reached"}}"#
        ));
        assert!(!q(
            r#"{"type":"error","status":500,"error":{"message":"see usage_limit_reached docs"}}"#
        ));
        assert!(q(
            r#"{"type":"response.failed","response":{"error":{"code":"usage_limit_reached"}}}"#
        ));
        assert!(q(
            r#"{"type":"error","error":{"type":"insufficient_quota"}}"#
        ));
        assert!(q(
            r#"{"type":"error","status":429,"error":{"message":"x"}}"#
        ));
        assert!(!q(
            r#"{"type":"response.output_text.delta","delta":"usage_limit_reached"}"#
        ));
    }

    #[test]
    fn failed_rate_limit_is_a_model_rate_limit() {
        // Codex retries `response.failed` `rate_limit_exceeded` (temporary limit); the router
        // must hear it as a rate limit (model cooldown), never as a usage limit.
        let ev = r#"{"type":"response.failed","response":{"error":{"code":"rate_limit_exceeded","message":"Rate limit reached for gpt-5.1 on tokens per min (TPM). Please try again in 11.054s."}}}"#;
        assert!(matches!(
            event_kind(ev),
            EventKind::Quota(Outcome::RateLimited {
                usage_limit: false,
                ..
            })
        ));
        let v: serde_json::Value = serde_json::from_str(ev).unwrap();
        assert_eq!(quota_event(&v), (true, false));
        assert_eq!(errors::event_code(&v), "rate_limit_exceeded");
        // Same code on an `error` frame without a status.
        let ev =
            r#"{"type":"error","error":{"type":"rate_limit_error","code":"rate_limit_exceeded"}}"#;
        assert!(matches!(
            event_kind(ev),
            EventKind::Quota(Outcome::RateLimited {
                usage_limit: false,
                ..
            })
        ));
    }

    #[test]
    fn turn_rate_limit_is_a_model_rate_limit() {
        // "Codex is at capacity" per-turn limit: a temporary rate limit (model cooldown and
        // switch before output), never a usage limit, and never mistaken for an overload by
        // its message.
        for ev in [
            r#"{"type":"response.failed","response":{"error":{"code":"turn_rate_limit_exceeded","message":"Codex is at capacity, please try again later"}}}"#,
            r#"{"type":"error","error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"Codex is at capacity, please try again later"}}"#,
        ] {
            assert!(
                matches!(
                    event_kind(ev),
                    EventKind::Quota(Outcome::RateLimited {
                        usage_limit: false,
                        ..
                    })
                ),
                "{ev}"
            );
            let v: serde_json::Value = serde_json::from_str(ev).unwrap();
            assert_eq!(quota_event(&v), (true, false), "{ev}");
        }
        let body = br#"{"error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"Codex is at capacity, please try again later"}}"#;
        assert_eq!(overload_body(429, body), None);
    }

    #[test]
    fn policy_capacity_unavailable_is_an_overload() {
        for ev in [
            r#"{"type":"response.failed","response":{"error":{"code":"policy_capacity_unavailable","message":"x"}}}"#,
            r#"{"type":"error","error":{"code":"policy_capacity_unavailable"}}"#,
        ] {
            assert!(
                matches!(event_kind(ev), EventKind::Overloaded(ref c) if c == "policy_capacity_unavailable"),
                "{ev}"
            );
        }
        assert_eq!(
            overload_body(503, br#"{"error":{"code":"policy_capacity_unavailable"}}"#).as_deref(),
            Some("policy_capacity_unavailable")
        );
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

    #[test]
    fn overload_detection() {
        let k = |t: &str| event_kind(t);
        let ov = |t: &str| matches!(k(t), EventKind::Overloaded(_));
        assert!(ov(
            r#"{"type":"service_unavailable_error","code":"server_is_overloaded","message":"x"}"#
        ));
        assert!(ov(r#"{"type":"error","error":{"code":"slow_down"}}"#));
        assert!(ov(
            r#"{"type":"response.failed","response":{"error":{"code":"server_is_overloaded"}}}"#
        ));
        assert!(ov(
            r#"{"type":"error","error":{"message":"Selected model is at capacity."}}"#
        ));
        assert!(ov(
            r#"{"type":"error","error":{"type":"service_unavailable_error"}}"#
        ));
        // Never from output text, and an explicit other code wins over the message.
        assert!(!ov(
            r#"{"type":"response.output_text.delta","delta":"server is overloaded"}"#
        ));
        assert!(!ov(
            r#"{"type":"error","error":{"code":"context_length_exceeded","message":"overloaded"}}"#
        ));
        assert!(matches!(
            k(
                r#"{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"overloaded"}}"#
            ),
            EventKind::Quota(_)
        ));
        assert_eq!(
            overload_body(
                400,
                br#"{"error":{"message":"Our servers are currently overloaded."}}"#
            )
            .as_deref(),
            Some("server_is_overloaded")
        );
        assert_eq!(
            overload_body(
                503,
                br#"{"error":{"type":"server_error","code":"server_is_overloaded"}}"#
            )
            .as_deref(),
            Some("server_is_overloaded")
        );
        assert_eq!(overload_body(503, b"busy"), None);
        assert_eq!(
            overload_body(401, br#"{"error":{"message":"overloaded"}}"#),
            None
        );
        let mut h = HeaderMap::new();
        h.insert(
            "x-retry-metadata",
            HeaderValue::from_static("NO_MORE_RETRY"),
        );
        assert!(no_more_retry(&h));
    }

    #[test]
    fn encrypted_rejection_kind() {
        let ev = r#"{"type":"error","error":{"message":"The encrypted content for item rs_1 could not be verified. Reason: Encrypted content could not be decrypted or parsed.","type":"invalid_request_error","param":"input","code":"invalid_encrypted_content"}}"#;
        assert!(
            matches!(event_kind(ev), EventKind::Encrypted(m) if m.contains("decrypted or parsed"))
        );
        // Not an overload / quota / content event.
        let v: serde_json::Value = serde_json::from_str(ev).unwrap();
        assert!(overload_code(&v).is_none() && !quota_event(&v).0);
    }

    #[test]
    fn bookkeeping_events_are_preamble() {
        let p = |t: &str| matches!(event_kind(t), EventKind::Preamble);
        assert!(p(
            r#"{"type":"response.metadata","response_id":"r1","metadata":{"openai_chatgpt_moderation_metadata":{"prompt":{}}}}"#
        ));
        assert!(p(
            r#"{"type":"response.queued","response":{"id":"r1","status":"queued"}}"#
        ));
        assert!(p(r#"{"type":"keepalive"}"#));
        // Real content / unknown events stay Content.
        assert!(!p(r#"{"type":"response.output_text.delta","delta":"hi"}"#));
        assert!(!p(r#"{"type":"unknown_event","x":1}"#));
        // Empty item / part announcements hold; anything with content commits.
        assert!(p(
            r#"{"type":"response.output_item.added","item":{"type":"message","role":"assistant","content":[]}}"#
        ));
        assert!(p(
            r#"{"type":"response.output_item.added","item":{"type":"reasoning","summary":[],"encrypted_content":null}}"#
        ));
        assert!(p(
            r#"{"type":"response.content_part.added","part":{"type":"output_text","annotations":[],"text":""}}"#
        ));
        assert!(p(
            r#"{"type":"response.reasoning_summary_part.added","part":{"type":"summary_text","text":""}}"#
        ));
        assert!(p(
            r#"{"type":"response.output_item.added","item":{"type":"function_call","name":"shell","arguments":""}}"#
        ));
        assert!(p(
            r#"{"type":"response.output_item.added","item":{"type":"custom_tool_call","name":"apply_patch","input":""}}"#
        ));
        assert!(!p(
            r#"{"type":"response.output_item.added","item":{"type":"function_call","name":"shell","arguments":"{}"}}"#
        ));
        assert!(!p(
            r#"{"type":"response.output_item.added","item":{"type":"message","content":[{"type":"output_text","text":"hi"}]}}"#
        ));
        assert!(!p(
            r#"{"type":"response.output_item.added","item":{"type":"reasoning","summary":[],"encrypted_content":"gAAA"}}"#
        ));
        assert!(!p(
            r#"{"type":"response.content_part.added","part":{"type":"output_text","text":"hi"}}"#
        ));
        assert!(!p(
            r#"{"type":"response.output_item.added","item":{"type":"web_search_call"}}"#
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ws::frames::replayable;

    #[test]
    fn classifies() {
        let (k, d, _) = classify(
            r#"{"type":"error","status":429,"error":{"type":"usage_limit_reached","resets_in_seconds":60},"headers":{"x-codex-primary-used-percent":"100"}}"#,
        );
        assert!(d);
        match k {
            proxy::EventKind::Quota(Outcome::RateLimited {
                usage_limit,
                retry_after,
                quota,
            }) => {
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
            proxy::EventKind::Quota(_)
        ));
        assert!(classify(r#"{"type":"response.completed"}"#).1);
        assert_eq!(
            classify(r#"{"type":"response.created"}"#).0,
            proxy::EventKind::Preamble
        );
        assert_eq!(
            classify(r#"{"type":"response.output_text.delta"}"#).0,
            proxy::EventKind::Content
        );
        assert!(replayable(r#"{"type":"response.create","input":[]}"#));
    }

    /// Codex (`map_wrapped_websocket_error_event`) ends a turn on an `error` frame only for
    /// a non-2xx `status`, or the codes it maps itself; any other error frame is ignored and
    /// it keeps reading. The proxy must not settle the turn on such a frame.
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
