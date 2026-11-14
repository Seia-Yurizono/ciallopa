//! Usage error codes shared with admin labels and descriptions.

pub const LOCAL: &[&str] = &[
    // client / lifecycle
    "client_closed",
    "shutdown",
    // routing (no attempt started)
    "no_account",
    "no_usable_account",
    "all_cooling",
    "all_quota_exhausted",
    "first_quota_reset",
    "model_cooldown",
    // quota / rate limit seen in a stream
    "usage_limit",
    "rate_limit_exceeded",
    // network (HTTP and WebSocket dial)
    "connect",
    "dns",
    "tls",
    "timeout",
    "timeout_connect",
    "transport",
    "proxy_connect",
    "proxy_timeout",
    "header_timeout",
    "preparation_timeout",
    "idle_timeout",
    // error body reads
    "error_body_timeout",
    "error_body_too_large",
    "error_body_read",
    // egress blocked before the upstream
    "cf_blocked",
    "challenge",
    // SSE stream
    "stream_read_error",
    "response_decode_error",
    "unsupported_response_encoding",
    "stream_truncated",
    "upstream_error_event",
    "response_failed",
    "image_output_missing",
    // WebSocket stream
    "upstream_ws_close",
    "upstream_ws_eof",
    "upstream_ws_read_error",
    "upstream_ws_send_error",
    // observation / defensive
    "observation_incomplete",
    "internal",
];

/// Upstream codes passed through as is that the UI explains. Any other upstream code is
/// shown verbatim without an explanation.
pub const UPSTREAM: &[&str] = &[
    "usage_limit_reached",
    "invalid_encrypted_content",
    "context_length_exceeded",
    "invalid_request_error",
    "websocket_connection_limit_reached",
    "previous_response_not_found",
    "server_is_overloaded",
    "policy_capacity_unavailable",
    "turn_rate_limit_exceeded",
    "inference_hook_denied",
    "content_policy_violation",
    "invalid_access_program",
    "unsupported_access_program",
    "access_program_not_enabled",
    "invalid_workspace_selected",
    "codex_workspace_access_denied",
    "budget_reached",
];

pub const LEGACY: &[&str] = &[
    // Split into upstream_ws_close / _eof / _read_error / _send_error.
    "upstream_closed",
];

#[cfg(test)]
mod tests {
    use super::*;

    /// Literal codes handed to the recording calls, or returned by the classifiers, in
    /// non-test source must be listed. A heuristic scan: it catches a new literal at the usual
    /// call sites, which is where new codes appear.
    #[test]
    fn literal_codes_in_source_are_listed() {
        const CALLS: &[&str] = &[
            "fail(\"",
            "fail_msg(\"",
            "network(\"",
            "SRC_LOCAL, \"",
            "SRC_NETWORK, \"",
            "SRC_STREAM, \"",
            "code: \"",
            // ws::upstream_end's (class, code, message)
            "\"eof\", \"",
            "\"read_error\", \"",
        ];
        // errors.rs / egress.rs are classifiers: every returned literal is a code.
        const RETURNS: &[&str] = &["=> \"", "return \"", "else { \""];
        let sources: [(&str, &str, bool); 17] = [
            ("proxy.rs", include_str!("proxy.rs"), false),
            ("attempt/mod.rs", include_str!("attempt/mod.rs"), false),
            ("classify.rs", include_str!("classify.rs"), false),
            ("sse.rs", include_str!("sse.rs"), false),
            ("http/exchange.rs", include_str!("http/exchange.rs"), false),
            ("http/pump.rs", include_str!("http/pump.rs"), false),
            ("ws/mod.rs", include_str!("ws/mod.rs"), false),
            ("ws/connect.rs", include_str!("ws/connect.rs"), false),
            ("ws/session.rs", include_str!("ws/session.rs"), false),
            ("ws/frames.rs", include_str!("ws/frames.rs"), false),
            ("usage.rs", include_str!("usage.rs"), false),
            ("images/mod.rs", include_str!("images/mod.rs"), false),
            (
                "images/request.rs",
                include_str!("images/request.rs"),
                false,
            ),
            (
                "images/wrapper.rs",
                include_str!("images/wrapper.rs"),
                false,
            ),
            ("budget.rs", include_str!("budget.rs"), false),
            ("errors.rs", include_str!("errors.rs"), true),
            ("egress.rs", include_str!("egress.rs"), true),
        ];
        let mut found = 0;
        for (name, src, classifier) in sources {
            let src = src.split("\n#[cfg(test)]\nmod tests").next().unwrap();
            // rustfmt splits long calls over lines: collapse whitespace so `fail(\n "x"` matches.
            let body = src
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .replace("( ", "(");
            let pats = CALLS.iter().chain(if classifier { RETURNS } else { &[] });
            for pat in pats {
                for (i, _) in body.match_indices(pat) {
                    let rest = &body[i + pat.len()..];
                    let code = &rest[..rest.find('"').unwrap()];
                    let is_code = !code.is_empty()
                        && code
                            .bytes()
                            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
                    if !is_code {
                        continue;
                    }
                    found += 1;
                    assert!(
                        LOCAL.contains(&code),
                        "{name}: `{code}` is recorded but not in codes::LOCAL"
                    );
                }
            }
        }
        for (code, _) in crate::usage::DESCRIPTIONS {
            assert!(
                LOCAL.contains(code),
                "usage::describe explains `{code}`, which nothing records"
            );
        }
        let mut all: Vec<_> = LOCAL.iter().chain(UPSTREAM).chain(LEGACY).collect();
        let n = all.len();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), n, "a code is listed twice");
        assert!(found > 30, "scan found only {found} recording sites");
    }
}
