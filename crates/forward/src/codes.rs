//! The error codes that can appear in a usage record's `error`, as one list. The admin UI
//! labels exactly these (server/tests/it/ui_labels.rs), and `usage::describe` explains only
//! these, so a new code cannot ship unlabelled and a removed one leaves no dead label.

/// Codes the proxy itself records (local decisions, network / stream classification).
/// Dynamic families are not listed: `http_<status>` (unclassified status) and
/// `incomplete_<reason>` (see `usage::incomplete_code`).
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

/// No longer produced, still present in older rows.
pub const LEGACY: &[&str] = &[
    // Split into upstream_ws_close / _eof / _read_error / _send_error.
    "upstream_closed",
];
