use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

/// Stable local id of an account = auth file stem (e.g. `alice@example.com-plus`).
pub type AccountId = String;

/// Routing-relevant view of an account. Produced by `AccountStore`, consumed by `Router`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Account {
    pub id: AccountId,
    pub email: Option<String>,
    /// ChatGPT workspace/account id sent as `chatgpt-account-id`.
    pub chatgpt_account_id: Option<String>,
    pub plan_type: Option<String>,
    /// Higher first; default 0 (CPA semantics).
    pub priority: i32,
    /// For weighted-round-robin; default 1, <=0 excludes. Max 1_000_000.
    pub weight: i64,
    pub disabled: bool,
}

/// Management change of an account's routing fields (stored in its auth file).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountPatch {
    pub disabled: Option<bool>,
    pub weight: Option<i64>,
    pub priority: Option<i32>,
}

/// What forwarding needs to call upstream on behalf of an account.
#[derive(Clone, PartialEq)]
pub struct Credentials {
    pub account: AccountId,
    pub access_token: String,
    pub chatgpt_account_id: Option<String>,
}
impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("account", &self.account)
            .field("access_token", &"<redacted>")
            .field("chatgpt_account_id", &self.chatgpt_account_id)
            .finish()
    }
}

/// Session identity extracted from a client request (CPA session/info.go order).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionInfo {
    /// e.g. `codex:<thread>`, `pck:<prompt_cache_key>`, `conv:<id>`, `derived:<hash>`.
    pub key: Option<String>,
    pub parent_key: Option<String>,
    pub is_subagent: bool,
    /// Codex fork (`forked_from_thread_id`): inherits parent binding but is not a subagent.
    pub is_fork: bool,
}

/// Input to `Router::select`.
#[derive(Debug, Clone, Default)]
pub struct RouteRequest {
    pub model: String,
    pub session: SessionInfo,
    /// Accounts already tried in this request (failover within one request).
    pub exclude: Vec<AccountId>,
    /// When the current upstream attempt was sent. A later `Success` only clears cooldowns
    /// recorded before this instant (an in-flight success must not undo a newer cooldown).
    pub attempt_at: Option<std::time::SystemTime>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Selection {
    pub account: AccountId,
    /// True when chosen via an existing session binding.
    pub from_binding: bool,
}

/// Quota window parsed from `x-codex-*` headers / usage_limit bodies.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QuotaWindow {
    pub used_percent: f64,
    pub window_minutes: Option<u64>,
    pub resets_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QuotaSnapshot {
    pub primary: Option<QuotaWindow>,
    pub secondary: Option<QuotaWindow>,
}

/// Result of one upstream attempt, reported by forwarding to the router.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Success {
        quota: Option<QuotaSnapshot>,
    },
    /// 429 / usage_limit_reached. `retry_after` from body reset time, else Retry-After.
    RateLimited {
        retry_after: Option<Duration>,
        usage_limit: bool,
        quota: Option<QuotaSnapshot>,
    },
    /// 401/403 after a forced refresh also failed.
    AuthFailed,
    /// 408 / 5xx / connect error / empty stream / idle timeout.
    Transient {
        status: Option<u16>,
    },
    /// 404 / model not supported for this account (cooldown for that model: Retry-After else 12h).
    ModelUnsupported {
        retry_after: Option<Duration>,
    },
    /// Upstream 408 / 5xx. The account is cooled for `cooldown` only after
    /// `TRANSIENT_STREAK` consecutive failures; `None` never cools (feature off, or it is
    /// the last usable account). Any success resets the streak.
    ServerError {
        status: u16,
        cooldown: Option<Duration>,
    },
    /// Upstream capacity / account-level throttling (`server_is_overloaded`, `slow_down`,
    /// `service_unavailable_error`, "at capacity"), in-stream or as HTTP status. Never a quota
    /// or credential problem. After `streak` consecutive ones the account is cooled for
    /// `base * 2^level` (capped at `max`, level grows per cooldown, reset by a success);
    /// `cool = false` (last usable account, or feature off) only counts.
    Overloaded {
        streak: u32,
        base: Duration,
        max: Duration,
        cool: bool,
    },
    /// Cloudflare challenge page (403 + challenge markers): >=10s exponential cooldown.
    Challenge,
    /// 4xx caused by the request itself; not the account's fault.
    ClientError {
        status: u16,
    },
}
