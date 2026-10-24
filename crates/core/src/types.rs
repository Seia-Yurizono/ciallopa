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

/// A `*.json` file in the auth directory that is not loaded as an account. Carries file
/// metadata only, never file contents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkippedAuthFile {
    /// File name inside the auth directory.
    pub file: String,
    pub reason: SkipReason,
    /// `type` of a non-codex file (`claude`, `gemini`, ...), or the I/O error of a failed read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Owner, group and permission bits of the file, when they can be read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<u32>,
    /// The file still exists but cannot be read or parsed right now, and an account already
    /// loaded from it stays as it was (last good contents, cooldowns, bindings, connections).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub kept: bool,
}

/// Why an auth file is not loaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    /// The server user may not read the file.
    PermissionDenied,
    /// Any other read failure.
    ReadError,
    /// Not a JSON object.
    InvalidJson,
    /// A CPA auth file of another provider (`type` other than `codex`).
    UnsupportedType,
    /// No `access_token` (for example a Codex CLI `auth.json`, which goes through import).
    MissingTokens,
    /// The file name cannot be an account id.
    InvalidName,
}

impl SkipReason {
    /// The file is still there but unreadable or unparsable (permissions, I/O error, a half
    /// written file): a rescan keeps an account already loaded from it instead of removing it.
    /// Only a file that is gone removes its account.
    pub fn keeps_account(self) -> bool {
        matches!(
            self,
            Self::PermissionDenied | Self::ReadError | Self::InvalidJson
        )
    }
}

/// Result of importing one auth document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportOutcome {
    /// Written as a new account file.
    Added(AccountId),
    /// The same account is already present; nothing was written.
    Exists(AccountId),
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
    /// 404 whose body explicitly says the model is not supported / does not exist for this
    /// account (cooldown for that model: Retry-After else 12h).
    ModelUnsupported {
        retry_after: Option<Duration>,
    },
    /// Any other 404: likely transient (upstream blip, model rollout). That model on that
    /// account cools for Retry-After, else `cooldown` (`[upstream_opts] not_found_cooldown_secs`;
    /// zero = not cooled, only failed over).
    NotFound {
        retry_after: Option<Duration>,
        cooldown: Duration,
    },
    /// 402: account-level. `deactivated` = `deactivated_workspace` (the ChatGPT workspace was
    /// deactivated); otherwise a billing / payment problem. The whole account cools for
    /// `cooldown` (zero = not cooled, only failed over).
    PaymentRequired {
        deactivated: bool,
        cooldown: Duration,
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
    /// Cloudflare challenge or block page in front of the upstream (403 with `cf-mitigated`, a
    /// challenge page, or a Cloudflare `1xxx` error such as 1010). It blocks this host's egress,
    /// not the account, so it never cools an account or a model and never moves a binding.
    EgressBlocked,
    /// 4xx caused by the request itself; not the account's fault.
    ClientError {
        status: u16,
    },
}
