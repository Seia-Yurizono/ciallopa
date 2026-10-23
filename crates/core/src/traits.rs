use crate::{
    Account, AccountId, AccountPatch, AuthError, Credentials, ImportOutcome, Outcome, RouteError,
    RouteRequest, Selection, SkippedAuthFile,
};
use async_trait::async_trait;

/// Owned by the **auth** worker (crate `ciallopa-auth`).
#[async_trait]
pub trait AccountStore: Send + Sync + 'static {
    /// Snapshot of all known accounts (including disabled; router filters).
    async fn list(&self) -> Vec<Account>;
    /// Valid credentials; refreshes (with per-account lock) if expiring within 5 min.
    async fn credentials(&self, id: &AccountId) -> Result<Credentials, AuthError>;
    /// Called by forwarding after an upstream 401: refresh now, ignoring expiry.
    async fn force_refresh(&self, id: &AccountId) -> Result<Credentials, AuthError>;
    /// Re-scan auth_dir (new logins / imports).
    async fn reload(&self) -> Result<(), AuthError>;
    /// Accounts whose refresh token was rejected (`refresh_token_reused`, `invalid_grant`, ...):
    /// excluded from routing until re-login. Default: none.
    async fn relogin_required(&self) -> Vec<AccountId> {
        vec![]
    }
    /// Change `disabled` / `weight` / `priority` (same locks as token refresh).
    async fn update_account(
        &self,
        _id: &AccountId,
        _p: &AccountPatch,
    ) -> Result<Account, AuthError> {
        Err(AuthError::Unimplemented)
    }
    /// Delete the account's auth file (same locks as token refresh).
    async fn remove_account(&self, _id: &AccountId) -> Result<(), AuthError> {
        Err(AuthError::Unimplemented)
    }
    /// Import a Codex CLI `auth.json` (or CPA codex auth file) document; returns the account id.
    async fn import_account(&self, _doc: &serde_json::Value) -> Result<AccountId, AuthError> {
        Err(AuthError::Unimplemented)
    }
    /// Import a document only when its account is not present yet (same ChatGPT account id,
    /// or the same email when either side has no account id). Does not re-scan; the caller
    /// reloads once after a batch.
    async fn import_new_account(
        &self,
        _doc: &serde_json::Value,
    ) -> Result<ImportOutcome, AuthError> {
        Err(AuthError::Unimplemented)
    }
    /// Files of the auth directory that the last scan did not load, with the reason.
    async fn skipped(&self) -> Vec<SkippedAuthFile> {
        vec![]
    }
}

/// Owned by the **routing** worker (crate `ciallopa-routing`). Sync + cheap: no I/O.
pub trait Router: Send + Sync + 'static {
    /// Pick an account among `accounts` honoring cooldown, priority, strategy,
    /// weights, session affinity and quota-threshold avoidance (new sessions only).
    fn select(&self, req: &RouteRequest, accounts: &[Account]) -> Result<Selection, RouteError>;
    /// Feed back the result of an attempt (cooldown, quota, rebind-on).
    fn report(&self, req: &RouteRequest, account: &AccountId, outcome: &Outcome);
    /// Hot-reload of `[routing]`; runtime state (cooldowns, bindings) is kept.
    fn update_config(&self, _cfg: &crate::config::RoutingConfig) {}
    /// Forget everything about a removed account (bindings, cooldowns, quota).
    fn forget_account(&self, _id: &AccountId) {}
    /// When the account is usable again for any model (None = not cooling down).
    fn cooling_until(&self, _id: &AccountId) -> Option<std::time::SystemTime> {
        None
    }
    /// Why the account is cooling down (`usage_limit`, `rate_limited`, `auth`, `transient`,
    /// `overloaded`, `challenge`, `model_unsupported`, `not_found`, `payment_required`,
    /// `workspace_deactivated`); None = not cooling.
    fn cooling_reason(&self, _id: &AccountId) -> Option<&'static str> {
        None
    }
    /// One upstream attempt was made on this account (24 h overload ratio denominator).
    fn note_attempt(&self, _id: &AccountId) {}
    /// Real output reached the client from this account: resets its overload streak/backoff.
    fn content_ok(&self, _id: &AccountId) {}
    /// Upstream attempts and overloaded ones on this account over the last 24 h (in memory).
    fn overload_stats(&self, _id: &AccountId) -> (u64, u64) {
        (0, 0)
    }
    /// Manual "clear cooldown": drop every cooldown and failure streak of the account.
    /// Returns whether anything was cleared.
    fn clear_cooldown(&self, _id: &AccountId) -> bool {
        false
    }
    /// The account's usage windows were reset upstream (a manually used reset credit): drop
    /// its usage-limit / rate-limit cooldowns (account- and model-level) and their backoff.
    /// Other cooldowns (auth, 402, 404, 5xx, overload, challenge) are kept.
    /// Returns whether a live cooldown was cleared.
    fn clear_quota_cooldown(&self, _id: &AccountId) -> bool {
        false
    }
}
