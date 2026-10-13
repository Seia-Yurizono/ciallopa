use crate::{
    Account, AccountId, AuthError, Credentials, Outcome, RouteError, RouteRequest, Selection,
};
use async_trait::async_trait;

/// Owned by the **auth** worker (crate `cpa-lite-auth`).
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
}

/// Owned by the **routing** worker (crate `cpa-lite-routing`). Sync + cheap: no I/O.
pub trait Router: Send + Sync + 'static {
    /// Pick an account among `accounts` honoring cooldown, priority, strategy,
    /// weights, session affinity and quota-threshold avoidance (new sessions only).
    fn select(&self, req: &RouteRequest, accounts: &[Account]) -> Result<Selection, RouteError>;
    /// Feed back the result of an attempt (cooldown, quota, rebind-on).
    fn report(&self, req: &RouteRequest, account: &AccountId, outcome: &Outcome);
}
