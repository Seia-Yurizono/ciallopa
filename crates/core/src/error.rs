use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("account not found: {0}")]
    NotFound(String),
    #[error("refresh token invalid or revoked; re-login required")]
    ReloginRequired,
    #[error("token refresh failed: {0}")]
    Refresh(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid auth file: {0}")]
    Invalid(String),
    #[error("not implemented")]
    Unimplemented,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum RouteError {
    #[error("no accounts configured")]
    NoAccounts,
    /// Every candidate is cooling down. `retry_after` = until the first one recovers;
    /// `quota` = every one is out of usage quota; `earliest_quota` = the first one to recover
    /// is out of usage quota (its reset is `retry_after`). Only these may be answered with a
    /// synthesized `usage_limit_reached`.
    #[error("all accounts cooling down")]
    AllCoolingDown {
        retry_after: Duration,
        quota: bool,
        earliest_quota: bool,
    },
    #[error("not implemented")]
    Unimplemented,
}
