//! Account selection with exact CPA semantics. **Owner: routing worker.**
//! M0: placeholder (first enabled account in highest priority tier). See
//! cpa-lite-design README §4 for the full spec to implement in M1.
use cpa_lite_core::config::RoutingConfig;
use cpa_lite_core::{
    Account, AccountId, Outcome, RouteError, RouteRequest, Router, Selection, SessionInfo,
};
use http::HeaderMap;

pub struct CpaRouter {
    #[allow(dead_code)]
    cfg: RoutingConfig,
}
impl CpaRouter {
    pub fn new(cfg: RoutingConfig) -> Self {
        Self { cfg }
    }
}

impl Router for CpaRouter {
    fn select(&self, req: &RouteRequest, accounts: &[Account]) -> Result<Selection, RouteError> {
        let mut c: Vec<&Account> = accounts
            .iter()
            .filter(|a| !a.disabled && a.weight > 0 && !req.exclude.contains(&a.id))
            .collect();
        if c.is_empty() {
            return Err(RouteError::NoAccounts);
        }
        c.sort_by(|a, b| b.priority.cmp(&a.priority).then(a.id.cmp(&b.id)));
        Ok(Selection {
            account: c[0].id.clone(),
            from_binding: false,
        })
    }
    fn report(&self, _req: &RouteRequest, _account: &AccountId, _outcome: &Outcome) {
        // TODO(routing, M1): cooldown, quota snapshot, rebind-on.
    }
}

/// Session extraction (CPA sdk/cliproxy/session/info.go order). M0 handles only
/// the `session_id` header and body `prompt_cache_key`.
pub fn extract_session(headers: &HeaderMap, body: Option<&serde_json::Value>) -> SessionInfo {
    let hdr = |n: &str| {
        headers
            .get(n)
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.is_empty())
    };
    let key = hdr("session_id")
        .or_else(|| hdr("session-id"))
        .map(|s| format!("codex:{s}"))
        .or_else(|| {
            body.and_then(|b| b.get("prompt_cache_key"))
                .and_then(|v| v.as_str())
                .map(|s| format!("pck:{s}"))
        });
    SessionInfo {
        key,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acct(id: &str, p: i32) -> Account {
        Account {
            id: id.into(),
            email: None,
            chatgpt_account_id: None,
            plan_type: None,
            priority: p,
            weight: 1,
            disabled: false,
        }
    }

    #[test]
    fn placeholder_prefers_priority() {
        let r = CpaRouter::new(RoutingConfig::default());
        let s = r
            .select(&RouteRequest::default(), &[acct("a", 0), acct("b", 5)])
            .unwrap();
        assert_eq!(s.account, "b");
        assert_eq!(
            r.select(&RouteRequest::default(), &[]),
            Err(RouteError::NoAccounts)
        );
    }

    #[test]
    fn session_from_header() {
        let mut h = HeaderMap::new();
        h.insert("session_id", "abc".parse().unwrap());
        assert_eq!(extract_session(&h, None).key.as_deref(), Some("codex:abc"));
        let b = serde_json::json!({"prompt_cache_key":"k"});
        assert_eq!(
            extract_session(&HeaderMap::new(), Some(&b)).key.as_deref(),
            Some("pck:k")
        );
    }
}
