//! OAuth constants and wire calls, mirroring codex-rs `login` (and CPA `internal/auth/codex`).
use base64::Engine;
use ciallopa_core::AuthError;
use serde::Deserialize;
use serde_json::Value;
use sha2::Digest;
use std::time::Duration;

/// codex-rs `login/src/auth/manager.rs::CLIENT_ID` == CPA `ClientID`.
pub const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
pub const ISSUER: &str = "https://auth.openai.com";
/// codex-rs `DEFAULT_ORIGINATOR`.
pub const ORIGINATOR: &str = "codex_cli_rs";
/// codex-rs `build_authorize_url` scope.
pub const SCOPE: &str =
    "openid profile email offline_access api.connectors.read api.connectors.invoke";
pub const LOCAL_REDIRECT_URI: &str = "http://localhost:1455/auth/callback";

/// Where the OAuth server lives (overridable for tests) + the HTTP client to reach it.
#[derive(Clone)]
pub struct OAuthClient {
    pub issuer: String,
    pub client_id: String,
    pub http: reqwest::Client,
    /// Per-account clients for token refresh (same builder, separate connection pools).
    pub pools: Option<std::sync::Arc<ciallopa_upstream::ClientPool>>,
}

/// `User-Agent` + `originator` of the latest learned Codex client (codex-rs sends both as
/// default headers of its HTTP client, including for token refresh).
pub fn client_headers() -> reqwest::header::HeaderMap {
    let (ua, orig) = ciallopa_core::fingerprint::headers();
    let mut h = reqwest::header::HeaderMap::new();
    if let Ok(v) = reqwest::header::HeaderValue::from_str(&ua) {
        h.insert(reqwest::header::USER_AGENT, v);
    }
    if let Ok(v) = reqwest::header::HeaderValue::from_str(&orig) {
        h.insert("originator", v);
    }
    h
}

impl OAuthClient {
    /// Production endpoints via the pinned upstream client (honours the global egress proxy).
    pub fn new(proxy_url: &str, ipv6: bool) -> Result<Self, AuthError> {
        let opts = ciallopa_upstream::ClientOptions {
            proxy_url: proxy_url.to_string(),
            ipv6,
            connect_timeout: Some(Duration::from_secs(10)),
            // auth.openai.com is not a ChatGPT cookie host (codex-rs keeps none for it).
            cloudflare_cookies: false,
        };
        let http = ciallopa_upstream::build_reqwest(&opts)
            .map_err(|e| AuthError::Refresh(format!("http client: {e}")))?;
        let mut c = Self::with_issuer(ISSUER, http);
        c.pools = Some(std::sync::Arc::new(ciallopa_upstream::ClientPool::new(
            opts,
        )));
        Ok(c)
    }
    pub fn with_issuer(issuer: &str, http: reqwest::Client) -> Self {
        Self {
            issuer: issuer.trim_end_matches('/').to_string(),
            client_id: CLIENT_ID.to_string(),
            http,
            pools: None,
        }
    }
    /// Client for one account's token traffic: its own pool when configured.
    pub fn http_for(&self, account: &str) -> reqwest::Client {
        self.pools
            .as_ref()
            .and_then(|p| p.get(account).ok())
            .unwrap_or_else(|| self.http.clone())
    }
    pub fn token_url(&self) -> String {
        format!("{}/oauth/token", self.issuer)
    }

    pub fn authorize_url(&self, redirect_uri: &str, pkce: &Pkce, state: &str) -> String {
        let mut u =
            url::Url::parse(&format!("{}/oauth/authorize", self.issuer)).unwrap_or_else(|_| {
                url::Url::parse("https://auth.openai.com/oauth/authorize").expect("static url")
            });
        u.query_pairs_mut()
            .append_pair("response_type", "code")
            .append_pair("client_id", &self.client_id)
            .append_pair("redirect_uri", redirect_uri)
            .append_pair("code_challenge", &pkce.challenge)
            .append_pair("code_challenge_method", "S256")
            .append_pair("state", state)
            .append_pair("scope", SCOPE)
            .append_pair("id_token_add_organizations", "true")
            .append_pair("codex_cli_simplified_flow", "true")
            .append_pair("originator", ORIGINATOR);
        u.into()
    }

    /// authorization_code grant, form-encoded (codex-rs `TokenEncoding::Form`).
    pub async fn exchange_code(
        &self,
        code: &str,
        redirect_uri: &str,
        verifier: &str,
    ) -> Result<TokenResponse, AuthError> {
        let body = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("grant_type", "authorization_code")
            .append_pair("client_id", &self.client_id)
            .append_pair("code", code)
            .append_pair("redirect_uri", redirect_uri)
            .append_pair("code_verifier", verifier)
            .finish();
        let resp = self
            .http
            .post(self.token_url())
            .headers(client_headers())
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await
            .map_err(|e| AuthError::Refresh(format!("token exchange: {}", e.without_url())))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(AuthError::Refresh(format!(
                "token exchange failed: {status}"
            )));
        }
        resp.json()
            .await
            .map_err(|_| AuthError::Refresh("token exchange: invalid response".into()))
    }

    /// refresh_token grant, JSON (codex-rs `request_chatgpt_token_refresh`, `TokenEncoding::Json`).
    /// Sent as a manual refresh (`refresh_reason: "forced"`).
    pub async fn refresh(&self, refresh_token: &str) -> Result<TokenResponse, RefreshFailure> {
        self.refresh_as(None, refresh_token, RefreshReason::Forced)
            .await
    }
    /// Refresh over `account`'s own connection pool (if any).
    pub async fn refresh_as(
        &self,
        account: Option<&str>,
        refresh_token: &str,
        reason: RefreshReason,
    ) -> Result<TokenResponse, RefreshFailure> {
        let http = match account {
            Some(a) => self.http_for(a),
            None => self.http.clone(),
        };
        let resp = http
            .post(self.token_url())
            .headers(client_headers())
            .header("Content-Type", "application/json")
            // codex-rs 0.164 `OAuthClient::exchange`: the pairs go through a BTreeMap, so the
            // keys are sorted (client_id, grant_type, refresh_reason, refresh_token).
            .json(&refresh_body(&self.client_id, refresh_token, reason))
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| RefreshFailure::Transient(e.without_url().to_string()))?;
        let status = resp.status();
        if status.is_success() {
            return resp
                .json()
                .await
                .map_err(|_| RefreshFailure::Transient("invalid refresh response".into()));
        }
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        let code = error_code(&body);
        let permanent = status == reqwest::StatusCode::UNAUTHORIZED
            || matches!(
                code.as_deref(),
                Some(
                    "refresh_token_expired" | "refresh_token_reused" | "refresh_token_invalidated"
                )
            )
            || (status == reqwest::StatusCode::BAD_REQUEST
                && code.as_deref() == Some("invalid_grant"));
        let msg = format!("{status} {}", code.unwrap_or_default());
        Err(if permanent {
            RefreshFailure::Permanent(msg)
        } else {
            RefreshFailure::Transient(msg)
        })
    }
}

/// Why a token is refreshed; sent as `refresh_reason` (codex-rs 0.164 `RefreshReason`,
/// openai/codex#53991). Diagnostics for the token endpoint only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefreshReason {
    /// The access token expires within the refresh window.
    ProactiveExpiry,
    /// `last_refresh` is older than the refresh interval.
    Scheduled,
    /// The upstream answered 401 with the current token.
    UnauthorizedRecovery,
    /// Manual refresh.
    Forced,
}

impl RefreshReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProactiveExpiry => "proactive_expiry",
            Self::Scheduled => "scheduled",
            Self::UnauthorizedRecovery => "unauthorized_recovery",
            Self::Forced => "forced",
        }
    }
}

fn refresh_body(client_id: &str, refresh_token: &str, reason: RefreshReason) -> Value {
    let m: std::collections::BTreeMap<&str, &str> = [
        ("grant_type", "refresh_token"),
        ("client_id", client_id),
        ("refresh_token", refresh_token),
        ("refresh_reason", reason.as_str()),
    ]
    .into_iter()
    .collect();
    serde_json::to_value(m).unwrap_or(Value::Null)
}

/// `{"error":"x"}` or `{"error":{"code":"x"}}`, lower-cased.
fn error_code(v: &Value) -> Option<String> {
    let e = v.get("error")?;
    e.as_str()
        .or_else(|| e.get("code").and_then(Value::as_str))
        .map(str::to_ascii_lowercase)
}

#[derive(Debug, PartialEq)]
pub enum RefreshFailure {
    /// invalid_grant / 401 / refresh_token_{expired,reused,invalidated}: re-login needed.
    Permanent(String),
    Transient(String),
}

#[derive(Debug, Clone, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub id_token: Option<String>,
    #[serde(default)]
    pub expires_in: Option<i64>,
}

pub struct Pkce {
    pub verifier: String,
    pub challenge: String,
}
impl Pkce {
    /// codex-rs `generate_pkce`: 64 random bytes, base64url, S256.
    pub fn generate() -> Self {
        let mut b = [0u8; 64];
        rand::fill(&mut b);
        let verifier = b64(&b);
        Self::from_verifier(verifier)
    }
    pub fn from_verifier(verifier: String) -> Self {
        let challenge = b64(&sha2::Sha256::digest(verifier.as_bytes()));
        Self {
            verifier,
            challenge,
        }
    }
}
pub fn random_state() -> String {
    let mut b = [0u8; 32];
    rand::fill(&mut b);
    b64(&b)
}
fn b64(b: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b)
}

/// Claims we use from the id_token / access_token (no signature check, like codex-rs and CPA).
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Claims {
    pub email: Option<String>,
    pub chatgpt_account_id: Option<String>,
    pub plan_type: Option<String>,
    pub exp: Option<i64>,
}
pub fn parse_jwt(jwt: &str) -> Option<Claims> {
    let mut it = jwt.split('.');
    let (_, p, _) = (it.next()?, it.next()?, it.next()?);
    let v: Value = serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(p.trim_end_matches('='))
            .ok()?,
    )
    .ok()?;
    let a = v.get("https://api.openai.com/auth");
    let s = |x: Option<&Value>| {
        x.and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
    };
    Some(Claims {
        email: s(v.get("email")).or_else(|| {
            s(v.get("https://api.openai.com/profile")
                .and_then(|p| p.get("email")))
        }),
        chatgpt_account_id: s(a.and_then(|a| a.get("chatgpt_account_id"))),
        plan_type: s(a.and_then(|a| a.get("chatgpt_plan_type"))),
        exp: v.get("exp").and_then(Value::as_i64),
    })
}

#[cfg(test)]
pub(crate) fn fake_jwt(payload: &Value) -> String {
    format!("e30.{}.sig", b64(payload.to_string().as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn jwt_and_pkce() {
        let t = fake_jwt(
            &json!({"email":"a@x","exp":5,"https://api.openai.com/auth":{"chatgpt_account_id":"acc","chatgpt_plan_type":"plus"}}),
        );
        let c = parse_jwt(&t).unwrap();
        assert_eq!(c.email.as_deref(), Some("a@x"));
        assert_eq!(c.chatgpt_account_id.as_deref(), Some("acc"));
        assert_eq!(c.plan_type.as_deref(), Some("plus"));
        assert_eq!(c.exp, Some(5));
        assert!(parse_jwt("nope").is_none());
        // cross-checked with python hashlib
        let p = Pkce::from_verifier("dBjftJeZ4CQP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk".into());
        assert_eq!(p.challenge, "gqPMLHcFRPeR9EKSdmCe60jXybIZiq1XkslY2SWa00Q");
        assert_eq!(Pkce::generate().verifier.len(), 86);
    }
    #[test]
    fn authorize_url_params() {
        let o = OAuthClient::with_issuer(ISSUER, reqwest::Client::new());
        let u = o.authorize_url(LOCAL_REDIRECT_URI, &Pkce::from_verifier("v".into()), "st");
        let q: std::collections::HashMap<_, _> = url::Url::parse(&u)
            .unwrap()
            .query_pairs()
            .into_owned()
            .collect();
        assert_eq!(q["client_id"], CLIENT_ID);
        assert_eq!(q["scope"], SCOPE);
        assert_eq!(q["redirect_uri"], LOCAL_REDIRECT_URI);
        assert_eq!(q["originator"], "codex_cli_rs");
        assert_eq!(q["codex_cli_simplified_flow"], "true");
        assert!(
            u.starts_with("https://auth.openai.com/oauth/authorize?response_type=code&client_id=")
        );
    }
}
