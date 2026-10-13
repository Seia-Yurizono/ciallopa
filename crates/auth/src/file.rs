use crate::oauth::{parse_jwt, OAuthClient, RefreshFailure, TokenResponse};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cpa_lite_core::{Account, AccountId, AccountStore, AuthError, Credentials};
use serde_json::Value;
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// On-disk format: CPA codex auth JSON (plain, 0600) + cpa-lite routing fields.
/// Unknown fields are preserved on rewrite.
#[derive(Debug, Clone)]
pub struct AuthFile {
    pub id: AccountId,
    pub raw: Value,
}
impl AuthFile {
    pub(crate) fn s(&self, k: &str) -> Option<String> {
        self.raw.get(k).and_then(Value::as_str).map(str::to_owned)
    }
    pub fn to_account(&self) -> Account {
        Account {
            id: self.id.clone(),
            email: self.s("email"),
            chatgpt_account_id: self.s("account_id"),
            plan_type: self.s("plan_type"),
            priority: self
                .raw
                .get("priority")
                .and_then(Value::as_i64)
                .unwrap_or(0) as i32,
            weight: self
                .raw
                .get("weight")
                .and_then(Value::as_i64)
                .unwrap_or(1)
                .min(1_000_000),
            disabled: self
                .raw
                .get("disabled")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }
    }
}

/// Atomic write (tmp + fsync + rename), file mode 0600, dir 0700.
pub fn write_auth_file(dir: &Path, id: &str, v: &Value) -> Result<PathBuf, AuthError> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    if id.contains('/') || id.starts_with('.') || id.is_empty() {
        return Err(AuthError::Invalid(format!("bad id {id:?}")));
    }
    std::fs::create_dir_all(dir)?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    let path = dir.join(format!("{id}.json"));
    let tmp = dir.join(format!(".{id}.json.tmp"));
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)?;
    f.write_all(&serde_json::to_vec_pretty(v).map_err(|e| AuthError::Invalid(e.to_string()))?)?;
    f.sync_all()?;
    std::fs::rename(&tmp, &path)?;
    Ok(path)
}

/// Refresh when the access token expires within this window (INTERFACES: `exp < now+5m`).
pub const REFRESH_SKEW: chrono::Duration = chrono::Duration::minutes(5);
/// A `force_refresh` arriving within this window of a completed refresh reuses it (single-flight).
const FORCE_DEDUP: Duration = Duration::from_secs(10);

impl AuthFile {
    /// `expired` (RFC3339, CPA) or else the access-token JWT `exp`.
    pub fn expires_at(&self) -> Option<DateTime<Utc>> {
        if let Some(t) = self
            .s("expired")
            .and_then(|e| DateTime::parse_from_rfc3339(&e).ok())
        {
            return Some(t.with_timezone(&Utc));
        }
        self.s("access_token")
            .and_then(|t| parse_jwt(&t))
            .and_then(|c| c.exp)
            .and_then(|e| DateTime::from_timestamp(e, 0))
    }
    fn needs_refresh(&self, now: DateTime<Utc>) -> bool {
        self.expires_at().is_some_and(|e| e < now + REFRESH_SKEW)
    }
    fn needs_relogin(&self) -> bool {
        self.raw.get("needs_relogin").and_then(Value::as_bool) == Some(true)
    }
    fn credentials(&self) -> Result<Credentials, AuthError> {
        let access_token = self
            .s("access_token")
            .filter(|t| !t.is_empty())
            .ok_or_else(|| AuthError::Invalid("missing access_token".into()))?;
        Ok(Credentials {
            account: self.id.clone(),
            access_token,
            chatgpt_account_id: self.s("account_id"),
        })
    }
}

/// Build/merge a CPA-compatible codex auth JSON from a token response.
/// Unknown fields of `prev` (priority, weight, ...) are kept; `disabled`/`needs_relogin` cleared.
pub fn merge_tokens(prev: Option<&Value>, t: &TokenResponse) -> Value {
    let mut v = prev.cloned().unwrap_or_else(|| serde_json::json!({}));
    let now = Utc::now();
    let obj = match v.as_object_mut() {
        Some(o) => o,
        None => return merge_tokens(None, t),
    };
    obj.insert("type".into(), "codex".into());
    obj.insert("access_token".into(), t.access_token.clone().into());
    if let Some(r) = t.refresh_token.as_ref().filter(|r| !r.is_empty()) {
        obj.insert("refresh_token".into(), r.clone().into());
    }
    if let Some(i) = t.id_token.as_ref().filter(|r| !r.is_empty()) {
        obj.insert("id_token".into(), i.clone().into());
    }
    let id_claims = t
        .id_token
        .as_deref()
        .and_then(parse_jwt)
        .unwrap_or_default();
    let at_claims = parse_jwt(&t.access_token).unwrap_or_default();
    let pick = |a: &Option<String>, b: &Option<String>| a.clone().or_else(|| b.clone());
    if let Some(x) = pick(&id_claims.chatgpt_account_id, &at_claims.chatgpt_account_id) {
        obj.insert("account_id".into(), x.into());
    }
    if let Some(x) = pick(&id_claims.email, &at_claims.email) {
        obj.insert("email".into(), x.into());
    }
    if let Some(x) = pick(&id_claims.plan_type, &at_claims.plan_type) {
        obj.insert("plan_type".into(), x.into());
    }
    let exp = t
        .expires_in
        .map(|s| now + chrono::Duration::seconds(s))
        .or_else(|| at_claims.exp.and_then(|e| DateTime::from_timestamp(e, 0)));
    if let Some(e) = exp {
        obj.insert("expired".into(), rfc3339(e).into());
    }
    obj.insert("last_refresh".into(), rfc3339(now).into());
    // A successful (re)login re-enables accounts that refresh had disabled; a manual
    // `disabled: true` without `needs_relogin` is the user's choice and is kept.
    if obj.remove("needs_relogin").and_then(|v| v.as_bool()) == Some(true) {
        obj.remove("disabled");
    }
    v
}
pub(crate) fn rfc3339(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// CPA `CredentialFileName` (without hash): `codex-<email>-<plan>`.
pub fn account_id_for(v: &Value) -> String {
    let email = v.get("email").and_then(Value::as_str).unwrap_or("unknown");
    let plan: Vec<String> = v
        .get("plan_type")
        .and_then(Value::as_str)
        .unwrap_or("")
        .split(|c: char| !c.is_alphanumeric())
        .filter(|p| !p.is_empty())
        .map(str::to_lowercase)
        .collect();
    let base = if plan.is_empty() {
        format!("codex-{email}")
    } else {
        format!("codex-{email}-{}", plan.join("-"))
    };
    base.replace(['/', '\\'], "_")
}

type RefreshLock = Arc<tokio::sync::Mutex<Option<Instant>>>;

pub struct FileAccountStore {
    dir: PathBuf,
    files: RwLock<Vec<AuthFile>>,
    oauth: Option<OAuthClient>,
    locks: std::sync::Mutex<HashMap<AccountId, RefreshLock>>,
}
impl FileAccountStore {
    /// Store without refresh capability (tests / read-only use).
    pub async fn open(dir: PathBuf) -> Result<Self, AuthError> {
        Self::build(dir, None).await
    }
    /// Store that refreshes tokens through `oauth`.
    pub async fn open_with(dir: PathBuf, oauth: OAuthClient) -> Result<Self, AuthError> {
        Self::build(dir, Some(oauth)).await
    }
    async fn build(dir: PathBuf, oauth: Option<OAuthClient>) -> Result<Self, AuthError> {
        let s = Self {
            dir,
            files: RwLock::new(vec![]),
            oauth,
            locks: Default::default(),
        };
        s.reload().await?;
        Ok(s)
    }
    async fn get(&self, id: &AccountId) -> Result<AuthFile, AuthError> {
        self.files
            .read()
            .await
            .iter()
            .find(|f| &f.id == id)
            .cloned()
            .ok_or_else(|| AuthError::NotFound(id.clone()))
    }
    async fn put(&self, f: AuthFile) -> Result<(), AuthError> {
        write_auth_file(&self.dir, &f.id, &f.raw)?;
        let mut files = self.files.write().await;
        match files.iter_mut().find(|x| x.id == f.id) {
            Some(x) => *x = f,
            None => files.push(f),
        }
        Ok(())
    }
    fn lock_for(&self, id: &AccountId) -> RefreshLock {
        let mut m = self.locks.lock().unwrap_or_else(|e| e.into_inner());
        m.entry(id.clone()).or_default().clone()
    }

    /// Single-flight refresh per account.
    async fn refresh(&self, id: &AccountId, force: bool) -> Result<Credentials, AuthError> {
        let lock = self.lock_for(id);
        let mut last = lock.lock().await;
        let f = self.get(id).await?;
        if f.needs_relogin() {
            return Err(AuthError::ReloginRequired);
        }
        let now = Utc::now();
        if !force && !f.needs_refresh(now) {
            return f.credentials(); // someone else refreshed while we waited
        }
        if force && last.is_some_and(|t| t.elapsed() < FORCE_DEDUP) {
            return f.credentials();
        }
        let oauth = self
            .oauth
            .as_ref()
            .ok_or_else(|| AuthError::Refresh("refresh not configured".into()))?;
        let rt = f
            .s("refresh_token")
            .filter(|r| !r.is_empty())
            .ok_or(AuthError::ReloginRequired)?;
        match oauth.refresh(&rt).await {
            Ok(t) => {
                let nf = AuthFile {
                    id: f.id.clone(),
                    raw: merge_tokens(Some(&f.raw), &t),
                };
                self.put(nf.clone()).await?;
                *last = Some(Instant::now());
                tracing::info!(account = %id, "token refreshed");
                nf.credentials()
            }
            Err(RefreshFailure::Permanent(why)) => {
                tracing::warn!(account = %id, %why, "refresh token rejected; account disabled, re-login required");
                let mut raw = f.raw.clone();
                if let Some(o) = raw.as_object_mut() {
                    o.insert("disabled".into(), true.into());
                    o.insert("needs_relogin".into(), true.into());
                }
                self.put(AuthFile { id: f.id, raw }).await?;
                Err(AuthError::ReloginRequired)
            }
            Err(RefreshFailure::Transient(why)) => {
                let still_valid = f.expires_at().is_some_and(|e| e > now);
                if !force && still_valid {
                    tracing::warn!(account = %id, %why, "proactive refresh failed; using current token");
                    f.credentials()
                } else {
                    Err(AuthError::Refresh(why))
                }
            }
        }
    }

    fn scan(dir: &Path) -> Result<Vec<AuthFile>, AuthError> {
        let mut out = vec![];
        let rd = match std::fs::read_dir(dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e.into()),
        };
        for e in rd.flatten() {
            let p = e.path();
            let name = e.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') || p.extension().and_then(|x| x.to_str()) != Some("json") {
                continue;
            }
            let raw: Value = match std::fs::read(&p)
                .ok()
                .and_then(|b| serde_json::from_slice(&b).ok())
            {
                Some(v) => v,
                None => {
                    tracing::warn!(file = %name, "skipping unreadable auth file");
                    continue;
                }
            };
            let id = name.trim_end_matches(".json").to_string();
            out.push(AuthFile { id, raw });
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }
}

#[async_trait]
impl AccountStore for FileAccountStore {
    async fn list(&self) -> Vec<Account> {
        self.files
            .read()
            .await
            .iter()
            .map(AuthFile::to_account)
            .collect()
    }
    async fn credentials(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        let f = self.get(id).await?;
        if f.needs_relogin() {
            return Err(AuthError::ReloginRequired);
        }
        if f.needs_refresh(Utc::now()) && self.oauth.is_some() {
            return self.refresh(id, false).await;
        }
        f.credentials()
    }
    async fn force_refresh(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        self.refresh(id, true).await
    }
    async fn reload(&self) -> Result<(), AuthError> {
        let v = Self::scan(&self.dir)?;
        *self.files.write().await = v;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn write_and_list() {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path().join("auths");
        let p = write_auth_file(
            &dir,
            "a@x-plus",
            &serde_json::json!({"type":"codex","email":"a@x","access_token":"t","account_id":"acc","priority":2,"weight":3}),
        )
        .unwrap();
        assert_eq!(
            std::fs::metadata(&p).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let s = FileAccountStore::open(dir).await.unwrap();
        let l = s.list().await;
        assert_eq!(l.len(), 1);
        assert_eq!((l[0].priority, l[0].weight), (2, 3));
        let c = s.credentials(&"a@x-plus".into()).await.unwrap();
        assert_eq!(c.chatgpt_account_id.as_deref(), Some("acc"));
        assert!(!format!("{c:?}").contains("\"t\""));
        assert!(write_auth_file(d.path(), "../x", &serde_json::json!({})).is_err());
    }
}
