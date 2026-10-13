use async_trait::async_trait;
use cpa_lite_core::{Account, AccountId, AccountStore, AuthError, Credentials};
use serde_json::Value;
use std::io::Write;
use std::path::{Path, PathBuf};
use tokio::sync::RwLock;

/// On-disk format: CPA codex auth JSON (plain, 0600) + cpa-lite routing fields.
/// Unknown fields are preserved on rewrite.
#[derive(Debug, Clone)]
pub struct AuthFile {
    pub id: AccountId,
    pub raw: Value,
}
impl AuthFile {
    fn s(&self, k: &str) -> Option<String> {
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

pub struct FileAccountStore {
    dir: PathBuf,
    files: RwLock<Vec<AuthFile>>,
}
impl FileAccountStore {
    pub async fn open(dir: PathBuf) -> Result<Self, AuthError> {
        let s = Self {
            dir,
            files: RwLock::new(vec![]),
        };
        s.reload().await?;
        Ok(s)
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
        // TODO(auth, M1): refresh when exp < now+5min under a per-account lock.
        let files = self.files.read().await;
        let f = files
            .iter()
            .find(|f| &f.id == id)
            .ok_or_else(|| AuthError::NotFound(id.clone()))?;
        let access_token = f
            .s("access_token")
            .ok_or_else(|| AuthError::Invalid("missing access_token".into()))?;
        Ok(Credentials {
            account: id.clone(),
            access_token,
            chatgpt_account_id: f.s("account_id"),
        })
    }
    async fn force_refresh(&self, _id: &AccountId) -> Result<Credentials, AuthError> {
        Err(AuthError::Unimplemented)
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
