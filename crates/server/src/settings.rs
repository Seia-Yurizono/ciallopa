//! Live settings: one apply pipeline for every change source (config file watch, SIGHUP,
//! `POST /admin/v1/config/reload`, `PUT /admin/v1/config`, key management) and the admin
//! config API.
//!
//! `config.toml` is the only source of truth. A change is applied as
//! read → parse + validate → semantic checks → diff → build → atomic swap; any failure leaves
//! the running process unchanged. Keys that only take effect at startup keep their running
//! value and are reported as `pending_restart`.
use crate::reset_credits::ResetCredits;
use axum::{
    extract::{Path as AxPath, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use ciallopa_core::settings::{self as cs, Change, Effect};
use ciallopa_core::Config;
use ciallopa_forward::{AppState, ClientKey};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Debounce for file-change events (editors write in several steps).
pub const DEBOUNCE: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct LastReload {
    pub at_ms: i64,
    pub source: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub applied: Vec<Change>,
}

#[derive(Default)]
struct Status {
    /// Hash of the config file content currently applied (self-write / no-op skip).
    applied_hash: Option<String>,
    /// Hash of the price table file currently loaded.
    prices_hash: Option<String>,
    pending: Vec<Change>,
    last: Option<LastReload>,
}

#[derive(Debug)]
pub struct ApplyError {
    pub status: StatusCode,
    pub errors: Vec<Value>,
}
impl ApplyError {
    fn new(status: StatusCode, msg: impl Into<String>) -> Self {
        Self {
            status,
            errors: vec![json!({"message": msg.into()})],
        }
    }
    fn invalid(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::UNPROCESSABLE_ENTITY, msg)
    }
    fn message(&self) -> String {
        self.errors
            .iter()
            .filter_map(|e| e.get("message").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("; ")
    }
}
impl IntoResponse for ApplyError {
    fn into_response(self) -> Response {
        let msg = self.message();
        (
            self.status,
            Json(json!({"error": {"message": msg}, "errors": self.errors})),
        )
            .into_response()
    }
}

/// Live settings handle shared by the watcher, the signal handler and the admin API.
pub struct Settings {
    path: Option<PathBuf>,
    pub(crate) st: AppState,
    /// Startup config: running values of restart-only keys.
    boot: Arc<Config>,
    pub(crate) rc: ResetCredits,
    oauth_pools: Option<Arc<ciallopa_upstream::ClientPool>>,
    apply: tokio::sync::Mutex<()>,
    status: Mutex<Status>,
    known_accounts: Mutex<HashSet<String>>,
    audit_path: Option<PathBuf>,
    /// Last price check (「检查更新」) waiting for 「应用」.
    price_check: Mutex<Option<PriceCheck>>,
}

struct PriceCheck {
    token: String,
    source: String,
    changes: Vec<ciallopa_usage::prices::Change>,
    local_hash: String,
    at: std::time::Instant,
}

/// A price check older than this must be repeated before applying.
const PRICE_CHECK_TTL: Duration = Duration::from_secs(15 * 60);
/// Models of the usage log looked at for the "unpriced" hint.
const UNPRICED_WINDOW_MS: i64 = 30 * 86_400_000;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub fn hash(b: &[u8]) -> String {
    Sha256::digest(b)
        .iter()
        .take(8)
        .map(|x| format!("{x:02x}"))
        .collect()
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Settings {
    /// `path`: the config file (`None` = built-in defaults only; reload / write unavailable).
    /// `oauth_pools`: per-account token-refresh clients (follow `proxy_url`).
    pub fn new(
        st: AppState,
        path: Option<PathBuf>,
        oauth_pools: Option<Arc<ciallopa_upstream::ClientPool>>,
    ) -> Arc<Self> {
        let boot = st.config.load();
        let audit_path = path.as_ref().map(|p| {
            let mut s = p.as_os_str().to_owned();
            s.push(".audit.jsonl");
            PathBuf::from(s)
        });
        let applied_hash = path
            .as_ref()
            .map(|p| hash(&std::fs::read(p).unwrap_or_default()));
        let prices_hash = Some(hash(&std::fs::read(&boot.usage.prices).unwrap_or_default()));
        Arc::new(Self {
            path,
            st,
            boot,
            rc: ResetCredits::default(),
            oauth_pools,
            apply: Default::default(),
            status: Mutex::new(Status {
                applied_hash,
                prices_hash,
                ..Default::default()
            }),
            known_accounts: Default::default(),
            audit_path,
            price_check: Default::default(),
        })
    }

    /// The running configuration snapshot.
    pub fn running(&self) -> Arc<Config> {
        self.st.config.load()
    }

    #[doc(hidden)]
    pub fn st_config_write_enabled(&self) -> bool {
        self.st.config.load().ui.allow_config_write
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    fn read_file(&self) -> Result<String, ApplyError> {
        let Some(p) = &self.path else {
            return Ok(String::new());
        };
        match std::fs::read_to_string(p) {
            Ok(s) => Ok(s),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
            Err(e) => Err(ApplyError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("read {}: {e}", p.display()),
            )),
        }
    }

    /// Current file content and its version (ETag).
    pub fn file_version(&self) -> Result<(String, String), ApplyError> {
        let s = self.read_file()?;
        let v = hash(s.as_bytes());
        Ok((s, v))
    }

    /// Parse + validate + semantic checks (client buildable, price table parses).
    fn check(
        &self,
        text: &str,
    ) -> Result<(Config, Option<ciallopa_usage::PriceTable>), ApplyError> {
        let c = Config::parse(text).map_err(|e| ApplyError::invalid(e.to_string()))?;
        ciallopa_upstream::build_reqwest(&ciallopa_upstream::ClientOptions::from(&c))
            .map_err(|e| ApplyError::invalid(format!("proxy_url: {e}")))?;
        let prices = if self.st.usage.is_some() {
            Some(
                ciallopa_usage::PriceTable::load(&c.usage.prices)
                    .map_err(|e| ApplyError::invalid(format!("usage.prices: {e}")))?,
            )
        } else {
            None
        };
        Ok((c, prices))
    }

    /// Re-read the config file and apply it. `Err` = nothing changed.
    pub async fn reload(
        &self,
        source: &str,
        actor: Option<&str>,
    ) -> Result<LastReload, ApplyError> {
        let _g = self.apply.lock().await;
        let r = self.reload_locked(source, actor);
        if let Err(e) = &r {
            tracing::warn!(source, error = %e.message(), "config reload rejected; running config unchanged");
            lock(&self.status).last = Some(LastReload {
                at_ms: now_ms(),
                source: source.into(),
                ok: false,
                error: Some(e.message()),
                applied: vec![],
            });
        }
        r
    }

    fn reload_locked(&self, source: &str, actor: Option<&str>) -> Result<LastReload, ApplyError> {
        if self.path.is_none() {
            return Err(ApplyError::new(
                StatusCode::CONFLICT,
                "no config file (started with built-in defaults)",
            ));
        }
        let text = self.read_file()?;
        let (new, prices) = self.check(&text)?;
        let cur = self.st.config.load();
        let eff = cs::effective(&self.boot, &new);
        let applied = cs::diff(&cur, &eff);

        // Build first (the only step that can fail), then swap everything.
        self.st
            .upstream
            .reconfigure(&eff)
            .map_err(|e| ApplyError::invalid(format!("proxy_url: {e}")))?;
        if let Some(p) = &self.oauth_pools {
            let mut o = p.options();
            o.proxy_url = eff.effective_proxy();
            o.ipv6 = eff.ipv6;
            let _ = p.reconfigure(o);
        }
        self.st.router.update_config(&eff.routing);
        if let Err(e) = ciallopa_core::logging::set_level(&eff.log.level) {
            tracing::warn!(error = %e, "log level not applied");
        }
        let prices_hash = hash(&std::fs::read(&eff.usage.prices).unwrap_or_default());
        if let (Some(u), Some(p)) = (&self.st.usage, prices) {
            u.set_weights(eff.usage.quota_weights.clone());
            u.set_retention_days(eff.usage.retention_days);
            u.set_prices(p);
        }
        self.st.config.store(eff);

        let pending: Vec<Change> = cs::diff(&self.boot, &new)
            .into_iter()
            .filter(|c| c.effect == Effect::Restart)
            .collect();
        let rep = LastReload {
            at_ms: now_ms(),
            source: source.into(),
            ok: true,
            error: None,
            applied: applied.clone(),
        };
        {
            let mut s = lock(&self.status);
            s.applied_hash = Some(hash(text.as_bytes()));
            s.prices_hash = Some(prices_hash);
            s.pending = pending;
            s.last = Some(rep.clone());
        }
        if !applied.is_empty() {
            tracing::info!(source, changed = applied.len(), "config reloaded");
            self.audit(actor, "reload", source, &applied);
        }
        Ok(rep)
    }

    /// Whether the file (or price table) differs from what is applied (watcher filter).
    fn changed_on_disk(&self) -> bool {
        let s = lock(&self.status);
        let cfg = self
            .path
            .as_ref()
            .map(|p| hash(&std::fs::read(p).unwrap_or_default()));
        let prices = hash(&std::fs::read(&self.st.config.load().usage.prices).unwrap_or_default());
        cfg != s.applied_hash || Some(prices) != s.prices_hash
    }

    pub fn pending(&self) -> Vec<Change> {
        lock(&self.status).pending.clone()
    }

    fn audit(&self, actor: Option<&str>, action: &str, source: &str, changes: &[Change]) {
        let line = json!({
            "ts_ms": now_ms(),
            "actor": actor,
            "action": action,
            "source": source,
            "changes": changes,
        });
        tracing::info!(target: "ciallopa::audit", actor, action, source, changes = %serde_json::to_string(changes).unwrap_or_default(), "settings change");
        if let Some(p) = &self.audit_path {
            use std::io::Write;
            use std::os::unix::fs::OpenOptionsExt;
            let r = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .mode(0o600)
                .open(p)
                .and_then(|mut f| writeln!(f, "{line}"));
            if let Err(e) = r {
                tracing::warn!(error = %e, "audit log write failed");
            }
        }
    }

    /// Account-level audit entry (`changes` already secret-free).
    pub(crate) fn audit_account(
        &self,
        actor: Option<&str>,
        action: &str,
        account: &str,
        detail: Value,
    ) {
        let c = Change {
            key: format!("account:{account}"),
            old: Value::Null,
            new: detail,
            effect: Effect::Hot,
        };
        self.audit(actor, action, "admin", &[c]);
    }

    pub fn audit_tail(&self, limit: usize) -> Vec<Value> {
        let Some(p) = &self.audit_path else {
            return vec![];
        };
        let s = std::fs::read_to_string(p).unwrap_or_default();
        let mut v: Vec<Value> = s
            .lines()
            .rev()
            .take(limit)
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect();
        v.shrink_to_fit();
        v
    }

    // ---------------- accounts ----------------

    /// Forget runtime state of an account that no longer exists.
    pub fn cleanup_account(&self, id: &str) {
        let id = id.to_string();
        self.st.router.forget_account(&id);
        self.st.upstream.pools.remove(&id);
        if let Some(p) = &self.oauth_pools {
            p.remove(&id);
        }
        ciallopa_forward::forget_turn_state(&id);
        self.rc.forget(&id);
        lock(&self.known_accounts).remove(&id);
    }

    /// Re-scan the auth directory; clean up accounts that disappeared. Returns removed ids.
    pub async fn sync_accounts(&self) -> Result<Vec<String>, String> {
        Ok(self.sync_accounts_diff().await?.1)
    }

    /// [`Self::sync_accounts`], returning `(added, removed)` account ids.
    pub async fn sync_accounts_diff(&self) -> Result<(Vec<String>, Vec<String>), String> {
        self.st.accounts.reload().await.map_err(|e| e.to_string())?;
        let now: HashSet<String> = self
            .st
            .accounts
            .list()
            .await
            .into_iter()
            .map(|a| a.id)
            .collect();
        let (gone, mut added): (Vec<String>, Vec<String>) = {
            let k = lock(&self.known_accounts);
            (
                k.difference(&now).cloned().collect(),
                now.difference(&k).cloned().collect(),
            )
        };
        added.sort();
        for id in &gone {
            tracing::info!(account = %id, "account removed; runtime state dropped");
            self.cleanup_account(id);
        }
        *lock(&self.known_accounts) = now;
        Ok((added, gone))
    }

    pub async fn init_known_accounts(&self) {
        let ids = self.st.accounts.list().await.into_iter().map(|a| a.id);
        *lock(&self.known_accounts) = ids.collect();
    }

    // ---------------- write path ----------------

    /// Write `text` atomically (tmp + fsync + rename, permissions kept, previous content in
    /// `<file>.bak`), then reload. A rejected reload restores the previous content.
    async fn write_and_apply(
        &self,
        text: &str,
        expected_version: &str,
        actor: Option<&str>,
        action: &str,
    ) -> Result<LastReload, ApplyError> {
        let path = self
            .path
            .clone()
            .ok_or_else(|| ApplyError::new(StatusCode::CONFLICT, "no config file"))?;
        let _g = self.apply.lock().await;
        let (old, ver) = self.file_version()?;
        if ver != expected_version {
            return Err(ApplyError::new(
                StatusCode::PRECONDITION_FAILED,
                "config file changed since it was read; reload and try again",
            ));
        }
        self.check(text)?;
        let existed = path.exists();
        atomic_write(&path, text.as_bytes(), existed.then_some(old.as_bytes()))
            .map_err(|e| ApplyError::new(StatusCode::INTERNAL_SERVER_ERROR, write_error(&e)))?;
        match self.reload_locked(action, actor) {
            Ok(r) => Ok(r),
            Err(e) => {
                let restore = if existed {
                    atomic_write(&path, old.as_bytes(), None)
                } else {
                    std::fs::remove_file(&path)
                };
                if let Err(re) = restore {
                    tracing::error!(error = %re, "could not restore config after rejected write");
                }
                Err(e)
            }
        }
    }
}

/// Message for a failed config write. A rename that cannot replace the file (busy mount point,
/// read-only or foreign file system) almost always means the file itself is bind-mounted.
pub fn write_error(e: &std::io::Error) -> String {
    const EBUSY: i32 = 16;
    const EXDEV: i32 = 18;
    const EROFS: i32 = 30;
    match e.raw_os_error() {
        Some(EBUSY | EXDEV | EROFS) => format!(
            "write: {e}; the config is saved by renaming a new file over it in its directory: mount the config directory (writable), not the file"
        ),
        _ => format!("write: {e}"),
    }
}

/// Atomic file replace keeping the original mode; `backup` is written to `<path>.bak` first.
pub fn atomic_write(path: &Path, data: &[u8], backup: Option<&[u8]>) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let mode = std::fs::metadata(path)
        .map(|m| m.permissions().mode() & 0o777)
        .unwrap_or(0o600);
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "config.toml".into());
    let dir = path
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if let Some(b) = backup {
        let bak = dir.join(format!("{name}.bak"));
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(mode)
            .open(&bak)?;
        f.write_all(b)?;
        f.sync_all()?;
    }
    let tmp = dir.join(format!(".{name}.tmp-{}", std::process::id()));
    let replace = || -> std::io::Result<()> {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(mode)
            .open(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(mode))?;
        std::fs::rename(&tmp, path)
    };
    if let Err(e) = replace() {
        // e.g. EBUSY when the file itself is a bind mount: don't leave the temp file behind.
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

// ---------------- toml_edit patching ----------------

/// Apply flat `{ "routing.strategy": "fill-first", ... }` changes to the document, keeping
/// comments and layout. `null` removes the key (built-in default). Only editable keys.
pub fn patch_document(text: &str, changes: &Map<String, Value>) -> Result<String, ApplyError> {
    let mut doc: toml_edit::DocumentMut = text
        .parse()
        .map_err(|e: toml_edit::TomlError| ApplyError::invalid(format!("config file: {e}")))?;
    let mut errors = vec![];
    for (k, v) in changes {
        let Some(f) = cs::field(k) else {
            errors.push(json!({"key": k, "message": "unknown key"}));
            continue;
        };
        if !f.editable {
            errors.push(json!({"key": k, "message": "not editable from the management page"}));
            continue;
        }
        let parts: Vec<&str> = k.split('.').collect();
        if v.is_null() {
            remove_path(doc.as_table_mut(), &parts);
            continue;
        }
        let nv: toml_edit::Value = match (f.kind, v) {
            ("bool", Value::Bool(b)) => (*b).into(),
            ("int", Value::Number(n)) if n.as_i64().is_some_and(|x| x >= 0) => {
                n.as_i64().unwrap_or_default().into()
            }
            ("float", Value::Number(n)) if n.as_f64().is_some() => {
                n.as_f64().unwrap_or_default().into()
            }
            ("string" | "duration", Value::String(s)) => s.as_str().into(),
            ("enum", Value::String(s)) if f.options.contains(&s.as_str()) => s.as_str().into(),
            _ => {
                errors.push(json!({"key": k, "message": format!("expected {}", f.kind)}));
                continue;
            }
        };
        set_path(doc.as_table_mut(), &parts, nv);
    }
    if !errors.is_empty() {
        return Err(ApplyError {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            errors,
        });
    }
    Ok(doc.to_string())
}

fn set_path(t: &mut toml_edit::Table, parts: &[&str], mut v: toml_edit::Value) {
    let (last, head) = match parts.split_last() {
        Some(x) => x,
        None => return,
    };
    let mut cur: &mut dyn toml_edit::TableLike = t;
    for p in head {
        if cur.get(p).is_none() {
            let mut nt = toml_edit::Table::new();
            nt.set_implicit(true);
            cur.insert(p, toml_edit::Item::Table(nt));
        }
        match cur.get_mut(p).and_then(|i| i.as_table_like_mut()) {
            Some(n) => cur = n,
            None => return,
        }
    }
    // Keep the value's own decoration (e.g. a trailing `# comment`).
    match cur.get_mut(last) {
        // In place: keeps the key's leading comments and the value's trailing comment.
        Some(item) => {
            if let Some(old) = item.as_value() {
                *v.decor_mut() = old.decor().clone();
            }
            *item = toml_edit::Item::Value(v);
        }
        None => {
            cur.insert(last, toml_edit::Item::Value(v));
        }
    }
}

fn remove_path(t: &mut toml_edit::Table, parts: &[&str]) {
    let (last, head) = match parts.split_last() {
        Some(x) => x,
        None => return,
    };
    let mut cur: &mut dyn toml_edit::TableLike = t;
    for p in head {
        match cur.get_mut(p).and_then(|i| i.as_table_like_mut()) {
            Some(n) => cur = n,
            None => return,
        }
    }
    cur.remove(last);
}

/// Append `{ label, key }` to `api_keys` (array or array of tables).
pub fn add_key(text: &str, label: &str, key: &str) -> Result<String, ApplyError> {
    let mut doc: toml_edit::DocumentMut = text
        .parse()
        .map_err(|e: toml_edit::TomlError| ApplyError::invalid(format!("config file: {e}")))?;
    match doc.get_mut("api_keys") {
        Some(toml_edit::Item::ArrayOfTables(a)) => {
            let mut t = toml_edit::Table::new();
            t.insert("label", toml_edit::value(label));
            t.insert("key", toml_edit::value(key));
            a.push(t);
        }
        Some(toml_edit::Item::Value(toml_edit::Value::Array(a))) => {
            let mut t = toml_edit::InlineTable::new();
            t.insert("label", label.into());
            t.insert("key", key.into());
            a.push(t);
        }
        Some(_) => return Err(ApplyError::invalid("api_keys has an unexpected type")),
        None => {
            let mut a = toml_edit::Array::new();
            let mut t = toml_edit::InlineTable::new();
            t.insert("label", label.into());
            t.insert("key", key.into());
            a.push(t);
            // Root key/values are always rendered before the first table.
            doc.as_table_mut().insert("api_keys", toml_edit::value(a));
        }
    }
    Ok(doc.to_string())
}

/// Remove the `api_keys` entry whose resolved label is `label`.
pub fn remove_key(text: &str, label: &str) -> Result<String, ApplyError> {
    let mut doc: toml_edit::DocumentMut = text
        .parse()
        .map_err(|e: toml_edit::TomlError| ApplyError::invalid(format!("config file: {e}")))?;
    let matches = |key: Option<&str>, lab: Option<&str>| -> bool {
        match (lab, key) {
            (Some(l), _) => l.trim() == label,
            (None, Some(k)) => ciallopa_core::config::default_key_label(k) == label,
            _ => false,
        }
    };
    let found = match doc.get_mut("api_keys") {
        Some(toml_edit::Item::ArrayOfTables(a)) => {
            let i = a.iter().position(|t| {
                matches(
                    t.get("key").and_then(|v| v.as_str()),
                    t.get("label").and_then(|v| v.as_str()),
                )
            });
            i.map(|i| a.remove(i)).is_some()
        }
        Some(toml_edit::Item::Value(toml_edit::Value::Array(a))) => {
            let i = a.iter().position(|v| match v {
                toml_edit::Value::String(s) => matches(Some(s.value()), None),
                toml_edit::Value::InlineTable(t) => matches(
                    t.get("key").and_then(|v| v.as_str()),
                    t.get("label").and_then(|v| v.as_str()),
                ),
                _ => false,
            });
            i.map(|i| a.remove(i)).is_some()
        }
        _ => false,
    };
    if !found {
        return Err(ApplyError::new(StatusCode::NOT_FOUND, "unknown key label"));
    }
    Ok(doc.to_string())
}

// ---------------- watcher / signals ----------------

/// Watch the config file, the price table and the auth directory (debounced). Config / price
/// changes go through [`Settings::reload`] unless the content equals what is applied (own
/// writes, touch); auth directory changes re-scan accounts.
pub fn spawn_watcher(s: Arc<Settings>) -> Option<notify::RecommendedWatcher> {
    use notify::{RecursiveMode, Watcher};
    let cfg = s.st.config.load();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<PathBuf>>();
    let mut w = match notify::recommended_watcher(move |r: notify::Result<notify::Event>| {
        if let Ok(e) = r {
            if !matches!(e.kind, notify::EventKind::Access(_)) {
                let _ = tx.send(e.paths);
            }
        }
    }) {
        Ok(w) => w,
        Err(e) => {
            tracing::warn!(error = %e, "file watcher unavailable; use SIGHUP or the reload API");
            return None;
        }
    };
    let parent = |p: &Path| {
        p.parent()
            .filter(|d| !d.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    };
    let mut dirs: Vec<PathBuf> = vec![];
    if let Some(p) = s.path() {
        dirs.push(parent(p));
    }
    dirs.push(parent(&cfg.usage.prices));
    for d in dirs.iter().collect::<HashSet<_>>() {
        if let Err(e) = w.watch(d, RecursiveMode::NonRecursive) {
            tracing::debug!(dir = %d.display(), error = %e, "not watching");
        }
    }
    let auth_dir = cfg.auth_dir.clone();
    if let Err(e) = w.watch(&auth_dir, RecursiveMode::NonRecursive) {
        tracing::debug!(dir = %auth_dir.display(), error = %e, "not watching auth dir");
    }
    let names: Vec<PathBuf> = s
        .path()
        .into_iter()
        .map(Path::to_path_buf)
        .chain([cfg.usage.prices.clone()])
        .collect();
    tokio::spawn(async move {
        while let Some(first) = rx.recv().await {
            let mut paths = first;
            let deadline = tokio::time::sleep(DEBOUNCE);
            tokio::pin!(deadline);
            loop {
                tokio::select! {
                    _ = &mut deadline => break,
                    m = rx.recv() => match m { Some(p) => paths.extend(p), None => return },
                }
            }
            let cfg_hit = paths
                .iter()
                .any(|p| names.iter().any(|n| p.file_name() == n.file_name()));
            let auth_hit = paths.iter().any(|p| {
                p.parent() == Some(auth_dir.as_path())
                    && p.extension().and_then(|x| x.to_str()) == Some("json")
                    && !p
                        .file_name()
                        .is_some_and(|n| n.to_string_lossy().starts_with('.'))
            });
            if cfg_hit && s.changed_on_disk() {
                let _ = s.reload("file", None).await;
            }
            if auth_hit {
                if let Err(e) = s.sync_accounts().await {
                    tracing::warn!(error = %e, "auth dir rescan failed");
                }
            }
        }
    });
    Some(w)
}

/// `SIGHUP` → reload.
pub fn spawn_sighup(s: Arc<Settings>) {
    #[cfg(unix)]
    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};
        let Ok(mut sig) = signal(SignalKind::hangup()) else {
            return;
        };
        while sig.recv().await.is_some() {
            let _ = s.reload("sighup", None).await;
            let _ = s.sync_accounts().await;
        }
    });
}

// ---------------- HTTP API ----------------

pub(crate) fn err(status: StatusCode, msg: &str) -> Response {
    (status, Json(json!({"error": {"message": msg}}))).into_response()
}

/// Same CSRF rules as the reset-credit consume action: key in a header (checked by the
/// middleware), JSON body (forces a CORS preflight), no cookies.
/// `Some(rejection)` when the request does not follow them.
pub(crate) fn csrf(headers: &HeaderMap) -> Option<Response> {
    let json_ct = headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.to_ascii_lowercase().starts_with("application/json"));
    if !json_ct || headers.contains_key(http::header::COOKIE) {
        return Some(err(
            StatusCode::BAD_REQUEST,
            "send JSON with Authorization: Bearer (no cookies)",
        ));
    }
    None
}

impl Settings {
    /// Write actions need `[ui] allow_config_write = true` (file only) + the CSRF rules.
    pub(crate) fn write_guard(&self, headers: &HeaderMap) -> Option<Response> {
        if !self.st.config.load().ui.allow_config_write {
            return Some(err(
                StatusCode::FORBIDDEN,
                "changes from the management page are disabled; set [ui] allow_config_write = true in the config file",
            ));
        }
        csrf(headers)
    }
}

pub(crate) fn actor(k: &Option<axum::Extension<ClientKey>>) -> Option<String> {
    k.as_ref().map(|k| k.0.label.to_string())
}

fn if_match(h: &HeaderMap) -> Option<String> {
    h.get(http::header::IF_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.trim()
                .trim_start_matches("W/")
                .trim_matches('"')
                .to_string()
        })
}

fn with_etag(mut r: Response, v: &str) -> Response {
    if let Ok(h) = HeaderValue::from_str(&format!("\"{v}\"")) {
        r.headers_mut().insert(http::header::ETAG, h);
    }
    r
}

type S = Arc<Settings>;

pub fn routes(s: S) -> Router {
    Router::new()
        .route("/admin/v1/config", get(get_config).put(put_config))
        .route("/admin/v1/config/validate", post(validate))
        .route("/admin/v1/config/reload", post(reload_api))
        .route("/admin/v1/config/keys", post(new_key))
        .route("/admin/v1/config/keys/{label}", delete(delete_key))
        .route("/admin/v1/config/audit", get(audit))
        .route("/admin/v1/proxy/test", post(proxy_test))
        .route("/admin/v1/prices", get(prices_status))
        .route("/admin/v1/prices/check", post(prices_check))
        .route("/admin/v1/prices/apply", post(prices_apply))
        .with_state(s)
}

impl Settings {
    /// Price table facts for the management page: entries, file time, source and the models
    /// used in the last 30 days that have no (standard) price.
    pub async fn prices_status(&self) -> Value {
        let cfg = self.st.config.load();
        let path = &cfg.usage.prices;
        let updated_ms = std::fs::metadata(path)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64);
        let (models, unpriced) = match &self.st.usage {
            Some(u) => {
                let t = u.prices();
                let now = now_ms();
                let one = ciallopa_usage::prices::Usage {
                    input: 1,
                    cached: 0,
                    cache_write: 0,
                    output: 1,
                };
                let used = u
                    .models_since(now - UNPRICED_WINDOW_MS)
                    .await
                    .unwrap_or_default();
                let unpriced: Vec<String> = used
                    .into_iter()
                    .filter(|m| {
                        t.cost(Some(m), ciallopa_usage::Tier::Standard, now, one)
                            .is_none()
                    })
                    .collect();
                (t.models.len(), unpriced)
            }
            None => (0, vec![]),
        };
        let src = cfg.usage.prices_source.trim();
        json!({
            "usage_enabled": self.st.usage.is_some(),
            "path": path.display().to_string(),
            "source": src,
            "enabled": !src.is_empty(),
            "models": models,
            "updated_ms": updated_ms.filter(|_| models > 0),
            "unpriced": unpriced,
        })
    }
}

fn price_error(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, Json(json!({"error": {"message": msg.into()}}))).into_response()
}

async fn prices_status(State(s): State<S>) -> Response {
    Json(s.prices_status().await).into_response()
}

/// 「检查更新」: download the dataset and diff it against the table. Writes nothing.
async fn prices_check(State(s): State<S>) -> Response {
    if s.st.usage.is_none() {
        return price_error(
            StatusCode::CONFLICT,
            "usage log disabled: no costs to price",
        );
    }
    let cfg = s.st.config.load();
    let (source, remote) = match crate::prices::fetch_entries(&cfg, "").await {
        Ok(x) => x,
        Err(e) => return price_error(StatusCode::BAD_GATEWAY, e.to_string()),
    };
    let path = cfg.usage.prices.clone();
    let local_hash = crate::prices::file_hash(&path);
    let local = match ciallopa_usage::PriceTable::load(&path) {
        Ok(t) => t,
        Err(e) => {
            return price_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("usage.prices: {e}"),
            )
        }
    };
    let changes = ciallopa_usage::prices::diff(&local, &remote);
    let n =
        |k: fn(&ciallopa_usage::prices::Change) -> bool| changes.iter().filter(|c| k(c)).count();
    let added = n(|c| matches!(c, ciallopa_usage::prices::Change::Add(_)));
    let updated = n(|c| matches!(c, ciallopa_usage::prices::Change::Update { .. }));
    let token = (added + updated > 0).then(|| {
        let b: [u8; 12] = rand::random();
        b.iter().map(|x| format!("{x:02x}")).collect::<String>()
    });
    let shown: Vec<Value> = changes.iter().map(crate::prices::change_json).collect();
    *lock(&s.price_check) = token.as_ref().map(|t| PriceCheck {
        token: t.clone(),
        source: source.clone(),
        changes,
        local_hash,
        at: std::time::Instant::now(),
    });
    Json(json!({
        "source": source,
        "remote_models": remote.len(),
        "added": added,
        "updated": updated,
        "token": token,
        "changes": shown,
    }))
    .into_response()
}

#[derive(Deserialize)]
struct ApplyPrices {
    token: String,
}

/// 「应用」: write the checked changes (previous file → `.toml.bak`) and use them at once.
async fn prices_apply(
    State(s): State<S>,
    key: Option<axum::Extension<ClientKey>>,
    Json(req): Json<ApplyPrices>,
) -> Response {
    let _g = s.apply.lock().await;
    let Some(chk) = lock(&s.price_check).take() else {
        return price_error(
            StatusCode::CONFLICT,
            "no pending price check: run 检查更新 again",
        );
    };
    if chk.token != req.token || chk.at.elapsed() > PRICE_CHECK_TTL {
        return price_error(
            StatusCode::CONFLICT,
            "the price check expired: run 检查更新 again",
        );
    }
    let path = s.st.config.load().usage.prices.clone();
    if crate::prices::file_hash(&path) != chk.local_hash {
        return price_error(
            StatusCode::CONFLICT,
            "the price table changed after the check: run 检查更新 again",
        );
    }
    let local = match ciallopa_usage::PriceTable::load(&path) {
        Ok(t) => t,
        Err(e) => {
            return price_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("usage.prices: {e}"),
            )
        }
    };
    let (table, bak) = match crate::prices::write_table(&path, &local, &chk.changes, &chk.source) {
        Ok(x) => x,
        Err(e) => {
            return price_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("write {}: {e}", path.display()),
            )
        }
    };
    let applied = chk.changes.iter().filter(|c| c.actionable()).count();
    let models = table.models.len();
    if let Some(u) = &s.st.usage {
        u.set_prices(table);
    }
    // Own write: the file watcher must not reload it again.
    lock(&s.status).prices_hash = Some(crate::prices::file_hash(&path));
    tracing::info!(
        actor = key.as_ref().map(|k| k.0.label.to_string()).unwrap_or_default(),
        applied,
        models,
        path = %path.display(),
        "price table updated"
    );
    Json(json!({
        "applied": applied,
        "models": models,
        "backup": bak.map(|b| b.display().to_string()),
    }))
    .into_response()
}

async fn get_config(State(s): State<S>) -> Response {
    let (text, version) = match s.file_version() {
        Ok(x) => x,
        Err(e) => return e.into_response(),
    };
    let running = s.st.config.load();
    let (file_cfg, file_error) = match Config::parse(&text) {
        Ok(c) => (Some(c), None),
        Err(e) => (None, Some(e.to_string())),
    };
    let last = lock(&s.status).last.clone();
    let body = json!({
        "version": version,
        "path": s.path().map(|p| p.display().to_string()),
        "write_enabled": running.ui.allow_config_write && s.path().is_some(),
        "values": file_cfg.as_ref().map(cs::masked_values),
        "running": cs::masked_values(&running),
        "file_error": file_error,
        "schema": cs::SCHEMA,
        "defaults": cs::masked_values(&Config::default()),
        "pending_restart": s.pending(),
        "last_reload": last,
        "overrides": {"log.level": std::env::var_os("RUST_LOG").is_some().then_some("RUST_LOG")},
        "egress": {"proxy": cs::mask_proxy(&running.effective_proxy()), "direct": running.effective_proxy().is_empty()},
        "runtime": {
            "inflight": s.st.upstream.drain.inflight(),
            "limit": running.max_inflight_requests,
            "rejected": s.st.upstream.drain.rejected(),
            "ws_connections": s.st.upstream.drain.active(),
            "draining": s.st.upstream.drain.phase() != ciallopa_upstream::drain::Phase::Running,
        },
    });
    with_etag(Json(body).into_response(), &version)
}

#[derive(Deserialize, Default)]
struct ProxyTestReq {
    /// Candidate address (may be unsaved). Absent = the saved `proxy_url`; the masked form of
    /// the saved one (`user:***@…`) also means the saved one. `""` = direct.
    proxy_url: Option<String>,
}

/// Egress check: `GET <upstream origin>/cdn-cgi/trace` through the candidate proxy with a
/// fresh client: no account, no credentials, no cookies, not the account pools.
async fn proxy_test(
    State(s): State<S>,
    headers: HeaderMap,
    body: Option<Json<ProxyTestReq>>,
) -> Response {
    if let Some(r) = csrf(&headers) {
        return r;
    }
    let req = body.map(|b| b.0).unwrap_or_default();
    let saved = s
        .read_file()
        .ok()
        .and_then(|t| Config::parse(&t).ok())
        .unwrap_or_else(|| Config::clone(&s.st.config.load()))
        .proxy_url;
    let url = match req.proxy_url {
        None => saved,
        Some(u) if u.trim() == cs::mask_proxy(&saved) => saved,
        Some(u) => u.trim().to_string(),
    };
    let running = s.st.config.load();
    let mut probe = Config::clone(&running);
    probe.proxy_url = url.clone();
    probe.proxy_enabled = true;
    if let Err(e) = probe.validate() {
        return err(StatusCode::UNPROCESSABLE_ENTITY, &e.to_string());
    }
    let opts = ciallopa_upstream::ClientOptions {
        proxy_url: probe.effective_proxy(),
        connect_timeout: Some(Duration::from_secs(
            running.upstream_opts.connect_timeout_secs.clamp(1, 15),
        )),
        cloudflare_cookies: false,
        ipv6: running.ipv6,
    };
    let client = match ciallopa_upstream::build_reqwest(&opts) {
        Ok(c) => c,
        Err(e) => return err(StatusCode::UNPROCESSABLE_ENTITY, &format!("proxy_url: {e}")),
    };
    let target = match reqwest::Url::parse(&running.upstream) {
        Ok(mut u) => {
            u.set_path("/cdn-cgi/trace");
            u.set_query(None);
            u.set_fragment(None);
            u
        }
        Err(e) => return err(StatusCode::UNPROCESSABLE_ENTITY, &format!("upstream: {e}")),
    };
    let base = json!({
        "proxy": cs::mask_proxy(&opts.proxy_url),
        "direct": opts.proxy_url.is_empty(),
        "target": target.as_str(),
    });
    let t0 = std::time::Instant::now();
    let res = client
        .get(target.clone())
        .timeout(Duration::from_secs(20))
        .send()
        .await;
    let mut out = base.as_object().cloned().unwrap_or_default();
    match res {
        Ok(r) => {
            let status = r.status();
            let text = r.text().await.unwrap_or_default();
            out.insert("latency_ms".into(), json!(t0.elapsed().as_millis() as u64));
            out.insert("status".into(), json!(status.as_u16()));
            let kv: Map<String, Value> = text
                .lines()
                .filter_map(|l| l.split_once('='))
                .filter(|(k, _)| matches!(*k, "ip" | "loc" | "colo" | "http" | "tls"))
                .map(|(k, v)| (k.to_string(), json!(v.trim())))
                .collect();
            let ok = status.is_success() && kv.contains_key("ip");
            out.insert("ok".into(), json!(ok));
            out.extend(kv);
            if !ok {
                out.insert(
                    "error".into(),
                    json!(format!("unexpected response: HTTP {}", status.as_u16())),
                );
            }
        }
        Err(e) => {
            out.insert("ok".into(), json!(false));
            out.insert("latency_ms".into(), json!(t0.elapsed().as_millis() as u64));
            let stage = if e.is_timeout() {
                "timeout"
            } else if e.is_connect() {
                "connect"
            } else {
                "transport"
            };
            out.insert("stage".into(), json!(stage));
            out.insert("error".into(), json!(error_chain(&e)));
        }
    }
    Json(Value::Object(out)).into_response()
}

fn error_chain(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut cur = e.source();
    while let Some(c) = cur {
        let m = c.to_string();
        if !out.ends_with(&m) {
            out.push_str(": ");
            out.push_str(&m);
        }
        cur = c.source();
    }
    out
}

#[derive(Deserialize)]
struct ChangesReq {
    #[serde(default)]
    changes: Map<String, Value>,
}

/// Dry run: what a PUT with these changes would do. Never writes.
async fn validate(
    State(s): State<S>,
    headers: HeaderMap,
    body: Option<Json<ChangesReq>>,
) -> Response {
    if let Some(r) = csrf(&headers) {
        return r;
    }
    let Some(Json(req)) = body else {
        return err(
            StatusCode::BAD_REQUEST,
            "body must be JSON {\"changes\": {...}}",
        );
    };
    let (text, version) = match s.file_version() {
        Ok(x) => x,
        Err(e) => return e.into_response(),
    };
    let cur = match Config::parse(&text) {
        Ok(c) => c,
        Err(e) => return ApplyError::invalid(format!("current config file: {e}")).into_response(),
    };
    let out = patch_document(&text, &req.changes).and_then(|t| s.check(&t).map(|(c, _)| c));
    match out {
        Ok(new) => Json(
            json!({"ok": true, "version": version, "errors": [], "diff": cs::diff(&cur, &new)}),
        )
        .into_response(),
        Err(e) => (
            StatusCode::OK,
            Json(json!({"ok": false, "version": version, "errors": e.errors, "diff": []})),
        )
            .into_response(),
    }
}

async fn put_config(
    State(s): State<S>,
    key: Option<axum::Extension<ClientKey>>,
    headers: HeaderMap,
    body: Option<Json<ChangesReq>>,
) -> Response {
    if let Some(r) = s.write_guard(&headers) {
        return r;
    }
    let Some(expected) = if_match(&headers) else {
        return err(
            StatusCode::PRECONDITION_REQUIRED,
            "If-Match: \"<version>\" is required",
        );
    };
    let Some(Json(req)) = body else {
        return err(
            StatusCode::BAD_REQUEST,
            "body must be JSON {\"changes\": {...}}",
        );
    };
    if req.changes.is_empty() {
        return err(StatusCode::BAD_REQUEST, "no changes");
    }
    let (text, _) = match s.file_version() {
        Ok(x) => x,
        Err(e) => return e.into_response(),
    };
    let new_text = match patch_document(&text, &req.changes) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let actor = actor(&key);
    match s
        .write_and_apply(&new_text, &expected, actor.as_deref(), "admin")
        .await
    {
        Ok(r) => after_write(&s, r),
        Err(e) => e.into_response(),
    }
}

fn after_write(s: &Settings, r: LastReload) -> Response {
    let version = s.file_version().map(|x| x.1).unwrap_or_default();
    with_etag(
        Json(json!({"ok": true, "version": version, "applied": r.applied, "pending_restart": s.pending()}))
            .into_response(),
        &version,
    )
}

async fn reload_api(
    State(s): State<S>,
    key: Option<axum::Extension<ClientKey>>,
    headers: HeaderMap,
) -> Response {
    if let Some(r) = csrf(&headers) {
        return r;
    }
    let actor = actor(&key);
    let r = s.reload("api", actor.as_deref()).await;
    let _ = s.sync_accounts().await;
    match r {
        Ok(r) => after_write(&s, r),
        Err(e) => e.into_response(),
    }
}

#[derive(Deserialize)]
struct NewKeyReq {
    label: String,
}

pub(crate) fn gen_key() -> String {
    let b: [u8; 24] = rand::random();
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!("sk-cpa-{h}")
}

/// Server-generated key; the plaintext is returned exactly once.
async fn new_key(
    State(s): State<S>,
    key: Option<axum::Extension<ClientKey>>,
    headers: HeaderMap,
    body: Option<Json<NewKeyReq>>,
) -> Response {
    if let Some(r) = s.write_guard(&headers) {
        return r;
    }
    let Some(expected) = if_match(&headers) else {
        return err(
            StatusCode::PRECONDITION_REQUIRED,
            "If-Match: \"<version>\" is required",
        );
    };
    let Some(Json(req)) = body else {
        return err(
            StatusCode::BAD_REQUEST,
            "body must be JSON {\"label\": \"...\"}",
        );
    };
    let label = req.label.trim().to_string();
    let ok = !label.is_empty()
        && label.len() <= 64
        && label
            .chars()
            .all(|c| c.is_alphanumeric() || "-_.@".contains(c));
    if !ok {
        return err(
            StatusCode::BAD_REQUEST,
            "label: 1-64 letters, digits, - _ . @",
        );
    }
    if s.st.config.keys().iter().any(|k| k.label == label) {
        return err(StatusCode::CONFLICT, "label already exists");
    }
    let (text, _) = match s.file_version() {
        Ok(x) => x,
        Err(e) => return e.into_response(),
    };
    let new = gen_key();
    let new_text = match add_key(&text, &label, &new) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let actor = actor(&key);
    match s
        .write_and_apply(&new_text, &expected, actor.as_deref(), "admin:key-add")
        .await
    {
        Ok(_) => {
            let version = s.file_version().map(|x| x.1).unwrap_or_default();
            with_etag(
                Json(json!({"ok": true, "label": label, "key": new, "version": version}))
                    .into_response(),
                &version,
            )
        }
        Err(e) => e.into_response(),
    }
}

async fn delete_key(
    State(s): State<S>,
    key: Option<axum::Extension<ClientKey>>,
    AxPath(label): AxPath<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(r) = s.write_guard(&headers) {
        return r;
    }
    let Some(expected) = if_match(&headers) else {
        return err(
            StatusCode::PRECONDITION_REQUIRED,
            "If-Match: \"<version>\" is required",
        );
    };
    let actor = actor(&key);
    if actor.as_deref() == Some(label.as_str()) {
        return err(
            StatusCode::BAD_REQUEST,
            "cannot delete the key used for this session",
        );
    }
    let (text, _) = match s.file_version() {
        Ok(x) => x,
        Err(e) => return e.into_response(),
    };
    let new_text = match remove_key(&text, &label) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    match s
        .write_and_apply(&new_text, &expected, actor.as_deref(), "admin:key-delete")
        .await
    {
        Ok(r) => after_write(&s, r),
        Err(e) => e.into_response(),
    }
}

#[derive(Deserialize)]
struct AuditQ {
    limit: Option<usize>,
}

async fn audit(State(s): State<S>, Query(q): Query<AuditQ>) -> Response {
    Json(json!({"entries": s.audit_tail(q.limit.unwrap_or(50).min(500))})).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_error_hints_at_directory_mount() {
        for code in [16, 18, 30] {
            let m = write_error(&std::io::Error::from_raw_os_error(code));
            assert!(m.contains("mount the config directory"), "{m}");
        }
        let m = write_error(&std::io::Error::from_raw_os_error(13));
        assert!(!m.contains("mount"), "{m}");
    }

    #[test]
    fn atomic_write_failed_rename_leaves_no_temp_file() {
        let d = tempfile::tempdir().unwrap();
        // A non-empty directory in place of the file: the final rename fails.
        let target = d.path().join("config.toml");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("keep"), b"x").unwrap();
        assert!(atomic_write(&target, b"new", None).is_err());
        let left: Vec<_> = std::fs::read_dir(d.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(left, vec!["config.toml".to_string()], "{left:?}");
    }

    #[test]
    fn patch_keeps_comments_and_layout() {
        let src = "# top comment\nlisten = \"127.0.0.1:8317\"\n\n[routing]\n# strategy comment\nstrategy = \"round-robin\"   # inline\nsession_affinity = true\n";
        let mut ch = Map::new();
        ch.insert("routing.strategy".into(), json!("fill-first"));
        ch.insert("usage.quota_weights.output".into(), json!(3.5));
        let out = patch_document(src, &ch).unwrap();
        assert!(out.contains("# top comment"));
        assert!(out.contains("# strategy comment"));
        assert!(
            out.contains("strategy = \"fill-first\"   # inline"),
            "{out}"
        );
        let c = Config::parse(&out).unwrap();
        assert_eq!(
            c.routing.strategy,
            ciallopa_core::config::Strategy::FillFirst
        );
        assert_eq!(c.usage.quota_weights.output, 3.5);
        // not editable / wrong type
        let mut bad = Map::new();
        bad.insert("listen".into(), json!("0.0.0.0:1"));
        bad.insert("ui.allow_config_write".into(), json!(true));
        bad.insert("routing.session_affinity".into(), json!("yes"));
        assert_eq!(patch_document(src, &bad).unwrap_err().errors.len(), 3);
        // null = back to default
        let mut rm = Map::new();
        rm.insert("routing.session_affinity".into(), Value::Null);
        assert!(!patch_document(src, &rm)
            .unwrap()
            .contains("session_affinity"));
    }

    #[test]
    fn keys_add_remove_all_layouts() {
        for src in [
            "api_keys = [\"sk-plain-0123456789abcdef\"]\n",
            "[[api_keys]]\nlabel = \"a\"\nkey = \"sk-a-0123456789abcdef\"\n",
            "listen = \"127.0.0.1:1\"\n[routing]\nstrategy = \"fill-first\"\n",
        ] {
            let t = add_key(src, "new", "sk-cpa-new-0123456789abcdef").unwrap();
            let c = Config::parse(&t).unwrap();
            assert!(c.keys().iter().any(|k| k.label == "new"), "{t}");
            let t2 = remove_key(&t, "new").unwrap();
            let c2 = Config::parse(&t2).unwrap();
            assert!(!c2.keys().iter().any(|k| k.label == "new"));
            assert_eq!(
                c2.routing.strategy,
                Config::parse(src).unwrap().routing.strategy
            );
        }
        let lab = ciallopa_core::config::default_key_label("sk-plain-0123456789abcdef");
        let t = remove_key("api_keys = [\"sk-plain-0123456789abcdef\"]\n", &lab).unwrap();
        assert!(Config::parse(&t).unwrap().keys().is_empty());
        assert!(remove_key("api_keys = []\n", "x").is_err());
    }
}
