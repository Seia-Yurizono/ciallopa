//! Account id aliases for write-time redaction.
//!
//! Internal routing still uses the real [`crate::types::AccountId`]. At every process
//! boundary that persists or logs an account (usage DB labels, attempt chains, audit,
//! affinity file, stderr), convert with [`Aliaser::alias`] so emails embedded in ids
//! never land on disk.
use base64::Engine;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

/// Prefix of every alias (`acct_` + 8 hex chars, optional `-N` on collision).
pub const ALIAS_PREFIX: &str = "acct_";
const KEY_LEN: usize = 32;
const HEX_LEN: usize = 8; // 32-bit

/// Process-wide aliaser installed at startup so log sites can call [`a`] without plumbing.
static CURRENT: OnceLock<Arc<Aliaser>> = OnceLock::new();

/// Install the process-wide aliaser (first call wins; later calls are ignored).
pub fn set_current(aliaser: Arc<Aliaser>) {
    let _ = CURRENT.set(aliaser);
}

/// Alias for logs / opportunistic call sites. Without a current aliaser, returns `id` as-is.
pub fn a(id: &str) -> String {
    match CURRENT.get() {
        Some(al) => al.alias(id),
        None => id.to_string(),
    }
}

/// Current process aliaser, if installed.
pub fn current() -> Option<Arc<Aliaser>> {
    CURRENT.get().cloned()
}

/// Stable, non-reversible account label written to disk and logs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AccountAlias(String);

impl AccountAlias {
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Display for AccountAlias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for AccountAlias {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// HMAC-SHA256 account aliaser. Key is 32 random bytes, usually persisted in usage kv.
pub struct Aliaser {
    key: [u8; KEY_LEN],
    /// account_id → final alias (includes collision suffix).
    forward: RwLock<HashMap<String, String>>,
    /// alias → account_id for currently registered accounts.
    reverse: RwLock<HashMap<String, String>>,
    /// base alias (no suffix) → how many distinct ids claimed it (for -2, -3, …).
    collisions: Mutex<HashMap<String, Vec<String>>>,
}

impl Aliaser {
    /// Build from a raw 32-byte key.
    pub fn from_key(key: [u8; KEY_LEN]) -> Arc<Self> {
        Arc::new(Self {
            key,
            forward: RwLock::new(HashMap::new()),
            reverse: RwLock::new(HashMap::new()),
            collisions: Mutex::new(HashMap::new()),
        })
    }

    /// In-process random key (usage disabled: aliases only need to be stable for this run).
    pub fn ephemeral() -> Arc<Self> {
        let mut key = [0u8; KEY_LEN];
        rand::RngCore::fill_bytes(&mut rand::rng(), &mut key);
        Self::from_key(key)
    }

    /// Decode a standard-base64 key (as stored in kv).
    pub fn from_b64(s: &str) -> Result<Arc<Self>, String> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(s.trim())
            .map_err(|e| format!("alias_key base64: {e}"))?;
        if bytes.len() != KEY_LEN {
            return Err(format!(
                "alias_key length {} (want {KEY_LEN})",
                bytes.len()
            ));
        }
        let mut key = [0u8; KEY_LEN];
        key.copy_from_slice(&bytes);
        Ok(Self::from_key(key))
    }

    pub fn key_b64(&self) -> String {
        base64::engine::general_purpose::STANDARD.encode(self.key)
    }

    pub fn key_bytes(&self) -> [u8; KEY_LEN] {
        self.key
    }

    /// Register currently loaded account ids so [`resolve`] works and collisions are stable.
    pub fn register(&self, account_id: &str) {
        let _ = self.alias(account_id);
    }

    pub fn register_many<I, S>(&self, ids: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for id in ids {
            self.register(id.as_ref());
        }
    }

    /// True when `s` already looks like an alias (`acct_` + hex…), including collision suffixes.
    pub fn looks_like_alias(s: &str) -> bool {
        let rest = match s.strip_prefix(ALIAS_PREFIX) {
            Some(r) => r,
            None => return false,
        };
        // acct_ + 8 hex, optional -N
        let (hex, suf) = match rest.split_once('-') {
            Some((h, n)) => (h, Some(n)),
            None => (rest, None),
        };
        if hex.len() != HEX_LEN || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return false;
        }
        match suf {
            None => true,
            Some(n) => !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()),
        }
    }

    /// Alias for `account_id`. Idempotent: already-aliased strings pass through.
    pub fn alias(&self, account_id: &str) -> String {
        if account_id.is_empty() {
            return String::new();
        }
        if Self::looks_like_alias(account_id) {
            return account_id.to_string();
        }
        if let Ok(fwd) = self.forward.read() {
            if let Some(a) = fwd.get(account_id) {
                return a.clone();
            }
        }
        let base = self.base_alias(account_id);
        let assigned = {
            let mut cols = self
                .collisions
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let list = cols.entry(base.clone()).or_default();
            if let Some(pos) = list.iter().position(|id| id == account_id) {
                if pos == 0 {
                    base.clone()
                } else {
                    format!("{base}-{}", pos + 1)
                }
            } else {
                list.push(account_id.to_string());
                if list.len() == 1 {
                    base.clone()
                } else {
                    format!("{base}-{}", list.len())
                }
            }
        };
        if let Ok(mut fwd) = self.forward.write() {
            fwd.insert(account_id.to_string(), assigned.clone());
        }
        if let Ok(mut rev) = self.reverse.write() {
            rev.insert(assigned.clone(), account_id.to_string());
        }
        assigned
    }

    /// Reverse lookup among **currently registered** accounts only.
    pub fn resolve(&self, alias: &str) -> Option<String> {
        self.reverse
            .read()
            .ok()
            .and_then(|m| m.get(alias).cloned())
    }

    /// For affinity load: resolve an alias, or keep a raw (pre-migration) id as-is.
    pub fn resolve_binding(&self, stored: &str) -> String {
        if Self::looks_like_alias(stored) {
            self.resolve(stored).unwrap_or_else(|| stored.to_string())
        } else {
            stored.to_string()
        }
    }

    /// Map a query/filter value that may be a raw id, an email-bearing id, or an alias → alias for SQL.
    pub fn to_query_alias(&self, value: &str) -> String {
        self.alias(value)
    }

    fn base_alias(&self, account_id: &str) -> String {
        let mac = hmac_sha256(&self.key, account_id.as_bytes());
        let mut hex = String::with_capacity(HEX_LEN);
        for b in mac.iter().take(HEX_LEN / 2) {
            hex.push_str(&format!("{b:02x}"));
        }
        format!("{ALIAS_PREFIX}{hex}")
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        let dig = Sha256::digest(key);
        k[..32].copy_from_slice(&dig);
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; BLOCK];
    let mut opad = [0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let mut inner = Sha256::new();
    inner.update(ipad);
    inner.update(data);
    let ih = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(ih);
    let out = outer.finalize();
    let mut mac = [0u8; 32];
    mac.copy_from_slice(&out);
    mac
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn alias_stable_for_same_key() {
        let key = [7u8; 32];
        let a1 = Aliaser::from_key(key);
        let a2 = Aliaser::from_key(key);
        let id = "codex-deadbeef-user@example.com-plus";
        let x = a1.alias(id);
        let y = a2.alias(id);
        assert_eq!(x, y);
        assert!(x.starts_with("acct_"));
        assert_eq!(x.len(), "acct_".len() + 8);
        assert!(!x.contains('@'));
        assert!(!x.contains("example"));
    }

    #[test]
    fn alias_idempotent_and_resolve() {
        let al = Aliaser::from_key([1u8; 32]);
        let id = "codex-aaaa-alice@ex.com-plus";
        let a = al.alias(id);
        assert_eq!(al.alias(&a), a);
        assert_eq!(al.resolve(&a).as_deref(), Some(id));
    }

    #[test]
    fn collision_gets_suffix() {
        // Force collision by stubbing via register order on hand-crafted bases is hard with
        // real HMAC; instead verify two different ids never share an alias string.
        let al = Aliaser::from_key([9u8; 32]);
        let mut seen = std::collections::HashSet::new();
        for i in 0..64 {
            let id = format!("codex-{i:04x}-u{i}@ex.com-plus");
            let a = al.alias(&id);
            assert!(seen.insert(a), "duplicate alias");
        }
    }

    #[test]
    fn looks_like_alias_accepts_suffix() {
        assert!(Aliaser::looks_like_alias("acct_deadbeef"));
        assert!(Aliaser::looks_like_alias("acct_deadbeef-2"));
        assert!(!Aliaser::looks_like_alias("codex-x-a@b.com-plus"));
        assert!(!Aliaser::looks_like_alias("acct_short"));
    }

    #[test]
    fn resolve_binding_old_and_new() {
        let al = Aliaser::from_key([3u8; 32]);
        let id = "codex-bb-bob@ex.com-plus";
        let a = al.alias(id);
        assert_eq!(al.resolve_binding(&a), id);
        assert_eq!(al.resolve_binding(id), id);
    }

    #[test]
    fn b64_roundtrip() {
        let al = Aliaser::ephemeral();
        let s = al.key_b64();
        let al2 = Aliaser::from_b64(&s).unwrap();
        let id = "codex-cc-c@ex.com-plus";
        assert_eq!(al.alias(id), al2.alias(id));
    }
}
