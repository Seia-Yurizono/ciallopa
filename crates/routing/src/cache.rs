//! Port of CPA `sdk/cliproxy/auth/session_cache.go`: alias groups sharing one binding,
//! sliding TTL, at most one `pck:` alias + 64 stable aliases per group, bounded size.
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime};

const MAX_STABLE_ALIASES: usize = 64;
pub(crate) const DEFAULT_MAX_ENTRIES: usize = 65536;

#[derive(Clone, Debug, PartialEq)]
struct Group {
    auth: String,
    expires: SystemTime,
    aliases: Vec<String>,
    seq: u64,
}

pub(crate) struct SessionCache {
    ttl: Duration,
    max_entries: usize,
    entries: HashMap<String, u64>, // alias -> stable group id
    groups: HashMap<u64, Group>,
    lru: BTreeMap<u64, u64>, // refresh sequence -> group id
    next: u64,
    /// Bumped by every change (including sliding-TTL refreshes); persistence compares it.
    generation: u64,
}

/// One binding group as persisted across restarts.
#[derive(Clone, Debug, PartialEq)]
pub struct AffinityBinding {
    pub account: String,
    /// Unix seconds.
    pub expires: u64,
    pub aliases: Vec<String>,
}

fn unix(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn is_pck(alias: &str) -> bool {
    alias.starts_with("pck:")
        || alias
            .split_once("::")
            .is_some_and(|(_, r)| r.starts_with("pck:"))
}

fn compact(aliases: Vec<String>) -> Vec<String> {
    let (mut has_pck, mut stable) = (false, 0);
    let mut out = Vec::with_capacity(aliases.len());
    for a in aliases {
        if is_pck(&a) {
            if has_pck {
                continue;
            }
            has_pck = true;
        } else {
            if stable >= MAX_STABLE_ALIASES {
                continue;
            }
            stable += 1;
        }
        out.push(a);
    }
    out
}

fn merge(mut base: Vec<String>, more: &[String]) -> Vec<String> {
    for a in more {
        if !a.is_empty() && !base.contains(a) {
            base.push(a.clone());
        }
    }
    base.retain(|a| !a.is_empty());
    base
}

impl SessionCache {
    pub fn new(ttl: Duration, max_entries: usize) -> Self {
        let ttl = if ttl.is_zero() {
            Duration::from_secs(1800)
        } else {
            ttl
        };
        Self {
            ttl,
            max_entries: max_entries.max(1),
            entries: HashMap::new(),
            groups: HashMap::new(),
            lru: BTreeMap::new(),
            next: 0,
            generation: 0,
        }
    }

    /// New TTL for bindings written from now on (existing deadlines are kept).
    pub fn set_ttl(&mut self, ttl: Duration) {
        if !ttl.is_zero() {
            self.ttl = ttl;
        }
    }

    fn remove_group(&mut self, id: u64) -> Option<Group> {
        let g = self.groups.remove(&id)?;
        self.lru.remove(&g.seq);
        self.generation += 1;
        for a in &g.aliases {
            self.entries.remove(a);
        }
        Some(g)
    }

    fn evict(&mut self) {
        while self.entries.len() > self.max_entries {
            let Some((_, &oldest)) = self.lru.first_key_value() else {
                break;
            };
            self.remove_group(oldest);
        }
    }

    fn insert_group(&mut self, auth: String, expires: SystemTime, aliases: Vec<String>) {
        if aliases.is_empty() {
            return;
        }
        // Import may steal aliases, but leaves the surviving groups in their LRU position.
        for a in &aliases {
            if let Some(old) = self.entries.get(a).copied() {
                let g = self.groups.get_mut(&old).expect("alias group");
                g.aliases.retain(|x| x != a);
                if g.aliases.is_empty() {
                    self.remove_group(old);
                }
            }
        }
        self.next += 1;
        let id = self.next;
        for a in &aliases {
            self.entries.insert(a.clone(), id);
        }
        self.groups.insert(
            id,
            Group {
                auth,
                expires,
                aliases,
                seq: id,
            },
        );
        self.lru.insert(id, id);
        self.generation += 1;
        self.evict();
    }

    fn live(&mut self, key: &str, now: SystemTime) -> Option<u64> {
        let id = *self.entries.get(key)?;
        if now >= self.groups.get(&id)?.expires {
            self.remove_group(id);
            return None;
        }
        Some(id)
    }

    fn refresh(&mut self, id: u64, now: SystemTime) {
        let g = self.groups.get_mut(&id).expect("live group");
        self.lru.remove(&g.seq);
        self.next += 1;
        g.seq = self.next;
        g.expires = now + self.ttl;
        self.lru.insert(g.seq, id);
        self.generation += 1;
    }

    // Lookup order decides which aliases survive the next compaction.
    fn promote(&mut self, id: u64, key: &str) {
        let g = self.groups.get_mut(&id).expect("live group");
        if let Some(pos) = g.aliases.iter().position(|a| a == key) {
            g.aliases[..=pos].rotate_right(1);
        }
    }

    pub fn get(&mut self, key: &str, now: SystemTime) -> Option<String> {
        let id = self.live(key, now)?;
        Some(self.groups[&id].auth.clone())
    }

    pub fn get_and_refresh(&mut self, key: &str, now: SystemTime) -> Option<String> {
        let id = self.live(key, now)?;
        self.refresh(id, now);
        self.promote(id, key);
        Some(self.groups[&id].auth.clone())
    }

    pub fn set_aliases(&mut self, auth: &str, keys: &[String], now: SystemTime) {
        if auth.is_empty() {
            return;
        }
        let mut ids = Vec::new();
        for key in keys {
            if let Some(id) = self.live(key, now) {
                if !ids.contains(&id) {
                    ids.push(id);
                }
            }
        }
        // The common bound-request path changes neither group identity nor alias mappings.
        if let [id] = ids.as_slice() {
            if keys
                .iter()
                .all(|k| k.is_empty() || self.entries.get(k) == Some(id))
            {
                let id = *id;
                let g = self.groups.get_mut(&id).expect("live group");
                if g.auth != auth {
                    g.auth = auth.to_string();
                }
                for key in keys.iter().rev() {
                    self.promote(id, key);
                }
                self.refresh(id, now);
                return;
            }
        }
        let mut aliases = merge(vec![], keys);
        for id in &ids {
            aliases = merge(aliases, &self.groups[id].aliases);
        }
        let aliases = compact(aliases);
        if aliases.is_empty() {
            return;
        }
        self.next += 1;
        let seq = self.next;
        let id = ids.first().copied().unwrap_or(seq);
        for old in ids {
            let g = self.groups.remove(&old).expect("live group");
            self.lru.remove(&g.seq);
            for alias in &g.aliases {
                if !aliases.contains(alias) {
                    self.entries.remove(alias);
                }
            }
        }
        for alias in &aliases {
            if self.entries.get(alias) != Some(&id) {
                self.entries.insert(alias.clone(), id);
            }
        }
        self.groups.insert(
            id,
            Group {
                auth: auth.to_string(),
                expires: now + self.ttl,
                aliases,
                seq,
            },
        );
        self.lru.insert(seq, id);
        self.generation += 1;
        self.evict();
    }

    pub fn set(&mut self, key: &str, auth: &str, now: SystemTime) {
        self.set_aliases(auth, &[key.to_string()], now);
    }

    pub fn touch(&mut self, key: &str, auth: &str, now: SystemTime) -> bool {
        let Some(id) = self.live(key, now) else {
            return false;
        };
        if self.groups[&id].auth != auth {
            return false;
        }
        self.refresh(id, now);
        self.promote(id, key);
        true
    }

    pub fn compare_and_delete(&mut self, key: &str, auth: &str) -> bool {
        let Some(id) = self.entries.get(key).copied() else {
            return false;
        };
        let g = self.groups.get_mut(&id).expect("alias group");
        if g.auth != auth {
            return false;
        }
        g.aliases.retain(|a| a != key);
        self.entries.remove(key);
        if g.aliases.is_empty() {
            self.remove_group(id);
        } else {
            // Deletion previously reinserted surviving aliases as the newest group.
            self.lru.remove(&g.seq);
            self.next += 1;
            g.seq = self.next;
            self.lru.insert(g.seq, id);
            self.generation += 1;
        }
        true
    }

    pub fn invalidate_auth(&mut self, auth: &str) {
        let ids: Vec<_> = self
            .groups
            .iter()
            .filter(|(_, g)| g.auth == auth)
            .map(|(&id, _)| id)
            .collect();
        for id in ids {
            self.remove_group(id);
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Live groups, oldest first (the eviction order).
    pub fn export(&self, now: SystemTime) -> Vec<AffinityBinding> {
        self.lru
            .values()
            .filter_map(|id| self.groups.get(id))
            .filter(|g| g.expires > now)
            .map(|g| AffinityBinding {
                account: g.auth.clone(),
                expires: unix(g.expires),
                aliases: g.aliases.clone(),
            })
            .collect()
    }

    /// Add persisted groups (oldest first). Expired ones, ones for accounts `keep` rejects
    /// and empty ones are dropped; a deadline beyond the current TTL is shortened to it.
    /// Returns the number of groups restored.
    pub fn import(
        &mut self,
        bindings: Vec<AffinityBinding>,
        now: SystemTime,
        keep: &dyn Fn(&str) -> bool,
    ) -> usize {
        let mut n = 0;
        for b in bindings {
            let expires =
                (SystemTime::UNIX_EPOCH + Duration::from_secs(b.expires)).min(now + self.ttl);
            if expires <= now || b.account.is_empty() || !keep(&b.account) {
                continue;
            }
            let aliases = compact(merge(vec![], &b.aliases));
            if aliases.is_empty() {
                continue;
            }
            self.insert_group(b.account, expires, aliases);
            n += 1;
        }
        n
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn t0() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)
    }

    #[test]
    fn get_and_refresh_slides_ttl() {
        let mut c = SessionCache::new(Duration::from_secs(10), DEFAULT_MAX_ENTRIES);
        c.set("s", "a", t0());
        assert_eq!(
            c.get_and_refresh("s", t0() + Duration::from_secs(8))
                .as_deref(),
            Some("a")
        );
        assert_eq!(
            c.get("s", t0() + Duration::from_secs(15)).as_deref(),
            Some("a")
        );
        assert_eq!(c.get("s", t0() + Duration::from_secs(19)), None);
    }

    #[test]
    fn aliases_share_binding_and_compare_and_delete_keeps_survivors() {
        let mut c = SessionCache::new(Duration::from_secs(60), DEFAULT_MAX_ENTRIES);
        c.set_aliases("a", &["p".into(), "f".into()], t0());
        assert_eq!(c.get("f", t0()).as_deref(), Some("a"));
        assert!(!c.compare_and_delete("p", "b"));
        assert!(c.compare_and_delete("p", "a"));
        assert_eq!(c.get("p", t0()), None);
        assert_eq!(c.get("f", t0()).as_deref(), Some("a"));
        // rebinding one alias moves the whole merged group
        c.set_aliases("b", &["x".into(), "f".into()], t0());
        assert_eq!(c.get("x", t0()).as_deref(), Some("b"));
        assert_eq!(c.get("f", t0()).as_deref(), Some("b"));
    }

    #[test]
    fn compaction_keeps_single_prompt_cache_alias() {
        let mut c = SessionCache::new(Duration::from_secs(60), DEFAULT_MAX_ENTRIES);
        c.set_aliases(
            "a",
            &["codex::pck:1::m".into(), "codex::conv:1::m".into()],
            t0(),
        );
        c.set_aliases(
            "a",
            &["codex::pck:2::m".into(), "codex::conv:1::m".into()],
            t0(),
        );
        assert_eq!(c.get("codex::pck:2::m", t0()).as_deref(), Some("a"));
        assert_eq!(c.get("codex::pck:1::m", t0()), None);
        assert_eq!(c.get("codex::conv:1::m", t0()).as_deref(), Some("a"));
    }

    #[test]
    fn capacity_bounded() {
        let mut c = SessionCache::new(Duration::from_secs(60), 3);
        for i in 0..10 {
            c.set(&format!("k{i}"), "a", t0());
        }
        assert!(c.len() <= 3);
        assert_eq!(c.get("k9", t0()).as_deref(), Some("a"));
        assert_eq!(c.get("k0", t0()), None);
    }

    #[test]
    fn export_import_round_trip_drops_expired_and_unknown_accounts() {
        let mut c = SessionCache::new(Duration::from_secs(100), DEFAULT_MAX_ENTRIES);
        c.set_aliases("a", &["p".into(), "f".into()], t0());
        c.set("q", "gone", t0());
        c.set("old", "a", t0() - Duration::from_secs(90));
        let g = c.generation();
        let saved = c.export(t0());
        assert_eq!(c.generation(), g, "export is read-only");
        assert_eq!(saved.len(), 3);
        // Restart 20 s after saving; the `old` binding expired at t0+10.
        let later = t0() + Duration::from_secs(20);
        let mut d = SessionCache::new(Duration::from_secs(100), DEFAULT_MAX_ENTRIES);
        let n = d.import(saved, later, &|id| id == "a");
        assert_eq!(n, 1);
        assert_eq!(d.get("p", later).as_deref(), Some("a"));
        assert_eq!(d.get("f", later).as_deref(), Some("a"));
        assert_eq!(d.get("q", later), None);
        assert_eq!(d.get("old", later), None);
        // The original deadline is kept (no fresh TTL from the restart).
        assert_eq!(d.get("p", t0() + Duration::from_secs(101)), None);
    }

    #[test]
    fn import_caps_deadlines_at_the_current_ttl() {
        let far = AffinityBinding {
            account: "a".into(),
            expires: unix(t0()) + 86_400,
            aliases: vec!["s".into()],
        };
        let mut c = SessionCache::new(Duration::from_secs(60), DEFAULT_MAX_ENTRIES);
        assert_eq!(c.import(vec![far], t0(), &|_| true), 1);
        assert_eq!(
            c.get("s", t0() + Duration::from_secs(59)).as_deref(),
            Some("a")
        );
        assert_eq!(c.get("s", t0() + Duration::from_secs(61)), None);
    }

    #[test]
    fn touch_requires_matching_auth() {
        let mut c = SessionCache::new(Duration::from_secs(10), DEFAULT_MAX_ENTRIES);
        c.set("s", "a", t0());
        assert!(!c.touch("s", "b", t0() + Duration::from_secs(5)));
        assert!(c.touch("s", "a", t0() + Duration::from_secs(5)));
        assert_eq!(
            c.get("s", t0() + Duration::from_secs(12)).as_deref(),
            Some("a")
        );
        c.invalidate_auth("a");
        assert_eq!(c.get("s", t0()), None);
    }
}
