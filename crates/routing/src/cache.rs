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
    entries: HashMap<String, u64>, // alias -> group seq
    groups: BTreeMap<u64, Group>,  // insertion order = eviction order
    next: u64,
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
            groups: BTreeMap::new(),
            next: 0,
        }
    }

    /// New TTL for bindings written from now on (existing deadlines are kept).
    pub fn set_ttl(&mut self, ttl: Duration) {
        if !ttl.is_zero() {
            self.ttl = ttl;
        }
    }

    fn remove_group(&mut self, seq: u64) -> Option<Group> {
        let g = self.groups.remove(&seq)?;
        for a in &g.aliases {
            if self.entries.get(a) == Some(&seq) {
                self.entries.remove(a);
            }
        }
        Some(g)
    }

    fn insert_group(&mut self, auth: String, expires: SystemTime, aliases: Vec<String>) {
        if aliases.is_empty() {
            return;
        }
        // An alias can belong to only one group: steal it from older groups.
        for a in &aliases {
            if let Some(old) = self.entries.get(a).copied() {
                if let Some(g) = self.groups.get_mut(&old) {
                    g.aliases.retain(|x| x != a);
                    if g.aliases.is_empty() {
                        self.groups.remove(&old);
                    }
                }
            }
        }
        self.next += 1;
        let seq = self.next;
        for a in &aliases {
            self.entries.insert(a.clone(), seq);
        }
        self.groups.insert(
            seq,
            Group {
                auth,
                expires,
                aliases,
                seq,
            },
        );
        while self.entries.len() > self.max_entries {
            let Some((&oldest, _)) = self.groups.iter().next() else {
                break;
            };
            self.remove_group(oldest);
        }
    }

    fn live(&mut self, key: &str, now: SystemTime) -> Option<Group> {
        let seq = *self.entries.get(key)?;
        let g = self.groups.get(&seq)?.clone();
        if now >= g.expires {
            self.remove_group(seq);
            return None;
        }
        Some(g)
    }

    pub fn get(&mut self, key: &str, now: SystemTime) -> Option<String> {
        self.live(key, now).map(|g| g.auth)
    }

    pub fn get_and_refresh(&mut self, key: &str, now: SystemTime) -> Option<String> {
        let g = self.live(key, now)?;
        self.remove_group(g.seq);
        let aliases = compact(merge(vec![key.to_string()], &g.aliases));
        self.insert_group(g.auth.clone(), now + self.ttl, aliases);
        Some(g.auth)
    }

    pub fn set_aliases(&mut self, auth: &str, keys: &[String], now: SystemTime) {
        if auth.is_empty() {
            return;
        }
        let mut aliases = merge(vec![], keys);
        for k in keys {
            if let Some(seq) = self.entries.get(k).copied() {
                let Some(g) = self.groups.get(&seq).cloned() else {
                    continue;
                };
                self.remove_group(seq);
                if now < g.expires {
                    aliases = merge(aliases, &g.aliases);
                }
            }
        }
        let aliases = compact(aliases);
        self.insert_group(auth.to_string(), now + self.ttl, aliases);
    }

    pub fn set(&mut self, key: &str, auth: &str, now: SystemTime) {
        self.set_aliases(auth, &[key.to_string()], now);
    }

    pub fn touch(&mut self, key: &str, auth: &str, now: SystemTime) -> bool {
        match self.live(key, now) {
            Some(g) if g.auth == auth => {
                self.remove_group(g.seq);
                let aliases = compact(merge(vec![key.to_string()], &g.aliases));
                self.insert_group(g.auth, now + self.ttl, aliases);
                true
            }
            _ => false,
        }
    }

    pub fn compare_and_delete(&mut self, key: &str, auth: &str) -> bool {
        let Some(seq) = self.entries.get(key).copied() else {
            return false;
        };
        let Some(g) = self.groups.get(&seq).cloned() else {
            return false;
        };
        if g.auth != auth {
            return false;
        }
        self.remove_group(seq);
        let surviving: Vec<String> = g.aliases.into_iter().filter(|a| a != key).collect();
        self.insert_group(g.auth, g.expires, surviving);
        true
    }

    pub fn invalidate_auth(&mut self, auth: &str) {
        let seqs: Vec<u64> = self
            .groups
            .values()
            .filter(|g| g.auth == auth)
            .map(|g| g.seq)
            .collect();
        for s in seqs {
            self.remove_group(s);
        }
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
