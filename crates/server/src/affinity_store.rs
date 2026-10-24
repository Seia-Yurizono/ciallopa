//! Session-affinity bindings across restarts, in `session_affinity.json` next to the usage
//! DB path. Written atomically (temp file, fsync, rename), only after a change and at most
//! once per [`WRITE_EVERY`]; flushed on graceful shutdown. Loading drops bindings that have
//! expired and bindings to accounts that no longer exist.
use ciallopa_routing::{AffinityBinding, CpaRouter};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A restart loses at most this much binding churn.
pub const WRITE_EVERY: Duration = Duration::from_secs(30);
const VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct Snapshot {
    version: u32,
    bindings: Vec<Binding>,
}

#[derive(Serialize, Deserialize)]
struct Binding {
    account: String,
    /// Unix seconds.
    expires: u64,
    aliases: Vec<String>,
}

pub fn path(cfg: &ciallopa_core::Config) -> PathBuf {
    cfg.usage.path.with_file_name("session_affinity.json")
}

pub struct Store {
    router: Arc<CpaRouter>,
    path: PathBuf,
    /// Router generation last written (u64::MAX: nothing yet).
    written: AtomicU64,
    write: Mutex<()>,
}

impl Store {
    pub fn new(router: Arc<CpaRouter>, path: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            router,
            path,
            written: AtomicU64::new(u64::MAX),
            write: Mutex::new(()),
        })
    }

    /// Restore saved bindings for the accounts `known` accepts. Returns the number restored.
    pub fn load(&self, known: &dyn Fn(&str) -> bool) -> usize {
        let s = match std::fs::read(&self.path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return 0,
            Err(e) => {
                tracing::warn!(path = %self.path.display(), error = %e, "session affinity not restored");
                return 0;
            }
        };
        let snap: Snapshot = match serde_json::from_slice(&s) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(path = %self.path.display(), error = %e, "session affinity file unreadable; starting empty");
                return 0;
            }
        };
        if snap.version != VERSION {
            return 0;
        }
        let total = snap.bindings.len();
        let bindings = snap
            .bindings
            .into_iter()
            .map(|b| AffinityBinding {
                account: b.account,
                expires: b.expires,
                aliases: b.aliases,
            })
            .collect();
        let n = self.router.affinity_import(bindings, known);
        tracing::info!(
            restored = n,
            dropped = total - n,
            "session affinity restored"
        );
        n
    }

    /// Write the bindings if they changed since the last write (blocking).
    pub fn flush(&self) {
        let _w = self.write.lock().unwrap_or_else(|e| e.into_inner());
        let generation = self.router.affinity_generation();
        if self.written.load(Ordering::Relaxed) == generation {
            return;
        }
        let snap = Snapshot {
            version: VERSION,
            bindings: self
                .router
                .affinity_export()
                .into_iter()
                .map(|b| Binding {
                    account: b.account,
                    expires: b.expires,
                    aliases: b.aliases,
                })
                .collect(),
        };
        match write_atomic(&self.path, &snap) {
            Ok(()) => self.written.store(generation, Ordering::Relaxed),
            Err(e) => {
                tracing::warn!(path = %self.path.display(), error = %e, "persisting session affinity failed")
            }
        }
    }

    /// Background writer: checks every [`WRITE_EVERY`] and writes only after a change.
    pub fn spawn(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(WRITE_EVERY);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await;
            loop {
                tick.tick().await;
                if me.router.affinity_generation() == me.written.load(Ordering::Relaxed) {
                    continue;
                }
                let m = me.clone();
                let _ = tokio::task::spawn_blocking(move || m.flush()).await;
            }
        })
    }
}

fn write_atomic(path: &Path, snap: &Snapshot) -> std::io::Result<()> {
    let body = serde_json::to_vec(snap).map_err(std::io::Error::other)?;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("json.tmp");
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(&tmp)?;
    f.write_all(&body)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn router() -> Arc<CpaRouter> {
        Arc::new(CpaRouter::new(Default::default()))
    }

    fn binding(account: &str, expires: u64, alias: &str) -> AffinityBinding {
        AffinityBinding {
            account: account.into(),
            expires,
            aliases: vec![alias.into()],
        }
    }

    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    #[test]
    fn writes_only_after_a_change_and_restores_known_accounts() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("session_affinity.json");
        let r = router();
        r.affinity_import(
            vec![
                binding("a", now() + 3600, "s1"),
                binding("b", now() + 3600, "s2"),
            ],
            &|_| true,
        );
        let st = Store::new(r.clone(), p.clone());
        st.flush();
        assert!(p.exists());
        std::fs::remove_file(&p).unwrap();
        st.flush();
        assert!(!p.exists(), "unchanged bindings are not rewritten");
        r.affinity_import(vec![binding("a", now() + 3600, "s3")], &|_| true);
        st.flush();
        assert!(p.exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&p).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        assert!(!dir.path().join("session_affinity.json.tmp").exists());

        let r2 = router();
        let st2 = Store::new(r2.clone(), p.clone());
        assert_eq!(st2.load(&|id| id == "a"), 2);
        let saved = r2.affinity_export();
        assert!(saved.iter().all(|b| b.account == "a"));
        assert_eq!(saved.len(), 2);
    }

    #[test]
    fn expired_missing_or_corrupt_files_restore_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("session_affinity.json");
        let st = Store::new(router(), p.clone());
        assert_eq!(st.load(&|_| true), 0);
        std::fs::write(&p, b"{not json").unwrap();
        assert_eq!(st.load(&|_| true), 0);
        std::fs::write(
            &p,
            format!(
                r#"{{"version":1,"bindings":[{{"account":"a","expires":{},"aliases":["s"]}}]}}"#,
                now() - 1
            ),
        )
        .unwrap();
        assert_eq!(st.load(&|_| true), 0);
    }
}
