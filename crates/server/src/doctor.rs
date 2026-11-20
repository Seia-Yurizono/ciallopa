//! `ciallopa doctor`: a read-only self-check of config, storage, egress and accounts.
//!
//! It loads the config with the loader `serve` uses, opens the usage database read-only, makes
//! credential-free requests to the upstream through the effective egress, and decodes the
//! account tokens locally. It never refreshes a token, calls a model, writes a file or runs
//! `checkpoint` / `VACUUM`. Each check is a plain function returning [`Check`]s; `id` and
//! `status` are the stable contract of `--json`.
use ciallopa_core::{settings::mask_proxy, Config, SkipReason};
use ciallopa_upstream::{ClientOptions, UpstreamClient, UpstreamError};
use serde::Serialize;
use serde_json::{json, Value};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Whole-probe bound of each network request (TLS probe, WebSocket Upgrade, admin API).
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// Clock difference to the upstream above which the tokens' `exp` checks start to lie.
const CLOCK_SKEW_WARN_SECS: i64 = 60;
const CERT_WARN_DAYS: i64 = 14;
/// A token that expires within this window is reported (a running service renews it earlier).
const TOKEN_WARN_SECS: i64 = 24 * 3600;
const MIB: u64 = 1024 * 1024;
/// `-wal` above this is worth a look; below `WAL_RATIO_MIN` it is the normal ~4 MiB
/// auto-checkpoint cycle whatever the database size.
const WAL_WARN_BYTES: u64 = 64 * MIB;
const WAL_RATIO_MIN: u64 = 16 * MIB;
const DISK_WARN_BYTES: u64 = 1024 * MIB;
const DISK_FAIL_BYTES: u64 = 64 * MIB;

/// `(id, layer, title)` of every check: the order of the report and the single place that
/// names them.
const META: &[(&str, &str, &str)] = &[
    ("config.parse", "config", "Configuration"),
    ("config.perm", "config", "Config file permissions"),
    ("paths.auth_dir", "storage", "Account directory"),
    ("paths.data_dir", "storage", "Usage database directory"),
    ("db.open", "storage", "Usage database"),
    ("db.schema", "storage", "Database schema version"),
    ("db.quick_check", "storage", "Database integrity"),
    ("db.wal_size", "storage", "WAL size"),
    ("db.disk_free", "storage", "Free disk space"),
    ("egress.proxy", "network", "Egress proxy"),
    ("egress.connect", "network", "Upstream connection"),
    ("upstream.tls", "network", "Upstream TLS"),
    ("upstream.clock", "network", "Clock against upstream"),
    ("upstream.ws", "network", "Responses WebSocket Upgrade"),
    ("accounts.expiry", "accounts", "Account token expiry"),
    (
        "accounts.state",
        "accounts",
        "Account state in the running service",
    ),
];
const LAYERS: &[(&str, &str)] = &[
    ("config", "Config"),
    ("storage", "Storage"),
    ("network", "Network"),
    ("accounts", "Accounts"),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Pass,
    Skip,
    Warn,
    Fail,
    Error,
}

/// One finding: one element of the `--json` `checks` array.
#[derive(Debug, Clone, Serialize)]
pub struct Check {
    pub id: &'static str,
    pub layer: &'static str,
    pub status: Status,
    pub title: &'static str,
    /// What was observed (the "problem" when the status is not `pass`).
    pub detail: String,
    pub next: Option<String>,
    pub evidence: Value,
    /// Wall time of the probe that produced it (checks derived from one request share it).
    pub duration_ms: u64,
}

fn pass(id: &'static str, detail: impl Into<String>) -> Check {
    mk(id, Status::Pass, detail)
}
fn warn(id: &'static str, detail: impl Into<String>) -> Check {
    mk(id, Status::Warn, detail)
}
fn fail(id: &'static str, detail: impl Into<String>) -> Check {
    mk(id, Status::Fail, detail)
}
fn skip(id: &'static str, detail: impl Into<String>) -> Check {
    mk(id, Status::Skip, detail)
}

fn mk(id: &'static str, status: Status, detail: impl Into<String>) -> Check {
    let (layer, title) = META
        .iter()
        .find(|m| m.0 == id)
        .map_or(("other", id), |m| (m.1, m.2));
    Check {
        id,
        layer,
        status,
        title,
        detail: detail.into(),
        next: None,
        evidence: Value::Null,
        duration_ms: 0,
    }
}
impl Check {
    fn next(mut self, text: impl Into<String>) -> Self {
        self.next = Some(text.into());
        self
    }
    fn ev(mut self, v: Value) -> Self {
        self.evidence = v;
        self
    }
}

/// 0: no `fail`; 1: at least one `fail`; 2: a check could not run (`error`) and nothing failed.
pub fn exit_code(checks: &[Check]) -> i32 {
    if checks.iter().any(|c| c.status == Status::Fail) {
        1
    } else if checks.iter().any(|c| c.status == Status::Error) {
        2
    } else {
        0
    }
}

/// Run every check and print the report; returns the process exit code.
pub async fn run(config_path: &Path, json: bool, verbose: bool, full: bool) -> i32 {
    let (checks, secrets) = collect(config_path, full).await;
    let text = if json {
        render_json(&checks)
    } else {
        render_human(&checks, config_path, verbose)
    };
    println!("{}", scrub(text, &secrets));
    exit_code(&checks)
}

/// All checks in report order, plus the secrets of the loaded config (to keep out of output).
pub async fn collect(config_path: &Path, full: bool) -> (Vec<Check>, Vec<String>) {
    let (mut checks, cfg) = config_checks(config_path);
    let mut secrets = vec![];
    if let Some(cfg) = cfg {
        secrets = cfg.keys().into_iter().map(|k| k.key).collect();
        let cfg = std::sync::Arc::new(cfg);
        let (storage, network, accounts) = tokio::join!(
            storage_checks(cfg.clone(), full),
            network_checks(cfg.clone()),
            account_checks(cfg),
        );
        checks.extend(storage);
        checks.extend(network);
        checks.extend(accounts);
    } else {
        for m in META.iter().filter(|m| !m.0.starts_with("config.")) {
            checks.push(mk(m.0, Status::Skip, "the configuration did not load"));
        }
    }
    checks.sort_by_key(|c| META.iter().position(|m| m.0 == c.id));
    (checks, secrets)
}

fn scrub(mut text: String, secrets: &[String]) -> String {
    for s in secrets.iter().filter(|s| s.len() >= 8) {
        text = text.replace(s.as_str(), "***");
    }
    text
}

// ---- rendering -------------------------------------------------------------------------

fn summary(checks: &[Check]) -> Value {
    let n = |s| checks.iter().filter(|c| c.status == s).count();
    json!({
        "pass": n(Status::Pass), "warn": n(Status::Warn), "fail": n(Status::Fail),
        "skip": n(Status::Skip), "error": n(Status::Error),
    })
}

pub fn render_json(checks: &[Check]) -> String {
    let report = json!({
        "schema_version": 1,
        "ciallopa_version": env!("CARGO_PKG_VERSION"),
        "generated_at": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "summary": summary(checks),
        "checks": checks,
    });
    serde_json::to_string_pretty(&report).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
}

fn render_human(checks: &[Check], config_path: &Path, verbose: bool) -> String {
    use std::fmt::Write;
    let mut o = String::new();
    let uid = crate::host::euid();
    let _ = writeln!(
        o,
        "ciallopa doctor {} · config {} · checked as uid {}",
        env!("CARGO_PKG_VERSION"),
        config_path.display(),
        uid.map_or("?".into(), |u| u.to_string())
    );
    if uid == Some(0) && crate::host::service_user().is_some() {
        let _ = writeln!(
            o,
            "note: running as root, which can read what the {u} service cannot. For its view: sudo -u {u} ciallopa -c {} doctor",
            crate::host::DEB_CONFIG,
            u = crate::host::SERVICE_USER
        );
    }
    for (layer, name) in LAYERS {
        let _ = writeln!(o, "\n{name}");
        for c in checks.iter().filter(|c| c.layer == *layer) {
            let mark = match c.status {
                Status::Pass => "✓",
                Status::Warn => "!",
                Status::Fail => "✗",
                Status::Skip => "-",
                Status::Error => "?",
            };
            match c.status {
                Status::Pass | Status::Skip => {
                    let _ = writeln!(o, "  {mark} {:<16} {}", c.id, c.detail);
                }
                _ => {
                    let _ = writeln!(o, "  {mark} {:<16} {}", c.id, c.title);
                    let _ = writeln!(o, "      problem: {}", c.detail);
                    if let Some(n) = &c.next {
                        let _ = writeln!(o, "      next:    {n}");
                    }
                }
            }
            if verbose && !c.evidence.is_null() {
                let _ = writeln!(o, "      evidence: {} ({} ms)", c.evidence, c.duration_ms);
            }
        }
    }
    let s = summary(checks);
    let _ = writeln!(
        o,
        "\n{} fail · {} warn · {} pass · {} skip · {} error{}",
        s["fail"],
        s["warn"],
        s["pass"],
        s["skip"],
        s["error"],
        if verbose { "" } else { "   (-v for evidence)" }
    );
    o.trim_end().to_string()
}

// ---- config ----------------------------------------------------------------------------

/// What `serve` needs before it listens: the file opens, parses and validates, and the HTTP and
/// OAuth clients build (a `proxy_url` that passes validation can still fail there).
fn load(path: &Path) -> Result<Config, String> {
    std::fs::File::open(path)
        .map_err(|e| format!("cannot open: {e} (uid:gid {})", crate::host::ids()))?;
    let cfg = Config::load(path).map_err(|e| e.to_string())?;
    UpstreamClient::new(&cfg).map_err(|e| format!("upstream client: {e}"))?;
    crate::oauth_client(&cfg).map_err(|e| format!("oauth client: {e}"))?;
    Ok(cfg)
}

fn config_checks(path: &Path) -> (Vec<Check>, Option<Config>) {
    let shown = path.display().to_string();
    let cfg = match load(path) {
        Ok(c) => c,
        Err(problem) => {
            let c = fail("config.parse", format!("{shown}: {problem}"))
                .next("fix the line named above; `ciallopa init` writes a new config, -c <path> picks another");
            return (
                vec![c, skip("config.perm", "the configuration did not load")],
                None,
            );
        }
    };
    let keys = cfg.keys();
    let admins = keys.iter().filter(|k| k.admin).count();
    let mut notes = cfg.advisories();
    if keys.is_empty() {
        notes.push("api_keys is empty: every proxied request gets 401".into());
    }
    let parse = if notes.is_empty() {
        pass(
            "config.parse",
            format!(
                "loads; listen {}, {} api keys ({admins} admin)",
                cfg.listen,
                keys.len()
            ),
        )
    } else {
        warn("config.parse", notes.join("; ")).next("serve logs the same warnings at startup")
    }
    .ev(
        json!({"path": shown, "listen": cfg.listen.to_string(), "upstream": cfg.upstream,
        "api_keys": keys.len(), "admin_keys": admins}),
    );

    let mode = std::fs::metadata(path).map(|m| m.mode() & 0o7777).ok();
    let perm = match mode {
        Some(m) if !perm_ok(m, !keys.is_empty()) => warn(
            "config.perm",
            format!("mode {m:04o} lets other users read the file, which holds api_keys"),
        )
        .next(format!("chmod o-rwx {shown}")),
        Some(m) => pass("config.perm", format!("mode {m:04o}")),
        None => mk("config.perm", Status::Error, "cannot stat the config file"),
    }
    .ev(json!({"mode": mode.map(|m| format!("{m:04o}"))}));
    (vec![parse, perm], Some(cfg))
}

/// Others must not read a config that holds keys.
fn perm_ok(mode: u32, has_keys: bool) -> bool {
    !has_keys || mode & 0o007 == 0
}

// ---- storage ---------------------------------------------------------------------------

/// Run blocking work; a panic becomes an `error` check instead of a dead doctor.
async fn blocking(id: &'static str, f: impl FnOnce() -> Vec<Check> + Send + 'static) -> Vec<Check> {
    let t = Instant::now();
    let mut out = tokio::task::spawn_blocking(f)
        .await
        .unwrap_or_else(|e| vec![mk(id, Status::Error, format!("check task failed: {e}"))]);
    stamp(&mut out, t);
    out
}
fn stamp(checks: &mut [Check], since: Instant) {
    let ms = since.elapsed().as_millis() as u64;
    for c in checks {
        c.duration_ms = ms;
    }
}

/// `access(2)` with the process's own uid: also reports a read-only filesystem for `W_OK`.
fn access(p: &Path, mode: libc::c_int) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(p.as_os_str().as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    // SAFETY: `c` is a valid NUL-terminated path for the duration of the call.
    if unsafe { libc::access(c.as_ptr(), mode) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn owner(p: &Path) -> String {
    std::fs::metadata(p).map_or("?".into(), |m| {
        format!("{}:{} {:04o}", m.uid(), m.gid(), m.mode() & 0o7777)
    })
}

async fn storage_checks(cfg: std::sync::Arc<Config>, full: bool) -> Vec<Check> {
    let c = cfg.clone();
    let mut out = blocking("paths.data_dir", move || data_dir_check(&c)).await;
    let c = cfg;
    out.extend(blocking("db.open", move || db_checks(&c, full)).await);
    out
}

fn data_dir_check(cfg: &Config) -> Vec<Check> {
    if !cfg.usage.enabled {
        return vec![skip(
            "paths.data_dir",
            "usage log is disabled ([usage] enabled = false)",
        )];
    }
    let db = &cfg.usage.path;
    let dir = db
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    // A missing directory is created by the service; what must be writable is the nearest
    // directory that exists.
    let existing = dir.ancestors().find(|a| a.exists()).unwrap_or(dir);
    let mut problems = vec![];
    if let Err(e) = access(existing, libc::W_OK | libc::X_OK) {
        problems.push(format!(
            "{} is not writable: {e} (owner {})",
            existing.display(),
            owner(existing)
        ));
    }
    if db.exists() {
        if let Err(e) = access(db, libc::R_OK | libc::W_OK) {
            problems.push(format!(
                "{} is not readable and writable: {e} (owner {})",
                db.display(),
                owner(db)
            ));
        }
    }
    let ev = json!({"dir": dir.display().to_string(), "checked_dir": existing.display().to_string(),
        "uid": crate::host::euid()});
    if problems.is_empty() {
        let created = if existing == dir {
            ""
        } else {
            " (will be created)"
        };
        vec![pass(
            "paths.data_dir",
            format!("{} is writable{created}", dir.display()),
        )
        .ev(ev)]
    } else {
        vec![fail("paths.data_dir", problems.join("; "))
            .next(format!(
                "make it writable for uid:gid {}: chown -R <service user> {}, or fix a read-only mount",
                crate::host::ids(),
                crate::host::shown(dir)
            ))
            .ev(ev)]
    }
}

/// `-wal` above 64 MiB, or above 16 MiB and half the database: a reader holds a transaction
/// open (checkpoint starvation) or the writer is stuck.
fn wal_status(wal: u64, main: u64) -> Status {
    if wal >= WAL_WARN_BYTES || (wal >= WAL_RATIO_MIN && wal * 2 >= main) {
        Status::Warn
    } else {
        Status::Pass
    }
}
/// Behind the database: migrated at the next start. Ahead: a newer binary wrote it.
fn schema_status(found: i64, expected: i64) -> Status {
    match found.cmp(&expected) {
        std::cmp::Ordering::Equal => Status::Pass,
        std::cmp::Ordering::Less => Status::Warn,
        std::cmp::Ordering::Greater => Status::Fail,
    }
}
fn disk_status(free: u64, db_bytes: u64) -> Status {
    if free < DISK_FAIL_BYTES {
        Status::Fail
    } else if free < DISK_WARN_BYTES || free < db_bytes.saturating_mul(2) {
        Status::Warn
    } else {
        Status::Pass
    }
}

fn free_bytes(dir: &Path) -> std::io::Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(dir.as_os_str().as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    // SAFETY: zeroed `statvfs` is a valid out-parameter; `c` is a valid path.
    let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut s) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(s.f_bavail as u64 * s.f_frsize as u64)
}

fn mib(b: u64) -> String {
    format!("{:.1} MiB", b as f64 / MIB as f64)
}

/// Open the database for reading without creating a file next to it. With `-wal` and `-shm`
/// present (a running service) a plain read-only open is exact. Without them the file holds
/// everything and `immutable=1` reads it without touching the directory: a plain open would
/// create empty `-wal`/`-shm` files, owned by root when doctor runs as root, which the service
/// user then cannot open. Anything in between is not opened.
fn open_db(db: &Path) -> Result<Option<rusqlite::Connection>, String> {
    use rusqlite::OpenFlags;
    let side = |ext: &str| {
        let mut p = db.as_os_str().to_owned();
        p.push(ext);
        std::fs::metadata(PathBuf::from(p)).ok()
    };
    let (wal, shm) = (side("-wal"), side("-shm"));
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI;
    let conn = match (wal.as_ref().map(|m| m.len()), shm.is_some()) {
        (Some(_), true) => rusqlite::Connection::open_with_flags(db, flags),
        (None | Some(0), false) => {
            let mut uri = String::from("file:");
            for b in db.as_os_str().as_encoded_bytes() {
                match b {
                    b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'/' | b'.' | b'_' | b'-' | b'~' => {
                        uri.push(*b as char)
                    }
                    _ => uri.push_str(&format!("%{b:02X}")),
                }
            }
            uri.push_str("?immutable=1");
            rusqlite::Connection::open_with_flags(uri, flags)
        }
        _ => return Ok(None),
    };
    conn.map(Some).map_err(|e| e.to_string())
}

/// `wal` or `delete`, from the file header (bytes 18-19 are 2 in WAL mode). `PRAGMA
/// journal_mode` cannot be used: an immutable connection always answers `delete`.
fn journal_mode(db: &Path) -> std::io::Result<&'static str> {
    use std::io::Read;
    let mut h = [0u8; 20];
    std::fs::File::open(db)?.read_exact(&mut h)?;
    if &h[..16] != b"SQLite format 3\0" {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bad header",
        ));
    }
    Ok(if h[18] == 2 && h[19] == 2 {
        "wal"
    } else {
        "delete"
    })
}

fn db_checks(cfg: &Config, full: bool) -> Vec<Check> {
    const IDS: [&str; 5] = [
        "db.open",
        "db.schema",
        "db.quick_check",
        "db.wal_size",
        "db.disk_free",
    ];
    let skip_all = |why: String| {
        IDS.iter()
            .map(|id| mk(id, Status::Skip, why.clone()))
            .collect()
    };
    if !cfg.usage.enabled {
        return skip_all("usage log is disabled".into());
    }
    let db = &cfg.usage.path;
    let Ok(md) = std::fs::metadata(db) else {
        return skip_all(format!(
            "{} does not exist yet (the first serve creates it)",
            db.display()
        ));
    };
    let unopened = |check: Check, why: &str| {
        let mut v = vec![check];
        v.extend(IDS[1..3].iter().map(|id| mk(id, Status::Skip, why)));
        v.extend(file_checks(db, md.len()));
        v
    };
    let conn = match open_db(db) {
        Ok(Some(c)) => c,
        Ok(None) => {
            return unopened(
                warn("db.open", "a -wal file without -shm is left over from an unclean stop")
                    .next("ciallopa recovers it at the next start; doctor does not open it, to avoid creating files"),
                "the database was not opened",
            )
        }
        Err(e) => {
            return unopened(
                fail("db.open", format!("cannot open {} read-only: {e}", db.display())).next(format!(
                    "owner is {}; this check ran as uid:gid {}: it needs read access (and write access to -shm while the service runs)",
                    owner(db),
                    crate::host::ids()
                )),
                "the database did not open",
            )
        }
    };
    let mut out = vec![];
    out.push(match journal_mode(db) {
        Ok(m) if m == "wal" => pass("db.open", format!("opened read-only, journal_mode={m}")),
        Ok(m) => warn("db.open", format!("journal_mode={m}, expected wal"))
            .next("start ciallopa once: it switches the database to WAL"),
        Err(e) => fail(
            "db.open",
            format!(
                "{} is not readable as an SQLite database: {e}",
                db.display()
            ),
        ),
    });
    let version: Result<i64, _> = conn.query_row("PRAGMA user_version", [], |r| r.get(0));
    out.push(match version {
        Ok(v) => {
            let want = ciallopa_usage::SCHEMA_VERSION;
            match schema_status(v, want) {
                Status::Pass => pass("db.schema", format!("schema v{v}")),
                Status::Warn => warn(
                    "db.schema",
                    format!("schema v{v} is behind this binary (v{want})"),
                )
                .next("it migrates when ciallopa starts; back up the database first"),
                s => mk(
                    "db.schema",
                    s,
                    format!("schema v{v} is newer than this binary (v{want})"),
                )
                .next("a newer ciallopa wrote it: run that version again, or restore a backup"),
            }
            .ev(json!({"found": v, "expected": want}))
        }
        Err(e) => fail("db.schema", format!("PRAGMA user_version: {e}")),
    });
    out.push(if full {
        match conn.query_row("PRAGMA quick_check", [], |r| r.get::<_, String>(0)) {
            Ok(r) if r == "ok" => pass("db.quick_check", "quick_check: ok"),
            Ok(r) => fail("db.quick_check", format!("quick_check: {r}"))
                .next("restore the latest backup; the file is damaged"),
            Err(e) => fail("db.quick_check", format!("quick_check: {e}")),
        }
    } else {
        skip(
            "db.quick_check",
            "not run; pass --full (reads the whole file)",
        )
    });
    out.extend(file_checks(db, md.len()));
    out
}

/// `-wal` size and free space: file metadata only.
fn file_checks(db: &Path, main: u64) -> Vec<Check> {
    let mut wal_path = db.as_os_str().to_owned();
    wal_path.push("-wal");
    let wal = std::fs::metadata(PathBuf::from(wal_path)).map_or(0, |m| m.len());
    let wal_check = match wal_status(wal, main) {
        Status::Pass => pass("db.wal_size", format!("WAL {} / database {}", mib(wal), mib(main))),
        s => mk(
            "db.wal_size",
            s,
            format!("WAL {} against a {} database: a long read transaction may be blocking the checkpoint", mib(wal), mib(main)),
        )
        .next("find the long reader (a stuck sqlite3 shell or backup) and close it; the WAL shrinks once it ends"),
    }
    .ev(json!({"wal_bytes": wal, "db_bytes": main}));
    let dir = db
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let disk = match free_bytes(dir) {
        Ok(free) => match disk_status(free, main + wal) {
            Status::Pass => pass("db.disk_free", format!("{} free", mib(free))),
            s => mk(
                "db.disk_free",
                s,
                format!("only {} free for a {} database", mib(free), mib(main + wal)),
            )
            .next("free space on the data disk; a full disk makes the usage writer drop records"),
        }
        .ev(json!({"free_bytes": free})),
        Err(e) => mk(
            "db.disk_free",
            Status::Error,
            format!("statvfs {}: {e}", dir.display()),
        ),
    };
    vec![wal_check, disk]
}

// ---- network ---------------------------------------------------------------------------

async fn network_checks(cfg: std::sync::Arc<Config>) -> Vec<Check> {
    let (proxy, http, ws) = tokio::join!(
        async { vec![proxy_check(&cfg)] },
        timed(probe_upstream(&cfg)),
        timed(probe_ws(&cfg)),
    );
    let mut out = proxy;
    out.extend(http);
    out.extend(ws);
    out
}

async fn timed(f: impl std::future::Future<Output = Vec<Check>>) -> Vec<Check> {
    let t = Instant::now();
    let mut v = f.await;
    stamp(&mut v, t);
    v
}

const ENV_PROXIES: [&str; 6] = [
    "HTTPS_PROXY",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
    "HTTP_PROXY",
    "http_proxy",
];

/// The gateway only uses `proxy_url` / `proxy_enabled`; proxy environment variables, which
/// most tools honour, are ignored (`build_reqwest` calls `no_proxy()`).
fn proxy_check(cfg: &Config) -> Check {
    let effective = cfg.effective_proxy();
    let env: Vec<(&str, String)> = ENV_PROXIES
        .iter()
        .filter_map(|n| {
            std::env::var(n)
                .ok()
                .filter(|v| !v.is_empty())
                .map(|v| (*n, mask_proxy(&v)))
        })
        .collect();
    let names: Vec<&str> = env.iter().map(|e| e.0).collect();
    let ev = json!({"effective": mask_proxy(&effective), "config_proxy_url": mask_proxy(&cfg.proxy_url),
        "proxy_enabled": cfg.proxy_enabled, "env_ignored": env.iter().map(|e| json!({e.0: e.1})).collect::<Vec<_>>()});
    if effective.is_empty() && !env.is_empty() {
        return warn("egress.proxy", format!("connecting directly: {} is set but ciallopa ignores proxy environment variables", names.join(", ")),
        )
        .next("set proxy_url in the config (settings page: egress proxy) if the network needs a proxy")
        .ev(ev);
    }
    let mut detail = if effective.is_empty() {
        if cfg.proxy_url.is_empty() {
            "direct (no proxy_url)".to_string()
        } else {
            "direct: proxy_url is set but proxy_enabled = false".to_string()
        }
    } else {
        format!("via {}", mask_proxy(&effective))
    };
    if !env.is_empty() {
        detail.push_str(&format!(" (ignoring {})", names.join(", ")));
    }
    pass("egress.proxy", detail).ev(ev)
}

/// Origin of the upstream plus `/cdn-cgi/trace` (the path the settings page's proxy test uses).
fn probe_url(upstream: &str) -> Result<reqwest::Url, String> {
    let mut u = reqwest::Url::parse(upstream).map_err(|e| format!("upstream: {e}"))?;
    u.set_path("/cdn-cgi/trace");
    u.set_query(None);
    u.set_fragment(None);
    Ok(u)
}

fn chain(e: &dyn std::error::Error) -> String {
    let mut s = e.to_string();
    let mut src = e.source();
    while let Some(x) = src {
        s.push_str(": ");
        s.push_str(&x.to_string());
        src = x.source();
    }
    s
}

/// What a failed TLS handshake usually means, from rustls' own wording.
fn tls_hint(err: &str) -> &'static str {
    let e = err.to_ascii_lowercase();
    if e.contains("unknownissuer") || e.contains("unknown issuer") {
        "an unknown certificate authority: a TLS-intercepting proxy, or no CA bundle (install ca-certificates)"
    } else if e.contains("expired") || e.contains("notvalidyet") || e.contains("not valid yet") {
        "the certificate is outside its validity: check the system clock first"
    } else {
        "check the proxy and any TLS interception on the path"
    }
}

fn looks_like_tls(err: &str) -> bool {
    let e = err.to_ascii_lowercase();
    ["certificate", "tls", "handshake", "invalid peer", "alert"]
        .iter()
        .any(|k| e.contains(k))
}

/// One credential-free GET through the same client construction the gateway uses; it yields
/// the connection, TLS (ALPN via the negotiated HTTP version, certificate) and clock checks.
async fn probe_upstream(cfg: &Config) -> Vec<Check> {
    let skipped = |why: &str| {
        vec![
            skip("egress.connect", why),
            skip("upstream.tls", why),
            skip("upstream.clock", why),
        ]
    };
    let url = match probe_url(&cfg.upstream) {
        Ok(u) => u,
        Err(e) => return skipped(&e),
    };
    let via = if cfg.effective_proxy().is_empty() {
        "direct".to_string()
    } else {
        mask_proxy(&cfg.effective_proxy())
    };
    let host = url.host_str().unwrap_or("?").to_string();
    let opts = ClientOptions {
        proxy_url: cfg.effective_proxy(),
        connect_timeout: Some(PROBE_TIMEOUT),
        cloudflare_cookies: false,
        ipv6: cfg.ipv6,
    };
    let client = match ciallopa_upstream::reqwest_builder(&opts)
        .and_then(|b| b.tls_info(true).build().map_err(UpstreamError::from))
    {
        Ok(c) => c,
        Err(e) => return skipped(&format!("cannot build the client: {e}")),
    };
    let t = Instant::now();
    let res = client.get(url.clone()).timeout(PROBE_TIMEOUT).send().await;
    let ms = t.elapsed().as_millis() as u64;
    let https = url.scheme() == "https";
    let resp = match res {
        Ok(r) => r,
        Err(e) => {
            let err = chain(&e);
            let ev = json!({"host": host, "via": via, "error": err});
            if e.is_timeout() {
                let mut v = skipped("no connection");
                v[0] = fail("egress.connect", format!("no answer from {host} within {}s ({via})", PROBE_TIMEOUT.as_secs()))
                    .next("check DNS and the firewall; with a proxy, check that it is up and allows CONNECT to the host")
                    .ev(ev);
                return v;
            }
            if https && looks_like_tls(&err) {
                let mut v = skipped("the TLS handshake failed");
                v[0] = pass(
                    "egress.connect",
                    format!("reached {host} ({via}), then the handshake failed"),
                )
                .ev(ev.clone());
                v[1] = fail(
                    "upstream.tls",
                    format!("TLS handshake with {host} failed: {err}"),
                )
                .next(format!("this usually means {}", tls_hint(&err)))
                .ev(ev);
                return v;
            }
            let mut v = skipped("no connection");
            v[0] = fail("egress.connect", format!("cannot reach {host} ({via}): {err}"))
                .next("check DNS, the firewall and proxy_url; the settings page's proxy test shows the same path")
                .ev(ev);
            return v;
        }
    };
    let mut out = vec![pass(
        "egress.connect",
        format!(
            "reached {host} via {via}: HTTP {} in {ms} ms",
            resp.status().as_u16()
        ),
    )
    .ev(json!({"host": host, "via": via, "status": resp.status().as_u16(), "connect_ms": ms}))];

    out.push(if https {
        let h2 = resp.version() == reqwest::Version::HTTP_2;
        let days = resp
            .extensions()
            .get::<reqwest::tls::TlsInfo>()
            .and_then(|i| i.peer_certificate())
            .and_then(cert_not_after)
            .map(|exp| (exp - unix_now()).div_euclid(86_400));
        let ev = json!({"alpn": if h2 { "h2" } else { "http/1.1" }, "cert_days_left": days});
        match (h2, days) {
            (_, Some(d)) if d < CERT_WARN_DAYS => warn(
                "upstream.tls",
                format!("the certificate of {host} expires in {d} days"),
            )
            .next("renew it, or check what terminates TLS in front of the upstream")
            .ev(ev),
            (false, _) => warn(
                "upstream.tls",
                "negotiated HTTP/1.1, not h2: something on the path strips ALPN",
            )
            .next(
                "the gateway works but is not Codex-shaped; check the proxy and any TLS inspection",
            )
            .ev(ev),
            (_, d) => pass(
                "upstream.tls",
                format!(
                    "TLS ok, ALPN h2{}",
                    d.map_or(String::new(), |d| format!(
                        ", certificate valid {d} more days"
                    ))
                ),
            )
            .ev(ev),
        }
    } else {
        skip("upstream.tls", "the upstream is plain http://")
    });

    out.push(
        match resp
            .headers()
            .get(reqwest::header::DATE)
            .and_then(|v| v.to_str().ok())
            .and_then(|d| chrono::DateTime::parse_from_rfc2822(d).ok())
        {
            Some(date) => {
                let skew = unix_now() - date.timestamp();
                let ev = json!({"skew_secs": skew});
                if skew.abs() > CLOCK_SKEW_WARN_SECS {
                    warn("upstream.clock", format!("this machine's clock is {} s {} the upstream's", skew.abs(), if skew > 0 { "ahead of" } else { "behind" }))
                        .next("sync the clock (chrony / systemd-timesyncd; in a VM or WSL, resync after sleep): token expiry and TLS validity depend on it")
                        .ev(ev)
                } else {
                    pass("upstream.clock", format!("clock within {} s of the upstream", skew.abs())).ev(ev)
                }
            }
            None => skip("upstream.clock", "the response had no usable Date header"),
        },
    );
    out
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// `notAfter` of a DER X.509 certificate, Unix seconds. Only the validity field is read.
fn cert_not_after(der: &[u8]) -> Option<i64> {
    // (tag, content, rest) of the first DER element.
    fn tlv(b: &[u8]) -> Option<(u8, &[u8], &[u8])> {
        let (&tag, b) = b.split_first()?;
        let (&l, b) = b.split_first()?;
        let (len, b) = if l < 0x80 {
            (l as usize, b)
        } else {
            let n = (l & 0x7f) as usize;
            if n == 0 || n > 4 || b.len() < n {
                return None;
            }
            (
                b[..n].iter().fold(0usize, |a, &x| a << 8 | x as usize),
                &b[n..],
            )
        };
        (b.len() >= len).then(|| (tag, &b[..len], &b[len..]))
    }
    let (_, cert, _) = tlv(der)?;
    let (_, tbs, _) = tlv(cert)?;
    let (tag, _, after_version) = tlv(tbs)?;
    // [0] version (absent in v1), then serial, signature algorithm, issuer.
    let mut rest = if tag == 0xA0 { after_version } else { tbs };
    for _ in 0..3 {
        rest = tlv(rest)?.2;
    }
    let (_, validity, _) = tlv(rest)?;
    let (_, _, not_after) = tlv(validity)?;
    let (tag, text, _) = tlv(not_after)?;
    let text = std::str::from_utf8(text).ok()?;
    let full = match tag {
        0x17 => {
            // UTCTime YYMMDDHHMMSSZ: 00..49 is 20xx.
            let yy: u32 = text.get(..2)?.parse().ok()?;
            format!("{}{}", if yy < 50 { "20" } else { "19" }, text.get(..12)?)
        }
        0x18 => text.get(..14)?.to_string(),
        _ => return None,
    };
    chrono::NaiveDateTime::parse_from_str(&full, "%Y%m%d%H%M%S")
        .ok()
        .map(|t| t.and_utc().timestamp())
}

/// Upgrade request without credentials: 101, or 401/403, proves TLS, the path and the egress
/// let a WebSocket Upgrade through; it says nothing about authorization.
async fn probe_ws(cfg: &Config) -> Vec<Check> {
    let up = match UpstreamClient::new(cfg) {
        Ok(u) => u,
        Err(e) => return vec![skip("upstream.ws", format!("cannot build the client: {e}"))],
    };
    let path = "/codex/responses";
    let ev = |status: Option<u16>| json!({"url": up.ws_url(path), "status": status});
    match tokio::time::timeout(PROBE_TIMEOUT, up.ws_connect(path, Default::default())).await {
        Err(_) => vec![fail("upstream.ws", format!("no WebSocket answer within {}s", PROBE_TIMEOUT.as_secs()))
            .next("check the proxy: it must allow CONNECT to the upstream and pass Upgrade")
            .ev(ev(None))],
        Ok(Ok(_)) => vec![pass("upstream.ws", "Upgrade accepted (101)").ev(ev(Some(101)))],
        Ok(Err(e)) => match ciallopa_upstream::ws::handshake_status(&e) {
            Some((s @ (401 | 403), ..)) => vec![pass("upstream.ws", format!("Upgrade reached the upstream, answered {s} without credentials (expected)")).ev(ev(Some(s)))],
            Some((429, ..)) => vec![warn("upstream.ws", "the upstream answered 429 to the Upgrade")
                .next("rate limited at the edge: retry later, or check whether the egress IP is shared")
                .ev(ev(Some(429)))],
            Some((s, ..)) => vec![fail("upstream.ws", format!("the Upgrade got HTTP {s} instead of 101/401/403"))
                .next("a proxy or CDN in front is not forwarding WebSocket Upgrade, or `upstream` points at the wrong path")
                .ev(ev(Some(s)))],
            None => {
                let msg = chain(&e);
                let next = if msg.contains("https:// egress proxy") {
                    "use an http:// or socks5h:// proxy_url: an https:// proxy cannot carry WebSockets"
                } else {
                    "check the egress proxy and DNS; HTTP working while WebSocket fails usually means the proxy blocks Upgrade"
                };
                vec![fail("upstream.ws", format!("WebSocket connect failed: {msg}")).next(next).ev(ev(None))]
            }
        },
    }
}

// ---- accounts --------------------------------------------------------------------------

async fn account_checks(cfg: std::sync::Arc<Config>) -> Vec<Check> {
    let c = cfg.clone();
    let (files, state) = tokio::join!(
        blocking("accounts.expiry", move || account_files(&c)),
        timed(account_state(&cfg)),
    );
    let mut out = files;
    out.extend(state);
    out
}

fn fmt_secs(s: i64) -> String {
    let (d, h, m) = (s / 86_400, s % 86_400 / 3600, s % 3600 / 60);
    match (d, h) {
        (0, 0) => format!("{m}m"),
        (0, _) => format!("{h}h {m}m"),
        _ => format!("{d}d {h}h"),
    }
}

/// Token health of one account: `exp` is the access token's expiry, if known.
fn token_status(exp: Option<i64>, has_refresh: bool, now: i64) -> (Status, String) {
    match exp {
        None => (Status::Warn, "no readable expiry".into()),
        Some(e) if e <= now && has_refresh => (
            Status::Warn,
            format!(
                "access token expired {} ago (a refresh token is present)",
                fmt_secs(now - e)
            ),
        ),
        Some(e) if e <= now => (
            Status::Fail,
            format!(
                "access token expired {} ago and there is no refresh token",
                fmt_secs(now - e)
            ),
        ),
        Some(e) if e - now < TOKEN_WARN_SECS => (
            Status::Warn,
            format!("access token expires in {}", fmt_secs(e - now)),
        ),
        Some(e) => (
            Status::Pass,
            format!("access token valid {}", fmt_secs(e - now)),
        ),
    }
}

/// Auth directory and token expiry, from the files only (the tokens are decoded, never
/// verified, never refreshed).
fn account_files(cfg: &Config) -> Vec<Check> {
    let dir = &cfg.auth_dir;
    let shown = crate::host::shown(dir);
    let skip_expiry = |why: String| skip("accounts.expiry", why);
    let dir_fail = |why: String| {
        vec![
            fail("paths.auth_dir", why).next(format!(
                "give uid:gid {} read access to {shown}",
                crate::host::ids()
            )),
            skip_expiry("the account directory is not readable".into()),
        ]
    };
    match std::fs::metadata(dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return vec![
            warn(
                "paths.auth_dir",
                format!("{shown} does not exist (the service creates it when it starts)"),
            )
            .next("add an account with `ciallopa login`, or point auth_dir at the right directory"),
            skip_expiry("no account directory".into()),
        ]
        }
        Err(e) => return dir_fail(format!("cannot stat {shown}: {e}")),
        Ok(m) if !m.is_dir() => return dir_fail(format!("{shown} is not a directory")),
        Ok(_) => {}
    }
    if let Err(e) = access(dir, libc::R_OK | libc::X_OK) {
        return dir_fail(format!(
            "{shown} is not readable: {e} (owner {})",
            owner(dir)
        ));
    }
    let scan = match ciallopa_auth::file::scan_dir(dir) {
        Ok(s) => s,
        Err(e) => return dir_fail(format!("cannot read {shown}: {e}")),
    };
    let mut worst = Status::Pass;
    let mut problems = vec![];
    for (s, ..) in &scan.skipped {
        let (status, what) = match s.reason {
            SkipReason::PermissionDenied => (Status::Fail, "not readable by this user".to_string()),
            SkipReason::ReadError => (
                Status::Fail,
                format!("read error ({})", s.detail.clone().unwrap_or_default()),
            ),
            SkipReason::InvalidJson => (Status::Fail, "not valid JSON".to_string()),
            SkipReason::MissingTokens => (
                Status::Warn,
                "no access_token (a Codex CLI auth.json goes through `login --method import`)"
                    .to_string(),
            ),
            SkipReason::UnsupportedType => (
                Status::Warn,
                format!(
                    "not a Codex account file (type {})",
                    s.detail.clone().unwrap_or_default()
                ),
            ),
            SkipReason::InvalidName => (
                Status::Warn,
                "file name cannot be an account id".to_string(),
            ),
        };
        worst = worst.max(status);
        let who = match (s.uid, s.gid, s.mode) {
            (Some(u), Some(g), Some(m)) => format!(" (owner {u}:{g} {m:04o})"),
            _ => String::new(),
        };
        problems.push(format!("{}: {what}{who}", s.file));
    }
    if scan.files.is_empty() && scan.skipped.is_empty() {
        worst = Status::Warn;
        problems.push("no account files".into());
    }
    let dir_ev = json!({"dir": dir.display().to_string(), "accounts": scan.files.len(), "skipped": scan.skipped.len(), "uid": crate::host::euid()});
    let dir_check = if worst == Status::Pass {
        pass("paths.auth_dir", format!("{shown}: {} account files readable", scan.files.len()))
    } else {
        mk("paths.auth_dir", worst, problems.join("; "))
            .next(if scan.files.is_empty() {
                "add an account with `ciallopa login`"
            } else {
                "fix the files named above: chown/chmod so the service user reads them, or remove files that are not accounts"
            })
    }
    .ev(dir_ev);

    let now = unix_now();
    let mut worst = Status::Pass;
    let mut lines = vec![];
    let mut accounts = vec![];
    for f in &scan.files {
        let exp = f
            .raw
            .get("access_token")
            .and_then(Value::as_str)
            .and_then(ciallopa_auth::oauth::parse_jwt)
            .and_then(|c| c.exp)
            .or_else(|| f.expires_at().map(|t| t.timestamp()));
        let has_refresh = f
            .raw
            .get("refresh_token")
            .and_then(Value::as_str)
            .is_some_and(|t| !t.is_empty());
        let relogin = f.raw.get("needs_relogin").and_then(Value::as_bool) == Some(true);
        let (status, note) = if relogin {
            (
                Status::Fail,
                format!(
                    "the service marked it needs_relogin ({})",
                    f.raw
                        .get("relogin_reason")
                        .and_then(Value::as_str)
                        .unwrap_or("refresh failed")
                ),
            )
        } else {
            token_status(exp, has_refresh, now)
        };
        worst = worst.max(status);
        if status != Status::Pass {
            lines.push(format!("{}: {note}", f.id));
        }
        accounts.push(
            json!({"id": f.id, "status": status, "expires_in_secs": exp.map(|e| e - now),
            "has_refresh_token": has_refresh, "needs_relogin": relogin}),
        );
    }
    let expiry = if scan.files.is_empty() {
        skip_expiry("no accounts to check".into())
    } else if worst == Status::Pass {
        pass("accounts.expiry", format!("{} accounts, every access token valid for over 24h", scan.files.len()))
    } else {
        mk("accounts.expiry", worst, lines.join("; ")).next(if worst == Status::Fail {
            "log the account in again: `ciallopa login`, or the accounts page"
        } else {
            "a running service refreshes tokens itself (doctor never does): if it is stopped, start it; if it is running, check egress and the clock above"
        })
    }
    .ev(json!({"accounts": accounts}));
    vec![dir_check, expiry]
}

/// Cooldowns and status from the running service: `GET /admin/v1/accounts` with an admin key
/// of the config. No admin key, or no service, is a skip.
async fn account_state(cfg: &Config) -> Vec<Check> {
    let skip = |why: String| vec![skip("accounts.state", why)];
    let Some(key) = cfg.keys().into_iter().find(|k| k.admin) else {
        return skip(
            "no admin key in api_keys (admin = true), so the admin API cannot be read".into(),
        );
    };
    let mut addr = cfg.listen;
    if addr.ip().is_unspecified() {
        addr.set_ip(match addr {
            std::net::SocketAddr::V4(_) => std::net::Ipv4Addr::LOCALHOST.into(),
            std::net::SocketAddr::V6(_) => std::net::Ipv6Addr::LOCALHOST.into(),
        });
    }
    let opts = ClientOptions {
        ipv6: addr.is_ipv6(),
        ..Default::default()
    };
    let client = match ciallopa_upstream::build_reqwest(&opts) {
        Ok(c) => c,
        Err(e) => return skip(format!("cannot build the client: {e}")),
    };
    let res = client
        .get(format!("http://{addr}/admin/v1/accounts"))
        .bearer_auth(&key.key)
        .timeout(PROBE_TIMEOUT)
        .send()
        .await;
    let resp = match res {
        Ok(r) => r,
        Err(_) => {
            return skip(format!(
                "the service is not reachable at {addr} (not running, or listening elsewhere)"
            ))
        }
    };
    if !resp.status().is_success() {
        return skip(format!(
            "the service at {addr} answered HTTP {} to the admin key (it may run another config)",
            resp.status().as_u16()
        ));
    }
    let Ok(body) = resp.json::<Value>().await else {
        return skip(format!("the service at {addr} did not return account JSON"));
    };
    let list = body["accounts"].as_array().cloned().unwrap_or_default();
    if list.is_empty() {
        return skip("the running service has no accounts loaded".into());
    }
    let now_ms = unix_now() * 1000;
    let count = |s: &str| list.iter().filter(|a| a["status"] == s).count();
    let (normal, cooling, invalid) = (count("normal"), count("cooling"), count("token_invalid"));
    let lines: Vec<String> = list
        .iter()
        .filter(|a| matches!(a["status"].as_str(), Some("cooling" | "token_invalid")))
        .map(|a| {
            let until = a["cooling_until_ms"]
                .as_i64()
                .map(|u| format!(" for {}", fmt_secs(((u - now_ms) / 1000).max(0))));
            format!(
                "{}: {}{}{}",
                a["id"].as_str().unwrap_or("?"),
                a["status"].as_str().unwrap_or("?"),
                a["cooling_reason"]
                    .as_str()
                    .map(|r| format!(" ({r})"))
                    .unwrap_or_default(),
                until.unwrap_or_default()
            )
        })
        .collect();
    let ev = json!({"accounts": list.iter().map(|a| json!({
        "id": a["id"], "status": a["status"], "cooling_until_ms": a["cooling_until_ms"], "cooling_reason": a["cooling_reason"]
    })).collect::<Vec<_>>()});
    let total = list.len();
    vec![if normal == 0 {
        fail("accounts.state", format!("no account can serve now: {}", lines.join("; ")))
            .next("wait for the cooldown, or log the accounts in again; the accounts page shows the reason")
    } else if cooling + invalid > 0 {
        mk("accounts.state", if invalid > 0 { Status::Fail } else { Status::Warn }, format!("{normal} of {total} accounts serving; {}", lines.join("; ")))
            .next("cooling clears by itself; token_invalid needs `ciallopa login` for that account")
    } else {
        pass("accounts.state", format!("{normal} of {total} accounts serving"))
    }
    .ev(ev)]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(id: &'static str, s: Status) -> Check {
        mk(id, s, "x")
    }

    #[test]
    fn config_permissions_warn_only_with_keys_and_other_access() {
        assert!(perm_ok(0o600, true));
        assert!(perm_ok(0o640, true));
        assert!(!perm_ok(0o644, true));
        assert!(!perm_ok(0o602, true));
        assert!(perm_ok(0o644, false), "no keys, nothing to protect");
    }

    #[test]
    fn schema_version_compares_with_the_binary() {
        assert_eq!(schema_status(22, 22), Status::Pass);
        assert_eq!(schema_status(20, 22), Status::Warn);
        assert_eq!(schema_status(0, 22), Status::Warn);
        assert_eq!(schema_status(23, 22), Status::Fail);
    }

    #[test]
    fn wal_is_judged_by_size_and_ratio_with_a_floor() {
        // The normal ~4 MiB auto-checkpoint cycle is never a warning, even on a tiny database.
        assert_eq!(wal_status(4 * MIB, MIB), Status::Pass);
        assert_eq!(wal_status(15 * MIB, MIB), Status::Pass);
        assert_eq!(wal_status(16 * MIB, 30 * MIB), Status::Warn);
        assert_eq!(wal_status(16 * MIB, 40 * MIB), Status::Pass);
        assert_eq!(wal_status(64 * MIB, 10_000 * MIB), Status::Warn);
        assert_eq!(wal_status(0, 0), Status::Pass);
    }

    #[test]
    fn disk_space_thresholds() {
        assert_eq!(disk_status(50 * MIB, MIB), Status::Fail);
        assert_eq!(disk_status(500 * MIB, MIB), Status::Warn);
        assert_eq!(
            disk_status(2000 * MIB, 1500 * MIB),
            Status::Warn,
            "less than twice the database"
        );
        assert_eq!(disk_status(4000 * MIB, 1500 * MIB), Status::Pass);
    }

    /// A JWT with only a payload; the signature is never read.
    fn jwt(exp: i64) -> String {
        use base64::Engine;
        let p =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("{{\"exp\":{exp}}}"));
        format!("e30.{p}.sig")
    }

    #[test]
    fn token_exp_is_decoded_without_verifying() {
        let claims = ciallopa_auth::oauth::parse_jwt(&jwt(1_900_000_000)).unwrap();
        assert_eq!(claims.exp, Some(1_900_000_000));
        assert!(ciallopa_auth::oauth::parse_jwt("not-a-jwt").is_none());
    }

    #[test]
    fn token_status_levels() {
        let now = 1_000_000;
        assert_eq!(
            token_status(Some(now + 3 * 86_400), true, now).0,
            Status::Pass
        );
        assert_eq!(token_status(Some(now + 3600), true, now).0, Status::Warn);
        assert_eq!(
            token_status(Some(now - 60), true, now).0,
            Status::Warn,
            "expired but renewable"
        );
        assert_eq!(token_status(Some(now - 60), false, now).0, Status::Fail);
        assert_eq!(token_status(None, true, now).0, Status::Warn);
        assert!(token_status(Some(now + 3600), true, now).1.contains("1h"));
    }

    /// `openssl req -x509 -newkey ec -subj /CN=doctor-test -days 3650`: a real certificate,
    /// `notAfter=Nov 17 18:45:07 2036 GMT` (UTCTime).
    const REAL_CERT: &str = "3082016e30820114a003020102020101300a06082a8648ce3d04030230163114301206035504030c0b646f63746f722d74657374301e170d3236313132303138343530375a170d3336313131373138343530375a30163114301206035504030c0b646f63746f722d746573743059301306072a8648ce3d020106082a8648ce3d0301070342000419226e8dc9b1f9180b6bf93aae2c581b3bc37510910cb2a70b5056ee0c9482c84109f78decb98c488e91ddae0ddf2c60a4bd36ad05250cfa913887de8c12a037a3533051301d0603551d0e041604141e43403a67a8d9c7259dbb9c0eb7f7dd2a65fde0301f0603551d230418301680141e43403a67a8d9c7259dbb9c0eb7f7dd2a65fde0300f0603551d130101ff040530030101ff300a06082a8648ce3d04030203480030450221008e0f1bd3815a773cb0466e36a95095eb6c4fc91f15ad5fd90a5c7e9e95b8da4302204d36c8154fb5a86e737ac04f8465cd237c4366981f2ad14f1ec50c15e85233f0";

    #[test]
    fn certificate_not_after_is_read_from_der() {
        let real: Vec<u8> = (0..REAL_CERT.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&REAL_CERT[i..i + 2], 16).unwrap())
            .collect();
        assert_eq!(cert_not_after(&real), Some(2_110_560_307));
        // UTCTime and GeneralizedTime (dates from 2050), and garbage.
        let utc = cert_der(b"\x17\x0d261121000000Z", b"\x17\x0d361118000000Z");
        assert_eq!(cert_not_after(&utc), Some(chrono_ts("20361118000000")));
        let gen = cert_der(b"\x17\x0d261121000000Z", b"\x18\x0f20601201000000Z");
        assert_eq!(cert_not_after(&gen), Some(chrono_ts("20601201000000")));
        assert_eq!(cert_not_after(b"\x30\x03\x02\x01\x01"), None);
        assert_eq!(cert_not_after(b""), None);
    }
    fn chrono_ts(s: &str) -> i64 {
        chrono::NaiveDateTime::parse_from_str(s, "%Y%m%d%H%M%S")
            .unwrap()
            .and_utc()
            .timestamp()
    }
    fn der(tag: u8, content: &[u8]) -> Vec<u8> {
        let mut v = vec![tag];
        if content.len() < 128 {
            v.push(content.len() as u8);
        } else {
            v.extend([0x82, (content.len() >> 8) as u8, content.len() as u8]);
        }
        v.extend(content);
        v
    }
    /// Certificate ::= SEQ { TBS SEQ { [0] version, serial, sigalg, issuer, validity, … } }.
    fn cert_der(not_before: &[u8], not_after: &[u8]) -> Vec<u8> {
        let validity = der(0x30, &[not_before, not_after].concat());
        let tbs = der(
            0x30,
            &[
                der(0xA0, &der(0x02, &[2])),
                der(0x02, &[1]),
                der(0x30, &der(0x06, &[1, 2, 3])),
                der(0x30, &[]),
                validity,
                der(0x30, &[]),
            ]
            .concat(),
        );
        der(0x30, &[tbs, der(0x30, &[]), der(0x03, &[0])].concat())
    }

    #[test]
    fn exit_code_follows_the_worst_status() {
        assert_eq!(
            exit_code(&[
                check("config.parse", Status::Pass),
                check("db.open", Status::Warn),
                check("db.schema", Status::Skip)
            ]),
            0
        );
        assert_eq!(
            exit_code(&[
                check("config.parse", Status::Warn),
                check("db.open", Status::Fail),
                check("db.schema", Status::Error)
            ]),
            1
        );
        assert_eq!(
            exit_code(&[
                check("config.parse", Status::Pass),
                check("db.open", Status::Error)
            ]),
            2
        );
        assert_eq!(exit_code(&[]), 0);
    }

    #[test]
    fn json_has_the_contract_fields() {
        let checks = vec![
            mk("config.parse", Status::Pass, "ok").ev(json!({"k": 1})),
            mk("db.wal_size", Status::Warn, "big").next("close the reader"),
        ];
        let v: Value = serde_json::from_str(&render_json(&checks)).unwrap();
        assert_eq!(v["schema_version"], 1);
        assert_eq!(v["ciallopa_version"], env!("CARGO_PKG_VERSION"));
        assert!(v["generated_at"].as_str().unwrap().ends_with('Z'));
        assert_eq!(
            v["summary"],
            json!({"pass": 1, "warn": 1, "fail": 0, "skip": 0, "error": 0})
        );
        let c = &v["checks"][1];
        assert_eq!(c["id"], "db.wal_size");
        assert_eq!(c["layer"], "storage");
        assert_eq!(c["status"], "warn");
        assert_eq!(c["next"], "close the reader");
        for k in ["title", "detail", "evidence", "duration_ms"] {
            assert!(c.get(k).is_some(), "{k}");
        }
        assert!(v["checks"][0]["next"].is_null());
    }

    #[test]
    fn every_check_id_is_named_in_meta_once() {
        let ids: std::collections::HashSet<_> = META.iter().map(|m| m.0).collect();
        assert_eq!(ids.len(), META.len());
        assert!(META.iter().all(|m| LAYERS.iter().any(|l| l.0 == m.1)));
    }

    #[test]
    fn human_output_groups_by_layer_and_gives_problem_and_next() {
        let checks = vec![
            mk("config.parse", Status::Pass, "loads"),
            mk("config.perm", Status::Warn, "mode 0644").next("chmod o-rwx x"),
            mk("db.open", Status::Skip, "no db"),
        ];
        let t = render_human(&checks, Path::new("c.toml"), false);
        assert!(t.contains("\nConfig\n") && t.contains("\nStorage\n"));
        assert!(t.contains("problem: mode 0644") && t.contains("next:    chmod o-rwx x"));
        assert!(t.contains("0 fail · 1 warn · 1 pass · 1 skip"), "{t}");
    }

    #[test]
    fn proxy_credentials_are_masked_and_env_proxies_reported_as_ignored() {
        let cfg = Config::parse("proxy_url = \"socks5h://user:hunter2pw@127.0.0.1:1080\"").unwrap();
        let c = proxy_check(&cfg);
        assert_eq!(c.status, Status::Pass);
        let s = serde_json::to_string(&c).unwrap();
        assert!(s.contains("socks5h://***@127.0.0.1:1080"), "{s}");
        assert!(!s.contains("hunter2pw") && !s.contains("user:"), "{s}");
    }

    #[test]
    fn scrub_removes_api_keys() {
        let out = scrub(
            "failed with sk-secret-0123456789 here".into(),
            &["sk-secret-0123456789".into()],
        );
        assert_eq!(out, "failed with *** here");
    }

    #[test]
    fn tls_failures_are_explained() {
        assert!(tls_hint("invalid peer certificate: UnknownIssuer").contains("ca-certificates"));
        assert!(tls_hint("invalid peer certificate: NotValidYet").contains("clock"));
        assert!(looks_like_tls(
            "error trying to connect: invalid peer certificate: Expired"
        ));
        assert!(!looks_like_tls("tcp connect error: Connection refused"));
    }
}
