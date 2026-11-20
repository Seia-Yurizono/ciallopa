//! `ciallopa doctor` end to end: the compiled binary against a temp data directory and a local
//! mock upstream. It must leave every file as it found it, send no credentials, and put no
//! secret in its output.
#![cfg(unix)]
#![allow(clippy::unwrap_used)]
use axum::{
    http::{HeaderMap, Method, StatusCode, Uri},
    response::IntoResponse,
    Router,
};
use base64::Engine;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

const ADMIN_KEY: &str = "sk-doctor-admin-key-0123456789";
const REFRESH: &str = "rt-doctor-refresh-secret";

fn jwt(exp: i64) -> String {
    let p = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!(r#"{{"exp":{exp}}}"#));
    format!("e30.{p}.doctor-signature")
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

type Seen = Arc<Mutex<Vec<(String, String, bool)>>>;

/// Mock upstream: records `(method, path, has Authorization)`; answers the TLS probe's trace
/// path with 200 and the Responses path with 401, as the real upstream does without credentials.
async fn mock_upstream() -> (String, Seen) {
    let seen = Seen::default();
    let log = seen.clone();
    let app = Router::new().fallback(move |m: Method, u: Uri, h: HeaderMap| {
        let log = log.clone();
        async move {
            log.lock().unwrap().push((
                m.to_string(),
                u.path().to_string(),
                h.contains_key("authorization"),
            ));
            if u.path().ends_with("/codex/responses") {
                StatusCode::UNAUTHORIZED.into_response()
            } else {
                "ip=127.0.0.1\n".into_response()
            }
        }
    });
    (super::support::serve(app).await, seen)
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Config, two accounts (one healthy, one expired without a refresh token) and a cleanly
/// closed usage database (no `-wal` / `-shm`), all under `dir`.
fn fixture(dir: &Path, upstream: &str, port: u16) -> std::path::PathBuf {
    let auth = dir.join("auths");
    std::fs::create_dir(&auth).unwrap();
    for (name, exp, refresh) in [
        ("codex-ok", now() + 3 * 86_400, true),
        ("codex-gone", now() - 3600, false),
    ] {
        let mut v = serde_json::json!({"type": "codex", "email": format!("{name}@example.invalid"),
            "access_token": jwt(exp), "account_id": name});
        if refresh {
            v["refresh_token"] = REFRESH.into();
        }
        std::fs::write(auth.join(format!("{name}.json")), v.to_string()).unwrap();
    }
    let usage = ciallopa_core::config::UsageConfig {
        path: dir.join("usage.db"),
        ..Default::default()
    };
    let rec = ciallopa_usage::Recorder::open_as(&usage, "doctor-test").unwrap();
    rec.shutdown();
    drop(rec);
    let c = rusqlite::Connection::open(&usage.path).unwrap();
    c.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
        .unwrap();
    drop(c);
    let path = dir.join("config.toml");
    std::fs::write(
        &path,
        format!(
            r#"listen = "127.0.0.1:{port}"
api_keys = [{{ label = "admin", key = "{ADMIN_KEY}", admin = true }}]
upstream = "http://{upstream}/backend-api"
auth_dir = "{}"
[usage]
path = "{}"
[ui]
reset_credits_refresh_mins = 0
"#,
            auth.display(),
            usage.path.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o644)).unwrap();
    path
}

/// Every file under `dir` with its size and mtime.
fn snapshot(dir: &Path) -> BTreeMap<String, (u64, std::time::SystemTime)> {
    fn walk(dir: &Path, out: &mut BTreeMap<String, (u64, std::time::SystemTime)>) {
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            let m = std::fs::metadata(&p).unwrap();
            if m.is_dir() {
                walk(&p, out);
            }
            out.insert(p.display().to_string(), (m.len(), m.modified().unwrap()));
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, &mut out);
    out
}

async fn doctor(config: &Path, args: &[&str]) -> (i32, String) {
    let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_ciallopa"))
        .arg("-c")
        .arg(config)
        .arg("doctor")
        .args(args)
        .output()
        .await
        .unwrap();
    (
        out.status.code().unwrap(),
        String::from_utf8(out.stdout).unwrap(),
    )
}

fn status<'a>(report: &'a Value, id: &str) -> &'a str {
    report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["id"] == id)
        .unwrap_or_else(|| panic!("no check {id}"))["status"]
        .as_str()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn doctor_reports_without_writing_or_leaking() {
    let dir = tempfile::tempdir().unwrap();
    let (upstream, seen) = mock_upstream().await;
    let config = fixture(dir.path(), &upstream, free_port());
    let before = snapshot(dir.path());

    let (code, out) = doctor(&config, &["--json", "--full"]).await;
    let report: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(report["schema_version"], 1);
    let want = [
        ("config.parse", "pass"),
        ("config.perm", "warn"), // 0644 with an api key
        ("paths.auth_dir", "pass"),
        ("paths.data_dir", "pass"),
        ("db.open", "pass"),
        ("db.schema", "pass"),
        ("db.quick_check", "pass"),
        ("db.wal_size", "pass"),
        ("db.disk_free", "pass"),
        ("egress.proxy", "pass"),
        ("egress.connect", "pass"),
        ("upstream.tls", "skip"), // plain http mock
        ("upstream.clock", "pass"),
        ("upstream.ws", "pass"),     // 401 without credentials
        ("accounts.expiry", "fail"), // codex-gone: expired, no refresh token
        ("accounts.state", "skip"),  // no service listening
    ];
    for (id, s) in want {
        assert_eq!(status(&report, id), s, "{id}: {report:#}");
    }
    assert_eq!(report["checks"].as_array().unwrap().len(), want.len());
    assert_eq!(code, 1, "a failing check exits 1");
    let expiry = &report["checks"][14];
    assert!(expiry["detail"].as_str().unwrap().contains("codex-gone"));
    assert!(!expiry["detail"].as_str().unwrap().contains("codex-ok"));

    // Human output and the same exit code; nothing secret in either.
    let (code_h, human) = doctor(&config, &["-v"]).await;
    assert_eq!(code_h, 1);
    assert!(
        human.contains("problem:") && human.contains("next:"),
        "{human}"
    );
    for secret in [ADMIN_KEY, REFRESH, "doctor-signature"] {
        assert!(!out.contains(secret) && !human.contains(secret), "{secret}");
    }

    // Read-only: not a byte, mtime or new file (no -wal/-shm) under the data directory.
    assert_eq!(snapshot(dir.path()), before);
    // Credential-free upstream traffic only, and never a model call.
    let seen = seen.lock().unwrap();
    assert!(!seen.is_empty());
    for (method, path, auth) in seen.iter() {
        assert_eq!(method, "GET", "{path}");
        assert!(!auth, "{path} carried credentials");
        assert!(path.ends_with("/cdn-cgi/trace") || path.ends_with("/codex/responses"));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn doctor_reads_a_running_service_and_a_missing_config_fails() {
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, BufReader};
    let dir = tempfile::tempdir().unwrap();
    let (upstream, _) = mock_upstream().await;
    let port = free_port();
    let config = fixture(dir.path(), &upstream, port);
    let mut serve = tokio::process::Command::new(env!("CARGO_BIN_EXE_ciallopa"))
        .arg("-c")
        .arg(&config)
        .arg("serve")
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(serve.stderr.take().unwrap()).lines();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while let Some(l) = lines.next_line().await.unwrap() {
            if l.contains("ciallopa listening") {
                return;
            }
        }
        panic!("serve exited early");
    })
    .await
    .unwrap();

    let (_, out) = doctor(&config, &["--json"]).await;
    let report: Value = serde_json::from_str(&out).unwrap();
    // The service's -wal/-shm exist: a plain read-only open reads the live database.
    assert_eq!(status(&report, "db.open"), "pass", "{report:#}");
    assert_eq!(status(&report, "db.schema"), "pass");
    // Both accounts are `normal` for the service: it does not look at token expiry.
    assert_eq!(status(&report, "accounts.state"), "pass", "{report:#}");
    assert!(!out.contains(ADMIN_KEY));

    let (code, out) = doctor(&dir.path().join("missing.toml"), &["--json"]).await;
    let report: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(code, 1);
    assert_eq!(status(&report, "config.parse"), "fail");
    assert_eq!(status(&report, "db.open"), "skip");
}
