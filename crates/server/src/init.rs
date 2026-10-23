//! `ciallopa init`: write a first config from the bundled example with a random admin key.
//! Everything the user needs next (the key, the Codex client snippet, the next commands) goes
//! to stdout of this command only, never to the log.
use crate::host;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// Overrides `listen` in the generated file (the container image sets `0.0.0.0:8317`).
pub const LISTEN_ENV: &str = "CIALLOPA_INIT_LISTEN";

const EXAMPLE: &str = include_str!("../../../config.example.toml");
const EXAMPLE_LISTEN: &str = "listen = \"127.0.0.1:8317\"";

/// Where the config is used from; decides the commands printed after it is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    /// The `.deb`: `/etc/ciallopa/config.toml`, service user `ciallopa`.
    Deb,
    /// A container (Docker Compose).
    Container,
    /// Anything else (tarball).
    Plain,
}

#[derive(Debug)]
pub struct Generated {
    pub text: String,
    pub key: String,
    pub listen: SocketAddr,
}

/// The example config with `api_keys` replaced by one admin key (and `listen` by `listen`);
/// comments and layout stay as they are.
pub fn render(key: &str, listen: Option<SocketAddr>) -> Result<Generated, String> {
    let mut keys = 0;
    let mut listens = 0;
    let mut out = String::with_capacity(EXAMPLE.len() + 64);
    for line in EXAMPLE.lines() {
        if line.starts_with("api_keys = ") {
            keys += 1;
            out.push_str(&format!(
                "api_keys = [{{ label = \"admin\", key = \"{key}\", admin = true }}]"
            ));
        } else if line == EXAMPLE_LISTEN && listen.is_some() {
            listens += 1;
            out.push_str(&format!(
                "listen = \"{}\"",
                listen.unwrap_or_else(default_listen)
            ));
        } else {
            out.push_str(line);
        }
        out.push('\n');
    }
    if keys != 1 || (listen.is_some() && listens != 1) {
        return Err("bundled example config has an unexpected layout".into());
    }
    let cfg = ciallopa_core::Config::parse(&out).map_err(|e| format!("generated config: {e}"))?;
    Ok(Generated {
        text: out,
        key: key.to_string(),
        listen: cfg.listen,
    })
}

fn default_listen() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 8317))
}

/// `listen` from [`LISTEN_ENV`]: `None` when unset or empty.
pub fn listen_override() -> Result<Option<SocketAddr>, String> {
    match std::env::var(LISTEN_ENV) {
        Ok(v) if !v.trim().is_empty() => v
            .trim()
            .parse()
            .map(Some)
            .map_err(|_| format!("{LISTEN_ENV}={v:?} is not an address like 0.0.0.0:8317")),
        _ => Ok(None),
    }
}

pub fn layout(path: &Path) -> Layout {
    // The image uses the same config path as the package but has no `ciallopa` user; the
    // package may itself be installed inside a container, which is then used like a host.
    let packaged = host::service_user().is_some();
    if path == Path::new(host::DEB_CONFIG) && packaged {
        Layout::Deb
    } else if host::in_container() && !packaged {
        Layout::Container
    } else {
        Layout::Plain
    }
}

/// Write `text` to `path` with mode 0600, refusing to replace an existing file. A missing
/// parent directory is created with mode 0700. A permission error names the fix for `layout`.
pub fn write_new(path: &Path, text: &str, layout: Layout) -> Result<(), String> {
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
    let denied = |e: &std::io::Error, what: &Path| {
        if e.kind() == std::io::ErrorKind::PermissionDenied {
            format!(
                "cannot write {}: permission denied (running as uid:gid {}); {}",
                host::shown(what),
                host::ids(),
                write_fix(layout)
            )
        } else {
            format!("cannot write {}: {e}", host::shown(what))
        }
    };
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        if !dir.exists() {
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(dir)
                .map_err(|e| denied(&e, dir))?;
        }
    }
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                format!(
                    "config file {} already exists; not overwritten",
                    path.display()
                )
            } else {
                denied(&e, path)
            }
        })?;
    f.write_all(text.as_bytes())
        .and_then(|_| f.sync_all())
        .map_err(|e| {
            let _ = std::fs::remove_file(path);
            denied(&e, path)
        })
}

/// How to get write access for `init`, per layout.
fn write_fix(layout: Layout) -> String {
    match layout {
        Layout::Deb => format!(
            "run it with sudo, which hands the new file to the {u} user:\n  sudo ciallopa -c {p} init",
            u = host::SERVICE_USER,
            p = host::DEB_CONFIG
        ),
        Layout::Container => format!(
            "the mounted directories must belong to the uid:gid in the compose file's `user:` line. \
On the host, next to compose.yaml, give them to that user:\n  sudo chown {ids} config data\n\
or set `user:` to the uid:gid that owns them (ls -nd config data)",
            ids = host::ids()
        ),
        Layout::Plain => "make the directory writable by this user".to_string(),
    }
}

/// How the config named for a command turned out to be unusable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unusable {
    Missing,
    Denied,
}

/// The deb config's state as seen by this process: `Some(true)` exists, `Some(false)` missing,
/// `None` unknown (not visible to this user).
fn deb_config_state() -> Option<bool> {
    match std::fs::metadata(host::DEB_CONFIG) {
        Ok(_) => Some(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Some(false),
        Err(_) => None,
    }
}

/// The error text for a command that cannot use its config file.
/// `path` is the config path (absolute), `named` says whether it came from `-c` /
/// `CIALLOPA_CONFIG` (else the default `./config.toml`), `flag` is the `-c` the user typed,
/// and `rest` the command line after the program name with the config option removed.
pub fn config_error(
    path: &Path,
    named: bool,
    flag: Option<&Path>,
    what: Unusable,
    rest: &[String],
) -> String {
    let deb_box = host::deb_installed();
    let deb_layout = path == Path::new(host::DEB_CONFIG) && host::service_user().is_some();
    let serving = rest.is_empty() || rest[0] == "serve" || rest[0].starts_with('-');
    let deb_cmd = deb_command(serving, rest);
    let mut out = match what {
        Unusable::Denied => format!(
            "error: cannot read {}: permission denied (running as uid:gid {})",
            host::shown(path),
            host::ids()
        ),
        Unusable::Missing if !named && deb_box => format!(
            "error: no config file at {} (without -c, ciallopa reads config.toml in the current directory)",
            path.display()
        ),
        Unusable::Missing => format!("error: no config file at {}", host::shown(path)),
    };
    let then = if serving {
        "start the service, which runs the server".to_string()
    } else {
        format!("run the command as the {} user", host::SERVICE_USER)
    };
    match what {
        Unusable::Denied if deb_layout => out.push_str(&format!(
            "\n/etc/ciallopa belongs to the {u} service user; {then}:\n  {deb_cmd}",
            u = host::SERVICE_USER
        )),
        Unusable::Denied if host::in_container() && host::service_user().is_none() => {
            out.push_str(&format!("; {}", write_fix(Layout::Container)))
        }
        Unusable::Denied => out.push_str(
            "; run the command as the user that owns the file, or make it readable for this user",
        ),
        Unusable::Missing if !named && deb_box => {
            let init = format!("sudo ciallopa -c {} init", host::DEB_CONFIG);
            out.push_str(&format!(
                "\nThe {} service uses {}",
                host::SERVICE_USER,
                host::DEB_CONFIG
            ));
            match deb_config_state() {
                Some(true) => out.push_str(&format!("; {then}:\n  {deb_cmd}")),
                Some(false) => out.push_str(&format!(
                    ", which does not exist yet. Create it:\n  {init}\nthen {then}:\n  {deb_cmd}"
                )),
                None => out.push_str(&format!(
                    ". If it does not exist yet, create it:\n  {init}\nthen {then}:\n  {deb_cmd}"
                )),
            }
        }
        Unusable::Missing if deb_layout => out.push_str(&format!(
            "; create it with \"sudo ciallopa -c {} init\"",
            host::DEB_CONFIG
        )),
        Unusable::Missing => {
            let init = match flag {
                Some(p) => format!("ciallopa -c {} init", p.display()),
                None => "ciallopa init".to_string(),
            };
            out.push_str(&format!("; run \"{init}\""));
            if layout(path) == Layout::Container {
                out.push_str("\n(with Docker Compose: docker compose run --rm ciallopa init)");
            }
        }
    }
    out
}

/// The same command for the `.deb` layout: as the service user with its config, or the
/// service itself for the server.
fn deb_command(serving: bool, rest: &[String]) -> String {
    if serving {
        return "sudo systemctl start ciallopa".to_string();
    }
    let args: Vec<String> = rest.iter().map(|a| shell_quote(a)).collect();
    format!(
        "sudo -u {u} HOME=/var/lib/{u} ciallopa -c {p} {}",
        args.join(" "),
        u = host::SERVICE_USER,
        p = host::DEB_CONFIG
    )
}

fn shell_quote(a: &str) -> String {
    let plain = !a.is_empty()
        && a.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_./:=@%+-,~".contains(&b));
    if plain {
        a.to_string()
    } else {
        format!("'{}'", a.replace('\'', r"'\''"))
    }
}

/// The command line after the program name without the config option (`-c X`, `-cX`,
/// `--config X`, `--config=X`).
pub fn args_without_config(args: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if a == "-c" || a == "--config" {
            it.next();
        } else if a.starts_with("--config=")
            || (a.starts_with("-c") && a.len() > 2 && !a.starts_with("--"))
        {
        } else {
            out.push(a.clone());
        }
    }
    out
}

/// As root for the `.deb` path: hand the file to the service user.
fn chown_for_service(path: &Path) -> Result<(), String> {
    if host::euid() != Some(0) {
        return Ok(());
    }
    let Some(u) = host::service_user() else {
        return Ok(());
    };
    std::os::unix::fs::chown(path, Some(u.uid), Some(u.gid))
        .map_err(|e| format!("chown {} to {}: {e}", path.display(), host::SERVICE_USER))
}

/// `ciallopa init` for `path`: returns the text to print.
pub fn run(path: &Path) -> Result<String, String> {
    let listen = listen_override()?;
    let g = render(&crate::settings::gen_key(), listen)?;
    let abs = absolute(path);
    let layout = layout(&abs);
    write_new(&abs, &g.text, layout)?;
    if layout == Layout::Deb {
        chown_for_service(&abs)?;
    }
    Ok(instructions(&abs, &g, layout))
}

fn absolute(p: &Path) -> PathBuf {
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|d| d.join(p))
            .unwrap_or_else(|_| p.to_path_buf())
    }
}

/// What to do next: the key, the Codex client config, and the commands for `layout`.
pub fn instructions(path: &Path, g: &Generated, layout: Layout) -> String {
    let port = g.listen.port();
    let (login, start) = match layout {
        Layout::Deb => (
            format!(
                "sudo -u {u} HOME=/var/lib/{u} ciallopa -c {p} login",
                u = host::SERVICE_USER,
                p = host::DEB_CONFIG
            ),
            "sudo systemctl start ciallopa".to_string(),
        ),
        Layout::Container => (
            "docker compose exec ciallopa ciallopa login".to_string(),
            "docker compose up -d".to_string(),
        ),
        Layout::Plain => (
            format!("ciallopa -c {} login", path.display()),
            format!("ciallopa -c {}", path.display()),
        ),
    };
    let steps = match layout {
        Layout::Container => format!("  {start}\n  {login}"),
        _ => format!("  {login}\n  {start}"),
    };
    // A config in the current directory on a machine with the package: the service reads
    // another file.
    let deb_note = if layout == Layout::Plain && host::deb_installed() {
        format!(
            "\nThe {u} service reads {p}, not this file. To set up the service instead:\n  sudo ciallopa -c {p} init\n",
            u = host::SERVICE_USER,
            p = host::DEB_CONFIG
        )
    } else {
        String::new()
    };
    format!(
        r#"Wrote {path} (mode 0600{host}) with a new admin API key:

  {key}

Clients send it as the API key, and it opens the management pages. It is stored in the file
under api_keys; keep it secret.

Codex client, $CODEX_HOME/config.toml (default ~/.codex/config.toml):

model_provider = "cpa"

[model_providers.cpa]
name = "cpa"
base_url = "http://127.0.0.1:{port}/v1"   # or https://<your domain>/v1 behind your reverse proxy
env_key = "CPA_KEY"
wire_api = "responses"
supports_websockets = true

[model_providers.cpa.capabilities]
remote_compaction = "v2"

Give Codex the key in $CODEX_HOME/.env (default ~/.codex/.env), or export it in your shell profile:

CPA_KEY={key}

Restart a running Codex app-server daemon so it reads the new key:

  codex app-server daemon restart

Management pages: http://127.0.0.1:{port}/ui
(from another machine: ssh -L {port}:127.0.0.1:{port} <server>, then open the same URL)

Next:
{steps}
{deb_note}"#,
        path = path.display(),
        host = host::compose_host_path(path)
            .map(|h| format!("; {h} on the host, with the example compose file"))
            .unwrap_or_default(),
        key = g.key,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_replaces_only_key_and_listen() {
        let g = render("sk-cpa-0123456789abcdef0123", None).unwrap();
        assert!(g.text.contains(
            "api_keys = [{ label = \"admin\", key = \"sk-cpa-0123456789abcdef0123\", admin = true }]"
        ));
        assert!(g.text.contains(EXAMPLE_LISTEN));
        assert!(!g.text.contains(ciallopa_core::config::EXAMPLE_API_KEY));
        // Comments survive; everything but the key line is unchanged.
        let diff: Vec<_> = EXAMPLE
            .lines()
            .zip(g.text.lines())
            .filter(|(a, b)| a != b)
            .collect();
        assert_eq!(diff.len(), 1, "{diff:?}");
        assert_eq!(EXAMPLE.lines().count(), g.text.lines().count());
        assert!(g.text.contains("# Plain strings may only forward requests"));
        let c = ciallopa_core::Config::parse(&g.text).unwrap();
        assert_eq!(c.keys().len(), 1);
        assert!(c.keys()[0].admin);
        assert_eq!(c.keys()[0].label, "admin");

        let g = render(
            "sk-cpa-0123456789abcdef0123",
            Some("0.0.0.0:8317".parse().unwrap()),
        )
        .unwrap();
        assert!(g.text.contains("listen = \"0.0.0.0:8317\""));
        assert_eq!(g.listen.to_string(), "0.0.0.0:8317");
    }

    #[test]
    fn write_new_refuses_to_overwrite_and_sets_modes() {
        use std::os::unix::fs::PermissionsExt;
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("sub").join("config.toml");
        write_new(&p, "a = 1\n", Layout::Plain).unwrap();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&p), 0o600);
        assert_eq!(mode(p.parent().unwrap()), 0o700);
        let e = write_new(&p, "b = 2\n", Layout::Plain).unwrap_err();
        assert!(e.contains("already exists; not overwritten"), "{e}");
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "a = 1\n");
    }

    #[test]
    fn instructions_name_the_layout_commands() {
        let g = render("sk-cpa-0123456789abcdef0123", None).unwrap();
        let deb = instructions(Path::new(host::DEB_CONFIG), &g, Layout::Deb);
        assert!(deb.contains(
            "sudo -u ciallopa HOME=/var/lib/ciallopa ciallopa -c /etc/ciallopa/config.toml login"
        ));
        assert!(deb.contains("sudo systemctl start ciallopa"));
        assert!(deb.contains("CPA_KEY=sk-cpa-0123456789abcdef0123"));
        assert!(deb.contains("codex app-server daemon restart"));
        assert!(deb.contains("remote_compaction = \"v2\""));
        let c = instructions(
            Path::new("/etc/ciallopa/config.toml"),
            &g,
            Layout::Container,
        );
        assert!(c.contains("docker compose exec ciallopa ciallopa login"));
        let t = instructions(Path::new("/home/u/cpa/config.toml"), &g, Layout::Plain);
        assert!(t.contains("ciallopa -c /home/u/cpa/config.toml login"));
    }

    #[test]
    fn write_fix_per_layout() {
        assert!(write_fix(Layout::Deb).contains("sudo ciallopa -c /etc/ciallopa/config.toml init"));
        let c = write_fix(Layout::Container);
        assert!(
            c.contains("sudo chown ") && c.contains(" config data"),
            "{c}"
        );
        assert!(c.contains("`user:`"), "{c}");
    }

    #[test]
    fn deb_command_repeats_the_subcommand() {
        let v = |a: &[&str]| a.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let rest = args_without_config(&v(&[
            "-c",
            "/etc/ciallopa/config.toml",
            "login",
            "--method",
            "paste",
        ]));
        assert_eq!(rest, v(&["login", "--method", "paste"]));
        assert_eq!(
            args_without_config(&v(&["--config=/x", "healthcheck"])),
            v(&["healthcheck"])
        );
        assert_eq!(
            args_without_config(&v(&["-c/x", "--config", "/y", "login"])),
            v(&["login"])
        );
        assert_eq!(
            deb_command(false, &rest),
            "sudo -u ciallopa HOME=/var/lib/ciallopa ciallopa -c /etc/ciallopa/config.toml login --method paste"
        );
        assert_eq!(
            deb_command(false, &v(&["login", "--method", "import", "--from", "/tmp/a b.json"])),
            "sudo -u ciallopa HOME=/var/lib/ciallopa ciallopa -c /etc/ciallopa/config.toml login --method import --from '/tmp/a b.json'"
        );
        assert!(deb_command(true, &[]).starts_with("sudo systemctl start ciallopa"));
        assert_eq!(shell_quote("it's"), r"'it'\''s'");
    }
}
