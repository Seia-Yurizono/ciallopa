//! Facts about the machine the binary runs on: effective uid, the packaged `ciallopa` system
//! user, container detection. `/proc` and `/etc/passwd` are read directly.
use std::path::{Path, PathBuf};

/// Name of the system user the `.deb` creates.
pub const SERVICE_USER: &str = "ciallopa";
/// Config path of the `.deb` layout.
pub const DEB_CONFIG: &str = "/etc/ciallopa/config.toml";
/// systemd unit the `.deb` installs.
const DEB_UNIT: &str = "/usr/lib/systemd/system/ciallopa.service";
/// Container paths of the two mounts in `packaging/docker/compose.yaml`, with their host side.
const COMPOSE_MOUNTS: [(&str, &str); 2] = [
    ("/etc/ciallopa", "config"),
    ("/var/lib/ciallopa/.ciallopa", "data"),
];

/// Effective uid of this process (owner of `/proc/self`).
pub fn euid() -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata("/proc/self").ok().map(|m| m.uid())
}

/// Effective gid of this process (group of `/proc/self`).
pub fn egid() -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata("/proc/self").ok().map(|m| m.gid())
}

/// `uid:gid` of this process for messages (`?` when unknown).
pub fn ids() -> String {
    let show = |v: Option<u32>| v.map(|v| v.to_string()).unwrap_or_else(|| "?".into());
    format!("{}:{}", show(euid()), show(egid()))
}

/// Files and directories this process creates are private to its user (as `UMask=0077` in the
/// systemd unit), whatever umask the shell or container runtime passed on.
pub fn restrict_umask() {
    // SAFETY: umask(2) only swaps the process file-mode mask and cannot fail.
    unsafe {
        libc::umask(0o077);
    }
}

/// A local user account from a passwd-format file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemUser {
    pub uid: u32,
    pub gid: u32,
    pub home: PathBuf,
}

/// Look `name` up in a passwd-format text.
pub fn find_user(passwd: &str, name: &str) -> Option<SystemUser> {
    passwd.lines().find_map(|l| {
        let f: Vec<&str> = l.split(':').collect();
        (f.len() >= 7 && f[0] == name).then(|| {
            Some(SystemUser {
                uid: f[2].parse().ok()?,
                gid: f[3].parse().ok()?,
                home: PathBuf::from(f[5]),
            })
        })?
    })
}

/// The `ciallopa` system user from `/etc/passwd`, when the package created it.
pub fn service_user() -> Option<SystemUser> {
    find_user(&std::fs::read_to_string("/etc/passwd").ok()?, SERVICE_USER)
}

/// The `.deb` is installed here: its system user exists along with `/etc/ciallopa` or its unit.
pub fn deb_installed() -> bool {
    service_user().is_some()
        && (Path::new("/etc/ciallopa").is_dir() || Path::new(DEB_UNIT).exists())
}

/// Running in an application container (Docker, Podman, containerd, Kubernetes), where ports
/// are published from outside. System containers (LXC, OpenVZ, systemd-nspawn VPSes) do not
/// count: they behave like a host for networking.
pub fn in_container() -> bool {
    container_signals(
        Path::new("/.dockerenv").exists(),
        Path::new("/run/.containerenv").exists(),
        std::env::var_os(crate::init::LISTEN_ENV).is_some(),
        &std::fs::read_to_string("/proc/self/cgroup").unwrap_or_default(),
    )
}

/// [`in_container`] from its inputs: the Docker / Podman marker files, the image's init
/// variable, or a cgroup path (cgroup v1, or v2 without a cgroup namespace) naming an
/// application container runtime.
pub fn container_signals(
    dockerenv: bool,
    containerenv: bool,
    image_env: bool,
    cgroup: &str,
) -> bool {
    const RUNTIMES: [&str; 4] = ["docker", "containerd", "kubepods", "libpod"];
    dockerenv
        || containerenv
        || image_env
        || cgroup.lines().any(|l| {
            l.splitn(3, ':')
                .nth(2)
                .is_some_and(|path| RUNTIMES.iter().any(|r| path.contains(r)))
        })
}

/// The host side of `p` when it lies in one of the example compose file's mounts, for messages
/// shown inside the container (`config/config.toml` for `/etc/ciallopa/config.toml`).
pub fn compose_host_path(p: &Path) -> Option<String> {
    if !in_container() || service_user().is_some() {
        return None;
    }
    compose_host_side(p)
}

fn compose_host_side(p: &Path) -> Option<String> {
    COMPOSE_MOUNTS.iter().find_map(|(inside, host)| {
        let rest = p.strip_prefix(inside).ok()?;
        Some(if rest.as_os_str().is_empty() {
            format!("{host}/")
        } else {
            format!("{host}/{}", rest.display())
        })
    })
}

/// `p`, followed by its host side when inside a container with the example compose mounts.
pub fn shown(p: &Path) -> String {
    match compose_host_path(p) {
        Some(h) => format!(
            "{} ({h} on the host, with the example compose file)",
            p.display()
        ),
        None => p.display().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passwd_lookup() {
        let p = "root:x:0:0:root:/root:/bin/bash\nciallopa:x:998:997:ciallopa proxy:/var/lib/ciallopa:/usr/sbin/nologin\nbroken:x\n";
        assert_eq!(
            find_user(p, "ciallopa"),
            Some(SystemUser {
                uid: 998,
                gid: 997,
                home: "/var/lib/ciallopa".into()
            })
        );
        assert_eq!(find_user(p, "nobody"), None);
        assert_eq!(find_user(p, "broken"), None);
    }

    #[test]
    fn container_detection_ignores_system_containers() {
        // LXC / OpenVZ / nspawn VPS running the .deb under systemd: no markers, plain cgroups.
        assert!(!container_signals(
            false,
            false,
            false,
            "0::/system.slice/ciallopa.service\n"
        ));
        assert!(!container_signals(false, false, false, "0::/\n"));
        assert!(!container_signals(
            false,
            false,
            false,
            "12:pids:/lxc/101/system.slice/ciallopa.service\n11:memory:/lxc/101\n"
        ));
        assert!(!container_signals(false, false, false, ""));
        // Application containers.
        assert!(container_signals(true, false, false, "0::/\n"));
        assert!(container_signals(false, true, false, "0::/\n"));
        assert!(container_signals(false, false, true, ""));
        assert!(container_signals(
            false,
            false,
            false,
            "12:pids:/docker/4f1c2e9a0b\n0::/system.slice/docker-4f1c.scope\n"
        ));
        assert!(container_signals(
            false,
            false,
            false,
            "0::/kubepods.slice/kubepods-burstable.slice/cri-containerd-ab12.scope\n"
        ));
        assert!(container_signals(
            false,
            false,
            false,
            "0::/machine.slice/libpod-9e2f.scope/container\n"
        ));
    }

    #[test]
    fn compose_host_side_maps_the_two_mounts() {
        assert_eq!(
            compose_host_side(Path::new("/etc/ciallopa/config.toml")).as_deref(),
            Some("config/config.toml")
        );
        assert_eq!(
            compose_host_side(Path::new("/var/lib/ciallopa/.ciallopa/auths")).as_deref(),
            Some("data/auths")
        );
        assert_eq!(
            compose_host_side(Path::new("/var/lib/ciallopa/.ciallopa")).as_deref(),
            Some("data/")
        );
        assert_eq!(compose_host_side(Path::new("/var/lib/ciallopa/x")), None);
        assert_eq!(compose_host_side(Path::new("/etc/ciallopa2/a")), None);
    }
}
