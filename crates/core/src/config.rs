//! TOML config (see README "Config example (v3)").
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("read {0}: {1}")]
    Read(PathBuf, std::io::Error),
    #[error("parse: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum Strategy {
    #[default]
    RoundRobin,
    WeightedRoundRobin,
    FillFirst,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum RebindOn {
    Any,
    #[default]
    Quota,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoutingConfig {
    pub strategy: Strategy,
    pub session_affinity: bool,
    /// e.g. "1h", "30m", "90s".
    pub session_affinity_ttl: String,
    pub session_affinity_subagents: bool,
    pub session_affinity_rebind_on: RebindOn,
    /// Used fraction (0..=1) at/over which an account gets no *new* sessions.
    pub quota_avoid_threshold: f64,
}
impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            strategy: Strategy::RoundRobin,
            session_affinity: true,
            session_affinity_ttl: "1h".into(),
            session_affinity_subagents: true,
            session_affinity_rebind_on: RebindOn::Quota,
            quota_avoid_threshold: 0.99,
        }
    }
}
impl RoutingConfig {
    pub fn ttl(&self) -> Result<Duration, ConfigError> {
        parse_duration(&self.session_affinity_ttl)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UpstreamOpts {
    pub stream_idle_timeout_secs: u64,
    /// Number of *retries* after the first attempt for transport errors / 408 / 5xx, before any
    /// byte reaches the client (Codex `request_max_retries`): default 4 ⇒ up to 5 requests.
    /// 401/403/404/429 failovers to another account do not consume retries.
    pub max_attempts: u32,
    pub connect_timeout_secs: u64,
}
impl Default for UpstreamOpts {
    fn default() -> Self {
        Self {
            stream_idle_timeout_secs: 300,
            max_attempts: 4,
            connect_timeout_secs: 30,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ImagesConfig {
    pub enabled: bool,
    pub main_model: String,
    /// `originator` sent for Images API requests from non-Codex clients (absent header only).
    pub default_originator: String,
    /// `User-Agent` for non-Codex clients (used when the client UA does not start with "codex").
    pub default_user_agent: String,
}
/// Codex CLI version the network stack is pinned to (see Cargo.toml / scripts/fp-check.sh).
pub const PINNED_CODEX_VERSION: &str = "0.162.1";
impl Default for ImagesConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            main_model: "gpt-5.4-mini".into(),
            default_originator: "codex_exec".into(),
            default_user_agent: format!("codex_exec/{PINNED_CODEX_VERSION}"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogConfig {
    pub level: String,
}
impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub listen: SocketAddr,
    /// Static client keys (Bearer or x-api-key). Empty = reject everything but /healthz.
    pub api_keys: Vec<String>,
    pub upstream: String,
    pub auth_dir: PathBuf,
    /// Global egress proxy: http://, https://, socks5://, socks5h://. Empty = direct.
    pub proxy_url: String,
    pub max_body_mb: u64,
    pub routing: RoutingConfig,
    pub upstream_opts: UpstreamOpts,
    pub images: ImagesConfig,
    pub log: LogConfig,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 8317)),
            api_keys: vec![],
            upstream: "https://chatgpt.com/backend-api".into(),
            auth_dir: PathBuf::from("~/.cpa-lite/auths"),
            proxy_url: String::new(),
            max_body_mb: 16,
            routing: RoutingConfig::default(),
            upstream_opts: UpstreamOpts::default(),
            images: ImagesConfig::default(),
            log: LogConfig::default(),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let s = std::fs::read_to_string(path).map_err(|e| ConfigError::Read(path.into(), e))?;
        Self::parse(&s)
    }
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        let mut c: Config = toml::from_str(s)?;
        c.auth_dir = expand_tilde(&c.auth_dir);
        c.validate()?;
        Ok(c)
    }
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.routing.ttl()?;
        if !(0.0..=1.0).contains(&self.routing.quota_avoid_threshold) {
            return Err(ConfigError::Invalid(
                "routing.quota_avoid_threshold must be in 0..=1".into(),
            ));
        }
        if !self.proxy_url.is_empty() {
            let ok = ["http://", "https://", "socks5://", "socks5h://"]
                .iter()
                .any(|p| self.proxy_url.starts_with(p));
            if !ok {
                return Err(ConfigError::Invalid(
                    "proxy_url scheme must be http/https/socks5/socks5h".into(),
                ));
            }
        }
        if self.api_keys.iter().any(|k| k.len() < 16) {
            return Err(ConfigError::Invalid(
                "api_keys entries must be >= 16 chars".into(),
            ));
        }
        if self.upstream_opts.max_attempts == 0 {
            return Err(ConfigError::Invalid(
                "upstream_opts.max_attempts must be >= 1".into(),
            ));
        }
        Ok(())
    }
}

pub fn parse_duration(s: &str) -> Result<Duration, ConfigError> {
    let s = s.trim();
    let bad = || ConfigError::Invalid(format!("bad duration {s:?} (use 90s/30m/1h)"));
    let (num, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).ok_or_else(bad)?);
    let n: u64 = num.parse().map_err(|_| bad())?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        _ => return Err(bad()),
    };
    Ok(Duration::from_secs(secs))
}

fn expand_tilde(p: &Path) -> PathBuf {
    match (p.strip_prefix("~"), std::env::var_os("HOME")) {
        (Ok(rest), Some(home)) => PathBuf::from(home).join(rest),
        _ => p.to_path_buf(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_bind_loopback() {
        let c = Config::parse("").unwrap();
        assert!(c.listen.ip().is_loopback());
        assert_eq!(c.routing.strategy, Strategy::RoundRobin);
        assert_eq!(c.routing.session_affinity_rebind_on, RebindOn::Quota);
        assert_eq!(c.routing.ttl().unwrap(), Duration::from_secs(3600));
    }

    #[test]
    fn parses_full() {
        let c = Config::parse(
            r#"listen = "127.0.0.1:9000"
api_keys = ["sk-cpa-0123456789abcdef"]
proxy_url = "socks5h://127.0.0.1:1080"
[routing]
strategy = "weighted-round-robin"
session_affinity_rebind_on = "any"
quota_avoid_threshold = 0.9
"#,
        )
        .unwrap();
        assert_eq!(c.routing.strategy, Strategy::WeightedRoundRobin);
        assert_eq!(c.api_keys.len(), 1);
    }

    #[test]
    fn rejects_bad() {
        assert!(Config::parse("proxy_url = \"ftp://x\"").is_err());
        assert!(Config::parse("api_keys = [\"short\"]").is_err());
        assert!(Config::parse("[routing]\nsession_affinity_ttl = \"1d\"").is_err());
        assert!(Config::parse("admin_port = 1").is_err());
    }
}
