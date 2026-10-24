//! TOML config (see README "Config example (v3)").
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum Strategy {
    #[default]
    RoundRobin,
    WeightedRoundRobin,
    FillFirst,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum RebindOn {
    Any,
    #[default]
    Quota,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoutingConfig {
    pub strategy: Strategy,
    pub session_affinity: bool,
    /// e.g. "1h", "30m", "90s".
    pub session_affinity_ttl: String,
    pub session_affinity_subagents: bool,
    pub session_affinity_rebind_on: RebindOn,
    /// Used fraction (0..=1) at/over which an account gets no *new* sessions.
    /// 0 (default) disables proactive avoidance: accounts are switched only on
    /// upstream errors (429 / usage_limit cooldown + transparent failover).
    pub quota_avoid_threshold: f64,
    /// When every candidate account is cooling down and the first one recovers within this
    /// many seconds, the request is held and routed once it recovers. 0 = answer at once.
    pub cooldown_wait_secs: u64,
}
/// Upper bound for `routing.cooldown_wait_secs`.
pub const MAX_COOLDOWN_WAIT_SECS: u64 = 120;
impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            strategy: Strategy::RoundRobin,
            session_affinity: true,
            session_affinity_ttl: "24h".into(),
            session_affinity_subagents: true,
            session_affinity_rebind_on: RebindOn::Quota,
            quota_avoid_threshold: 0.0,
            cooldown_wait_secs: 20,
        }
    }
}
impl RoutingConfig {
    pub fn ttl(&self) -> Result<Duration, ConfigError> {
        parse_duration(&self.session_affinity_ttl)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct UpstreamOpts {
    pub stream_idle_timeout_secs: u64,
    /// Per HTTP attempt: sending the request and waiting for response headers. Successful
    /// streaming bodies keep their separate idle timeout, without an absolute generation limit.
    pub response_header_timeout_secs: u64,
    /// Cumulative preparation time across retries and account switches: HTTP credentials,
    /// response headers, error bodies and retry waits; WS credentials, handshakes and retry
    /// waits. WS has one budget for the initial handshake and a fresh budget per turn's
    /// pre-output reconnects. Successful stream reads and WS frame writes are not charged.
    pub preparation_timeout_secs: u64,
    /// Per-request budget of HTTP failovers for transport errors / 408 / 5xx, before output.
    /// Each budgeted failover moves to another usable account. Default 1 permits one such
    /// switch, not a total limit of two upstream attempts: account errors and recognized
    /// overload use separate rules. Overload can retry once on the last usable account
    /// according to `overload_retry_wait_secs`; WS handshakes have their own retry limit.
    pub max_attempts: u32,
    pub connect_timeout_secs: u64,
    /// zstd-compress (level 3) plaintext HTTP `POST /responses` bodies before forwarding, like
    /// official Codex does with ChatGPT auth. Bodies that already carry a `Content-Encoding`
    /// are forwarded untouched; models / images / passthrough / WebSocket are never compressed.
    pub compress_requests: bool,
    /// Keep an in-memory, per-account jar of ChatGPT infrastructure cookies (Cloudflare
    /// `__cf_bm`/`__cflb`/`_cfuvid`/`cf_clearance`/`cf_chl_*`… and the `__oailb` routing
    /// cookie) and replay them like official Codex. Off by default. Client `Cookie` headers are
    /// never forwarded and upstream `Set-Cookie` never reaches clients either way.
    pub cloudflare_cookies: bool,
    /// Cooldown (seconds) after 3 consecutive upstream 5xx on one account; `Retry-After`
    /// overrides it, capped at 60. 0 = never cool on 5xx. The last usable account is never cooled.
    pub transient_cooldown_secs: u64,
    /// Consecutive overloaded answers (`server_is_overloaded` / `slow_down` / "at capacity")
    /// on one account before it is cooled. 0 = never cool on overload.
    pub overload_streak: u32,
    /// First overload cooldown (seconds); doubles on each further cooldown until a success.
    pub overload_cooldown_secs: u64,
    /// Upper bound of the overload cooldown (seconds).
    pub overload_cooldown_max_secs: u64,
    /// When the only usable account (or every account) is overloaded before any output, wait
    /// about this long (jittered to 2/3–5/3 of it; an upstream `Retry-After` is honored up to
    /// 10 s) and retry once on the same account. 0 = no wait-and-retry.
    pub overload_retry_wait_secs: u64,
    /// Account cooldown (seconds) after an upstream 402 `deactivated_workspace` (the ChatGPT
    /// workspace was deactivated); the request moves to another account. 0 = never cool.
    pub deactivated_cooldown_secs: u64,
    /// Account cooldown (seconds) after any other upstream 402 (payment / billing problem);
    /// the request moves to another account. 0 = never cool.
    pub payment_required_cooldown_secs: u64,
    /// Cooldown (seconds) of one model on one account after an upstream 404 that does not say
    /// the model is unsupported (`Retry-After` wins); the request moves to another account.
    /// A 404 that explicitly names the model as unsupported always cools it for 12 h.
    /// 0 = never cool on such a 404.
    pub not_found_cooldown_secs: u64,
}
impl Default for UpstreamOpts {
    fn default() -> Self {
        Self {
            stream_idle_timeout_secs: 300,
            response_header_timeout_secs: 300,
            preparation_timeout_secs: 600,
            max_attempts: 1,
            connect_timeout_secs: 30,
            compress_requests: true,
            cloudflare_cookies: false,
            transient_cooldown_secs: 15,
            overload_streak: 2,
            overload_cooldown_secs: 120,
            overload_cooldown_max_secs: 600,
            overload_retry_wait_secs: 3,
            deactivated_cooldown_secs: 86_400,
            payment_required_cooldown_secs: 1_800,
            not_found_cooldown_secs: 600,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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
pub const PINNED_CODEX_VERSION: &str = "0.164.0";
impl Default for ImagesConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            main_model: "gpt-5.5".into(),
            default_originator: "codex_exec".into(),
            default_user_agent: format!("codex_exec/{PINNED_CODEX_VERSION}"),
        }
    }
}

/// Built-in management page at `/ui` (read-only except the opt-in reset-credit action).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct UiConfig {
    pub enabled: bool,
    /// Allow `POST /admin/v1/accounts/{id}/reset-credits/consume` (spends a 重置卡; irreversible).
    /// Default `false`: the API answers 403 and the page shows no button. Never automatic.
    pub allow_reset_consume: bool,
    /// Background refresh of each account's reset-credit list (read-only GET), minutes.
    /// `0` = only on demand.
    pub reset_credits_refresh_mins: u64,
    /// Allow configuration and account changes from the management page / admin API
    /// (`PUT /admin/v1/config`, key and account management). Default `false`: read-only.
    /// Like `allow_reset_consume`, it can only be turned on in the config file.
    pub allow_config_write: bool,
}
impl Default for UiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            allow_reset_consume: false,
            reset_credits_refresh_mins: 60,
            allow_config_write: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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

/// One client key. Config accepts a plain string (label = `key-<sha256(key)[:8]>`) or a
/// table `{ label = "laptop", key = "sk-...", admin = true }`. Only tables with
/// `admin = true` may call `/admin/v1/*` (settings, key / account management, reset-credit
/// consume); plain strings are never admin.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum ApiKeyEntry {
    Plain(String),
    Labeled {
        label: String,
        key: String,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        admin: bool,
    },
}

#[derive(Clone, PartialEq, Eq)]
pub struct ApiKey {
    pub label: String,
    pub key: String,
    /// May call the management API (`/admin/v1/*`).
    pub admin: bool,
}
impl std::fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiKey")
            .field("label", &self.label)
            .field("key", &"<redacted>")
            .field("admin", &self.admin)
            .finish()
    }
}

/// Default label for an unlabeled key: never the key itself.
pub fn default_key_label(key: &str) -> String {
    use sha2::Digest;
    let d = sha2::Sha256::digest(key.as_bytes());
    let hex: String = d.iter().take(4).map(|b| format!("{b:02x}")).collect();
    format!("key-{hex}")
}

impl ApiKeyEntry {
    pub fn resolve(&self) -> ApiKey {
        match self {
            ApiKeyEntry::Plain(k) => ApiKey {
                label: default_key_label(k),
                key: k.clone(),
                admin: false,
            },
            ApiKeyEntry::Labeled { label, key, admin } => ApiKey {
                label: label.trim().to_string(),
                key: key.clone(),
                admin: *admin,
            },
        }
    }
}

/// Weights for estimating "tokens per 1% of quota" (see docs/USAGE.md). `output` covers
/// reasoning tokens too (they are part of output_tokens upstream).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct QuotaWeights {
    /// Uncached input tokens.
    pub input: f64,
    pub cached: f64,
    pub output: f64,
    /// Minimum used-% increase between two snapshots of one window before estimating.
    pub min_delta_pct: f64,
}
impl Default for QuotaWeights {
    fn default() -> Self {
        Self {
            input: 1.0,
            cached: 0.1,
            output: 4.0,
            min_delta_pct: 5.0,
        }
    }
}

/// Per-request usage log (SQLite, see docs/USAGE.md).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct UsageConfig {
    pub enabled: bool,
    /// SQLite file (WAL mode; `-wal`/`-shm` siblings are created next to it).
    pub path: PathBuf,
    /// Raw rows older than this are purged (hourly). 0 = keep forever.
    pub retention_days: u32,
    /// Bounded in-memory queue between the proxy and the writer thread; full = drop + count.
    pub queue_size: usize,
    /// Max rows per write transaction.
    pub batch_size: usize,
    /// Max time a record waits in the writer before its batch is committed.
    pub flush_ms: u64,
    pub quota_weights: QuotaWeights,
    /// Local price table for notional cost. Missing file ⇒ every cost is null (unpriced).
    /// Filled and updated on request from `prices_source` (management page or
    /// `ciallopa prices sync`).
    pub prices: PathBuf,
    /// Price dataset used by a price check: LiteLLM `model_prices_and_context_window.json` or
    /// models.dev `api.json` (http(s) URL). Empty = checks are off.
    pub prices_source: String,
}

/// LiteLLM's price dataset (per-tier and long-context prices for OpenAI models).
pub const DEFAULT_PRICES_SOURCE: &str =
    "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json";
impl Default for UsageConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: PathBuf::from("~/.ciallopa/usage.db"),
            retention_days: 90,
            queue_size: 8192,
            batch_size: 256,
            flush_ms: 50,
            quota_weights: QuotaWeights::default(),
            prices: PathBuf::from("~/.ciallopa/prices.toml"),
            prices_source: DEFAULT_PRICES_SOURCE.into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub listen: SocketAddr,
    /// Static client keys (Bearer or x-api-key). Empty = reject everything but /healthz.
    /// Plain strings or `[[api_keys]] label = "..." key = "..."`; see [`Config::keys`].
    pub api_keys: Vec<ApiKeyEntry>,
    /// Name of this proxy instance in usage records. Default: hostname.
    pub instance_id: String,
    pub upstream: String,
    pub auth_dir: PathBuf,
    /// Global egress proxy: http://, https://, socks5://, socks5h://. Empty = direct.
    /// `socks5://` is used as `socks5h://` (host names are resolved by the proxy).
    pub proxy_url: String,
    /// Use `proxy_url`. Off keeps the address but connects directly. Missing key = on.
    pub proxy_enabled: bool,
    /// Allow IPv6 for direct upstream connections and for reaching the proxy host (Happy
    /// Eyeballs). `false` (default): IPv4 only. Target DNS behind a proxy is the proxy's.
    pub ipv6: bool,
    /// Incoming HTTP request body limit in MiB (default 64, the WebSocket message limit).
    pub max_body_mb: u64,
    /// Simultaneous forwarded HTTP requests and WebSocket connections. Zero disables
    /// rejection, but existing requests are still counted for reload and shutdown.
    pub max_inflight_requests: usize,
    /// Accept a client's `permessage-deflate` offer on the Responses WebSocket (RFC 7692;
    /// Codex always offers it). Read at each handshake; open connections keep what they
    /// negotiated. Off, or without an offer, the client leg is uncompressed.
    pub ws_compression: bool,
    pub routing: RoutingConfig,
    pub upstream_opts: UpstreamOpts,
    pub images: ImagesConfig,
    pub log: LogConfig,
    pub usage: UsageConfig,
    pub ui: UiConfig,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 8317)),
            api_keys: vec![],
            instance_id: String::new(),
            upstream: "https://chatgpt.com/backend-api".into(),
            auth_dir: PathBuf::from("~/.ciallopa/auths"),
            proxy_url: String::new(),
            proxy_enabled: true,
            ipv6: false,
            max_body_mb: 64,
            max_inflight_requests: 64,
            ws_compression: true,
            routing: RoutingConfig::default(),
            upstream_opts: UpstreamOpts::default(),
            images: ImagesConfig::default(),
            log: LogConfig::default(),
            usage: UsageConfig::default(),
            ui: UiConfig::default(),
        }
    }
}

/// The placeholder key in `config.example.toml`. A config that still contains it is rejected
/// (it is an admin key everyone can read in the repository).
pub const EXAMPLE_API_KEY: &str = "sk-cpa-change-me-0123456789";

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let s = std::fs::read_to_string(path).map_err(|e| ConfigError::Read(path.into(), e))?;
        Self::parse(&s)
    }
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        let mut c: Config = toml::from_str(s)?;
        c.auth_dir = expand_tilde(&c.auth_dir);
        c.usage.path = expand_tilde(&c.usage.path);
        c.usage.prices = expand_tilde(&c.usage.prices);
        if c.instance_id.trim().is_empty() {
            c.instance_id = hostname();
        }
        c.validate()?;
        Ok(c)
    }
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !(1..=4096).contains(&self.max_body_mb) {
            return Err(ConfigError::Invalid(
                "max_body_mb must be in 1..=4096".into(),
            ));
        }
        self.routing.ttl()?;
        if !(0.0..=1.0).contains(&self.routing.quota_avoid_threshold) {
            return Err(ConfigError::Invalid(
                "routing.quota_avoid_threshold must be in 0..=1".into(),
            ));
        }
        let src = self.usage.prices_source.trim();
        if !(src.is_empty() || src.starts_with("https://") || src.starts_with("http://")) {
            return Err(ConfigError::Invalid(
                "usage.prices_source must be an http(s) URL or empty".into(),
            ));
        }
        if self.routing.cooldown_wait_secs > MAX_COOLDOWN_WAIT_SECS {
            return Err(ConfigError::Invalid(format!(
                "routing.cooldown_wait_secs must be in 0..={MAX_COOLDOWN_WAIT_SECS}"
            )));
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
        let keys = self.keys();
        if keys.iter().any(|k| k.key.len() < 16) {
            return Err(ConfigError::Invalid(
                "api_keys entries must be >= 16 chars".into(),
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for k in &keys {
            if k.label.is_empty() || k.label.len() > 64 {
                return Err(ConfigError::Invalid(
                    "api_keys label must be 1..=64 chars".into(),
                ));
            }
            if !seen.insert(k.label.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate api_keys label {:?}",
                    k.label
                )));
            }
        }
        if keys.iter().any(|k| k.key == EXAMPLE_API_KEY) {
            return Err(ConfigError::Invalid(format!(
                "api_keys contains the example key {EXAMPLE_API_KEY}; replace it with a key of your own (`ciallopa init` writes a config with a random one)"
            )));
        }
        let mut uniq = std::collections::HashSet::new();
        if !keys.iter().all(|k| uniq.insert(k.key.as_str())) {
            return Err(ConfigError::Invalid("duplicate api_keys key".into()));
        }
        if self.usage.enabled && (self.usage.queue_size == 0 || self.usage.batch_size == 0) {
            return Err(ConfigError::Invalid(
                "usage.queue_size and usage.batch_size must be >= 1".into(),
            ));
        }
        crate::logging::validate_level(&self.log.level).map_err(ConfigError::Invalid)?;
        if self.upstream_opts.max_attempts == 0 {
            return Err(ConfigError::Invalid(
                "upstream_opts.max_attempts must be >= 1".into(),
            ));
        }
        for (key, value) in [
            (
                "response_header_timeout_secs",
                self.upstream_opts.response_header_timeout_secs,
            ),
            (
                "preparation_timeout_secs",
                self.upstream_opts.preparation_timeout_secs,
            ),
        ] {
            if !(1..=86_400).contains(&value) {
                return Err(ConfigError::Invalid(format!(
                    "upstream_opts.{key} must be in 1..=86400"
                )));
            }
        }
        Ok(())
    }
}

impl Config {
    /// The egress proxy actually used: `proxy_url` when `proxy_enabled` and non-empty
    /// (`socks5://` normalized to `socks5h://`), else `""` (direct).
    pub fn effective_proxy(&self) -> String {
        if self.proxy_enabled {
            normalize_proxy_url(&self.proxy_url)
        } else {
            String::new()
        }
    }
}

/// `socks5://…` → `socks5h://…` (DNS through the proxy on every path); others unchanged.
pub fn normalize_proxy_url(u: &str) -> String {
    let u = u.trim();
    match u.strip_prefix("socks5://") {
        Some(rest) => format!("socks5h://{rest}"),
        None => u.to_string(),
    }
}

impl Config {
    /// Resolved client keys with labels (labels default to `key-<sha256 prefix>`).
    pub fn keys(&self) -> Vec<ApiKey> {
        self.api_keys.iter().map(ApiKeyEntry::resolve).collect()
    }
}

/// `/proc/sys/kernel/hostname`, `$HOSTNAME`, else "ciallopa".
pub fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .or_else(|| std::env::var("HOSTNAME").ok())
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "ciallopa".into())
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

/// Data dir of the old project name (`cpa-lite`), used as a fallback only.
pub const LEGACY_DIR: &str = ".cpa-lite";
/// Current data dir under `$HOME`.
pub const DATA_DIR: &str = ".ciallopa";

static LEGACY_USED: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

/// `Some(old dir)` when a `~/.ciallopa/…` path was redirected to the legacy `~/.cpa-lite`
/// (log a hint after logging is up).
pub fn legacy_dir_in_use() -> Option<&'static Path> {
    LEGACY_USED.get().map(PathBuf::as_path)
}

fn expand_tilde(p: &Path) -> PathBuf {
    match (p.strip_prefix("~"), std::env::var_os("HOME")) {
        (Ok(rest), Some(home)) => {
            let home = PathBuf::from(home);
            let (out, legacy) = resolve_home(&home, rest, |d| d.exists());
            if legacy {
                let _ = LEGACY_USED.set(home.join(LEGACY_DIR));
            }
            out
        }
        _ => p.to_path_buf(),
    }
}

/// `~/.ciallopa/x` → `$HOME/.cpa-lite/x` iff `~/.ciallopa` is missing and `~/.cpa-lite` exists.
fn resolve_home(home: &Path, rest: &Path, exists: impl Fn(&Path) -> bool) -> (PathBuf, bool) {
    if let Ok(sub) = rest.strip_prefix(DATA_DIR) {
        let (new, old) = (home.join(DATA_DIR), home.join(LEGACY_DIR));
        if !exists(&new) && exists(&old) {
            return (old.join(sub), true);
        }
    }
    (home.join(rest), false)
}

/// `CIALLOPA_<name>`, falling back to the legacy `CPA_LITE_<name>`; the bool says the old
/// name was used.
pub fn env_var(name: &str) -> Option<(String, bool)> {
    if let Ok(v) = std::env::var(format!("CIALLOPA_{name}")) {
        return Some((v, false));
    }
    std::env::var(format!("CPA_LITE_{name}"))
        .ok()
        .map(|v| (v, true))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_key_is_rejected() {
        let e = Config::parse(&format!("api_keys = [\"{EXAMPLE_API_KEY}\"]")).unwrap_err();
        assert!(e.to_string().contains("example key"), "{e}");
        let e = Config::parse(&format!(
            "api_keys = [{{ label = \"a\", key = \"{EXAMPLE_API_KEY}\", admin = true }}]"
        ))
        .unwrap_err();
        assert!(e.to_string().contains("ciallopa init"), "{e}");
        assert!(Config::parse("api_keys = [\"sk-cpa-0123456789abcdef\"]").is_ok());
    }

    #[test]
    fn legacy_data_dir_fallback() {
        let h = Path::new("/h");
        let rest = Path::new(".ciallopa/usage.db");
        let has =
            |set: &'static [&'static str]| move |p: &Path| set.iter().any(|s| p == Path::new(s));
        // only the old dir exists → use it
        assert_eq!(
            resolve_home(h, rest, has(&["/h/.cpa-lite"])),
            (PathBuf::from("/h/.cpa-lite/usage.db"), true)
        );
        // new dir exists (or neither) → new dir
        assert_eq!(
            resolve_home(h, rest, has(&["/h/.ciallopa", "/h/.cpa-lite"])),
            (PathBuf::from("/h/.ciallopa/usage.db"), false)
        );
        assert_eq!(
            resolve_home(h, rest, has(&[])),
            (PathBuf::from("/h/.ciallopa/usage.db"), false)
        );
        // unrelated paths untouched
        assert!(!resolve_home(h, Path::new(".codex/auth.json"), has(&["/h/.cpa-lite"])).1);
    }

    #[test]
    fn defaults_bind_loopback() {
        let c = Config::parse("").unwrap();
        assert!(c.listen.ip().is_loopback());
        assert_eq!(c.routing.strategy, Strategy::RoundRobin);
        assert_eq!(c.routing.session_affinity_rebind_on, RebindOn::Quota);
        assert_eq!(c.routing.ttl().unwrap(), Duration::from_secs(24 * 3600));
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
    fn labeled_and_plain_keys() {
        let c = Config::parse(
            r#"instance_id = "nas"
api_keys = ["sk-plain-0123456789abcdef", { label = "laptop", key = "sk-laptop-0123456789" }]
"#,
        )
        .unwrap();
        let k = c.keys();
        assert_eq!(k[0].label, default_key_label("sk-plain-0123456789abcdef"));
        assert!(k[0].label.starts_with("key-") && k[0].label.len() == 12);
        assert!(!k[0].label.contains("sk-"));
        assert_eq!(k[1].label, "laptop");
        assert_eq!(c.instance_id, "nas");
        assert!(!format!("{:?}", k[1]).contains("sk-laptop"));
        let t = Config::parse("[[api_keys]]\nlabel = \"phone\"\nkey = \"sk-phone-0123456789ab\"\n")
            .unwrap();
        assert_eq!(t.keys()[0].label, "phone");
        assert!(!Config::parse("").unwrap().instance_id.is_empty());
        assert!(Config::parse(
            "api_keys = [{ label = \"a\", key = \"sk-0123456789abcdefg\" }, { label = \"a\", key = \"sk-0123456789abcdefh\" }]"
        )
        .is_err());
    }

    #[test]
    fn effective_proxy() {
        let c = Config::parse("").unwrap();
        assert!(c.proxy_enabled);
        assert_eq!(c.effective_proxy(), "");
        let c = Config::parse("proxy_url = \"socks5://u:p@h:1080\"").unwrap();
        assert_eq!(c.effective_proxy(), "socks5h://u:p@h:1080");
        let c = Config::parse("proxy_url = \"http://h:3128\"\nproxy_enabled = false").unwrap();
        assert_eq!(c.effective_proxy(), "");
        assert_eq!(c.proxy_url, "http://h:3128");
        assert_eq!(normalize_proxy_url("socks5h://h:1"), "socks5h://h:1");
        assert_eq!(normalize_proxy_url("https://h:1"), "https://h:1");
    }

    #[test]
    fn rejects_bad() {
        assert!(Config::parse("proxy_url = \"ftp://x\"").is_err());
        assert!(Config::parse("api_keys = [\"short\"]").is_err());
        assert!(Config::parse("[routing]\nsession_affinity_ttl = \"1d\"").is_err());
        assert!(Config::parse("admin_port = 1").is_err());
    }
}
