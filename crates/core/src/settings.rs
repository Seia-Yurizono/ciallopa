//! Live configuration: an atomically swappable snapshot of [`Config`], the settings schema
//! (which keys apply hot and which need a restart, which the management page may edit) and
//! helpers to flatten, diff and mask configs for the admin API.
use crate::config::{ApiKey, Config};
use arc_swap::ArcSwap;
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

/// The running configuration. Readers call [`SharedConfig::load`] once per request and keep
/// that snapshot for the whole request; a reload swaps the pointer atomically.
#[derive(Debug)]
pub struct SharedConfig {
    cfg: ArcSwap<Config>,
    keys: ArcSwap<Vec<ApiKey>>,
}

impl SharedConfig {
    pub fn new(cfg: Config) -> Self {
        let keys = cfg.keys();
        Self {
            cfg: ArcSwap::from_pointee(cfg),
            keys: ArcSwap::from_pointee(keys),
        }
    }
    pub fn load(&self) -> Arc<Config> {
        self.cfg.load_full()
    }
    /// Resolved client keys of the current snapshot.
    pub fn keys(&self) -> Arc<Vec<ApiKey>> {
        self.keys.load_full()
    }
    pub fn store(&self, cfg: Config) {
        self.keys.store(Arc::new(cfg.keys()));
        self.cfg.store(Arc::new(cfg));
    }
}

impl From<Config> for SharedConfig {
    fn from(c: Config) -> Self {
        Self::new(c)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Effect {
    /// Applied to the running process by a reload.
    Hot,
    /// Recorded as pending; takes effect after a restart.
    Restart,
}

#[derive(Debug, Clone, Serialize)]
pub struct Field {
    pub key: &'static str,
    pub kind: &'static str,
    pub effect: Effect,
    /// May be changed from the management page (`PUT /admin/v1/config`).
    pub editable: bool,
    pub secret: bool,
    /// Allowed values for `enum` kinds.
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    pub options: &'static [&'static str],
    pub desc: &'static str,
}

const fn f(
    key: &'static str,
    kind: &'static str,
    effect: Effect,
    editable: bool,
    desc: &'static str,
) -> Field {
    Field {
        key,
        kind,
        effect,
        editable,
        secret: false,
        options: &[],
        desc,
    }
}

use Effect::{Hot, Restart};

/// Every config key, in file order.
pub const SCHEMA: &[Field] = &[
    f("listen", "string", Restart, false, "监听地址"),
    Field {
        secret: true,
        ..f(
            "api_keys",
            "keys",
            Hot,
            false,
            "客户端访问密钥（页面上只能删除或生成新密钥）",
        )
    },
    f(
        "instance_id",
        "string",
        Restart,
        true,
        "实例名（写入用量记录）",
    ),
    f("upstream", "string", Restart, true, "上游地址"),
    f("auth_dir", "path", Restart, false, "账号文件目录"),
    Field {
        secret: true,
        ..f(
            "proxy_url",
            "string",
            Hot,
            true,
            "出口代理（http/https/socks5/socks5h，空 = 直连）",
        )
    },
    f(
        "max_body_mb",
        "int",
        Restart,
        true,
        "请求体上限（MB，最低按 20 计）",
    ),
    f(
        "ws_compression",
        "bool",
        Hot,
        true,
        "客户端 WebSocket 压缩（permessage-deflate，客户端请求时启用，新连接生效）",
    ),
    Field {
        options: &["round-robin", "weighted-round-robin", "fill-first"],
        ..f("routing.strategy", "enum", Hot, true, "选号策略")
    },
    f("routing.session_affinity", "bool", Hot, true, "会话亲和"),
    f(
        "routing.session_affinity_ttl",
        "duration",
        Hot,
        true,
        "亲和保持时长（90s/30m/1h）",
    ),
    f(
        "routing.session_affinity_subagents",
        "bool",
        Hot,
        true,
        "子代理沿用父会话账号",
    ),
    Field {
        options: &["quota", "any"],
        ..f(
            "routing.session_affinity_rebind_on",
            "enum",
            Hot,
            true,
            "何时换绑账号",
        )
    },
    f(
        "routing.quota_avoid_threshold",
        "float",
        Hot,
        true,
        "额度回避阈值（0–1，0 = 关闭）",
    ),
    f(
        "upstream_opts.stream_idle_timeout_secs",
        "int",
        Hot,
        true,
        "流空闲超时（秒）",
    ),
    f(
        "upstream_opts.max_attempts",
        "int",
        Hot,
        true,
        "5xx/网络错误时换号重试次数（不重试同一账号）",
    ),
    f(
        "upstream_opts.transient_cooldown_secs",
        "int",
        Hot,
        true,
        "上游连续 3 次 5xx 后的冷却（秒，0 = 不冷却）",
    ),
    f(
        "upstream_opts.overload_streak",
        "int",
        Hot,
        true,
        "同一账号连续几次 overloaded 后冷却（0 = 不冷却）",
    ),
    f(
        "upstream_opts.overload_cooldown_secs",
        "int",
        Hot,
        true,
        "overloaded 冷却起始时长（秒，之后每次翻倍）",
    ),
    f(
        "upstream_opts.overload_cooldown_max_secs",
        "int",
        Hot,
        true,
        "overloaded 冷却上限（秒）",
    ),
    f(
        "upstream_opts.overload_retry_wait_secs",
        "int",
        Hot,
        true,
        "仅剩一个可用账号或全部 overloaded 时，等待约几秒后同号重试 1 次（0 = 不重试）",
    ),
    f(
        "upstream_opts.connect_timeout_secs",
        "int",
        Hot,
        true,
        "连接超时（秒）",
    ),
    f(
        "upstream_opts.compress_requests",
        "bool",
        Hot,
        true,
        "zstd 压缩 /responses 请求体",
    ),
    f(
        "upstream_opts.cloudflare_cookies",
        "bool",
        Hot,
        true,
        "按账号保存 Cloudflare cookie",
    ),
    f("images.enabled", "bool", Hot, true, "图片接口"),
    f("images.main_model", "string", Hot, true, "图片接口主模型"),
    f(
        "images.default_originator",
        "string",
        Hot,
        true,
        "非 Codex 客户端的 originator",
    ),
    f(
        "images.default_user_agent",
        "string",
        Hot,
        true,
        "非 Codex 客户端的 User-Agent",
    ),
    Field {
        options: &["error", "warn", "info", "debug", "trace"],
        ..f("log.level", "enum", Hot, true, "日志级别（RUST_LOG 优先）")
    },
    f("usage.enabled", "bool", Restart, true, "用量日志"),
    f("usage.path", "path", Restart, false, "用量数据库文件"),
    f(
        "usage.retention_days",
        "int",
        Hot,
        true,
        "明细保留天数（0 = 永久）",
    ),
    f("usage.queue_size", "int", Restart, true, "写入队列长度"),
    f("usage.batch_size", "int", Restart, true, "每批写入行数"),
    f(
        "usage.flush_ms",
        "int",
        Restart,
        true,
        "写入最长等待（毫秒）",
    ),
    f(
        "usage.quota_weights.input",
        "float",
        Hot,
        true,
        "额度估算权重：未缓存输入",
    ),
    f(
        "usage.quota_weights.cached",
        "float",
        Hot,
        true,
        "额度估算权重：缓存输入",
    ),
    f(
        "usage.quota_weights.output",
        "float",
        Hot,
        true,
        "额度估算权重：输出",
    ),
    f(
        "usage.quota_weights.min_delta_pct",
        "float",
        Hot,
        true,
        "额度估算最小变化（%）",
    ),
    f(
        "usage.prices",
        "path",
        Hot,
        false,
        "价格表文件（内容变化自动重新加载）",
    ),
    f("ui.enabled", "bool", Restart, false, "管理页"),
    f(
        "ui.allow_reset_consume",
        "bool",
        Hot,
        false,
        "允许手动使用重置卡（只能在文件里修改）",
    ),
    f(
        "ui.reset_credits_refresh_mins",
        "int",
        Hot,
        true,
        "重置卡后台刷新间隔（分钟，0 = 仅手动）",
    ),
    f(
        "ui.allow_config_write",
        "bool",
        Hot,
        false,
        "允许在管理页修改配置和账号（只能在文件里修改）",
    ),
];

pub fn field(key: &str) -> Option<&'static Field> {
    SCHEMA.iter().find(|f| f.key == key)
}

/// `{ "routing.strategy": "round-robin", ... }` for every schema key (`api_keys` stays a list).
pub fn flatten(c: &Config) -> BTreeMap<String, Value> {
    let v = serde_json::to_value(c).unwrap_or(Value::Null);
    let mut out = BTreeMap::new();
    for f in SCHEMA {
        let mut cur = &v;
        for part in f.key.split('.') {
            cur = cur.get(part).unwrap_or(&Value::Null);
        }
        out.insert(f.key.to_string(), cur.clone());
    }
    out
}

/// `sk-cpa-…abcd` style hint; never more than the last 4 characters.
pub fn key_hint(key: &str) -> String {
    let tail: String = key
        .chars()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    let head: String = key.chars().take(3).collect();
    format!("{head}…{tail}")
}

/// Value as shown by the admin API: client keys as label + hint, proxy password hidden.
pub fn mask(key: &str, v: &Value, c: Option<&Config>) -> Value {
    match key {
        "api_keys" => match c {
            Some(c) => Value::Array(
                c.keys()
                    .iter()
                    .map(|k| serde_json::json!({"label": k.label, "hint": key_hint(&k.key), "admin": k.admin}))
                    .collect(),
            ),
            None => Value::String("<set>".into()),
        },
        "proxy_url" => Value::String(mask_proxy(v.as_str().unwrap_or_default())),
        _ => v.clone(),
    }
}

/// `socks5h://user:pass@host:1080` → `socks5h://user:***@host:1080`.
pub fn mask_proxy(u: &str) -> String {
    let Some((scheme, rest)) = u.split_once("://") else {
        return u.to_string();
    };
    match rest.rsplit_once('@') {
        Some((cred, host)) => {
            let user = cred.split(':').next().unwrap_or_default();
            let pw = if cred.contains(':') { ":***" } else { "" };
            format!("{scheme}://{user}{pw}@{host}")
        }
        None => u.to_string(),
    }
}

/// Masked `{key: value}` map of a config.
pub fn masked_values(c: &Config) -> Map<String, Value> {
    flatten(c)
        .into_iter()
        .map(|(k, v)| {
            let m = mask(&k, &v, Some(c));
            (k, m)
        })
        .collect()
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Change {
    pub key: String,
    /// Masked.
    pub old: Value,
    /// Masked.
    pub new: Value,
    pub effect: Effect,
}

/// Keys whose value differs, with masked old/new values.
pub fn diff(old: &Config, new: &Config) -> Vec<Change> {
    let (a, b) = (flatten(old), flatten(new));
    SCHEMA
        .iter()
        .filter(|f| a.get(f.key) != b.get(f.key))
        .map(|f| {
            let null = Value::Null;
            Change {
                key: f.key.to_string(),
                old: mask(f.key, a.get(f.key).unwrap_or(&null), Some(old)),
                new: mask(f.key, b.get(f.key).unwrap_or(&null), Some(new)),
                effect: f.effect,
            }
        })
        .collect()
}

/// `new` with every restart-only key taken from `running`: what a reload can apply now.
pub fn effective(running: &Config, new: &Config) -> Config {
    let mut c = new.clone();
    c.listen = running.listen;
    c.instance_id = running.instance_id.clone();
    c.upstream = running.upstream.clone();
    c.auth_dir = running.auth_dir.clone();
    c.max_body_mb = running.max_body_mb;
    c.usage.enabled = running.usage.enabled;
    c.usage.path = running.usage.path.clone();
    c.usage.queue_size = running.usage.queue_size;
    c.usage.batch_size = running.usage.batch_size;
    c.usage.flush_ms = running.usage.flush_ms;
    c.ui.enabled = running.ui.enabled;
    c
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_covers_every_config_key() {
        // Every leaf of the default config is in the schema and vice versa.
        fn leaves(v: &Value, pre: &str, out: &mut Vec<String>) {
            match v {
                Value::Object(m) => {
                    for (k, x) in m {
                        let p = if pre.is_empty() {
                            k.clone()
                        } else {
                            format!("{pre}.{k}")
                        };
                        leaves(x, &p, out);
                    }
                }
                _ => out.push(pre.to_string()),
            }
        }
        let mut l = vec![];
        leaves(
            &serde_json::to_value(Config::default()).unwrap(),
            "",
            &mut l,
        );
        let mut s: Vec<String> = SCHEMA.iter().map(|f| f.key.to_string()).collect();
        l.sort();
        s.sort();
        assert_eq!(l, s);
    }

    #[test]
    fn diff_masks_and_classifies() {
        let a = Config::parse("api_keys = [\"sk-cpa-0123456789abcdef\"]").unwrap();
        let mut b = a.clone();
        b.proxy_url = "socks5h://u:secretpw@h:1080".into();
        b.listen = "127.0.0.1:9999".parse().unwrap();
        let d = diff(&a, &b);
        assert_eq!(d.len(), 2);
        let s = serde_json::to_string(&d).unwrap();
        assert!(!s.contains("secretpw") && s.contains("u:***@h"));
        assert_eq!(
            d.iter().find(|c| c.key == "listen").unwrap().effect,
            Effect::Restart
        );
        let m = serde_json::to_string(&masked_values(&a)).unwrap();
        assert!(!m.contains("0123456789abcdef") && m.contains("cdef"));
        let e = effective(&a, &b);
        assert_eq!(e.listen, a.listen);
        assert_eq!(e.proxy_url, b.proxy_url);
    }
}
