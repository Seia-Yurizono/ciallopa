//! Live configuration: an atomically swappable snapshot of [`Config`], the settings schema
//! (which keys apply hot and which need a restart, which the management page may edit) and
//! helpers to flatten, diff and mask configs for the admin API.
use crate::config::{ApiKey, Config};
use arc_swap::ArcSwap;
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

/// The running configuration. Each [`SharedConfig::load`] returns a complete snapshot;
/// a reload swaps the pointer atomically. Retain request-scoped options explicitly across
/// awaits; live routing/error policies may read again at their documented boundaries.
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
    /// Short label.
    pub desc: &'static str,
    /// One-line helper shown under the label: what it does, units, ranges, `0 = …`.
    pub help: &'static str,
    /// Per-option label and description for `enum` kinds that need them (same order as `options`).
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    pub choices: &'static [Choice],
}

/// One option of an `enum` setting as the management page presents it.
#[derive(Debug, Clone, Serialize)]
pub struct Choice {
    pub value: &'static str,
    pub label: &'static str,
    pub help: &'static str,
}

const fn c(value: &'static str, label: &'static str, help: &'static str) -> Choice {
    Choice { value, label, help }
}

const fn f(
    key: &'static str,
    kind: &'static str,
    effect: Effect,
    editable: bool,
    desc: &'static str,
    help: &'static str,
) -> Field {
    Field {
        key,
        kind,
        effect,
        editable,
        secret: false,
        options: &[],
        desc,
        help,
        choices: &[],
    }
}

use Effect::{Hot, Restart};

/// Every config key, in file order.
pub const SCHEMA: &[Field] = &[
    f("listen", "string", Restart, false, "监听地址", "ciallopa 接收客户端请求的地址和端口"),
    Field { secret: true, ..f("api_keys", "keys", Hot, false, "客户端访问密钥", "页面上只能删除或生成新密钥") },
    f("instance_id", "string", Restart, true, "实例名", "写入每条用量记录，多个实例共用一个用量数据库时用来区分"),
    f("upstream", "string", Restart, true, "上游地址", "ChatGPT 后端地址，一般不需要改"),
    f("auth_dir", "path", Restart, false, "账号文件目录", "每个账号一个 JSON 文件，目录有变化时自动重新扫描"),
    Field { secret: true, ..f("proxy_url", "string", Hot, true, "出口代理地址", "支持 http://、https://、socks5:// 和 socks5h://（socks5 也由代理解析域名），留空 = 直连") },
    f("proxy_enabled", "bool", Hot, true, "启用出口代理", "关闭时保留代理地址，改为直连"),
    f("ipv6", "bool", Hot, true, "允许 IPv6 出口", "开启时 IPv6 与 IPv4 竞速连接，关闭时只用 IPv4；直连和连接代理主机都适用"),
    f("max_body_mb", "int", Hot, true, "请求体上限（MiB）", "取值 1–4096 MiB，超过直接拒绝，下一个请求生效"),
    f("max_buffered_body_mb", "int", Hot, true, "请求体缓存总量（MiB）", "所有转发中的 HTTP 请求体和 WebSocket 消息合计最多占用这么多内存，HTTP 下进程实际占用约为它的 2 倍。没有空间时新请求最多等 1 秒，仍不够就返回 503，WebSocket 收到同样的错误帧，连接保持。0 = 不限，否则不能小于请求体上限。"),
    f("max_inflight_requests", "int", Hot, true, "转发在途上限", "同时转发的 HTTP 请求和 WebSocket 连接数上限：HTTP 占用到响应结束，WebSocket 占用到连接关闭（含空闲）。达到上限时新请求直接返回 503，不排队；调低不会中断已有请求，0 = 不限。"),
    f("ws_compression", "bool", Hot, true, "客户端 WebSocket 压缩", "客户端请求 permessage-deflate 时启用（Codex 总会请求），对新连接生效"),
    Field { options: &["round-robin", "weighted-round-robin", "fill-first"], choices: &[c("round-robin", "轮询", "按账号名顺序轮流分配，每个模型各自轮转；不看权重，适合让各账号用量均匀"), c("weighted-round-robin", "加权轮询", "按账号权重的比例分配，权重 0 的账号不参与；适合额度不同的账号按比例分担"), c("fill-first", "用满再换", "总是用按账号名排第一的可用账号，它冷却或停用后才轮到下一个；不看权重，适合集中消耗一个账号、其余留作备用")], ..f("routing.strategy", "enum", Hot, true, "选号策略", "新会话和没有会话标识的请求怎么挑账号。先去掉停用、冷却中的账号，再只在优先级最高的一档里按策略选；已绑定的会话只要原账号可用就继续用它。") },
    f("routing.session_affinity", "bool", Hot, true, "会话亲和", "同一会话的后续请求固定用同一个账号（按会话和模型绑定）。关闭后每个请求都重新选号，对话中途换号时上游可能拒绝历史里的加密推理。"),
    f("routing.session_affinity_ttl", "duration", Hot, true, "亲和保持时长", "会话这么久没有请求就解除绑定，每次请求重新计时；格式如 90s、30m、24h"),
    f("routing.session_affinity_subagents", "bool", Hot, true, "子代理沿用父会话账号", "Codex 子代理使用父会话绑定的账号；关闭则子代理按选号策略另选账号"),
    Field { options: &["quota", "any"], choices: &[c("quota", "仅额度或令牌问题", "原账号额度用尽、令牌失效、402 或被停用时才换绑；短暂故障（如连续 5xx）期间临时借用其他账号，恢复后回到原账号"), c("any", "任何不可用", "原账号因任何原因不能用就换绑到新账号，之后留在新账号")], ..f("routing.session_affinity_rebind_on", "enum", Hot, true, "何时换绑账号", "绑定的账号暂时不能用时，这次请求总会改用其他账号；这里决定绑定是否跟着换") },
    f("routing.quota_avoid_threshold", "float", Hot, true, "额度回避阈值", "取值 0–1，0 = 关闭。新会话跳过最近一次观测到已用比例（任一未重置的额度窗口）达到该值的账号，全部达到时不回避；已绑定且可用的会话不受影响。"),
    f("routing.cooldown_wait_secs", "int", Hot, true, "全部冷却时最长等待（秒）", "所有账号都在冷却、且有账号会在这段时间内恢复时，请求等它恢复（WebSocket 握手最多 10 秒）；最大 120，0 = 不等待，直接返回错误"),
    f("upstream_opts.stream_idle_timeout_secs", "int", Hot, true, "流空闲超时（秒）", "上游流式响应这么久没有新数据就断开"),
    f("upstream_opts.response_header_timeout_secs", "int", Hot, true, "响应头等待上限（秒）", "每次 HTTP 尝试从发送请求（含上传请求体）到收到响应头的最长时间，超时按网络错误换号、不冷却账号；只用于 HTTP，不限制之后的流式生成。取值 1–86400。"),
    f("upstream_opts.preparation_timeout_secs", "int", Hot, true, "准备阶段总预算（秒）", "一个请求在重试和换号上累计可花的时间：获取令牌、等待响应头、读取错误、冷却等待和重试等待都计入，正常生成不计入；用完后不再重试，返回 504。WebSocket 握手和每个 turn 各有一份预算。取值 1–86400。"),
    f("upstream_opts.max_attempts", "int", Hot, true, "换号重试次数", "首字节前遇到网络错误、408 或 5xx 时最多换几次账号，不重试同一账号；401/402/403/404/429 换号不占次数；WebSocket 握手另有固定的 4 次重试"),
    f("upstream_opts.transient_cooldown_secs", "int", Hot, true, "5xx 冷却（秒）", "同一账号连续 3 次 5xx 后冷却这么久（上游 Retry-After 优先，最长 60 秒），最后一个可用账号不冷却；0 = 不冷却"),
    f("upstream_opts.overload_streak", "int", Hot, true, "overloaded 冷却阈值（次）", "同一账号连续这么多次 overloaded 后开始冷却，最后一个可用账号不冷却；0 = 不冷却"),
    f("upstream_opts.overload_cooldown_secs", "int", Hot, true, "overloaded 冷却起始（秒）", "之后每次翻倍直到上限，有正常输出后重置"),
    f("upstream_opts.overload_cooldown_max_secs", "int", Hot, true, "overloaded 冷却上限（秒）", "翻倍后的最长冷却时间"),
    f("upstream_opts.overload_retry_wait_secs", "int", Hot, true, "overloaded 同号重试等待（秒）", "没有其他可用账号或全部 overloaded 时，等待约这么久后在同一账号重试 1 次；0 = 不重试"),
    f("upstream_opts.deactivated_cooldown_secs", "int", Hot, true, "402 工作区停用冷却（秒）", "上游返回 402 deactivated_workspace 时换号，并让该账号冷却这么久；0 = 不冷却"),
    f("upstream_opts.payment_required_cooldown_secs", "int", Hot, true, "其他 402 冷却（秒）", "付费或账单问题的 402：换号，并让该账号冷却这么久；0 = 不冷却"),
    f("upstream_opts.not_found_cooldown_secs", "int", Hot, true, "404 模型冷却（秒）", "上游 404 且没有明确说模型不支持时，该账号的这个模型冷却这么久（明确不支持固定 12 小时）；0 = 只换号不冷却"),
    f("upstream_opts.connect_timeout_secs", "int", Hot, true, "连接超时（秒）", "连接上游或代理超过这么久算网络错误"),
    f("upstream_opts.compress_requests", "bool", Hot, true, "压缩请求体（zstd）", "用 zstd 压缩发往上游的 /responses 请求体，与官方 Codex 一致；WebSocket 和其他接口不压缩"),
    f("upstream_opts.cloudflare_cookies", "bool", Hot, true, "按账号保存 Cloudflare cookie", "在内存中按账号保存上游基础设施 cookie，只在该账号的请求里回放，账号之间不共享"),
    f("images.enabled", "bool", Hot, true, "图片接口", "开放 /v1/images/generations 和 /v1/images/edits"),
    f("images.main_model", "string", Hot, true, "图片接口主模型", "非 gpt-image-* 的图片请求用这个模型包装"),
    f("images.default_originator", "string", Hot, true, "默认 originator", "只用于没有发送 originator 的非 Codex 客户端"),
    f("images.default_user_agent", "string", Hot, true, "默认 User-Agent", "客户端 User-Agent 不以 codex 开头时使用"),
    Field { options: &["error", "warn", "info", "debug", "trace"], ..f("log.level", "enum", Hot, true, "日志级别", "设置了环境变量 RUST_LOG 时以它为准") },
    f("usage.enabled", "bool", Restart, true, "用量日志", "记录每个请求的账号、模型、token 和状态，概览页的数据都来自这里"),
    f("usage.path", "path", Restart, false, "用量数据库文件", "SQLite 文件，旁边还有 -wal 和 -shm 文件"),
    f("usage.retention_days", "int", Hot, true, "明细保留天数", "更早的请求明细自动清除；0 = 永久保留"),
    f("usage.queue_size", "int", Restart, true, "写入队列长度", "代理与写入线程之间最多缓存的记录数，队列满时新记录会被丢弃"),
    f("usage.batch_size", "int", Restart, true, "每批写入行数", "每个数据库事务最多写入的记录数"),
    f("usage.flush_ms", "int", Restart, true, "写入最长等待（毫秒）", "不满一批时最多等这么久就写入"),
    f("usage.quota_weights.input", "float", Hot, true, "额度估算权重：未缓存输入", "估算每 1% 额度对应多少 token 时，每个未缓存输入 token 的权重"),
    f("usage.quota_weights.cached", "float", Hot, true, "额度估算权重：缓存输入", "每个缓存命中的输入 token 的权重"),
    f("usage.quota_weights.output", "float", Hot, true, "额度估算权重：输出", "每个输出 token（含推理）的权重"),
    f("usage.quota_weights.min_delta_pct", "float", Hot, true, "额度估算最小变化（%）", "当前窗口内已用比例变化小于这个值时不给出估算"),
    f("usage.prices", "path", Hot, false, "价格表文件", "内容变化时自动重新加载"),
    f("usage.prices_source", "string", Hot, true, "价格数据来源", "点「检查更新」时从这里下载，留空 = 不检查"),
    f("ui.enabled", "bool", Restart, false, "管理页", "关闭后 /ui 的三个页面都不可用"),
    f("ui.allow_reset_consume", "bool", Hot, false, "允许手动使用重置卡", "关闭时页面不显示使用按钮，接口也会拒绝"),
    f("ui.reset_credits_refresh_mins", "int", Hot, true, "重置卡后台刷新间隔（分钟）", "后台定期向上游只读查询各账号的重置卡；0 = 只在手动刷新时查询"),
    f("ui.allow_config_write", "bool", Hot, false, "允许在管理页修改", "开启后可在管理页修改配置和账号"),
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
    fn every_field_is_explained() {
        for f in SCHEMA {
            assert!(!f.help.is_empty(), "{} has no help", f.key);
            for t in [f.desc, f.help]
                .into_iter()
                .chain(f.choices.iter().map(|c| c.help))
            {
                // one sentence: no trailing 。; several: every one ends with 。
                let n = t.matches('。').count();
                assert!(n == 0 || (t.ends_with('。') && n >= 2), "{}: {t}", f.key);
            }
            if !f.choices.is_empty() {
                let v: Vec<&str> = f.choices.iter().map(|c| c.value).collect();
                assert_eq!(v, f.options, "{}", f.key);
            }
        }
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
