//! Price table and the per-request cost formula.
//!
//! Costs are **notional** for subscription (ChatGPT plan) traffic: they show what the same
//! tokens would cost on the API, not what was billed.
//!
//! The table is a local TOML file (`[usage] prices`). It is filled and updated from a public
//! price dataset on request only ([`parse_source`] → [`diff`] → [`apply`]): LiteLLM's
//! `model_prices_and_context_window.json` or models.dev's `api.json`.
//!
//! ```text
//! cost = (in − cached − cache_write)·P_in + cached·P_cache + cache_write·P_write + out·P_out
//! ```
//! `out` already contains reasoning tokens (not added again). Long context: when total input
//! (incl. cached) > `long.threshold`, the three input-side prices × `long.in_x`, output ×
//! `long.out_x`. Tier: an explicit entry for the tier wins, else the standard entry ×
//! multiplier (`flex_x`, `fast_x`, `ultra_x`; per model or `[defaults]`); no multiplier ⇒ the
//! tier is unpriced. Dated entries
//! (`effective_from` / `effective_until`, inclusive, UTC) win over undated ones on the
//! request's date. No match ⇒ `None` (never 0).
//!
//! Two local overrides the dataset never changes ([`diff`] skips them):
//! - `ignore = ["auto", …]`: models that are never priced (free routers, local aliases).
//!   They cost exactly 0 on every tier and date and do not count as unpriced. An exact 0
//!   (not "excluded") keeps totals exact: excluding them would still mark every total that
//!   contains them as a lower bound (≥).
//! - `custom = true` on an entry: a price set by hand on the management page.
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::Path;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "kebab-case")]
pub enum Tier {
    #[default]
    Standard,
    Flex,
    /// "Fast mode" (formerly priority processing).
    #[serde(alias = "priority", alias = "fast")]
    PriorityFast,
    /// Ultrafast mode (select models).
    Ultrafast,
}
impl Tier {
    /// From a request / response `service_tier` value.
    pub fn from_service_tier(s: Option<&str>) -> Tier {
        match s.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
            Some("flex") => Tier::Flex,
            Some("priority" | "fast" | "priority-fast") => Tier::PriorityFast,
            Some("ultrafast") => Tier::Ultrafast,
            _ => Tier::Standard,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Tier::Standard => "standard",
            Tier::Flex => "flex",
            Tier::PriorityFast => "priority-fast",
            Tier::Ultrafast => "ultrafast",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Long {
    /// Total input tokens (incl. cached) above which long-context prices apply.
    pub threshold: i64,
    pub in_x: f64,
    pub out_x: f64,
}

/// One `[[model]]` entry. Prices in USD per 1M tokens.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    pub model: String,
    #[serde(default, skip_serializing_if = "is_std")]
    pub tier: Tier,
    #[serde(rename = "in")]
    pub input: f64,
    /// Cached input (cache read). Missing ⇒ input price.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<f64>,
    /// Cache write. Missing ⇒ input price.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write: Option<f64>,
    pub out: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub long: Option<Long>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flex_x: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_x: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ultra_x: Option<f64>,
    /// `YYYY-MM-DD`, inclusive (UTC).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_from: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_until: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Set by hand (management page): price syncs never change or report it.
    #[serde(default, skip_serializing_if = "is_false")]
    pub custom: bool,
}
fn is_false(b: &bool) -> bool {
    !*b
}
fn is_std(t: &Tier) -> bool {
    *t == Tier::Standard
}
impl Entry {
    fn dated(&self) -> bool {
        self.effective_from.is_some() || self.effective_until.is_some()
    }
    fn active_on(&self, day: i64) -> bool {
        let ok = |d: &Option<String>, f: fn(i64, i64) -> bool| {
            d.as_deref().and_then(parse_date).is_none_or(|x| f(day, x))
        };
        ok(&self.effective_from, |d, x| d >= x) && ok(&self.effective_until, |d, x| d <= x)
    }
}

/// Tier multipliers for entries without their own. `None` ⇒ that tier is unpriced.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Defaults {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flex_x: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fast_x: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ultra_x: Option<f64>,
}
/// Flex ×0.5, Fast ×2 (the multipliers of current OpenAI models).
impl Default for Defaults {
    fn default() -> Self {
        Self {
            flex_x: Some(0.5),
            fast_x: Some(2.0),
            ultra_x: None,
        }
    }
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PriceTable {
    /// Models never priced: cost 0, never reported as unpriced (lowercase).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ignore: Vec<String>,
    pub defaults: Defaults,
    #[serde(rename = "model")]
    pub models: Vec<Entry>,
}

/// Tokens of one request.
#[derive(Debug, Clone, Copy, Default)]
pub struct Usage {
    pub input: i64,
    pub cached: i64,
    pub cache_write: i64,
    pub output: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum PriceError {
    #[error("prices: {0}")]
    Io(#[from] std::io::Error),
    #[error("prices: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("prices: {0}")]
    Invalid(String),
}

impl PriceTable {
    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }
    pub fn parse(s: &str) -> Result<Self, PriceError> {
        let t: PriceTable = toml::from_str(s)?;
        for m in &t.ignore {
            check_model(m).map_err(PriceError::Invalid)?;
        }
        for e in &t.models {
            for d in [&e.effective_from, &e.effective_until]
                .into_iter()
                .flatten()
            {
                if parse_date(d).is_none() {
                    return Err(PriceError::Invalid(format!(
                        "{}: bad date {d:?} (YYYY-MM-DD)",
                        e.model
                    )));
                }
            }
            let ps = [
                Some(e.input),
                e.cache,
                e.write,
                Some(e.out),
                e.flex_x,
                e.fast_x,
                e.ultra_x,
                t.defaults.flex_x,
                t.defaults.fast_x,
                t.defaults.ultra_x,
            ];
            if ps.iter().flatten().any(|p| !p.is_finite() || *p < 0.0) {
                return Err(PriceError::Invalid(format!(
                    "{}: negative/invalid price",
                    e.model
                )));
            }
        }
        Ok(t)
    }
    /// Missing file ⇒ empty table (all costs null).
    pub fn load(p: &Path) -> Result<Self, PriceError> {
        match std::fs::read_to_string(p) {
            Ok(s) => Self::parse(&s),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e.into()),
        }
    }
    pub fn to_toml(&self) -> String {
        toml::to_string(self).unwrap_or_default()
    }

    /// On the `ignore` list (case-insensitive).
    pub fn is_ignored(&self, model: &str) -> bool {
        let m = model.trim();
        self.ignore.iter().any(|x| x.eq_ignore_ascii_case(m))
    }

    fn find(&self, model: &str, tier: Tier, day: i64) -> Option<&Entry> {
        let m = model.trim();
        let mut undated = None;
        for e in &self.models {
            if e.model.eq_ignore_ascii_case(m) && e.tier == tier && e.active_on(day) {
                if e.dated() {
                    return Some(e);
                }
                undated.get_or_insert(e);
            }
        }
        undated
    }

    /// USD for one request; `None` = unknown model/tier/date (caller flags `unknown_price`).
    /// Ignored models: `Some(0.0)`.
    pub fn cost(&self, model: Option<&str>, tier: Tier, ts_ms: i64, u: Usage) -> Option<f64> {
        let model = model?;
        if self.is_ignored(model) {
            return Some(0.0);
        }
        let day = ts_ms.div_euclid(86_400_000);
        let (e, mult) = match self.find(model, tier, day) {
            Some(e) => (e, 1.0),
            None => {
                let e = self.find(model, Tier::Standard, day)?;
                let x = match tier {
                    Tier::Standard => 1.0,
                    Tier::Flex => e.flex_x.or(self.defaults.flex_x)?,
                    Tier::PriorityFast => e.fast_x.or(self.defaults.fast_x)?,
                    Tier::Ultrafast => e.ultra_x.or(self.defaults.ultra_x)?,
                };
                (e, x)
            }
        };
        let (mut pi, mut pc, mut pw, mut po) = (
            e.input,
            e.cache.unwrap_or(e.input),
            e.write.unwrap_or(e.input),
            e.out,
        );
        if let Some(l) = e.long.filter(|l| u.input > l.threshold) {
            pi *= l.in_x;
            pc *= l.in_x;
            pw *= l.in_x;
            po *= l.out_x;
        }
        let cached = u.cached.max(0);
        let write = u.cache_write.max(0);
        let fresh = (u.input - cached - write).max(0) as f64;
        let usd = fresh * pi + cached as f64 * pc + write as f64 * pw + u.output.max(0) as f64 * po;
        Some(usd * mult / 1e6)
    }
}

/// `YYYY-MM-DD` → days since 1970-01-01.
pub fn parse_date(s: &str) -> Option<i64> {
    let mut it = s.trim().splitn(3, '-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let d: i64 = it.next()?.parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    // Howard Hinnant's days_from_civil
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146_097 + doe - 719_468)
}

// ---------------------------------------------------------------- price datasets

/// One price change proposed by a sync.
#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    Add(Box<Entry>),
    Update {
        old: Box<Entry>,
        new: Box<Entry>,
    },
    /// Local model has a dated (promo) entry: never touched automatically.
    SkipDated(String),
    /// Local model has a hand-set (`custom`) price: never touched automatically.
    SkipCustom(String),
    /// Model on the local `ignore` list: not added.
    SkipIgnored(String),
    /// Local undated entry not found in the source (kept).
    Missing(String),
}
impl Change {
    /// `Add` / `Update` (what a write would change).
    pub fn actionable(&self) -> bool {
        matches!(self, Change::Add(_) | Change::Update { .. })
    }
}

/// `model` or `model (tier)`.
pub fn label(e: &Entry) -> String {
    if e.tier == Tier::Standard {
        e.model.clone()
    } else {
        format!("{} ({})", e.model, e.tier.name())
    }
}

/// OpenAI text models a Codex client can use: `gpt-*`, `codex*`, `o<digit>*`, without
/// audio / realtime / image / search / embedding variants and dated snapshots.
pub fn relevant_model(id: &str) -> bool {
    let id = id.trim().to_ascii_lowercase();
    let family = id.starts_with("gpt-")
        || id.starts_with("codex")
        || (id.starts_with('o') && id[1..].starts_with(|c: char| c.is_ascii_digit()));
    let excluded = [
        "realtime",
        "audio",
        "image",
        "tts",
        "transcribe",
        "search",
        "embedding",
    ]
    .iter()
    .any(|x| id.contains(x));
    let dated = id.len() > 11 && {
        let tail = &id[id.len() - 11..];
        tail.starts_with('-') && parse_date(&tail[1..]).is_some()
    };
    family && !excluded && !dated && !id.contains('/')
}

fn r4(x: f64) -> f64 {
    (x * 10_000.0).round() / 10_000.0
}

/// USD per token → USD per 1M tokens, rounded to 1e-6 (drops float noise).
fn per_m(v: Option<&serde_json::Value>) -> Option<f64> {
    v.and_then(serde_json::Value::as_f64)
        .map(|x| (x * 1e12).round() / 1e6)
}

fn empty_entry(model: &str, tier: Tier, input: f64, out: f64) -> Entry {
    Entry {
        model: model.to_string(),
        tier,
        input,
        cache: None,
        write: None,
        out,
        long: None,
        flex_x: None,
        fast_x: None,
        ultra_x: None,
        effective_from: None,
        effective_until: None,
        note: None,
        custom: false,
    }
}

/// Upper bound for a hand-set price (USD per 1M tokens): catches a per-token value typed
/// into a per-million field the other way round, and typos.
pub const MAX_PRICE: f64 = 10_000.0;

/// A model id for `ignore` / a hand-set price: 1–128 chars, no control characters.
pub fn check_model(m: &str) -> Result<(), String> {
    let t = m.trim();
    if t.is_empty() {
        return Err("model is empty".into());
    }
    if t.chars().count() > 128 {
        return Err("model is longer than 128 characters".into());
    }
    if t.chars().any(char::is_control) {
        return Err("model contains control characters".into());
    }
    Ok(())
}

/// One edit from the management page.
#[derive(Debug, Clone, PartialEq)]
pub enum Edit {
    /// Stop pricing (cost 0, no unpriced warning).
    Ignore,
    Unignore,
    /// Hand-set standard price (USD / 1M): replaces the model's undated standard entry and
    /// takes it off the ignore list. Dated (promo) and tier entries stay.
    Set {
        input: f64,
        cache: Option<f64>,
        out: f64,
    },
    /// Remove a hand-set price (only `custom` entries).
    Unset,
}

impl PriceTable {
    /// Apply one edit; `Ok(false)` = nothing changed. The model is stored lowercase.
    pub fn edit(&mut self, model: &str, e: &Edit) -> Result<bool, String> {
        check_model(model)?;
        let m = model.trim().to_ascii_lowercase();
        let std_undated =
            |x: &Entry| x.model.eq_ignore_ascii_case(&m) && x.tier == Tier::Standard && !x.dated();
        match e {
            Edit::Ignore => {
                if self.is_ignored(&m) {
                    return Ok(false);
                }
                self.ignore.push(m);
                self.ignore.sort();
            }
            Edit::Unignore => {
                let n = self.ignore.len();
                self.ignore.retain(|x| !x.eq_ignore_ascii_case(&m));
                return Ok(self.ignore.len() != n);
            }
            Edit::Set { input, cache, out } => {
                let ok = |p: f64| p.is_finite() && (0.0..=MAX_PRICE).contains(&p);
                if !ok(*input) || !ok(*out) || cache.is_some_and(|c| !ok(c)) {
                    return Err(format!("prices must be numbers from 0 to {MAX_PRICE}"));
                }
                let mut n = empty_entry(&m, Tier::Standard, *input, *out);
                n.cache = *cache;
                n.custom = true;
                self.ignore.retain(|x| !x.eq_ignore_ascii_case(&m));
                match self.models.iter_mut().find(|x| std_undated(x)) {
                    Some(x) => {
                        n.note = x.note.take();
                        *x = n;
                    }
                    None => self.models.push(n),
                }
            }
            Edit::Unset => {
                let n = self.models.len();
                self.models.retain(|x| !(std_undated(x) && x.custom));
                if self.models.len() == n {
                    return Err(format!("{m} has no custom price"));
                }
            }
        }
        Ok(true)
    }
}

/// Entries from LiteLLM's `model_prices_and_context_window.json` (provider `openai`, chat /
/// responses models). Long context comes from `*_above_<N>k_tokens`; Fast / Flex / Ultrafast
/// from the `_priority` / `_flex` / `_ultrafast` prices: a multiplier when input and output
/// scale alike, else an explicit tier entry.
pub fn from_litellm(json: &serde_json::Value) -> Vec<Entry> {
    let mut out = vec![];
    let Some(obj) = json.as_object() else {
        return out;
    };
    for (id, m) in obj {
        let provider = m.get("litellm_provider").and_then(|v| v.as_str());
        let mode = m.get("mode").and_then(|v| v.as_str());
        if provider != Some("openai")
            || !matches!(mode, Some("chat" | "responses"))
            || !relevant_model(id)
        {
            continue;
        }
        let p = |k: &str| per_m(m.get(k));
        let (Some(pi), Some(po)) = (p("input_cost_per_token"), p("output_cost_per_token")) else {
            continue;
        };
        let id = id.to_ascii_lowercase();
        let mut e = empty_entry(&id, Tier::Standard, pi, po);
        e.cache = p("cache_read_input_token_cost");
        e.write = p("cache_creation_input_token_cost");
        // input_cost_per_token_above_272k_tokens (no tier suffix)
        let long_k = m.as_object().and_then(|o| {
            o.keys().find_map(|k| {
                k.strip_prefix("input_cost_per_token_above_")?
                    .strip_suffix("k_tokens")?
                    .parse::<i64>()
                    .ok()
            })
        });
        if let Some(n) = long_k {
            if let (Some(li), true) = (
                p(&format!("input_cost_per_token_above_{n}k_tokens")),
                pi > 0.0,
            ) {
                let lo = p(&format!("output_cost_per_token_above_{n}k_tokens"));
                e.long = Some(Long {
                    threshold: n * 1000,
                    in_x: r4(li / pi),
                    out_x: lo.filter(|_| po > 0.0).map_or(1.0, |lo| r4(lo / po)),
                });
            }
        }
        let mut extra = vec![];
        for (suf, tier) in [
            ("_priority", Tier::PriorityFast),
            ("_flex", Tier::Flex),
            ("_ultrafast", Tier::Ultrafast),
        ] {
            let (Some(ti), Some(to)) = (
                p(&format!("input_cost_per_token{suf}")),
                p(&format!("output_cost_per_token{suf}")),
            ) else {
                continue;
            };
            let (xi, xo) = (
                ti / pi.max(f64::MIN_POSITIVE),
                to / po.max(f64::MIN_POSITIVE),
            );
            if pi > 0.0 && po > 0.0 && (xi - xo).abs() <= 0.01 * xi {
                let x = Some(r4(xi));
                match tier {
                    Tier::PriorityFast => e.fast_x = x,
                    Tier::Flex => e.flex_x = x,
                    _ => e.ultra_x = x,
                }
            } else {
                let mut t = empty_entry(&id, tier, ti, to);
                t.cache = p(&format!("cache_read_input_token_cost{suf}"));
                t.write = p(&format!("cache_creation_input_token_cost{suf}"));
                t.long = e.long;
                extra.push(t);
            }
        }
        out.push(e);
        out.extend(extra);
    }
    out.sort_by_key(|e| (e.model.clone(), e.tier));
    out
}

/// Standard entries from a models.dev `api.json` for one provider (e.g. `openai`). Long
/// context from the `context` tier; Fast from `experimental.modes.fast.cost`.
pub fn from_models_dev(json: &serde_json::Value, provider: &str) -> Vec<Entry> {
    let mut out = vec![];
    let Some(models) = json
        .get(provider)
        .and_then(|p| p.get("models"))
        .and_then(|m| m.as_object())
    else {
        return out;
    };
    for (id, m) in models {
        if !relevant_model(id) {
            continue;
        }
        let Some(c) = m.get("cost") else { continue };
        let f = |k: &str| c.get(k).and_then(|v| v.as_f64());
        let (Some(input), Some(out_p)) = (f("input"), f("output")) else {
            continue;
        };
        let long = c
            .get("tiers")
            .and_then(|t| t.as_array())
            .and_then(|ts| {
                ts.iter()
                    .find(|t| t.pointer("/tier/type").and_then(|x| x.as_str()) == Some("context"))
            })
            .and_then(|t| {
                let size = t.pointer("/tier/size")?.as_i64()?;
                let ti = t.get("input")?.as_f64()?;
                let to = t.get("output")?.as_f64()?;
                (input > 0.0 && out_p > 0.0).then(|| Long {
                    threshold: size,
                    in_x: r4(ti / input),
                    out_x: r4(to / out_p),
                })
            });
        let fast_x = m
            .pointer("/experimental/modes/fast/cost/input")
            .and_then(|v| v.as_f64())
            .filter(|_| input > 0.0)
            .map(|x| r4(x / input));
        let mut e = empty_entry(&id.to_ascii_lowercase(), Tier::Standard, input, out_p);
        e.cache = f("cache_read");
        e.write = f("cache_write");
        e.long = long;
        e.fast_x = fast_x;
        out.push(e);
    }
    out.sort_by(|a, b| a.model.cmp(&b.model));
    out
}

/// Entries of a dataset: models.dev `api.json` (has `openai.models`) or LiteLLM (model →
/// `input_cost_per_token`). Only [`relevant_model`]s.
pub fn parse_source(json: &serde_json::Value) -> Result<Vec<Entry>, String> {
    let v = if json.pointer("/openai/models").is_some() {
        from_models_dev(json, "openai")
    } else if json
        .as_object()
        .is_some_and(|o| o.values().any(|v| v.get("input_cost_per_token").is_some()))
    {
        from_litellm(json)
    } else {
        return Err("unknown price dataset (expected LiteLLM model_prices_and_context_window.json or models.dev api.json)".into());
    };
    if v.is_empty() {
        return Err("no OpenAI model prices in the dataset".into());
    }
    let bad = v.iter().find(|e| {
        [
            Some(e.input),
            e.cache,
            e.write,
            Some(e.out),
            e.flex_x,
            e.fast_x,
            e.ultra_x,
        ]
        .iter()
        .flatten()
        .any(|p| !p.is_finite() || *p < 0.0)
    });
    if let Some(e) = bad {
        return Err(format!(
            "{}: negative/invalid price in the dataset",
            e.model
        ));
    }
    Ok(v)
}

/// Diff the local undated entries against `remote` (matched by model and tier). Local
/// per-model extras (multipliers the source does not give, `note`) survive an update; models
/// with a dated (promo) entry are never touched.
pub fn diff(local: &PriceTable, remote: &[Entry]) -> Vec<Change> {
    let mut ch = vec![];
    let custom: BTreeSet<String> = local
        .models
        .iter()
        .filter(|e| e.custom)
        .map(|e| e.model.to_ascii_lowercase())
        .collect();
    let dated: BTreeSet<String> = local
        .models
        .iter()
        .filter(|e| e.dated())
        .map(|e| e.model.to_ascii_lowercase())
        .collect();
    let mut skipped = BTreeSet::new();
    for r in remote {
        let key = r.model.to_ascii_lowercase();
        if local.is_ignored(&key) {
            if skipped.insert(key) {
                ch.push(Change::SkipIgnored(r.model.clone()));
            }
            continue;
        }
        if custom.contains(&key) {
            if skipped.insert(key) {
                ch.push(Change::SkipCustom(r.model.clone()));
            }
            continue;
        }
        if dated.contains(&key) {
            if skipped.insert(key) {
                ch.push(Change::SkipDated(r.model.clone()));
            }
            continue;
        }
        let cur = local
            .models
            .iter()
            .find(|e| e.model.eq_ignore_ascii_case(&r.model) && e.tier == r.tier && !e.dated());
        match cur {
            None => ch.push(Change::Add(Box::new(r.clone()))),
            Some(c) => {
                let mut n = r.clone();
                n.model = c.model.clone();
                n.flex_x = r.flex_x.or(c.flex_x);
                n.fast_x = r.fast_x.or(c.fast_x);
                n.ultra_x = r.ultra_x.or(c.ultra_x);
                n.note = c.note.clone();
                if n != *c {
                    ch.push(Change::Update {
                        old: Box::new(c.clone()),
                        new: Box::new(n),
                    });
                }
            }
        }
    }
    for e in local.models.iter().filter(|e| !e.dated() && !e.custom) {
        if !remote
            .iter()
            .any(|r| r.model.eq_ignore_ascii_case(&e.model) && r.tier == e.tier)
        {
            ch.push(Change::Missing(label(e)));
        }
    }
    ch
}

pub fn apply(local: &PriceTable, changes: &[Change]) -> PriceTable {
    let mut t = local.clone();
    for c in changes {
        match c {
            Change::Add(e) => t.models.push((**e).clone()),
            Change::Update { old, new } => {
                if let Some(x) = t.models.iter_mut().find(|x| **x == **old) {
                    *x = (**new).clone();
                }
            }
            Change::SkipDated(_)
            | Change::SkipCustom(_)
            | Change::SkipIgnored(_)
            | Change::Missing(_) => {}
        }
    }
    t
}

/// One-line price summary: `in 2 cache 0.2 write 2.5 out 10 long>272000: in×2 out×1.5 fast×2`.
pub fn fmt_entry(e: &Entry) -> String {
    let o = |x: Option<f64>| x.map_or("-".into(), |v| v.to_string());
    let mut s = format!(
        "in {} cache {} write {} out {}",
        e.input,
        o(e.cache),
        o(e.write),
        e.out
    );
    if let Some(l) = e.long {
        s += &format!(" long>{}: in×{} out×{}", l.threshold, l.in_x, l.out_x);
    }
    for (n, x) in [
        ("flex", e.flex_x),
        ("fast", e.fast_x),
        ("ultrafast", e.ultra_x),
    ] {
        if let Some(x) = x {
            s += &format!(" {n}×{x}");
        }
    }
    s
}

/// Human-readable diff (one line per change).
pub fn render_diff(changes: &[Change]) -> String {
    let mut s = String::new();
    for c in changes {
        let line = match c {
            Change::Add(e) => format!("+ {:<28} {}", label(e), fmt_entry(e)),
            Change::Update { old, new } => {
                format!(
                    "~ {:<28} {}\n  {:<28} → {}",
                    label(old),
                    fmt_entry(old),
                    "",
                    fmt_entry(new)
                )
            }
            Change::SkipDated(m) => format!("! {m:<28} has dated/promo entry locally; not touched"),
            Change::SkipCustom(m) => format!("! {m:<28} has a custom price locally; not touched"),
            Change::SkipIgnored(m) => format!("! {m:<28} is ignored locally; not added"),
            Change::Missing(m) => format!("? {m:<28} not in source; kept"),
        };
        s.push_str(&line);
        s.push('\n');
    }
    s
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    const T: &str = r#"
[defaults]
flex_x = 0.5
fast_x = 2.0

[[model]]
model = "m"
in = 2.0
cache = 0.2
write = 2.5
out = 12.0
long = { threshold = 272000, in_x = 2.0, out_x = 1.5 }

[[model]]
model = "fx"
in = 5.0
cache = 0.5
out = 30.0
fast_x = 2.5

[[model]]
model = "fx"
tier = "flex"
in = 1.0
out = 1.0

[[model]]
model = "promo"
in = 4.0
cache = 0.4
out = 20.0
effective_until = "2026-11-21"

[[model]]
model = "promo"
in = 9.0
out = 90.0
effective_from = "2026-11-22"
"#;

    fn ms(d: &str) -> i64 {
        parse_date(d).unwrap() * 86_400_000
    }
    fn close(a: Option<f64>, b: f64) {
        let a = a.unwrap();
        assert!((a - b).abs() < 1e-12, "{a} vs {b}");
    }

    #[test]
    fn formula_cached_write_reasoning_in_out() {
        let t = PriceTable::parse(T).unwrap();
        let u = Usage {
            input: 200_000,
            cached: 120_000,
            cache_write: 20_000,
            output: 100_000,
        };
        // 60k·2 + 120k·0.2 + 20k·2.5 + 100k·12 (reasoning already in out)
        close(
            t.cost(Some("m"), Tier::Standard, 0, u),
            (60_000.0 * 2.0 + 120_000.0 * 0.2 + 20_000.0 * 2.5 + 100_000.0 * 12.0) / 1e6,
        );
        // missing write price ⇒ input price
        let u2 = Usage {
            input: 100_000,
            cached: 0,
            cache_write: 100_000,
            output: 0,
        };
        close(t.cost(Some("fx"), Tier::Standard, 0, u2), 0.5);
        // case-insensitive model
        assert!(t.cost(Some("M"), Tier::Standard, 0, u).is_some());
    }

    #[test]
    fn long_context_threshold() {
        let t = PriceTable::parse(T).unwrap();
        let at = Usage {
            input: 272_000,
            cached: 72_000,
            cache_write: 0,
            output: 1_000,
        };
        close(
            t.cost(Some("m"), Tier::Standard, 0, at),
            (200_000.0 * 2.0 + 72_000.0 * 0.2 + 1_000.0 * 12.0) / 1e6,
        );
        let over = Usage {
            input: 272_001,
            ..at
        };
        close(
            t.cost(Some("m"), Tier::Standard, 0, over),
            (200_001.0 * 4.0 + 72_000.0 * 0.4 + 1_000.0 * 18.0) / 1e6,
        );
    }

    #[test]
    fn tiers() {
        let t = PriceTable::parse(T).unwrap();
        let u = Usage {
            input: 100_000,
            cached: 0,
            cache_write: 0,
            output: 100_000,
        };
        close(t.cost(Some("m"), Tier::Flex, 0, u), 1.4 * 0.5); // default flex ×0.5
        close(t.cost(Some("m"), Tier::PriorityFast, 0, u), 1.4 * 2.0); // default fast ×2
        close(t.cost(Some("fx"), Tier::PriorityFast, 0, u), 3.5 * 2.5); // per-model fast
        close(t.cost(Some("fx"), Tier::Flex, 0, u), 0.2); // explicit tier entry wins
        assert_eq!(
            Tier::from_service_tier(Some("priority")),
            Tier::PriorityFast
        );
        assert_eq!(Tier::from_service_tier(Some("auto")), Tier::Standard);
        assert_eq!(Tier::from_service_tier(None), Tier::Standard);
    }

    #[test]
    fn promo_date_boundary() {
        let t = PriceTable::parse(T).unwrap();
        let u = Usage {
            input: 1_000_000,
            cached: 0,
            cache_write: 0,
            output: 0,
        };
        close(
            t.cost(
                Some("promo"),
                Tier::Standard,
                ms("2026-11-21") + 86_399_999,
                u,
            ),
            4.0,
        );
        close(
            t.cost(Some("promo"), Tier::Standard, ms("2026-11-22"), u),
            9.0,
        );
        let only_promo = PriceTable::parse(
            "[[model]]\nmodel=\"p\"\nin=1.0\nout=1.0\neffective_until=\"2026-11-21\"\n",
        )
        .unwrap();
        assert!(only_promo
            .cost(Some("p"), Tier::Standard, ms("2026-11-22"), u)
            .is_none());
        assert!(PriceTable::parse(
            "[[model]]\nmodel=\"p\"\nin=1.0\nout=1.0\neffective_until=\"21.11.2026\"\n"
        )
        .is_err());
        assert_eq!(parse_date("1970-01-01"), Some(0));
        assert_eq!(parse_date("2026-11-21"), Some(20_778));
    }

    #[test]
    fn unknown_is_none_not_zero() {
        let t = PriceTable::parse(T).unwrap();
        let u = Usage {
            input: 10,
            cached: 0,
            cache_write: 0,
            output: 10,
        };
        assert!(t.cost(Some("nope"), Tier::Standard, 0, u).is_none());
        assert!(t.cost(None, Tier::Standard, 0, u).is_none());
        assert!(PriceTable::default()
            .cost(Some("m"), Tier::Standard, 0, u)
            .is_none());
    }

    #[test]
    fn litellm_mapping() {
        let j: serde_json::Value = serde_json::from_str(
            r#"{"sample_spec":{"input_cost_per_token":0},
            "gpt-6-sol":{"litellm_provider":"openai","mode":"chat",
              "input_cost_per_token":2e-06,"cache_read_input_token_cost":2e-07,
              "cache_creation_input_token_cost":2.5e-06,"output_cost_per_token":1e-05,
              "input_cost_per_token_above_272k_tokens":4e-06,"output_cost_per_token_above_272k_tokens":1.5e-05,
              "input_cost_per_token_priority":4e-06,"output_cost_per_token_priority":2e-05,
              "input_cost_per_token_flex":1e-06,"output_cost_per_token_flex":5e-06,
              "input_cost_per_token_above_272k_tokens_flex":2e-06},
            "gpt-odd":{"litellm_provider":"openai","mode":"responses",
              "input_cost_per_token":1e-06,"output_cost_per_token":1e-05,
              "input_cost_per_token_priority":3e-06,"output_cost_per_token_priority":2e-05,
              "cache_read_input_token_cost_priority":3e-07},
            "gpt-4o-2024-05-13":{"litellm_provider":"openai","mode":"chat","input_cost_per_token":5e-06,"output_cost_per_token":1.5e-05},
            "gpt-realtime":{"litellm_provider":"openai","mode":"chat","input_cost_per_token":4e-06,"output_cost_per_token":1.6e-05},
            "azure/gpt-6-sol":{"litellm_provider":"azure","mode":"chat","input_cost_per_token":2e-06,"output_cost_per_token":1e-05},
            "text-embedding-3-small":{"litellm_provider":"openai","mode":"embedding","input_cost_per_token":2e-08,"output_cost_per_token":0}}"#,
        )
        .unwrap();
        let v = parse_source(&j).unwrap();
        let names: Vec<String> = v.iter().map(label).collect();
        assert_eq!(names, ["gpt-6-sol", "gpt-odd", "gpt-odd (priority-fast)"]);
        let s = &v[0];
        assert_eq!(
            (s.input, s.cache, s.write, s.out),
            (2.0, Some(0.2), Some(2.5), 10.0)
        );
        assert_eq!(
            s.long,
            Some(Long {
                threshold: 272_000,
                in_x: 2.0,
                out_x: 1.5
            })
        );
        assert_eq!(
            (s.fast_x, s.flex_x, s.ultra_x),
            (Some(2.0), Some(0.5), None)
        );
        // input ×3 / output ×2 ⇒ explicit Fast entry
        assert_eq!(
            (v[1].fast_x, v[2].input, v[2].cache, v[2].out),
            (None, 3.0, Some(0.3), 20.0)
        );
        let t = PriceTable {
            models: v,
            ..Default::default()
        };
        let u = Usage {
            input: 1_000_000,
            cached: 0,
            cache_write: 0,
            output: 0,
        };
        close(t.cost(Some("gpt-odd"), Tier::PriorityFast, 0, u), 3.0);
        assert!(
            t.cost(Some("gpt-6-sol"), Tier::Ultrafast, 0, u).is_none(),
            "no ultrafast price"
        );
        assert!(relevant_model("o4-mini") && relevant_model("gpt-5.3-codex"));
        assert!(!relevant_model("omni-moderation-latest") && !relevant_model("gpt-image-2"));
        assert!(parse_source(&serde_json::json!({"x": 1})).is_err());
    }

    #[test]
    fn models_dev_and_diff() {
        let j: serde_json::Value = serde_json::from_str(
            r#"{"openai":{"models":{
            "m":{"cost":{"input":2,"output":13,"cache_read":0.2,"cache_write":2.5,
                 "tiers":[{"input":4,"output":19.5,"tier":{"type":"context","size":272000}}]},
                 "experimental":{"modes":{"fast":{"cost":{"input":4,"output":26}}}}},
            "gpt-new":{"cost":{"input":1,"output":2}},
            "promo":{"cost":{"input":4,"output":20}},
            "nocost":{}}}}"#,
        )
        .unwrap();
        // "m" / "promo" / "nocost" are not gpt-* names: only relevant models are read
        assert_eq!(from_models_dev(&j, "openai").len(), 1);
        let j = serde_json::to_string(&j)
            .unwrap()
            .replace("\"m\"", "\"gpt-m\"")
            .replace("\"promo\"", "\"gpt-promo\"");
        let r = parse_source(&serde_json::from_str(&j).unwrap()).unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].fast_x, Some(2.0));
        let t = PriceTable::parse(
            &T.replace("\"m\"", "\"gpt-m\"")
                .replace("\"promo\"", "\"gpt-promo\""),
        )
        .unwrap();
        let d = diff(&t, &r);
        assert!(d
            .iter()
            .any(|c| matches!(c, Change::Add(e) if e.model == "gpt-new")));
        assert!(d
            .iter()
            .any(|c| matches!(c, Change::SkipDated(m) if m == "gpt-promo")));
        assert!(d
            .iter()
            .any(|c| matches!(c, Change::Missing(m) if m == "fx")));
        assert!(d
            .iter()
            .any(|c| matches!(c, Change::Missing(m) if m == "fx (flex)")));
        let up = d
            .iter()
            .find_map(|c| match c {
                Change::Update { new, .. } => Some((**new).clone()),
                _ => None,
            })
            .unwrap();
        assert_eq!((up.out, up.long.unwrap().out_x), (13.0, 1.5));
        let t2 = apply(&t, &d);
        let back = PriceTable::parse(&t2.to_toml()).unwrap();
        assert_eq!(back, t2);
        assert!(diff(&back, &r).iter().all(|c| !c.actionable()));
        assert!(render_diff(&d).contains("+ gpt-new"));
    }

    #[test]
    fn ignore_and_custom_edits() {
        let mut t = PriceTable::parse(T).unwrap();
        let u = Usage {
            input: 1000,
            cached: 0,
            cache_write: 0,
            output: 1000,
        };
        assert!(t.cost(Some("auto"), Tier::Standard, 0, u).is_none());
        assert_eq!(t.edit(" Auto ", &Edit::Ignore), Ok(true));
        assert_eq!(t.edit("AUTO", &Edit::Ignore), Ok(false), "idempotent");
        assert_eq!(t.ignore, ["auto"]);
        for tier in [
            Tier::Standard,
            Tier::Flex,
            Tier::PriorityFast,
            Tier::Ultrafast,
        ] {
            assert_eq!(
                t.cost(Some("auto"), tier, 0, u),
                Some(0.0),
                "exact 0 on {tier:?}"
            );
        }
        // round trip: `ignore` written before the tables, read back
        let back = PriceTable::parse(&t.to_toml()).unwrap();
        assert_eq!(back, t);
        assert!(t.to_toml().starts_with("ignore = [\"auto\"]"));
        assert_eq!(t.edit("auto", &Edit::Unignore), Ok(true));
        assert_eq!(t.edit("auto", &Edit::Unignore), Ok(false));
        assert!(t.cost(Some("auto"), Tier::Standard, 0, u).is_none());

        // custom price: new model, then replacing an undated standard entry (promo stays)
        let set = |i: f64, o: f64| Edit::Set {
            input: i,
            cache: None,
            out: o,
        };
        t.edit("auto", &Edit::Ignore).unwrap();
        assert_eq!(t.edit("auto", &set(1.0, 2.0)), Ok(true));
        assert!(
            t.ignore.is_empty(),
            "a price takes the model off the ignore list"
        );
        close(t.cost(Some("auto"), Tier::Standard, 0, u), 0.003);
        let n = t.models.len();
        t.edit("m", &set(3.0, 4.0)).unwrap();
        assert_eq!(t.models.len(), n, "replaced, not added");
        let m = t.models.iter().find(|e| e.model == "m").unwrap();
        assert!(m.custom && m.long.is_none() && m.cache.is_none());
        let back = PriceTable::parse(&t.to_toml()).unwrap();
        assert_eq!(back, t);
        // a sync never touches custom or ignored models and does not report them missing
        t.edit("zz-free", &Edit::Ignore).unwrap();
        let remote = vec![
            empty_entry("m", Tier::Standard, 9.0, 9.0),
            empty_entry("zz-free", Tier::Standard, 9.0, 9.0),
        ];
        let ch = diff(&t, &remote);
        assert!(ch.contains(&Change::SkipCustom("m".into())));
        assert!(ch.contains(&Change::SkipIgnored("zz-free".into())));
        assert!(!ch.iter().any(|c| c.actionable()));
        assert!(!ch.contains(&Change::Missing("auto".into())));
        assert_eq!(apply(&t, &ch), t);
        // unset: only custom entries
        assert_eq!(t.edit("auto", &Edit::Unset), Ok(true));
        assert!(t.edit("fx", &Edit::Unset).is_err(), "not custom");
        assert!(t.cost(Some("auto"), Tier::Standard, 0, u).is_none());
        // validation
        for bad in ["", "  ", "a\nb", &"x".repeat(129)] {
            assert!(t.edit(bad, &Edit::Ignore).is_err(), "{bad:?}");
        }
        for (i, o) in [(-1.0, 1.0), (1.0, f64::NAN), (1.0, MAX_PRICE + 1.0)] {
            assert!(t.edit("q", &set(i, o)).is_err());
        }
        assert!(PriceTable::parse("ignore = [\"\"]").is_err());
    }
}
